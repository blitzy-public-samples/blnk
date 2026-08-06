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
# Provisions a RUNNING Kafka broker for Blnk event streaming. Three things are created,
# in this order: the eight topics, one sample subscriber principal, and that principal's
# ACLs. That is the whole job.
#
# WHAT IS CREATED
#
# Four category topics and their four dead-letter siblings, every name derived from
# KAFKA_TOPIC_PREFIX (default "blnk"):
#
#     <prefix>.transactions        <prefix>.transactions.dlt
#     <prefix>.balances            <prefix>.balances.dlt
#     <prefix>.identities          <prefix>.identities.dlt
#     <prefix>.system              <prefix>.system.dlt
#
# Only the four CATEGORY names are written down below; each dead-letter name is derived by
# appending ".dlt", exactly as event_topics.go's DLTFor does. Deriving rather than listing
# is what structurally prevents the two halves of the catalogue drifting apart.
#
# There are four categories rather than the three named in the requirement because two real
# event types - ledger.created and system.error - belong to none of transactions, balances
# or identities, while the requirement also demands that every event formerly delivered by
# webhook be published. The fourth category resolves that tension following the identical
# naming convention, so no event type is silently dropped. Do not "correct" this to six
# topics: model.EventCategory routes events into four categories and event_topics.go
# composes four topic names from them, and a name this script does not create is a name the
# relay cannot publish to.
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
# --if-not-exists, ACL addition is a no-op when the binding already exists, and the SCRAM
# credential is an upsert. A topic with too FEW partitions is grown; a topic with MORE is
# left alone with a warning, because Kafka cannot reduce a partition count and doing so
# would move keys between partitions and break the per-aggregate ordering guarantee that
# keying by ledger ID exists to provide.
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
#   KAFKA_MIN_PARTITIONS                  6
#   KAFKA_REPLICATION_FACTOR              1                     1 locally, 3 in production
#   KAFKA_SASL_ADMIN_USER                 admin
#   KAFKA_SASL_ADMIN_SECRET               (required, no default)
#   KAFKA_SECURITY_PROTOCOL               SASL_PLAINTEXT        SASL_SSL in production
#   KAFKA_CLIENT_CONFIG                   (unset)               use this properties file
#                                                               verbatim instead of writing
#                                                               a temporary one
#   KAFKA_SCRAM_ITERATIONS                4096                  the SCRAM minimum
#   KAFKA_PROVISION_TIMEOUT_SECONDS       60                    readiness budget
#   KAFKA_PROVISION_POLL_INTERVAL_SECONDS 2                     readiness poll interval
#   KAFKA_SAMPLE_SUBSCRIBER_USER          blnk-sample-subscriber
#   KAFKA_SAMPLE_SUBSCRIBER_SECRET        (unset)               generated when empty, and
#                                                               then printed exactly once
#   KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX  blnk-sample-subscriber
#   KAFKA_SAMPLE_SUBSCRIBER_TOPICS        (the four category topics)
#   KAFKA_SKIP_SAMPLE_SUBSCRIBER          (unset)               truthy skips the principal
#                                                               and its ACLs entirely
#   KAFKA_CONTAINER                       kafka                 for docker exec delegation
#   KAFKA_COMPOSE_SERVICE                 kafka                 for docker compose exec
#   KAFKA_PROVISION_CONTAINER_SCRIPT      /scripts/kafka-provision.sh
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
#   scripts/kafka-provision.sh
#
# No arguments, in any context. "make kafka_provision" invokes it bare.

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

# Partitions per topic. Six is the required minimum. Existing topics are grown to this
# count and never shrunk.
KAFKA_MIN_PARTITIONS="${KAFKA_MIN_PARTITIONS:-6}"

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

# The SASL/SCRAM administrative principal this script authenticates as. .env.example ships
# the key empty and documents "admin" as the principal the local stack bootstraps, and
# scripts/kafka-bootstrap.sh seeds that same name, so "admin" is the fallback here. The
# three must stay aligned: this script authenticates with the credential that script
# created.
KAFKA_SASL_ADMIN_USER="${KAFKA_SASL_ADMIN_USER:-admin}"

# That principal's password. Deliberately has no default and never will: a credential must
# not be guessable from source. Validated by require_admin_secret, written once into the
# client-properties file, and never printed.
KAFKA_SASL_ADMIN_SECRET="${KAFKA_SASL_ADMIN_SECRET:-}"

# Security protocol for the admin connection. SASL_PLAINTEXT is right for the local
# single-broker stack; production pairs SCRAM with TLS, so set SASL_SSL there (and supply
# the truststore settings through KAFKA_CLIENT_CONFIG).
KAFKA_SECURITY_PROTOCOL="${KAFKA_SECURITY_PROTOCOL:-SASL_PLAINTEXT}"

# An operator-supplied client-properties file. When set it is used verbatim: nothing is
# generated and nothing is deleted. This is the way to add TLS material, a different
# mechanism, or any other CLI client setting this script does not model.
KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-}"

# PBKDF2 iteration count for the sample subscriber's SCRAM credential. 4096 is the minimum
# Kafka accepts and the default here, matching event_admin.go's DefaultScramIterations.
KAFKA_SCRAM_ITERATIONS="${KAFKA_SCRAM_ITERATIONS:-4096}"

# Readiness budget. Compose gates kafka-init on the broker healthcheck, but the makefile
# target and stack.sh's fallback do not, so the wait cannot be skipped - and it cannot be
# unbounded either, or a broker that never comes up hangs the bring-up instead of failing
# it.
KAFKA_PROVISION_TIMEOUT_SECONDS="${KAFKA_PROVISION_TIMEOUT_SECONDS:-60}"
KAFKA_PROVISION_POLL_INTERVAL_SECONDS="${KAFKA_PROVISION_POLL_INTERVAL_SECONDS:-2}"

# The sample subscriber principal, its consumer-group namespace, and the topics it may
# read. See ensure_sample_subscriber and grant_subscriber_acls for the grant shape and for
# why the dead-letter topics are excluded from the default.
KAFKA_SAMPLE_SUBSCRIBER_USER="${KAFKA_SAMPLE_SUBSCRIBER_USER:-blnk-sample-subscriber}"
KAFKA_SAMPLE_SUBSCRIBER_SECRET="${KAFKA_SAMPLE_SUBSCRIBER_SECRET:-}"
KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX="${KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX:-blnk-sample-subscriber}"
KAFKA_SAMPLE_SUBSCRIBER_TOPICS="${KAFKA_SAMPLE_SUBSCRIBER_TOPICS:-}"

# Escape hatch for a shared or production broker, where minting a sample credential is
# exactly the wrong thing to do. Set it truthy and the principal and its ACLs are skipped;
# the topics are still assured.
KAFKA_SKIP_SAMPLE_SUBSCRIBER="${KAFKA_SKIP_SAMPLE_SUBSCRIBER:-}"

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

# Fixed rather than configurable. Blnk standardises on SCRAM-SHA-512: event_admin.go
# provisions subscriber credentials with kafka.ScramMechanismSha512 and
# scripts/kafka-bootstrap.sh seeds the administrative principal with the same mechanism, so
# the broker needs exactly one enabled.
readonly SCRAM_MECHANISM="SCRAM-SHA-512"
readonly MIN_SCRAM_ITERATIONS=4096

# The required partition floor, quoted in diagnostics so the message explains the rule
# rather than just restating the configured value.
readonly REQUIRED_MIN_PARTITIONS=6

# Kafka requires this prefix on a principal name in an ACL binding, and "*" is its
# any-host pattern. Both match event_admin.go's kafkaPrincipalPrefix and ACLHostAny.
readonly ACL_PRINCIPAL_PREFIX="User:"
readonly ACL_HOST_ANY="*"

# The KRaft authorizer that has to be active for an ACL to mean anything. Named here only
# so the readiness-failure diagnosis can point at it.
readonly REQUIRED_AUTHORIZER="org.apache.kafka.metadata.authorizer.StandardAuthorizer"

# THE topic catalogue. Only the categories are written down; the dead-letter names are
# derived. This order is the canonical one - it is eventCategoryOrder in event_topics.go
# and it fixes the order of that file's AllTopics, AllDeadLetterTopics and
# AllTopicsWithDeadLetters, which provisioning is compared against.
readonly EVENT_CATEGORIES=(transactions balances identities system)

# The suffix that forms a dead-letter sibling. This is the published <topic>.dlt naming
# convention and it must equal event_topics.go's DeadLetterTopicSuffix. Blnk owns the .dlt
# sibling of every topic it owns; subscribers building their own consumer-side
# dead-lettering must choose names outside that space.
readonly DEAD_LETTER_SUFFIX=".dlt"

# Resolved during main; declared here so the data flow between the steps is visible.
TOPICS_CLI=""
CONFIGS_CLI=""
ACLS_CLI=""
CLI_FLAVOUR=""
CLIENT_CONFIG=""
GENERATED_CLIENT_CONFIG=""
CATEGORY_TOPICS=()
DEAD_LETTER_TOPICS=()
ALL_TOPICS=()
SUMMARY_TOPICS=()
SUMMARY_PARTITIONS=()
SUBSCRIBER_TOPICS=()
SUBSCRIBER_USER=""
SUBSCRIBER_GROUP_PREFIX=""
SUBSCRIBER_PROVISIONED="no"
CONTAINER_RUNTIME=()
CONTAINER_STDIN_FLAG=()
CONTAINER_TARGET=""
CONTAINER_RUNTIME_LABEL=""

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

    # Below the floor is legal Kafka but violates the topic-and-partition design, so it is a
    # warning rather than a failure: an operator experimenting on a laptop should not be
    # blocked, but nobody should reach production having lowered it by accident.
    if ((10#$KAFKA_MIN_PARTITIONS < REQUIRED_MIN_PARTITIONS)); then
        warn "KAFKA_MIN_PARTITIONS is ${KAFKA_MIN_PARTITIONS}, below the required minimum of ${REQUIRED_MIN_PARTITIONS}." \
            "Topics will be created with fewer partitions than the design calls for, which" \
            "caps how far a subscriber can scale out. Ordering is unaffected - messages are" \
            "keyed by ledger ID, so events for one aggregate share a partition at any count." \
            "Fix: unset KAFKA_MIN_PARTITIONS to accept the default of ${REQUIRED_MIN_PARTITIONS}."
    fi
}

# Require the administrative secret, and refuse an unsubstituted template placeholder.
#
# Neither branch prints the value. This mirrors scripts/kafka-bootstrap.sh's check of the
# same variable deliberately: the two scripts authenticate as the same principal, so they
# must fail the same way for the same cause, or an operator debugging one learns nothing
# about the other.
#
# .env.example ships this key empty - deliberately, since stack.sh --init substitutes
# {POSTGRES_PASSWORD} and nothing else with a global sed, so any other brace placeholder
# would survive into .env as a literal password - which makes the empty case the one
# operators actually hit. The placeholder branch remains as defence in depth for a
# hand-edited .env.
require_admin_secret() {
    if [[ -z "$KAFKA_SASL_ADMIN_SECRET" ]]; then
        die "KAFKA_SASL_ADMIN_SECRET is not set." \
            "The broker cannot be provisioned without authenticating, and there is no" \
            "default by design: a credential must not be guessable from source." \
            "Fix: create .env with './stack.sh --init' if you have not already, then set" \
            "KAFKA_SASL_ADMIN_SECRET in it - .env.example ships that key empty and --init" \
            "does not generate this one - or export the variable for this process." \
            "It must be the same password scripts/kafka-bootstrap.sh seeded the" \
            "'${KAFKA_SASL_ADMIN_USER}' principal with."
    fi

    if [[ "$KAFKA_SASL_ADMIN_SECRET" == "{KAFKA_SASL_ADMIN_SECRET}" ]] ||
        [[ "$KAFKA_SASL_ADMIN_SECRET" == "{"*"}" ]]; then
        die "KAFKA_SASL_ADMIN_SECRET still holds an unsubstituted {PLACEHOLDER} value." \
            "It was copied from a template and never replaced with a real secret. Using it" \
            "would authenticate with a literal placeholder as the password, which fails" \
            "indistinguishably from a wrong password." \
            "Fix: run './stack.sh --init' to create .env, then set KAFKA_SASL_ADMIN_SECRET" \
            "to the password scripts/kafka-bootstrap.sh seeded, or export it for this" \
            "process. Nothing has been provisioned and the value is not echoed."
    fi
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

# Derive the eight names from the four categories and the configured prefix.
#
# The prefix is trimmed of whitespace and of a leading or trailing separator, matching
# event_topics.go's topicPrefixTrimCutset, so that KAFKA_TOPIC_PREFIX=acme. and
# KAFKA_TOPIC_PREFIX=acme resolve identically instead of producing acme..transactions. A
# prefix that trims away to nothing falls back to the default for the same reason that file
# does: an empty prefix would yield names beginning with a bare dot.
#
# The resulting order is deliberate and matches AllTopicsWithDeadLetters: all four category
# topics, then all four dead-letter siblings.
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

    CATEGORY_TOPICS=()
    DEAD_LETTER_TOPICS=()
    for category in "${EVENT_CATEGORIES[@]}"; do
        topic="${prefix}.${category}"
        CATEGORY_TOPICS+=("$topic")
        DEAD_LETTER_TOPICS+=("${topic}${DEAD_LETTER_SUFFIX}")
    done

    ALL_TOPICS=("${CATEGORY_TOPICS[@]}" "${DEAD_LETTER_TOPICS[@]}")
}

# The topics the sample subscriber is authorised to read.
#
# The default is the four CATEGORY topics and deliberately NOT the dead-letter topics. A
# subscriber consumes events; a dead-letter topic holds events Blnk failed to publish and is
# operator-facing, triaged and replayed through the internal events API rather than read by
# a subscriber. Excluding them is also what gives the subscriber-isolation criterion
# something to prove: the isolation test asserts an authorization failure on a topic, on a
# dead-letter topic, and on a list operation outside the grant, and if the sample principal
# could read the DLTs that assertion would be vacuous.
#
# KAFKA_SAMPLE_SUBSCRIBER_TOPICS overrides the default with a comma-separated list, for a
# subscriber that legitimately needs a different slice.
resolve_subscriber_topics() {
    local entry

    SUBSCRIBER_TOPICS=()
    if [[ -z "$(trim "$KAFKA_SAMPLE_SUBSCRIBER_TOPICS")" ]]; then
        SUBSCRIBER_TOPICS=("${CATEGORY_TOPICS[@]}")
        return 0
    fi

    while IFS= read -r entry; do
        SUBSCRIBER_TOPICS+=("$entry")
    done < <(split_list "$KAFKA_SAMPLE_SUBSCRIBER_TOPICS" ",")

    if ((${#SUBSCRIBER_TOPICS[@]} == 0)); then
        die "KAFKA_SAMPLE_SUBSCRIBER_TOPICS is set but contains no usable topic name." \
            "Value: '${KAFKA_SAMPLE_SUBSCRIBER_TOPICS}'" \
            "Fix: give a comma-separated list of topic names, or unset it to grant the" \
            "four category topics: $(join_commas "${CATEGORY_TOPICS[@]}")."
    fi
}

# Resolve and validate the sample principal's identity before the broker is touched.
#
# Deliberately a precondition rather than a check inside ensure_sample_subscriber: an empty
# principal name is a configuration error, and finding it only after eight topics have been
# assured means the operator reads a failure at the end of an otherwise successful run.
# Skipped entirely when the sample principal is skipped, because then neither value is used.
require_valid_subscriber() {
    if is_truthy "$KAFKA_SKIP_SAMPLE_SUBSCRIBER"; then
        return 0
    fi

    SUBSCRIBER_USER="$(trim "$KAFKA_SAMPLE_SUBSCRIBER_USER")"
    SUBSCRIBER_GROUP_PREFIX="$(trim "$KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX")"

    if [[ -z "$SUBSCRIBER_USER" ]]; then
        die "KAFKA_SAMPLE_SUBSCRIBER_USER is empty." \
            "A principal needs a name." \
            "Fix: unset it to accept the default of blnk-sample-subscriber, or set" \
            "KAFKA_SKIP_SAMPLE_SUBSCRIBER=1 to skip the sample principal entirely."
    fi

    if [[ -z "$SUBSCRIBER_GROUP_PREFIX" ]]; then
        die "KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX is empty." \
            "Without a group prefix the subscriber could authenticate and describe its" \
            "topics but never join a consumer group, because the prefixed group grant would" \
            "have nothing to match - which looks like a consumer that starts and then does" \
            "nothing at all." \
            "Fix: unset it to accept the default of blnk-sample-subscriber."
    fi
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
# credential on a command line eight times over.
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

    # Only names, never values. Every one of these is propagated only if it is declared here,
    # so the container falls back to this script's own defaults for the rest.
    local passthrough=(
        BLNK_KAFKA_PROVISION_IN_CONTAINER
        KAFKA_BOOTSTRAP_SERVER
        KAFKA_TOPIC_PREFIX
        KAFKA_MIN_PARTITIONS
        KAFKA_REPLICATION_FACTOR
        KAFKA_SASL_ADMIN_USER
        KAFKA_SASL_ADMIN_SECRET
        KAFKA_SECURITY_PROTOCOL
        KAFKA_SCRAM_ITERATIONS
        KAFKA_PROVISION_TIMEOUT_SECONDS
        KAFKA_PROVISION_POLL_INTERVAL_SECONDS
        KAFKA_SAMPLE_SUBSCRIBER_USER
        KAFKA_SAMPLE_SUBSCRIBER_SECRET
        KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX
        KAFKA_SAMPLE_SUBSCRIBER_TOPICS
        KAFKA_SKIP_SAMPLE_SUBSCRIBER
    )

    # The resolved values are exported so that the bare "-e NAME" form has something to
    # copy. KAFKA_BOOTSTRAP_SERVER is exported after resolution rather than as it arrived,
    # so the container provisions the broker this process decided on; KAFKA_CLIENT_CONFIG is
    # NOT propagated, because a path that exists on the host almost certainly does not exist
    # in the container, and silently reading a different file would be worse than writing a
    # fresh one there.
    export BLNK_KAFKA_PROVISION_IN_CONTAINER=1
    export KAFKA_BOOTSTRAP_SERVER
    export KAFKA_TOPIC_PREFIX KAFKA_MIN_PARTITIONS KAFKA_REPLICATION_FACTOR
    export KAFKA_SASL_ADMIN_USER KAFKA_SASL_ADMIN_SECRET KAFKA_SECURITY_PROTOCOL
    export KAFKA_SCRAM_ITERATIONS
    export KAFKA_PROVISION_TIMEOUT_SECONDS KAFKA_PROVISION_POLL_INTERVAL_SECONDS
    export KAFKA_SAMPLE_SUBSCRIBER_USER KAFKA_SAMPLE_SUBSCRIBER_SECRET
    export KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX KAFKA_SAMPLE_SUBSCRIBER_TOPICS
    export KAFKA_SKIP_SAMPLE_SUBSCRIBER

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
    return 0
}

# Registered for INT and TERM as well as EXIT. bash runs an EXIT trap after a signal has
# been handled, so naming the signals is what converts a Ctrl-C into an ordinary exit that
# takes the credential file with it, rather than a kill that leaves it behind.
trap cleanup EXIT INT TERM

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

    local directory="${TMPDIR:-/tmp}"
    if [[ ! -d "$directory" || ! -w "$directory" ]]; then
        die "the temporary directory '${directory}' is not a writable directory." \
            "A client-properties file has to be written somewhere outside the repository," \
            "because the CLI has no inline credential flag and ./scripts is mounted" \
            "read-only in the compose stack." \
            "Fix: set TMPDIR to a writable directory, or pass KAFKA_CLIENT_CONFIG pointing" \
            "at a properties file you manage yourself."
    fi

    local previous_umask path=""
    previous_umask="$(umask)"
    umask 077

    if command -v mktemp >/dev/null 2>&1; then
        path="$(mktemp "${directory%/}/blnk-kafka-provision-XXXXXX" 2>/dev/null || true)"
    fi

    # Fallback for an image without mktemp. O_EXCL is approximated with a existence test plus
    # "set -o noclobber" on the redirection, which fails rather than truncating if the path
    # was created between the test and the write.
    if [[ -z "$path" ]]; then
        path="${directory%/}/blnk-kafka-provision-$$-${RANDOM}"
        if ! (set -o noclobber && : >"$path") 2>/dev/null; then
            umask "$previous_umask"
            die "could not create a client-properties file in '${directory}'." \
                "Tried mktemp and then '${path}'." \
                "Fix: set TMPDIR to a writable directory, or pass KAFKA_CLIENT_CONFIG."
        fi
    fi

    umask "$previous_umask"

    # Recorded before the write, so that a failure part-way through still leaves the trap a
    # path to clean up.
    GENERATED_CLIENT_CONFIG="$path"
    CLIENT_CONFIG="$path"

    # Belt and braces on top of the umask: an inherited-directory oddity or a fallback path
    # that already existed must not leave the credential readable.
    chmod 600 "$path" 2>/dev/null || true

    # The one and only place the secret is written. Assembled with printf into a file whose
    # mode is already 0600; no echo of any part of it, and no intermediate command line.
    if ! printf '%s\n' \
        "security.protocol=${KAFKA_SECURITY_PROTOCOL}" \
        "sasl.mechanism=${SCRAM_MECHANISM}" \
        "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_SASL_ADMIN_USER}\" password=\"${KAFKA_SASL_ADMIN_SECRET}\";" \
        >"$path"; then
        die "could not write the client-properties file '${path}'." \
            "Fix: check that ${directory} is writable, or pass KAFKA_CLIENT_CONFIG pointing" \
            "at a properties file you manage yourself."
    fi

    log "wrote a temporary client configuration for this run" \
        "path      : ${path} (mode 0600, removed on exit)" \
        "protocol  : ${KAFKA_SECURITY_PROTOCOL}" \
        "mechanism : ${SCRAM_MECHANISM}" \
        "principal : ${KAFKA_SASL_ADMIN_USER}"
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

kafka_topics() {
    "$TOPICS_CLI" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --command-config "$CLIENT_CONFIG" "$@" </dev/null
}

kafka_configs() {
    "$CONFIGS_CLI" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --command-config "$CLIENT_CONFIG" "$@" </dev/null
}

kafka_acls() {
    "$ACLS_CLI" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --command-config "$CLIENT_CONFIG" "$@" </dev/null
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
            ok "broker reachable and authenticated as ${KAFKA_SASL_ADMIN_USER} (attempt ${attempt})"
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
        "  2. authentication is failing because scripts/kafka-bootstrap.sh never seeded the" \
        "     ${KAFKA_SASL_ADMIN_USER} principal, or seeded it with a different password." \
        "     In KRaft mode SCRAM credentials live in the metadata log and can only be" \
        "     created there while storage is formatted, so a broker whose storage was" \
        "     formatted without --add-scram can authenticate nobody at all." \
        "  3. authorization is failing because '${KAFKA_SASL_ADMIN_USER}' is not in the" \
        "     broker's super.users while this authorizer is active:" \
        "       ${REQUIRED_AUTHORIZER}" \
        "     The credential is then valid but every topic and ACL operation is denied," \
        "     which is the failure mode hardest to guess at because the password is not" \
        "     the problem. Add 'super.users=User:${KAFKA_SASL_ADMIN_USER}' to the broker" \
        "     configuration."
}

# ---------------------------------------------------------------------------------------
# Step one: the eight topics
#
# Create if absent, grow if under-partitioned, leave alone and warn if over-partitioned.
# Never fail on a topic that already exists, and never attempt a shrink.
#
# This is the same rule event_admin.go's EnsureTopics applies at runtime through
# CreateTopics and CreatePartitions, and the two are kept identical on purpose: an operator
# who provisions with this script and then lets the service assure topics on start-up must
# not see the two disagree.
# ---------------------------------------------------------------------------------------

# Read a topic's current partition count, or print nothing when it cannot be determined.
#
# --describe exits non-zero with a Java stack trace for a topic that does not exist, so the
# call is guarded and its output discarded: an unknown count is a legitimate answer here and
# the caller decides what it means. The count is parsed out of the summary line's
# "PartitionCount: N" field, which every Kafka 2.x-and-later release emits.
topic_partition_count() {
    local topic="$1" output
    output="$(kafka_topics --describe --topic "$topic" 2>/dev/null || true)"

    if [[ "$output" =~ PartitionCount:[[:space:]]*([0-9]+) ]]; then
        printf '%s' "${BASH_REMATCH[1]}"
    fi
}

# Create one topic if it is absent, then reconcile its partition count upwards only.
#
# Records the observed count in SUMMARY_PARTITIONS so that the closing summary reports what
# is actually on the broker rather than what was requested.
ensure_topic() {
    local topic="$1" target="$2"
    local output current

    # --if-not-exists is Kafka's own creation idempotency: an existing topic is a no-op with
    # exit 0, whatever its partition count, so the reconciliation below is reached either way.
    #
    # Output is captured and shown only on failure. On success it carries nothing but Kafka's
    # standing advisory that topic names mixing '.' and '_' can collide in metric names -
    # which every name here triggers, by design, since the catalogue is dot-separated and
    # event_topics.go composes it that way. Eight copies of that warning per run would bury
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
            "  2. ${KAFKA_SASL_ADMIN_USER} lacks Create authority on the cluster. Add it to" \
            "     the broker's super.users, or grant Create on the cluster resource."
    fi

    current="$(topic_partition_count "$topic")"

    if [[ -z "$current" ]]; then
        # Reaching here means the topic was created or already existed, but describing it
        # failed. Not fatal: the topic is there, and a describe can fail on a metadata
        # refresh race. Reported so the summary does not claim a count it never read.
        warn "created or found topic '${topic}' but could not read its partition count." \
            "The topic exists; only the reconciliation check was skipped. Re-running this" \
            "script will grow it if it turns out to have fewer than ${target} partitions."
        SUMMARY_TOPICS+=("$topic")
        SUMMARY_PARTITIONS+=("unknown")
        return 0
    fi

    if ((10#$current < 10#$target)); then
        log "growing '${topic}' from ${current} to ${target} partitions"
        if ! output="$(kafka_topics --alter --topic "$topic" --partitions "$target" 2>&1)"; then
            # Re-read before deciding this is a failure. Two provisioners racing each other -
            # the compose one-shot and a manual "make kafka_provision" - both try to grow, and
            # the loser is told the topic already has that many partitions. The topic is
            # correct, so that is success, not an error.
            current="$(topic_partition_count "$topic")"
            if [[ -n "$current" ]] && ((10#$current >= 10#$target)); then
                log "'${topic}' already has ${current} partitions; another provisioner grew it"
            else
                printf '%s\n' "$output" | redact >&2
                die "could not grow topic '${topic}' to ${target} partitions." \
                    "The broker's own output is above. Kafka increases a partition count with" \
                    "an alter and cannot decrease one, so this is a genuine failure rather" \
                    "than a no-op." \
                    "Fix: give '${KAFKA_SASL_ADMIN_USER}' Alter authority on the topic - add it" \
                    "to the broker's super.users, or grant Alter on the topic resource. If the" \
                    "existing partition count is deliberate, set KAFKA_MIN_PARTITIONS to it" \
                    "instead so no alter is attempted."
            fi
        else
            current="$target"
        fi
        SUMMARY_TOPICS+=("$topic")
        SUMMARY_PARTITIONS+=("$current")
        return 0
    fi

    if ((10#$current == 10#$target)); then
        log "'${topic}' exists with ${current} partitions, already correct"
        SUMMARY_TOPICS+=("$topic")
        SUMMARY_PARTITIONS+=("$current")
        return 0
    fi

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
    SUMMARY_TOPICS+=("$topic")
    SUMMARY_PARTITIONS+=("$current")
}

# Assure all eight topics, category topics first and then their dead-letter siblings.
ensure_topics() {
    local topic

    log "assuring ${#ALL_TOPICS[@]} topics" \
        "partitions         : ${KAFKA_MIN_PARTITIONS}" \
        "replication factor : ${KAFKA_REPLICATION_FACTOR}"

    SUMMARY_TOPICS=()
    SUMMARY_PARTITIONS=()
    for topic in "${ALL_TOPICS[@]}"; do
        ensure_topic "$topic" "$KAFKA_MIN_PARTITIONS"
    done

    ok "all ${#SUMMARY_TOPICS[@]} topics are present"
}

# ---------------------------------------------------------------------------------------
# Step two: the sample subscriber principal
#
# One SCRAM-SHA-512 credential, so that a developer can point a consumer at the local stack
# without first learning how to mint a principal. Production subscribers are provisioned by
# event_admin.go's ProvisionSubscriberPrincipal through AlterUserScramCredentials, in
# process, and never come through here.
# ---------------------------------------------------------------------------------------

# Generate a password for the sample principal.
#
# openssl is the primary source, with a /dev/urandom fallback for an image without it. Both
# outputs are filtered to alphanumerics, which is not cosmetic: kafka-configs parses
# --add-config as a comma-separated list with '[' and ']' delimiting a value, so a password
# containing any of those characters would be misparsed rather than rejected. Restricting the
# alphabet makes that impossible instead of merely unlikely, and it stays inside the
# printable-ASCII range where SASLprep normalisation is a no-op - the same restriction
# event_admin.go's validateSCRAMPassword enforces on the Go side.
generate_password() {
    local password=""

    if command -v openssl >/dev/null 2>&1; then
        password="$(openssl rand -base64 24 2>/dev/null | LC_ALL=C tr -dc 'A-Za-z0-9' || true)"
    fi

    if [[ -z "$password" && -r /dev/urandom ]]; then
        # head closes the pipe, so tr is killed by SIGPIPE and the pipeline reports failure
        # under "set -o pipefail". Guarded, because that failure is the expected outcome.
        password="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom 2>/dev/null | head -c 32 || true)"
    fi

    # Short output means the filter ate almost everything, which means the source produced
    # almost nothing. Refusing beats provisioning a guessable credential.
    if ((${#password} < 16)); then
        die "could not generate a password for the sample subscriber principal." \
            "Tried 'openssl rand' and then /dev/urandom; neither yielded enough entropy." \
            "Fix: set KAFKA_SAMPLE_SUBSCRIBER_SECRET to a password of your own, or set" \
            "KAFKA_SKIP_SAMPLE_SUBSCRIBER=1 to skip the sample principal entirely."
    fi

    printf '%s' "$password"
}

# Create or update the sample principal's SCRAM credential, then grant its ACLs.
#
# The credential operation is an upsert, so re-running replaces the password rather than
# failing. That is the right behaviour for a development convenience credential and it is
# what AlterUserScramCredentials does on the Go side too.
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

    local password generated="no"
    if [[ -n "$KAFKA_SAMPLE_SUBSCRIBER_SECRET" ]]; then
        password="$KAFKA_SAMPLE_SUBSCRIBER_SECRET"
    else
        password="$(generate_password)"
        generated="yes"
    fi

    log "provisioning the sample subscriber principal" \
        "principal  : ${ACL_PRINCIPAL_PREFIX}${user}" \
        "mechanism  : ${SCRAM_MECHANISM}, ${KAFKA_SCRAM_ITERATIONS} iterations"

    # The one and only place the sample password is used in a command. Assembled into a
    # single variable and passed as a single argument so that no shell quoting can split it
    # and no partially interpolated command string exists to be logged by accident.
    #
    # An honest limitation, recorded rather than glossed over: a secret passed as a
    # command-line argument is briefly visible in the process table. It is accepted here
    # because the CLI offers no alternative and this is a local-development convenience
    # credential. Real subscriber credentials never touch a command line - event_admin.go
    # provisions them in process over the Kafka protocol.
    local scram_config output
    scram_config="${SCRAM_MECHANISM}=[iterations=${KAFKA_SCRAM_ITERATIONS},password=${password}]"

    if ! output="$(kafka_configs --alter --add-config "$scram_config" \
        --entity-type users --entity-name "$user" 2>&1)"; then
        printf '%s\n' "$output" | redact >&2
        die "could not provision the SCRAM credential for '${user}'." \
            "The broker's own output is above, with credential-bearing lines removed. The" \
            "two usual causes:" \
            "  1. ${KAFKA_SASL_ADMIN_USER} lacks Alter authority on the cluster - add it to" \
            "     the broker's super.users;" \
            "  2. the broker does not have ${SCRAM_MECHANISM} among its enabled mechanisms." \
            "Kafka implements SCRAM-SHA-256 and SCRAM-SHA-512 only, and Blnk standardises" \
            "on SHA-512 so that the broker needs exactly one enabled."
    fi

    grant_subscriber_acls "$user" "$group_prefix"

    SUBSCRIBER_PROVISIONED="yes"

    # The generated password is printed exactly once, here, because a credential nobody can
    # read is a credential nobody can use. An operator-supplied one is never printed: they
    # already have it, and echoing it would put it in a log for no benefit at all.
    if [[ "$generated" == "yes" ]]; then
        printf '%s\n' ""
        printf '%s\n' "${YEL}  ==================================================================${NC}"
        printf '%s\n' "${YEL}   LOCAL DEVELOPMENT SAMPLE CREDENTIAL - shown once, not stored${NC}"
        printf '%s\n' "${YEL}  ==================================================================${NC}"
        printf '%s\n' "    user     : ${user}"
        printf '%s\n' "    password : ${password}"
        printf '%s\n' "    mechanism: ${SCRAM_MECHANISM} (${KAFKA_SCRAM_ITERATIONS} iterations)"
        printf '%s\n' "    group    : ${group_prefix}* (prefixed)"
        printf '%s\n' ""
        printf '%s\n' "    This password was generated for the local stack and is NOT recorded"
        printf '%s\n' "    anywhere. Copy it now if you want it; re-running this script mints a"
        printf '%s\n' "    new one. NEVER use it in production, and never commit it: real"
        printf '%s\n' "    subscriber credentials are issued by"
        printf '%s\n' "    POST /subscribers/{id}/kafka-credentials, which returns the secret"
        printf '%s\n' "    once and persists only a non-reversible reference to it."
        printf '%s\n' "${YEL}  ==================================================================${NC}"
        printf '%s\n' ""
    else
        log "used the KAFKA_SAMPLE_SUBSCRIBER_SECRET you supplied; it is not echoed"
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
# THREE PROHIBITIONS, HELD BY HAND
#
# event_admin_test.go asserts all three for the Go path. There is no equivalent automated
# test for a shell script, which is exactly why they are written down here:
#
#   1. NEVER a wildcard topic pattern. No --topic '*', no wildcard resource-pattern type.
#      Every topic is named explicitly with a literal pattern. A wildcard grant is
#      indistinguishable from no isolation at all.
#   2. NEVER Write. Subscribers consume; only Blnk's relay produces. A subscriber that could
#      produce to a category topic could forge ledger events.
#   3. NEVER surface the administrative password. It exists only inside the 0600
#      client-properties file, and no log line in this script can interpolate it.
# ---------------------------------------------------------------------------------------

grant_subscriber_acls() {
    local user="$1" group_prefix="$2"
    local principal="${ACL_PRINCIPAL_PREFIX}${user}"
    local topic output

    log "granting ACLs to ${principal}" \
        "topics : $(join_commas "${SUBSCRIBER_TOPICS[@]}")" \
        "group  : ${group_prefix} (prefixed)" \
        "host   : ${ACL_HOST_ANY}"

    # kafka-acls --add is idempotent: re-adding an existing binding succeeds as a no-op, so
    # re-runs need no existence check.
    for topic in "${SUBSCRIBER_TOPICS[@]}"; do
        if ! output="$(kafka_acls --add \
            --allow-principal "$principal" \
            --allow-host "$ACL_HOST_ANY" \
            --operation Read \
            --operation Describe \
            --topic "$topic" \
            --resource-pattern-type literal 2>&1)"; then
            printf '%s\n' "$output" | redact >&2
            die "could not grant Read and Describe on topic '${topic}' to ${principal}." \
                "The broker's own output is above. The two usual causes:" \
                "  1. ${KAFKA_SASL_ADMIN_USER} lacks Alter authority on the cluster - add it" \
                "     to the broker's super.users;" \
                "  2. no authorizer is configured, so the broker has nowhere to store an ACL." \
                "     KRaft needs ${REQUIRED_AUTHORIZER}; without it ACLs are meaningless and" \
                "     subscriber isolation cannot be enforced at all."
        fi
    done

    if ! output="$(kafka_acls --add \
        --allow-principal "$principal" \
        --allow-host "$ACL_HOST_ANY" \
        --operation Read \
        --group "$group_prefix" \
        --resource-pattern-type prefixed 2>&1)"; then
        printf '%s\n' "$output" | redact >&2
        die "could not grant Read on consumer groups prefixed '${group_prefix}' to ${principal}." \
            "The broker's own output is above. Without this binding the principal can" \
            "authenticate and describe its topics but cannot join a consumer group or commit" \
            "an offset, which looks like a consumer that starts and then does nothing." \
            "Fix: the same two causes as the topic grants above - give" \
            "'${KAFKA_SASL_ADMIN_USER}' Alter authority on the cluster by adding it to the" \
            "broker's super.users, and make sure KRaft has ${REQUIRED_AUTHORIZER} configured" \
            "so the broker has somewhere to store an ACL at all."
    fi

    ok "granted Read and Describe on ${#SUBSCRIBER_TOPICS[@]} topics, and Read on the '${group_prefix}' group namespace"
}

# ---------------------------------------------------------------------------------------
# The closing summary
#
# What makes a successful run verifiable at a glance, and the only place the resolved
# catalogue is shown as a whole. The topic list is what to compare against
# event_topics.go's AllTopicsWithDeadLetters when a subscriber reports seeing no events.
# ---------------------------------------------------------------------------------------

print_summary() {
    local index

    printf '%s\n' ""
    ok "Kafka is provisioned for Blnk event streaming"
    printf '%s\n' ""
    printf '%s\n' "Broker:"
    printf '%s\n' "  ${KAFKA_BOOTSTRAP_SERVER} (${KAFKA_SECURITY_PROTOCOL}, ${SCRAM_MECHANISM} as ${KAFKA_SASL_ADMIN_USER})"
    printf '%s\n' ""
    printf '%s\n' "Topics (name, partitions, replication factor):"
    for index in "${!SUMMARY_TOPICS[@]}"; do
        printf '  %-34s %-4s %s\n' \
            "${SUMMARY_TOPICS[index]}" \
            "${SUMMARY_PARTITIONS[index]}" \
            "${KAFKA_REPLICATION_FACTOR}"
    done
    printf '%s\n' ""

    case "$SUBSCRIBER_PROVISIONED" in
        yes)
            printf '%s\n' "Sample subscriber:"
            printf '%s\n' "  principal            ${ACL_PRINCIPAL_PREFIX}${SUBSCRIBER_USER}"
            printf '%s\n' "  consumer group       ${SUBSCRIBER_GROUP_PREFIX}* (prefixed)"
            printf '%s\n' "  readable topics      $(join_commas "${SUBSCRIBER_TOPICS[@]}")"
            printf '%s\n' "  granted operations   Read, Describe on those topics; Read on that group namespace"
            printf '%s\n' "  NOT granted          Write anywhere, and no access to the .dlt topics"
            ;;
        skipped)
            printf '%s\n' "Sample subscriber:"
            printf '%s\n' "  skipped - KAFKA_SKIP_SAMPLE_SUBSCRIBER is set"
            ;;
        *)
            printf '%s\n' "Sample subscriber:"
            printf '%s\n' "  not provisioned"
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

main() {
    log "provisioning Kafka topics, the sample subscriber principal and its ACLs"

    # Cheapest possible exit, and the common local case: no Kafka configured at all.
    skip_when_kafka_unconfigured

    # Pure decisions, no network.
    require_valid_geometry
    require_valid_iterations
    resolve_topics
    resolve_subscriber_topics
    require_valid_subscriber
    require_admin_secret

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

    prepare_client_config
    wait_for_broker

    ensure_topics
    ensure_sample_subscriber

    print_summary
}

main "$@"
