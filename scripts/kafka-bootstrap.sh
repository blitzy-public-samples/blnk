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
# Formats a KRaft-mode Kafka broker's storage and seeds the SASL/SCRAM administrative
# credential into the cluster metadata log in the same step. That is the whole job.
#
# WHY THIS RUNS BEFORE THE BROKER'S FIRST START
#
# In KRaft mode SCRAM credentials live in the __cluster_metadata log rather than in
# ZooKeeper, and a broker cannot authenticate a SASL client until at least one credential
# is already there. That is a chicken-and-egg problem: the usual runtime command,
#
#     kafka-configs --alter --add-config 'SCRAM-SHA-512=[password=...]' \
#       --entity-type users --entity-name <user>
#
# needs an authenticated connection, so it cannot create the first credential. The only
# resolution is to inject it while the storage is being formatted, before the broker has
# ever run, which is what this script does with "kafka-storage format --add-scram".
#
# Per-subscriber runtime principals are NOT created here. They are added afterwards, once
# the broker is up and this bootstrap credential can authenticate, by
# scripts/kafka-provision.sh (locally) and by the Go admin client in event_admin.go (in
# production, via AlterUserScramCredentials). The ordering is bootstrap, then broker,
# then provisioning.
#
# HARD VERSION FLOOR: KAFKA 3.5 / CONFLUENT PLATFORM 7.5.0
#
# The "--add-scram" flag of "kafka-storage format" was added in Kafka 3.5 (Confluent
# Platform 7.5.0). Earlier releases simply do not have it, so they cannot be bootstrapped
# at all and local bring-up fails outright. This is a floor on the broker image tag, not a
# preference. The floor is asserted below by asking the CLI whether it has the flag, which
# is the last point at which a KAFKA_IMAGE override below the floor can still be
# diagnosed rather than surfacing later as an unexplained authentication failure.
#
# IDEMPOTENCY
#
# Safe to run on every bring-up. "docker compose down && docker compose up" reuses the
# kafka_data volume, so storage is already formatted from the second start onwards. That
# is detected (via meta.properties in every configured log directory) and skipped, never
# treated as an error. Kafka's own --ignore-formatted flag is passed as well; the two
# mechanisms are complementary rather than redundant.
#
# REQUIREMENTS
#
# bash 4.4 or later, and the Kafka CLI tools. The version floor is not decorative: this
# script runs under "set -u", and expanding "$@" when there are no positional parameters
# was only made safe in bash 4.4. Every container image in the stack is well past that.
#
# Intended to run inside the Kafka container, or on a host where the Kafka distribution is
# installed. Nothing is written outside the broker's own log directories - in the compose
# stack ./scripts is mounted read-only.
#
# USAGE
#
#   scripts/kafka-bootstrap.sh
#       Format storage, report what happened, exit 0. The caller starts the broker
#       itself - for example an entrypoint of ["/bin/sh", "-c"] with a &&-chained
#       command, the pattern docker-compose.yaml already uses for the server service.
#
#   scripts/kafka-bootstrap.sh kafka-server-start.sh /etc/kafka/server.properties
#       Format storage, then replace this process with the given command. This is the
#       conventional container-entrypoint form and lets a compose service declare its
#       entrypoint once. The command runs only after a successful bootstrap.
#
#   scripts/kafka-bootstrap.sh --help
#       Print the above, every variable this script reads, and exit 0. Answered before the
#       Kafka CLI is looked for, so the question can be asked anywhere.
#
# Apart from -h/--help there are no options - only an optional trailing command, and an
# argument that LOOKS like an option is refused rather than run as one. Configuration comes
# from the environment; see the configuration block below.
#
# REQUIRED CONFIGURATION
#
# KAFKA_SASL_ADMIN_USER and KAFKA_SASL_ADMIN_SECRET must BOTH be set, and neither has a
# default. They are one pair with one meaning, identical here, in
# scripts/kafka-provision.sh and in config.KafkaConfig.SASLAdminCredentials. The username
# used to fall back to the literal principal "admin", which made a single .env mean
# "authenticate as admin" to the scripts and "no SASL at all" to the service; nothing
# invents a principal for you now.
#
# Both values must be drawn from the safe credential alphabet documented further down.
# Kafka's --add-scram value grammar has no escape sequence, so a password containing a
# comma or a bracket cannot be expressed in it and would be silently truncated into a
# credential nobody can authenticate with. Such a value is refused up front, by name.

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
# .env.example is the source of truth for the two names it declares -
# KAFKA_SASL_ADMIN_USER and KAFKA_SASL_ADMIN_SECRET - and the defaults below track it.
# The other four are bootstrap-only knobs that no other component reads, so they are
# deliberately absent from .env.example rather than duplicated into it.
# ---------------------------------------------------------------------------------------

# The SASL/SCRAM administrative principal seeded into the metadata log.
#
# NO DEFAULT, DELIBERATELY. This used to fall back to the literal principal "admin" when
# the variable was empty, and that made one .env mean two different things: the scripts
# seeded and authenticated as "admin", while the Go clients read the same empty value as
# "no SASL configured" and connected anonymously. An operator reading either half learned
# the wrong rule about the other.
#
# The username and the secret are now ONE PAIR with a single meaning, identical here, in
# scripts/kafka-provision.sh, and in config.KafkaConfig.SASLAdminCredentials:
#
#     both set          SASL/SCRAM-SHA-512 as the named principal
#     both empty        no SASL at all
#     exactly one set   a misconfiguration, refused by name
#
# Because this script's entire job is to seed a SCRAM credential, "no SASL at all" is not
# a state it can act on: require_admin_credentials refuses an empty pair here and names
# both variables. The local stack conventionally uses "admin"; set it explicitly.
KAFKA_SASL_ADMIN_USER="${KAFKA_SASL_ADMIN_USER:-}"

# That principal's password. Deliberately has no default and never will: a credential must
# not be guessable from source, and a present-but-wrong password would produce a broker
# nobody can authenticate against. Validated by require_admin_credentials, used exactly
# once, and never printed.
KAFKA_SASL_ADMIN_SECRET="${KAFKA_SASL_ADMIN_SECRET:-}"

# SCRAM iteration count. 4096 is the minimum Kafka accepts and the default here. Kafka
# implements SCRAM-SHA-256 and SCRAM-SHA-512 only, and Blnk standardises on SHA-512.
KAFKA_SCRAM_ITERATIONS="${KAFKA_SCRAM_ITERATIONS:-4096}"

# The cluster ID to format with. Generated with "kafka-storage random-uuid" when unset,
# which only ever happens on a genuinely first format because an already-formatted volume
# is skipped. It is not a secret and is echoed, so it can be pinned here to make a rebuild
# reproducible.
KAFKA_CLUSTER_ID="${KAFKA_CLUSTER_ID:-}"

# The KRaft server.properties to format against. Auto-detected from a short list of
# well-known image layouts when unset; see detect_kraft_config.
KAFKA_KRAFT_CONFIG="${KAFKA_KRAFT_CONFIG:-}"

# Log directories to probe for existing formatting. Comma-separated, matching Kafka's own
# log.dirs syntax. Resolved by resolve_log_dirs: this value wins when set, otherwise
# log.dirs from the resolved configuration file, otherwise DEFAULT_LOG_DIRS.
KAFKA_LOG_DIRS="${KAFKA_LOG_DIRS:-}"

# The SCRAM mechanism is fixed rather than configurable. Subscriber credentials are
# provisioned as SCRAM-SHA-512 by the Go admin client, and keeping the administrative
# principal on the same mechanism means the broker needs exactly one enabled.
readonly SCRAM_MECHANISM="SCRAM-SHA-512"
readonly MIN_SCRAM_ITERATIONS=4096
readonly DEFAULT_LOG_DIRS="/var/lib/kafka/data"
readonly REQUIRED_AUTHORIZER="org.apache.kafka.metadata.authorizer.StandardAuthorizer"

# ---------------------------------------------------------------------------------------
# THE SAFE CREDENTIAL ALPHABET
#
# Two allow-lists, shared character-for-character with scripts/kafka-provision.sh. Any
# credential this script handles is checked against one of them before it is placed into a
# Kafka command, and a value outside the alphabet is REFUSED rather than escaped.
#
# WHY AN ALLOW-LIST RATHER THAN ESCAPING
#
# The value passed to "kafka-storage format --add-scram" is not a shell word, it is a
# sentence in Kafka's own mini-grammar:
#
#     SCRAM-SHA-512=[name=<user>,password=<secret>,iterations=<n>]
#
# Kafka parses that by splitting on ',' and '=' inside '[' ... ']'. THE GRAMMAR HAS NO
# ESCAPE SEQUENCE AT ALL - there is no backslash form, no quoting form, and no length
# prefix - so a password containing a comma or a bracket cannot be expressed in it. Shell
# quoting does not help: quoting delivers the bytes intact to Kafka, and it is Kafka that
# then misreads them. The failure is silent in the worst way, too: 'a,b' is parsed as the
# end of the password followed by an unrecognised key, so a credential is seeded that is
# not the one the operator supplied and nothing they can authenticate with.
# scripts/kafka-provision.sh has the same problem twice over, in --add-config and in the
# JAAS properties value.
#
# An allow-list turns that class of corruption into an immediate, named refusal, and it is
# the only mechanism available given a grammar with no escapes.
#
# THE PRINCIPAL LIST IS STRICTER, AND FOR AN ADDITIONAL REASON
#
# A principal name reaches ACL bindings as "User:<name>", where '*' is Kafka's wildcard.
# A principal of '*' would therefore not be a corrupted grant but a grant to EVERYONE, so
# the principal alphabet excludes it along with every grammar character. What remains is
# the conventional identifier set - letters, digits, '.', '_', '@', '+' and '-' - which is
# what real Kafka principals look like.
#
# RELATIONSHIP TO THE GO SIDE
#
# event_admin.go's validateSCRAMPassword requires printable ASCII (0x21-0x7E). This
# alphabet is a strict SUBSET of that, and the difference is deliberate rather than an
# inconsistency: the Go path sends the password over the Kafka protocol as a length-prefixed
# field, where no grammar applies and every printable byte is safe, while these scripts must
# put it through a CLI grammar that cannot express those bytes at all. A credential accepted
# here is therefore always accepted by the Go path as well.
#
# The excluded printable characters, and what each one breaks:
#   , = [ ]   Kafka's --add-scram / --add-config value grammar
#   " \       the JAAS value and Java Properties escaping used by the provision script
#   ;         terminates a JAAS login-module entry
#   ' ` $     shell metacharacters; excluded as defence in depth, not because quoting is
#             wrong here, but so that a credential can never depend on it being right
#   space     Java Properties line handling, and word splitting in any consumer of it
# Control characters and non-ASCII are excluded by construction.
# ---------------------------------------------------------------------------------------
readonly CREDENTIAL_SAFE_ERE='^[A-Za-z0-9!#%&()*+./:<>?@_{|}~^-]+$'

readonly PRINCIPAL_SAFE_ERE='^[A-Za-z0-9._@+-]+$'
readonly CREDENTIAL_SAFE_DESCRIPTION="letters, digits and ! # % & ( ) * + - . / : < > ? @ ^ _ { | } ~"
readonly PRINCIPAL_SAFE_DESCRIPTION="letters, digits and . _ @ + -"

# ---------------------------------------------------------------------------------------
# CREDENTIAL STRENGTH FLOOR (S6-07)
#
# The alphabet above answers "can Kafka's grammar carry this value", which is a question
# about SYNTAX and says nothing whatever about strength. A one-character password passes it:
# "a" is drawn entirely from the allowed set. This script's whole job is to seed the SCRAM
# credential the brokers and every administrative client authenticate with, so the alphabet
# check on its own would let it seed that credential with a password brute-forced in one
# guess - and then report success.
#
# TWO NUMBERS RATHER THAN ONE, because length alone is not strength. A 32-character run of
# one letter is 32 characters and one character of entropy, and a padded or repeated value
# is exactly what a hurried operator produces. Requiring 16 DISTINCT characters rejects that
# class without rejecting anything a generator produces: 32 characters drawn uniformly from
# the 62-character alphanumeric set contain 16 or more distinct characters with overwhelming
# probability.
#
# 32 IS THE SAME FLOOR THE REST OF THE STACK USES. It is what .env.example recommends, what
# the failure messages here recommend, and what scripts/kafka-provision.sh both enforces and
# generates - deliberately identical, so that the bootstrap credential a broker is formatted
# with cannot be weaker than the subscriber credentials provisioned against it. The two
# scripts hold their own copies rather than sharing a library because they are independent
# entry points that must each be runnable alone; the values and the reasoning are the same
# on both sides, and a change to one belongs in the other.
#
# A SCRAM-SHA-512 credential derived from fewer than 32 printable characters is the weakest
# link in a chain whose other end is PBKDF2 at MIN_SCRAM_ITERATIONS (4096) iterations.
# ---------------------------------------------------------------------------------------
readonly MIN_SECRET_LENGTH=32
readonly MIN_SECRET_DISTINCT=16

# Resolved during main; declared here so the data flow between the steps is visible.
STORAGE_CLI=""
KRAFT_CONFIG=""
LOG_DIRS=""

# Every scratch file this script creates that has held, or could hold, a credential.
# cleanup() removes all of them on any exit path, including a signal.
SCRATCH_FILES=()

# ---------------------------------------------------------------------------------------
# Diagnostics
#
# In every one of these, the first argument is the headline and any further arguments are
# printed as indented continuation lines. Keeping continuation lines as separate arguments
# is what lets the failure messages below be genuinely actionable without embedding
# newlines and stray indentation in string literals.
#
# Note what is absent: there is no shell tracing anywhere in this script, and no helper
# that can print KAFKA_SASL_ADMIN_SECRET. "set -x" would echo the assembled --add-scram
# argument, password included, into the container log.
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

# Strip leading and trailing whitespace. Used for values read out of a properties file and
# for entries in delimited lists, where surrounding spaces are legal.
trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "$value"
}

# Split a delimited list into one trimmed, non-empty entry per line. Used for the
# comma-separated log directory list and the semicolon-separated super.users list. Written
# with parameter expansion rather than by reassigning IFS so that no global state is
# mutated and no word-splitting surprises are possible.
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

# Read one key out of the resolved properties file. An unreadable or missing file yields an
# empty answer rather than an error, so every caller is safe under "set -e".
#
# Kafka loads its configuration with java.util.Properties, so this mirrors the parts of that
# format a Kafka operator actually uses: '#' and '!' introduce comments, either '=' or ':'
# separates key from value, whitespace around the separator is legal, and a later assignment
# overrides an earlier one. The key is matched as a quoted literal inside the regex so that
# the dots in names like log.dirs cannot act as wildcards.
read_property() {
    local key="$1" line value=""
    if [[ ! -r "$KRAFT_CONFIG" ]]; then
        return 0
    fi
    # The '|| [[ -n "$line" ]]' tail makes a final line without a trailing newline count.
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="$(trim "$line")"
        if [[ -z "$line" || "$line" == "#"* || "$line" == "!"* ]]; then
            continue
        fi
        if [[ ! "$line" =~ ^"${key}"[[:space:]]*[=:](.*)$ ]]; then
            continue
        fi
        value="$(trim "${BASH_REMATCH[1]}")"
    done <"$KRAFT_CONFIG"
    printf '%s' "$value"
}

# ---------------------------------------------------------------------------------------
# Preconditions
#
# Everything here runs before any storage is touched, and each failure names the variable
# or the fix the operator needs rather than leaving them to infer it.
# ---------------------------------------------------------------------------------------

# Locate the storage CLI. Confluent Platform images expose unsuffixed wrappers on PATH;
# Apache Kafka distributions ship kafka-storage.sh. Probe PATH for both, in that order,
# then fall back to the well-known installation directories - PATH alone is not enough,
# because apache/kafka images install the tools in /opt/kafka/bin without adding that
# directory to PATH, so a PATH-only probe fails inside the very container this script is
# meant to run in.
detect_storage_cli() {
    local candidate
    for candidate in kafka-storage kafka-storage.sh; do
        if command -v "$candidate" >/dev/null 2>&1; then
            STORAGE_CLI="$(command -v "$candidate")"
            return 0
        fi
    done

    for candidate in \
        /opt/kafka/bin/kafka-storage.sh \
        /opt/kafka/bin/kafka-storage \
        /usr/bin/kafka-storage \
        /usr/bin/kafka-storage.sh \
        /opt/bitnami/kafka/bin/kafka-storage.sh \
        /usr/local/kafka/bin/kafka-storage.sh; do
        if [[ -x "$candidate" ]]; then
            STORAGE_CLI="$candidate"
            return 0
        fi
    done

    die "neither 'kafka-storage' nor 'kafka-storage.sh' could be found." \
        "Searched PATH for both names, then /opt/kafka/bin, /usr/bin," \
        "/opt/bitnami/kafka/bin and /usr/local/kafka/bin." \
        "This script formats a broker's own storage, so it is meant to run inside the" \
        "Kafka container - as that service's entrypoint - or on a host where the Kafka" \
        "distribution is installed. It cannot bootstrap a broker from anywhere else."
}

# Best-effort version detection, used only to enrich a diagnostic. Apache Kafka's
# kafka-storage has no --version subcommand and answers with a usage error, so the result
# is filtered: anything that does not look like a version number is discarded and the
# caller simply omits it rather than pasting an error string into a message.
detect_kafka_version() {
    local raw
    raw="$("$STORAGE_CLI" --version 2>&1 || true)"
    raw="$(trim "${raw%%$'\n'*}")"
    if [[ "$raw" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?([-.][A-Za-z0-9]+)*$ ]]; then
        printf '%s' "$raw"
    fi
}

# Assert the version floor by capability rather than by parsing a version string: ask the
# CLI whether "format" accepts --add-scram. Simpler, more honest, and it works on releases
# whose --version output is unhelpful. The probe is guarded because a CLI below the floor
# can exit non-zero for an unrecognised subcommand, which must not abort under "set -e".
require_add_scram_support() {
    local help_output
    help_output="$("$STORAGE_CLI" format --help 2>&1 || true)"
    if [[ "$help_output" == *"--add-scram"* ]]; then
        return 0
    fi

    local detected
    detected="$(detect_kafka_version)"
    die "this Kafka release does not support 'kafka-storage format --add-scram'${detected:+ (detected version ${detected})}." \
        "The flag arrived in Kafka 3.5 (Confluent Platform 7.5.0). Without it a SCRAM" \
        "credential cannot be seeded into the KRaft metadata log, so the broker can never" \
        "authenticate a SASL client and bring-up fails outright." \
        "CLI in use: ${STORAGE_CLI}" \
        "Fix: point KAFKA_IMAGE at Kafka 3.5 or later. No specific stack version is named" \
        "here on purpose - a version written into a diagnostic is correct on the day it is" \
        "written and misleading from the next image bump onward, and this message said 3.9.1" \
        "for a while after the compose default moved past it. The floor is what matters, and" \
        "the version actually running is reported above when it could be detected; the tested" \
        "pin is KAFKA_IMAGE in docker-compose.yaml and .env.example."
}

# Refuse a credential that Kafka's SCRAM grammar cannot carry.
#
# Takes the variable NAME and its value, and prints only the name - never the value, since
# the value is a secret in every call. See the safe-alphabet block above for why a refusal
# is the only correct response to a character the grammar has no escape for.
require_safe_credential() {
    local name="$1" value="$2"
    if [[ ! "$value" =~ $CREDENTIAL_SAFE_ERE ]]; then
        die "${name} contains a character that Kafka's SCRAM credential grammar cannot carry." \
            "Allowed: ${CREDENTIAL_SAFE_DESCRIPTION}" \
            "The value is not echoed. It is refused rather than escaped because" \
            "'${SCRAM_MECHANISM}=[name=...,password=...]' is parsed by splitting on ',' and" \
            "'=' and has no escape sequence at all, so a password containing one of those" \
            "characters would be silently truncated into a credential nobody can" \
            "authenticate with - including you, on the next bring-up." \
            "Fix: generate the secret from the allowed set. For example:" \
            "  openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c 32" \
            "No storage has been touched."
    fi

    require_strong_credential "$name" "$value"
}

# ---------------------------------------------------------------------------------------
# Credential scratch files (Q4-20)
# ---------------------------------------------------------------------------------------

# Remove every scratch file, on every exit path.
#
# Registered for EXIT as well as the signals, so an interrupted or failed run cannot leave a
# file containing the administrative password behind in the container's filesystem. It is
# deliberately silent and always succeeds: it runs while an error is already being reported,
# and a cleanup failure must not replace that error with its own.
cleanup() {
    local file
    # An explicit conditional rather than '[[ -n $file ]] && rm ... || true'. In that form
    # the '||' binds to the whole '&&' chain, so it fires both when the name is empty AND
    # when the removal fails - one branch standing for two unrelated conditions. It happened
    # to behave correctly, but it read as though the '|| true' guarded only the rm, and the
    # next person to add a step to this loop would have had to work that out from scratch.
    for file in ${SCRATCH_FILES[@]+"${SCRATCH_FILES[@]}"}; do
        if [[ -n "$file" ]]; then
            rm -f "$file" 2>/dev/null || true
        fi
    done
    SCRATCH_FILES=()
}
trap cleanup EXIT INT TERM

# Create an empty scratch file that only this user can read, and register it for cleanup.
#
# Prints the path. The umask is set BEFORE creation and restored afterwards, so the file is
# never momentarily world-readable between mktemp and chmod - a window a chmod-after-write
# approach leaves open. The chmod is belt and braces for a filesystem or a TMPDIR whose
# behaviour makes the umask ineffective.
new_scratch_file() {
    local label="$1" path previous_umask
    previous_umask="$(umask)"
    umask 077
    path="$(mktemp "${TMPDIR:-/tmp}/blnk-kafka-bootstrap-${label}.XXXXXX")" || {
        umask "$previous_umask"
        die "could not create a scratch file for the ${label} step." \
            "Fix: make sure ${TMPDIR:-/tmp} is writable inside this container." \
            "No storage has been touched."
    }
    umask "$previous_umask"
    chmod 600 "$path" 2>/dev/null || true
    SCRATCH_FILES+=("$path")
    printf '%s' "$path"
}

# Does this CLI expand argparse4j "@file" argument files?
#
# This is the whole basis of the Q4-20 remedy, so it is PROBED rather than assumed: support
# comes from argparse4j's fromFilePrefix, which the Kafka tools enable but which is not
# guaranteed across every release and repackaging this script may run against.
#
# The probe is side-effect free. It writes a file whose only content is "--help" and runs
# "format @thatfile": if expansion works the parser reads --help from the file and exits 0
# after printing the usage block; if it does not, "@path" is left as an unrecognised
# positional argument and the parser exits non-zero. Either way no storage is touched,
# because --help short-circuits before any formatting decision.
supports_argument_file() {
    local probe output
    probe="$(new_scratch_file argfile-probe)"
    printf '%s\n' "--help" >"$probe"

    # Failure is an expected outcome here, so the pipeline is guarded against "set -e".
    output="$("$STORAGE_CLI" format "@${probe}" 2>&1 || true)"
    rm -f "$probe"

    # The usage block naming --add-scram is the positive signal. Matching on the flag rather
    # than on an exit code keeps the probe honest: some argparse4j versions print usage to
    # stderr and still exit 0, and a bare exit code cannot tell "help printed" from
    # "argument rejected".
    [[ "$output" == *"--add-scram"* && "$output" != *"unrecognized arguments"* ]]
}

# Count the distinct characters in a value, without ever printing any of them.
#
# fold -w1 puts one character per line, sort -u collapses duplicates, wc -l counts what is
# left. Every stage is a pipe, so no intermediate ever reaches a command line or a log.
# LC_ALL=C keeps "distinct" byte-wise rather than locale-dependent, which is what makes the
# count reproducible across the environments this script runs in.
count_distinct_characters() {
    local value="$1"
    printf '%s' "$value" | LC_ALL=C fold -w1 | LC_ALL=C sort -u | wc -l | tr -d '[:space:]'
}

# Refuse a secret too short or too repetitive to derive a broker credential from (S6-07).
#
# # Why this credential in particular
#
# It is not one account among many. This principal is seeded into the KRaft metadata log by
# 'kafka-storage format --add-scram' and placed in the broker's super.users, so it can create
# topics, mint and alter every other credential, and rewrite every ACL. A Kafka SASL
# handshake has no rate limit and no lockout, so a weak password on it is guessable at
# whatever rate an attacker can open connections.
#
# It is also close to UNFIXABLE after the fact. The credential lives in the metadata log from
# the moment storage is formatted, so a weak one cannot simply be corrected later - see the
# rotation guidance elsewhere in this script, which comes down to discarding the log.
# Refusing before formatting is the only cheap moment, which is why this runs where it does.
#
# # The floors
#
# MIN_SECRET_LENGTH and MIN_SECRET_DISTINCT mirror event_admin.go's MinSCRAMPasswordLength
# and MinSCRAMPasswordDistinctChars, and match the pair in scripts/kafka-provision.sh. All
# three being EQUAL is the point: a floor that differs between paths is a floor an operator
# can route around by choosing the more permissive path, and the shell paths used to be the
# permissive ones. A change to any one of the three belongs in the other two.
#
# # What is printed
#
# Neither the value nor any part of it - only its LENGTH and its DISTINCT COUNT, which is
# what the operator needs in order to fix it and is not enough to guess it. A length is not a
# secret; a prefix would be. (An earlier duplicate of this function withheld the length too,
# on the reasoning that reporting it narrows the search space. It does, but only for a value
# that is being REFUSED and therefore never used, and withholding it leaves an operator
# guessing at which of two floors they failed.)
require_strong_credential() {
    local name="$1" value="$2" distinct

    if ((${#value} < MIN_SECRET_LENGTH)); then
        die "${name} is ${#value} characters, below the ${MIN_SECRET_LENGTH}-character minimum." \
            "The alphabet check alone let a ONE-character password through: a single letter" \
            "is drawn entirely from the allowed set, so it says nothing about strength. This" \
            "is the credential the brokers themselves authenticate with." \
            "The value is not echoed, only its length." \
            "Fix: generate one from the allowed set. For example:" \
            "  openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c ${MIN_SECRET_LENGTH}" \
            "Set the same value in the broker's SCRAM JAAS configuration." \
            "No storage has been touched."
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
            "No storage has been touched."
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
            "sequence, and into ACL bindings as 'User:<name>', where '*' is the wildcard" \
            "that matches every principal - so a name containing one of those characters" \
            "would either be truncated or, in the case of '*', grant everyone access." \
            "Fix: use a plain identifier, for example 'admin'." \
            "No storage has been touched."
    fi
}

# Require a COMPLETE administrative credential pair, refuse an unsubstituted template
# placeholder, and hold both values to the safe alphabet.
#
# No branch prints the secret. The pair contract enforced here is the same one
# config.KafkaConfig.SASLAdminCredentials applies in Go and scripts/kafka-provision.sh
# applies for the runtime connection, so an operator debugging one learns the right rule
# about the others. This script is the one place where an EMPTY pair is also refused: its
# whole purpose is to seed a SCRAM credential into the metadata log, so "no SASL" is not a
# state it can act on - it is a request to do nothing, made by running the one command
# whose only job is to do this.
#
# .env.example ships both keys EMPTY rather than carrying a {PLACEHOLDER}, deliberately:
# stack.sh --init substitutes {POSTGRES_PASSWORD} and nothing else with a global sed, so any
# other brace placeholder would survive into .env as a literal password. --init then GENERATES
# both of these keys - the principal and a random 32-character password - through set_env_value
# rather than through that sed, which is why they can be empty in the template and still
# correct in a generated .env.
#
# So the empty case reaching this function means .env was hand-written or predates the Kafka
# keys, and the messages below say so. The placeholder branch remains as defence in depth for a
# hand-edited .env, because formatting storage with a literal placeholder yields a broker
# nobody can authenticate against and the failure would surface a long way from its cause.
require_admin_credentials() {
    local user secret
    # Trimmed before every test, so that a value carrying a trailing newline from a secret
    # store reads as absence rather than as a principal nobody created. This matches what
    # config.KafkaConfig.SASLAdminCredentials does in Go.
    user="$(trim "$KAFKA_SASL_ADMIN_USER")"
    secret="$(trim "$KAFKA_SASL_ADMIN_SECRET")"

    if [[ -z "$user" && -z "$secret" ]]; then
        die "neither KAFKA_SASL_ADMIN_USER nor KAFKA_SASL_ADMIN_SECRET is set." \
            "This script exists to seed a SASL/SCRAM credential into the KRaft metadata" \
            "log, so it needs both: a principal to create and a password to create it" \
            "with. Neither has a default - the username used to fall back to 'admin', and" \
            "that silently disagreed with the Go clients, which read the same empty value" \
            "as 'no SASL at all'." \
            "Fix: run './stack.sh --init'. It creates .env and GENERATES BOTH of these -" \
            "the principal 'admin' and a random 32-character password - so the usual cause" \
            "of seeing this message is that .env was written by hand or predates the Kafka" \
            "keys. Set them yourself in .env, or export them for this process. Use the same" \
            "values as the broker's SCRAM JAAS configuration." \
            "If you meant to run a broker with no SASL listener, you do not need this" \
            "script at all: format the storage without --add-scram."
    fi

    if [[ -z "$user" ]]; then
        die "KAFKA_SASL_ADMIN_SECRET is set but KAFKA_SASL_ADMIN_USER is empty." \
            "A SCRAM credential is a principal AND a password; there is nothing to name" \
            "this one after. The username is never defaulted, so no principal is invented" \
            "for you." \
            "Fix: set KAFKA_SASL_ADMIN_USER to the principal the brokers and" \
            "scripts/kafka-provision.sh will authenticate as - 'admin' is the conventional" \
            "local choice. The secret is not echoed and no storage has been touched."
    fi

    if [[ -z "$secret" ]]; then
        die "KAFKA_SASL_ADMIN_USER is set to '${user}' but KAFKA_SASL_ADMIN_SECRET is empty." \
            "The bootstrap SCRAM credential cannot be seeded without it, and there is no" \
            "default by design: a credential must not be guessable from source." \
            "Fix: run './stack.sh --init', which generates this value into .env - .env.example" \
            "ships the key empty and --init fills it with a random 32-character password. Or" \
            "set it in .env yourself, or export it for this process. Use the same value as" \
            "the broker's SCRAM JAAS configuration."
    fi

    if [[ "$secret" == "{"*"}" ]]; then
        die "KAFKA_SASL_ADMIN_SECRET still holds an unsubstituted {PLACEHOLDER} value." \
            "It was copied from a template and never replaced with a real secret. Using it" \
            "would format storage with a literal placeholder as the password, producing a" \
            "broker that rejects every client for a reason visible nowhere near here." \
            "Fix: run './stack.sh --init' to create .env, then set KAFKA_SASL_ADMIN_SECRET" \
            "to the password the broker's SCRAM JAAS configuration uses, or export it for" \
            "this process. No storage has been touched and the value is not echoed."
    fi

    require_safe_principal "KAFKA_SASL_ADMIN_USER" "$user"
    # Syntax first, then strength: the alphabet check answers whether Kafka's grammar can
    # carry the value at all, and the strength check answers whether it is worth carrying.
    # Both are needed - the alphabet check alone accepts a one-character password.
    require_safe_credential "KAFKA_SASL_ADMIN_SECRET" "$secret"
    require_strong_credential "KAFKA_SASL_ADMIN_SECRET" "$secret"

    # Publish the trimmed values, so that everything downstream - the --add-scram argument,
    # the super.users advisory and every diagnostic - works from exactly what was validated.
    KAFKA_SASL_ADMIN_USER="$user"
    KAFKA_SASL_ADMIN_SECRET="$secret"
}

# The iteration count must be a whole number at or above the SCRAM minimum; Kafka rejects
# anything lower outright.
require_valid_iterations() {
    if [[ ! "$KAFKA_SCRAM_ITERATIONS" =~ ^[0-9]+$ ]]; then
        die "KAFKA_SCRAM_ITERATIONS must be a positive integer, but is '${KAFKA_SCRAM_ITERATIONS}'." \
            "Fix: unset it to accept the default of ${MIN_SCRAM_ITERATIONS}, or set a whole number."
    fi

    # 10# forces base 10 so that a padded value such as 08192 is not read as invalid octal.
    if ((10#$KAFKA_SCRAM_ITERATIONS < MIN_SCRAM_ITERATIONS)); then
        die "KAFKA_SCRAM_ITERATIONS is ${KAFKA_SCRAM_ITERATIONS}, below the SCRAM minimum of ${MIN_SCRAM_ITERATIONS}." \
            "Kafka supports SCRAM-SHA-256 and SCRAM-SHA-512 and rejects a lower iteration" \
            "count outright." \
            "Fix: unset it to accept the default of ${MIN_SCRAM_ITERATIONS}, or raise the value."
    fi
}

# Resolve the KRaft properties file: an explicit KAFKA_KRAFT_CONFIG wins, otherwise probe
# the well-known layouts in order - /etc/kafka/kraft (Confluent images) then
# /opt/kafka/config/kraft and /opt/kafka/config (Apache images).
detect_kraft_config() {
    if [[ -n "$KAFKA_KRAFT_CONFIG" ]]; then
        if [[ ! -f "$KAFKA_KRAFT_CONFIG" ]]; then
            die "KAFKA_KRAFT_CONFIG is set to '${KAFKA_KRAFT_CONFIG}', which is not a file." \
                "Fix: point it at the server.properties the broker actually starts with," \
                "or unset it to let this script probe the well-known image layouts."
        fi
        KRAFT_CONFIG="$KAFKA_KRAFT_CONFIG"
        return 0
    fi

    local candidate
    for candidate in \
        /etc/kafka/kraft/server.properties \
        /opt/kafka/config/kraft/server.properties \
        /opt/kafka/config/server.properties; do
        if [[ -f "$candidate" ]]; then
            KRAFT_CONFIG="$candidate"
            return 0
        fi
    done

    die "no KRaft server.properties could be found." \
        "Probed /etc/kafka/kraft/server.properties," \
        "/opt/kafka/config/kraft/server.properties and /opt/kafka/config/server.properties." \
        "Fix: set KAFKA_KRAFT_CONFIG to the configuration file the broker starts with." \
        "A stack that mounts its own broker configuration - as Blnk's does - always needs" \
        "this, because a mounted path is by definition not one of the image defaults."
}

# Decide which directories to probe for existing formatting. Reading log.dirs out of the
# resolved configuration matters because an image's bundled default and the configuration a
# real stack mounts routinely disagree: apache/kafka's bundled KRaft file says
# /tmp/kraft-combined-logs while Blnk's broker says /var/lib/kafka/data. Probing the wrong
# directory would let this script skip a format that is genuinely needed.
resolve_log_dirs() {
    if [[ -n "$KAFKA_LOG_DIRS" ]]; then
        LOG_DIRS="$KAFKA_LOG_DIRS"
        return 0
    fi

    local from_config
    from_config="$(read_property "log.dirs")"
    if [[ -n "$from_config" ]]; then
        LOG_DIRS="$from_config"
        return 0
    fi

    LOG_DIRS="$DEFAULT_LOG_DIRS"
}

# Kafka's super.users is a semicolon-separated list of principals. Entries are compared
# whole and trimmed rather than by substring, so that a grant to User:admin2 cannot be
# mistaken for a grant to User:admin.
super_users_contains() {
    local list="$1" wanted="$2" entry
    while IFS= read -r entry; do
        if [[ "$entry" == "$wanted" ]]; then
            return 0
        fi
    done < <(split_list "$list" ";")
    return 1
}

# Broker-configuration advisories. Warnings by design, never failures: the properties file
# may be templated by the image entrypoint after this script runs, and being wrong about it
# must not block bring-up. Every probe goes through read_property, which cannot abort.
advise_broker_config() {
    if [[ ! -r "$KRAFT_CONFIG" ]]; then
        warn "cannot read ${KRAFT_CONFIG}, so the broker-configuration advisories were skipped."
        return 0
    fi

    local authorizer
    authorizer="$(read_property "authorizer.class.name")"
    if [[ "$authorizer" != "$REQUIRED_AUTHORIZER" ]]; then
        warn "authorizer.class.name is not the KRaft StandardAuthorizer in ${KRAFT_CONFIG}." \
            "Found:    ${authorizer:-<unset>}" \
            "Expected: ${REQUIRED_AUTHORIZER}" \
            "Without it the broker accepts ACLs but never enforces them, so every" \
            "subscriber can read every topic and the subscriber-isolation test" \
            "(event_isolation_integration_test.go, acceptance criterion V-5) passes" \
            "vacuously while proving nothing."
    fi

    local super_users
    super_users="$(read_property "super.users")"
    if ! super_users_contains "$super_users" "User:${KAFKA_SASL_ADMIN_USER}"; then
        warn "super.users in ${KRAFT_CONFIG} does not list User:${KAFKA_SASL_ADMIN_USER}." \
            "Found: ${super_users:-<unset>}" \
            "Once the authorizer is active, the topic creation and ACL grants performed by" \
            "scripts/kafka-provision.sh will be denied for this principal."
    fi
}

# ---------------------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------------------

# Kafka writes meta.properties into each log directory on a successful format, so its
# presence is the durable marker that storage has been bootstrapped - and therefore that
# the SCRAM credential is already in the metadata log. Success is reported only when every
# configured directory has one, so a partially formatted set still goes through format,
# where --ignore-formatted deals with the directories that are already done.
already_formatted() {
    local dir probed=0
    while IFS= read -r dir; do
        probed=$((probed + 1))
        if [[ ! -f "${dir}/meta.properties" ]]; then
            return 1
        fi
    done < <(split_list "$LOG_DIRS" ",")

    # Nothing probed means nothing is known to be formatted.
    if ((probed == 0)); then
        return 1
    fi
    return 0
}

# Resolve the cluster ID: the pinned value if there is one, otherwise a fresh one from the
# CLI. Because an already-formatted volume is skipped, a generated ID is only ever used on
# a genuinely first format.
resolve_cluster_id() {
    local cluster_id="$KAFKA_CLUSTER_ID" source="KAFKA_CLUSTER_ID"

    if [[ -z "$cluster_id" ]]; then
        source="kafka-storage random-uuid"
        cluster_id="$("$STORAGE_CLI" random-uuid 2>/dev/null || true)"
        cluster_id="$(trim "${cluster_id%%$'\n'*}")"
        if [[ -z "$cluster_id" ]]; then
            die "'${STORAGE_CLI} random-uuid' produced no cluster ID." \
                "Fix: set KAFKA_CLUSTER_ID to a base64 UUID, for example one generated on" \
                "another machine with the same command."
        fi
    fi

    # A cluster ID is a base64url token. Rejecting anything else here turns a confusing
    # CLI parse error into a named-variable diagnosis.
    if [[ ! "$cluster_id" =~ ^[A-Za-z0-9_-]+$ ]]; then
        die "the cluster ID from ${source} is not a valid base64 token." \
            "Fix: set KAFKA_CLUSTER_ID to a value made up of letters, digits, '-' and '_'."
    fi

    printf '%s' "$cluster_id"
}

# Format the storage, seeding the administrative SCRAM credential in the same operation.
bootstrap_storage() {
    local cluster_id
    cluster_id="$(resolve_cluster_id)"

    # The one and only place the secret is used. It is assembled into a single variable and
    # passed as a single argument, so no shell quoting can split it and no partially
    # interpolated command string exists to be logged by accident.
    #
    # BOTH INTERPOLATED VALUES HAVE ALREADY BEEN HELD TO THE SAFE ALPHABET by
    # require_admin_credentials, which is what makes this line correct rather than merely
    # conventional. Shell quoting delivers the bytes to Kafka intact; it is KAFKA that then
    # splits this value on ',' and '=' with no escape sequence available, so a comma or a
    # bracket here would silently seed a credential that is not the one supplied. The
    # alphabet check is the only defence against that, and it runs before anything is
    # formatted.
    #
    # THE SECRET DOES NOT GO INTO argv WHEN THE CLI CAN AVOID IT (Q4-20).
    #
    # A value passed as a command-line argument is visible in the process table to every
    # process in the namespace for as long as the command runs, and "kafka-storage format"
    # runs long enough to be caught. "format --help" advertises no file-based alternative
    # for --add-scram, which is what makes this look unavoidable - but the Kafka tools are
    # built on argparse4j with fromFilePrefix enabled, so ANY argument, including
    # --add-scram and its value, can be supplied from an "@file" instead. The file is
    # created mode 0600, holds the credential for the duration of one command, and is
    # removed immediately afterwards and again by the EXIT trap.
    #
    # Verified against apache/kafka 3.9: formatting with "@argfile" carrying --add-scram
    # produces a 434-byte bootstrap.checkpoint containing the principal, against 249 bytes
    # and no principal without it, so the flag is genuinely honoured from the file rather
    # than silently dropped.
    #
    # THE FALLBACK IS STILL argv, AND IT WARNS. supports_argument_file() probes rather than
    # assumes, because fromFilePrefix is not guaranteed across every Kafka repackaging. When
    # the probe says no, the credential goes on the command line as before - the alternative
    # would be to refuse to bootstrap at all, which turns a disclosure risk that only
    # matters to co-tenants of this container into a total bring-up failure. The operator is
    # told, by name, which path was taken and why.
    #
    # WHAT REMAINS EXPOSED EITHER WAY. Nothing here removes the secret from the environment
    # of this process, which is where it arrived from. That is a different exposure with a
    # different remedy (a secret store), and it is not this script's to close. Production
    # subscriber credentials never touch a command line at all: they are provisioned in
    # process by the Go admin client, event_admin.go via AlterUserScramCredentials.
    local scram_credential
    scram_credential="${SCRAM_MECHANISM}=[name=${KAFKA_SASL_ADMIN_USER},password=${KAFKA_SASL_ADMIN_SECRET},iterations=${KAFKA_SCRAM_ITERATIONS}]"

    # Assemble the format invocation, putting --add-scram and its value behind an argument
    # file whenever the CLI will read one. Everything else stays on the command line: a
    # cluster ID, a config path and an idempotency flag are not secrets, and keeping them
    # visible is what makes the log line above worth reading.
    #
    # --ignore-formatted complements the meta.properties probe above: it is Kafka's own
    # idempotency flag and covers the partially formatted multi-directory case the probe
    # deliberately declines to treat as done. The CLI's own stdout and stderr are left
    # attached so that a failure explains itself.
    local -a format_args=(format --cluster-id "$cluster_id" --config "$KRAFT_CONFIG" --ignore-formatted)
    local scram_argfile="" delivery="an argument file (mode 0600); the secret is not in argv"

    if supports_argument_file; then
        scram_argfile="$(new_scratch_file add-scram)"
        # One argument per line: argparse4j reads an @file that way, so the flag and its
        # value must not share a line. printf, not echo, so no value is ever interpreted.
        printf '%s\n' "--add-scram" "$scram_credential" >"$scram_argfile"
        format_args+=("@${scram_argfile}")
    else
        # The credential still HAS to be seeded - a format that silently omitted --add-scram
        # would succeed and produce a broker no client can ever authenticate against, which is
        # a far worse outcome than the argv exposure this branch is conceding.
        format_args+=(--add-scram "$scram_credential")
        delivery="the command line, because this CLI does not expand @argument-files"
        warn "this Kafka CLI does not support @argument-files, so the bootstrap SCRAM" \
            "credential must be passed as a command-line argument, where it is briefly" \
            "visible in this container's process table." \
            "CLI in use: ${STORAGE_CLI}" \
            "This affects only the bootstrap credential for this local broker. It is not" \
            "how production credentials are issued - those are provisioned in process by" \
            "event_admin.go and never reach a command line." \
            "To close it: use an image whose Kafka tools expand @argument-files (verified" \
            "working on apache/kafka 3.9), or format storage out of band and mount the" \
            "already-formatted volume."
    fi

    log "formatting KRaft storage" \
        "log dirs   : ${LOG_DIRS}" \
        "cluster ID : ${cluster_id}" \
        "config     : ${KRAFT_CONFIG}" \
        "admin user : ${KAFKA_SASL_ADMIN_USER}" \
        "mechanism  : ${SCRAM_MECHANISM}, ${KAFKA_SCRAM_ITERATIONS} iterations" \
        "credential : delivered via ${delivery}"

    if ! "$STORAGE_CLI" "${format_args[@]}"; then
        # Remove the credential file before reporting, so the secret is gone from disk even
        # if the operator spends the next hour reading the diagnosis. The EXIT trap would
        # also do this; doing it here makes the ordering explicit rather than incidental.
        [[ -n "$scram_argfile" ]] && rm -f "$scram_argfile"
        die "'kafka-storage format' failed; its own output is above." \
            "The three usual causes:" \
            "  1. the image is below the Kafka 3.5 / Confluent Platform 7.5.0 floor, so" \
            "     --add-scram is unsupported - check KAFKA_IMAGE;" \
            "  2. ${KRAFT_CONFIG} is not the file the broker starts with, does not match" \
            "     this image's layout, or holds a value Kafka rejects - it validates the" \
            "     whole configuration before formatting, and the output above names the" \
            "     offending setting. Set KAFKA_KRAFT_CONFIG to the right file;" \
            "  3. the cluster ID disagrees with an existing meta.properties in one of the" \
            "     log directories, which the output above names. Unset KAFKA_CLUSTER_ID to" \
            "     reuse the volume's own ID, or remove the volume to start clean." \
            "The SCRAM credential was delivered via ${delivery}; it is not echoed here and" \
            "any scratch file holding it has been removed."
    fi

    # The credential has served its purpose; remove it now rather than at exit.
    [[ -n "$scram_argfile" ]] && rm -f "$scram_argfile"

    ok "KRaft storage formatted with cluster ID ${cluster_id}." \
        "The SCRAM principal ${KAFKA_SASL_ADMIN_USER} (${SCRAM_MECHANISM}) is now in the" \
        "__cluster_metadata log, so the broker can start and authenticate SASL clients." \
        "Next: start the broker, then run scripts/kafka-provision.sh to create the topics," \
        "the per-category dead-letter topics and the subscriber principals."
}

# ---------------------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------------------

# usage prints what this script does, how it is invoked and every variable it reads.
#
# IT EXISTS BECAUSE ASKING WAS ANSWERED WITH THE WRONG FAILURE. `--help` used to fall
# through into detect_storage_cli and exit 1 with "neither 'kafka-storage' nor
# 'kafka-storage.sh' could be found" - true of the host asking the question, and no answer to
# it. A script whose entire interface is an environment contract has to be able to state that
# contract, and the state of the host's PATH must not decide whether it can.
usage() {
    printf '%s\n' "Usage: scripts/kafka-bootstrap.sh [-h|--help] [command [argument ...]]"
    printf '%s\n' ""
    printf '%s\n' "Formats Kafka KRaft storage and seeds the administrative SCRAM credential into the"
    printf '%s\n' "cluster metadata log in the SAME operation, which is the only way the first"
    printf '%s\n' "credential can exist: a KRaft broker cannot authenticate a SASL client until one is"
    printf '%s\n' "already in the log, and the runtime command that would add one needs an"
    printf '%s\n' "authenticated connection. Run it BEFORE the broker's first start."
    printf '%s\n' ""
    printf '%s\n' "Idempotent: already-formatted storage is detected and skipped, never treated as an"
    printf '%s\n' "error, so it is safe on every bring-up."
    printf '%s\n' ""
    printf '%s\n' "With no arguments it formats, reports and exits 0, and the caller starts the broker."
    printf '%s\n' "With a trailing command it replaces itself with that command after a successful"
    printf '%s\n' "bootstrap - the conventional container-entrypoint form, and it takes that step on the"
    printf '%s\n' "already-formatted path too:"
    printf '%s\n' ""
    printf '%s\n' "  scripts/kafka-bootstrap.sh kafka-server-start.sh /etc/kafka/server.properties"
    printf '%s\n' ""
    printf '%s\n' "There are no options beyond -h/--help. Everything else is configured through the"
    printf '%s\n' "environment, and the header of this file documents each variable at its point of use:"
    printf '%s\n' ""
    printf '  %-27s %s\n' "KAFKA_SASL_ADMIN_USER" "(required, no default) the principal to seed"
    printf '  %-27s %s\n' "KAFKA_SASL_ADMIN_SECRET" "(required, no default) its password"
    printf '  %-27s %s\n' "KAFKA_SCRAM_ITERATIONS" "4096, which is also the minimum"
    printf '  %-27s %s\n' "KAFKA_CLUSTER_ID" "(unset) generated when absent; pin to reproduce"
    printf '  %-27s %s\n' "KAFKA_KRAFT_CONFIG" "(unset) auto-detected from known image layouts"
    printf '  %-27s %s\n' "KAFKA_LOG_DIRS" "(unset) falls back to log.dirs, then a default"
    printf '%s\n' ""
    printf '%s\n' "Both credential values must be drawn from the safe alphabet described in this file's"
    printf '%s\n' "header: Kafka's --add-scram grammar has no escape sequence, so a value carrying a"
    printf '%s\n' "comma or a bracket is refused rather than silently truncated."
    printf '%s\n' ""
    printf '%s\n' "Runtime per-subscriber principals are NOT created here. They are added once the"
    printf '%s\n' "broker is up, by scripts/kafka-provision.sh locally and by the Go admin client in"
    printf '%s\n' "production. The order is: bootstrap, then broker, then provisioning."
}

# parse_arguments answers -h/--help/help and refuses an unrecognised OPTION, leaving anything
# else alone: a trailing command is this script's documented second form, and it is passed
# through untouched. Only the FIRST argument is inspected, because everything after it belongs
# to that command rather than to this script.
#
# An unrecognised option is refused rather than exec'd. `scripts/kafka-bootstrap.sh --dry-run`
# would otherwise be handed to the shell as a command name and fail as "not found", which
# reads like a broken image rather than a flag this script does not have.
parse_arguments() {
    case "${1:-}" in
        -h | --help | help )
            usage
            exit 0
            ;;
        -* )
            usage >&2
            die "unrecognised option '${1}'." \
                "This script takes -h/--help and an optional trailing COMMAND to run after a" \
                "successful bootstrap; it has no other options, and everything else is" \
                "configured through the environment as the usage above lists." \
                "No storage has been touched."
            ;;
    esac
}

main() {
    # BEFORE ANYTHING ELSE, including CLI detection: asking what this script does must not
    # depend on whether the host it is asked on has a Kafka distribution installed.
    parse_arguments "$@"

    log "bootstrapping Kafka KRaft storage for Blnk event streaming"

    detect_storage_cli
    log "using storage CLI ${STORAGE_CLI}"

    require_add_scram_support
    require_admin_credentials
    require_valid_iterations

    detect_kraft_config
    log "using KRaft configuration ${KRAFT_CONFIG}"

    resolve_log_dirs
    advise_broker_config

    if already_formatted; then
        ok "KRaft storage in ${LOG_DIRS} is already formatted." \
            "The bootstrap SCRAM credential is already in the metadata log, so there is" \
            "nothing to do. Runtime principals are added by scripts/kafka-provision.sh" \
            "against the running broker, not from here."
    else
        bootstrap_storage
    fi

    # Optional command chaining. With no arguments the script simply finishes and the caller
    # starts the broker itself; with arguments it becomes the given command.
    #
    # Note that the hand-off happens on the already-formatted path too. That is the point:
    # from the second bring-up onwards against a reused kafka_data volume, formatting is
    # skipped, and a script that stopped there would leave the broker permanently unstarted.
    if (($# > 0)); then
        # Only the program name is echoed. A chained command's arguments are not this
        # script's to inspect and could themselves carry a credential.
        log "bootstrap complete, handing off to ${1}"
        exec "$@"
    fi

    log "bootstrap complete"
}

main "$@"
