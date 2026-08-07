#!/bin/bash
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
# This script simplifies work with the docker-compose stack, execute it without parameters for help.

# Error handling
set -o errexit          # Exit on most errors
set -o errtrace         # Make sure any error trap is inherited
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o nounset          # Exits if any of the variables is not set

GRN='\033[0;32m' && YEL='\033[1;33m' && RED='\033[0;31m' && BLU='\033[0;34m' && NC='\033[0m'

declare env=".env"                  # name of the standard environment file
declare example=".env.example"      # name of the sample environment file
declare COMPOSE_CL="docker-compose" # docker-compose command syntax varies. You can override it in the .env

# The provisioning script this stack delegates topic and principal creation to. Path is
# relative to the working directory, exactly as ${env}, ${example} and $COMPOSE_FILE are,
# so all four resolve against the same tree - deriving this one from $0 instead would let it
# come from a different checkout than the .env it is being configured by.
declare kafka_provision_script="scripts/kafka-provision.sh"

# The administrative Kafka principal --init writes into ${env}. A principal NAME, not a secret,
# which is why it is a literal here at all: no compose service defaults it any more - a default
# for a cluster superuser's credential is a hardcoded credential, and both Kafka scripts refuse a
# half-configured pair by name - so ${env} is the single place the name and its password are
# written, and every service reads them from there. That is what keeps the credential the broker
# is bootstrapped with and the one its clients present from drifting apart. The password is
# generated per stack by --init and never written down in this file.
declare kafka_admin_principal="admin"

# The steady-state producer principal --init writes into ${env}, and the same default the
# kafka-init service carries so that the principal the script mints is the principal the
# server and worker present. A principal NAME, not a secret; its password is generated per
# stack and never written down here.
#
# It exists as a SEPARATE identity from the administrative one above because Blnk refuses to
# publish as a cluster superuser: an administrative credential can create topics, mint SCRAM
# credentials and rewrite ACLs, so a leaked publisher credential that happened to be the
# administrative one would compromise the cluster's authorization state rather than merely
# allow publishing.
declare kafka_producer_principal="blnk-producer"

# The only mode ${env} may ever have: read and write for its owner, nothing for anyone else.
#
# ${env} holds the PostgreSQL password, the Kafka broker superuser secret and the Kafka
# producer secret. It was created by copying ${example}, which is a committed file and
# therefore world-readable, so a mode-0644 ${env} handed every local account and every
# process outside this stack the credential that can rewrite every ACL in the broker. On a
# shared or multi-tenant host that is a credential disclosure, not a theoretical one.
declare env_file_mode="600"
# The number of RANDOM BYTES behind each generated Kafka credential.
#
# THE BYTE COUNT MUST STAY A MULTIPLE OF THREE. base64 pads a remainder with "=", and "=" is
# outside the credential alphabet kafka-bootstrap.sh and kafka-provision.sh share - Kafka's
# --add-scram value grammar has no escape sequence for it - so a padded password is refused
# and the broker never bootstraps. 24 bytes give 32 unpadded characters, which is exactly the
# 32-character floor both scripts and event_admin.go's MinSCRAMPasswordLength enforce.
declare kafka_secret_bytes=24
# EVERY VARIABLE THE PROVISIONING SCRIPT READS, in one list, forwarded to it when the host
# fallback runs it directly.
#
# One list rather than a hand-written argument sequence, because the hand-written one had drifted:
# it carried ten names out of the twenty-odd the script documents, so a stack whose ${env}
# pinned a sample principal, a consumer-group prefix, a topic grant, a rotation or skip request,
# an operator-supplied client configuration or a readiness budget got that setting when compose
# ran the one-shot and SILENTLY LOST IT when this fallback ran instead. The two paths provisioned
# differently from the same ${env}, and which one ran depended only on whether the host happened
# to have the Kafka CLI - so the difference was invisible and unreproducible.
#
# THIS LIST IS THE SCRIPT'S OWN DELEGATION LIST. scripts/kafka-provision.sh forwards exactly
# these names when it re-executes itself inside the broker container, so the two are the same
# interface seen from either side, and TestKafkaProvisionScript_HandsOffEverySupportedSetting
# asserts they stay in step. Adding a variable to the script means adding it here.
#
# Deliberately absent: BLNK_KAFKA_PROVISION_IN_CONTAINER, which is the script's own
# already-delegated marker and setting it from here would tell a host-side run it must not
# delegate. KAFKA_BROKERS and KAFKA_COMPOSE_SERVICE are absent too because provision_kafka
# computes both - the effective broker list and the service it was asked about - and a stale
# ${env} value must not win over either.
declare -a kafka_provision_passthrough=(
    KAFKA_BOOTSTRAP_SERVER
    KAFKA_TOPIC_PREFIX
    KAFKA_MIN_PARTITIONS
    KAFKA_REPLICATION_FACTOR
    KAFKA_ALLOW_PARTITION_GROWTH
    KAFKA_SASL_USER
    KAFKA_SASL_SECRET
    KAFKA_SASL_SECRET_FILE
    KAFKA_PRODUCER_USER
    KAFKA_PRODUCER_SECRET
    KAFKA_ROTATE_PRODUCER_SECRET
    KAFKA_SKIP_PRODUCER_PRINCIPAL
    KAFKA_SASL_ADMIN_USER
    KAFKA_SASL_ADMIN_SECRET
    KAFKA_SECURITY_PROTOCOL
    KAFKA_SCRAM_ITERATIONS
    KAFKA_PROVISION_TIMEOUT_SECONDS
    KAFKA_PROVISION_POLL_INTERVAL_SECONDS
    KAFKA_SAMPLE_SUBSCRIBER_USER
    KAFKA_SAMPLE_SUBSCRIBER_SECRET
    KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE
    KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX
    KAFKA_SAMPLE_SUBSCRIBER_TOPICS
    KAFKA_SKIP_SAMPLE_SUBSCRIBER
    KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET
    KAFKA_CLIENT_CONFIG
    KAFKA_CLIENT_CONFIG_CONTAINER_PATH
    KAFKA_CONTAINER
    KAFKA_PROVISION_CONTAINER_SCRIPT
)

# KAFKA_BROKERS as the SHELL supplied it, captured before anything can overwrite it:
# showenv() sources ${env} ahead of main(), so an assignment there would replace it. Compose
# resolves interpolation from the shell environment FIRST and from --env-file only after, and
# this reproduces that precedence - reading the post-source value alone would skip the Kafka
# checks for someone running "KAFKA_BROKERS=kafka:9092 ${0} -u" against a .env that leaves
# the key empty.
#
# TWO VARIABLES, BECAUSE "UNSET" AND "SET TO EMPTY" ARE DIFFERENT ANSWERS. Capturing the value
# alone conflated them, and the conflation broke precedence in one direction: compose treats an
# explicitly empty shell KAFKA_BROKERS as a real value that overrides --env-file, so
# "KAFKA_BROKERS= ${0} -u" turns publishing OFF for the containers - but this script, seeing an
# empty capture, fell through to the .env value and then verified a broker the application was
# not going to use. Someone deliberately disabling Kafka for one run would have had their
# bring-up fail on a Kafka problem that did not concern them.
#
# ${VAR+yes} expands to "yes" when VAR is declared AT ALL, empty or not, and to nothing when it
# is unset - which is exactly the distinction, and the only portable way to make it. The value
# uses ${VAR-} rather than ${VAR} so that this file does not depend on nounset staying off.
declare KAFKA_BROKERS_DECLARED_IN_SHELL="${KAFKA_BROKERS+yes}"
declare KAFKA_BROKERS_FROM_SHELL="${KAFKA_BROKERS-}"

help() {
    printf "\n \
    Usage:${BLU} ${0} ${GRN}parameters${NC}\n \
    ${GRN}--pull, -p${NC}\t\t Pull the repo from registry\n \
    ${GRN}--up,-u${NC}\t\t Provision Kafka, then spin the stack up\n \
    ${GRN}--build,-b${NC}\t\t Provision Kafka, then build and spin the stack up\n \
    ${GRN}--down,-d${NC}\t\t Shut down, keeping every data volume\n \
    ${GRN}--purge${NC}\t\t Shut down AND DELETE every data volume\n \
    ${GRN}--restart,-r${NC}\t Cold-restart, then verify Kafka provisioning\n \
    ${GRN}--init,-i${NC}\t\t Create .env, or fill in the keys an existing one is missing\n \
    \n \
    A bring-up FAILS when this project's own Kafka broker cannot be verified - it never\n \
    became healthy, or its catalogue could not be provisioned. Nothing is torn down; the\n \
    exit status is what says the stack cannot publish an event yet. An external broker or\n \
    an empty ${GRN}KAFKA_BROKERS${NC} is reported and never fatal.\n \
    \n \
    The Kafka broker is OPT-IN: it and its provisioning one-shot sit behind the\n \
    ${GRN}kafka${NC} compose profile, which this script enables only when ${GRN}KAFKA_BROKERS${NC}\n \
    is set. With it empty the ledger runs exactly as it did before Kafka existed.\n \
    Teardown always includes the profile, so nothing is left behind.\n \
    \n \
    Examples:
    ${BLU} ${0} ${GRN}-u${NC}\n\
    ${BLU} ${0} ${GRN}--purge --yes${NC}\n\
    \n\
    "
}

showenv() {
    if [ -r "${env}" ]
    then
        # ${env} is a fixed relative path declared at the top of this file, not a value from
        # the caller, so there is nothing dynamic to follow - but ShellCheck cannot know that
        # from a variable, and SC1090 is the right warning for the general case. The directive
        # records the judgement rather than silencing it blindly: .env is generated data whose
        # keys ShellCheck could not usefully check even if it could find the file.
        # shellcheck source=/dev/null
        source "${env}"
    fi
    printf "\n ==== Environment ====\n"
    printf "      Stack name : ${YEL}$COMPOSE_PROJECT_NAME${NC}\n"
    printf "    Compose file : ${BLU}$COMPOSE_FILE${NC}\n"
    printf "  CL parameter 0 : ${BLU}${0}${NC}\n"
    printf "  CL parameter 1 : ${BLU}${1}${NC}\n"
    printf "  CL parameter 2 : ${BLU}${2}${NC}\n"
    printf "  CL parameter 3 : ${BLU}${3}${NC}\n"
    if [ -r ${env} ]
    then
      printf "             env : ${BLU}${env}${NC}\n"
    else
      printf "             env : ${RED}${env}${NC} not found. You may want to initialize the stack with -i parameter\n"
    fi
    printf " =====================\n"
}

# =======================================================================================
# Environment file
# =======================================================================================

# Writes a value into a key of ${env}, in whichever shape ${example} declares that key.
#
# ${example} declares keys in TWO shapes and both are handled here, so neither can produce a
# .env that looks configured and is not:
#
# THAT SECOND SHAPE IS WHY THIS FUNCTION EXISTS RATHER THAN A SECOND SED LINE. A second brace
# placeholder was never an option, and handling the empty assignment is what lets ${example}
# ship the Kafka keys empty and still yield a correct ${env}.
#
# ${example}, scripts/kafka-bootstrap.sh and scripts/kafka-provision.sh all described --init as
# substituting {POSTGRES_PASSWORD} and nothing else, and each drew a conclusion from it - that
# --init generates none of the Kafka credentials - which stopped being true here and left three
# diagnostics telling an operator the one command that would fix their problem would not. All
# three now say what --init actually does. Their BEHAVIOUR was always unaffected: they read the
# admin pair from the environment and refuse an empty or still-braced value, and --init fills
# it, so those paths are simply unreachable on a stack initialised from here.
#
# The sed delimiter is "|" because base64 output contains "/" and "+", which a "/" delimiter
# could not carry. It contains no "&", no "\" and no "|" - the characters sed reads specially
# on the replacement side - so a generated value needs no escaping. Keep any future value
# inside that alphabet.
#
# An assignment that ALREADY carries a value is left alone. That is an operator's deliberate
# pin, and overwriting it would replace a credential the broker may already be holding. That
# property is also what makes it safe to run this over an ${env} that is already in service:
# only a placeholder, an empty assignment or an absent key is ever written, so a PostgreSQL
# password the running database is still authenticating with cannot be swapped out from under
# it. See initialize_env.
#
# Each outcome is reported by KEY NAME and never by value, so that the operator can see which
# keys were filled without the credential reaching a terminal, a scrollback buffer or - when
# this runs in CI - a build log.
set_env_value() {
    local key="${1}" value="${2}"

    if grep -qF "{${key}}" "${env}"
    then
        sed -i "s|{${key}}|${value}|g" "${env}"
        printf '%b\n' " ${GRN}${key}${NC} generated into ${env}."
    elif grep -qE "^${key}=[[:space:]]*$" "${env}"
    then
        sed -i "s|^${key}=[[:space:]]*$|${key}=${value}|" "${env}"
        printf '%b\n' " ${GRN}${key}${NC} generated into ${env}."
    elif grep -qE "^${key}=" "${env}"
    then
        printf '%b\n' " ${BLU}${key}${NC} is already set in ${env}; left untouched."
    else
        printf '%s\n' "" "# Added by ${0} --init because ${example} does not declare it." "${key}=${value}" >>"${env}"
        printf '%b\n' " ${GRN}${key}${NC} appended to ${env}, which did not declare it."
    fi
}

# Generate a Kafka SCRAM credential that satisfies the floors every consumer of it enforces.
#
# Both floors are checked rather than assumed, and the draw is repeated until one passes. The
# length is deterministic - 24 bytes always base64-encode to 32 unpadded characters - but the
# DISTINCT-CHARACTER count is not: kafka-bootstrap.sh, kafka-provision.sh and
# event_admin.go all refuse a secret using fewer than 16 distinct characters, and while a
# 32-character draw from base64's 64-character alphabet clears that with overwhelming
# probability, "overwhelming" is not "always". An unlucky draw would produce a .env that every
# one of those three refuses, and the operator would be left reading a weak-credential error
# about a credential they never chose.
#
# The alphabet is filtered to the characters those grammars can carry, so a secret containing
# "+", "/" or "=" is re-drawn rather than escaped - there is no escape sequence available in
# either grammar. Filtering can shorten the result, which is why the length is re-checked
# after it rather than trusted from the byte count.
generate_kafka_secret() {
    local secret="" attempt=0 distinct=0

    while [ "${attempt}" -lt 100 ]
    do
        attempt=$((attempt + 1))
        secret="$(openssl rand -base64 $((kafka_secret_bytes * 2)) | LC_ALL=C tr -dc 'A-Za-z0-9' | head -c 32 || true)"

        if [ "${#secret}" -lt 32 ]
        then
            continue
        fi

        distinct="$(printf '%s' "${secret}" | fold -w1 | sort -u | wc -l | tr -d ' ')"
        if [ "${distinct}" -ge 16 ]
        then
            printf '%s' "${secret}"
            return 0
        fi
    done

    printf "Could not generate a Kafka credential meeting the 32-character and 16-distinct-character floors after %s attempts.\n" "${attempt}" >&2
    printf "This means 'openssl rand' is not producing usable entropy on this host. Nothing has been written.\n" >&2
    exit 1
}

# Refuse to leave ${env} readable by anyone but its owner.
#
# It holds every secret --init generates - the Postgres password, and the Kafka administrative
# password whose principal sits in the broker's super.users. Enforced rather than merely set,
# because the file may predate this check: a stack initialised by an earlier version of --init
# has a world-readable .env today, and saying nothing would leave it that way for ever.
#
# Repaired rather than refused. The remedy is one chmod, the operator would only run it
# themselves, and failing the command over something this script can fix would be pedantry -
# but it is ANNOUNCED, because a permission that was wrong once may be wrong again through
# whatever set it.
require_private_env_file() {
    local mode=""

    if [ ! -e "${env}" ]
    then
        return 0
    fi

    # stat's spelling differs between GNU and BSD; both are tried and an unknown one is
    # skipped rather than guessed at, since a wrong format string would report a mode that is
    # not the file's.
    mode="$(stat -c '%a' "${env}" 2>/dev/null || stat -f '%Lp' "${env}" 2>/dev/null || true)"
    if [ -z "${mode}" ]
    then
        return 0
    fi

    case "${mode}" in
        600|400 )
            return 0
            ;;
    esac

    printf '%b\n' " ${YEL}${env}${NC} was mode ${RED}${mode}${NC}, which lets other accounts on this host read every" \
        " secret in it - including ${BLU}KAFKA_SASL_ADMIN_SECRET${NC}, whose principal is in the broker's" \
        " super.users. Tightening it to ${GRN}600${NC}."
    chmod 600 "${env}" || printf '%b\n' " ${RED}Could not change the mode of ${env}. Fix it by hand: chmod 600 ${env}${NC}"
}

# Reports any {PLACEHOLDER} that survived into ${env}. A surviving one IS a literal password:
# nothing downstream replaces it, so the service it belongs to authenticates with the brace
# text itself and fails in a way that names neither this file nor that one. It means
# ${example} has grown a placeholder --init does not know about, so the name is printed and
# the remedy is to teach --init about it.
warn_unsubstituted_placeholders() {
    local leftovers=""

    leftovers="$(grep -oE '\{[A-Z][A-Z0-9_]*\}' "${env}" | sort -u | tr '\n' ' ' || true)"
    if [ -n "${leftovers}" ]
    then
        printf '%b\n' " ${YEL}Unsubstituted placeholders remain in ${env}${NC}: ${leftovers}"
        printf '%b\n' " Set each one by hand before spinning up the stack, or teach --init in ${0} to generate it."
    fi
}

# Force ${env} to owner-only permissions, repairing an insecure file rather than refusing it.
#
# WHY THIS IS NOT A CHECK-AND-ABORT. An abort would leave the insecure file exactly as it
# found it and block the operator from doing anything about it with this script, so the
# credential stays exposed and the only route forward is a chmod the message has to teach.
# Repairing closes the exposure immediately and says so. The one thing it must never do is
# stay silent: a permission that was wrong yesterday means the secret has already been
# readable, so the operator needs to know in order to decide whether to rotate.
#
# Ownership is deliberately NOT changed. chown requires privilege this script does not ask
# for, and a file owned by another user is a situation an operator must resolve knowingly -
# so it is reported and the mode is still tightened, which is the part that can be done.
enforce_env_permissions() {
    local mode="" owner=""

    if [ ! -e "${env}" ]
    then
        return 0
    fi

    mode="$(stat -c '%a' "${env}" 2>/dev/null || true)"

    # A mode this script cannot read is a mode it cannot verify, so it is tightened anyway:
    # an unverifiable permission must not be treated as an acceptable one.
    if [ -z "${mode}" ] || [ "${mode}" != "${env_file_mode}" ]
    then
        if chmod "${env_file_mode}" "${env}" 2>/dev/null
        then
            if [ -n "${mode}" ] && [ "${mode}" != "${env_file_mode}" ]
            then
                printf '%b\n' " ${YEL}Tightened permissions on ${env}${NC}: ${mode} -> ${env_file_mode} (owner read/write only)."
                printf '%b\n' " It carries the PostgreSQL password and the Kafka superuser and producer secrets, and it was"
                printf '%b\n' " readable beyond its owner. If this host is shared, treat those credentials as disclosed:"
                printf '%b\n' " re-run ${BLU}${0} --purge${NC} and ${BLU}${0} --init${NC} after deleting ${env} to mint new ones."
            fi
        else
            printf '%b\n' " ${RED}Could not set ${env} to mode ${env_file_mode}${NC}; it may be owned by another user."
            owner="$(stat -c '%U' "${env}" 2>/dev/null || true)"
            if [ -n "${owner}" ]
            then
                printf '%b\n' " Owner: ${BLU}${owner}${NC}. Fix by hand: ${BLU}chmod ${env_file_mode} ${env}${NC}"
            fi
        fi
    fi
}

# Generate a per-stack secret in the alphabet every consumer of ${env} can carry.
#
# Report whether a key in ${env} still needs a value: absent, empty, or still a placeholder.
#
# It is what lets the ensure step below be quiet on a stack that is already configured while
# still filling a gap. set_env_value alone would print "already carries a value" for every key
# on every bring-up, which trains an operator to ignore this script's output.
env_value_missing() {
    local key="${1}" line=""

    if [ ! -r "${env}" ]
    then
        return 0
    fi

    line="$(grep -E "^${key}=" "${env}" | tail -n 1 || true)"
    if [ -z "${line}" ]
    then
        return 0
    fi

    case "${line#*=}" in
        "" | "{"*"}" ) return 0 ;;
        * ) return 1 ;;
    esac
}

# The value a key currently carries in ${env}, or nothing when the key is absent, empty or
# still a brace placeholder.
#
# It exists so that a credential can be written under a SECOND name with the value the first
# already holds, rather than with a fresh draw. The two producer names are resolved in a fixed
# order, so two independent generations would have the application present one credential
# while the broker was minted with the other.
#
# The LAST matching assignment wins, matching env_value_missing and matching how a shell
# sourcing the file would read it: a key appended by an earlier --init run sits below the
# template's own line for the same key.
env_value_from() {
    local key="${1}" line=""

    if [ ! -r "${env}" ]
    then
        return 0
    fi

    line="$(grep -E "^${key}=" "${env}" | tail -n 1 || true)"
    if [ -z "${line}" ]
    then
        return 0
    fi

    case "${line#*=}" in
        "{"*"}" ) return 0 ;;
        * ) printf '%s' "${line#*=}" ;;
    esac
}

# Make sure ${env} carries every credential the compose stack REQUIRES, generating what is
# missing, and leaving every existing value alone.
#
# WHY THIS RUNS ON EVERY BRING-UP AND NOT ONLY ON --init. The kafka and kafka-init services
# declare KAFKA_SASL_ADMIN_SECRET with compose's required-variable form, because a broker
# superuser password with a known default committed to this repository handed the cluster's
# whole access model to anyone who could reach the published port. Requiring it means a .env
# written before this change - or one an operator hand-copied from ${example}, which ships the
# key empty - now fails interpolation before a single container starts. Filling the gap here
# is what keeps the sanctioned path ("${0} --up") working while leaving the insecure default
# genuinely unavailable: an operator running "docker compose up" directly still has to supply
# one, and the refusal message tells them how.
#
# An existing value is NEVER overwritten. The broker's KRaft metadata log was formatted with
# the admin password and holds the producer's SCRAM credential, so replacing either here would
# lock the stack out of its own broker and the only copy of the old value would be gone.
ensure_env_secrets() {
    local generated=""

    if [ ! -r "${env}" ]
    then
        return 0
    fi

    enforce_env_permissions

    if env_value_missing "KAFKA_SASL_ADMIN_USER"
    then
        set_env_value "KAFKA_SASL_ADMIN_USER" "${kafka_admin_principal}" >/dev/null
        generated="${generated} KAFKA_SASL_ADMIN_USER"
    fi

    if env_value_missing "KAFKA_SASL_ADMIN_SECRET"
    then
        set_env_value "KAFKA_SASL_ADMIN_SECRET" "$(generate_kafka_secret)" >/dev/null
        generated="${generated} KAFKA_SASL_ADMIN_SECRET"
    fi

    if env_value_missing "KAFKA_PRODUCER_USER"
    then
        set_env_value "KAFKA_PRODUCER_USER" "${kafka_producer_principal}" >/dev/null
        generated="${generated} KAFKA_PRODUCER_USER"
    fi

    if env_value_missing "KAFKA_PRODUCER_SECRET"
    then
        set_env_value "KAFKA_PRODUCER_SECRET" "$(generate_kafka_secret)" >/dev/null
        generated="${generated} KAFKA_PRODUCER_SECRET"
    fi

    # THE SAME IDENTITY UNDER ITS OTHER PAIR OF NAMES, and it must be the same VALUE rather
    # than a fresh draw: KAFKA_SASL_* is resolved BEFORE KAFKA_PRODUCER_* everywhere, so two
    # independent generations would have the application present one credential while the
    # broker was minted with the other. env_value_from reads what was just written, or what
    # was already there, which is what keeps the two pairs equal in both cases.
    if env_value_missing "KAFKA_SASL_USER"
    then
        set_env_value "KAFKA_SASL_USER" "$(env_value_from "KAFKA_PRODUCER_USER")" >/dev/null
        generated="${generated} KAFKA_SASL_USER"
    fi

    if env_value_missing "KAFKA_SASL_SECRET"
    then
        set_env_value "KAFKA_SASL_SECRET" "$(env_value_from "KAFKA_PRODUCER_SECRET")" >/dev/null
        generated="${generated} KAFKA_SASL_SECRET"
    fi

    # Re-asserted after writing: set_env_value edits in place with sed -i, and a future
    # rewrite-and-rename would not necessarily carry the mode across.
    enforce_env_permissions

    if [ -n "${generated}" ]
    then
        printf '%b\n' " ${YEL}Filled missing Kafka credentials in ${env}${NC}:${generated}"
        printf '%b\n' " Each secret is generated per stack, written only into ${env} (git-ignored) and never printed."
        printf '%b\n' " If the broker volume predates them, its metadata log holds the OLD credentials: run"
        printf '%b\n' " ${BLU}${0} --purge${NC} to discard that volume so the broker is re-bootstrapped with these."
    fi
}

# =======================================================================================
# The Kafka compose profile
#
# Both compose files put the broker and its provisioning one-shot behind
# `profiles: ["kafka"]`, so a plain bring-up starts the ledger with NO broker. That is the
# state .env.example ships: KAFKA_BROKERS empty, the event publisher resolved to its no-op,
# and Blnk serving and processing transactions exactly as it did before Kafka existed. It is
# a supported steady state and a documented validation gate, not a degraded one, and it is
# also what keeps a developer who has no interest in event streaming from paying for a JVM.
#
# Starting the broker is therefore a decision - and the decision has already been made
# somewhere. KAFKA_BROKERS is what makes Blnk publish, so it is what brings the broker up:
# nobody has to learn a second switch, and the two cannot disagree.
#
# COMPOSE_PROFILES rather than a --profile flag, for two reasons. It cannot be misplaced
# relative to a subcommand the way a global flag can, and it reaches the compose invocations
# inside compose_container_id, wait_for_kafka_init and provision_kafka without each of them
# having to thread a flag through.
# =======================================================================================

# The value COMPOSE_PROFILES should carry for a command that must include the Kafka services,
# preserving whatever profiles are already selected.
#
# Merged rather than assigned, because .env may legitimately set COMPOSE_PROFILES=monitoring:
# overwriting it would silently drop Prometheus from the very bring-up an operator asked for.
compose_profiles_with_kafka() {
    local existing="${COMPOSE_PROFILES:-}"

    case ",${existing}," in
        *,kafka,*)
            printf '%s' "${existing}"
            ;;
        *)
            if [ -n "${existing}" ]
            then
                printf '%s,kafka' "${existing}"
            else
                printf 'kafka'
            fi
            ;;
    esac
}

# The profiles a START-UP command runs with: Kafka only when brokers are configured.
startup_compose_profiles() {
    if [ -n "$(effective_kafka_brokers)" ]
    then
        compose_profiles_with_kafka
    else
        printf '%s' "${COMPOSE_PROFILES:-}"
    fi
}

# The profiles a TEARDOWN command runs with: Kafka ALWAYS, whatever the brokers say.
#
# Teardown is not symmetrical with bring-up and must not be. A broker container can have been
# started by an earlier run whose .env did configure brokers, or by "docker compose --profile
# kafka up" by hand; a "down" that omitted the profile would leave that container running and
# its kafka_data volume held while reporting success - verified behaviour, not caution: compose
# excludes a profiled service from "down" unless its profile is active. Including the profile
# when there is nothing to remove costs nothing at all.
teardown_compose_profiles() {
    compose_profiles_with_kafka
}

# =======================================================================================
# Local Kafka stack
#
# The event-publishing pipeline needs a broker that is not merely created but LISTENING and
# answering AUTHENTICATED requests, and a topic catalogue that already exists - the broker is
# configured with auto-creation off, so a topic nobody provisioned is a topic the relay
# cannot publish to.
#
# Compose does both jobs: the kafka service healthcheck completes a full SASL/SCRAM handshake
# and an authorized metadata request, and the kafka-init one-shot provisions the catalogue
# behind that gate. What follows therefore WAITS FOR AND VERIFIES that work rather than
# duplicating it, and runs scripts/kafka-provision.sh directly only when the one-shot did not
# run or did not succeed.
#
# WHAT COMPOSE NO LONGER DOES is gate the application services on any of it. server and worker
# used to carry "kafka: condition: service_healthy", which made the API wait on a broker the
# shipped configuration tells it to ignore - and gated it on the wrong property anyway, since a
# healthy broker says a handshake works and says nothing about whether the topics exist. The
# ORDERING now lives here instead, in stage_kafka, which starts the broker and provisions it
# before the ledger is launched at all.
# =======================================================================================

# Kafka status lines, with any continuation arguments indented beneath the first.
#
# %b rather than the variable-bearing format string used elsewhere in this file: the colours
# are stored as literal escape sequences, %b is what expands them, and the rendered output is
# identical while no caller's text is ever read as a format.
kafka_message() {
    local colour="${1}" line

    printf '%b\n' " ${colour}kafka${NC} : ${2}"
    for line in "${@:3}"
    do
        printf '%b\n' "         ${line}"
    done
}

kafka_note() { kafka_message "${BLU}" "$@"; }
kafka_ok() { kafka_message "${GRN}" "$@"; }
kafka_warn() { kafka_message "${YEL}" "$@"; }
kafka_error() { kafka_message "${RED}" "$@"; }

# The broker list this stack is actually configured with, resolved exactly as Compose
# resolves it: the shell environment first, ${env} second.
#
# THERE IS NO THIRD STEP, because the Compose files declare ${KAFKA_BROKERS:-} — an EMPTY
# default. Publishing is opt-in in two halves (the "kafka" profile so a broker exists, and a
# non-empty KAFKA_BROKERS so Blnk speaks to it), so an absent name means "no brokers" both
# here and in the containers. A local default reproduced in this file would have to be kept
# in step with two Compose projections by hand, and a divergence would make this script
# verify a broker the application is not publishing to — or skip verifying one it is.
#
# AN EMPTY LIST REMAINS A LEGITIMATE STEADY STATE, NOT AN ERROR. Setting KAFKA_BROKERS= to an
# explicit empty value — in the shell or in ${env} — selects it, the event publisher resolves
# to its no-op implementation, and Blnk starts, serves and processes transactions exactly as
# it did before Kafka existed. Every check below is then skipped, never failed.
effective_kafka_brokers() {
    # DECLARED, not non-empty. A shell KAFKA_BROKERS= is a deliberate "no brokers for this run"
    # and compose honours it over --env-file, so this must too; testing the value instead would
    # silently fall back to ${env} and verify a broker the containers are not using.
    if [ -n "${KAFKA_BROKERS_DECLARED_IN_SHELL}" ]
    then
        printf '%s' "${KAFKA_BROKERS_FROM_SHELL}"
    else
        # showenv() has sourced ${env} by now, so an unset shell name reads the file's value
        # here, exactly as --env-file gives it to the containers.
        printf '%s' "${KAFKA_BROKERS-}"
    fi
}

# The container id backing a compose service, or nothing when that service has no container -
# because $COMPOSE_FILE does not declare it, or because it was never started.
#
# Both forms of "ps" are tried. "-aq" includes a one-shot that has already EXITED, which is
# precisely the state a successful kafka-init is in, but it is not accepted by every release
# the COMPOSE_CL override can point at, so "-q" is the fallback. Failures are swallowed on
# purpose: "does this service have a container" is the question being asked, and errexit must
# not turn the answer "no" into an aborted bring-up.
compose_container_id() {
    local service="${1}" id=""

    id="$(${COMPOSE_CL} --env-file "${env}" -f "${COMPOSE_FILE}" ps -aq "${service}" 2>/dev/null | head -n 1 || true)"
    if [ -z "${id}" ]
    then
        id="$(${COMPOSE_CL} --env-file "${env}" -f "${COMPOSE_FILE}" ps -q "${service}" 2>/dev/null | head -n 1 || true)"
    fi

    printf '%s' "${id}"
}

# One templated field of a container's state, or nothing when the container is gone or the
# field is unset. docker is called directly, as the --pull branch already does, so no new
# dependency is introduced; --format keeps the answer to one word instead of parsing JSON.
container_field() {
    docker inspect --format "${2}" "${1}" 2>/dev/null || true
}

# Waits for the broker to report HEALTHY rather than merely to exist, because a listening
# socket says nothing about whether SASL is usable and everything downstream assumes it is.
#
# 0 healthy, 1 the broker's own probe says unhealthy, 2 still starting at the deadline,
# 3 the container is not running at all. 1 and 2 share a diagnosis - a broker that is up and
# refusing the handshake - while 3 is a broker that stopped, and the caller says so
# differently because the two are read in different places.
#
# A container with no healthcheck - possible with a custom $COMPOSE_FILE - is accepted as
# soon as it is running, there being nothing better to wait for.
wait_for_kafka_healthy() {
    local id="${1}" health="" state="" deadline="" announced="no"
    local timeout="${KAFKA_WAIT_TIMEOUT_SECONDS:-180}"
    local interval="${KAFKA_WAIT_POLL_INTERVAL_SECONDS:-5}"

    # A non-numeric override would otherwise evaluate to zero in the arithmetic below and
    # produce an instant timeout that looks like a broken broker.
    [[ "${timeout}" =~ ^[0-9]+$ ]] || timeout=180
    [[ "${interval}" =~ ^[1-9][0-9]*$ ]] || interval=5

    deadline=$(($(date +%s) + timeout))

    while :
    do
        health="$(container_field "${id}" '{{if .State.Health}}{{.State.Health.Status}}{{end}}')"
        state="$(container_field "${id}" '{{.State.Status}}')"

        case "${health}" in
            healthy)
                return 0
                ;;
            unhealthy)
                return 1
                ;;
        esac

        case "${state}" in
            exited | dead)
                return 3
                ;;
            running)
                if [ -z "${health}" ]
                then
                    return 0
                fi
                ;;
        esac

        if [ "$(date +%s)" -ge "${deadline}" ]
        then
            return 2
        fi

        # Announced once, and only when there is actually a wait: the staged bring-up names
        # kafka-init alongside the broker and compose blocks on ITS "service_healthy" condition
        # before starting it, so the common case is a single conclusive probe here and silence is
        # the right output for it. (The application services carry no such condition - see the
        # section header - so on a bring-up that skipped staging this really can wait.)
        if [ "${announced}" = "no" ]
        then
            kafka_note "waiting up to ${timeout}s for the broker to complete its first SASL handshake..."
            announced="yes"
        fi

        sleep "${interval}"
    done
}

# Verifies that the one-shot provisioning service RAN AND SUCCEEDED, waiting for it while it
# is still going. Compose starts it behind the broker's healthcheck, but "up -d" does not wait
# for a one-shot to finish, so its outcome can only be read afterwards - here.
#
# 0 it exited 0, 1 it exited non-zero or is still running at the deadline, 2 the service has
# no container at all. The caller provisions directly in the last two cases, which is the
# whole reason 2 is distinguished rather than folded into 1.
wait_for_kafka_init() {
    local service="${1}" id="" state="" code="" deadline=""
    local timeout="${KAFKA_WAIT_TIMEOUT_SECONDS:-180}"
    local interval="${KAFKA_WAIT_POLL_INTERVAL_SECONDS:-5}"

    [[ "${timeout}" =~ ^[0-9]+$ ]] || timeout=180
    [[ "${interval}" =~ ^[1-9][0-9]*$ ]] || interval=5

    id="$(compose_container_id "${service}")"
    if [ -z "${id}" ]
    then
        return 2
    fi

    deadline=$(($(date +%s) + timeout))

    while :
    do
        state="$(container_field "${id}" '{{.State.Status}}')"

        case "${state}" in
            exited)
                code="$(container_field "${id}" '{{.State.ExitCode}}')"
                if [ "${code}" = "0" ]
                then
                    return 0
                fi
                return 1
                ;;
            dead | "")
                return 1
                ;;
        esac

        if [ "$(date +%s)" -ge "${deadline}" ]
        then
            return 1
        fi

        sleep "${interval}"
    done
}

# Provisions the topic catalogue and the sample subscriber principal directly. A FALLBACK,
# never the primary path: the catalogue is created by exactly one script whether compose runs
# it inside the broker image or this runs it on the host, so the work is delegated rather than
# restated - a second implementation is a second thing to drift.
#
# That script takes no arguments and reads everything from the environment, so the hand-off
# below is the entire interface, and kafka_provision_passthrough at the top of this file is the
# whole of it. Values are ${env}'s, because showenv sourced it before main ran. On a host the
# Kafka CLI is normally absent; the script handles that itself by re-executing inside the broker
# container over docker, and KAFKA_CONTAINER and KAFKA_PROVISION_CONTAINER_SCRIPT - both in the
# allowlist - are what tell it where to go.
#
# WHY THE FULL LIST MATTERS, with one variable as the illustration.
# KAFKA_ALLOW_PARTITION_GROWTH is CONSENT to an operation that re-maps keys already written: add
# partitions to a topic that holds records and one ledger's events split across partitions,
# after which a consumer can observe them out of order with nothing failing to say so. Compose
# passes it to kafka-init and the Go admin client reads it from configuration. A fallback that
# dropped it would have the same ${env} refuse a growth through one path and answer differently
# through the other, decided by nothing more than whether this host has the Kafka CLI. Every
# other name on that list has its own version of the same argument, which is why the list is
# complete rather than curated.
provision_kafka() {
    local brokers="${1}" service="${2}"

    if [ ! -x "${kafka_provision_script}" ]
    then
        kafka_warn "${kafka_provision_script} is not executable from here, so provisioning was skipped." \
            "Run ${0} from the repository root, or restore the file's execute bit."
        return 1
    fi

    # Built from the allowlist, and ONLY for names that are actually declared. Forwarding an
    # undeclared name as an empty assignment is not neutral: the script defaults each of these
    # with the ${VAR:-default} form, and an empty value satisfies that form, so passing
    # KAFKA_SAMPLE_SUBSCRIBER_USER= would replace "blnk-sample-subscriber" with nothing and the
    # run would fail on an empty principal it was never given. ${!name+declared} tests
    # declaration rather than content, so an operator's deliberate empty value still crosses.
    local assignments=() name
    for name in "${kafka_provision_passthrough[@]}"
    do
        if [ -n "${!name+declared}" ]
        then
            assignments+=("${name}=${!name}")
        fi
    done

    # THE PRODUCER PAIR IS NOT REWRITTEN HERE, and it used to be: two assignments mapped
    # KAFKA_SASL_* onto KAFKA_PRODUCER_* unconditionally, AFTER the allowlist, so they won.
    # ${example} documents setting only the KAFKA_PRODUCER_* pair as the normal compose route,
    # and for such a .env those assignments forwarded two EMPTY values - erasing the
    # operator's producer credential on this path alone and failing the run on a secret it was
    # never given. Both pairs now cross verbatim through the allowlist above, and the script
    # applies the same KAFKA_SASL_* first, KAFKA_PRODUCER_* second precedence the compose
    # services and config.KafkaConfig apply, so all three agree by construction.
    #
    # "env" rather than exporting, so nothing here leaks a credential into this shell's
    # environment for every later command in the bring-up to inherit. The two computed values
    # come last so they win over any ${env} entry of the same name that reached the list: the
    # broker list is the EFFECTIVE one, which honours a shell-level override, and the service is
    # the one the caller asked about.
    env "${assignments[@]}" \
        KAFKA_BROKERS="${brokers}" \
        KAFKA_COMPOSE_SERVICE="${service}" \
        "${kafka_provision_script}"
}

# Confirms that events have somewhere to go: a broker that authenticates and a catalogue that
# exists.
#
# THE VERDICT IS THE RETURN VALUE, and it turns on whether this stack owns the broker.
#
#   0 - there was nothing to verify (no brokers configured, or the broker is not one of this
#       compose project's services), or everything verified: the broker authenticates and the
#       catalogue exists.
#   1 - Kafka is configured AND local, and its state could not be verified: the broker never
#       became healthy, its container is not running, or provisioning did not complete.
#
# WHY A LOCAL FAILURE IS FATAL AND AN EXTERNAL ONE IS NOT. This used to return 0 on every path
# and every call site added "|| true" on top, so "./stack.sh -u" printed the diagnosis in red
# and then exited 0. A bring-up that reports success without the topics and ACLs the relay
# needs is not a bring-up: the next thing to happen is a publish failure nobody connects to
# this run, and in CI the step that was supposed to prove the stack works goes green. The
# outbox argument - that events wait durably until a broker accepts them - is about DURABILITY,
# and it is still true; it was never an argument for reporting success.
#
# The distinction is ownership, not severity. When ${COMPOSE_FILE} declares the broker service
# this script started it and is the only thing that can be held responsible for its state. When
# it does not, the broker belongs to somebody else - a managed cluster, a shared development
# broker - and this script can neither fix nor be blamed for it, so it says what it observed
# and stays out of the way. An empty broker list is the same case in the other direction: the
# publisher resolves to its no-op implementation on purpose, and there is nothing to verify.
#
# NOTHING IS TORN DOWN on a failure. "up -d" has already completed and the database, the queue,
# the search index and the API are serving; what changes is the exit status, so that a human
# sees a red summary and a script chained with "&&" stops instead of continuing against a
# broker that cannot take an event.
ensure_kafka() {
    local brokers="" service="" init_service="" id="" health=0 init=0

    brokers="$(effective_kafka_brokers)"
    service="${KAFKA_COMPOSE_SERVICE:-kafka}"
    init_service="${KAFKA_INIT_COMPOSE_SERVICE:-kafka-init}"

    if [ -z "${brokers}" ]
    then
        kafka_note "KAFKA_BROKERS is empty, so event publishing is off and there is nothing to verify." \
            "That is a supported steady state and not a problem: the publisher resolves to its no-op" \
            "implementation and Blnk runs exactly as it did before Kafka existed." \
            "The broker does NOT come up either: kafka and kafka-init sit behind the ${BLU}kafka${NC} Compose" \
            "profile, so nothing Kafka-shaped is started or provisioned unless you ask for it." \
            "Publishing events therefore takes BOTH halves in ${env}: ${BLU}COMPOSE_PROFILES=kafka${NC} so there is a" \
            "broker, and ${BLU}KAFKA_BROKERS=kafka:9092${NC} so Blnk publishes to it - plus a webhook deprecation" \
            "date, which Blnk requires whenever brokers are configured."
        return 0
    fi

    id="$(compose_container_id "${service}")"
    if [ -z "${id}" ]
    then
        kafka_note "no container for the \"${service}\" service, so the broker checks were skipped." \
            "THE LIKELIEST CAUSE, now that Kafka is opt-in: ${BLU}KAFKA_BROKERS${NC} is set to ${brokers} but the" \
            "${BLU}kafka${NC} Compose profile is not selected, so no broker was started - and the relay will retry" \
            "against nothing. Set ${BLU}COMPOSE_PROFILES=kafka${NC} in ${env}, or pass ${BLU}--profile kafka${NC}, then bring the" \
            "stack up again." \
            "Also expected when ${COMPOSE_FILE} does not declare the service, when only some services" \
            "were named, or when the broker is external - in which case assure its topics yourself."
        return 0
    fi

    wait_for_kafka_healthy "${id}" || health=$?
    case "${health}" in
        0)
            kafka_ok "broker healthy - SASL/SCRAM handshake completed and an authorized request answered."
            ;;
        1 | 2)
            kafka_error "the \"${service}\" service never became healthy, so the topic catalogue is unverified." \
                "The rest of the stack is up and stays up; this bring-up is reported as FAILED because Blnk is" \
                "configured for ${brokers} and cannot publish an event until that broker authenticates." \
                "Most likely ${env} now carries a different ${BLU}KAFKA_SASL_ADMIN_SECRET${NC} than the one the" \
                "kafka_data volume was formatted with. Bootstrapping is one-shot and deliberately skips an" \
                "already-formatted volume, so the seeded credential is never updated and every handshake fails." \
                "Fix: discard that volume and let it re-bootstrap. ${BLU}${0} --purge${NC} removes EVERY data volume," \
                "the ledger's included, so remove only this project's kafka_data volume to keep the rest." \
                "Otherwise read the broker's own account: ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} logs ${service}"
            return 1
            ;;
        *)
            kafka_error "the \"${service}\" container is not running, so the topic catalogue is unverified." \
                "The rest of the stack is up and stays up; this bring-up is reported as FAILED because Blnk is" \
                "configured for ${brokers} and that broker is this project's own service." \
                "Read why it stopped with:" \
                "${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} logs ${service}"
            return 1
            ;;
    esac

    wait_for_kafka_init "${init_service}" || init=$?
    case "${init}" in
        0)
            kafka_ok "topics and the sample subscriber principal are provisioned - \"${init_service}\" completed."
            return 0
            ;;
        2)
            kafka_warn "no \"${init_service}\" one-shot ran, so provisioning is being done from here instead."
            ;;
        *)
            kafka_warn "\"${init_service}\" did not complete, so provisioning is being retried from here." \
                "Its own output explains why: ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} logs ${init_service}"
            ;;
    esac

    if provision_kafka "${brokers}" "${service}"
    then
        kafka_ok "provisioned by ${kafka_provision_script}."
        return 0
    fi

    kafka_error "provisioning did not complete, so the topic catalogue and the ACLs are unverified." \
        "The rest of the stack is up and stays up; this bring-up is reported as FAILED because the broker is" \
        "this project's own service and auto-creation is off, so a topic nobody provisioned is a topic the" \
        "relay cannot publish to. Its output above says why." \
        "Re-run it on its own once the cause is fixed: ${BLU}${kafka_provision_script}${NC}"

    return 1
}

# Starts the broker and its provisioning one-shot, then waits until the catalogue exists.
#
# ONLY those two services are started here. The application services follow in the caller, so
# the relay's first poll happens against a broker whose topics are already there.
#
# Whether the staged broker bring-up applies to this invocation.
#
# THREE SITUATIONS ARE NOT PROBLEMS and must not be reported as any. Each returns 1, and a 1
# here means "nothing to stage", never "something is wrong":
#
#   1. The caller NAMED SERVICES. "./stack.sh --up postgres redis" asks for two services, and
#      staging a broker into that would start a container the operator did not ask for. Flags
#      are ignored in that judgement, because "--build" is not a service name.
#   2. THE BROKER LIST IS EMPTY. Event publishing is off, which is a supported steady state;
#      ensure_kafka reports on it in full. Staging a broker for a stack configured to ignore
#      it would start a JVM nobody asked for and gate the API on it - exactly what the Compose
#      files no longer do.
#   3. ${COMPOSE_FILE} DECLARES NO BROKER. A custom projection, or an external managed broker,
#      has nothing to stage. ensure_kafka says so afterwards.
#
# The third check runs "config --services" WITH the Kafka profile selected, because the broker
# and its one-shot sit behind it: without the profile the service is absent from the listing
# and a perfectly ordinary projection would look like case 3.
#
# Parameters: the arguments the caller is forwarding to "compose up".
kafka_staging_applicable() {
    local arg

    for arg in "$@"
    do
        case "${arg}" in
            -*)
                continue
                ;;
            *)
                kafka_note "specific services were named, so the broker was not staged ahead of them." \
                    "Run ${BLU}${0} --up${NC} with no service names for the ordered bring-up."

                return 1
                ;;
        esac
    done

    if [ -z "$(effective_kafka_brokers)" ]
    then
        return 1
    fi

    if ! COMPOSE_PROFILES="$(compose_profiles_with_kafka)" \
        ${COMPOSE_CL} --env-file "${env}" -f "${COMPOSE_FILE}" config --services 2>/dev/null |
        grep -qx "${KAFKA_COMPOSE_SERVICE:-kafka}"
    then
        return 1
    fi

    return 0
}

# 0 when the broker is ready, or when the operator has opted out through KAFKA_REQUIRE_READY.
# 1 when Kafka is configured and could not be made ready, which fails the bring-up before the
# ledger is started - deliberately, because a bring-up that reports success while the event
# pipeline cannot deliver is the failure mode this whole section exists to remove.
stage_kafka() {
    local service init_service

    service="${KAFKA_COMPOSE_SERVICE:-kafka}"
    init_service="${KAFKA_INIT_COMPOSE_SERVICE:-kafka-init}"

    kafka_note "starting the broker and provisioning its topics before the application services..."

    # Both named together so that compose applies kafka-init's own "service_healthy" condition:
    # it waits for the broker's SASL handshake to succeed before the one-shot is started. "up"
    # does not wait for a one-shot to FINISH, which is what ensure_kafka does below.
    # THE PROFILE IS SELECTED HERE, and it has to be: both services sit behind
    # profiles: ["kafka"], so naming them without it starts nothing at all and compose says
    # so only in passing. kafka_staging_applicable has already established that brokers are
    # configured, which is what makes selecting it correct rather than presumptuous.
    if ! COMPOSE_PROFILES="$(compose_profiles_with_kafka)" \
        ${COMPOSE_CL} --env-file "${env}" -f "${COMPOSE_FILE}" up -d "${service}" "${init_service}"
    then
        kafka_error "the broker services could not be started, so nothing was provisioned." \
            "Read compose's own output above."

        if is_truthy "${KAFKA_REQUIRE_READY:-1}"
        then
            return 1
        fi

        return 0
    fi

    if ensure_kafka
    then
        return 0
    fi

    if is_truthy "${KAFKA_REQUIRE_READY:-1}"
    then
        kafka_error "the bring-up stops here rather than starting the ledger against a broker it cannot publish to." \
            "Nothing has been lost and nothing is half-started: only the broker services were" \
            "launched, and no event has been captured yet." \
            "Fix the broker and re-run, or set ${BLU}KAFKA_REQUIRE_READY=0${NC} to bring the stack up" \
            "anyway - events then accumulate in the transactional outbox and the relay retries" \
            "topic assurance on its own until the catalogue appears."

        return 1
    fi

    kafka_warn "KAFKA_REQUIRE_READY is off, so the bring-up continues with an unverified broker." \
        "Events accumulate in the transactional outbox and the relay keeps retrying topic assurance."

    return 0
}

# "compose up -d" with the Kafka readiness gate in front of it.
#
# Parameters: forwarded verbatim to "up", so a caller may pass --build or a list of services.
staged_up() {
    local staged="no"

    # Every bring-up path routes through here, so this is where ${env} is completed. The
    # broker cannot be bootstrapped without an administrative secret and the publisher cannot
    # authenticate without a producer pair, and both are generated per stack rather than
    # shipped, so a .env written before either existed is repaired before anything starts.
    ensure_env_secrets

    if kafka_staging_applicable "$@"
    then
        stage_kafka || return 1
        staged="yes"
    fi

    # The start-up profile set, so a stack with brokers configured brings its broker up and a
    # stack without one does not. Passing nothing here would leave the broker unstarted for a
    # deployment that is publishing to it, and the relay retrying against nothing.
    COMPOSE_PROFILES="$(startup_compose_profiles)" \
        ${COMPOSE_CL} --env-file "${env}" -f "${COMPOSE_FILE}" up -d "$@" || return 1

    if [ "${staged}" = "no" ]
    then
        # Nothing was verified ahead of the application, so report on the broker now. Advisory
        # by construction: the stack is already running, and failing here would change the exit
        # code and nothing else.
        ensure_kafka || true
    fi
}

# =======================================================================================
# Teardown that deletes data
# =======================================================================================

# Whether an explicit consent flag was given. Accepted anywhere among the arguments so that
# both "--purge --yes" and "--purge -y" work, and nothing is forwarded to compose: "down
# --volumes" takes what it needs from $COMPOSE_FILE, and a consent flag reaching it would
# only make it fail.
purge_consent_given() {
    local argument

    for argument in "$@"
    do
        case "${argument}" in
            -y | --yes | --force)
                return 0
                ;;
        esac
    done

    return 1
}

# Shuts the stack down AND DELETES its data volumes.
#
# Separate from --down, and deliberately so. --down keeps every volume, which is what makes
# it safe to run a hundred times a day, and adding --volumes to it would have silently
# destroyed developers' ledger data. The two are different operations and this is the
# destructive one, so it is spelled in full with no single-letter alias - a typo cannot reach
# it - and it does nothing until consent is explicit.
#
# It exists because bootstrapping a KRaft broker is one-shot: the SCRAM credential is seeded
# while storage is formatted, and an already-formatted volume is skipped on every later start.
# So once ${env} carries a new KAFKA_SASL_ADMIN_SECRET, discarding kafka_data is the only way
# the broker can be re-seeded with it. Removing just that volume is the surgical alternative
# and is named in the message below.
purge() {
    local answer=""

    printf '%b\n' " ${RED}This deletes data.${NC} \"down --volumes\" removes every named volume this compose"
    printf '%b\n' " project owns: the ledger's PostgreSQL data in pg_data, the search index in"
    printf '%b\n' " typesense_data, and the broker's KRaft metadata log in kafka_data - the bootstrapped"
    printf '%b\n' " SCRAM credentials, every ACL and every message with it. None of it is recoverable."
    printf '%b\n' " Use ${BLU}${0} --down${NC} to stop the stack and keep all three."

    if purge_consent_given "$@"
    then
        answer="yes"
    elif [ -t 0 ]
    then
        printf '%b' " Type ${YEL}yes${NC} to continue: "
        # Guarded because a closed input would otherwise end the script through errexit
        # rather than through the refusal below, which is the answer that ought to be given.
        read -r answer || answer=""
    else
        printf '%b\n' " ${RED}Refusing to purge without confirmation.${NC} Re-run with ${BLU}--yes${NC} when this is intended."
        return 1
    fi

    if [ "${answer}" != "yes" ]
    then
        printf '%b\n' " ${YEL}Nothing was deleted.${NC}"
        return 0
    fi

    # The teardown profile set, so that the broker's container and its kafka_data volume are
    # actually removed. Without it a profiled service is excluded and the volume this
    # operation exists to discard survives - which is precisely the volume an operator runs
    # --purge to re-bootstrap.
    COMPOSE_PROFILES="$(teardown_compose_profiles)" ${COMPOSE_CL} --env-file "${env}" -f "${COMPOSE_FILE}" down --volumes
}

main() {
    case "${1}" in
        --pull | -p )
            docker image prune -a --force --filter "until=72h"
            # The start-up profile set, not the teardown one: pull what this stack would run.
            # Pulling the broker image for a stack that never starts it would cost several
            # hundred megabytes for nothing.
            COMPOSE_PROFILES="$(startup_compose_profiles)" ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} pull "${@:2}"
            ;;
        --up | -u )
            staged_up "${@:2}"
            ;;
        --down | -d )
            COMPOSE_PROFILES="$(teardown_compose_profiles)" ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} down
            ;;
        --purge )
            purge "${@:2}"
            ;;
        --build | -b )
            staged_up --build "${@:2}"
            ;;
        --restart | -r )
            # The teardown profile set, so a broker started by an earlier run is actually
            # stopped rather than left holding its port while the stack comes back up.
            COMPOSE_PROFILES="$(teardown_compose_profiles)" ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} down
            staged_up "${@:2}"
            ;;
        --init | -i )
    	    #checking if .env is available:
            if [ -r ${env} ]
            then
                printf "The environment file ${RED}${env}${NC} is already available. If you want to start from scratch, delete it and restart.\n"
                # An existing file is not left as found. Its permissions are still repaired
                # and any missing Kafka credential is still generated, because the two
                # defects this guards against are exactly the ones an ALREADY-EXISTING file
                # has: it was created by an older --init that never set a mode, and it
                # predates the credentials the compose stack now requires. Saying "already
                # available" and returning would leave a world-readable superuser secret in
                # place and a stack that cannot start.
                ensure_env_secrets
            else
                # CREATED PRIVATE FROM THE FIRST BYTE, not created and then tightened.
                #
                # This used to be a plain `cp` under the ambient umask, so .env was typically
                # mode 644 - world-readable - and every secret --init generates went into it:
                # the Postgres password, the Kafka administrative password whose principal is
                # in super.users, and the producer password. Any local account, and any
                # process running as any other user, could read all three.
                #
                # `install -m 600` sets the mode as part of the copy rather than afterwards.
                # That ordering is the point: a create-then-chmod leaves a window, however
                # brief, in which the file exists with the permissive mode and the secrets are
                # already being written into it. The surrounding umask is set as well, because
                # the later `sed -i` writes a TEMPORARY FILE in the same directory and takes
                # its mode from the umask, not from the file it is replacing - so without it
                # the secrets would appear world-readable in that temporary copy even though
                # the final file was private.
                printf "Creating file: ${YEL}${env}${NC} with secrets... Check it before you spin up the stack.\n"

                # THE MODE IS SET BEFORE THE FIRST SECRET EXISTS, and that ordering is the
                # whole of this fix.
                #
                # ${example} is a committed template and is mode 0644, as it should be — it
                # contains no secrets. "cp" preserves nothing and creates the destination
                # under the process umask, which on a typical developer machine is 022, so
                # ${env} was created world-readable and every credential generated below was
                # written into a world-readable file: a PostgreSQL password and a Kafka
                # SUPERUSER password, on a stack that publishes the broker to the host.
                #
                # Two changes, both needed. "umask 077" makes the file 0600 AT CREATION, so
                # there is no window in which it exists readable and empty-of-secrets — a
                # chmod after the writes would leave exactly that window, and a chmod after
                # the copy would still be one syscall late. The explicit chmod that follows
                # is belt and braces for a filesystem or umask that did not honour it, and it
                # is checked rather than assumed: a .env this script could not protect must
                # not then have credentials written into it.
                umask 077
                cp ${example} ${env}
                chmod 600 ${env}

                if [ "$(stat -c '%a' ${env} 2>/dev/null || echo unknown)" != "600" ]
                then
                    printf '%b\n' " ${RED}Refusing to write secrets into ${env}${NC}: its mode could not be set to 600."
                    printf '%b\n' " Every value below is a credential — a PostgreSQL password and a Kafka superuser"
                    printf '%b\n' " password — and this stack publishes the broker to the host. Fix the filesystem"
                    printf '%b\n' " permissions, delete ${env}, and run ${BLU}${0} --init${NC} again."
                    rm -f ${env}
                    exit 1
                fi

                POSTGRES_PASSWORD=$(openssl rand -base64 15)
                sed -i "s|{POSTGRES_PASSWORD}|$POSTGRES_PASSWORD|g" ${env}
                # The Kafka administrative principal and its password, generated the same way
                # and for the same reason: one credential per stack, created locally, written
                # only into ${env} - which is git-ignored - and never into this file.
                #
                # The broker's storage is formatted with this password and its clients then
                # authenticate with it, so the pair has to be set together; ${example} ships
                # both empty and each script refuses a half-configured pair by name. The
                # kafka and kafka-init services now REQUIRE the secret rather than defaulting
                # it, so generating it here is what makes a fresh stack start at all.
                #
                # GENERATED, THEN WRITTEN. Writing an unassigned variable here left the key
                # EMPTY while set_env_value still printed "generated into .env" - a stack
                # whose broker cannot bootstrap at all, reported as a success.
                KAFKA_SASL_ADMIN_SECRET=$(generate_kafka_secret)
                set_env_value "KAFKA_SASL_ADMIN_USER" "${kafka_admin_principal}"
                set_env_value "KAFKA_SASL_ADMIN_SECRET" "$KAFKA_SASL_ADMIN_SECRET"

                # THE STEADY-STATE PRODUCER PRINCIPAL: a SECOND, separate credential, and the
                # separation is the point rather than an inconvenience.
                #
                # The administrative principal above is a cluster superuser — it creates
                # topics, mints SCRAM credentials for any principal and rewrites every ACL.
                # The server and worker do none of that; they publish. Blnk therefore REFUSES
                # to publish as the administrator: with an administrative pair configured and
                # no producer pair its event publisher fails to construct and neither process
                # starts. Generating this pair here is what makes the correct configuration
                # the default one, instead of leaving an operator to discover the refusal.
                #
                # ONE IDENTITY, WRITTEN UNDER BOTH NAMES IT IS READ UNDER.
                #
                # The producer pair is resolved from KAFKA_SASL_USER / KAFKA_SASL_SECRET first
                # and from KAFKA_PRODUCER_USER / KAFKA_PRODUCER_SECRET second - by
                # config.KafkaConfig, by the compose server and worker services, and by
                # scripts/kafka-provision.sh, all three in that order. Writing only one of the
                # two pairs would leave the other empty in ${env}, which reads as "not
                # configured" to anyone editing the file by hand and invites a second,
                # different credential being filled in beside the first. The symptom of that
                # divergence is a SASL handshake failure with two correct-looking
                # configurations.
                #
                # So both pairs are written, from ONE generated value. The credential the
                # broker is minted with and the credential the application presents are then
                # the same string however it is resolved, and there is no ordering in which
                # they can disagree.
                KAFKA_PRODUCER_SECRET=$(generate_kafka_secret)
                set_env_value "KAFKA_PRODUCER_USER" "${kafka_producer_principal}"
                set_env_value "KAFKA_PRODUCER_SECRET" "$KAFKA_PRODUCER_SECRET"
                set_env_value "KAFKA_SASL_USER" "${kafka_producer_principal}"
                set_env_value "KAFKA_SASL_SECRET" "$KAFKA_PRODUCER_SECRET"

                # The sample subscriber's password, generated here for a reason that is about
                # DISCLOSURE rather than about convenience.
                #
                # scripts/kafka-provision.sh no longer prints a generated password: it runs as
                # the compose kafka-init service, so its stdout is a container log that retains
                # the credential for the container's lifetime, hands it to anyone who can run
                # "docker compose logs", and forwards it to whatever collects the host's logs.
                # Generating it here instead puts it in a mode-0600 file the operator already
                # owns, and the script then applies a SUPPLIED secret, which it never echoes.
                #
                # Unlike the two pairs above this one is optional: with it empty the sample
                # principal is skipped and Blnk publishes normally. It is generated anyway so
                # that a local consumer works out of the box.
                # Generated with the same helper as the two pairs above, so it clears the same
                # 32-character and 16-distinct-character floors kafka-provision.sh enforces on a
                # SUPPLIED secret. A raw base64 draw can carry "+" and "/", which the shared
                # credential alphabet does accept, but it checks neither floor - and a secret the
                # script then refuses would skip the sample principal with a weak-credential
                # error about a credential the operator never chose.
                KAFKA_SAMPLE_SUBSCRIBER_SECRET=$(generate_kafka_secret)
                set_env_value "KAFKA_SAMPLE_SUBSCRIBER_SECRET" "$KAFKA_SAMPLE_SUBSCRIBER_SECRET"

                warn_unsubstituted_placeholders

                # Re-asserted after every write, for the reason given in
                # enforce_env_permissions: the mode is the one property of this file that
                # must hold no matter how it was edited.
                enforce_env_permissions

                #checking if .env created successfully:
                if [ ! -r ${env} ]
                then
                    printf "Error creating environment file ${RED}${env}${NC}. Please, check if an ${BLU}.env${NC} file available, resolve and restart.\n"
                    exit 1
                fi
            fi
            ;;
        * ) help
            ;;
    esac

}

showenv "$@"
time main "$@" # calls the main procedure and prints time used to execute
