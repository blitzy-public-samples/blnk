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

# The administrative Kafka principal --init writes into ${env}. A principal NAME, not a
# secret, and the same default the kafka, kafka-init, server and worker services already
# carry, so the credential the broker is bootstrapped with and the one its clients present
# cannot drift apart. Its password is generated per stack and never written down here.
declare kafka_admin_principal="admin"

# KAFKA_BROKERS as the SHELL supplied it, captured before anything can overwrite it:
# showenv() sources ${env} ahead of main(), so an assignment there would replace it. Compose
# resolves interpolation from the shell environment FIRST and from --env-file only after, and
# this reproduces that precedence - reading the post-source value alone would skip the Kafka
# checks for someone running "KAFKA_BROKERS=kafka:9092 ${0} -u" against a .env that leaves
# the key empty. An unset name expands to empty here because nounset is deliberately off.
declare KAFKA_BROKERS_FROM_SHELL="${KAFKA_BROKERS}"

help() {
    printf "\n \
    Usage:${BLU} ${0} ${GRN}parameters${NC}\n \
    ${GRN}--pull, -p${NC}\t\t Pull the repo from registry\n \
    ${GRN}--up,-u${NC}\t\t Spin up, then verify Kafka provisioning\n \
    ${GRN}--build,-b${NC}\t\t Build the stack, then verify Kafka provisioning\n \
    ${GRN}--down,-d${NC}\t\t Shut down, keeping every data volume\n \
    ${GRN}--purge${NC}\t\t Shut down AND DELETE every data volume\n \
    ${GRN}--restart,-r${NC}\t Cold-restart, then verify Kafka provisioning\n \
    ${GRN}--init,-i${NC}\t\t Create a .env file, generating its secrets\n \
    \n \
    Examples:
    ${BLU} ${0} ${GRN}-u${NC}\n\
    ${BLU} ${0} ${GRN}--purge --yes${NC}\n\
    \n\
    "
}

showenv() {
    if [ -r ${env} ]
    then
        source ${env}
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
# ${example} is the source of truth for the shape and it uses two. POSTGRES_PASSWORD ships as
# the brace placeholder {POSTGRES_PASSWORD}, substituted by the line in --init below. The
# Kafka keys ship as an EMPTY assignment instead, with their intended value shown in a
# neighbouring comment, deliberately: a second brace placeholder in that file would have
# survived into .env as a literal password for as long as --init substituted only the one
# name. Both shapes are handled here, so neither can produce a .env that looks configured
# and is not.
#
# THAT SECOND SHAPE IS WHY THIS FUNCTION EXISTS RATHER THAN A SECOND SED LINE. ${example},
# scripts/kafka-bootstrap.sh and scripts/kafka-provision.sh each still describe --init as
# substituting {POSTGRES_PASSWORD} and nothing else, which was true when they were written
# and is what this change supersedes. Their BEHAVIOUR is unaffected and improves: they read
# the admin pair from the environment and refuse an empty or still-braced value, and --init
# now fills it, so the paths carrying that description become unreachable on a stack
# initialised from here. Handling the empty assignment - not only the placeholder - is what
# makes that true without any of those three files having to be edited in step.
#
# The sed delimiter is "|" for the same reason as the POSTGRES_PASSWORD line: base64 output
# contains "/" and "+", which a "/" delimiter could not carry. It contains no "&" and no
# "\" - the two characters sed reads specially on the replacement side - and no "|", so a
# generated value needs no escaping. Keep any future value inside that alphabet.
#
# An assignment that ALREADY carries a value is left alone. That is an operator's deliberate
# pin, and overwriting it would replace a credential the broker may already be holding.
set_env_value() {
    local key="${1}" value="${2}"

    if grep -qF "{${key}}" "${env}"
    then
        sed -i "s|{${key}}|${value}|g" "${env}"
    elif grep -qE "^${key}=[[:space:]]*$" "${env}"
    then
        sed -i "s|^${key}=[[:space:]]*$|${key}=${value}|" "${env}"
    elif grep -qE "^${key}=" "${env}"
    then
        printf '%b\n' " ${BLU}${key}${NC} already carries a value in ${env}; leaving it untouched."
    else
        printf '%s\n' "" "# Added by ${0} --init because ${example} does not declare it." "${key}=${value}" >>"${env}"
    fi
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

# =======================================================================================
# Local Kafka stack
#
# The event-publishing pipeline needs a broker that is not merely created but LISTENING and
# answering AUTHENTICATED requests, and a topic catalogue that already exists - the broker is
# configured with auto-creation off, so a topic nobody provisioned is a topic the relay
# cannot publish to. Compose does both jobs itself: the kafka service healthcheck completes a
# full SASL/SCRAM handshake and an authorized metadata request, and the kafka-init one-shot
# provisions the catalogue behind that gate. What follows therefore WAITS FOR AND VERIFIES
# that work rather than duplicating it, and runs scripts/kafka-provision.sh directly only
# when the one-shot did not run or did not succeed.
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

# The broker list this stack is actually configured with, shell environment first and ${env}
# second, matching how Compose resolves the same name.
#
# AN EMPTY LIST IS A LEGITIMATE STEADY STATE, NOT AN ERROR. It is what ${example} ships: with
# no brokers the event publisher resolves to its no-op implementation and Blnk starts, serves
# and processes transactions exactly as it did before Kafka existed. Every check below is
# skipped - never failed - when this prints nothing.
effective_kafka_brokers() {
    if [ -n "${KAFKA_BROKERS_FROM_SHELL}" ]
    then
        printf '%s' "${KAFKA_BROKERS_FROM_SHELL}"
    else
        printf '%s' "${KAFKA_BROKERS}"
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

        # Announced once, and only when there is actually a wait: "up -d" already blocks on
        # the healthcheck through the depends_on conditions, so the common case is a single
        # conclusive probe and silence is the right output for it.
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
# below is the entire interface. Every name is one it documents reading, each is passed even
# when empty because it defaults all of them with the ${VAR:-default} form, and the values are
# ${env}'s because showenv sourced it. The broker list is the effective one so a shell-level
# override is not silently dropped. On a host the Kafka CLI is normally absent; the script
# handles that itself by re-executing inside the broker container over docker, and
# KAFKA_CONTAINER and KAFKA_COMPOSE_SERVICE are what tell it where to go.
provision_kafka() {
    local brokers="${1}" service="${2}"

    if [ ! -x "${kafka_provision_script}" ]
    then
        kafka_warn "${kafka_provision_script} is not executable from here, so provisioning was skipped." \
            "Run ${0} from the repository root, or restore the file's execute bit."
        return 1
    fi

    KAFKA_BROKERS="${brokers}" \
        KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER}" \
        KAFKA_TOPIC_PREFIX="${KAFKA_TOPIC_PREFIX}" \
        KAFKA_MIN_PARTITIONS="${KAFKA_MIN_PARTITIONS}" \
        KAFKA_REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR}" \
        KAFKA_SASL_ADMIN_USER="${KAFKA_SASL_ADMIN_USER}" \
        KAFKA_SASL_ADMIN_SECRET="${KAFKA_SASL_ADMIN_SECRET}" \
        KAFKA_SECURITY_PROTOCOL="${KAFKA_SECURITY_PROTOCOL}" \
        KAFKA_SCRAM_ITERATIONS="${KAFKA_SCRAM_ITERATIONS}" \
        KAFKA_CONTAINER="${KAFKA_CONTAINER}" \
        KAFKA_COMPOSE_SERVICE="${service}" \
        "${kafka_provision_script}"
}

# Confirms, after the stack is up, that events have somewhere to go: a broker that
# authenticates and a catalogue that exists.
#
# ALWAYS SUCCEEDS. A Kafka problem is reported in full and never fatal - the database, the
# queue and the API came up perfectly well, and an unprovisioned broker leaves events waiting
# in the transactional outbox until it is fixed, which is exactly what the outbox is for.
# Every call site guards this with "|| true" as well, so that no future edit here can turn a
# broker hiccup into a failed bring-up.
ensure_kafka() {
    local brokers="" service="" init_service="" id="" health=0 init=0

    brokers="$(effective_kafka_brokers)"
    service="${KAFKA_COMPOSE_SERVICE:-kafka}"
    init_service="${KAFKA_INIT_COMPOSE_SERVICE:-kafka-init}"

    if [ -z "${brokers}" ]
    then
        kafka_note "KAFKA_BROKERS is empty, so event publishing is off and there is nothing to verify." \
            "That is a supported steady state and not a problem: the publisher resolves to its no-op" \
            "implementation and Blnk runs exactly as it did before Kafka existed. The broker and its" \
            "one-shot provisioning still come up with the rest of the stack." \
            "To publish events set ${BLU}KAFKA_BROKERS${NC} in ${env} - ${BLU}kafka:9092${NC} on the compose network - and" \
            "a webhook deprecation date with it, which Blnk requires whenever brokers are configured."
        return 0
    fi

    id="$(compose_container_id "${service}")"
    if [ -z "${id}" ]
    then
        kafka_note "no container for the \"${service}\" service, so the broker checks were skipped." \
            "Expected when ${COMPOSE_FILE} does not declare it, or when only some services were named." \
            "Blnk is configured for ${brokers}; assure that broker's topics yourself if it is external."
        return 0
    fi

    wait_for_kafka_healthy "${id}" || health=$?
    case "${health}" in
        0)
            kafka_ok "broker healthy - SASL/SCRAM handshake completed and an authorized request answered."
            ;;
        1 | 2)
            kafka_error "the \"${service}\" service never became healthy, so the topic catalogue is unverified." \
                "The rest of the stack is up, and events accumulate in the outbox until a broker accepts them." \
                "Most likely ${env} now carries a different ${BLU}KAFKA_SASL_ADMIN_SECRET${NC} than the one the" \
                "kafka_data volume was formatted with. Bootstrapping is one-shot and deliberately skips an" \
                "already-formatted volume, so the seeded credential is never updated and every handshake fails." \
                "Fix: discard that volume and let it re-bootstrap. ${BLU}${0} --purge${NC} removes EVERY data volume," \
                "the ledger's included, so remove only this project's kafka_data volume to keep the rest." \
                "Otherwise read the broker's own account: ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} logs ${service}"
            return 0
            ;;
        *)
            kafka_error "the \"${service}\" container is not running, so the topic catalogue is unverified." \
                "The rest of the stack is up. Read why it stopped with:" \
                "${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} logs ${service}"
            return 0
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
    else
        kafka_warn "provisioning did not complete, and the bring-up is being left alone rather than failed." \
            "Events wait in the outbox until the catalogue exists; re-run ${kafka_provision_script} once it can."
    fi

    return 0
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

    ${COMPOSE_CL} --env-file "${env}" -f "${COMPOSE_FILE}" down --volumes
}

main() {
    case "${1}" in
        --pull | -p )
            docker image prune -a --force --filter "until=72h"
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} pull "${@:2}"
            ;;
        --up | -u )
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} up -d "${@:2}"
            ensure_kafka || true
            ;;
        --down | -d )
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} down
            ;;
        --purge )
            purge "${@:2}"
            ;;
        --build | -b )
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} up -d --build "${@:2}"
            ensure_kafka || true
            ;;
        --restart | -r )
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} down
            ${COMPOSE_CL} --env-file ${env} -f ${COMPOSE_FILE} up -d "${@:2}"
            ensure_kafka || true
            ;;
        --init | -i )
    	    #checking if .env is available:
            if [ -r ${env} ]
            then
                printf "The environment file ${RED}${env}${NC} is already available. If you want to start from scratch, delete it and restart.\n"
            else
                # copying _env into the .env if not found:
                printf "Creating file: ${YEL}${env}${NC} with secrets... Check it before you spin up the stack.\n"
                cp ${example} ${env}
                POSTGRES_PASSWORD=$(openssl rand -base64 15)
                sed -i "s|{POSTGRES_PASSWORD}|$POSTGRES_PASSWORD|g" ${env}
                # The Kafka administrative principal and its password, generated the same way
                # and for the same reason: one credential per stack, created locally, written
                # only into ${env} - which is git-ignored - and never into this file.
                #
                # The broker's storage is formatted with this password and its clients then
                # authenticate with it, so the pair has to be set together; ${example} ships
                # both empty and each script refuses a half-configured pair by name.
                #
                # THE BYTE COUNT MUST STAY A MULTIPLE OF THREE. base64 pads a remainder with
                # "=", and "=" is outside the credential alphabet that kafka-bootstrap.sh and
                # kafka-provision.sh share - Kafka's --add-scram value grammar has no escape
                # sequence for it - so a padded password is refused and the broker never
                # bootstraps. 24 bytes give 32 unpadded characters.
                KAFKA_SASL_ADMIN_SECRET=$(openssl rand -base64 24)
                set_env_value "KAFKA_SASL_ADMIN_USER" "${kafka_admin_principal}"
                set_env_value "KAFKA_SASL_ADMIN_SECRET" "$KAFKA_SASL_ADMIN_SECRET"
                warn_unsubstituted_placeholders
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
