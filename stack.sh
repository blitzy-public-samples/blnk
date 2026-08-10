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

# The Compose command line, DETECTED rather than assumed. Empty here and resolved by
# resolve_compose_cl once ${env} has been sourced, so an explicit pin in ${env} still wins.
#
# It was pinned to "docker-compose" - the standalone v1 script - and ${example} shipped the same
# pin uncommented. On any host with only the Compose v2+ plugin, which is every current Docker
# installation, every compose call in this script failed with "command not found": the ledger
# never started, and the Kafka stages this file now owns could not run at all. A default that
# names a binary the host may not have is not a default.
#
# The v1 script is no longer accepted at all, by detection or by an explicit pin: the compose
# files need Compose ${COMPOSE_MINIMUM_VERSION} or newer for depends_on.required, and v1 cannot
# parse them. resolve_compose_cl version-checks whatever it ends up with.
declare COMPOSE_CL=""

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
# EVERY VARIABLE THE PROVISIONING SCRIPT READS, forwarded to it when the host fallback runs it
# directly — READ FROM THE SCRIPT ITSELF rather than restated here.
#
# It used to be a literal list, and before that a hand-written argument sequence, and both had
# drifted. The literal list carried the wrong producer skip flag and omitted the CLI timeout
# pair and the producer secret file; the argument sequence before it carried ten names out of
# the twenty-odd the script documents. Either way a stack whose ${env} pinned one of the missing
# settings got it when compose ran the one-shot and SILENTLY LOST IT when this fallback ran
# instead — the two paths provisioning differently from the same ${env}, with which one ran
# decided by nothing more than whether the host happened to have the Kafka CLI. Invisible, and
# unreproducible.
#
# "scripts/kafka-provision.sh --print-interface-host" emits one variable name per line and
# exits without touching anything, so this is not a copy that can fall behind: it IS the
# script's declaration. --host adds the delegation targets (KAFKA_CONTAINER,
# KAFKA_PROVISION_CONTAINER_SCRIPT and the client-configuration container path), which a
# host-side invoker may set and which the script deliberately does not forward into the
# container.
#
# Deliberately absent from what the script emits, and therefore from this: KAFKA_BROKERS and
# KAFKA_COMPOSE_SERVICE, because provision_kafka computes both — the effective broker list and
# the service it was asked about — and a stale ${env} value must not win over either.
#
# Read here at startup rather than inside provision_kafka so that a script that cannot be read
# is reported once, at the point the list would have been built, rather than as an empty
# forwarding later. An empty list is treated as fatal by resolve_kafka_provision_interface,
# because silently forwarding nothing is the exact failure this replaced.
declare -a kafka_provision_passthrough=()

# Populate kafka_provision_passthrough from the provisioning script's own declaration.
#
# Called from main before any Kafka work. It is a function rather than a top-level command
# substitution so that a failure can be REPORTED with the reason instead of leaving an empty
# array behind: forwarding no variables at all would provision defaults and report success,
# which is the silent-divergence failure mode this whole mechanism exists to remove.
resolve_kafka_provision_interface() {
    if [ ! -r "${kafka_provision_script}" ]
    then
        # Not fatal on its own: provision_kafka already refuses when the script is not
        # executable, and that message tells the operator what to do. This only notes that the
        # interface could not be read, so a later "provisioning was skipped" is not a surprise.
        return 1
    fi

    local -a names=()
    # shellcheck disable=SC2312 # the exit status is checked immediately below, on the array
    while IFS= read -r name
    do
        case "${name}" in
            # Only real variable names. A blank line or anything a future usage change might
            # print alongside them is ignored rather than forwarded as an assignment.
            [A-Z]*) names+=("${name}") ;;
        esac
    done < <(bash "${kafka_provision_script}" --print-interface-host 2>/dev/null)

    if [ "${#names[@]}" -eq 0 ]
    then
        kafka_warn "could not read the provisioning interface from ${kafka_provision_script}." \
            "The host fallback would forward no settings at all and provision defaults, so it" \
            "is better to know now. Check that the script is intact and that" \
            "'${kafka_provision_script} --print-interface-host' prints variable names."
        return 1
    fi

    kafka_provision_passthrough=("${names[@]}")
    return 0
}

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
    Usage:${BLU} %s ${GRN}parameters${NC}\n \
    ${GRN}--pull, -p${NC}\t\t Pull this stack's images from the registry. Set\n \
    \t\t\t STACK_PRUNE_IMAGES=1 to ALSO prune every unused image on the\n \
    \t\t\t host older than 72h first — host-global, not just this project\n \
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
    ${BLU} %s ${GRN}-u${NC}\n\
    ${BLU} %s ${GRN}--purge --yes${NC}\n\
    \n\
    " "${0}" "${0}" "${0}"
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
    printf "      Stack name : ${YEL}%s${NC}\n" "${COMPOSE_PROJECT_NAME}"
    printf "    Compose file : ${BLU}%s${NC}\n" "${COMPOSE_FILE}"
    printf "  CL parameter 0 : ${BLU}%s${NC}\n" "${0}"
    printf "  CL parameter 1 : ${BLU}%s${NC}\n" "${1}"
    printf "  CL parameter 2 : ${BLU}%s${NC}\n" "${2}"
    printf "  CL parameter 3 : ${BLU}%s${NC}\n" "${3}"
    if [ -r ${env} ]
    then
      printf "             env : ${BLU}%s${NC}\n" "${env}"
    else
      printf "             env : ${RED}%s${NC} not found. You may want to initialize the stack with -i parameter\n" "${env}"
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
#
# write_env_substitution performs the edit. It exists as a separate function because the
# mechanism matters as much as the result, and the mechanism is the fix for two defects.
#
# THE VALUE NEVER REACHES ARGV. This used to be `sed -i "s|{KEY}|${value}|g"`, which places the
# credential in sed's command line - and on Linux /proc/<pid>/cmdline is mode 444, readable by
# every account on the host, while /proc/<pid>/environ is mode 400, readable only by the owner.
# So the old form published the PostgreSQL password and the Kafka superuser password to any
# local user who ran `ps` or read /proc during the fraction of a second sed lived, and it did so
# in the same function whose comment promises that values are "never" printed. The value now
# crosses into awk through the ENVIRONMENT, which is the private one of those two channels.
#
# THE REPLACEMENT IS LITERAL. A value reaching a sed replacement is not literal text: "&" means
# "the whole match" and "\1" a backreference, so a generated secret containing either produced a
# .env holding something other than the secret, and the service then failed to authenticate with
# a credential that had been silently altered. awk's index()/substr() splice below has no
# metacharacters at all.
#
# IT DOES NOT DEPEND ON GNU sed. `sed -i` with no suffix argument is a GNU extension; BSD and
# macOS sed read the next argument as the backup suffix, so on those platforms the old form
# consumed "${env}" as a suffix and edited nothing, or errored. awk plus an explicit rename is
# POSIX and behaves the same everywhere.
#
# $1 mode  - "placeholder" to replace every {KEY} occurrence, "blank" to fill a valueless KEY= line
# $2 key   - the environment key name
# $3 value - the value to write; never printed, never passed as an argument
# require_env_key_name refuses anything that is not a usable environment key name.
#
# Called from set_env_value BEFORE it chooses a branch, and again from write_env_substitution,
# because the two are reachable independently and the check has to hold for both. The append
# branch is why this is factored out at all: it writes "KEY=value" straight onto the end of
# ${env} without rewriting the file, so a check living only in write_env_substitution left it
# unguarded - a malformed key was appended and reported as a success, and a key containing a
# newline would have injected whole lines of its own into a file full of credentials.
#
# The permitted set is [A-Za-z0-9_], which is what a shell, docker compose and envconfig can all
# read back. Everything else is refused by name rather than sanitised, because silently altering
# the key an operator asked for produces a .env that does not say what they think it says.
require_env_key_name() {
    case "${1}" in
        "" | *[!A-Za-z0-9_]* )
            printf '%b\n' " ${RED}Refusing to edit ${env}${NC}: '${1}' is not a valid environment key name."
            printf '%b\n' " Keys may contain only letters, digits and underscores."
            return 1
            ;;
    esac
}

write_env_substitution() {
    local mode="${1}" key="${2}" value="${3}" tmp=""

    require_env_key_name "${key}" || return 1

    # The temporary file is created BESIDE ${env} and under umask 077.
    #
    # Beside it, because the rename at the end is only atomic within one filesystem - a reader
    # of ${env} sees either the old content or the new, never a partial write.
    #
    # Under umask 077, because the temporary file holds the same secrets as the file it will
    # become. Creating it under the ambient umask would leave the credentials world-readable in
    # the temporary copy even though the final file is 600.
    tmp="$( umask 077; mktemp "${env}.XXXXXX" )" || {
        printf '%b\n' " ${RED}Could not create a temporary file beside ${env}${NC}; nothing was written."
        return 1
    }

    if ! SEV_MODE="${mode}" SEV_KEY="${key}" SEV_VALUE="${value}" awk '
        BEGIN {
            mode  = ENVIRON["SEV_MODE"]
            key   = ENVIRON["SEV_KEY"]
            value = ENVIRON["SEV_VALUE"]
            ph    = "{" key "}"
            phlen = length(ph)
            klen  = length(key)
        }
        mode == "placeholder" {
            # A literal global replace, built by splicing rather than by substitution, so no
            # character in the value carries a meaning.
            out = ""
            line = $0
            while ((at = index(line, ph)) > 0) {
                out  = out substr(line, 1, at - 1) value
                line = substr(line, at + phlen)
            }
            print out line
            next
        }
        mode == "blank" {
            # Compared by position rather than by regex for the same reason.
            if (substr($0, 1, klen + 1) == key "=") {
                rest = substr($0, klen + 2)
                if (rest ~ /^[ \t]*$/) {
                    print key "=" value
                    next
                }
            }
            print
            next
        }
        { print }
    ' "${env}" >"${tmp}"
    then
        rm -f "${tmp}"
        printf '%b\n' " ${RED}Failed to rewrite ${env}${NC}; it is unchanged."
        return 1
    fi

    # Asserted rather than assumed: mktemp under umask 077 should already be 600, but this file
    # is about to BECOME ${env}, and its mode is the mode ${env} will have.
    chmod 600 "${tmp}" 2>/dev/null || true

    if ! mv -f "${tmp}" "${env}"
    then
        rm -f "${tmp}"
        printf '%b\n' " ${RED}Could not replace ${env}${NC}; it is unchanged."
        return 1
    fi
}

set_env_value() {
    local key="${1}" value="${2}"

    # CHECKED BEFORE THE BRANCH IS CHOSEN, so the append arm below is covered too.
    require_env_key_name "${key}" || return 1

    if grep -qF "{${key}}" "${env}"
    then
        write_env_substitution "placeholder" "${key}" "${value}" || return 1
        printf '%b\n' " ${GRN}${key}${NC} generated into ${env}."
    elif grep -qE "^${key}=[[:space:]]*$" "${env}"
    then
        write_env_substitution "blank" "${key}" "${value}" || return 1
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

# NOTE ON A CONTROL THAT USED TO LIVE HERE.
#
# A second, weaker copy of the permission check stood at this point: require_private_env_file.
# It was DEAD CODE - defined, never called - and it duplicated enforce_env_permissions above,
# which does the same job better and is called from initialize_env, the Kafka credential
# backfill and --init. Two competing implementations of one security control is worse than one,
# because a reader cannot tell which is authoritative and a fix applied to the wrong copy looks
# applied while changing nothing.
#
# It is deleted rather than wired up, and its one genuine advantage was carried across first:
# it tried BSD's `stat -f '%Lp'` as well as GNU's `stat -c '%a'`, so enforce_env_permissions now
# tries both. Nothing was lost with it.


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

    # stat's spelling differs between GNU and BSD, and BOTH are tried. GNU coreutils uses
    # -c '%a'; BSD and macOS use -f '%Lp'. Only the GNU form used to be attempted, so on macOS
    # every invocation fell into the unverifiable branch below and re-chmod'd a file that was
    # already correct, announcing a permission change that had not happened. An unknown third
    # spelling is still left empty and handled as unverifiable, because a wrong format string
    # would report a mode that is not the file's.
    mode="$(stat -c '%a' "${env}" 2>/dev/null || stat -f '%Lp' "${env}" 2>/dev/null || true)"

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

    # Re-asserted after writing: set_env_value replaces ${env} by writing a temporary file and
    # renaming it over the original, so the mode ${env} ends up with is the temporary file's.
    # write_env_substitution creates that file under umask 077 and chmods it 600 before the
    # rename, but this is the check that makes the guarantee rather than trusting it.
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

# Run docker compose with the EFFECTIVE broker list pinned, and with this stack's env file and
# compose file already applied.
#
# EVERY COMPOSE INVOCATION IN THIS FILE GOES THROUGH HERE, and the reason is a cascade that made
# the script and the containers disagree about the one value that decides whether Blnk publishes
# at all.
#
# showenv() sources ${env} before main() runs. If the caller had already EXPORTED KAFKA_BROKERS,
# that source does not shadow it - assigning to an exported name keeps the export attribute and
# replaces the value, so every later child process inherits ${env}'s value. Meanwhile this script
# decides with KAFKA_BROKERS_FROM_SHELL, captured before the source, which is correct: compose
# resolves the shell environment ahead of --env-file. The two then differ. "KAFKA_BROKERS=broker-a
# ./stack.sh -u" against a .env naming broker-b had this script wait for, authenticate to and
# verify the catalogue on broker-a while the server published to broker-b - and the
# bring-up reported success.
#
# Pinning the value on the invocation removes the disagreement at its source rather than
# reproducing the precedence rule in two places: compose prefers the invocation environment over
# --env-file, so what it interpolates is exactly what effective_kafka_brokers decided. An empty
# result is passed as an explicit empty value, which is the right answer and not an omission -
# it is how "no brokers for this run" reaches the containers, and the compose files declare
# ${KAFKA_BROKERS:-} so an empty value selects the documented no-op publisher.
#
# Going through one function is also what keeps it true: a compose call added later that forgets
# the pin is visible as a call that does not use this helper.
compose() {
    KAFKA_BROKERS="$(effective_kafka_brokers)" \
        ${COMPOSE_CL} --env-file "${env}" -f "${COMPOSE_FILE}" "$@"
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

    id="$(compose ps -aq "${service}" 2>/dev/null | head -n 1 || true)"
    if [ -z "${id}" ]
    then
        id="$(compose ps -q "${service}" 2>/dev/null | head -n 1 || true)"
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
# below is the entire interface, and kafka_provision_passthrough is the whole of it — read from
# the script's own "--print-interface-host" at startup rather than restated here, so it cannot
# fall behind. Values are ${env}'s, because showenv sourced it before main ran. On a host the
# Kafka CLI is normally absent; the script handles that itself by re-executing inside the broker
# container over docker, and KAFKA_CONTAINER and KAFKA_PROVISION_CONTAINER_SCRIPT - both in the
# host-only half of that interface - are what tell it where to go.
#
# WHY THE FULL LIST MATTERS, with one variable as the illustration.
# KAFKA_ALLOW_PARTITION_GROWTH is CONSENT to an operation that re-maps keys already written: add
# partitions to a topic that holds records and one ledger's events split across partitions,
# after which a consumer can observe them out of order with nothing failing to say so. Compose
# passes it to kafka-init and the Go admin client reads it from configuration. A fallback that
# dropped it would have the same ${env} refuse a growth through one path and answer differently
# through the other, decided by nothing more than whether this host has the Kafka CLI. Every
# other name on that list has its own version of the same argument, which is why the list is
# read from the script rather than curated by hand.
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
    # An empty interface means resolve_kafka_provision_interface could not read the script's
    # declaration, and it has already said so. Forwarding nothing would provision DEFAULTS and
    # report success — the silent divergence this mechanism exists to remove — so it refuses.
    if [ "${#kafka_provision_passthrough[@]}" -eq 0 ]
    then
        kafka_warn "the provisioning interface is empty, so nothing would be forwarded." \
            "Provisioning was skipped rather than run with defaults." \
            "Re-run once '${kafka_provision_script} --print-interface-host' prints variable names."
        return 1
    fi

    # THE PRODUCER PAIR IS NOT REWRITTEN HERE, and it used to be: two assignments mapped
    # KAFKA_SASL_* onto KAFKA_PRODUCER_* unconditionally, AFTER the allowlist, so they won.
    # ${example} documents setting only the KAFKA_PRODUCER_* pair as the normal compose route,
    # and for such a .env those assignments forwarded two EMPTY values - erasing the
    # operator's producer credential on this path alone and failing the run on a secret it was
    # never given. Both pairs now cross verbatim through the allowlist above, and the script
    # applies the same KAFKA_SASL_* first, KAFKA_PRODUCER_* second precedence the compose
    # services and config.KafkaConfig apply, so all three agree by construction.
    #
    # A SUBSHELL WITH export, NOT `env KEY=value`.
    #
    # The goal has not changed: nothing here may leak a credential into this shell's environment
    # for every later command in the bring-up to inherit. A subshell achieves that as completely
    # as `env` did - the exports die with the subshell - while fixing what `env` got wrong.
    #
    # What it got wrong is that `env KAFKA_SASL_ADMIN_SECRET=... script` places the broker
    # superuser's password in env's ARGV, and /proc/<pid>/cmdline is mode 444 on Linux: readable
    # by every account on the host for as long as provisioning runs, which is seconds, not
    # microseconds. The `export` builtin is executed by this shell itself, so there is no new
    # command line for anyone to read; the values reach the script through its environment,
    # /proc/<pid>/environ, which is mode 400 and readable only by its owner.
    #
    # The two computed values are exported last so they win over any ${env} entry of the same
    # name that reached the passthrough list: the broker list is the EFFECTIVE one, which honours
    # a shell-level override, and the service is the one the caller asked about.
    #
    # `exec` replaces the subshell rather than forking under it, so the exit status is the
    # script's own and there is one fewer process holding the credentials.
    (
        local name
        for name in "${kafka_provision_passthrough[@]}"
        do
            if [ -n "${!name+declared}" ]
            then
                export "${name}=${!name}"
            fi
        done

        export KAFKA_BROKERS="${brokers}"
        export KAFKA_COMPOSE_SERVICE="${service}"

        exec "${kafka_provision_script}"
    )
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
        compose config --services 2>/dev/null |
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
        compose up -d "${service}" "${init_service}"
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
        compose up -d "$@" || return 1

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
    COMPOSE_PROFILES="$(teardown_compose_profiles)" compose down --volumes
}

# =======================================================================================
# The Compose command line
# =======================================================================================

# The lowest Compose release that can read this repository's compose files.
#
# It is not a preference. Both docker-compose.yaml and docker-compose.dev.yaml declare
# "depends_on: <service>: required: false" on the Kafka dependency, and that field is what makes
# the broker OPT-IN: without it a profiled service that was never started is a hard dependency
# failure and the default bring-up cannot start at all. Compose added "required" in 2.20.0, so a
# host below that version does not merely lose a feature - it cannot parse the file, and the
# error it prints names a field rather than a version.
declare -r COMPOSE_MINIMUM_VERSION="2.20.0"

# The version of a Compose invocation, as "MAJOR.MINOR.PATCH", or nothing when it cannot be
# determined.
#
# "version --short" is used rather than parsing the human-readable banner, because the banner's
# wording has changed across releases while --short has always printed the bare number. A
# leading "v" is stripped and any pre-release or build suffix is discarded, so "v2.20.0-rc.1"
# reads as 2.20.0 - the field this requirement is about landed in the release, and a release
# candidate of it has the field.
#
# Every failure is swallowed and reported as an empty string: a wrapper that does not implement
# "version --short" is a legitimate thing for an operator to point COMPOSE_CL at, and errexit
# must not turn "cannot tell" into an aborted bring-up.
#
# Parameters:
#   - $1: the Compose invocation to interrogate, word-split on purpose because "docker compose"
#     is two words.
compose_version_of() {
    local invocation="${1}" raw=""

    # shellcheck disable=SC2086
    raw="$(${invocation} version --short 2>/dev/null | head -n 1 || true)"
    raw="${raw#v}"

    case "${raw}" in
        [0-9]*.[0-9]*)
            printf '%s' "${raw%%[!0-9.]*}"
            ;;
    esac
}

# Whether a "MAJOR.MINOR.PATCH" string is at least COMPOSE_MINIMUM_VERSION.
#
# Compared field by field as integers rather than lexically, because "2.9.0" sorts after
# "2.20.0" as text and that is exactly the comparison this repository needs to get right.
#
# Parameters:
#   - $1: the version to test. An empty or unparseable value answers "no".
compose_version_at_least() {
    local candidate="${1}" required="${COMPOSE_MINIMUM_VERSION}"
    local c_major c_minor c_patch r_major r_minor r_patch

    if [ -z "${candidate}" ]
    then
        return 1
    fi

    IFS='.' read -r c_major c_minor c_patch <<< "${candidate}"
    IFS='.' read -r r_major r_minor r_patch <<< "${required}"

    c_major="${c_major:-0}"; c_minor="${c_minor:-0}"; c_patch="${c_patch:-0}"
    r_major="${r_major:-0}"; r_minor="${r_minor:-0}"; r_patch="${r_patch:-0}"

    if [ "${c_major}" -ne "${r_major}" ]
    then
        [ "${c_major}" -gt "${r_major}" ]
        return $?
    fi

    if [ "${c_minor}" -ne "${r_minor}" ]
    then
        [ "${c_minor}" -gt "${r_minor}" ]
        return $?
    fi

    [ "${c_patch}" -ge "${r_patch}" ]
}

# Print the refusal an unusable Compose earns, then leave.
#
# Parameters:
#   - $1: what was found, phrased for the operator.
refuse_compose_cl() {
    printf '%b\n' " ${RED}${1}${NC}"
    printf '%b\n' " Every command in this script drives Compose, so none of them can run."
    printf '%b\n' " This repository's compose files use ${BLU}depends_on.required${NC}, which Compose"
    printf '%b\n' " added in ${BLU}${COMPOSE_MINIMUM_VERSION}${NC} and which is what makes the Kafka broker opt-in."
    printf '%b\n' " Install Docker with a Compose plugin at ${COMPOSE_MINIMUM_VERSION} or newer, or set"
    printf '%b\n' " ${BLU}COMPOSE_CL${NC} in ${BLU}${env}${NC} to a command that starts one here."
    exit 1
}

# Decide how to invoke Compose, once, before any command runs.
#
# WHY THIS IS DETECTED. Docker ships the Compose v2+ plugin with every current release, invoked
# as "docker compose". This script used to pin the standalone "docker-compose" script and
# ${example} shipped that pin uncommented, so on a host without it every call here died with
# "docker-compose: command not found" - the ledger never came up, and the Kafka staging this file
# owns never ran. Detecting costs one probe and removes a whole class of "it does not work on my
# machine".
#
# WHY THERE IS NO v1 FALLBACK. The standalone script is Compose v1, it is end-of-life, and it
# cannot read this repository's compose files at all: depends_on.required arrived in v2.20.0.
# Falling back to it would replace a clear "install a newer Compose" with a parse error naming a
# field, on a host that was never going to work - so the version is checked and the fallback is
# gone.
#
# PRECEDENCE, and it matters. An explicit COMPOSE_CL - exported by the caller or pinned in
# ${env}, which showenv has already sourced by the time this runs - is used verbatim and is never
# replaced: an operator who names a wrapper, a remote context or an absolute path has said
# something this function has no business second-guessing. It is still VERSION-CHECKED, which is
# a different act: the requirement belongs to the compose files rather than to the invocation, so
# it holds however Compose is reached. A wrapper whose version cannot be read is warned about and
# then trusted, because refusing there would break the very indirection the override exists for.
#
# WHY IT CAN REFUSE. With no usable Compose, every command in this script would fail anyway, one
# confusing message at a time. Saying so once, by name and with the version required, is the
# difference between a missing prerequisite and a broken script. --init and the help text are
# exempt because neither touches Compose, and an operator's first act on a fresh checkout is
# usually --init.
#
# Parameters:
#   - $1: the subcommand being run, so the Compose-free ones are not held to this requirement.
resolve_compose_cl() {
    # Named for what it is rather than "command", which would read as the shell builtin used
    # in the probe below.
    local subcommand="${1:-}"
    local found=""

    if [ -n "${COMPOSE_CL}" ]
    then
        found="$(compose_version_of "${COMPOSE_CL}")"
        if [ -z "${found}" ]
        then
            printf '%b\n' " ${YEL}COMPOSE_CL is set to '${COMPOSE_CL}', whose version could not be read.${NC}"
            printf '%b\n' " Continuing with it. This repository needs Compose ${COMPOSE_MINIMUM_VERSION} or newer:"
            printf '%b\n' " if bring-up fails on ${BLU}depends_on.required${NC}, that is the reason."

            return 0
        fi

        if ! compose_version_at_least "${found}"
        then
            refuse_compose_cl "COMPOSE_CL is set to '${COMPOSE_CL}', which is Compose ${found}."
        fi

        return 0
    fi

    if docker compose version >/dev/null 2>&1
    then
        COMPOSE_CL="docker compose"
        found="$(compose_version_of "${COMPOSE_CL}")"

        if [ -n "${found}" ] && ! compose_version_at_least "${found}"
        then
            refuse_compose_cl "The Docker Compose plugin on this host is version ${found}."
        fi

        return 0
    fi

    case "${subcommand}" in
        --init | -i | --help | -h | "" )
            # Nothing here speaks to Compose. Leave the value empty rather than refusing, so a
            # fresh checkout can still be initialised on a host where Docker is not installed
            # yet, and let the first command that needs Compose be the one that reports it.
            return 0
            ;;
    esac

    refuse_compose_cl "The Docker Compose plugin is not available on this host."
}

main() {
    # Resolve the Compose command line first: COMPOSE_CL now starts EMPTY rather than assuming
    # the legacy "docker-compose" binary, so every later compose call depends on this having
    # run. It is passed the subcommand so a Docker-less host can still be initialised.
    resolve_compose_cl "${1}"

    # Read the provisioning interface from the script that owns it, before any subcommand can
    # need it. A failure here is reported by the resolver and is not fatal: provision_kafka
    # refuses with its own message when the script is unusable, and every subcommand that does
    # not provision is unaffected.
    resolve_kafka_provision_interface || true

    case "${1}" in
        --pull | -p )
            # NO HOST-GLOBAL PRUNE BY DEFAULT (SEC-8). This arm used to begin with
            #     docker image prune -a --force --filter "until=72h"
            # which deletes EVERY unused image on the Docker host older than 72 hours — not
            # this project's images, every image reachable by this daemon, including those
            # belonging to other projects, other checkouts of this repository and anything
            # else sharing the host or a CI runner. help() documented this arm only as "Pull
            # the repo from registry", so an operator asking to pull got an unannounced
            # reclaim of the whole image cache, with no confirmation and no way to opt out;
            # on a shared or CI host what it reclaimed was somebody else's build cache, and
            # the cost reappeared as an unexplained cold rebuild somewhere unrelated.
            #
            # Reclaiming is still available, but it must be asked for by name and it says
            # what it is about to do first.
            if [[ "${STACK_PRUNE_IMAGES:-}" == "1" || "${STACK_PRUNE_IMAGES:-}" == "true" ]]; then
                printf '%b\n' " ${YEL}STACK_PRUNE_IMAGES is set: pruning ALL unused Docker images older than 72h${NC}" \
                    " This is ${RED}HOST-GLOBAL${NC} and is not limited to this project. Images belonging to" \
                    " other projects, other checkouts and other users of this Docker daemon will be" \
                    " deleted if nothing currently references them." \
                    " Unset STACK_PRUNE_IMAGES to pull without reclaiming."
                docker image prune -a --force --filter "until=72h"
            fi
            # The start-up profile set, not the teardown one: pull what this stack would run.
            # Pulling the broker image for a stack that never starts it would cost several
            # hundred megabytes for nothing.
            COMPOSE_PROFILES="$(startup_compose_profiles)" compose pull "${@:2}"
            ;;
        --up | -u )
            staged_up "${@:2}"
            ;;
        --down | -d )
            COMPOSE_PROFILES="$(teardown_compose_profiles)" compose down
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
            COMPOSE_PROFILES="$(teardown_compose_profiles)" compose down
            staged_up "${@:2}"
            ;;
        --init | -i )
    	    #checking if .env is available:
            if [ -r ${env} ]
            then
                printf "The environment file ${RED}%s${NC} is already available. If you want to start from scratch, delete it and restart.\n" "${env}"
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
                # every later edit writes a TEMPORARY FILE in the same directory and takes its
                # mode from the umask, not from the file it is replacing - so without it the
                # secrets would appear world-readable in that temporary copy even though the
                # final file was private. write_env_substitution sets umask 077 itself for the
                # same reason; this is the belt to that braces.
                printf "Creating file: ${YEL}%s${NC} with secrets... Check it before you spin up the stack.\n" "${env}"

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

                # ROUTED THROUGH set_env_value LIKE EVERY OTHER SECRET, and it used not to be:
                # this line was a bare `sed -i "s|{POSTGRES_PASSWORD}|$POSTGRES_PASSWORD|g"`,
                # which put the database password into sed's world-readable argv and depended on
                # GNU sed's in-place extension. One writer for every credential means one place
                # where that is fixed, and it already is - see write_env_substitution.
                POSTGRES_PASSWORD=$(openssl rand -base64 15)
                set_env_value "POSTGRES_PASSWORD" "$POSTGRES_PASSWORD" >/dev/null
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
                    printf "Error creating environment file ${RED}%s${NC}. Please, check if an ${BLU}.env${NC} file available, resolve and restart.\n" "${env}"
                    exit 1
                fi
            fi
            ;;
        # An explicit request for the usage banner, and a bare invocation, which is read as
        # one. Both SUCCEED: printing what was asked for is not an error, and a wrapper that
        # runs "./stack.sh --help" to check the script is present should not see a failure.
        --help | -h | "" )
            help
            ;;
        # ANYTHING ELSE IS A MISTAKE, AND IT EXITS NON-ZERO.
        #
        # This arm used to be `* ) help`, sharing the success path above, so `./stack.sh
        # --buld` printed the usage banner and returned 0. Nothing was brought up, nothing was
        # torn down, and every caller was told it had worked — a CI job or a provisioning
        # wrapper that mistyped a subcommand recorded success against a stack that had never
        # started. That is the one class of failure this script otherwise avoids everywhere:
        # `--down` with no ${env} exits 1, a broker that never becomes healthy exits non-zero,
        # and a purge without consent refuses. A typo was the sole exception.
        #
        # The argument is named back rather than only the usage printed, because the mistake is
        # usually a single transposed character and an operator reading a wall of usage text
        # does not always see which word of theirs was not understood.
        * )
            printf "\n ${RED}==> error:${NC} unrecognised argument ${YEL}%s${NC}. Nothing was started, stopped or changed.\n" "${1}"
            help
            exit 1
            ;;
    esac

}

showenv "$@"
time main "$@" # calls the main procedure and prints time used to execute
