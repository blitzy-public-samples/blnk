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
# Refuses a `kubectl apply` that would deploy an unresolved or mutable image. That is the
# whole job, and the word that matters in it is BEFORE.
#
# WHY A SEPARATE GATE RATHER THAN A BETTER PLACEHOLDER
#
# The manifests in infrastructure/k8s-manifests/ deliberately carry an unresolvable
# placeholder for the application image:
#
#     image: blnk:REPLACE_WITH_PINNED_DIGEST
#
# That placeholder is not an oversight and must not be "fixed" by putting a real tag
# there. It replaced a published `jerryenebeli/blnk:0.13.3`, which was a REAL image built
# before the event-streaming pipeline existed — so `kubectl apply` succeeded, every probe
# passed, and the cluster happily ran a binary with no relay and no /events endpoints
# beside a ConfigMap full of KAFKA_* keys it had never heard of. A deployment that fails
# is recoverable in minutes; a deployment that succeeds against the wrong binary is
# discovered when someone asks why no events arrived.
#
# But a placeholder alone only moves the failure, it does not make it early. Kubernetes
# does not validate image references at admission: it accepts the string, schedules the
# pod, and the kubelet discovers the problem when it tries to pull. The operator's
# feedback is therefore an ImagePullBackOff several seconds after a successful-looking
# apply, on a workload that is now PARTIALLY rolled out — the old ReplicaSet scaled down,
# the new one unable to start. That is the defect this script closes: it turns a pull-time
# failure into a pre-apply refusal, with the remedy named.
#
# WHAT IT CHECKS, AND WHY EACH ONE IS HERE
#
#   1. No unresolved image placeholder survives into an apply. This is the primary check.
#   2. Every image THIS CHANGE OWNS is pinned by DIGEST. A tag is a mutable pointer, so
#      two clusters applying an identical manifest weeks apart can run different bytes
#      with nothing in any diff to show for it. Digests are content-addressed and cannot.
#   3. No `:latest` anywhere. It is the maximally mutable case of (2) and worth naming
#      separately because it reads as harmless.
#
# WHY (2) IS SCOPED RATHER THAN UNIVERSAL
#
# The digest requirement is enforced as an ERROR for the images the event-streaming work
# owns and pins — the Blnk application build and the Kafka broker — and reported as a
# WARNING for the other third-party workloads in this folder (postgres, redis, typesense,
# jaeger, prometheus). Those carry tag-only references that predate this change. Promoting
# them to errors here would do one of two bad things: fail every run of this gate until an
# unrelated six-manifest change lands, or pressure whoever hits it into pinning six
# images they were not reviewing. The warning keeps the gap VISIBLE and attributable
# instead of silently accepted; raising them is a deliberate follow-up, not a side effect
# of this script existing.
#   4. The server workload and its migration init container share ONE image. Different
#      builds would migrate to one schema and serve against another. A Go contract test
#      asserts this too; it is repeated here because this is the check that runs against
#      the manifests an operator is ACTUALLY about to apply, which may be a rendered copy.
#
# HOW TO RESOLVE THE APPLICATION IMAGE
#
# Build and push this commit, resolve the digest, then render the manifests. The digest is
# what `docker push` prints, and `docker inspect` can recover it afterwards:
#
#     docker build -t "${REGISTRY}/blnk:${GIT_SHA}" .
#     docker push "${REGISTRY}/blnk:${GIT_SHA}"
#     BLNK_IMAGE="$(docker inspect --format '{{index .RepoDigests 0}}' \
#       "${REGISTRY}/blnk:${GIT_SHA}")"
#
# Then either render to a directory and apply that:
#
#     BLNK_IMAGE="${BLNK_IMAGE}" ./scripts/k8s-preflight.sh --render ./rendered
#     kubectl apply -f ./rendered
#
# or check an already-rendered tree:
#
#     ./scripts/k8s-preflight.sh ./rendered
#
# EXIT STATUS
#
#   0  every image is resolved, digest-pinned and consistent; safe to apply
#   1  at least one problem, each reported with the file, the container and the remedy
#
# This script reads manifests and, with --render, writes copies. It never contacts a
# cluster and never runs kubectl, so it is safe in CI with no kubeconfig.

set -euo pipefail

# ---------------------------------------------------------------------------------------
# Output helpers. Same shapes as scripts/kafka-bootstrap.sh so the three scripts read
# alike, including the "colour only when attached to a terminal" rule: these run in CI far
# more often than in a shell, and escape sequences in a log are noise.
# ---------------------------------------------------------------------------------------

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    RED=$'\033[0;31m'; GRN=$'\033[0;32m'; YEL=$'\033[0;33m'
    BLU=$'\033[0;34m'; NC=$'\033[0m'
else
    RED=''; GRN=''; YEL=''; BLU=''; NC=''
fi

info() { printf '%s\n' "${BLU}==>${NC} ${1}"; }
ok()   { printf '%s\n' "${GRN}==>${NC} ${1}"; }
warn() { printf '%s\n' "${YEL}==> warning:${NC} ${1}" >&2; }
die()  { printf '%s\n' "${RED}==> error:${NC} ${1}" >&2; exit 1; }

# A finding, on stderr, indented so a run with several reads as a list rather than a wall.
finding() { printf '%s\n' "${RED}  x${NC} ${1}" >&2; }
detail()  { printf '%s\n' "    ${1}" >&2; }

# ---------------------------------------------------------------------------------------
# The placeholders this repository ships. Kept as an explicit list rather than a general
# "does it look like a digest" heuristic, so that the error message can name WHICH
# placeholder was found and what it is for.
# ---------------------------------------------------------------------------------------

readonly APP_PLACEHOLDER='REPLACE_WITH_PINNED_DIGEST'
readonly COMPOSE_PLACEHOLDER='set-blnk-image-explicitly-see-env-example'

usage() {
    cat <<'USAGE'
Usage:
  scripts/k8s-preflight.sh [MANIFEST_DIR]
      Check MANIFEST_DIR (default: infrastructure/k8s-manifests) and refuse any
      unresolved placeholder, unpinned tag, :latest, or server/migrate image mismatch.

  BLNK_IMAGE=<repo@sha256:...> scripts/k8s-preflight.sh --render OUT_DIR [MANIFEST_DIR]
      Copy MANIFEST_DIR to OUT_DIR with the application-image placeholder replaced by
      BLNK_IMAGE, then check OUT_DIR. Apply OUT_DIR, not the source tree.

Options:
  -h, --help     This text.

Environment:
  BLNK_IMAGE     Digest-pinned application image. Required by --render.
  NO_COLOR       Any value disables colour.
USAGE
}

# ---------------------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------------------

RENDER_DIR=''
MANIFEST_DIR=''

while [ "$#" -gt 0 ]; do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --render)
            [ "$#" -ge 2 ] || die '--render requires an output directory.' \
                'Usage: --render OUT_DIR'
            RENDER_DIR="$2"; shift 2 ;;
        -*) die "unknown option: $1" 'Run with --help for usage.' ;;
        *)
            [ -z "${MANIFEST_DIR}" ] || die "unexpected extra argument: $1"
            MANIFEST_DIR="$1"; shift ;;
    esac
done

MANIFEST_DIR="${MANIFEST_DIR:-infrastructure/k8s-manifests}"

[ -d "${MANIFEST_DIR}" ] || die "manifest directory not found: ${MANIFEST_DIR}" \
    'Run this from the repository root, or pass the directory explicitly.'

# ---------------------------------------------------------------------------------------
# Rendering. Deliberately a COPY rather than an in-place edit: an in-place substitution
# would leave the working tree carrying a digest that must never be committed, and the
# next `git status` would invite someone to commit it.
# ---------------------------------------------------------------------------------------

if [ -n "${RENDER_DIR}" ]; then
    : "${BLNK_IMAGE:?--render requires BLNK_IMAGE to be set to a digest-pinned image reference}"

    case "${BLNK_IMAGE}" in
        *@sha256:*) : ;;
        *) die "BLNK_IMAGE is not digest-pinned: ${BLNK_IMAGE}" \
               'Expected the form repository@sha256:<64 hex>, which is what' \
               '`docker inspect --format "{{index .RepoDigests 0}}"` prints.' ;;
    esac

    mkdir -p "${RENDER_DIR}"
    # -T so a second run replaces the contents rather than nesting a directory inside.
    cp -RT "${MANIFEST_DIR}/." "${RENDER_DIR}/" 2>/dev/null \
        || cp -R "${MANIFEST_DIR}/." "${RENDER_DIR}/"

    rendered=0
    for f in "${RENDER_DIR}"/*.yaml; do
        [ -f "${f}" ] || continue
        grep -q "${APP_PLACEHOLDER}" "${f}" || continue
        # awk with the value in ENVIRON and a literal index()/substr() splice, never a
        # regex replacement: an image reference contains '/' and '@' and a digest is hex,
        # so sed's delimiter and backreference handling are both hazards here. This is the
        # same transport rule stack.sh uses for env substitution.
        tmp="$(umask 077; mktemp "${f}.XXXXXX")"
        PF_NEEDLE="${APP_PLACEHOLDER}" PF_VALUE="${BLNK_IMAGE}" awk '
            BEGIN { needle = ENVIRON["PF_NEEDLE"]; value = ENVIRON["PF_VALUE"];
                    nlen = length(needle) }
            {
                line = $0
                while ((p = index(line, needle)) > 0) {
                    line = substr(line, 1, p - 1) value substr(line, p + nlen)
                }
                print line
            }
        ' "${f}" > "${tmp}"
        mv -f "${tmp}" "${f}"
        rendered=$((rendered + 1))
    done

    # The placeholder appears as `blnk:REPLACE_...`; substituting the token alone would
    # leave the `blnk:` prefix stranded in front of a full reference. Repair that here
    # rather than complicating the splice above.
    for f in "${RENDER_DIR}"/*.yaml; do
        [ -f "${f}" ] || continue
        tmp="$(umask 077; mktemp "${f}.XXXXXX")"
        PF_VALUE="${BLNK_IMAGE}" awk '
            BEGIN { value = ENVIRON["PF_VALUE"]; stray = "blnk:" value }
            {
                line = $0
                while ((p = index(line, stray)) > 0) {
                    line = substr(line, 1, p - 1) value substr(line, p + length(stray))
                }
                print line
            }
        ' "${f}" > "${tmp}"
        mv -f "${tmp}" "${f}"
    done

    info "rendered ${rendered} manifest(s) into ${RENDER_DIR}" \
        "application image: ${BLNK_IMAGE}"
    MANIFEST_DIR="${RENDER_DIR}"
fi

# ---------------------------------------------------------------------------------------
# Image extraction.
#
# Deliberately textual rather than a YAML parse, for two reasons. It has no dependency on
# python, yq or a Go binary, so it runs in the same minimal CI image that runs shellcheck;
# and the thing being checked IS the literal text an operator is about to hand to kubectl.
# A parser would happily normalise `image: "blnk:REPLACE_WITH_PINNED_DIGEST"` into
# something a naive comparison then missed.
#
# Only `image:` keys are considered, and only outside comments — a manifest here carries
# far more commentary than YAML, and several comments legitimately mention image
# references while explaining why a pin is what it is.
# ---------------------------------------------------------------------------------------

# Prints "line<TAB>image" for every uncommented image: key in a file.
extract_images() {
    awk '
        {
            line = $0
            sub(/^[[:space:]]+/, "", line)
            if (line ~ /^#/)         next    # a comment
            if (line !~ /^-?[[:space:]]*image:[[:space:]]*/) next
            sub(/^-?[[:space:]]*image:[[:space:]]*/, "", line)
            sub(/[[:space:]]*#.*$/, "", line)             # trailing comment
            gsub(/^["'"'"']|["'"'"']$/, "", line)         # surrounding quotes
            if (line == "") next
            print NR "\t" line
        }
    ' "$1"
}

# Prints the image for one container name in one manifest, or nothing. Tracks `name:` and
# returns the first `image:` that follows the requested container.
image_for_container() {
    awk -v want="$2" '
        {
            line = $0
            sub(/^[[:space:]]+/, "", line)
            if (line ~ /^#/) next
            if (line ~ /^-?[[:space:]]*name:[[:space:]]*/) {
                v = line
                sub(/^-?[[:space:]]*name:[[:space:]]*/, "", v)
                sub(/[[:space:]]*#.*$/, "", v)
                gsub(/^["'"'"']|["'"'"']$/, "", v)
                current = v
                next
            }
            if (line ~ /^-?[[:space:]]*image:[[:space:]]*/ && current == want) {
                v = line
                sub(/^-?[[:space:]]*image:[[:space:]]*/, "", v)
                sub(/[[:space:]]*#.*$/, "", v)
                gsub(/^["'"'"']|["'"'"']$/, "", v)
                print v
                exit
            }
        }
    ' "$1"
}

# ---------------------------------------------------------------------------------------
# The checks
# ---------------------------------------------------------------------------------------

problems=0
checked_images=0
unpinned_third_party=0

info "preflight: ${MANIFEST_DIR}"

for manifest in "${MANIFEST_DIR}"/*.yaml; do
    [ -f "${manifest}" ] || continue
    base="$(basename "${manifest}")"

    while IFS=$'\t' read -r lineno image; do
        [ -n "${image:-}" ] || continue
        checked_images=$((checked_images + 1))

        # --- Check 1: unresolved placeholders -------------------------------------------
        case "${image}" in
            *"${APP_PLACEHOLDER}"*)
                problems=$((problems + 1))
                finding "${base}:${lineno} carries the UNRESOLVED application-image placeholder"
                detail "found:  ${image}"
                detail 'This placeholder is intentional in the committed tree: it stops an'
                detail 'apply from silently running a pre-implementation build. Resolve it'
                detail 'for the apply, not in git:'
                detail ''
                detail '  BLNK_IMAGE="repo@sha256:..." \'
                detail '    ./scripts/k8s-preflight.sh --render ./rendered'
                detail '  kubectl apply -f ./rendered'
                continue ;;
            *"${COMPOSE_PLACEHOLDER}"*)
                problems=$((problems + 1))
                finding "${base}:${lineno} carries the Compose image placeholder"
                detail "found:  ${image}"
                detail 'Set BLNK_IMAGE to a digest-pinned reference. See .env.example.'
                continue ;;
        esac

        # A manifest may legitimately template its image from a variable; that is resolved
        # by whatever renders it, and this script cannot judge the result.
        case "${image}" in
            *'${'*|*'$('*)
                warn "${base}:${lineno} templates its image (${image}); not checkable here"
                continue ;;
        esac

        # --- Check 3: :latest ------------------------------------------------------------
        case "${image}" in
            *:latest|*:latest@*)
                problems=$((problems + 1))
                finding "${base}:${lineno} uses the :latest tag"
                detail "found:  ${image}"
                detail 'latest is the maximally mutable reference: the same manifest means'
                detail 'different bytes on every pull. Pin a release AND its digest.'
                continue ;;
        esac

        # --- Check 2: digest pinning ------------------------------------------------------
        case "${image}" in
            *@sha256:*) : ;;
            *)
                # Owned by this change, and therefore an error: the Blnk application build
                # and the Kafka broker. Everything else is a pre-existing tag-only
                # reference and is reported without failing the gate — see the header.
                owned='no'
                case "${image}" in
                    apache/kafka*|blnk*|*/blnk*) owned='yes' ;;
                esac
                case "${base}" in
                    server-deployment.yaml|worker-deployment.yaml) owned='yes' ;;
                esac

                if [ "${owned}" = 'yes' ]; then
                    problems=$((problems + 1))
                    finding "${base}:${lineno} is not pinned by digest"
                    detail "found:  ${image}"
                    detail 'A tag is a mutable pointer, so two clusters applying this same file'
                    detail 'weeks apart can run different bytes with no diff to show for it.'
                    detail 'Resolve and append the digest:'
                    detail ''
                    detail "  docker pull ${image}"
                    detail "  docker inspect --format '{{index .RepoDigests 0}}' ${image}"
                else
                    unpinned_third_party=$((unpinned_third_party + 1))
                    warn "${base}:${lineno} third-party image is tag-only: ${image} (pre-existing; not failing the gate)"
                fi
                continue ;;
        esac
    done < <(extract_images "${manifest}")
done

# --- Check 4: the server and its migration init container share one image -----------------
#
# Checked separately because it is a relationship between two images rather than a property
# of one. Different builds here migrate to one schema and serve against another, which
# presents as arbitrary runtime errors rather than as a failed deploy.
server_manifest="${MANIFEST_DIR}/server-deployment.yaml"
if [ -f "${server_manifest}" ]; then
    server_image="$(image_for_container "${server_manifest}" 'server' || true)"
    migrate_image="$(image_for_container "${server_manifest}" 'migrate' || true)"
    if [ -n "${server_image}" ] && [ -n "${migrate_image}" ]; then
        if [ "${server_image}" != "${migrate_image}" ]; then
            problems=$((problems + 1))
            finding 'server-deployment.yaml: the server and migrate images DIFFER'
            detail "server:   ${server_image}"
            detail "migrate:  ${migrate_image}"
            detail 'The init container migrates the schema the server then serves against.'
            detail 'Two builds would migrate to one schema and serve another.'
        fi
    fi
fi

# ---------------------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------------------

if [ "${problems}" -gt 0 ]; then
    printf '%s\n' '' >&2
    die "preflight FAILED: ${problems} problem(s) across ${checked_images} image reference(s)." \
        'Nothing was applied. Fix the findings above and re-run.'
fi

ok "preflight passed: ${checked_images} image reference(s) checked; every owned image is resolved and digest-pinned."
if [ "${unpinned_third_party}" -gt 0 ]; then
    warn "${unpinned_third_party} third-party image(s) are tag-only rather than digest-pinned. Pre-existing, and outside the error scope of this gate - see the header note."
fi
if [ -n "${RENDER_DIR}" ]; then
    info "apply the RENDERED tree, not the source tree:" "  kubectl apply -f ${RENDER_DIR}"
fi
