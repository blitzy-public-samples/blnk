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
# Resolves every EXTERNAL link in every tracked Markdown file and fails if one is dead.
#
# WHY THIS EXISTS, AND WHY IT RUNS ON A SCHEDULE RATHER THAN ONLY ON A PULL REQUEST
#
# Two links in README.md pointed at documentation pages that had been retired:
#
#     https://docs.blnkfinance.com/api-reference
#     https://docs.blnkfinance.com/tutorials/quick-start/create-your-first-ledger-balance-and-transaction
#
# Both returned 404. Neither had been touched in the commit that broke them, because the
# commit that broke them was not in this repository at all — the documentation site moved
# its own pages. That is the whole reason this check is scheduled: link rot here is caused
# by a THIRD PARTY changing, so a check that only runs when somebody edits a Markdown file
# can never see it. The first reader to find it is otherwise a prospective user following
# the Quick start, which is the worst possible place to be wrong.
#
# The complementary half of this check is hermetic and lives in the Go test suite
# (TestDocumentationLinks_*, event_docs_links_test.go): repository-relative links and
# section anchors are resolved offline, on every single test run, with no network. Split
# that way deliberately — the part that can be checked without a network is checked
# ALWAYS, and only genuinely external resolution is left to this script.
#
# WHAT COUNTS AS A FAILURE, AND WHAT DELIBERATELY DOES NOT
#
# A dead link is a permanent, host-answered "this is not here": 404 and 410. Those fail.
#
# Everything else a real network does is NOT evidence that a link is dead, and treating it
# as such produces a check nobody trusts and everybody learns to re-run until it passes:
#
#   429  rate limited. The link is fine; we asked too fast. Retried, then accepted.
#   403  many sites refuse unattended clients outright. Not a statement about the URL.
#   999  LinkedIn's non-standard refusal. Same.
#   000  curl could not complete the request at all — DNS, TLS, timeout. That is OUR
#        network failing, not their page. Retried, then reported as UNVERIFIED.
#
# UNVERIFIED links are printed, and they do not fail the run. This is the deliberate
# trade: a scheduled job that fails on somebody else's flaky TLS handshake trains the team
# to ignore it, and an ignored check is worth strictly less than no check, because it also
# costs attention. A 404 is unambiguous and is the thing that actually shipped broken.
#
# Usage:
#   scripts/check-doc-links.sh              # every tracked Markdown file
#   scripts/check-doc-links.sh README.md    # only the files named
#
# Exit status: 0 when no link is dead, 1 when at least one is.

set -euo pipefail

# Retries per URL before a verdict is recorded. Anything transient gets three chances.
readonly ATTEMPTS=3

# Per-request ceiling. Documentation hosts are CDN-backed; 25s is generous.
readonly TIMEOUT=25

# A browser-shaped User-Agent. Unattended clients are refused by enough hosts that the
# default curl agent turns real pages into 403s, which is noise, not signal.
readonly AGENT='Mozilla/5.0 (compatible; blnk-doc-link-check/1.0; +https://github.com/blnkfinance/blnk)'

# ---------------------------------------------------------------------------
# Collect the files under check
# ---------------------------------------------------------------------------

cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"

declare -a FILES=()
if [[ $# -gt 0 ]]; then
	FILES=("$@")
else
	# Tracked files only. An untracked scratch note is not part of the published
	# documentation and must not be able to fail the build.
	while IFS= read -r f; do FILES+=("$f"); done < <(git ls-files '*.md')
fi

if [[ ${#FILES[@]} -eq 0 ]]; then
	echo "check-doc-links: no Markdown files to check" >&2
	exit 0
fi

# ---------------------------------------------------------------------------
# Extract the external links
#
# Both Markdown forms carry links — inline ](https://…) and bare autolinks — so the URL
# is matched directly rather than the syntax around it. Trailing punctuation that belongs
# to the SENTENCE and not to the URL is stripped, as are the closing delimiters of the
# constructs a URL sits inside.
# ---------------------------------------------------------------------------

extract_links() {
	grep -ohE 'https?://[^][)"'"'"'`<>{} ]+' "${FILES[@]}" 2>/dev/null |
		sed -E 's/[.,;:!?]+$//' |
		sort -u |
		grep -vEf <(printf '%s\n' "${NOT_A_LINK[@]}")
}

# Hosts that are ILLUSTRATIONS, not links. Documentation is full of URLs that are meant to
# be read and never fetched: the local development server, the in-cluster service names the
# Compose and Kubernetes stacks use, and the domains RFC 2606 and RFC 6761 reserve for
# exactly this purpose. Probing them is guaranteed to fail, which would bury the one
# genuinely dead link among a dozen meaningless ones — the failure mode that makes a link
# checker useless. They are excluded by design rather than tolerated as noise.
readonly NOT_A_LINK=(
	'^https?://localhost([:/]|$)'
	'^https?://127\.0\.0\.1([:/]|$)'
	'^https?://0\.0\.0\.0([:/]|$)'
	'^https?://\[::1\]([:/]|$)'
	'^https?://[^/]*\.example([:/]|$)'      # RFC 6761 reserved TLD
	'^https?://[^/]*example\.(com|net|org)([:/]|$)' # RFC 2606 reserved second level
	'^https?://[^/]*\.internal([:/]|$)'
	'^https?://[^/]*\.local([:/]|$)'
	'^https?://[^/]*\.svc(\.|[:/]|$)'   # Kubernetes cluster DNS
	'^https?://(blnk-)?(server|worker|kafka|postgres|redis|typesense|jaeger|prometheus)([-.:/]|$)'
	'@' # a credential-bearing example URL is never fetched, on purpose
)

declare -a LINKS=()
while IFS= read -r u; do [[ -n "$u" ]] && LINKS+=("$u"); done < <(extract_links)

echo "check-doc-links: ${#LINKS[@]} unique external link(s) across ${#FILES[@]} file(s)"
echo

# ---------------------------------------------------------------------------
# Resolve each one
#
# HEAD first because it is cheap, then GET on anything unsatisfying: a surprising number
# of documentation hosts answer HEAD with 403 or 405 while serving GET perfectly. Falling
# back keeps those from being reported as failures they are not.
# ---------------------------------------------------------------------------

probe() {
	local url="$1" code

	# curl writes %{http_code} even when it exits non-zero, so a `|| echo 000` fallback
	# CONCATENATES onto whatever it already wrote — turning a real "404" into "404000",
	# which then matches no status pattern and gets misreported as merely unverified. The
	# failure is normalised here instead: take curl's own output, and substitute 000 only
	# when it produced nothing usable.
	code=$(curl -sS -o /dev/null -w '%{http_code}' -I -L \
		--max-time "${TIMEOUT}" -A "${AGENT}" "${url}" 2>/dev/null) || true
	[[ "${code}" =~ ^[0-9]{3}$ ]] || code=000

	# HEAD is cheap, but a surprising number of documentation hosts answer it with 403 or
	# 405 while serving GET perfectly. Fall back rather than report a failure that is not.
	case "${code}" in
	2?? | 3??) ;;
	*)
		code=$(curl -sS -o /dev/null -w '%{http_code}' -L \
			--max-time "${TIMEOUT}" -A "${AGENT}" "${url}" 2>/dev/null) || true
		[[ "${code}" =~ ^[0-9]{3}$ ]] || code=000
		;;
	esac

	printf '%s' "${code}"
}

declare -a DEAD=()
declare -a UNVERIFIED=()
ok=0

for url in "${LINKS[@]}"; do
	code=000
	for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
		code=$(probe "${url}")
		case "${code}" in
		404 | 410) break ;;                                    # settled, and bad
		2?? | 3??) break ;;                                    # settled, and good
		*) [[ ${attempt} -lt ${ATTEMPTS} ]] && sleep "${attempt}" ;;
		esac
	done

	case "${code}" in
	2?? | 3??)
		ok=$((ok + 1))
		printf '  %-4s %s\n' "${code}" "${url}"
		;;
	404 | 410)
		DEAD+=("${code} ${url}")
		printf '  %-4s %s   <-- DEAD\n' "${code}" "${url}"
		;;
	*)
		UNVERIFIED+=("${code} ${url}")
		printf '  %-4s %s   <-- unverified (not a failure)\n' "${code}" "${url}"
		;;
	esac
done

# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------

echo
echo "check-doc-links: ${ok} reachable, ${#DEAD[@]} dead, ${#UNVERIFIED[@]} unverified"

if [[ ${#UNVERIFIED[@]} -gt 0 ]]; then
	echo
	echo "Unverified — the host did not give a usable answer in ${ATTEMPTS} attempts. These do"
	echo "not fail the run; a rate limit or a refused unattended client is not a dead link."
	printf '  %s\n' "${UNVERIFIED[@]}"
fi

if [[ ${#DEAD[@]} -gt 0 ]]; then
	echo
	echo "FAILED. The following link(s) are permanently gone (404/410) and are published in"
	echo "this repository's Markdown. Replace each with a live URL — and confirm the"
	echo "replacement actually contains what the link text promises, rather than only that"
	echo "it returns 200:"
	printf '  %s\n' "${DEAD[@]}"
	exit 1
fi

echo
echo "check-doc-links: OK — no dead links."
