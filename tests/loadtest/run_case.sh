#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <case-name> [queue-mode]"
  echo "cases: hot-to-cold-no-shard | cold-to-hot-no-shard | hot-to-cold-shard | cold-to-hot-shard"
  echo "       events | event-streaming  (the event-streaming acceptance run: V-1 throughput"
  echo "                and p99, V-3 dead-letter rate. Both names run the same case;"
  echo "                'event-streaming' is canonical and names the artifacts)"
  echo "queue-mode: normal | spread | hot (default: normal, transaction cases only)"
  echo
  echo "environment, event case:"
  echo "  RAW_OUTPUT=1     also write the raw k6 NDJSON stream (OFF by default: a 30-minute"
  echo "                   acceptance run at 500/s writes millions of records and gigabytes"
  echo "                   of generator I/O, which can contaminate the very verdict it is"
  echo "                   meant to evidence). Give NDJSON_OUT a .gz path to compress it."
  echo "  NDJSON_OUT=PATH  raw stream destination; setting it implies RAW_OUTPUT=1"
  echo "  SUMMARY_OUT=PATH acceptance artifact (default tests/loadtest/summary-event-streaming.json)"
  echo
  echo "credentials are read from the environment and are NEVER placed on a command line:"
  echo "  BLNK_METRICS_BEARER_TOKEN | METRICS_BEARER_TOKEN   guards /metrics"
  echo "  BLNK_SERVER_SECRET_KEY | MASTER_KEY                reads GET /events/stats"
  echo "  API_KEY | BLNK_API_KEY                             posts the load"
  echo "  \`set -a; . ./.env; set +a\` exports all three."
  exit 1
fi

CASE_NAME="$1"
QUEUE_MODE="${2:-normal}"

# ---------------------------------------------------------------------------
# require_http_url / display_url — validate BEFORE reporting, report LESS than you were given
#
# Two separate jobs, and they are separate because doing only one of them is what went wrong.
#
# VALIDATE FIRST. The runner used to echo URL and METRICS_URL and only afterwards let k6 or
# events.js discover that one of them was unusable. So the first thing an operator saw was the
# malformed value presented as the endpoint under test, and the actual complaint arrived later
# from a different program in a different vocabulary. Refusing here, before anything is printed,
# means the run names its own bad input.
#
# REPORT A SANITISED FORM. A URL is a place a credential hides in plain sight: userinfo
# (http://user:token@host/), a query (?api_key=...), a fragment. This runner's whole reason for
# not putting secrets in argv is that argv is world-readable through /proc — and echoing a URL
# with userinfo into stdout is worse, because stdout becomes a CI log that is retained,
# searchable and often public. display_url therefore prints scheme, host, port and path and
# NOTHING else: no userinfo, no query, no fragment. The value k6 receives is unchanged; only
# what is written to the terminal is narrowed.
# ---------------------------------------------------------------------------

# require_http_url refuses anything that is not an absolute http(s) URL with a host.
#
# $1 the variable NAME, used in the message so the operator knows which one to fix.
# $2 the value.
require_http_url() {
  local name="$1" value="$2"

  if [[ ! "${value}" =~ ^https?://[^/?#[:space:]]+ ]]; then
    echo "error: ${name} must be an absolute http:// or https:// URL with a host."
    echo "       Got a value that is not one. It is not echoed here, because a malformed URL is"
    echo "       exactly the kind that carries a pasted credential."
    echo "       Example: ${name}=http://localhost:5001/transactions"
    exit 1
  fi
}

# display_url renders a URL for human eyes: scheme://host[:port][path], nothing more.
#
# The transformation is deliberately crude and total. It strips the fragment, then the query,
# then any userinfo up to and including the last '@' in the authority — in that order, so a
# credential in the query cannot survive by containing an '@'. Whatever is left is safe to
# print: a host and a path are not secrets, and reporting them is the point of the line.
display_url() {
  local value="$1" scheme rest authority path

  value="${value%%#*}"
  value="${value%%\?*}"

  scheme="${value%%://*}"
  rest="${value#*://}"

  authority="${rest%%/*}"
  if [[ "${rest}" == */* ]]; then
    path="/${rest#*/}"
  else
    path=""
  fi

  # Userinfo is everything before the LAST '@' in the authority. Using the last one rather
  # than the first is what handles a password containing an '@', which is common enough to
  # matter and would otherwise leak the tail of the credential as part of the "host".
  if [[ "${authority}" == *@* ]]; then
    authority="[redacted-userinfo]@${authority##*@}"
  fi

  printf '%s://%s%s' "${scheme}" "${authority}" "${path}"
}

# Hoisted above the event-streaming branch, because that branch derives METRICS_URL from it. It
# used to sit between two copies of that branch, so the first copy could not see it and the second
# could — which is how one of them echoed a metrics endpoint different from the one k6 received.
URL="${URL:-http://localhost:5001/transactions}"

# ---------------------------------------------------------------------------
# URL hygiene, in the SHELL, before anything is printed or executed.
# ---------------------------------------------------------------------------
#
# events.js refuses a credential-bearing URL during init and redacts every URL it writes into the
# summary. That protects the artefact and it does NOT protect this script's own output: the lines
# below echo the endpoints they are about to use, and they run BEFORE k6 starts — so a URL
# carrying userinfo, a query token or a fragment was printed verbatim to the terminal and into
# whatever CI log captures it, and the derivation-failure branch printed it a second time in its
# error message. A token in a CI log is a leaked token whatever the callee does afterwards.
#
# So the same two rules live here, deliberately duplicated in shell rather than deferred: redact
# what is printed, and REFUSE what would be sent. The semantics mirror redactURL/
# urlCarriesCredential in events.js exactly — drop the fragment, then the query, then any userinfo
# between "://" and the first "/" — so the runner and the scenario cannot disagree about what
# counts as a credential in a URL.

# redact_url prints a URL with every credential-bearing component removed, leaving a marker.
redact_url() {
  local url="$1"
  local redacted=0
  local scheme rest authority path_part

  [[ -z "${url}" ]] && return 0

  # Fragment first, then query: a fragment can contain a "?" and stripping in the other order
  # would leave it behind.
  if [[ "${url}" == *"#"* ]]; then
    url="${url%%#*}"
    redacted=1
  fi
  if [[ "${url}" == *"?"* ]]; then
    url="${url%%\?*}"
    redacted=1
  fi

  # Userinfo sits between "://" and the FIRST "/" after it, so a "@" later in the path is not
  # mistaken for a credential separator.
  if [[ "${url}" == *"://"* ]]; then
    scheme="${url%%://*}"
    rest="${url#*://}"
    if [[ "${rest}" == */* ]]; then
      authority="${rest%%/*}"
      path_part="/${rest#*/}"
    else
      authority="${rest}"
      path_part=""
    fi
    if [[ "${authority}" == *"@"* ]]; then
      # The LAST "@" wins, matching the scenario's lastIndexOf: a password may itself contain one.
      authority="${authority##*@}"
      redacted=1
    fi
    url="${scheme}://${authority}${path_part}"
  fi

  if [[ "${redacted}" -eq 1 ]]; then
    printf '%s (redacted)' "${url}"
  else
    printf '%s' "${url}"
  fi
}

# url_carries_credential succeeds when a URL has a component this runner refuses to accept.
url_carries_credential() {
  local url="$1"

  [[ -z "${url}" ]] && return 1
  [[ "$(redact_url "${url}")" == *" (redacted)" ]]
}

# refuse_credential_bearing_url exits rather than sending or printing a credential.
#
# Refusal rather than redaction, for the reason events.js gives: redaction protects the output,
# while a credential in a URL is also sent on the wire in a way the operator did not intend and
# reaches places neither file controls — a proxy access log, an HTTP client error, a k6 tag.
refuse_credential_bearing_url() {
  local name="$1"
  local value="$2"

  if url_carries_credential "${value}"; then
    echo "error: ${name} carries userinfo, a query string or a fragment."
    echo "       This runner refuses it rather than redacting it: such a URL is sent on the wire,"
    echo "       written into summary artifacts and printed into CI logs. Supply the endpoint"
    echo "       without credentials and authenticate with API_KEY, MASTER_KEY or"
    echo "       METRICS_BEARER_TOKEN, which travel as headers and are never printed."
    echo "       Received: $(redact_url "${value}")"
    exit 1
  fi
}

# require_clean_url is the event-streaming branch's spelling of the same refusal, kept as a
# one-line delegation rather than a second implementation: two independently written guards are
# free to drift into two different notions of a "clean" URL, and the branch below depends on this
# one running before anything is printed.
require_clean_url() {
  refuse_credential_bearing_url "$1" "$2"
}

# ---------------------------------------------------------------------------
# The event-streaming case
# ---------------------------------------------------------------------------
#
# Dispatched BEFORE the transaction defaults below are applied, and that ordering is the
# whole point of putting it here rather than in the case statement further down.
#
# The transaction cases default to RATE=300 for DURATION=30s. Acceptance criterion V-1 is
# stated at 500 events per second sustained for thirty minutes, and V-3's dead-letter rate is
# stated over the same window. Reaching events.js through the defaults above would therefore
# have produced a summary that reported PASS on all three verdicts after thirty seconds at
# 300/s — the run's own load parameters silently substituted for the criterion's, which is
# the same class of defect as certifying the latency target from the wrong histogram.
#
# So this case forwards RATE, DURATION, VUS and MAX_VUS ONLY when the caller set them, and
# otherwise lets events.js apply its own defaults, which ARE the criterion's figures. An
# operator who wants a two-minute smoke run passes DURATION=2m explicitly and can see in the
# artifact that they did.
#
# `events` IS AN ALIAS, NORMALISED ONTO THE ONE CASE NAME rather than dispatched by a
# second branch. Both names were once implemented as their own branch, forwarding a different set
# of variables and spelling the master key differently — one passed MASTER_KEY, the other
# BLNK_MASTER_KEY, and events.js accepts either, so both appeared to work while forwarding
# different things. An operator who read one document got one behaviour and an operator who read
# the other got the other. Normalising the name keeps ONE dispatch point, so there is exactly one
# set of forwarded variables no matter which name the operator typed.
#
# CONTRACT-m01: the substitution is now REPORTED rather than silent. REQUESTED_CASE_NAME keeps
# what the operator typed so the banner can echo both names and the artifact stem they resolve
# to. The finding was that one spelling is normalised away and the operator could not see which
# case actually ran or which files to open — the normalisation itself is right, and undoing it
# would restore the two-divergent-branches defect described above, so the substitution is
# reported instead of removed.
#
# `event-streaming` IS THE CANONICAL NAME AND THE ARTEFACT STEM, and that direction was settled
# from the code rather than chosen: events.js's own SUMMARY_OUT default is
# `tests/loadtest/summary-event-streaming.json`, which is what a bare
# `k6 run tests/loadtest/events.js` writes with no runner involved at all, and the README
# documents `summary-event-streaming.json` and `run-event-streaming.ndjson` throughout.
#
# THAT PAIR OF FILENAMES IS A FROZEN INTERFACE between parties that never see each other's
# source: the acceptance record cites the case name, CI collects the two files BY NAME, the
# dashboard reads them, and events.js writes its own provenance block into one of them. A run
# that writes a renamed pair fails no build and no assertion — it produces perfectly correct
# numbers under filenames nothing else collects, which is the quietest possible way to lose an
# acceptance result. So the alias resolves ONTO the frozen name, never away from it, and the two
# defaults below are spelled out literally rather than derived from a variable so that no future
# edit to the case name can rename the artefacts as a side effect. .gitignore matches them by
# glob, so it is stem-agnostic.
REQUESTED_CASE_NAME="${CASE_NAME}"
if [[ "${CASE_NAME}" == "events" ]]; then
  CASE_NAME="event-streaming"
fi

if [[ "${CASE_NAME}" == "event-streaming" ]]; then
  EVENTS_OUT_DIR="tests/loadtest"
  EVENTS_SUMMARY_OUT="${SUMMARY_OUT:-${EVENTS_OUT_DIR}/summary-event-streaming.json}"
  EVENTS_NDJSON_OUT="${NDJSON_OUT:-${EVENTS_OUT_DIR}/run-event-streaming.ndjson}"
  # THE TEMPORARY DESTINATIONS, defined beside the real ones so every writer below agrees on
  # them. k6 writes here and the paths are promoted only after a zero exit; see the artifact
  # lifecycle note further down.
  events_run_id="$$-$(date +%s)"
  EVENTS_SUMMARY_TMP="${EVENTS_SUMMARY_OUT}.partial.${events_run_id}"
  EVENTS_NDJSON_TMP="${EVENTS_NDJSON_OUT}.partial.${events_run_id}"

  # THIS ARRAY CARRIES NO CREDENTIAL VALUE, and that is a deliberate property to preserve
  # (SEC-M13). Every value appended to it is non-sensitive — endpoints, the scenario name, load
  # shape, fixture and gate settings — and it is the ONLY thing handed to k6 on the command line.
  #
  # THE THREE CREDENTIALS TRAVEL IN THE ENVIRONMENT INSTEAD, and the difference is not cosmetic.
  # `-e NAME=value` becomes an argv element of the k6 process. /proc/<pid>/cmdline is mode 0444 on
  # Linux, so ANY user on the host can read it while k6 runs; it is what `ps`, a container
  # runtime's process view, auditd's execve records and many CI log collectors display, and those
  # logs outlive the run. /proc/<pid>/environ is mode 0400 and readable only by the process owner.
  # k6 runs with --include-system-env-vars enabled by default, so an exported variable arrives in
  # __ENV exactly as an -e one would: the transport changes, nothing else does.
  #
  # Everything in this array is safe to see in `ps`, and several entries are actively USEFUL
  # there: the metrics endpoint, the target URL and the pinned scenario are what an operator
  # diagnosing a wrong-looking run needs to confirm. Adding a credential here would trade that
  # transparency for an exposure. So: do not echo the credentials, do not `set -x` around the
  # invocation, and do not add any of them back to events_args.
  events_args=()

  # --- Credential-bearing endpoints are refused BEFORE anything is printed -----------------
  #
  # events.js performs this same refusal during init, and that is one step too late for the
  # runner: the messages below print URL and METRICS_URL, so a URL carrying userinfo or a query
  # token reached the terminal, the CI log and the scrollback before the scenario rejected it.
  # The check therefore happens here, first, and every print goes through redact_url.
  #
  # The refusal, the redaction and the credential predicate are the hoisted helpers above — one
  # implementation, so the runner cannot end up with two notions of what counts as a credential
  # in a URL. require_clean_url is this branch's spelling of refuse_credential_bearing_url and
  # delegates to it for exactly that reason.
  require_clean_url URL "${URL}"
  require_clean_url METRICS_URL "${METRICS_URL:-}"

  # Captured before the derivation below can fill it in, so the announcement can tell an operator
  # which of the two endpoints they actually chose.
  METRICS_URL_SUPPLIED="${METRICS_URL:-}"

  # The verdicts are computed from Blnk's own /metrics, scraped before and after the run, so
  # the bearer token is not optional when metrics are protected. The master key is what reads
  # GET /events/stats — which is now REQUIRED rather than merely corroborating, because it is the
  # only source that reports the whole unsettled outbox population the settling gates wait on
  # (pending, processing, failed-awaiting-its-dead-letter-write, replaying). Both are taken from
  # the same BLNK_* names ./.env already exports, so a sourced environment needs no extra flags.
  #
  # METRICS_URL is derived from URL when it was not given, so a run against a non-default host
  # needs one variable rather than two, and the value the runner ECHOES below is the value k6
  # receives. THIS DERIVATION IS NOT REDUNDANT WITH events.js: that file defaults METRICS_URL to
  # the literal http://localhost:5001/metrics and derives only EVENTS_STATS_URL, LEDGERS_URL and
  # BALANCES_URL. Remove it and a run against a non-default URL measures localhost instead —
  # a clean set of verdicts for a deployment nobody asked about.
  #
  # The substitution replaces the /transactions path with /metrics, which is precisely what
  # events.js's own siblingURL(URL, "/transactions", ...) does for /ledgers and /balances, so the
  # runner and the scenario agree on how a sibling endpoint is spelled. It is deliberately NOT
  # `dirname`: dirname is not URL-aware and answers `http:` for a URL with no path, so
  # URL=http://localhost:5001 yielded METRICS_URL=http:/metrics, every scrape failed, and the run
  # reported withheld verdicts rather than naming the malformed endpoint. A URL that cannot be
  # transformed is refused here instead of guessed at, because the guess is not visibly wrong.
  #
  # A CREDENTIAL-BEARING URL IS REFUSED BEFORE IT IS PRINTED, both for the value the caller gave
  # and for the one derived from it. The refusal is first because the diagnostics below — the
  # derivation failure included — echo these endpoints, and events.js's own refusal happens later,
  # inside a process this script has already logged for.
  refuse_credential_bearing_url "URL" "${URL}"
  refuse_credential_bearing_url "METRICS_URL" "${METRICS_URL:-}"

  # VALIDATED BEFORE IT IS REPORTED OR TRANSFORMED. A value that is not an absolute http(s) URL
  # with a host cannot yield a usable metrics sibling either, and letting it through produced the
  # worst available outcome: the endpoint was printed as the one under test and the real complaint
  # arrived later, from k6, in a different vocabulary. require_http_url never echoes the value it
  # rejected, because a malformed URL is exactly the kind that carries a pasted credential.
  require_http_url "URL" "${URL}"

  if [[ -z "${METRICS_URL:-}" ]]; then
    if [[ "${URL}" == */transactions* ]]; then
      METRICS_URL="${URL%/transactions*}/metrics"
    else
      echo "error: METRICS_URL cannot be derived from URL=$(redact_url "${URL}")"
      echo "       URL is expected to contain the /transactions path. Either correct it, or name"
      echo "       the metrics endpoint directly, for example:"
      echo "       METRICS_URL=http://localhost:5001/metrics \\"
      echo "         bash tests/loadtest/run_case.sh event-streaming"
      exit 1
    fi
  fi
  require_http_url "METRICS_URL" "${METRICS_URL}"
  # The DERIVED endpoint is guarded on its own terms as well. It is a new string, built from URL
  # by substitution, and a transformation is where a component that was tolerable in the input can
  # end up somewhere it is not.
  refuse_credential_bearing_url "METRICS_URL" "${METRICS_URL}"
  # AND THE DERIVATION IS ANNOUNCED. It used to be silent, so a run against a non-default host
  # measured an endpoint the operator never named and had no way to notice. Both URLs are rendered
  # in their narrowest safe form — scheme, host, port and path, no userinfo, query or fragment.
  if [[ -z "${METRICS_URL_SUPPLIED}" ]]; then
    echo "note: METRICS_URL was not given; derived $(display_url "${METRICS_URL}")"
    echo "      from $(display_url "${URL}")"
  fi
  events_args+=(-e "METRICS_URL=${METRICS_URL}")

  # The metrics endpoint is guarded by MetricsAuthHandler whenever server.secure is true, and an
  # unauthenticated scrape then collects NOTHING — which does not fail loudly. It produces a run
  # whose verdicts are all withheld and whose degraded reason is a scrape that did not succeed.
  if [[ -z "${BLNK_METRICS_BEARER_TOKEN:-${METRICS_BEARER_TOKEN:-}}" ]]; then
    echo "note: no METRICS_BEARER_TOKEN or BLNK_METRICS_BEARER_TOKEN is set."
    echo "      If this deployment has server.secure enabled, every /metrics scrape will be"
    echo "      refused and the run will report withheld verdicts rather than a pass or a fail."
  fi
  if [[ -z "${BLNK_SERVER_SECRET_KEY:-${MASTER_KEY:-}}" ]]; then
    echo "note: no MASTER_KEY or BLNK_SERVER_SECRET_KEY is set."
    echo "      GET /events/stats is master-key gated and it is the ONLY source that reports every"
    echo "      unsettled outbox state. Without it the settling gates fall back to"
    echo "      blnk_outbox_pending, which is pending plus processing only and cannot see a failed"
    echo "      row awaiting its dead-letter write or a replay in flight — so the gates report an"
    echo "      incomplete population and the run withholds all three verdicts rather than"
    echo "      certifying a window whose failing tail it never observed."
  fi

  # EXPORTED, NOT PASSED AS ARGUMENTS. See the note on events_args above: argv is world-readable
  # through /proc/<pid>/cmdline, the environment is not. The names are the ones events.js reads, so
  # the BLNK_*-prefixed spellings ./.env ships are translated here rather than in the scenario.
  #
  # Each is exported only when a value exists: exporting an empty METRICS_BEARER_TOKEN would
  # override the scenario's own resolution with nothing, which is the same defect the old
  # `-e NAME=` guard existed to avoid.
  #
  # CREDENTIALS TRAVEL BY ENVIRONMENT, NEVER BY ARGV (SEC-M13).
  #
  # These three used to be appended to events_args as `-e NAME=value`, which put the metrics
  # bearer token, the master key and the API key into k6's command line. That is an exposure
  # rather than a style question, and the asymmetry is measurable on this host:
  #
  #   /proc/<pid>/cmdline   -r--r--r--   any user can read it, for as long as k6 runs
  #   /proc/<pid>/environ   -r--------   only the process owner can
  #
  # On top of the local read, argv is what `ps` prints, what a CI runner echoes when it reports
  # the command it spawned, and what a shell history or a crash trace preserves after the run is
  # over. A credential in argv therefore leaks in several directions at once, none of which
  # requires the leak to be noticed to have happened.
  #
  # k6 passes the real system environment into __ENV by default, so an exported variable arrives
  # exactly as `-e` delivered it — the file already relied on this for SCENARIO, and the
  # invocation below now makes it EXPLICIT rather than ambient (see the flag there).
  #
  # THE RENAMES ARE LOAD-BEARING, not cosmetic. events.js reads:
  #   METRICS_BEARER_TOKEN            and NOT BLNK_METRICS_BEARER_TOKEN
  #   MASTER_KEY or BLNK_MASTER_KEY   and NOT BLNK_SERVER_SECRET_KEY
  #   API_KEY or BLNK_API_KEY
  # so the first two must be re-exported under the name the scenario looks for. Exporting only
  # the BLNK_* names ./.env ships would leave the scenario unable to find either: the metrics
  # scrape would be refused and every verdict withheld, and GET /events/stats would answer 401.
  #
  # THE EXPORTS THEMSELVES SIT IN THE SUBSHELL THAT INVOKES k6, further down, so the credentials
  # exist in the environment of exactly one process tree and not in this script's own environment
  # for anything that might run after it. They are STILL CONDITIONAL: exporting an EMPTY value
  # overrides the scenario's own resolution with nothing, so an unset credential must stay unset
  # rather than become an empty string.
  #
  # WARNED ABOUT RATHER THAN GUESSED AT. The load POSTs transactions and provisions ledgers and
  # balances, so without a key every one of those is refused — and the failure surfaces as an
  # abort inside setup() complaining about the partition-key spread, several layers away from the
  # 401 that caused it. The note names the cause up front.
  #
  # ./.env ships the MASTER key as BLNK_SERVER_SECRET_KEY and no API key at all, so a run that
  # sources it is exactly this case. The master key is deliberately NOT borrowed here: handing the
  # load generator a superuser credential implicitly is a decision the operator should make out
  # loud, and it is spelled out in tests/loadtest/README.md for a local stack.
  if [[ -z "${API_KEY:-${BLNK_API_KEY:-}}" ]]; then
    echo "note: no API_KEY or BLNK_API_KEY is set."
    echo "      If this deployment requires authentication, every POST /ledgers, POST /balances"
    echo "      and POST /transactions will be refused with 401 — which surfaces as the run"
    echo "      aborting in setup() over the partition-key spread rather than as an auth error."
    echo "      On a local stack: API_KEY=\"\$BLNK_SERVER_SECRET_KEY\""
  fi

  events_args+=(-e "URL=${URL}")

  # The scenario identifier is PINNED, not inherited — the same rule the transaction arms below
  # follow, where the case name decides the scenario and any ambient value is overwritten.
  #
  # It matters here because k6 runs with --include-system-env-vars enabled by default, so every
  # variable exported in the caller's shell arrives in __ENV whether the runner forwarded it or
  # not, and events.js recognises exactly one scenario name: `event_publish`, throwing
  # `Unknown SCENARIO=` on anything else. An operator who had exported SCENARIO=hot_source while
  # working on a transaction case would otherwise watch the acceptance run abort during init over
  # a scenario they never asked for. The literal below is the same name events.js falls back to,
  # so pinning it changes no behaviour beyond closing that leak.
  events_args+=(-e "SCENARIO=event_publish")

  # Load shape, fixtures, settling and attribution: forwarded only when explicitly set, per the
  # note above. None of these is a credential, so argv is the right transport for them — and
  # forwarding explicitly rather than relying on inheritance means a value set but not exported in
  # the caller's shell still reaches the run, which is how an operator following the documented
  # `VAR=1 bash tests/loadtest/run_case.sh events` form expects it to behave.
  #
  # LEDGER_PAIRS and ALLOW_FIXTURE_CREATION are here because events.js REFUSES to provision
  # without one of them: it would otherwise create LEDGER_SPREAD ledgers and twice as many
  # balances in a database with no delete endpoint for either. ISOLATED_INSTANCE and its
  # relaxations are here because the acceptance run refuses to measure a shared deployment.
  for setting in RATE DURATION VUS MAX_VUS LEDGER_SPREAD TARGET_EVENTS_PER_SEC \
    MAX_DEAD_LETTER_RATIO MAX_P99_PUBLISH_SECONDS REQUIRE_METRICS \
    LEDGER_PAIRS ALLOW_FIXTURE_CREATION MIN_LEDGER_SPREAD SMOKE \
    ISOLATED_INSTANCE REQUIRE_ISOLATION ISOLATION_PROBE_SECONDS \
    ISOLATION_MAX_FOREIGN_EVENTS \
    REQUIRE_DRAIN DRAIN_FLOOR DRAIN_STABLE_SAMPLES DRAIN_POLL_SECONDS \
    PRE_DRAIN_BUDGET_SECONDS POST_DRAIN_BUDGET_SECONDS; do
    if [[ -n "${!setting:-}" ]]; then
      events_args+=(-e "${setting}=${!setting}")
    fi
  done

  # ATTRIBUTION IS A PREREQUISITE OF THIS CASE, refused here as well as in the scenario.
  #
  # Every figure the run produces is a delta of PROCESS-GLOBAL counters and a quantile over a
  # process-global histogram: blnk_events_published_total and its siblings carry no run or workload
  # dimension, and giving them one would mean unbounded label cardinality in the server, paid for
  # permanently to serve a benchmark. So a deployment serving any other traffic during the run
  # contributes its events to the same counters — which INFLATES throughput, ENLARGES the
  # dead-letter denominator and mixes the latency population, the passing direction for two of the
  # three verdicts.
  #
  # The scenario checks this too, and empirically: it takes an idle probe before the load and
  # refuses to continue if the counters move. Refusing here as well costs nothing and states the
  # requirement at the point an operator reads the command, rather than a minute into the run.
  if [[ "${CASE_NAME}" == "event-streaming" ]] &&
    [[ "${ISOLATED_INSTANCE:-0}" != "1" ]] &&
    [[ "${REQUIRE_ISOLATION:-1}" == "1" ]] &&
    [[ "${SMOKE:-0}" != "1" ]]; then
    echo "error: the event-streaming acceptance case requires a Blnk instance DEDICATED to this run."
    echo "       Its verdicts are deltas of process-global counters, so any other client's events"
    echo "       are counted as this run's work: throughput is inflated, the dead-letter"
    echo "       denominator is enlarged and the latency population is a mixture. Results from a"
    echo "       shared deployment are not attributable to this invocation and must not be quoted"
    echo "       against V-1 or V-3."
    echo
    echo "       Point the run at a stack nobody else is using and acknowledge it:"
    echo "         ISOLATED_INSTANCE=1 bash tests/loadtest/run_case.sh events"
    echo
    echo "       Or opt out explicitly, for a shakeout whose numbers are not quoted:"
    echo "         SMOKE=1 ... bash tests/loadtest/run_case.sh events"
    echo "         REQUIRE_ISOLATION=0 bash tests/loadtest/run_case.sh events"
    exit 1
  fi

  # A FIXTURE DECISION IS REFUSED HERE RATHER THAN IN setup() (M-16).
  #
  # events.js will not provision a spread implicitly, and it is right not to: the run creates up
  # to LEDGER_SPREAD ledgers and twice that many balances, and Blnk exposes no DELETE endpoint for
  # either, so they are PERMANENT and every later run and benchmark sees them.
  #
  # Its refusal lives in setup(), which k6 reaches only after initialising the scenarios, starting
  # the sampler and evaluating every threshold — so the operator's screen filled with a complete
  # verdict table reading FAIL on all six criteria and "ALL CRITERIA FAIL — NOT certified by this
  # run", and the one line that actually explained it scrolled past underneath. A missing
  # acknowledgement is indistinguishable, at a glance, from a pipeline that measured badly, and the
  # second reading costs an investigation. Refusing here puts the message where nothing can bury
  # it, and costs nothing.
  #
  # The condition mirrors the scenario's exactly, so the two cannot disagree about what counts as
  # a decision: the gate applies only when a spread would actually be provisioned, so
  # LEDGER_SPREAD=0 — the deliberate single-aggregate measurement — passes through untouched, as
  # does any pre-supplied LEDGER_PAIRS.
  events_spread="${LEDGER_SPREAD:-128}"
  if [[ "${events_spread}" =~ ^[0-9]+$ ]] && ((events_spread > 0)) &&
    [[ -z "${LEDGER_PAIRS:-}" ]] &&
    [[ "${ALLOW_FIXTURE_CREATION:-0}" != "1" ]] &&
    [[ "${SMOKE:-0}" != "1" ]]; then
    echo "This case would provision ${events_spread} ledgers and $((events_spread * 2)) balances," >&2
    echo "and Blnk has no DELETE endpoint for either — so they are PERMANENT and every later run" >&2
    echo "and benchmark sees them. Refusing here rather than after a full run's output." >&2
    echo "" >&2
    echo "Choose one:" >&2
    echo "  ALLOW_FIXTURE_CREATION=1   this database is disposable or per-run. Required for a" >&2
    echo "                             run whose numbers are quoted against V-1 or V-3." >&2
    echo "  LEDGER_PAIRS='[{\"source\":\"bln_...\",\"destination\":\"bln_...\"}]'" >&2
    echo "                             reuse fixtures that already exist; nothing is created." >&2
    echo "  SMOKE=1                    a shakeout. Permits the single-aggregate fallback, and" >&2
    echo "                             its numbers must NOT be quoted against V-1 or V-3." >&2
    echo "  LEDGER_SPREAD=0            measure single-aggregate ordering deliberately." >&2
    echo "" >&2
    echo "Example, for a disposable database:" >&2
    echo "  ALLOW_FIXTURE_CREATION=1 bash tests/loadtest/run_case.sh ${REQUESTED_CASE_NAME}" >&2
    exit 1
  fi

  # THE RAW STREAM IS OPT-IN, AND THE SUMMARY IS THE ACCEPTANCE ARTIFACT (PERF-M12).
  #
  # `--out json=` was unconditional, so every run wrote k6's raw NDJSON: one record per metric
  # sample per request. At the criterion's own load — 500 events per second for thirty minutes,
  # with several samples per iteration — that is tens of millions of records and gigabytes
  # written by the load generator itself, while it is trying to measure sub-second latency.
  #
  # That is not merely wasteful, it is CIRCULAR: the generator's own disk and CPU contention
  # shows up as dropped iterations and inflated latency, so the mandatory artifact degrades the
  # verdict it exists to evidence. And the verdicts do not come from it — every V-1, V-3 and p99
  # number is computed inside the scenario and written to the summary, which is why the summary
  # stays on by default and this does not.
  #
  # Enable it for DIAGNOSIS, at a load where the cost is affordable: a smoke run whose per-request
  # detail explains something the summary only aggregates. Setting NDJSON_OUT implies it, so
  # naming a destination is enough and there is no way to ask for the file and not get it.
  #
  # BOUNDING IT: a `.gz` destination is written compressed by k6 directly — measured at roughly a
  # quarter of the plain size on this stream — so `NDJSON_OUT=... .gz` is the cheap way to keep
  # the detail. The estimate below is printed rather than enforced: refusing to run would be
  # worse than a warned-about large file, but an operator should not discover the size afterwards.
  RAW_OUTPUT="${RAW_OUTPUT:-}"
  if [[ -n "${NDJSON_OUT:-}" ]]; then
    RAW_OUTPUT="1"
  fi

  k6_out_args=()
  if [[ -n "${RAW_OUTPUT}" && "${RAW_OUTPUT}" != "0" && "${RAW_OUTPUT}" != "false" ]]; then
    k6_out_args+=(--out "json=${EVENTS_NDJSON_TMP}")
    if [[ "${EVENTS_NDJSON_OUT}" == *.gz ]]; then
      echo "note: raw k6 NDJSON enabled, gzip-compressed -> ${EVENTS_NDJSON_OUT}"
    else
      echo "warning: raw k6 NDJSON enabled UNCOMPRESSED -> ${EVENTS_NDJSON_OUT}"
      echo "         A full acceptance run (500/s for 30m) writes on the order of tens of"
      echo "         millions of records and several GiB here, and that I/O competes with the"
      echo "         latency the run is measuring. Prefer a .gz destination, or a short DURATION:"
      # `bash $0` rather than `$0`, because this file ships mode 0644 and every documented
      # invocation in tests/loadtest/README.md spells it the same way. A hint that cannot be
      # pasted is worse than no hint: it sends the reader to a permission error.
      echo "         NDJSON_OUT=tests/loadtest/run-event-streaming.ndjson.gz bash $0 ${REQUESTED_CASE_NAME}"
    fi
  fi

  echo "Running event-streaming acceptance case"
  # Both names are echoed so the alias is visible rather than silently substituted (CONTRACT-m01).
  if [[ "${REQUESTED_CASE_NAME}" != "${CASE_NAME}" ]]; then
    echo "  case         : ${CASE_NAME} (requested as ${REQUESTED_CASE_NAME}; same case)"
  else
    echo "  case         : ${CASE_NAME}"
  fi
  # REDACTED, both of them. events.js redacts what it writes into the summary; these two lines are
  # this script's own output and are printed before k6 starts, so they need their own redaction.
  # A credential-bearing value never reaches here — it is refused above — so this is defence in
  # depth for the case where the refusal is ever relaxed.
  echo "  transactions : $(redact_url "${URL}")"
  echo "  metrics      : $(redact_url "${METRICS_URL}")"
  echo "  summary      : ${EVENTS_SUMMARY_OUT}"
  echo "  raw NDJSON   : $(
    [[ ${#k6_out_args[@]} -gt 0 ]] && echo "${EVENTS_NDJSON_OUT}" || echo "off (RAW_OUTPUT=1 or NDJSON_OUT=PATH to enable)"
  )"
  echo "  load shape   : $(
    [[ -n "${RATE:-}${DURATION:-}" ]] && echo "caller-specified" ||
      echo "events.js defaults: 550/s offered for 30m, judged against V-1's 500/s"
  )"
  echo "  attribution: $(
    [[ "${ISOLATED_INSTANCE:-0}" == "1" ]] &&
      echo "dedicated instance acknowledged; the scenario also probes for foreign traffic before the load" ||
      echo "NOT acknowledged — the isolation requirement was relaxed, so these figures are not attributable to this run"
  )"
  echo "  fixtures: $(
    if [[ -n "${LEDGER_PAIRS:-}" ]]; then
      echo "reusing the pairs in LEDGER_PAIRS; nothing is created"
    elif [[ "${ALLOW_FIXTURE_CREATION:-0}" == "1" ]]; then
      echo "created by this run and PERMANENT (Blnk has no delete endpoint for a ledger or a balance)"
    elif [[ "${LEDGER_SPREAD:-}" == "0" ]]; then
      echo "none needed — LEDGER_SPREAD=0 measures ONE aggregate deliberately, which is right for ordering work and wrong for throughput"
    elif [[ "${SMOKE:-0}" == "1" ]]; then
      echo "none named — SMOKE=1 permits the single-aggregate fallback, so this run measures one key's serialisation ceiling and its figures are not quoted against V-1"
    else
      echo "none named — the scenario will refuse to provision; pass LEDGER_PAIRS or ALLOW_FIXTURE_CREATION=1"
    fi
  )"

  # No queue benchmark. That tool measures Redis asynq depth for the transaction pipeline; the
  # event pipeline's unsettled depth is the outbox census the scenario reads itself — the whole
  # population from which a Kafka publish is still owed, from GET /events/stats, with
  # blnk_outbox_pending on /metrics as a pending-plus-processing-only fallback. Requiring a Redis
  # DSN here would block a run that has no use for one.
  #
  # ---------------------------------------------------------------------------------
  # ARTIFACT LIFECYCLE: A FAILED RUN MUST LEAVE NO SUCCESSFUL-LOOKING ARTIFACT BEHIND.
  #
  # k6 writes the summary from handleSummary, which runs only if the test reached the end of
  # its lifecycle. A run that failed during init — an unknown scenario, an unreachable
  # deployment, a refused acceptance guard — or that was interrupted therefore writes NO
  # summary at all, and the file at the stable path was then whatever the last run put there.
  # A thirty-minute PASS from last week is indistinguishable from this run's result: same
  # path, same shape, and a collector or a reviewer reading it is reading a stale verdict as
  # a current one. The NDJSON stream has the opposite problem — k6 opens it immediately and
  # writes as it goes, so an interrupted run leaves a TRUNCATED file that parses as a short
  # run rather than as a failure.
  #
  # So: write to unique temporary paths beside the destinations, remove any stale destination
  # BEFORE launching so that a failure leaves an ABSENCE rather than a lie, and move the
  # temporaries into place only after k6 has exited 0. `mv` within one directory is atomic on
  # POSIX, so a reader never sees a partial file at the destination.
  # ---------------------------------------------------------------------------------
  rm -f -- "${EVENTS_SUMMARY_OUT}" "${EVENTS_NDJSON_OUT}"

  events_cleanup_partials() {
    rm -f -- "${EVENTS_SUMMARY_TMP}" "${EVENTS_NDJSON_TMP}"
  }
  # INT and TERM are trapped as well as EXIT, and not only for tidiness: a bash trap on EXIT
  # alone still runs on a signal, but the default disposition would leave the exit status of
  # the signal rather than of k6, and an operator who interrupts a run must not find a
  # promoted artifact. Both handlers remove partials and nothing else.
  trap events_cleanup_partials EXIT
  trap 'events_cleanup_partials; trap - EXIT; exit 130' INT
  trap 'events_cleanup_partials; trap - EXIT; exit 143' TERM

  # No queue benchmark. That tool measures Redis asynq depth for the transaction pipeline;
  # the event pipeline's backlog is blnk_outbox_pending on /metrics, which the scenario reads
  # itself. Requiring a Redis DSN here would block a run that has no use for one.
  #
  # --include-system-env-vars IS PASSED EXPLICITLY, and it is not decoration. It defaults to
  # true, which is what lets the exported credentials above reach __ENV — but the default is
  # overridable from the environment, and K6_INCLUDE_SYSTEM_ENV_VARS=false in a caller's shell
  # turns the whole credential channel off. Verified on this toolchain: with that variable set,
  # an exported value arrives EMPTY, and this flag restores it. The failure it prevents is the
  # quiet kind — a refused metrics scrape and a 401 on the stats endpoint produce a run that
  # completes and reports withheld verdicts, not one that stops and says why.
  #
  # `|| events_k6_status=$?` rather than letting `set -e` abort: the status has to be inspected
  # to decide whether to promote, and an aborted shell would leave the partials in place and
  # report nothing about them.
  events_k6_status=0
  # A SUBSHELL, so the three credentials are exported into k6's process tree and nowhere else,
  # and each only when a value exists — an exported empty string is present-and-blank in __ENV,
  # which events.js treats differently from absent for the API key and the master key.
  (
    if [[ -n "${BLNK_METRICS_BEARER_TOKEN:-${METRICS_BEARER_TOKEN:-}}" ]]; then
      export METRICS_BEARER_TOKEN="${BLNK_METRICS_BEARER_TOKEN:-${METRICS_BEARER_TOKEN:-}}"
    fi
    if [[ -n "${BLNK_SERVER_SECRET_KEY:-${MASTER_KEY:-}}" ]]; then
      export MASTER_KEY="${BLNK_SERVER_SECRET_KEY:-${MASTER_KEY:-}}"
    fi
    if [[ -n "${API_KEY:-${BLNK_API_KEY:-}}" ]]; then
      export API_KEY="${API_KEY:-${BLNK_API_KEY:-}}"
    fi

    k6 run \
      --include-system-env-vars=true \
      "${k6_out_args[@]}" \
      -e "SUMMARY_OUT=${EVENTS_SUMMARY_TMP}" \
      "${events_args[@]}" \
      tests/loadtest/events.js
  ) || events_k6_status=$?

  if [[ "${events_k6_status}" -ne 0 ]]; then
    events_cleanup_partials
    trap - EXIT INT TERM
    echo ""
    echo "error: k6 exited ${events_k6_status}; NO artifact was written."
    echo "       ${EVENTS_SUMMARY_OUT} and ${EVENTS_NDJSON_OUT} were removed before the run and"
    echo "       have deliberately NOT been recreated, so nothing here can be read as this"
    echo "       run's result. A k6 exit of 99 means a threshold failed and the summary is"
    echo "       genuinely absent only if the run also failed to reach handleSummary; any"
    echo "       other non-zero status means the run did not complete."
    exit "${events_k6_status}"
  fi

  # PROMOTION. The summary is required: a zero exit with no summary means handleSummary did not
  # run, and an absent verdict must not be reported as a generated file.
  if [[ ! -s "${EVENTS_SUMMARY_TMP}" ]]; then
    events_cleanup_partials
    trap - EXIT INT TERM
    echo "error: k6 exited 0 but wrote no summary to ${EVENTS_SUMMARY_TMP}."
    echo "       The run produced no verdict, so no artifact has been published."
    exit 1
  fi

  mv -f -- "${EVENTS_SUMMARY_TMP}" "${EVENTS_SUMMARY_OUT}"
  # The NDJSON stream is best-effort: --out json is a diagnostic, and a run whose verdict was
  # written is still a valid run if the stream was not requested or could not be opened.
  if [[ -e "${EVENTS_NDJSON_TMP}" ]]; then
    mv -f -- "${EVENTS_NDJSON_TMP}" "${EVENTS_NDJSON_OUT}"
  fi

  events_cleanup_partials
  trap - EXIT INT TERM

  echo "Generated files:"
  echo "  ${EVENTS_SUMMARY_OUT}"
  if [[ ${#k6_out_args[@]} -gt 0 ]]; then
    echo "  ${EVENTS_NDJSON_OUT}"
  fi
  exit 0
fi

# THE EARLY EXIT ABOVE IS THE POINT rather than a shortcut (PERF-P14). events.js documented a
# `run_case.sh event-streaming` command while the runner had no such case, so it printed "unknown
# case" — the one instruction a reader of that file is most likely to follow. That name is now the
# canonical one, and `events` normalises onto it, so both spellings reach the branch above and both
# produce the frozen filenames.
#
# It cannot reuse the pipeline below. That pipeline REQUIRES a Redis DSN and refuses to run
# without one, starts tools/queue_benchmark.go to measure asynq queue drain, and runs script.js.
# None of the three applies here: this scenario reads its verdicts from the server's /metrics
# endpoint, so it needs METRICS_URL and a bearer token instead of Redis, and a queue-drain
# benchmark would measure the transaction pipeline rather than the event pipeline.
#
# The branch above deliberately restates NONE of events.js's own defaults. DURATION, RATE, VUS,
# MAX_VUS and every threshold default live in that file — V-1 and V-3 are stated over 30 minutes
# at 500 events/second, which it offers at 550/s so the measured rate has somewhere to fall
# from — so a second set of numbers here would be a second opinion able to
# disagree with the acceptance criteria. That is why every load-shape variable is forwarded ONLY
# when the caller set it, and why an empty credential is not exported as an empty value: a
# METRICS_BEARER_TOKEN present in __ENV and empty overrides the file's own resolution with nothing.

DURATION="${DURATION:-30s}"
RATE="${RATE:-300}"
VUS="${VUS:-200}"
MAX_VUS="${MAX_VUS:-800}"
REDIS_DSN="${BLNK_REDIS_DNS:-${REDIS_DSN:-}}"

OUT_DIR="tests/loadtest"
SUMMARY_OUT="${OUT_DIR}/summary-${CASE_NAME}.json"
NDJSON_OUT="${OUT_DIR}/run-${CASE_NAME}.ndjson"
QUEUE_OUT="${OUT_DIR}/queue-summary-${CASE_NAME}.json"

SCENARIO=""
SOURCE_BUCKETS="1"
DESTINATION_BUCKETS="1"
QUEUE_PREFIXES="new:transaction_"

case "${CASE_NAME}" in
  hot-to-cold-no-shard)
    SCENARIO="hot_source"
    ;;
  cold-to-hot-no-shard)
    SCENARIO="hot_destination"
    ;;
  hot-to-cold-shard)
    SCENARIO="hot_source"
    SOURCE_BUCKETS="${SOURCE_BUCKETS_OVERRIDE:-8}"
    ;;
  cold-to-hot-shard)
    SCENARIO="hot_destination"
    DESTINATION_BUCKETS="${DESTINATION_BUCKETS_OVERRIDE:-8}"
    ;;
  *)
    echo "unknown case: ${CASE_NAME}"
    echo "queue cases: hot-to-cold-no-shard | cold-to-hot-no-shard | hot-to-cold-shard | cold-to-hot-shard"
    echo "event case:  event-streaming (also accepted: events)"
    exit 1
    ;;
esac

if [[ "${QUEUE_MODE}" == "hot" ]]; then
  QUEUE_PREFIXES="new:transaction_,hot_"
fi

if [[ -z "${REDIS_DSN}" ]]; then
  echo "BLNK_REDIS_DNS or REDIS_DSN is required for queue benchmarking"
  exit 1
fi

echo "Starting queue benchmark for ${CASE_NAME}"
go run ./tests/loadtest/tools/queue_benchmark.go \
  -redis-dsn "${REDIS_DSN}" \
  -queue-prefixes "${QUEUE_PREFIXES}" \
  -wait \
  -out "${QUEUE_OUT}" &
QUEUE_BENCH_PID=$!

cleanup() {
  if kill -0 "${QUEUE_BENCH_PID}" >/dev/null 2>&1; then
    kill -INT "${QUEUE_BENCH_PID}" >/dev/null 2>&1 || true
    wait "${QUEUE_BENCH_PID}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

echo "Running ${CASE_NAME}"
k6 run \
  --out "json=${NDJSON_OUT}" \
  -e SUMMARY_OUT="${SUMMARY_OUT}" \
  -e URL="${URL}" \
  -e SCENARIO="${SCENARIO}" \
  -e DURATION="${DURATION}" \
  -e RATE="${RATE}" \
  -e VUS="${VUS}" \
  -e MAX_VUS="${MAX_VUS}" \
  -e SOURCE_BUCKETS="${SOURCE_BUCKETS}" \
  -e DESTINATION_BUCKETS="${DESTINATION_BUCKETS}" \
  tests/loadtest/script.js

echo "Waiting for queue drain benchmark to finish"
wait "${QUEUE_BENCH_PID}"

trap - EXIT

echo "Generated files:"
echo "  ${SUMMARY_OUT}"
echo "  ${NDJSON_OUT}"
echo "  ${QUEUE_OUT}"
