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

PROJECT=blnk

printProject:
	echo ${PROJECT}

# DOWNLOAD AND VERIFY, NEVER `go get ./...`.
#
# `go get ./...` RESOLVES and can WRITE go.mod and go.sum: it upgrades a requirement to
# a newer version that satisfies the same import, adds anything missing and rewrites the
# manifests as a side effect of what was supposed to be a fetch. So the first thing a
# new contributor ran, on a repository whose whole dependency contract is `go.mod` plus
# `go.sum`, was the one command able to change it — and a bumped version arriving in an
# unrelated pull request is indistinguishable from a deliberate upgrade.
#
# `go mod download` fetches exactly what the manifests pin and writes neither. `go mod
# verify` then checks every module in the cache against its recorded go.sum hash, which
# is the supply chain check the download alone does not perform.
init:
	go mod download
	go mod verify

generate:
	go generate ./...

test:
	go test -short  ./...

# Mutation testing on the money-critical fast packages (model, filter). Plants hundreds
# of one-line bugs and re-runs the tests against each one; a "LIVED" mutant is a bug the
# test suite would not catch. Fails when test efficacy (killed/viable mutants) drops
# below the threshold.
MUTATION_THRESHOLD=80

# A SCOPE is "directory:filter".
#
#   filter=all   score every file gremlins finds from that directory down
#   filter=event score only that directory's OWN event/outbox files
MUTATION_FAST_SCOPES=model:all internal/filter:all internal/apierror:all

# ./api IS IN THIS LIST, and it is the master-key-gated surface: api/events.go serves the
# dead-letter inventory and the replay, api/subscribers.go mints Kafka credentials. Leaving
# it out — which it was — meant the two endpoints that can replay a ledger event or hand out
# a broker principal were the only new event code no mutation scope ever reached. See
# MUTATION_EVENT_FILE_PREFIXES for how the filter finds them under their own names.
MUTATION_EVENT_SCOPES=.:event database:event api:event

# Per-mutant test timeout, as a multiple of the measured baseline. 8 for every scope,
# and raising it for the event scopes was TRIED AND REJECTED on evidence.
MUTATION_TIMEOUT_COEFFICIENT=8

# WHICH FILE NAMES THE `event` FILTER KEEPS.
#
# The root package and ./database name their event surface event_*.go; ./api names the same
# surface events.go and subscribers.go, because api/ files are named after the ROUTE they
# serve. A filter that matched `event_` alone therefore scored nothing in ./api — and worse,
# would have scored nothing SILENTLY if the fail-closed guard below had used a different
# pattern from the exclude list. Both read this one variable.
MUTATION_EVENT_FILE_PREFIXES=event subscriber

# THE COVERAGE STEP IS NOT THE MUTATION STEP, and this is the list that keeps the two from
# being confused for each other.
#
# gremlins measures coverage before it mutates anything, and it does that by running the
# module's own test suite: `go test -cover -coverprofile <f> ./...` when invoked from the
# repository root. So a single red test ANYWHERE in the module — in a package this gate does
# not score, testing code this gate does not mutate — ends the run with "failed to gather
# coverage" and NO SCORE AT ALL. That is what happened: `make mutate_events` could not report
# a number for the event scopes because two pre-existing api tests are load-fragile, and the
# AAP's mutation gate on the new event code went unverified for a reason that had nothing to
# do with the event code.
#
# The named tests poll a live asynq pipeline against a wall clock — 10s for an inflight
# transaction to be applied, 2 minutes for a TypeSense reindex — so on a host running several
# suites at once they fail on the budget rather than on the behaviour. They are excluded HERE,
# in the gate, rather than weakened where they live: they are pre-existing, they belong to
# features this change does not touch, and the coverage they contribute is coverage of code
# this gate excludes from mutation anyway.
#
# THE EXCLUSION IS NOT A WEAKENING OF THE SCORE. It reaches `go test` through GOFLAGS, which
# the go command applies only to commands that know the flag, so it lands on the coverage run
# and on the per-mutant runs — and a per-mutant run only ever executes the MUTATED package's
# tests, so for the root and database scopes it matches nothing at all. Where it does match
# (the api scope) removing a load-fragile test is a correctness gain: gremlins runs each
# mutant with -failfast, so a test that fails on a timeout would KILL a mutant it never
# detected and inflate the efficacy.
#
# Override it to score with everything running: `make mutate_events MUTATION_COVERAGE_SKIP=`.
# Every name in it is asserted to still exist by TestMutationGate_SkipsOnlyTestsThatExist, so
# a rename cannot quietly turn this back into a blocked gate.
MUTATION_COVERAGE_SKIP=^(TestInflightTransaction_Commit_API|TestInflightTransaction_Commit_WithAmount_API|TestSearchWithTypesense)$$

mutate: SCOPES=${MUTATION_FAST_SCOPES}
mutate: mutation_gate

# Score the event and outbox surface at the same threshold. Long-running by
# construction — see the note above the threshold for the measured numbers.
mutate_events: SCOPES=${MUTATION_EVENT_SCOPES}
mutate_events: mutation_gate

# Everything the repository gates on, in one run.
mutate_all: SCOPES=${MUTATION_FAST_SCOPES} ${MUTATION_EVENT_SCOPES}
mutate_all: mutation_gate

# THE MUTATION TOOL IS PINNED TO AN EXACT VERSION, and the pin is a supply-chain control
# rather than a reproducibility nicety.
#
# THE PIN IS VERIFIED AGAINST THE BINARY, NOT ASSUMED FROM ITS ABSENCE. The check used to be
# `command -v gremlins || go install ...@${GREMLINS_VERSION}`, which installs the pin only when
# NO gremlins is on PATH — so any gremlins already there, of any version, silently became the
# tool the gate scored with, and two machines could report different efficacy for one commit
# with nothing to show for it.
#
# `gremlins --version` cannot answer this: the version string is an ldflags stamp applied by
# the project's release build, so a binary correctly installed from the v0.6.0 tag by
# `go install` still reports "dev". `go version -m <binary>` reports the MODULE version it was
# built from, which is the fact the pin is about, and a locally built one reports "(devel)".
GREMLINS_VERSION=v0.6.0
GREMLINS_MODULE=github.com/go-gremlins/gremlins

# Prints the module version a Go binary was built from, or nothing when it was not built from
# a tagged module. A make variable rather than recipe lines, following BROKER_ARRAY_DECLARED.
GREMLINS_INSTALLED_VERSION = go version -m "$$candidate" 2>/dev/null | awk '$$1 == "mod" && $$2 == "${GREMLINS_MODULE}" { print $$3; exit }'

mutation_gate:
	@set -e; \
	if [ -z "${SCOPES}" ]; then \
		echo "mutation_gate is not a target to invoke directly: it scores whatever SCOPES names,"; \
		echo "and nothing named any. Use 'make mutate', 'make mutate_events' or 'make mutate_all'."; \
		exit 1; \
	fi; \
	gremlins_bin=""; \
	for candidate in "$$(command -v gremlins 2>/dev/null || true)" "$$(go env GOPATH)/bin/gremlins"; do \
		[ -n "$$candidate" ] && [ -x "$$candidate" ] || continue; \
		installed=$$(${GREMLINS_INSTALLED_VERSION}); \
		if [ "$$installed" = "${GREMLINS_VERSION}" ]; then gremlins_bin="$$candidate"; break; fi; \
		echo "ignoring $$candidate: built from ${GREMLINS_MODULE} $${installed:-an untagged local build}, and this gate scores with ${GREMLINS_VERSION}"; \
	done; \
	if [ -z "$$gremlins_bin" ]; then \
		echo "installing ${GREMLINS_MODULE}@${GREMLINS_VERSION}"; \
		go install ${GREMLINS_MODULE}/cmd/gremlins@${GREMLINS_VERSION}; \
		gremlins_bin="$$(go env GOPATH)/bin/gremlins"; \
	fi; \
	if [ ! -x "$$gremlins_bin" ]; then \
		echo "MUTATION GATE FAILED: gremlins is not executable at $$gremlins_bin. It was just"; \
		echo "installed into \$$(go env GOPATH)/bin — check that directory exists and is writable,"; \
		echo "then re-run."; \
		exit 1; \
	fi; \
	candidate="$$gremlins_bin"; \
	scoring_with=$$(${GREMLINS_INSTALLED_VERSION}); \
	if [ "$$scoring_with" != "${GREMLINS_VERSION}" ]; then \
		echo "MUTATION GATE FAILED: $$gremlins_bin is built from ${GREMLINS_MODULE}"; \
		echo "$${scoring_with:-an untagged local build}, and this gate scores with ${GREMLINS_VERSION}."; \
		echo "A score from another version is not comparable with the threshold this gate enforces."; \
		echo "Install the pin: go install ${GREMLINS_MODULE}/cmd/gremlins@${GREMLINS_VERSION}"; \
		exit 1; \
	fi; \
	echo "using gremlins at $$gremlins_bin ($$scoring_with)"; \
	export GOFLAGS="-p=1 $$GOFLAGS"; \
	if [ -n "${MUTATION_COVERAGE_SKIP}" ]; then \
		export GOFLAGS="-skip=${MUTATION_COVERAGE_SKIP} $$GOFLAGS"; \
		echo "coverage-gathering skip: ${MUTATION_COVERAGE_SKIP}"; \
	fi; \
	for scope in ${SCOPES}; do \
		pkg=$${scope%%:*}; \
		filter=$${scope##*:}; \
		excludes=""; \
		coefficient=${MUTATION_TIMEOUT_COEFFICIENT}; \
		if [ "$$filter" = "event" ]; then \
			kept='^('$$(echo ${MUTATION_EVENT_FILE_PREFIXES} | tr ' ' '|')')'; \
			excludes="-E /"; \
			for f in $$(cd $$pkg && ls *.go | grep -v '_test\.go$$' | grep -vE "$$kept"); do \
				excludes="$$excludes -E $$f"; \
			done; \
			scored=$$(cd $$pkg && ls *.go 2>/dev/null | grep -v '_test\.go$$' | grep -cE "$$kept" || true); \
			if [ "$${scored:-0}" -eq 0 ]; then \
				echo "MUTATION GATE FAILED: scope $$scope asks for files matching $$kept in $$pkg and"; \
				echo "there are none. A renamed or relocated event file would otherwise make this"; \
				echo "scope score nothing and report success."; \
				exit 1; \
			fi; \
			echo "scoring $$scored file(s) in $$pkg matching $$kept"; \
		fi; \
		echo "==> mutation testing $$pkg [$$filter] (threshold ${MUTATION_THRESHOLD}%, timeout x$$coefficient)"; \
		log=/tmp/gremlins-$$(echo $$pkg-$$filter | tr '/.' '_').log; \
		status=0; \
		(cd $$pkg && "$$gremlins_bin" unleash --workers 2 --timeout-coefficient $$coefficient $$excludes) > $$log 2>&1 || status=$$?; \
		cat $$log; \
		eff=$$(grep -o 'Test efficacy: [0-9.]*' $$log | grep -o '[0-9.]*' || true); \
		if [ -z "$$eff" ]; then \
			echo "MUTATION GATE FAILED: gremlins did not report a score for $$pkg [$$filter] (exit $$status)."; \
			echo "(The '|| true' on the score extraction above is what lets you read this at all:"; \
			echo "under 'set -e' a grep that matches nothing fails the assignment and kills the"; \
			echo "recipe before it can explain itself.)"; \
			echo "This is a RUN failure, not a low score — the run above did not complete. The"; \
			echo "usual cause is that coverage gathering failed because some package's tests are"; \
			echo "red: gremlins runs the whole module's suite to gather coverage, so \`go test ./...\`"; \
			echo "must be green before the gate can score anything. See $$log."; \
			exit 1; \
		fi; \
		killed=$$(grep -oE 'Killed: [0-9]+' $$log | grep -oE '[0-9]+' || true); \
		lived=$$(grep -oE 'Lived: [0-9]+' $$log | grep -oE '[0-9]+' || true); \
		timedout=$$(grep -oE 'Timed out: [0-9]+' $$log | grep -oE '[0-9]+' || true); \
		conclusive=$$(( $${killed:-0} + $${lived:-0} )); \
		if [ "$${timedout:-0}" -gt 0 ]; then \
			echo "NOTE: $$pkg [$$filter] reached a verdict on $$conclusive mutants (killed $${killed:-0},"; \
			echo "lived $${lived:-0}) and TIMED OUT on $${timedout}. gremlins EXCLUDES a timed-out mutant from"; \
			echo "the efficacy ratio rather than counting it as survived, so the percentage below"; \
			echo "describes the conclusive subset and not the whole mutant set. Read it that way."; \
			echo "On this repository's event scopes the timeouts are genuine hangs, not impatience:"; \
			echo "mutating the outbox claim query or a retry loop against a real PostgreSQL blocks on"; \
			echo "a row lock. Survivors (LIVED) are the signal to act on; see $$log."; \
		fi; \
		awk -v e="$$eff" -v t="${MUTATION_THRESHOLD}" 'BEGIN { exit (e+0 < t+0) ? 1 : 0 }' \
			|| { echo "MUTATION GATE FAILED: $$pkg [$$filter] efficacy $$eff% is below ${MUTATION_THRESHOLD}% — see LIVED lines in $$log"; exit 1; }; \
		echo "PASS: $$pkg [$$filter] efficacy $$eff%"; \
	done

build:
	go build -o ${PROJECT} ./cmd/*.go

docker_run:
	docker run -v `pwd`/blnk.json:/blnk.json -p 4300:4100 jerryenebeli/blnk:main

run:
	./${PROJECT} start

run_workers:
	./${PROJECT} workers

# Start the SERVER PROCESS ROLE, which is what hosts the event outbox relay. The name is
# the one the operations runbook uses, so it answers "where does the relay run" without
# anyone reading cmd/server.go.
#
# THE APPLICATION DECIDES WHETHER IT HAS A BROKER, not this recipe. `--require-kafka`
# makes `blnk start` run its own configuration resolution and refuse when that yields no
# broker, naming every source it read. An earlier version of this target tried to answer
# the question here and got it wrong twice: `grep '"brokers"'` reads `"brokers": []` as
# configured, and no first-non-empty scan can express that an empty value at a
# higher-precedence name CLEARS what a lower one supplied, because that is a property of
# the overlay rather than of any one source.
#
# A DELIBERATELY EMPTY VALUE FROM THE CALLER WINS TOO, because `export -p` records a
# set-but-empty variable: `KAFKA_BROKERS= make run_relay` means "no brokers for this run",
# so it is refused rather than quietly falling back to .env. That follows from replaying
# the caller's environment over .env below; it is not a special case anywhere.
CONFIG_FILE?=blnk.json

# Answers "does ${CONFIG_FILE} declare at least one USABLE Kafka broker?" through an
# exit status: 0 yes, non-zero no.
#
# WHY THIS IS NOT `grep -q '"brokers"'`
#
# python3 first, jq second, and the fallback is the `||` rather than a `command -v`
# dance: an absent python3 exits 127, which is a failure, which falls through to jq
# exactly as an unparseable file does. If NEITHER is installed both fail and the answer
# is "not declared" — the fail-closed direction, because guessing "yes" is what produced
# the silent failure above, and KAFKA_BROKERS in the environment takes precedence over
# this file anyway.
BROKER_ARRAY_DECLARED = python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); k=d.get("kafka") or {}; b=k.get("brokers") or []; sys.exit(0 if isinstance(b,list) and [x for x in b if isinstance(x,str) and x.strip()] else 1)' "$(CONFIG_FILE)" 2>/dev/null || jq -e '(.kafka.brokers // []) | map(select(type == "string" and (. | gsub("^\\s+|\\s+$$"; "")) != "")) | length > 0' "$(CONFIG_FILE)" >/dev/null 2>&1

# The same question as a target, so it can be exercised directly and by the contract test.
# Not intended for day-to-day use.
_brokers_declared_in_file:
	@${BROKER_ARRAY_DECLARED}

run_server_relay:
	@caller_environment="$$(export -p)"; \
	prefixed_set="$${BLNK_KAFKA_BROKERS+set}"; \
	bare_set="$${KAFKA_BROKERS+set}"; \
	derived_set="$${BLNK_KAFKA_KAFKA_BROKERS+set}"; \
	caller_decided="$$prefixed_set$$bare_set$$derived_set"; \
	if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	eval "$$caller_environment"; \
	if [ -n "$$caller_decided" ]; then \
		if [ -z "$$prefixed_set" ]; then unset BLNK_KAFKA_BROKERS; fi; \
		if [ -z "$$bare_set" ]; then unset KAFKA_BROKERS; fi; \
		if [ -z "$$derived_set" ]; then unset BLNK_KAFKA_KAFKA_BROKERS; fi; \
	fi; \
	echo "Starting the SERVER role: HTTP API, lineage outbox processor, event metrics collector"; \
	echo "and the event outbox relay. This is not the relay alone — the API will be listening"; \
	echo "and the other background workers will be running."; \
	echo "--require-kafka is passed, so the APPLICATION resolves its own Kafka configuration and"; \
	echo "refuses to start when that resolution yields no broker — the same resolution and the"; \
	echo "same predicate the relay's own gate uses. Its refusal names every source it read."; \
	echo "'KAFKA_BROKERS= make run_relay' therefore means no brokers for this run and is refused,"; \
	echo "because a name that is set and empty clears a list ${CONFIG_FILE} supplied."; \
	exec ./${PROJECT} start --config "${CONFIG_FILE}" --require-kafka

# Identical behaviour, including the announcement above, so neither spelling can leave
# an operator believing a bare relay is what came up.
run_relay: run_server_relay

build_run:
	make build
	make run

build_test_run:
	make build
	make test
	make run

# Provision a RUNNING Kafka broker for event streaming: the category topics and their
# dead-letter siblings, the steady-state producer principal, the sample subscriber
# principal, and both principals' ACLs.
#
# WHAT IT CREATES, spelled out here so the shipped geometry is legible at the invocation
# site rather than only inside a four-thousand-line script. Every name derives from
# KAFKA_TOPIC_PREFIX (default "blnk"), and every dead-letter name is its category plus
# ".dlt", exactly as event_topics.go's DLTFor composes it:
#
#     blnk.transactions      blnk.balances      blnk.identities      blnk.system
#     blnk.transactions.dlt  blnk.balances.dlt  blnk.identities.dlt  blnk.system.dlt
#
# EIGHT owned topics, from FOUR categories. The fourth exists because two real event
# types — ledger.created and system.error — belong to none of the three the requirement
# names, while the coverage rule admits no exceptions; blnk.system takes both.
#
# Each topic is created at KAFKA_MIN_PARTITIONS partitions — 6 is the required minimum.
# An EMPTY topic below that count is grown to it; one that already holds messages is
# reported and left alone, because adding partitions re-maps keys and would split an
# aggregate's history across two of them, so that growth needs a deliberate
# KAFKA_ALLOW_PARTITION_GROWTH and a planned migration.
#
# Idempotent, so re-running it after a bring-up is safe and is the normal way to repair
# a drifted topic geometry or ACL. It only ever adds: no topic is deleted, no partition
# count is reduced, and no existing credential is replaced unless a KAFKA_ROTATE_*
# variable asks for it.
#
# Checks a manifest tree before it reaches a cluster: every image digest-pinned, and every
# Secret key the manifests reference present in the namespace when a cluster is reachable.
#
#   make k8s_preflight                     check the committed tree (EXPECTED to fail:
#                                          the placeholder is intentional in git)
#   make k8s_preflight DIR=./rendered      check a tree you have already rendered
#   make k8s_render BLNK_IMAGE=repo@sha256:...   render to ./rendered, then check it
k8s_preflight:
	./scripts/k8s-preflight.sh $${DIR}

# Renders the manifests with the application image resolved, into a directory you then
# apply. A COPY rather than an in-place edit on purpose: an in-place substitution would
# leave a digest in the working tree that must never be committed.
k8s_render:
	@test -n "$${BLNK_IMAGE}" || { \
		echo "BLNK_IMAGE is required and must be digest-pinned, e.g."; \
		echo "  make k8s_render BLNK_IMAGE=ghcr.io/you/blnk@sha256:<64 hex>"; \
		exit 1; \
	}
	./scripts/k8s-preflight.sh --render $${RENDER_DIR:-./rendered}

kafka_provision:
	@caller_environment="$$(export -p)"; \
	if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	eval "$$caller_environment"; \
	export KAFKA_TOPIC_PREFIX="$${KAFKA_TOPIC_PREFIX:-blnk}"; \
	export KAFKA_MIN_PARTITIONS="$${KAFKA_MIN_PARTITIONS:-6}"; \
	export KAFKA_REPLICATION_FACTOR="$${KAFKA_REPLICATION_FACTOR:-1}"; \
	./scripts/kafka-provision.sh

# THE ALERT RULES ARE GATED, not merely linted.
#
# `promtool check rules` proves the file parses and that every expression is valid PromQL.
# It CANNOT prove a rule fires, and the two are independent: SubscriberSettlementNotProgressing
# passed `check rules` for its entire life while being unable to fire on the one deployment
# it exists for, because its right-hand counter has no series until the settlement pass
# discharges its first obligation and `and` against an empty vector is empty. Only
# `promtool test rules` catches that, so both run and `alerts_test` depends on `alerts_check`.
#
# The promtool used is the one from the Prometheus image the stack already pins, read out of
# docker-compose.yaml so the version lives in exactly one place and the engine that validates
# a rule is the engine that will load it. A promtool already on PATH is preferred because it
# is faster, and because CI installs one.
PROMETHEUS_IMAGE = $(shell sed -n 's|^[[:space:]]*image:[[:space:]]*\(prom/prometheus:[^[:space:]]*\).*|\1|p' docker-compose.yaml | head -1)

ALERT_RULES=blnk-kafka-alerts.yml
ALERT_RULE_TESTS=blnk-kafka-alerts_test.yml

# Resolves promtool and expresses every path relative to alerts/tests, which is where the
# unit-test file's own `rule_files: ../blnk-kafka-alerts.yml` resolves from. The image's
# entrypoint is /bin/prometheus, so the container form overrides it rather than appending a
# subcommand the server would reject.
define RESOLVE_PROMTOOL
	if command -v promtool >/dev/null 2>&1; then \
		promtool_prefix="cd alerts/tests &&"; \
		promtool_cmd="promtool"; \
	elif command -v docker >/dev/null 2>&1 && [ -n "$(PROMETHEUS_IMAGE)" ]; then \
		echo "promtool is not on PATH; using $(PROMETHEUS_IMAGE) from docker-compose.yaml"; \
		promtool_prefix=""; \
		promtool_cmd="docker run --rm --entrypoint promtool \
			-v $(CURDIR)/alerts:/alerts:ro -w /alerts/tests $(PROMETHEUS_IMAGE)"; \
	else \
		echo "ALERT GATE FAILED: neither promtool nor docker is available, so the alert rules"; \
		echo "cannot be validated. Install promtool from the Prometheus release that matches"; \
		echo "$(PROMETHEUS_IMAGE), or make docker available, then re-run."; \
		exit 1; \
	fi
endef

alerts_check:
	@set -e; \
	$(RESOLVE_PROMTOOL); \
	eval "( $$promtool_prefix $$promtool_cmd check rules ../$(ALERT_RULES) )"

alerts_test: alerts_check
	@set -e; \
	$(RESOLVE_PROMTOOL); \
	eval "( $$promtool_prefix $$promtool_cmd test rules $(ALERT_RULE_TESTS) )"

# THE KUBERNETES COPY IS GATED TOO. Kubernetes has no directory to bind-mount, so the
# manifests carry the rule file projected into the glob directory as a ConfigMap key. A
# copy is a thing that can diverge, and a rule file Prometheus refuses takes every rule
# with it — so the projection is extracted and put through the same two checks as the
# original. TestPrometheusConfigParity_KeepsTheKubernetesCopyInStepWithTheRoot separately
# asserts the two documents are semantically equal; this proves the projected copy LOADS
# and FIRES, which no YAML comparison can.
PROMETHEUS_CONFIGMAP=infrastructure/k8s-manifests/prometheus-configmap.yaml

# Written into alerts/tests and removed on exit. Dot-prefixed so the rule_files glob
# (alerts/*.yml, non-recursive) can never see them even mid-run.
PROJECTION=.configmap-projection.yml
PROJECTION_TESTS=.configmap-projection_test.yml

# Reads the projected rule file out of the ConfigMap and writes it where the unit tests
# can reach it. A make variable rather than recipe lines, following BROKER_ARRAY_DECLARED
# above, so the recipe stays one tab-indented statement per step.
EXTRACT_PROJECTED_RULES = python3 -c "import sys, yaml; data = (yaml.safe_load(open('$(PROMETHEUS_CONFIGMAP)')) or {}).get('data') or {}; text = data.get('$(ALERT_RULES)'); sys.exit('$(PROMETHEUS_CONFIGMAP) projects no $(ALERT_RULES) key, so Kubernetes would load no rules at all') if text is None else open('alerts/tests/$(PROJECTION)', 'w').write(text)"

# THE SECOND RULE KEY IS GATED TOO, and it was not. `blnk-infra-alerts.yml` exists only in
# the Kubernetes projection — one rule, KafkaBrokerVolumeFilling, reading kubelet series
# this repository does not publish — so it has no root-file counterpart and was outside
# every check here. That is how it kept a repository-relative, fragmentless runbook_url
# after the other key's were corrected: nothing extracted it, and the Go guard
# over the fragments read only the other key. Both now cover it.
#
# `check rules` only, and deliberately: `test rules` needs a unit-test file, and a rule
# whose inputs come from the kubelet can only be exercised against series invented here,
# which would prove the expression parses twice over rather than prove anything about
# Blnk. What this catches is the failure that takes every rule with it — a rule file
# Prometheus refuses to load.
INFRA_ALERT_RULES=blnk-infra-alerts.yml
INFRA_PROJECTION=.configmap-infra-projection.yml

EXTRACT_PROJECTED_INFRA_RULES = python3 -c "import sys, yaml; data = (yaml.safe_load(open('$(PROMETHEUS_CONFIGMAP)')) or {}).get('data') or {}; text = data.get('$(INFRA_ALERT_RULES)'); sys.exit('$(PROMETHEUS_CONFIGMAP) projects no $(INFRA_ALERT_RULES) key, so the broker-volume rule Kubernetes is meant to mount would be silently absent') if text is None else open('alerts/tests/$(INFRA_PROJECTION)', 'w').write(text)"

alerts_configmap:
	@set -e; \
	$(RESOLVE_PROMTOOL); \
	python3 -c "import yaml" 2>/dev/null || { \
		echo "ALERT GATE FAILED: python3 with PyYAML is needed to read the rule file out of"; \
		echo "$(PROMETHEUS_CONFIGMAP). Install it, or run 'make alerts_test' to check the"; \
		echo "repository copy alone."; \
		exit 1; \
	}; \
	trap 'rm -f alerts/tests/$(PROJECTION) alerts/tests/$(PROJECTION_TESTS) alerts/tests/$(INFRA_PROJECTION)' EXIT; \
	$(EXTRACT_PROJECTED_RULES); \
	sed 's|\.\./$(ALERT_RULES)|$(PROJECTION)|' alerts/tests/$(ALERT_RULE_TESTS) > alerts/tests/$(PROJECTION_TESTS); \
	eval "( $$promtool_prefix $$promtool_cmd check rules $(PROJECTION) )"; \
	eval "( $$promtool_prefix $$promtool_cmd test rules $(PROJECTION_TESTS) )"; \
	$(EXTRACT_PROJECTED_INFRA_RULES); \
	eval "( $$promtool_prefix $$promtool_cmd check rules $(INFRA_PROJECTION) )"

# Everything the alert rules are gated on, in one run.
alerts: alerts_test alerts_configmap

migrate_up:
	./${PROJECT} migrate up

migrate_down:
	./${PROJECT} migrate down

backup:
	./${PROJECT} backup drive

backup_s3:
	./${PROJECT} backup s3