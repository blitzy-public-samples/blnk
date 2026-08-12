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
MUTATION_EVENT_SCOPES=.:event database:event

# Per-mutant test timeout, as a multiple of the measured baseline. 8 for every scope,
# and raising it for the event scopes was TRIED AND REJECTED on evidence.
MUTATION_TIMEOUT_COEFFICIENT=8

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
GREMLINS_VERSION=v0.6.0

mutation_gate:
	@command -v gremlins >/dev/null 2>&1 || go install github.com/go-gremlins/gremlins/cmd/gremlins@${GREMLINS_VERSION}
	@set -e; \
	if [ -z "${SCOPES}" ]; then \
		echo "mutation_gate is not a target to invoke directly: it scores whatever SCOPES names,"; \
		echo "and nothing named any. Use 'make mutate', 'make mutate_events' or 'make mutate_all'."; \
		exit 1; \
	fi; \
	gremlins_bin=$$(command -v gremlins 2>/dev/null || true); \
	if [ -z "$$gremlins_bin" ]; then gremlins_bin="$$(go env GOPATH)/bin/gremlins"; fi; \
	if [ ! -x "$$gremlins_bin" ]; then \
		echo "MUTATION GATE FAILED: gremlins is not executable at $$gremlins_bin. It was just"; \
		echo "installed into \$$(go env GOPATH)/bin, which is not on this shell's PATH — add it"; \
		echo "(export PATH=\"\$$(go env GOPATH)/bin:\$$PATH\") and re-run."; \
		exit 1; \
	fi; \
	echo "using gremlins at $$gremlins_bin"; \
	export GOFLAGS="-p=1 $$GOFLAGS"; \
	for scope in ${SCOPES}; do \
		pkg=$${scope%%:*}; \
		filter=$${scope##*:}; \
		excludes=""; \
		coefficient=${MUTATION_TIMEOUT_COEFFICIENT}; \
		if [ "$$filter" = "event" ]; then \
			excludes="-E /"; \
			for f in $$(cd $$pkg && ls *.go | grep -v '_test\.go$$' | grep -v '^event_'); do \
				excludes="$$excludes -E $$f"; \
			done; \
			if [ -z "$$(cd $$pkg && ls event_*.go 2>/dev/null | grep -v '_test\.go$$')" ]; then \
				echo "MUTATION GATE FAILED: scope $$scope asks for event files in $$pkg and there"; \
				echo "are none. A renamed or relocated event file would otherwise make this scope"; \
				echo "score nothing and report success."; \
				exit 1; \
			fi; \
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

# Start the SERVER PROCESS ROLE, which is what hosts the event outbox relay.
#
# and because it is the name the operations runbook documents. It answers "where does
# the relay run" without reading cmd/server.go.
#
#   it tested the config file with `grep '"brokers"'`, which reads `"brokers": []` as configured;
#
#   and no first-non-empty scan can express that an empty value at a higher-precedence name CLEARS
#   what a lower one supplied, because that is a property of the overlay and not of any one source.
#
# A DELIBERATELY EMPTY VALUE FROM THE CALLER WINS TOO, because `export -p` records a
# set-but- empty variable: `KAFKA_BROKERS= make run_relay` means "no brokers for this
# run", so it is refused rather than quietly falling back to .env.
# docs/kafka-operations.md documents that behaviour by example and it is a property of
# the replay, not a special case in the guard.
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
#   The ADMIN PRINCIPAL AND SECRET. KAFKA_SASL_ADMIN_USER and KAFKA_SASL_ADMIN_SECRET are
#   forwarded, never defaulted. That principal is a broker super-user — it mints SCRAM
#   credentials and rewrites every ACL — and the SASL listener is published on the host, so a
#   default committed here would hand anyone who can reach that port the cluster's entire
#   authorization model. `./stack.sh --init` generates both into a mode-0600 .env, which is
#   exactly where the sourcing above picks them up; without them the script refuses before
#   touching the broker and names the remedy.
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

migrate_up:
	./${PROJECT} migrate up

migrate_down:
	./${PROJECT} migrate down

backup:
	./${PROJECT} backup drive

backup_s3:
	./${PROJECT} backup s3