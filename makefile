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

init:
	go get ./...

generate:
	go generate ./...

test:
	go test -short  ./...

# Mutation testing on the money-critical fast packages.
# Plants hundreds of one-line bugs and re-runs the tests against each one;
# a "LIVED" mutant is a bug the test suite would not catch. Fails when test
# efficacy (killed/viable mutants) drops below the threshold. Takes ~10 min.
# Triage survivors by reading the LIVED lines; model/mutation_killers_test.go
# has examples of boundary tests written to kill them.
# (The threshold is enforced here by parsing the score: gremlins' own
# --threshold-efficacy flag does not affect its exit code.)
#
# WHICH EVENT-STREAMING CODE THIS SCORES, AND WHICH IT DOES NOT
#
# `model` covers model/event.go: the LedgerEvent contract, the status and
# category vocabularies, the zero-loss audit arithmetic and the subscriber
# key-scope authorization predicates. Those are the money-critical parts of the
# event pipeline and they are scored here.
#
# `internal/apierror` covers the error-code table, including the GEN_GONE
# mapping the post-sunset 410 depends on: an unmapped code silently becomes a
# 500, so the mapping is exactly the kind of one-line fact a mutant can flip
# without any test noticing.
#
# The ROOT package's event_*.go files are DELIBERATELY NOT a target, and the
# reason is arithmetic rather than preference. gremlins re-runs the covering
# test binary once per mutant; the root package's suite takes ~90 seconds, and
# the event files carry several hundred mutants, so one pass would run for many
# hours. A gate documented as taking ten minutes cannot host it. Those files are
# covered instead by the unit, live-broker and real-database tests beside them,
# and their behavioural coverage is asserted there rather than scored here.
#
# A statement whose ONLY coverage comes from another package is reported
# NOT COVERED and is never scored, because gremlins scores a package by running
# that package's own tests. Eleven functions in model/event.go were in that
# position — exercised only from the root package — which is why
# model/event_broker_record_test.go and model/event_key_scope_test.go exist: to
# bring them inside the gate rather than merely inside the suite.
#
# WHAT THE RESIDUAL "NOT COVERED" COUNT MEANS, so nobody chases it
#
# A handful of mutants in model/event.go stay NOT COVERED no matter what tests
# are written, and the reason is structural rather than a gap. Go's cover tool
# emits a ZERO-STATEMENT block for an empty `case` clause — the character-class
# arms of CanonicalizeSubscriberIdentifier and isLegalKafkaTopicName are all
# empty-bodied — and no block at all for a const declaration. gremlins matches a
# mutant to a coverage block, so a mutant sitting on either kind is reported NOT
# COVERED even though the line is evaluated on every call. Those arms ARE tested,
# behaviourally, in model/event_predicates_test.go; they just cannot be scored.
#
# One mutant in model LIVES and always will: the `>=` in
# EventOutboxAudit.UnconfirmedRows. Weakening it to `>` produces identical
# behaviour, because when the two counts are equal the fall-through subtraction
# returns zero anyway. It is a semantically equivalent mutant, not a missing test.
#
# GOFLAGS=-p=1 IS LOAD-BEARING, not tidiness. Before scoring anything gremlins
# gathers coverage by running the WHOLE module's test suite, and this repository's
# suite talks to shared Postgres, Redis, Typesense and Kafka instances. Run at
# default package parallelism, packages that write transactions concurrently trip
# assertions that other packages make over the whole table — so the gate failed
# intermittently during coverage gathering, before a single mutant was planted.
# Serialising the suite is what CI already does for the same reason
# (.github/workflows/go.yml runs `go test -race -p 1 ./...`), and it costs about
# two minutes of a ten-minute gate.
MUTATION_THRESHOLD=80
MUTATION_PACKAGES=model internal/filter internal/apierror
mutate:
	@command -v gremlins >/dev/null 2>&1 || go install github.com/go-gremlins/gremlins/cmd/gremlins@latest
	@set -e; \
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
	for pkg in ${MUTATION_PACKAGES}; do \
		echo "==> mutation testing $$pkg (threshold ${MUTATION_THRESHOLD}%)"; \
		log=/tmp/gremlins-$$(basename $$pkg).log; \
		status=0; \
		(cd $$pkg && "$$gremlins_bin" unleash --workers 2 --timeout-coefficient 8) > $$log 2>&1 || status=$$?; \
		cat $$log; \
		eff=$$(grep -o 'Test efficacy: [0-9.]*' $$log | grep -o '[0-9.]*'); \
		if [ -z "$$eff" ]; then \
			echo "MUTATION GATE FAILED: gremlins did not report a score for $$pkg (exit $$status)."; \
			echo "This is a RUN failure, not a low score — the run above did not complete. The"; \
			echo "usual cause is that coverage gathering failed because some package's tests are"; \
			echo "red: gremlins runs the whole module's suite to gather coverage, so \`go test ./...\`"; \
			echo "must be green before the gate can score anything. See $$log."; \
			exit 1; \
		fi; \
		awk -v e="$$eff" -v t="${MUTATION_THRESHOLD}" 'BEGIN { exit (e+0 < t+0) ? 1 : 0 }' \
			|| { echo "MUTATION GATE FAILED: $$pkg efficacy $$eff% is below ${MUTATION_THRESHOLD}% — see LIVED lines in $$log"; exit 1; }; \
		echo "PASS: $$pkg efficacy $$eff%"; \
	done

build:
	go build -o ${PROJECT} ./cmd/*.go

docker_run:
	docker run -v `pwd`/blnk.json:/blnk.json -p 4300:4100 jerryenebeli/blnk:main

run:
	./${PROJECT} start

run_workers:
	./${PROJECT} workers

# Run the process that hosts the EVENT OUTBOX RELAY.
#
# There is deliberately no separate relay binary, and therefore no relay SUBCOMMAND for this
# target to invoke: cmd/main.go registers exactly start, workers, migrate and verify-chain, so
# `./${PROJECT} start` is not a stand-in for a relay command, it IS how the relay is run. The
# relay is started by the SERVER role — cmd/server.go calls startEventRelay immediately after
# the outbox background work it is modelled on, the fund-lineage outbox processor, which is
# this repository's established home for an outbox relay and avoids standing up a fourth asynq
# server for one poll loop. That call is CONDITIONAL ON BROKERS BEING CONFIGURED: with an empty
# broker list it logs one info line and starts nothing.
#
# So this target is an alias for the server role, and it exists for two reasons. It answers
# "where does the relay run" without reading cmd/server.go, and it is the entry point for
# running the relay in ISOLATION — a local operator watching a backlog drain, or a load test
# measuring publish throughput — both of which want the publishing process up and nothing else
# claiming the outbox rows they are measuring.
#
# KAFKA_BROKERS is checked FIRST because the failure it prevents is silent: with no brokers
# the publisher resolves to the no-op, the relay refuses to start, and the server comes up
# looking entirely healthy while every captured event stays pending in blnk.event_outbox.
# A missing variable is worth one line of refusal here rather than a backlog discovered later.
run_relay:
	@if [ -z "$${KAFKA_BROKERS}" ]; then \
		echo "KAFKA_BROKERS is not set, so the relay would not start and every captured event would"; \
		echo "stay pending in blnk.event_outbox. Set it (and the KAFKA_SASL_USER/KAFKA_SASL_SECRET"; \
		echo "producer pair) first — './stack.sh --init' writes them to a 0600 .env."; \
		exit 1; \
	fi
	./${PROJECT} start

build_run:
	make build
	make run

build_test_run:
	make build
	make test
	make run

# Provision a RUNNING Kafka broker for event streaming: the category topics and their
# dead-letter siblings, the steady-state producer principal, the sample subscriber principal,
# and both principals' ACLs.
#
# WHAT IT CREATES, spelled out here so the shipped geometry is legible at the invocation site
# rather than only inside a four-thousand-line script. Every name derives from
# KAFKA_TOPIC_PREFIX (default "blnk"), and every dead-letter name is its category plus ".dlt",
# exactly as event_topics.go's DLTFor composes it:
#
#     blnk.transactions      blnk.balances      blnk.identities
#     blnk.ledgers           blnk.system
#     blnk.transactions.dlt  blnk.balances.dlt  blnk.identities.dlt
#     blnk.ledgers.dlt       blnk.system.dlt
#
# Ten owned topics, each at KAFKA_MIN_PARTITIONS partitions — 6 is the required minimum. An
# EMPTY topic below that count is grown to it; one that already holds messages is reported and
# left alone, because adding partitions re-maps keys and would split an aggregate's history
# across two of them, so that growth needs a deliberate KAFKA_ALLOW_PARTITION_GROWTH and a
# planned migration.
#
# Then two principals. The steady-state PRODUCER (KAFKA_PRODUCER_USER, default blnk-producer) is
# granted Write and Describe on those topics and nothing else — no Read, no consumer group, no
# cluster operation. One sample SUBSCRIBER (KAFKA_SAMPLE_SUBSCRIBER_USER, default
# blnk-sample-subscriber) is granted the mirror image: Read and Describe on the topics it may
# consume, and Read on its own prefixed consumer-group namespace. Those ACLs are what the
# subscriber-isolation test asserts against, and they are only enforced because the broker runs
# the KRaft StandardAuthorizer — without it ACLs are accepted and ignored.
#
# FIVE categories, not the three the requirement names, because ledger.created and system.error
# belong to none of transactions, balances and identities while every event formerly delivered
# by webhook must still be published. ledger.created is ordinary ledger data a webhook
# subscriber receives today, so it needs a GRANTABLE home; system.error carries Blnk's own error
# text and must stay ungrantable. Hence blnk.ledgers and blnk.system. model.EventCategory routes
# events into exactly these five and event_topics.go composes exactly these ten names, so do not
# "correct" the count here without changing both.
#
# REPLICATION FACTOR IS 1 LOCALLY AND 3 IN PRODUCTION, which is the whole reason it is a
# variable. A single-broker KRaft cluster cannot satisfy 3 — topic creation fails outright — so
# a hardcoded 3 would make local bring-up impossible, while a hardcoded 1 would silently ship
# unreplicated topics onto a multi-broker cluster, where losing one broker loses events. So
# KAFKA_REPLICATION_FACTOR carries it: 1 below and in .env.example, 3 in the Go default
# (config/config.go) and in the Kubernetes manifests. The script reads every topic's factor back
# from the broker and fails the run if one is below the configured value, so a local 1 cannot
# quietly survive a promotion to production.
#
# Idempotent, so re-running it after a bring-up is safe and is the normal way to repair a
# drifted topic geometry or ACL. It only ever adds: no topic is deleted, no partition count is
# reduced, and no existing credential is replaced unless a KAFKA_ROTATE_* variable asks for it.
#
# NEITHER CREDENTIAL IS GENERATED. Both principals are provisioned only from a secret you
# supply, because a generated one has to be printed to be usable and this script's usual home
# is the compose kafka-init one-shot, whose stdout Docker captures into a container log. So:
#
#     KAFKA_PRODUCER_SECRET=<secret> make kafka_provision
#     KAFKA_SAMPLE_SUBSCRIBER_SECRET=<secret> make kafka_provision
#
# `./stack.sh --init` generates KAFKA_SASL_SECRET into a 0600 .env, and the script reads that
# variable directly as the producer secret — so after --init the producer principal needs no
# extra argument, and the application and the broker are configured from one value.
#
# The script finds the Kafka CLI or delegates into the broker container itself, so this works
# whether or not a Kafka distribution is installed on the host.
# .env SUPPLIES DEFAULTS; THE COMMAND LINE WINS. That ordering is the whole of this recipe.
#
# It used to be `set -a; [ -f .env ] && . ./.env; set +a`, which sources .env AFTER the caller's
# environment already exists - so every key .env declares silently overwrote the value the
# caller had just passed. `KAFKA_BROKERS=localhost:29092 make kafka_provision` against a .env
# that leaves KAFKA_BROKERS empty reported "KAFKA_BROKERS is declared but empty, so Kafka is not
# configured here" and did nothing, while the comment block above advertises exactly that form
# for the two secrets. Only keys absent from .env got through, which is the most confusing
# possible half of the behaviour.
#
# The snapshot-and-restore is what fixes it without giving up on sourcing. Sourcing is worth
# keeping: .env is shell-quoted data (values with spaces, quoted assignments) and hand-parsing
# it would get those wrong. So the caller's exported environment is captured in re-inputtable
# form first, .env is sourced with allexport, and the snapshot is then replayed on top - which
# restores the caller's value for every key that appears in both and leaves .env-only keys
# alone. This is Compose's own precedence (the shell wins over --env-file) and the same rule
# stack.sh applies to the value it captures before sourcing.
#
# A deliberately EMPTY value from the caller wins too, because `export -p` records a set-but-
# empty variable: `KAFKA_SKIP_SAMPLE_SUBSCRIBER= make kafka_provision` means "not set", not
# "fall back to .env".
#
# THE THREE `${VAR:-default}` FALLBACKS BELOW ARE THE LAST RESORT, and their position in the
# recipe is what makes them one: they run AFTER the snapshot is replayed, so they fill only what
# neither the caller nor .env supplied. Full precedence, highest first: the command line, then
# .env, then these. They pin the geometry this comment block claims — prefix, the 6-partition
# minimum and the local replication factor — at the invocation site, so a bare
# `make kafka_provision` provisions the documented local shape whether or not an .env exists.
#
# WHAT IS DELIBERATELY NOT DEFAULTED HERE, because in both cases a default would be a bug:
#
#   The BROKER LIST. Left entirely to KAFKA_BROKERS and the script's own resolution. Defaulting
#   it would mean writing KAFKA_BOOTSTRAP_SERVER, which the script reads as an explicit override
#   of its "KAFKA_BROKERS is declared but empty, so Kafka is not configured here" skip — the
#   check that answers a Kafka-less deployment instantly and with an explanation. Overriding it
#   would replace that with a sixty-second readiness timeout against a broker nobody started, on
#   every checkout that has not opted in. Opting into the local Kafka stack means setting
#   KAFKA_BROKERS (and selecting the `kafka` compose profile — .env.example says so at both
#   keys), so once an operator has opted in, the bare form already reaches their broker. To
#   provision one broker without configuring the application for it, name it for the one run:
#   `KAFKA_BOOTSTRAP_SERVER=localhost:9092 make kafka_provision`.
#
#   The ADMIN PRINCIPAL AND SECRET. KAFKA_SASL_ADMIN_USER and KAFKA_SASL_ADMIN_SECRET are
#   forwarded, never defaulted. That principal is a broker super-user — it mints SCRAM
#   credentials and rewrites every ACL — and the SASL listener is published on the host, so a
#   default committed here would hand anyone who can reach that port the cluster's entire
#   authorization model. `./stack.sh --init` generates both into a mode-0600 .env, which is
#   exactly where the sourcing above picks them up; without them the script refuses before
#   touching the broker and names the remedy.
#
# Everything else the script accepts is forwarded by inheritance rather than enumerated, because
# the script publishes its own interface: `scripts/kafka-provision.sh --print-interface-host`
# prints every variable it reads, which is how stack.sh builds its passthrough list. A list
# copied into this recipe would be a second copy to keep in step, and a name missing from it
# would fail silently and plausibly.
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