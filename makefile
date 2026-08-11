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
# `go get ./...` RESOLVES and can WRITE go.mod and go.sum: it upgrades a requirement to a newer
# version that satisfies the same import, adds anything missing and rewrites the manifests as a
# side effect of what was supposed to be a fetch. So the first thing a new contributor ran, on a
# repository whose whole dependency contract is `go.mod` plus `go.sum`, was the one command able
# to change it — and a bumped version arriving in an unrelated pull request is indistinguishable
# from a deliberate upgrade.
#
# `go mod download` fetches exactly what the manifests pin and writes neither. `go mod verify`
# then checks every module in the cache against its recorded go.sum hash, which is the supply
# chain check the download alone does not perform. Upgrading a dependency is a deliberate
# `go get <module>@<version>` followed by `go mod tidy`, reviewed as its own change.
init:
	go mod download
	go mod verify

generate:
	go generate ./...

test:
	go test -short  ./...

# Mutation testing on the money-critical fast packages (model, filter).
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
# The EVENT AND OUTBOX code IS scored, at this same threshold, by the separate
# `mutate_events` target. The AAP requires it (§0.7.2: new code in the event and
# outbox paths must meet the same bar as existing money-critical packages), and
# it is a separate target for a measured reason rather than an excuse. A dry run
# of the two event scopes reports:
#
#   .        1,140 runnable, 147 not covered, 88.58% mutator coverage
#            (event_admin 408, event_subscriber 197, event_dlt 165,
#            event_relay 137, event_publisher 114, event_outbox 72,
#            event_metrics 49, event_sunset 38, event_retention 30,
#            event_topics 28, event_metrics_support 26,
#            event_publisher_telemetry 23)
#   database 218 runnable, 66 not covered, 76.76% mutator coverage
#            (event_outbox 180, event_subscriber 104 planted)
#
# gremlins re-runs the mutated file's PACKAGE test binary once per mutant. On a
# 4-CPU machine the root suite takes ~46s under -short and ~120s without it, and
# database's ~33s and ~45s. 1,358 runnable mutants at those rates is hours, not
# minutes, however many workers are used.
#
# So the split is by RUNTIME, not by importance: `mutate` stays the ten-minute
# gate it documents itself as, `mutate_events` is the long-running one, and
# `mutate_all` runs both so that neither is silently skipped. The event target
# scores ONLY the event and outbox files — every other file in those two packages
# is excluded — so its runtime buys event coverage rather than re-scoring code
# already covered above.
#
# THE GATE LOGIC EXISTS EXACTLY ONCE, in the `mutation_gate` recipe below, which
# the three targets reach through a target-specific SCOPES variable. A second
# copy specialised for the event scopes is how the two would drift until only one
# of them enforced the threshold.
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
# Three mutants in model/event.go LIVE and always will, and all three sit on the
# same shape: a guard that clamps an arithmetic result at zero, in
# PartitionOffsetInterval.Records, EventRecordIntervalAudit.UncorroboratedRows and
# EventRecordIntervalAudit.DuplicatedRecords. Each reads `if <value> < 0 { return 0 }`
# (or `<=` against the interval's own lower bound), and relaxing the comparison by
# one produces identical behaviour, because in the single case the mutation newly
# admits the value is exactly zero and the fall-through returns zero anyway. They are
# semantically equivalent mutants, not missing tests, so no boundary test can kill
# them: the two branches compute the same answer. The clamps are kept because they
# state the invariant the callers rely on — none of the three quantities can be
# negative — where a reader looks for it.
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

# A SCOPE is "directory:filter".
#
#   filter=all   score every file gremlins finds from that directory down
#   filter=event score only that directory's OWN event/outbox files
#
# `-E /` IS LOAD-BEARING, and it is the whole reason filter=event is not simply a
# list of files to skip. `gremlins unleash` walks the directory tree DOWNWARD from
# where it runs, not just the package it was pointed at — run from the repository
# root it plants mutants in api/, cmd/, database/, internal/ and model/ too, and a
# scope that only listed the root's own non-event files would silently score the
# entire module. Excluding any filepath containing a slash confines the run to the
# directory's own files, and the per-file excludes then narrow that to the event
# ones. This is invisible for the three filter=all scopes because none of them has
# a subpackage.
#
# The filter exists because the event code lives in two packages it shares with a
# great deal of other code — the repository root and database/ — and the AAP fixes
# those paths (§0.5.1 lists event_publisher.go, event_relay.go, event_dlt.go and
# the rest at the repository ROOT, and database/event_outbox.go alongside the
# other repositories). Extracting them into packages of their own to make them
# separately scorable is therefore not available: it would move files the AAP
# places. Filtering the mutant set is how they get scored where they are.
MUTATION_FAST_SCOPES=model:all internal/filter:all internal/apierror:all
MUTATION_EVENT_SCOPES=.:event database:event

# Per-mutant test timeout, as a multiple of the measured baseline. 8 for every
# scope, and raising it for the event scopes was TRIED AND REJECTED on evidence.
#
# database:event at coefficient 8 completes in 13 minutes and reports Killed 32,
# Lived 0, Not covered 66, TIMED OUT 186. The timeouts are not a tight-timeout
# artefact: at coefficient 40 the same scope had not reported a single mutant after
# 40 minutes. They are genuine hangs — this scope mutates the outbox claim query and
# the retry loops, against a REAL PostgreSQL, so a mutant that inverts a loop
# condition or a lock predicate blocks on a row lock rather than failing. Five times
# the patience buys five times the waiting and the same verdict.
#
# So 8 is the operating point, and the honest consequence is reported rather than
# hidden: see the inconclusive-mutant report in the recipe. A TIMED OUT mutant is
# EXCLUDED from gremlins' efficacy ratio rather than counted as survived, so a green
# efficacy line on this scope describes the CONCLUSIVE subset and the recipe says so
# in as many words. What still fails the build is what should: an efficacy below the
# threshold, and a run that produced no score at all.
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
#
# `@latest` resolves at install time, so whatever the upstream module publishes next runs
# HERE — on a developer's machine and in CI — with the module cache's own privileges and no
# review in between. It also makes two runs of the same commit score differently the moment
# upstream changes a mutator or a default, so a gate that failed cannot be reproduced and a
# gate that passed cannot be trusted.
#
# v0.6.0 is the current release of github.com/go-gremlins/gremlins and is the version the
# thresholds below were measured against. Moving it is a deliberate change: bump this
# constant, re-measure the efficacy figures documented above MUTATION_THRESHOLD, and update
# them in the same commit — a mutator added upstream changes the denominator, so an unchanged
# threshold against a new version is a different gate wearing the same number.
#
# The checksum is not restated here because the module proxy already supplies one: go install
# verifies the module against GOSUMDB (sum.golang.org) for a version not present in go.sum,
# so a tampered artifact for this exact version fails the install rather than running.
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
# THE NAME SAYS SERVER BECAUSE A SERVER IS WHAT STARTS. This target runs `./${PROJECT} start`,
# which brings up the whole HTTP API, the lineage outbox processor, the metrics collector and
# the event relay together. It is NOT isolated relay execution, and an earlier version of this
# comment claimed it was — which mattered, because an operator who believes only a relay is
# running will not expect the API to be listening on its port, will not expect the other
# background workers to be claiming rows, and will read a load test taken against it as
# measuring the relay alone.
#
# There is deliberately no separate relay binary and no relay SUBCOMMAND to invoke instead:
# cmd/main.go registers exactly start, workers, migrate and verify-chain, and AAP §0.4.4 places
# the relay in the server role beside the fund-lineage outbox processor it is modelled on —
# this repository's established home for an outbox relay, which also avoids standing up a
# fourth asynq server for one poll loop. AAP §0.3.2 records a standalone relay command as
# OPTIONAL, so adding one would be a new process role rather than a fix to this target. Hence
# `./${PROJECT} start` is not a stand-in for a relay command; it IS how the relay is run.
#
# `run_relay` remains as an alias because AAP §0.5.1 Group 7 names that target, and because it
# is the name the operations runbook documents. It answers "where does the relay run" without
# reading cmd/server.go. Both spellings print what is actually starting.
#
# # Why the guard exists, and why the APPLICATION makes the decision
#
# startEventRelay is CONDITIONAL ON BROKERS BEING CONFIGURED: with an empty broker list it logs
# a single info line and starts nothing, so the server comes up looking entirely healthy while
# every captured event stays pending in blnk.event_outbox. That silence is the only failure this
# guard is here to convert into a refusal.
#
# THE REFUSAL IS `--require-kafka`, A FLAG ON THE BINARY, AND NOT A SHELL TEST. This recipe used
# to resolve the broker list itself, and a shell reimplementation of that resolution cannot agree
# with the loader — it did not, in three separate ways, each of which made this target ANNOUNCE a
# relay the application would then decline to start:
#
#   it took the FIRST NON-EMPTY of KAFKA_BROKERS, BLNK_KAFKA_KAFKA_BROKERS and BLNK_KAFKA_BROKERS
#   in that order, while the loader's precedence is BLNK_KAFKA_BROKERS first, then KAFKA_BROKERS,
#   then the derived key, then the file — so `BLNK_KAFKA_BROKERS= KAFKA_BROKERS=host:9092 make
#   run_relay` was announced as configured from the bare name while the application resolved NO
#   brokers, an explicitly empty higher-precedence name being exactly how Kafka is turned off for
#   one run;
#
#   it tested the config file with `grep '"brokers"'`, which reads `"brokers": []` as configured;
#
#   and no first-non-empty scan can express that an empty value at a higher-precedence name CLEARS
#   what a lower one supplied, because that is a property of the overlay and not of any one source.
#
# `./${PROJECT} start --require-kafka` asks the question after the same load, of the same struct,
# with the same predicate the relay's own gate uses — blnk.KafkaBrokersConfigured on
# cfg.Kafka.Brokers — and names every source in its refusal. One resolution, one definition of
# "configured", nothing here to drift from it. The application also owns the DEEPER validation it
# always owned: config.validateKafkaTopicPrefix refuses a prefix that cannot compose a legal topic
# name and config.validateKafkaSASLCredentials refuses a half-configured administrative principal.
#
# # WHY .env IS SOURCED, AND WHY IT IS THE FIFTH SOURCE RATHER THAN THE FIRST
#
# The application reads its configuration from the PROCESS ENVIRONMENT, and `./stack.sh --init`
# writes KAFKA_BROKERS and the producer pair into a mode-0600 .env — a file, whose assignments are
# not exported into anyone's shell. So a target that passed on only the environment refused the
# operator who had just run the setup instruction this recipe's own error message recommends,
# and pointed them at the very file it declined to read. That was the defect, and it was worse
# than a plain refusal because the remedy printed was already satisfied. Sourcing .env is
# therefore make's ONE job here: assembling the environment the application then resolves from.
#
# So .env is sourced here exactly as `kafka_provision` sources it, and the ordering is the same
# one Compose applies to --env-file: .env SUPPLIES DEFAULTS, THE CALLER'S ENVIRONMENT WINS.
#
# THE SNAPSHOT-AND-RESTORE IS WHAT ENFORCES THAT ORDERING. A bare `set -a; . ./.env; set +a`
# runs after the caller's environment already exists, so every key .env declares OVERWRITES the
# value just passed on the command line — the precedence inverted, and only for keys that
# happen to appear in both, which is the most confusing possible half of the behaviour. Sourcing
# is still the right mechanism, because .env is shell-quoted data that hand-parsing would get
# wrong. So the caller's environment is captured in re-inputtable form FIRST, .env is sourced
# with allexport, and the snapshot is replayed on top.
#
# A DELIBERATELY EMPTY VALUE FROM THE CALLER WINS TOO, because `export -p` records a set-but-
# empty variable: `KAFKA_BROKERS= make run_relay` means "no brokers for this run", so it is
# refused rather than quietly falling back to .env. docs/kafka-operations.md documents that
# behaviour by example and it is a property of the replay, not a special case in the guard.
#
# THE START HAPPENS IN THE SAME SHELL, and that is not a style choice. Each line of a recipe is
# its own shell, so a `./${PROJECT} start` on a line of its own would run with the environment
# make handed it and see NONE of what was just sourced — the guard would pass and the server
# would still come up with no brokers, which is the silent failure this target exists to
# prevent, reached by a longer route. Nothing else reads .env for the binary: configuration
# arrives through envconfig, which reads the environment and no file. `exec` replaces the shell
# so signals and the exit status reach the server directly.
CONFIG_FILE?=blnk.json

# Answers "does ${CONFIG_FILE} declare at least one USABLE Kafka broker?" through an exit
# status: 0 yes, non-zero no.
#
# WHY THIS IS NOT `grep -q '"brokers"'`
#
# That test passed on an EMPTY array. `"kafka": { "brokers": [] }` contains the string
# `"brokers"`, so the target reported the file as a configured source and started the server —
# which then resolved no writer, ran no relay, and left every captured event sitting in
# blnk.event_outbox at status pending while the API looked completely healthy. That is the exact
# failure the broker check exists to prevent, so the check has to read the ARRAY rather than the
# key. `"brokers": [""]` and `"brokers": ["  "]` fail for the same reason: a blank string is not
# an address kafka-go can dial.
#
# A VARIABLE RATHER THAN A SUB-MAKE, and that is not a style preference. The first version of
# this fix put the parse in its own target and had run_server_relay invoke it with $(MAKE) —
# which broke `make -n`. GNU make EXECUTES any recipe line containing $(MAKE) even under -n, so
# that the sub-make can print its own commands; run_server_relay's recipe is a single
# backslash-continued line, so the whole thing ran, reached `exec ./blnk start`, and a DRY RUN
# started a server. That is worse than a cosmetic bug here: a live server hosts the event relay,
# which claims blnk.event_outbox rows, and the setup notes require the server to be DOWN while
# the event and outbox suites run. Expanding a variable keeps -n a dry run.
#
# python3 first, jq second, and the fallback is the `||` rather than a `command -v` dance: an
# absent python3 exits 127, which is a failure, which falls through to jq exactly as an
# unparseable file does. If NEITHER is installed both fail and the answer is "not declared" —
# the fail-closed direction, because guessing "yes" is what produced the silent failure above,
# and KAFKA_BROKERS in the environment takes precedence over this file anyway.
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

# The AAP-named alias. Identical behaviour, including the announcement above, so neither
# spelling can leave an operator believing a bare relay is what came up.
run_relay: run_server_relay

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
#     blnk.transactions      blnk.balances      blnk.identities      blnk.system
#     blnk.transactions.dlt  blnk.balances.dlt  blnk.identities.dlt  blnk.system.dlt
#
# EIGHT owned topics, from FOUR categories. The fourth exists because two real event types —
# ledger.created and system.error — belong to none of the three the requirement names, while the
# coverage rule admits no exceptions; blnk.system takes both. It is CREATED AND NEVER GRANTED to
# a subscriber: system.error's frozen payload renders internal error text verbatim and the
# category is the catch-all, so it is an operator topic like every .dlt sibling.
# There is deliberately no fifth blnk.ledgers category: one was implemented and reverted, and
# model/event.go's catalogue is frozen at four.
#
# Each topic is created at KAFKA_MIN_PARTITIONS partitions — 6 is the required minimum. An
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
# FOUR categories, not the three the requirement names, because ledger.created and system.error
# belong to none of transactions, balances and identities while every event formerly delivered by
# webhook must still be published. Both go to blnk.system, which follows the identical naming
# convention so nothing about the scheme is special-cased. model.EventCategory routes events into
# exactly these four and event_topics.go composes exactly these eight names, so do not "correct"
# the count here without changing both.
#
# There is no fifth blnk.ledgers category. One was implemented, on the reasoning that
# ledger.created is ordinary ledger data a webhook subscriber receives today and therefore needs
# a GRANTABLE home; it was reverted because the catalogue is frozen at four. The consequence is
# recorded rather than hidden: ledger.created shares the ungrantable blnk.system topic, so it has
# no subscriber Kafka route — it stays published, observable and replayable for an operator, and
# giving it a subscriber route means revising the frozen catalogue. See docs/event-streaming.md.
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
# TWO WAYS TO SUPPLY THE PRINCIPAL SECRETS, and neither of them prints one.
#
# SUPPLIED. Put the value in the 0600 .env this recipe sources — KAFKA_PRODUCER_SECRET and
# KAFKA_SAMPLE_SUBSCRIBER_SECRET — or export it from a secret manager in the calling shell.
# Do NOT write it inline on the command line: an assignment there is visible in /proc for the
# life of the process and lands in shell history.
#
# GENERATED TO A FILE. `./stack.sh --init` generates KAFKA_SASL_SECRET into a 0600 .env, and
# the script reads that variable directly as the producer secret — so after --init the producer
# principal needs no extra argument and the application and the broker are configured from one
# value. Failing that, scripts/kafka-provision.sh will generate a missing secret itself, but
# only into a mode-0600 file you nominate through KAFKA_SASL_SECRET_FILE or
# KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE; it reports the PATH and never the value.
#
# What is never done is printing a generated secret, because this script's usual home is the
# compose kafka-init one-shot, whose stdout Docker captures into a container log that anyone
# who can read logs can read.
#
# The script finds the Kafka CLI or delegates into the broker container itself, so this works
# whether or not a Kafka distribution is installed on the host.
# .env SUPPLIES DEFAULTS; THE CALLER'S ENVIRONMENT WINS. That ordering is the whole of this
# recipe, and it is Compose's own precedence (the shell wins over --env-file) as well as the
# rule stack.sh applies.
#
# THE SNAPSHOT-AND-RESTORE IS WHAT ENFORCES IT. A plain `set -a; . ./.env; set +a` sources .env
# after the caller's environment already exists, so every key .env declares overwrites the value
# the caller just passed - and only keys absent from .env get through, which is the most
# confusing possible half of the behaviour. Sourcing is still worth keeping, because .env is
# shell-quoted data (values with spaces, quoted assignments) that hand-parsing would get wrong.
# So the caller's exported environment is captured in re-inputtable form FIRST, .env is sourced
# with allexport, and the snapshot is replayed on top - restoring the caller's value for every
# key present in both and leaving .env-only keys alone.
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
# Refuses a `kubectl apply` that would deploy an unresolved or mutable image, BEFORE the
# apply rather than at pull time. Kubernetes does not validate image references at
# admission, so `blnk:REPLACE_WITH_PINNED_DIGEST` is admitted, scheduled, and only fails
# when the kubelet pulls — by which point the Deployment is partially rolled out.
#
#   make k8s_preflight                     check the committed tree (EXPECTED to fail:
#                                          the placeholder is intentional in git)
#   make k8s_preflight DIR=./rendered      check a tree you have already rendered
#   make k8s_render BLNK_IMAGE=repo@sha256:...   render to ./rendered, then check it
#
# Neither target contacts a cluster or reads a kubeconfig, so both are safe in CI.
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