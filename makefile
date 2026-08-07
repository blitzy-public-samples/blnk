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

# Mutation testing on the money-critical fast packages (model, filter).
# Plants hundreds of one-line bugs and re-runs the tests against each one;
# a "LIVED" mutant is a bug the test suite would not catch. Fails when test
# efficacy (killed/viable mutants) drops below the threshold. Takes ~10 min.
# Triage survivors by reading the LIVED lines; model/mutation_killers_test.go
# has examples of boundary tests written to kill them.
# (The threshold is enforced here by parsing the score: gremlins' own
# --threshold-efficacy flag does not affect its exit code.)
MUTATION_THRESHOLD=80
mutate:
	@command -v gremlins >/dev/null 2>&1 || go install github.com/go-gremlins/gremlins/cmd/gremlins@latest
	@for pkg in model internal/filter; do \
		echo "==> mutation testing $$pkg (threshold ${MUTATION_THRESHOLD}%)"; \
		log=/tmp/gremlins-$$(basename $$pkg).log; \
		(cd $$pkg && gremlins unleash --workers 2 --timeout-coefficient 8) | tee $$log; \
		eff=$$(grep -o 'Test efficacy: [0-9.]*' $$log | grep -o '[0-9.]*'); \
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
# There is deliberately no separate relay binary. The relay is started by the server role,
# beside the fund-lineage outbox processor it is modelled on, which is the repository's
# established home for an outbox relay and avoids standing up a fourth asynq server for one
# poll loop. So this target runs the server — and exists because "where does the relay run"
# is otherwise answerable only by reading cmd/server.go.
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
# Idempotent, so re-running it after a bring-up is safe and is the normal way to repair a
# drifted topic geometry or ACL.
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
kafka_provision:
	@set -a; [ -f .env ] && . ./.env; set +a; ./scripts/kafka-provision.sh

migrate_up:
	./${PROJECT} migrate up

migrate_down:
	./${PROJECT} migrate down

backup:
	./${PROJECT} backup drive

backup_s3:
	./${PROJECT} backup s3