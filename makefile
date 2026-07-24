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

RECON_COVERAGE_THRESHOLD=80
test:
	go test -short ./...
	@echo "==> recon-agent tests (coverage gate ${RECON_COVERAGE_THRESHOLD}%)"
	@cd recon-agent && go test -covermode=atomic -coverprofile=coverage.out ./... && \
		total=$$(go tool cover -func=coverage.out | awk '/^total:/ {print $$3}' | tr -d '%'); \
		echo "recon-agent total coverage: $$total%"; \
		awk -v c="$$total" -v t="${RECON_COVERAGE_THRESHOLD}" 'BEGIN { exit (c+0 < t+0) ? 1 : 0 }' \
			|| { echo "COVERAGE GATE FAILED: recon-agent $$total% is below ${RECON_COVERAGE_THRESHOLD}%"; exit 1; }

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

build_run:
	make build
	make run

build_test_run:
	make build
	make test
	make run

migrate_up:
	./${PROJECT} migrate up

migrate_down:
	./${PROJECT} migrate down

backup:
	./${PROJECT} backup drive

backup_s3:
	./${PROJECT} backup s3

# ---- recon-agent feature targets ----

# These targets are phony. In particular `seed` shares its name with the
# seed/ directory, so without .PHONY `make seed` would be treated as
# "up to date" and silently skip its recipe. `demo` and `test` are likewise
# not file-producing targets.
.PHONY: seed demo test

# Load internal ledger/balances/transactions and upload the 6-break external CSV into Blnk via its HTTP API.
seed:
	set -a; [ -f .env ] && . ./.env; set +a; \
	go run ./seed

# Run the recon-agent pipeline end-to-end (ingest → derive breaks → classify → gate → remediate/escalate → audit)
# and print the summary table (breaks in / auto-resolved / escalated / audit count). Must finish within 60s;
# runs in one-shot mode (does NOT block on the HITL server).
demo:
	set -a; [ -f .env ] && . ./.env; set +a; \
	cd recon-agent && go run ./cmd -once
