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

# Recon-agent coverage floor (Rule 5.9): the whole recon-agent module is the
# authorized scope (AAP §0.6.1 authorizes recon-agent/** — including cmd/** — and
# all *_test.go files within), so coverage over ./... in that module is coverage
# over authorized files only.
RECON_COVERAGE_THRESHOLD=80
# Global wall-clock budget for `make demo` (AAP success criterion: <= 60s).
DEMO_TIMEOUT=60
# Address the demo's bundled deterministic LLM stub binds to. It is overridable
# so a busy port can be avoided; the demo points the agent's LLM_BASE_URL here.
DEMO_LLM_ADDR=127.0.0.1:11434

# Run the recon-agent pipeline end-to-end (ingest → derive breaks → classify → gate → remediate/escalate → audit),
# print the summary table (breaks in / auto-resolved / escalated / audit count), and ASSERT the AAP acceptance
# criteria via the canonical scorer, exiting NONZERO on any failed criterion (findings M-01, M-17).
#
# Determinism: the demo launches the bundled OpenAI-compatible stub LLM
# (eval/llmstub) and points the agent's LLM_BASE_URL at it, so classifications
# — and therefore the auto-resolve/escalate outcomes — are reproducible and the
# scorer's hard gates (exactly six, >=5 labels, >=1 Blnk-confirmed clearance,
# routing safety, audit parity) are meaningful. Blnk still DECIDES every
# clearance via its own dry-run reconciliation (Rule 5.3); the stub only supplies
# the language-inference step. Production inference uses the real LLM_BASE_URL
# from the environment (docker-compose / .env), not this stub.
#
# A hard <=$(DEMO_TIMEOUT)s deadline bounds the pipeline. The stub is started in
# the background and trap-killed on exit. Requires a reachable Blnk (BLNK_BASE_URL)
# and the agent DB (AGENT_DATABASE_URL), and that `make seed` has established the
# internal ledger. Runs one-shot mode (does NOT block on the HITL server).
demo:
	set -a; [ -f .env ] && . ./.env; set +a; \
	stub=$$(mktemp) || exit 1; \
	go build -o "$$stub" ./eval/llmstub || { echo "demo: failed to build llmstub"; rm -f "$$stub"; exit 1; }; \
	"$$stub" -addr $(DEMO_LLM_ADDR) & stubpid=$$!; \
	trap 'kill $$stubpid 2>/dev/null; rm -f "$$stub"' EXIT INT TERM; \
	ready=0; for i in $$(seq 1 50); do \
		kill -0 $$stubpid 2>/dev/null || { echo "demo: the stub we launched exited early — is $(DEMO_LLM_ADDR) already in use?"; exit 1; }; \
		if curl -sf http://$(DEMO_LLM_ADDR)/healthz >/dev/null 2>&1; then ready=1; break; fi; \
		sleep 0.2; \
	done; \
	[ "$$ready" = 1 ] || { echo "demo: deterministic LLM stub did not become ready on $(DEMO_LLM_ADDR)"; exit 1; }; \
	echo "demo: running pipeline (<=$(DEMO_TIMEOUT)s) against Blnk with the deterministic stub LLM"; \
	( cd recon-agent && LLM_BASE_URL=http://$(DEMO_LLM_ADDR)/v1 timeout $(DEMO_TIMEOUT) go run ./cmd -once ); \
	rc=$$?; \
	[ $$rc -eq 0 ] || { echo "demo: pipeline failed or timed out (exit $$rc)"; exit 1; }; \
	echo "demo: scoring acceptance criteria (canonical scorer)"; \
	go run ./eval/scorer \
		-corpus eval/recon_corpus.jsonl \
		-resolved recon-agent/recon_resolved.jsonl \
		-summary recon-agent/recon_summary.json \
		-csv seed/external_transactions.csv

# Run the full test suite with the recon-agent coverage gate ALWAYS evaluated
# (finding M-02). Order and behavior:
#   1. recon-agent UNIT tests with a TRAPPED, temporary coverage profile (never
#      leaks coverage.out into the tree) and a >=$(RECON_COVERAGE_THRESHOLD)% gate
#      over the authorized module (Rule 5.9). Run FIRST so an unrelated Blnk root
#      test failure can never prevent the feature gate from being reached.
#   2. recon-agent TAGGED INTEGRATION suite with SKIP DETECTION: a skipped
#      integration test (e.g. missing BLNK_BASE_URL / AGENT_DATABASE_URL /
#      unreachable Blnk) is treated as a FAILURE so it cannot false-green the
#      suite (Gate 10 requires the end-to-end test to actually run).
#   3. Blnk ROOT unit tests with a CORRECTLY SCOPED environment: only
#      BLNK_TYPESENSE_DNS is set and BLNK_DATA_SOURCE_DNS is explicitly UNSET, so
#      the config-package file-load tests do not fail as an environment artifact.
test:
	@echo "==> recon-agent unit tests + >=$(RECON_COVERAGE_THRESHOLD)% coverage gate (trapped temp profile)"
	@cd recon-agent && cov=$$(mktemp) || exit 1; \
		trap 'rm -f "$$cov"' EXIT INT TERM; \
		go test -covermode=atomic -coverprofile="$$cov" ./... || exit 1; \
		total=$$(go tool cover -func="$$cov" | awk '/^total:/ {print $$3}' | tr -d '%'); \
		echo "recon-agent total coverage: $$total%"; \
		awk -v c="$$total" -v t="$(RECON_COVERAGE_THRESHOLD)" 'BEGIN { exit (c+0 < t+0) ? 1 : 0 }' \
			|| { echo "COVERAGE GATE FAILED: recon-agent $$total% is below $(RECON_COVERAGE_THRESHOLD)%"; exit 1; }
	@echo "==> recon-agent tagged integration suite (skip detection: a SKIP fails the suite)"
	@cd recon-agent && out=$$(mktemp) || exit 1; \
		trap 'rm -f "$$out"' EXIT INT TERM; \
		go test -tags integration -count=1 ./internal/ -v > "$$out" 2>&1; rc=$$?; \
		cat "$$out"; \
		[ $$rc -eq 0 ] || { echo "INTEGRATION SUITE FAILED (exit $$rc)"; exit 1; }; \
		if grep -q -- '--- SKIP' "$$out"; then \
			echo "INTEGRATION SUITE SKIPPED — refusing to false-green (set BLNK_BASE_URL + AGENT_DATABASE_URL and start Blnk; Gate 10)"; \
			exit 1; \
		fi; \
		grep -q -- '--- PASS' "$$out" || { echo "INTEGRATION SUITE did not execute any test"; exit 1; }
	@echo "==> Blnk root unit tests (scoped env: typesense only; DATA_SOURCE_DNS unset)"
	env -u BLNK_DATA_SOURCE_DNS BLNK_TYPESENSE_DNS=$${BLNK_TYPESENSE_DNS:-http://localhost:8108} go test -short -p 1 ./...
