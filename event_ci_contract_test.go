/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// THE ACCEPTANCE SELECTION IS ITSELF UNDER TEST.
//
// Subscriber isolation, per-aggregate ordering, crash recovery, replay fidelity and the
// 410 sunset are proved by
// suites that need a live Kafka broker and a migrated database.
//
// That makes the job's SELECTOR a load-bearing part of the proof, and it is the one
// part nothing else could check.
//
// It had happened.
//
// The workflow now declares its families as data and checks them before using them.
package blnk

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ciWorkflowPath is the only workflow in this repository, relative to the module root.
const ciWorkflowPath = ".github/workflows/go.yml"

// ciAcceptanceFamily is one declared (package, test-name pattern) pair from the workflow.
type ciAcceptanceFamily struct {
	pkg     string
	pattern string
}

// readCIWorkflow returns the workflow source.
//
// Parameters:
//   - t *testing.T: the test, failed when the workflow is missing.
//
// Returns:
//   - string: the workflow source.
func readCIWorkflow(t *testing.T) string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(moduleRootDir(t), ciWorkflowPath))
	require.NoError(t, err, "%s must exist: it is where every acceptance suite that needs a "+
		"broker is run", ciWorkflowPath)

	return string(raw)
}

// declaredAcceptanceFamilies parses the family table out of the workflow's heredoc.
//
// Read from the workflow rather than restated here on purpose.
//
// Parameters:
//   - t *testing.T: the test, failed when the table cannot be found.
//
// Returns:
//   - []ciAcceptanceFamily: the declared families, in declaration order.
func declaredAcceptanceFamilies(t *testing.T) []ciAcceptanceFamily {
	t.Helper()

	workflow := readCIWorkflow(t)

	const open = "<<'FAMILIES'"
	const close = "FAMILIES"

	start := strings.Index(workflow, open)
	require.Positive(t, start,
		"%s must declare its acceptance families in a <<'FAMILIES' heredoc, so the selection is "+
			"data that can be checked rather than a regexp buried in a command line",
		ciWorkflowPath)

	body := workflow[start+len(open):]
	end := strings.Index(body, close)
	require.Positive(t, end, "the FAMILIES heredoc must be terminated")
	body = body[:end]

	families := make([]ciAcceptanceFamily, 0, 12)
	for _, line := range strings.Split(body, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		families = append(families, ciAcceptanceFamily{pkg: fields[0], pattern: fields[1]})
	}

	require.NotEmpty(t, families, "the acceptance family table must not be empty")

	return families
}

// packageTestNames enumerates the top-level test functions of a package by parsing its
// test files.
//
// Static enumeration rather than `go test -list`, for two reasons: it needs no build of
// the package under test, so this test cannot fail for an unrelated compilation reason
// in a sibling package, and it sees exactly what `-run` will see, which is the set of
// `func TestXxx(t *testing.T)` declarations.
//
// Parameters:
//   - t *testing.T: the test.
//   - pkg string: the package path as the workflow spells it, e.g. "." or "./database".
//
// Returns:
//   - []string: the test function names.
func packageTestNames(t *testing.T, pkg string) []string {
	t.Helper()

	dir := filepath.Join(moduleRootDir(t), filepath.Clean(strings.TrimPrefix(pkg, "./")))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "the package directory %q named in the acceptance table must exist", pkg)

	declaration := regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]*)\(t \*testing\.T\)`)

	names := make([]string, 0, 256)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		source, readErr := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, readErr)

		for _, match := range declaration.FindAllStringSubmatch(string(source), -1) {
			names = append(names, match[1])
		}
	}

	return names
}

// TestKafkaAcceptanceSelection_MatchesAtLeastOneTestPerDeclaredFamily is the guard on
// the selector.
//
// Every declared family must match a test that exists.
func TestKafkaAcceptanceSelection_MatchesAtLeastOneTestPerDeclaredFamily(t *testing.T) {
	for _, family := range declaredAcceptanceFamilies(t) {
		declared := family

		t.Run(declared.pkg+" "+declared.pattern, func(t *testing.T) {
			pattern, err := regexp.Compile(declared.pattern)
			require.NoError(t, err,
				"%q must be a valid Go regexp: `go test -run` compiles it with RE2, so a "+
					"backreference or a lookahead selects nothing and fails no build",
				declared.pattern)

			matched := make([]string, 0, 32)
			for _, name := range packageTestNames(t, declared.pkg) {
				if pattern.MatchString(name) {
					matched = append(matched, name)
				}
			}

			assert.NotEmpty(t, matched,
				"THE ACCEPTANCE FAMILY %q MATCHES NO TEST IN %s. The Kafka job selects it, so "+
					"nothing runs, nothing skips, and the fail-on-skip gate reports success over "+
					"the absence. Either the family was renamed — update the FAMILIES table in "+
					"%s — or the suite it selected is gone and the criterion it proved has no "+
					"coverage", declared.pattern, declared.pkg, ciWorkflowPath)
		})
	}
}

// TestKafkaAcceptanceSelection_CoversEveryPackageThatHoldsALiveSuite pins the SET of
// packages.
//
// The property the previous selector lost was not that a pattern was wrong; it was that
// two whole packages were unreached.
func TestKafkaAcceptanceSelection_CoversEveryPackageThatHoldsALiveSuite(t *testing.T) {
	packages := map[string]bool{}
	for _, family := range declaredAcceptanceFamilies(t) {
		packages[family.pkg] = true
	}

	for _, required := range []struct {
		pkg string
		why string
	}{
		{".", "the root package holds the isolation, ordering, recovery, dual-delivery, " +
			"dead-letter and replay-fidelity suites"},
		{"./database", "the repository layer holds every _RealDB suite: the FIFO claim, the " +
			"SKIP LOCKED contention proof, the lease expiry, the snapshot reads and the V-2 census"},
		{"./api", "the HTTP surface holds the master-key gating, the replay endpoint and the " +
			"410 sunset"},
	} {
		assert.True(t, packages[required.pkg],
			"%s is not selected by the Kafka acceptance job, so none of its suites runs there: %s",
			required.pkg, required.why)
	}
}

// TestKafkaAcceptanceJob_SuppliesTheSubscriberFacingBrokerList is the guard on the one
// environment variable whose absence made this job unpassable.
//
// Credential issuance refuses when KAFKA_SUBSCRIBER_BROKERS is unset — a deployment
// must name the addresses a subscriber will actually connect to rather than leaking its
// internal bootstrap list by omission — and the isolation fixture refuses to paper over
// that refusal, so every TestEventIsolation_ test skips without it. With the
// fail-on-skip gate in place, that is a job that cannot go green on a healthy broker.
func TestKafkaAcceptanceJob_SuppliesTheSubscriberFacingBrokerList(t *testing.T) {
	workflow := readCIWorkflow(t)

	assert.Contains(t, workflow, "KAFKA_SUBSCRIBER_BROKERS:",
		"the Kafka acceptance job must set KAFKA_SUBSCRIBER_BROKERS. Without it credential "+
			"issuance refuses by design, every TestEventIsolation_ test skips, and the "+
			"fail-on-skip gate fails the job on a broker that was working perfectly")

	// The variable the isolation fixture actually reads, asserted from the fixture's own constant
	// rather than from a literal, so a rename in one place cannot leave the other behind.
	assert.Contains(t, workflow, eventIsolationSubscriberBrokersEnv+":",
		"the workflow must set %s, which is the name event_isolation_integration_test.go reads",
		eventIsolationSubscriberBrokersEnv)

	// THE GATE ITSELF MUST SURVIVE. Setting the variable makes the suites run; the gate is what
	// makes a future regression that stops them running a red build rather than a green one.
	assert.Contains(t, workflow, "Fail if any Kafka acceptance test skipped",
		"the fail-on-skip gate is the reason this job exists and must not be removed")
	assert.Contains(t, workflow, `.Action == "skip"`,
		"the gate must read the machine-readable skip records rather than grepping human output")
}

// TestKafkaAcceptanceJob_ChecksTheSelectionBeforeRunningIt asserts the precondition
// step exists.
//
// The Go test above catches a family that matches nothing in this repository.
func TestKafkaAcceptanceJob_ChecksTheSelectionBeforeRunningIt(t *testing.T) {
	workflow := readCIWorkflow(t)

	assert.Contains(t, workflow, "Require every declared family to match at least one test",
		"the Kafka job must verify its selection before using it: a selector that matches "+
			"nothing skips nothing, so the fail-on-skip gate cannot see the absence")
	assert.Contains(t, workflow, "go test -list",
		"the precondition must enumerate the selection rather than assuming it")
}

// TestDependencyManifests_AreCheckedRatherThanRepairedInCI is the manifest-check guard.
//
// `go mod tidy` MUTATES go.mod and go.sum.
func TestDependencyManifests_AreCheckedRatherThanRepairedInCI(t *testing.T) {
	workflow := readCIWorkflow(t)

	// COMMAND LINES ONLY. The prose above each step names the command too, and counting
	// occurrences in comments would make the two totals depend on how much the steps
	// explain themselves rather than on what they run.
	tidy := 0
	for _, line := range strings.Split(workflow, "\n") {
		if strings.TrimSpace(line) == "go mod tidy" {
			tidy++
		}
	}
	require.Positive(t, tidy, "the workflow is expected to tidy-check the manifests")

	verified := 0
	for _, line := range strings.Split(workflow, "\n") {
		if strings.TrimSpace(line) == "git diff --exit-code go.mod go.sum" {
			verified++
		}
	}
	assert.Equal(t, tidy, verified,
		"every `go mod tidy` in %s must be followed by `git diff --exit-code go.mod go.sum`; "+
			"%d tidy invocation(s) and %d verification(s) means at least one job repairs the "+
			"manifests and then tests the repaired tree", ciWorkflowPath, tidy, verified)
}

// TestMakeInit_DownloadsAndVerifiesRatherThanResolving is the other half of the manifest-check guard.
//
// `go get ./...` resolves and can WRITE the manifests: it upgrades a requirement to a
// newer version satisfying the same import and rewrites go.mod and go.sum as a side
// effect of what reads like a fetch.
func TestMakeInit_DownloadsAndVerifiesRatherThanResolving(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(moduleRootDir(t), "makefile"))
	require.NoError(t, err)
	makefile := string(raw)

	recipe := makeTargetRecipe(t, makefile, "init")

	assert.NotContains(t, recipe, "go get",
		"`make init` must not run `go get`: it resolves and can rewrite go.mod and go.sum, so "+
			"the setup command becomes able to change the dependency contract. Use `go mod "+
			"download` to fetch exactly what is pinned")
	assert.Contains(t, recipe, "go mod download",
		"`make init` must fetch exactly what the manifests pin")
	assert.Contains(t, recipe, "go mod verify",
		"and verify every fetched module against its recorded go.sum hash, which the download "+
			"alone does not do")
}

// makeTargetRecipe extracts one target's recipe lines from a makefile.
//
// Parameters:
//   - t *testing.T: the test, failed when the target is absent.
//   - makefile string: the makefile source.
//   - target string: the target name.
//
// Returns:
//   - string: the recipe, newline-joined.
func makeTargetRecipe(t *testing.T, makefile, target string) string {
	t.Helper()

	lines := strings.Split(makefile, "\n")
	recipe := make([]string, 0, 8)
	inTarget := false

	for _, line := range lines {
		if strings.HasPrefix(line, target+":") {
			inTarget = true

			continue
		}
		if !inTarget {
			continue
		}
		// A recipe line is tab-indented. The first line that is neither tab-indented nor blank
		// ends the recipe.
		if strings.HasPrefix(line, "\t") {
			recipe = append(recipe, strings.TrimPrefix(line, "\t"))

			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}

		break
	}

	require.NotEmpty(t, recipe, "the makefile must declare a %q target with a recipe", target)

	return strings.Join(recipe, "\n")
}

// TestAlertRules_AreGatedByCIRatherThanByARunbookStep is the guard on the gap that made
// V-4's "each rule proven by driving its gauge past the threshold" true only on paper.
//
// The rule file had a promtool unit-test file, and NOTHING RAN IT. It was reachable only
// as a copy-and-paste line in the operations runbook, so a rule could be edited into
// silence and merged, and the one artefact able to catch that sat in the repository
// unexecuted. An untested alert rule is a worse failure than a broken one: a rule that
// does not fire and a system that is healthy look identical from outside, and only one of
// them is visible.
//
// So the assertions here are about the GATE, not about the rules: that a job exists, that
// it runs the make target rather than its own copy of the commands, and that the target
// runs both halves. Whether each rule fires is what
// alerts/tests/blnk-kafka-alerts_test.yml asserts, and `make alerts` is what makes that
// file's verdict count for anything.
func TestAlertRules_AreGatedByCIRatherThanByARunbookStep(t *testing.T) {
	root := moduleRootDir(t)

	raw, err := os.ReadFile(filepath.Join(root, "makefile"))
	require.NoError(t, err)
	makefile := string(raw)

	t.Run("the makefile gates both halves and the Kubernetes projection", func(t *testing.T) {
		// BOTH SUBCOMMANDS, and they are not interchangeable. `check rules` proves the file
		// parses and that every expression is valid PromQL; `test rules` proves a rule FIRES.
		// SubscriberSettlementNotProgressing passed the first for its entire life while being
		// unable to satisfy the second.
		check := makeTargetRecipe(t, makefile, "alerts_check")
		assert.Contains(t, check, "check rules",
			"`make alerts_check` must run promtool's rule-file check")

		test := makeTargetRecipe(t, makefile, "alerts_test")
		assert.Contains(t, test, "test rules",
			"`make alerts_test` must run promtool's rule UNIT TESTS, which is the half that "+
				"proves a rule fires; `check rules` alone cannot")

		// The dependency is what makes one command run both, so a contributor cannot unit-test
		// rules that do not parse and read the resulting error as a test failure.
		assert.Regexp(t, `(?m)^alerts_test: .*\balerts_check\b`, makefile,
			"`alerts_test` must depend on `alerts_check`")

		projection := makeTargetRecipe(t, makefile, "alerts_configmap")
		assert.Contains(t, projection, "check rules",
			"the Kubernetes projection must be checked as well as the repository copy: "+
				"Kubernetes has no directory to bind-mount, so the manifests carry a COPY, and a "+
				"rule file Prometheus refuses takes every rule in it down")
		assert.Contains(t, projection, "test rules",
			"and it must be unit-tested too, so the deployed copy is proven to FIRE rather than "+
				"merely to parse")
		assert.Regexp(t, `(?m)^alerts: .*\balerts_test\b.*\balerts_configmap\b`, makefile,
			"`make alerts` must run the repository copy and the Kubernetes projection")
	})

	t.Run("a CI job runs the gate", func(t *testing.T) {
		// PARSED RATHER THAN GREPPED. Every claim below is about a `run:` script, and the
		// prose in this workflow names the same commands it runs — so a substring search over
		// the file text is satisfied by a comment that describes a step somebody deleted.
		scripts := ciJobRunScripts(t, "alerts")
		require.NotEmpty(t, scripts,
			"%s must declare an `alerts` job; the rule file had a promtool unit-test file for "+
				"its whole life with nothing running it, and that is the gap this job closes",
			ciWorkflowPath)

		joined := strings.Join(scripts, "\n")

		// THE MAKE TARGET RATHER THAN THE COMMANDS. A job that spelled `promtool test rules`
		// itself would drift from what a contributor runs locally, and the two disagreeing is
		// how the rules stopped being tested in the first place.
		assertRunsCommand(t, scripts, "make alerts",
			"the alerts job must run `make alerts`, the same target a contributor runs")

		assert.Contains(t, joined, "promtool",
			"and it must install a promtool to run it with")

		// PINNED TO THE IMAGE, not to a version literal. The engine that validates a rule must
		// be the engine that loads it, and the pin already exists in docker-compose.yaml — a
		// second copy in the workflow is a second thing to update.
		assert.Contains(t, joined, "prom/prometheus",
			"the promtool version must be derived from the prom/prometheus image the stack "+
				"pins, so the validating engine and the loading engine cannot diverge")
		assert.Contains(t, joined, "sha256sum -c",
			"and the download must be checksum-verified: an unverified fetch is a supply-chain "+
				"hole in the one job whose purpose is assurance")
	})

	t.Run("every rule has a unit test", func(t *testing.T) {
		// THE GATE IS WORTH NOTHING IF THE FILE COVERS ONE RULE. It covered exactly one for its
		// whole life, and this is the assertion that keeps a rule added later from arriving
		// untested — which is indistinguishable from arriving broken.
		rules := readYAMLFile(t, filepath.Join(root, "alerts", "blnk-kafka-alerts.yml"))

		declared := map[string]struct{}{}

		groups, ok := rules["groups"].([]interface{})
		require.True(t, ok, "the rule file must carry a groups list")

		for _, group := range groups {
			entries, ok := group.(map[string]interface{})["rules"].([]interface{})
			require.True(t, ok, "each group must carry a rules list")

			for _, entry := range entries {
				name, ok := entry.(map[string]interface{})["alert"].(string)
				require.True(t, ok, "each rule must be an alerting rule with a name")
				declared[name] = struct{}{}
			}
		}
		require.NotEmpty(t, declared)

		unitTests := readYAMLFile(t, filepath.Join(root, "alerts", "tests",
			"blnk-kafka-alerts_test.yml"))

		cases, ok := unitTests["tests"].([]interface{})
		require.True(t, ok, "the unit-test file must carry a tests list")

		// BOTH DIRECTIONS, tracked separately. A rule asserted only to fire proves nothing
		// about its boundary — one written `>=` where the requirement says "more than" passes a
		// firing-only test while paging on a value that is exactly at the limit — and a rule
		// asserted only NOT to fire is satisfied by a rule that can never fire at all.
		fires := map[string]struct{}{}
		quiet := map[string]struct{}{}

		for _, testCase := range cases {
			assertions, ok := testCase.(map[string]interface{})["alert_rule_test"].([]interface{})
			if !ok {
				continue
			}

			for _, assertion := range assertions {
				fields, ok := assertion.(map[string]interface{})
				if !ok {
					continue
				}

				name, ok := fields["alertname"].(string)
				if !ok {
					continue
				}

				if expected, ok := fields["exp_alerts"].([]interface{}); ok && len(expected) > 0 {
					fires[name] = struct{}{}

					continue
				}

				quiet[name] = struct{}{}
			}
		}

		missingFiring := make([]string, 0, len(declared))
		missingQuiet := make([]string, 0, len(declared))

		for name := range declared {
			if _, ok := fires[name]; !ok {
				missingFiring = append(missingFiring, name)
			}

			if _, ok := quiet[name]; !ok {
				missingQuiet = append(missingQuiet, name)
			}
		}

		sort.Strings(missingFiring)
		sort.Strings(missingQuiet)

		assert.Empty(t, missingFiring,
			"these rules have no case that drives them past their threshold and asserts the "+
				"alert fires, so nothing establishes that they CAN fire: %v", missingFiring)
		assert.Empty(t, missingQuiet,
			"these rules have no case below their threshold or inside their dwell asserting "+
				"silence, so nothing establishes that they fire only when they should: %v",
			missingQuiet)
	})
}

// ciJobRunScripts returns the `run:` scripts of one job in the CI workflow, in the order
// the job declares them.
//
// The workflow is PARSED rather than searched as text. Its steps are documented in prose
// that names the very commands they run, so a substring assertion over the file cannot
// tell a step that exists from a comment about one that used to.
//
// Parameters:
//   - t *testing.T: the test.
//   - job string: the job key, as it appears under `jobs:`.
//
// Returns:
//   - []string: every step's `run` script; empty when the job is absent, which the caller
//     asserts on rather than this helper failing, so "no such job" reads as the finding it
//     is.
func ciJobRunScripts(t *testing.T, job string) []string {
	t.Helper()

	workflow := readYAMLFile(t, filepath.Join(moduleRootDir(t), ciWorkflowPath))

	jobs, ok := workflow["jobs"].(map[string]interface{})
	require.True(t, ok, "%s must carry a jobs map", ciWorkflowPath)

	declared, ok := jobs[job].(map[string]interface{})
	if !ok {
		return nil
	}

	steps, ok := declared["steps"].([]interface{})
	require.Truef(t, ok, "job %q must carry a steps list", job)

	scripts := make([]string, 0, len(steps))

	for _, step := range steps {
		fields, ok := step.(map[string]interface{})
		if !ok {
			continue
		}

		if script, ok := fields["run"].(string); ok {
			scripts = append(scripts, script)
		}
	}

	return scripts
}

// assertRunsCommand asserts that one of the scripts executes a command, judged on the
// script's own lines rather than on the whole text.
//
// A `run:` script carries shell comments too, so "does this job run X" and "does this job
// mention X" are different questions and only the first one is worth asserting.
//
// Parameters:
//   - t *testing.T: the test.
//   - scripts []string: the candidate scripts.
//   - command string: the command line to find, compared after trimming.
//   - message string: what the absence means.
func assertRunsCommand(t *testing.T, scripts []string, command, message string) {
	t.Helper()

	for _, script := range scripts {
		for _, line := range strings.Split(script, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == command || strings.HasPrefix(trimmed, command+" ") {
				return
			}
		}
	}

	assert.Fail(t, message, "no step runs %q; the scripts are:\n%s",
		command, strings.Join(scripts, "\n---\n"))
}

// TestDependencies_YAMLParsingStaysTestOnly bounds the one dependency this change lists
// as direct without adding it to the build.
//
// gopkg.in/yaml.v3 is required, at this exact version, by gin, gorp and sonic already, so
// it costs nothing in go.sum. What it must not become is a PRODUCTION dependency: the
// service parses no YAML at runtime — its configuration is JSON plus environment
// variables — and a YAML parser reachable from a request path is a parser attack surface
// this service has no reason to carry. The declared requirement exists so the deployment
// parity tests can read the alert rules, their ConfigMap copy and the Kubernetes
// manifests as data rather than as text.
//
// The assertion is over the whole module, not over a list of files, so a new production
// file cannot quietly acquire the import.
func TestDependencies_YAMLParsingStaysTestOnly(t *testing.T) {
	root := moduleRootDir(t)

	var offending []string

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.IsDir() {
			// Neither is part of this module's source: .git holds objects, and a vendor tree
			// (if one is ever added) holds other modules' files.
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return fs.SkipDir
			}

			return nil
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}

		source, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		if !strings.Contains(string(source), `"gopkg.in/yaml.v3"`) {
			return nil
		}

		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			relative = path
		}

		offending = append(offending, relative)

		return nil
	})
	require.NoError(t, err, "walking the module source")

	assert.Emptyf(t, offending,
		"gopkg.in/yaml.v3 is declared as a TEST-ONLY requirement in go.mod, and these production "+
			"files import it: %s.\n\nEither move the parsing into a test, or amend the note in go.mod "+
			"that says no production file does this — a requirement documented as test-only and used "+
			"in production is a dependency contract nobody can read off the manifest",
		strings.Join(offending, ", "))
}

// makeVariable reads one simple `NAME=value` assignment out of a makefile.
//
// Deliberately not a make invocation: `make -p` would also expand and evaluate, and what
// these tests need is the DECLARED text, so that an assertion fails on the declaration a
// reader will edit rather than on something make computed from it.
//
// Parameters:
//   - t *testing.T: the test, failed when the variable is not declared.
//   - makefile string: the makefile source.
//   - name string: the variable name.
//
// Returns:
//   - string: the declared value, trimmed.
func makeVariable(t *testing.T, makefile, name string) string {
	t.Helper()

	for _, line := range strings.Split(makefile, "\n") {
		if !strings.HasPrefix(line, name+"=") && !strings.HasPrefix(line, name+" =") {
			continue
		}

		_, value, _ := strings.Cut(line, "=")

		return strings.TrimSpace(value)
	}

	require.Failf(t, "missing make variable", "the makefile must declare %s", name)

	return ""
}

// expandMakeVariables substitutes one level of ${NAME} references from the makefile's own
// simple assignments.
//
// It exists because this makefile deliberately holds its longer commands in variables — the
// note above BROKER_ARRAY_DECLARED explains why — so a recipe assertion that read the raw
// text would report a command as absent purely because it was declared one line higher up.
// One level is enough for that convention and stops short of reimplementing make.
//
// Parameters:
//   - t *testing.T: the test, for the helper marker.
//   - makefile string: the makefile source.
//   - text string: the text to expand.
//
// Returns:
//   - string: text with every resolvable ${NAME} replaced; unresolved names are left alone.
func expandMakeVariables(t *testing.T, makefile, text string) string {
	t.Helper()

	reference := regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

	return reference.ReplaceAllStringFunc(text, func(match string) string {
		name := reference.FindStringSubmatch(match)[1]

		for _, line := range strings.Split(makefile, "\n") {
			if !strings.HasPrefix(line, name+"=") && !strings.HasPrefix(line, name+" =") {
				continue
			}

			_, value, _ := strings.Cut(line, "=")

			return strings.TrimSpace(value)
		}

		return match
	})
}

// TestMutationGate_ScoresWithThePinnedToolRatherThanWhateverIsOnPath guards the pin the
// mutation threshold is only meaningful against.
//
// The gate used to install ${GREMLINS_VERSION} only when NO gremlins was on PATH, so any
// gremlins already installed — any version, including an untagged local build — silently
// became the tool that decided whether the repository met its own threshold. Two machines
// could then report different efficacy for one commit with nothing in any diff to show for
// it, which is the whole failure a pin exists to prevent.
//
// The check has to read the binary's MODULE version rather than its --version output:
// gremlins stamps that string at release time, so a binary installed correctly from the
// v0.6.0 tag by `go install` still says "dev". That is why this test asserts on
// `go version -m` specifically, and why it fails if the recipe reverts to the presence test.
func TestMutationGate_ScoresWithThePinnedToolRatherThanWhateverIsOnPath(t *testing.T) {
	makefile := readRepoFile(t, "makefile")
	declared := makeTargetRecipe(t, makefile, "mutation_gate")
	recipe := expandMakeVariables(t, makefile, declared)

	require.NotEmpty(t, makeVariable(t, makefile, "GREMLINS_VERSION"),
		"the makefile must pin an exact gremlins version")

	assert.Contains(t, recipe, "go version -m",
		"the gate must verify the module version of the binary it is about to score with; "+
			"`gremlins --version` cannot answer this, because that string is an ldflags stamp "+
			"applied by the release build and a correctly installed pin still reports \"dev\"")
	assert.Contains(t, declared, "${GREMLINS_VERSION}",
		"the verification must compare against the pin rather than against a literal")

	assert.NotContains(t, declared, "command -v gremlins >/dev/null 2>&1 || go install",
		"a presence test is not a version check: it installs the pin only when nothing is "+
			"there, so any gremlins already on PATH becomes the tool this gate scores with")

	// And the outcome of a mismatch must be a refusal, not a warning: a score from another
	// version compared against this threshold is a number with no meaning.
	assert.Contains(t, recipe, "MUTATION GATE FAILED",
		"a version mismatch the gate cannot repair must fail the gate")
}

// TestMutationGate_ScoresTheEventSurfaceOfEveryPackageThatHasOne guards the two ways this
// gate could report success while scoring nothing.
//
// The first is a missing SCOPE: MUTATION_EVENT_SCOPES covered the root package and
// ./database and not ./api, so api/events.go — the master-key-gated dead-letter inventory
// and replay — and api/subscribers.go — which mints Kafka credentials — were the only new
// event code no scope reached.
//
// The second is the FILTER. api/ files are named after the route they serve, so the event
// surface there is events.go and subscribers.go rather than event_*.go. A filter matching
// `event_` alone would have added the api scope and scored nothing in it.
func TestMutationGate_ScoresTheEventSurfaceOfEveryPackageThatHasOne(t *testing.T) {
	root := moduleRootDir(t)
	makefile := readRepoFile(t, "makefile")

	scopes := strings.Fields(makeVariable(t, makefile, "MUTATION_EVENT_SCOPES"))
	require.NotEmpty(t, scopes, "the makefile must declare at least one event mutation scope")

	prefixes := strings.Fields(makeVariable(t, makefile, "MUTATION_EVENT_FILE_PREFIXES"))
	require.NotEmpty(t, prefixes, "the makefile must declare which file names the event filter keeps")

	// Every package holding the pipeline's own code must be scored. Named here rather than
	// discovered, because "which packages own event code" is the question the scope list is
	// answering and a test that derived it from the same rule could not disagree.
	for _, pkg := range []string{".", "database", "api"} {
		assert.Containsf(t, scopes, pkg+":event",
			"MUTATION_EVENT_SCOPES must score %s: its event files are otherwise mutated by no "+
				"scope at all, and an unscored file is indistinguishable from a fully killed one "+
				"in the gate's output. Scopes: %v", pkg, scopes)
	}

	// And the filter must actually select files in each of them.
	for _, scope := range scopes {
		directory, filter, _ := strings.Cut(scope, ":")
		if filter != "event" {
			continue
		}

		entries, err := os.ReadDir(filepath.Join(root, directory))
		require.NoErrorf(t, err, "scope %s names a directory that must be readable", scope)

		matched := make([]string, 0, 8)
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}

			for _, prefix := range prefixes {
				if strings.HasPrefix(name, prefix) {
					matched = append(matched, name)

					break
				}
			}
		}

		assert.NotEmptyf(t, matched,
			"scope %s selects no file: the event filter keeps names beginning with %v, and %s/ "+
				"holds none of them. The gate refuses this at runtime rather than scoring nothing, "+
				"so this is a declaration to fix rather than a failure to work around",
			scope, prefixes, directory)
	}
}

// TestMutationGate_SkipsOnlyTestsThatExist is the rot guard on the coverage-gathering skip.
//
// gremlins gathers coverage by running the whole module's suite, so one red test anywhere
// ends the run with "failed to gather coverage" and no score — which is how the AAP's
// mutation gate on the new event code came to be unverified while two pre-existing,
// load-fragile api tests were failing on their wall-clock budgets. The gate now excludes
// those tests by name from the coverage run.
//
// A list of names is a thing that rots. A rename would leave the skip matching nothing,
// the gate blocked again, and the only symptom a coverage error that names none of this —
// so the names are checked against the tree here, where the failure says what to edit.
func TestMutationGate_SkipsOnlyTestsThatExist(t *testing.T) {
	root := moduleRootDir(t)
	makefile := readRepoFile(t, "makefile")

	skip := makeVariable(t, makefile, "MUTATION_COVERAGE_SKIP")
	require.NotEmpty(t, skip,
		"the makefile must declare MUTATION_COVERAGE_SKIP; an empty declaration is a legitimate "+
			"OVERRIDE at the command line but not a legitimate default, because the default is what "+
			"CI and a new contributor get")

	// The regex is an anchored alternation of exact test names — `^(A|B|C)$` with make's `$$`
	// escaping — so the names are recoverable without evaluating it.
	names := strings.Split(strings.Trim(strings.TrimSuffix(strings.TrimPrefix(skip, "^("), "$$"), ")"), "|")
	require.NotEmpty(t, names, "MUTATION_COVERAGE_SKIP must name the tests it excludes: %q", skip)

	declarations := map[string]string{}

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return fs.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}

		source, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			relative = path
		}

		for _, name := range names {
			if strings.Contains(string(source), "func "+name+"(t *testing.T)") {
				declarations[name] = relative
			}
		}

		return nil
	})
	require.NoError(t, err, "walking the module's tests")

	for _, name := range names {
		assert.Containsf(t, declarations, name,
			"MUTATION_COVERAGE_SKIP excludes %q from the coverage run and no test by that name "+
				"exists any more. The skip then matches nothing, the load-fragile test it was "+
				"excluding runs again, and the gate goes back to reporting \"failed to gather "+
				"coverage\" with nothing in the message about this list. Update the makefile", name)
	}

	// The excluded tests must belong to packages this gate does not mutate. Excluding a test
	// that covers scored code would remove coverage the score is computed from, which is a
	// weakening rather than a repair.
	for name, file := range declarations {
		assert.Truef(t, strings.HasPrefix(file, "api/"),
			"%s is excluded from the coverage run but lives in %s. The exclusion is only safe for "+
				"tests of code no event scope mutates; a test of scored code must be repaired or "+
				"made load-tolerant instead of skipped", name, file)
	}
}
