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
	"os"
	"path/filepath"
	"regexp"
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
