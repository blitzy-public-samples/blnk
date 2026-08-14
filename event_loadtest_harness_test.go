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

// EXECUTABLE coverage of the throughput/latency and dead-letter harness:
// tests/loadtest/events.js
// and tests/loadtest/run_case.sh.
//
// That file reads events.js as TEXT and asserts that particular source strings are
// present or absent.
//
// It did not distinguish them.
//
// So these tests RUN it:
//
//   - the fixture harness (`k6 run -e FIXTURES=1 tests/loadtest/events.js`) drives the
//     real exposition parser, label matcher, histogram collector, quantile
//     interpolation, counter and bucket deltas, backlog reader, summary readers and
//     verdict rules over a table of fixtures with exact expected answers, and its
//     self-check mode proves the harness is capable of failing;
//   - the runner is driven with a STUB k6 on PATH, which is what makes its two security
//     and integrity contracts testable: that no credential appears in the k6 process's
//     argv, and that a failed run leaves no artifact a reader could mistake for this
//     run's verdict.
package blnk

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// harnessK6Timeout bounds a fixture run. The fixture mode performs no I/O at all, so a run that
// takes longer than this is hung rather than slow, and the bound exists so a hang is reported as
// one instead of stalling the package until the go test timeout kills the binary.
const harnessK6Timeout = 90 * time.Second

// harnessStubTimeout bounds a runner invocation whose k6 is a shell stub. Everything in it is
// local: a few `rm`s, one `mv` and the stub itself.
const harnessStubTimeout = 60 * time.Second

// harnessSecrets are the credential values the runner tests drive.
//
// They are distinctive strings rather than realistic ones so that a leak is unambiguous
// when it is found in a captured command line, and they are DELIBERATELY not passed to
// any assertion helper as an operand — see harnessAssertAbsent.
const (
	harnessMetricsToken = "fixture-metrics-bearer-2f8c41d90b7e"
	harnessMasterKey    = "fixture-master-key-6ad3e5719c02"
	harnessAPIKey       = "fixture-api-key-b41f70c8e9d5"
)

// harnessRequireK6 resolves the k6 binary or skips.
//
// A SKIP and not a failure, because k6 is a separate tool rather than a Go dependency
// and a contributor without it must still be able to run the suite.
//
// Parameters:
//   - t *testing.T: the test, skipped when k6 is not on PATH.
//
// Returns:
//   - string: the absolute path to k6.
func harnessRequireK6(t *testing.T) string {
	t.Helper()

	binary, err := exec.LookPath("k6")
	if err != nil {
		t.Skip("k6 is not on PATH, so the event-streaming fixture harness cannot be executed. " +
			"Install k6 (https://k6.io/docs/get-started/installation/) to run it; the acceptance " +
			"criteria V-1 and V-3 are decided by tests/loadtest/events.js and this is the only " +
			"test that executes it")
	}

	return binary
}

// runK6Fixtures runs the fixture mode with the given extra environment and returns its
// combined output and exit status.
//
// Parameters:
//   - t *testing.T: the test.
//   - env []string: extra KEY=value entries for the k6 process.
//
// Returns:
//   - string: combined stdout and stderr.
//   - int: the process exit code.
func runK6Fixtures(t *testing.T, env []string) (string, int) {
	t.Helper()

	binary := harnessRequireK6(t)

	args := []string{"run", "--quiet", "-e", "FIXTURES=1"}
	for _, entry := range env {
		args = append(args, "-e", entry)
	}
	args = append(args, filepath.Join("tests", "loadtest", "events.js"))

	// BOUNDED, so a hang is reported as one. See harnessK6Timeout: the fixture mode
	// performs no I/O, so nothing here can legitimately take this long, and an unbounded
	// run would stall the package until the go test timeout killed the whole binary with
	// no indication of which test was stuck.
	runContext, cancelRun := context.WithTimeout(context.Background(), harnessK6Timeout)
	defer cancelRun()

	command := exec.CommandContext(runContext, binary, args...)
	command.Dir = harnessRepositoryRoot(t)
	// A run with NO inherited environment except the minimum k6 needs. k6 enables
	// --include-system-env-vars by default, so an ambient DURATION, SMOKE or
	// RATE_WINDOW_SECONDS in the developer's shell would reach __ENV and change which
	// verdict entries this run's configuration applies — the fixtures assert on that, so
	// the environment has to be pinned.
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
	}

	output, err := command.CombinedOutput()
	require.NotErrorIsf(t, runContext.Err(), context.DeadlineExceeded,
		"the fixture run did not finish within %s. It performs no I/O, so it is hung rather than "+
			"slow. output:\n%s", harnessK6Timeout, output)

	status := 0
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "k6 could not be executed at all: %v", err)
		status = exitErr.ExitCode()
	}

	return string(output), status
}

// harnessRepositoryRoot resolves the directory the runner and the scenario are
// addressed from.
//
// The Go test's working directory IS the package directory, which for the root package
// is the repository root, and both artefacts are referenced by paths relative to it.
//
// Parameters:
//   - t *testing.T: the test, failed when the layout is not what it expects.
//
// Returns:
//   - string: the repository root.
func harnessRepositoryRoot(t *testing.T) string {
	t.Helper()

	root, err := os.Getwd()
	require.NoError(t, err)

	for _, relative := range []string{
		filepath.Join("tests", "loadtest", "events.js"),
		filepath.Join("tests", "loadtest", "run_case.sh"),
	} {
		_, statErr := os.Stat(filepath.Join(root, relative))
		require.NoError(t, statErr, "%s must exist relative to the package directory", relative)
	}

	return root
}

// TestEventLoadHarness_FixtureModeAssertsTheRealVerdictRules is the test the aggregate
// verdict defect needed.
//
// It runs the fixture mode, which asserts — among 47 other exact answers.
func TestEventLoadHarness_FixtureModeAssertsTheRealVerdictRules(t *testing.T) {
	output, status := runK6Fixtures(t, nil)

	assert.Equal(t, 0, status,
		"the event-streaming fixture harness must pass. Every fixture states its exact expected "+
			"answer in tests/loadtest/events.js and the run aborts naming those that disagreed:\n%s",
		output)

	assert.Contains(t, output, "verdict    PASS",
		"the fixture summary must report a passing verdict")

	// THE HARNESS MUST HAVE RUN SOMETHING. A fixture mode that registered zero checks
	// would report a clean run, and "no failures" out of nothing is the vacuous pass every
	// other assertion in this file is written to avoid.
	held, total := harnessCheckCounts(t, output)
	assert.Positive(t, total,
		"the fixture harness registered NO checks, so its clean run asserts nothing")
	assert.Equal(t, total, held,
		"every fixture must have held; %d of %d did", held, total)
	assert.NotContains(t, output, "checks     NONE RAN",
		"the fixture iteration did not run at all, so nothing was asserted")

	// The fixture mode must not have contacted anything. A deployment reached by accident
	// would make the harness environment-dependent, and provisioning is the expensive,
	// irreversible part of a real run.
	assert.NotContains(t, output, "provisioned",
		"fixture mode must not provision ledgers or balances: it short-circuits setup")
	assert.NotContains(t, output, "http_req_duration",
		"fixture mode must issue no HTTP requests at all")
}

// harnessCheckCounts reads the harness's own "<held> of <total> checks held" line.
//
// Parameters:
//   - t *testing.T: the test, failed when the line is absent or unparseable.
//   - output string: the fixture run's combined output.
//
// Returns:
//   - int: how many checks held.
//   - int: how many checks ran.
func harnessCheckCounts(t *testing.T, output string) (int, int) {
	t.Helper()

	matches := regexp.MustCompile(`\[fixture\] (\d+) of (\d+) checks held`).
		FindStringSubmatch(output)
	require.Len(t, matches, 3,
		"the fixture run must report '<held> of <total> checks held'; its absence means the "+
			"iteration did not complete:\n%s", output)

	held, err := strconv.Atoi(matches[1])
	require.NoError(t, err)
	total, err := strconv.Atoi(matches[2])
	require.NoError(t, err)

	return held, total
}

// TestEventLoadHarness_FixtureModeFailsWhenAnAnswerIsWrong proves the harness is
// capable of failing.
func TestEventLoadHarness_FixtureModeFailsWhenAnAnswerIsWrong(t *testing.T) {
	output, status := runK6Fixtures(t, []string{"FIXTURES_SELFCHECK=1"})

	assert.NotEqual(t, 0, status,
		"a deliberately wrong expectation must fail the run. A zero status here means the "+
			"harness does not compare its answers, so its passing run proves nothing:\n%s", output)

	assert.Contains(t, output, "[fixture] FAIL",
		"the disagreeing check must be named, with both the expected and the actual answer")
	assert.Contains(t, output, "fixtures disagreed with their expected answer",
		"and the abort must say how many disagreed")
	assert.Contains(t, output, "verdict    FAIL",
		"the fixture summary must report the failure rather than a pass assembled from an "+
			"empty slate; the summary runs in a SEPARATE k6 runtime, so it reads the failure "+
			"counter rather than in-process state")
}

// ---------------------------------------------------------------------------------------
// The shell runner, driven with a stub k6.
// ---------------------------------------------------------------------------------------

// harnessRunner is one prepared invocation of tests/loadtest/run_case.sh against a stub k6.
type harnessRunner struct {
	// dir is the working directory the runner is invoked from: a temporary tree carrying
	// tests/loadtest/run_case.sh and a placeholder events.js.
	dir string
	// argvPath is where the stub writes the command line it was invoked with, one argument per
	// line.
	argvPath string
	// envPath is where the stub writes its own environment, one KEY=value per line.
	envPath string
}

// newHarnessRunner builds a temporary tree in which run_case.sh can be executed with a
// stub k6.
//
// Parameters:
//   - t *testing.T: the test.
//   - stubExit int: the status the stub k6 exits with.
//   - stubSummary string: what the stub writes to the SUMMARY_OUT path it is given;
//     empty writes nothing, which models a run that never reached handleSummary.
//
// Returns:
//   - harnessRunner: the prepared invocation.
func newHarnessRunner(t *testing.T, stubExit int, stubSummary string) harnessRunner {
	t.Helper()

	root := harnessRepositoryRoot(t)
	dir := t.TempDir()

	loadtest := filepath.Join(dir, "tests", "loadtest")
	require.NoError(t, os.MkdirAll(loadtest, 0o755))

	script, err := os.ReadFile(filepath.Join(root, "tests", "loadtest", "run_case.sh"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(loadtest, "run_case.sh"), script, 0o755))

	// A placeholder rather than the real scenario: the stub never reads it, and copying
	// 270 KB of JavaScript per subtest would be waste. Its PRESENCE matters, because the
	// argv assertions check that the runner handed k6 this path.
	require.NoError(t, os.WriteFile(
		filepath.Join(loadtest, "events.js"),
		[]byte("// placeholder: the stub k6 in this test never reads the scenario\n"),
		0o644,
	))

	binDir := filepath.Join(dir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))

	runner := harnessRunner{
		dir:      dir,
		argvPath: filepath.Join(dir, "k6-argv.txt"),
		envPath:  filepath.Join(dir, "k6-env.txt"),
	}

	// THE STUB. It records its command line and its environment, optionally writes the
	// summary it was told to write, and exits with the configured status.
	stub := strings.Join([]string{
		"#!/usr/bin/env bash",
		"set -uo pipefail",
		"printf '%s\\n' \"$@\" > " + shellQuote(runner.argvPath),
		"env > " + shellQuote(runner.envPath),
		"summary=''",
		"stream=''",
		"for arg in \"$@\"; do",
		"  case \"${arg}\" in",
		"    SUMMARY_OUT=*) summary=\"${arg#SUMMARY_OUT=}\" ;;",
		"    json=*) stream=\"${arg#json=}\" ;;",
		"  esac",
		"done",
		// The NDJSON stream is opened and written as the run proceeds, which is what real k6
		// does and what makes an interrupted run leave a TRUNCATED file rather than none. It
		// is written whatever the exit status, so a failing run genuinely produces a partial
		// the runner has to clean up.
		"if [ -n \"${stream}\" ]; then",
		"  printf '%s\\n' '{\"type\":\"Point\",\"metric\":\"stub\"}' > \"${stream}\"",
		"fi",
		// The summary, by contrast, is written from handleSummary and therefore only when the
		// run reached the end of its lifecycle. An empty stubSummary models the run that did
		// not.
		"if [ -n " + shellQuote(stubSummary) + " ] && [ -n \"${summary}\" ]; then",
		"  printf '%s' " + shellQuote(stubSummary) + " > \"${summary}\"",
		"fi",
		"exit " + harnessItoa(stubExit),
		"",
	}, "\n")
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "k6"), []byte(stub), 0o755))

	return runner
}

// harnessItoa renders a small non-negative integer without importing strconv for one
// call site.
//
// Parameters:
//   - value int: the value.
//
// Returns:
//   - string: the decimal rendering.
func harnessItoa(value int) string {
	if value == 0 {
		return "0"
	}

	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}

	return digits
}

// run invokes the runner for one case name and returns its output and status.
//
// Parameters:
//   - t *testing.T: the test.
//   - caseName string: the case as an operator would type it.
//
// Returns:
//   - string: combined stdout and stderr.
//   - int: the exit status.
func (r harnessRunner) run(t *testing.T, caseName string) (string, int) {
	t.Helper()

	command := exec.Command("bash", filepath.Join("tests", "loadtest", "run_case.sh"), caseName)
	command.Dir = r.dir
	command.Env = []string{
		"PATH=" + filepath.Join(r.dir, "bin") + string(os.PathListSeparator) + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		// The three credentials, under the names ./.env exports rather than the names
		// events.js reads: the rename is part of what the runner does, and asserting on the
		// renamed forms is what proves it did it.
		"BLNK_METRICS_BEARER_TOKEN=" + harnessMetricsToken,
		"BLNK_SERVER_SECRET_KEY=" + harnessMasterKey,
		"API_KEY=" + harnessAPIKey,
		// THE TWO PREREQUISITES THE RUNNER REFUSES A RUN WITHOUT, stated here for the same
		// reason an operator states them: an acceptance run is measured off process-global
		// counters, so it needs an instance dedicated to it, and provisioning a spread adds
		// ledgers and balances that Blnk has no endpoint to delete. Both refusals happen
		// before k6 is started, so without them this harness would assert on the refusal's
		// output instead of on the runner's artifact handling.
		"ISOLATED_INSTANCE=1",
		"ALLOW_FIXTURE_CREATION=1",
		// THE RAW NDJSON STREAM IS OPT-IN: a thirty-minute run at the criterion's load writes
		// tens of millions of records from the load generator while it is trying to measure
		// sub-second latency. The lifecycle assertions below are about that stream as well as
		// the summary, so this harness asks for it explicitly.
		"RAW_OUTPUT=1",
	}

	done := make(chan struct{})
	var output []byte
	var runErr error
	go func() {
		output, runErr = command.CombinedOutput()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(harnessStubTimeout):
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		<-done
		t.Fatalf("the runner did not finish within %s; output so far:\n%s",
			harnessStubTimeout, output)
	}

	status := 0
	if runErr != nil {
		exitErr, ok := runErr.(*exec.ExitError)
		require.True(t, ok, "the runner could not be executed: %v", runErr)
		status = exitErr.ExitCode()
	}

	return string(output), status
}

// harnessAssertAbsent asserts that a haystack does not contain a secret, WITHOUT
// passing the secret or the haystack to an assertion helper.
//
// Parameters:
//   - t *testing.T: the test.
//   - haystack string: the text to search.
//   - needle string: the secret that must not appear.
//   - message string: what its presence would mean, in words only.
func harnessAssertAbsent(t *testing.T, haystack, needle, message string) {
	t.Helper()

	assert.False(t, strings.Contains(haystack, needle), message)
}

// harnessReadLines reads a capture file as lines, failing when it is absent.
//
// Parameters:
//   - t *testing.T: the test.
//   - path string: the capture file.
//
// Returns:
//   - []string: the lines, without trailing newlines.
func harnessReadLines(t *testing.T, path string) []string {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err, "the stub k6 must have written %s; if it did not, the runner never "+
		"invoked k6 at all", path)

	return strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
}

// TestEventLoadRunner_PassesNoCredentialOnTheCommandLine is the CWE-214 guard on the
// acceptance runner.
//
// On Linux argv is readable through /proc/<pid>/cmdline by any process of the same
// user, is reported by `ps` to every user by default, and is recorded verbatim by
// auditd's execve events, by container runtimes and by most APM agents. A run of this
// harness therefore disclosed the deployment's master key — the credential that
// authorises the event management surface — to everything watching the host.
func TestEventLoadRunner_PassesNoCredentialOnTheCommandLine(t *testing.T) {
	runner := newHarnessRunner(t, 0, `{"metrics":{}}`)

	output, status := runner.run(t, "events")
	require.Equal(t, 0, status, "the stubbed run must succeed; output:\n%s", output)

	argv := strings.Join(harnessReadLines(t, runner.argvPath), "\n")

	harnessAssertAbsent(t, argv, harnessMetricsToken,
		"THE METRICS BEARER TOKEN IS IN THE k6 COMMAND LINE. Process arguments are readable "+
			"through /proc, are reported by ps and are recorded by audit and container "+
			"telemetry. Export it into the environment instead of passing -e "+
			"METRICS_BEARER_TOKEN=<value>")
	harnessAssertAbsent(t, argv, harnessMasterKey,
		"THE MASTER KEY IS IN THE k6 COMMAND LINE. It authorises the whole event management "+
			"surface, and argv is not a private channel. Export it instead of passing -e "+
			"MASTER_KEY=<value>")
	harnessAssertAbsent(t, argv, harnessAPIKey,
		"THE API KEY IS IN THE k6 COMMAND LINE. Export it instead of passing -e API_KEY=<value>")

	// The credential NAMES must also be absent as -e assignments, so a future change cannot
	// reintroduce the channel with a different value.
	for _, name := range []string{"METRICS_BEARER_TOKEN=", "MASTER_KEY=", "API_KEY="} {
		assert.False(t, strings.Contains(argv, name),
			"%s must not appear in the k6 command line at all: an -e assignment of a credential "+
				"is the channel this test exists to close, whatever value it carries", name)
	}

	// AND THEY MUST STILL ARRIVE. A runner that simply stopped forwarding them would pass
	// every assertion above and silently produce a run with no authenticated scrape, whose
	// verdicts are all withheld — a failure that looks like a measurement.
	environment := harnessReadLines(t, runner.envPath)
	present := map[string]bool{}
	for _, entry := range environment {
		if index := strings.Index(entry, "="); index > 0 {
			present[entry[:index]] = true
		}
	}
	for _, name := range []string{"METRICS_BEARER_TOKEN", "MASTER_KEY", "API_KEY"} {
		assert.True(t, present[name],
			"%s must reach the k6 process through its environment: events.js reads that name, "+
				"and ./.env spells the same credential differently, so the runner has to rename "+
				"it", name)
	}

	// The renaming must preserve the VALUE, or the scrape authenticates with the wrong secret
	// and every verdict is withheld. Compared without either operand reaching a message.
	assert.True(t, harnessEnvironmentCarries(environment, "MASTER_KEY", harnessMasterKey),
		"MASTER_KEY reached k6 with a value other than the configured master key")
	assert.True(t, harnessEnvironmentCarries(environment, "METRICS_BEARER_TOKEN", harnessMetricsToken),
		"METRICS_BEARER_TOKEN reached k6 with a value other than the configured bearer token")
	assert.True(t, harnessEnvironmentCarries(environment, "API_KEY", harnessAPIKey),
		"API_KEY reached k6 with a value other than the configured API key")

	// The non-sensitive values are still in argv, where they belong: an operator reading `ps`
	// must be able to see which deployment is under load and which scenario is running.
	assert.Contains(t, argv, "URL=", "the target URL stays in argv, where it is diagnostic")
	assert.Contains(t, argv, "SCENARIO=event_publish",
		"the scenario is pinned in argv rather than inherited")
}

// harnessEnvironmentCarries reports whether an environment listing binds name to value.
//
// Written as a predicate so that neither the name's value nor the expected secret is
// ever an assertion operand.
//
// Parameters:
//   - environment []string: KEY=value entries.
//   - name string: the variable to look for.
//   - value string: the value it must carry.
//
// Returns:
//   - bool: true when the binding is present and exact.
func harnessEnvironmentCarries(environment []string, name, value string) bool {
	for _, entry := range environment {
		if entry == name+"="+value {
			return true
		}
	}

	return false
}

// TestEventLoadRunner_AFailedRunLeavesNoStaleArtifactBehind is the artifact-integrity
// guard.
//
// The summary is written by handleSummary, which runs only when the test reaches the
// end of its lifecycle.
func TestEventLoadRunner_AFailedRunLeavesNoStaleArtifactBehind(t *testing.T) {
	runner := newHarnessRunner(t, 99, "")

	summary := filepath.Join(runner.dir, "tests", "loadtest", "summary-event-streaming.json")
	ndjson := filepath.Join(runner.dir, "tests", "loadtest", "run-event-streaming.ndjson")

	stale := `{"blnk_event_streaming":{"verdicts_all_hold":true},"metrics":{}}`
	require.NoError(t, os.WriteFile(summary, []byte(stale), 0o644))
	require.NoError(t, os.WriteFile(ndjson, []byte(`{"type":"Point"}`+"\n"), 0o644))

	output, status := runner.run(t, "events")

	assert.Equal(t, 99, status,
		"the runner must report k6's own failure status rather than absorbing it; output:\n%s",
		output)

	_, summaryErr := os.Stat(summary)
	assert.True(t, os.IsNotExist(summaryErr),
		"A STALE SUMMARY SURVIVED A FAILED RUN at %s. It carries verdicts_all_hold=true from an "+
			"earlier run and is indistinguishable from this run's result: same path, same shape. "+
			"The destination must be removed before launch and recreated only on success",
		summary)

	_, ndjsonErr := os.Stat(ndjson)
	assert.True(t, os.IsNotExist(ndjsonErr),
		"a stale NDJSON stream survived a failed run at %s; a truncated or superseded stream "+
			"parses as a short run rather than as a failure", ndjson)

	assert.Contains(t, output, "NO artifact was written",
		"the failure must say plainly that nothing was published, or an operator looks for a "+
			"file that is deliberately absent")

	harnessAssertNoPartials(t, filepath.Join(runner.dir, "tests", "loadtest"))
}

// TestEventLoadRunner_PromotesTheArtifactOnlyWhenTheRunSucceeded is the other half: a
// successful run must publish its verdict, atomically, at the documented path.
//
// Without this the fix for the stale-artifact defect could be "never write an
// artifact", which would pass every assertion in the test above.
func TestEventLoadRunner_PromotesTheArtifactOnlyWhenTheRunSucceeded(t *testing.T) {
	body := `{"metrics":{},"blnk_event_streaming":{"verdicts_all_hold":true}}`
	runner := newHarnessRunner(t, 0, body)

	output, status := runner.run(t, "events")
	require.Equal(t, 0, status, "output:\n%s", output)

	summary := filepath.Join(runner.dir, "tests", "loadtest", "summary-event-streaming.json")
	written, err := os.ReadFile(summary)
	require.NoError(t, err, "a successful run must publish its summary at the documented path")
	assert.JSONEq(t, body, string(written),
		"the published summary must be exactly what k6 wrote, not a rewrite of it")

	assert.Contains(t, output, "Generated files:")
	assert.Contains(t, output, "summary-event-streaming.json")

	// The temporary the run wrote through must not survive. A left-behind `.partial.` file
	// beside the artifact is a second copy of the verdict that no reader is looking for
	// and no run cleans up.
	harnessAssertNoPartials(t, filepath.Join(runner.dir, "tests", "loadtest"))
}

// TestEventLoadRunner_RefusesAZeroExitThatProducedNoVerdict covers the third outcome,
// which is neither a clean pass nor an honest failure.
//
// k6 can exit 0 without having reached handleSummary — a run aborted through
// `exec.test.abort()` in a configuration where no threshold failed, for instance.
func TestEventLoadRunner_RefusesAZeroExitThatProducedNoVerdict(t *testing.T) {
	runner := newHarnessRunner(t, 0, "")

	output, status := runner.run(t, "events")

	assert.NotEqual(t, 0, status,
		"a run that produced no summary must fail, whatever k6's own status was; output:\n%s",
		output)
	assert.Contains(t, output, "wrote no summary",
		"and it must name the reason rather than reporting a generated file")

	summary := filepath.Join(runner.dir, "tests", "loadtest", "summary-event-streaming.json")
	_, err := os.Stat(summary)
	assert.True(t, os.IsNotExist(err),
		"no artifact may be published for a run that produced no verdict")

	harnessAssertNoPartials(t, filepath.Join(runner.dir, "tests", "loadtest"))
}

// TestEventLoadRunner_NamesArtifactsUnderEitherSpelling pins the artifact contract for
// the documented alias.
//
// `events` and `event-streaming` are two names for one case, and normalising them onto
// one dispatch point is right — two branches once forwarded different variables and
// spelled the master key differently, so both names appeared to work while doing
// different things.
func TestEventLoadRunner_NamesArtifactsUnderEitherSpelling(t *testing.T) {
	body := `{"metrics":{}}`

	const (
		frozenSummary = "summary-event-streaming.json"
		frozenNDJSON  = "run-event-streaming.ndjson"
	)

	for _, spelling := range []string{"events", "event-streaming"} {
		caseName := spelling

		t.Run(caseName, func(t *testing.T) {
			runner := newHarnessRunner(t, 0, body)

			output, status := runner.run(t, caseName)
			require.Equal(t, 0, status, "output:\n%s", output)

			loadtest := filepath.Join(runner.dir, "tests", "loadtest")

			_, summaryErr := os.Stat(filepath.Join(loadtest, frozenSummary))
			assert.NoError(t, summaryErr,
				"`run_case.sh %s` must write %s: the artifact name is the frozen one, not one "+
					"derived from the spelling, so every collector and the acceptance record look "+
					"for the same file whichever name was typed",
				caseName, frozenSummary)

			_, ndjsonErr := os.Stat(filepath.Join(loadtest, frozenNDJSON))
			assert.NoError(t, ndjsonErr, "and %s beside it", frozenNDJSON)

			// THE RESOLVED NAME IS ANNOUNCED. Freezing the artifact name is only half of it: an
			// operator who typed the alias must still be able to see which case ran and which
			// file to open, or the substitution is silent again in the other direction.
			assert.Contains(t, output, frozenSummary,
				"the run must name the artifact it wrote, so the operator does not have to "+
					"derive it from the case name they used")
			if caseName != "event-streaming" {
				assert.Contains(t, output, "requested as "+caseName,
					"a run invoked under the alias must echo both names; a silent substitution "+
						"leaves an operator unable to tell which case ran")
			}

			// One case, one scenario: the alias changes nothing about what is measured.
			argv := strings.Join(harnessReadLines(t, runner.argvPath), "\n")
			assert.Contains(t, argv, "SCENARIO=event_publish",
				"both spellings must run the SAME scenario; a second dispatch branch is how the "+
					"two names came to forward different variables")
			assert.Contains(t, argv, filepath.Join("tests", "loadtest", "events.js"),
				"and the same scenario file")
			assert.Contains(t, argv, "SUMMARY_OUT=",
				"and k6 must be told where to write, so a direct run and a runner-driven run "+
					"land in the same place")
		})
	}
}

// harnessAssertNoPartials requires that no in-flight artifact was left in a directory.
//
// Parameters:
//   - t *testing.T: the test.
//   - dir string: the directory to scan.
func harnessAssertNoPartials(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, entry := range entries {
		assert.False(t, strings.Contains(entry.Name(), ".partial."),
			"%s was left behind: the runner writes through a temporary and must remove it on "+
				"every path, including the failing and interrupted ones", entry.Name())
	}
}
