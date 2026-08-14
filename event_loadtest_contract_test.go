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

// PROVENANCE CONTRACT ASSERTIONS for the throughput-and-latency verdict in
// tests/loadtest/events.js.
//
// 500 events per second sustained, with a p99 capture-to-dispatch latency under two
// seconds — is met.
//
// The specific defect these tests exist to prevent had exactly that shape. The p99
// selection accepted blnk_events_publish_duration_seconds as a FALLBACK when the
// capture-to-dispatch histogram had no observations:
//
//	if (capture.quantile.value !== null) { ... } else if (brokerWrite.quantile.value !== null) {
//	  latencySource = P99_SOURCE_PUBLISH_DURATION;
//	  chosen = brokerWrite;                       // <- a different interval entirely
//	}
package blnk

import (
	"os"
	"path"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadTestScriptPath is the artefact under contract.
const loadTestScriptPath = "tests/loadtest/events.js"

// loadTestScript returns the event scenario's source.
func loadTestScript(t *testing.T) string {
	t.Helper()

	return readRepoFile(t, loadTestScriptPath)
}

// scriptRegion returns the text of events.js between two anchor statements, exclusive
// of the closing anchor.
func scriptRegion(t *testing.T, source, openAnchor, closeAnchor string) string {
	t.Helper()

	require.Equalf(t, 1, strings.Count(source, openAnchor),
		"%s must contain the opening anchor %q exactly once", loadTestScriptPath, openAnchor)
	require.Equalf(t, 1, strings.Count(source, closeAnchor),
		"%s must contain the closing anchor %q exactly once", loadTestScriptPath, closeAnchor)

	start := strings.Index(source, openAnchor)
	end := strings.Index(source, closeAnchor)
	require.Greaterf(t, end, start,
		"%s: the closing anchor %q must follow the opening anchor %q",
		loadTestScriptPath, closeAnchor, openAnchor)

	return source[start:end]
}

// scriptFunctionBody returns the text of a named top-level function in events.js.
//
// It exists because some function bodies end on a statement that is not unique in the
// file — `return o;` closes two of them — so an anchored region cannot address them
// unambiguously.
func scriptFunctionBody(t *testing.T, source, declaration string) string {
	t.Helper()

	require.Equalf(t, 1, strings.Count(source, declaration),
		"%s must declare %q exactly once", loadTestScriptPath, declaration)

	start := strings.Index(source, declaration)
	end := strings.Index(source[start:], "\n}\n")
	require.Greaterf(t, end, 0, "%s: %q must be a closed function", loadTestScriptPath, declaration)

	return source[start : start+end]
}

// stripLineComments removes whole-line JavaScript comments from a region.
func stripLineComments(region string) string {
	lines := strings.Split(region, "\n")
	kept := make([]string, 0, len(lines))

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") ||
			strings.HasPrefix(trimmed, "/*") ||
			strings.HasPrefix(trimmed, "*/") ||
			strings.HasPrefix(trimmed, "*") {
			continue
		}

		kept = append(kept, line)
	}

	return strings.Join(kept, "\n")
}

// collapseWhitespace reduces every run of whitespace in a region to a single space.
func collapseWhitespace(region string) string {
	return strings.Join(strings.Fields(region), " ")
}

// ---------------------------------------------------------------------------
// The latency verdict may be certified from one series and one series only
// ---------------------------------------------------------------------------

// TestLoadTestP99_IsCertifiedFromCaptureToDispatchAlone pins the selection.
//
// The assertion is about what the block may READ, not about which constant it happens
// to assign.
func TestLoadTestP99_IsCertifiedFromCaptureToDispatchAlone(t *testing.T) {
	source := loadTestScript(t)
	selection := stripLineComments(scriptRegion(t,
		source,
		"var latencySource =",
		"p99SourceCode.add(latencySource);",
	))

	assert.Containsf(t, selection, "capture.quantile.value !== null",
		"%s: the V-1 p99 must be taken from the capture-to-dispatch histogram", loadTestScriptPath)
	assert.Containsf(t, selection, "? capture : null;",
		"%s: the capture histogram must be the only figure that can be recorded, and the absence "+
			"of a sample must resolve to none rather than to another series", loadTestScriptPath)

	// The whole defect, in one assertion: publish_duration must not be reachable from the
	// selection at all. It is a strictly shorter interval, so any path that lets it become
	// `chosen` would certify the verdict against a measurement it is not stated over.
	assert.NotContainsf(t, selection, "brokerWrite",
		"%s: the p99 selection must not read publish_duration. Its clock starts at the relay "+
			"claim, so it excludes the queue wait and reports its smallest figures for the "+
			"backlog V-1 exists to catch. When capture-to-dispatch has no samples the run is "+
			"NOT CERTIFIABLE — it is not certified from a shorter interval", loadTestScriptPath)
	assert.NotContainsf(t, selection, "P99_SOURCE_PUBLISH_DURATION",
		"%s: no branch of the p99 selection may resolve to the publish-duration source",
		loadTestScriptPath)

	// Exactly one source is assignable, so the "else if" shape cannot have returned in
	// another guise. Enumerated rather than counted, because the initialiser `chosen =
	// null` is itself an assignment and a bare count would be satisfied by any second
	// branch that replaced it.
	assignments := regexp.MustCompile(`chosen\s*=\s*[^;]*?\?\s*([A-Za-z][A-Za-z0-9_]*)\s*:\s*([A-Za-z][A-Za-z0-9_]*)`).
		FindAllStringSubmatch(selection, -1)

	require.Lenf(t, assignments, 1,
		"%s: `chosen` must be resolved exactly once, so there is one place a source can enter",
		loadTestScriptPath)
	assert.Equalf(t, []string{"capture", "null"}, assignments[0][1:],
		"%s: the p99 must have exactly one possible provenance — the capture-to-dispatch "+
			"histogram, or none at all", loadTestScriptPath)
}

// TestLoadTestVerdictAvailability_KeysOnTheCaptureHistogram pins the fail-closed flag.
//
// event_publish_verdicts_available is the gate that makes the other three verdicts
// meaningful: a run that measured nothing reports zeroes, two of which satisfy their
// `<` thresholds.
func TestLoadTestVerdictAvailability_KeysOnTheCaptureHistogram(t *testing.T) {
	source := loadTestScript(t)
	reason := stripLineComments(scriptRegion(t,
		source,
		"var reason = REASON_AVAILABLE;",
		"degradedReasonCode.add(reason);",
	))

	assert.Containsf(t, reason, "} else if (capture.quantile.value === null) {",
		"%s: availability must be decided by the presence of capture-to-dispatch observations",
		loadTestScriptPath)
	assert.Containsf(t, reason, "reason = REASON_NO_FIRST_ATTEMPT_SAMPLES;",
		"%s: a run with no first-attempt latency samples must say so", loadTestScriptPath)

	assert.NotContainsf(t, reason, "latencySource",
		"%s: availability must not be inferred from which source was chosen. Keying on the "+
			"chosen source is what let a fallback make an uncertifiable run look certifiable; "+
			"key on the capture histogram, which is the population V-1 is stated over",
		loadTestScriptPath)
}

// TestLoadTestDiagnostics_CarryNoThreshold pins the demotion.
func TestLoadTestDiagnostics_CarryNoThreshold(t *testing.T) {
	source := loadTestScript(t)
	thresholds := stripLineComments(
		scriptFunctionBody(t, source, "function verdictThresholds() {"),
	)

	// The four verdicts that ARE gated. Named individually so that losing one is a failure:
	// a threshold table that silently shrinks certifies less than it claims to.
	for _, metric := range []string{
		"M_EVENTS_PER_SEC",
		"M_P99_SECONDS",
		"M_DEAD_LETTER_RATIO",
		"M_VERDICTS_AVAILABLE",
	} {
		assert.Containsf(t, thresholds, "o["+metric+"]",
			"%s: %s must carry a threshold", loadTestScriptPath, metric)
	}

	// M_QUEUE_WAIT_P99 is named among the forbidden entries although no such constant
	// exists: the figure is a DIFFERENCE and not a queue-wait percentile, and naming the
	// misleading spelling here means re-introducing it under that name fails too.
	for _, diagnostic := range []string{
		"M_BROKER_WRITE_P99",
		"M_P99_DIFFERENCE",
		"M_QUEUE_WAIT_P99",
	} {
		assert.NotContainsf(t, thresholds, diagnostic,
			"%s: %s is a diagnostic-only supporting figure and must not be thresholded — "+
				"publish_duration and the p99 difference explain a verdict, they do not decide one",
			loadTestScriptPath, diagnostic)
	}

	// The p99 threshold must be the published two-second bound rather than an unrelated literal.
	assert.Containsf(t, thresholds, `o[M_P99_SECONDS] = ["value<" + MAX_P99_PUBLISH_SECONDS];`,
		"%s: the p99 verdict must be gated on MAX_P99_PUBLISH_SECONDS", loadTestScriptPath)
}

// TestLoadTestP99_IsDocumentedAgainstTheCaptureSeries pins the reported provenance.
//
// The summary publishes an `equivalent_promql` beside each verdict so the figure can be
// reproduced in Prometheus.
func TestLoadTestP99_IsDocumentedAgainstTheCaptureSeries(t *testing.T) {
	source := loadTestScript(t)

	assert.Containsf(t, source,
		`const PROMQL_P99 =`+"\n"+
			`  'histogram_quantile(0.99, sum by (le) (rate(blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1"}[5m])))';`,
		"%s: V-1's published query must be the capture-to-dispatch p99 at attempt=1",
		loadTestScriptPath)

	assert.Containsf(t, source,
		`blnk_events_publish_duration_seconds_bucket{attempt="1",outcome="dispatched"}`,
		"%s: the diagnostic's published query must name publish_duration, narrowed to "+
			"successful writes", loadTestScriptPath)

	// The DIFFERENCE of the two, which is the only reason keeping the shorter interval in
	// the report is worth anything — and it is a diagnostic-only supporting figure rather
	// than a queue-wait percentile.
	assert.Containsf(t, source, "const PROMQL_P99_DIFFERENCE = PROMQL_P99 + \" - \" + PROMQL_BROKER_WRITE;",
		"%s: the diagnostic must be derived as capture-to-dispatch minus the broker write, and it "+
			"is named as the DIFFERENCE it is: the difference of two p99 figures is not the p99 of "+
			"the difference, and a constant called QUEUE_WAIT would invite exactly that reading",
		loadTestScriptPath)
}

// TestLoadTestP99SourceCodes_StayStableForOldArtefacts pins the numbering.
//
// P99_SOURCE_PUBLISH_DURATION is kept at 2 although it can no longer be emitted.
//
// What the test enforces is the pair: the code exists with its original value, AND it
// is only ever DECODED, never produced.
func TestLoadTestP99SourceCodes_StayStableForOldArtefacts(t *testing.T) {
	source := loadTestScript(t)

	for declaration, meaning := range map[string]string{
		"const P99_SOURCE_NONE = 0;":                "nothing was computable",
		"const P99_SOURCE_CAPTURE_TO_DISPATCH = 1;": "the interval V-1 is stated over",
		"const P99_SOURCE_PUBLISH_DURATION = 2;":    "reserved, and no longer emitted",
	} {
		assert.Containsf(t, source, declaration,
			"%s: the source code for %q must keep its number so artefacts stay comparable",
			loadTestScriptPath, meaning)
	}

	// The reserved code may appear only where an OLD artefact is decoded or labelled. Any other
	// occurrence is a production site, which is the fallback returning.
	assignment := regexp.MustCompile(`latencySource\s*=\s*P99_SOURCE_PUBLISH_DURATION`)
	assert.Falsef(t, assignment.MatchString(source),
		"%s: P99_SOURCE_PUBLISH_DURATION must never be assigned as the p99's provenance — it "+
			"is retained for decoding artefacts from the version that did emit it",
		loadTestScriptPath)

	assert.Containsf(t, source, "RESERVED AND NEVER EMITTED",
		"%s: the reserved code must say that it is reserved, or a later reader will take it "+
			"for a live branch and wire it back up", loadTestScriptPath)
}

// ---------------------------------------------------------------------------
// The runner must not substitute its own load shape for the criterion's
// ---------------------------------------------------------------------------

// loadTestRunnerPath is the helper that invokes the scenarios.
const loadTestRunnerPath = "tests/loadtest/run_case.sh"

// loadTestGuidePath is the guide operators run the acceptance case from.
const loadTestGuidePath = "tests/loadtest/README.md"

// eventStreamingDispatch is the runner's canonical dispatch for the acceptance case.
//
// The literal is shared by every test below that has to find the branch, so a rename shows up as
// one failure with one cause rather than as several tests disagreeing about where the branch is.
const eventStreamingDispatch = `if [[ "${CASE_NAME}" == "event-streaming" ]]; then`

// eventStreamingSummaryArtefact and eventStreamingNDJSONArtefact are the FROZEN filenames.
const (
	eventStreamingSummaryArtefact = "summary-event-streaming.json"
	eventStreamingNDJSONArtefact  = "run-event-streaming.ndjson"
)

// TestLoadTestRunner_KeepsTheFrozenEventCaseAndArtefactNames pins the case identifier
// and the two output filenames the acceptance record is filed under.
//
// These three strings are an interface between parties that never see each other's
// source.
//
// It happened: the runner was changed to make `events` the canonical name and to emit
// `summary-events.json` and `run-events.ndjson`, the guide was updated to document the
// renamed pair, and events.js went on naming the frozen pair in its own report.
//
// The canonical dispatch, the alias NORMALISING ONTO it rather than dispatching
// separately, both frozen defaults in the runner, the same default in the scenario for
// a direct `k6 run`, and the absence of the renamed pair anywhere in the three files.
func TestLoadTestRunner_KeepsTheFrozenEventCaseAndArtefactNames(t *testing.T) {
	runner := readRepoFile(t, loadTestRunnerPath)
	scenario := loadTestScript(t)
	guide := readRepoFile(t, loadTestGuidePath)

	require.Containsf(t, runner, eventStreamingDispatch,
		"%s: `event-streaming` is the frozen case identifier and must be the name the runner "+
			"dispatches on", loadTestRunnerPath)

	// The alias points AT the canonical name. The reverse — normalising `event-streaming`
	// onto `events` — is what renamed the artefacts, because the filenames are derived
	// from the name the branch runs under.
	assert.Truef(t,
		strings.Contains(runner, `if [[ "${CASE_NAME}" == "events" ]]; then`+"\n"+`  CASE_NAME="event-streaming"`),
		"%s: `events` must normalise ONTO `event-streaming`, so both spellings run one branch "+
			"and produce one pair of filenames", loadTestRunnerPath)

	for _, artefact := range []string{eventStreamingSummaryArtefact, eventStreamingNDJSONArtefact} {
		assert.Truef(t, strings.Contains(runner, artefact),
			"%s: %s is a frozen artefact name and must be the runner's default",
			loadTestRunnerPath, artefact)
	}

	// The scenario's own default matters independently: `k6 run tests/loadtest/events.js` with no
	// runner must land in the same file, or a direct invocation writes its numbers elsewhere.
	assert.Truef(t,
		strings.Contains(scenario, `__ENV.SUMMARY_OUT || "tests/loadtest/`+eventStreamingSummaryArtefact+`"`),
		"%s: a direct run must default to the frozen summary name, not to a second name that "+
			"only the runner knows how to produce", loadTestScriptPath)

	// The renamed pair must not survive anywhere in the harness, including in prose: a guide that
	// documents the wrong filename sends an operator looking for a file no run writes.
	for path, contents := range map[string]string{
		loadTestRunnerPath: runner,
		loadTestScriptPath: scenario,
		loadTestGuidePath:  guide,
	} {
		for _, renamed := range []string{"summary-events.json", "run-events.ndjson"} {
			assert.Falsef(t, strings.Contains(contents, renamed),
				"%s still refers to %q. The frozen names are %s and %s; a run that writes the "+
					"renamed pair produces correct numbers under filenames CI, the dashboard and "+
					"the acceptance record do not collect",
				path, renamed, eventStreamingSummaryArtefact, eventStreamingNDJSONArtefact)
		}
	}
}

// TestLoadTestRunner_KeepsCredentialsOutOfArgvAndOutOfOutput pins the two ways a runner
// can disclose privileged material.
//
// Passing the metrics bearer token, the FULL-PRIVILEGE master key or an API key to k6 as
// `-e NAME=value` puts it in argv. A process's arguments are world-readable through
// /proc/<pid>/cmdline for as long as it runs, and they are what `ps`, a container
// runtime's process view and many CI log collectors display.
//
// The runner prints the transaction and metrics endpoints, and events.js refuses a
// credential-bearing URL during init — one step too late on its own, because the printing
// comes first, so userinfo or a query token would reach the terminal, the CI log and the
// scrollback before the refusal.
func TestLoadTestRunner_KeepsCredentialsOutOfArgvAndOutOfOutput(t *testing.T) {
	runner := readRepoFile(t, loadTestRunnerPath)

	dispatch := strings.Index(runner, eventStreamingDispatch)
	require.Greaterf(t, dispatch, 0, "%s must dispatch the event-streaming case", loadTestRunnerPath)

	branchEnd := strings.Index(runner[dispatch:], "\n  exit 0\nfi\n")
	require.Greaterf(t, branchEnd, 0,
		"%s: the event-streaming branch must terminate with `exit 0` inside its own `fi`",
		loadTestRunnerPath)
	// Comments are stripped before anything is asserted, and every assertion below is
	// phrased so that a failure reports its own message rather than echoing the whole
	// branch. The runner's rationale prose deliberately quotes the constructs that were
	// removed, so an unstripped search for `-e MASTER_KEY=` matches the comment explaining
	// why it is gone.
	code := stripShellComments(runner[dispatch : dispatch+branchEnd])

	// No credential may reach the argument array, under either the normalised name or the BLNK_
	// name it is resolved from.
	for _, credential := range []string{
		"METRICS_BEARER_TOKEN", "MASTER_KEY", "API_KEY",
		"BLNK_METRICS_BEARER_TOKEN", "BLNK_SERVER_SECRET_KEY", "BLNK_API_KEY",
	} {
		for _, spelling := range []string{
			`-e "` + credential + `=`,
			`-e ` + credential + `=`,
		} {
			assert.Falsef(t, strings.Contains(code, spelling),
				"%s: %s must not be passed to k6 as a command-line argument (found %q). argv is "+
					"world-readable through /proc/<pid>/cmdline for the life of the process, and "+
					"is what ps, a container runtime's process view and many CI log collectors "+
					"display; export it into the child environment instead, which k6 reads "+
					"because --include-system-env-vars defaults to true",
				loadTestRunnerPath, credential, spelling)
		}
	}

	// And each is exported instead, conditionally: an exported empty value is present in __ENV and
	// overrides the scenario's own resolution with nothing.
	for _, export := range []string{
		`export METRICS_BEARER_TOKEN=`,
		`export MASTER_KEY=`,
		`export API_KEY=`,
	} {
		assert.Truef(t, strings.Contains(code, export),
			"%s: the credential channel is the child environment, so %q must appear",
			loadTestRunnerPath, export)
	}

	// The endpoints are still passed as arguments, and that is deliberate: they carry no credential
	// by the time they are passed, and an invocation in a CI log should say what it measured.
	for _, endpoint := range []string{`-e "URL=${URL}"`, `-e "METRICS_URL=${METRICS_URL}"`} {
		assert.Truef(t, strings.Contains(code, endpoint),
			"%s: %q must stay in the invocation — it is refused above if it carries a credential, "+
				"and having it in the command is what makes a CI log self-describing",
			loadTestRunnerPath, endpoint)
	}

	// ORDERING: the refusal precedes the first print of an endpoint. This is the assertion the
	// original code would have failed while still containing a perfectly good refusal downstream.
	refusal := strings.Index(code, `refuse_credential_bearing_url "URL" "${URL}"`)
	require.Greaterf(t, refusal, 0,
		"%s: the runner must refuse a credential-bearing URL itself. Leaving it to events.js's "+
			"own init check is too late — the runner prints the endpoints first",
		loadTestRunnerPath)

	firstEndpointEcho := strings.Index(code, `echo "  transactions :`)
	require.Greaterf(t, firstEndpointEcho, 0,
		"%s: the runner is expected to announce the endpoints it measured", loadTestRunnerPath)
	assert.Lessf(t, refusal, firstEndpointEcho,
		"%s: the credential refusal must run BEFORE the first endpoint is printed, or userinfo "+
			"and query tokens reach the terminal and the CI log before anything rejects them",
		loadTestRunnerPath)

	// And the print itself is redacted, so a reordering cannot silently make it unsafe.
	for _, printed := range []string{
		`echo "  transactions : $(redact_url "${URL}")"`,
		`echo "  metrics      : $(redact_url "${METRICS_URL}")"`,
	} {
		assert.Truef(t, strings.Contains(code, printed),
			"%s: every endpoint print must go through redact_url; %q is missing",
			loadTestRunnerPath, printed)
	}
}

// TestLoadTestRunner_LeavesTheEventLoadShapeToTheScenario pins the second place the
// latency verdict could be quietly detached from what it is stated over.
//
// run_case.sh exists for the four transaction topology cases and defaults them to
// RATE=300 for DURATION=30s, which are sensible figures for comparing queue shapes.
//
// The runner therefore forwards RATE, DURATION, VUS and MAX_VUS only when the CALLER
// set them, leaving events.js to apply its own defaults, which are the criterion's
// figures.
func TestLoadTestRunner_LeavesTheEventLoadShapeToTheScenario(t *testing.T) {
	runner := readRepoFile(t, loadTestRunnerPath)

	eventsDispatch := strings.Index(runner, eventStreamingDispatch)
	require.Greaterf(t, eventsDispatch, 0,
		"%s must dispatch an `event-streaming` case, or the event-streaming acceptance run has "+
			"no documented invocation and its verdicts are never produced", loadTestRunnerPath)

	// Ordering, asserted against the transaction defaults themselves rather than a line number.
	for _, transactionDefault := range []string{
		`DURATION="${DURATION:-30s}"`,
		`RATE="${RATE:-300}"`,
	} {
		assignment := strings.Index(runner, transactionDefault)
		require.Greaterf(t, assignment, 0,
			"%s: the transaction default %s must still be present — this test is about the "+
				"events case not INHERITING it, not about removing it",
			loadTestRunnerPath, transactionDefault)
		assert.Lessf(t, eventsDispatch, assignment,
			"%s: the events case must be dispatched BEFORE %s is assigned. Reaching events.js "+
				"through the transaction defaults would certify V-1 (500/s for 30m) from a "+
				"thirty-second run at 300/s",
			loadTestRunnerPath, transactionDefault)
	}

	// The events branch ALONE, bounded at its own terminator. Slicing to the end of the
	// file would drag the transaction path in and make every absence assertion below
	// vacuous — the transaction invocation legitimately passes an unconditional -e RATE=
	// and drives script.js.
	branchEnd := strings.Index(runner[eventsDispatch:], "\n  exit 0\nfi\n")
	require.Greaterf(t, branchEnd, 0,
		"%s: the events branch must terminate with `exit 0` inside its own `fi`, so it cannot "+
			"fall through into the transaction path", loadTestRunnerPath)

	// Conditional forwarding. The loop passes a setting only when it is non-empty in the
	// caller's environment, so an unset RATE reaches the scenario as absent rather than as 300.
	eventsCase := runner[eventsDispatch : eventsDispatch+branchEnd]
	assert.Containsf(t, eventsCase, `if [[ -n "${!setting:-}" ]]; then`,
		"%s: the events case must forward a load setting only when the caller set it",
		loadTestRunnerPath)

	for _, forwarded := range []string{"RATE", "DURATION", "VUS", "MAX_VUS"} {
		assert.Containsf(t, eventsCase, forwarded,
			"%s: %s must be forwardable when the caller sets it explicitly",
			loadTestRunnerPath, forwarded)
	}

	// The events case must not hand the scenario a load shape of its own under any name.
	for _, substitution := range []string{
		`-e "RATE=${RATE}"`,
		`-e "DURATION=${DURATION}"`,
		`-e RATE=`,
		`-e DURATION=`,
	} {
		assert.NotContainsf(t, eventsCase, substitution,
			"%s: the events case must not pass a load shape unconditionally (%s) — an unset "+
				"value would arrive as the runner's default rather than the criterion's",
			loadTestRunnerPath, substitution)
	}

	// And it must drive the event scenario, not the transaction one.
	assert.Containsf(t, eventsCase, "tests/loadtest/events.js",
		"%s: the events case must run the event scenario", loadTestRunnerPath)
	assert.NotContainsf(t, eventsCase, "tests/loadtest/script.js",
		"%s: the events case must not fall through to the transaction scenario",
		loadTestRunnerPath)
}

// ---------------------------------------------------------------------------
// The measured window must contain the WHOLE unsettled population
// ---------------------------------------------------------------------------

// TestLoadTestSettlingGate_CoversEveryUnsettledOutboxState pins the settling
// population.
//
// The gates at both ends of the measured window waited for the `pending` count alone.
func TestLoadTestSettlingGate_CoversEveryUnsettledOutboxState(t *testing.T) {
	source := loadTestScript(t)

	census := stripLineComments(
		scriptFunctionBody(t, source, "function probeEventStats() {"),
	)

	for _, state := range []string{"pending", "processing", "failed", "replaying"} {
		assert.Containsf(t, census, `readOptionalNumber(body, "`+state+`")`,
			"%s: probeEventStats must read the %q count out of GET /events/stats — it is part of "+
				"the population the settling gates wait on, and a state that is never read is a "+
				"state the window can close over", loadTestScriptPath, state)
	}

	assert.Containsf(t, census,
		"pending + processing + failed + replaying",
		"%s: the unsettled total must be the sum of all four non-terminal Kafka states. A gate "+
			"over `pending` alone closes the window on the rows most likely to be dead-lettered",
		loadTestScriptPath)

	assert.NotContainsf(t, census, `readOptionalNumber(body, "webhook_pending")`,
		"%s: webhook_pending must NOT be in the settling population. Its Kafka leg is "+
			"acknowledged and counted; only the deprecated HTTP leg is owed, so including it would "+
			"tie the acceptance window's length to the transport being retired",
		loadTestScriptPath)

	// ALL FOUR OR NOTHING. A response missing one state must not be summed: the missing count
	// would read as zero, which is the same "the tail was excluded" defect one level up.
	assert.Containsf(t, collapseWhitespace(census),
		"pending === null || processing === null || failed === null || replaying === null",
		"%s: an incomplete census must yield a null total rather than a partial sum",
		loadTestScriptPath)

	// And the gate must wait on that total, not on the pending literal.
	gate := stripLineComments(scriptRegion(t,
		source,
		"  while (Date.now() < deadline) {",
		"  // The budget expired.",
	))

	assert.Containsf(t, gate, "readUnsettledDepth()",
		"%s: the settling gate must read the unsettled population, not a pending-only depth",
		loadTestScriptPath)
	assert.Containsf(t, gate, "if (reading.unsettled <= DRAIN_FLOOR) {",
		"%s: quiescence must be judged against the TOTAL unsettled depth", loadTestScriptPath)
	assert.Containsf(t, gate, "if (stable >= DRAIN_STABLE_SAMPLES) {",
		"%s: quiescence must require the configured number of confirmations, at both ends — both "+
			"ends call this one gate", loadTestScriptPath)
	assert.NotContainsf(t, gate, "reading.pending",
		"%s: the gate must not read a `pending` field; the whole point is that the pending count "+
			"alone is not the population", loadTestScriptPath)
}

// TestLoadTestSettlingGate_FailsClosedOnAnIncompletePopulation pins the fail-closed
// half.
//
// The live census is master-key gated.
//
// So a reading counts only when it is complete, an incomplete gate reports
// `state_unavailable`, and teardown turns that into its own degraded reason rather than
// into "the relay is behind" — because the two remedies are different: a master key
// versus a bigger budget or a faster relay.
func TestLoadTestSettlingGate_FailsClosedOnAnIncompletePopulation(t *testing.T) {
	source := loadTestScript(t)

	depth := stripLineComments(
		scriptFunctionBody(t, source, "function readUnsettledDepth() {"),
	)

	assert.Containsf(t, depth, "complete: true",
		"%s: the live census reading must declare itself complete", loadTestScriptPath)
	assert.Containsf(t, depth, "complete: false",
		"%s: the gauge reading must declare itself INCOMPLETE — it is pending plus processing "+
			"only", loadTestScriptPath)

	gate := stripLineComments(scriptRegion(t,
		source,
		"  while (Date.now() < deadline) {",
		"  // The budget expired.",
	))
	assert.Containsf(t, gate, "var counts = reading.complete;",
		"%s: an incomplete reading must not count towards quiescence", loadTestScriptPath)

	// The reason cascade must distinguish the two causes, and the more specific one must win.
	reason := stripLineComments(scriptRegion(t,
		source,
		"var reason = REASON_AVAILABLE;",
		"degradedReasonCode.add(reason);",
	))
	assert.Containsf(t, reason, "reason = settlementStateUnavailable",
		"%s: an unsettled window whose population could not be read completely must report the "+
			"specific cause", loadTestScriptPath)
	assert.Containsf(t, reason, "REASON_SETTLEMENT_STATE_UNAVAILABLE",
		"%s: the incomplete-population cause must have its own reason code, so an operator is "+
			"sent for a master key rather than for a bigger budget", loadTestScriptPath)
}

// TestLoadTestDeadLetterNumerator_IsASameWindowDelta pins the dead-letter rate's arithmetic.
//
// The numerator comes from the exported counter's delta and from nowhere else, while
// its PROVENANCE could be set to the stats endpoint on the strength of any non-null
// cumulative census value.
func TestLoadTestDeadLetterNumerator_IsASameWindowDelta(t *testing.T) {
	source := loadTestScript(t)

	// setup() must take the baseline census, or there is no left-hand side to the delta.
	setup := stripLineComments(scriptFunctionBody(t, source, "export function setup() {"))
	assert.Containsf(t, setup, "var statsBaseline = probeEventStats();",
		"%s: setup must take a baseline /events/stats reading beside the baseline scrape",
		loadTestScriptPath)
	assert.Containsf(t, setup, "statsBaseline: statsBaseline,",
		"%s: the baseline census must travel to teardown through setup data — teardown is the "+
			"only stage that can difference it", loadTestScriptPath)

	numerator := stripLineComments(scriptRegion(t,
		source,
		"  // --- The dead-letter numerator and its provenance, resolved BEFORE the arithmetic",
		// The region terminator, and it moved when the divisor was corrected: the whole-run
		// mean now divides by the MEASURED WINDOW so its numerator and denominator describe
		// the same interval. See the divisor comment in the scenario for the 2.2x
		// overstatement dividing by the load interval produced.
		"  // THE DIVISOR IS THE MEASURED WINDOW",
	))

	assert.Containsf(t, numerator,
		"var censusDelta = stats.deadLettered - statsBaseline.deadLettered;",
		"%s: the census fallback must be a same-window DELTA, never a cumulative reading",
		loadTestScriptPath)
	assert.Containsf(t, numerator, "censusDelta < 0 ? null : censusDelta",
		"%s: a negative census delta must be refused rather than clamped to zero — it is not a "+
			"count of what this window produced", loadTestScriptPath)
	assert.Containsf(t, collapseWhitespace(numerator),
		"} else if (statsDeadLetteredWindow !== null) { deadLetterProvenance = DEAD_LETTER_SOURCE_STATS_ENDPOINT;",
		"%s: the stats endpoint may only be SELECTED as the provenance when it actually yields a "+
			"window delta", loadTestScriptPath)
	assert.Containsf(t, numerator,
		"deadLetteredEvents = statsDeadLetteredWindow;",
		"%s: when the stats endpoint is the selected provenance its value must BE the numerator. "+
			"Selecting a source and then not using its value is the defect", loadTestScriptPath)

	// The numerator must not be re-derived from the series after the provenance says otherwise.
	assert.NotContainsf(t, collapseWhitespace(numerator),
		"var deadLetteredEvents = deadLetteredChange.value === null ? 0 : deadLetteredChange.value;",
		"%s: the numerator must be taken from the selected provenance rather than unconditionally "+
			"from the series delta", loadTestScriptPath)

	// And an unmeasured numerator must still withhold the verdict.
	reason := stripLineComments(scriptRegion(t,
		source,
		"var reason = REASON_AVAILABLE;",
		"degradedReasonCode.add(reason);",
	))
	assert.Containsf(t, reason,
		"} else if (deadLetterProvenance === DEAD_LETTER_SOURCE_NONE) {",
		"%s: with no series and no window delta, V-3 must be withheld rather than scored from a "+
			"coerced zero", loadTestScriptPath)
}

// TestLoadTestAcceptance_RequiresAnIsolatedInstance pins the attribution contract.
//
// Every verdict is a delta of PROCESS-GLOBAL counters and a quantile over a
// process-global histogram.
func TestLoadTestAcceptance_RequiresAnIsolatedInstance(t *testing.T) {
	source := loadTestScript(t)

	assert.Containsf(t, source,
		"const ISOLATED_INSTANCE = numberFrom(__ENV.ISOLATED_INSTANCE, 0) === 1;",
		"%s: the isolated-instance acknowledgement must default to NOT acknowledged",
		loadTestScriptPath)
	assert.Containsf(t, source,
		"const REQUIRE_ISOLATION = numberFrom(__ENV.REQUIRE_ISOLATION, 1);",
		"%s: the isolation requirement must be on by default, in the same shape as REQUIRE_DRAIN",
		loadTestScriptPath)

	probe := stripLineComments(
		scriptFunctionBody(t, source, "function verifyInstanceIsolation() {"),
	)
	assert.Containsf(t, probe, "if (!ISOLATED_INSTANCE) {",
		"%s: a run without the acknowledgement must not be treated as isolated",
		loadTestScriptPath)
	assert.Containsf(t, probe, "var before = terminalEventTotal(scrapeMetrics());",
		"%s: the probe must read the terminal event counters before the idle interval",
		loadTestScriptPath)
	assert.Containsf(t, probe, "sleep(ISOLATION_PROBE_SECONDS);",
		"%s: the probe must observe an interval in which this run offers no load",
		loadTestScriptPath)
	assert.Containsf(t, probe,
		"if (outcome.foreignEvents > ISOLATION_MAX_FOREIGN_EVENTS) {",
		"%s: events counted while no load was offered must fail the contract", loadTestScriptPath)
	assert.Containsf(t, probe, "if (before === null || after === null) {",
		"%s: a probe that could not be taken must fail the contract rather than pass it — the "+
			"finding is that the contract could not be ESTABLISHED", loadTestScriptPath)

	// PREFLIGHT: the refusal must happen in setup, before the load is offered, so a
	// thirty-minute acceptance run is not spent producing an unquotable number.
	setup := stripLineComments(scriptFunctionBody(t, source, "export function setup() {"))
	assert.Containsf(t, setup, "var isolation = verifyInstanceIsolation();",
		"%s: setup must check attribution", loadTestScriptPath)
	assert.Containsf(t, setup,
		"if (!SMOKE && REQUIRE_ISOLATION === 1 && !isolation.verified) {",
		"%s: acceptance mode must FAIL PREFLIGHT when the isolated-instance contract cannot be "+
			"established, with SMOKE and REQUIRE_ISOLATION=0 as the explicit opt-outs",
		loadTestScriptPath)
	assert.Containsf(t, setup, "throw new Error(",
		"%s: the preflight failure must abort the run rather than being logged and continued",
		loadTestScriptPath)

	// And a relaxed run must still withhold its verdicts under the availability gate.
	reason := stripLineComments(scriptRegion(t,
		source,
		"var reason = REASON_AVAILABLE;",
		"degradedReasonCode.add(reason);",
	))
	assert.Containsf(t, reason, "} else if (!isolationHeld) {",
		"%s: an unattributable population must degrade the verdict inputs", loadTestScriptPath)
	assert.Containsf(t, reason, "reason = REASON_INSTANCE_NOT_ISOLATED;",
		"%s: and it must say so under its own reason code", loadTestScriptPath)

	sound := stripLineComments(scriptRegion(t,
		source,
		"  var measurementSound =",
		// The FULL condition, because the offered-load figure is recorded by a second gate
		// whose first clause is identical; a prefix anchor now matches both.
		"  if (measurementSound && published.code !== SERIES_CODE_ABSENT && measuredWindow > 0) {",
	))
	assert.Containsf(t, sound, "isolationHeld",
		"%s: no verdict gauge may be recorded for a population that is not this run's — an "+
			"unrecorded Gauge reads as 0 and 0 passes both `<` thresholds, so recording a "+
			"contaminated figure is the one way this file could report a false pass",
		loadTestScriptPath)
}

// eventsRunnerBranch returns the text of run_case.sh's `events` case, bounded at its
// own terminator.
func eventsRunnerBranch(t *testing.T, runner string) string {
	t.Helper()

	start := strings.Index(runner, `if [[ "${CASE_NAME}" == "events" ]]; then`)
	require.Greaterf(t, start, 0, "%s must dispatch an `events` case", loadTestRunnerPath)

	end := strings.Index(runner[start:], "\n  exit 0\nfi\n")
	require.Greaterf(t, end, 0,
		"%s: the events branch must terminate with `exit 0` inside its own `fi`",
		loadTestRunnerPath)

	return runner[start : start+end]
}

// TestLoadTestRunner_KeepsCredentialsOutOfArgv pins the secret transport.
//
// The metrics bearer token, the master key and an API key were each passed to k6 as `-e
// NAME=value`, which makes them ARGV ELEMENTS of the k6 process. Argv is not private:
// any account on the host can read /proc/<pid>/cmdline, and process-audit daemons,
// container runtimes and CI diagnostics collect it verbatim into logs this script does
// not control. The master key alone authorises every management endpoint in Blnk.
func TestLoadTestRunner_KeepsCredentialsOutOfArgv(t *testing.T) {
	branch := eventsRunnerBranch(t, readRepoFile(t, loadTestRunnerPath))

	for _, secret := range []string{
		"METRICS_BEARER_TOKEN",
		"MASTER_KEY",
		"API_KEY",
		"BLNK_METRICS_BEARER_TOKEN",
		"BLNK_SERVER_SECRET_KEY",
		"BLNK_API_KEY",
	} {
		assert.NotContainsf(t, branch, `-e "`+secret+`=`,
			"%s: %s must not be passed to k6 as an argument — argv is world-readable through "+
				"/proc/<pid>/cmdline and is collected by audit and CI tooling. Export it instead; "+
				"k6 includes system environment variables in __ENV by default",
			loadTestRunnerPath, secret)
		assert.NotContainsf(t, branch, "-e "+secret+"=",
			"%s: %s must not be passed to k6 as an argument in any quoting style",
			loadTestRunnerPath, secret)
	}

	// The positive half: each one is exported under the name events.js reads.
	for _, exported := range []string{
		"export METRICS_BEARER_TOKEN",
		"export MASTER_KEY",
		"export API_KEY",
	} {
		assert.Containsf(t, branch, exported,
			"%s: the credential must reach the scenario through the inherited environment (%q)",
			loadTestRunnerPath, exported)
	}

	// And the empty-value guard survives the move.
	assert.Containsf(t, branch,
		`if [[ -n "${BLNK_METRICS_BEARER_TOKEN:-${METRICS_BEARER_TOKEN:-}}" ]]; then`,
		"%s: a credential must be exported only when a value exists — an exported empty string "+
			"overrides the scenario's own resolution with nothing", loadTestRunnerPath)
}

// TestLoadTestRunner_RefusesAndRedactsCredentialBearingURLs pins the shell-side URL
// hygiene.
//
// events.js refuses a credential-bearing URL during init and redacts every URL it
// writes into the summary. Neither protects THIS script's output: the runner echoes the
// endpoints it is about to use, and the METRICS_URL-derivation failure echoed the raw
// URL in its error message — both before k6 exists.
func TestLoadTestRunner_RefusesAndRedactsCredentialBearingURLs(t *testing.T) {
	runner := readRepoFile(t, loadTestRunnerPath)
	branch := eventsRunnerBranch(t, runner)

	for _, helper := range []string{
		"redact_url() {",
		"url_carries_credential() {",
		"refuse_credential_bearing_url() {",
	} {
		assert.Containsf(t, runner, helper,
			"%s: the shell needs its own %s — the scenario's refusal happens inside a process this "+
				"script has already logged for", loadTestRunnerPath, helper)
	}

	// The refusal covers the caller's URL and the derived metrics endpoint, and it runs BEFORE
	// anything is echoed.
	refusal := strings.Index(branch, `refuse_credential_bearing_url "URL"`)
	require.Greaterf(t, refusal, 0,
		"%s: the events case must refuse a credential-bearing URL", loadTestRunnerPath)
	assert.Containsf(t, branch, `refuse_credential_bearing_url "METRICS_URL"`,
		"%s: the metrics endpoint must be refused on the same terms as the transactions endpoint",
		loadTestRunnerPath)

	firstEcho := strings.Index(branch, `echo "  transactions :`)
	require.Greaterf(t, firstEcho, 0,
		"%s: the events case must announce the endpoints it uses", loadTestRunnerPath)
	assert.Lessf(t, refusal, firstEcho,
		"%s: the refusal must precede every diagnostic that prints an endpoint", loadTestRunnerPath)

	// Every echo of either endpoint is redacted, including the derivation failure.
	for _, redacted := range []string{
		`echo "  transactions : $(redact_url "${URL}")"`,
		`echo "  metrics      : $(redact_url "${METRICS_URL}")"`,
		`echo "error: METRICS_URL cannot be derived from URL=$(redact_url "${URL}")"`,
	} {
		assert.Containsf(t, branch, redacted,
			"%s: this diagnostic must print a redacted endpoint (%q)", loadTestRunnerPath, redacted)
	}

	assert.NotContainsf(t, branch, `echo "  transactions : ${URL}"`,
		"%s: the raw URL must not be echoed", loadTestRunnerPath)
	assert.NotContainsf(t, branch, `URL=${URL}"`+"\n",
		"%s: the derivation failure must not echo the raw URL", loadTestRunnerPath)
}

// TestLoadTestRunner_RefusesAnUnattributableAcceptanceRun pins the runner's half of the
// attribution contract.
//
// The scenario checks isolation empirically and refuses a minute into the run. The
// runner refuses at the point the operator reads the command, which is where the
// requirement belongs: the run's figures are process-global, so a shared deployment
// produces numbers that are not attributable to the invocation however carefully the
// rest of the harness measures them.
func TestLoadTestRunner_RefusesAnUnattributableAcceptanceRun(t *testing.T) {
	branch := eventsRunnerBranch(t, readRepoFile(t, loadTestRunnerPath))

	assert.Containsf(t, branch, `[[ "${ISOLATED_INSTANCE:-0}" != "1" ]]`,
		"%s: an acceptance events run must require the isolated-instance acknowledgement, and its "+
			"default must be NOT acknowledged", loadTestRunnerPath)
	assert.Containsf(t, branch, `[[ "${REQUIRE_ISOLATION:-1}" == "1" ]]`,
		"%s: the requirement must be on by default", loadTestRunnerPath)
	assert.Containsf(t, branch, `[[ "${SMOKE:-0}" != "1" ]]`,
		"%s: SMOKE must remain the documented shakeout opt-out", loadTestRunnerPath)
	assert.Containsf(t, branch, "ISOLATED_INSTANCE=1 bash tests/loadtest/run_case.sh events",
		"%s: the refusal must print the exact command that satisfies it", loadTestRunnerPath)

	// It must actually stop the run rather than warn.
	isolationGate := strings.Index(branch, `[[ "${ISOLATED_INSTANCE:-0}" != "1" ]]`)
	require.Greaterf(t, isolationGate, 0, "%s: the isolation gate must exist", loadTestRunnerPath)
	assert.Containsf(t, branch[isolationGate:], "exit 1",
		"%s: an unattributable acceptance run must be refused, not merely warned about",
		loadTestRunnerPath)

	// And the k6 invocation must come after the gate.
	invocation := strings.Index(branch, "k6 run ")
	require.Greaterf(t, invocation, 0, "%s: the events case must invoke k6", loadTestRunnerPath)
	assert.Lessf(t, isolationGate, invocation,
		"%s: the gate must precede the load, or it costs the thirty minutes it exists to save",
		loadTestRunnerPath)
}

// TestLoadTestRunner_ForwardsTheFixtureAndSettlingContract pins the variables the
// documented commands depend on.
//
// events.js REFUSES to provision fixtures unless LEDGER_PAIRS names existing ones or
// ALLOW_FIXTURE_CREATION=1 acknowledges that the run permanently adds ledgers and
// balances to a database with no delete endpoint for either. A runner that dropped them
// silently turned a documented command into an abort before load generation, and the
// guide's own examples were written against the abort.
func TestLoadTestRunner_ForwardsTheFixtureAndSettlingContract(t *testing.T) {
	branch := eventsRunnerBranch(t, readRepoFile(t, loadTestRunnerPath))

	forwarding := strings.Index(branch, "for setting in RATE DURATION")
	require.Greaterf(t, forwarding, 0,
		"%s: the events case must forward its settings from one conditional loop",
		loadTestRunnerPath)

	loop := branch[forwarding:]
	if end := strings.Index(loop, "\n  done\n"); end > 0 {
		loop = loop[:end]
	}

	for _, setting := range []string{
		// The fixture contract, without which the documented command aborts.
		"LEDGER_PAIRS",
		"ALLOW_FIXTURE_CREATION",
		// The attribution contract.
		"ISOLATED_INSTANCE",
		"REQUIRE_ISOLATION",
		"ISOLATION_PROBE_SECONDS",
		"ISOLATION_MAX_FOREIGN_EVENTS",
		// The settling contract, whose budgets an operator has to be able to raise.
		"REQUIRE_DRAIN",
		"PRE_DRAIN_BUDGET_SECONDS",
		"POST_DRAIN_BUDGET_SECONDS",
	} {
		assert.Containsf(t, loop, setting,
			"%s: %s must be forwardable to the scenario when the caller sets it — the guide's "+
				"commands depend on it", loadTestRunnerPath, setting)
	}

	assert.Containsf(t, loop, `if [[ -n "${!setting:-}" ]]; then`,
		"%s: every one of them must be forwarded only when the caller set it, so an unset value "+
			"reaches the scenario as absent rather than as the runner's opinion", loadTestRunnerPath)
}

// ---------------------------------------------------------------------------
// The guide must describe the harness that exists
// ---------------------------------------------------------------------------

// loadTestGuidePath is the runbook an operator reads before quoting a verdict.

// guideEventCommands returns every `run_case.sh events` invocation the guide
// advertises, one logical command per element, taken only from its fenced shell blocks.
func guideEventCommands(t *testing.T, guide string) []string {
	t.Helper()

	var commands []string
	inFence := false
	pending := ""

	for _, raw := range strings.Split(guide, "\n") {
		trimmed := strings.TrimSpace(raw)

		if strings.HasPrefix(trimmed, "```") {
			// A fence boundary always ends an unterminated continuation.
			inFence = strings.HasPrefix(trimmed, "```bash")
			pending = ""
			continue
		}
		if !inFence || trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		if strings.HasSuffix(trimmed, `\`) {
			pending += strings.TrimSuffix(trimmed, `\`) + " "
			continue
		}

		logical := collapseWhitespace(pending + trimmed)
		pending = ""

		if strings.Contains(logical, "run_case.sh events") ||
			strings.Contains(logical, "run_case.sh event-streaming") {
			commands = append(commands, logical)
		}
	}

	require.NotEmptyf(t, commands,
		"%s must show at least one event-streaming invocation, or an operator has nothing to copy",
		loadTestGuidePath)

	return commands
}

// guideSection returns the text of one Markdown section of the guide, from its heading
// to the next heading at the same or a higher level.
//
// Assertions are scoped to a section rather than to the whole document for two reasons.
func guideSection(t *testing.T, guide, heading string) string {
	t.Helper()

	require.Equalf(t, 1, strings.Count(guide, "\n"+heading+"\n"),
		"%s must carry the section %q exactly once", loadTestGuidePath, heading)

	rest := guide[strings.Index(guide, "\n"+heading+"\n")+1+len(heading):]
	level := strings.Count(strings.SplitN(heading, " ", 2)[0], "#")

	for depth := level; depth >= 2; depth-- {
		if next := strings.Index(rest, "\n"+strings.Repeat("#", depth)+" "); next > 0 {
			rest = rest[:next]
		}
	}

	return rest
}

// TestLoadTestGuide_DocumentsOnlyRunnableEventCommands pins every advertised command to
// the two prerequisites the scenario actually enforces.
//
// events.js refuses to provision fixtures unless LEDGER_PAIRS names existing ones or
// ALLOW_FIXTURE_CREATION=1 acknowledges that the run permanently adds ledgers and
// balances to a database with no delete endpoint for either, and — since the
// attribution fix — both the runner and the scenario refuse an acceptance run that has
// not been declared to own its instance. Every command the guide advertised predated
// both refusals, so the documented way to run the acceptance case aborted before load
// generation.
func TestLoadTestGuide_DocumentsOnlyRunnableEventCommands(t *testing.T) {
	commands := guideEventCommands(t, readRepoFile(t, loadTestGuidePath))

	for _, command := range commands {
		attributable := strings.Contains(command, "ISOLATED_INSTANCE=1") ||
			strings.Contains(command, "SMOKE=1") ||
			strings.Contains(command, "REQUIRE_ISOLATION=0")
		assert.Truef(t, attributable,
			"%s: %q must carry ISOLATED_INSTANCE=1 — or an explicit SMOKE=1/REQUIRE_ISOLATION=0 "+
				"opt-out — because both the runner and the scenario refuse an acceptance run whose "+
				"process-global counters cannot be attributed to it",
			loadTestGuidePath, command)

		fixtured := strings.Contains(command, "LEDGER_PAIRS") ||
			strings.Contains(command, "ALLOW_FIXTURE_CREATION=1") ||
			strings.Contains(command, "SMOKE=1") ||
			strings.Contains(command, "LEDGER_SPREAD=0")
		assert.Truef(t, fixtured,
			"%s: %q must make a fixture decision — LEDGER_PAIRS to reuse, "+
				"ALLOW_FIXTURE_CREATION=1 to acknowledge a permanent addition, SMOKE=1 for a "+
				"shakeout, or LEDGER_SPREAD=0 for the deliberate single-aggregate run — or the "+
				"scenario throws before offering any load",
			loadTestGuidePath, command)
	}

	// Both fixture paths must be DEMONSTRATED, not merely named: an operator on a shared
	// database cannot use the disposable form, and one on a disposable database should not
	// be pushed into maintaining a pairs file.
	joined := strings.Join(commands, "\n")
	assert.Containsf(t, joined, "ALLOW_FIXTURE_CREATION=1",
		"%s: the disposable-database path must appear as a runnable command", loadTestGuidePath)
	assert.Containsf(t, joined, "LEDGER_PAIRS=",
		"%s: the reusable-fixtures path must appear as a runnable command", loadTestGuidePath)
}

// TestLoadTestGuide_StatesTheSettlingPopulationAndItsFailClosedRule pins the guide to
// the gate that was actually built.
//
// The guide described the settling gates as waiting for the outbox's PENDING depth to
// reach zero, which is what the gates once did and is not what settlement means.
//
// The four states by name, `webhook_pending`'s exclusion and the reason for it —
// otherwise the next reader "completes" the population with it and ties the acceptance
// window's length to the transport being retired — and the fail-closed rule, because a
// run without a master key reads the population from blnk_outbox_pending, which is
// pending plus processing only. That run withholds its verdicts, and an operator who
// does not know it will read the withheld rows as a stack problem rather than as a
// missing credential.
func TestLoadTestGuide_StatesTheSettlingPopulationAndItsFailClosedRule(t *testing.T) {
	guide := guideSection(t, readRepoFile(t, loadTestGuidePath),
		`### The settling gates: what "settled" covers`)

	// The section's own table is the population, one row per state. Its ROWS are what this
	// asserts on: a gate that waits on fewer states than the scenario computes, or a guide
	// that adds the excluded one, changes the row set — while the reason column beside each
	// row is prose and is free to be rewritten.
	population := markdownTableIn(t, guide, loadTestGuidePath,
		[]string{"State", "Why it is un-settled"})

	for _, state := range []string{"pending", "processing", "failed", "replaying"} {
		_, present := population.Row(state)
		assert.Truef(t, present,
			"%s: the settling population must carry a row for %q, or the guide describes a narrower "+
				"gate than the one that runs. Rows present: %v",
			loadTestGuidePath, state, population.Keys())
	}

	// The excluded state, asserted as the ABSENCE OF A ROW rather than as a sentence about
	// it: a later reader who "completes" the population adds a row, and that is what ties
	// the acceptance window's length to the transport being retired.
	_, excluded := population.Row("webhook_pending")
	assert.Falsef(t, excluded,
		"%s: webhook_pending must NOT be a row in the settling population — such a row's Kafka leg "+
			"is already acknowledged and counted, so waiting for it would make the window's length "+
			"depend on the deprecated HTTP leg", loadTestGuidePath)

	assert.Lenf(t, population.Keys(), 4,
		"%s: the settling population is exactly the four states from which a Kafka publish is still "+
			"owed. Rows present: %v", loadTestGuidePath, population.Keys())

	// The fallback source and the withheld-verdict flag, by the identifiers a reader greps
	// for rather than by the sentences that introduce them.
	for _, identifier := range []string{"blnk_outbox_pending", "event_publish_verdicts_available"} {
		assert.Containsf(t, readRepoFile(t, loadTestGuidePath), identifier,
			"%s must name %s: it is what makes a master-key-less run uncertifiable rather than "+
				"merely less precise, and a reader cannot look up a series the guide never names",
			loadTestGuidePath, identifier)
	}
}

// TestLoadTestGuide_StatesTheSameWindowDeadLetterNumerator pins that arithmetic as
// documented.
func TestLoadTestGuide_StatesTheSameWindowDeadLetterNumerator(t *testing.T) {
	guide := guideSection(t, readRepoFile(t, loadTestGuidePath), "### Where each verdict comes from")

	// Every assertion here names an ARTEFACT the scenario emits or a series it reads, so the
	// section's prose can be rewritten while the arithmetic it describes stays answerable to
	// the same identifiers a reader would grep for.
	for identifier, why := range map[string]string{
		"verdicts.dead_letter_ratio.census_numerator": "the summary field carrying the baseline reading, the final reading and the delta, " +
			"without which the arithmetic cannot be redone by hand",
		"blnk_events_dead_lettered_total": "the counter source of the numerator, differenced over the window",
		"event_publish_verdicts_available": "the flag that goes to 0 when neither source yields a " +
			"window delta, which is how a withheld verdict is told apart from a measured zero",
		"GET /events/stats": "the census the fallback numerator is read from",
	} {
		assert.Containsf(t, guide, identifier,
			"%s: the verdict section must name %s — %s", loadTestGuidePath, identifier, why)
	}
}

// TestLoadTestGuide_DoesNotReadTheP99DifferenceAsAQueueWait pins the two figures the
// guide must label as diagnostics rather than as verdict inputs, by the series names the
// scenario publishes them under.
//
// The names are the contract: `event_publish_p99_difference_seconds` is the difference of
// two independently ranked p99 values, which is not the p99 of any difference and can be
// negative, and `blnk_events_publish_duration_seconds` starts its clock at the relay's
// claim rather than at capture. A guide that introduces either without naming it cannot
// be looked up against the summary artefact, and that — rather than any particular
// sentence about queue waits — is what a reader is left unable to check.
func TestLoadTestGuide_DoesNotReadTheP99DifferenceAsAQueueWait(t *testing.T) {
	guide := readRepoFile(t, loadTestGuidePath)
	verdicts := guideSection(t, guide, "### Reading the verdict honestly")

	for series, why := range map[string]string{
		"event_publish_p99_difference_seconds": "the difference of two independently ranked p99 " +
			"values, which is a diagnostic and not a percentile of anything",
		"blnk_events_publish_duration_seconds": "the publish-duration histogram, whose clock starts " +
			"at the relay's claim rather than at capture, so it cannot certify V-1",
	} {
		assert.Containsf(t, verdicts, series,
			"%s: the verdict section must name %s — %s", loadTestGuidePath, series, why)
	}

	// The scenario's own provenance keys, which is where a reader goes to confirm the
	// labelling. Asserted against the script so the guide and the artefact cannot describe
	// different figures.
	source := loadTestScript(t)
	for _, key := range []string{"diagnosticPublishSeries", "diagnosticPublishP99"} {
		assert.Containsf(t, source, key,
			"%s: the publish-duration figure travels in the summary under %s, which is what makes it "+
				"a labelled diagnostic rather than an unmarked latency number", loadTestScriptPath, key)
	}
}

// ---------------------------------------------------------------------------
// The verdict must be decided by what was measured, and by what was registered
// ---------------------------------------------------------------------------

// TestLoadTestHarness_VerdictInputsAreMeasuredNotAssumed pins the eight defects that
// together would make the latency and dead-letter verdicts untrustworthy, whichever way
// they came out.
//
// Every one of them is the same shape: a figure that LOOKS measured and is not.
func TestLoadTestHarness_VerdictInputsAreMeasuredNotAssumed(t *testing.T) {
	source := loadTestScript(t)

	t.Run("PERF-m04 the dropped-iteration tolerance is inclusive", func(t *testing.T) {
		assert.Containsf(t, source, `o["dropped_iterations"] = ["count<=" + maxDroppedIterations()]`,
			"%s: maxDroppedIterations returns a MAXIMUM TOLERATED count and floors at 1 so that the "+
				"single iteration k6's arrival-rate executor drops while ramping VUs does not fail "+
				"the run. Expressed exclusively as `count<`, that floor forbids the one drop it "+
				"exists to allow, and a tolerance of N accepts only N-1", loadTestScriptPath)
	})

	t.Run("PERF-M10 there is one sampling cadence", func(t *testing.T) {
		assert.Containsf(t, source, "const SUBWINDOW_SECONDS = RATE_WINDOW_SECONDS;",
			"%s: SUBWINDOW_SECONDS must be an ALIAS of RATE_WINDOW_SECONDS, not a second knob. Two "+
				"constants for one concept defaulted to 30 and 60 while every sleep the sampler "+
				"performs read the first — so the enable gate demanded a duration the measurement "+
				"never needed, the warm-up excluded two windows while claiming one, and the summary "+
				"reported a window width the verdict was not stated over", loadTestScriptPath)

		for _, alias := range []string{"__ENV.SUBWINDOW_SECONDS", "__ENV.SAMPLE_INTERVAL_SECONDS"} {
			assert.Containsf(t, source, alias,
				"%s: %s must remain an accepted spelling of the cadence, or a runbook invoking the "+
					"older name silently gets the default instead of the value it asked for",
				loadTestScriptPath, alias)
		}
	})

	t.Run("PERF-M10 every sampler-fed threshold is gated on the sampler", func(t *testing.T) {
		body := scriptFunctionBody(t, source, "function verdictThresholds() {")
		code := stripLineComments(body)

		gate := strings.Index(code, "if (RATE_SAMPLER_ENABLED) {")
		require.Greaterf(t, gate, 0,
			"%s: verdictThresholds must gate the sampler's thresholds on RATE_SAMPLER_ENABLED",
			loadTestScriptPath)
		closes := strings.Index(code[gate:], "\n  }\n")
		require.Greaterf(t, closes, 0,
			"%s: the sampler threshold block must close at function-body indentation",
			loadTestScriptPath)
		samplerBlock := code[gate : gate+closes]

		// Every metric only the sampler scenario feeds. Registering any of these outside the
		// block means an unsampled run is judged on a metric that received no samples: the
		// Counter is evaluated at zero and FAILS, the Rate passes VACUOUSLY, and the run
		// fails naming a subwindow count on a configuration that deliberately has no
		// subwindows.
		for _, metric := range []string{
			"M_WINDOW_EVENTS_PER_SEC",
			"M_RATE_WINDOWS_COUNTED",
			"M_INTERVAL_EVENTS_PER_SEC",
			"M_SUBWINDOW_MET_TARGET",
			"M_SUBWINDOWS_QUALIFYING",
		} {
			assert.Containsf(t, samplerBlock, "o["+metric+"]",
				"%s: %s is fed ONLY by the sampler scenario, so its threshold must be registered "+
					"inside the RATE_SAMPLER_ENABLED block. Outside it, a run with the sampler off "+
					"— which is what RATE_WINDOW_SECONDS=0 asks for, and what any run too short "+
					"for two windows gets — cannot pass however healthy the pipeline is",
				loadTestScriptPath, metric)
		}

		// And the unconditional ones must NOT be inside it: gating the latency or dead-letter
		// threshold on the sampler would silently withdraw the p99 and the dead-letter rate entirely.
		for _, metric := range []string{"M_P99_SECONDS", "M_DEAD_LETTER_RATIO", "M_VERDICTS_AVAILABLE"} {
			assert.NotContainsf(t, samplerBlock, "o["+metric+"]",
				"%s: %s must be registered unconditionally — it does not come from the sampler, and "+
					"gating it would withdraw a criterion whenever the sampler is off",
				loadTestScriptPath, metric)
		}
	})

	t.Run("PERF-M09 verdict rows resolve against the registered graph", func(t *testing.T) {
		// No row may hand-wire a threshold family. Hand-wiring is what let one row display
		// min(window_events_per_second) beside interval_events_per_second's p(50) result.
		assert.NotContainsf(t, source, "thresholds: thresholdVerdicts(",
			"%s: a verdict row must not attach a threshold family by hand. Declare the metrics it "+
				"is `certified_by` and let verdictThresholdsFor resolve them against the same "+
				"verdictThresholds() that builds `options`, so a row can neither report a family "+
				"that was never registered nor omit one that was", loadTestScriptPath)

		assert.GreaterOrEqualf(t, strings.Count(source, "certified_by:"), 5,
			"%s: every verdict row must declare the metrics it is certified by", loadTestScriptPath)

		// The aggregate must exclude diagnostics and require at least one real verdict.
		assert.Containsf(t, source, "provenance.verdicts_all_hold = certifyingRows > 0 ? allHold : false;",
			"%s: verdicts_all_hold must be computed over the CERTIFYING rows only, and must require "+
				"at least one. Treating every row as certifying made it unable to be true in either "+
				"configuration — with the sampler on the whole-run mean has no registered threshold, "+
				"and with it off the sustained row has no value — so the headline line read FAIL on "+
				"every run, including runs k6 itself exited 0 on", loadTestScriptPath)

		// A diagnostic must be distinguishable from a failure, in the JSON and on the terminal.
		assert.Containsf(t, source, `return "n/a ";`,
			"%s: verdictMark must render a null verdict as its own outcome. A row with no registered "+
				"threshold was measured and reported but not asserted on; printing FAIL beside a "+
				"healthy figure states the opposite of the truth", loadTestScriptPath)

		// EVERY CERTIFYING ROW MUST BE ABLE TO HOLD. verdictHolds requires a non-null
		// `value`, which is right — an unrecorded Gauge reads as 0 and 0 satisfies both `<
		// ceiling` verdicts, so an absent figure must not count as a passing one. But the
		// subwindow row is a FRACTION rather than a measurement against a bound and carried
		// no `value` at all, so it reported FAIL with both of its thresholds green.
		assert.Containsf(t, source, "value: subwindowFraction,",
			"%s: the sustained-subwindow row must publish the figure its verdict is read from. The "+
				"fraction is the right one: null exactly when NO subwindow was judged, which is the "+
				"unmeasured case the value check exists to catch, and a number whenever one was",
			loadTestScriptPath)
		assert.Containsf(t, source, "fraction_meeting_target: subwindowFraction,",
			"%s: the row's verdict figure and its reported figure must be the SAME expression, or "+
				"the number a reader sees and the number the verdict was taken from can diverge",
			loadTestScriptPath)
	})

	t.Run("PERF-C01 the drain gate counts every unsettled state", func(t *testing.T) {
		// readUnsettledDepth, not readPendingDepth: the name states that the gate does not
		// read `pending` alone, so the name asserted here is the one the correct
		// implementation carries.
		body := scriptFunctionBody(t, source, "function readUnsettledDepth() {")

		assert.Containsf(t, body, "stats.unsettled !== null",
			"%s: readUnsettledDepth must gate on the UNSETTLED total, not on `pending`. Only "+
				"dispatched and dead_lettered are terminal, so pending is one of several "+
				"non-terminal states and the smallest of them during an outage", loadTestScriptPath)
		assert.NotContainsf(t, body, "if (stats.pending !== null) {",
			"%s: gating on `pending` alone declares the outbox drained while rows are in "+
				"processing, failed or replaying. `failed` is the damaging one: it "+
				"has not yet incremented blnk_events_dead_lettered_total, so closing the window "+
				"removes those rows from V-3's NUMERATOR and understates the dead-letter rate — the "+
				"one direction that lets a failing pipeline certify against a 0.001 threshold",
			loadTestScriptPath)

		// Each state the gate waits on must actually be read.
		//
		// FOUR, not five.
		probe := scriptFunctionBody(t, source, "function probeEventStats() {")
		for _, status := range []string{"pending", "processing", "failed", "replaying"} {
			assert.Containsf(t, probe, `readOptionalNumber(body, "`+status+`")`,
				"%s: probeEventStats must read the %q count; a state it does not read is a state the "+
					"gate treats as empty", loadTestScriptPath, status)
		}
		// INCOMPLETE MEANS NULL, NOT ZERO. The census propagates unreadability into the total
		// instead of tracking it in a separate flag, so there is no way to hold a summed
		// total and a "not really complete" marker that disagree with each other: an absent
		// key makes `unsettled` null, the gate reports that it could not read the population,
		// and the run withholds.
		assert.Containsf(t, collapseWhitespace(probe),
			"? null : pending + processing + failed + replaying;",
			"%s: a MISSING status key must mark the count incomplete rather than contribute zero. "+
				"Silence read as 'nothing owed' is the whole finding", loadTestScriptPath)
	})

	t.Run("PERF-M07 the freshness gauge is actually collected", func(t *testing.T) {
		body := scriptFunctionBody(t, source, "function collectSnapshot(index) {")

		assert.Containsf(t, body, "SERIES_COLLECTION_AGE[c]",
			"%s: collectSnapshot must put the collection-age gauge into snapshot.gauges. "+
				"readUnsettledDepth looks it up there to decide whether a reading is FRESH, so while "+
				"it was declared and never collected the lookup could only miss: age was always "+
				"null, every reading counted as a new observation, and DRAIN_STABLE_SAMPLES was "+
				"satisfied by three polls of ONE collection — exactly what that constant exists to "+
				"prevent, since the server republishes the backlog on its own tick",
			loadTestScriptPath)
		assert.Containsf(t, body, "SERIES_REPAIR_BACKLOG[c]",
			"%s: collectSnapshot must collect the repair backlogs. blnk_outbox_pending is pending "+
				"plus processing only, so without them the gauge fallback reproduces the PERF-C01 "+
				"defect — and an unset master key is all it takes to reach that path",
			loadTestScriptPath)
	})

	t.Run("PERF-M01 the dead-letter numerator comes from its named source", func(t *testing.T) {
		// The window delta off the census is statsDeadLetteredWindow, and the source is a
		// three-valued provenance rather than a bare null check — none / series / stats
		// endpoint, published as a gauge so a reader can see which one produced the number.
		assert.Containsf(t, source, "deadLetteredEvents = statsDeadLetteredWindow;",
			"%s: when the provenance names the stats endpoint, the numerator must be the endpoint's "+
				"own delta. It used to read the metrics delta unconditionally — null for an absent "+
				"series, so 0 — and V-3 then reported a 0.000000 dead-letter rate, passed its "+
				"`value<0.001` threshold, and credited a source that had contributed nothing",
			loadTestScriptPath)
		assert.Containsf(t, source, "} else if (statsDeadLetteredWindow !== null) {",
			"%s: the stats provenance may be claimed only when a usable delta EXISTS. Claiming it "+
				"whenever the endpoint merely answered is what decoupled the label from the "+
				"arithmetic", loadTestScriptPath)
		assert.Containsf(t, source, "baseline.statsBaseline",
			"%s: a rate needs two readings, so setup must capture the outbox baseline the delta is "+
				"taken against — after the pre-load gate, so provisioning's events are excluded",
			loadTestScriptPath)
	})

	t.Run("PERF-M08 the measurement is single-process or it is withheld", func(t *testing.T) {
		assert.Containsf(t, source, "const SERVER_REPLICAS = numberFrom(__ENV.SERVER_REPLICAS, 1);",
			"%s: the number of processes the metrics endpoint fronts must be DECLARED. Every verdict "+
				"is a delta of a per-process counter read from one URL, and this repository ships a "+
				"horizontally scaled server behind a Service", loadTestScriptPath)
		assert.Containsf(t, source, "reason = REASON_MEASUREMENT_NOT_SINGLE_PROCESS;",
			"%s: a declared replica count other than 1 must WITHHOLD the verdicts. A run against "+
				"several replicas measures one replica's share and reporting it as the pipeline's "+
				"throughput understates it by the replica count", loadTestScriptPath)
		assert.Containsf(t, source, "samplerIdentityChanges++",
			"%s: the sampler must compare the process identity between consecutive readings. The "+
				"baseline-versus-final check cannot see this — it compares the two ends and never "+
				"the samples between them — so a run whose sampler rotated across replicas passed "+
				"it while every per-interval delta spanned unrelated counters", loadTestScriptPath)
		assert.Containsf(t, source, "M_SAMPLER_IDENTITY_CHANGES,\n  );",
			"%s: the observed identity changes must be read back from the gauge in buildProvenance. "+
				"k6 gives each VU its own module context, so the sampler's counter does not exist in "+
				"teardown's and the gauge is the only channel it has", loadTestScriptPath)
	})
}

// loadTestGuidePath is the guide an operator runs the acceptance case from.
// TestLoadTestGuide_PublishesARunnableRecipe pins the runnable recipe the guide owes.
//
// This guide is the only instruction for producing the throughput, latency and
// dead-letter evidence, and its
// commands were not runnable: `events.js` refuses to create ledgers and balances
// implicitly — Blnk has no DELETE endpoint for either, so anything provisioned is
// permanent — and it aborts inside `setup()` until one of three fixture choices is
// stated. The documented commands stated none, so the canonical path ended in a stack
// trace.
func TestLoadTestGuide_PublishesARunnableRecipe(t *testing.T) {
	guide := readRepoFile(t, loadTestGuidePath)

	t.Run("DOC-M11 every runnable command states a fixture choice", func(t *testing.T) {
		blocks := regexp.MustCompile("(?s)```bash\n(.*?)```").FindAllStringSubmatch(guide, -1)
		require.NotEmpty(t, blocks, "%s must contain bash examples", loadTestGuidePath)

		invoked := 0
		for _, block := range blocks {
			body := block[1]
			if !strings.Contains(body, "run_case.sh events") &&
				!strings.Contains(body, "run_case.sh event-streaming") {
				continue
			}
			// A block may opt out by SAYING it is incomplete, which is how the two-names example
			// shows the spelling without pretending to be a recipe.
			if strings.Contains(body, "incomplete") {
				continue
			}

			invoked++
			assert.Truef(t,
				strings.Contains(body, "SMOKE=1") ||
					strings.Contains(body, "ALLOW_FIXTURE_CREATION=1") ||
					strings.Contains(body, "LEDGER_PAIRS"),
				"%s: this command invokes the event case with no fixture choice, so it ABORTS in "+
					"setup() rather than running — Blnk cannot delete a ledger or a balance, so "+
					"events.js requires LEDGER_PAIRS, ALLOW_FIXTURE_CREATION=1 or SMOKE=1 before it "+
					"will provision anything:\n%s", loadTestGuidePath, strings.TrimSpace(body))
		}

		require.GreaterOrEqualf(t, invoked, 2,
			"%s must document at least a full recipe and a smoke run", loadTestGuidePath)
	})

	t.Run("DOC-M11 the recipe names every required variable", func(t *testing.T) {
		for variable, why := range map[string]string{
			"API_KEY": "./.env ships no API key, so without one every POST /ledgers, POST /balances " +
				"and POST /transactions is refused with 401 — and the run then aborts blaming the " +
				"partition-key spread, several layers from the cause",
			"SERVER_REPLICAS": "every verdict is a delta of a per-process counter read from one URL, " +
				"so the number of processes that URL fronts has to be declared or the verdicts are " +
				"withheld",
			"LEDGER_SPREAD": "acceptance mode requires that many distinct partition keys and refuses " +
				"to run on fewer, so a reuse command supplying N pairs must say so",
			"DRAIN_FLOOR": "on a shared database the outbox never reaches zero, so the settling gate " +
				"exhausts its budget and the run withholds its verdicts",
			"blnk workers": "the worker drains the transaction queue; with only the server running " +
				"the load is accepted and never processed, and no event is ever published",
		} {
			assert.Containsf(t, guide, variable,
				"%s must document %s: %s", loadTestGuidePath, variable, why)
		}
	})

	t.Run("DOC-m03 the p99 difference is never called a queue wait", func(t *testing.T) {
		// The phrase may appear only in a sentence that DENIES it. Quantiles are not
		// subtractive: the difference of two independently ranked p99 values is not any
		// event's latency, and can be negative.
		for _, line := range strings.Split(guide, "\n") {
			if !strings.Contains(line, "queue wait") {
				continue
			}
			assert.Truef(t, strings.Contains(line, "not"),
				"%s: this line calls the difference of two p99 values a queue wait without denying "+
					"it. Quantiles do not subtract to a duration — the event at the 99th percentile "+
					"of one population is not the event at the 99th percentile of the other, and the "+
					"result can be negative: %s", loadTestGuidePath, strings.TrimSpace(line))
		}
	})

	t.Run("the guide agrees with the runner about artifacts", func(t *testing.T) {
		// The raw NDJSON stream is opt-in, so a default run writes ONE file. The guide is held
		// to naming the two variables that turn the second one on, which is what a reader needs
		// to reach it; how many files a run writes is then answerable from those names rather
		// than from a promise in prose.
		for _, opt := range []string{"RAW_OUTPUT", "NDJSON_OUT"} {
			assert.Containsf(t, guide, opt,
				"%s must document %s, or the raw stream is unreachable from the guide",
				loadTestGuidePath, opt)
		}
	})
}

// stripShellComments removes whole-line shell comments from a region.
//
// The shell counterpart of stripLineComments, and needed for the same reason: the
// runner's rationale comments deliberately NAME the argv form that was removed, so an
// absence assertion over the raw text would report the explanation as the defect.
func stripShellComments(region string) string {
	lines := strings.Split(region, "\n")
	kept := make([]string, 0, len(lines))

	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}

		kept = append(kept, line)
	}

	return strings.Join(kept, "\n")
}

// eventsRunnerBranchStripped returns the runner's events case alone, with comments
// stripped.
func eventsRunnerBranchStripped(t *testing.T) string {
	t.Helper()

	runner := readRepoFile(t, loadTestRunnerPath)

	start := strings.Index(runner, `if [[ "${CASE_NAME}" == "events" ]]; then`)
	require.Greaterf(t, start, 0, "%s must dispatch an `events` case", loadTestRunnerPath)

	length := strings.Index(runner[start:], "\n  exit 0\nfi\n")
	require.Greaterf(t, length, 0,
		"%s: the events branch must terminate with `exit 0` inside its own `fi`",
		loadTestRunnerPath)

	return stripShellComments(runner[start : start+length])
}

// eventCredentialEnvNames are the three credentials the events case supplies to the
// scenario, under the names events.js actually reads them by.
var eventCredentialEnvNames = []string{"METRICS_BEARER_TOKEN", "MASTER_KEY", "API_KEY"}

// TestLoadTestRunner_KeepsCredentialsOutOfProcessArgv keeps secrets out of argv.
//
// The three values are secrets: the metrics bearer token reads /metrics, the master key
// reads GET /events/stats, and the API key posts the load.
func TestLoadTestRunner_KeepsCredentialsOutOfProcessArgv(t *testing.T) {
	eventsCase := eventsRunnerBranchStripped(t)

	for _, name := range eventCredentialEnvNames {
		// Both quoting styles, because the runner uses one and the transaction path the other,
		// and a re-introduction could pick either.
		for _, argvForm := range []string{`-e "` + name + `=`, `-e ` + name + `=`} {
			assert.NotContainsf(t, eventsCase, argvForm,
				"%s: the events case must not pass %s on the command line (%q). argv is "+
					"world-readable through /proc/<pid>/cmdline and is captured by CI logs and "+
					"`ps`, so a credential there leaks to any user on the host and outlives the "+
					"run. Export it instead: k6 passes the real environment into __ENV",
				loadTestRunnerPath, name, argvForm)
		}

		assert.Containsf(t, eventsCase, `export `+name+`=`,
			"%s: the events case must export %s so the scenario receives it through the "+
				"environment rather than through argv. events.js reads this exact name — the "+
				"BLNK_* spelling ./.env ships is NOT one it looks for — so dropping the export "+
				"leaves the scrape unauthenticated and every verdict withheld",
			loadTestRunnerPath, name)
	}

	// A catch-all for a form the loop above cannot enumerate: any single line that combines an
	// -e flag with a credential name, however it is quoted or interpolated.
	for _, line := range strings.Split(eventsCase, "\n") {
		if !strings.Contains(line, "-e ") {
			continue
		}

		for _, name := range eventCredentialEnvNames {
			assert.NotContainsf(t, line, name,
				"%s: this line puts %s on an -e flag, which places its value in k6's argv: %s",
				loadTestRunnerPath, name, strings.TrimSpace(line))
		}
	}

	// The environment channel is only reliable if it is pinned. --include-system-env-vars
	// defaults to true, but K6_INCLUDE_SYSTEM_ENV_VARS=false in the caller's shell
	// overrides that default and silently empties every exported value — verified against
	// the pinned k6. The resulting failure is the quiet kind: the run completes and
	// reports withheld verdicts.
	assert.Containsf(t, eventsCase, "--include-system-env-vars",
		"%s: the events case must pass --include-system-env-vars explicitly. It defaults to "+
			"true, but an ambient K6_INCLUDE_SYSTEM_ENV_VARS=false turns the credential channel "+
			"off without any error — the scrape is refused, the stats call 401s, and the run "+
			"reports withheld verdicts as though the deployment were at fault",
		loadTestRunnerPath)
}

// TestLoadTestRunner_MakesTheRawStreamOptIn keeps the raw stream opt-in.
//
// The raw k6 NDJSON stream was written on every run.
//
// The summary therefore stays unconditional and the raw stream becomes opt-in.
func TestLoadTestRunner_MakesTheRawStreamOptIn(t *testing.T) {
	eventsCase := eventsRunnerBranchStripped(t)

	// The k6 invocation itself, from the command to the scenario it runs. --out must not
	// appear here as a literal: an unconditional flag is the defect regardless of how the
	// path is built.
	invocationStart := strings.Index(eventsCase, "k6 run")
	require.Greaterf(t, invocationStart, 0, "%s: the events case must invoke k6", loadTestRunnerPath)
	invocationEnd := strings.Index(eventsCase[invocationStart:], "tests/loadtest/events.js")
	require.Greaterf(t, invocationEnd, 0,
		"%s: the k6 invocation must name the event scenario", loadTestRunnerPath)
	invocation := eventsCase[invocationStart : invocationStart+invocationEnd]

	assert.NotContainsf(t, invocation, "--out",
		"%s: the k6 invocation must not carry a literal --out. A 30-minute run at 500/s writes "+
			"tens of millions of raw records, and that generator I/O competes with the latency "+
			"the run exists to measure — so the artifact degrades the verdict it evidences. "+
			"Build the flag conditionally and expand it",
		loadTestRunnerPath)

	assert.Containsf(t, invocation, `"${k6_out_args[@]}"`,
		"%s: the k6 invocation must expand the conditionally-built output arguments, so a run "+
			"with raw output disabled passes no --out at all", loadTestRunnerPath)

	assert.Containsf(t, invocation, `-e "SUMMARY_OUT=`,
		"%s: the summary must remain UNCONDITIONAL — it is the acceptance artifact, and every "+
			"V-1, V-3 and p99 verdict is read from it. Making it opt-in too would leave a run "+
			"that evidences nothing", loadTestRunnerPath)

	// The opt-in itself, and the two ways of asking for it.
	assert.Containsf(t, eventsCase, "RAW_OUTPUT",
		"%s: the events case must consult RAW_OUTPUT so the raw stream can be requested",
		loadTestRunnerPath)
	assert.Containsf(t, eventsCase, `if [[ -n "${NDJSON_OUT:-}" ]]; then`,
		"%s: naming NDJSON_OUT must imply RAW_OUTPUT, so there is no way to ask for the file "+
			"and not get it", loadTestRunnerPath)
}

// TestLoadTestRunner_NamesArtefactsAsTheScenarioDoes pins CONTRACT-m01.
//
// events.js writes its summary to __ENV.SUMMARY_OUT or, absent that, to its OWN default
// — which is what a bare `k6 run tests/loadtest/events.js` produces with no runner
// involved.
func TestLoadTestRunner_NamesArtefactsAsTheScenarioDoes(t *testing.T) {
	runner := readRepoFile(t, loadTestRunnerPath)

	defaultSummary := regexp.
		MustCompile(`const SUMMARY_OUT = __ENV\.SUMMARY_OUT \|\| "([^"]+)";`).
		FindStringSubmatch(loadTestScript(t))
	require.Lenf(t, defaultSummary, 2,
		"%s must declare a single default SUMMARY_OUT; it is the artefact name a run with no "+
			"runner produces, and therefore the one every other consumer has to agree with",
		loadTestScriptPath)

	scenarioDefault := defaultSummary[1]
	directory, file := path.Split(scenarioDefault)
	directory = strings.TrimSuffix(directory, "/")
	stem := strings.TrimSuffix(strings.TrimPrefix(file, "summary-"), ".json")
	require.NotEmptyf(t, stem, "%s: the default summary must be named summary-<stem>.json, got %q",
		loadTestScriptPath, file)

	assert.Containsf(t, runner, `EVENTS_OUT_DIR="`+directory+`"`,
		"%s: the runner must write into the same directory the scenario defaults to (%s)",
		loadTestRunnerPath, directory)
	assert.Containsf(t, runner, "/summary-"+stem+".json",
		"%s: the runner's default summary must be summary-%s.json, matching what %s writes "+
			"when it is run directly. Two stems for one run means the README can document only "+
			"one of them, and a reader opening the other finds nothing",
		loadTestRunnerPath, stem, loadTestScriptPath)
	assert.Containsf(t, runner, "/run-"+stem+".ndjson",
		"%s: the raw stream must share the summary's stem, so one run's artefacts sort together",
		loadTestRunnerPath)

	// Both names accepted, and the alias reported rather than silently applied.
	assert.Containsf(t, runner, `if [[ "${CASE_NAME}" == "event-streaming" ]]; then`,
		"%s: `event-streaming` must remain accepted — it is a documented invocation, and "+
			"removing it returns the reader to an `unknown case` message", loadTestRunnerPath)
	assert.Containsf(t, runner, `REQUESTED_CASE_NAME="${CASE_NAME}"`,
		"%s: the name the operator typed must be preserved before normalisation, so the banner "+
			"can report the substitution instead of hiding it", loadTestRunnerPath)
	assert.Containsf(t, runner, `"${REQUESTED_CASE_NAME}" != "${CASE_NAME}"`,
		"%s: the runner must report when the case it ran differs from the one requested",
		loadTestRunnerPath)
}

// TestLoadTestRunner_IsExecutable pins the file mode in the index.
func TestLoadTestRunner_IsExecutable(t *testing.T) {
	info, err := os.Stat(loadTestRunnerPath)
	require.NoErrorf(t, err, "%s must exist", loadTestRunnerPath)

	assert.NotZerof(t, info.Mode().Perm()&0o111, "%s must be committed executable (100755), as "+
		"scripts/kafka-bootstrap.sh and scripts/kafka-provision.sh are. Set it with "+
		"`git update-index --chmod=+x %s`", loadTestRunnerPath, loadTestRunnerPath)
}

// TestLoadTestRunner_ValidatesAndSanitisesEveryURLItReports pins two separate mistakes.
//
// FIRST, it reported before it validated.
//
// SECOND, it reported the value verbatim. A URL is somewhere a credential hides in
// plain sight — http://user:token@host/, or ?api_key=...
//
// So: require_http_url refuses a non-http(s) value without echoing it, and display_url
// renders scheme, host, port and path only. The value k6 receives is unchanged; only
// what reaches the terminal is narrowed.
func TestLoadTestRunner_ValidatesAndSanitisesEveryURLItReports(t *testing.T) {
	runner := readRepoFile(t, loadTestRunnerPath)

	require.Containsf(t, runner, "require_http_url() {",
		"%s must define a URL validator", loadTestRunnerPath)
	require.Containsf(t, runner, "display_url() {",
		"%s must define a sanitising renderer for anything it prints", loadTestRunnerPath)

	// The validator refuses without echoing. Asserted on the absence of the value from its own
	// message, because a refusal that prints the bad value defeats the point of refusing to.
	validatorStart := strings.Index(runner, "require_http_url() {")
	validatorEnd := strings.Index(runner[validatorStart:], "\n}\n")
	require.Greater(t, validatorEnd, 0, "require_http_url must be a closed function")
	validator := runner[validatorStart : validatorStart+validatorEnd]

	// Scoped to the lines that PRINT. The function must of course reference ${value} to
	// test it; what it must not do is put it on the terminal.
	for _, line := range strings.Split(validator, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "echo ") && !strings.HasPrefix(trimmed, "printf ") {
			continue
		}
		assert.NotContainsf(t, trimmed, "${value}",
			"%s: require_http_url prints the value it rejected (%q). A malformed URL is exactly "+
				"the kind that carries a pasted credential, and this message is what reaches the "+
				"CI log", loadTestRunnerPath, trimmed)
	}

	// VALIDATION PRECEDES REPORTING, asserted by position rather than by reading the order out.
	urlCheck := strings.Index(runner, `require_http_url "URL" "${URL}"`)
	require.Greaterf(t, urlCheck, 0,
		"%s: URL must be validated explicitly", loadTestRunnerPath)
	metricsCheck := strings.Index(runner, `require_http_url "METRICS_URL" "${METRICS_URL}"`)
	require.Greaterf(t, metricsCheck, 0,
		"%s: METRICS_URL must be validated too — it is derived from URL, so a bad URL yields a "+
			"bad METRICS_URL and every scrape fails with withheld verdicts", loadTestRunnerPath)

	report := strings.Index(runner, `echo "  transactions :`)
	require.Greaterf(t, report, 0, "%s: the events case must report its endpoints", loadTestRunnerPath)
	assert.Lessf(t, urlCheck, report,
		"%s: URL must be validated BEFORE it is reported, or a malformed value is presented as "+
			"the endpoint under test and the real complaint arrives later from k6", loadTestRunnerPath)
	assert.Lessf(t, metricsCheck, report,
		"%s: METRICS_URL must be validated before it is reported, for the same reason",
		loadTestRunnerPath)

	// NOTHING PRINTS A RAW URL. Every echo of either variable must go through display_url.
	for _, raw := range []string{
		`echo "  transactions : ${URL}"`,
		`echo "  metrics      : ${METRICS_URL}"`,
		`from URL=${URL}"`,
	} {
		assert.NotContainsf(t, runner, raw,
			"%s: %s prints a URL verbatim, including any userinfo or query credential in it. "+
				"Route it through display_url", loadTestRunnerPath, raw)
	}
	assert.Containsf(t, runner, `$(display_url "${URL}")`,
		"%s: URL must be reported through display_url", loadTestRunnerPath)
	assert.Containsf(t, runner, `$(display_url "${METRICS_URL}")`,
		"%s: METRICS_URL must be reported through display_url", loadTestRunnerPath)

	// The renderer must strip all three hiding places. Order matters and is asserted by
	// presence of each step, because dropping any one leaves a channel open.
	renderStart := strings.Index(runner, "display_url() {")
	renderEnd := strings.Index(runner[renderStart:], "\n}\n")
	require.Greater(t, renderEnd, 0, "display_url must be a closed function")
	renderer := runner[renderStart : renderStart+renderEnd]
	assert.Containsf(t, renderer, `value="${value%%#*}"`,
		"%s: display_url must strip the fragment", loadTestRunnerPath)
	assert.Containsf(t, renderer, `value="${value%%\?*}"`,
		"%s: display_url must strip the query, where an api_key= commonly sits", loadTestRunnerPath)
	assert.Containsf(t, renderer, `${authority##*@}`,
		"%s: display_url must strip userinfo, and must cut at the LAST '@' so a password "+
			"containing '@' does not leave its tail behind as part of the host", loadTestRunnerPath)
}

// TestLoadTestRunner_RefusesAnUnacknowledgedFixtureSpreadBeforeStartingK6 is the guard.
//
// # The README documented the certifying run as
//
// set -a;. ./.env; set +a bash tests/loadtest/run_case.sh events
//
// and that command could not complete. events.js refuses to provision fixtures without
// one of LEDGER_PAIRS, ALLOW_FIXTURE_CREATION=1 or SMOKE=1, because provisioning
// permanently adds LEDGER_SPREAD (128) ledgers and twice that many balances to a
// database it cannot clean up — Blnk exposes no DELETE for a ledger or a balance. The
// refusal is correct.
func TestLoadTestRunner_RefusesAnUnacknowledgedFixtureSpreadBeforeStartingK6(t *testing.T) {
	runner := readRepoFile(t, loadTestRunnerPath)

	// The gate must sit BEFORE the k6 invocation, or it has not moved the failure at all.
	gate := strings.Index(runner, "This case would provision")
	require.Greaterf(t, gate, 0,
		"%s must refuse an unacknowledged fixture spread itself, rather than letting the "+
			"scenario's setup() do it after a full verdict table has printed", loadTestRunnerPath)

	// Anchored on the INVOCATION rather than on `--include-system-env-vars`, because the
	// flag is named in four rationale comments before it is ever passed, and the first of
	// those sits above the gate — so matching the flag would compare the gate against a
	// comment and pass or fail for the wrong reason. `k6 run` inside the events branch is
	// the real thing.
	invocation := strings.Index(runner, "\n    k6 run \\")
	require.Greaterf(t, invocation, 0,
		"%s: the events branch must invoke `k6 run` inside its subshell", loadTestRunnerPath)
	assert.Lessf(t, gate, invocation,
		"%s: the fixture-mode refusal must come BEFORE k6 is started. After it, the operator "+
			"has already paid for an initialised scenario and a screen of FAIL verdicts",
		loadTestRunnerPath)

	// Every escape the scenario honours must be honoured here too. A runner stricter than
	// the scenario would block a legitimate run — LEDGER_SPREAD=0 is a deliberate
	// single-aggregate measurement, not an oversight — and one looser than the scenario
	// would let the buried failure back in.
	for _, escape := range []string{
		"LEDGER_PAIRS",
		"ALLOW_FIXTURE_CREATION",
		"SMOKE",
		"LEDGER_SPREAD",
	} {
		assert.Containsf(t, runner, escape,
			"%s: the fixture gate must recognise %s, which the scenario accepts as a valid "+
				"fixture mode", loadTestRunnerPath, escape)
	}

	// And the refusal has to be actionable. A gate that reports a problem without naming the
	// remedy relocates the guesswork rather than removing it.
	refusal := runner[gate:]
	if end := strings.Index(refusal, "\n  fi\n"); end > 0 {
		refusal = refusal[:end]
	}
	for _, remedy := range []string{
		"ALLOW_FIXTURE_CREATION=1",
		"LEDGER_PAIRS=",
		"SMOKE=1",
		"LEDGER_SPREAD=0",
	} {
		assert.Containsf(t, refusal, remedy,
			"%s: the refusal must name %s as one of the ways out", loadTestRunnerPath, remedy)
	}

	// The fixture trio must also be FORWARDED. It arrives through
	// --include-system-env-vars today, but the runner's forwarded set is its published
	// contract, and a variable that works only by ambient inheritance is one no reader of
	// this file knows is supported.
	forwarding := scriptRegion(t, runner, "for setting in RATE DURATION", "done")
	for _, forwarded := range []string{"LEDGER_PAIRS", "ALLOW_FIXTURE_CREATION", "SMOKE"} {
		assert.Containsf(t, forwarding, forwarded,
			"%s: %s must be forwarded explicitly, not left to k6's "+
				"--include-system-env-vars default", loadTestRunnerPath, forwarded)
	}
}

// TestLoadTestOfferedRate_IsReportedAsOfferedRatherThanAsTheTarget is the guard.
//
// events.js offers 550 events/sec, not 500: RATE defaults to ceil(TARGET_EVENTS_PER_SEC
// * LOAD_HEADROOM_RATIO) = ceil(500 * 1.1).
//
// What the runner and the README SAY about that rate is the part that has to match it.
func TestLoadTestOfferedRate_IsReportedAsOfferedRatherThanAsTheTarget(t *testing.T) {
	// The derivation itself must remain target x headroom, so the two numbers stay linked
	// rather than both being hardcoded and drifting apart.
	script := loadTestScript(t)
	assert.Contains(t, script, "TARGET_EVENTS_PER_SEC * LOAD_HEADROOM_RATIO",
		"events.js must DERIVE the offered rate from the target and the headroom ratio; two "+
			"independently hardcoded numbers drift apart and then disagree about what the run does")

	// The runner's announcement must name both, so an operator reading the terminal cannot
	// come away with only one of them.
	runner := readRepoFile(t, loadTestRunnerPath)
	announcement := ""
	for _, line := range strings.Split(runner, "\n") {
		if strings.Contains(line, "events.js defaults") && strings.Contains(line, "echo") {
			announcement = line
			break
		}
	}
	require.NotEmptyf(t, announcement,
		"%s must announce which load shape it used", loadTestRunnerPath)
	assert.Containsf(t, announcement, "550",
		"%s: the default-shape announcement must state the OFFERED rate (550/s). Naming only "+
			"the target invites quoting 500 as the load that was driven", loadTestRunnerPath)
	assert.Containsf(t, announcement, "500",
		"%s: it must also state the rate the verdict is JUDGED against (500/s), or the "+
			"headroom reads as a relaxed bar", loadTestRunnerPath)

	// And the README must not describe the default shape as plain "500/s for 30 minutes".
	readme := readRepoFile(t, "tests/loadtest/README.md")
	for _, stale := range []string{
		"500 events/sec for 30 minutes",
		"500/s for 30 minutes",
	} {
		assert.NotContainsf(t, readme, stale,
			"tests/loadtest/README.md: %q describes the default shape using the TARGET rate. "+
				"The run offers 550/s and is judged at 500/s, and both belong in any sentence "+
				"that describes the shape", stale)
	}

	// The absence checks above are necessary but evadable: any rephrasing that drops the
	// offered rate without reproducing one of those two exact strings would slip through.
	// So the offered rate is also asserted POSITIVELY.
	assert.Containsf(t, readme, "550",
		"tests/loadtest/README.md must state the OFFERED rate (550/s) somewhere. Describing "+
			"only the 500/s target leaves a reader quoting a load the run never drove")
}

// TestLoadTestReadmeCertifyingRun_IsRunnableAsDocumented is the second half of the
// guard.
//
// The runner refusing early is only half a fix: if the README still prints a command
// that the runner now refuses, the documentation is still wrong and the refusal simply
// arrives sooner. The certifying invocation must carry a fixture decision that permits
// quoting the result.
func TestLoadTestReadmeCertifyingRun_IsRunnableAsDocumented(t *testing.T) {
	readme := readRepoFile(t, "tests/loadtest/README.md")

	// Scoped to the acceptance-run section rather than searched file-wide. An invocation
	// of the runner appears many times in this README — general usage examples, the
	// certifying run, the fixture-reuse form and two shortened runs — and only one of them
	// is the command that claims to certify the latency and dead-letter verdicts.
	section := strings.Index(readme, "## The event-streaming acceptance run")
	require.Greaterf(t, section, 0,
		"tests/loadtest/README.md must carry an acceptance-run section")
	body := readme[section:]
	runItWith := strings.Index(body, "Run it with:")
	require.Greaterf(t, runItWith, 0,
		"the acceptance-run section must introduce its command with `Run it with:`")

	certifying := ""
	for _, line := range strings.Split(body[runItWith:], "\n") {
		if strings.Contains(line, "run_case.sh event-streaming") ||
			strings.Contains(line, "run_case.sh events") {
			certifying = strings.TrimSpace(line)
			break
		}
	}
	require.NotEmptyf(t, certifying,
		"the acceptance-run section must document a `run_case.sh event-streaming` invocation")

	// SMOKE=1 would satisfy the runner but explicitly forfeits the right to quote the numbers,
	// so it cannot be what the certifying command uses.
	assert.NotContainsf(t, certifying, "SMOKE=1",
		"the certifying command must not use SMOKE=1: it permits the single-aggregate fallback "+
			"and its numbers must not be quoted against V-1 or V-3. Got %q", certifying)

	assert.Truef(t,
		strings.Contains(certifying, "ALLOW_FIXTURE_CREATION=1") ||
			strings.Contains(certifying, "LEDGER_PAIRS="),
		"the documented certifying run must carry a fixture decision the runner accepts and "+
			"that permits quoting the result — ALLOW_FIXTURE_CREATION=1 or LEDGER_PAIRS. "+
			"Without one it aborts, which is exactly the defect this guards. Got %q", certifying)
}

// queueBenchmarkPath is the drain-measuring tool the runner launches alongside k6. Its verdict
// is what the queue cases are for, so its EXIT STATUS is part of the harness contract rather
// than an implementation detail of a helper binary.
const queueBenchmarkPath = "tests/loadtest/tools/queue_benchmark.go"

// TestQueueBenchmark_FailsTheRunWhenTheQueuesNeverDrain pins the exit-status contract of the
// drain benchmark and the runner's propagation of it.
//
// WHAT WENT WRONG. The tool measured the drain correctly and reported it correctly — the
// summary carried `drained: false` with a failing pass/fail pair — and then returned 0. Every
// caller that decides pass or fail from a process status rather than by parsing JSON therefore
// recorded a pass, including tests/loadtest/run_case.sh, which runs under `set -euo pipefail`
// and waits on the process. A queue case whose backlog was still growing when the clock ran
// out reported a successful run.
//
// The three properties below are what make the verdict trustworthy, and each fails
// differently: without the flag the tool cannot tell a timeout from a drain, without the exit
// the status contradicts the summary, and without the ordering the failure destroys the
// evidence that explains it.
func TestQueueBenchmark_FailsTheRunWhenTheQueuesNeverDrain(t *testing.T) {
	source := readRepoFile(t, queueBenchmarkPath)

	t.Run("the timeout is recorded where it happens, not inferred later", func(t *testing.T) {
		// "Ended with work still queued" has three causes — the timeout, an interrupt, and a
		// drain a later arrival added to — and only the first is a failure of the measurement.
		// Inferring it after the loop cannot separate them, so it is recorded at the break.
		assert.Containsf(t, collapseWhitespace(source),
			"if time.Now().After(startDeadline) { timedOut = true",
			"%s: the deadline break must set timedOut. Deciding afterwards from "+
				"`!isDrained(end)` alone cannot distinguish a timeout from an interrupt, and "+
				"failing an interrupted run turns run_case.sh's own teardown into a failure",
			queueBenchmarkPath)
	})

	t.Run("a timed-out run that never drained exits non-zero", func(t *testing.T) {
		assert.Containsf(t, collapseWhitespace(source),
			"if timedOut && !isDrained(endSnapshot) {",
			"%s: the tool must exit non-zero when it timed out with work still queued. Both "+
				"conditions are required: timedOut alone would fail a run that drained on the "+
				"final poll, and !isDrained alone would fail an interrupted run",
			queueBenchmarkPath)

		assert.Containsf(t, source, "os.Exit(1)",
			"%s: the non-drained timeout must exit non-zero, or the status keeps contradicting "+
				"the summary", queueBenchmarkPath)
	})

	t.Run("the summary is written before the failing exit", func(t *testing.T) {
		// The artifact is how the failure gets diagnosed — how far the backlog got, whether it
		// was moving at all. Exiting first would report the failure and destroy its evidence.
		finalWrite := strings.Index(source, "failed to write final summary")
		failingExit := strings.LastIndex(source, "queue benchmark FAILED")
		require.Positivef(t, finalWrite, "%s must write a final summary", queueBenchmarkPath)
		require.Positivef(t, failingExit, "%s must report a failed drain", queueBenchmarkPath)

		assert.Lessf(t, finalWrite, failingExit,
			"%s: the final summary must be written BEFORE the failing exit. A tool that exits "+
				"first reports the failure and deletes the evidence for it in one step",
			queueBenchmarkPath)
	})

	t.Run("an interrupt is not a failure", func(t *testing.T) {
		// run_case.sh's EXIT trap sends SIGINT when it tears the benchmark down early. A partial
		// result that was explicitly asked for is not a failed measurement.
		assert.NotContainsf(t, collapseWhitespace(source),
			"if interrupted { os.Exit(1)",
			"%s: an interrupted run must still exit 0. run_case.sh's cleanup trap sends SIGINT, "+
				"so failing on interrupt makes every early teardown look like a failed drain",
			queueBenchmarkPath)
	})

	t.Run("the runner propagates the status after reporting the artefacts", func(t *testing.T) {
		runner := readRepoFile(t, loadTestRunnerPath)
		collapsed := collapseWhitespace(runner)

		assert.Containsf(t, collapsed,
			`wait "${QUEUE_BENCH_PID}" || QUEUE_BENCH_STATUS=$?`,
			"%s: the wait must capture the benchmark's status. A bare `wait` under `set -e` "+
				"aborts here, before the Generated files list — which is the only place this "+
				"script says where the summary it just wrote actually is", loadTestRunnerPath)

		assert.Containsf(t, collapsed,
			`exit "${QUEUE_BENCH_STATUS}"`,
			"%s: the captured status must be re-raised as this script's exit code. Capturing it "+
				"and not re-raising it is the original defect moved one level up",
			loadTestRunnerPath)

		// Order, not just presence: the paths have to be printed before the exit.
		filesList := strings.Index(runner, "Generated files:")
		reRaise := strings.Index(runner, `exit "${QUEUE_BENCH_STATUS}"`)
		require.Positivef(t, filesList, "%s must list the generated files", loadTestRunnerPath)
		require.Positivef(t, reRaise, "%s must re-raise the benchmark status", loadTestRunnerPath)

		assert.Lessf(t, filesList, reRaise,
			"%s: the generated-file list must come BEFORE the failing exit, or a failed drain "+
				"reports no way to find the summary that explains it", loadTestRunnerPath)
	})
}
