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

// PROVENANCE CONTRACT ASSERTIONS for the V-1 verdict in tests/loadtest/events.js.
//
// # Why a Go test asserts on JavaScript
//
// events.js is the artefact that decides whether acceptance criterion V-1 — 500 events per
// second sustained, with a p99 capture-to-dispatch latency under two seconds — is met. Nothing
// else in the repository makes that judgement, and nothing checks the judgement itself: no CI
// job runs k6, so the script's decision logic has no compiler, no type system and no test
// harness of its own. A defect in it is not a failing build; it is a PASS that was never
// earned, which is strictly worse than a failure because it is acted upon.
//
// The specific defect these tests exist to prevent had exactly that shape. The p99 selection
// accepted blnk_events_publish_duration_seconds as a FALLBACK when the capture-to-dispatch
// histogram had no observations:
//
//	if (capture.quantile.value !== null) { ... } else if (brokerWrite.quantile.value !== null) {
//	  latencySource = P99_SOURCE_PUBLISH_DURATION;
//	  chosen = brokerWrite;                       // <- a different interval entirely
//	}
//
// The two instruments do not time the same thing. V-1 is stated over the interval a subscriber
// waits — from the outbox row being committed to the broker acknowledging the write — while
// publish_duration's clock starts at the relay's CLAIM, and therefore excludes the row waiting
// for the next poll tick, the poll interval and the claim query. That excluded segment is
// precisely the component that grows when the relay falls behind, so the fallback reported its
// smallest figures in the exact circumstance the target exists to catch, and the threshold row
// read PASS. Recording the substitution in a provenance field did not help: a CI gate and an
// operator both read the verdict, not the provenance.
//
// # What is asserted, and why it is asserted structurally
//
// The behaviour was verified by extracting the script's own decision text and running it over
// synthetic histogram readings, which is how the defect was confirmed present before the fix
// and absent after it. That exercise is not repeatable in CI, so what remains here are the
// STRUCTURAL properties the behaviour rests on, each phrased so that restoring the fallback in
// any of its available forms fails a Go test:
//
//	the selection may read only the capture histogram,
//	the availability flag must key on that histogram rather than on the chosen source,
//	no threshold may be attached to either diagnostic figure, and
//	the thresholded metric must be documented against the capture series.
//
// The regions are located by their own anchor statements rather than by line number, and
// comments are removed before absence is asserted, so that prose which merely NAMES the
// removed fallback — as the rationale comments deliberately do — cannot satisfy or defeat an
// assertion about code.
package blnk

import (
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

// scriptRegion returns the text of events.js between two anchor statements, exclusive of the
// closing anchor.
//
// Anchors are used in preference to line numbers because the surrounding rationale comments are
// long and expected to be edited; an anchored region survives that, while a line range would
// drift silently and start asserting about the wrong code. Both anchors are required to appear
// exactly once, so a region that has been duplicated or renamed fails loudly here rather than
// quietly narrowing what the test looks at.
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
// It exists because some function bodies end on a statement that is not unique in the file —
// `return o;` closes two of them — so an anchored region cannot address them unambiguously. The
// body is taken from the declaration to the first line consisting of a single closing brace,
// which is where Prettier puts the end of every top-level function in this script.
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
//
// It is deliberately line-wise rather than a general lexer: it drops only lines whose trimmed
// form OPENS a comment, so it can never truncate a string literal that happens to contain "//",
// of which this script has several (metric help text, PromQL, URLs). events.js is formatted by
// Prettier, which puts comments in the regions asserted on below on their own lines, so nothing
// is missed in practice. Absence assertions run over the stripped text because the rationale
// comments name the removed fallback on purpose — a test that grepped the raw region would
// report the explanation as the defect.
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

// ---------------------------------------------------------------------------
// V-1 may be certified from one series and one series only
// ---------------------------------------------------------------------------

// TestLoadTestP99_IsCertifiedFromCaptureToDispatchAlone pins the selection.
//
// The assertion is about what the block may READ, not about which constant it happens to
// assign. That framing matters: a re-introduced fallback could name a new source code, or
// reuse P99_SOURCE_CAPTURE_TO_DISPATCH and quietly point `chosen` at the wrong histogram, and
// an assertion phrased against the constant would miss both. Requiring that the block mention
// no series other than `capture` closes every variant at once, because a figure that is never
// read cannot be certified.
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
	// `chosen` certifies V-1 against a measurement V-1 is not stated over.
	assert.NotContainsf(t, selection, "brokerWrite",
		"%s: the p99 selection must not read publish_duration. Its clock starts at the relay "+
			"claim, so it excludes the queue wait and reports its smallest figures for the "+
			"backlog V-1 exists to catch. When capture-to-dispatch has no samples the run is "+
			"NOT CERTIFIABLE — it is not certified from a shorter interval", loadTestScriptPath)
	assert.NotContainsf(t, selection, "P99_SOURCE_PUBLISH_DURATION",
		"%s: no branch of the p99 selection may resolve to the publish-duration source",
		loadTestScriptPath)

	// Exactly one source is assignable, so the "else if" shape cannot have returned in another
	// guise. Enumerated rather than counted, because the initialiser `chosen = null` is itself an
	// assignment and a bare count would be satisfied by any second branch that replaced it.
	// The selection is a conditional expression rather than a chain of assignments, so the
	// provenance is enumerated from the operands of that expression: whatever `chosen` can hold
	// appears here, and a second source could only be added by extending it.
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
// event_publish_verdicts_available is the gate that makes the other three verdicts meaningful:
// a run that measured nothing reports zeroes, two of which satisfy their `<` thresholds. Before
// the fix this reason keyed on `latencySource === P99_SOURCE_NONE`, which the fallback had
// already moved off zero — so the run was declared certifiable on the strength of a series V-1
// is not stated over, and the flag that exists to fail closed failed open instead.
//
// Keying on the capture histogram directly makes the flag independent of how many sources the
// selection grows later: without first-attempt capture observations V-1 has no population,
// whatever else was scraped.
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
//
// Removing the fallback keeps publish_duration in the report on purpose: differenced against
// capture-to-dispatch it yields the queue wait, which is how an operator tells a slow broker
// from a relay backlog. That is only safe while neither figure can decide anything. A threshold
// on either one would re-create the original defect through the k6 threshold table instead of
// through the selection — same false PASS, different mechanism — so the absence is asserted
// here rather than left to reviewer attention.
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

	for _, diagnostic := range []string{"M_BROKER_WRITE_P99", "M_QUEUE_WAIT_P99"} {
		assert.NotContainsf(t, thresholds, diagnostic,
			"%s: %s is a diagnostic and must not be thresholded — publish_duration and the "+
				"queue-wait split explain a verdict, they do not decide one", loadTestScriptPath,
			diagnostic)
	}

	// The p99 threshold must be the V-1 bound rather than an unrelated literal.
	assert.Containsf(t, thresholds, `o[M_P99_SECONDS] = ["value<" + MAX_P99_PUBLISH_SECONDS];`,
		"%s: the p99 verdict must be gated on MAX_P99_PUBLISH_SECONDS", loadTestScriptPath)
}

// TestLoadTestP99_IsDocumentedAgainstTheCaptureSeries pins the reported provenance.
//
// The summary publishes an `equivalent_promql` beside each verdict so the figure can be
// reproduced in Prometheus. If that query names publish_duration while the verdict is computed
// from capture-to-dispatch — or the reverse — then anyone who checks the number gets a
// different one and has no way to tell which of the two is the criterion. The two are asserted
// together for that reason: the thresholded metric's query must name the capture series, and
// the diagnostic's must name publish_duration.
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

	// The queue wait is the DIFFERENCE of the two, which is the only reason keeping the shorter
	// interval in the report is worth anything.
	assert.Containsf(t, source, "const PROMQL_P99_DIFFERENCE = PROMQL_P99 + \" - \" + PROMQL_BROKER_WRITE;",
		"%s: the queue wait must be derived as capture-to-dispatch minus the broker write, and it "+
			"is named as the DIFFERENCE it is: the difference of two p99 figures is not the p99 of "+
			"the difference, and a constant called QUEUE_WAIT would invite exactly that reading",
		loadTestScriptPath)
}

// TestLoadTestP99SourceCodes_StayStableForOldArtefacts pins the numbering.
//
// P99_SOURCE_PUBLISH_DURATION is kept at 2 although it can no longer be emitted. Deleting it
// would renumber any code added afterwards, and summaries are compared across runs — including
// runs produced before the fallback was removed, which really did emit a 2. A reader diffing
// two artefacts must not have one integer mean two different things.
//
// What the test enforces is the pair: the code exists with its original value, AND it is only
// ever DECODED, never produced. `p99SourceCode.add(...)` is the single emission point, and the
// preceding test already fixes what may reach it.
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

// TestLoadTestRunner_LeavesTheEventLoadShapeToTheScenario pins the second place the V-1
// verdict could be quietly detached from what V-1 says.
//
// run_case.sh exists for the four transaction topology cases and defaults them to RATE=300 for
// DURATION=30s, which are sensible figures for comparing queue shapes. V-1 is stated at 500
// events per second sustained for thirty minutes, and V-3's dead-letter rate is stated over
// that same window. An events case that reached the scenario through those defaults would emit
// a summary reporting PASS on all three verdicts after thirty seconds at 300/s: not a wrong
// number, but a right number for a load nobody claimed anything about — the same substitution
// the p99 fallback made, one layer out.
//
// The runner therefore forwards RATE, DURATION, VUS and MAX_VUS only when the CALLER set them,
// leaving events.js to apply its own defaults, which are the criterion's figures. That is
// asserted here in two halves, because either alone can be defeated: the events case must be
// dispatched before the transaction defaults are assigned, and the forwarding must be
// conditional.
func TestLoadTestRunner_LeavesTheEventLoadShapeToTheScenario(t *testing.T) {
	runner := readRepoFile(t, loadTestRunnerPath)

	eventsDispatch := strings.Index(runner, `if [[ "${CASE_NAME}" == "events" ]]; then`)
	require.Greaterf(t, eventsDispatch, 0,
		"%s must dispatch an `events` case, or the event-streaming acceptance run has no "+
			"documented invocation and its verdicts are never produced", loadTestRunnerPath)

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

	// The events branch ALONE, bounded at its own terminator. Slicing to the end of the file
	// would drag the transaction path in and make every absence assertion below vacuous — the
	// transaction invocation legitimately passes an unconditional -e RATE= and drives script.js.
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
