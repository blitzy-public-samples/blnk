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

// Tests for the LOCAL STACK's event-streaming configuration: the two compose projections,
// the bring-up script and the provisioning script.
//
// # Why a Go test asserts on YAML and shell
//
// These files are the only part of the event pipeline with no compiler and no type system
// behind them. A stray `depends_on` entry, a credential default that creeps back in, or a
// published port that loses its host binding are all silent: the stack still comes up, and
// the defect shows itself as a sixty-second delay on a laptop, or not at all until the
// broker is on a shared network. Every assertion below encodes a specific defect that was
// present and is now fixed, so that the fix cannot be undone by an edit that looks
// harmless.
//
// The compose files are PARSED rather than grepped wherever the property is structural — a
// dependency is a map entry, not a line of text — and read as text only where the property
// genuinely is textual, such as an interpolation default. Both projections are checked in
// every case, because docker-compose.dev.yaml differs from docker-compose.yaml only in
// building from source and is otherwise expected to be identical: a fix applied to one and
// not the other leaves half the developers on the defect.
package blnk

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// composeProjections are the two files that must stay in step.
var composeProjections = []string{"docker-compose.yaml", "docker-compose.dev.yaml"}

// composeApplicationServices are the services that run Blnk itself, as opposed to the
// infrastructure it talks to.
var composeApplicationServices = []string{"server", "worker"}

// composeServices returns one compose projection's service map.
func stackComposeServices(t *testing.T, file string) map[string]interface{} {
	t.Helper()

	document := readYAMLFile(t, filepath.Join(moduleRootDir(t), file))

	services, ok := document["services"].(map[string]interface{})
	require.Truef(t, ok, "%s must declare a services map", file)

	return services
}

// composeService returns one service's definition from one projection.
func composeService(t *testing.T, file, name string) map[string]interface{} {
	t.Helper()

	service, ok := stackComposeServices(t, file)[name].(map[string]interface{})
	require.Truef(t, ok, "%s must declare the %q service", file, name)

	return service
}

// composeDependencyNames returns the service names one service depends on, accepting both
// `depends_on` forms: the short list and the long map that can carry a condition.
func composeDependencyNames(t *testing.T, file, name string) []string {
	t.Helper()

	raw, present := composeService(t, file, name)["depends_on"]
	if !present {
		return nil
	}

	switch dependencies := raw.(type) {
	case map[string]interface{}:
		names := make([]string, 0, len(dependencies))
		for dependency := range dependencies {
			names = append(names, dependency)
		}

		return names
	case []interface{}:
		names := make([]string, 0, len(dependencies))
		for _, dependency := range dependencies {
			names = append(names, strings.TrimSpace(toStringValue(dependency)))
		}

		return names
	default:
		t.Fatalf("%s: the %q service's depends_on is neither a list nor a map", file, name)

		return nil
	}
}

// toStringValue renders a YAML scalar as the string it spells.
func toStringValue(value interface{}) string {
	if text, ok := value.(string); ok {
		return text
	}

	return ""
}

// readRepoFile reads a repository file as text.
func readRepoFile(t *testing.T, relative string) string {
	t.Helper()

	contents, err := os.ReadFile(filepath.Join(moduleRootDir(t), relative))
	require.NoErrorf(t, err, "%s must be readable", relative)

	return string(contents)
}

// ---------------------------------------------------------------------------
// F16 — a stack with no broker must still start
// ---------------------------------------------------------------------------

// TestCompose_ApplicationServicesDoNotWaitOnTheBroker is the anti-regression guard for the
// no-broker startup path.
//
// Both application services used to carry "kafka: condition: service_healthy" and
// "kafka-init: condition: service_completed_successfully" UNCONDITIONALLY. KAFKA_BROKERS is
// EMPTY in the shipped configuration — a supported steady state in which the publisher
// resolves to its no-op and Blnk behaves exactly as it did before Kafka existed — so the
// shipped stack made the API wait out the broker's sixty-second start period for a JVM it had
// been told to ignore, and a broker that could not bootstrap at all left the API permanently
// unstarted rather than merely un-Kafka'd. An optional dependency was manufacturing an outage.
//
// # Why the dependency is made OPTIONAL rather than deleted
//
// Deleting the two entries fixes the outage and loses something real with it: when the
// operator HAS asked for Kafka, starting the relay against a broker that is merely created —
// not listening, not authenticating, with no topics — is the failure the conditions were
// added for. So the entries stay, and what changes is that they become non-required
// dependencies on services that sit behind a Compose profile:
//
//	docker compose up                  -> kafka is outside the enabled set, `required: false`
//	                                      makes that not an error, nothing is waited on
//	docker compose --profile kafka up  -> both conditions apply, unweakened
//
// Each half is inert without the other, which is why this test asserts the pair and
// TestCompose_KafkaIsOptInSoTheNoBrokerSteadyStateStarts asserts the conditions survive.
// What must never hold again is an UNCONDITIONAL wait, and that is the assertion here.
func TestCompose_ApplicationServicesDoNotWaitOnTheBroker(t *testing.T) {
	for _, file := range composeProjections {
		// The gate is only optional because the services it names are outside the default
		// enabled set. Assert that first: `required: false` on a service that starts anyway
		// buys nothing.
		for _, gated := range []string{"kafka", "kafka-init"} {
			profiles := composeService(t, file, gated)["profiles"]
			assert.Containsf(t, profiles, "kafka",
				"%s: the %q service must sit behind the \"kafka\" profile, or it starts on every "+
					"`docker compose up` and imposes a broker on deployments that publish nothing",
				file, gated)
		}

		for _, service := range composeApplicationServices {
			dependencies := composeDependencyNames(t, file, service)

			for _, gated := range []string{"kafka", "kafka-init"} {
				if !containsString(dependencies, gated) {
					// Absent entirely is also a stack that does not wait. Nothing further to
					// check for this one.
					continue
				}

				entry, isMap := composeService(t, file, service)["depends_on"].(map[string]interface{})
				require.Truef(t, isMap,
					"%s: the %q service names %q in depends_on, so depends_on must be the long map "+
						"form — only that form can carry the `required` flag that makes the wait optional",
					file, service, gated)

				gate, isMap := entry[gated].(map[string]interface{})
				require.Truef(t, isMap,
					"%s: the %q service's %q dependency must be the long map form", file, service, gated)

				assert.Equalf(t, false, gate["required"],
					"%s: the %q service's %q dependency must be `required: false`. Without it the "+
						"profile does not make Kafka optional: Compose refuses to start a service whose "+
						"dependency is outside the enabled set, so the API waits on — or is blocked for "+
						"ever by — infrastructure it was configured to ignore",
					file, service, gated)
			}

			// The three that remain are load-bearing and must not be lost while the Kafka
			// entries are being made optional: Blnk cannot serve a request without its
			// database or its queue.
			assert.Containsf(t, dependencies, "postgres", "%s: %q must still depend on postgres", file, service)
			assert.Containsf(t, dependencies, "redis", "%s: %q must still depend on redis", file, service)
		}
	}
}

// containsString reports whether a slice holds a value, so the loop above can distinguish
// "the dependency is absent" from "the dependency is present and must be optional".
func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}

	return false
}

// TestCompose_ProvisioningStillWaitsForTheBroker asserts the gate that IS correct was kept.
//
// kafka-init speaks an authenticated SASL protocol to a broker that must already have loaded
// its metadata. Removing the application gate is right; removing this one would make
// provisioning fail against a merely-started broker in a way that reads as a wrong password.
func TestCompose_ProvisioningStillWaitsForTheBroker(t *testing.T) {
	for _, file := range composeProjections {
		service := composeService(t, file, "kafka-init")

		dependencies, ok := service["depends_on"].(map[string]interface{})
		require.Truef(t, ok, "%s: kafka-init must use the long depends_on form so it can carry a condition", file)

		broker, ok := dependencies["kafka"].(map[string]interface{})
		require.Truef(t, ok, "%s: kafka-init must depend on the broker", file)

		assert.Equalf(t, "service_healthy", broker["condition"],
			"%s: kafka-init must wait for the broker to be HEALTHY, not merely started: a SASL "+
				"handshake against a broker still loading metadata fails in a way that looks like a "+
				"rejected credential", file)
	}
}

// ---------------------------------------------------------------------------
// F17 — provisioning happens before the relay is admitted
// ---------------------------------------------------------------------------

// TestStackScript_ProvisionsKafkaBeforeStartingTheApplication asserts the ordering moved into
// stack.sh, and that its outcome is no longer discarded.
//
// The bring-up used to run "compose up -d" for everything at once and verify the broker
// afterwards, with every call site written as "ensure_kafka || true". So the ledger was already
// accepting writes and the relay already polling while the topics were being created — and the
// script printed a success line over it. Nothing was lost, because capture is transactional, but
// a publish into a topic that does not exist burns an attempt against the row's retry budget, so
// a cold start could dead-letter a backlog over a condition that would have cleared by itself.
func TestStackScript_ProvisionsKafkaBeforeStartingTheApplication(t *testing.T) {
	script := readRepoFile(t, "stack.sh")

	require.Contains(t, script, "stage_kafka",
		"stack.sh must have a stage that brings the broker up and provisions it before the "+
			"application services")
	require.Contains(t, script, "staged_up",
		"and the bring-up paths must route through it rather than calling compose up directly")

	// The three paths that start the stack. Each must go through the staged bring-up, and none
	// may reach compose's own up directly — which is what would put the ledger and the topics
	// back in a race.
	for _, invocation := range []string{
		`--up | -u )
            staged_up "${@:2}"`,
		`--build | -b )
            staged_up --build "${@:2}"`,
	} {
		assert.Containsf(t, script, invocation,
			"the bring-up path must be staged:\n%s", invocation)
	}

	assert.False(t, strings.Contains(script, "ensure_kafka || true\n            ;;"),
		"no bring-up path may discard the Kafka outcome with `|| true`; that is what let a "+
			"success line be printed over a pipeline that could not deliver")

	// The gate must be escapable, because a developer with a broken broker still needs the rest
	// of the stack. An unconditional gate would be traded for the outage the F16 fix removed.
	assert.Contains(t, script, "KAFKA_REQUIRE_READY",
		"the gate must have a documented escape hatch, so a broken broker does not block the "+
			"database, the queue and the API as well")
}

// TestStackScript_PinsOneEffectiveBrokerValueOnEveryComposeInvocation is the M-3 guard: the
// script and the containers must not disagree about the one value that decides whether Blnk
// publishes.
//
// THE CASCADE. showenv() sources ${env} before main() runs. If the caller had already EXPORTED
// KAFKA_BROKERS, that source does not shadow it — assigning to an exported name keeps the export
// attribute and replaces the value — so every later child process inherits ${env}'s value. The
// script meanwhile decides with KAFKA_BROKERS_FROM_SHELL, captured before the source, which is
// correct because compose resolves the shell environment ahead of --env-file. The two then
// differ, and "KAFKA_BROKERS=broker-a ./stack.sh -u" against a .env naming broker-b had the
// script wait for, authenticate to and verify the catalogue on broker-a while the server and
// worker published to broker-b — with the bring-up reporting success.
//
// The fix is structural: one `compose` function pins the effective value on every invocation, so
// there is no second place for the precedence rule to be re-derived. This asserts the structure,
// because that is what makes it hold for a compose call added later.
func TestStackScript_PinsOneEffectiveBrokerValueOnEveryComposeInvocation(t *testing.T) {
	stack := readRepoFile(t, "stack.sh")

	// The helper exists and pins the value it was given by the one resolver.
	assert.Contains(t, stack, `KAFKA_BROKERS="$(effective_kafka_brokers)" \`,
		"the compose helper must pin the EFFECTIVE broker list on the invocation, so what compose "+
			"interpolates is what this script decided rather than whatever survived sourcing .env")

	// EVERY invocation goes through it. Counting raw ${COMPOSE_CL} uses is the assertion that
	// keeps this true: one is the helper's own, and the rest must be printed advice rather than
	// executed commands, because an executed one would be a call that skipped the pin.
	for _, line := range strings.Split(stack, "\n") {
		if !strings.Contains(line, "${COMPOSE_CL}") {
			continue
		}

		trimmed := strings.TrimSpace(line)

		// The helper's own invocation.
		if strings.HasPrefix(trimmed, "${COMPOSE_CL} --env-file \"${env}\" -f \"${COMPOSE_FILE}\" \"$@\"") {
			continue
		}

		// A TEST of the variable, not an invocation of it. resolve_compose_cl has to read
		// COMPOSE_CL to honour an explicit pin — exported by the caller or set in ${env} — before
		// it probes the host for a Compose implementation, and COMPOSE_CL now starts EMPTY rather
		// than assuming the legacy "docker-compose" binary. Reading the variable interpolates no
		// command and so cannot skip the broker pin; an executed call still fails below.
		if strings.HasPrefix(trimmed, `if [ -n "${COMPOSE_CL}" ]`) ||
			strings.HasPrefix(trimmed, `if [ -z "${COMPOSE_CL}" ]`) {
			continue
		}

		// THE VERSION PROBE, and it is the one function allowed to receive the variable.
		//
		// resolve_compose_cl must know which Compose it is about to use, because the compose files
		// need depends_on.required and that field arrived in 2.20.0 — a host below it cannot parse
		// them at all. compose_version_of runs `<invocation> version --short`, which reads no
		// compose file, interpolates no service and consults no KAFKA_BROKERS, so it is outside
		// the precedence problem this guard exists for rather than an exception to it.
		//
		// Named explicitly rather than allowing "any function call", so a helper added later that
		// runs a REAL compose command on the side still fails here.
		if strings.Contains(trimmed, `compose_version_of "${COMPOSE_CL}"`) {
			continue
		}

		// Everything that survives to here must be a READ of the variable's value rather than an
		// execution of it: printed advice, a message argument, a comparison. What fails is
		// COMMAND POSITION — the only place an invocation can skip the pin.
		//
		// Tested by position rather than by "the line starts with a quote", which was the earlier
		// rule and was wrong in both directions: it rejected a printf whose message merely names
		// the variable, and it accepted a continuation line that began with a quote and went on to
		// execute one.
		commandPositions := []string{
			`${COMPOSE_CL} `,
			`${COMPOSE_CL}"`,
			`${COMPOSE_CL}` + "\n",
		}
		executed := false
		for _, opener := range []string{"", "$(", "`", "| ", "|| ", "&& ", "; ", "then ", "do ", "! "} {
			for _, position := range commandPositions {
				if strings.HasPrefix(trimmed, opener+position) {
					executed = true
				}
			}
			if strings.Contains(trimmed, opener+`${COMPOSE_CL} `) && opener != "" {
				executed = true
			}
		}

		assert.Falsef(t, executed,
			"stack.sh line %q invokes compose directly. Every invocation must go through the "+
				"compose() helper, or it receives whichever KAFKA_BROKERS survived sourcing .env "+
				"instead of the effective one", trimmed)
	}

	// THE VERSION FLOOR ITSELF, asserted here because the resolver is the only thing that enforces
	// it and a silent removal would reintroduce F-33: a Compose v1 host that fails on
	// depends_on.required rather than on a version check, with an error naming a field.
	assert.Contains(t, stack, `COMPOSE_MINIMUM_VERSION="2.20.0"`,
		"the resolver must state the version floor the compose files require, because "+
			"depends_on.required arrived in Compose 2.20.0 and an older release cannot parse them")
	assert.NotContains(t, stack, `command -v docker-compose`,
		"the standalone docker-compose script is Compose v1, below the floor, and must not be "+
			"probed for: falling back to it replaces a clear version refusal with a parse error")
	assert.NotContains(t, stack, `COMPOSE_CL="docker-compose"`,
		"nothing may resolve to Compose v1")

	// ONE resolver, not two. Both copies were identical, so the later silently shadowed the
	// former and a fix applied to the first would have had no effect at all.
	assert.Equal(t, 1, strings.Count(stack, "\nresolve_compose_cl() {"),
		"resolve_compose_cl must be defined exactly once; a duplicate definition shadows the "+
			"first and makes an edit to it a no-op")

	// The two-variable capture that makes the precedence reproducible must survive: "unset" and
	// "set to empty" are different answers, and compose treats an explicitly empty shell value as
	// a real value that overrides --env-file.
	assert.Contains(t, stack, `declare KAFKA_BROKERS_DECLARED_IN_SHELL="${KAFKA_BROKERS+yes}"`,
		"declaration must be captured separately from value, so an explicit KAFKA_BROKERS= is "+
			"honoured as 'no brokers for this run' rather than falling back to .env")
	assert.Contains(t, stack, `declare KAFKA_BROKERS_FROM_SHELL="${KAFKA_BROKERS-}"`,
		"and the pre-source value must be captured before sourcing .env can replace it")

	// The host fallback pins it too, and did already — it is the precedent this generalises.
	assert.Contains(t, stack, `KAFKA_BROKERS="${brokers}" \`,
		"provision_kafka must keep passing the effective list explicitly, so the broker the "+
			"catalogue is created on is the broker the application publishes to")
}

// TestStackScript_LeavesTheNonApplicableCasesUnfailed asserts the gate does not invent work.
//
// Three situations are not problems and must not be reported as any: no broker list, a compose
// file that declares no broker, and a caller who named specific services. Failing on those would
// make "./stack.sh --up postgres" fail because Kafka was not provisioned.
func TestStackScript_LeavesTheNonApplicableCasesUnfailed(t *testing.T) {
	script := readRepoFile(t, "stack.sh")

	require.Contains(t, script, "kafka_staging_applicable",
		"the applicability decision must be its own named function, so the three cases it "+
			"excludes are readable in one place")

	for _, evidence := range []string{
		"effective_kafka_brokers",
		"config --services",
	} {
		assert.Containsf(t, script, evidence,
			"kafka_staging_applicable must consider %q: an empty broker list and a compose file with "+
				"no broker are both supported states, not failures", evidence)
	}
}

// ---------------------------------------------------------------------------
// F20 — no shipped superuser credential, and no broker on every interface
// ---------------------------------------------------------------------------

// TestCompose_ShipsNoDefaultKafkaAdminCredential is the CWE-798 guard.
//
// Every service that needs the administrative pair used to default it to "admin" and a fixed
// placeholder secret. That principal is in the broker's super.users: it can create topics, mint
// a SCRAM credential for any subscriber and rewrite every ACL on the cluster — so a published
// default for it is not one weak local password but the key from which every other credential on
// the broker can be minted, shared by every deployment that never overrode it.
//
// The assertion is textual because the property is textual: what must not exist is an
// interpolation DEFAULT, and a parsed document shows only the resolved value.
func TestCompose_ShipsNoDefaultKafkaAdminCredential(t *testing.T) {
	for _, file := range composeProjections {
		contents := readRepoFile(t, file)

		for _, variable := range []string{"KAFKA_SASL_ADMIN_USER", "KAFKA_SASL_ADMIN_SECRET"} {
			// The only accepted form is the empty default: "${NAME:-}". Anything between the
			// dash and the closing brace is a shipped credential.
			marker := "${" + variable + ":-"
			for _, line := range strings.Split(contents, "\n") {
				index := strings.Index(line, marker)
				if index < 0 {
					continue
				}

				remainder := line[index+len(marker):]
				closing := strings.Index(remainder, "}")
				require.Positivef(t, closing+1, "%s: malformed interpolation of %s: %s", file, variable, line)

				assert.Emptyf(t, remainder[:closing],
					"%s: %s must have an EMPTY default. A value here is a hardcoded credential for a "+
						"cluster superuser — the one identity that can mint every other credential on the "+
						"broker. .env.example ships it empty and ./stack.sh --init generates it.\n  %s",
					file, variable, strings.TrimSpace(line))
			}
		}

		assert.NotContainsf(t, contents, "blnk-local-dev-admin-secret",
			"%s: the placeholder administrative secret must be gone entirely, comments included", file)
	}
}

// TestCompose_PublishesTheBrokerOnLoopbackByDefault asserts the host binding.
//
// A bare "9092:29092" publishes on 0.0.0.0. On a laptop on a café or office network that exposes
// a broker holding ledger events, and a SASL listener guarding them, to every machine on that
// network. Nothing in the stack needs it: server, worker and kafka-init all reach the broker over
// the compose network's own listener, which is never published.
func TestCompose_PublishesTheBrokerOnLoopbackByDefault(t *testing.T) {
	for _, file := range composeProjections {
		ports, ok := composeService(t, file, "kafka")["ports"].([]interface{})
		require.Truef(t, ok, "%s: the kafka service must publish a port", file)
		require.Lenf(t, ports, 1, "%s: the kafka service must publish exactly one port — the host listener", file)

		mapping := toStringValue(ports[0])
		require.NotEmptyf(t, mapping, "%s: the published port must be the short string form", file)

		assert.Containsf(t, mapping, "${KAFKA_OUTER_HOST:-127.0.0.1}",
			"%s: the broker's published port must be bound to a host address defaulting to LOOPBACK. "+
				"Got %q", file, mapping)

		// The advertised host has to be overridable alongside the binding. Kafka answers every
		// client with the ADVERTISED address, so a broker exposed on a LAN address while still
		// advertising "localhost" completes the handshake and then hands the client its own
		// loopback — a failure that reads as a broker fault rather than as a configuration one.
		assert.Containsf(t, readRepoFile(t, file), "${KAFKA_OUTER_ADVERTISED_HOST:-localhost}",
			"%s: the advertised host must be overridable with the binding, or exposing the broker "+
				"deliberately produces a broker that authenticates and then cannot be read", file)
	}
}

// TestCompose_ShipsNoSampleSubscriberGroupPrefix asserts the shipped stack can actually
// provision itself.
//
// # The defect
//
// The sample subscriber's consumer-group namespace is DERIVED by kafka-provision.sh as
// "<principal>." — terminated, because a PREFIXED group grant on an unterminated
// "blnk-sample-subscriber" also matches "blnk-sample-subscriber-evil" — and the script refuses
// any override that disagrees with the derivation, printing what it derived and provisioning
// nothing.
//
// Both compose files defaulted the variable to the unterminated "blnk-sample-subscriber", which
// is precisely the value the guard rejects, and .env.example shipped the same. So a stock
// `docker compose --profile kafka up kafka kafka-init` — the exact command the isolation test's
// own skip message tells an operator to run — exited 1 with nothing provisioned: no topics, no
// principals, and therefore a broker against which the V-5 acceptance suite can only skip. The
// remedy the script printed, "unset the variable", could not be applied through Compose either,
// because `:-` re-supplied the default on every run.
//
// The trailing-terminator rule is CORRECT security behaviour and must not be relaxed to make
// the default work. The defaults are what change, and this test is what keeps them changed: the
// only acceptable shipped value is empty, which is how the script is told to derive.
func TestCompose_ShipsNoSampleSubscriberGroupPrefix(t *testing.T) {
	const variable = "KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX"

	for _, file := range composeProjections {
		marker := "${" + variable + ":-"
		found := false

		for _, line := range strings.Split(readRepoFile(t, file), "\n") {
			index := strings.Index(line, marker)
			if index < 0 {
				continue
			}

			found = true
			remainder := line[index+len(marker):]
			closing := strings.Index(remainder, "}")
			require.Positivef(t, closing+1, "%s: malformed interpolation of %s: %s", file, variable, line)

			assert.Emptyf(t, remainder[:closing],
				"%s: %s must interpolate with an EMPTY default. Any value here is passed to "+
					"kafka-provision.sh, which derives the namespace from the principal and REFUSES a "+
					"disagreeing override — so a non-empty default makes kafka-init exit 1 and "+
					"provision nothing on a stock bring-up, and Compose re-supplies it however many "+
					"times an operator unsets it.\n  %s",
				file, variable, strings.TrimSpace(line))
		}

		assert.Truef(t, found,
			"%s must still pass %s through to kafka-init: the script reads it to DETECT a stale "+
				"override and say it is no longer honoured, and dropping it would make a stale value "+
				"in an operator's environment silently ignored instead", file, variable)
	}

	// The template has to agree, or an operator who copies it to .env reintroduces the failure
	// through --env-file even though the compose default is now empty.
	for _, line := range strings.Split(readRepoFile(t, ".env.example"), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, variable+"=") {
			continue
		}

		assert.Equalf(t, variable+"=", trimmed,
			".env.example must ship %s empty: Compose reads --env-file as well as the shell, so a "+
				"value here fails the provisioning script's derivation guard exactly as the compose "+
				"default used to", variable)
	}
}

// TestEnvExample_ShipsTheKafkaCredentialsEmpty asserts the template agrees with the compose
// files, which is what makes removing the defaults a completed change rather than a broken one.
func TestEnvExample_ShipsTheKafkaCredentialsEmpty(t *testing.T) {
	lines := strings.Split(readRepoFile(t, ".env.example"), "\n")

	assignments := map[string]string{}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		key, value, found := strings.Cut(trimmed, "=")
		if !found {
			continue
		}

		assignments[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}

	for _, variable := range []string{
		"KAFKA_SASL_ADMIN_USER",
		"KAFKA_SASL_ADMIN_SECRET",
		"KAFKA_SASL_USER",
		"KAFKA_SASL_SECRET",
	} {
		value, declared := assignments[variable]
		require.Truef(t, declared, ".env.example must declare %s, so an operator knows it exists", variable)
		assert.Emptyf(t, value,
			"%s must ship EMPTY. A credential committed to a template is a credential in every "+
				"clone of this repository", variable)
	}

	require.Contains(t, assignments, "KAFKA_OUTER_HOST",
		".env.example must declare KAFKA_OUTER_HOST, or the loopback binding is undocumented")
	assert.Equal(t, "127.0.0.1", assignments["KAFKA_OUTER_HOST"],
		"and it must default to loopback, matching the compose files")
	assert.Equal(t, "localhost", assignments["KAFKA_OUTER_ADVERTISED_HOST"],
		"with an advertised host that resolves to the same endpoint")
}

// TestStackScript_GeneratesBothKafkaPrincipals asserts --init produces the pair the compose
// files no longer default, AND the least-privileged producer identity.
//
// Removing the compose defaults without this would trade a security defect for a usability one:
// the operator would be told to supply a credential with no supported way to obtain it.
func TestStackScript_GeneratesBothKafkaPrincipals(t *testing.T) {
	script := readRepoFile(t, "stack.sh")

	for _, variable := range []string{
		"KAFKA_SASL_ADMIN_USER",
		"KAFKA_SASL_ADMIN_SECRET",
		"KAFKA_SASL_USER",
		"KAFKA_SASL_SECRET",
	} {
		assert.Containsf(t, script, `set_env_value "`+variable+`"`,
			"stack.sh --init must write %s into .env; the compose files no longer default it, so "+
				"this is the supported way to obtain one", variable)
	}

	assert.Contains(t, script, "kafka_producer_principal",
		"and the producer principal must be named once, so the identity stack.sh writes and the "+
			"identity kafka-provision.sh mints are the same")
}

// TestStackScript_DefinesEachFunctionExactlyOnce is Q-02.
//
// # What was wrong
//
// resolve_compose_cl was defined TWICE, sixty-five lines apart, together with a duplicated
// section banner and a duplicated block of documentation. The two bodies were byte-identical,
// so nothing misbehaved — which is exactly what makes it worth a guard. In a shell script the
// LAST definition silently replaces the earlier one, so the copy a reader finds first, edits,
// and satisfies themselves about is not necessarily the copy that runs. The next divergent
// edit to the wrong copy would change nothing at all and would be indistinguishable from a
// change that did not work, on the function that decides whether the whole stack can talk to
// Docker.
//
// # Why the assertion is over EVERY function rather than that one
//
// The defect is a property of the file — 1,500 lines of shell with no compiler, where a
// redefinition is not an error and not a warning — so pinning the single function that
// happened to be duplicated would leave the next one unguarded. Counting every definition
// costs nothing and states the invariant that actually matters: one name, one body.
func TestStackScript_DefinesEachFunctionExactlyOnce(t *testing.T) {
	script := readRepoFile(t, "stack.sh")

	// Top-level POSIX definitions only: `name() {` at column zero. A nested or indented
	// helper is out of scope here, and this file has none.
	definition := regexp.MustCompile(`(?m)^([A-Za-z_][A-Za-z0-9_]*)\(\)\s*\{`)

	counts := map[string]int{}
	for _, match := range definition.FindAllStringSubmatch(script, -1) {
		counts[match[1]]++
	}

	require.NotEmpty(t, counts, "the definition pattern must actually match this script")

	for name, count := range counts {
		assert.Equalf(t, 1, count,
			"stack.sh defines %s() %d times. A shell script silently keeps the LAST definition, so "+
				"the copy a reader edits may not be the copy that runs — and identical copies make "+
				"the next divergent edit invisible rather than wrong. Keep one definition.",
			name, count)
	}

	// The function the finding named, asserted by name as well as by count, so the regression
	// this test exists for is legible without decoding the loop above.
	assert.Equal(t, 1, counts["resolve_compose_cl"],
		"resolve_compose_cl must be defined exactly once")

	// The duplicated banner went with it. Two identically titled sections is how the second
	// copy escaped review in the first place: each section read correctly on its own.
	assert.Equal(t, 1, strings.Count(script, "# The Compose command line\n"),
		"the duplicated section banner must be gone too; two identically titled sections is what "+
			"made the duplicate body look like the only body")
}

// ---------------------------------------------------------------------------
// F20 — the publisher is a separate, least-privileged identity
// ---------------------------------------------------------------------------

// TestKafkaProvisionScript_MintsALeastPrivilegedProducer asserts the provisioning script creates
// the producer principal and grants it only what publishing needs.
//
// Before it did, .env.example documented a "blnk-producer" principal that nothing anywhere
// created — so an operator who followed the documentation got an authentication failure, and the
// realistic response was to leave KAFKA_SASL_USER empty and publish as the administrator. A
// documented identity with no way to obtain it is not privilege separation.
func TestKafkaProvisionScript_MintsALeastPrivilegedProducer(t *testing.T) {
	script := readRepoFile(t, filepath.Join("scripts", "kafka-provision.sh"))

	require.Contains(t, script, "ensure_producer_principal",
		"the provisioning script must mint the producer principal")
	require.Contains(t, script, "grant_producer_acls",
		"and grant it its ACLs")

	// It has to be called, not merely defined. A provisioning function nothing invokes is the
	// same defect as a relay nothing starts.
	main := script[strings.LastIndex(script, "\nmain() {"):]
	assert.Contains(t, main, "ensure_producer_principal",
		"main must invoke it; a function nothing calls provisions nothing")

	// The grant, and the absence of everything else. The producer path must never acquire the
	// authority the administrative principal has, because the whole point of the second identity
	// is that a compromise of the publish path is not a compromise of the cluster.
	grant := script[strings.Index(script, "grant_producer_acls() {"):]
	grant = grant[:strings.Index(grant, "\n}\n")]

	assert.Contains(t, grant, "--operation Write",
		"the producer must be granted Write: that is what publishing is")
	assert.Contains(t, grant, "--operation Describe",
		"and Describe, so the writer can see a topic's partitions")
	assert.Contains(t, grant, "--resource-pattern-type literal",
		"every topic must be named with a LITERAL pattern; a wildcard grant is indistinguishable "+
			"from no privilege separation at all")

	for _, forbidden := range []string{
		"--operation Read",
		"--operation Create",
		"--operation Alter",
		"--operation ClusterAction",
		"--operation IdempotentWrite",
		"--group",
		"--cluster",
	} {
		assert.NotContainsf(t, grant, forbidden,
			"the producer grant must not include %q. Blnk publishes and does not consume its own "+
				"topics; topic assurance is the administrative client's work; and the Go client the "+
				"relay publishes with does not implement the idempotent producer, so no cluster grant "+
				"is needed either", forbidden)
	}
}

// TestKafkaProvisionScript_ReadsTheProducerIdentityTheApplicationUses asserts the script and the
// application resolve the SAME two variables.
//
// A separate KAFKA_PRODUCER_* pair would have been the obvious shape and the wrong one: the
// script would then mint one identity while the server and worker authenticated as another, and
// the symptom would be an authentication failure with two correct-looking configurations.
func TestKafkaProvisionScript_ReadsTheProducerIdentityTheApplicationUses(t *testing.T) {
	script := readRepoFile(t, filepath.Join("scripts", "kafka-provision.sh"))

	assert.Contains(t, script, `KAFKA_SASL_USER="${KAFKA_SASL_USER:-}"`,
		"the script must read KAFKA_SASL_USER — the same name config.KafkaConfig resolves — so the "+
			"principal it creates and the principal the relay presents cannot diverge")
	assert.Contains(t, script, `KAFKA_SASL_SECRET="${KAFKA_SASL_SECRET:-}"`,
		"and KAFKA_SASL_SECRET, for the same reason")

	// Both must survive delegation into the broker container, which is how the compose one-shot
	// runs. A variable left out of the interface is silently replaced by the script's own
	// default — here, "not configured" — so the producer would be skipped with no diagnosis.
	interface_ := kafkaProvisionInterface(t)

	for _, variable := range []string{
		"KAFKA_SASL_USER",
		"KAFKA_SASL_SECRET",
		"KAFKA_SKIP_PRODUCER",
		"KAFKA_ROTATE_PRODUCER_SECRET",
	} {
		assert.Containsf(t, interface_, variable,
			"%s must be forwarded when this script delegates into the broker container, or the "+
				"compose one-shot silently falls back to this script's own default", variable)
	}
}

// kafkaProvisionInterface returns the provisioning script's own canonical variable list, by
// asking the script for it.
//
// IT EXECUTES THE SCRIPT rather than parsing it, and that is the point: "--print-interface" is
// the contract ./stack.sh reads at bring-up, so a test that read the array declaration by regex
// could pass while the flag printed something else entirely — and it is the flag's output that
// governs what a real run forwards.
//
// The call is safe to make from a unit test because parse_arguments runs before every side
// effect: nothing is read from configuration, no broker is contacted and nothing is provisioned.
// A pruned environment is passed anyway, so a developer with real Kafka variables exported runs
// the same test CI does.
func kafkaProvisionInterface(t *testing.T, flags ...string) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	path := filepath.Join(moduleRootDir(t), "scripts", "kafka-provision.sh")
	arguments := append([]string{path}, flags...)
	if len(flags) == 0 {
		arguments = append(arguments, "--print-interface")
	}

	command := exec.CommandContext(ctx, "bash", arguments...)
	command.Env = []string{"PATH=" + os.Getenv("PATH")}

	output, err := command.Output()
	require.NoError(t, err,
		"scripts/kafka-provision.sh --print-interface must succeed with no environment at all: "+
			"stack.sh calls it on every bring-up, and a failure there means no settings are forwarded")

	names := make([]string, 0, 40)
	for _, line := range strings.Split(string(output), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			names = append(names, trimmed)
		}
	}

	require.NotEmpty(t, names, "the interface must not be empty, or this test proves nothing")

	return names
}

// TestKafkaProvisionScript_HandsOffEverySupportedSetting is the static equality assertion the
// three invocation paths are held to.
//
// FOUR PATHS PROVISION THE SAME BROKER: the script run directly, the script re-executing itself
// inside the broker container, ./stack.sh's host fallback, and the compose kafka-init one-shot.
// Each used to carry its own hand-written list of variables to forward, and they had drifted in
// every direction at once — stack.sh omitted the CLI timeout pair, the producer secret file and
// the real skip flag; both compose blocks omitted those plus partition growth, the readiness
// budget, the subscriber skip and rotation flags, and the client configuration.
//
// A MISSING NAME DOES NOT ERROR, WHICH IS WHY THIS TEST EXISTS. The receiving run defaults the
// variable and reports success, so the same .env provisioned differently depending on which path
// ran — and which ran depended on nothing more than whether the host happened to have the Kafka
// CLI. Invisible, and unreproducible.
//
// stack.sh no longer restates the list at all: it reads "--print-interface-host" at startup, so
// it cannot drift and there is nothing to assert but the absence of a literal copy. The compose
// files cannot execute anything at parse time, so they must restate — and this is what holds the
// restatement to the declaration.
func TestKafkaProvisionScript_HandsOffEverySupportedSetting(t *testing.T) {
	declared := kafkaProvisionInterface(t)

	t.Run("both compose one-shots set exactly the declared interface", func(t *testing.T) {
		for _, file := range composeProjections {
			environment, ok := composeService(t, file, "kafka-init")["environment"].(map[string]interface{})
			require.Truef(t, ok, "%s: kafka-init must declare an environment map", file)

			set := make([]string, 0, len(environment))
			for name := range environment {
				// TZ is the container's own concern and not part of the provisioning contract.
				if strings.HasPrefix(name, "KAFKA_") {
					set = append(set, name)
				}
			}

			// EQUALITY, in both directions. A missing name is a setting silently lost; an
			// EXTRA name is a setting compose believes it is passing and the script never
			// reads, which is just as misleading to whoever maintains the .env.
			assert.ElementsMatchf(t, declared, set,
				"%s: kafka-init's environment must be exactly the interface "+
					"'scripts/kafka-provision.sh --print-interface' declares. A name the script "+
					"reads and this block omits is defaulted by the one-shot and reported as "+
					"success, so the compose path provisions something other than what the .env "+
					"asked for while stack.sh's host fallback honours it", file)
		}
	})

	t.Run("stack.sh reads the interface instead of restating it", func(t *testing.T) {
		stack := readRepoFile(t, "stack.sh")

		assert.Contains(t, stack, "--print-interface-host",
			"stack.sh must read the interface from the script that owns it, so the two cannot drift")
		assert.Contains(t, stack, "resolve_kafka_provision_interface",
			"and it must do so through the resolver, which reports a failure rather than leaving an "+
				"empty forwarding behind")

		// A literal array would be a second copy, and a second copy is what drifted before.
		assert.NotContains(t, stack, "declare -a kafka_provision_passthrough=(\n",
			"stack.sh must not declare a literal pass-through list: reading the script's own "+
				"declaration is what makes the two the same interface rather than two lists that "+
				"agree today")

		// The host-only half must reach a host-side invoker, since it is how the script is told
		// where to delegate. Asserted through the flag, so the split itself is exercised.
		hostOnly := kafkaProvisionInterface(t, "--print-interface-host")
		assert.Subset(t, hostOnly, declared,
			"the host form must be a superset of the container form: it adds delegation targets "+
				"rather than replacing the interface")
		assert.Contains(t, hostOnly, "KAFKA_CONTAINER",
			"a host-side invoker must be able to name the container to delegate into")
		assert.NotContains(t, declared, "KAFKA_CONTAINER",
			"and that name must NOT cross into the container, where it is meaningless")
	})

	t.Run("one skip flag governs the producer, and the retired name is refused", func(t *testing.T) {
		script := readRepoFile(t, filepath.Join("scripts", "kafka-provision.sh"))

		assert.Contains(t, declared, "KAFKA_SKIP_PRODUCER",
			"the canonical skip flag must be part of the interface every path forwards")
		assert.NotContains(t, declared, "KAFKA_SKIP_PRODUCER_PRINCIPAL",
			"the retired flag must not be forwarded anywhere")

		// It is READ in exactly one place — the retired-variable refusal — and nowhere else.
		// Two variables used to gate different halves of one decision: one skipped the
		// producer's VALIDATION and the other skipped the broker MUTATION, and the forwarding
		// was split the same way, so neither entry point could express "skip the producer"
		// completely and each half looked like a bug in the other.
		assert.Contains(t, script, `"KAFKA_SKIP_PRODUCER_PRINCIPAL=KAFKA_SKIP_PRODUCER"`,
			"the retired name must be declared as retired, with its replacement, so a stack still "+
				"setting it is told rather than silently losing the skip it asked for")
		assert.NotContains(t, script, `is_truthy "$KAFKA_SKIP_PRODUCER_PRINCIPAL"`,
			"nothing may still branch on the retired flag")
		assert.NotContains(t, script, `KAFKA_SKIP_PRODUCER_PRINCIPAL="${KAFKA_SKIP_PRODUCER_PRINCIPAL:-}"`,
			"and it must not be defaulted, or the refusal would fire on every run")

		// Declared once. It was declared twice, in one block, with near-identical prose.
		assert.Equal(t, 1,
			strings.Count(script, `KAFKA_SKIP_PRODUCER="${KAFKA_SKIP_PRODUCER:-}"`),
			"the canonical flag must have exactly one declaration")

		for _, gate := range []string{
			`require_valid_producer() {`,
			`ensure_producer_principal() {`,
		} {
			require.Contains(t, script, gate)
		}

		// Validation and mutation must read the SAME name. Counting the reads is what proves
		// they were unified rather than merely renamed in one of the two places.
		assert.GreaterOrEqual(t, strings.Count(script, `is_truthy "$KAFKA_SKIP_PRODUCER"`), 3,
			"the validation gate, the rotation-destination check and the mutation gate must all "+
				"read the one canonical flag")
	})
}

// TestCompose_PassesTheProducerIdentityToProvisioning asserts the compose one-shot hands the
// producer pair to the script, which is what makes a local `docker compose up` produce a
// least-privileged publisher rather than only documenting one.
func TestCompose_PassesTheProducerIdentityToProvisioning(t *testing.T) {
	for _, file := range composeProjections {
		environment, ok := composeService(t, file, "kafka-init")["environment"].(map[string]interface{})
		require.Truef(t, ok, "%s: kafka-init must declare an environment map", file)

		for _, variable := range []string{"KAFKA_SASL_USER", "KAFKA_SASL_SECRET"} {
			value, present := environment[variable]
			require.Truef(t, present,
				"%s: kafka-init must receive %s, or the principal the server and worker authenticate "+
					"as is never created", file, variable)
			assert.Equalf(t, "${"+variable+":-}", toStringValue(value),
				"%s: %s must be interpolated with an EMPTY default — the same expression the server "+
					"and worker read, so one .env configures both sides", file, variable)
		}
	}
}

// TestLogLevel_IsProjectedByEveryRuntimeSurfaceThatAdvertisesIt closes the gap between an
// instruction and the deployments it is given to.
//
// The event pipeline's per-event diagnostics are emitted at DEBUG on purpose — the
// successful-publish lines in the relay and the publisher, the metrics collector's per-tick
// summary, and the notice that a consumer-lag series has been retired — because at the 500
// events per second this pipeline targets, a line per published event saying "it worked" is
// volume rather than observability. .env.example therefore tells an operator to raise the
// level to investigate event delivery, and docs/metrics.md repeats it.
//
// That instruction is only true where the variable actually reaches the process. It reached a
// binary started by hand and NOTHING ELSE: no compose service forwarded it and no Deployment
// projected it, so an operator following the documentation on the two deployments Blnk ships
// changed the level and saw no additional line, with nothing to explain why. The failure is
// silent in both directions — the pipeline looks quiet and the setting looks ineffective.
//
// Every surface is asserted here rather than one per file so the four projections cannot drift
// apart: a key added to the compose files and forgotten in the manifests leaves the same gap
// on the deployment that is hardest to debug.
func TestLogLevel_IsProjectedByEveryRuntimeSurfaceThatAdvertisesIt(t *testing.T) {
	const variable = "BLNK_LOG_LEVEL"

	// The template advertises it. This is the claim the projections below have to honour, so
	// it is asserted rather than assumed: were the key ever removed from the template, the
	// rest of this test would be enforcing a contract nobody had published.
	assert.Containsf(t, readRepoFile(t, ".env.example"), variable+"=",
		".env.example must declare %s: it is where the instruction to raise the level for an "+
			"event-delivery investigation is published", variable)

	t.Run("both compose services forward it", func(t *testing.T) {
		for _, file := range composeProjections {
			for _, service := range composeApplicationServices {
				environment, ok := composeService(t, file, service)["environment"].(map[string]interface{})
				require.Truef(t, ok, "%s: the %q service must declare an environment map", file, service)

				value, present := environment[variable]
				require.Truef(t, present,
					"%s: the %q service must forward %s, or raising the level has no effect on the "+
						"compose stack — which is the deployment an operator is most likely to be "+
						"debugging", file, service, variable)

				// Compose's PASS-THROUGH form: a key with no value copies the variable when it
				// is set and leaves it ENTIRELY ABSENT when it is not. `${NAME:-}` would set an
				// empty string instead, which is the shape that silently defeats the
				// BLNK_-prefixed alias block this key belongs to.
				assert.Emptyf(t, toStringValue(value),
					"%s: %s must use compose's pass-through form — the key with nothing after the "+
						"colon — so an unset variable is absent from the container rather than "+
						"present and empty.\n  got: %q", file, variable, toStringValue(value))
			}
		}
	})

	t.Run("the ConfigMap declares it and both Deployments project it", func(t *testing.T) {
		root := moduleRootDir(t)

		configMap := readYAMLFile(t, filepath.Join(root, "infrastructure", "k8s-manifests", "blnk-config.yaml"))
		data, ok := configMap["data"].(map[string]interface{})
		require.True(t, ok, "blnk-config.yaml must declare a data map")

		declared, present := data[variable]
		require.Truef(t, present,
			"blnk-config.yaml must declare %s, or the Deployments below have no key to reference "+
				"and every pod fails to start", variable)
		assert.Emptyf(t, toStringValue(declared),
			"%s must ship EMPTY, which means info: debug is verbose in proportion to throughput and "+
				"is a diagnostic setting for the duration of an investigation, not a deployment "+
				"default", variable)

		for _, manifest := range []string{"server-deployment.yaml", "worker-deployment.yaml"} {
			deployment := readYAMLFile(t, filepath.Join(root, "infrastructure", "k8s-manifests", manifest))

			reference := deploymentEnvConfigMapKey(t, deployment, manifest, variable)
			assert.Equalf(t, "blnk-config", reference["name"],
				"%s: %s must come from the blnk-config ConfigMap", manifest, variable)
			assert.Equalf(t, variable, reference["key"],
				"%s: %s must reference the key of the same name, or the value an operator edits is "+
					"not the value the pod reads", manifest, variable)
		}
	})
}

// deploymentEnvConfigMapKey returns one Deployment env entry's configMapKeyRef.
//
// It walks the parsed manifest rather than grepping because the property is structural: the
// entry has to be a valueFrom.configMapKeyRef, and a `value:` carrying a literal — which a
// grep for the name would accept — would hard-code the level into the manifest and take a
// rollout to change either way.
//
// Parameters:
//   - t *testing.T: for the fatal on a manifest that does not have the expected shape.
//   - deployment map[string]interface{}: the parsed Deployment.
//   - manifest string: the file name, for messages.
//   - variable string: the environment variable to find.
//
// Returns:
//   - map[string]interface{}: the configMapKeyRef of that entry.
func deploymentEnvConfigMapKey(
	t *testing.T,
	deployment map[string]interface{},
	manifest, variable string,
) map[string]interface{} {
	t.Helper()

	spec, ok := deployment["spec"].(map[string]interface{})
	require.Truef(t, ok, "%s must declare a spec", manifest)
	template, ok := spec["template"].(map[string]interface{})
	require.Truef(t, ok, "%s must declare a pod template", manifest)
	podSpec, ok := template["spec"].(map[string]interface{})
	require.Truef(t, ok, "%s must declare a pod spec", manifest)
	containers, ok := podSpec["containers"].([]interface{})
	require.Truef(t, ok && len(containers) > 0, "%s must declare at least one container", manifest)
	container, ok := containers[0].(map[string]interface{})
	require.Truef(t, ok, "%s: the first container must be a mapping", manifest)
	environment, ok := container["env"].([]interface{})
	require.Truef(t, ok, "%s: the container must declare an env list", manifest)

	for _, raw := range environment {
		entry, isMap := raw.(map[string]interface{})
		if !isMap || toStringValue(entry["name"]) != variable {
			continue
		}

		valueFrom, hasValueFrom := entry["valueFrom"].(map[string]interface{})
		require.Truef(t, hasValueFrom,
			"%s: %s must be projected from the ConfigMap, not written as a literal value",
			manifest, variable)

		reference, hasReference := valueFrom["configMapKeyRef"].(map[string]interface{})
		require.Truef(t, hasReference,
			"%s: %s must use a configMapKeyRef", manifest, variable)

		return reference
	}

	t.Fatalf("%s: the container env list does not project %s", manifest, variable)

	return nil
}

// ---------------------------------------------------------------------------
// The production broker workload
//
// Both assertions below cover failures that a `kubectl apply` reports as SUCCESS. The
// manifest is accepted, the objects are created, and what goes wrong goes wrong minutes
// later inside a container or not at all until a Kafka upgrade — so neither is catchable by
// review of a diff or by any dry run, and each is asserted here instead.
// ---------------------------------------------------------------------------

// kafkaStatefulSet returns the parsed broker StatefulSet.
func kafkaStatefulSet(t *testing.T) map[string]interface{} {
	t.Helper()

	return readYAMLFile(t, filepath.Join(
		moduleRootDir(t), "infrastructure", "k8s-manifests", "kafka-statefulset.yaml",
	))
}

// kafkaBootstrapScript returns the text of the init container's shell body.
func kafkaBootstrapScript(t *testing.T) string {
	t.Helper()

	spec, ok := kafkaStatefulSet(t)["spec"].(map[string]interface{})
	require.True(t, ok, "kafka-statefulset.yaml must declare a spec")
	template, ok := spec["template"].(map[string]interface{})
	require.True(t, ok, "kafka-statefulset.yaml must declare a pod template")
	podSpec, ok := template["spec"].(map[string]interface{})
	require.True(t, ok, "kafka-statefulset.yaml must declare a pod spec")
	initContainers, ok := podSpec["initContainers"].([]interface{})
	require.Truef(t, ok && len(initContainers) > 0,
		"kafka-statefulset.yaml must declare the bootstrap init container: in KRaft a SCRAM "+
			"credential has to be seeded while the metadata log is created, so the broker cannot "+
			"authenticate anyone without it")

	container, ok := initContainers[0].(map[string]interface{})
	require.True(t, ok, "the first init container must be a mapping")
	command, ok := container["command"].([]interface{})
	require.Truef(t, ok && len(command) > 0, "the init container must declare a command")

	return toStringValue(command[len(command)-1])
}

// assertValidKafkaClusterID holds a value to what a Kafka cluster ID actually is.
//
// It is the base64url encoding of a 16-byte UUID: exactly 22 characters from
// [A-Za-z0-9_-], which is what `kafka-storage random-uuid` emits and what Uuid.fromString
// accepts — it rejects anything longer than 22 outright. Kafka's two reserved IDs are
// excluded as well, because a cluster claiming the zero UUID or the metadata topic ID is
// not a cluster anyone should be running.
//
// Parameters:
//   - t *testing.T: the test.
//   - source string: where the value came from, for the message.
//   - id string: the value to judge.
func assertValidKafkaClusterID(t *testing.T, source, id string) {
	t.Helper()

	assert.Lenf(t, id, 22,
		"%s: a Kafka cluster ID is EXACTLY 22 characters — the base64url encoding of a 16-byte "+
			"UUID. Kafka's Uuid.fromString rejects anything longer, and apache/kafka 3.9's format "+
			"tool does NOT check: it writes whatever string it is given into meta.properties and "+
			"the broker starts on it, so a wrong value works until something parses it as a UUID "+
			"and by then the volume is formatted with it. Generate one with "+
			"'kafka-storage.sh random-uuid'.\n  got: %q (%d characters)", source, id, len(id))

	decoded, err := base64.RawURLEncoding.DecodeString(id)
	require.NoErrorf(t, err,
		"%s: a cluster ID must decode as unpadded base64url; %q does not", source, id)
	assert.Lenf(t, decoded, 16,
		"%s: a cluster ID must decode to 16 bytes, the width of a UUID; %q decodes to %d",
		source, id, len(decoded))

	for _, reserved := range []string{"AAAAAAAAAAAAAAAAAAAAAA", "AAAAAAAAAAAAAAAAAAAAAQ"} {
		assert.NotEqualf(t, reserved, id,
			"%s: %q is one of Kafka's reserved UUIDs and cannot name a cluster", source, id)
	}
}

// TestKafkaStatefulSet_ComesUpInParallelBecauseOrderedReadyDeadlocksAQuorum covers a
// permanent cold-start deadlock that reports itself as a slow broker.
//
// Three combined broker+controller replicas are all KRaft voters, so committing metadata
// needs 2 of 3. Readiness is an authenticated, authorized `kafka-topics --list`, which
// cannot answer until the quorum has elected a leader. OrderedReady creates pod N+1 only
// once pod N is Ready. So kafka-0 waits for a quorum that needs kafka-1, and Kubernetes will
// not create kafka-1 until kafka-0 is Ready.
//
// Nothing breaks the cycle on its own: a failing readiness probe does not restart a pod, the
// startup probe is a TCP check that passes regardless, and no timeout applies. The set sits
// at 1/3 for ever.
//
// The policy is asserted together with the two facts that make it necessary, so the
// assertion cannot outlive its reason: were the set ever reduced to a single replica, or its
// readiness reduced to something that does not need the quorum, this test says so instead of
// enforcing a policy nobody can explain.
func TestKafkaStatefulSet_ComesUpInParallelBecauseOrderedReadyDeadlocksAQuorum(t *testing.T) {
	spec, ok := kafkaStatefulSet(t)["spec"].(map[string]interface{})
	require.True(t, ok, "kafka-statefulset.yaml must declare a spec")

	replicas, isNumber := spec["replicas"].(int)
	require.Truef(t, isNumber, "the broker StatefulSet must declare a replica count")
	require.GreaterOrEqualf(t, replicas, 3, "the KRaft quorum needs three voters to tolerate one loss")

	assert.Equalf(t, "Parallel", toStringValue(spec["podManagementPolicy"]),
		"with %d voting replicas and a quorum-dependent readiness probe, OrderedReady deadlocks "+
			"the cold start permanently: pod 0 cannot become Ready without a majority, and no "+
			"second pod is created until it is. podManagementPolicy governs creation and scaling "+
			"only — rolling updates stay ordered through updateStrategy — so Parallel costs "+
			"nothing on an upgrade", replicas)

	// The voter list is static configuration and must name every replica, or the majority the
	// policy above is chosen for cannot be reached however the pods are created.
	script := kafkaBootstrapScript(t)
	for ordinal := 0; ordinal < replicas; ordinal++ {
		assert.Containsf(t, script, fmt.Sprintf("%d@kafka-%d.kafka-headless.", ordinal, ordinal),
			"the KRaft voter list must name kafka-%d: it cannot be discovered, and it must be "+
				"identical on every broker", ordinal)
	}

	// And readiness must still be the authenticated check the policy is reasoned about. A TCP
	// probe here would remove the deadlock and make the assertion above unexplainable.
	template, ok := spec["template"].(map[string]interface{})
	require.True(t, ok)
	podSpec, ok := template["spec"].(map[string]interface{})
	require.True(t, ok)
	containers, ok := podSpec["containers"].([]interface{})
	require.True(t, ok && len(containers) > 0)
	broker, ok := containers[0].(map[string]interface{})
	require.True(t, ok)
	probe, ok := broker["readinessProbe"].(map[string]interface{})
	require.True(t, ok, "the broker must declare a readiness probe")
	exec, ok := probe["exec"].(map[string]interface{})
	require.Truef(t, ok,
		"readiness must be an EXEC probe: a broker that has bound its port but cannot "+
			"authenticate anyone is not ready, and a TCP check would call it ready and let the "+
			"relay publish into a cluster that rejects every connection")
	probeCommand, ok := exec["command"].([]interface{})
	require.True(t, ok && len(probeCommand) > 0)
	assert.Containsf(t, toStringValue(probeCommand[len(probeCommand)-1]), "--command-config",
		"the readiness check must authenticate, which is what makes it depend on the quorum")
}

// TestKafkaStatefulSet_RefusesToFormatWithoutAValidClusterID covers the one step in this
// deployment that cannot be corrected afterwards.
//
// The cluster ID is written into meta.properties, and a broker refuses a log whose ID
// disagrees with its configuration — so a wrong value is fixed by destroying the volume. The
// manifest once carried a 26-character fallback, which is not a cluster ID at all: it
// exceeds the 22 characters Uuid.fromString accepts and decodes to 19 bytes rather than 16.
// apache/kafka 3.9 formatted with it anyway and the broker started, which is what made the
// value dangerous rather than harmless — it would have survived until something parsed it as
// a UUID, with every volume already carrying it.
//
// So: no fallback in the format command, the shape enforced before the format runs, and any
// value actually configured — in the ConfigMap or in the compose files — held to the same
// shape here.
func TestKafkaStatefulSet_RefusesToFormatWithoutAValidClusterID(t *testing.T) {
	script := kafkaBootstrapScript(t)

	t.Run("the format command has no fallback", func(t *testing.T) {
		assert.Containsf(t, script, `--cluster-id "${KAFKA_CLUSTER_ID}"`,
			"the ID must be taken from the ConfigMap alone")

		// No DEFAULTING expansion anywhere in the script. `${KAFKA_CLUSTER_ID:-}` with an
		// empty default is fine and is what the emptiness check below uses — under `set -u`
		// it is how an unset variable is tested without aborting. `${KAFKA_CLUSTER_ID:-X}`
		// for any non-empty X is the defect: it formats the volume with an ID nobody chose,
		// and every deployment that left it unset would share that ID, which defeats the one
		// check Kafka does make — a broker refusing to join a cluster whose ID is not its own.
		const expansion = "${KAFKA_CLUSTER_ID:-"
		for offset := 0; ; {
			index := strings.Index(script[offset:], expansion)
			if index < 0 {
				break
			}

			at := offset + index + len(expansion)
			require.Lessf(t, at, len(script), "truncated expansion of KAFKA_CLUSTER_ID")
			assert.Equalf(t, byte('}'), script[at],
				"a NON-EMPTY shell default for the cluster ID formats the volume with an ID "+
					"nobody chose: %q", script[at-len(expansion):min(at+24, len(script))])

			offset = at
		}
	})

	t.Run("the shape is enforced before the format", func(t *testing.T) {
		formatAt := strings.Index(script, "kafka-storage.sh format")
		require.Positive(t, formatAt, "the init container must format the storage")

		preamble := script[:formatAt]
		assert.Containsf(t, preamble, `if [ -z "${KAFKA_CLUSTER_ID:-}" ]; then`,
			"an empty ID must be refused BEFORE the format, which is the last moment it is "+
				"still recoverable")
		assert.Containsf(t, preamble, `*[!A-Za-z0-9_-]*`,
			"the base64url alphabet must be enforced before the format")
		assert.Containsf(t, preamble, `[ "${#KAFKA_CLUSTER_ID}" -ne 22 ]`,
			"the 22-character width must be enforced before the format: it is the difference "+
				"between a UUID and a string that merely looks like one")
		assert.Containsf(t, preamble, "random-uuid",
			"every refusal must name the command that produces a valid value, or it is not "+
				"actionable at three in the morning")
	})

	t.Run("the ConfigMap declares the key and ships no value", func(t *testing.T) {
		configMap := readYAMLFile(t, filepath.Join(
			moduleRootDir(t), "infrastructure", "k8s-manifests", "blnk-config.yaml",
		))
		data, ok := configMap["data"].(map[string]interface{})
		require.True(t, ok, "blnk-config.yaml must declare a data map")

		value, present := data["KAFKA_CLUSTER_ID"]
		require.Truef(t, present,
			"blnk-config.yaml must DECLARE KAFKA_CLUSTER_ID even though it ships empty: the key "+
				"is where an operator learns the value is required, how to generate one, and that "+
				"it can never be changed after the first format")

		configured := toStringValue(value)
		if configured == "" {
			return
		}

		// A value committed here reaches production, so it is held to the real shape rather
		// than trusted to have come from the right command.
		assertValidKafkaClusterID(t, "blnk-config.yaml KAFKA_CLUSTER_ID", configured)
	})

	t.Run("the compose default is a valid cluster ID", func(t *testing.T) {
		// The local stack DOES default it, because a developer's broker is disposable and
		// re-created constantly. That default is still a real cluster ID.
		for _, file := range composeProjections {
			marker := "${KAFKA_CLUSTER_ID:-"
			text := readRepoFile(t, file)
			index := strings.Index(text, marker)
			require.Positivef(t, index, "%s must interpolate KAFKA_CLUSTER_ID for the broker", file)

			remainder := text[index+len(marker):]
			closing := strings.Index(remainder, "}")
			require.Positive(t, closing, "%s: malformed interpolation of KAFKA_CLUSTER_ID", file)

			assertValidKafkaClusterID(t, file+" KAFKA_CLUSTER_ID default", remainder[:closing])
		}
	})
}
