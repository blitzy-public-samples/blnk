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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
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
	//
	// Asserted on the export rather than on a line continuation. This used to read
	// `KAFKA_BROKERS="${brokers}" \`, which was the middle of an `env KEY=value ... script`
	// invocation. That invocation was replaced by a subshell that exports instead, because
	// `env KEY=value` puts the Kafka superuser password in argv and /proc/<pid>/cmdline is
	// world-readable. The GUARANTEE this line exists to protect is unchanged — the effective
	// broker list is still pinned explicitly — so the assertion follows it to its new spelling
	// rather than being deleted.
	assert.Contains(t, stack, `export KAFKA_BROKERS="${brokers}"`,
		"provision_kafka must keep passing the effective list explicitly, so the broker the "+
			"catalogue is created on is the broker the application publishes to")
	assert.Contains(t, stack, `export KAFKA_COMPOSE_SERVICE="${service}"`,
		"and the service the caller asked about must be pinned the same way, after the "+
			"passthrough loop, so it wins over any .env entry of the same name")

	// AND THE CREDENTIALS MUST NOT GO BACK INTO ARGV.
	//
	// provision_kafka forwards KAFKA_SASL_ADMIN_SECRET and KAFKA_PRODUCER_SECRET to the
	// provisioning script. `env NAME=value cmd` places both in the command line of a real
	// process, and on Linux /proc/<pid>/cmdline is mode 444 — readable by every account on the
	// host for as long as provisioning runs. The `export` builtin is executed by the shell
	// itself, so no command line is created and the values reach the child through its
	// environment, /proc/<pid>/environ, which is mode 400.
	assert.NotContains(t, stack, `env "${assignments[@]}"`,
		"provisioning credentials must not be forwarded as `env NAME=value` arguments: argv is "+
			"world-readable through /proc, so that publishes the broker superuser password to "+
			"every local account. Export them in a subshell instead")
	assert.Contains(t, stack, `export "${name}=${!name}"`,
		"the passthrough must forward each declared variable with the export builtin, which "+
			"creates no command line, rather than as an argument to env")
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

	// The grant is expressed as a DESIRED SET of canonical descriptors rather than as a
	// sequence of "kafka-acls --add" invocations, because the script reconciles the principal's
	// bindings to it instead of only appending to whatever is already there. So the assertions
	// below read the descriptors, which are where the operations and the pattern type now live.
	assert.Contains(t, grant, `acl_descriptor TOPIC "$topic" LITERAL WRITE`,
		"the producer must be granted Write on a LITERAL topic pattern: that is what publishing "+
			"is, and a wildcard pattern would be indistinguishable from no privilege separation")
	assert.Contains(t, grant, `acl_descriptor TOPIC "$topic" LITERAL DESCRIBE`,
		"and Describe, so the writer can see a topic's partitions")

	// The owned shapes are what the reconciler is allowed to REMOVE, so they bound the damage a
	// reconciliation can do as tightly as the desired set bounds the grant. Write and Describe
	// on a literal topic, and nothing else: a producer principal carrying anything further is
	// refused rather than silently narrowed, because the script did not create it.
	assert.Contains(t, grant, `ACL_OWNED_SHAPES=(
        "TOPIC|LITERAL|WRITE"
        "TOPIC|LITERAL|DESCRIBE"
    )`,
		"the shapes the reconciler owns must be exactly the two it grants; a wider ownership "+
			"claim would let provisioning delete a binding it did not create")

	assert.Contains(t, grant, `reconcile_principal_acls "$principal" "producer"`,
		"the grant must be RECONCILED, not appended to. kafka-acls --add is idempotent but it "+
			"only ever widens, so an --add-only producer grant kept full Write on every topic of "+
			"a previous KAFKA_TOPIC_PREFIX indefinitely")

	for _, forbidden := range []string{
		"READ",
		"CREATE",
		"ALTER",
		"CLUSTERACTION",
		"IDEMPOTENTWRITE",
		"--group",
		"--cluster",
		"GROUP|",
	} {
		assert.NotContainsf(t, grant, forbidden,
			"the producer grant must not include %q. Blnk publishes and does not consume its own "+
				"topics; topic assurance is the administrative client's work; and the Go client the "+
				"relay publishes with does not implement the idempotent producer, so no cluster grant "+
				"is needed either", forbidden)
	}
}

// TestKafkaProvisionScript_ReconcilesACLsRatherThanOnlyAddingThem asserts the provisioning
// script CONVERGES a principal's bindings on the configured set, in both directions.
//
// Every grant in the script used to be a bare "kafka-acls --add", justified as idempotent. It is
// idempotent and it is not convergent: --add can only widen. Three ordinary configuration
// changes therefore did not take effect, and each left the broker serving MORE than the
// configuration described while the run reported success and the summary printed the smaller set:
//
//   - removing a category from KAFKA_SAMPLE_SUBSCRIBER_TOPICS,
//   - changing KAFKA_TOPIC_PREFIX, which left both principals fully granted on the whole
//     previous namespace,
//   - renaming the sample subscriber or its consumer group, which left a reserved group
//     namespace nothing owned.
//
// The reconciler's shape is asserted here rather than only its existence, because two of its
// properties are what make it safe to let a provisioning script DELETE an ACL at all.
func TestKafkaProvisionScript_ReconcilesACLsRatherThanOnlyAddingThem(t *testing.T) {
	script := readRepoFile(t, filepath.Join("scripts", "kafka-provision.sh"))

	require.Contains(t, script, "reconcile_principal_acls() {",
		"the script must have a reconciler; an --add-only provisioner cannot narrow a grant")

	reconciler := script[strings.Index(script, "reconcile_principal_acls() {"):]
	reconciler = reconciler[:strings.Index(reconciler, "\n}\n")]

	// DELETE BEFORE CREATE. Every partial failure must leave the principal narrower than the
	// configuration, never broader — the same order, for the same reason, as
	// reconcileSubscriberACLs in event_admin.go.
	removeAt := strings.Index(reconciler, "apply_acl_binding --remove")
	addAt := strings.Index(reconciler, "apply_acl_binding --add")
	require.NotEqual(t, -1, removeAt, "the reconciler must remove surplus bindings")
	require.NotEqual(t, -1, addAt, "and create missing ones")
	assert.Less(t, removeAt, addAt,
		"removal must come BEFORE creation, so a reconciliation that dies between the two has "+
			"taken access away it was about to re-grant rather than left access nothing describes")

	// FAIL CLOSED ON A FOREIGN GRANT. A binding the script does not own and that can GRANT
	// makes the effective access broader than the configuration by an amount the script cannot
	// bound, so it refuses rather than reporting a grant it cannot state.
	assert.Contains(t, reconciler, "foreign_allow",
		"a foreign ALLOW binding must be recognised rather than silently tolerated")
	foreignRefusal := strings.Index(reconciler, "${#foreign_allow[@]} > 0")
	require.NotEqual(t, -1, foreignRefusal, "and refused")
	assert.Less(t, foreignRefusal, removeAt,
		"the refusal must come before anything is written, so a principal whose effective grant "+
			"cannot be stated is left exactly as it was found")

	// A DENY IS NEVER REMOVED. It can only narrow the effective grant, so deleting one would
	// WIDEN access as a side effect of provisioning.
	assert.Contains(t, reconciler, "foreign_deny",
		"a foreign DENY must be reported and left in place: removing it would widen access")

	// --force, or a removal run non-interactively abandons itself at the confirmation prompt
	// and the stale binding survives while the run reports success.
	applier := script[strings.Index(script, "apply_acl_binding() {"):]
	applier = applier[:strings.Index(applier, "\n}\n")]
	assert.Contains(t, applier, "--force",
		"kafka-acls --remove prompts for confirmation; without --force the compose one-shot "+
			"reads EOF and abandons the removal")

	// VERIFIED, not assumed. The success claim has to be a statement about the broker, and the
	// two divergences are not symmetric: a revoked binding that is still there means the grant
	// is still too BROAD and is fatal, while a granted binding that is still missing means it is
	// too NARROW, which fails safe and announces itself at the client's next connect.
	assert.Contains(t, reconciler, "still_present",
		"the end state must be re-read and compared, so 'reconciled' means the broker holds "+
			"exactly the desired set and not merely that every command exited zero")
	assert.Contains(t, reconciler, "still_missing",
		"in both directions")

	fatalOnBroad := strings.Index(reconciler, "${#still_present[@]} > 0")
	warnOnNarrow := strings.Index(reconciler, "${#still_missing[@]} > 0")
	require.NotEqual(t, -1, fatalOnBroad, "a surviving revoked binding must be detected")
	require.NotEqual(t, -1, warnOnNarrow, "and so must a missing granted one")
	assert.Contains(t, reconciler[fatalOnBroad:warnOnNarrow], "die ",
		"a grant that is still too broad must be FATAL: the run must not report a narrowing it "+
			"cannot see")
	assert.Contains(t, reconciler[warnOnNarrow:], "warn ",
		"a grant that is still too narrow must be reported rather than fatal; it fails safe, and "+
			"aborting would leave a half-provisioned deployment over the less dangerous divergence")

	// Both principals must go through it. A reconciled producer beside an --add-only subscriber
	// is the same defect confined to one identity.
	for _, function := range []string{"grant_producer_acls", "grant_subscriber_acls"} {
		body := script[strings.Index(script, function+"() {"):]
		body = body[:strings.Index(body, "\n}\n")]
		assert.Containsf(t, body, "reconcile_principal_acls",
			"%s must reconcile rather than append", function)
		assert.NotContainsf(t, body, "kafka_acls --add",
			"%s must not add bindings directly; that is the --add-only path this replaced",
			function)
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

// measuredEventOutboxRowBytes is the storage cost of one blnk.event_outbox row, MEASURED.
//
// 2,372 bytes: heap 2,048 including page overhead, plus 323 across all seventeen indexes, with
// no TOAST because the tuple is 1,888 bytes and the threshold is 2 KiB. Taken on PostgreSQL 16
// over 500,000 rows in a table created with `CREATE TABLE ... (LIKE blnk.event_outbox INCLUDING
// ALL)`, shaped like a real transaction.applied event read out of a running deployment. Roughly
// 88% of it is the event body, held three times over — the queryable JSONB projection, the
// byte-exact webhook body and the byte-exact envelope, each with its reason stated at the column
// in sql/1781248800.sql.
//
// It is a CONSTANT here rather than a comment because the assertions below are arithmetic on it,
// and the whole substance of PERF-C03 is that a retention period and a volume size were chosen
// without this number between them.
const measuredEventOutboxRowBytes = 2372

// eventRetentionBloatFactor is the multiplier for dead tuples and autovacuum headroom.
//
// Every row is UPDATEd at least twice on its way to a terminal state — claimed, then dispatched —
// and `status` is indexed, so neither update can be HOT: each leaves a dead tuple and rewrites
// index entries. 1.5 is a planning allowance, not padding.
const eventRetentionBloatFactor = 1.5

// validatedEventsPerSecond is the throughput acceptance criterion V-1 states the pipeline is
// validated at, and therefore the rate every capacity number in the manifests must hold at.
const validatedEventsPerSecond = 500

// TestManifests_RetentionStorageAndBrokerRetentionAgree is the PERF-C03 guard, and it exists
// because nothing was checking three numbers that have to be chosen together.
//
// # What went wrong, and why no single file was obviously incorrect
//
// blnk-config.yaml shipped ninety days of event-outbox retention, reasoned entirely from data
// protection and perfectly defensible on that basis alone. postgres-statefulset.yaml shipped a
// 10Gi volume, which is an unremarkable size for a reference manifest. kafka-statefulset.yaml
// documented, at its own log.retention.hours, that broker retention must stay LONGER than the
// outbox's — and asserted that nothing was in tension because retention "ships disabled".
//
// Every one of those three statements read correctly on its own. Together they were: ninety days
// at the validated rate is 8.4 TiB against a 10Gi volume, 858x apart and on the volume the
// LEDGER lives on; and ninety days is nearly thirteen times the broker's seven, so the coupling
// the third file documented was violated by the first.
//
// # Why the assertions are arithmetic rather than fixed numbers
//
// Pinning "3" and "600Gi" would fail the moment someone legitimately changed either, and would
// teach them nothing about what else had to move. These assert the RELATIONSHIPS, so a future
// change to any one number is accepted exactly when the others were changed with it — and the
// failure message says which one is short and by how much.
func TestManifests_RetentionStorageAndBrokerRetentionAgree(t *testing.T) {
	retentionDays := manifestRetentionDays(t)
	if retentionDays == 0 {
		t.Skip("retention is disabled in the manifests, so nothing is coupled to it")
	}
	require.Positivef(t, retentionDays,
		"RELAY_EVENT_RETENTION_DAYS must be zero or positive; a negative period is meaningless")

	t.Run("outbox retention stays inside the broker's own retention", func(t *testing.T) {
		brokerHours := manifestBrokerRetentionHours(t)
		require.Positive(t, brokerHours,
			"kafka-statefulset.yaml must declare log.retention.hours for this coupling to hold")

		assert.LessOrEqualf(t, retentionDays, brokerHours/24,
			"RELAY_EVENT_RETENTION_DAYS is %d but the broker retains only %d hours (%d days). The "+
				"daily outbox-versus-offset reconciliation compares outbox rows against broker "+
				"records and can only do that over a window BOTH sides still hold, so outbox rows "+
				"outliving their records leaves rows with nothing to reconcile against. Raise "+
				"log.retention.hours in the same change or lower the retention period",
			retentionDays, brokerHours, brokerHours/24)
	})

	t.Run("the PostgreSQL volume holds the outbox that retention implies", func(t *testing.T) {
		volumeBytes := manifestPostgresVolumeBytes(t)
		require.Positive(t, volumeBytes,
			"postgres-statefulset.yaml must declare a pg-data volumeClaimTemplate size")

		perDay := float64(validatedEventsPerSecond) * 86400 * measuredEventOutboxRowBytes
		steadyState := perDay * float64(retentionDays) * eventRetentionBloatFactor

		assert.Greaterf(t, float64(volumeBytes), steadyState,
			"the pg-data volume is %.1f GiB but %d days of outbox at the validated %d events/second "+
				"is %.1f GiB of steady state (%.1f GiB/day x %d days x %.1f bloat headroom, from the "+
				"measured %d bytes/row). The outbox shares this volume with the LEDGER, so an "+
				"over-generous retention period does not buy more history — it exhausts the volume "+
				"the ledger writes to",
			float64(volumeBytes)/(1<<30), retentionDays, validatedEventsPerSecond,
			steadyState/(1<<30), perDay/(1<<30), retentionDays, eventRetentionBloatFactor,
			measuredEventOutboxRowBytes)

		// AND WITH ROOM FOR EVERYTHING ELSE ON IT. The ledger's own tables are never purged, so
		// their growth is unbounded and no assertion can size them — but a volume that fits the
		// outbox and nothing else is a volume that fills, so a margin is required rather than
		// merely advisable. A fifth is the smallest share that is honestly an allowance.
		assert.Greaterf(t, float64(volumeBytes)*0.8, steadyState,
			"the outbox's %.1f GiB steady state must leave at least a fifth of the %.1f GiB volume "+
				"for the ledger's own tables, WAL up to max_wal_size, and the filesystem reserve",
			steadyState/(1<<30), float64(volumeBytes)/(1<<30))
	})

	t.Run("purge capacity clears the arrival rate twice over", func(t *testing.T) {
		data := manifestConfigMapData(t)
		batchSize := manifestIntSetting(t, data, "RELAY_EVENT_RETENTION_BATCH_SIZE")
		maxBatches := manifestIntSetting(t, data, "RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP")
		require.Positive(t, batchSize, "the batch size must be positive for capacity to be finite")
		require.Positive(t, maxBatches,
			"the batch ceiling must be positive here; 0 means UNSET and -1 means unbounded, and a "+
				"manifest that ships a deliberate capacity must state it")

		// The sweep runs hourly, so one sweep's capacity IS the hourly capacity.
		capacityPerHour := batchSize * maxBatches
		arrivalsPerHour := validatedEventsPerSecond * 3600

		assert.GreaterOrEqualf(t, capacityPerHour, 2*arrivalsPerHour,
			"purge capacity is %d rows/hour against %d arriving at the validated %d events/second. "+
				"Capacity BELOW arrival does not slow the table's growth, it permits it — the "+
				"sweeper never catches up and the retention period is not enforced however short it "+
				"is set. And merely exceeding arrival is not enough: a sweeper that cannot catch up "+
				"needs recovery capacity, because one sweep cut short by the ten-minute timeout "+
				"leaves a deficit measured in rows the period says should already be gone",
			capacityPerHour, arrivalsPerHour, validatedEventsPerSecond)
	})
}

// TestManifests_KafkaDataClaimsMatchTheStatefulSetTemplate pins the broker's storage claims
// to the workload that adopts them (AAP-M03).
//
// # The failure this catches, and why nothing else catches it
//
// kafka-data-persistentvolumeclaim.yaml pre-provisions the three claims the StatefulSet
// would otherwise create for itself. Adoption is by NAME and nothing else: Kubernetes
// looks for `<template>-<set>-<ordinal>`, and if it finds a claim there it uses it AS IT
// FINDS IT. It does not reconcile the claim against the template.
//
// That makes every property below a silent-divergence hazard rather than a validation
// error. A claim requesting less than the template is adopted at the smaller size, and the
// broker starts, and runs, and fills a disk that the retention arithmetic said had room. A
// renamed set or template orphans all three: new claims appear under the new names and
// these sit unbound, which is exactly the idle-volume defect that had this file deleted
// once already. A missing ordinal after a replica increase leaves one broker provisioned
// from the template and its siblings from this file, differing in whatever the two
// disagree on. `kubectl apply` reports success in all four cases.
//
// So the assertions DERIVE what they expect from the StatefulSet rather than restating it.
// Nothing here is a literal that a geometry change would have to remember to update — the
// count comes from `replicas`, the names from the template name and the set name, the size
// and access mode from the template itself. Change the workload and this test follows it;
// change one side only and it fails.
func TestManifests_KafkaDataClaimsMatchTheStatefulSetTemplate(t *testing.T) {
	statefulSet := kafkaStatefulSet(t)

	metadata, ok := statefulSet["metadata"].(map[string]interface{})
	require.True(t, ok, "kafka-statefulset.yaml must declare metadata")
	setName := toStringValue(metadata["name"])
	require.NotEmpty(t, setName, "the StatefulSet must be named for its claim names to be derivable")

	spec, ok := statefulSet["spec"].(map[string]interface{})
	require.True(t, ok, "kafka-statefulset.yaml must declare a spec")

	replicas, ok := spec["replicas"].(int)
	require.Truef(t, ok, "kafka-statefulset.yaml must declare an integer replica count")
	require.Positive(t, replicas, "a broker set with no replicas has no storage to claim")

	templates, ok := spec["volumeClaimTemplates"].([]interface{})
	require.Truef(t, ok && len(templates) == 1,
		"the broker must declare exactly one volumeClaimTemplate; a second one changes the claim "+
			"names this test derives and every claim in the PVC manifest would be orphaned")

	template, ok := templates[0].(map[string]interface{})
	require.True(t, ok, "the volumeClaimTemplate must be a mapping")
	templateMeta, ok := template["metadata"].(map[string]interface{})
	require.True(t, ok, "the volumeClaimTemplate must declare metadata")
	templateName := toStringValue(templateMeta["name"])
	require.NotEmpty(t, templateName, "the volumeClaimTemplate must be named")

	templateSpec, ok := template["spec"].(map[string]interface{})
	require.True(t, ok, "the volumeClaimTemplate must declare a spec")
	templateResources, _ := templateSpec["resources"].(map[string]interface{})
	templateRequests, _ := templateResources["requests"].(map[string]interface{})
	templateBytes := parseKubernetesQuantityBytes(t, toStringValue(templateRequests["storage"]))

	templateModes := stringListValue(templateSpec["accessModes"])
	require.NotEmpty(t, templateModes, "the volumeClaimTemplate must declare an access mode")

	claims := readYAMLDocuments(t, filepath.Join(
		moduleRootDir(t), "infrastructure", "k8s-manifests", "kafka-data-persistentvolumeclaim.yaml",
	))

	byName := make(map[string]map[string]interface{}, len(claims))
	for _, claim := range claims {
		assert.Equalf(t, "PersistentVolumeClaim", toStringValue(claim["kind"]),
			"kafka-data-persistentvolumeclaim.yaml must hold only claims")

		claimMeta, isMap := claim["metadata"].(map[string]interface{})
		require.True(t, isMap, "every claim must declare metadata")
		byName[toStringValue(claimMeta["name"])] = claim
	}

	t.Run("one claim per broker, named as the StatefulSet will look for it", func(t *testing.T) {
		expected := make([]string, 0, replicas)
		for ordinal := 0; ordinal < replicas; ordinal++ {
			expected = append(expected, fmt.Sprintf("%s-%s-%d", templateName, setName, ordinal))
		}

		actual := make([]string, 0, len(byName))
		for name := range byName {
			actual = append(actual, name)
		}
		sort.Strings(actual)
		sort.Strings(expected)

		assert.Equalf(t, expected, actual,
			"the claims in kafka-data-persistentvolumeclaim.yaml must be exactly %v, because that "+
				"is the <template>-<set>-<ordinal> form the StatefulSet resolves. A claim under any "+
				"other name is never mounted — it provisions a volume nothing uses, which is the "+
				"defect that had this manifest deleted rather than fixed. A MISSING ordinal is the "+
				"mirror image: that broker's claim comes from the template while its siblings come "+
				"from this file, so the two can differ in size or class with nothing reporting it",
			expected)
	})

	t.Run("no bare singleton claim, which cannot serve three brokers", func(t *testing.T) {
		_, present := byName[templateName]
		assert.Falsef(t, present,
			"a claim named exactly %q must NOT exist. It is the shape this file held before it was "+
				"deleted: one ReadWriteOnce claim is one volume mountable by one node, so three "+
				"brokers either fail to schedule apart or write concurrently into one KRaft "+
				"metadata log — and corrupting the metadata log is not an error the broker reports, "+
				"it is a cluster that will not elect a controller after the next restart",
			templateName)
	})

	t.Run("every claim requests what the template requests", func(t *testing.T) {
		for name, claim := range byName {
			claimSpec, isMap := claim["spec"].(map[string]interface{})
			require.Truef(t, isMap, "%s must declare a spec", name)

			resources, _ := claimSpec["resources"].(map[string]interface{})
			requests, _ := resources["requests"].(map[string]interface{})
			claimBytes := parseKubernetesQuantityBytes(t, toStringValue(requests["storage"]))

			assert.Equalf(t, templateBytes, claimBytes,
				"%s requests %d bytes against the volumeClaimTemplate's %d. Kubernetes ADOPTS a "+
					"pre-existing claim rather than reconciling it, so the smaller number wins "+
					"silently and the broker runs with less disk than log.retention.bytes was sized "+
					"against — which surfaces as a broker wedged on a full volume, not as a "+
					"manifest error",
				name, claimBytes, templateBytes)

			assert.Equalf(t, templateModes, stringListValue(claimSpec["accessModes"]),
				"%s declares access modes the template does not. The scheduler places the pod using "+
					"the CLAIM's modes, so a mismatch here is a pod that cannot be placed where the "+
					"workload assumed it could", name)

			assert.Equalf(t, toStringValue(templateSpec["storageClassName"]),
				toStringValue(claimSpec["storageClassName"]),
				"%s and the volumeClaimTemplate must name the same storage class (both omit it "+
					"today, taking the cluster default). Naming it on one side only means the "+
					"adopted volume is backed by storage the workload never asked for — different "+
					"IOPS, different durability, possibly a class that cannot expand", name)

			claimMeta, _ := claim["metadata"].(map[string]interface{})
			assert.Equalf(t, "blnk", toStringValue(claimMeta["namespace"]),
				"%s must live in the blnk namespace alongside the StatefulSet. A claim in another "+
					"namespace is invisible to the set, so the set creates its own and this one "+
					"provisions a volume nothing mounts", name)
		}
	})
}

// TestManifests_KafkaVolumeHoldsTheTopicGeometryItIsSizedFor pins the broker volume to the
// partition count the application actually creates (DOC-m02).
//
// # Why the arithmetic is asserted and the prose is asserted with it
//
// The volume size, `log.retention.bytes`, the topic count and the partition count are one
// derivation split across three files: the category list lives in Go, KAFKA_MIN_PARTITIONS
// in the ConfigMap, and the retention bytes and volume size in the StatefulSet. Nothing
// links them, so the manifests once documented a cluster of 10 topics and 60 partitions on
// a 200Gi volume while shipping 8 topics and 48 on 160Gi. The VALUE was right and the
// justification was wrong, which is the durable kind of wrong: an operator sizing the next
// change reads the prose, and every number in it was off.
//
// The first subtest derives the geometry from the code and asserts the volume holds it. The
// second asserts the manifests' own prose states the derived numbers — unusual for a test,
// and correct here, because a stale explanation beside a correct value is precisely the
// defect and there is no other mechanism that would ever notice it.
func TestManifests_KafkaVolumeHoldsTheTopicGeometryItIsSizedFor(t *testing.T) {
	categories := model.AllEventCategories()
	require.NotEmpty(t, categories, "the event catalogue must resolve at least one category")

	// Each category ships a main topic and a .dlt sibling — EnsureTopics creates both, and
	// both consume partitions on every broker.
	topics := 2 * len(categories)

	data := manifestConfigMapData(t)
	partitionsPerTopic := manifestIntSetting(t, data, "KAFKA_MIN_PARTITIONS")
	replicationFactor := manifestIntSetting(t, data, "KAFKA_REPLICATION_FACTOR")
	require.Positive(t, partitionsPerTopic, "KAFKA_MIN_PARTITIONS must be positive")
	require.Positive(t, replicationFactor, "KAFKA_REPLICATION_FACTOR must be positive")

	statefulSet := kafkaStatefulSet(t)
	spec, ok := statefulSet["spec"].(map[string]interface{})
	require.True(t, ok, "kafka-statefulset.yaml must declare a spec")
	brokers, ok := spec["replicas"].(int)
	require.Truef(t, ok && brokers > 0, "the broker set must declare a positive replica count")

	require.LessOrEqualf(t, replicationFactor, brokers,
		"KAFKA_REPLICATION_FACTOR is %d against %d brokers. Topic creation FAILS outright when the "+
			"factor exceeds the broker count, so the relay comes up against a cluster with no topics",
		replicationFactor, brokers)

	// Every partition is stored `replicationFactor` times across `brokers` brokers, so each
	// broker holds this share of the total. With RF == brokers that is every partition.
	partitionsPerBroker := topics * partitionsPerTopic * replicationFactor / brokers

	retentionBytes := kafkaBrokerRetentionBytes(t)
	volumeBytes := kafkaDataVolumeBytes(t)

	t.Run("retention across every partition fits the volume with headroom", func(t *testing.T) {
		logBytes := int64(partitionsPerBroker) * retentionBytes

		assert.Lessf(t, logBytes, volumeBytes,
			"%d partitions per broker x %d bytes of log.retention.bytes = %d bytes of log data "+
				"against a %d byte volume. log.retention.bytes is PER PARTITION, which is the "+
				"subtlety: a broker enforces it partition by partition and never consults the "+
				"volume, so it fills the disk and wedges rather than trimming further",
			partitionsPerBroker, retentionBytes, logBytes, volumeBytes)

		// Segments, indexes, the metadata log and in-flight compaction all live on the same
		// volume, and a broker that reaches 100% does not degrade, it stops.
		utilisation := float64(logBytes) / float64(volumeBytes)
		assert.LessOrEqualf(t, utilisation, 0.75,
			"retained log data would occupy %.0f%% of the broker volume. Beyond about three "+
				"quarters there is no room for the segment churn, offset indexes and metadata log "+
				"that share the disk, and a full disk takes the broker down rather than degrading it",
			utilisation*100)
		assert.GreaterOrEqualf(t, utilisation, 0.25,
			"retained log data would occupy only %.0f%% of the broker volume, so most of it is "+
				"provisioned and paid for and unusable. Either log.retention.bytes is smaller than "+
				"intended or the volume was sized for a geometry that has since shrunk",
			utilisation*100)
	})

	t.Run("the manifests explain the geometry they actually ship", func(t *testing.T) {
		gibibyte := int64(1) << 30
		require.Zerof(t, volumeBytes%gibibyte,
			"this assertion reads the volume size back as whole GiB; %d bytes is not a whole "+
				"number of them", volumeBytes)

		partitionPhrase := fmt.Sprintf("%d partitions", partitionsPerBroker)
		volumePhrase := fmt.Sprintf("%dGi", volumeBytes/gibibyte)
		topicPhrase := fmt.Sprintf("%d topics", topics)

		for _, manifest := range []string{
			filepath.Join("infrastructure", "k8s-manifests", "kafka-statefulset.yaml"),
			filepath.Join("infrastructure", "k8s-manifests", "prometheus-configmap.yaml"),
		} {
			contents := readRepoFile(t, manifest)

			assert.Containsf(t, contents, partitionPhrase,
				"%s must state the real per-broker partition count (%s). It once documented 60 "+
					"against the 48 it shipped, and an operator sizing the next volume from that "+
					"prose would provision for a cluster that does not exist",
				manifest, partitionPhrase)
			assert.Containsf(t, contents, volumePhrase,
				"%s must state the real volume size (%s); it once said 200Gi against the 160Gi it "+
					"shipped", manifest, volumePhrase)
		}

		assert.Containsf(t, readRepoFile(t,
			filepath.Join("infrastructure", "k8s-manifests", "kafka-statefulset.yaml")),
			topicPhrase,
			"kafka-statefulset.yaml must state the real topic count (%s). It is derived from the "+
				"event categories in model/event.go, so adding a category changes it — and the "+
				"partition count and the volume with it", topicPhrase)
	})
}

// kafkaBrokerRetentionBytes extracts log.retention.bytes from the broker's rendered
// server.properties in kafka-statefulset.yaml.
//
// The setting lives inside a shell heredoc rather than in a structured field, so it is read
// as text — the same approach manifestBrokerRetentionHours takes for log.retention.hours.
func kafkaBrokerRetentionBytes(t *testing.T) int64 {
	t.Helper()

	contents := readRepoFile(t, filepath.Join(
		"infrastructure", "k8s-manifests", "kafka-statefulset.yaml",
	))

	matches := regexp.MustCompile(`(?m)^\s*log\.retention\.bytes=(-?\d+)\s*$`).
		FindStringSubmatch(contents)
	require.Lenf(t, matches, 2,
		"kafka-statefulset.yaml must declare log.retention.bytes exactly once in the rendered "+
			"server.properties; the volume can only be sized against a stated per-partition cap")

	value, err := strconv.ParseInt(matches[1], 10, 64)
	require.NoError(t, err, "log.retention.bytes must be an integer")
	require.Positivef(t, value,
		"log.retention.bytes is %d. Kafka reads -1 as UNLIMITED, which means the broker never "+
			"trims by size and the volume arithmetic below is vacuous — the disk fills instead",
		value)

	return value
}

// kafkaDataVolumeBytes returns the per-broker volume size from the StatefulSet's
// volumeClaimTemplate.
func kafkaDataVolumeBytes(t *testing.T) int64 {
	t.Helper()

	spec, ok := kafkaStatefulSet(t)["spec"].(map[string]interface{})
	require.True(t, ok, "kafka-statefulset.yaml must declare a spec")

	templates, ok := spec["volumeClaimTemplates"].([]interface{})
	require.Truef(t, ok && len(templates) > 0,
		"kafka-statefulset.yaml must declare a volumeClaimTemplate")

	template, ok := templates[0].(map[string]interface{})
	require.True(t, ok, "the volumeClaimTemplate must be a mapping")
	templateSpec, _ := template["spec"].(map[string]interface{})
	resources, _ := templateSpec["resources"].(map[string]interface{})
	requests, _ := resources["requests"].(map[string]interface{})

	return parseKubernetesQuantityBytes(t, toStringValue(requests["storage"]))
}

// stringListValue renders a YAML sequence as a string slice, so two sequences can be
// compared without caring whether the decoder produced strings or interfaces.
func stringListValue(value interface{}) []string {
	entries, ok := value.([]interface{})
	if !ok {
		return nil
	}

	rendered := make([]string, 0, len(entries))
	for _, entry := range entries {
		rendered = append(rendered, toStringValue(entry))
	}

	return rendered
}

// manifestConfigMapData returns the ConfigMap's data map from blnk-config.yaml.
func manifestConfigMapData(t *testing.T) map[string]interface{} {
	t.Helper()

	configMap := readYAMLFile(t, filepath.Join(
		moduleRootDir(t), "infrastructure", "k8s-manifests", "blnk-config.yaml",
	))
	data, ok := configMap["data"].(map[string]interface{})
	require.True(t, ok, "blnk-config.yaml must declare a data map")

	return data
}

// manifestIntSetting reads one ConfigMap setting as an integer.
//
// ConfigMap values are strings by definition, so the quoting is part of the contract: an
// unquoted number in a ConfigMap is a manifest that will not apply.
func manifestIntSetting(t *testing.T, data map[string]interface{}, key string) int {
	t.Helper()

	raw, present := data[key]
	require.Truef(t, present, "blnk-config.yaml must declare %s", key)

	text := toStringValue(raw)
	value, err := strconv.Atoi(strings.TrimSpace(text))
	require.NoErrorf(t, err, "blnk-config.yaml %s must be an integer, got %q", key, text)

	return value
}

// manifestRetentionDays reads RELAY_EVENT_RETENTION_DAYS from the ConfigMap.
func manifestRetentionDays(t *testing.T) int {
	t.Helper()

	return manifestIntSetting(t, manifestConfigMapData(t), "RELAY_EVENT_RETENTION_DAYS")
}

// manifestBrokerRetentionHours extracts log.retention.hours from the broker's rendered
// server.properties in kafka-statefulset.yaml.
//
// It is read out of the manifest text rather than from a structured field because that is where
// it lives — the property block is a shell heredoc inside an init container — and reading it
// from anywhere else would let the two drift.
func manifestBrokerRetentionHours(t *testing.T) int {
	t.Helper()

	manifest := readRepoFile(t, filepath.Join("infrastructure", "k8s-manifests", "kafka-statefulset.yaml"))

	matches := regexp.MustCompile(`(?m)^\s*log\.retention\.hours=(\d+)\s*$`).FindStringSubmatch(manifest)
	require.Lenf(t, matches, 2,
		"kafka-statefulset.yaml must set log.retention.hours exactly once; the outbox retention "+
			"period is bounded by it")

	hours, err := strconv.Atoi(matches[1])
	require.NoError(t, err, "log.retention.hours must be an integer")

	return hours
}

// manifestPostgresVolumeBytes returns the pg-data volumeClaimTemplate request in bytes.
func manifestPostgresVolumeBytes(t *testing.T) int64 {
	t.Helper()

	document := readYAMLFile(t, filepath.Join(
		moduleRootDir(t), "infrastructure", "k8s-manifests", "postgres-statefulset.yaml",
	))

	spec, ok := document["spec"].(map[string]interface{})
	require.True(t, ok, "postgres-statefulset.yaml must declare a spec")

	templates, ok := spec["volumeClaimTemplates"].([]interface{})
	require.NotEmpty(t, templates, "postgres-statefulset.yaml must declare a volumeClaimTemplate")
	require.True(t, ok, "volumeClaimTemplates must be a list")

	for _, entry := range templates {
		template, isMap := entry.(map[string]interface{})
		if !isMap {
			continue
		}

		metadata, _ := template["metadata"].(map[string]interface{})
		if toStringValue(metadata["name"]) != "pg-data" {
			continue
		}

		templateSpec, _ := template["spec"].(map[string]interface{})
		resources, _ := templateSpec["resources"].(map[string]interface{})
		requests, _ := resources["requests"].(map[string]interface{})

		return parseKubernetesQuantityBytes(t, toStringValue(requests["storage"]))
	}

	require.FailNow(t, "postgres-statefulset.yaml must declare a volumeClaimTemplate named pg-data")

	return 0
}

// parseKubernetesQuantityBytes converts a Kubernetes storage quantity to bytes.
//
// Only the binary suffixes are handled, and an unrecognised one FAILS rather than being read as
// bytes: silently treating "600G" as 600 bytes would make the capacity assertions above pass on a
// manifest that provisions nothing, which is the opposite of what they are for.
func parseKubernetesQuantityBytes(t *testing.T, quantity string) int64 {
	t.Helper()

	trimmed := strings.TrimSpace(quantity)
	require.NotEmpty(t, trimmed, "a storage request must name a quantity")

	multipliers := []struct {
		suffix string
		scale  int64
	}{
		{"Ti", 1 << 40},
		{"Gi", 1 << 30},
		{"Mi", 1 << 20},
		{"Ki", 1 << 10},
	}

	for _, unit := range multipliers {
		if !strings.HasSuffix(trimmed, unit.suffix) {
			continue
		}

		value, err := strconv.ParseInt(strings.TrimSuffix(trimmed, unit.suffix), 10, 64)
		require.NoErrorf(t, err, "storage quantity %q must be an integer with a binary suffix", quantity)

		return value * unit.scale
	}

	require.FailNowf(t, "unrecognised storage quantity",
		"%q must use a binary suffix (Ki, Mi, Gi, Ti); reading it as bytes would let the capacity "+
			"assertions pass on a volume that provisions nothing", quantity)

	return 0
}

// ---------------------------------------------------------------------------
// The relay target delegates broker resolution to the application
// ---------------------------------------------------------------------------

// makefilePath is the target file operators invoke the server role through.
const makefilePath = "makefile"

// relayTargetName is the target that starts the role hosting the event outbox relay.
const relayTargetName = "run_server_relay"

// TestMakeRelayTarget_DelegatesBrokerResolutionToTheApplication pins the boundary between what
// make may decide and what only the application can.
//
// # What make legitimately does
//
// It sources .env with the caller's environment replayed on top, because `./stack.sh --init`
// writes KAFKA_BROKERS into a mode-0600 .env whose assignments are exported into nobody's shell —
// so a target that read only the process environment refused the operator who had just run the
// setup instruction it recommends. That layering is make's business and stays.
//
// # What it must NOT do
//
// Decide whether brokers are configured. The recipe used to resolve the list itself, and a shell
// reimplementation of the loader's resolution cannot agree with it — it did not, in three ways,
// every one of which made the target ANNOUNCE a relay that would not start:
//
//	it took the first non-empty of KAFKA_BROKERS, BLNK_KAFKA_KAFKA_BROKERS and BLNK_KAFKA_BROKERS
//	in that order, while the loader ranks them the other way at the top — BLNK_KAFKA_BROKERS wins,
//	then KAFKA_BROKERS, then the derived key, then the file (config.eventStreamingEnvOverride);
//
//	it tested the config file with `grep '"brokers"'`, which reads `"brokers": []` as configured;
//
//	and no first-non-empty scan can express that an explicitly EMPTY higher-precedence name CLEARS
//	what a lower one supplied — `BLNK_KAFKA_BROKERS= KAFKA_BROKERS=host:9092` is no brokers to the
//	application and a configured deployment to the scan.
//
// The replacement is `--require-kafka`, which asks the question after the same load, of the same
// struct, with the same predicate startEventRelay gates on. This test asserts the shell
// resolution is GONE rather than merely that the flag is present, because a target that did both
// would still be able to refuse a deployment the application would have admitted.
func TestMakeRelayTarget_DelegatesBrokerResolutionToTheApplication(t *testing.T) {
	makefile := readRepoFile(t, makefilePath)

	start := strings.Index(makefile, "\n"+relayTargetName+":\n")
	require.Positivef(t, start, "%s must define the %s target", makefilePath, relayTargetName)

	// The recipe runs to the first line that is neither blank nor tab-indented — the next target
	// or the next comment block.
	recipeLines := make([]string, 0, 32)
	for _, line := range strings.Split(makefile[start+len(relayTargetName)+2:], "\n") {
		if line != "" && !strings.HasPrefix(line, "\t") {
			break
		}

		recipeLines = append(recipeLines, line)
	}
	recipe := strings.Join(recipeLines, "\n")
	require.NotEmptyf(t, strings.TrimSpace(recipe), "%s: the %s recipe must not be empty",
		makefilePath, relayTargetName)

	// Comments cannot appear inside a recipe's shell continuation, so nothing is stripped here:
	// every line asserted about below is executed.
	assert.Truef(t, strings.Contains(recipe, `exec ./${PROJECT} start --config "${CONFIG_FILE}" --require-kafka`),
		"%s: the %s recipe must exec the server with --require-kafka, so the APPLICATION decides "+
			"whether brokers are configured", makefilePath, relayTargetName)

	// THE SHELL RESOLUTION, in every form it took. Each of these RANKS or TESTS the sources —
	// picks a winner, reads a value, or inspects the config file — and ranking is the application's
	// job, done after the same load, of the same struct, with the same predicate.
	//
	// What is forbidden is deciding, not mentioning. Scoping .env's defaults by SET-NESS (below)
	// names the aliases without ordering them: it removes contributions the caller did not ask
	// for and then lets the loader rank whatever is left, so it cannot disagree with the loader
	// about which one wins — it makes no claim about that at all. Announcing a winner is the form
	// that can disagree, and the announcement assertion further down is what forbids it.
	for _, resolution := range []string{
		`grep -q '"brokers"'`,
		"caller_brokers",
		`for candidate in`,
		"BROKERS_SOURCE",
		"winning_alias",
	} {
		assert.Falsef(t, strings.Contains(recipe, resolution),
			"%s: the %s recipe must not resolve the broker list itself (found %q). A shell "+
				"approximation of the loader's precedence cannot agree with it, and the failure "+
				"mode is announcing a relay that never starts",
			makefilePath, relayTargetName, resolution)
	}

	// AND IT MUST NOT NAME A SOURCE IN ITS OUTPUT. This is the assertion that actually closes the
	// finding: the recipe announced "brokers from KAFKA_BROKERS" while the application resolved
	// BLNK_KAFKA_BROKERS, so an operator was told about a deployment other than the one that came
	// up. The application's own refusal names every source it read, which is the only description
	// that cannot drift from the resolution.
	for _, line := range strings.Split(recipe, "\n") {
		if !strings.Contains(line, "echo") {
			continue
		}

		for _, alias := range relayAliases {
			assert.Falsef(t, strings.Contains(line, "("+alias+")"),
				"%s: the %s recipe announces %s as the source it resolved from. Only the loader "+
					"knows which alias wins, so an announcement made here can name one the "+
					"application did not use: %s", makefilePath, relayTargetName, alias,
				strings.TrimSpace(line))
		}
	}

	// THE SET-NESS SCOPING STAYS, because it fixes the defect the layering alone leaves open. With
	// .env sourced under the caller's environment, an alias present only in .env survives the
	// replay — so `.env` declaring BLNK_KAFKA_BROKERS outranks a caller who ran
	// `KAFKA_BROKERS=host:9092 make run_relay`, and a DEFAULT file outvotes an explicit override.
	// A caller who named any alias therefore suppresses the ones they did not name. Note what this
	// does NOT do: it never chooses between the caller's own aliases, which is the loader's to
	// decide and is left entirely to it.
	for _, scoping := range []string{
		`prefixed_set="$${BLNK_KAFKA_BROKERS+set}"`,
		`bare_set="$${KAFKA_BROKERS+set}"`,
		`derived_set="$${BLNK_KAFKA_KAFKA_BROKERS+set}"`,
		`if [ -n "$$caller_decided" ]; then`,
	} {
		assert.Truef(t, strings.Contains(recipe, scoping),
			"%s: the %s recipe must scope .env's defaults to the aliases the caller did NOT name "+
				"(%q missing), or a value parked in .env outvotes an explicit override on the "+
				"command line", makefilePath, relayTargetName, scoping)
	}

	// And the .env layering stays, because it is the part make is actually for.
	for _, layering := range []string{
		`caller_environment="$$(export -p)"`,
		`if [ -f .env ]; then set -a; . ./.env; set +a; fi`,
		`eval "$$caller_environment"`,
	} {
		assert.Truef(t, strings.Contains(recipe, layering),
			"%s: the %s recipe must keep sourcing .env with the caller's environment replayed on "+
				"top (%q missing): stack.sh --init writes the brokers there, and a set-but-empty "+
				"value from the caller must survive the replay so the application sees it and "+
				"refuses", makefilePath, relayTargetName, layering)
	}

	// The exec must be in the SAME shell as the sourcing. Each LINE of a recipe is its own shell,
	// so an exec standing on its own would run with make's environment and see none of .env — the
	// application would then resolve no brokers and refuse a correctly configured deployment,
	// which is the same silent-failure shape from the other direction.
	//
	// A continuation line is also tab-indented, so what distinguishes the two is whether the
	// PRECEDING line ends in a backslash. That is what is asserted.
	execAt := strings.Index(recipe, "exec ./${PROJECT}")
	require.Positivef(t, execAt, "%s: the %s recipe must exec the server", makefilePath, relayTargetName)
	assert.Truef(t, strings.HasSuffix(recipe[:execAt], "\\\n\t"),
		"%s: the exec must be a CONTINUATION of the shell that sourced .env — the line before it "+
			"must end in a backslash. On a line of its own it gets make's environment and sees "+
			"nothing that was sourced", makefilePath)
}

// TestStackScript_WritesSecretsIntoEnvWithoutPuttingThemInArgv pins the mechanism by which
// --init fills .env, because the mechanism was the defect.
//
// stack.sh generates and writes three credentials: the PostgreSQL password, the Kafka
// administrative password whose principal is in the broker's super.users, and the producer
// password. Each was written with `sed -i "s|{KEY}|${value}|g" .env`, which is wrong in three
// independent ways, and this test pins the fix for each.
//
//  1. SECRECY. `sed` is a real process, and on Linux /proc/<pid>/cmdline is mode 444 — readable
//     by every account on the host — while /proc/<pid>/environ is mode 400, readable only by the
//     owner. The old form published all three credentials to any local user who ran `ps` during
//     the fraction of a second sed lived. It did so inside a function whose own comment promises
//     that values are reported "by KEY NAME and never by value".
//
//  2. CORRECTNESS. A value reaching a sed replacement is not literal: `&` means the whole match
//     and `\1` a backreference. A generated secret containing either was written altered, and the
//     service then failed to authenticate with a credential nobody had mistyped.
//
//  3. PORTABILITY. `sed -i` with no suffix is a GNU extension. BSD and macOS sed read the next
//     argument as a backup suffix, so on those platforms the form consumed the filename as a
//     suffix and edited nothing.
//
// The replacement is write_env_substitution: the value crosses into awk through ENVIRON, the
// substitution is a literal index()/substr() splice, and the file is replaced by an atomic
// rename of a temporary file created under umask 077.
func TestStackScript_WritesSecretsIntoEnvWithoutPuttingThemInArgv(t *testing.T) {
	stack := readRepoFile(t, "stack.sh")

	// NO EXECUTABLE `sed -i` ANYWHERE. Comments explaining the removal are expected and are
	// excluded by requiring the line not to be a comment, so the prose that documents this fix
	// does not defeat the assertion that enforces it.
	for i, line := range strings.Split(stack, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || trimmed == "" {
			continue
		}
		assert.NotContainsf(t, trimmed, "sed -i",
			"stack.sh:%d: `sed -i` is both a GNU-only in-place form and, when a secret is "+
				"interpolated into it, a disclosure of that secret through world-readable argv. "+
				"Use write_env_substitution: %s", i+1, trimmed)
	}

	require.Contains(t, stack, "write_env_substitution() {",
		"stack.sh must define write_env_substitution — the argv-free, literal, portable writer "+
			"that replaced sed -i. Removing sed -i without providing a replacement would mean "+
			"--init silently writes nothing")

	// THE VALUE CROSSES ON THE ENVIRONMENT, NOT ARGV.
	assert.Contains(t, stack, `SEV_MODE="${mode}" SEV_KEY="${key}" SEV_VALUE="${value}" awk '`,
		"write_env_substitution must pass the value to awk through the environment. `awk -v "+
			"value=...` would put it back in argv, which is the whole defect")
	assert.NotContains(t, stack, `awk -v value=`,
		"awk -v places the value in argv, which is mode 444 through /proc")

	// THE SUBSTITUTION IS A LITERAL SPLICE, not a regex substitution, so no character in a
	// generated secret carries a meaning.
	assert.Contains(t, stack, "while ((at = index(line, ph)) > 0)",
		"the placeholder replacement must splice with index()/substr() so that a secret "+
			"containing & or a backslash is written verbatim")
	for _, forbidden := range []string{"gsub(", "sub("} {
		assert.NotContainsf(t, stack, forbidden,
			"awk's %s treats & in the replacement as the matched text, which silently alters a "+
				"generated secret", forbidden)
	}

	// THE REPLACEMENT IS ATOMIC AND PRIVATE.
	assert.Contains(t, stack, `tmp="$( umask 077; mktemp "${env}.XXXXXX" )"`,
		"the temporary file must be created beside ${env} so the rename is atomic, and under "+
			"umask 077 so the secrets are never world-readable in the temporary copy")
	assert.Contains(t, stack, `mv -f "${tmp}" "${env}"`,
		"the file must be replaced by a rename, which is atomic: a concurrent reader sees the "+
			"old content or the new, never a partial write")

	// EVERY SECRET GOES THROUGH THE ONE WRITER. The PostgreSQL password used to bypass
	// set_env_value with its own bare sed -i, so the fix applied to the shared writer did not
	// reach it.
	assert.Contains(t, stack, `set_env_value "POSTGRES_PASSWORD" "$POSTGRES_PASSWORD"`,
		"the PostgreSQL password must be written through set_env_value like every other "+
			"credential, so there is one place where the writing mechanism is correct")

	// THE KEY IS VALIDATED BEFORE A BRANCH IS CHOSEN, which is what covers the append arm. A
	// check living only inside write_env_substitution left `set_env_value` free to append a
	// malformed key — including one containing a newline, which would inject whole lines into a
	// file full of credentials — and to report it as a success.
	require.Contains(t, stack, "require_env_key_name() {",
		"stack.sh must define a key-name check")
	setEnvStart := strings.Index(stack, "set_env_value() {")
	require.Greater(t, setEnvStart, 0, "set_env_value must exist")
	setEnvEnd := strings.Index(stack[setEnvStart:], "\n}\n")
	require.Greater(t, setEnvEnd, 0, "set_env_value must be a closed function")
	setEnv := stack[setEnvStart : setEnvStart+setEnvEnd]
	keyCheck := strings.Index(setEnv, `require_env_key_name "${key}" || return 1`)
	firstBranch := strings.Index(setEnv, `if grep -qF "{${key}}"`)
	require.Greaterf(t, keyCheck, 0,
		"set_env_value must validate the key name; without it the append branch writes an "+
			"unvalidated key straight into .env")
	require.Greater(t, firstBranch, 0, "set_env_value must branch on the placeholder form")
	assert.Lessf(t, keyCheck, firstBranch,
		"the key must be validated BEFORE the branch is chosen, so the append arm is covered too")
}

// TestStackScript_ReadsFileModesPortably pins the loss-free removal of a dead security control.
//
// Two implementations of one control stood in this script: enforce_env_permissions, which is
// called from three places, and require_private_env_file, which was defined and never called.
// Two competing copies is worse than one, because a reader cannot tell which is authoritative and
// a fix applied to the wrong copy looks applied while changing nothing.
//
// The dead one was deleted — but it had one genuine advantage over the live one: it tried BSD's
// `stat -f '%Lp'` as well as GNU's `stat -c '%a'`. Without that, enforce_env_permissions could
// not read a mode on macOS, fell into its unverifiable branch on every call, and re-chmod'd a
// file that was already correct while announcing a permission change that had not happened.
// So the advantage was carried across first. This test pins both halves: the duplicate is gone,
// and what made it worth keeping is not.
func TestStackScript_ReadsFileModesPortably(t *testing.T) {
	stack := readRepoFile(t, "stack.sh")

	assert.NotContains(t, stack, "require_private_env_file() {",
		"require_private_env_file was dead code duplicating enforce_env_permissions and must "+
			"stay deleted; two implementations of one security control is worse than one")

	require.Contains(t, stack, "enforce_env_permissions() {",
		"the surviving permission control must still exist")
	assert.Contains(t, stack,
		`stat -c '%a' "${env}" 2>/dev/null || stat -f '%Lp' "${env}" 2>/dev/null`,
		"enforce_env_permissions must try both stat spellings. GNU coreutils uses -c '%a' and "+
			"BSD/macOS uses -f '%Lp'; trying only the first makes every macOS invocation report "+
			"an unverifiable mode and re-chmod a file that was already correct")
}

// copyIntoSandbox copies one repository file into the sandbox at a given mode.
func copyIntoSandbox(t *testing.T, from, to string, mode os.FileMode) {
	t.Helper()

	content, err := os.ReadFile(from) //nolint:gosec // a fixed path inside the repository
	require.NoErrorf(t, err, "reading %s for the sandbox", from)
	require.NoErrorf(t, os.WriteFile(to, content, mode), "writing %s into the sandbox", to)
}

// runStackScript executes the real stack.sh in a disposable sandbox with every external
// command it can reach replaced by a recording stub, and returns what happened.
//
// # Why the script is EXECUTED rather than read
//
// The property under test is a control-flow OUTCOME — which case arm a word lands on, and
// what exit status that arm leaves — and no amount of text matching can establish it. The
// arm being guarded here used to read `* ) help`, so `./stack.sh --buld` printed the usage
// banner and exited 0: nothing was started, nothing was torn down, and every caller was told
// it had worked. A grep for the word `exit` in that file finds thirty of them and says
// nothing about whether a typo reaches one.
//
// # Why it is safe to run
//
// The sandbox is a temporary directory holding a copy of stack.sh, a copy of .env.example
// and a copy of the compose projection, so every relative path the script resolves —
// `.env`, `.env.example`, `scripts/kafka-provision.sh`, `${COMPOSE_FILE}` — lands inside it
// and never in the repository. The environment is built from scratch rather than inherited,
// so an ambient .env, a developer's KAFKA_BROKERS or an exported COMPOSE_CL cannot change
// what is being tested. PATH puts the stub directory first, and the only names the script
// reaches for that could touch a real host — `docker` and the provisioning script — are
// both stubs that record their arguments and do nothing.
//
// The stubs are deliberately PERMISSIVE: docker exits 0 for everything and answers the
// version probe with a Compose new enough to satisfy COMPOSE_MINIMUM_VERSION. That matters,
// because a stub that failed would let a dispatch test pass for the wrong reason — the run
// would abort before reaching the arm under test, and `exit 1` would look like the
// unknown-argument refusal.
//
// Parameters:
//   - t *testing.T: owns the sandbox's lifetime.
//   - argv []string: the arguments to invoke with. An empty slice is the bare invocation,
//     which is a case in its own right.
//
// Returns:
//   - stackDispatchOutcome: the exit status, the operator-visible output, the recorded
//     invocations and the sandbox to inspect for side effects.
func runStackScript(t *testing.T, argv []string) stackDispatchOutcome {
	t.Helper()

	root := moduleRootDir(t)
	sandbox := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(sandbox, "bin"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(sandbox, "scripts"), 0o750))

	// The real script, copied rather than symlinked: it resolves .env, .env.example and
	// scripts/kafka-provision.sh against the WORKING DIRECTORY, and a symlink would leave
	// those resolving into the repository.
	copyIntoSandbox(t, filepath.Join(root, "stack.sh"), filepath.Join(sandbox, "stack.sh"), 0o700)
	// .env.example and the compose projection are present so that a REGRESSION IS VISIBLE.
	// Without .env.example an accidental fall-through into the --init arm would fail for a
	// missing template instead of creating a .env, and "no .env was created" would hold for
	// the wrong reason.
	copyIntoSandbox(t, filepath.Join(root, ".env.example"), filepath.Join(sandbox, ".env.example"), 0o644)
	copyIntoSandbox(t,
		filepath.Join(root, "docker-compose.yaml"),
		filepath.Join(sandbox, "docker-compose.yaml"), 0o644)

	log := filepath.Join(sandbox, "invocations.log")
	require.NoError(t, os.WriteFile(log, nil, 0o600))

	recorder := "#!/usr/bin/env bash\n" +
		"printf '%s' \"$(basename \"$0\")\" >> " + shellQuote(log) + "\n" +
		"for arg in \"$@\"; do printf ' %s' \"$arg\" >> " + shellQuote(log) + "; done\n" +
		"printf '\\n' >> " + shellQuote(log) + "\n"

	// docker: records, answers the Compose version probe with a version above the floor, and
	// succeeds at everything else. `basename $0` is `docker`, so the recorded line reads as
	// the command an operator would have typed.
	dockerStub := recorder +
		"if [ \"${1:-}\" = compose ] && [ \"${2:-}\" = version ]\n" +
		"then\n" +
		"  if [ \"${3:-}\" = --short ]; then printf '2.29.0\\n'; else printf 'Docker Compose version v2.29.0\\n'; fi\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 0\n"
	require.NoError(t,
		os.WriteFile(filepath.Join(sandbox, "bin", "docker"), []byte(dockerStub), 0o700))

	// The provisioning script: records, and answers --print-interface-host with a plausible
	// variable list so resolve_kafka_provision_interface succeeds. Any OTHER invocation is a
	// side effect, and the assertions below say so.
	provisionStub := recorder +
		"if [ \"${1:-}\" = --print-interface-host ]\n" +
		"then\n" +
		"  printf 'KAFKA_TOPIC_PREFIX\\nKAFKA_MIN_PARTITIONS\\nKAFKA_REPLICATION_FACTOR\\n'\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 0\n"
	require.NoError(t,
		os.WriteFile(filepath.Join(sandbox, "scripts", "kafka-provision.sh"),
			[]byte(provisionStub), 0o700))

	command := exec.Command("bash", filepath.Join(sandbox, "stack.sh")) //nolint:gosec // a copy of a repository file in a temporary directory
	command.Args = append(command.Args, argv...)
	command.Dir = sandbox
	// Built from scratch rather than inherited. An ambient KAFKA_BROKERS, an exported
	// COMPOSE_CL or a COMPOSE_PROFILES from the developer's shell would each change which
	// branch the script takes, and the dispatcher's contract must hold without reference to
	// any of them.
	command.Env = []string{
		"PATH=" + filepath.Join(sandbox, "bin") +
			":/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME=" + filepath.Join(sandbox, "home"),
	}

	output, err := command.CombinedOutput()

	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		require.ErrorAsf(t, err, &exitErr,
			"stack.sh %v could not be run at all: %v\n%s", argv, err, output)
		exitCode = exitErr.ExitCode()
	}

	recorded, readErr := os.ReadFile(log) //nolint:gosec // a path this test wrote
	require.NoError(t, readErr, "the invocation log must be readable")

	invocations := make([]string, 0, 8)
	for _, line := range strings.Split(string(recorded), "\n") {
		if strings.TrimSpace(line) != "" {
			invocations = append(invocations, strings.TrimSpace(line))
		}
	}

	return stackDispatchOutcome{
		exitCode:    exitCode,
		output:      string(output),
		invocations: invocations,
		sandbox:     sandbox,
	}
}

// assertStackChangedNothing is the zero-side-effect half of the dispatch contract.
//
// "Nothing was started, stopped or changed" is a claim the script PRINTS, and a printed claim
// is worth exactly as much as the check behind it. Three things are asserted, each of which
// the sandbox is arranged to make observable:
//
//   - No Compose subcommand ran. The version probes are excluded because they are read-only
//     and happen before any arm is chosen; anything else — `up`, `down`, `pull`, `ps` — is an
//     action on the host.
//   - The provisioning script was not RUN, only interrogated. `--print-interface-host` prints
//     a variable list and exits; any other invocation creates topics, mints SCRAM credentials
//     and rewrites ACLs.
//   - No .env was created. The sandbox holds a .env.example precisely so that a fall-through
//     into the --init arm would produce one.
//
// TestStackScript_TheDispatchHarnessCanSeeSideEffects proves all three are observable, so a
// failure of this helper means the dispatcher acted rather than that the harness went blind.
func assertStackChangedNothing(t *testing.T, outcome stackDispatchOutcome) {
	t.Helper()

	assert.Emptyf(t, outcome.composeSubcommands(),
		"no Compose subcommand may run: the script printed that nothing was started, stopped "+
			"or changed, and a compose invocation is exactly how it would have been.\n"+
			"recorded: %v", outcome.invocations)

	for _, call := range outcome.provisioningInvocations() {
		assert.Equalf(t, "kafka-provision.sh --print-interface-host", call,
			"the provisioning script may only be INTERROGATED for its variable list here. Any "+
				"other invocation creates topics, mints SCRAM credentials and rewrites ACLs on "+
				"whatever broker the environment points at.\nrecorded: %v", outcome.invocations)
	}

	_, err := os.Stat(filepath.Join(outcome.sandbox, ".env"))
	assert.Truef(t, os.IsNotExist(err),
		"no .env may be created: the sandbox holds a .env.example, so an accidental "+
			"fall-through into the --init arm would generate one — with a PostgreSQL password "+
			"and two Kafka credentials in it")
}

// TestStackScript_TheDispatchHarnessCanSeeSideEffects is the non-vacuity guard for the two
// tests above, and it is not optional.
//
// Every assertion in assertStackChangedNothing is an assertion that something did NOT happen,
// and such an assertion passes just as readily against a harness that cannot see the thing at
// all — a stub that was never on PATH, a log that was never written, a sandbox the script
// never ran in. So the same harness is pointed at two subcommands whose side effects are not
// in question, and required to observe them:
//
//   - `--down` must reach Compose. It is one line in the dispatcher and touches nothing else.
//   - `--init` must create a .env. It is the one subcommand whose entire purpose is to write
//     that file.
//
// If either of these stops being observed, the "changed nothing" assertions above have become
// vacuous and this test is what says so.
func TestStackScript_TheDispatchHarnessCanSeeSideEffects(t *testing.T) {
	t.Run("a teardown reaches Compose", func(t *testing.T) {
		outcome := runStackScript(t, []string{"--down"})

		require.Equalf(t, 0, outcome.exitCode,
			"the stubbed teardown must succeed, or this guard is measuring an aborted run "+
				"instead of an observed side effect.\n--- output ---\n%s", outcome.output)

		acting := outcome.composeSubcommands()
		require.NotEmptyf(t, acting,
			"the harness must OBSERVE a compose subcommand for --down. It did not, which means "+
				"the docker stub was not reached — and every \"no compose subcommand ran\" "+
				"assertion in this file is therefore vacuous.\nrecorded: %v", outcome.invocations)

		joined := strings.Join(acting, "\n")
		assert.Contains(t, joined, " down",
			"the observed subcommand must be the teardown itself.\nrecorded: %v", acting)
	})

	t.Run("an initialisation creates the environment file", func(t *testing.T) {
		outcome := runStackScript(t, []string{"--init"})

		require.Equalf(t, 0, outcome.exitCode,
			"the stubbed --init must succeed.\n--- output ---\n%s", outcome.output)

		info, err := os.Stat(filepath.Join(outcome.sandbox, ".env"))
		require.NoErrorf(t, err,
			"the harness must OBSERVE the .env --init creates. It did not, which means the "+
				"\"no .env was created\" assertions elsewhere in this file are vacuous")

		// While the file is in hand: the mode is the one property of it that is a security
		// boundary rather than a convenience, and --init is the only thing that creates it.
		assert.Equalf(t, os.FileMode(0o600), info.Mode().Perm(),
			"a generated .env holds the PostgreSQL password and both Kafka credentials, so it "+
				"must be private from the first byte")
	})
}

// TestStackScript_TheUsageBannerSucceeds is the other half of MIN-12: the three invocations
// that ask for the banner must SUCCEED, and must equally change nothing.
//
// A bare invocation is read as a request for help, which is why the script's own comment
// calls it out: a wrapper that runs `./stack.sh --help` to check the script is present must
// not see a failure. Making the unknown-argument arm exit 1 is only correct if these three
// stay at 0 — otherwise the fix trades a silent success for a spurious failure, and the
// dispatcher would have to be read to know which.
func TestStackScript_TheUsageBannerSucceeds(t *testing.T) {
	requests := []struct {
		name string
		argv []string
	}{
		{name: "the long flag", argv: []string{"--help"}},
		{name: "the short flag", argv: []string{"-h"}},
		{name: "a bare invocation", argv: nil},
		{name: "an explicitly empty argument", argv: []string{""}},
	}

	// Every subcommand the banner advertises, so that a flag deleted from the dispatcher
	// without being deleted from the banner — or the reverse — is caught here rather than by
	// an operator following documentation into the unknown-argument arm.
	advertised := []string{
		"--pull, -p", "--up,-u", "--build,-b", "--down,-d",
		"--purge", "--restart,-r", "--init,-i",
	}

	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			outcome := runStackScript(t, request.argv)

			require.Equalf(t, 0, outcome.exitCode,
				"stack.sh %v must exit 0: printing what was asked for is not an error, and a "+
					"wrapper that runs it to check the script is present must not see a "+
					"failure.\n--- output ---\n%s", request.argv, outcome.output)

			assert.Contains(t, outcome.output, "Usage:", "the banner must be printed")
			for _, flag := range advertised {
				assert.Containsf(t, outcome.output, flag,
					"the usage banner must document %q; a subcommand the dispatcher accepts and "+
						"the banner omits is undiscoverable, and one the banner advertises and "+
						"the dispatcher rejects sends the operator into the refusal arm", flag)
			}

			// The banner must NOT claim a failure. This is the specific confusion the old
			// `* ) help` arm created in reverse: help and refusal printed the identical text.
			assert.NotContainsf(t, outcome.output, "unrecognised argument",
				"a deliberate request for help must not be reported as a mistake")

			assertStackChangedNothing(t, outcome)
		})
	}
}

// TestStackScript_AnUnrecognisedArgumentFailsAndChangesNothing is MIN-12, and it is the
// executable half of a fix that until now existed only as a case arm and a comment.
//
// # The defect
//
// The catch-all arm read `* ) help`, sharing the SUCCESS path with `--help`. So
// `./stack.sh --buld` printed the usage banner and exited 0. Nothing was pulled, built,
// started, stopped or purged, and the caller was told it had worked. A CI job or a
// provisioning wrapper that mistyped a subcommand recorded success against a stack that had
// never started — and every other failure in this script is loud: `--down` without an
// environment file exits 1, a broker that never becomes healthy exits non-zero, a purge
// without consent refuses. The typo was the sole silent one.
//
// # Why this test exists
//
// The fix is four lines of shell inside a `case`, which is exactly the kind of edit that a
// later refactor merges back into the arm above it without anyone noticing. The finding is
// blunt about the consequence: "a typo can regress to green/no-op". So the contract is
// pinned by running the script, on a sandbox where a side effect WOULD be observable, and
// asserting all three halves of it — the status, the diagnostic that names the argument, and
// the absence of any action.
//
// The typos are chosen to be the ones an operator actually makes: a transposed character, a
// missing pair of dashes, the wrong case, a single-letter flag that was never defined, and a
// value-looking word. Each one lands on the same arm, and each must fail.
func TestStackScript_AnUnrecognisedArgumentFailsAndChangesNothing(t *testing.T) {
	mistakes := []struct {
		name string
		argv []string
	}{
		{name: "a transposed character", argv: []string{"--buld"}},
		{name: "the dashes were forgotten", argv: []string{"up"}},
		{name: "the wrong case", argv: []string{"--UP"}},
		{name: "a short flag that was never defined", argv: []string{"-x"}},
		{name: "a subcommand from a different tool", argv: []string{"start"}},
		{name: "an argument that only looks like a flag", argv: []string{"---up"}},
	}

	for _, mistake := range mistakes {
		t.Run(mistake.name, func(t *testing.T) {
			outcome := runStackScript(t, mistake.argv)

			require.Equalf(t, 1, outcome.exitCode,
				"stack.sh %v must exit 1. This arm used to be `* ) help`, which shares the "+
					"success path with --help, so a mistyped subcommand printed the usage banner "+
					"and returned 0 — a CI job or provisioning wrapper then recorded SUCCESS "+
					"against a stack that had never started.\n--- output ---\n%s",
				mistake.argv, outcome.output)

			// The argument is named back, because the mistake is usually one transposed
			// character and an operator reading a wall of usage text does not always see
			// which word of theirs was not understood.
			assert.Containsf(t, outcome.output, mistake.argv[0],
				"the refusal must NAME the argument that was not understood; the usage banner "+
					"alone leaves the operator to spot their own typo in it")
			assert.Containsf(t, outcome.output, "unrecognised argument",
				"the refusal must say what went wrong in words, not only in an exit status")
			assert.Containsf(t, outcome.output, "Nothing was started, stopped or changed",
				"the refusal must state that the host is untouched: the operator's next question "+
					"after a non-zero exit is whether they now need to clean something up")

			// Usage is still printed. Refusing and then explaining is the point; refusing in
			// silence would be a regression of a different kind.
			assert.Containsf(t, outcome.output, "Usage:",
				"the refusal must still print the usage banner, so the operator can see the "+
					"subcommand they meant")

			assertStackChangedNothing(t, outcome)
		})
	}
}

// stackDispatchOutcome is everything one sanitized stack.sh run produced.
type stackDispatchOutcome struct {
	// exitCode is the status the script left, which for this dispatcher is the whole
	// contract: it is what a CI job or a provisioning wrapper reads.
	exitCode int
	// output is stdout and stderr combined, colour escapes included, exactly as an operator
	// sees it.
	output string
	// invocations is one line per stubbed external command, in the order they ran, so a
	// side effect is observable rather than inferred.
	invocations []string
	// sandbox is the working directory the run had, so a test can look for files it created.
	sandbox string
}

// composeSubcommands returns the recorded compose invocations that asked Compose to DO
// something, discarding the two version probes resolve_compose_cl makes before any
// subcommand runs.
//
// The distinction is the whole point of the dispatch tests. `docker compose version` is
// read-only and unavoidable — it is how the script decides whether Compose is usable at all
// — while `docker compose ... down` or `... up -d` changes the host. Filtering the probes
// here rather than in each caller keeps "nothing was started, stopped or changed" a single
// readable assertion.
func (outcome stackDispatchOutcome) composeSubcommands() []string {
	acting := make([]string, 0, len(outcome.invocations))

	for _, line := range outcome.invocations {
		if !strings.HasPrefix(line, "docker ") {
			continue
		}
		// The probe, in both of its forms: `docker compose version` and, when that
		// succeeds, `docker compose version --short`.
		if line == "docker compose version" || line == "docker compose version --short" {
			continue
		}

		acting = append(acting, line)
	}

	return acting
}

// provisioningInvocations returns the recorded calls to the provisioning script.
func (outcome stackDispatchOutcome) provisioningInvocations() []string {
	calls := make([]string, 0, len(outcome.invocations))

	for _, line := range outcome.invocations {
		if strings.HasPrefix(line, "kafka-provision.sh") {
			calls = append(calls, line)
		}
	}

	return calls
}
