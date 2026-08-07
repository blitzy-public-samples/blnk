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
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	// runs. A variable left out of the pass-through is silently replaced by the script's own
	// default — here, "not configured" — so the producer would be skipped with no diagnosis.
	passthrough := script[strings.Index(script, "local passthrough=("):]
	passthrough = passthrough[:strings.Index(passthrough, "\n    )")]

	for _, variable := range []string{
		"KAFKA_SASL_USER",
		"KAFKA_SASL_SECRET",
		"KAFKA_SKIP_PRODUCER_PRINCIPAL",
		"KAFKA_ROTATE_PRODUCER_SECRET",
	} {
		assert.Containsf(t, passthrough, variable,
			"%s must be forwarded when this script delegates into the broker container, or the "+
				"compose one-shot silently falls back to this script's own default", variable)
	}
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
