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

package blnk

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Assertions in this file read the Compose files AS DATA and never spell a line number.
// Both files are edited in step and both are long, so a line reference is stale the first
// time either grows — and a test that asserts on a stale line is worse than no test, because
// it fails for the wrong reason and gets deleted rather than fixed.
//
// They are data assertions rather than a real bring-up so they hold in CI with no Docker.
// The runtime halves — that a bare `docker compose up` starts no broker, that
// `--profile kafka up` still waits for the broker's health and for provisioning to complete,
// and that the worker's rendered environment carries no administrative credential — were
// verified separately against a real Compose.

// composeFiles is every Compose projection that carries the event-streaming configuration.
// docker-compose.dev.yaml builds from source and is otherwise a parallel of the shipped file;
// a fix applied to one and not the other is the failure this list exists to catch.
var composeFiles = []string{"docker-compose.yaml", "docker-compose.dev.yaml"}

// blnkKafkaProfile is the Compose profile the two Kafka services sit behind. It is the
// mechanism that makes Kafka opt-in, and .env.example documents it as COMPOSE_PROFILES=kafka.
const blnkKafkaProfile = "kafka"

// TestCompose_KafkaIsOptInSoTheNoBrokerSteadyStateStarts is the graceful-degradation guard.
//
// # What was wrong
//
// The server and worker services depended on `kafka: service_healthy` and
// `kafka-init: service_completed_successfully` UNCONDITIONALLY. Blnk's documented steady
// state is an empty KAFKA_BROKERS, in which the publisher resolves to its no-op and the
// service runs exactly as it did before Kafka existed — but `docker compose up` still refused
// to start it until a broker it was never going to speak to had passed a SASL healthcheck and
// a provisioning one-shot had exited zero. A deployment that publishes no events was blocked
// on infrastructure it does not use, and a broker that failed to bootstrap took the ledger API
// down with it.
//
// # Why both halves are asserted
//
// Profiles alone would not fix it: a service outside the enabled set is still a dependency
// Compose refuses to satisfy unless the dependency is marked `required: false`. And
// `required: false` alone would not fix it either, because without a profile the Kafka
// services are in the default set and start regardless. Each half is inert without the other,
// which is exactly why a test that checked only one would pass over a broken stack.
//
// # Why the conditions must SURVIVE
//
// The fix must not be "delete the dependency". When the profile IS selected the operator has
// asked for Kafka, and starting the relay against a broker that is merely created — not
// listening, not authenticating, with no topics — reproduces the failure the conditions were
// added for: the first publish fails and retries against nothing. So the conditions are
// asserted present and unweakened alongside `required: false`.
func TestCompose_KafkaIsOptInSoTheNoBrokerSteadyStateStarts(t *testing.T) {
	root := moduleRootDir(t)

	// The condition each dependency must still carry once it is optional.
	wantConditions := map[string]string{
		"kafka":      "service_healthy",
		"kafka-init": "service_completed_successfully",
	}

	for _, composeFile := range composeFiles {
		t.Run(composeFile, func(t *testing.T) {
			services := composeServices(t, filepath.Join(root, composeFile), composeFile)

			t.Run("both Kafka services sit behind the profile", func(t *testing.T) {
				for name := range wantConditions {
					service, declared := services[name].(map[string]interface{})
					require.True(t, declared, "%s must declare a %q service", composeFile, name)

					profiles := composeStringList(t, service["profiles"])
					assert.Contains(t, profiles, blnkKafkaProfile,
						"the %q service must sit behind the %q profile, or it starts on every "+
							"`docker compose up` and imposes a broker on deployments that publish nothing",
						name, blnkKafkaProfile)
				}
			})

			t.Run("the server depends on them optionally", func(t *testing.T) {
				// THE SERVER ONLY, and that is the whole point of this sub-test's name. The
				// server hosts the outbox relay and the dead-letter writer, so it is the one
				// process that dials a broker and the one that must not start against a broker
				// which is not listening or a catalogue which was never provisioned.
				const consumer = "server"

				service, declared := services[consumer].(map[string]interface{})
				require.True(t, declared, "%s must declare a %q service", composeFile, consumer)

				dependencies, isMap := service["depends_on"].(map[string]interface{})
				require.True(t, isMap,
					"%s's depends_on must be the long map form: only that form can carry a "+
						"condition and a required flag, and both are load-bearing here", consumer)

				for name, wantCondition := range wantConditions {
					entry, present := dependencies[name].(map[string]interface{})
					require.True(t, present,
						"%s must still declare its %q dependency: dropping it entirely would let "+
							"the relay start against a broker that is not listening", consumer, name)

					assert.Equal(t, false, entry["required"],
						"%s's %q dependency must be `required: false`, or the profile does not "+
							"make Kafka optional — Compose refuses to start a service whose "+
							"dependency is outside the enabled set", consumer, name)

					assert.Equal(t, wantCondition, entry["condition"],
						"%s's %q dependency must KEEP its condition: opting in to Kafka means "+
							"waiting for a broker that authenticates and for provisioning that "+
							"succeeded, not merely for a container that exists", consumer, name)
				}
			})

			t.Run("the worker depends on neither", func(t *testing.T) {
				// THE WORKER NEVER DIALS A BROKER, so gating its start-up on one made an
				// unrelated dependency into a reason this process could not come up. It writes
				// outbox rows; the relay in the server role publishes them. While the two
				// conditions were here, an unhealthy broker or a failed provisioning one-shot
				// stopped transaction processing, transaction hooks and search indexing — none
				// of which involve Kafka at all.
				//
				// Asserted as an ABSENCE rather than left untested because the previous shape of
				// this file required the dependency on both services, so restoring it would
				// otherwise look like a correction.
				worker, declared := services["worker"].(map[string]interface{})
				require.True(t, declared, "%s must declare a worker service", composeFile)

				dependencies, isMap := worker["depends_on"].(map[string]interface{})
				require.True(t, isMap, "the worker must declare a depends_on map")

				for name := range wantConditions {
					assert.NotContains(t, dependencies, name,
						"the worker must not depend on %q: it publishes nothing, so a broker it "+
							"never dials must not be able to keep it from starting", name)
				}
			})
		})
	}
}

// TestCompose_NoKafkaCredentialIsKnownFromSource is the security guard.
//
// # What was wrong, and why it was not a small thing
//
// KAFKA_SASL_ADMIN_SECRET defaulted to a literal readable in the Compose file. That value is
// the password of a Kafka SUPERUSER — `super.users=User:${KAFKA_SASL_ADMIN_USER}` in the
// rendered broker configuration — on a broker the same file PUBLISHES TO THE HOST. Anyone who
// could reach the port could create and delete topics, mint SCRAM credentials for any
// principal, and rewrite every ACL, which includes granting themselves Read on every
// subscriber's events and revoking every real subscriber's credential. A credential that is in
// the source is not a credential.
//
// Worse, that same default was injected into the WORKER — a process that turned out to publish
// nothing at all, so it held the strongest identity in the cluster for no capability whatsoever.
// The worker's Kafka credentials are gone entirely now; the sub-test below asserts their absence
// rather than their shape.
//
// # Why an empty default is the right answer rather than a required one
//
// Compose interpolates the whole file on every command, so `${VAR:?message}` would fail
// `docker compose up` even for a stack that selected no Kafka profile — which would undo the
// graceful degradation the test above exists to protect. An empty default degrades correctly
// instead: nothing reads it with the profile inactive, and with the profile active
// scripts/kafka-bootstrap.sh refuses an empty administrative secret BY NAME, so the failure is
// loud, immediate and local to the broker rather than a puzzling authentication error later.
func TestCompose_NoKafkaCredentialIsKnownFromSource(t *testing.T) {
	root := moduleRootDir(t)

	// Every environment key whose value is a secret. A default for any of these puts a
	// credential in the repository, whatever the value happens to be today.
	secretKeys := []string{
		"KAFKA_SASL_ADMIN_SECRET",
		"KAFKA_SASL_SECRET",
		"KAFKA_PRODUCER_SECRET",
		"KAFKA_SAMPLE_SUBSCRIBER_SECRET",
	}

	for _, composeFile := range composeFiles {
		t.Run(composeFile, func(t *testing.T) {
			path := filepath.Join(root, composeFile)
			services := composeServices(t, path, composeFile)

			t.Run("no secret carries a literal default", func(t *testing.T) {
				for name, raw := range services {
					service, isMap := raw.(map[string]interface{})
					if !isMap {
						continue
					}
					environment, isMap := service["environment"].(map[string]interface{})
					if !isMap {
						continue
					}

					for _, key := range secretKeys {
						value, present := environment[key]
						if !present {
							continue
						}

						rendered, isString := value.(string)
						require.True(t, isString, "%s.%s must interpolate a string", name, key)

						// Only two shapes are acceptable: an empty default, or a default that
						// is itself another VARIABLE reference. Anything else is a literal.
						assert.NotRegexp(t,
							`:-\s*[^}$\s][^}]*}`, rendered,
							"%s.%s in %s defaults to a literal value. A secret with a default in a "+
								"committed file is a secret in the repository — and this one is a "+
								"Kafka credential on a broker published to the host. Ship it empty and "+
								"let the bootstrap script refuse it by name, or default it from "+
								"another variable that is itself empty.",
							name, key, composeFile)
					}
				}
			})

			t.Run("the worker holds no Kafka credential at all", func(t *testing.T) {
				// THE WORKER PUBLISHES NOTHING, so it needs no broker credential of any kind —
				// not the administrative pair, and not the producer pair either.
				//
				// This assertion used to be the opposite for the producer pair: the worker was
				// REQUIRED to receive it, on the belief that transaction.rejected was published
				// from its rejection handler. It is not. That handler's post-transaction actions
				// insert an outbox row inside the ledger transaction, and the relay — which
				// cmd/server.go starts and only the server role runs — is what publishes it.
				// Nothing in this process ever produced a Kafka message, so the credential bought
				// standing Write and Describe authority over every Blnk-owned topic, dead-letter
				// siblings included, for a capability that was never exercised. A compromise of
				// the worker could forge any ledger event onto any topic, and a subscriber cannot
				// tell a forged event from a real one when both arrive under valid producer
				// credentials.
				//
				// blnk.NewBlnkForRole is the enforcement in code — `blnk workers` resolves to
				// ProcessRoleWorker, which selects the no-op publisher before the broker list is
				// read — and this is the environment half of the same decision: a credential that
				// is not in the container's environment is not in `docker inspect`, not in
				// /proc/<pid>/environ, and not in a crash dump.
				worker, declared := services["worker"].(map[string]interface{})
				require.True(t, declared, "%s must declare a worker service", composeFile)

				environment, isMap := worker["environment"].(map[string]interface{})
				require.True(t, isMap, "the worker must declare an environment block")

				forbidden := []string{
					// Credentials: this role writes nothing and administers nothing.
					"KAFKA_SASL_USER", "KAFKA_SASL_SECRET",
					"KAFKA_SASL_ADMIN_USER", "KAFKA_SASL_ADMIN_SECRET",
					"KAFKA_ALLOW_ADMIN_PRODUCER",
					// TLS material: it opens no broker connection, so it verifies no certificate.
					"KAFKA_TLS_ENABLED", "KAFKA_TLS_CA_FILE",
					"KAFKA_TLS_INSECURE_SKIP_VERIFY", "KAFKA_INSECURE_LOCAL_DEV",
					// Credential issuance is a server endpoint.
					"KAFKA_SUBSCRIBER_BROKERS",
					// Topic geometry is applied only by the server's topic assurance.
					"KAFKA_MIN_PARTITIONS", "KAFKA_REPLICATION_FACTOR",
					"KAFKA_ALLOW_PARTITION_GROWTH",
					// Only the relay waits, and only the server runs one.
					"RELAY_RETRY_BASE_BACKOFF_MS", "RELAY_RETRY_MAX_BACKOFF_MS",
				}
				for _, key := range forbidden {
					assert.NotContains(t, environment, key,
						"the worker must not receive %s: it captures events into the outbox and "+
							"publishes none, so this key gives it either authority or material it "+
							"cannot use. Add it back only in the same change that gives this "+
							"process a write path to justify it", key)
					assert.NotContains(t, environment, "BLNK_"+key,
						"the worker must not receive BLNK_%s either: the prefixed alias resolves "+
							"exactly as the bare name does, so passing one and not the other only "+
							"hides the credential from a reader of this file", key)
				}

				// WHAT IT MUST STILL RECEIVE. Capture is gated on the broker list rather than on
				// the publisher, and the row's topic and retry budget are decided where the row
				// is written — so these four are configuration this role genuinely applies, and
				// removing any of them would make the worker's events diverge from the server's.
				for _, key := range []string{
					"KAFKA_BROKERS",
					"KAFKA_TOPIC_PREFIX",
					"RELAY_MAX_RETRY_ATTEMPTS",
					"WEBHOOK_DEPRECATION_SUNSET_DATE",
				} {
					assert.Contains(t, environment, key,
						"the worker must still receive %s: capture is gated on the broker list, "+
							"the row's topic and max_attempts are resolved at capture time, and the "+
							"legacy webhook handler runs in this role during the dual-delivery "+
							"window. Drop it and this role's events take a different path from the "+
							"server's for the same event type", key)
				}
			})

			t.Run("the publishing service defaults its producer pair from one source", func(t *testing.T) {
				// One value in .env both mints the principal on the broker (KAFKA_PRODUCER_*, read
				// by scripts/kafka-provision.sh) and is presented by the publishing process
				// (KAFKA_SASL_*). Two independent values would drift, and the symptom of drift is
				// a SASL handshake failure that reads exactly like a wrong password.
				//
				// One service, not two: the worker presents no credential at all, which the
				// sub-test above asserts.
				const consumer = "server"

				service := services[consumer].(map[string]interface{})
				environment := service["environment"].(map[string]interface{})

				assert.Contains(t, environment["KAFKA_SASL_USER"], "KAFKA_PRODUCER_USER",
					"%s's KAFKA_SASL_USER must default from KAFKA_PRODUCER_USER, the key "+
						"scripts/kafka-provision.sh creates the principal from", consumer)
				assert.Contains(t, environment["KAFKA_SASL_SECRET"], "KAFKA_PRODUCER_SECRET",
					"%s's KAFKA_SASL_SECRET must default from KAFKA_PRODUCER_SECRET, so the "+
						"credential the broker holds and the one this process presents are one "+
						"line of .env", consumer)
			})

			t.Run("the broker still enforces ACLs", func(t *testing.T) {
				// Not a credential question, but it belongs with them: without the KRaft
				// StandardAuthorizer the broker ACCEPTS every ACL and applies none, so the
				// per-principal grants provisioned above would be decorative and the subscriber
				// isolation criterion would pass vacuously.
				contents, err := os.ReadFile(path)
				require.NoError(t, err)

				assert.Contains(t, string(contents),
					"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
					"%s's rendered broker configuration must name the KRaft StandardAuthorizer, or "+
						"every ACL is accepted and none is enforced", composeFile)
				assert.Contains(t, string(contents), "allow.everyone.if.no.acl.found=false",
					"%s must deny by default, or a resource with no matching ACL is allowed and the "+
						"grants become decorative", composeFile)
			})
		})
	}
}

// composeServices reads a Compose file and returns its services map.
func composeServices(t *testing.T, path, label string) map[string]interface{} {
	t.Helper()

	compose := readYAMLFile(t, path)

	services, isMap := compose["services"].(map[string]interface{})
	require.True(t, isMap, "%s must declare a services map", label)

	return services
}

// composeStringList normalises a Compose field that may be absent, a single string or a list.
//
// Returning an empty slice for an absent field rather than failing is what lets the caller's
// assertion be the one that reports the problem, in its own words.
func composeStringList(t *testing.T, raw interface{}) []string {
	t.Helper()

	switch value := raw.(type) {
	case nil:
		return nil
	case string:
		return []string{strings.TrimSpace(value)}
	case []interface{}:
		items := make([]string, 0, len(value))
		for _, item := range value {
			text, isString := item.(string)
			require.True(t, isString, "a profiles entry must be a string")
			items = append(items, strings.TrimSpace(text))
		}

		return items
	default:
		require.Failf(t, "unexpected shape", "cannot read %T as a string list", raw)

		return nil
	}
}

// ---------------------------------------------------------------------------
// The shipped defaults must be defaults scripts/kafka-provision.sh ACCEPTS
// ---------------------------------------------------------------------------

// kafkaProvisionScript is the provisioning entrypoint both the compose kafka-init service and
// the makefile's kafka_provision target run.
const kafkaProvisionScript = "scripts/kafka-provision.sh"

// kafkaProvisionValidationComplete is the line the script logs once every PURE decision has
// been made — geometry, iterations, the topic catalogue, and all three principals — and before
// it touches the network. Reaching it means every value it was handed was accepted; failing to
// reach it means one of them was refused.
//
// It is the only synchronisation point in the script that separates "the configuration is
// usable" from "the broker is reachable", which is what lets the assertion below run in CI
// with no Docker and no Kafka CLI.
const kafkaProvisionValidationComplete = "resolved the topic catalogue"

// TestCompose_EveryShippedKafkaDefaultIsAcceptedByTheProvisioningScript closes the gap that let
// the local stack ship unprovisionable.
//
// # What was wrong
//
// Both compose files passed the kafka-init service
// `KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX: ${KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX:-blnk-sample-subscriber}`
// and .env.example set the same unterminated value uncommented. The script DERIVES that
// namespace as the principal plus the group terminator — "blnk-sample-subscriber." — and
// refuses any explicit value that differs, because the namespace is granted as a PREFIXED ACL
// and choosing it means choosing how far the grant reaches. So
// `docker compose --profile kafka up -d kafka kafka-init` and `make kafka_provision` BOTH
// exited 1 with "Nothing has been provisioned", every time, on a clean checkout — and because
// compose resolves `${VAR:-default}` for an empty value as well as an unset one, no value an
// operator could export would suppress it.
//
// # Why the existing tests could not catch it
//
// event_compose_test.go and event_stack_test.go read the kafka-init environment block and
// assert things ABOUT it — that no secret carries a literal default, that the worker holds no
// administrative credential. event_provisioning_test.go asserts the script's own credential
// floors. Nothing compared the two: no test asked whether the values one side SENDS are values
// the other side ACCEPTS. That is a whole class of defect, not one variable, and it is
// invisible to both halves read alone.
//
// # What this asserts, and why by execution
//
// The compose defaults and .env.example's assignments are collected exactly as a plain
// bring-up would resolve them, and the real script is then RUN against them. Everything up to
// the log line above is pure decision-making, so the assertion is that the script reaches it:
// any refusal of any shipped value happens before it and is reported here with the script's own
// words. Re-implementing the script's validators in Go would only assert that the copy agrees
// with itself.
//
// Only the CREDENTIALS are supplied by the test, because they are the values the repository
// deliberately does NOT ship (see TestCompose_NoKafkaCredentialIsKnownFromSource and
// TestEnvExample_ShipsTheKafkaCredentialsEmpty) and without them the script skips the
// principals whose validation is most of the point. Everything else is exactly what a clean
// checkout sends.
func TestCompose_EveryShippedKafkaDefaultIsAcceptedByTheProvisioningScript(t *testing.T) {
	root := moduleRootDir(t)

	script := filepath.Join(root, kafkaProvisionScript)
	if _, err := os.Stat(script); err != nil {
		require.NoErrorf(t, err, "%s must exist: it is what kafka-init runs", kafkaProvisionScript)
	}

	// A secret long enough to clear the script's own strength floor, and obviously synthetic.
	const testSecret = "blnk-compose-guard-secret-0123456789abcdef"

	envExample := envExampleAssignments(t, root)

	for _, composeFile := range composeFiles {
		t.Run(composeFile, func(t *testing.T) {
			services := composeServices(t, filepath.Join(root, composeFile), composeFile)

			initService, declared := services["kafka-init"].(map[string]interface{})
			require.Truef(t, declared, "%s must declare a kafka-init service", composeFile)

			environment, isMap := initService["environment"].(map[string]interface{})
			require.True(t, isMap, "kafka-init must declare an environment block")

			// A DERIVED value must never arrive with a LITERAL default. Compose substitutes
			// `${VAR:-default}` for an empty value as well as an unset one, so a non-empty default
			// here is not a default — it is a value that always arrives, and the only spelling the
			// script derives is built from another variable in this same block. The key is still
			// passed, with an empty default, because that is what lets the script SEE a stale
			// override an operator is still exporting and say it is no longer honoured; dropping
			// the key would make such a value silently ignored instead. Empty means "derive".
			if declaredPrefix, passed := environment["KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX"]; passed {
				assert.Equal(t, "${KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX:-}", declaredPrefix,
					"%s must pass KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX to kafka-init with an EMPTY "+
						"default: the consumer-group namespace is DERIVED from the principal by %s, "+
						"which refuses any namespace it did not derive, and compose has no way to send "+
						"\"unset\" — so any literal here always arrives and fails provisioning",
					composeFile, kafkaProvisionScript)
			}

			// The whole shipped surface, resolved the way a bring-up resolves it: compose's own
			// defaults first, then .env.example, which is what `make kafka_provision` sources.
			resolved := composeResolvedDefaults(t, environment)
			for key, value := range envExample {
				if strings.HasPrefix(key, "KAFKA_") {
					resolved[key] = value
				}
			}

			// The credentials the repository deliberately ships empty. The administrative
			// PRINCIPAL is one of them: it is never defaulted, precisely so that no identity is
			// invented for an operator, so a guard that supplied only the secrets would be
			// stopped by the half-configured-pair check before reaching anything it is testing.
			resolved["KAFKA_SASL_ADMIN_USER"] = "admin"
			resolved["KAFKA_SASL_ADMIN_SECRET"] = testSecret
			resolved["KAFKA_PRODUCER_SECRET"] = testSecret
			resolved["KAFKA_SAMPLE_SUBSCRIBER_SECRET"] = testSecret

			// Bounded, and pointed nowhere: the subject is the validation phase, so the network
			// phase must cost as little as possible whether or not this host has a Kafka CLI.
			resolved["KAFKA_BOOTSTRAP_SERVER"] = "127.0.0.1:1"
			resolved["KAFKA_CONTAINER"] = "blnk-provision-guard-no-such-container"
			resolved["KAFKA_PROVISION_TIMEOUT_SECONDS"] = "1"
			resolved["KAFKA_PROVISION_POLL_INTERVAL_SECONDS"] = "1"
			resolved["KAFKA_CLI_TIMEOUT_SECONDS"] = "2"

			output, err := runKafkaProvisionValidation(t, root, resolved)

			assert.Containsf(t, output, kafkaProvisionValidationComplete,
				"%s refused a value %s ships to kafka-init, so the documented local bring-up cannot "+
					"provision the broker. The script stopped during validation, before it reached %q. "+
					"Its own words:\n%s\n(exit: %v)",
				kafkaProvisionScript, composeFile, kafkaProvisionValidationComplete, output, err)
		})
	}
}

// envExampleAssignments returns the uncommented assignments .env.example makes.
//
// `make kafka_provision` sources .env, and .env is a copy of this template, so a value here is
// a value the script receives. Commented lines are excluded because they are documentation
// until an operator uncomments them.
func envExampleAssignments(t *testing.T, root string) map[string]string {
	t.Helper()

	contents, err := os.ReadFile(filepath.Join(root, ".env.example"))
	require.NoError(t, err, ".env.example must be readable")

	assignments := map[string]string{}
	for _, line := range strings.Split(string(contents), "\n") {
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

	return assignments
}

// composeResolvedDefaults renders a compose environment block the way a bring-up with an empty
// environment renders it: `${VAR:-default}` becomes the default, `${VAR}` and `${VAR:-}` become
// empty, and a literal stays as written.
//
// Nested references are not resolved. The one place they appear is the publishing services'
// producer pair, not kafka-init, so leaving such a value empty here reproduces what an operator
// who set nothing would get — which is the case under test.
func composeResolvedDefaults(t *testing.T, environment map[string]interface{}) map[string]string {
	t.Helper()

	interpolation := regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)(?::?-(.*))?\}$`)

	resolved := make(map[string]string, len(environment))
	for key, raw := range environment {
		value, isString := raw.(string)
		if !isString {
			// A numeric or boolean scalar, which YAML gives us untyped; render it the way
			// compose passes it to the process.
			resolved[key] = fmt.Sprintf("%v", raw)

			continue
		}

		if match := interpolation.FindStringSubmatch(value); match != nil {
			resolved[key] = match[2]

			continue
		}

		resolved[key] = value
	}

	return resolved
}

// runKafkaProvisionValidation runs the provisioning script with exactly the supplied
// environment and returns its combined output.
//
// The environment is REPLACED rather than extended, so a KAFKA_* variable that happens to be
// exported in the developer's or CI runner's shell cannot mask a bad shipped default — the
// value under test must be the one the repository ships. PATH is minimal for the same reason:
// it keeps a host that has a Kafka distribution installed from wandering into the network phase
// on a longer timeout than the bounded one above.
//
// A non-zero exit is expected and is not asserted on: the script cannot finish without a
// broker. The caller asserts on how FAR it got.
func runKafkaProvisionValidation(t *testing.T, root string, environment map[string]string) (string, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	command := exec.CommandContext(ctx, "bash", kafkaProvisionScript)
	command.Dir = root
	command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
	for key, value := range environment {
		command.Env = append(command.Env, key+"="+value)
	}

	output, err := command.CombinedOutput()
	require.NotErrorIsf(t, ctx.Err(), context.DeadlineExceeded,
		"%s did not finish inside the guard's budget; its output so far:\n%s",
		kafkaProvisionScript, output)

	return string(output), err
}

// composeImageRef returns a service's raw, uninterpolated image reference.
//
// Raw rather than rendered on purpose: the interpolation DEFAULT is the thing under test in
// two of the guards below, and `docker compose config` would have already collapsed it into
// whatever the ambient environment happened to say.
func composeImageRef(t *testing.T, services map[string]interface{}, service, label string) string {
	t.Helper()

	declared, isMap := services[service].(map[string]interface{})
	require.Truef(t, isMap, "%s must declare a %q service", label, service)

	image, isString := declared["image"].(string)
	require.Truef(t, isString, "%s: service %q must declare an image", label, service)

	return strings.TrimSpace(image)
}

// TestCompose_ThirdPartyImagesArePinnedByTagAndDigest is the supply-chain guard.
//
// # What was wrong
//
// The jaeger and prometheus services were pinned to `latest`. A floating tag means the image
// a rebuilt stack runs is whatever the registry resolved to that morning, which defeats the
// point of having a tested stack: the next `docker compose pull` can change the Prometheus
// major version — Prometheus 3 rejected some Prometheus 2 configuration outright — or move
// Jaeger under a collector whose defaults differ, with nothing in the repository recording
// that anything moved.
//
// # Why the digest and not just the tag
//
// A tag is a mutable pointer. The same `v3.13.2` can be repushed over a different image, so a
// version tag alone records intent rather than identity. The digest is what makes a rebuild
// byte-identical to the tested stack, and pinning both means a mismatch fails the pull instead
// of resolving quietly.
//
// # Why "latest" is asserted absent everywhere, not just on these two
//
// The two services that carried it are fixed, but the defect is a habit rather than an
// incident. Asserting across every service is what stops the next one arriving.
func TestCompose_ThirdPartyImagesArePinnedByTagAndDigest(t *testing.T) {
	root := moduleRootDir(t)

	// Third-party services whose image must name a version AND a digest. The Blnk services
	// are excluded deliberately and covered by their own guard: their reference is an
	// operator input with no shipped default, so there is no digest to assert here.
	wantPinned := []string{"jaeger", "prometheus", "redis"}

	digestPin := regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)

	for _, composeFile := range composeFiles {
		t.Run(composeFile, func(t *testing.T) {
			services := composeServices(t, filepath.Join(root, composeFile), composeFile)

			t.Run("no service floats on a mutable tag", func(t *testing.T) {
				for name, raw := range services {
					declared, isMap := raw.(map[string]interface{})
					if !isMap {
						continue
					}

					image, isString := declared["image"].(string)
					if !isString {
						continue // built from source rather than pulled
					}

					assert.NotContainsf(t, image, ":latest",
						"%s: service %q pins :latest, so the image a rebuilt stack runs is "+
							"whatever the registry resolved to that morning", composeFile, name)
				}
			})

			for _, name := range wantPinned {
				t.Run(name+" names a version and a digest", func(t *testing.T) {
					image := composeImageRef(t, services, name, composeFile)

					assert.Regexpf(t, digestPin, image,
						"%s: service %q must be pinned by digest (…@sha256:<64 hex>), not by "+
							"tag alone — a tag can be repushed over a different image; got %q",
						composeFile, name, image)

					tag := image
					if at := strings.Index(image, "@"); at >= 0 {
						tag = image[:at]
					}
					assert.Containsf(t, tag, ":",
						"%s: service %q must keep a human-readable version tag alongside the "+
							"digest, so a reader can tell WHICH release is pinned; got %q",
						composeFile, name, image)
				})
			}

			t.Run("jaeger is on the supported v2 line", func(t *testing.T) {
				image := composeImageRef(t, services, "jaeger", composeFile)

				// jaegertracing/all-in-one is Jaeger v1, which reached end-of-life on
				// 31 December 2025 and receives no further security patches — the image
				// says so itself on startup. Pinning its digest would have frozen a
				// knowingly unpatched image, so the pin has to be on the v2 repository.
				assert.NotContainsf(t, image, "jaegertracing/all-in-one",
					"%s: jaeger must not pin the v1 all-in-one image, which is end-of-life "+
						"and unpatched; use the v2 jaegertracing/jaeger image", composeFile)
				assert.Containsf(t, image, "jaegertracing/jaeger:2.",
					"%s: jaeger must pin a v2 release; got %q", composeFile, image)
			})

			t.Run("no v1-only Jaeger switch is left behind", func(t *testing.T) {
				declared, isMap := services["jaeger"].(map[string]interface{})
				require.Truef(t, isMap, "%s must declare a jaeger service", composeFile)

				// COLLECTOR_OTLP_ENABLED was the v1 switch for OTLP ingest. v2 enables OTLP
				// by default and ignores the variable, so leaving it set would read as
				// load-bearing configuration while doing nothing at all.
				rendered := fmt.Sprintf("%v", declared["environment"])
				assert.NotContainsf(t, rendered, "COLLECTOR_OTLP_ENABLED",
					"%s: COLLECTOR_OTLP_ENABLED is a Jaeger v1 switch that v2 ignores — remove "+
						"it rather than carry it as configuration that does nothing", composeFile)
			})
		})
	}
}

// TestCompose_RedisIsPatchedAndCanBeAuthenticated is the CVE-2025-49844 guard.
//
// # What was wrong
//
// Both Compose files pinned redis:7.2.4 with no authentication. 7.2.4 is vulnerable to
// CVE-2025-49844 ("RediShell", CVSS 10.0), a use-after-free in the Lua interpreter's garbage
// collector that lets a client holding credentials escape the Lua sandbox and execute
// arbitrary code on the host; the 7.2 line was fixed in 7.2.11. With no password, "holding
// credentials" is satisfied by anyone who can reach the port, so the two halves compounded.
//
// # Why the version assertion is a floor and not an equality
//
// Pinning the exact string would make this test the thing that has to be edited on every
// future upgrade, and a test that must be edited to allow a patch is a test that gets edited
// carelessly. The floor is the property that matters: on the 7.2 line, at or above the patch
// that carries the fix.
//
// # Why authentication is asserted as WIRED rather than as ON
//
// It is off by default, deliberately: 37 Go test files dial an unauthenticated localhost:6379
// as a literal and the CI workflow publishes its redis service the same way, so requiring a
// password unconditionally would break `make test` for a defect that lives in the image. What
// must hold is that setting one variable turns it on — and that the compensating control for
// the default, the loopback bind, is still there.
func TestCompose_RedisIsPatchedAndCanBeAuthenticated(t *testing.T) {
	root := moduleRootDir(t)

	// The 7.2 line's fix for CVE-2025-49844. Everything from 7.2.0 to 7.2.10 is vulnerable.
	const (
		wantMajor = 7
		wantMinor = 2
		fixPatch  = 11
	)

	for _, composeFile := range composeFiles {
		t.Run(composeFile, func(t *testing.T) {
			services := composeServices(t, filepath.Join(root, composeFile), composeFile)
			redis, isMap := services["redis"].(map[string]interface{})
			require.Truef(t, isMap, "%s must declare a redis service", composeFile)

			t.Run("the image carries the CVE-2025-49844 fix", func(t *testing.T) {
				image := composeImageRef(t, services, "redis", composeFile)

				var major, minor, patch int
				_, err := fmt.Sscanf(strings.TrimPrefix(image, "redis:"), "%d.%d.%d",
					&major, &minor, &patch)
				require.NoErrorf(t, err,
					"%s: the redis image must name an explicit x.y.z version so the guard can "+
						"compare it against the CVE fix floor; got %q", composeFile, image)

				assert.Equalf(t, wantMajor, major,
					"%s: redis must stay on the %d.x line the stack is built against; got %q",
					composeFile, wantMajor, image)
				assert.Equalf(t, wantMinor, minor,
					"%s: redis must stay on the %d.%d line, so an upgrade is fixes only and "+
						"carries no changed defaults; got %q",
					composeFile, wantMajor, wantMinor, image)
				assert.GreaterOrEqualf(t, patch, fixPatch,
					"%s: redis %d.%d.%d is vulnerable to CVE-2025-49844 (CVSS 10.0, Lua sandbox "+
						"escape to RCE); the %d.%d line was fixed in %d.%d.%d. Blnk cannot apply "+
						"the usual EVAL/EVALSHA denial mitigation because asynq and internal/lock "+
						"both require Lua, so the upgrade is the only remediation available",
					composeFile, major, minor, patch, wantMajor, wantMinor,
					wantMajor, wantMinor, fixPatch)
			})

			t.Run("one variable turns authentication on at the server", func(t *testing.T) {
				command := strings.Join(composeStringList(t, redis["command"]), " ")

				assert.Containsf(t, command, "--requirepass",
					"%s: the redis command must pass --requirepass when REDIS_PASSWORD is set, "+
						"or there is no way to authenticate the server at all", composeFile)
				assert.Containsf(t, command, "REDIS_PASSWORD",
					"%s: --requirepass must read REDIS_PASSWORD, the one variable .env.example "+
						"documents alongside the matching BLNK_REDIS_DNS form", composeFile)

				// The unset case has to reproduce the image default rather than approximate
				// it, because that default is what 37 test files and CI depend on.
				assert.Containsf(t, command, "else exec redis-server",
					"%s: with REDIS_PASSWORD empty the service must exec plain redis-server, "+
						"which is what keeps the unauthenticated localhost:6379 contract the "+
						"test suite and .github/workflows/go.yml rely on", composeFile)

				// `exec` and not a wrapping shell: redis-server must be PID 1 so
				// `docker stop` delivers SIGTERM to Redis rather than to sh.
				assert.NotContainsf(t, command, "; redis-server",
					"%s: redis-server must be exec'd so it is PID 1 and receives SIGTERM",
					composeFile)

				environment, isMap := redis["environment"].(map[string]interface{})
				require.Truef(t, isMap,
					"%s: the redis service must declare a mapping-form environment carrying "+
						"REDIS_PASSWORD", composeFile)
				password, declared := environment["REDIS_PASSWORD"]
				require.Truef(t, declared,
					"%s: the redis service must pass REDIS_PASSWORD through, or the command's "+
						"branch can never see it", composeFile)
				assert.Containsf(t, fmt.Sprintf("%v", password), "REDIS_PASSWORD",
					"%s: REDIS_PASSWORD must be interpolated from the environment rather than "+
						"inlined, so no password is ever committed here", composeFile)
			})

			t.Run("the healthcheck authenticates when a password is set", func(t *testing.T) {
				test, isMap := redis["healthcheck"].(map[string]interface{})
				require.Truef(t, isMap, "%s: the redis service must declare a healthcheck",
					composeFile)

				probe := strings.Join(composeStringList(t, test["test"]), " ")
				assert.Containsf(t, probe, "REDIS_PASSWORD",
					"%s: the healthcheck must authenticate when a password is set, or it reports "+
						"NOAUTH and marks a healthy Redis unhealthy", composeFile)
				assert.Containsf(t, probe, "ping",
					"%s: the healthcheck must still be a PING", composeFile)
			})

			t.Run("the default stays bound to loopback", func(t *testing.T) {
				ports := composeStringList(t, redis["ports"])
				require.Lenf(t, ports, 1, "%s: redis must publish exactly one port mapping",
					composeFile)

				// The loopback bind is the compensating control for shipping without a
				// password. Losing it while authentication is still opt-in would put an
				// unauthenticated Redis holding TRANSACTION_QUEUE on the network.
				assert.Containsf(t, ports[0], "REDIS_OUTER_HOST:-127.0.0.1",
					"%s: redis must default to a 127.0.0.1 bind; authentication is opt-in, so "+
						"the loopback default is what keeps the shipped stack safe", composeFile)
			})
		})
	}
}

// TestCompose_ApplicationImageHasNoStaleDefault is the SEC-03 guard.
//
// # What was wrong
//
// docker-compose.yaml defaulted both the server and the worker to a published Blnk release.
// Because that image really exists, `docker compose up` SUCCEEDED and handed the operator a
// ledger built from code older than their checkout: no Kafka publishing, no outbox relay, no
// /events endpoints, no subscriber registry, and no error explaining the absence. Every CVE
// fixed after that release was present too.
//
// # Why the fix is "no default" rather than "a newer default"
//
// There is no digest that would be correct, because the image for the commit in the working
// tree is not published. Any default at all reintroduces the same class of defect one release
// later. So the image is an operator input, and the shipped fallback is a reference that
// cannot resolve — `docker compose up server` stops with a message naming the variable
// instead of running the wrong code.
//
// # Why interpolation must still succeed
//
// The `${VAR:?message}` form would have been the obvious way to demand a value, but Compose
// interpolates the whole file for every command: it breaks `docker compose config` and the
// infrastructure-only paths this project depends on, including `make kafka_provision` and
// bringing up postgres or the broker alone. An unresolvable default gates starting the
// application without gating anything else, which is why the guard asserts both halves.
func TestCompose_ApplicationImageHasNoStaleDefault(t *testing.T) {
	root := moduleRootDir(t)

	// Both roles run the same binary. A default on one and not the other would give an
	// operator two process roles from two different builds.
	const composeFile = "docker-compose.yaml"

	services := composeServices(t, filepath.Join(root, composeFile), composeFile)

	for _, service := range []string{"server", "worker"} {
		t.Run(service, func(t *testing.T) {
			image := composeImageRef(t, services, service, composeFile)

			assert.Truef(t, strings.HasPrefix(image, "${BLNK_IMAGE"),
				"%s: service %q must take its image from BLNK_IMAGE, so both roles come from "+
					"one operator-chosen build; got %q", composeFile, service, image)

			assert.NotContainsf(t, image, "jerryenebeli/blnk:",
				"%s: service %q must not fall back to a published release. That fallback "+
					"resolves, so `docker compose up` succeeds and silently runs a build older "+
					"than the checkout — with no Kafka publishing, no relay and no /events",
				composeFile, service)

			// The fallback must be present (so interpolation succeeds and infrastructure-only
			// commands keep working) and must not be pullable (so starting the app fails
			// closed). The sentinel is both.
			assert.Containsf(t, image, ":-set-blnk-image-explicitly",
				"%s: service %q must keep an unresolvable sentinel default. `${BLNK_IMAGE:?…}` "+
					"would break `docker compose config` and every infrastructure-only command, "+
					"because Compose interpolates the whole file whichever service you name; "+
					"got %q", composeFile, service, image)
			assert.NotContainsf(t, image, "/",
				"%s: service %q sentinel must not look like a real repository path, or it could "+
					"one day resolve to somebody's image; got %q", composeFile, service, image)
		})
	}

	t.Run("the variable is documented where an operator will look", func(t *testing.T) {
		assignments := envExampleAssignments(t, root)

		value, declared := assignments["BLNK_IMAGE"]
		require.True(t, declared,
			".env.example must declare BLNK_IMAGE: it is now required to start the application, "+
				"and a required variable that appears in no template is one an operator meets "+
				"only as a pull failure")
		assert.Empty(t, value,
			".env.example must leave BLNK_IMAGE empty rather than suggest a release, which is "+
				"the whole point of removing the default")
	})

	t.Run("the Redis password is documented with its DSN counterpart", func(t *testing.T) {
		assignments := envExampleAssignments(t, root)

		for _, key := range []string{"REDIS_PASSWORD", "REDIS_OUTER_HOST", "REDIS_OUTER_PORT"} {
			_, declared := assignments[key]
			assert.Truef(t, declared,
				".env.example must declare %s: Compose reads it, and REDIS_PASSWORD only works "+
					"when BLNK_REDIS_DNS carries the same value in its userinfo, so the two have "+
					"to be documented together", key)
		}

		// The coupling is the part an operator gets wrong, so it must be stated in the file
		// and not only in the compose comment.
		template, err := os.ReadFile(filepath.Join(root, ".env.example"))
		require.NoError(t, err)
		assert.Contains(t, string(template), "redis://:",
			".env.example must show the authenticated DSN form; a password set on the server "+
				"without the matching BLNK_REDIS_DNS locks Blnk out of its own queue")
	})
}

// k8sManifestContainer returns a named container from a Kubernetes workload manifest.
//
// The nested type assertions live here, once, rather than at each call site: written inline
// they form a chain gofmt refuses to wrap, and an unreadable assertion is one nobody checks.
func k8sManifestContainer(t *testing.T, root, manifest, container string) map[string]interface{} {
	t.Helper()

	document := readYAMLFile(t, filepath.Join(root, "infrastructure", "k8s-manifests", manifest))

	spec, isMap := document["spec"].(map[string]interface{})
	require.Truef(t, isMap, "%s must declare a spec", manifest)
	template, isMap := spec["template"].(map[string]interface{})
	require.Truef(t, isMap, "%s must declare spec.template", manifest)
	podSpec, isMap := template["spec"].(map[string]interface{})
	require.Truef(t, isMap, "%s must declare spec.template.spec", manifest)
	containers, isList := podSpec["containers"].([]interface{})
	require.Truef(t, isList, "%s must declare containers", manifest)

	// initContainers are searched too: the server's migration runs as one, and a caller
	// asking for it by name should not have to know which list it lives in.
	if initContainers, declared := podSpec["initContainers"].([]interface{}); declared {
		containers = append(containers, initContainers...)
	}

	for _, raw := range containers {
		declared, isMap := raw.(map[string]interface{})
		if isMap && declared["name"] == container {
			return declared
		}
	}

	require.FailNowf(t, "container not found", "%s declares no container named %q", manifest,
		container)

	return nil
}

// k8sManifestImage returns the image of a named container in a Kubernetes workload manifest.
func k8sManifestImage(t *testing.T, root, manifest, container string) string {
	t.Helper()

	declared := k8sManifestContainer(t, root, manifest, container)

	image, isString := declared["image"].(string)
	require.Truef(t, isString, "%s: container %q must declare an image", manifest, container)

	return strings.TrimSpace(image)
}

// TestKubernetesManifests_CarryTheSamePinsAsCompose is the projection-drift guard.
//
// # What was wrong
//
// The manifests under infrastructure/k8s-manifests are kompose projections of
// docker-compose.yaml — their own annotations say so — so every image defect in the Compose
// file was reproduced there. redis-deployment.yaml pinned the same CVE-2025-49844-vulnerable
// redis:7.2.4 and jaeger-deployment.yaml the same end-of-life `all-in-one:latest`. Fixing the
// Compose file alone would have left the cluster, which is the deployment that actually faces
// a network, running the vulnerable images.
//
// # Why parity is the assertion rather than a second literal
//
// Restating the digest here would make two places to edit and one to forget. Comparing the
// manifest against the Compose file it is derived from is the invariant that actually needs to
// hold, and it fails whichever side is upgraded alone.
//
// # Scope
//
// redis, jaeger and prometheus — every service that appears in both projections. The Blnk
// server and worker are excluded because their image is an operator input with no shipped
// default on either side, so there is no pin to compare; TestKubernetesWorkloads_AreHardened
// asserts their placeholder instead.
func TestKubernetesManifests_CarryTheSamePinsAsCompose(t *testing.T) {
	root := moduleRootDir(t)

	// manifest -> the compose service whose pin it must match, keyed by container name.
	projections := []struct {
		manifest  string
		container string
		service   string
	}{
		{manifest: "redis-deployment.yaml", container: "redis", service: "redis"},
		{manifest: "jaeger-deployment.yaml", container: "jaeger", service: "jaeger"},
		{manifest: "prometheus-deployment.yaml", container: "prometheus", service: "prometheus"},
	}

	services := composeServices(t, filepath.Join(root, "docker-compose.yaml"),
		"docker-compose.yaml")

	for _, projection := range projections {
		t.Run(projection.manifest, func(t *testing.T) {
			manifestImage := k8sManifestImage(t, root, projection.manifest, projection.container)
			composeImage := composeImageRef(t, services, projection.service,
				"docker-compose.yaml")

			assert.Equalf(t, composeImage, manifestImage,
				"%s must pin the same image as the %q service in docker-compose.yaml. These "+
					"manifests are kompose projections of that file, so a pin upgraded on one "+
					"side and not the other leaves the cluster on the old image — and the "+
					"cluster is the deployment that faces a network",
				projection.manifest, projection.service)
		})
	}

	t.Run("the cluster Redis can be authenticated from a Secret", func(t *testing.T) {
		redis := k8sManifestContainer(t, root, "redis-deployment.yaml", "redis")

		// A Service resolves from every pod that can reach the namespace, so the loopback
		// bind that makes the Compose default safe has no equivalent here. The password
		// must be reachable, and it must come from a Secret rather than this manifest.
		rendered := fmt.Sprintf("%v", redis["env"])
		assert.Containsf(t, rendered, "REDIS_PASSWORD",
			"redis-deployment.yaml must accept a REDIS_PASSWORD; there is no loopback bind in "+
				"a cluster, and Redis holds TRANSACTION_QUEUE")
		assert.Containsf(t, rendered, "secretKeyRef",
			"redis-deployment.yaml must read the password from a Secret, not from a literal or "+
				"a ConfigMap")

		args := strings.Join(composeStringList(t, redis["args"]), " ")
		assert.Containsf(t, args, "--requirepass",
			"redis-deployment.yaml must pass --requirepass when REDIS_PASSWORD is set")
		assert.Containsf(t, args, "else exec redis-server",
			"redis-deployment.yaml must still start plain redis-server when no password is "+
				"provisioned, so applying this manifest to a cluster that has no Secret yet "+
				"does not crash-loop it")
	})

	t.Run("no v1-only Jaeger switch survives in the cluster manifest", func(t *testing.T) {
		jaeger := k8sManifestContainer(t, root, "jaeger-deployment.yaml", "jaeger")

		rendered := fmt.Sprintf("%v", jaeger["env"])
		assert.NotContains(t, rendered, "COLLECTOR_OTLP_ENABLED",
			"COLLECTOR_OTLP_ENABLED is a Jaeger v1 switch that v2 ignores; carrying it forward "+
				"reads as load-bearing configuration while doing nothing")
	})
}

// k8sPodSpec returns the pod spec of a Kubernetes workload manifest.
func k8sPodSpec(t *testing.T, root, manifest string) map[string]interface{} {
	t.Helper()

	document := readYAMLFile(t, filepath.Join(root, "infrastructure", "k8s-manifests", manifest))

	spec, isMap := document["spec"].(map[string]interface{})
	require.Truef(t, isMap, "%s must declare a spec", manifest)
	template, isMap := spec["template"].(map[string]interface{})
	require.Truef(t, isMap, "%s must declare spec.template", manifest)
	podSpec, isMap := template["spec"].(map[string]interface{})
	require.Truef(t, isMap, "%s must declare spec.template.spec", manifest)

	return podSpec
}

// TestKubernetesWorkloads_AreHardened is the workload-hardening guard.
//
// # What was wrong
//
// Every application workload ran with the Kubernetes defaults, which are permissive by
// design: root, a writable root filesystem, every capability the runtime grants, a mounted
// ServiceAccount token, no resource requests or limits, and no probes. The Blnk image declares
// no USER, so root was not a choice anyone made — it was what happened.
//
// # Why resources are the least obvious of these and not the least important
//
// With no requests the pods were BestEffort, so they were first to be evicted under node
// pressure — the server owns the event relay and the worker drains TRANSACTION_QUEUE, so the
// two most important processes were the two the kubelet would kill first. Requests also make
// the existing HPAs work: server-hpa.yaml and worker-hpa.yaml scale on `Utilization`, which is
// a percentage OF THE REQUEST, so with no request the autoscalers reported <unknown> and never
// scaled. They were configured and inert.
//
// # Why the probes are asserted per workload rather than generically
//
// Each has a different correct target and getting it wrong is silent. The server answers GET /
// unauthenticated. The worker's /health is registered directly on its monitoring mux and is
// deliberately NOT wrapped in MetricsAuthHandler, so it is the only path there that does not
// need a bearer token — probing /metrics would start reporting 401 as unhealthy the moment
// secure mode was switched on. Prometheus distinguishes /-/ready from /-/healthy.
func TestKubernetesWorkloads_AreHardened(t *testing.T) {
	root := moduleRootDir(t)

	workloads := []struct {
		manifest  string
		container string
		probePath string
		uid       int
		// mountsAPIToken marks the one workload that legitimately needs a
		// ServiceAccount token: Prometheus, whose scrape jobs discover their targets
		// through the Kubernetes API. See the token subtest for why asserting
		// otherwise silences the whole alerting pipeline.
		mountsAPIToken bool
	}{
		{manifest: "server-deployment.yaml", container: "server", probePath: "/", uid: 10001},
		{manifest: "worker-deployment.yaml", container: "worker", probePath: "/health", uid: 10001},
		{manifest: "prometheus-deployment.yaml", container: "prometheus", probePath: "/-/",
			uid: 65534, mountsAPIToken: true},
	}

	for _, workload := range workloads {
		t.Run(workload.manifest, func(t *testing.T) {
			podSpec := k8sPodSpec(t, root, workload.manifest)
			container := k8sManifestContainer(t, root, workload.manifest, workload.container)

			t.Run("runs as a non-root user under a restricted profile", func(t *testing.T) {
				security, isMap := podSpec["securityContext"].(map[string]interface{})
				require.Truef(t, isMap,
					"%s must declare a pod securityContext; the image declares no USER, so "+
						"without one the workload runs as root", workload.manifest)

				assert.Equalf(t, true, security["runAsNonRoot"],
					"%s must set runAsNonRoot, so an image later rebuilt as root fails to "+
						"start rather than quietly gaining privilege", workload.manifest)
				assert.EqualValuesf(t, workload.uid, security["runAsUser"],
					"%s must pin runAsUser", workload.manifest)
				assert.EqualValuesf(t, workload.uid, security["fsGroup"],
					"%s must set fsGroup to the same id, or projected Secret material stays "+
						"root-owned and unreadable to the process that needs it",
					workload.manifest)

				seccomp, isMap := security["seccompProfile"].(map[string]interface{})
				require.Truef(t, isMap, "%s must declare a seccompProfile", workload.manifest)
				assert.Equalf(t, "RuntimeDefault", seccomp["type"],
					"%s must use the RuntimeDefault seccomp profile", workload.manifest)
			})

			t.Run("drops privilege at the container level", func(t *testing.T) {
				security, isMap := container["securityContext"].(map[string]interface{})
				require.Truef(t, isMap, "%s: container %q must declare a securityContext",
					workload.manifest, workload.container)

				assert.Equalf(t, false, security["allowPrivilegeEscalation"],
					"%s must set allowPrivilegeEscalation: false", workload.manifest)
				assert.Equalf(t, true, security["readOnlyRootFilesystem"],
					"%s must set readOnlyRootFilesystem: true; every runtime write these "+
						"workloads make has an explicit writable volume", workload.manifest)

				capabilities, isMap := security["capabilities"].(map[string]interface{})
				require.Truef(t, isMap, "%s must declare capabilities", workload.manifest)
				assert.Equalf(t, []interface{}{"ALL"}, capabilities["drop"],
					"%s must drop ALL capabilities; none of these processes needs one",
					workload.manifest)
			})

			t.Run("mounts a ServiceAccount token only where one is called for",
				func(t *testing.T) {
					// The application workloads call no Kubernetes API. The server is the
					// process most exposed to the network and the worker processes
					// untrusted transaction payloads off a queue, so a namespace-scoped
					// credential is exactly what should not be sitting in either
					// filesystem.
					//
					// PROMETHEUS IS THE EXCEPTION, and asserting `false` for it was the
					// defect rather than the safeguard. Its scrape jobs discover targets
					// through `kubernetes_sd_configs: role: pod`, so it calls the API on
					// every discovery refresh — and with no token file it logs `Cannot
					// create service discovery` once and then serves happily with zero
					// targets. Pod healthy, rules loaded, nothing collected, every alert
					// permanently unable to fire. `automountServiceAccountToken: false`
					// on this workload is therefore not hardening, it is the thing that
					// silences the alerting pipeline; the least-privilege statement that
					// belongs here instead is the namespace-scoped Role in
					// prometheus-rbac.yaml, which grants pods and nothing else.
					//
					// TestPrometheusDiscovery_ShipsBothHalvesTogether owns the positive
					// side of that in full.
					if workload.mountsAPIToken {
						assert.Equalf(t, true, podSpec["automountServiceAccountToken"],
							"%s calls the Kubernetes API for target discovery, so it must "+
								"mount its ServiceAccount token; without the token file "+
								"discovery fails at startup and the pod then reports "+
								"healthy while scraping nothing", workload.manifest)

						return
					}

					assert.Equalf(t, false, podSpec["automountServiceAccountToken"],
						"%s must set automountServiceAccountToken: false", workload.manifest)
				})

			t.Run("declares requests and limits", func(t *testing.T) {
				resources, isMap := container["resources"].(map[string]interface{})
				require.Truef(t, isMap,
					"%s: container %q must declare resources. Without requests the pod is "+
						"BestEffort and first to be evicted, and the Utilization-based HPA "+
						"cannot compute a percentage so it never scales",
					workload.manifest, workload.container)

				for _, field := range []string{"requests", "limits"} {
					budget, isMap := resources[field].(map[string]interface{})
					require.Truef(t, isMap, "%s must declare resources.%s",
						workload.manifest, field)
					assert.NotEmptyf(t, budget["cpu"], "%s: resources.%s.cpu must be set",
						workload.manifest, field)
					assert.NotEmptyf(t, budget["memory"],
						"%s: resources.%s.memory must be set", workload.manifest, field)
				}

				// Equal on purpose: a Go heap or a TSDB given a limit above its request is
				// OOMKilled under exactly the burst the request was sized for.
				assert.Equalf(t,
					resources["requests"].(map[string]interface{})["memory"],
					resources["limits"].(map[string]interface{})["memory"],
					"%s must request and limit the same memory", workload.manifest)
			})

			t.Run("declares all three probes against a reachable path", func(t *testing.T) {
				for _, probe := range []string{"startupProbe", "readinessProbe",
					"livenessProbe"} {
					declared, isMap := container[probe].(map[string]interface{})
					require.Truef(t, isMap, "%s: container %q must declare a %s",
						workload.manifest, workload.container, probe)

					get, isMap := declared["httpGet"].(map[string]interface{})
					require.Truef(t, isMap, "%s: %s must be an httpGet probe",
						workload.manifest, probe)

					path, isString := get["path"].(string)
					require.True(t, isString, "%s: %s must name a path",
						workload.manifest, probe)
					assert.Truef(t, strings.HasPrefix(path, workload.probePath),
						"%s: %s must probe an UNAUTHENTICATED path under %q, or it reports "+
							"401 as unhealthy once secure mode is on; got %q",
						workload.manifest, probe, workload.probePath, path)

					// A named port survives a renumbering; a literal does not.
					assert.IsTypef(t, "", get["port"],
						"%s: %s must target the port by NAME", workload.manifest, probe)
				}

				// Liveness must be the most forgiving of the three: readiness drains a
				// stalled pod from its Service, and only a truly wedged one should be
				// restarted — restarting the server discards the relay lease it holds.
				liveness := container["livenessProbe"].(map[string]interface{})
				readiness := container["readinessProbe"].(map[string]interface{})
				assert.Greaterf(t, liveness["failureThreshold"], readiness["failureThreshold"],
					"%s: livenessProbe.failureThreshold must exceed readinessProbe's, so "+
						"traffic drains before the process is killed", workload.manifest)
			})
		})
	}

	t.Run("the Blnk workloads exec the binary directly", func(t *testing.T) {
		// PID 1 has to be blnk. The server container previously started as
		// `/bin/sh -c "blnk migrate up ... && blnk start ..."`, and sh does not forward
		// signals: the SIGTERM Kubernetes sends on every rollout went to the shell, so
		// cmd/server.go's handler — which drains the lineage and event relays and closes
		// the database pool — never ran and the process was SIGKILLed at the end of the
		// grace period.
		for _, workload := range []struct{ manifest, container string }{
			{manifest: "server-deployment.yaml", container: "server"},
			{manifest: "worker-deployment.yaml", container: "worker"},
		} {
			container := k8sManifestContainer(t, root, workload.manifest, workload.container)
			command := composeStringList(t, container["command"])

			require.NotEmptyf(t, command, "%s: container %q must declare a command",
				workload.manifest, workload.container)
			assert.Equalf(t, "blnk", command[0],
				"%s: container %q must exec blnk directly so it is PID 1 and receives "+
					"SIGTERM; a /bin/sh -c wrapper swallows the signal and the graceful "+
					"shutdown never runs", workload.manifest, workload.container)
			assert.NotContainsf(t, strings.Join(command, " "), "&&",
				"%s: chaining with && requires a shell, which reintroduces the swallowed "+
					"signal. Run migrations in an init container instead",
				workload.manifest, workload.container)
		}

		// The migration still has to happen, and before the server serves.
		podSpec := k8sPodSpec(t, root, "server-deployment.yaml")
		initContainers, isList := podSpec["initContainers"].([]interface{})
		require.Truef(t, isList,
			"server-deployment.yaml must run the migration as an init container, which is "+
				"both what frees the server container to exec directly and what stops a "+
				"server serving against a half-migrated schema")

		migrate := initContainers[0].(map[string]interface{})
		assert.Contains(t, composeStringList(t, migrate["command"]), "migrate",
			"the init container must run the migration")

		// Two builds would migrate to one schema and serve against another.
		server := k8sManifestContainer(t, root, "server-deployment.yaml", "server")
		assert.Equal(t, server["image"], migrate["image"],
			"the migration init container and the server must run the SAME image; different "+
				"builds would migrate to one schema and serve against another")
	})

	t.Run("the application image is an operator input with no stale default", func(t *testing.T) {
		// Same defect and same resolution as docker-compose.yaml: the manifests deployed
		// jerryenebeli/blnk:0.13.3, a real published image built before this
		// implementation, so `kubectl apply` succeeded and the cluster ran a binary with
		// no relay and no /events beside a ConfigMap full of KAFKA_* keys it had never
		// heard of.
		for _, workload := range []struct{ manifest, container string }{
			{manifest: "server-deployment.yaml", container: "server"},
			{manifest: "server-deployment.yaml", container: "migrate"},
			{manifest: "worker-deployment.yaml", container: "worker"},
		} {
			image := k8sManifestImage(t, root, workload.manifest, workload.container)

			assert.NotContainsf(t, image, "jerryenebeli/blnk:",
				"%s: container %q must not deploy a published release built before this "+
					"implementation", workload.manifest, workload.container)
			assert.Containsf(t, image, "REPLACE_WITH_PINNED_DIGEST",
				"%s: container %q must carry a placeholder an operator has to replace with "+
					"this commit's digest; got %q",
				workload.manifest, workload.container, image)
		}
	})

	// The placeholder above is necessary but not sufficient, and this subtest is the
	// difference between the two.
	//
	// # What was still wrong
	//
	// Kubernetes does not validate image references at admission. It accepts
	// `blnk:REPLACE_WITH_PINNED_DIGEST` as a perfectly well-formed reference, admits the
	// object, schedules the pod, and the problem surfaces only when the kubelet tries to
	// pull. So the operator's feedback was an ImagePullBackOff several seconds AFTER an
	// apply that reported success — on a Deployment that is by then partially rolled out,
	// with the previous ReplicaSet scaling down and the new one unable to start. The
	// placeholder made the failure certain; it did not make it early, and "certain but
	// late" is what turns a thirty-second fix into an incident.
	//
	// # Why this is a script rather than more manifest
	//
	// Nothing expressible inside a manifest fails before admission: an absent `image` key
	// is rejected by the API server, but it is also rejected by `kubectl create
	// --dry-run=client`, which would break the static validation this repository already
	// runs over the whole folder. The gate therefore has to live outside the YAML, and it
	// has to be runnable with no cluster and no kubeconfig so CI can run it too.
	t.Run("a preflight gate refuses an unresolved image before any apply", func(t *testing.T) {
		script := filepath.Join(root, "scripts", "k8s-preflight.sh")

		st, err := os.Stat(script)
		require.NoError(t, err,
			"scripts/k8s-preflight.sh must exist: it is what converts a pull-time "+
				"ImagePullBackOff into a pre-apply refusal")
		assert.NotZerof(t, st.Mode().Perm()&0o111,
			"scripts/k8s-preflight.sh must be committed executable (mode %04o); a gate "+
				"an operator has to remember to invoke through `bash` is a gate that gets "+
				"skipped", st.Mode().Perm())

		// EXECUTED, not merely read. A text assertion here would pass against a script
		// whose logic was inverted, and the whole value of this file is its exit status.
		manifests := filepath.Join(root, "infrastructure", "k8s-manifests")

		refuse := exec.Command(script, manifests)
		refuse.Env = append(os.Environ(), "NO_COLOR=1")
		refusedOutput, refuseErr := refuse.CombinedOutput()

		require.Errorf(t, refuseErr,
			"the preflight must FAIL against the committed tree, which carries the "+
				"placeholder by design. A gate that passes here would let the "+
				"placeholder reach a cluster.\n--- output ---\n%s", refusedOutput)
		assert.Contains(t, string(refusedOutput), "UNRESOLVED application-image placeholder",
			"the refusal must name the placeholder as the reason, so the operator knows "+
				"what to resolve rather than only that something failed")
		assert.Contains(t, string(refusedOutput), "--render",
			"the refusal must carry the remedy; a gate that reports a problem without "+
				"the fix relocates the guesswork rather than removing it")

		// And it must ACCEPT a resolved tree, or it is unusable and will be bypassed.
		// The digest here is syntactically valid and refers to nothing: the gate checks
		// the SHAPE of the reference and never contacts a registry, which is what keeps
		// it runnable in CI with no network.
		rendered := filepath.Join(t.TempDir(), "rendered")
		const fakeDigest = "ghcr.io/blnkfinance/blnk@sha256:" +
			"1111111111111111111111111111111111111111111111111111111111111111"

		accept := exec.Command(script, "--render", rendered, manifests)
		accept.Env = append(os.Environ(), "NO_COLOR=1", "BLNK_IMAGE="+fakeDigest)
		acceptedOutput, acceptErr := accept.CombinedOutput()

		require.NoErrorf(t, acceptErr,
			"the preflight must PASS once the image is resolved to a digest, otherwise "+
				"there is no way to deploy and the gate gets removed rather than "+
				"satisfied.\n--- output ---\n%s", acceptedOutput)

		// The rendered tree must actually carry the digest, and must not have left the
		// `blnk:` prefix stranded in front of it.
		for _, name := range []string{"server-deployment.yaml", "worker-deployment.yaml"} {
			body, readErr := os.ReadFile(filepath.Join(rendered, name))
			require.NoError(t, readErr)
			assert.NotContainsf(t, string(body), "REPLACE_WITH_PINNED_DIGEST",
				"%s: rendering must resolve every placeholder occurrence, not the first", name)
			assert.NotContainsf(t, string(body), "blnk:"+fakeDigest,
				"%s: the `blnk:` prefix must not survive in front of the resolved "+
					"reference, which would produce an unpullable image", name)
			assert.Containsf(t, string(body), fakeDigest,
				"%s: the rendered manifest must carry the supplied digest", name)
		}

		// Rendering must be a COPY. An in-place edit would leave a digest in the working
		// tree that must never be committed, and the next `git status` would invite
		// exactly that.
		for _, name := range []string{"server-deployment.yaml", "worker-deployment.yaml"} {
			body, readErr := os.ReadFile(filepath.Join(manifests, name))
			require.NoError(t, readErr)
			assert.Containsf(t, string(body), "REPLACE_WITH_PINNED_DIGEST",
				"%s: --render must not modify the SOURCE tree; the committed manifests "+
					"must keep their placeholder", name)
			assert.NotContainsf(t, string(body), fakeDigest,
				"%s: --render leaked a resolved digest into the committed tree", name)
		}
	})

	// A SHAPE assertion on the one manifest that can silently corrupt a metadata log.
	//
	// # What must never exist, and what may
	//
	// A SINGLETON kafka-data PersistentVolumeClaim used to exist here, generated by
	// Kompose. A PVC is ONE claim bound to ONE volume, and ReadWriteOnce means one node may
	// mount it at a time — while this StatefulSet runs three replicas, each of which needs
	// its own KRaft metadata log and its own partition segments. Three brokers sharing one
	// claim either fail to schedule onto separate nodes or, far worse, write concurrently
	// into a single log directory, which corrupts the metadata log rather than reporting an
	// error. Nothing ever mounted it either, so applying the folder simply provisioned an
	// idle volume for someone to find later.
	//
	// What is forbidden is therefore the SINGLETON, not the file. A claim whose name is one
	// of the per-ordinal names Kubernetes derives from the template —
	// <template>-<statefulset>-<ordinal>, i.e. kafka-data-kafka-0/-1/-2 — is not shared by
	// anything: the StatefulSet ADOPTS the existing object instead of creating a second, and
	// pre-provisioning them is the only way to bind a particular PersistentVolume, a named
	// local disk or a restored snapshot to a particular broker, which one template cannot
	// express. That is why this file may exist and why pg-data's presence beside
	// postgres-statefulset.yaml is not the asymmetry it looks like.
	//
	// So this asserts SHAPE rather than absence, which is also strictly stronger: an absence
	// assertion passes on a file that exists with the wrong shape, while this one catches the
	// singleton, a name no pod will ever ask for, an access mode that contradicts the
	// template, and a size that silently disagrees with it.
	t.Run("kafka storage is per-pod only, with no standalone claim to share", func(t *testing.T) {
		manifests := filepath.Join(root, "infrastructure", "k8s-manifests")

		// And the positive half FIRST: the template is the ONLY declaration the cluster needs,
		// so it is what everything else is checked against.
		sts := readYAMLFile(t, filepath.Join(manifests, "kafka-statefulset.yaml"))
		spec, ok := sts["spec"].(map[string]interface{})
		require.True(t, ok, "kafka-statefulset.yaml must have a spec")

		templates, ok := spec["volumeClaimTemplates"].([]interface{})
		require.Truef(t, ok && len(templates) > 0,
			"kafka-statefulset.yaml must declare volumeClaimTemplates: it is the ONLY "+
				"declaration of the broker's storage, and without it each pod would get an "+
				"emptyDir and lose its metadata on every reschedule")

		first, ok := templates[0].(map[string]interface{})
		require.True(t, ok)
		meta, ok := first["metadata"].(map[string]interface{})
		require.True(t, ok, "the claim template must carry metadata")
		assert.Equal(t, "kafka-data", meta["name"],
			"the template name is what Kubernetes prefixes onto each ordinal to make "+
				"kafka-data-kafka-0/-1/-2, and alerts/blnk-kafka-alerts.yml plus the "+
				"prometheus infra rule match that name pattern")

		// EVERY STANDALONE CLAIM, if the optional pre-provisioning file is present at all,
		// must be one of the names the template derives. Anything else is a claim no pod will
		// ever mount, and a claim named `kafka-data` alone is the singleton three brokers
		// would share.
		claims := filepath.Join(manifests, "kafka-data-persistentvolumeclaim.yaml")
		if _, statErr := os.Stat(claims); os.IsNotExist(statErr) {
			// Equally valid: the StatefulSet creates the same claims itself from the template.
			return
		}

		replicas, ok := spec["replicas"].(int)
		require.Truef(t, ok, "kafka-statefulset.yaml must declare an integer replica count")

		permitted := map[string]bool{}
		for ordinal := 0; ordinal < replicas; ordinal++ {
			permitted[fmt.Sprintf("kafka-data-kafka-%d", ordinal)] = true
		}

		templateSpec, ok := first["spec"].(map[string]interface{})
		require.True(t, ok, "the claim template must carry a spec")

		found := map[string]bool{}
		for _, claim := range readYAMLDocuments(t, claims) {
			if claim["kind"] != "PersistentVolumeClaim" {
				continue
			}

			claimMeta, isMap := claim["metadata"].(map[string]interface{})
			require.True(t, isMap, "every claim must carry metadata")
			name, _ := claimMeta["name"].(string)

			assert.Truef(t, permitted[name],
				"kafka-data-persistentvolumeclaim.yaml declares a claim named %q. Only the "+
					"names Kubernetes derives from the template are adoptable — "+
					"kafka-data-kafka-0..%d — and a claim named anything else is either the "+
					"SINGLETON three brokers would share into one KRaft metadata log, or an "+
					"idle volume no pod will ever mount",
				name, replicas-1)
			found[name] = true

			claimSpec, isMap := claim["spec"].(map[string]interface{})
			require.Truef(t, isMap, "%s must carry a spec", name)
			assert.Equalf(t, templateSpec["accessModes"], claimSpec["accessModes"],
				"%s must match the template's access mode, or the StatefulSet refuses to "+
					"adopt it and the pod stays Pending with no explanation of why", name)
			assert.Equalf(t, templateSpec["resources"], claimSpec["resources"],
				"%s must request the same storage as the template, or a broker's retention "+
					"budget silently differs from the one MAJ-19 derived", name)
		}

		assert.Len(t, found, replicas,
			"pre-provisioning is all-or-nothing per ordinal: a partial set leaves the "+
				"remaining brokers on claims the StatefulSet creates with the template's "+
				"defaults, which is the asymmetry this file exists to avoid")
	})
}

// TestKubernetesConfig_ProjectsCredentialsFromSecrets is the SEC-01 and SEC-07 guard.
//
// # What was wrong
//
// Two defects that compounded. The manifests projected no authentication settings at all, and
// api/middleware/auth.go calls c.Next() BEFORE looking for a credential whenever
// Server.Secure is false — so a cluster deployed from these files served the whole ledger API
// to anyone who could reach it: create transactions, read every identity, mint API keys. And
// the ConfigMap carried the PostgreSQL password in plaintext with sslmode=disable, in an object
// stored unencrypted, readable by anything with get-configmap, and printed in full by
// `kubectl describe`.
//
// # Why the assertions are split between "in a Secret" and "not in the ConfigMap"
//
// Adding a Secret reference does not remove a committed credential. Both halves have to hold
// or the credential is simply in two places, and the ConfigMap copy is the readable one.
func TestKubernetesConfig_ProjectsCredentialsFromSecrets(t *testing.T) {
	root := moduleRootDir(t)

	configMap := readYAMLFile(t, filepath.Join(root, "infrastructure", "k8s-manifests",
		"blnk-config.yaml"))
	data, isMap := configMap["data"].(map[string]interface{})
	require.True(t, isMap, "blnk-config.yaml must declare a data map")

	t.Run("secure mode is on, so authentication is actually enforced", func(t *testing.T) {
		// This single key decides whether the API asks for a credential at all.
		assert.Equal(t, "true", data["BLNK_SERVER_SECURE"],
			"blnk-config.yaml must set BLNK_SERVER_SECURE to \"true\". false is the zero "+
				"value, and auth.go admits every request unauthenticated when it is false")
	})

	t.Run("the ConfigMap carries no credential", func(t *testing.T) {
		rendered := fmt.Sprintf("%v", data)

		assert.NotContains(t, rendered, "sslmode=disable",
			"blnk-config.yaml must not ship sslmode=disable: that is not a weak cipher, it "+
				"is no cipher — every ledger row, and the password on the first exchange, "+
				"crosses the pod network in cleartext")
		assert.Contains(t, rendered, "sslmode=verify-full",
			"the committed DSN must use verify-full, the only mode that also verifies the "+
				"server certificate matches the host being dialled")
		assert.NotContains(t, rendered, ":password@",
			"blnk-config.yaml must not embed a database password; a ConfigMap is stored "+
				"unencrypted and printed in full by `kubectl describe`")
		assert.NotContains(t, rendered, "blnk-api-key",
			"blnk-config.yaml must not embed the published TypeSense literal")

		// Absence is required rather than merely tolerated: an authenticated Redis DSN
		// carries its password in the userinfo, so the DSN itself is a credential.
		_, declared := data["BLNK_REDIS_DNS"]
		assert.False(t, declared,
			"BLNK_REDIS_DNS must not be a ConfigMap key; it is Secret-projected in both "+
				"Deployments because an authenticated DSN carries its password inline")
	})

	t.Run("the Deployments project every credential from a Secret", func(t *testing.T) {
		// key -> the workloads that must project it. The master key is server-only on
		// purpose: nothing in the worker role authenticates an inbound API caller, so the
		// key would be an unused credential in a pod that processes untrusted payloads.
		wantSecretEnv := map[string][]string{
			"BLNK_SERVER_SECRET_KEY":    {"server"},
			"BLNK_METRICS_BEARER_TOKEN": {"server", "worker"},
			"BLNK_DATA_SOURCE_DNS":      {"server", "worker"},
			"BLNK_REDIS_DNS":            {"server", "worker"},
			"BLNK_TYPESENSE_KEY":        {"server", "worker"},
		}

		for key, roles := range wantSecretEnv {
			for _, role := range roles {
				container := k8sManifestContainer(t, root, role+"-deployment.yaml", role)

				entries, isList := container["env"].([]interface{})
				require.Truef(t, isList, "%s must declare env", role)

				var found map[string]interface{}
				for _, raw := range entries {
					entry, isMap := raw.(map[string]interface{})
					if isMap && entry["name"] == key {
						found = entry

						break
					}
				}
				require.NotNilf(t, found, "the %s must project %s", role, key)

				from, isMap := found["valueFrom"].(map[string]interface{})
				require.Truef(t, isMap, "%s: %s must use valueFrom, never an inline value",
					role, key)
				_, fromSecret := from["secretKeyRef"]
				assert.Truef(t, fromSecret,
					"%s: %s must come from a secretKeyRef, not a ConfigMap — it is a "+
						"credential, and a ConfigMap is readable by anything holding "+
						"get-configmap in the namespace", role, key)
			}
		}

		// The master key must NOT reach the worker, and that asymmetry is deliberate.
		worker := k8sManifestContainer(t, root, "worker-deployment.yaml", "worker")
		assert.NotContains(t, fmt.Sprintf("%v", worker["env"]), "BLNK_SERVER_SECRET_KEY",
			"the worker must not receive the master key: it authenticates no inbound API "+
				"caller, so the key would be an unused credential inside the pod that "+
				"processes untrusted transaction payloads")
	})

	t.Run("the server is not published in plaintext", func(t *testing.T) {
		service := readYAMLFile(t, filepath.Join(root, "infrastructure", "k8s-manifests",
			"server-service.yaml"))
		spec, isMap := service["spec"].(map[string]interface{})
		require.True(t, isMap, "server-service.yaml must declare a spec")

		// Blnk terminates no TLS — cmd/server.go always uses plain ListenAndServe and
		// serveTLS is unreachable dead code — so a LoadBalancer on this Service published
		// unencrypted HTTP straight to the internet, carrying the master key in a header
		// and one-time SCRAM passwords in response bodies.
		assert.Equal(t, "ClusterIP", spec["type"],
			"server-service.yaml must be ClusterIP. Blnk terminates no TLS, so exposing it "+
				"directly publishes plaintext HTTP; terminate at an Ingress instead")

		ports, isList := spec["ports"].([]interface{})
		require.True(t, isList, "server-service.yaml must declare ports")
		for _, raw := range ports {
			port := raw.(map[string]interface{})
			assert.NotContainsf(t, []interface{}{80, 443}, port["port"],
				"server-service.yaml must not publish port %v: nothing listens on it, "+
					"because the CertMagic path those ports were for is dead code",
				port["port"])
		}
	})
}

// mebibytes parses the subset of Kubernetes and JVM size suffixes these manifests use.
//
// Returning bytes-as-MiB rather than a resource.Quantity keeps the comparison below
// dependency-free: the guard needs to know whether one number is smaller than another, not to
// reimplement quantity arithmetic.
func mebibytes(t *testing.T, value string) int {
	t.Helper()

	var size int
	var suffix string
	_, err := fmt.Sscanf(value, "%d%s", &size, &suffix)
	require.NoErrorf(t, err, "cannot read %q as a size", value)

	switch strings.ToLower(suffix) {
	case "mi", "m":
		return size
	case "gi", "g":
		return size * 1024
	default:
		require.Failf(t, "unsupported size suffix", "%q in %q", suffix, value)

		return 0
	}
}

// TestKafkaStatefulSet_CanColdStartAndIsHardened guards the four defects that made the broker
// set unable to start, unable to read its own keystore, and unbounded.
//
// # SEC-04, and why OrderedReady was a deadlock rather than a preference
//
// OrderedReady creates kafka-1 only after kafka-0 is READY, and kafka-0's readiness probe is an
// authenticated kafka-topics.sh --list, which needs a metadata response, which needs the KRaft
// controller quorum to have elected a leader. The voter list enumerates all three replicas, so
// a majority is two — and with only kafka-0 running there is one voter of three. No majority,
// no leader, no readiness, so kafka-1 is never created and the quorum can never reach two. The
// set never starts and never explains why.
//
// # SEC-05, and why the keystore mode and the fsGroup are one assertion
//
// A Secret volume's files are owned by root with their group taken from the pod's fsGroup.
// Mode 0400 is owner-read-only, and the apache/kafka image runs as uid 1000 — so the broker
// could not open its own keystore. Both listeners are SASL_SSL, so that is not degradation: TLS
// initialisation fails and no listener binds. Group-readable mode without an fsGroup is equally
// useless, and an fsGroup without group-readable mode likewise, which is why neither is
// asserted alone.
//
// # SEC-11
//
// Neither container declared a security context or a resource budget, and the heap was an
// invisible 1G default from kafka-server-start.sh. A memory limit chosen without reference to
// that default is an OOMKill under load, so the two are asserted against each other rather than
// merely asserted present.
//
// # SEC-17
//
// The hard half is enforced here: required hostname anti-affinity, so three replicas of a
// replication-factor-3 cluster cannot land on one node and be lost together. Dedicated
// controllers remain a documented scaling step rather than a guarded property, because that is
// a multi-manifest topology change.
func TestKafkaStatefulSet_CanColdStartAndIsHardened(t *testing.T) {
	root := moduleRootDir(t)

	const manifest = "kafka-statefulset.yaml"

	document := readYAMLFile(t, filepath.Join(root, "infrastructure", "k8s-manifests", manifest))
	spec, isMap := document["spec"].(map[string]interface{})
	require.True(t, isMap, "kafka-statefulset.yaml must declare a spec")

	podSpec := k8sPodSpec(t, root, manifest)
	bootstrap := k8sManifestContainer(t, root, manifest, "kafka-bootstrap")
	broker := k8sManifestContainer(t, root, manifest, "kafka")

	t.Run("a multi-voter quorum can cold-start", func(t *testing.T) {
		replicas, isInt := spec["replicas"].(int)
		require.True(t, isInt, "kafka-statefulset.yaml must declare replicas")

		if replicas > 1 {
			assert.Equal(t, "Parallel", spec["podManagementPolicy"],
				"with more than one replica the pods MUST be created in parallel. Under "+
					"OrderedReady, kafka-0's readiness probe needs a KRaft quorum majority "+
					"that cannot exist until kafka-1 is created, and kafka-1 is not created "+
					"until kafka-0 is Ready — the set never starts")
		}

		// The startup probe is what buys formatting and election their time; without it,
		// parallel start would be killed by liveness mid-bootstrap.
		startup, isMap := broker["startupProbe"].(map[string]interface{})
		require.True(t, isMap,
			"the broker must keep a startupProbe: on a cold cluster it is not listening "+
				"until storage is formatted and a leader elected, and without one liveness "+
				"restarts it for being slow rather than broken")
		assert.GreaterOrEqual(t, startup["failureThreshold"], 12,
			"the startup budget must be generous enough for quorum formation")
	})

	t.Run("replicas cannot be co-located", func(t *testing.T) {
		affinity, isMap := podSpec["affinity"].(map[string]interface{})
		require.True(t, isMap,
			"kafka-statefulset.yaml must declare anti-affinity. Three replicas on one node "+
				"is three replicas of nothing: min.insync.replicas=2 is satisfied by copies "+
				"that die together, and the node loss takes the whole controller quorum with "+
				"the data")

		anti, isMap := affinity["podAntiAffinity"].(map[string]interface{})
		require.True(t, isMap, "podAntiAffinity is required")
		required, isList := anti["requiredDuringSchedulingIgnoredDuringExecution"].([]interface{})
		require.Truef(t, isList,
			"the anti-affinity must be requiredDuringScheduling, not preferred: a preference "+
				"the scheduler may ignore is the defect, not the fix")

		rule := required[0].(map[string]interface{})
		assert.Equal(t, "kubernetes.io/hostname", rule["topologyKey"],
			"the anti-affinity must key on hostname, which every node carries")

		// The spread constraint has to agree with the anti-affinity rather than tolerate
		// what it forbids.
		constraints, isList := podSpec["topologySpreadConstraints"].([]interface{})
		require.True(t, isList, "the topology spread constraint must survive")
		constraint := constraints[0].(map[string]interface{})
		assert.Equal(t, "DoNotSchedule", constraint["whenUnsatisfiable"],
			"the spread constraint must be DoNotSchedule so it agrees with the hard "+
				"anti-affinity; ScheduleAnyway only ever protected a single-node cluster "+
				"that could not honour replication factor 3 in the first place")
	})

	t.Run("the broker can read its own keystore", func(t *testing.T) {
		security, isMap := podSpec["securityContext"].(map[string]interface{})
		require.True(t, isMap, "kafka-statefulset.yaml must declare a pod securityContext")

		assert.Equal(t, true, security["runAsNonRoot"], "the broker must not run as root")
		// 1000 is the apache/kafka image's own appuser, so the PVC and the pre-existing
		// log directory keep their ownership across an upgrade.
		assert.EqualValues(t, 1000, security["runAsUser"],
			"runAsUser must be the image's appuser uid (1000), or the persistent volume's "+
				"existing ownership stops matching the process")
		assert.EqualValues(t, security["runAsUser"], security["fsGroup"],
			"fsGroup must equal runAsUser: it is what gives the projected Secret files a "+
				"group the broker belongs to")

		volumes, isList := podSpec["volumes"].([]interface{})
		require.True(t, isList, "volumes must be declared")

		var tlsMode int
		for _, raw := range volumes {
			volume := raw.(map[string]interface{})
			if volume["name"] != "kafka-tls" {
				continue
			}
			secret := volume["secret"].(map[string]interface{})
			mode, isInt := secret["defaultMode"].(int)
			require.True(t, isInt, "the kafka-tls volume must pin a defaultMode")
			tlsMode = mode
		}
		require.NotZero(t, tlsMode, "the kafka-tls volume must be declared")

		// YAML parses a leading-zero literal as octal, so 0440 arrives as 288.
		assert.NotZerof(t, tlsMode&0o040,
			"the kafka-tls material must be GROUP-readable (mode & 040). Secret files are "+
				"owned by root with the group taken from fsGroup, so an owner-only mode like "+
				"0400 leaves the broker unable to open its keystore — and because both "+
				"listeners are SASL_SSL, it then binds no listener at all. Got %#o", tlsMode)
		assert.Zerof(t, tlsMode&0o004,
			"the kafka-tls material must NOT be world-readable: a private key readable by "+
				"any uid in the pod defeats the point of projecting it. Got %#o", tlsMode)
	})

	t.Run("both containers are restricted and bounded", func(t *testing.T) {
		assert.Equal(t, false, podSpec["automountServiceAccountToken"],
			"the broker never calls the Kubernetes API — its peers come from the static "+
				"KRaft voter list — so the token must not be mounted")

		for name, container := range map[string]map[string]interface{}{
			"kafka-bootstrap": bootstrap,
			"kafka":           broker,
		} {
			security, isMap := container["securityContext"].(map[string]interface{})
			require.Truef(t, isMap, "%s must declare a securityContext", name)
			assert.Equalf(t, false, security["allowPrivilegeEscalation"],
				"%s must set allowPrivilegeEscalation: false", name)
			assert.Equalf(t, true, security["readOnlyRootFilesystem"],
				"%s must set readOnlyRootFilesystem: true", name)
			assert.Equalf(t, []interface{}{"ALL"},
				security["capabilities"].(map[string]interface{})["drop"],
				"%s must drop ALL capabilities", name)

			resources, isMap := container["resources"].(map[string]interface{})
			require.Truef(t, isMap,
				"%s must declare resources. A pod's QoS class is computed across ALL its "+
					"containers including init ones, so an unbounded init container makes "+
					"the broker BestEffort however the broker itself is sized", name)
			for _, field := range []string{"requests", "limits"} {
				budget, isMap := resources[field].(map[string]interface{})
				require.Truef(t, isMap, "%s must declare resources.%s", name, field)
				assert.NotEmptyf(t, budget["cpu"], "%s: resources.%s.cpu", name, field)
				assert.NotEmptyf(t, budget["memory"], "%s: resources.%s.memory", name, field)
			}

			// kafka-run-class.sh writes log4j output to /opt/kafka/logs inside the image,
			// which a read-only root filesystem forbids. kafka-authorizer.log in
			// particular is the record of every StandardAuthorizer denial — the evidence
			// a subscriber-isolation investigation reads.
			var logDirMounted bool
			for _, raw := range container["volumeMounts"].([]interface{}) {
				if raw.(map[string]interface{})["mountPath"] == "/opt/kafka/logs" {
					logDirMounted = true
				}
			}
			assert.Truef(t, logDirMounted,
				"%s must mount a writable volume at /opt/kafka/logs; kafka-run-class.sh "+
					"mkdir -p's it and writes there, which a read-only root forbids", name)
		}
	})

	t.Run("the heap fits inside the memory limit", func(t *testing.T) {
		var heap string
		for _, raw := range broker["env"].([]interface{}) {
			entry := raw.(map[string]interface{})
			if entry["name"] == "KAFKA_HEAP_OPTS" {
				heap, _ = entry["value"].(string)
			}
		}
		require.NotEmptyf(t, heap,
			"the broker must state KAFKA_HEAP_OPTS. kafka-server-start.sh exports "+
				"-Xmx1G -Xms1G when it is unset, so the heap is 1G whether or not anyone "+
				"chose it — and a memory limit set without reference to that is an OOMKill "+
				"under load")

		var xmx string
		for _, option := range strings.Fields(heap) {
			if strings.HasPrefix(option, "-Xmx") {
				xmx = strings.TrimPrefix(option, "-Xmx")
			}
		}
		require.NotEmptyf(t, xmx, "KAFKA_HEAP_OPTS must set -Xmx; got %q", heap)

		limit := broker["resources"].(map[string]interface{})["limits"].(map[string]interface{})
		limitMiB := mebibytes(t, limit["memory"].(string))
		heapMiB := mebibytes(t, xmx)

		assert.Lessf(t, heapMiB, limitMiB,
			"the JVM heap (%dMiB) must be smaller than the container memory limit (%dMiB), "+
				"or the kernel kills the broker before the heap is full", heapMiB, limitMiB)
		// Off-heap is not a rounding error for a broker: metaspace, thread stacks and the
		// direct byte buffers every network and log read allocates all live outside -Xmx.
		assert.LessOrEqualf(t, heapMiB*2, limitMiB,
			"the memory limit (%dMiB) must be at least twice the heap (%dMiB) to leave room "+
				"for metaspace, thread stacks, direct byte buffers and page cache",
			limitMiB, heapMiB)
	})
}

// TestPodDisruptionBudgets_GovernEachWorkloadExactlyOnce is the guard for a duplication that
// Kubernetes accepts and then behaves unpredictably under.
//
// # What was wrong
//
// Two PodDisruptionBudgets targeted the identical Kafka pod selector: a generic `kafka` in
// blnk-poddisruptionbudgets.yaml and the purpose-built `kafka-quorum` shipped alongside the
// StatefulSet it protects. Kubernetes documents a pod matched by more than one budget as an
// UNSUPPORTED configuration — it admits both, `kubectl get pdb` lists both, and which one the
// eviction API honours is not something the manifests decide.
//
// # Why it was benign, and why that is the problem
//
// The two agreed: both said maxUnavailable: 1. So nothing was observably wrong, which is
// precisely what let it ship. The eviction budget only ever matters during a node drain or a
// cluster upgrade, and that is exactly when a later edit to either file — raising one to
// maxUnavailable: 2, or narrowing a selector — would silently change how many of three KRaft
// voters may go at once. Losing two brokers costs the controller quorum AND the in-sync
// replica minimum simultaneously, so the blast radius of the ambiguity is the whole cluster's
// availability.
//
// # What this asserts
//
// One budget per selector across the WHOLE manifest folder, not per file — the duplication
// spanned two files, so a per-file check would have found nothing. Multi-document files are
// read in full for the same reason: `kafka-quorum` is the second document of
// kafka-statefulset.yaml, which is where it belongs.
func TestPodDisruptionBudgets_GovernEachWorkloadExactlyOnce(t *testing.T) {
	root := moduleRootDir(t)
	manifestDir := filepath.Join(root, "infrastructure", "k8s-manifests")

	entries, err := os.ReadDir(manifestDir)
	require.NoError(t, err, "the manifest folder must be readable")

	// selector fingerprint -> the budgets claiming it, each named with its file so a failure
	// says where both copies are rather than only that there are two.
	claimed := map[string][]string{}
	var budgets int

	// Every budget's selector, and every workload's pod labels, collected in the same pass so
	// the two can be matched afterwards. A selector that matches no workload is the OTHER
	// silent shape this file's own documentation warns about: admitted, zero expected pods,
	// nothing protected.
	type labelledObject struct {
		description string
		namespace   string
		labels      map[string]interface{}
	}

	var (
		budgetSelectors []labelledObject
		workloads       []labelledObject
	)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		for _, document := range readYAMLDocuments(t, filepath.Join(manifestDir, entry.Name())) {
			if podLabels, namespace, isWorkload := workloadPodLabels(document); isWorkload {
				workloads = append(workloads, labelledObject{
					description: workloadDescription(entry.Name(), document),
					namespace:   namespace,
					labels:      podLabels,
				})
			}

			if document["kind"] != "PodDisruptionBudget" {
				continue
			}
			budgets++

			metadata, isMap := document["metadata"].(map[string]interface{})
			require.Truef(t, isMap, "%s: a PodDisruptionBudget must carry metadata", entry.Name())
			name, _ := metadata["name"].(string)

			spec, isMap := document["spec"].(map[string]interface{})
			require.Truef(t, isMap, "%s: budget %q must carry a spec", entry.Name(), name)

			// A budget whose selector matches nothing is admitted, reports zero expected
			// pods and protects nothing — the same silent shape as the duplication.
			selector, isMap := spec["selector"].(map[string]interface{})
			require.Truef(t, isMap,
				"%s: budget %q must declare a selector; an empty selector matches every pod "+
					"in the namespace", entry.Name(), name)
			labels, isMap := selector["matchLabels"].(map[string]interface{})
			require.Truef(t, isMap && len(labels) > 0,
				"%s: budget %q must select by matchLabels", entry.Name(), name)

			fingerprint := fmt.Sprintf("%s|%v", metadata["namespace"], labels)
			claimed[fingerprint] = append(claimed[fingerprint],
				fmt.Sprintf("%s (%s)", name, entry.Name()))

			budgetNamespace, _ := metadata["namespace"].(string)
			budgetSelectors = append(budgetSelectors, labelledObject{
				description: fmt.Sprintf("%s (%s)", name, entry.Name()),
				namespace:   budgetNamespace,
				labels:      labels,
			})
		}
	}

	require.NotZero(t, budgets,
		"the manifest folder must ship PodDisruptionBudgets; without them a single node drain "+
			"can take every replica of a role")

	require.NotEmpty(t, workloads,
		"no workload was found to match the budgets against, which means this comparison is "+
			"vacuous rather than that the budgets are correct")

	for _, budget := range budgetSelectors {
		var governed []string

		for _, workload := range workloads {
			if budget.namespace == workload.namespace && labelsSatisfy(workload.labels, budget.labels) {
				governed = append(governed, workload.description)
			}
		}

		assert.NotEmptyf(t, governed,
			"budget %s selects %v in namespace %q and no workload in this folder carries those "+
				"pod labels. Such a budget is ADMITTED and reports zero expected pods, so it "+
				"protects nothing while reading in review as though it did — the same silent "+
				"shape as the duplication above. Check the selector against the workload's "+
				"spec.template.metadata.labels",
			budget.description, budget.labels, budget.namespace)
	}

	for fingerprint, owners := range claimed {
		assert.Lenf(t, owners, 1,
			"selector %s is governed by %d PodDisruptionBudgets — %v. Kubernetes documents a "+
				"pod matched by several budgets as unsupported: both are admitted and which "+
				"one the eviction API honours is undefined, so the budget actually protecting "+
				"these pods during a node drain is whichever was consulted. Keep the one whose "+
				"name and location state its purpose and delete the other, or give them "+
				"non-overlapping selectors", fingerprint, len(owners), owners)
	}
}

// workloadPodLabels returns the pod-template labels of a workload document.
//
// Only the kinds whose pods an eviction can remove are considered, because those are the only
// ones a PodDisruptionBudget can govern.
//
// Parameters:
//   - document map[string]interface{}: one decoded manifest document.
//
// Returns:
//   - map[string]interface{}: the pod-template labels.
//   - string: the workload's namespace.
//   - bool: whether the document was a workload with pod labels at all.
func workloadPodLabels(document map[string]interface{}) (map[string]interface{}, string, bool) {
	switch document["kind"] {
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet":
	default:
		return nil, "", false
	}

	metadata, _ := document["metadata"].(map[string]interface{})
	namespace, _ := metadata["namespace"].(string)

	spec, isMap := document["spec"].(map[string]interface{})
	if !isMap {
		return nil, "", false
	}

	template, isMap := spec["template"].(map[string]interface{})
	if !isMap {
		return nil, "", false
	}

	templateMetadata, isMap := template["metadata"].(map[string]interface{})
	if !isMap {
		return nil, "", false
	}

	labels, isMap := templateMetadata["labels"].(map[string]interface{})
	if !isMap || len(labels) == 0 {
		return nil, "", false
	}

	return labels, namespace, true
}

// workloadDescription renders a workload as "kind/name (file)" for a failure message.
//
// Parameters:
//   - fileName string: the manifest file.
//   - document map[string]interface{}: the workload document.
//
// Returns:
//   - string: the description.
func workloadDescription(fileName string, document map[string]interface{}) string {
	metadata, _ := document["metadata"].(map[string]interface{})
	name, _ := metadata["name"].(string)

	return fmt.Sprintf("%v/%s (%s)", document["kind"], name, fileName)
}

// labelsSatisfy reports whether every label in a selector is present with the same value in a
// workload's pod labels, which is what matchLabels means.
//
// Parameters:
//   - podLabels map[string]interface{}: the workload's pod-template labels.
//   - selector map[string]interface{}: the budget's matchLabels.
//
// Returns:
//   - bool: whether the selector matches.
func labelsSatisfy(podLabels, selector map[string]interface{}) bool {
	for key, want := range selector {
		got, present := podLabels[key]
		if !present || fmt.Sprint(got) != fmt.Sprint(want) {
			return false
		}
	}

	return len(selector) > 0
}

// eventStreamingEnvTagPattern extracts the environment variable name from an envconfig tag
// in config/config.go, restricted to the four families this feature owns.
//
// Reading the tags rather than a written-down list is the whole point: a list would be a
// second copy of the contract, and a second copy is what let eleven documented knobs go
// unforwarded while every file involved read correctly on its own.
var eventStreamingEnvTagPattern = regexp.MustCompile(
	`envconfig:"((?:KAFKA|RELAY|WEBHOOK|EVENT)_[A-Z0-9_]+)"`,
)

// eventStreamingEnvTags returns every environment variable name config/config.go resolves in
// the Kafka, relay, webhook-window and event-metrics families.
//
// WEBHOOK_DEPRECATION_START_DATE is excluded because it is DERIVED rather than read: the
// window's opening instant is computed from the sunset date, and config.go carries no
// envconfig tag for it.
func eventStreamingEnvTags(t *testing.T) []string {
	t.Helper()

	source := readRepoFile(t, filepath.Join("config", "config.go"))

	seen := make(map[string]struct{})
	for _, match := range eventStreamingEnvTagPattern.FindAllStringSubmatch(source, -1) {
		if strings.HasPrefix(match[1], "WEBHOOK_DEPRECATION_START") {
			continue
		}

		seen[match[1]] = struct{}{}
	}

	require.NotEmpty(t, seen, "config/config.go must declare event-streaming envconfig tags")

	tags := make([]string, 0, len(seen))
	for tag := range seen {
		tags = append(tags, tag)
	}

	return tags
}

// workerExemptEventStreamingEnv is every event-streaming variable the WORKER service does not
// receive, each with the reason it does not.
//
// It is a declared table rather than an inferred difference so that adding a knob to
// config.go forces a DECISION about the worker instead of defaulting to silence in either
// direction: forget to forward one it needs and the test fails, forward one it cannot use and
// the test fails too. Both mistakes have shipped in this file, in both directions.
//
// The role's boundary is ProcessRole.PublishesEvents, an allowlist naming only the server. So
// the worker builds the no-op publisher, opens no broker connection and administers nothing —
// while still CAPTURING events into the outbox inside the ledger transaction, which is why the
// four keys it does receive are the four capture reads.
var workerExemptEventStreamingEnv = map[string]string{
	"KAFKA_SUBSCRIBER_BROKERS":        "only credential issuance reports it, and only the server serves that endpoint",
	"KAFKA_KEY_SCOPE_ENFORCEMENT":     "the same: it gates issuance, which this role does not serve",
	"KAFKA_KEY_SCOPE_GATEWAY_BROKERS": "the addresses issuance reports under that declaration",
	"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL": "the control endpoint issuance binds a key scope at, and " +
		"deregistration withdraws it from; both endpoints are served by the server alone",
	"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TOKEN": "the credential for that call, and this role never " +
		"makes it — forwarding it would widen what a compromise of the worker is worth for no capability",
	"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TIMEOUT_MS": "it bounds a call this role does not make",
	"KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS": "the deployment's declaration about what a SUBSCRIBER " +
		"credential may read, consulted only where credentials are issued",
	"KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS": "the deployment's declaration about whether a " +
		"SUBSCRIBER may hold the internal category topic, consulted where a grant is validated and " +
		"where ACLs are bound — both of which are subscriber-management operations the server " +
		"serves alone. It cannot affect a capture: an outbox row's destination comes from the event " +
		"type and the configured prefix, never from who may read the topic",
	"KAFKA_HISTORICAL_TOPIC_PREFIXES": "a new row's topic is composed from the CONFIGURED prefix alone, so the " +
		"historical list can never decide whether an insert is accepted; only the publisher, the " +
		"dead-letter write and replay read stored topics, and all three run in the server",
	"KAFKA_SASL_USER":                             "this role opens no broker connection, so it presents no credential",
	"KAFKA_SASL_SECRET":                           "the same",
	"KAFKA_SASL_ADMIN_USER":                       "topic assurance, provisioning, ACL management and offset reads all run in the server",
	"KAFKA_SASL_ADMIN_SECRET":                     "the same",
	"KAFKA_TLS_ENABLED":                           "no broker connection to secure",
	"KAFKA_TLS_CA_FILE":                           "the same",
	"KAFKA_TLS_CERT_FILE":                         "the same",
	"KAFKA_TLS_KEY_FILE":                          "the same",
	"KAFKA_TLS_SERVER_NAME":                       "the same",
	"KAFKA_TLS_INSECURE_SKIP_VERIFY":              "the same",
	"KAFKA_INSECURE_LOCAL_DEV":                    "the same",
	"KAFKA_MIN_PARTITIONS":                        "topic assurance runs in the server, before its relay starts",
	"KAFKA_REPLICATION_FACTOR":                    "the same",
	"KAFKA_ALLOW_PARTITION_GROWTH":                "the same",
	"KAFKA_ALLOW_ADMIN_PRODUCER":                  "it governs how a PUBLISHER authenticates, and this role builds none",
	"RELAY_RETRY_BASE_BACKOFF_MS":                 "only a relay waits between attempts, and only the server runs one",
	"RELAY_RETRY_MAX_BACKOFF_MS":                  "the same",
	"RELAY_EVENT_RETENTION_DAYS":                  "the retention sweeper runs in the server",
	"RELAY_EVENT_RETENTION_BATCH_SIZE":            "the same",
	"RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP": "the same",
	"RELAY_REPAIR_BATCH_SIZE": "the repair sweep runs inside the relay's own tick, and only the " +
		"server hosts a relay. The worker's capture path writes a row and commits; nothing it " +
		"does reads how a later sweep is batched",
	"RELAY_REPAIR_MAX_BATCHES_PER_TICK": "the same: it bounds a sweep the worker never performs",
	"RELAY_REPAIR_CONCURRENCY": "the same, and it mirrors the publish loop's width, " +
		"which is a relay-only structure",
	"RELAY_SUBSCRIBER_METRICS_BUDGET": "the consumer-lag collector needs an admin client, which only the server builds",
	"EVENT_METRICS_SUBSCRIBER_BUDGET": "the same ceiling under its other published spelling",
}

// composeServiceEnvironment returns a service's environment block as a map, preserving a nil
// value for compose's pass-through form.
//
// The nil is the load-bearing part. `BLNK_X:` with nothing after the colon copies the variable
// from .env when it is set there and leaves it ENTIRELY ABSENT when it is not, whereas
// `BLNK_X: ${BLNK_X:-}` sets it to the empty string — and because applyPrefixedEnvAliases
// decides on os.LookupEnv, presence rather than non-emptiness, an empty prefixed value WINS
// over the bare name and blanks it. So the two forms are not interchangeable and this map has
// to be able to tell them apart.
func composeServiceEnvironment(t *testing.T, composeFile, service string) map[string]interface{} {
	t.Helper()

	services := composeServices(t, filepath.Join(moduleRootDir(t), composeFile), composeFile)

	definition, isMap := services[service].(map[string]interface{})
	require.Truef(t, isMap, "%s must define the %s service", composeFile, service)

	environment, isMap := definition["environment"].(map[string]interface{})
	require.Truef(t, isMap, "%s: the %s service must declare an environment map", composeFile, service)

	return environment
}

// eventStreamingKeysOf returns the sorted event-streaming environment keys one service
// receives in one Compose file, under either spelling.
func eventStreamingKeysOf(t *testing.T, composeFile, service string) []string {
	t.Helper()

	environment := composeServiceEnvironment(t, composeFile, service)

	keys := make([]string, 0, len(environment))
	for key := range environment {
		trimmed := strings.TrimPrefix(key, "BLNK_")
		if !strings.HasPrefix(trimmed, "KAFKA_") &&
			!strings.HasPrefix(trimmed, "RELAY_") &&
			!strings.HasPrefix(trimmed, "WEBHOOK_") &&
			!strings.HasPrefix(trimmed, "EVENT_") {
			continue
		}

		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

// TestCompose_ForwardsEveryEventStreamingKnobTheApplicationResolves is M-10, executed.
//
// # The defect
//
// config/config.go resolves a knob, .env.example documents it, and nothing forwarded it into
// the container. Eleven were in that state at once — KAFKA_ALLOW_ADMIN_PRODUCER, the four TLS
// certificate settings, the three retention settings, and both spellings of the consumer-lag
// budget — plus BLNK_KAFKA_SUBSCRIBER_BROKERS, which was documented as resolvable under the
// prefix and forwarded under neither name.
//
// The symptom is silence in the direction that reads as success: an operator sets the key in
// .env, `docker compose up` accepts it, the container never sees it, the application applies
// its default, and nothing anywhere reports a problem. For a retention period that means no
// sweeping; for a TLS certificate path it means an unauthenticated client against a broker
// that accepts both; for the admin-producer switch it means the escape hatch cannot be closed
// or opened deliberately.
//
// # Why this is derived from config.go rather than listed here
//
// A list in a test is a second copy of the contract, and the reason eleven knobs drifted is
// that every copy of the contract read correctly on its own. The tags in config.go are the one
// definition every other file is a projection of, so this test reads them and compares.
func TestCompose_ForwardsEveryEventStreamingKnobTheApplicationResolves(t *testing.T) {
	tags := eventStreamingEnvTags(t)

	for _, composeFile := range composeFiles {
		t.Run(composeFile, func(t *testing.T) {
			serverEnv := composeServiceEnvironment(t, composeFile, "server")

			t.Run("the server receives every knob under both of its names", func(t *testing.T) {
				for _, tag := range tags {
					_, bare := serverEnv[tag]
					assert.Truef(t, bare,
						"%s: the server service must forward %s. config/config.go resolves it and "+
							".env.example documents it, so an operator who sets it in .env and sees "+
							"compose accept the bring-up has no way to learn it never reached the "+
							"process", composeFile, tag)

					_, alias := serverEnv["BLNK_"+tag]
					assert.Truef(t, alias,
						"%s: the server service must also forward BLNK_%s. config/config.go "+
							"documents every one of these keys as resolvable under the BLNK_ prefix, "+
							"with the prefixed form winning when both are set; a promise that holds "+
							"for a host-run binary and not for the compose stack is worse than no "+
							"promise", composeFile, tag)
				}
			})

			t.Run("every prefixed alias uses the pass-through form", func(t *testing.T) {
				// ONLY THE ALIASES OF THIS FAMILY. A top-level setting whose envconfig tag
				// already carries the prefix — BLNK_ENABLE_OBSERVABILITY,
				// BLNK_METRICS_BEARER_TOKEN — is resolved by envconfig itself under that one
				// name, so `${...:-default}` is the right form for it and shipping a default
				// is the point. The pass-through requirement is specific to the names
				// applyPrefixedEnvAliases overlays ON TOP OF a bare name.
				aliases := make(map[string]struct{}, len(tags))
				for _, tag := range tags {
					aliases["BLNK_"+tag] = struct{}{}
				}

				for key, value := range serverEnv {
					if _, isAlias := aliases[key]; !isAlias {
						continue
					}

					assert.Nilf(t, value,
						"%s: %s must have NOTHING after its colon. `${...}` would set it to the "+
							"empty string, and applyPrefixedEnvAliases decides on presence rather "+
							"than non-emptiness — so an empty prefixed value WINS over the bare name "+
							"and blanks it, disabling the very key it was meant to forward. Got %#v",
						composeFile, key, value)
				}
			})

			t.Run("the worker receives what capture reads and nothing more", func(t *testing.T) {
				workerEnv := composeServiceEnvironment(t, composeFile, "worker")

				for _, tag := range tags {
					_, forwarded := workerEnv[tag]
					reason, exempt := workerExemptEventStreamingEnv[tag]

					switch {
					case exempt:
						// LEAST PRIVILEGE, ASSERTED IN THE OTHER DIRECTION. Forwarding a key
						// this role cannot use is not harmless: for a credential it widens
						// what a compromise of the process is worth, and for anything else it
						// implies the role honours a setting it never reads.
						assert.Falsef(t, forwarded,
							"%s: the worker service must NOT forward %s — %s", composeFile, tag, reason)
						_, aliasForwarded := workerEnv["BLNK_"+tag]
						assert.Falsef(t, aliasForwarded,
							"%s: nor BLNK_%s, for the same reason — %s", composeFile, tag, reason)
					default:
						assert.Truef(t, forwarded,
							"%s: the worker service must forward %s, or record it in "+
								"workerExemptEventStreamingEnv with the reason it does not. The "+
								"worker CAPTURES events into the outbox inside the ledger "+
								"transaction, so a capture-relevant knob it cannot see is a row "+
								"written with the wrong destination or the wrong retry budget",
							composeFile, tag)
						_, aliasForwarded := workerEnv["BLNK_"+tag]
						assert.Truef(t, aliasForwarded,
							"%s: and BLNK_%s, so the prefixed spelling works in both roles",
							composeFile, tag)
					}
				}
			})
		})
	}
}

// TestCompose_BothProjectionsForwardTheSameEventStreamingKeys asserts the production and
// development Compose files agree, key for key and role for role.
//
// They are edited by hand, in step, and the development one is the copy a contributor reaches
// for — so a knob added to only one of them produces a stack that behaves differently from the
// one CI and production run, which is the hardest kind of difference to attribute.
func TestCompose_BothProjectionsForwardTheSameEventStreamingKeys(t *testing.T) {
	require.Len(t, composeFiles, 2, "this comparison assumes exactly two projections")

	for _, service := range []string{"server", "worker"} {
		t.Run(service, func(t *testing.T) {
			first := eventStreamingKeysOf(t, composeFiles[0], service)
			second := eventStreamingKeysOf(t, composeFiles[1], service)

			assert.Equal(t, first, second,
				"%s and %s must forward the SAME event-streaming keys to %s; a key in one and not "+
					"the other makes the development stack behave differently from the one CI and "+
					"production run", composeFiles[0], composeFiles[1], service)
		})
	}
}

// TestRelayTarget_RequiresAnActuallyUsableBrokerList is the M-15 guard.
//
// # What was wrong
//
// `make run_relay` decided whether blnk.json counted as a configured broker source with
//
//	grep -q '"brokers"' "${CONFIG_FILE}"
//
// and `"kafka": { "brokers": [] }` contains that string. So an empty array passed the check, the
// target announced the file as its configuration source and started the server — which then
// resolved no writer, ran no event relay, and left every captured event sitting in
// blnk.event_outbox at status `pending` while the HTTP API answered normally and every probe
// stayed green. The whole purpose of the broker check is to refuse exactly that state before it
// starts, so a check that a declared-but-empty array satisfies is worse than none: it converts a
// loud misconfiguration into a silent one.
//
// `"brokers": [""]` and `"brokers": ["  "]` are the same defect wearing a different hat — a blank
// string is not a broker, and kafka-go would fail to dial it just as surely.
//
// # Why this is asserted behaviourally and not only textually
//
// The text assertion pins the fix; the exec assertion pins the BEHAVIOUR, and only the second
// one would notice a JSON expression that was subtly wrong — a jq filter that returned a
// non-empty array as false, say, or a python snippet that raised on a missing "kafka" key and
// was read as success. Both halves are here because the textual one still runs where `make` is
// not on PATH.
func TestRelayTarget_RequiresAnActuallyUsableBrokerList(t *testing.T) {
	root := moduleRootDir(t)
	makefile := readRepoFile(t, "makefile")

	// COMMENTS ARE STRIPPED BEFORE THE ABSENCE IS ASSERTED, the same rule
	// event_loadtest_contract_test.go applies for the same reason. The rationale comment above
	// the replacement target deliberately QUOTES the old `grep -q '"brokers"'` form while
	// explaining why it was wrong, and prose that names a removed construct must not be able to
	// fail an assertion about code.
	recipes := make([]string, 0, 64)
	for _, line := range strings.Split(makefile, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		recipes = append(recipes, line)
	}
	executable := strings.Join(recipes, "\n")

	assert.NotContains(t, executable, `grep -q '"brokers"'`,
		"makefile: a substring test for \"brokers\" is satisfied by an EMPTY array, which is "+
			"the misconfiguration the check exists to refuse. Parse the array instead")
	assert.Contains(t, executable, "BROKER_ARRAY_DECLARED",
		"makefile: run_server_relay must delegate the broker-array question to something that "+
			"actually parses JSON")

	// AND IT MUST NOT DO SO WITH $(MAKE). GNU make executes any recipe line containing $(MAKE)
	// even under -n, so that a sub-make can print its own commands. run_server_relay's recipe is
	// a single backslash-continued line ending in `exec ./blnk start`, so a $(MAKE) anywhere in
	// it turns `make -n run_relay` into a real server start. That is not cosmetic: the server
	// hosts the event relay, the relay claims blnk.event_outbox rows, and the event and outbox
	// suites require it to be DOWN. The first version of this fix had exactly that bug.
	relayRecipe := executable
	if start := strings.Index(relayRecipe, "\nrun_server_relay:\n"); start >= 0 {
		relayRecipe = relayRecipe[start:]
		if end := strings.Index(relayRecipe, "\nexec ./"); end >= 0 {
			relayRecipe = relayRecipe[:end]
		}
	}
	assert.NotContains(t, relayRecipe, "$(MAKE)",
		"makefile: run_server_relay must not invoke $(MAKE). Make runs such a line even under "+
			"-n, so `make -n run_relay` would reach `exec ./blnk start` and actually start a "+
			"server. Expand a variable instead")

	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("make is not on PATH; the textual assertions above still applied")
	}

	// Each shape, and the answer the relay target depends on. rc 0 means "declared and
	// usable", any non-zero means "do not treat this file as a broker source".
	for _, shape := range []struct {
		name    string
		config  string
		usable  bool
		because string
	}{
		{
			name:    "empty array",
			config:  `{"kafka":{"brokers":[]}}`,
			usable:  false,
			because: "this is the original defect: the key is present and there is no broker",
		},
		{
			name:    "single blank entry",
			config:  `{"kafka":{"brokers":[""]}}`,
			usable:  false,
			because: "an empty string is not an address kafka-go can dial",
		},
		{
			name:    "whitespace-only entry",
			config:  `{"kafka":{"brokers":["   "]}}`,
			usable:  false,
			because: "whitespace is not an address either, and trims to nothing",
		},
		{
			name:    "no brokers key",
			config:  `{"kafka":{}}`,
			usable:  false,
			because: "the kafka block exists but declares no brokers",
		},
		{
			name:    "no kafka block",
			config:  `{}`,
			usable:  false,
			because: "a config file with no kafka block declares no brokers",
		},
		{
			name:    "brokers is a string, not an array",
			config:  `{"kafka":{"brokers":"a:9092"}}`,
			usable:  false,
			because: "the field is []string; a bare string is malformed and must not be guessed at",
		},
		{
			name:    "not JSON at all",
			config:  `this is not json`,
			usable:  false,
			because: "an unparseable file must not be reported as a configured source",
		},
		{
			name:    "one real broker",
			config:  `{"kafka":{"brokers":["kafka:9092"]}}`,
			usable:  true,
			because: "this is the case the relay target must accept",
		},
		{
			name:    "a blank entry alongside a real one",
			config:  `{"kafka":{"brokers":["","kafka:9092"]}}`,
			usable:  true,
			because: "one usable address is enough to bootstrap; kafka-go discovers the rest",
		},
	} {
		t.Run(shape.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "blnk.json")
			require.NoError(t, os.WriteFile(path, []byte(shape.config), 0o600))

			cmd := exec.Command("make", "--no-print-directory", "-s",
				"_brokers_declared_in_file", "CONFIG_FILE="+path)
			cmd.Dir = root
			output, err := cmd.CombinedOutput()

			if shape.usable {
				assert.NoErrorf(t, err,
					"%s must be reported as a usable broker source (%s).\n--- output ---\n%s",
					shape.config, shape.because, output)
				return
			}
			assert.Errorf(t, err,
				"%s must NOT be reported as a usable broker source (%s). Accepting it starts a "+
					"server whose relay never runs, with events accumulating in the outbox "+
					"behind a healthy-looking API.\n--- output ---\n%s",
				shape.config, shape.because, output)
		})
	}
}
