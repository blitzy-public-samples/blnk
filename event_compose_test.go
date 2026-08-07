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
	"os"
	"path/filepath"
	"strings"
	"testing"

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

			t.Run("the publishing services depend on them optionally", func(t *testing.T) {
				// The server AND the worker: the worker publishes too — transaction.rejected
				// comes from its rejection handler — so a fix applied to one leaves the other
				// unable to start without a broker.
				for _, consumer := range []string{"server", "worker"} {
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
// Worse, that same default was injected into the WORKER, a process that only publishes and has
// no administrative work to do at all.
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

			t.Run("the worker holds no administrative credential", func(t *testing.T) {
				// The worker publishes and never administers. Only cmd/server.go builds an admin
				// client — for topic assurance before the relay starts, and for the offset reads
				// behind reconciliation — so a second copy of the superuser credential widens the
				// credential's exposure for no capability the process uses.
				worker, declared := services["worker"].(map[string]interface{})
				require.True(t, declared, "%s must declare a worker service", composeFile)

				environment, isMap := worker["environment"].(map[string]interface{})
				require.True(t, isMap, "the worker must declare an environment block")

				for _, key := range []string{"KAFKA_SASL_ADMIN_USER", "KAFKA_SASL_ADMIN_SECRET"} {
					assert.NotContains(t, environment, key,
						"the worker must not receive %s: it publishes and never administers, and "+
							"config.KafkaConfig accepts an EMPTY administrative pair — what it "+
							"refuses is a half-configured one", key)
				}

				// And it must still receive the producer pair, or it cannot publish at all: with
				// an administrative pair configured elsewhere and no producer pair here, the event
				// publisher refuses to construct and this process does not start.
				for _, key := range []string{"KAFKA_SASL_USER", "KAFKA_SASL_SECRET"} {
					assert.Contains(t, environment, key,
						"the worker must receive %s: it publishes transaction.rejected from its "+
							"rejection handler, and Blnk will not publish as the administrator", key)
				}
			})

			t.Run("the publishing services default their producer pair from one source", func(t *testing.T) {
				// One value in .env both mints the principal on the broker (KAFKA_PRODUCER_*, read
				// by scripts/kafka-provision.sh) and is presented by the publishing processes
				// (KAFKA_SASL_*). Two independent values would drift, and the symptom of drift is
				// a SASL handshake failure that reads exactly like a wrong password.
				for _, consumer := range []string{"server", "worker"} {
					service := services[consumer].(map[string]interface{})
					environment := service["environment"].(map[string]interface{})

					assert.Contains(t, environment["KAFKA_SASL_USER"], "KAFKA_PRODUCER_USER",
						"%s's KAFKA_SASL_USER must default from KAFKA_PRODUCER_USER, the key "+
							"scripts/kafka-provision.sh creates the principal from", consumer)
					assert.Contains(t, environment["KAFKA_SASL_SECRET"], "KAFKA_PRODUCER_SECRET",
						"%s's KAFKA_SASL_SECRET must default from KAFKA_PRODUCER_SECRET, so the "+
							"credential the broker holds and the one this process presents are one "+
							"line of .env", consumer)
				}
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
