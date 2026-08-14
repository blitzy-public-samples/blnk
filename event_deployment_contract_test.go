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

// EXECUTABLE coverage of two deployment surfaces that decide whether the relay runs at
// all and whether the broker survives sustained load: the `run_relay` make target's
// broker resolution, and the Kafka StatefulSet's storage budget.
//
// The make recipe reimplements, in shell, a precedence rule that config/config.go
// implements in Go — and reimplemented it wrongly, in a direction that makes the guard
// ANNOUNCE a different broker list from the one the application then uses.
package blnk

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------------------
// `make run_relay` resolves brokers exactly as the application does.
// ---------------------------------------------------------------------------------------

// relayAliases are the three environment names the application accepts for the broker
// list, in its OWN precedence order, highest first.
var relayAliases = []string{
	"BLNK_KAFKA_BROKERS",
	"KAFKA_BROKERS",
	"BLNK_KAFKA_KAFKA_BROKERS",
}

// relayMakeHarness is a temporary tree in which `make run_relay` can be executed without
// starting anything.
type relayMakeHarness struct {
	dir string
	// effective is where the stub `blnk` records the environment and argv it was executed with.
	effective string
}

// newRelayMakeHarness copies the real makefile into a temporary tree and installs a
// stub `blnk`.
//
// The makefile is COPIED rather than paraphrased.
//
// Parameters:
//   - t *testing.T: the test.
//   - dotenv string: the contents of .env, or empty for no file.
//   - configFile string: the contents of blnk.json, or empty for no file.
//
// Returns:
//   - relayMakeHarness: the prepared tree.
func newRelayMakeHarness(t *testing.T, dotenv, configFile string) relayMakeHarness {
	t.Helper()

	root := moduleRootDir(t)
	dir := t.TempDir()

	makefile, err := os.ReadFile(filepath.Join(root, "makefile"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "makefile"), makefile, 0o644))

	if dotenv != "" {
		require.NoError(t, os.WriteFile(filepath.Join(dir, ".env"), []byte(dotenv), 0o600))
	}
	if configFile != "" {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "blnk.json"), []byte(configFile), 0o644))
	}

	harness := relayMakeHarness{dir: dir, effective: filepath.Join(dir, "effective.txt")}

	// THE STUB IS THE APPLICATION'S SEAT. It records exactly what `./blnk start` would
	// have inherited, which is the only thing that decides whether the relay runs: the
	// recipe's announcement is a claim ABOUT that environment, and the two must agree.
	stub := "#!/usr/bin/env bash\n" +
		"{\n" +
		"  echo \"ARGV=$*\"\n" +
		"  for name in " + strings.Join(relayAliases, " ") + "; do\n" +
		"    eval \"present=\\${$name+set}\"\n" +
		"    eval \"value=\\${$name-}\"\n" +
		"    echo \"$name ${present:-unset} $value\"\n" +
		"  done\n" +
		"} > " + shellQuote(harness.effective) + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "blnk"), []byte(stub), 0o755))

	return harness
}

// run invokes `make run_relay` with the given caller environment.
//
// Parameters:
//   - t *testing.T: the test.
//   - caller map[string]string: the caller's environment. A value of relayUnset means
//     the name is left unset, which is a different input from an empty string and the
//     distinction this whole test exists for.
//
// Returns:
//   - string: combined stdout and stderr.
//   - int: the exit status.
func (h relayMakeHarness) run(t *testing.T, caller map[string]string) (string, int) {
	t.Helper()

	command := exec.Command("make", "run_relay")
	command.Dir = h.dir
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
	}
	for name, value := range caller {
		command.Env = append(command.Env, name+"="+value)
	}

	output, err := command.CombinedOutput()
	status := 0
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "make could not be executed: %v", err)
		status = exitErr.ExitCode()
	}

	return string(output), status
}

// effectiveAlias reports what the stub `blnk` observed for one alias.
//
// Parameters:
//   - t *testing.T: the test, failed when the stub did not run.
//   - name string: the alias.
//
// Returns:
//   - bool: whether the variable was set at all.
//   - string: its value.
func (h relayMakeHarness) effectiveAlias(t *testing.T, name string) (bool, string) {
	t.Helper()

	raw, err := os.ReadFile(h.effective)
	require.NoError(t, err, "the stub blnk must have run; if it did not, the guard refused and "+
		"this case expected it to start")

	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.SplitN(line, " ", 3)
		if len(fields) < 2 || fields[0] != name {
			continue
		}

		value := ""
		if len(fields) == 3 {
			value = fields[2]
		}

		return fields[1] == "set", value
	}

	t.Fatalf("the stub recorded no observation for %s", name)

	return false, ""
}

// effectiveArgv reports the command line the stub `blnk` was executed with.
//
// It is the observable that replaced the recipe's own exit status.
//
// Parameters:
//   - t *testing.T: the test, failed when the stub did not run.
//
// Returns:
//   - string: the arguments, as the stub recorded them.
func (h relayMakeHarness) effectiveArgv(t *testing.T) string {
	t.Helper()

	raw, err := os.ReadFile(h.effective)
	require.NoError(t, err, "the stub blnk must have run; the recipe execs it unconditionally and "+
		"leaves the refusal to the application")

	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "ARGV=") {
			return strings.TrimPrefix(line, "ARGV=")
		}
	}

	t.Fatal("the stub recorded no argv")

	return ""
}

// applicationEffectiveBrokers reports the broker list the application would resolve
// from an environment, using envconfig's own rule: the first name that is SET wins,
// whatever its value.
//
// This is the oracle the make recipe is tested against.
//
// Parameters:
//   - environment map[string]string: alias to value, for the aliases that are set.
//   - present map[string]bool: which aliases are set at all.
//
// Returns:
//   - string: the winning alias, or "" when none is set.
//   - string: its value.
func applicationEffectiveBrokers(
	environment map[string]string, present map[string]bool,
) (string, string) {
	for _, alias := range relayAliases {
		if present[alias] {
			return alias, environment[alias]
		}
	}

	return "", ""
}

// TestMakeRunRelay_ResolvesBrokersExactlyAsTheApplicationDoes is the broker-resolution contract, executed.
//
// A server started with no broker configured comes up looking healthy while every
// captured event stays pending in blnk.event_outbox, so something has to refuse.
//
// No shell reimplementation of the loader's precedence can be trusted to agree with the
// loader, and a guard that disagrees is worse than none: it either refuses a deployment
// the application would have admitted, or admits one whose relay never starts, and in
// both cases it says so confidently. So the recipe passes `--require-kafka` and the
// APPLICATION asks the question after the same load, of the same struct, with the same
// predicate startEventRelay gates on — and its refusal names every source it read.
//
// Assembling the environment the application resolves FROM.
//
// Replaying an `export -p` snapshot restores values the caller HAD but cannot remove a
// name .env introduced, so a caller who named any alias also suppresses the ones they
// did not name — otherwise BLNK_KAFKA_BROKERS parked in .env outvotes
// `KAFKA_BROKERS=host:9092 make run_relay`, and a defaults file beats an explicit
// override.
func TestMakeRunRelay_ResolvesBrokersExactlyAsTheApplicationDoes(t *testing.T) {
	const dotenvPrefixed = "BLNK_KAFKA_BROKERS=env-prefixed:9092\n"
	const dotenvBare = "KAFKA_BROKERS=env-bare:9092\n"
	const configWithBrokers = `{"kafka":{"brokers":["file:9092"]}}`

	for _, scenario := range []struct {
		name string
		// dotenv and configFile are the files present in the tree.
		dotenv     string
		configFile string
		// caller is the environment `make` is invoked with. An entry with an empty value models
		// `NAME= make run_relay`, which is SET and empty.
		caller map[string]string
		// starts is whether the guard must let the server start.
		starts bool
		// wins is the alias the recipe must announce and the application must resolve, empty when
		// the answer comes from the config file or nothing starts.
		wins string
		// wantValue is the broker list the application must end up with.
		wantValue string
		// suppressed lists aliases that must NOT reach the application, because .env introduced
		// them and the caller had already decided.
		suppressed []string
	}{
		{
			name:   "nothing set anywhere is refused",
			starts: false,
		},
		{
			name:       "the config file alone is enough to start",
			configFile: configWithBrokers,
			starts:     true,
		},
		{
			name:      "the bare contract name from the caller",
			caller:    map[string]string{"KAFKA_BROKERS": "caller-bare:9092"},
			starts:    true,
			wins:      "KAFKA_BROKERS",
			wantValue: "caller-bare:9092",
		},
		{
			name:      "the derived nested key alone",
			caller:    map[string]string{"BLNK_KAFKA_KAFKA_BROKERS": "derived:9092"},
			starts:    true,
			wins:      "BLNK_KAFKA_KAFKA_BROKERS",
			wantValue: "derived:9092",
		},
		{
			name: "the prefixed alias OUTRANKS the bare name",
			caller: map[string]string{
				"KAFKA_BROKERS":      "caller-bare:9092",
				"BLNK_KAFKA_BROKERS": "caller-prefixed:9092",
			},
			starts:    true,
			wins:      "BLNK_KAFKA_BROKERS",
			wantValue: "caller-prefixed:9092",
		},
		{
			name: "the bare name OUTRANKS the derived nested key",
			caller: map[string]string{
				"KAFKA_BROKERS":            "caller-bare:9092",
				"BLNK_KAFKA_KAFKA_BROKERS": "derived:9092",
			},
			starts:    true,
			wins:      "KAFKA_BROKERS",
			wantValue: "caller-bare:9092",
		},
		{
			name:      ".env supplies a default when the caller expressed no opinion",
			dotenv:    dotenvPrefixed,
			starts:    true,
			wins:      "BLNK_KAFKA_BROKERS",
			wantValue: "env-prefixed:9092",
		},
		{
			name:      "the caller OUTRANKS .env for the same name",
			dotenv:    dotenvBare,
			caller:    map[string]string{"KAFKA_BROKERS": "caller-bare:9092"},
			starts:    true,
			wins:      "KAFKA_BROKERS",
			wantValue: "caller-bare:9092",
		},
		{
			// THE CASE THE OLD RECIPE GOT WRONG IN BOTH WAYS AT ONCE.
			name:       "an explicitly EMPTY caller value is refused even though .env declares a higher alias",
			dotenv:     dotenvPrefixed,
			caller:     map[string]string{"KAFKA_BROKERS": ""},
			starts:     false,
			suppressed: []string{"BLNK_KAFKA_BROKERS"},
		},
		{
			name:       "an explicitly EMPTY caller value beats the config file too",
			dotenv:     dotenvPrefixed,
			configFile: configWithBrokers,
			caller:     map[string]string{"KAFKA_BROKERS": ""},
			starts:     false,
			suppressed: []string{"BLNK_KAFKA_BROKERS"},
		},
		{
			name:       "a caller alias suppresses .env's OTHER aliases rather than being outranked by them",
			dotenv:     dotenvPrefixed,
			caller:     map[string]string{"KAFKA_BROKERS": "caller-bare:9092"},
			starts:     true,
			wins:       "KAFKA_BROKERS",
			wantValue:  "caller-bare:9092",
			suppressed: []string{"BLNK_KAFKA_BROKERS"},
		},
	} {
		target := scenario

		t.Run(target.name, func(t *testing.T) {
			harness := newRelayMakeHarness(t, target.dotenv, target.configFile)
			output, status := harness.run(t, target.caller)

			require.Equal(t, 0, status,
				"the recipe itself must not refuse: it hands the application an environment and "+
					"lets --require-kafka decide; output:\n%s", output)

			// THE REFUSAL IS DELEGATED, and that is observable in argv rather than in a status.
			// Without this flag a deployment with no broker starts a server whose relay never
			// runs, and nothing says so.
			argv := harness.effectiveArgv(t)
			assert.Contains(t, argv, "--require-kafka",
				"the application must be asked to refuse a deployment with no broker; the recipe "+
					"no longer knows enough to refuse one itself")

			// AND THE RECIPE NAMES NO SOURCE OF ITS OWN. Only the loader knows which alias wins,
			// so an announcement made in the recipe can name one the application did not use —
			// which is exactly the defect the inverted precedence produced.
			for _, alias := range relayAliases {
				assert.NotContains(t, output, "("+alias+")",
					"the recipe announced %s as the source it resolved from; the application's "+
						"own refusal is what names the sources it read", alias)
			}

			// WHAT THE APPLICATION WOULD SEE, read off the process that stood in for it.
			observed := map[string]string{}
			present := map[string]bool{}
			for _, alias := range relayAliases {
				isSet, value := harness.effectiveAlias(t, alias)
				present[alias] = isSet
				observed[alias] = value
			}

			for _, alias := range target.suppressed {
				assert.False(t, present[alias],
					"%s reached the application even though the caller had already decided the "+
						"broker list. .env supplies DEFAULTS; it must not outvote a caller who "+
						"named an alias, and this alias outranks the one they named", alias)
			}

			winner, value := applicationEffectiveBrokers(observed, present)

			if !target.starts {
				// THE APPLICATION MUST END UP WITH NOTHING, which is what makes --require-kafka
				// refuse. Either no alias reached it and no config file declared a list, or an
				// alias reached it SET AND EMPTY — which envconfig reads as configured-with-none
				// and which therefore CLEARS whatever a lower-precedence source supplied.
				if winner == "" {
					assert.Empty(t, target.configFile,
						"no alias decided this case, so only the config file could still supply "+
							"a broker list — and it must not, or the application would start")
				} else {
					assert.Empty(t, value,
						"%s reached the application with a usable broker list, so --require-kafka "+
							"would admit a deployment this case expects it to refuse", winner)
				}

				return
			}

			if target.wins != "" {
				assert.Equal(t, target.wins, winner,
					"the application would resolve its brokers from %s, not %s: envconfig takes "+
						"the first name that is SET in the order %s", winner, target.wins,
					strings.Join(relayAliases, " > "))
				assert.Equal(t, target.wantValue, value,
					"the application would use a different broker list from the one intended")
			} else {
				assert.Empty(t, winner,
					"no environment alias should have decided this case")
				assert.Contains(t, output, "blnk.json",
					"a run that starts on the config file must say so")
			}
		})
	}
}

// TestMakeRunRelay_PassesNoSecretOnTheCommandLine is the CWE-214 companion.
//
// The recipe sources .env, which holds the SASL producer and administrative secrets at
// mode 0600, and then execs the server.
func TestMakeRunRelay_PassesNoSecretOnTheCommandLine(t *testing.T) {
	const secret = "make-relay-fixture-secret-7c31ab90de"

	harness := newRelayMakeHarness(t,
		"KAFKA_BROKERS=env-bare:9092\nKAFKA_SASL_SECRET="+secret+"\n", "")

	output, status := harness.run(t, nil)
	require.Equal(t, 0, status, "output:\n%s", output)

	// Asserted as a boolean so the secret is not rendered into a failure message.
	assert.False(t, strings.Contains(output, secret),
		"the recipe ECHOED a credential from .env. Its announcement is written to the terminal "+
			"and to whatever captures CI output; nothing from .env beyond the broker list may "+
			"appear there")

	recorded, err := os.ReadFile(harness.effective)
	require.NoError(t, err)
	for _, line := range strings.Split(string(recorded), "\n") {
		if strings.HasPrefix(line, "ARGV=") {
			assert.False(t, strings.Contains(line, secret),
				"a credential reached the server's command line; it must travel in the "+
					"environment, which is where .env put it")
			// NO blnk.json EXISTS IN THIS HARNESS, so --config is correctly absent. This
			// assertion used to demand `--config blnk.json` here, which pinned the defect
			// rather than the contract: naming a file that is not there is fatal in
			// cmd/main.go, so the invocation it required could not start. The config-flag
			// contract in all three of its cases is held by
			// TestMakeRunRelay_PassesConfigOnlyWhenItMeansSomething below; this one cares only
			// that nothing beyond the role and the delegating flag reaches argv.
			assert.Equal(t, `ARGV=start --require-kafka`, line,
				"the recipe must exec the server with exactly the delegating invocation and "+
					"nothing else: the role, and the flag that makes the application refuse a "+
					"deployment with no broker. No blnk.json exists here, so --config must be "+
					"omitted and the application configured from its environment")
		}
	}
}

// TestMakeRunRelay_PassesConfigOnlyWhenItMeansSomething holds the config-flag contract of the
// relay recipe in all three of its cases.
//
// WHAT WENT WRONG. The recipe passed `--config "${CONFIG_FILE}"` unconditionally. cmd/main.go
// draws a deliberate line — an explicitly NAMED configuration file that cannot be read is
// fatal, while the DEFAULT path's absence is tolerated, because environment-only configuration
// is a first-class mode and is how the compose stack, the Kubernetes manifests and this entire
// test suite run. Passing the flag always erased that line: it turned the tolerated case into
// the fatal one, so `make run_relay` was the only way to start Blnk that could not start it
// from its environment. A clean checkout configured through `.env` — the arrangement
// `stack.sh --init` produces and the runbook documents — failed with the application demanding
// a file no instruction had asked anyone to create. Every other run target passes no `--config`
// at all, so this was the odd one out as well as the broken one.
//
// The fix is not "never pass it" either, which would break `make run_relay
// CONFIG_FILE=/etc/blnk/prod.json` by silently ignoring the path. The flag has to be passed
// when it means something and omitted when it does not, and that is three distinct cases.
func TestMakeRunRelay_PassesConfigOnlyWhenItMeansSomething(t *testing.T) {
	const configWithBroker = `{"kafka":{"brokers":["file-declared:9092"]}}`

	t.Run("no config file and none named: the flag is OMITTED", func(t *testing.T) {
		// THE CASE THAT WAS BROKEN. The application must be left to configure itself from the
		// environment, exactly as `make run` already does.
		harness := newRelayMakeHarness(t, "KAFKA_BROKERS=env-bare:9092\n", "")

		output, status := harness.run(t, nil)
		require.Equal(t, 0, status, "output:\n%s", output)

		argv := harness.effectiveArgv(t)
		assert.NotContains(t, argv, "--config",
			"with no blnk.json present and none named, --config must be omitted. Naming a file "+
				"that is not there is fatal in cmd/main.go, so passing it here is the difference "+
				"between a relay that starts from .env and one that refuses to start at all")
		assert.Contains(t, argv, "--require-kafka",
			"omitting --config must not also drop the delegating flag; the application still has "+
				"to refuse a deployment with no broker")

		// AND IT MUST SAY SO. An operator who expected a file to be read needs to know one was
		// not, or a stale blnk.json in another directory becomes a long debugging session.
		assert.Contains(t, output, "--config is not passed",
			"the recipe must announce that it is configuring the application from the "+
				"environment rather than from a file")
	})

	t.Run("a config file IS present: the flag is passed", func(t *testing.T) {
		// Unchanged behaviour for everyone who has a blnk.json, which is also the file the
		// broker-declaration gate reads.
		harness := newRelayMakeHarness(t, "", configWithBroker)

		output, status := harness.run(t, nil)
		require.Equal(t, 0, status, "output:\n%s", output)

		assert.Contains(t, harness.effectiveArgv(t), "--config blnk.json",
			"a blnk.json that exists must still be passed, or the recipe would ignore the file "+
				"an operator put there and start from a different configuration than the gate read")
	})

	t.Run("a NAMED file is passed even when it is absent", func(t *testing.T) {
		// Naming a file is a statement that the file holds the configuration. Omitting the flag
		// here would silently start the application from somewhere else, which is precisely the
		// typo-becomes-a-deployment failure cmd/main.go's fatal diagnostic exists to prevent.
		// The recipe must therefore pass it and let the application refuse.
		harness := newRelayMakeHarness(t, "KAFKA_BROKERS=env-bare:9092\n", "")

		command := exec.Command("make", "run_relay", "CONFIG_FILE=absent-on-purpose.json")
		command.Dir = harness.dir
		command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
		output, err := command.CombinedOutput()
		require.NoError(t, err, "the recipe must not refuse; the application decides. output:\n%s", output)

		assert.Contains(t, harness.effectiveArgv(t), "--config absent-on-purpose.json",
			"a file named on the command line must reach the application even though it is not "+
				"there, so the application can report that the file it was told to read is "+
				"missing. Omitting it would start from the environment and never mention the path")
	})
}

// ---------------------------------------------------------------------------------------
// The Kafka StatefulSet's storage budget is derived, so it is checked.
// ---------------------------------------------------------------------------------------

// kafkaStatefulSetPath and kafkaConfigPath are the two manifests the storage budget spans.
const (
	kafkaStatefulSetPath = "infrastructure/k8s-manifests/kafka-statefulset.yaml"
	kafkaConfigPath      = "infrastructure/k8s-manifests/blnk-config.yaml"
)

// kafkaRetentionHeadroom is the fraction of the volume the log data is budgeted to
// occupy.
const kafkaRetentionHeadroom = 0.60

// TestKafkaStatefulSet_StorageCoversThePerBrokerRetentionFloor derives the claim rather
// than reading it.
//
// The claim was 10Gi at one point, which is under three hours of a 2 KiB event at
// 500/second.
func TestKafkaStatefulSet_StorageCoversThePerBrokerRetentionFloor(t *testing.T) {
	root := moduleRootDir(t)

	statefulSet := readYAMLFile(t, filepath.Join(root, kafkaStatefulSetPath))
	spec, ok := statefulSet["spec"].(map[string]interface{})
	require.True(t, ok, "%s must declare a spec", kafkaStatefulSetPath)

	brokers := yamlInt(t, spec, "replicas")
	require.Positive(t, brokers, "the broker count decides how the partitions are spread")

	// The geometry comes from the code that creates the topics, not from a number written
	// down beside the volume: EnsureTopics provisions one topic and one .dlt sibling per
	// category, so adding a category adds two topics and this floor rises with it
	// automatically.
	categories := len(model.AllEventCategories())
	require.Positive(t, categories, "the topic catalogue must not be empty")
	topics := categories * 2

	config := readYAMLFile(t, filepath.Join(root, kafkaConfigPath))
	configData, ok := config["data"].(map[string]interface{})
	require.True(t, ok, "%s must declare a data map", kafkaConfigPath)

	partitions, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(configData["KAFKA_MIN_PARTITIONS"])))
	require.NoError(t, err, "KAFKA_MIN_PARTITIONS must be numeric in %s", kafkaConfigPath)
	replication, err := strconv.Atoi(
		strings.TrimSpace(fmt.Sprint(configData["KAFKA_REPLICATION_FACTOR"])))
	require.NoError(t, err, "KAFKA_REPLICATION_FACTOR must be numeric in %s", kafkaConfigPath)

	require.LessOrEqual(t, replication, brokers,
		"a replication factor above the broker count makes topic creation fail outright with "+
			"INVALID_REPLICATION_FACTOR: %d replicas requested across %d brokers",
		replication, brokers)

	retentionBytes := statefulSetRetentionBytes(t, filepath.Join(root, kafkaStatefulSetPath))

	// EVERY broker holds `replication` copies of each partition spread over `brokers`
	// brokers, so its share is the whole catalogue scaled by replication/brokers. With RF
	// equal to the broker count that is the whole catalogue, which is the case this
	// deployment is in and the one an eyeballed figure gets wrong.
	partitionsPerBroker := float64(topics*partitions) * float64(replication) / float64(brokers)
	logBytesPerBroker := partitionsPerBroker * float64(retentionBytes)
	floorBytes := logBytesPerBroker / kafkaRetentionHeadroom

	claimed := statefulSetClaimedStorageBytes(t, spec)

	assert.GreaterOrEqual(t, float64(claimed), floorBytes,
		"THE KAFKA VOLUME IS TOO SMALL FOR ITS OWN RETENTION BUDGET. %d topics (%d categories "+
			"and their .dlt siblings) x %d partitions x RF %d over %d brokers puts %.0f "+
			"partitions on every broker; at log.retention.bytes=%d per partition that is %.1f "+
			"GiB of log data, and Kafka takes a log directory OFFLINE rather than shedding data "+
			"when it fills, so the volume must be at least %.1f GiB to leave the %.0f%% headroom "+
			"a segment roll needs. The claim is %.1f GiB. Change the claim and "+
			"log.retention.bytes together.",
		topics, categories, partitions, replication, brokers,
		partitionsPerBroker, retentionBytes,
		logBytesPerBroker/math.Pow(2, 30), floorBytes/math.Pow(2, 30),
		kafkaRetentionHeadroom*100, float64(claimed)/math.Pow(2, 30))

	// AND THE STORAGE MUST BE DURABLE. A KRaft broker that came back with an empty volume
	// has lost its metadata log and its share of every partition, so an emptyDir is not a
	// smaller version of this — it is a different, silently lossy deployment.
	templates, ok := spec["volumeClaimTemplates"].([]interface{})
	require.True(t, ok, "%s must claim its storage through volumeClaimTemplates: a StatefulSet "+
		"re-binds each claim to the same pod across restarts, which is what makes the KRaft log "+
		"durable", kafkaStatefulSetPath)
	require.Len(t, templates, 1, "one claim per broker")

	template, ok := templates[0].(map[string]interface{})
	require.True(t, ok)
	templateSpec, ok := template["spec"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{"ReadWriteOnce"}, templateSpec["accessModes"],
		"a broker's log directory is written by exactly one pod")

	// The log directory must come from the CLAIM and from nowhere else. Asserted
	// structurally rather than by forbidding the word emptyDir anywhere in the file: this
	// pod legitimately uses emptyDir for the memory-backed generated configuration and for
	// the two log-output directories, and a textual ban would fail on those while missing
	// the one that matters — a pod-level volume shadowing the claim under the same name,
	// which mounts a directory that does not survive a reschedule and takes the KRaft
	// metadata log with it.
	metadata, ok := template["metadata"].(map[string]interface{})
	require.True(t, ok, "the claim template must be named")
	claimName := fmt.Sprint(metadata["name"])
	require.NotEmpty(t, claimName)

	podSpec, ok := spec["template"].(map[string]interface{})
	require.True(t, ok, "the StatefulSet must declare a pod template")
	podSpecBody, ok := podSpec["spec"].(map[string]interface{})
	require.True(t, ok)

	if volumes, present := podSpecBody["volumes"].([]interface{}); present {
		for _, entry := range volumes {
			volume, isMap := entry.(map[string]interface{})
			require.True(t, isMap)
			assert.NotEqual(t, claimName, fmt.Sprint(volume["name"]),
				"a pod-level volume named %q SHADOWS the claim template of the same name, so the "+
					"broker's log directory would not survive a reschedule and the KRaft metadata "+
					"log would be lost with it", claimName)
		}
	}
}

// statefulSetRetentionBytes reads log.retention.bytes out of the broker configuration
// the StatefulSet generates.
//
// Parameters:
//   - t *testing.T: the test, failed when the setting is absent.
//   - path string: the manifest path.
//
// Returns:
//   - int64: the per-partition retention budget in bytes.
func statefulSetRetentionBytes(t *testing.T, path string) int64 {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	matches := regexp.MustCompile(`(?m)^\s*log\.retention\.bytes=(\d+)\s*$`).
		FindStringSubmatch(string(raw))
	require.Len(t, matches, 2,
		"%s must set log.retention.bytes explicitly. Without it the broker takes its default of "+
			"-1, which means NO SIZE LIMIT, and the volume then decides when the broker stops",
		path)

	value, err := strconv.ParseInt(matches[1], 10, 64)
	require.NoError(t, err)
	require.Positive(t, value, "a retention budget of zero would keep nothing")

	return value
}

// statefulSetClaimedStorageBytes reads the volume claim's requested size in bytes.
//
// Parameters:
//   - t *testing.T: the test, failed when the claim or its quantity is malformed.
//   - spec map[string]interface{}: the StatefulSet spec.
//
// Returns:
//   - int64: the requested storage in bytes.
func statefulSetClaimedStorageBytes(t *testing.T, spec map[string]interface{}) int64 {
	t.Helper()

	templates, ok := spec["volumeClaimTemplates"].([]interface{})
	require.True(t, ok, "the StatefulSet must declare volumeClaimTemplates")
	require.NotEmpty(t, templates)

	template, ok := templates[0].(map[string]interface{})
	require.True(t, ok)
	templateSpec, ok := template["spec"].(map[string]interface{})
	require.True(t, ok)
	resources, ok := templateSpec["resources"].(map[string]interface{})
	require.True(t, ok)
	requests, ok := resources["requests"].(map[string]interface{})
	require.True(t, ok)

	return parseKubernetesQuantity(t, fmt.Sprint(requests["storage"]))
}

// parseKubernetesQuantity converts a Kubernetes storage quantity into bytes.
//
// Only the binary suffixes a volume claim realistically uses are accepted, and an
// unrecognised one is a failure rather than a silent zero: a quantity this test could
// not read would make the floor comparison pass for the wrong reason.
//
// Parameters:
//   - t *testing.T: the test.
//   - quantity string: for example "160Gi".
//
// Returns:
//   - int64: the value in bytes.
func parseKubernetesQuantity(t *testing.T, quantity string) int64 {
	t.Helper()

	quantity = strings.TrimSpace(quantity)
	for suffix, multiplier := range map[string]int64{
		"Ki": 1 << 10,
		"Mi": 1 << 20,
		"Gi": 1 << 30,
		"Ti": 1 << 40,
	} {
		if number, found := strings.CutSuffix(quantity, suffix); found {
			value, err := strconv.ParseInt(number, 10, 64)
			require.NoError(t, err, "%q must carry an integer quantity", quantity)

			return value * multiplier
		}
	}

	value, err := strconv.ParseInt(quantity, 10, 64)
	require.NoError(t, err,
		"%q is not a storage quantity this test recognises; add its suffix rather than letting "+
			"the comparison pass on a zero", quantity)

	return value
}

// yamlInt reads an integer field out of a decoded YAML map.
//
// Parameters:
//   - t *testing.T: the test, failed when the field is absent or not numeric.
//   - node map[string]interface{}: the map.
//   - key string: the field.
//
// Returns:
//   - int: the value.
func yamlInt(t *testing.T, node map[string]interface{}, key string) int {
	t.Helper()

	value, present := node[key]
	require.True(t, present, "the manifest must declare %q", key)

	number, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
	require.NoError(t, err, "%q must be numeric", key)

	return number
}
