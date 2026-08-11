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
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file guards the LOCAL PROVISIONING PROJECTION: what the automatic Kafka bring-up
// hands scripts/kafka-provision.sh, and what that makes the script do with a credential.
//
// It exists because the defect it guards had no compile error and no failing test to reveal
// it. The sample subscriber's SASL password was simply left unset, the script read that as
// "generate one", and a generated password is printed — correctly, for a human who asked for
// it. What made that a security finding rather than a feature is WHERE it was printed from:
// the kafka-init service runs unattended on every `docker compose up`, and its stdout is the
// service log, which Docker retains for the lifetime of the container. A SASL credential
// nobody asked to see was therefore recorded durably, readable by anyone who can reach the
// daemon, and re-readable long after the run.
//
// # How it is closed, and why NOT by shipping a password
//
// The first fix considered was to ship a known password as the compose default, so the script
// took its "supplied" path and printed nothing. That closes the log disclosure and opens a
// worse one: a credential with a default in a committed file is a credential in every clone of
// this repository, identical on every developer's broker, and one copy-paste away from a
// deployment that is not loopback-bound. TestCompose_NoKafkaCredentialIsKnownFromSource
// forbids it for exactly that reason.
//
// So the disclosure is closed in the SCRIPT instead, which is where it can be closed
// completely: kafka-provision.sh will not print a generated credential at all. Given a
// supplied secret it applies it and never echoes it; given a destination file it writes the
// generated value there at mode 0600 and reports only the path; given neither it SKIPS the
// sample principal and says what to set. The topic catalogue — the part the relay actually
// needs — is assured either way, so a bring-up carrying no .env still succeeds. `./stack.sh
// --init` generates the value into a mode-0600 .env, which is the intended local route.
//
// The assertions below therefore pin: no shipped literal, no path that invents its own value,
// and no path that can print one.
//
// Every assertion below reads the compose files, stack.sh and the provisioning script AS DATA,
// so the whole file runs in CI with no Docker and no broker.

// composeFilesWithKafkaInit are the two compose projections that run the provisioning
// one-shot. They are maintained by hand as parallel files, so every assertion here runs
// against both: a fix applied to one of them is the failure mode this list exists to catch.
var composeFilesWithKafkaInit = []string{"docker-compose.yaml", "docker-compose.dev.yaml"}

// sampleSubscriberSecretVar is the environment variable that decides whether the
// provisioning script generates a password or is given one.
const sampleSubscriberSecretVar = "KAFKA_SAMPLE_SUBSCRIBER_SECRET"

// kafkaInitEnvironment returns the kafka-init service's environment block from composeFile.
//
// The values are returned RAW — as `${NAME:-default}` interpolation expressions rather than
// as resolved values — because that is precisely what is under test. Resolving them (with
// `docker compose config`, say) would answer "what does this evaluate to on this machine
// right now", when the question is "what does this evaluate to on a machine that has set
// nothing", which is every first bring-up.
//
// Parameters:
//   - t *testing.T: failed when the file, the service or the block is missing.
//   - composeFile string: the file name, relative to the module root.
//
// Returns:
//   - map[string]string: variable name to raw value.
func kafkaInitEnvironment(t *testing.T, composeFile string) map[string]string {
	t.Helper()

	compose := readYAMLFile(t, filepath.Join(moduleRootDir(t), composeFile))

	services, ok := compose["services"].(map[string]interface{})
	require.True(t, ok, "%s must declare services", composeFile)

	initService, ok := services["kafka-init"].(map[string]interface{})
	require.True(t, ok, "%s must declare the kafka-init one-shot", composeFile)

	block, ok := initService["environment"].(map[string]interface{})
	require.True(t, ok, "the kafka-init service must configure itself through an environment map")

	environment := map[string]string{}
	for name, value := range block {
		text, isString := value.(string)
		require.True(t, isString, "%s in %s must be a string", name, composeFile)
		environment[name] = text
	}

	return environment
}

// composeDefault splits a `${NAME:-default}` expression into its operator and its default.
//
// The OPERATOR is returned because the difference between `:-` and `-` is the whole
// assertion in one of the tests below and is invisible on a casual read.
//
// Parameters:
//   - t *testing.T: failed when the value is not an interpolation expression.
//   - expression string: the raw compose value.
//
// Returns:
//   - string: the operator, either ":-" or "-".
//   - string: the default value.
func composeDefault(t *testing.T, expression string) (string, string) {
	t.Helper()

	inner, ok := strings.CutPrefix(expression, "${")
	require.True(t, ok, "%q must be an interpolation expression", expression)
	inner, ok = strings.CutSuffix(inner, "}")
	require.True(t, ok, "%q must be an interpolation expression", expression)

	if name, def, found := strings.Cut(inner, ":-"); found {
		require.NotEmpty(t, name)

		return ":-", def
	}

	name, def, found := strings.Cut(inner, "-")
	require.True(t, found, "%q must supply a default", expression)
	require.NotEmpty(t, name)

	return "-", def
}

// TestKafkaInitService_NeverPrintsAGeneratedPasswordIntoTheServiceLog is the F-20 guard.
//
// Two properties together are the fix, and each is inert without the other.
//
// THE COMPOSE DEFAULT MUST BE EMPTY. A literal here would be a credential in every clone of
// this repository; TestCompose_NoKafkaCredentialIsKnownFromSource forbids it. The key must
// still be PASSED, though, and passed with the `:-` operator: the script's own defaulting uses
// ${VAR:-...}, so a key omitted from this block is a key the script cannot receive at all, and
// `${VAR-default}` would substitute only for an UNSET name while .env.example ships Kafka
// credentials blank rather than absent.
//
// THE SCRIPT MUST REFUSE TO PRINT. With the default empty, the unattended path is the one
// where no secret was supplied — so the guarantee has to live in the script: it delivers a
// generated credential to a mode-0600 file and nowhere else, and with no file configured it
// provisions no credential at all rather than printing one.
func TestKafkaInitService_NeverPrintsAGeneratedPasswordIntoTheServiceLog(t *testing.T) {
	for _, composeFile := range composeFilesWithKafkaInit {
		t.Run(composeFile, func(t *testing.T) {
			environment := kafkaInitEnvironment(t, composeFile)

			expression, present := environment[sampleSubscriberSecretVar]
			require.True(t, present,
				"kafka-init must pass %s; omitting it entirely leaves the script unable to receive a "+
					"supplied secret at all", sampleSubscriberSecretVar)

			operator, secret := composeDefault(t, expression)

			assert.Equal(t, ":-", operator,
				"the default must apply to an EMPTY value as well as an unset one: .env.example ships Kafka "+
					"credentials blank, and ${VAR-default} substitutes only for an unset name")
			assert.Empty(t, secret,
				"the default must be EMPTY. A password with a default in a committed file is a password in "+
					"every clone of this repository, identical on every broker; the log disclosure this "+
					"guards is closed in the script instead, where it can be closed completely")

			// And the delivery channel must be passable, or the only way to obtain a generated
			// credential is the one the script refuses.
			_, present = environment[sampleSubscriberSecretFileVar]
			assert.True(t, present,
				"kafka-init must pass %s: it is the mode-0600 destination a generated credential is "+
					"written to, and without it a run that has no supplied secret can only skip the "+
					"principal", sampleSubscriberSecretFileVar)
		})
	}

	// The script half. Read as text, because what is being asserted is the absence of a
	// printing path rather than a value.
	script := readProvisioningScript(t)

	// The helper this used to name was deliver_generated_secret. It is now a PAIR —
	// stage_generated_secret before the broker is altered and commit_staged_secret after —
	// because delivery has to be PROVEN POSSIBLE before a credential is activated: a broker
	// that accepted a credential whose file could not then be written leaves a live account
	// with an unrecoverable password. Both halves are asserted, so the split cannot silently
	// collapse back into a single post-mutation write.
	assert.Contains(t, script, "stage_generated_secret",
		"the script must stage a generated credential into a mode-0600 file BEFORE altering the "+
			"broker; a printf of the value is what put a password into the container log, and a "+
			"write attempted only afterwards is what stranded live credentials")
	assert.Contains(t, script, "commit_staged_secret",
		"and it must move the staged file into place once the broker has accepted the credential, "+
			"so the destination's contents and its mode change together")
	assert.Contains(t, script, "skipping the sample subscriber principal: no password to give it",
		"and with no supplied secret and no destination file it must SKIP the principal and say so, "+
			"rather than generating one it can only print")

	// The topics are assured on that path regardless, which is what makes skipping acceptable
	// instead of failing: the relay needs the catalogue, not the sample consumer.
	assert.NotContains(t, script, `die "no delivery channel is configured for a generated sample subscriber password."`,
		"the missing channel must not FAIL the run: that refusal ran in the decision phase, before "+
			"the catalogue was assured, so a bring-up carrying no .env provisioned no topics at all "+
			"and the compose gate then held the server and worker back too")
}

// TestSampleSubscriberSecret_IsNeverInventedByAnAutomaticProvisioningPath guards a divergence
// that a per-path default would make possible.
//
// Three automatic paths provision the same principal on the same broker: the kafka-init service
// in each compose file, and stack.sh's host fallback for when the CLI is not available inside
// the compose projection. The script PRESERVES an existing credential, so whichever path runs
// FIRST decides the live password, and a later run of a path carrying a different value upserts
// over it. A developer's consumer then stops authenticating at an arbitrary later bring-up, with
// the previous password valid at the moment they copied it and invalid afterwards, and nothing in
// any log saying what changed it.
//
// The rule that removes the possibility is that NO path may invent a value: each passes the
// operator's own through unchanged, so they cannot disagree whatever order they run in.
func TestSampleSubscriberSecret_IsNeverInventedByAnAutomaticProvisioningPath(t *testing.T) {
	for _, composeFile := range composeFilesWithKafkaInit {
		_, secret := composeDefault(t, kafkaInitEnvironment(t, composeFile)[sampleSubscriberSecretVar])
		assert.Emptyf(t, secret,
			"%s must not supply a %s of its own; two paths with two defaults provision two different "+
				"credentials and the order decides which one is live", composeFile, sampleSubscriberSecretVar)
	}

	stack, err := os.ReadFile(filepath.Join(moduleRootDir(t), "stack.sh"))
	require.NoError(t, err, "stack.sh must be readable")

	// The host fallback must FORWARD the variable rather than default it. The pass-through is an
	// allowlist tested on DECLARATION, so an operator's value — including a deliberate empty one
	// — crosses verbatim, and a name left off the list is silently replaced by the script's own
	// default instead.
	//
	// ASSERTED AGAINST THE SCRIPT'S OWN INTERFACE rather than against a literal list in stack.sh,
	// because stack.sh no longer has one: it reads "--print-interface-host" at startup, so the two
	// cannot drift and the only meaningful question is whether these two names are IN that
	// interface. This used to parse a literal array out of stack.sh, which is the sort of
	// assertion that silently stops testing anything the moment the thing it parses moves.
	forwarded := kafkaProvisionInterface(t, "--print-interface-host")

	for _, variable := range []string{sampleSubscriberSecretVar, sampleSubscriberSecretFileVar} {
		assert.Containsf(t, forwarded, variable,
			"the provisioning interface must include %s, or the host fallback runs with the "+
				"script's default rather than with what the operator configured", variable)
	}

	assert.Contains(t, string(stack), "--print-interface-host",
		"and stack.sh must forward that interface rather than a copy of it")

	assert.NotRegexp(t,
		regexp.MustCompile(regexp.QuoteMeta(sampleSubscriberSecretVar)+`="\$\{`+
			regexp.QuoteMeta(sampleSubscriberSecretVar)+`:-[^}]+\}"`),
		string(stack),
		"and it must not substitute a default of its own for %s: that is the per-path divergence this "+
			"test exists to prevent", sampleSubscriberSecretVar)
}

// TestGeneratedSampleSubscriberSecret_SatisfiesTheProvisioningScriptsCredentialFloors asserts
// the value the supported route generates is one the script will actually accept.
//
// This is where a well-meant edit does the most damage. Kafka's `--add-config` value grammar
// splits on `,` and `=` with no escape sequence at all, and the JAAS properties value ends at
// the first unescaped `"` and terminates at `;` — so the script REFUSES a password containing
// any of them rather than escaping it, and dies before touching the broker. It also enforces a
// length and a distinct-character floor. `./stack.sh --init` is the documented way to obtain
// this credential, so a generator that drew from a wider alphabet would turn every local
// bring-up into a hard provisioning failure whose message names the variable but, correctly,
// not the value.
//
// The alphabet is READ FROM THE SCRIPT rather than restated here. Restating it would create a
// second copy of a rule that already exists in exactly one place, and the copy would be the one
// that went stale — leaving this test certifying a generator against an alphabet the script no
// longer enforces.
func TestGeneratedSampleSubscriberSecret_SatisfiesTheProvisioningScriptsCredentialFloors(t *testing.T) {
	script := readProvisioningScript(t)

	declaration := regexp.MustCompile(`(?m)^readonly CREDENTIAL_SAFE_ERE='([^']+)'`).FindStringSubmatch(script)
	require.NotNil(t, declaration,
		"kafka-provision.sh must declare CREDENTIAL_SAFE_ERE; this test validates the generator against "+
			"the script's own alphabet rather than a second copy of it")

	alphabet, err := regexp.Compile(declaration[1])
	require.NoError(t, err, "the script's credential alphabet must be a pattern this test can evaluate")

	// Self-check: the pattern must actually reject the characters it exists to reject, or a
	// pattern that had degenerated into "anything" would certify any generator at all.
	for _, forbidden := range []string{"a=b", "a,b", `a"b`, "a;b", "a b"} {
		require.False(t, alphabet.MatchString(forbidden),
			"the extracted alphabet must still reject %q, or it is not the guard it appears to be", forbidden)
	}

	stack, err := os.ReadFile(filepath.Join(moduleRootDir(t), "stack.sh"))
	require.NoError(t, err, "stack.sh must be readable")

	// ONE GENERATOR, and every Kafka credential --init writes must come from it. A second
	// generator is how one of the three credentials ends up drawn from a wider alphabet than the
	// script accepts, which is a failure the operator cannot diagnose from the message.
	assert.Contains(t, string(stack), "generate_kafka_secret",
		"stack.sh must generate Kafka credentials through a helper that enforces the script's floors")

	draw := regexp.MustCompile(`tr -dc '([^']+)'`).FindStringSubmatch(string(stack))
	require.NotNil(t, draw,
		"generate_kafka_secret must filter its draw to an explicit character class, so the alphabet it "+
			"can emit is readable here rather than assumed")

	// Every character the generator can emit must satisfy the script's alphabet. Expanding the
	// class by hand keeps this a real containment check rather than a string comparison.
	for _, candidate := range expandCharacterClass(t, draw[1]) {
		assert.Truef(t, alphabet.MatchString(candidate),
			"generate_kafka_secret can emit %q, which the provisioning script's credential alphabet "+
				"refuses; the script dies before touching the broker rather than escaping it", candidate)
	}

	// And the floors the script enforces on a supplied secret must be the floors the generator
	// clears, or an unlucky draw produces a .env every consumer refuses.
	assert.Contains(t, string(stack), "-lt 32",
		"the generator must enforce the same 32-character length floor the script does")
	assert.Contains(t, string(stack), "-ge 16",
		"and the same 16-distinct-character floor")
}

// expandCharacterClass expands a tr-style character class such as "A-Za-z0-9" into the
// individual characters it matches.
//
// Ranges are expanded rather than pattern-matched so that a class widened by one character —
// the way an alphabet quietly acquires "+/" — is caught as a concrete counterexample.
func expandCharacterClass(t *testing.T, class string) []string {
	t.Helper()

	characters := make([]string, 0, len(class))
	for index := 0; index < len(class); index++ {
		if index+2 < len(class) && class[index+1] == '-' {
			for character := class[index]; character <= class[index+2]; character++ {
				characters = append(characters, string(character))
			}
			index += 2

			continue
		}

		characters = append(characters, string(class[index]))
	}

	require.NotEmpty(t, characters, "the character class must expand to something")

	return characters
}

// readProvisioningScript returns scripts/kafka-provision.sh as text.
func readProvisioningScript(t *testing.T) string {
	t.Helper()

	contents, err := os.ReadFile(filepath.Join(moduleRootDir(t), "scripts", "kafka-provision.sh"))
	require.NoError(t, err, "the provisioning script must be readable")

	return string(contents)
}

// sampleSubscriberSecretFileVar is the mode-0600 destination a generated sample credential is
// written to. It is the only channel the script will deliver a generated password over.
const sampleSubscriberSecretFileVar = "KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE"

// stubbedKafkaCLI writes fake kafka-topics, kafka-configs and kafka-acls onto a directory it
// returns, along with the path of the file every invocation is appended to.
//
// # Why the script is EXECUTED rather than read
//
// Every other assertion in this file inspects the script's text, and text is the wrong
// instrument for finding F-11. The defect was not a missing string — it was a control-flow
// outcome: a failed `--describe` fell through to the arm that generates a password and upserts
// it, so a working credential was rotated with nothing asked for and nothing reported. Only
// running the thing shows which arm was taken, and the evidence that matters is the ABSENCE of
// an `--alter --add-config 'SCRAM-...'` call for either principal.
//
// The stubs make the whole run succeed except the user describe, which is exactly the shape of
// an unreachable broker or an admin principal without DescribeConfigs: topics list and create
// fine, ACLs are added fine, and only the credential probe fails.
//
// Parameters:
//   - t *testing.T: owns the temporary directory's lifetime.
//   - describeExit int: the exit status the stub returns for `--describe --entity-type users`.
//     Zero makes the probe determinate; non-zero makes it indeterminate.
//   - describeOutput string: what that call prints, so a determinate probe can report a
//     credential as present or absent.
//
// Returns:
//   - string: the directory to put at the front of PATH.
//   - string: the path of the invocation log.
func stubbedKafkaCLI(t *testing.T, describeExit int, describeOutput string) (string, string) {
	t.Helper()

	dir := t.TempDir()
	invocations := filepath.Join(dir, "invocations.log")

	// One stub body serves all three tools. It records its own name and every argument, then
	// branches only on the case this test cares about.
	//
	// `kafka-configs --describe --entity-type users` is the probe. Everything else — topic
	// creation, ACL addition, and any credential ALTER — succeeds silently, because the point
	// is to prove the alter is never ISSUED, not that it would have failed.
	stub := "#!/usr/bin/env bash\n" +
		"printf '%s' \"$(basename \"$0\")\" >> " + invocations + "\n" +
		"for arg in \"$@\"; do printf ' %s' \"$arg\" >> " + invocations + "; done\n" +
		"printf '\\n' >> " + invocations + "\n" +
		// A topic describe must return a parseable geometry line, or the script retries three
		// times and then fails on an unreadable topic — which would stop the run before it
		// reached the credential arm this test is about. The values match what the run asks
		// for, so the geometry check is satisfied rather than merely survived.
		"if [[ \"$(basename \"$0\")\" == kafka-topics ]]; then\n" +
		"  for arg in \"$@\"; do\n" +
		"    if [[ \"$arg\" == --describe ]]; then\n" +
		"      printf 'Topic: stub\\tTopicId: stub\\tPartitionCount: 6\\tReplicationFactor: 1\\tConfigs: \\n'\n" +
		"      exit 0\n" +
		"    fi\n" +
		"  done\n" +
		"fi\n" +
		"if [[ \"$(basename \"$0\")\" == kafka-configs ]]; then\n" +
		"  users=no; describe=no\n" +
		"  for arg in \"$@\"; do\n" +
		"    [[ \"$arg\" == users ]] && users=yes\n" +
		"    [[ \"$arg\" == --describe ]] && describe=yes\n" +
		"  done\n" +
		"  if [[ $users == yes && $describe == yes ]]; then\n" +
		"    printf '%s\\n' " + shellQuote(describeOutput) + "\n" +
		"    exit " + strconv.Itoa(describeExit) + "\n" +
		"  fi\n" +
		"fi\n" +
		"exit 0\n"

	for _, name := range []string{"kafka-topics", "kafka-configs", "kafka-acls"} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(stub), 0o700), "writing the %s stub", name)
	}

	return dir, invocations
}

// shellQuote renders a string as a single-quoted shell word.
func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// runProvisioningScript executes scripts/kafka-provision.sh against stubbed CLIs and returns
// its combined output, the recorded invocations, and whether it exited zero.
//
// The environment is built from scratch rather than inherited, so an ambient .env or a
// developer's KAFKA_* variables cannot change what is being tested. PATH carries the stub
// directory first and the real toolchain after it, because the script itself runs `basename`,
// `mktemp`, `chmod` and friends.
func runProvisioningScript(t *testing.T, stubDir string, extra map[string]string) (string, string, bool) {
	t.Helper()

	root := moduleRootDir(t)
	secretFile := filepath.Join(t.TempDir(), "sample-secret")

	environment := map[string]string{
		"PATH": stubDir + ":/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME": t.TempDir(),
		// An explicit bootstrap server switches off the "Kafka is not configured" early exit
		// without needing a broker to exist.
		"KAFKA_BOOTSTRAP_SERVER": "stub:9092",
		"KAFKA_SASL_ADMIN_USER":  "admin",
		// At least 32 characters from the allowed alphabet, which the script enforces before it
		// touches the broker. A shorter literal is refused with a strength error and the run
		// never reaches the probe this test is about.
		// #nosec G101 -- a literal for a stubbed broker that is never contacted.
		"KAFKA_SASL_ADMIN_SECRET": "AdminSecretForTheStubbedBroker0123456789",
		// The producer pair is supplied so the producer arm does not need the probe: this test
		// is about the SUBSCRIBER's indeterminate probe unless a case overrides it.
		// #nosec G101 -- likewise a stub literal.
		"KAFKA_PRODUCER_SECRET": "ProducerSecretForTheStubbedBroker0123456",
		// A destination for a generated sample credential. Its presence is what makes the
		// no-write property meaningful: without it the script would decline to generate for an
		// unrelated reason and the test would pass vacuously.
		sampleSubscriberSecretFileVar: secretFile,
		// Keep the run fast: nothing here waits for a real broker.
		"KAFKA_PROVISION_TIMEOUT_SECONDS":       "5",
		"KAFKA_PROVISION_POLL_INTERVAL_SECONDS": "1",
		"KAFKA_REPLICATION_FACTOR":              "1",
	}
	for key, value := range extra {
		if value == "" {
			delete(environment, key)

			continue
		}

		environment[key] = value
	}

	command := exec.Command("bash", filepath.Join(root, "scripts", "kafka-provision.sh")) //nolint:gosec // a fixed path inside the repository
	command.Dir = root
	command.Env = make([]string, 0, len(environment))
	for key, value := range environment {
		command.Env = append(command.Env, key+"="+value)
	}

	output, err := command.CombinedOutput()

	return string(output), secretFile, err == nil
}

// TestKafkaProvisionScript_AnIndeterminateProbeWritesNoCredential is finding F-11, executed.
//
// # The defect
//
// `kafka-configs --describe --entity-type users` can fail for reasons that say nothing about
// the credential: an unreachable broker, an admin principal without DescribeConfigs, a TLS
// failure, a missing CLI. The probe folded every one of those onto "absent" — and absent is the
// branch that generates a password and upserts it whenever a secret-file destination is
// configured. So an indeterminate probe silently ROTATED a working producer or subscriber
// credential: no KAFKA_ROTATE_* asked for it, every consumer holding the old password stopped
// authenticating, and the run reported success, because the broker accepts the new credential
// and nothing compares it with the old one.
//
// # Why this test runs the script
//
// The property is the ABSENCE of a call, on a specific control-flow path. A text assertion can
// check that a guard exists; only execution can show that the guard is REACHED before the arm
// that writes. The stub therefore fails exactly the user describe and succeeds at everything
// else, and the assertion is that no `--alter` carrying a SCRAM config was issued for either
// principal.
func TestKafkaProvisionScript_AnIndeterminateProbeWritesNoCredential(t *testing.T) {
	t.Run("neither principal is altered when the probe fails", func(t *testing.T) {
		stubDir, invocations := stubbedKafkaCLI(t, 1, "Error while executing config command: not authorized")

		// The producer secret is REMOVED for this case, so the producer arm reaches the probe
		// too. Both principals must survive the indeterminate answer untouched.
		output, secretFile, _ := runProvisioningScript(t, stubDir, map[string]string{
			"KAFKA_PRODUCER_SECRET":      "",
			"KAFKA_PRODUCER_SECRET_FILE": filepath.Join(t.TempDir(), "producer-secret"),
		})

		recorded, err := os.ReadFile(invocations)
		require.NoError(t, err, "the stub must have been invoked at all; output was:\n%s", output)

		// The credential write is `kafka-configs --alter --add-config-file <properties>`: the
		// password never appears on a command line, so what identifies the call is the pairing
		// of --alter with the user entity type, not the mechanism string.
		calls := string(recorded)
		for _, line := range strings.Split(calls, "\n") {
			if !strings.HasPrefix(line, "kafka-configs ") || !strings.Contains(line, "--alter") {
				continue
			}

			assert.NotContainsf(t, line, "users",
				"no SCRAM credential may be written after an indeterminate probe: this call rotates a "+
					"credential nobody asked to rotate, and every consumer holding the old password "+
					"stops authenticating.\nCall was: %s\nScript output:\n%s", line, output)
		}

		// And nothing may be delivered to the secret file either: a file written here is a
		// password that only exists because the probe failed.
		_, statErr := os.Stat(secretFile)
		assert.Truef(t, os.IsNotExist(statErr),
			"no generated password may be delivered after an indeterminate probe; %s exists", secretFile)

		// The operator has to be told, or a skipped credential looks like a successful one.
		assert.Contains(t, output, "unknown",
			"the run must report the indeterminate state; a silent skip is how an operator concludes "+
				"the credential was provisioned.\nScript output:\n%s"+output)
	})

	t.Run("a determinate absent probe still provisions, so the guard is not a blanket refusal", func(t *testing.T) {
		// The counterpart, and the reason the fix is a narrowing rather than a refusal. A probe
		// that SUCCEEDS and reports no SCRAM-SHA-512 credential must still generate one — that
		// is the documented first-run behaviour, and a guard that blocked it would break
		// bring-up on every fresh broker.
		stubDir, invocations := stubbedKafkaCLI(t, 0, "Configs for user-principal 'blnk-sample-subscriber' are ")

		output, secretFile, _ := runProvisioningScript(t, stubDir, nil)

		recorded, err := os.ReadFile(invocations)
		require.NoError(t, err, "the stub must have been invoked; output was:\n%s", output)

		assert.Regexp(t, regexp.MustCompile(`kafka-configs [^\n]*--alter[^\n]*blnk-sample-subscriber`), string(recorded),
			"a determinate ABSENT probe must still write the credential: this is the fresh-broker "+
				"path, and blocking it would mean no bring-up could ever provision one.\nScript output:\n"+output)

		contents, readErr := os.ReadFile(secretFile)
		require.NoError(t, readErr, "the generated password must be delivered to its file; output:\n"+output)
		assert.NotEmpty(t, strings.TrimSpace(string(contents)),
			"and the delivered file must actually carry the password")
	})

	t.Run("a determinate present probe preserves the credential", func(t *testing.T) {
		// The third state. A credential that exists must not be rewritten, which is the
		// behaviour that already worked and must keep working.
		stubDir, invocations := stubbedKafkaCLI(t, 0,
			"Configs for user-principal 'blnk-sample-subscriber' are SCRAM-SHA-512=salt=abc,stored_key=def")

		output, secretFile, _ := runProvisioningScript(t, stubDir, nil)

		recorded, err := os.ReadFile(invocations)
		require.NoError(t, err, "the stub must have been invoked; output was:\n%s", output)

		// Scoped to the SUBSCRIBER principal. The producer's password is supplied explicitly in
		// this case's environment, so its credential is written on the "supplied" arm — which is
		// correct, needs no probe, and is not what this case is about.
		for _, line := range strings.Split(string(recorded), "\n") {
			if strings.HasPrefix(line, "kafka-configs ") && strings.Contains(line, "--alter") {
				assert.NotContainsf(t, line, "blnk-sample-subscriber",
					"an existing credential must be preserved, not rewritten.\nCall was: %s", line)
			}
		}

		_, statErr := os.Stat(secretFile)
		assert.Truef(t, os.IsNotExist(statErr),
			"and nothing may be generated when a credential already exists; %s exists.\nOutput:\n%s",
			secretFile, output)
	})
}

// TestKafkaProvisionScript_CredentialProbeIsAPredicate pins the shape of the probe that
// decides whether an existing SCRAM password may be preserved.
//
// It was written to PRINT its answer — "exists", "absent" or "unknown" on stdout — and to
// return 0 in every case, while both call sites used it as the condition of an `elif`. Every
// caller therefore read "yes" whatever the broker had said, and the answer itself was emitted
// into the operator's console in the middle of a sentence.
//
// The two callers failed differently and both were worse than an error:
//
//   - the producer arm took its `preserved` short-circuit, granted ACLs and reported SUCCESS
//     with no credential on the broker at all. Bring-up was green and the server and worker
//     then failed SASL authentication with nothing in the provisioning output to explain it;
//   - the subscriber arm had no such short-circuit, so it fell through to the credential
//     upsert with an EMPTY password. The broker refuses that, so the one run an operator makes
//     to repair a drifted ACL failed on a principal and a broker that were both healthy.
//
// It also made two subscriber arms unreachable, so the documented one-time secret-file
// delivery never happened on a first run.
//
// The assertion is on the CONTRACT rather than on the implementation: nothing is written to
// stdout, and there is a path that returns non-zero. A probe that cannot say "no" is not a
// probe.
//
// # It is now a TRI-STATE, and that is a second defect this pins
//
// Answering only yes-or-no was itself wrong, because there are three answers. "The broker says
// this principal has no SHA-512 credential" licenses minting one; "the broker did not answer"
// licenses nothing. Folding the two into a single non-zero return meant a transient describe
// failure — a restart in progress, a timeout, an administrative principal without
// DescribeConfigs on user entities — was read as absence, so the disposition chain skipped its
// `preserved` arm and fell into generate-and-upsert. A WORKING credential was replaced with a
// newly generated password under no rotation flag, every consumer and publishing process holding
// the old one stopped authenticating, and the run printed success. The operator had asked only
// for topics to be assured.
//
// So the work is split across two functions and this asserts the contract of BOTH: the probe
// reports three distinct outcomes, and the predicate the call sites use RESOLVES the unknown one
// by aborting rather than by guessing. Splitting it is why the round-trip and the local `status`
// are asserted against the probe while the stdout and non-zero rules are asserted against both —
// the contract moved, it did not weaken.
func TestKafkaProvisionScript_CredentialProbeIsAPredicate(t *testing.T) {
	script := readProvisioningScript(t)

	body := shellFunctionBody(t, script, "scram_credential_state")

	for _, printed := range []string{`printf '%s' "exists"`, `printf '%s' "absent"`, `printf '%s' "unknown"`} {
		assert.NotContainsf(t, body, printed,
			"the probe must not print its answer (%s): the predicate built on it is used as the "+
				"condition of an `elif`, so a printed answer is read as `true` whatever the broker said — "+
				"and it lands in the operator's output mid-sentence", printed)
	}

	// THREE OUTCOMES, NOT TWO. This is finding F-11: a failed describe used to be reported as
	// "absent", and absent is the branch that GENERATES a password and upserts it — so an
	// unreachable broker or an unauthorised admin silently rotated a working credential.
	for _, state := range []string{`SCRAM_CREDENTIAL_STATE="present"`, `SCRAM_CREDENTIAL_STATE="absent"`, `SCRAM_CREDENTIAL_STATE="unknown"`} {
		assert.Containsf(t, body, state,
			"the probe must be able to report %s: folding an indeterminate answer onto absent is what "+
				"let a failed describe rotate a credential nobody asked to rotate", state)
	}

	// The predicate the call sites use must answer NO — for absent AND for unknown, which is
	// what routes an indeterminate probe to the no-write arm rather than to the generation arm.
	predicate := shellFunctionBody(t, script, "scram_credential_is_present")
	assert.Contains(t, predicate, `[[ "$SCRAM_CREDENTIAL_STATE" == "present" ]]`,
		"the predicate must be true for PRESENT alone; true for `!= absent` would make an unknown "+
			"state preserve a credential that may not exist, and true for `!= present` would make it "+
			"generate one over a credential that does")

	// THE UNKNOWN ANSWER IS RESOLVED BY A GATE, NOT BY A WRAPPER THAT EXITS.
	//
	// There were two wrappers, from two independent attempts at this finding, and neither was
	// reachable: one tested `((probe == SCRAM_PROBE_UNKNOWN))` against a `scram_credential_probe`
	// helper that does not exist, and one read the state function's exit code and `die`d on the
	// third answer. Both are gone, and what replaced them is require_determinate_credential_state
	// — consulted by BOTH disposition chains, immediately after the predicate and before the arm
	// that generates a password.
	//
	// The abort went with them, deliberately. The finding is that a credential must not be
	// written on an answer the broker never gave, and skipping the principal satisfies that
	// completely; aborting additionally discards the idempotent ACL assertion that follows, which
	// is the repair an operator re-running this script during an incident came for, and in the
	// compose stack it turns a briefly unauthorised describe into a bring-up that cannot start at
	// all, because kafka-init is a one-shot the application services wait on. The script's
	// reporting surface is built for the skip: "indeterminate" is a distinct disposition from
	// "skipped" in all three summary printers, because they tell an operator to fix different
	// things.
	//
	// So the refusal is asserted on the gate, and the escape hatch with it.
	gate := shellFunctionBody(t, script, "require_determinate_credential_state")

	assert.Contains(t, gate, `[[ "$SCRAM_CREDENTIAL_STATE" != "unknown" ]]`,
		"the gate must key on the UNKNOWN state specifically: anything weaker either refuses a "+
			"determinate answer or lets an indeterminate one through to the generation arm")
	assert.Contains(t, gate, "REFUSING TO GUESS",
		"and it must say so in the operator's output — a principal silently left alone is "+
			"indistinguishable from one that was provisioned")
	assert.Contains(t, gate, "return 1",
		"the gate must be able to answer NO; a gate that always returns 0 is not a gate, and both "+
			"call sites use it as the condition of an `elif`")
	assert.Contains(t, gate, "KAFKA_ALLOW_SCRAM_PROBE_FAILURE",
		"and the refusal must have exactly one documented escape hatch, for a broker that can never "+
			"answer a describe on users, so accepting the risk is a deliberate act rather than the default")
	assert.NotContains(t, gate, "die ",
		"the gate must NOT exit the run: the ACL assertion that follows is idempotent, carries no "+
			"secret, and is the one repair a re-run can still make for a principal whose credential "+
			"cannot be inspected")

	// One round-trip, not two. The original issued the describe twice and discarded the first
	// answer, which doubled an administrative call on every bring-up for nothing.
	assert.Equal(t, 1, strings.Count(body, "kafka_configs --describe"),
		"the probe must ask the broker exactly once; the discarded first describe was pure duplication")

	// `status` must be function-local. As a global it leaked this probe's exit status into
	// every later reader of the name.
	assert.Regexp(t, regexp.MustCompile(`local [^\n]*\bstatus\b`), body,
		"the probe must declare `status` local, or its exit status escapes into whatever else reads "+
			"that name")

	// Both credential-writing call sites must consult the no-write guard, and they must do it
	// BEFORE the arm that generates a password. Order is the whole property: placed after,
	// the guard is unreachable and the rotation happens anyway.
	for _, function := range []string{"ensure_sample_subscriber", "ensure_producer_principal"} {
		arm := shellFunctionBody(t, script, function)

		guard := strings.Index(arm, "require_determinate_credential_state")
		require.GreaterOrEqualf(t, guard, 0,
			"%s must consult require_determinate_credential_state, or an indeterminate probe reaches "+
				"its generation arm and rotates a live credential", function)

		generate := strings.Index(arm, "_SECRET_FILE\")\" ]]; then")
		require.GreaterOrEqualf(t, generate, 0, "%s must have a secret-file generation arm to guard", function)

		assert.Lessf(t, guard, generate,
			"%s must consult the guard BEFORE the secret-file generation arm; after it, the guard "+
				"never runs and the credential is rotated before anything checks whether it existed",
			function)
	}

	// The third answer has to be distinguishable from the other two, or a caller cannot tell
	// "the broker said no" from "the broker did not answer" — which is the whole defect. It is
	// distinguishable on BOTH channels the probe writes, and the literal `return 2` is named
	// rather than written out: SCRAM_PROBE_UNKNOWN is declared readonly at the top of the script
	// beside SCRAM_PROBE_EXISTS and SCRAM_PROBE_ABSENT, so the three codes cannot drift apart.
	assert.Contains(t, body, `return "$SCRAM_PROBE_UNKNOWN"`,
		"scram_credential_state must report UNKNOWN as its own exit code; folding it into absent is "+
			"what let an unanswered probe rotate a live credential")
	assert.Contains(t, body, `return "$SCRAM_PROBE_ABSENT"`,
		"and absent must be its own code too, or the two answers that license opposite actions are "+
			"the same answer")

	// THE GLOBAL AND THE CODE ARE WRITTEN BY THE SAME FUNCTION, ON EVERY PATH. Two channels for
	// one fact is only safe while one function sets both together — the merge of two attempts at
	// this probe once left the exit codes in place and the assignments out, so a principal that
	// held a credential reported "unknown" and a principal with none took the whole run down
	// under `set -e`.
	for _, assignment := range []string{
		`SCRAM_CREDENTIAL_STATE="present"`,
		`SCRAM_CREDENTIAL_STATE="absent"`,
		`SCRAM_CREDENTIAL_STATE="unknown"`,
	} {
		assert.Containsf(t, body, assignment,
			"every path out of the probe must set the state the gate reads: %s", assignment)
	}

	// And the predicate has to invoke it in a form a non-zero exit cannot kill. `set -euo
	// pipefail` is in force, so a bare call returns 1 for the most ordinary outcome there is — a
	// fresh broker with no credential — and takes the run with it.
	assert.Contains(t, predicate, `scram_credential_state "$user" || true`,
		"the predicate must tolerate the probe's non-zero answers; a bare call aborts the run on a "+
			"broker that simply has no credential yet")

	// The subscriber arm must carry the same short-circuit the producer arm always had.
	subscriber := shellFunctionBody(t, script, "ensure_sample_subscriber")

	assert.Contains(t, subscriber, `if [[ "$SUBSCRIBER_SECRET_DISPOSITION" == "preserved" ]]; then`,
		"the sample subscriber arm must short-circuit on a PRESERVED credential — assert the ACLs and "+
			"return — instead of falling through to the upsert with an empty password, which is what the "+
			"broker rejected")

	assert.NotContains(t, subscriber, "subscriber_credential_exists",
		"the ACL-repair guard must call a function that exists. `subscriber_credential_exists` was never "+
			"defined, so under `set -e` inside an `if` condition it evaluated as false after printing "+
			"`command not found`, and the repair silently never happened")
}

// TestKafkaProvisionScript_NoSCRAMPasswordReachesACommandLine holds the rule for BOTH
// principals, which is the whole point of it.
//
// The subscriber's upsert used `--add-config-file` and documented it as "a complete remedy
// rather than a mitigation". The producer's built
// `SCRAM-SHA-512=[iterations=N,password=SECRET]` and passed it to the CLI as an argument, with
// a comment claiming the CLI offered no alternative — contradicted two hundred lines earlier
// in the same script against the same image. So the asymmetry protected the local convenience
// credential and exposed the one the server and worker authenticate with in every environment:
// argv is readable through /proc/<pid>/cmdline for the life of the JVM start, by anything that
// samples the process table.
//
// The bracketed form is asserted specifically because it is the form that only exists to
// survive an OPTION parser. Its presence anywhere in this script means a credential is being
// passed as an argument.
func TestKafkaProvisionScript_NoSCRAMPasswordReachesACommandLine(t *testing.T) {
	script := readProvisioningScript(t)

	for _, function := range []string{"ensure_sample_subscriber", "ensure_producer_principal"} {
		body := shellFunctionBody(t, script, function)

		assert.Containsf(t, body, "--add-config-file",
			"%s must upsert the credential from a mode-0600 file: a password passed as a command-line "+
				"argument is readable in the process table for the life of the JVM start", function)

		assert.NotContainsf(t, body, "--add-config \"",
			"%s must not pass a credential-bearing --add-config value as an argument", function)

		assert.NotContainsf(t, body, "password=${password}]",
			"%s must not build the bracketed --add-config value: the brackets exist only to survive the "+
				"OPTION parser, so their presence means the secret is on a command line", function)
	}
}

// shellFunctionBody returns the text of one shell function, from its `name() {` header to the
// closing brace in column one.
//
// Scoped rather than whole-file, because every assertion above is about ONE function's
// behaviour and the script documents the rejected alternatives in prose: a whole-file
// `NotContains` would fail on a comment explaining the defect it forbids.
//
// Parameters:
//   - t *testing.T: failed when the function is not found.
//   - script string: the script's text.
//   - name string: the function name, without parentheses.
//
// Returns:
//   - string: the function body, comments included.
func shellFunctionBody(t *testing.T, script, name string) string {
	t.Helper()

	header := "\n" + name + "() {\n"
	start := strings.Index(script, header)
	require.GreaterOrEqualf(t, start, 0, "scripts/kafka-provision.sh must define %s()", name)

	rest := script[start+len(header):]
	end := strings.Index(rest, "\n}\n")
	require.GreaterOrEqualf(t, end, 0, "%s() must be closed by a brace in column one", name)

	return rest[:end]
}

// ---------------------------------------------------------------------------
// SEC-15 — the credential is delivered without following a link and without a
// window in which it is empty or partial
// ---------------------------------------------------------------------------

// TestSecretDelivery_RefusesSymlinksAndWritesAtomically is SEC-15.
//
// # The two defects, and why the destination makes them exploitable
//
// A generated credential was delivered with `printf … >"$destination"` followed by
// `chmod 600 "$destination"`. Both of those RESOLVE A SYMLINK, and the destination is normally a
// compose bind mount — a host directory shared with every other process on the host. So anyone
// who could write that directory could pre-place a link and receive the credential, and the
// 0600 that the code was careful to apply landed on the LINK TARGET rather than on the path the
// script chose. A dangling link was worse still: it is a request to create a file wherever it
// points. Measured against the pre-fix body, a link at the destination transferred the password
// into the target file verbatim and set that file to 0600.
//
// Separately the write was three steps — truncate, chmod, write — so a reader arriving between
// the first and the last saw an EMPTY or PARTIAL credential. On a rotation that reader is the
// broker or the operator, and a partial password is indistinguishable from a wrong one.
//
// # What is asserted, and why it is asserted on the source
//
// The delivery cannot be executed here: it runs inside the provisioning script, against a
// broker, as a container one-shot. Both properties are STRUCTURAL — a test that the destination
// is not opened directly, and that the visible transition is a rename — so the script's text is
// the artefact that carries them. Each assertion names the construct rather than the outcome, so
// a re-introduced direct write fails here even if it happens to work on the machine that made it.
func TestSecretDelivery_RefusesSymlinksAndWritesAtomically(t *testing.T) {
	script := readProvisioningScript(t)

	// THE DELIVERY IS TWO PHASES, and the assertions are split across them accordingly.
	//
	// SEC-15 was answered twice: once as a single call that staged and renamed in one go, and
	// once as a PAIR — stage the file before the broker is altered, commit it only once the
	// broker has accepted the credential. The pair is what every call site uses and it is the
	// better shape, because the single-shot form publishes a secret file for a credential that
	// may still fail to be created, leaving an operator holding a password the broker never
	// accepted. The single-shot form is retired; each of its properties is asserted below
	// against whichever half now owns it.
	staging := shellFunctionBody(t, script, "stage_generated_secret")
	commit := shellFunctionBody(t, script, "commit_staged_secret")

	assert.NotContains(t, script, "stage_secret_atomically() {",
		"the single-shot delivery must stay retired: two implementations of one security property "+
			"is how they diverge, and only one of them was ever reachable")

	t.Run("a symlink at the destination is refused before anything is created", func(t *testing.T) {
		require.Contains(t, staging, `if [[ -L "$destination" ]]; then`,
			"the destination must be tested with -L, which is true for a symlink whether or not its "+
				"target exists; a dangling link is the more dangerous case because it is a request to "+
				"create a file wherever it points")

		refusal := strings.Index(staging, `-L "$destination"`)
		creation := strings.Index(staging, "umask 077")
		require.Positive(t, creation, "the staging file must be created under a 077 umask")
		assert.Less(t, refusal, creation,
			"the refusal must come BEFORE anything is created, so a refused delivery leaves nothing "+
				"behind to clean up")

		assert.Contains(t, staging, "is a symbolic link",
			"and it must say so plainly: an operator has to know the path was rejected rather than "+
				"the broker having failed")
	})

	t.Run("the credential is written to a fresh exclusive file, never to the destination", func(t *testing.T) {
		// THE CORE OF THE FIX. The destination is never opened for writing at all — it is only
		// ever the target of a rename — so there is no descriptor through which a link could be
		// followed and no moment at which it holds half a credential.
		assert.NotContains(t, staging, `>"$destination"`,
			"the destination must never be opened for writing: that is the redirection that "+
				"followed a symlink and that truncated the file before the new value existed")
		assert.NotContains(t, staging, `chmod 600 "$destination"`,
			"and its mode must never be set through its own path, which applied 0600 to whatever a "+
				"link pointed at")

		assert.Contains(t, staging, `printf '%s\n' "$credential" >"$staged"`,
			"the credential must be written to the staged file, and by redirection rather than as a "+
				"command argument so it reaches no process-table entry")
		assert.Contains(t, staging, `chmod 600 "$staged"`,
			"the mode must be set on the staged file — freshly created and named by nothing else, so "+
				"there is no window and nothing to race")

		assert.Contains(t, staging, `set -o noclobber`,
			"the fallback creation must be exclusive, so it fails rather than truncating if anything "+
				"appears at that path between choosing the name and opening it")
	})

	t.Run("the staged file sits beside the destination so the rename is atomic", func(t *testing.T) {
		// rename(2) is atomic only WITHIN one filesystem. A staged file in TMPDIR would fail with
		// EXDEV or degrade into a copy, which is the non-atomic write again by another route. The
		// name is DERIVED FROM THE DESTINATION, which is what makes it beside it by construction:
		// no directory has to be spliced back on, so there is no way to get that splice wrong.
		assert.Contains(t, staging, `staged="${destination}.blnk-staged.$$"`,
			"the staged file must be created in the destination's own directory; a temporary file "+
				"elsewhere cannot be renamed atomically onto it")
		// The EXPANSION, not the word: the comment above the staged name says "never in TMPDIR"
		// and is right to, so forbidding the string would forbid the explanation along with the
		// mistake. What must not appear is a path drawn from it.
		assert.NotContains(t, staging, "$TMPDIR",
			"and specifically not under TMPDIR, which is a different filesystem in every container "+
				"this runs in — a rename across filesystems fails with EXDEV or degrades into a copy")
		assert.NotContains(t, staging, "mktemp -d",
			"nor a scratch directory of its own, for the same reason")
	})

	t.Run("the destination is replaced by a rename, and only after the broker accepted", func(t *testing.T) {
		// THE RENAME IS THE SECOND PHASE, and that is the point of splitting it. The write and the
		// publication are separated by the broker's own answer, so a destination is never replaced
		// for a credential that was not created.
		require.Contains(t, commit, `mv -f -- "$staged" "$destination"`,
			"the visible transition must be a rename: a reader then sees either the whole previous "+
				"credential or the whole new one, never nothing and never half")
		assert.NotContains(t, staging, `"$destination"`+" 2>/dev/null; then",
			"and staging must not publish anything itself; the commit is the only step that touches "+
				"the destination")

		assert.Contains(t, staging, `printf '%s\n' "$credential" >"$staged"`,
			"the credential must be written during staging, which is before the commit by "+
				"construction: the commit only ever receives a path staging already filled")
	})

	t.Run("every failure path removes the staged credential", func(t *testing.T) {
		// A staged file left behind is a mode-0600 copy of a live password sitting in a shared
		// host directory under a name nothing will ever look at again.
		//
		// THE EXIT TRAP IS THE REMOVER, not an `rm -f` per failure arm, and it is the stronger
		// form: it also covers a signal arriving part-way through, which no per-arm cleanup can.
		assert.Contains(t, staging, `GENERATED_SECRET_FILES+=("$staged")`,
			"the staged path must be registered for the EXIT trap BEFORE it is written, so a signal "+
				"part-way through still leaves the trap a path to remove")

		registration := strings.Index(staging, `GENERATED_SECRET_FILES+=("$staged")`)
		write := strings.Index(staging, `>"$staged" 2>/dev/null`)
		require.Positive(t, write, "staging must write the credential to the staged file")
		assert.Less(t, registration, write,
			"registration must come before the write, or a signal between them leaves a credential "+
				"on disk that nothing will remove")

		// THE ONE DELIBERATE EXCEPTION, and it must stay deliberate. A commit that fails AFTER the
		// broker accepted the credential leaves the staged file holding the ONLY copy of a live
		// password — a SCRAM verifier cannot be read back — so removing it would destroy the sole
		// means of using an account that now exists.
		assert.Contains(t, commit, `retain_secret_file "$staged"`,
			"a commit that fails after the broker accepted the credential must take the staged file "+
				"OFF the trap's list: it holds the only copy of a live password")
		assert.Contains(t, commit, "THE CREDENTIAL IS LIVE AT THE BROKER",
			"and it must say so, with the path, or the operator deletes the only copy of a password "+
				"they now need")
	})

	t.Run("both principals deliver through the pair rather than writing their own", func(t *testing.T) {
		// The property the retired single-shot helper was asserted for: exactly ONE implementation
		// of the mechanics, reached by every caller.
		for _, function := range []string{"ensure_sample_subscriber", "ensure_producer_principal"} {
			arm := shellFunctionBody(t, script, function)

			stage := strings.Index(arm, "stage_generated_secret")
			require.GreaterOrEqualf(t, stage, 0,
				"%s must stage its generated credential through the shared helper rather than writing "+
					"a file of its own", function)

			commitAt := strings.Index(arm, "commit_staged_secret")
			require.GreaterOrEqualf(t, commitAt, 0, "%s must commit the staged credential", function)

			assert.Lessf(t, stage, commitAt,
				"%s must stage before it commits; the order is what keeps a secret file from being "+
					"published for a credential the broker never accepted", function)

			assert.NotContainsf(t, arm, `chmod 600 "$destination"`,
				"%s must not set the destination's mode through its own path, which applied 0600 to "+
					"whatever a link pointed at", function)
		}

		// Q-03's pattern again, in this file: write_secret_file was an UNREACHABLE near-duplicate
		// of the delivery helper carrying the same two defects. Hardening a function nothing can
		// call while leaving it as a template for the next caller to adopt is not a fix, so it was
		// removed with the defect.
		assert.NotContains(t, script, "write_secret_file()",
			"the unreachable duplicate secret writer must stay removed: two implementations of one "+
				"security property is how they diverge, and only one of them was reachable")
	})
}

// ---------------------------------------------------------------------------
// The created / grown / unchanged summary must report only what it confirmed
// ---------------------------------------------------------------------------

// topicSummaryStub builds a Kafka CLI stub whose TOPIC LISTING is controllable, which is what
// makes the provisioning summary's classification observable without a broker.
//
// The existing stubbedKafkaCLI answers `--describe` and nothing else, because the tests it serves
// are about a credential probe. The summary is decided by a different question — does the topic
// EXIST before this run — and the script now asks that with `--list`, so this stub answers both:
// a list whose contents and exit status the caller chooses, and a describe that always returns a
// geometry matching what the run asks for, so the geometry check is satisfied rather than merely
// survived.
//
// # Making the listing FAIL requires care, because readiness uses it too
//
// The script waits for the broker by calling `kafka-topics --list` until it succeeds, so a stub
// that refuses every listing never gets past readiness and the summary is never reached. That is
// itself the reason an unreadable pre-state is rare in practice — readiness has already proved the
// listing works once. It is not impossible: a broker can become unreadable AFTER readiness, mid-run,
// through an election or a metadata refresh, which is exactly the transient that used to be
// reported as a creation.
//
// So failAfter reproduces that shape rather than a broker that was never reachable: the first
// `failAfter` listings succeed, and every later one fails.
//
// Parameters:
//   - failAfter int: how many listings succeed before the rest fail. Zero or negative means every
//     listing succeeds.
//   - listed []string: the topics the list reports when it succeeds.
//
// Returns:
//   - string: the directory to put first on PATH.
func topicSummaryStub(t *testing.T, failAfter int, listed []string) string {
	t.Helper()

	dir := t.TempDir()
	counter := filepath.Join(dir, "list-calls")

	stub := "#!/usr/bin/env bash\n" +
		"if [[ \"$(basename \"$0\")\" == kafka-topics ]]; then\n" +
		"  for arg in \"$@\"; do\n" +
		"    if [[ \"$arg\" == --list ]]; then\n" +
		"      printf 'x' >> " + counter + "\n" +
		"      calls=$(wc -c < " + counter + ")\n" +
		"      if ((" + strconv.Itoa(failAfter) + " > 0 && calls > " + strconv.Itoa(failAfter) + ")); then\n" +
		"        printf 'Error while executing topic command: broker not available\\n' >&2\n" +
		"        exit 1\n" +
		"      fi\n" +
		"      printf '%s' " + shellQuote(strings.Join(listed, "\n")) + "\n" +
		"      [[ -n " + shellQuote(strings.Join(listed, "\n")) + " ]] && printf '\\n'\n" +
		"      exit 0\n" +
		"    fi\n" +
		"  done\n" +
		"  for arg in \"$@\"; do\n" +
		"    if [[ \"$arg\" == --describe ]]; then\n" +
		"      printf 'Topic: stub\\tTopicId: stub\\tPartitionCount: 6\\tReplicationFactor: 1\\tConfigs: \\n'\n" +
		"      exit 0\n" +
		"    fi\n" +
		"  done\n" +
		"fi\n" +
		// A determinate "no credential yet" answer, so the credential arms take their ordinary
		// fresh-broker path and the run reaches the summary this test is about.
		"if [[ \"$(basename \"$0\")\" == kafka-configs ]]; then\n" +
		"  users=no; describe=no\n" +
		"  for arg in \"$@\"; do\n" +
		"    [[ \"$arg\" == users ]] && users=yes\n" +
		"    [[ \"$arg\" == --describe ]] && describe=yes\n" +
		"  done\n" +
		"  if [[ $users == yes && $describe == yes ]]; then\n" +
		"    printf 'Configs for user-principal '\"'\"'stub'\"'\"' are \\n'\n" +
		"    exit 0\n" +
		"  fi\n" +
		"fi\n" +
		"exit 0\n"

	for _, name := range []string{"kafka-topics", "kafka-configs", "kafka-acls"} {
		require.NoError(t,
			os.WriteFile(filepath.Join(dir, name), []byte(stub), 0o700),
			"writing the %s stub", name)
	}

	return dir
}

// readinessListings is how many `kafka-topics --list` calls the script makes before it starts
// assuring topics: exactly one, the readiness probe that waits for the broker to accept an
// authenticated request. A stub that fails every listing after this many describes a broker that
// was reachable at readiness and unreadable by the time each topic's pre-state was probed.
//
// Counted from the script's own invocations rather than read off the source: a stub that logged
// every call recorded one listing more than there are topics in a full run — one readiness probe
// and one per topic in the catalogue — with the first topic's `--create` arriving as the second
// call overall.
const readinessListings = 1

// allProvisionedTopics is the topic catalogue the script assures, in the order it assures them:
// the five category topics, then their dead-letter siblings.
var allProvisionedTopics = []string{
	"blnk.transactions", "blnk.balances", "blnk.identities", "blnk.ledgers", "blnk.system",
	"blnk.transactions.dlt", "blnk.balances.dlt", "blnk.identities.dlt", "blnk.ledgers.dlt", "blnk.system.dlt",
}

// TestKafkaProvisionScript_SummaryReportsOnlyConfirmedActions is finding F-10, executed.
//
// # What the summary is for, and what it was doing instead
//
// The closing line answers a question the geometry table cannot: not "is the catalogue correct
// now" but "was it correct when this run started". A full `created` count on a deployment that has been
// publishing for weeks means the catalogue was lost and silently rebuilt — and with it every
// offset — which is an incident, and which the daily outbox-versus-offset reconciliation in
// docs/kafka-operations.md relies on being able to detect. A count that is inferred rather than
// confirmed cannot carry that weight.
//
// Three things made it inferred:
//
//	the pre-state came from `topic_geometry`, which answers empty both for an absent topic and
//	for one the broker could not describe for a moment, so an EXISTING topic was reported as
//	created by this run;
//
//	`SUMMARY_GROWN` was appended BEFORE the alter, so a topic stayed counted as grown by this run
//	even when the alter failed and the re-read showed another provisioner had grown it;
//
//	and `unchanged` was the total minus the other two, so a topic counted in both — which the
//	independent appends allowed — made it NEGATIVE.
//
// # What is asserted
//
// The three classifications this can reach without a broker, each from a stub whose answers are
// unambiguous, plus the arithmetic property that makes the line trustworthy: the counts sum to
// the catalogue size, and none of them is negative.
func TestKafkaProvisionScript_SummaryReportsOnlyConfirmedActions(t *testing.T) {
	// summaryCount extracts one count from the closing summary.
	summaryCount := func(t *testing.T, output, label string) int {
		t.Helper()

		pattern := regexp.MustCompile(label + `\s*:\s*(\d+)`)
		match := pattern.FindStringSubmatch(output)
		require.NotNilf(t, match, "the summary must report %q; output was:\n%s", label, output)

		value, err := strconv.Atoi(match[1])
		require.NoError(t, err)

		return value
	}

	t.Run("a catalogue that already exists is unchanged, not created", func(t *testing.T) {
		stubDir := topicSummaryStub(t, 0, allProvisionedTopics)

		output, _, _ := runProvisioningScript(t, stubDir, nil)

		require.Contains(t, output, "topics are present with a verified geometry",
			"the run must reach its summary; output was:\n%s", output)
		assert.Equal(t, 0, summaryCount(t, output, "created"),
			"every topic was listed before the run, so nothing was created by it")
		assert.Equal(t, 0, summaryCount(t, output, "grown"))
		assert.Equal(t, len(allProvisionedTopics), summaryCount(t, output, "unchanged"),
			"and every one of them is unchanged")
		assert.NotContains(t, output, "raced     :",
			"nothing raced, so the raced line must not appear at all — an empty class printed "+
				"every run is a line that stops being read")
	})

	t.Run("a genuinely absent catalogue is created", func(t *testing.T) {
		stubDir := topicSummaryStub(t, 0, nil)

		output, _, _ := runProvisioningScript(t, stubDir, nil)

		require.Contains(t, output, "topics are present with a verified geometry",
			"the run must reach its summary; output was:\n%s", output)
		assert.Equal(t, len(allProvisionedTopics), summaryCount(t, output, "created"),
			"the list reported nothing before the run, so this run created the catalogue — the "+
				"claim the summary exists to be able to make")
		assert.Equal(t, 0, summaryCount(t, output, "unchanged"))
	})

	t.Run("an unreadable pre-state is raced, never created", func(t *testing.T) {
		// THE FINDING'S FIRST BULLET. Readiness gets its two listings and the broker then stops
		// answering them, so whether each topic existed is not this run's to report. Every topic
		// is still verified — the describe answers a geometry — so the run succeeds and the
		// catalogue is confirmed correct; what it must not do is claim to have built it.
		stubDir := topicSummaryStub(t, readinessListings, allProvisionedTopics)

		output, _, _ := runProvisioningScript(t, stubDir, nil)

		require.Contains(t, output, "topics are present with a verified geometry",
			"an unreadable pre-state must not fail the run — the geometry is still verified; "+
				"output was:\n%s", output)
		assert.Equal(t, 0, summaryCount(t, output, "created"),
			"a topic whose existence could not be established must NOT be reported as created by "+
				"this run: that is the reading an operator acts on as a lost catalogue")
		assert.Equal(t, 0, summaryCount(t, output, "grown"))
		assert.Equal(t, 0, summaryCount(t, output, "unchanged"))
		assert.Equal(t, len(allProvisionedTopics), summaryCount(t, output, "raced"),
			"it belongs to the class that names the uncertainty instead of resolving it")
		assert.Contains(t, output, "re-run to confirm",
			"and the line must say what to do about it")
	})

	t.Run("the counts partition the catalogue in every case", func(t *testing.T) {
		// The arithmetic property, which is what made `unchanged` able to go negative: the four
		// classes must be mutually exclusive and jointly exhaustive. Asserted over all three
		// stubs so no single classification can satisfy it by accident.
		for name, stub := range map[string]string{
			"existing":   topicSummaryStub(t, 0, allProvisionedTopics),
			"absent":     topicSummaryStub(t, 0, nil),
			"unreadable": topicSummaryStub(t, readinessListings, allProvisionedTopics),
		} {
			t.Run(name, func(t *testing.T) {
				output, _, _ := runProvisioningScript(t, stub, nil)
				require.Contains(t, output, "topics are present with a verified geometry",
					"output was:\n%s", output)

				total := 0
				for _, label := range []string{"created", "grown", "unchanged"} {
					count := summaryCount(t, output, label)
					assert.GreaterOrEqualf(t, count, 0, "%s must never be negative", label)
					total += count
				}

				if strings.Contains(output, "raced     :") {
					total += summaryCount(t, output, "raced")
				}

				assert.Equalf(t, len(allProvisionedTopics), total,
					"the classes must partition the catalogue: %d topics were assured, and the "+
						"counts add to %d. A topic counted twice, or in none of them, is how the "+
						"unchanged figure came to be computed as a negative number",
					len(allProvisionedTopics), total)
			})
		}
	})
}

// growthSummaryStub builds a stub whose topic starts UNDER-PARTITIONED and reaches the target once
// an alter has been seen, so the grown / raced distinction is observable without a broker.
//
// The describe answer is stateful because the distinction being tested is temporal: what the
// summary may claim depends on whether the growth this run requested is the growth that happened.
// A marker file written by the alter is what separates "before" from "after".
//
// kafka-get-offsets is stubbed to report every partition at offset zero, which makes each topic
// provably EMPTY and therefore growable without KAFKA_ALLOW_PARTITION_GROWTH. That keeps this test
// about the summary rather than about the consent gate, which has its own coverage.
//
// Parameters:
//   - alterExit int: the exit status of `kafka-topics --alter`. Zero is this run growing the topic;
//     non-zero with the marker still written is another provisioner having grown it first, which is
//     the race the script tolerates and the summary must not claim as its own work.
//
// Returns:
//   - string: the directory to put first on PATH.
func growthSummaryStub(t *testing.T, alterExit int) string {
	t.Helper()

	dir := t.TempDir()

	// THE MARKER IS PER TOPIC. A single shared marker made the first topic's alter change the
	// describe answer for every topic in the catalogue, so all but the first looked
	// already-correct and the test measured one growth instead of the whole catalogue — the stub,
	// not the script, deciding the outcome.
	topics := "#!/usr/bin/env bash\n" +
		"topic=\"\"\n" +
		"previous=\"\"\n" +
		"for arg in \"$@\"; do\n" +
		"  [[ \"$previous\" == --topic ]] && topic=\"$arg\"\n" +
		"  previous=\"$arg\"\n" +
		"done\n" +
		"marker=" + filepath.Join(dir, "grown-") + "\"${topic}\"\n" +
		"for arg in \"$@\"; do\n" +
		"  if [[ \"$arg\" == --list ]]; then\n" +
		"    printf '%s\\n' " + shellQuote(strings.Join(allProvisionedTopics, "\n")) + "\n" +
		"    exit 0\n" +
		"  fi\n" +
		"done\n" +
		"for arg in \"$@\"; do\n" +
		"  if [[ \"$arg\" == --alter ]]; then\n" +
		"    : > \"$marker\"\n" +
		"    exit " + strconv.Itoa(alterExit) + "\n" +
		"  fi\n" +
		"done\n" +
		"for arg in \"$@\"; do\n" +
		"  if [[ \"$arg\" == --describe ]]; then\n" +
		"    if [[ -f \"$marker\" ]]; then\n" +
		"      printf 'Topic: stub\\tTopicId: stub\\tPartitionCount: 6\\tReplicationFactor: 1\\tConfigs: \\n'\n" +
		"    else\n" +
		"      printf 'Topic: stub\\tTopicId: stub\\tPartitionCount: 3\\tReplicationFactor: 1\\tConfigs: \\n'\n" +
		"    fi\n" +
		"    exit 0\n" +
		"  fi\n" +
		"done\n" +
		"exit 0\n"

	offsets := "#!/usr/bin/env bash\n" +
		"topic=\"\"\n" +
		"while [[ $# -gt 0 ]]; do\n" +
		"  if [[ \"$1\" == --topic ]]; then topic=\"$2\"; fi\n" +
		"  shift\n" +
		"done\n" +
		"for partition in 0 1 2 3 4 5; do printf '%s:%s:0\\n' \"$topic\" \"$partition\"; done\n" +
		"exit 0\n"

	configs := "#!/usr/bin/env bash\n" +
		"users=no; describe=no\n" +
		"for arg in \"$@\"; do\n" +
		"  [[ \"$arg\" == users ]] && users=yes\n" +
		"  [[ \"$arg\" == --describe ]] && describe=yes\n" +
		"done\n" +
		"if [[ $users == yes && $describe == yes ]]; then\n" +
		"  printf 'Configs for user-principal '\"'\"'stub'\"'\"' are \\n'\n" +
		"fi\n" +
		"exit 0\n"

	for name, body := range map[string]string{
		"kafka-topics":      topics,
		"kafka-configs":     configs,
		"kafka-acls":        "#!/usr/bin/env bash\nexit 0\n",
		"kafka-get-offsets": offsets,
	} {
		require.NoError(t,
			os.WriteFile(filepath.Join(dir, name), []byte(body), 0o700),
			"writing the %s stub", name)
	}

	return dir
}

// TestKafkaProvisionScript_GrownIsClaimedOnlyWhenThisRunGrewIt is the finding's second bullet,
// executed.
//
// SUMMARY_GROWN used to be appended at the moment the script DECIDED to grow a topic, several lines
// before the alter was attempted. The alter failing is not an error here and must not be — two
// provisioners racing is an ordinary state, and the loser is told the topic already has the
// partitions it wanted — but the topic stayed counted as grown BY THIS RUN either way. The two arms
// below are the same code path with the same observable end state and different authorship, which
// is precisely the distinction the summary is read for.
func TestKafkaProvisionScript_GrownIsClaimedOnlyWhenThisRunGrewIt(t *testing.T) {
	summaryCount := func(t *testing.T, output, label string) int {
		t.Helper()

		match := regexp.MustCompile(label + `\s*:\s*(\d+)`).FindStringSubmatch(output)
		require.NotNilf(t, match, "the summary must report %q; output was:\n%s", label, output)

		value, err := strconv.Atoi(match[1])
		require.NoError(t, err)

		return value
	}

	t.Run("an alter this run made and confirmed is grown", func(t *testing.T) {
		output, _, _ := runProvisioningScript(t, growthSummaryStub(t, 0), nil)

		require.Contains(t, output, "topics are present with a verified geometry",
			"output was:\n%s", output)
		assert.Equal(t, len(allProvisionedTopics), summaryCount(t, output, "grown"),
			"this run requested the growth, the alter succeeded, and the re-read confirmed the "+
				"target was reached — which is the only combination that earns the claim")
		assert.Equal(t, 0, summaryCount(t, output, "created"))
		assert.Equal(t, 0, summaryCount(t, output, "unchanged"))
	})

	t.Run("an alter another provisioner won is raced, not grown", func(t *testing.T) {
		output, _, _ := runProvisioningScript(t, growthSummaryStub(t, 1), nil)

		require.Contains(t, output, "topics are present with a verified geometry",
			"a lost race must not fail the run: the topic has the partitions this run wanted, "+
				"which is success; output was:\n%s", output)
		assert.Contains(t, output, "another provisioner grew it",
			"the run must say what it observed")
		assert.Equal(t, 0, summaryCount(t, output, "grown"),
			"the alter FAILED and the partitions arrived by another hand, so this run did not "+
				"grow anything. Counting it as grown was the defect: the append happened before "+
				"the alter and nothing withdrew it")
		assert.Equal(t, len(allProvisionedTopics), summaryCount(t, output, "raced"),
			"it belongs to the class that reports another actor rather than crediting this one")
	})
}

// TestKafkaOperationsRunbook_TeachesArgvFreeCredentialCreation pins the runbook away from an
// example that leaked the credential it was creating.
//
// The "Adding a runtime SCRAM user" section showed:
//
//	docker compose exec kafka .../kafka-configs.sh --alter \
//	  --add-config "SCRAM-SHA-512=[iterations=4096,password=$NEW_PASSWORD]" ...
//
// which places a broker password in the argv of two processes at once — the host's docker client
// and kafka-configs.sh in the container. /proc/<pid>/cmdline is mode 444 on Linux, so every
// account on the host could read it for as long as the command ran, and it landed in shell
// history besides.
//
// Worse than the example was the advice under it, which recommended preferring "a
// --command-config-style properties file". That does not work for SCRAM: kafka-configs.sh
// normalises the mechanism name out of a properties file and rejects its own input with
// `Invalid credential property SCRAM_SHA_512=[...]`, verified against apache/kafka:3.9.2. An
// operator following the guidance hit an error the guidance did not predict.
//
// The section now feeds the password on stdin, which removes the host-side exposure entirely,
// records why a config file is not the answer so nobody re-derives it, and points at the API path
// where the question does not arise at all.
func TestKafkaOperationsRunbook_TeachesArgvFreeCredentialCreation(t *testing.T) {
	runbook := readRepoFile(t, filepath.Join("docs", "kafka-operations.md"))

	section := "### Adding a runtime SCRAM user"
	start := strings.Index(runbook, section)
	require.Greaterf(t, start, 0, "docs/kafka-operations.md must document adding a runtime SCRAM user")
	end := strings.Index(runbook[start+len(section):], "\n### ")
	require.Greater(t, end, 0, "the section must be bounded by the next heading")
	body := runbook[start : start+len(section)+end]

	// THE INLINE PASSWORD ARGUMENT MUST NOT COME BACK.
	assert.NotContains(t, body, "password=$NEW_PASSWORD]",
		"the runbook must not show a password interpolated into --add-config: that places it in "+
			"argv, and /proc/<pid>/cmdline is mode 444 — readable by every account on the host")

	// THE STDIN MECHANISM MUST BE THE ONE SHOWN.
	assert.Contains(t, body, `printf '%s\n' "$NEW_PASSWORD" | docker compose exec -T kafka`,
		"the password must be fed to the container on stdin, so it appears in no command line on "+
			"the host — neither the shell's nor the docker client's")
	assert.Contains(t, body, "read -r pw",
		"the container-side shell must read the password from stdin rather than receive it as an "+
			"argument")
	assert.Contains(t, body, "read -rs NEW_PASSWORD",
		"the runbook must show the password being read without echo, so it stays out of the "+
			"terminal and out of shell history")
	assert.Contains(t, body, "unset NEW_PASSWORD",
		"and being dropped from the shell afterwards")

	// THE DEAD END MUST BE RECORDED, so the next reader does not spend the afternoon rediscovering
	// that the obvious fix is the one that cannot work.
	assert.Contains(t, body, "Invalid credential property SCRAM_SHA_512",
		"the runbook must record that --add-config-file cannot express a SCRAM credential, and "+
			"quote the error it produces, because it is the natural thing to reach for and it "+
			"fails in a way that does not explain itself")
	assert.NotContains(t, body, "prefer a `--command-config`-style properties file",
		"that advice was wrong — a properties file cannot carry a SCRAM credential — and must "+
			"not be restored")

	// AND THE PATH WHERE THE PROBLEM DOES NOT EXIST MUST BE NAMED.
	assert.Contains(t, body, "AlterUserScramCredentials",
		"the runbook must point at the API path, which sends the credential over the Kafka "+
			"protocol from inside the server process and therefore has no command line at all")
}

// TestKafkaProvisionScript_ReportsExactlyWhatItDidToTheCatalogue is MIN-13, executed.
//
// # What the summary is for
//
// `kafka-topics --create --if-not-exists` makes creation idempotent by making it
// INDISTINGUISHABLE: an existing topic and a freshly created one both leave exit 0 and both
// land in the same reconciliation. So a run whose catalogue had been lost and silently rebuilt
// printed output identical to a run where nothing had changed — the whole catalogue, the right
// partition counts, and no hint that every offset had just restarted from zero. The daily
// outbox-versus-offset reconciliation in docs/kafka-operations.md cannot be performed without
// knowing the difference: it compares outbox rows against broker offsets, and a recreated
// topic invalidates the comparison silently.
//
// The dispositions are the answer to that, and their whole value is that they are EXACT. Two
// properties therefore have to hold together, and neither implies the other:
//
//   - The four buckets sum to the catalogue size, so no topic is unaccounted for.
//   - Each topic is in the bucket that describes what happened to it, so a count cannot be
//     right for the wrong reasons.
//
// # Why every case is a full run
//
// The dispositions are decided by comparisons across separate CLI invocations — a describe
// before the create against a describe after it, a partition count read, altered and re-read —
// so no static reading of the script can establish them. Each case below starts the stubbed
// broker in a different state and requires the exact totals, the exact names, and the final
// state of the broker itself.
func TestKafkaProvisionScript_ReportsExactlyWhatItDidToTheCatalogue(t *testing.T) {
	// Derived from the same source the script derives it from — the category list — rather
	// than written down. The script's own comment says the count follows EVENT_CATEGORIES so
	// that adding a category leaves no stale number behind, and a test carrying a literal 8
	// would be exactly that stale number.
	catalogue := AllTopicsWithDeadLettersForPrefix(DefaultTopicPrefix)
	require.NotEmpty(t, catalogue, "the topic catalogue must not be empty")

	t.Run("an absent catalogue is reported as created, every topic named", func(t *testing.T) {
		// The case that matters most operationally: this is what a rebuilt catalogue looks
		// like, and reporting it as "unchanged" is what made a lost catalogue invisible.
		stub := newKafkaCatalogueStub(t, alterSucceeds, topicsAreEmpty)

		outcome := runCatalogueProvisioning(t, stub, nil)
		require.Truef(t, outcome.succeeded,
			"provisioning an absent catalogue must succeed.\n--- output ---\n%s", outcome.output)

		parsed := outcome.dispositions(t)
		assert.Equal(t, len(catalogue), parsed.total, "every catalogue topic must be reported")
		assert.Equalf(t, len(catalogue), parsed.created,
			"every topic was absent and had to be created, so created must be %d. A run that "+
				"rebuilt the catalogue and reported it as unchanged is indistinguishable from "+
				"one that found it already correct — and every offset restarted from zero.\n"+
				"--- output ---\n%s", len(catalogue), outcome.output)
		assert.Zero(t, parsed.grown, "nothing existed to grow")
		assert.Zero(t, parsed.unchanged, "nothing existed to leave unchanged")
		assert.Zero(t, parsed.refused, "nothing existed to refuse")
		assert.ElementsMatchf(t, catalogue, parsed.createdNamed,
			"the created bucket must NAME each topic, not only count them: the count answers "+
				"\"how many\" and the operator's next question is \"which\"")

		// The broker must actually be where the summary says. A summary that is right about a
		// catalogue the run failed to build is the worse of the two failures.
		for _, topic := range catalogue {
			assert.Equalf(t, 6, stub.partitionsOf(t, topic),
				"%s must exist at the configured partition count", topic)
		}
	})

	t.Run("a correct catalogue is reported as unchanged, with nothing named", func(t *testing.T) {
		stub := newKafkaCatalogueStub(t, alterSucceeds, topicsAreEmpty)
		stub.seedCatalogue(t, 6, catalogue)

		outcome := runCatalogueProvisioning(t, stub, nil)
		require.Truef(t, outcome.succeeded,
			"provisioning an already-correct catalogue must succeed.\n--- output ---\n%s",
			outcome.output)

		parsed := outcome.dispositions(t)
		assert.Equal(t, len(catalogue), parsed.total)
		assert.Zerof(t, parsed.created,
			"nothing was created. The create call is still ISSUED — --if-not-exists makes it a "+
				"no-op — so a disposition derived from that call's exit status rather than from "+
				"the probe before it would report every topic as created here.\n--- output ---\n%s",
			outcome.output)
		assert.Zero(t, parsed.grown, "the geometry was already correct")
		assert.Equal(t, len(catalogue), parsed.unchanged, "every topic was already correct")
		assert.Zero(t, parsed.refused)
		assert.Empty(t, parsed.createdNamed)
		assert.Empty(t, parsed.grownNamed)

		// And the run must not have printed a refusal section, because there was nothing to
		// refuse. A bucket that appears on a healthy run is noise in the one output an
		// operator reads at a glance.
		assert.NotContains(t, outcome.output, "Left under-partitioned",
			"a healthy catalogue must not print an under-partitioned section")
	})

	t.Run("an under-partitioned empty catalogue is reported as grown", func(t *testing.T) {
		stub := newKafkaCatalogueStub(t, alterSucceeds, topicsAreEmpty)
		stub.seedCatalogue(t, 3, catalogue)

		outcome := runCatalogueProvisioning(t, stub, nil)
		require.Truef(t, outcome.succeeded,
			"growing an empty under-partitioned catalogue must succeed.\n--- output ---\n%s",
			outcome.output)

		parsed := outcome.dispositions(t)
		assert.Zero(t, parsed.created, "the topics existed")
		assert.Equalf(t, len(catalogue), parsed.grown,
			"every topic was below the configured count and provably empty, so every one was "+
				"grown.\n--- output ---\n%s", outcome.output)
		assert.Zero(t, parsed.unchanged)
		assert.Zero(t, parsed.refused, "an EMPTY topic is the one case growth is always allowed")
		assert.ElementsMatch(t, catalogue, parsed.grownNamed)

		for _, topic := range catalogue {
			assert.Equalf(t, 6, stub.partitionsOf(t, topic),
				"%s must have reached the configured partition count", topic)
		}
	})

	t.Run("losing a race to another provisioner is raced, and still succeeds", func(t *testing.T) {
		// Two provisioners run against one broker routinely: the compose kafka-init one-shot
		// and a manual `make kafka_provision`. The loser's alter fails, and it fails with a
		// message that says the topic ALREADY has that many partitions — so the topic is
		// correct and the run must not treat it as a failure.
		//
		// The disposition is `raced` rather than `grown`, and that is the deliberate answer of
		// TestKafkaProvisionScript_GrownIsClaimedOnlyWhenThisRunGrewIt: the summary is what an
		// operator reads to learn what THIS invocation did, so a run must not claim an alter it
		// did not issue. The catalogue is still fully accounted for, because `raced` is one of
		// the classes the identity below sums, and the per-topic line names what happened.
		stub := newKafkaCatalogueStub(t, alterLosesARace, topicsAreEmpty)
		stub.seedCatalogue(t, 3, catalogue)

		outcome := runCatalogueProvisioning(t, stub, nil)
		require.Truef(t, outcome.succeeded,
			"a lost growth race must NOT fail the run: the topic is at the target count, which "+
				"is the outcome that was wanted. Failing here would make a bring-up flaky in "+
				"exactly the configuration the stack ships — compose's one-shot and the makefile "+
				"target against one broker.\n--- output ---\n%s", outcome.output)

		assert.Containsf(t, outcome.output, "another provisioner grew it",
			"the run must SAY the topic was grown by someone else. It is the difference between "+
				"a benign race and an alter this run performed, and the audit trail needs the "+
				"distinction.\n--- output ---\n%s", outcome.output)

		parsed := outcome.dispositions(t)
		assert.Equalf(t, len(catalogue), parsed.raced,
			"the alter was another provisioner's, so this run must not claim it: the topics "+
				"belong to the class that names the uncertainty rather than resolving it")
		assert.ElementsMatch(t, catalogue, parsed.racedNamed,
			"and the raced line must name them, so a reader can re-run to confirm")
		assert.Zerof(t, parsed.grown,
			"`grown` is a claim about what THIS invocation did, and the summary is the only "+
				"record of that; a run that claims an alter it did not issue makes the audit "+
				"trail unusable for the one question it answers")
		assert.Zero(t, parsed.created)
		assert.Zero(t, parsed.unchanged)
		assert.Zero(t, parsed.refused)

		for _, topic := range catalogue {
			assert.Equalf(t, 6, stub.partitionsOf(t, topic),
				"%s must be at the configured count after the race", topic)
		}
	})

	t.Run("a refused growth is its own disposition and is not counted as unchanged", func(t *testing.T) {
		// The misclassification the finding names. A topic that holds records is deliberately
		// left under-partitioned, and the run still succeeds — an under-partitioned topic is a
		// throughput limit, not an outage. Reporting it as "unchanged" is what makes the
		// deliberate refusal indistinguishable from a catalogue that needed nothing.
		stub := newKafkaCatalogueStub(t, alterSucceeds, topicsHoldRecords)
		stub.seedCatalogue(t, 3, catalogue)

		outcome := runCatalogueProvisioning(t, stub, nil)
		require.Truef(t, outcome.succeeded,
			"a refused growth must not fail the run.\n--- output ---\n%s", outcome.output)

		parsed := outcome.dispositions(t)
		assert.Equalf(t, len(catalogue), parsed.refused,
			"every topic held records and growth was not permitted, so every one belongs in the "+
				"under-partitioned bucket.\n--- output ---\n%s", outcome.output)
		assert.Zerof(t, parsed.unchanged,
			"a refused growth is NOT \"unchanged\". Folding the two together printed "+
				"\"created=0 grown=0 unchanged=8\" for a catalogue in which every topic was "+
				"stuck at 3 partitions against a configured 6, with the only trace a per-topic "+
				"warning several screens up.\n--- output ---\n%s", outcome.output)
		assert.Zero(t, parsed.created)
		assert.Zero(t, parsed.grown)
		assert.ElementsMatch(t, catalogue, parsed.refusedNamed)

		// The closing summary — the one place the catalogue is shown as a whole — must
		// name them with the remedy, because nothing else will ever report this.
		assert.Containsf(t, outcome.output, "Left under-partitioned",
			"the closing summary must name the under-partitioned topics: no alert fires for a "+
				"throughput ceiling, no consumer errors, and the next run refuses it just as "+
				"quietly.\n--- output ---\n%s", outcome.output)
		assert.Contains(t, outcome.output, "KAFKA_ALLOW_PARTITION_GROWTH=true",
			"the refusal must carry the remedy that overrides it")
		assert.Contains(t, outcome.output, "KAFKA_MIN_PARTITIONS",
			"the refusal must carry the remedy that accepts the current count")

		// Nothing was grown on the broker either. The summary and the broker must agree.
		for _, topic := range catalogue {
			assert.Equalf(t, 3, stub.partitionsOf(t, topic),
				"%s must be left at its original partition count", topic)
		}
	})

	t.Run("an indeterminate record state refuses growth rather than guessing", func(t *testing.T) {
		// The offsets tool failed, so emptiness could not be proven. That is not evidence of
		// emptiness, and growing on the assumption would re-map ledger keys — silently, with
		// nothing failing to say the ordering guarantee had stopped holding.
		stub := newKafkaCatalogueStub(t, alterSucceeds, recordStateIsUnknown)
		stub.seedCatalogue(t, 3, catalogue)

		outcome := runCatalogueProvisioning(t, stub, nil)
		require.Truef(t, outcome.succeeded,
			"an indeterminate record state must not fail the run.\n--- output ---\n%s",
			outcome.output)

		parsed := outcome.dispositions(t)
		assert.Equal(t, len(catalogue), parsed.refused,
			"an unprovable emptiness must be refused, not guessed")
		assert.Zero(t, parsed.grown)
		assert.Contains(t, outcome.output, "COULD NOT BE DETERMINED",
			"the run must say WHY it declined, so the operator can fix the probe")

		for _, topic := range catalogue {
			assert.Equalf(t, 3, stub.partitionsOf(t, topic),
				"%s must be left alone when emptiness could not be proven", topic)
		}
	})

	t.Run("explicit consent grows a topic that holds records", func(t *testing.T) {
		// The override, so the refusal above is proven to be a decision rather than an
		// inability. Without this arm a script that could never grow a non-empty topic under
		// any circumstances would pass the refusal case too.
		stub := newKafkaCatalogueStub(t, alterSucceeds, topicsHoldRecords)
		stub.seedCatalogue(t, 3, catalogue)

		outcome := runCatalogueProvisioning(t, stub,
			map[string]string{"KAFKA_ALLOW_PARTITION_GROWTH": "true"})
		require.Truef(t, outcome.succeeded,
			"a consented growth must succeed.\n--- output ---\n%s", outcome.output)

		parsed := outcome.dispositions(t)
		assert.Equal(t, len(catalogue), parsed.grown,
			"consent was given, so every under-partitioned topic was grown")
		assert.Zero(t, parsed.refused, "nothing is refused once consent is given")
		assert.Contains(t, outcome.output, "ordering of existing events is not preserved",
			"the consented growth must still WARN what was traded away: the operator consented "+
				"to a re-mapping, and the run is the record of when it happened")

		for _, topic := range catalogue {
			assert.Equalf(t, 6, stub.partitionsOf(t, topic), "%s must have been grown", topic)
		}
	})

	t.Run("an over-partitioned catalogue is left alone and reported, never shrunk", func(t *testing.T) {
		stub := newKafkaCatalogueStub(t, alterSucceeds, topicsAreEmpty)
		stub.seedCatalogue(t, 12, catalogue)

		outcome := runCatalogueProvisioning(t, stub, nil)
		require.Truef(t, outcome.succeeded,
			"more partitions than configured is not an error.\n--- output ---\n%s", outcome.output)

		parsed := outcome.dispositions(t)
		assert.Zero(t, parsed.created)
		assert.Zerof(t, parsed.grown,
			"a topic above the configured count must not be touched at all: Kafka cannot reduce "+
				"a partition count, and reducing one would move ledger keys between partitions")
		assert.Equal(t, len(catalogue), parsed.unchanged)
		assert.Contains(t, outcome.output, "more than the configured",
			"the excess must be reported so configuration and reality can be reconciled")

		for _, topic := range catalogue {
			assert.Equalf(t, 12, stub.partitionsOf(t, topic),
				"%s must keep its partitions; no alter may be attempted", topic)
		}

		// No alter was ISSUED, which is the property behind "never shrunk". Kafka would refuse
		// it, so an attempt would merely be a noisy failure — but the script must not make one.
		for _, invocation := range outcome.invocations {
			assert.NotContainsf(t, invocation, "--alter --topic",
				"no partition alter may be issued for an over-partitioned catalogue: %s",
				invocation)
		}
	})

	t.Run("an alter that genuinely fails is fatal", func(t *testing.T) {
		// The other side of the race case. When the alter fails AND the topic is still below
		// the target, the topic is wrong and the run must say so rather than report a geometry
		// it does not have.
		stub := newKafkaCatalogueStub(t, alterIsRefused, topicsAreEmpty)
		stub.seedCatalogue(t, 3, catalogue)

		outcome := runCatalogueProvisioning(t, stub, nil)
		require.Falsef(t, outcome.succeeded,
			"an alter that failed and left the topic under-partitioned must fail the run: the "+
				"catalogue does not have the geometry this script exists to guarantee.\n"+
				"--- output ---\n%s", outcome.output)
		assert.Contains(t, outcome.output, "could not grow topic",
			"the failure must name what could not be done")
		assert.Contains(t, outcome.output, "Alter authority",
			"the failure must name the likely cause, which is an authorization gap")
	})
}

// catalogueStubMode names how the stubbed `kafka-topics --alter` behaves.
type catalogueStubMode string

const (
	// alterSucceeds is the ordinary growth: the alter is accepted and the topic reaches the
	// target partition count.
	alterSucceeds catalogueStubMode = "grow"
	// alterLosesARace is the concurrent-provisioner case. The topic reaches the target — some
	// other provisioner got there first — and the alter still reports failure, which is what
	// Kafka does when it is asked to grow a topic that already has that many partitions.
	alterLosesARace catalogueStubMode = "race"
	// alterIsRefused is an authorization failure: the alter fails and the topic does NOT reach
	// the target. This one must be fatal.
	alterIsRefused catalogueStubMode = "fail"
)

// catalogueRecordState names what the stubbed `kafka-get-offsets` reports.
type catalogueRecordState string

const (
	// topicsAreEmpty makes every partition report latest offset zero, which is the only state
	// in which growing a topic cannot re-map an existing key.
	topicsAreEmpty catalogueRecordState = "empty"
	// topicsHoldRecords makes every partition report a non-zero latest offset.
	topicsHoldRecords catalogueRecordState = "records"
	// recordStateIsUnknown makes the offsets tool fail, which is neither proof of emptiness nor
	// proof of records and must be treated as the former's absence.
	recordStateIsUnknown catalogueRecordState = "unknown"
)

// kafkaCatalogueStub is a stubbed broker: a directory of recording CLI stubs plus the mutable
// topic state they read and write between invocations.
//
// # Why the state is a directory of files
//
// The dispositions under test are all differences between two moments — a topic that did not
// exist before the create and does after, a partition count that was three and is now six —
// and the script observes each of them with a SEPARATE process invocation. A stub that
// answered from a fixed table could not model that: `--describe` before the create and
// `--describe` after it have to give different answers, and the only thing shared between two
// stub processes is the filesystem. One file per topic holding its partition count, absent
// when the topic does not exist, is the smallest thing that does it.
type kafkaCatalogueStub struct {
	// binDir goes at the front of PATH.
	binDir string
	// stateDir holds one `<topic>.partitions` file per existing topic.
	stateDir string
	// log is the file every stub invocation appends its argv to.
	log string
	// alter is how `--alter` behaves.
	alter catalogueStubMode
	// records is what the offsets tool reports.
	records catalogueRecordState
	// replication is the factor `--describe` reports, so the run can be held to a real
	// observed value rather than the configured one.
	replication int
}

// catalogueOutcome is one provisioning run against a stubbed broker.
type catalogueOutcome struct {
	// output is stdout and stderr combined.
	output string
	// succeeded is whether the script exited zero.
	succeeded bool
	// invocations is every stubbed CLI call, in order.
	invocations []string
	// stub is the broker it ran against, so a test can read the final topic state.
	stub kafkaCatalogueStub
}

// catalogueDisposition is the four-way verdict the script prints about what it DID to the
// catalogue, parsed back out of its own output.
type catalogueDisposition struct {
	total     int
	created   int
	grown     int
	unchanged int
	// refused is the "under-partitioned" bucket, which is printed only when non-empty.
	refused int
	// refusedNamed are the topic names listed beside that bucket.
	refusedNamed []string
	// raced is the class that names an uncertainty rather than resolving it: an unreadable
	// pre-state, an alter another provisioner won, or an accepted alter that has not converged.
	raced int
	// racedNamed are the topic names listed beside it.
	racedNamed []string
	// createdNamed and grownNamed are likewise the names, so a count cannot pass while the
	// list beside it is wrong.
	createdNamed []string
	grownNamed   []string
}

// newKafkaCatalogueStub writes a stubbed Kafka CLI that models a real topic catalogue.
//
// Four tools are stubbed, because the disposition logic reaches all four: kafka-topics for
// list, describe, create and alter; kafka-configs so the credential probe answers "present"
// and no principal is rewritten; kafka-acls so grants succeed; and kafka-get-offsets, WITHOUT
// which topic_record_state answers "unknown" for every topic and the growth gate refuses
// everything — which would make the grown case unreachable and the refused case pass for the
// wrong reason.
//
// Parameters:
//   - t *testing.T: owns the temporary directories.
//   - alter catalogueStubMode: how `--alter` behaves.
//   - records catalogueRecordState: what the offsets tool reports.
//
// Returns:
//   - kafkaCatalogueStub: the stub, ready to seed and run.
func newKafkaCatalogueStub(
	t *testing.T,
	alter catalogueStubMode,
	records catalogueRecordState,
) kafkaCatalogueStub {
	t.Helper()

	root := t.TempDir()
	stub := kafkaCatalogueStub{
		binDir:      filepath.Join(root, "bin"),
		stateDir:    filepath.Join(root, "state"),
		log:         filepath.Join(root, "invocations.log"),
		alter:       alter,
		records:     records,
		replication: 1,
	}

	require.NoError(t, os.MkdirAll(stub.binDir, 0o750))
	require.NoError(t, os.MkdirAll(stub.stateDir, 0o750))
	require.NoError(t, os.WriteFile(stub.log, nil, 0o600))

	// Every stub opens with the same recorder, so an assertion can be made about a call that
	// was or was not issued as well as about the summary it produced.
	recorder := "#!/usr/bin/env bash\n" +
		"printf '%s' \"$(basename \"$0\")\" >> \"$STUB_LOG\"\n" +
		"for arg in \"$@\"; do printf ' %s' \"$arg\" >> \"$STUB_LOG\"; done\n" +
		"printf '\\n' >> \"$STUB_LOG\"\n"

	// kafka-topics: the state machine. The geometry line is Kafka's own format, because
	// topic_geometry parses PartitionCount and ReplicationFactor out of it with a regular
	// expression and a shape it cannot read is a fatal "geometry could not be read".
	topics := recorder +
		"mode=\"\"; topic=\"\"; parts=\"\"\n" +
		"args=(\"$@\")\n" +
		"for ((i=0;i<${#args[@]};i++)); do\n" +
		"  case \"${args[i]}\" in\n" +
		"    --describe) mode=describe ;;\n" +
		"    --create) mode=create ;;\n" +
		"    --alter) mode=alter ;;\n" +
		"    --list) mode=list ;;\n" +
		"    --topic) topic=\"${args[i+1]}\" ;;\n" +
		"    --partitions) parts=\"${args[i+1]}\" ;;\n" +
		"  esac\n" +
		"done\n" +
		"state=\"$STUB_STATE/${topic}.partitions\"\n" +
		"case \"$mode\" in\n" +
		"  list)\n" +
		"    ls \"$STUB_STATE\" 2>/dev/null | sed 's/\\.partitions$//'\n" +
		"    exit 0 ;;\n" +
		"  describe)\n" +
		"    if [ -f \"$state\" ]; then\n" +
		"      printf 'Topic: %s\\tTopicId: AAAAAAAAAAAAAAAAAAAAAA\\tPartitionCount: %s\\tReplicationFactor: %s\\tConfigs: \\n' \\\n" +
		"        \"$topic\" \"$(cat \"$state\")\" \"$STUB_REPLICATION\"\n" +
		"      exit 0\n" +
		"    fi\n" +
		// A topic that does not exist: Kafka exits non-zero with a stack trace, and
		// topic_geometry guards the call and reads the empty answer as absence.
		"    printf 'Error while executing topic command : Topic %s does not exist\\n' \"$topic\" >&2\n" +
		"    exit 1 ;;\n" +
		"  create)\n" +
		// --if-not-exists semantics: an existing topic is a no-op at exit 0, whatever its
		// partition count. Reproducing that is what makes the "created" probe meaningful.
		"    [ -f \"$state\" ] || printf '%s' \"$parts\" > \"$state\"\n" +
		"    exit 0 ;;\n" +
		"  alter)\n" +
		"    case \"$STUB_ALTER_MODE\" in\n" +
		"      grow) printf '%s' \"$parts\" > \"$state\"; exit 0 ;;\n" +
		"      race)\n" +
		// The other provisioner already grew it, and Kafka reports the alter as a failure.
		"        printf '%s' \"$parts\" > \"$state\"\n" +
		"        printf 'Error: Topic currently has %s partitions, which is higher than the requested\\n' \"$parts\" >&2\n" +
		"        exit 1 ;;\n" +
		"      *)\n" +
		"        printf 'Error: Authorization failed: Alter on the topic resource\\n' >&2\n" +
		"        exit 1 ;;\n" +
		"    esac ;;\n" +
		"esac\n" +
		"exit 0\n"

	// kafka-configs: the credential probe answers PRESENT, so no principal is generated or
	// rotated. That keeps this file's subject the catalogue rather than the credentials, which
	// TestKafkaProvisionScript_CredentialProbeIsAPredicate already owns.
	configs := recorder +
		"users=no; describe=no\n" +
		"for arg in \"$@\"; do\n" +
		"  [ \"$arg\" = users ] && users=yes\n" +
		"  [ \"$arg\" = --describe ] && describe=yes\n" +
		"done\n" +
		"if [ $users = yes ] && [ $describe = yes ]\n" +
		"then\n" +
		"  printf 'SCRAM credential configs for user-principal are SCRAM-SHA-512=iterations=8192\\n'\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 0\n"

	acls := recorder + "exit 0\n"

	// kafka-get-offsets: `topic:partition:offset` per line, which is the shape
	// topic_record_state parses. A failing tool is how "unknown" is produced, because that is
	// how it happens for real — an unreachable broker or a principal without Describe.
	offsets := recorder +
		"if [ \"$STUB_RECORD_STATE\" = unknown ]\n" +
		"then\n" +
		"  printf 'Error: could not read offsets\\n' >&2\n" +
		"  exit 1\n" +
		"fi\n" +
		"topic=\"\"\n" +
		"args=(\"$@\")\n" +
		"for ((i=0;i<${#args[@]};i++)); do [ \"${args[i]}\" = --topic ] && topic=\"${args[i+1]}\"; done\n" +
		"offset=0\n" +
		"[ \"$STUB_RECORD_STATE\" = records ] && offset=17\n" +
		"for partition in 0 1 2; do printf '%s:%s:%s\\n' \"$topic\" \"$partition\" \"$offset\"; done\n" +
		"exit 0\n"

	for name, body := range map[string]string{
		"kafka-topics":      topics,
		"kafka-configs":     configs,
		"kafka-acls":        acls,
		"kafka-get-offsets": offsets,
	} {
		require.NoErrorf(t,
			os.WriteFile(filepath.Join(stub.binDir, name), []byte(body), 0o700),
			"writing the %s stub", name)
	}

	return stub
}

// seedCatalogue makes the given topics exist at a partition count, so a run can start from a
// catalogue that is already correct, already under-partitioned, or over-partitioned.
func (stub kafkaCatalogueStub) seedCatalogue(t *testing.T, partitions int, topics []string) {
	t.Helper()

	for _, topic := range topics {
		require.NoError(t, os.WriteFile(
			filepath.Join(stub.stateDir, topic+".partitions"),
			[]byte(strconv.Itoa(partitions)), 0o600))
	}
}

// partitionsOf reports a topic's partition count on the stubbed broker, or -1 when the topic
// does not exist. It is how a test checks that the broker ended up where the summary says.
func (stub kafkaCatalogueStub) partitionsOf(t *testing.T, topic string) int {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(stub.stateDir, topic+".partitions")) //nolint:gosec // a path this test wrote
	if os.IsNotExist(err) {
		return -1
	}
	require.NoErrorf(t, err, "reading the stubbed state of %s", topic)

	count, convErr := strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoErrorf(t, convErr, "the stubbed state of %s must be a number", topic)

	return count
}

// runCatalogueProvisioning executes the real scripts/kafka-provision.sh against a stubbed
// broker and returns what it printed and did.
//
// The environment is built from scratch, as in runProvisioningScript, so an ambient .env or a
// developer's KAFKA_* variables cannot decide the outcome. The sample subscriber is skipped
// by default: it needs a credential destination, and it is not what these cases are about.
func runCatalogueProvisioning(
	t *testing.T,
	stub kafkaCatalogueStub,
	extra map[string]string,
) catalogueOutcome {
	t.Helper()

	root := moduleRootDir(t)

	environment := map[string]string{
		"PATH": stub.binDir + ":/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME": t.TempDir(),
		// The stub's own channel, read by the four CLI stubs and by nothing in the script.
		"STUB_LOG":          stub.log,
		"STUB_STATE":        stub.stateDir,
		"STUB_ALTER_MODE":   string(stub.alter),
		"STUB_RECORD_STATE": string(stub.records),
		"STUB_REPLICATION":  strconv.Itoa(stub.replication),

		"KAFKA_BOOTSTRAP_SERVER": "stub:9092",
		"KAFKA_SASL_ADMIN_USER":  "admin",
		// #nosec G101 -- a literal for a stubbed broker that is never contacted.
		"KAFKA_SASL_ADMIN_SECRET": "AdminSecretForTheStubbedBroker0123456789",
		// #nosec G101 -- likewise.
		"KAFKA_PRODUCER_SECRET":        "ProducerSecretForTheStubbedBroker0123456",
		"KAFKA_SKIP_SAMPLE_SUBSCRIBER": "true",

		"KAFKA_PROVISION_TIMEOUT_SECONDS":       "5",
		"KAFKA_PROVISION_POLL_INTERVAL_SECONDS": "1",
		// One replica, matched by the stub's reported factor, so require_topic_replication is
		// satisfied by an OBSERVED value rather than skipped.
		"KAFKA_REPLICATION_FACTOR": "1",
	}
	for key, value := range extra {
		if value == "" {
			delete(environment, key)

			continue
		}

		environment[key] = value
	}

	command := exec.Command("bash", filepath.Join(root, "scripts", "kafka-provision.sh")) //nolint:gosec // a fixed path inside the repository
	command.Dir = root
	command.Env = make([]string, 0, len(environment))
	for key, value := range environment {
		command.Env = append(command.Env, key+"="+value)
	}

	output, err := command.CombinedOutput()

	recorded, readErr := os.ReadFile(stub.log) //nolint:gosec // a path this test wrote
	require.NoError(t, readErr, "the stub invocation log must be readable")

	invocations := make([]string, 0, 64)
	for _, line := range strings.Split(string(recorded), "\n") {
		if strings.TrimSpace(line) != "" {
			invocations = append(invocations, strings.TrimSpace(line))
		}
	}

	return catalogueOutcome{
		output:      string(output),
		succeeded:   err == nil,
		invocations: invocations,
		stub:        stub,
	}
}

// dispositionSummaryPattern reads the four buckets back out of the script's own output.
//
// Parsed from the rendered line rather than computed alongside the script, because the finding
// is about what the AUDIT TRAIL says: an operator reads these numbers, and a count that is
// right internally and wrong on screen is the defect.
var (
	dispositionTotalPattern   = regexp.MustCompile(`all (\d+) topics are present with a verified geometry`)
	dispositionCreatedPattern = regexp.MustCompile(`created\s+: (\d+)(?: \(([^)]*)\))?`)
	dispositionGrownPattern   = regexp.MustCompile(`grown\s+: (\d+)(?: \(([^)]*)\))?`)
	// A SIGN IS ACCEPTED here deliberately. "unchanged" is the one bucket that is computed by
	// subtraction rather than counted, so a bucket that stops being subtracted — or one that is
	// subtracted twice — renders as a negative number. Matching only digits would report that
	// as "the run printed no unchanged disposition", which sends a reader looking for a missing
	// line instead of at the arithmetic. Read the sign, then refuse it below.
	dispositionUnchangedPattern = regexp.MustCompile(`unchanged\s+: (-?\d+)`)
	dispositionRefusedPattern   = regexp.MustCompile(`under-partitioned : (\d+) \(([^)]*)\)`)
	// The class that ABSTAINS. It is printed only when it is non-empty — an empty class printed
	// every run is a line that stops being read — so it is optional here, and it has to be in
	// the accounting identity below or a raced topic is in no bucket at all and the four figures
	// silently stop summing to the catalogue.
	dispositionRacedPattern = regexp.MustCompile(`raced\s+: (\d+) \(([^)]*)\)`)
)

// dispositions parses the created / grown / unchanged / under-partitioned line.
func (outcome catalogueOutcome) dispositions(t *testing.T) catalogueDisposition {
	t.Helper()

	parsed := catalogueDisposition{}

	readCount := func(pattern *regexp.Regexp, label string, required bool) (int, []string) {
		match := pattern.FindStringSubmatch(outcome.output)
		if match == nil {
			require.Falsef(t, required,
				"the run must print a %q disposition; the summary is the audit trail this "+
					"whole mechanism exists to produce.\n--- output ---\n%s", label, outcome.output)

			return 0, nil
		}

		count, err := strconv.Atoi(match[1])
		require.NoErrorf(t, err, "the %q count must be a number, got %q", label, match[1])

		var named []string
		if len(match) > 2 && strings.TrimSpace(match[2]) != "" {
			named = strings.Fields(match[2])
		}

		return count, named
	}

	parsed.total, _ = readCount(dispositionTotalPattern, "total", true)
	parsed.created, parsed.createdNamed = readCount(dispositionCreatedPattern, "created", true)
	parsed.grown, parsed.grownNamed = readCount(dispositionGrownPattern, "grown", true)
	parsed.unchanged, _ = readCount(dispositionUnchangedPattern, "unchanged", true)
	parsed.refused, parsed.refusedNamed = readCount(dispositionRefusedPattern, "under-partitioned", false)
	parsed.raced, parsed.racedNamed = readCount(dispositionRacedPattern, "raced", false)

	// Held for EVERY case rather than restated in each. A bucket cannot be negative and the
	// four cannot sum to anything but the catalogue size: either failure means a topic is
	// counted twice or not at all, and the summary is then not a record of anything.
	require.GreaterOrEqualf(t, parsed.unchanged, 0,
		"the unchanged bucket is computed by subtraction, and a negative value means a "+
			"disposition is being subtracted that was never added, or added twice.\n"+
			"--- output ---\n%s", outcome.output)
	require.Equalf(t, parsed.total,
		parsed.created+parsed.grown+parsed.unchanged+parsed.refused+parsed.raced,
		"created + grown + unchanged + under-partitioned + raced must equal the catalogue size, "+
			"or some topic's disposition is unaccounted for.\n--- output ---\n%s", outcome.output)

	return parsed
}
