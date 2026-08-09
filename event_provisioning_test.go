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
