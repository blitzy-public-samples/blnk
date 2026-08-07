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
	"regexp"
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

	assert.Contains(t, script, "deliver_generated_secret",
		"the script must route a generated credential through the mode-0600 delivery helper; a "+
			"printf of the value is what put a password into the container log")
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
	passthrough := string(stack)
	passthrough = passthrough[strings.Index(passthrough, "kafka_provision_passthrough=("):]
	passthrough = passthrough[:strings.Index(passthrough, "\n)")]

	for _, variable := range []string{sampleSubscriberSecretVar, sampleSubscriberSecretFileVar} {
		assert.Containsf(t, passthrough, variable,
			"stack.sh's provisioning pass-through must forward %s, or the host fallback runs with the "+
				"script's default rather than with what the operator configured", variable)
	}

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
