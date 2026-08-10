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
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateAndAddDefaults_InsecureWarning(t *testing.T) {
	baseConfig := func() Configuration {
		return Configuration{
			ProjectName: "Test Project",
			DataSource:  DataSourceConfig{Dns: "some-dns"},
			Redis:       RedisConfig{Dns: "localhost:6379"},
		}
	}

	t.Run("warns when secure is disabled", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := baseConfig()
		cnf.Server.Secure = false
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		var warned bool
		for _, entry := range hook.AllEntries() {
			if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "authentication is DISABLED") {
				warned = true
			}
		}
		if !warned {
			t.Errorf("Expected a SECURITY warning when server.secure is false")
		}
	})

	t.Run("no warning when secure is enabled", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := baseConfig()
		cnf.Server.Secure = true
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, "authentication is DISABLED") {
				t.Errorf("Did not expect a SECURITY warning when server.secure is true")
			}
		}
	})
}

func TestValidateAndAddDefaults(t *testing.T) {
	// Test case with empty ProjectName and DataSource DNS
	cnf := Configuration{
		ProjectName: "",
		DataSource: DataSourceConfig{
			Dns: "",
		},
		Redis: RedisConfig{
			Dns: "localhost:6379",
		},
	}

	err := cnf.validateAndAddDefaults()
	if err == nil || err.Error() != "data source DNS is required" {
		t.Errorf("Expected data source DNS required error, got %v", err)
	}
	cnf = Configuration{
		ProjectName: "",
		DataSource: DataSourceConfig{
			Dns: "postgres://localhost:5432",
		},
		Redis: RedisConfig{
			Dns: "",
		},
	}

	err = cnf.validateAndAddDefaults()
	if err == nil || err.Error() != "redis DNS is required" {
		t.Errorf("Expected redis DNS required error, got %v", err)
	}
	// Test case with all required fields filled, expect no error
	cnf = Configuration{
		ProjectName: "Test Project",
		DataSource: DataSourceConfig{
			Dns: "some-dns",
		},
		Redis: RedisConfig{
			Dns: "localhost:6379",
		},
	}

	err = cnf.validateAndAddDefaults()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Test default port setting
	cnf.Server.Port = ""
	err = cnf.validateAndAddDefaults()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if cnf.Server.Port != DEFAULT_PORT {
		t.Errorf("Expected default port %s, got %s", DEFAULT_PORT, cnf.Server.Port)
	}
	if cnf.Transaction.LockWaitTimeout != 3*time.Second {
		t.Errorf("Expected default lock wait timeout %s, got %s", 3*time.Second, cnf.Transaction.LockWaitTimeout)
	}
	if !cnf.Transaction.EnableCoalescing {
		t.Errorf("Expected coalescing to default to enabled")
	}
}

func TestValidateAndAddDefaults_TransactionLockWaitTimeout(t *testing.T) {
	cnf := Configuration{
		ProjectName: "Test Project",
		DataSource: DataSourceConfig{
			Dns: "some-dns",
		},
		Redis: RedisConfig{
			Dns: "localhost:6379",
		},
		Transaction: TransactionConfig{
			LockWaitTimeout: 12,
		},
	}

	err := cnf.validateAndAddDefaults()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if cnf.Transaction.LockWaitTimeout != 12*time.Second {
		t.Fatalf("Expected LockWaitTimeout to be 12s, got %s", cnf.Transaction.LockWaitTimeout)
	}
}

func TestValidateAndAddDefaults_TransactionLockWaitTimeoutAlreadyDuration(t *testing.T) {
	cnf := Configuration{
		ProjectName: "Test Project",
		DataSource: DataSourceConfig{
			Dns: "some-dns",
		},
		Redis: RedisConfig{
			Dns: "localhost:6379",
		},
		Transaction: TransactionConfig{
			LockWaitTimeout: 5 * time.Second,
		},
	}

	err := cnf.validateAndAddDefaults()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if cnf.Transaction.LockWaitTimeout != 5*time.Second {
		t.Fatalf("Expected LockWaitTimeout to remain 5s, got %s", cnf.Transaction.LockWaitTimeout)
	}
}

func TestLoadConfigFromFile(t *testing.T) {
	// This test asserts what loadConfigFromFile does with a FILE, so the event-streaming
	// variables must not reach it from the surrounding environment. They are cleared because
	// an exported KAFKA_BROKERS — which is what the local stack and the integration tests set
	// — is picked up by envconfig for a fixture that states no deprecation window, and the
	// load then correctly refuses. The refusal is the intended behaviour; inheriting the
	// variable is not.
	clearEventStreamingEnv(t)
	// And the BLNK_-prefixed forms, for the same reason applied to the rest of the
	// configuration tree: the assertions below say a value came out of the FILE, and
	// an exported BLNK_DATA_SOURCE_DNS — the ordinary way to point this suite at a
	// relocated Postgres — overlays it, so without this sweep the DataSource.Dns
	// assertion fails while the loader is behaving exactly as designed.
	clearBlnkPrefixedEnv(t)

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	// t.Cleanup rather than a bare defer, and the failure fails the test: removing a file
	// this test just created cannot fail for a benign reason, and a run that leaks a temp
	// file into /tmp on every execution should not be able to report itself as clean.
	t.Cleanup(func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Errorf("unable to remove the temporary config file %q: %v", tmpFile.Name(), err)
		}
	})

	// Sample configuration to write to the temp file
	sampleConfig := Configuration{
		ProjectName: "Temp Project",
		DataSource: DataSourceConfig{
			Dns: "temp-dns",
		},
		Redis: RedisConfig{
			Dns: "temp-redis",
		},
		Transaction: TransactionConfig{
			LockWaitTimeout: 7,
		},
	}
	if err := json.NewEncoder(tmpFile).Encode(sampleConfig); err != nil {
		t.Fatalf("Unable to write to temporary file: %v", err)
	}
	// Closed so loadConfigFromFile can open it, and the error is checked because a
	// failed close can mean the encoded JSON was never flushed — which would make the
	// rest of this test assert against an empty file.
	require.NoError(t, tmpFile.Close())

	// Set an environment variable to override the project name
	// t.Setenv restores the previous value automatically at the end of the test, which
	// is what the defer was hand-rolling, and it fails the test outright if the
	// variable cannot be set rather than proceeding with an unset override.
	t.Setenv("BLNK_PROJECT_NAME", "Env Project")

	// Load the configuration from the file
	if err := loadConfigFromFile(tmpFile.Name()); err != nil {
		t.Fatalf("loadConfigFromFile failed: %v", err)
	}

	// Fetch the loaded configuration
	loadedConfig, err := Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	// Check if the environment variable override worked
	if loadedConfig.ProjectName != "Env Project" {
		t.Errorf("Expected ProjectName to be 'Env Project', got '%s'", loadedConfig.ProjectName)
	}

	// Check if the DNS was loaded correctly from the file
	if loadedConfig.DataSource.Dns != "temp-dns" {
		t.Errorf("Expected DataSource.Dns to be 'temp-dns', got '%s'", loadedConfig.DataSource.Dns)
	}
	if loadedConfig.Transaction.LockWaitTimeout != 7*time.Second {
		t.Errorf("Expected LockWaitTimeout to be '7s', got '%s'", loadedConfig.Transaction.LockWaitTimeout)
	}
}

func TestLoadConfigFromFileMonitoringDSN(t *testing.T) {
	// Cleared for the same reason as in TestLoadConfigFromFile: this fixture states no
	// deprecation window, so an inherited KAFKA_BROKERS would make the load refuse and the
	// monitoring DSN assertion never run.
	clearEventStreamingEnv(t)
	// The BLNK_ sweep is here for the same reason as in TestLoadConfigFromFile, and it is
	// load-bearing for THIS test specifically: BLNK_MONITORING_DSN and
	// BLNK_ENABLE_OBSERVABILITY are exactly the two variables a deployment exports, and
	// either one would make the assertions below read the environment's value while
	// claiming to read the file's.
	clearBlnkPrefixedEnv(t)

	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	// t.Cleanup rather than a bare defer, and the failure fails the test: removing a file
	// this test just created cannot fail for a benign reason, and a run that leaks a temp
	// file into /tmp on every execution should not be able to report itself as clean.
	t.Cleanup(func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Errorf("unable to remove the temporary config file %q: %v", tmpFile.Name(), err)
		}
	})

	sampleConfig := Configuration{
		ProjectName:         "Monitoring DSN Test",
		MonitoringDSN:       "https://blnk_obs_test@observe.blnk.cloud/monproj_test",
		EnableObservability: true,
		DataSource: DataSourceConfig{
			Dns: "temp-dns",
		},
		Redis: RedisConfig{
			Dns: "temp-redis",
		},
	}
	if err := json.NewEncoder(tmpFile).Encode(sampleConfig); err != nil {
		t.Fatalf("Unable to write to temporary file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Unable to close temporary file: %v", err)
	}

	if err := loadConfigFromFile(tmpFile.Name()); err != nil {
		t.Fatalf("loadConfigFromFile failed: %v", err)
	}

	loadedConfig, err := Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if loadedConfig.MonitoringDSN != "https://blnk_obs_test@observe.blnk.cloud/monproj_test" {
		t.Fatalf("Expected MonitoringDSN from config file, got %q", loadedConfig.MonitoringDSN)
	}
	if loadedConfig.RemoteMonitoringDSN() != "https://blnk_obs_test@observe.blnk.cloud/monproj_test" {
		t.Fatalf("Expected remote monitoring DSN from config file, got %q", loadedConfig.RemoteMonitoringDSN())
	}
}

func TestInitConfig(t *testing.T) {
	// Cleared for the same reason as in TestLoadConfigFromFile: InitConfig goes through the
	// same load pipeline, so an inherited KAFKA_BROKERS with no window in the fixture would
	// make it refuse.
	clearEventStreamingEnv(t)
	// InitConfig goes through envconfig too, so the BLNK_ sweep applies unchanged: both
	// assertions below name a value written into the fixture, and an ambient
	// BLNK_PROJECT_NAME or BLNK_DATA_SOURCE_DNS would answer them from the environment.
	clearBlnkPrefixedEnv(t)

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	// t.Cleanup rather than a bare defer, and the failure fails the test: removing a file
	// this test just created cannot fail for a benign reason, and a run that leaks a temp
	// file into /tmp on every execution should not be able to report itself as clean.
	t.Cleanup(func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Errorf("unable to remove the temporary config file %q: %v", tmpFile.Name(), err)
		}
	})

	// Sample configuration to write to the temp file
	sampleConfig := Configuration{
		ProjectName: "InitConfig Test",
		DataSource: DataSourceConfig{
			Dns: "init-config-dns",
		}, Redis: RedisConfig{
			Dns: "localhost:6379",
		},
	}
	if err := json.NewEncoder(tmpFile).Encode(sampleConfig); err != nil {
		t.Fatalf("Unable to write to temporary file: %v", err)
	}
	// Closed so InitConfig can open it; the error is checked because a failed close can
	// mean the encoded JSON was never flushed.
	require.NoError(t, tmpFile.Close())

	// Attempt to initialize the configuration using the temporary file
	if err := InitConfig(tmpFile.Name()); err != nil {
		t.Fatalf("InitConfig failed: %v", err)
	}

	// Fetch the loaded configuration to verify it was loaded correctly
	loadedConfig, err := Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	// Verify the configuration was loaded correctly
	if loadedConfig.ProjectName != "InitConfig Test" {
		t.Errorf("Expected ProjectName to be 'InitConfig Test', got '%s'", loadedConfig.ProjectName)
	}
	if loadedConfig.DataSource.Dns != "init-config-dns" {
		t.Errorf("Expected DataSource.Dns to be 'init-config-dns', got '%s'", loadedConfig.DataSource.Dns)
	}
}

// TestUploadURLTimeoutDefault verifies that an unset/zero upload URL timeout is
// defaulted to DEFAULT_UPLOAD_URL_TIMEOUT_SEC (30s).
func TestUploadURLTimeoutDefault(t *testing.T) {
	cnf := Configuration{
		ProjectName: "Test Project",
		DataSource:  DataSourceConfig{Dns: "some-dns"},
		Redis:       RedisConfig{Dns: "localhost:6379"},
	}
	cnf.Server.UploadURLTimeoutSec = 0
	if err := cnf.validateAndAddDefaults(); err != nil {
		t.Fatalf("validateAndAddDefaults failed: %v", err)
	}
	if cnf.Server.UploadURLTimeoutSec != DEFAULT_UPLOAD_URL_TIMEOUT_SEC {
		t.Errorf("expected default timeout %d, got %d", DEFAULT_UPLOAD_URL_TIMEOUT_SEC, cnf.Server.UploadURLTimeoutSec)
	}

	// An explicitly-configured value must be preserved.
	cnf2 := Configuration{
		ProjectName: "Test Project",
		DataSource:  DataSourceConfig{Dns: "some-dns"},
		Redis:       RedisConfig{Dns: "localhost:6379"},
	}
	cnf2.Server.UploadURLTimeoutSec = 7
	if err := cnf2.validateAndAddDefaults(); err != nil {
		t.Fatalf("validateAndAddDefaults failed: %v", err)
	}
	if cnf2.Server.UploadURLTimeoutSec != 7 {
		t.Errorf("expected explicit timeout 7 to be preserved, got %d", cnf2.Server.UploadURLTimeoutSec)
	}
}

// TestUploadWhitelistHostsParsing verifies parsing of the comma-separated
// whitelist: trimming, scheme-tolerance, case-folding, de-duplication, and
// deny-by-default (empty -> nil).
func TestUploadWhitelistHostsParsing(t *testing.T) {
	cases := []struct {
		name   string
		raw    string
		expect []string
	}{
		{
			name:   "mixed entries with schemes, empties, and dup",
			raw:    "example.com, https://x.com/path, ,Foo.COM, example.com",
			expect: []string{"example.com", "x.com", "foo.com"},
		},
		{
			name:   "bare hostnames only",
			raw:    "a.example.com, b.example.com",
			expect: []string{"a.example.com", "b.example.com"},
		},
		{
			name:   "empty whitelist is deny-by-default",
			raw:    "",
			expect: nil,
		},
		{
			name:   "only whitespace and commas",
			raw:    " , , ",
			expect: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cnf := Configuration{Server: ServerConfig{UploadDomainWhitelist: tc.raw}}
			got := cnf.UploadDomainWhitelistHosts()
			if len(got) != len(tc.expect) {
				t.Fatalf("expected %v, got %v", tc.expect, got)
			}
			for i := range got {
				if got[i] != tc.expect[i] {
					t.Errorf("entry %d: expected %q, got %q", i, tc.expect[i], got[i])
				}
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Event-streaming configuration coverage (Kafka, outbox relay, webhook sunset).
//
// Everything below this line is additive; the tests above are untouched. The
// helpers exist because these tests manipulate two pieces of process-global
// state the older tests do not guard: the environment, which envconfig reads,
// and the ConfigStore atomic.Value. Leaking either one makes a later assertion
// pass for the wrong reason, so both are saved and restored.
// -----------------------------------------------------------------------------

// eventStreamingEnvKeys lists every environment variable that can influence the
// Kafka, relay or webhook-deprecation configuration, in all three forms a reader
// might reach for.
//
// # It is DERIVED, because a hand-written list is what went wrong
//
// This used to be a literal slice maintained beside the configuration structs, and
// it fell behind them: KAFKA_HISTORICAL_TOPIC_PREFIXES and
// RELAY_SUBSCRIBER_METRICS_BUDGET were added to KafkaConfig and RelayConfig and
// never added here, so an ambient value for either survived clearEventStreamingEnv
// and could satisfy a default or precedence assertion for the wrong reason. Adding
// the two missing names would have fixed the symptom and left the mechanism —
// two lists that must agree, with nothing making them agree — intact. Reflecting
// over the structs makes them one list.
//
// # The three forms, and why each exists
//
// envconfig v1.4.0 derives a field's primary key by accumulating the prefix through
// every enclosing struct and appending the tag literal, then falls back to the bare
// tag literal as an alternate consulted only when the primary is unset. KafkaConfig
// is Configuration.Kafka, so its primary key is BLNK_KAFKA_<TAG> and its alternate
// is the bare, deployment-mandated <TAG> that AAP R-10 names.
//
// The intuitive-looking BLNK_<TAG> is neither of those keys and used to be read by
// nothing at all — a deployment that set BLNK_KAFKA_BROKERS silently got default
// behaviour. applyPrefixedEnvAliases now resolves it explicitly and with HIGHER
// precedence than the bare name, so all three forms are live; each is asserted by
// TestLoadConfigFromFile_KafkaEnvNameForms, and every form has to be cleared here
// because any one of them leaking in would be read.
var eventStreamingEnvKeys = deriveEventStreamingEnvKeys()

// eventStreamingEnvExtras are names cleared defensively that no struct tag produces.
//
// WebhookDeprecationStartDate carries a json tag and no envconfig tag, so envconfig
// derives BLNK_WEBHOOKDEPRECATIONSTARTDATE for it and the readable form below is
// read by nothing today. It is cleared anyway: the field is documented as the other
// end of the dual-delivery window, and an operator who exports the readable name
// after it acquires a tag should not be able to change what these tests observe.
var eventStreamingEnvExtras = []string{
	"WEBHOOK_DEPRECATION_START_DATE", "BLNK_WEBHOOK_DEPRECATION_START_DATE",
}

// deriveEventStreamingEnvKeys reads the envconfig tags off the configuration structs
// themselves and expands each into the three forms described above.
//
// The subtrees are named rather than discovered because "event streaming" is a
// judgement about which configuration this file's tests are allowed to disturb, not
// a property of the type: clearing Configuration.Redis or Configuration.DataSource
// here would break the tests that legitimately depend on them. Naming two structs
// and a field is stable in a way that naming twenty-five variables is not — a new
// field inside either struct is picked up with no edit to this file, which is the
// failure this replaces.
func deriveEventStreamingEnvKeys() []string {
	keys := make([]string, 0, 96)
	seen := make(map[string]struct{}, 96)

	add := func(forms ...string) {
		for _, form := range forms {
			if _, already := seen[form]; already {
				continue
			}
			seen[form] = struct{}{}
			keys = append(keys, form)
		}
	}

	// The prefix envconfig accumulates for each subtree, which is blnkEnvPrefix plus
	// the upper-cased name of the field on Configuration that holds it.
	subtrees := []struct {
		prefix string
		typ    reflect.Type
	}{
		{blnkEnvPrefix + "KAFKA_", reflect.TypeOf(KafkaConfig{})},
		{blnkEnvPrefix + "KAFKA_", reflect.TypeOf(KafkaTLSConfig{})},
		{blnkEnvPrefix + "RELAY_", reflect.TypeOf(RelayConfig{})},
	}

	for _, subtree := range subtrees {
		for i := 0; i < subtree.typ.NumField(); i++ {
			tag := subtree.typ.Field(i).Tag.Get("envconfig")
			if tag == "" {
				continue
			}
			add(tag, subtree.prefix+tag, blnkEnvPrefix+tag)
		}
	}

	// Webhook deprecation lives directly on Configuration, so its primary key takes
	// the bare prefix with no intervening subtree name.
	configuration := reflect.TypeOf(Configuration{})
	for i := 0; i < configuration.NumField(); i++ {
		field := configuration.Field(i)
		tag := field.Tag.Get("envconfig")
		if tag == "" || !strings.HasPrefix(tag, "WEBHOOK_DEPRECATION_") {
			continue
		}
		add(tag, blnkEnvPrefix+tag)
	}

	add(eventStreamingEnvExtras...)

	return keys
}

// TestEventStreamingEnvKeys_CoverEveryConfiguredVariable pins the derivation above
// against the variables AAP R-10 mandates by name.
//
// The derivation cannot omit a struct field, but it CAN be wrong about the shape of
// the names it builds — a mistaken subtree prefix would produce a full-looking list
// of keys that nothing reads, and every test that depends on clearing would go back
// to passing for the wrong reason with nothing to show for it. Asserting the exact
// eight names the requirement fixes, in every form, is the independent check: those
// are stated in the AAP rather than derived from the code, so the two cannot drift
// together.
func TestEventStreamingEnvKeys_CoverEveryConfiguredVariable(t *testing.T) {
	present := make(map[string]struct{}, len(eventStreamingEnvKeys))
	for _, key := range eventStreamingEnvKeys {
		present[key] = struct{}{}
	}

	// The eight names AAP R-10 mandates, plus the two whose absence was the finding.
	for _, expected := range []string{
		"KAFKA_BROKERS", "BLNK_KAFKA_KAFKA_BROKERS", "BLNK_KAFKA_BROKERS",
		"KAFKA_TOPIC_PREFIX", "BLNK_KAFKA_KAFKA_TOPIC_PREFIX", "BLNK_KAFKA_TOPIC_PREFIX",
		"KAFKA_SASL_ADMIN_USER", "BLNK_KAFKA_KAFKA_SASL_ADMIN_USER", "BLNK_KAFKA_SASL_ADMIN_USER",
		"KAFKA_SASL_ADMIN_SECRET", "BLNK_KAFKA_KAFKA_SASL_ADMIN_SECRET", "BLNK_KAFKA_SASL_ADMIN_SECRET",
		"RELAY_MAX_RETRY_ATTEMPTS", "BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS", "BLNK_RELAY_MAX_RETRY_ATTEMPTS",
		"RELAY_RETRY_BASE_BACKOFF_MS", "BLNK_RELAY_RELAY_RETRY_BASE_BACKOFF_MS",
		"BLNK_RELAY_RETRY_BASE_BACKOFF_MS",
		"RELAY_RETRY_MAX_BACKOFF_MS", "BLNK_RELAY_RELAY_RETRY_MAX_BACKOFF_MS",
		"BLNK_RELAY_RETRY_MAX_BACKOFF_MS",
		"WEBHOOK_DEPRECATION_SUNSET_DATE", "BLNK_WEBHOOK_DEPRECATION_SUNSET_DATE",
		// The two the hand-written list had fallen behind on.
		"KAFKA_HISTORICAL_TOPIC_PREFIXES", "BLNK_KAFKA_KAFKA_HISTORICAL_TOPIC_PREFIXES",
		"BLNK_KAFKA_HISTORICAL_TOPIC_PREFIXES",
		"RELAY_SUBSCRIBER_METRICS_BUDGET", "BLNK_RELAY_RELAY_SUBSCRIBER_METRICS_BUDGET",
		"BLNK_RELAY_SUBSCRIBER_METRICS_BUDGET",
	} {
		_, ok := present[expected]
		assert.Truef(t, ok,
			"%s is not cleared by clearEventStreamingEnv, so an ambient value for it survives "+
				"into every test in this file that asserts a DEFAULT or a PRECEDENCE. Either the "+
				"subtree prefixes in deriveEventStreamingEnvKeys are wrong or the field lost its "+
				"envconfig tag", expected)
	}

	// Every derived name must be complete in its forms: a bare tag whose prefixed
	// siblings are missing clears the alternate key and leaves the PRIMARY set, which
	// is the form envconfig prefers and therefore the one that would win.
	for _, key := range eventStreamingEnvKeys {
		if strings.HasPrefix(key, blnkEnvPrefix) {
			continue
		}

		_, aliased := present[blnkEnvPrefix+key]
		assert.Truef(t, aliased,
			"%s is cleared but %s%s is not, and applyPrefixedEnvAliases gives the prefixed form "+
				"HIGHER precedence, so the one left behind is the one that wins", key, blnkEnvPrefix, key)
	}
}

// clearEventStreamingEnv unsets every key in eventStreamingEnvKeys and restores
// whatever was set before when the test finishes.
//
// Clearing every form is mandatory rather than defensive. Because envconfig
// consults a prefixed primary key and then a bare alternate for the same field, a
// variable left behind by an earlier subtest — or exported by the surrounding
// environment — would satisfy a later assertion for the wrong reason. Restoring
// the prior values keeps the rest of the package's tests hermetic.
func clearEventStreamingEnv(t *testing.T) {
	t.Helper()

	saved := make(map[string]string, len(eventStreamingEnvKeys))
	for _, key := range eventStreamingEnvKeys {
		if value, ok := os.LookupEnv(key); ok {
			saved[key] = value
		}
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("Unable to unset %s: %v", key, err)
		}
	}

	t.Cleanup(func() {
		for _, key := range eventStreamingEnvKeys {
			value, ok := saved[key]
			if !ok {
				if err := os.Unsetenv(key); err != nil {
					t.Errorf("Unable to unset %s during cleanup: %v", key, err)
				}
				continue
			}
			if err := os.Setenv(key, value); err != nil {
				t.Errorf("Unable to restore %s during cleanup: %v", key, err)
			}
		}
	})
}

// blnkEnvPrefix is the prefix envconfig.Process("blnk", …) prepends to every
// primary key it derives, and therefore the prefix of every variable that can
// overlay a value read from a configuration file.
const blnkEnvPrefix = "BLNK_"

// clearBlnkPrefixedEnv unsets every BLNK_-prefixed variable present in the
// environment and restores exactly those when the test finishes.
//
// # Why the tests that load a FILE need this
//
// loadConfigFromFile decodes the file and then hands the struct to
// envconfig.Process("blnk", …), so any BLNK_-prefixed variable exported by the
// surrounding shell wins over the file. A test that writes a fixture and then
// asserts a value came out of that fixture is therefore asserting a property of
// the machine it runs on, not of the loader: exporting BLNK_DATA_SOURCE_DNS —
// which a developer pointing the suite at a relocated Postgres does as a matter
// of course, and which the project's own test guidance discusses — made
// TestLoadConfigFromFile and TestInitConfig fail on the DataSource.Dns
// assertion while the loader was behaving exactly as designed.
//
// # Why the sweep is by prefix rather than by an enumerated key list
//
// eventStreamingEnvKeys can be enumerated because those keys are a closed,
// deliberately-designed set with three spellings each. The Configuration tree is
// not: every exported field of every nested struct yields a key, and a field
// added later would silently re-open the same hole in a list that looked
// complete. Sweeping the prefix covers the whole tree by construction, including
// the BLNK_KAFKA_* forms, and cannot rot.
//
// # Why unset rather than set-to-empty
//
// envconfig applies an EMPTY value as a real override — it checks whether the
// variable is present, not whether it is non-blank — so t.Setenv(key, "") would
// blank the field instead of leaving the file's value alone, and for the two
// required fields that turns a hermeticity fix into a validation failure. The
// variables must genuinely be absent, which only os.Unsetenv achieves.
//
// # Composition with clearEventStreamingEnv
//
// Both helpers save what they find and restore only that, so calling them
// together is safe in either order: whichever runs first takes ownership of the
// keys they share, the second finds them already absent, and t.Cleanup's LIFO
// ordering hands the original values back. A variable the TEST sets for itself
// afterwards — BLNK_PROJECT_NAME in TestLoadConfigFromFile — is unaffected,
// because this sweep has already run by then.
//
// Parameters:
//   - t *testing.T: the test. A variable that can be neither unset nor restored
//     fails the test rather than being ignored, because either one silently
//     re-opens the hole this closes.
func clearBlnkPrefixedEnv(t *testing.T) {
	t.Helper()

	saved := make(map[string]string)
	for _, entry := range os.Environ() {
		// os.Environ returns KEY=VALUE; a value may itself contain '=', so the
		// split is on the FIRST separator only.
		separator := strings.Index(entry, "=")
		if separator <= 0 {
			continue
		}
		key := entry[:separator]
		if !strings.HasPrefix(key, blnkEnvPrefix) {
			continue
		}
		saved[key] = entry[separator+1:]
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("Unable to unset %s: %v", key, err)
		}
	}

	t.Cleanup(func() {
		for key, value := range saved {
			if err := os.Setenv(key, value); err != nil {
				t.Errorf("Unable to restore %s during cleanup: %v", key, err)
			}
		}
	})
}

// restoreConfigStore snapshots the package-level ConfigStore and puts it back
// when the test finishes, so a test that loads a configuration does not leave the
// store pointing at its fixture.
//
// The nil guard is required, not stylistic: atomic.Value panics on Store(nil), so
// a store that was empty to begin with must be left empty rather than "restored".
func restoreConfigStore(t *testing.T) {
	t.Helper()

	previous := ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			ConfigStore.Store(previous)
		}
	})
}

// TestMain seeds ConfigStore before any test runs, and that is what makes
// restoreConfigStore able to do its job.
//
// # The gap this closes
//
// ConfigStore is an atomic.Value, and atomic.Value CANNOT BE RESET TO NIL — Store(nil) panics.
// So restoreConfigStore, which snapshots the previous value and puts it back, is powerless in
// exactly one case: when the store was EMPTY when the test began. It has nothing to put back,
// and the test's own fixture is left in place for the rest of the process. Whichever mutating
// test happened to run first therefore decided the package's steady state, and under
// `-shuffle=on` that is a different test on every run.
//
// Seeding here removes the case rather than working around it. Every restoreConfigStore call
// now has a real previous value, so the store returns to THIS known baseline between tests
// instead of to whichever fixture got there first.
//
// The baseline is deliberately recognisable. Nothing should read it — every test that depends
// on configuration installs its own — so if a value from here ever shows up in a failure
// message, the test that produced it was reading the store when it meant to populate it.
func TestMain(m *testing.M) {
	baseline := Configuration{
		ProjectName: "config-package-test-baseline",
		DataSource:  DataSourceConfig{Dns: "postgres://baseline.invalid/never-connected"},
		Redis:       RedisConfig{Dns: "baseline.invalid:6379"},
	}
	ConfigStore.Store(&baseline)

	os.Exit(m.Run())
}

// TestProcessGlobalRestoration_ReturnsEveryMutatedGlobalToItsPriorValue is the guard for the
// three process globals this package's tests move: ConfigStore, the logrus level and
// BLNK_LOG_LEVEL.
//
// It asserts the MECHANISM rather than any particular test's tidiness, which is what makes it
// order-independent: it mutates each global inside a nested subtest through the same helper the
// real tests use, and checks the value is back once that subtest has finished. A helper that
// stopped restoring would fail here immediately instead of surfacing as an unrelated test
// failing under a shuffle seed nobody can reproduce.
func TestProcessGlobalRestoration_ReturnsEveryMutatedGlobalToItsPriorValue(t *testing.T) {
	t.Run("the configuration store", func(t *testing.T) {
		before := ConfigStore.Load()
		require.NotNil(t, before,
			"TestMain seeds the store precisely so restoreConfigStore always has something to "+
				"put back: atomic.Value cannot be reset to nil, so a store that starts empty "+
				"keeps whichever fixture reached it first")

		t.Run("mutating subtest", func(t *testing.T) {
			restoreConfigStore(t)

			fixture := eventStreamingBaseConfig()
			fixture.ProjectName = "restoration-probe"
			MockConfig(&fixture)

			current, ok := ConfigStore.Load().(*Configuration)
			require.True(t, ok)
			require.Equal(t, "restoration-probe", current.ProjectName,
				"the mutation must actually have happened, or this proves nothing")
		})

		assert.Same(t, before, ConfigStore.Load(),
			"the store must hold the SAME configuration pointer it held before the subtest ran")
	})

	t.Run("the logger level", func(t *testing.T) {
		before := logrus.GetLevel()

		t.Run("mutating subtest", func(t *testing.T) {
			pinLevel(t, logrus.PanicLevel)
			require.Equal(t, logrus.PanicLevel, logrus.GetLevel())
		})

		assert.Equal(t, before, logrus.GetLevel(),
			"the level is process global and the root package pins it to capture debug-only "+
				"lines, so a leak from here breaks tests in another package")
	})

	t.Run("the ambient BLNK_LOG_LEVEL", func(t *testing.T) {
		t.Setenv("BLNK_LOG_LEVEL", "warn")

		t.Run("mutating subtest", func(t *testing.T) {
			clearLogLevelEnv(t)

			_, present := os.LookupEnv("BLNK_LOG_LEVEL")
			require.False(t, present, "the helper must actually clear it")
		})

		value, present := os.LookupEnv("BLNK_LOG_LEVEL")
		assert.True(t, present,
			"a bare os.Unsetenv strips the variable for the REST OF THE PROCESS; the helper "+
				"saves and restores it instead")
		assert.Equal(t, "warn", value)
	})
}

// pinLevel sets the global logrus level for one test and puts the previous one back.
//
// It is package level rather than a closure inside a single test because the level is PROCESS
// GLOBAL and three separate tests in this file move it. A subtest that sets the level and
// relies on its parent's single cleanup leaves the level changed for every sibling that runs
// after it — which is invisible while the file runs in source order and becomes a failure the
// moment `-shuffle=on`, or a future `t.Parallel()`, reorders them. Worse, the root package
// pins the level to capture debug-only lines, so a leak from here breaks tests in another
// package entirely.
//
// Every mutating subtest calls this, so the restore is local to the mutation.
func pinLevel(t *testing.T, level logrus.Level) {
	t.Helper()

	previous := logrus.GetLevel()
	t.Cleanup(func() { logrus.SetLevel(previous) })
	logrus.SetLevel(level)
}

// clearLogLevelEnv unsets BLNK_LOG_LEVEL for one test and restores whatever was there.
//
// The variable is cleared explicitly rather than through clearEventStreamingEnv, which covers
// the Kafka block only: a value leaking in from the surrounding environment would satisfy a
// file-only assertion for the wrong reason. The save-and-restore matters as much as the unset —
// a bare os.Unsetenv strips the variable for the REST OF THE PROCESS, so a later test that
// expects the ambient value silently sees nothing.
func clearLogLevelEnv(t *testing.T) {
	t.Helper()

	saved, existed := os.LookupEnv("BLNK_LOG_LEVEL")
	if err := os.Unsetenv("BLNK_LOG_LEVEL"); err != nil {
		t.Fatalf("Unable to unset BLNK_LOG_LEVEL: %v", err)
	}

	t.Cleanup(func() {
		if !existed {
			return
		}
		if err := os.Setenv("BLNK_LOG_LEVEL", saved); err != nil {
			t.Errorf("Unable to restore BLNK_LOG_LEVEL: %v", err)
		}
	})
}

// eventStreamingBaseConfig returns the smallest Configuration that passes
// validateRequiredFields. Only the data source and Redis DNS values are required,
// so everything the event-streaming tests assert on is left zero-valued and is
// therefore genuinely supplied by the defaults or by the environment.
func eventStreamingBaseConfig() Configuration {
	return Configuration{
		ProjectName: "Test Project",
		DataSource:  DataSourceConfig{Dns: "some-dns"},
		Redis:       RedisConfig{Dns: "localhost:6379"},
	}
}

// testWindowStart and testWindowSunset are exactly WebhookDualDeliveryWindowDays
// apart, which is the only pairing resolveWebhookDeprecationWindow accepts when both
// ends are given. They are written out rather than computed so that a change to the
// window constant fails the arithmetic assertion in
// TestResolveWebhookDeprecationWindow rather than silently agreeing with itself.
const (
	testWindowStart  = "2026-08-05T00:00:00Z"
	testWindowSunset = "2026-09-04T00:00:00Z"
)

// kafkaEnabledConfig returns a Configuration with Kafka publishing switched on and a
// valid dual-delivery window, which is the combination every Kafka-configured test
// needs.
//
// It exists because configuring brokers WITHOUT a window is now a hard error: dual
// delivery would otherwise run forever with nothing to say so. A test that only wants
// to assert something about brokers should not have to rediscover that, and a test
// that wants to assert the error itself sets the brokers by hand.
func kafkaEnabledConfig(brokers ...string) Configuration {
	cnf := eventStreamingBaseConfig()
	cnf.Kafka.Brokers = brokers
	cnf.WebhookDeprecationSunsetDate = testWindowSunset

	return cnf
}

// writeTempEventConfigFile writes the minimal valid configuration — with NO
// dual-delivery window — to a temporary blnk.json and returns its path, removing the
// file when the test finishes.
//
// Tests use it so that they run through the real load pipeline — file decode,
// envconfig.Process("blnk", ...), applyPrefixedEnvAliases, then
// validateAndAddDefaults — because asserting against a hand-built struct would not
// exercise envconfig at all. Tests that configure brokers want
// writeTempEventConfigFileWithWindow instead; this one is for the cases that assert
// what happens WITHOUT a window.
func writeTempEventConfigFile(t *testing.T) string {
	t.Helper()

	return writeTempConfigFile(t, eventStreamingBaseConfig())
}

// writeTempEventConfigFileWithWindow is writeTempEventConfigFile plus a valid
// dual-delivery window.
//
// Tests that configure Kafka brokers need it, because brokers with no window is now a
// hard configuration error rather than a warning: dual delivery would otherwise have
// no end. Keeping it a separate helper means the tests that assert THAT error can
// still start from a fixture with no window at all.
func writeTempEventConfigFileWithWindow(t *testing.T) string {
	t.Helper()

	cnf := eventStreamingBaseConfig()
	cnf.WebhookDeprecationSunsetDate = testWindowSunset

	return writeTempConfigFile(t, cnf)
}

// writeTempConfigFile encodes any Configuration to a temporary blnk.json and returns
// its path, removing the file when the test finishes.
func writeTempConfigFile(t *testing.T, cnf Configuration) string {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Errorf("Unable to remove temporary file: %v", err)
		}
	})

	if err := json.NewEncoder(tmpFile).Encode(cnf); err != nil {
		t.Fatalf("Unable to write to temporary file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Unable to close temporary file: %v", err)
	}

	return tmpFile.Name()
}

// TestLoadConfigFromFile_PrefixedAliasWinsOverBareName pins the PRECEDENCE between the
// two names a nested Kafka or relay variable answers to.
//
// The rule is that the BLNK_-prefixed alias wins. That is not arbitrary: it is the
// precedence envconfig itself gives the top-level WebhookDeprecationSunsetDate, where
// the accumulated BLNK_ primary key beats the bare alternate. Making the nested fields
// behave the same way is what stops the answer to "which name wins?" from depending on
// whether a given setting happens to live inside a struct.
func TestLoadConfigFromFile_PrefixedAliasWinsOverBareName(t *testing.T) {
	cases := []struct {
		name       string
		bareKey    string
		bareValue  string
		aliasKey   string
		aliasValue string
		assert     func(t *testing.T, loaded *Configuration)
	}{
		{
			name:       "brokers",
			bareKey:    "KAFKA_BROKERS",
			bareValue:  "bare:9092",
			aliasKey:   "BLNK_KAFKA_BROKERS",
			aliasValue: "alias:9092",
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.Brokers, []string{"alias:9092"})
			},
		},
		{
			name:       "topic prefix",
			bareKey:    "KAFKA_TOPIC_PREFIX",
			bareValue:  "bare",
			aliasKey:   "BLNK_KAFKA_TOPIC_PREFIX",
			aliasValue: "alias",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.TopicPrefix != "alias" {
					t.Errorf("Expected the prefixed alias to win with 'alias', got %q", loaded.Kafka.TopicPrefix)
				}
			},
		},
		{
			// Both values sit INSIDE MaxRelayRetryAttempts on purpose. The budget is clamped
			// to that ceiling because the attempt number is a bounded metric attribute, so a
			// value above it resolves to the ceiling from either name form and the subtest
			// would pass without the alias ever having been read — a discriminating pair is
			// the whole point of this table.
			name:       "relay max retry attempts",
			bareKey:    "RELAY_MAX_RETRY_ATTEMPTS",
			bareValue:  "2",
			aliasKey:   "BLNK_RELAY_MAX_RETRY_ATTEMPTS",
			aliasValue: "4",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.MaxRetryAttempts != 4 {
					t.Errorf("Expected the prefixed alias to win with 4, got %d", loaded.Relay.MaxRetryAttempts)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearEventStreamingEnv(t)
			restoreConfigStore(t)

			configFile := writeTempEventConfigFileWithWindow(t)
			t.Setenv(tc.bareKey, tc.bareValue)
			t.Setenv(tc.aliasKey, tc.aliasValue)

			if err := loadConfigFromFile(configFile); err != nil {
				t.Fatalf("loadConfigFromFile failed: %v", err)
			}
			loaded, err := Fetch()
			if err != nil {
				t.Fatalf("Fetch failed: %v", err)
			}

			tc.assert(t, loaded)
		})
	}
}

// TestLoadConfigFromFile_RefusesKafkaBrokersWithNoWindow carries the missing-window rule
// through the REAL load pipeline, not just validateAndAddDefaults.
//
// # Why this must REFUSE rather than load with a warning
//
// It used to load, and the warning it emitted said that "legacy HTTP webhook delivery
// will run alongside Kafka INDEFINITELY". The runtime does the OPPOSITE:
// blnk.WebhookSunsetPassed fails closed on a publishing deployment with no usable
// window, answering that the sunset has ALREADY passed — dual delivery stops and the
// deprecated webhook management routes answer 410 Gone. Configuration and behaviour
// therefore disagreed about the single decision requirement R-12 is made of, and an
// operator reading the log was told the safer of the two answers while getting the other.
//
// The runtime's reading is the one worth keeping: carrying a deprecated, less protected
// transport indefinitely on the strength of an unset variable is worse than retiring it
// loudly. So the combination is refused at load, before any traffic is served, which is
// also what event_sunset.go's own documentation already claimed happened.
//
// The refusal must NAME the variable, because "invalid configuration" sends an operator
// looking through everything.
func TestLoadConfigFromFile_RefusesKafkaBrokersWithNoWindow(t *testing.T) {
	clearEventStreamingEnv(t)
	restoreConfigStore(t)

	configFile := writeTempEventConfigFile(t)
	t.Setenv("KAFKA_BROKERS", "broker-1:9092")

	err := loadConfigFromFile(configFile)
	if err == nil {
		t.Fatal("Expected brokers with no dual-delivery window to be REFUSED at load")
	}
	if !strings.Contains(err.Error(), "webhook_deprecation_sunset_date") {
		t.Errorf("Expected the error to name the missing setting, got %q", err.Error())
	}
}

// TestLoadConfigFromFile_LoadsWithNoBrokersAndNoWindow is the other side of that rule, and
// it is AAP §0.7.2's graceful degradation stated as a test.
//
// With KAFKA_BROKERS unset there is no transport to migrate to, so there is no window to
// describe and nothing has been mis-stated. The service must start and serve exactly as it
// did before this feature existed, with the publisher resolving to the no-op — which is what
// protects every deployment that has not adopted Kafka.
func TestLoadConfigFromFile_LoadsWithNoBrokersAndNoWindow(t *testing.T) {
	clearEventStreamingEnv(t)
	restoreConfigStore(t)

	configFile := writeTempEventConfigFile(t)

	if err := loadConfigFromFile(configFile); err != nil {
		t.Fatalf("Expected no brokers and no window to LOAD; got %v", err)
	}

	loaded, err := Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	if len(loaded.Kafka.Brokers) != 0 {
		t.Errorf("Expected no brokers, got %v", loaded.Kafka.Brokers)
	}
	if loaded.WebhookDeprecationSunsetDate != "" {
		t.Errorf("Expected no sunset date, got %q", loaded.WebhookDeprecationSunsetDate)
	}
	if loaded.WebhookDeprecationStartDate != "" {
		t.Errorf("Expected no derived window start either, got %q", loaded.WebhookDeprecationStartDate)
	}
}

// TestLoadConfigFromFile_RefusesAnUnparseableSunsetDate is the case that stays fatal.
//
// An operator who states a retirement instant and mis-types it must not be given the
// silent resolution "keep the legacy behaviour": that keeps the deprecated, less
// protected transport alive indefinitely with nothing failing. This is the distinction
// from an absent date, where nothing was stated at all.
func TestLoadConfigFromFile_RefusesAnUnparseableSunsetDate(t *testing.T) {
	clearEventStreamingEnv(t)
	restoreConfigStore(t)

	configFile := writeTempEventConfigFile(t)
	t.Setenv("KAFKA_BROKERS", "broker-1:9092")
	t.Setenv("WEBHOOK_DEPRECATION_SUNSET_DATE", "2026-13-45")

	err := loadConfigFromFile(configFile)
	if err == nil {
		t.Fatal("Expected loadConfigFromFile to refuse an unparseable sunset date")
	}
	if !strings.Contains(err.Error(), "webhook_deprecation_sunset_date") {
		t.Errorf("Expected the error to name the offending setting, got %q", err.Error())
	}
}

// TestWebhookDeprecationStartDate_IsNotAnEnvironmentVariable pins the R-10 contract
// surface.
//
// Requirement R-10 freezes the deployment contract at eight environment variables, of
// which exactly ONE describes this window. A second variable for the other end was a
// real convenience and still a contract violation, and it bought nothing derivable:
// the window is exactly WebhookDualDeliveryWindowDays long, so the sunset determines
// the start.
//
// The assertion is that setting the retired name has NO EFFECT, in either its bare or
// its prefixed form. A test that merely checked the struct tag would pass while the
// alias table still honoured the name.
func TestWebhookDeprecationStartDate_IsNotAnEnvironmentVariable(t *testing.T) {
	for _, name := range []string{
		"WEBHOOK_DEPRECATION_START_DATE",
		"BLNK_WEBHOOK_DEPRECATION_START_DATE",
	} {
		t.Run(name+" is ignored", func(t *testing.T) {
			clearEventStreamingEnv(t)
			restoreConfigStore(t)

			configFile := writeTempEventConfigFile(t)
			t.Setenv("KAFKA_BROKERS", "broker-1:9092")
			t.Setenv("WEBHOOK_DEPRECATION_SUNSET_DATE", testWindowSunset)

			// A start that is nowhere near one window before the sunset, so honouring it
			// would be unmistakable.
			t.Setenv(name, "2001-01-01T00:00:00Z")

			if err := loadConfigFromFile(configFile); err != nil {
				t.Fatalf("loadConfigFromFile failed: %v", err)
			}
			loaded, err := Fetch()
			if err != nil {
				t.Fatalf("Fetch failed: %v", err)
			}

			if loaded.WebhookDeprecationStartDate == "2001-01-01T00:00:00Z" {
				t.Fatalf("%s was honoured; it must not be part of the deployment contract", name)
			}

			// And the derived value is the only one that can appear.
			sunset, err := time.Parse(time.RFC3339, loaded.WebhookDeprecationSunsetDate)
			if err != nil {
				t.Fatalf("parsing the loaded sunset: %v", err)
			}

			want := sunset.Add(-WebhookDualDeliveryWindowDays * 24 * time.Hour).Format(time.RFC3339)
			if loaded.WebhookDeprecationStartDate != want {
				t.Errorf("Expected the window start to be derived as %q, got %q",
					want, loaded.WebhookDeprecationStartDate)
			}
		})
	}
}

// TestLoadConfigFromFile_AcceptsNoBrokersAndNoWindow is the graceful-degradation half
// of the same rule, and it is the state every deployment that has not adopted Kafka
// runs in. It must load without error and without a window.
func TestLoadConfigFromFile_AcceptsNoBrokersAndNoWindow(t *testing.T) {
	clearEventStreamingEnv(t)
	restoreConfigStore(t)

	configFile := writeTempEventConfigFile(t)

	if err := loadConfigFromFile(configFile); err != nil {
		t.Fatalf("Expected a Kafka-less configuration to load, got %v", err)
	}
	loaded, err := Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(loaded.Kafka.Brokers) != 0 {
		t.Errorf("Expected no brokers, got %v", loaded.Kafka.Brokers)
	}
	if loaded.WebhookDeprecationSunsetDate != "" {
		t.Errorf("Expected no sunset, got %q", loaded.WebhookDeprecationSunsetDate)
	}
}

// TestApplyPrefixedEnvAliases_RejectsMalformedValues proves a malformed alias is an
// ERROR rather than a silently discarded value.
//
// BLNK_RELAY_MAX_RETRY_ATTEMPTS=five must not quietly resolve to the default 5 and
// leave an operator believing the retry budget had been reduced. The same reasoning
// applies to every boolean: KAFKA_TLS_ENABLED=yeah must not read as false and
// silently take the connection back to plaintext.
func TestApplyPrefixedEnvAliases_RejectsMalformedValues(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value string
	}{
		{name: "a non-numeric retry attempt count", key: "BLNK_RELAY_MAX_RETRY_ATTEMPTS", value: "five"},
		{name: "a non-numeric backoff", key: "BLNK_RELAY_RETRY_BASE_BACKOFF_MS", value: "1s"},
		{name: "a non-numeric partition count", key: "BLNK_KAFKA_MIN_PARTITIONS", value: "six"},
		{name: "a non-boolean tls switch", key: "BLNK_KAFKA_TLS_ENABLED", value: "yeah"},
		{name: "a non-boolean local-dev switch", key: "BLNK_KAFKA_INSECURE_LOCAL_DEV", value: "sometimes"},
		{name: "a non-boolean admin-producer allowance", key: "BLNK_KAFKA_ALLOW_ADMIN_PRODUCER", value: "maybe"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearEventStreamingEnv(t)
			restoreConfigStore(t)

			configFile := writeTempEventConfigFileWithWindow(t)
			t.Setenv(tc.key, tc.value)

			err := loadConfigFromFile(configFile)
			if err == nil {
				t.Fatalf("Expected loadConfigFromFile to fail for %s=%q", tc.key, tc.value)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Errorf("Expected the error to name %s, got %q", tc.key, err.Error())
			}
		})
	}
}

// TestValidateSASLPair covers the one rule both Kafka clients now share: a SASL
// credential is either complete or absent.
//
// Half a credential used to be tolerated differently by the two of them — the
// publisher built a mechanism from whatever it had while the admin client skipped SASL
// entirely — so the same misconfiguration produced an authentication failure in one
// process and a silently unauthenticated connection in the other.
func TestValidateSASLPair(t *testing.T) {
	cases := []struct {
		name      string
		user      string
		secret    string
		wantError bool
	}{
		{name: "both set is valid", user: "producer", secret: "s3cret", wantError: false},
		{name: "neither set means no SASL", user: "", secret: "", wantError: false},
		{name: "whitespace-only counts as unset", user: "  ", secret: "\t", wantError: false},
		{name: "a user with no secret is refused", user: "producer", secret: "", wantError: true},
		{name: "a secret with no user is refused", user: "", secret: "s3cret", wantError: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSASLPair("producer", tc.user, tc.secret)
			if tc.wantError && err == nil {
				t.Fatalf("Expected an error for user=%q secret set=%t", tc.user, tc.secret != "")
			}
			if !tc.wantError && err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			// The secret must never appear in the message, in any branch.
			if err != nil && tc.secret != "" && strings.Contains(err.Error(), tc.secret) {
				t.Errorf("The error message leaked the secret: %q", err.Error())
			}
		})
	}
}

// TestProducerSASL covers the least-privilege selection the publisher depends on.
//
// The finding it guards: the steady-state publisher authenticated with the
// ADMINISTRATIVE principal, so compromising the busiest process in the deployment
// handed over authority to create topics, mint SCRAM credentials and rewrite ACLs.
// A dedicated producer principal is now REQUIRED — there is no fallback, because a
// fallback that warns leaves the excess privilege in place and only records it — so the
// administrative pair is never returned here and the caller is told to refuse instead.
func TestProducerSASL(t *testing.T) {
	t.Run("a dedicated producer principal is preferred and is not the admin", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.SASLUser = "blnk-producer"
		cnf.Kafka.SASLSecret = "producer-secret"
		cnf.Kafka.SASLAdminUser = "blnk-admin"
		cnf.Kafka.SASLAdminSecret = "admin-secret"

		user, secret, adminOnly := cnf.ProducerSASL()
		if user != "blnk-producer" || secret != "producer-secret" {
			t.Errorf("Expected the producer principal, got user %q", user)
		}
		if adminOnly {
			t.Error("Expected adminOnly to be false when a producer principal is configured")
		}
	})

	t.Run("an admin-only configuration yields no credential and is reported as fatal", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.SASLAdminUser = "blnk-admin"
		cnf.Kafka.SASLAdminSecret = "admin-secret"

		user, secret, adminOnly := cnf.ProducerSASL()
		if user != "" || secret != "" {
			t.Errorf("The administrative principal must never be offered to a producer, got user %q", user)
		}
		if !adminOnly {
			t.Error("Expected adminOnly to be true so the caller refuses to build a producer transport")
		}
	})

	t.Run("no credential at all yields no SASL", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()

		user, secret, adminOnly := cnf.ProducerSASL()
		if user != "" || secret != "" || adminOnly {
			t.Errorf("Expected no credentials, got user %q adminOnly %t", user, adminOnly)
		}
	})
}

// warnedAbout reports whether the captured log contains a warning holding the
// given substring. It is the loop the tests above write inline, factored out
// because the event-streaming tests assert both its positive and negative forms
// against several distinct messages.
func warnedAbout(hook *logtest.Hook, substring string) bool {
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, substring) {
			return true
		}
	}
	return false
}

// assertBrokerList compares a resolved broker list against the expectation,
// checking the length fatally before comparing entries so that a mismatch reports
// the whole slice rather than panicking on an index. A nil expectation is
// satisfied by any empty slice: "no brokers configured" is one state, however
// envconfig happens to represent it.
func assertBrokerList(t *testing.T, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("Expected Kafka.Brokers to be %v, got %v", want, got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("Kafka.Brokers entry %d: expected %q, got %q", i, want[i], got[i])
		}
	}
}

// TestValidateAndAddDefaults_KafkaAndRelayDefaults pins every default the event
// streaming pipeline is built on. These are contract values rather than
// preferences: the relay derives its retry schedule from the three relay values,
// topic and dead-letter names are derived from the prefix, and topic creation uses
// the partition count and replication factor. A change to any of them changes
// delivery behaviour, so each is asserted individually.
//
// The environment is cleared first, and the bare and prefixed forms of every key
// with it, so that only the code under test can contribute a value.
func TestValidateAndAddDefaults_KafkaAndRelayDefaults(t *testing.T) {
	clearEventStreamingEnv(t)

	hook := logtest.NewGlobal()
	defer hook.Reset()

	cnf := eventStreamingBaseConfig()
	if err := cnf.validateAndAddDefaults(); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Relay retry window: five attempts, a 1s base delay and a 30s cap.
	if cnf.Relay.MaxRetryAttempts != 5 {
		t.Errorf("Expected Relay.MaxRetryAttempts to be 5, got %d", cnf.Relay.MaxRetryAttempts)
	}
	if cnf.Relay.RetryBaseBackoffMS != 1000 {
		t.Errorf("Expected Relay.RetryBaseBackoffMS to be 1000, got %d", cnf.Relay.RetryBaseBackoffMS)
	}
	if cnf.Relay.RetryMaxBackoffMS != 30000 {
		t.Errorf("Expected Relay.RetryMaxBackoffMS to be 30000, got %d", cnf.Relay.RetryMaxBackoffMS)
	}

	// SEC-10: the subscriber measurement budget IS defaulted, and the value is asserted
	// because it decides how much of the consumer-lag signal exists. A subscriber past the
	// budget has no lag series at all, so a silent change here would silently shrink
	// monitoring coverage — the one kind of regression that makes the system look healthier
	// rather than worse.
	if cnf.Relay.SubscriberMetricsBudget != 200 {
		t.Errorf("Expected Relay.SubscriberMetricsBudget to be 200, got %d", cnf.Relay.SubscriberMetricsBudget)
	}

	// EVENT RETENTION IS NOT DEFAULTED, and the asymmetry with the three values above is
	// asserted rather than assumed. Those three have a correct answer that requirement R-4
	// fixes, so an unset value is filled in. A retention period has no correct answer this
	// code can know — it depends on jurisdiction, audit programme and any legal hold in force
	// — and getting it wrong DELETES ledger-adjacent evidence irreversibly. Zero means
	// retention is disabled, which is the only safe default for a destructive operation, and a
	// future change that "helpfully" supplied one would start deleting on every deployment
	// that upgraded.
	if cnf.Relay.EventRetentionDays != 0 {
		t.Errorf("Expected Relay.EventRetentionDays to default to 0 (retention disabled), got %d",
			cnf.Relay.EventRetentionDays)
	}
	if period := cnf.EventRetentionPeriod(); period != 0 {
		t.Errorf("Expected EventRetentionPeriod to be 0 while retention is disabled, got %s", period)
	}

	// Topic naming and geometry.
	if cnf.Kafka.TopicPrefix != "blnk" {
		t.Errorf("Expected Kafka.TopicPrefix to be 'blnk', got '%s'", cnf.Kafka.TopicPrefix)
	}
	if cnf.Kafka.MinPartitions != 6 {
		t.Errorf("Expected Kafka.MinPartitions to be 6, got %d", cnf.Kafka.MinPartitions)
	}
	if cnf.Kafka.ReplicationFactor != 3 {
		t.Errorf("Expected Kafka.ReplicationFactor to be 3, got %d", cnf.Kafka.ReplicationFactor)
	}

	// The consumer-lag sweep budget. A zero default would be indistinguishable from
	// "measure nothing", and the collector would publish an empty lag inventory on a
	// registry of any size — so the number the shipped binary uses is asserted here
	// rather than left to whatever the collector happens to fall back to.
	if cnf.Kafka.MetricsSubscriberBudget != 200 {
		t.Errorf("Expected Kafka.MetricsSubscriberBudget to be 200, got %d",
			cnf.Kafka.MetricsSubscriberBudget)
	}

	// No broker list and no credentials are ever invented. A shipped default for
	// either SASL field would be a credential baked into the binary.
	assertBrokerList(t, cnf.Kafka.Brokers, nil)
	if cnf.Kafka.SASLAdminUser != "" {
		t.Errorf("Expected Kafka.SASLAdminUser to stay empty, got '%s'", cnf.Kafka.SASLAdminUser)
	}
	if cnf.Kafka.SASLAdminSecret != "" {
		t.Errorf("Expected Kafka.SASLAdminSecret to stay empty, got a value of %d characters", len(cnf.Kafka.SASLAdminSecret))
	}

	// The admin-producer allowance must default to FALSE, and this assertion is the
	// guard on that. It permits the event publisher to authenticate with the
	// administrative credentials when no producer principal is configured — the
	// principal that creates topics, mints SCRAM credentials and rewrites ACLs. A
	// deployment reaches that state by leaving KAFKA_SASL_USER and KAFKA_SASL_SECRET
	// unset, which is where every deployment starts, so a default of true would mean an
	// ordinary rollout ran its whole data plane at maximum privilege with nothing but a
	// log line to say so. Defaulting it on would not look like a security change in a
	// diff, which is exactly why it is asserted here.
	if cnf.Kafka.AllowAdminProducer {
		t.Error("Expected Kafka.AllowAdminProducer to default to false; publishing as the Kafka administrator must be opted into explicitly")
	}

	// An unset sunset date means the sunset has not passed, so no date is invented.
	if cnf.WebhookDeprecationSunsetDate != "" {
		t.Errorf("Expected WebhookDeprecationSunsetDate to stay empty, got '%s'", cnf.WebhookDeprecationSunsetDate)
	}

	// Derive the schedule the three relay defaults produce — 1s, 2s, 4s, 8s, 16s
	// with the 30s cap deliberately never reached — so that changing a default
	// fails here instead of silently changing how long a failing event is retried.
	wantSchedule := []int{1000, 2000, 4000, 8000, 16000}
	if len(wantSchedule) != cnf.Relay.MaxRetryAttempts {
		t.Fatalf("Expected the schedule to cover all %d attempts, it covers %d", cnf.Relay.MaxRetryAttempts, len(wantSchedule))
	}
	delay := cnf.Relay.RetryBaseBackoffMS
	for attempt, want := range wantSchedule {
		if delay != want {
			t.Errorf("Retry delay for attempt %d: expected %dms, got %dms", attempt+1, want, delay)
		}
		if delay > cnf.Relay.RetryMaxBackoffMS {
			t.Errorf("Retry delay for attempt %d exceeds the %dms cap: %dms", attempt+1, cnf.Relay.RetryMaxBackoffMS, delay)
		}
		delay *= 2
		if delay > cnf.Relay.RetryMaxBackoffMS {
			delay = cnf.Relay.RetryMaxBackoffMS
		}
	}

	// Graceful degradation: an empty broker list is a supported steady state, not a
	// misconfiguration. It selects the no-op event publisher, which is what keeps a
	// deployment without Kafka — and this whole test suite — working. A warning here
	// would be noise on every Kafka-less run, so none may be emitted.
	for _, entry := range hook.AllEntries() {
		if entry.Level != logrus.WarnLevel {
			continue
		}
		message := strings.ToLower(entry.Message)
		if strings.Contains(message, "kafka") || strings.Contains(message, "broker") {
			t.Errorf("Did not expect a Kafka or broker warning for an unconfigured broker list, got %q", entry.Message)
		}
	}
}

// TestValidateAndAddDefaults_KafkaConfiguredValuesSurvive proves the defaults fill
// only unset values. Every assignment in setKafkaDefaults and setRelayDefaults is
// guarded on the zero value, and this is the test that keeps it that way.
func TestValidateAndAddDefaults_KafkaConfiguredValuesSurvive(t *testing.T) {
	clearEventStreamingEnv(t)

	// The replication factor is the case that matters most. A single-broker KRaft
	// cluster — the local Docker Compose stack — cannot satisfy a factor of 3;
	// topic creation fails outright. The value must therefore be genuinely
	// configuration-driven, so a configured 1 has to survive the production
	// default of 3 rather than be overwritten by it.
	t.Run("a replication factor of one survives the production default of three", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.ReplicationFactor = 1

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.Kafka.ReplicationFactor != 1 {
			t.Errorf("Expected Kafka.ReplicationFactor to remain 1, got %d", cnf.Kafka.ReplicationFactor)
		}
	})

	t.Run("a configured topic prefix and partition count survive", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.TopicPrefix = "acme"
		cnf.Kafka.MinPartitions = 12

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.Kafka.TopicPrefix != "acme" {
			t.Errorf("Expected Kafka.TopicPrefix to remain 'acme', got '%s'", cnf.Kafka.TopicPrefix)
		}
		if cnf.Kafka.MinPartitions != 12 {
			t.Errorf("Expected Kafka.MinPartitions to remain 12, got %d", cnf.Kafka.MinPartitions)
		}
	})

	t.Run("a configured lag-sweep budget below the ceiling survives", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.MetricsSubscriberBudget = 750

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.Kafka.MetricsSubscriberBudget != 750 {
			t.Errorf("Expected Kafka.MetricsSubscriberBudget to remain 750, got %d",
				cnf.Kafka.MetricsSubscriberBudget)
		}
	})

	t.Run("a configured relay retry window survives", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Relay = RelayConfig{MaxRetryAttempts: 3, RetryBaseBackoffMS: 250, RetryMaxBackoffMS: 5000}

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.Relay.MaxRetryAttempts != 3 {
			t.Errorf("Expected Relay.MaxRetryAttempts to remain 3, got %d", cnf.Relay.MaxRetryAttempts)
		}
		// The two backoff values are milliseconds and are stored verbatim; unlike
		// the queue's hot-pair TTL they are never reinterpreted as another unit.
		if cnf.Relay.RetryBaseBackoffMS != 250 {
			t.Errorf("Expected Relay.RetryBaseBackoffMS to remain 250, got %d", cnf.Relay.RetryBaseBackoffMS)
		}
		if cnf.Relay.RetryMaxBackoffMS != 5000 {
			t.Errorf("Expected Relay.RetryMaxBackoffMS to remain 5000, got %d", cnf.Relay.RetryMaxBackoffMS)
		}
	})

	t.Run("configured brokers and admin credentials survive", func(t *testing.T) {
		// Obviously fake values: this file must never carry a credential that
		// could be mistaken for a real one.
		const (
			adminUser   = "blnk-test-admin"
			adminSecret = "placeholder-not-a-real-secret"
		)
		cnf := kafkaEnabledConfig("broker-1:9092", "broker-2:9092")
		cnf.Kafka.SASLAdminUser = adminUser
		cnf.Kafka.SASLAdminSecret = adminSecret

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		assertBrokerList(t, cnf.Kafka.Brokers, []string{"broker-1:9092", "broker-2:9092"})
		if cnf.Kafka.SASLAdminUser != adminUser {
			t.Errorf("Expected Kafka.SASLAdminUser to remain '%s', got '%s'", adminUser, cnf.Kafka.SASLAdminUser)
		}
		if cnf.Kafka.SASLAdminSecret != adminSecret {
			t.Errorf("Expected Kafka.SASLAdminSecret to be preserved verbatim, got '%s'", cnf.Kafka.SASLAdminSecret)
		}
	})
}

// TestSetKafkaDefaults_MetricsSubscriberBudgetIsCorrectedNeverRefused pins both ends of the
// consumer-lag sweep budget's domain, and pins that a bad value is CORRECTED rather than
// fatal.
//
// # Why the budget needs a floor as well as a default
//
// The collector reads Kafka.MetricsSubscriberBudget to decide how many registry rows one lag
// sweep examines. A zero or negative value there does not mean "no limit" to the collector —
// it means the sweep window is empty, so no subscriber is ever measured, blnk.kafka.consumer_lag
// is published for nothing, and SubscriberConsumerLagHigh can never fire. That failure is
// silent by construction: the metric endpoint still answers, the series is simply absent. So
// every non-positive value has to resolve to the shipped default, not to itself.
//
// # Why the ceiling clamps instead of refusing
//
// A budget above MaxMetricsSubscriberBudget is a real operational hazard rather than a typo to
// reject: each subscriber measured costs an OffsetFetch and a ListOffsets round trip per
// authorised topic, and each subscriber-topic pair is an exported gauge series. So an
// unbounded budget makes one collection tick unbounded in both duration and cardinality, and a
// tick that outlasts the collection interval stops EVERY event gauge refreshing on schedule —
// including the dead-letter age that V-4's alert reads.
//
// It is clamped rather than fatal because that is how every other out-of-range value in this
// file is handled: a misconfigured metrics budget must never stop the ledger from serving. The
// correction is asserted to be LOGGED, because a silent clamp would leave an operator who
// asked for 50,000 believing they had it.
func TestSetKafkaDefaults_MetricsSubscriberBudgetIsCorrectedNeverRefused(t *testing.T) {
	for name, testCase := range map[string]struct {
		configured  int
		want        int
		wantClamped bool
	}{
		"unset falls back to the shipped default":    {configured: 0, want: 200},
		"a negative budget falls back":               {configured: -1, want: 200},
		"a large negative budget falls back":         {configured: -5000, want: 200},
		"one is honoured, small but not nonsensical": {configured: 1, want: 1},
		"the ceiling itself is not clamped":          {configured: MaxMetricsSubscriberBudget, want: MaxMetricsSubscriberBudget},
		"one above the ceiling is clamped": {
			configured: MaxMetricsSubscriberBudget + 1, want: MaxMetricsSubscriberBudget, wantClamped: true,
		},
		"a wildly oversized budget is clamped": {
			configured: 1_000_000, want: MaxMetricsSubscriberBudget, wantClamped: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			clearEventStreamingEnv(t)
			hook := logtest.NewGlobal()
			defer hook.Reset()

			cnf := eventStreamingBaseConfig()
			cnf.Kafka.MetricsSubscriberBudget = testCase.configured

			if err := cnf.validateAndAddDefaults(); err != nil {
				t.Fatalf("A bad metrics budget must never stop configuration loading, got %v", err)
			}
			if cnf.Kafka.MetricsSubscriberBudget != testCase.want {
				t.Errorf("Expected Kafka.MetricsSubscriberBudget %d for a configured %d, got %d",
					testCase.want, testCase.configured, cnf.Kafka.MetricsSubscriberBudget)
			}

			clamped := warnedAbout(hook, "above the supported ceiling")
			if clamped != testCase.wantClamped {
				t.Errorf("Expected the clamp warning to be logged=%t for a configured %d, got %t",
					testCase.wantClamped, testCase.configured, clamped)
			}
		})
	}
}

// TestValidateAndAddDefaults_KafkaSASLPairIsFatalOnlyWithBrokers pins where the pair
// contract is enforced, which is as important as the contract itself.
//
// TestResolveWebhookDeprecationWindow_AnUnusableWindowIsRefusedNeverInvented pins what
// happens when Kafka is configured and no usable retirement instant has been given.
//
// # Why an absent date with brokers is now a REFUSAL
//
// It was a warning, and the warning promised that dual delivery would continue
// indefinitely. blnk.WebhookSunsetPassed does the opposite: for a deployment that IS
// publishing, a missing or unparseable window fails closed and answers that the sunset has
// already passed, so the legacy leg stops and the deprecated management routes answer 410
// Gone. Two components disagreeing about the single decision requirement R-12 consists of is
// worse than either answer, and the fail-closed one is the safer of the two to keep — an
// unset variable must not be able to preserve a deprecated, less protected transport
// silently. So the combination is refused here, before any traffic is served, which is what
// event_sunset.go's documentation already said happened.
//
// A MALFORMED date remains fatal for the same reason it always was, with or without brokers:
// that IS a mis-statement, and it would otherwise resolve silently to "the sunset has not
// passed".
//
// # Why no window is derived either
//
// Inventing one from "now" would produce a sunset that MOVES ON EVERY RESTART, so the legacy
// transport's retirement instant would depend on when a pod last happened to start. The error
// names the variable instead, which is the outcome an operator can act on.
//
// The local-dev flag makes NO difference here, and that is asserted rather than assumed: an
// exception that derived a window under it would be one restart away from being the behaviour
// a production deployment gets the moment the flag is left set.
func TestResolveWebhookDeprecationWindow_AnUnusableWindowIsRefusedNeverInvented(t *testing.T) {
	clearEventStreamingEnv(t)

	for name, localDev := range map[string]bool{
		"local dev acknowledged": true,
		"not local development":  false,
	} {
		t.Run("brokers, no window, "+name, func(t *testing.T) {
			cnf := eventStreamingBaseConfig()
			cnf.Kafka.Brokers = []string{"kafka:9092"}
			cnf.Kafka.InsecureLocalDev = localDev

			err := cnf.resolveWebhookDeprecationWindow()
			if err == nil {
				t.Fatal("Expected brokers with no dual-delivery window to be refused")
			}

			// The refusal must NAME THE VARIABLE. "Invalid configuration" without the key is
			// a line an operator cannot act on.
			if !strings.Contains(err.Error(), "webhook_deprecation_sunset_date") {
				t.Errorf("Expected the error to name the missing setting, got %v", err)
			}

			if cnf.WebhookDeprecationStartDate != "" || cnf.WebhookDeprecationSunsetDate != "" {
				t.Errorf("Expected NO window to be invented, got start=%q sunset=%q",
					cnf.WebhookDeprecationStartDate, cnf.WebhookDeprecationSunsetDate)
			}
		})
	}

	t.Run("a half-written window cannot survive with one end", func(t *testing.T) {
		// A start that arrived from a configuration file with no sunset beside it is not a
		// window. Keeping it would leave the sunset decision resting on a value nothing
		// validates and the API's 410 guard reading a date that no longer has a partner. With
		// brokers configured the orphan is ALSO a refusal, so both properties are asserted at
		// once: cleared, and reported.
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.Brokers = []string{"kafka:9092"}
		cnf.WebhookDeprecationStartDate = testWindowStart

		if err := cnf.resolveWebhookDeprecationWindow(); err == nil {
			t.Fatal("Expected an orphaned start with brokers and no sunset to be refused")
		}
		if cnf.WebhookDeprecationStartDate != "" {
			t.Errorf("Expected the orphaned start to be cleared, got %q",
				cnf.WebhookDeprecationStartDate)
		}
	})

	t.Run("a half-written window with no brokers is cleared and accepted", func(t *testing.T) {
		// Without a transport there is no window, so an orphaned start is simply discarded:
		// nothing has been mis-stated about a migration that is not happening.
		cnf := eventStreamingBaseConfig()
		cnf.WebhookDeprecationStartDate = testWindowStart

		if err := cnf.resolveWebhookDeprecationWindow(); err != nil {
			t.Fatalf("Expected an orphaned start with no brokers to load cleanly, got %v", err)
		}
		if cnf.WebhookDeprecationStartDate != "" {
			t.Errorf("Expected the orphaned start to be cleared, got %q",
				cnf.WebhookDeprecationStartDate)
		}
	})

	t.Run("a malformed date is still fatal", func(t *testing.T) {
		// The other half of the decision above: an absent date is a choice not yet made, a
		// malformed one is a choice mis-stated, and only the second can silently mean
		// "the sunset has not passed" forever.
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.Brokers = []string{"kafka:9092"}
		cnf.Kafka.InsecureLocalDev = true
		cnf.WebhookDeprecationSunsetDate = "30 days from now"

		err := cnf.resolveWebhookDeprecationWindow()
		if err == nil {
			t.Fatal("Expected a malformed sunset date to refuse to load")
		}
		if !strings.Contains(err.Error(), "webhook_deprecation_sunset_date") {
			t.Errorf("Expected the error to name the offending field, got %v", err)
		}
	})

	t.Run("no brokers and local dev set: no window is invented", func(t *testing.T) {
		// There is nothing to migrate to, so there is no window to describe. Deriving
		// one here would make the deprecated transport look scheduled for retirement on
		// a deployment that never enabled Kafka.
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.InsecureLocalDev = true

		if err := cnf.resolveWebhookDeprecationWindow(); err != nil {
			t.Fatalf("Expected no brokers to load cleanly, got %v", err)
		}
		if cnf.WebhookDeprecationStartDate != "" || cnf.WebhookDeprecationSunsetDate != "" {
			t.Errorf("Expected no window with no brokers, got start=%q sunset=%q",
				cnf.WebhookDeprecationStartDate, cnf.WebhookDeprecationSunsetDate)
		}
	})

	t.Run("an explicit window is honoured and its start is derived", func(t *testing.T) {
		// The configured sunset is authoritative and the start is arithmetic, so "exactly
		// 30 days" is a property of the code rather than of the configuration.
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.Brokers = []string{"kafka:9092"}
		cnf.Kafka.InsecureLocalDev = true
		cnf.WebhookDeprecationStartDate = testWindowStart
		cnf.WebhookDeprecationSunsetDate = testWindowSunset

		if err := cnf.resolveWebhookDeprecationWindow(); err != nil {
			t.Fatalf("Expected an explicit window to be accepted, got %v", err)
		}

		wantSunset, err := time.Parse(time.RFC3339, testWindowSunset)
		if err != nil {
			t.Fatalf("the fixture sunset is not RFC3339: %v", err)
		}
		gotSunset, err := time.Parse(time.RFC3339, cnf.WebhookDeprecationSunsetDate)
		if err != nil {
			t.Fatalf("Resolved sunset is not RFC3339: %v", err)
		}
		if !gotSunset.Equal(wantSunset) {
			t.Errorf("Expected the configured sunset %s to be preserved, got %s",
				wantSunset.Format(time.RFC3339), gotSunset.Format(time.RFC3339))
		}

		gotStart, err := time.Parse(time.RFC3339, cnf.WebhookDeprecationStartDate)
		if err != nil {
			t.Fatalf("Resolved start is not RFC3339: %v", err)
		}
		if got := gotSunset.Sub(gotStart); got != WebhookDualDeliveryWindowDays*24*time.Hour {
			t.Errorf("Expected a window of exactly %d days, got %s",
				WebhookDualDeliveryWindowDays, got)
		}
	})
}

// Two properties are in tension and both must hold. A half-configured credential has
// to stop a deployment that actually uses Kafka, because it cannot be honoured and
// fails much later as a wrong-password or authorization error. But it must NOT stop a
// deployment with no brokers, because nothing reads it there and the graceful
// degradation that lets Blnk run entirely without Kafka is a shipped guarantee — the
// .env.example that leaves KAFKA_BROKERS unset would otherwise fail to load the
// moment an operator filled in one SASL key.
func TestValidateAndAddDefaults_KafkaSASLPairIsFatalOnlyWithBrokers(t *testing.T) {
	clearEventStreamingEnv(t)

	const secret = "placeholder-not-a-real-secret"

	t.Run("a half pair with brokers configured refuses to load", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.Brokers = []string{"broker-1:9092"}
		cnf.WebhookDeprecationSunsetDate = testWindowSunset
		cnf.Kafka.SASLAdminUser = "blnk-test-admin"

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected a half-configured credential to fail validation when brokers are configured")
		}
		if !strings.Contains(err.Error(), "KAFKA_SASL_ADMIN_SECRET") {
			t.Errorf("Expected the error to name the missing variable, got %v", err)
		}
	})

	t.Run("a secret without a user and brokers configured refuses to load", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.Brokers = []string{"broker-1:9092"}
		cnf.WebhookDeprecationSunsetDate = testWindowSunset
		cnf.Kafka.SASLAdminSecret = secret

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected a secret with no principal to fail validation when brokers are configured")
		}
		if !strings.Contains(err.Error(), "KAFKA_SASL_ADMIN_USER") {
			t.Errorf("Expected the error to name the missing variable, got %v", err)
		}
		if strings.Contains(err.Error(), secret) {
			t.Error("The error must never contain the secret's value")
		}
	})

	t.Run("a half pair with no brokers warns and still loads", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := eventStreamingBaseConfig()
		cnf.Kafka.SASLAdminUser = "blnk-test-admin"

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error when no broker is configured, got %v", err)
		}
		if !warnedAbout(hook, "half-configured") {
			t.Error("Expected a warning that the administrative SASL credential is half-configured")
		}
	})

	t.Run("a complete pair with brokers configured loads cleanly", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := eventStreamingBaseConfig()
		cnf.Kafka.Brokers = []string{"broker-1:9092"}
		cnf.WebhookDeprecationSunsetDate = testWindowSunset
		cnf.Kafka.SASLAdminUser = "blnk-test-admin"
		cnf.Kafka.SASLAdminSecret = secret

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error for a complete credential pair, got %v", err)
		}
		if warnedAbout(hook, "half-configured") {
			t.Error("A complete credential pair must not be warned about")
		}
	})

	t.Run("no credential at all with brokers configured loads cleanly", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := eventStreamingBaseConfig()
		cnf.Kafka.Brokers = []string{"broker-1:9092"}
		cnf.WebhookDeprecationSunsetDate = testWindowSunset

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected a plaintext broker to be supported, got %v", err)
		}
		if warnedAbout(hook, "half-configured") {
			t.Error("An entirely unset credential pair is a plaintext deployment, not a half-configured one")
		}
	})
}

// TestLoadConfigFromFile_KafkaEnvNameForms proves that every environment variable
// name form the deployment contract and the repository convention offer actually
// resolves into the new configuration.
//
// Read this before "correcting" the un-prefixed envconfig tags in config.go.
// envconfig builds a field's primary key by accumulating the prefix through every
// enclosing struct and appending the tag literal, then consults the bare tag
// literal as an alternate key only when the primary is unset. So:
//
//   - Configuration.Kafka contributes a segment, making the envconfig primary key
//     for Brokers BLNK_KAFKA_KAFKA_BROKERS and the alternate the mandated
//     KAFKA_BROKERS.
//   - The ordinary BLNK_KAFKA_BROKERS is neither of those keys. It USED TO RESOLVE
//     TO NOTHING, which meant a deployment spelling the variable the way every other
//     setting in config.go is spelled silently got no brokers and no error.
//     applyPrefixedEnvAliases resolves it explicitly now, and every alias in that
//     table is exercised below.
//   - WebhookDeprecationSunsetDate is a top-level field, so it accumulates no
//     intermediate segment and envconfig resolves both of its forms unaided.
//
// Adding a BLNK_ prefix to a tag would only change which bare name is honoured and
// would break the mandated one. Each form is exercised in isolation — one key set
// per subtest, every other form unset — because the point is that each resolves on
// its own, and because two forms set at once would prove nothing about either. The
// precedence between two forms that ARE both set is asserted separately, by
// TestLoadConfigFromFile_PrefixedAliasWinsOverBareName.
func TestLoadConfigFromFile_KafkaEnvNameForms(t *testing.T) {
	const sunsetDate = "2026-09-04T00:00:00Z"

	cases := []struct {
		name   string
		envKey string
		value  string
		assert func(t *testing.T, loaded *Configuration)
	}{
		{
			name:   "the mandated bare KAFKA_BROKERS resolves",
			envKey: "KAFKA_BROKERS",
			value:  "broker-1:9092,broker-2:9092",
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.Brokers, []string{"broker-1:9092", "broker-2:9092"})
			},
		},
		{
			name:   "the prefixed BLNK_KAFKA_KAFKA_BROKERS primary key resolves",
			envKey: "BLNK_KAFKA_KAFKA_BROKERS",
			value:  "broker-3:9092,broker-4:9092",
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.Brokers, []string{"broker-3:9092", "broker-4:9092"})
			},
		},
		{
			// THE FINDING. This is the name a reader of config.go would write, because
			// every other setting in that file is spelled this way — and it used to be
			// read by nothing at all, so a deployment that set it got no brokers, the
			// no-op publisher, and no error to explain why nothing was published.
			// applyPrefixedEnvAliases resolves it explicitly now.
			name:   "the ordinary BLNK_KAFKA_BROKERS alias resolves",
			envKey: "BLNK_KAFKA_BROKERS",
			value:  "broker-5:9092,broker-6:9092",
			assert: func(t *testing.T, loaded *Configuration) {
				// A comma-separated value through the alias must split the same way the
				// bare and nested forms do, so the alias is a genuine equivalent rather
				// than a single-value special case.
				assertBrokerList(t, loaded.Kafka.Brokers, []string{"broker-5:9092", "broker-6:9092"})
			},
		},
		{
			// The three forms of the subscriber-facing list, for the same reason the
			// three above exist. This one answered to its bare name and to envconfig's
			// BLNK_KAFKA_KAFKA_SUBSCRIBER_BROKERS artefact, but NOT to the ordinary
			// prefixed spelling — while config.go claimed every field of the struct was
			// aliased. A deployment writing the documented convention therefore got no
			// subscriber-facing brokers, and credential issuance refused with 503 and
			// nothing to explain why.
			name:   "the mandated bare KAFKA_SUBSCRIBER_BROKERS resolves",
			envKey: "KAFKA_SUBSCRIBER_BROKERS",
			value:  "public-1:9092,public-2:9092",
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.SubscriberBrokers, []string{"public-1:9092", "public-2:9092"})
			},
		},
		{
			name:   "the prefixed BLNK_KAFKA_KAFKA_SUBSCRIBER_BROKERS primary key resolves",
			envKey: "BLNK_KAFKA_KAFKA_SUBSCRIBER_BROKERS",
			value:  "public-3:9092",
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.SubscriberBrokers, []string{"public-3:9092"})
			},
		},
		{
			name:   "the ordinary BLNK_KAFKA_SUBSCRIBER_BROKERS alias resolves",
			envKey: "BLNK_KAFKA_SUBSCRIBER_BROKERS",
			value:  "public-4:9092,public-5:9092",
			assert: func(t *testing.T, loaded *Configuration) {
				// Split by the library, so the alias is a genuine equivalent of the bare
				// name rather than a single-value special case.
				assertBrokerList(t, loaded.Kafka.SubscriberBrokers, []string{"public-4:9092", "public-5:9092"})
			},
		},
		{
			// THE FINDING FOR THE HISTORICAL PREFIX LIST. Both compose files forward
			// BLNK_KAFKA_HISTORICAL_TOPIC_PREFIXES and .env.example documents that either
			// form resolves — and no field resolved it, so only the bare name and
			// envconfig's own BLNK_KAFKA_KAFKA_ artefact worked. The consequence is the
			// worst this variable has: a deployment that renamed KAFKA_TOPIC_PREFIX and
			// declared the old namespace through the documented, forwarded name got an
			// EMPTY historical list, and every event captured under the previous prefix was
			// refused at writer resolution after the next restart.
			name:   "the mandated bare KAFKA_HISTORICAL_TOPIC_PREFIXES resolves",
			envKey: "KAFKA_HISTORICAL_TOPIC_PREFIXES",
			value:  "legacy,older",
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.HistoricalTopicPrefixes, []string{"legacy", "older"})
			},
		},
		{
			name:   "the prefixed BLNK_KAFKA_KAFKA_HISTORICAL_TOPIC_PREFIXES primary key resolves",
			envKey: "BLNK_KAFKA_KAFKA_HISTORICAL_TOPIC_PREFIXES",
			value:  "legacy",
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.HistoricalTopicPrefixes, []string{"legacy"})
			},
		},
		{
			name:   "the ordinary BLNK_KAFKA_HISTORICAL_TOPIC_PREFIXES alias resolves",
			envKey: "BLNK_KAFKA_HISTORICAL_TOPIC_PREFIXES",
			value:  "legacy,older",
			assert: func(t *testing.T, loaded *Configuration) {
				// Split by the library, so the alias is a genuine equivalent of the bare
				// name rather than a single-value special case.
				assertBrokerList(t, loaded.Kafka.HistoricalTopicPrefixes, []string{"legacy", "older"})
			},
		},
		{
			// The key-scope declaration is an AUTHORIZATION control, so a form that
			// resolved to nothing would silently leave issuance refusing every key-scoped
			// subscriber on a deployment that had declared a gateway. Both of its names are
			// exercised for that reason, and so are both of the gateway list's.
			name:   "the mandated bare KAFKA_KEY_SCOPE_ENFORCEMENT resolves",
			envKey: "KAFKA_KEY_SCOPE_ENFORCEMENT",
			value:  KeyScopeEnforcementBrokerGateway,
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.KeyScopeEnforcement != KeyScopeEnforcementBrokerGateway {
					t.Errorf("Expected Kafka.KeyScopeEnforcement to be %q, got %q",
						KeyScopeEnforcementBrokerGateway, loaded.Kafka.KeyScopeEnforcement)
				}
			},
		},
		{
			name:   "the ordinary BLNK_KAFKA_KEY_SCOPE_ENFORCEMENT alias resolves",
			envKey: "BLNK_KAFKA_KEY_SCOPE_ENFORCEMENT",
			value:  KeyScopeEnforcementBrokerGateway,
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.KeyScopeEnforcement != KeyScopeEnforcementBrokerGateway {
					t.Errorf("Expected Kafka.KeyScopeEnforcement to be %q, got %q",
						KeyScopeEnforcementBrokerGateway, loaded.Kafka.KeyScopeEnforcement)
				}
			},
		},
		{
			name:   "the mandated bare KAFKA_KEY_SCOPE_GATEWAY_BROKERS resolves",
			envKey: "KAFKA_KEY_SCOPE_GATEWAY_BROKERS",
			value:  "gateway-1:9095,gateway-2:9095",
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.KeyScopeGatewayBrokers,
					[]string{"gateway-1:9095", "gateway-2:9095"})
			},
		},
		{
			name:   "the ordinary BLNK_KAFKA_KEY_SCOPE_GATEWAY_BROKERS alias resolves",
			envKey: "BLNK_KAFKA_KEY_SCOPE_GATEWAY_BROKERS",
			value:  "gateway-3:9095,gateway-4:9095",
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.KeyScopeGatewayBrokers,
					[]string{"gateway-3:9095", "gateway-4:9095"})
			},
		},
		{
			name:   "the ordinary BLNK_KAFKA_TOPIC_PREFIX alias resolves",
			envKey: "BLNK_KAFKA_TOPIC_PREFIX",
			value:  "acme",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.TopicPrefix != "acme" {
					t.Errorf("Expected Kafka.TopicPrefix to be 'acme', got '%s'", loaded.Kafka.TopicPrefix)
				}
			},
		},
		{
			name:   "the ordinary BLNK_KAFKA_MIN_PARTITIONS alias resolves",
			envKey: "BLNK_KAFKA_MIN_PARTITIONS",
			value:  "12",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.MinPartitions != 12 {
					t.Errorf("Expected Kafka.MinPartitions to be 12, got %d", loaded.Kafka.MinPartitions)
				}
			},
		},
		{
			// The lag-sweep budget is the newest variable in the table, so both of its
			// forms are exercised: the bare name the deployment documentation gives, and
			// the BLNK_-prefixed name a reader of config.go would write. An operator who
			// raised the budget through the form that resolved to nothing would find the
			// lag inventory still truncated with no error to explain it.
			name:   "the documented bare EVENT_METRICS_SUBSCRIBER_BUDGET resolves",
			envKey: "EVENT_METRICS_SUBSCRIBER_BUDGET",
			value:  "1500",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.MetricsSubscriberBudget != 1500 {
					t.Errorf("Expected Kafka.MetricsSubscriberBudget to be 1500, got %d",
						loaded.Kafka.MetricsSubscriberBudget)
				}
			},
		},
		{
			name:   "the ordinary BLNK_EVENT_METRICS_SUBSCRIBER_BUDGET alias resolves",
			envKey: "BLNK_EVENT_METRICS_SUBSCRIBER_BUDGET",
			value:  "1250",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.MetricsSubscriberBudget != 1250 {
					t.Errorf("Expected Kafka.MetricsSubscriberBudget to be 1250, got %d",
						loaded.Kafka.MetricsSubscriberBudget)
				}
			},
		},
		{
			name:   "the ordinary BLNK_KAFKA_TLS_ENABLED alias resolves",
			envKey: "BLNK_KAFKA_TLS_ENABLED",
			value:  "true",
			assert: func(t *testing.T, loaded *Configuration) {
				if !loaded.Kafka.TLS.Enabled {
					t.Error("Expected Kafka.TLS.Enabled to be true")
				}
			},
		},
		{
			name:   "the mandated bare KAFKA_TLS_ENABLED resolves",
			envKey: "KAFKA_TLS_ENABLED",
			value:  "true",
			assert: func(t *testing.T, loaded *Configuration) {
				if !loaded.Kafka.TLS.Enabled {
					t.Error("Expected Kafka.TLS.Enabled to be true")
				}
			},
		},
		{
			name:   "the mandated bare KAFKA_SASL_USER resolves for the producer principal",
			envKey: "KAFKA_SASL_USER",
			value:  "blnk-producer",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.SASLUser != "blnk-producer" {
					t.Errorf("Expected Kafka.SASLUser to be 'blnk-producer', got '%s'", loaded.Kafka.SASLUser)
				}
			},
		},
		{
			name:   "the bare KAFKA_ALLOW_ADMIN_PRODUCER resolves",
			envKey: "KAFKA_ALLOW_ADMIN_PRODUCER",
			value:  "true",
			assert: func(t *testing.T, loaded *Configuration) {
				if !loaded.Kafka.AllowAdminProducer {
					t.Error("Expected Kafka.AllowAdminProducer to be true")
				}
			},
		},
		{
			name:   "the prefixed BLNK_KAFKA_ALLOW_ADMIN_PRODUCER alias resolves",
			envKey: "BLNK_KAFKA_ALLOW_ADMIN_PRODUCER",
			value:  "true",
			assert: func(t *testing.T, loaded *Configuration) {
				if !loaded.Kafka.AllowAdminProducer {
					t.Error("Expected Kafka.AllowAdminProducer to be true")
				}
			},
		},
		{
			name:   "the mandated bare KAFKA_TOPIC_PREFIX resolves",
			envKey: "KAFKA_TOPIC_PREFIX",
			value:  "acme",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.TopicPrefix != "acme" {
					t.Errorf("Expected Kafka.TopicPrefix to be 'acme', got '%s'", loaded.Kafka.TopicPrefix)
				}
			},
		},
		{
			name:   "the prefixed BLNK_KAFKA_KAFKA_TOPIC_PREFIX primary key resolves",
			envKey: "BLNK_KAFKA_KAFKA_TOPIC_PREFIX",
			value:  "acme",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.TopicPrefix != "acme" {
					t.Errorf("Expected Kafka.TopicPrefix to be 'acme', got '%s'", loaded.Kafka.TopicPrefix)
				}
			},
		},
		{
			name:   "the conventional BLNK_KAFKA_TOPIC_PREFIX resolves through the overlay",
			envKey: "BLNK_KAFKA_TOPIC_PREFIX",
			value:  "acme",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.TopicPrefix != "acme" {
					t.Errorf("Expected Kafka.TopicPrefix to be 'acme', got '%s'", loaded.Kafka.TopicPrefix)
				}
			},
		},
		{
			name:   "the mandated bare KAFKA_SASL_ADMIN_USER resolves",
			envKey: "KAFKA_SASL_ADMIN_USER",
			value:  "blnk-test-admin",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.SASLAdminUser != "blnk-test-admin" {
					t.Errorf("Expected Kafka.SASLAdminUser to be 'blnk-test-admin', got '%s'", loaded.Kafka.SASLAdminUser)
				}
			},
		},
		{
			name:   "the conventional BLNK_KAFKA_SASL_ADMIN_USER resolves through the overlay",
			envKey: "BLNK_KAFKA_SASL_ADMIN_USER",
			value:  "blnk-test-admin",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.SASLAdminUser != "blnk-test-admin" {
					t.Errorf("Expected Kafka.SASLAdminUser to be 'blnk-test-admin', got '%s'", loaded.Kafka.SASLAdminUser)
				}
			},
		},
		{
			// Obviously fake: this file must never carry a value that could be
			// mistaken for a real credential.
			name:   "the mandated bare KAFKA_SASL_ADMIN_SECRET resolves",
			envKey: "KAFKA_SASL_ADMIN_SECRET",
			value:  "placeholder-not-a-real-secret",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.SASLAdminSecret != "placeholder-not-a-real-secret" {
					t.Errorf("Expected Kafka.SASLAdminSecret to resolve, got a value of %d characters",
						len(loaded.Kafka.SASLAdminSecret))
				}
			},
		},
		{
			name:   "the conventional BLNK_KAFKA_SASL_ADMIN_SECRET resolves through the overlay",
			envKey: "BLNK_KAFKA_SASL_ADMIN_SECRET",
			value:  "placeholder-not-a-real-secret",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.SASLAdminSecret != "placeholder-not-a-real-secret" {
					t.Errorf("Expected Kafka.SASLAdminSecret to resolve, got a value of %d characters",
						len(loaded.Kafka.SASLAdminSecret))
				}
			},
		},
		{
			name:   "the mandated bare KAFKA_MIN_PARTITIONS resolves",
			envKey: "KAFKA_MIN_PARTITIONS",
			value:  "12",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.MinPartitions != 12 {
					t.Errorf("Expected Kafka.MinPartitions to be 12, got %d", loaded.Kafka.MinPartitions)
				}
			},
		},
		{
			name:   "the conventional BLNK_KAFKA_MIN_PARTITIONS resolves through the overlay",
			envKey: "BLNK_KAFKA_MIN_PARTITIONS",
			value:  "12",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.MinPartitions != 12 {
					t.Errorf("Expected Kafka.MinPartitions to be 12, got %d", loaded.Kafka.MinPartitions)
				}
			},
		},
		{
			// A configured 1 is the single-broker local stack, and it must survive
			// rather than being replaced by the production default of 3 — through
			// either name form.
			name:   "the mandated bare KAFKA_REPLICATION_FACTOR resolves",
			envKey: "KAFKA_REPLICATION_FACTOR",
			value:  "1",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.ReplicationFactor != 1 {
					t.Errorf("Expected Kafka.ReplicationFactor to be 1, got %d", loaded.Kafka.ReplicationFactor)
				}
			},
		},
		{
			name:   "the conventional BLNK_KAFKA_REPLICATION_FACTOR resolves through the overlay",
			envKey: "BLNK_KAFKA_REPLICATION_FACTOR",
			value:  "1",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Kafka.ReplicationFactor != 1 {
					t.Errorf("Expected Kafka.ReplicationFactor to be 1, got %d", loaded.Kafka.ReplicationFactor)
				}
			},
		},
		{
			name:   "the mandated bare RELAY_MAX_RETRY_ATTEMPTS resolves",
			envKey: "RELAY_MAX_RETRY_ATTEMPTS",
			value:  "3",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.MaxRetryAttempts != 3 {
					t.Errorf("Expected Relay.MaxRetryAttempts to be 3, got %d", loaded.Relay.MaxRetryAttempts)
				}
			},
		},
		{
			name:   "the prefixed BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS primary key resolves",
			envKey: "BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS",
			value:  "3",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.MaxRetryAttempts != 3 {
					t.Errorf("Expected Relay.MaxRetryAttempts to be 3, got %d", loaded.Relay.MaxRetryAttempts)
				}
			},
		},
		{
			// The relay half of the same finding: BLNK_RELAY_MAX_RETRY_ATTEMPTS used to
			// resolve to nothing and leave the default 5 in place, so an operator who
			// had deliberately reduced the retry budget still got five attempts.
			name:   "the ordinary BLNK_RELAY_MAX_RETRY_ATTEMPTS alias resolves",
			envKey: "BLNK_RELAY_MAX_RETRY_ATTEMPTS",
			value:  "3",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.MaxRetryAttempts != 3 {
					t.Errorf("Expected Relay.MaxRetryAttempts to be 3, got %d", loaded.Relay.MaxRetryAttempts)
				}
			},
		},
		{
			name:   "the ordinary BLNK_RELAY_RETRY_BASE_BACKOFF_MS alias resolves",
			envKey: "BLNK_RELAY_RETRY_BASE_BACKOFF_MS",
			value:  "250",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.RetryBaseBackoffMS != 250 {
					t.Errorf("Expected Relay.RetryBaseBackoffMS to be 250, got %d", loaded.Relay.RetryBaseBackoffMS)
				}
			},
		},
		{
			// SEC-10's variable, in all three forms. It is exercised here rather than
			// trusted because a budget that silently fails to resolve leaves the default
			// 200 in place — and an operator who raised it to cover a larger registry
			// would believe coverage was complete while it was not, which is the precise
			// failure this variable was added to end.
			name:   "the bare RELAY_SUBSCRIBER_METRICS_BUDGET resolves",
			envKey: "RELAY_SUBSCRIBER_METRICS_BUDGET",
			value:  "750",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.SubscriberMetricsBudget != 750 {
					t.Errorf("Expected Relay.SubscriberMetricsBudget to be 750, got %d", loaded.Relay.SubscriberMetricsBudget)
				}
			},
		},
		{
			name:   "the prefixed BLNK_RELAY_RELAY_SUBSCRIBER_METRICS_BUDGET primary key resolves",
			envKey: "BLNK_RELAY_RELAY_SUBSCRIBER_METRICS_BUDGET",
			value:  "750",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.SubscriberMetricsBudget != 750 {
					t.Errorf("Expected Relay.SubscriberMetricsBudget to be 750, got %d", loaded.Relay.SubscriberMetricsBudget)
				}
			},
		},
		{
			name:   "the ordinary BLNK_RELAY_SUBSCRIBER_METRICS_BUDGET alias resolves",
			envKey: "BLNK_RELAY_SUBSCRIBER_METRICS_BUDGET",
			value:  "750",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.SubscriberMetricsBudget != 750 {
					t.Errorf("Expected Relay.SubscriberMetricsBudget to be 750, got %d", loaded.Relay.SubscriberMetricsBudget)
				}
			},
		},
		{
			name:   "the ordinary BLNK_RELAY_RETRY_MAX_BACKOFF_MS alias resolves",
			envKey: "BLNK_RELAY_RETRY_MAX_BACKOFF_MS",
			value:  "45000",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.RetryMaxBackoffMS != 45000 {
					t.Errorf("Expected Relay.RetryMaxBackoffMS to be 45000, got %d", loaded.Relay.RetryMaxBackoffMS)
				}
			},
		},
		{
			name:   "the mandated bare RELAY_RETRY_BASE_BACKOFF_MS resolves",
			envKey: "RELAY_RETRY_BASE_BACKOFF_MS",
			value:  "250",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.RetryBaseBackoffMS != 250 {
					t.Errorf("Expected Relay.RetryBaseBackoffMS to be 250, got %d", loaded.Relay.RetryBaseBackoffMS)
				}
			},
		},
		{
			name:   "the conventional BLNK_RELAY_RETRY_BASE_BACKOFF_MS resolves through the overlay",
			envKey: "BLNK_RELAY_RETRY_BASE_BACKOFF_MS",
			value:  "250",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.RetryBaseBackoffMS != 250 {
					t.Errorf("Expected Relay.RetryBaseBackoffMS to be 250, got %d", loaded.Relay.RetryBaseBackoffMS)
				}
			},
		},
		{
			name:   "the mandated bare RELAY_RETRY_MAX_BACKOFF_MS resolves",
			envKey: "RELAY_RETRY_MAX_BACKOFF_MS",
			value:  "45000",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.RetryMaxBackoffMS != 45000 {
					t.Errorf("Expected Relay.RetryMaxBackoffMS to be 45000, got %d", loaded.Relay.RetryMaxBackoffMS)
				}
			},
		},
		{
			name:   "the conventional BLNK_RELAY_RETRY_MAX_BACKOFF_MS resolves through the overlay",
			envKey: "BLNK_RELAY_RETRY_MAX_BACKOFF_MS",
			value:  "45000",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.RetryMaxBackoffMS != 45000 {
					t.Errorf("Expected Relay.RetryMaxBackoffMS to be 45000, got %d", loaded.Relay.RetryMaxBackoffMS)
				}
			},
		},
		{
			name:   "the prefixed BLNK_RELAY_RELAY_RETRY_MAX_BACKOFF_MS primary key resolves",
			envKey: "BLNK_RELAY_RELAY_RETRY_MAX_BACKOFF_MS",
			value:  "45000",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.RetryMaxBackoffMS != 45000 {
					t.Errorf("Expected Relay.RetryMaxBackoffMS to be 45000, got %d", loaded.Relay.RetryMaxBackoffMS)
				}
			},
		},
		{
			name:   "the mandated bare WEBHOOK_DEPRECATION_SUNSET_DATE resolves",
			envKey: "WEBHOOK_DEPRECATION_SUNSET_DATE",
			value:  sunsetDate,
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.WebhookDeprecationSunsetDate != sunsetDate {
					t.Errorf("Expected WebhookDeprecationSunsetDate to be '%s', got '%s'", sunsetDate, loaded.WebhookDeprecationSunsetDate)
				}
			},
		},
		{
			name:   "the prefixed BLNK_WEBHOOK_DEPRECATION_SUNSET_DATE resolves",
			envKey: "BLNK_WEBHOOK_DEPRECATION_SUNSET_DATE",
			value:  sunsetDate,
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.WebhookDeprecationSunsetDate != sunsetDate {
					t.Errorf("Expected WebhookDeprecationSunsetDate to be '%s', got '%s'", sunsetDate, loaded.WebhookDeprecationSunsetDate)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Per subtest, not per test: one leaked variable is the single most
			// likely way this test passes for the wrong reason.
			clearEventStreamingEnv(t)
			restoreConfigStore(t)

			// The fixture carries a valid dual-delivery window because several of
			// the cases below configure brokers, and Kafka publishing without a
			// window is a hard configuration error.
			configFile := writeTempEventConfigFileWithWindow(t)
			t.Setenv(tc.envKey, tc.value)

			// Going through loadConfigFromFile is the point: it is what runs
			// envconfig.Process("blnk", &cnf) and then applyPrefixedEnvAliases over
			// the decoded file.
			if err := loadConfigFromFile(configFile); err != nil {
				t.Fatalf("loadConfigFromFile failed: %v", err)
			}
			loaded, err := Fetch()
			if err != nil {
				t.Fatalf("Fetch failed: %v", err)
			}

			tc.assert(t, loaded)
		})
	}
}

// TestLoadConfigFromFile_KafkaEnvNamePrecedence pins the order the three live name
// forms resolve in when more than one is set at once.
//
// Coverage of each form in isolation, above, proves only that none of them is dead.
// It says nothing about which value a deployment actually gets when two names
// disagree — and two names disagreeing is not exotic: a Helm chart supplying the
// conventional BLNK_-prefixed form over a base image's .env carrying the bare form
// produces it on the first upgrade. The order below is the one documented on
// eventStreamingEnvOverride:
//
//  1. BLNK_KAFKA_BROKERS       — the conventional prefixed name (overlay primary)
//  2. KAFKA_BROKERS            — the mandated bare name        (overlay alternate)
//  3. BLNK_KAFKA_KAFKA_BROKERS — the key the nested pass derives
//
// Every subtest sets values that are distinguishable from one another, so a wrong
// answer names the form that won rather than merely failing.
func TestLoadConfigFromFile_KafkaEnvNamePrecedence(t *testing.T) {
	cases := []struct {
		name   string
		env    map[string]string
		assert func(t *testing.T, loaded *Configuration)
	}{
		{
			name: "the conventional prefixed name beats the mandated bare name",
			env: map[string]string{
				"BLNK_KAFKA_BROKERS": "conventional:9092",
				"KAFKA_BROKERS":      "bare:9092",
			},
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.Brokers, []string{"conventional:9092"})
			},
		},
		{
			name: "the conventional prefixed name beats the nested-pass key",
			env: map[string]string{
				"BLNK_KAFKA_BROKERS":       "conventional:9092",
				"BLNK_KAFKA_KAFKA_BROKERS": "nested:9092",
			},
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.Brokers, []string{"conventional:9092"})
			},
		},
		{
			name: "the mandated bare name beats the nested-pass key",
			env: map[string]string{
				"KAFKA_BROKERS":            "bare:9092",
				"BLNK_KAFKA_KAFKA_BROKERS": "nested:9092",
			},
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.Brokers, []string{"bare:9092"})
			},
		},
		{
			name: "all three forms set resolves to the conventional prefixed name",
			env: map[string]string{
				"BLNK_KAFKA_BROKERS":       "conventional:9092",
				"KAFKA_BROKERS":            "bare:9092",
				"BLNK_KAFKA_KAFKA_BROKERS": "nested:9092",
			},
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.Brokers, []string{"conventional:9092"})
			},
		},
		{
			name: "the same order holds for an integer relay value",
			env: map[string]string{
				"BLNK_RELAY_MAX_RETRY_ATTEMPTS":       "2",
				"RELAY_MAX_RETRY_ATTEMPTS":            "3",
				"BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS": "4",
			},
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.MaxRetryAttempts != 2 {
					t.Errorf("Expected Relay.MaxRetryAttempts to be 2 from the conventional prefixed name, got %d",
						loaded.Relay.MaxRetryAttempts)
				}
			},
		},
		{
			name: "the bare name still wins over the nested key for an integer value",
			env: map[string]string{
				"RELAY_RETRY_BASE_BACKOFF_MS":            "250",
				"BLNK_RELAY_RELAY_RETRY_BASE_BACKOFF_MS": "500",
			},
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.RetryBaseBackoffMS != 250 {
					t.Errorf("Expected Relay.RetryBaseBackoffMS to be 250 from the bare name, got %d",
						loaded.Relay.RetryBaseBackoffMS)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearEventStreamingEnv(t)
			restoreConfigStore(t)

			// The cases below configure brokers, and a configured broker makes the
			// webhook deprecation window mandatory rather than advisory. Supplying it
			// here keeps each case testing the one thing it is about — which environment
			// name wins — instead of failing on an unrelated required setting.
			t.Setenv("WEBHOOK_DEPRECATION_SUNSET_DATE", testWindowSunset)

			configFile := writeTempEventConfigFile(t)
			for key, value := range tc.env {
				t.Setenv(key, value)
			}

			if err := loadConfigFromFile(configFile); err != nil {
				t.Fatalf("loadConfigFromFile failed: %v", err)
			}
			loaded, err := Fetch()
			if err != nil {
				t.Fatalf("Fetch failed: %v", err)
			}

			tc.assert(t, loaded)
		})
	}
}

// TestLoadConfigFromFile_KafkaEnvOverrideDoesNotErasePlainJSONConfiguration is the
// companion property to the precedence test: an environment name that is NOT set must
// leave the value blnk.json supplied exactly as it is.
//
// This is the failure mode an overlay invites. Copying a value struct rather than only
// its set fields would write a zero over every Kafka and relay setting a JSON-only
// deployment relies on — and it would do so silently, because zeros are then replaced
// by defaults and the result looks plausible. The Kubernetes ConfigMap ships a literal
// blnk.json, so JSON-only configuration is a real deployment shape rather than a
// theoretical one.
func TestLoadConfigFromFile_KafkaEnvOverrideDoesNotErasePlainJSONConfiguration(t *testing.T) {
	fromFile := eventStreamingBaseConfig()
	fromFile.Kafka = KafkaConfig{
		Brokers:           []string{"file-1:9092", "file-2:9092"},
		TopicPrefix:       "from-file",
		SASLAdminUser:     "file-admin",
		SASLAdminSecret:   "placeholder-not-a-real-secret",
		MinPartitions:     9,
		ReplicationFactor: 2,
	}
	fromFile.Relay = RelayConfig{MaxRetryAttempts: 4, RetryBaseBackoffMS: 750, RetryMaxBackoffMS: 20000}
	// Supplied from the FILE rather than the environment, which is the deployment shape
	// this test is about: the file configures brokers, and a configured broker makes the
	// deprecation window mandatory.
	fromFile.WebhookDeprecationSunsetDate = testWindowSunset

	t.Run("no environment variable set leaves every file value intact", func(t *testing.T) {
		clearEventStreamingEnv(t)
		restoreConfigStore(t)

		if err := loadConfigFromFile(writeTempConfigFile(t, fromFile)); err != nil {
			t.Fatalf("loadConfigFromFile failed: %v", err)
		}
		loaded, err := Fetch()
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}

		assertBrokerList(t, loaded.Kafka.Brokers, []string{"file-1:9092", "file-2:9092"})
		if loaded.Kafka.TopicPrefix != "from-file" {
			t.Errorf("Expected Kafka.TopicPrefix to stay 'from-file', got '%s'", loaded.Kafka.TopicPrefix)
		}
		if loaded.Kafka.SASLAdminUser != "file-admin" {
			t.Errorf("Expected Kafka.SASLAdminUser to stay 'file-admin', got '%s'", loaded.Kafka.SASLAdminUser)
		}
		if loaded.Kafka.SASLAdminSecret == "" {
			t.Error("Expected Kafka.SASLAdminSecret from the file to survive, got an empty value")
		}
		if loaded.Kafka.MinPartitions != 9 {
			t.Errorf("Expected Kafka.MinPartitions to stay 9, got %d", loaded.Kafka.MinPartitions)
		}
		if loaded.Kafka.ReplicationFactor != 2 {
			t.Errorf("Expected Kafka.ReplicationFactor to stay 2, got %d", loaded.Kafka.ReplicationFactor)
		}
		if loaded.Relay.MaxRetryAttempts != 4 {
			t.Errorf("Expected Relay.MaxRetryAttempts to stay 4, got %d", loaded.Relay.MaxRetryAttempts)
		}
		if loaded.Relay.RetryBaseBackoffMS != 750 {
			t.Errorf("Expected Relay.RetryBaseBackoffMS to stay 750, got %d", loaded.Relay.RetryBaseBackoffMS)
		}
		if loaded.Relay.RetryMaxBackoffMS != 20000 {
			t.Errorf("Expected Relay.RetryMaxBackoffMS to stay 20000, got %d", loaded.Relay.RetryMaxBackoffMS)
		}
	})

	t.Run("one environment variable overrides only its own field", func(t *testing.T) {
		clearEventStreamingEnv(t)
		restoreConfigStore(t)

		t.Setenv("BLNK_KAFKA_TOPIC_PREFIX", "from-env")

		if err := loadConfigFromFile(writeTempConfigFile(t, fromFile)); err != nil {
			t.Fatalf("loadConfigFromFile failed: %v", err)
		}
		loaded, err := Fetch()
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}

		if loaded.Kafka.TopicPrefix != "from-env" {
			t.Errorf("Expected Kafka.TopicPrefix to be 'from-env', got '%s'", loaded.Kafka.TopicPrefix)
		}
		assertBrokerList(t, loaded.Kafka.Brokers, []string{"file-1:9092", "file-2:9092"})
		if loaded.Kafka.MinPartitions != 9 {
			t.Errorf("Expected Kafka.MinPartitions to stay 9, got %d", loaded.Kafka.MinPartitions)
		}
		if loaded.Relay.MaxRetryAttempts != 4 {
			t.Errorf("Expected Relay.MaxRetryAttempts to stay 4, got %d", loaded.Relay.MaxRetryAttempts)
		}
	})

	t.Run("an explicitly empty broker list turns Kafka off", func(t *testing.T) {
		clearEventStreamingEnv(t)
		restoreConfigStore(t)

		// Emptying the variable is how an operator disables publishing, and it must
		// beat the file rather than being mistaken for "not configured".
		t.Setenv("BLNK_KAFKA_BROKERS", "")

		if err := loadConfigFromFile(writeTempConfigFile(t, fromFile)); err != nil {
			t.Fatalf("loadConfigFromFile failed: %v", err)
		}
		loaded, err := Fetch()
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}

		assertBrokerList(t, loaded.Kafka.Brokers, nil)
	})
}

// TestKafkaBrokersParsing verifies how a KAFKA_BROKERS value becomes a broker
// list. envconfig splits the comma-separated value but does not trim the pieces,
// so " b:9092" would otherwise reach the dialler as an unusable address;
// setKafkaDefaults runs the value through normalizeBrokers, which trims every
// entry and drops the empty ones produced by consecutive or trailing commas. Both
// behaviours are asserted here because both are load-bearing.
//
// The empty and unset cases are the graceful-degradation path and must yield no
// brokers and no error, exactly as the whitelist test above treats an empty value
// as nil rather than as a failure.
func TestKafkaBrokersParsing(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		omitEnv bool
		expect  []string
	}{
		{
			name:   "three brokers keep their configured order",
			raw:    "broker-1:9092,broker-2:9092,broker-3:9092",
			expect: []string{"broker-1:9092", "broker-2:9092", "broker-3:9092"},
		},
		{
			name:   "a single broker yields one entry",
			raw:    "broker-1:9092",
			expect: []string{"broker-1:9092"},
		},
		{
			name:   "surrounding whitespace is trimmed from every entry",
			raw:    " broker-1:9092 , broker-2:9092 ",
			expect: []string{"broker-1:9092", "broker-2:9092"},
		},
		{
			name:   "an explicitly empty value yields no brokers",
			raw:    "",
			expect: nil,
		},
		{
			name:   "only whitespace and commas yields no brokers",
			raw:    " , , ",
			expect: nil,
		},
		{
			name:    "an unset variable yields no brokers",
			omitEnv: true,
			expect:  nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearEventStreamingEnv(t)
			restoreConfigStore(t)

			configFile := writeTempEventConfigFileWithWindow(t)
			if !tc.omitEnv {
				t.Setenv("KAFKA_BROKERS", tc.raw)
			}

			if err := loadConfigFromFile(configFile); err != nil {
				t.Fatalf("loadConfigFromFile failed: %v", err)
			}
			loaded, err := Fetch()
			if err != nil {
				t.Fatalf("Fetch failed: %v", err)
			}

			assertBrokerList(t, loaded.Kafka.Brokers, tc.expect)
		})
	}
}

// TestResolveWebhookDeprecationWindow covers the dual-delivery window, whose surface is
// exactly ONE setting.
//
// # The two failure modes are deliberately treated differently
//
// A date that will not parse is FATAL. An operator stated a retirement instant and got
// it wrong, and the silent resolution would be "keep the legacy behaviour" — so a
// single mis-typed variable would cancel the retirement of the very transport this
// feature exists to replace, with nothing failing and nothing to notice.
//
// An ABSENT date depends on whether the deployment publishes, and the two cases are
// genuinely different rather than one rule with an exception.
//
// With NO brokers, it is accepted quietly. Nothing was mis-stated, there is no Kafka
// transport to migrate to and therefore no window to state, and AAP §0.7.2 requires a
// deployment to keep starting and serving as Kafka configuration is introduced. The
// legacy webhook path keeps behaving exactly as it did before this feature existed —
// which is not "dual delivery continues", because with no brokers there is no second
// transport for anything to be dual about.
//
// With brokers, it is REFUSED, and KAFKA_INSECURE_LOCAL_DEV is the only acknowledgement
// that lifts the refusal. This is the case TestResolveWebhookDeprecationWindow_AnUnusable
// WindowIsRefusedNeverInvented covers, and the reasoning is recorded there: it was once a
// warning that promised dual delivery would continue indefinitely, but
// blnk.WebhookSunsetPassed fails CLOSED on a publishing deployment with no usable window,
// so the promise and the behaviour pointed in opposite directions. An unset variable must
// not be able to preserve a deprecated transport silently, so the combination is rejected
// before any traffic is served.
//
// # There is only one input, so the window cannot be inconsistent
//
// The start is DERIVED as sunset minus WebhookDualDeliveryWindowDays and carries no
// environment variable, per the eight-variable R-10 contract. With one settable end
// there is no second value to disagree with, so "exactly 30 days" is a property of the
// arithmetic rather than a rule that has to be enforced — which is why the cases that
// used to assert a mismatched pair being refused are gone rather than relaxed.
func TestResolveWebhookDeprecationWindow(t *testing.T) {
	clearEventStreamingEnv(t)

	t.Run("a valid sunset alone is accepted and back-fills the window start", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.WebhookDeprecationSunsetDate = testWindowSunset

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.WebhookDeprecationSunsetDate != testWindowSunset {
			t.Errorf("Expected the sunset to be preserved as %q, got %q", testWindowSunset, cnf.WebhookDeprecationSunsetDate)
		}
		// The start is derived so that both ends of the window are available to
		// describe it, and it must land exactly one window before the sunset.
		if cnf.WebhookDeprecationStartDate != testWindowStart {
			t.Errorf("Expected the derived window start to be %q, got %q", testWindowStart, cnf.WebhookDeprecationStartDate)
		}
	})

	t.Run("a start present on the struct is overwritten by the derived value", func(t *testing.T) {
		// The field is output-only. A value that reached it from a configuration file, or
		// from a caller assembling the struct directly, must not be able to describe a
		// window of a different length than the code guarantees.
		cnf := eventStreamingBaseConfig()
		cnf.WebhookDeprecationSunsetDate = testWindowSunset
		cnf.WebhookDeprecationStartDate = "2001-01-01T00:00:00Z"

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if cnf.WebhookDeprecationStartDate == "2001-01-01T00:00:00Z" {
			t.Fatal("The window start was honoured as an input; it must always be derived")
		}
		if cnf.WebhookDeprecationStartDate != testWindowStart {
			t.Errorf("Expected the derived window start to be %q, got %q",
				testWindowStart, cnf.WebhookDeprecationStartDate)
		}

		// And the interval is computed rather than trusted, so a change to
		// WebhookDualDeliveryWindowDays cannot leave this test agreeing with itself.
		start, err := time.Parse(time.RFC3339, cnf.WebhookDeprecationStartDate)
		if err != nil {
			t.Fatalf("parsing the derived start: %v", err)
		}
		sunset, err := time.Parse(time.RFC3339, cnf.WebhookDeprecationSunsetDate)
		if err != nil {
			t.Fatalf("parsing the sunset: %v", err)
		}
		if got := sunset.Sub(start); got != WebhookDualDeliveryWindowDays*24*time.Hour {
			t.Errorf("Expected the window to be exactly %d days, got %s",
				WebhookDualDeliveryWindowDays, got)
		}
	})

	t.Run("a malformed date is refused rather than ignored", func(t *testing.T) {
		malformed := []string{
			"04/09/2026",
			"not-a-date",
			"2026-09-04",
			"2026-09-04T00:00:00",
			"2026-13-01T00:00:00Z",
		}

		for _, value := range malformed {
			t.Run(value, func(t *testing.T) {
				cnf := eventStreamingBaseConfig()
				cnf.WebhookDeprecationSunsetDate = value

				err := cnf.validateAndAddDefaults()
				if err == nil {
					t.Fatalf("Expected an error for the malformed sunset %q", value)
				}
				if !strings.Contains(err.Error(), "webhook_deprecation_sunset_date") {
					t.Errorf("Expected the error to name the field, got %q", err.Error())
				}
			})
		}

		// A malformed value on the DERIVED field cannot fail, because it is not read.
		// This is asserted rather than assumed: an operator or a stale configuration file
		// supplying nonsense for a value the code owns must not be able to fail the load.
		t.Run("a malformed value on the derived start cannot fail the load", func(t *testing.T) {
			cnf := eventStreamingBaseConfig()
			cnf.WebhookDeprecationSunsetDate = testWindowSunset
			cnf.WebhookDeprecationStartDate = "not-a-date"

			if err := cnf.validateAndAddDefaults(); err != nil {
				t.Fatalf("Expected the derived start to be ignored, got %v", err)
			}
			if cnf.WebhookDeprecationStartDate != testWindowStart {
				t.Errorf("Expected the start to be replaced with %q, got %q",
					testWindowStart, cnf.WebhookDeprecationStartDate)
			}
		})
	})

	// An absent window with brokers configured is REFUSED, through the whole
	// validateAndAddDefaults chain rather than only through the window resolver, because
	// the chain is what a load actually runs. Accepting it with a warning is what let
	// configuration promise indefinite dual delivery while the runtime treated the same
	// state as already past the sunset — see resolveWebhookDeprecationWindow.
	//
	// The refusal must NAME the variable and STATE the consequence, so an operator can act
	// on it without reading the source.
	t.Run("an absent window is refused once kafka brokers are configured", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.Brokers = []string{"broker-1:9092"}

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected brokers with no window to be refused; accepting the configuration " +
				"is what left dual delivery and the 410 guard disagreeing about the same date")
		}
		if !strings.Contains(err.Error(), "webhook_deprecation_sunset_date") {
			t.Errorf("Expected the error to name the missing setting, got %q", err.Error())
		}
		if !strings.Contains(err.Error(), "WEBHOOK_DEPRECATION_SUNSET_DATE") {
			t.Errorf("Expected the error to name the environment variable to set, got %q", err.Error())
		}
		if !strings.Contains(err.Error(), "410 Gone") {
			t.Errorf("Expected the error to state the consequence of leaving it unset, got %q", err.Error())
		}

		if cnf.WebhookDeprecationSunsetDate != "" || cnf.WebhookDeprecationStartDate != "" {
			t.Errorf("Expected both ends to stay empty, got start %q sunset %q",
				cnf.WebhookDeprecationStartDate, cnf.WebhookDeprecationSunsetDate)
		}
	})

	// ... and the case that must keep working: no Kafka, no window, no error. This is
	// what every existing deployment and every existing test in this repository looks
	// like, and it is the graceful-degradation contract.
	t.Run("an absent window is accepted when no broker is configured", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := eventStreamingBaseConfig()

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.WebhookDeprecationSunsetDate != "" {
			t.Errorf("Expected the sunset to stay empty, got %q", cnf.WebhookDeprecationSunsetDate)
		}
		if cnf.WebhookDeprecationStartDate != "" {
			t.Errorf("Expected the window start to stay empty, got %q", cnf.WebhookDeprecationStartDate)
		}
		for _, entry := range hook.AllEntries() {
			if entry.Level == logrus.WarnLevel && strings.Contains(strings.ToLower(entry.Message), "sunset") {
				t.Errorf("Did not expect a sunset warning with no Kafka configured, got %q", entry.Message)
			}
		}
	})

	// Whitespace is trimmed before parsing, so a trailing newline from an environment
	// file cannot turn a correct date into a fatal error.
	t.Run("surrounding whitespace on a date is tolerated", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.WebhookDeprecationSunsetDate = "  " + testWindowSunset + "\n"

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.WebhookDeprecationSunsetDate != testWindowSunset {
			t.Errorf("Expected the sunset to be normalised to %q, got %q", testWindowSunset, cnf.WebhookDeprecationSunsetDate)
		}
	})

	// A date written with an offset and the same instant written as Z must be
	// indistinguishable, and the stored form must be canonical UTC.
	t.Run("an offset date is normalised to utc", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.WebhookDeprecationSunsetDate = "2026-09-04T01:00:00+01:00"

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.WebhookDeprecationSunsetDate != testWindowSunset {
			t.Errorf("Expected the sunset to be normalised to %q, got %q", testWindowSunset, cnf.WebhookDeprecationSunsetDate)
		}
	})

	// The required-field surface is unchanged: the data source and Redis DNS values
	// remain the only two, which is what keeps every existing configuration literal in
	// the suite valid.
	t.Run("no kafka or window field is a required field", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		if cnf.WebhookDeprecationSunsetDate != "" || len(cnf.Kafka.Brokers) != 0 {
			t.Fatalf("Expected the base fixture to leave the event-streaming fields unset")
		}

		if err := cnf.validateRequiredFields(); err != nil {
			t.Errorf("Expected validateRequiredFields to accept a config with no event-streaming values, got %v", err)
		}
	})
}

// TestValidateAndAddDefaults_RelayWindowWarnings covers what happens to an unusable relay
// retry window. Relay tuning is an operational knob: a bad value must degrade event delivery,
// never stop the server from starting, so every finding is a warning and
// validateAndAddDefaults still returns nil in every case below.
//
// # A negative value is NORMALISED, not merely reported
//
// It used to be reported and left in place, and the report was wrong about the outcome: the
// warning said "retries will not be delayed" while each consumer went on to substitute a
// value of its own. For RELAY_MAX_RETRY_ATTEMPTS the two consumers substituted DIFFERENT
// values — the row was stamped with the default of five by eventMaxAttempts while
// newRelayRetryPolicy allowed the ceiling of eight — so one negative produced two effective
// budgets and no log line named either. So each case below asserts BOTH halves of the fix:
// the warning names the variable and the value applied, and the effective value really is
// that one.
//
// The check runs after the defaults are applied, so it inspects effective values. That is why
// each case supplies non-zero values for the fields it is exercising — a zero means "unset"
// and is replaced by its default silently, with no warning to assert on.
func TestValidateAndAddDefaults_RelayWindowWarnings(t *testing.T) {
	clearEventStreamingEnv(t)

	const (
		baseAboveCapWarning  = "relay retry_base_backoff_ms exceeds retry_max_backoff_ms"
		negativeBaseWarning  = "relay retry_base_backoff_ms is negative"
		negativeCapWarning   = "relay retry_max_backoff_ms is negative"
		negativeAttemptsWarn = "relay max_retry_attempts is negative"
	)

	cases := []struct {
		name      string
		relay     RelayConfig
		want      []string
		unwanted  []string
		effective *RelayConfig
	}{
		{
			name:     "a base backoff above the cap warns",
			relay:    RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 40000, RetryMaxBackoffMS: 30000},
			want:     []string{baseAboveCapWarning},
			unwanted: []string{negativeBaseWarning, negativeCapWarning, negativeAttemptsWarn},
			// NEITHER value is corrected: both are individually valid, so the operator's
			// slower-but-capped schedule is honoured rather than second-guessed.
			effective: &RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 40000, RetryMaxBackoffMS: 30000},
		},
		{
			name:      "a negative base backoff warns and the default is applied",
			relay:     RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: -1, RetryMaxBackoffMS: 30000},
			want:      []string{negativeBaseWarning},
			unwanted:  []string{baseAboveCapWarning, negativeCapWarning, negativeAttemptsWarn},
			effective: &RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 1000, RetryMaxBackoffMS: 30000},
		},
		{
			// The inverted-window finding is NOT expected here any more, and its absence is
			// the point: the cap is normalised to its default of 30000 before the window is
			// inspected, so the base of 1000 no longer exceeds it. Reporting an inversion
			// against a value that had already been replaced was a warning about a state
			// that did not exist.
			name:      "a negative cap warns and the default is applied",
			relay:     RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 1000, RetryMaxBackoffMS: -1},
			want:      []string{negativeCapWarning},
			unwanted:  []string{negativeBaseWarning, negativeAttemptsWarn, baseAboveCapWarning},
			effective: &RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 1000, RetryMaxBackoffMS: 30000},
		},
		{
			// THE FINDING. A negative budget now resolves to ONE value, named in the log,
			// which is what stops eventMaxAttempts and newRelayRetryPolicy disagreeing about
			// how many attempts an event gets.
			name:      "a negative retry count warns and the default is applied",
			relay:     RelayConfig{MaxRetryAttempts: -1, RetryBaseBackoffMS: 1000, RetryMaxBackoffMS: 30000},
			want:      []string{negativeAttemptsWarn},
			unwanted:  []string{baseAboveCapWarning, negativeBaseWarning, negativeCapWarning},
			effective: &RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 1000, RetryMaxBackoffMS: 30000},
		},
		{
			name:      "the default window warns about nothing",
			relay:     RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 1000, RetryMaxBackoffMS: 30000},
			want:      nil,
			unwanted:  []string{baseAboveCapWarning, negativeBaseWarning, negativeCapWarning, negativeAttemptsWarn},
			effective: &RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 1000, RetryMaxBackoffMS: 30000},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hook := logtest.NewGlobal()
			defer hook.Reset()

			cnf := eventStreamingBaseConfig()
			cnf.Relay = tc.relay

			if err := cnf.validateAndAddDefaults(); err != nil {
				t.Fatalf("Expected a relay tuning problem to warn, not to fail; got %v", err)
			}

			for _, warning := range tc.want {
				if !warnedAbout(hook, warning) {
					t.Errorf("Expected a warning containing %q", warning)
				}
			}
			for _, warning := range tc.unwanted {
				if warnedAbout(hook, warning) {
					t.Errorf("Did not expect a warning containing %q", warning)
				}
			}

			// THE OTHER HALF. A warning that names an applied value is only true if that
			// value is the one every consumer will read, so the effective configuration is
			// asserted alongside the log line rather than instead of it.
			if tc.effective != nil {
				if cnf.Relay.MaxRetryAttempts != tc.effective.MaxRetryAttempts {
					t.Errorf("Expected an effective max_retry_attempts of %d, got %d",
						tc.effective.MaxRetryAttempts, cnf.Relay.MaxRetryAttempts)
				}
				if cnf.Relay.RetryBaseBackoffMS != tc.effective.RetryBaseBackoffMS {
					t.Errorf("Expected an effective retry_base_backoff_ms of %d, got %d",
						tc.effective.RetryBaseBackoffMS, cnf.Relay.RetryBaseBackoffMS)
				}
				if cnf.Relay.RetryMaxBackoffMS != tc.effective.RetryMaxBackoffMS {
					t.Errorf("Expected an effective retry_max_backoff_ms of %d, got %d",
						tc.effective.RetryMaxBackoffMS, cnf.Relay.RetryMaxBackoffMS)
				}
			}
		})
	}
}

// TestMockConfig_KafkaAndRelayDefaultsApplied proves the new configuration surface
// is safe for every test file that injects configuration through MockConfig.
//
// MockConfig fails closed: on a validation error it logs and returns without
// storing, leaving whatever a previous test stored in place. Fetch would then hand
// back that stale configuration and a naive assertion on the Kafka defaults could
// still pass. Asserting the project name first is what makes this test honest — it
// is the proof that this configuration was accepted and stored.
func TestMockConfig_KafkaAndRelayDefaultsApplied(t *testing.T) {
	clearEventStreamingEnv(t)
	restoreConfigStore(t)

	const projectName = "MockConfig Event Streaming"
	MockConfig(&Configuration{
		ProjectName: projectName,
		DataSource:  DataSourceConfig{Dns: "mock-dns"},
		Redis:       RedisConfig{Dns: "localhost:6379"},
	})

	loaded, err := Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if loaded == nil {
		t.Fatalf("Expected Fetch to return a configuration, got nil")
	}
	if loaded.ProjectName != projectName {
		t.Fatalf("Expected ProjectName to be '%s', got '%s' — MockConfig rejected the configuration", projectName, loaded.ProjectName)
	}

	if loaded.Kafka.TopicPrefix != "blnk" {
		t.Errorf("Expected Kafka.TopicPrefix to be 'blnk', got '%s'", loaded.Kafka.TopicPrefix)
	}
	if loaded.Kafka.MinPartitions != 6 {
		t.Errorf("Expected Kafka.MinPartitions to be 6, got %d", loaded.Kafka.MinPartitions)
	}
	if loaded.Relay.MaxRetryAttempts != 5 {
		t.Errorf("Expected Relay.MaxRetryAttempts to be 5, got %d", loaded.Relay.MaxRetryAttempts)
	}
	if loaded.Relay.RetryBaseBackoffMS != 1000 {
		t.Errorf("Expected Relay.RetryBaseBackoffMS to be 1000, got %d", loaded.Relay.RetryBaseBackoffMS)
	}
	if loaded.Relay.RetryMaxBackoffMS != 30000 {
		t.Errorf("Expected Relay.RetryMaxBackoffMS to be 30000, got %d", loaded.Relay.RetryMaxBackoffMS)
	}
	if loaded.Relay.SubscriberMetricsBudget != 200 {
		t.Errorf("Expected Relay.SubscriberMetricsBudget to be 200, got %d", loaded.Relay.SubscriberMetricsBudget)
	}

	// A bare fixture stays Kafka-less, which is what selects the no-op publisher.
	assertBrokerList(t, loaded.Kafka.Brokers, nil)
}

// TestSetRelayDefaults_SubscriberMetricsBudgetRefusesAnUnmeasurableValue pins the two
// values that would silently switch consumer-lag measurement off.
//
// # Why a zero or negative budget cannot be honoured
//
// The budget bounds how many subscribers ONE collection tick examines. Read literally, zero
// examines none and a negative value examines none — and a subscriber the collector does not
// examine has NO blnk_kafka_consumer_lag series, so SubscriberConsumerLagHigh cannot fire for
// it however far behind it falls. Honouring either value would therefore reach the exact
// condition blnk_kafka_subscribers_unmeasured exists to expose, by configuration rather than by
// scale, and it would do so while reporting no error at all.
//
// Zero additionally arrives by accident: an operator who writes RELAY_SUBSCRIBER_METRICS_BUDGET=
// with no value, or a ConfigMap key whose value was templated away, both produce it. There is no
// deployment for which "measure nobody" is the intent, so the default is substituted in both
// cases and a negative value — which can only be a mistake — is additionally warned about.
func TestSetRelayDefaults_SubscriberMetricsBudgetRefusesAnUnmeasurableValue(t *testing.T) {
	clearEventStreamingEnv(t)

	for _, tc := range []struct {
		name       string
		configured int
		wantWarn   bool
	}{
		{name: "unset falls back to the default", configured: 0, wantWarn: false},
		{name: "explicit zero falls back to the default", configured: 0, wantWarn: false},
		{name: "a negative budget is refused and warned about", configured: -1, wantWarn: true},
		{name: "a large negative budget is refused too", configured: -5000, wantWarn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := logtest.NewGlobal()
			defer hook.Reset()

			cnf := eventStreamingBaseConfig()
			cnf.Relay.SubscriberMetricsBudget = tc.configured

			if err := cnf.validateAndAddDefaults(); err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if cnf.Relay.SubscriberMetricsBudget != 200 {
				t.Errorf("Expected the budget to resolve to the default 200, got %d",
					cnf.Relay.SubscriberMetricsBudget)
			}

			warned := false
			for _, entry := range hook.AllEntries() {
				if strings.Contains(entry.Message, "subscriber_metrics_budget is negative") {
					warned = true

					// The variable name has to be on the entry, because that is what an
					// operator greps for to find the key they need to change.
					if entry.Data["variable"] != "RELAY_SUBSCRIBER_METRICS_BUDGET" {
						t.Errorf("Expected the warning to name the variable, got %v", entry.Data["variable"])
					}
				}
			}
			if warned != tc.wantWarn {
				t.Errorf("Expected warning=%v, got %v", tc.wantWarn, warned)
			}
		})
	}

	t.Run("a positive budget below the ceiling is honoured verbatim", func(t *testing.T) {
		// Honoured, not adjusted: the only cost of a larger budget within the supported range
		// is broker round trips an operator has chosen to spend, and quietly capping coverage
		// is the failure the whole variable exists to prevent.
		cnf := eventStreamingBaseConfig()
		cnf.Relay.SubscriberMetricsBudget = 2500

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.Relay.SubscriberMetricsBudget != 2500 {
			t.Errorf("Expected 2500 to be honoured verbatim, got %d", cnf.Relay.SubscriberMetricsBudget)
		}
		if cnf.Kafka.MetricsSubscriberBudget != 2500 {
			t.Errorf("Expected the other spelling of the same knob to agree, got %d",
				cnf.Kafka.MetricsSubscriberBudget)
		}
	})

	t.Run("a budget beyond the supported ceiling is clamped and SAID SO", func(t *testing.T) {
		// The ceiling is real rather than cautious: every measured subscriber-topic pair is a
		// retained gauge series, so an unbounded budget makes one collection tick unbounded in
		// cardinality as well as in duration — and a metrics pipeline that falls over takes
		// every alert this feature added with it.
		//
		// What answers the objection to capping is that it is LOUD. The warning names the
		// configured value and the applied one, so coverage is never capped silently, which
		// is the only property that made a ceiling unacceptable.
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := eventStreamingBaseConfig()
		cnf.Relay.SubscriberMetricsBudget = 25000

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.Relay.SubscriberMetricsBudget != MaxMetricsSubscriberBudget {
			t.Errorf("Expected the budget to be clamped to %d, got %d",
				MaxMetricsSubscriberBudget, cnf.Relay.SubscriberMetricsBudget)
		}
		if cnf.Kafka.MetricsSubscriberBudget != MaxMetricsSubscriberBudget {
			t.Errorf("Expected both spellings to hold the clamped value, got %d",
				cnf.Kafka.MetricsSubscriberBudget)
		}

		clamped := false
		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, "above the supported ceiling") {
				clamped = true
			}
		}
		if !clamped {
			t.Error("Expected the clamp to be reported, so coverage is never capped silently")
		}
	})
}

// TestKafkaConfig_SASLAdminCredentialsContract pins the SINGLE reading of the
// administrative SASL credential that the publisher, the admin client and both
// provisioning scripts share.
//
// The contract is deliberately three-valued rather than two-valued, and each value has
// to hold for a different reason:
//
//   - BOTH EMPTY is not an error. A broker with a plaintext listener is a supported
//     deployment, and the shipped .env.example leaves both keys empty.
//   - BOTH SET is SASL/SCRAM as the named principal.
//   - EXACTLY ONE SET has no honest interpretation and must be refused. A username
//     without a secret cannot authenticate. A secret without a username is the
//     dangerous half: every component used to ignore it silently and connect
//     anonymously while looking configured.
//
// Trimming is asserted because a value arriving from a Kubernetes secret or a
// hand-edited .env routinely carries a trailing newline, and whitespace must read as
// absence rather than as a principal nobody created.
//
// The refusal message must name the ENVIRONMENT VARIABLE, because that is the only one
// of variable, struct field and role that an operator can act on.
func TestKafkaConfig_SASLAdminCredentialsContract(t *testing.T) {
	// Obviously fake: this file must never carry a value that could be mistaken for a
	// real credential.
	const secret = "placeholder-not-a-real-secret"

	cases := []struct {
		name        string
		user        string
		secret      string
		wantUser    string
		wantSecret  string
		wantEnabled bool
		wantErrPart string
	}{
		{
			name:        "both empty selects no SASL and is valid",
			wantEnabled: false,
		},
		{
			name:        "both set enables SASL and hands back the pair",
			user:        "blnk-test-admin",
			secret:      secret,
			wantUser:    "blnk-test-admin",
			wantSecret:  secret,
			wantEnabled: true,
		},
		{
			name:        "a user without a secret is refused and names the secret",
			user:        "blnk-test-admin",
			wantEnabled: false,
			wantErrPart: "KAFKA_SASL_ADMIN_SECRET",
		},
		{
			name:        "a secret without a user is refused and names the user",
			secret:      secret,
			wantEnabled: false,
			wantErrPart: "KAFKA_SASL_ADMIN_USER",
		},
		{
			name:        "whitespace-only values read as absence",
			user:        "  \t ",
			secret:      " \n ",
			wantEnabled: false,
		},
		{
			name:        "surrounding whitespace is trimmed off a real pair",
			user:        "  blnk-test-admin\n",
			secret:      " " + secret + "\n",
			wantUser:    "blnk-test-admin",
			wantSecret:  secret,
			wantEnabled: true,
		},
		{
			name:        "whitespace around a user with no secret is still a refusal",
			user:        " blnk-test-admin ",
			secret:      "   ",
			wantEnabled: false,
			wantErrPart: "KAFKA_SASL_ADMIN_SECRET",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kafka := KafkaConfig{SASLAdminUser: tc.user, SASLAdminSecret: tc.secret}

			user, resolvedSecret, enabled := kafka.SASLAdminCredentials()
			if enabled != tc.wantEnabled {
				t.Errorf("Expected enabled to be %t, got %t", tc.wantEnabled, enabled)
			}
			if user != tc.wantUser {
				t.Errorf("Expected user '%s', got '%s'", tc.wantUser, user)
			}
			if resolvedSecret != tc.wantSecret {
				t.Errorf("Expected the resolved secret to be %d characters, got %d",
					len(tc.wantSecret), len(resolvedSecret))
			}

			err := kafka.ValidateSASLAdminCredentials()
			if tc.wantErrPart == "" {
				if err != nil {
					t.Errorf("Expected no error, got '%v'", err)
				}

				return
			}

			if err == nil {
				t.Fatalf("Expected an error naming %s, got none", tc.wantErrPart)
			}
			if !strings.Contains(err.Error(), tc.wantErrPart) {
				t.Errorf("Expected the error to name %s, got '%v'", tc.wantErrPart, err)
			}
			// The secret is never echoed, whichever half is missing.
			if tc.secret != "" && strings.Contains(err.Error(), secret) {
				t.Errorf("The error must not echo the secret, got '%v'", err)
			}
		})
	}
}

// TestValidateSASLPair_NamesTheRightVariablesForEachRole covers the producer arm of the
// same validator.
//
// The two roles configure two different pairs of variables, and an error that named the
// wrong pair would send an operator to change a value that was already correct. The role
// word is asserted as well, because for the producer the variable names alone
// (KAFKA_SASL_USER, KAFKA_SASL_SECRET) do not say which transport is affected.
func TestValidateSASLPair_NamesTheRightVariablesForEachRole(t *testing.T) {
	const secret = "placeholder-not-a-real-secret"

	if err := ValidateSASLPair("producer", "", ""); err != nil {
		t.Errorf("both empty is 'no SASL' and must be valid, got '%v'", err)
	}
	if err := ValidateSASLPair("producer", "blnk-producer", secret); err != nil {
		t.Errorf("both set must be valid, got '%v'", err)
	}

	userOnly := ValidateSASLPair("producer", "blnk-producer", "")
	if userOnly == nil {
		t.Fatal("a producer user without a secret must be refused")
	}
	for _, want := range []string{"producer", "KAFKA_SASL_SECRET"} {
		if !strings.Contains(userOnly.Error(), want) {
			t.Errorf("Expected the error to contain '%s', got '%v'", want, userOnly)
		}
	}
	if strings.Contains(userOnly.Error(), "KAFKA_SASL_ADMIN") {
		t.Errorf("the producer arm must not name the administrative variables, got '%v'", userOnly)
	}

	secretOnly := ValidateSASLPair("producer", "", secret)
	if secretOnly == nil {
		t.Fatal("a producer secret without a user must be refused")
	}
	if !strings.Contains(secretOnly.Error(), "KAFKA_SASL_USER") {
		t.Errorf("Expected the error to name KAFKA_SASL_USER, got '%v'", secretOnly)
	}
	if strings.Contains(secretOnly.Error(), secret) {
		t.Errorf("the error must not echo the secret, got '%v'", secretOnly)
	}
}

// TestLoadConfigFromFile_BareNameParseErrorNamesTheVariableThatWasSet closes the gap
// between the variable an operator set and the variable the failure named.
//
// envconfig derives a nested field's primary key by accumulating the prefix through every
// enclosing struct and treats the tag literal as an alternate, then always reports the
// PRIMARY in its ParseError. So setting the mandated bare RELAY_MAX_RETRY_ATTEMPTS=five
// used to fail with "assigning BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS to MaxRetryAttempts" —
// a name that appears nowhere in the operator's configuration and that searching for it
// will not find.
//
// The load must still FAIL, and fail for the same values it always did. What changes is
// only which name leads the message. Both are asserted, and the original wording is
// asserted to survive so no diagnostic detail is traded away for the better name.
func TestLoadConfigFromFile_BareNameParseErrorNamesTheVariableThatWasSet(t *testing.T) {
	cases := []struct {
		name       string
		key        string
		value      string
		nestedName string
	}{
		{
			name:       "the retry attempt count",
			key:        "RELAY_MAX_RETRY_ATTEMPTS",
			value:      "five",
			nestedName: "BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS",
		},
		{
			name:       "a backoff bound",
			key:        "RELAY_RETRY_BASE_BACKOFF_MS",
			value:      "1s",
			nestedName: "BLNK_RELAY_RELAY_RETRY_BASE_BACKOFF_MS",
		},
		{
			name:       "the partition count",
			key:        "KAFKA_MIN_PARTITIONS",
			value:      "six",
			nestedName: "BLNK_KAFKA_KAFKA_MIN_PARTITIONS",
		},
		{
			// Two structs deep: KafkaConfig.TLS. The accumulated primary doubles the
			// whole KAFKA_TLS segment, which is the case a single-segment recovery
			// would have missed.
			name:       "a doubly nested tls switch",
			key:        "KAFKA_TLS_ENABLED",
			value:      "yeah",
			nestedName: "BLNK_KAFKA_TLS_KAFKA_TLS_ENABLED",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearEventStreamingEnv(t)
			restoreConfigStore(t)

			configFile := writeTempEventConfigFileWithWindow(t)
			t.Setenv(tc.key, tc.value)

			err := loadConfigFromFile(configFile)
			if err == nil {
				t.Fatalf("Expected loadConfigFromFile to fail for %s=%q", tc.key, tc.value)
			}

			message := err.Error()
			if !strings.HasPrefix(message, tc.key+" ") {
				t.Errorf("Expected the error to LEAD with %s, got %q", tc.key, message)
			}
			if !strings.Contains(message, tc.value) {
				t.Errorf("Expected the error to quote the offending value %q, got %q", tc.value, message)
			}
			if !strings.Contains(message, tc.nestedName) {
				t.Errorf("Expected the nested name %s to be retained for reference, got %q",
					tc.nestedName, message)
			}
			if !strings.Contains(message, "envconfig.Process") {
				t.Errorf("Expected envconfig's own wording to be preserved by wrapping, got %q", message)
			}
		})
	}

	t.Run("the nested name is left alone when it is the variable that was set", func(t *testing.T) {
		clearEventStreamingEnv(t)
		restoreConfigStore(t)

		configFile := writeTempEventConfigFileWithWindow(t)
		t.Setenv("BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS", "five")

		err := loadConfigFromFile(configFile)
		if err == nil {
			t.Fatal("Expected loadConfigFromFile to fail")
		}
		if !strings.HasPrefix(err.Error(), "envconfig.Process") {
			t.Errorf("Expected envconfig's unaltered message when it already names the right variable, got %q",
				err.Error())
		}
	})

	t.Run("a valid bare value is still applied", func(t *testing.T) {
		clearEventStreamingEnv(t)
		restoreConfigStore(t)

		configFile := writeTempEventConfigFileWithWindow(t)
		t.Setenv("RELAY_MAX_RETRY_ATTEMPTS", "4")

		if err := loadConfigFromFile(configFile); err != nil {
			t.Fatalf("Expected a valid value to load, got %v", err)
		}

		cnf, err := Fetch()
		if err != nil {
			t.Fatalf("Unable to fetch the loaded configuration: %v", err)
		}
		if cnf.Relay.MaxRetryAttempts != 4 {
			t.Errorf("Expected MaxRetryAttempts 4, got %d", cnf.Relay.MaxRetryAttempts)
		}
	})
}

// TestBareEnvNameFrom covers the recovery in isolation, including what it must REFUSE.
//
// Returning a wrong bare name would be worse than returning none: the message would then
// confidently name a variable the operator did not set. Every non-matching shape must
// therefore yield the empty string so the caller falls back to envconfig's own wording.
func TestBareEnvNameFrom(t *testing.T) {
	cases := []struct {
		key  string
		want string
	}{
		{key: "BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS", want: "RELAY_MAX_RETRY_ATTEMPTS"},
		{key: "BLNK_KAFKA_KAFKA_MIN_PARTITIONS", want: "KAFKA_MIN_PARTITIONS"},
		{key: "BLNK_KAFKA_KAFKA_BROKERS", want: "KAFKA_BROKERS"},
		{key: "BLNK_KAFKA_TLS_KAFKA_TLS_ENABLED", want: "KAFKA_TLS_ENABLED"},
		{key: "BLNK_KAFKA_TLS_KAFKA_TLS_INSECURE_SKIP_VERIFY", want: "KAFKA_TLS_INSECURE_SKIP_VERIFY"},

		// Shapes that must NOT be described by a guess.
		{key: "BLNK_SERVER_BLNK_SERVER_MAX_UPLOAD_SIZE_MB", want: ""},
		{key: "BLNK_WEBHOOK_DEPRECATION_SUNSET_DATE", want: ""},
		{key: "KAFKA_MIN_PARTITIONS", want: ""},
		{key: "BLNK_", want: ""},
		{key: "BLNK_KAFKA", want: ""},
		{key: "", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			if got := bareEnvNameFrom(tc.key); got != tc.want {
				t.Errorf("bareEnvNameFrom(%q) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

// TestValidateAndAddDefaults_KafkaGeometryWarnings covers the load-time diagnostics for
// GEOMETRY values that are silently corrected later on.
//
// A negative partition count or replication factor is raised to a usable value by topic
// assurance, at the moment Kafka is first used — which for a deployment with no event
// traffic in flight can be long after start-up and a long way from the variable that
// caused it. Naming it at load turns a silent correction into a line in the boot log.
//
// These stay WARNINGS, and the distinction from the topic prefix is deliberate rather than
// inconsistent. A corrected geometry still delivers every event: six partitions instead of
// minus four is a different shape, not a lost message. An illegal PREFIX loses events
// outright — the rows are captured naming a topic the broker will never create, and their
// dead-letter names are illegal too — so it is fatal, and
// TestValidateKafkaTopicPrefix_RefusesAPrefixThatCannotComposeALegalTopicName owns it.
func TestValidateAndAddDefaults_KafkaGeometryWarnings(t *testing.T) {
	const (
		negativePartitionsWarning = "KAFKA_MIN_PARTITIONS is negative"
		negativeReplicationWarn   = "KAFKA_REPLICATION_FACTOR is negative"
	)

	cases := []struct {
		name     string
		kafka    KafkaConfig
		want     []string
		unwanted []string
	}{
		{
			name:     "a negative partition count warns",
			kafka:    KafkaConfig{MinPartitions: -4},
			want:     []string{negativePartitionsWarning},
			unwanted: []string{negativeReplicationWarn},
		},
		{
			name:     "a negative replication factor warns",
			kafka:    KafkaConfig{ReplicationFactor: -1},
			want:     []string{negativeReplicationWarn},
			unwanted: []string{negativePartitionsWarning},
		},
		{
			// Trimmed by the topic composer and by the prefix validation, so there is
			// nothing to report about something that is already handled correctly.
			name:     "surrounding whitespace and dots are not reported",
			kafka:    KafkaConfig{TopicPrefix: "  .blnk. \n"},
			want:     nil,
			unwanted: []string{negativePartitionsWarning, negativeReplicationWarn},
		},
		{
			name:     "a legal prefix with every permitted character warns about nothing",
			kafka:    KafkaConfig{TopicPrefix: "blnk-2_prod.eu"},
			want:     nil,
			unwanted: []string{negativePartitionsWarning, negativeReplicationWarn},
		},
		{
			name:     "an unset geometry warns about nothing",
			kafka:    KafkaConfig{},
			want:     nil,
			unwanted: []string{negativePartitionsWarning, negativeReplicationWarn},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearEventStreamingEnv(t)

			hook := logtest.NewGlobal()
			defer hook.Reset()

			cnf := eventStreamingBaseConfig()
			cnf.Kafka = tc.kafka

			if err := cnf.validateAndAddDefaults(); err != nil {
				t.Fatalf("Expected a Kafka geometry problem to warn, not to fail; got %v", err)
			}

			for _, warning := range tc.want {
				if !warnedAbout(hook, warning) {
					t.Errorf("Expected a warning containing %q", warning)
				}
			}
			for _, warning := range tc.unwanted {
				if warnedAbout(hook, warning) {
					t.Errorf("Did not expect a warning containing %q", warning)
				}
			}
		})
	}

	t.Run("a negative value is reported but never becomes the value in effect", func(t *testing.T) {
		clearEventStreamingEnv(t)

		cnf := eventStreamingBaseConfig()
		cnf.Kafka = KafkaConfig{MinPartitions: -4, ReplicationFactor: -1}

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected the load to succeed, got %v", err)
		}

		// The defaults do not replace a negative value here — zero alone means unset —
		// so the warning is the ONLY signal at load time, which is exactly why it had
		// to be added. The floor is applied where topics are provisioned.
		if cnf.Kafka.MinPartitions != -4 {
			t.Errorf("Expected the configured value to be preserved for the warning to describe, got %d",
				cnf.Kafka.MinPartitions)
		}
	})
}

// TestValidateKafkaTopicPrefix_RefusesAPrefixThatCannotComposeALegalTopicName is the
// configuration-load half of the fix for the prefix that only warned.
//
// The behaviour it replaces was the worst of the three available: the illegal value was
// reported and then USED, so producers kept writing outbox rows naming a topic the broker
// would never create. Every one of those rows exhausted its retry budget and then failed
// its dead-letter write too, because the dead-letter name was composed from the same
// illegal prefix — a ledger accepting mutations and silently notifying nobody. The warning
// also interpolated the configured value straight into a log message, which is a
// log-injection sink reachable by anyone who can set an environment variable.
//
// So the load now fails, and these are the cases it must fail on and the cases it must
// still accept.
func TestValidateKafkaTopicPrefix_RefusesAPrefixThatCannotComposeALegalTopicName(t *testing.T) {
	t.Run("an interior illegal character is fatal and is reported as a quoted rune", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = "blnk prod"

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected an interior space to fail the configuration load")
		}
		if !strings.Contains(err.Error(), "KAFKA_TOPIC_PREFIX") {
			t.Errorf("Expected the error to name the variable, got %q", err.Error())
		}
		if !strings.Contains(err.Error(), `' '`) {
			t.Errorf("Expected the offending character quoted, got %q", err.Error())
		}
	})

	t.Run("a control character is quoted rather than emitted", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = "blnk\nfake-log-record"

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected a newline in the prefix to fail the configuration load")
		}
		if strings.Contains(err.Error(), "\n") {
			t.Errorf("The error must not carry the raw control character: %q", err.Error())
		}
		if !strings.Contains(err.Error(), `'\n'`) {
			t.Errorf("Expected the newline quoted, got %q", err.Error())
		}
	})

	t.Run("a prefix too long to compose the longest topic name is fatal", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = strings.Repeat("a", MaxKafkaTopicPrefixLength+1)

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected an over-long prefix to fail the configuration load")
		}
		if !strings.Contains(err.Error(), "KAFKA_TOPIC_PREFIX") {
			t.Errorf("Expected the error to name the variable, got %q", err.Error())
		}
	})

	t.Run("the longest legal prefix is accepted", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = strings.Repeat("a", MaxKafkaTopicPrefixLength)

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected the longest legal prefix to be accepted, got %v", err)
		}
	})

	t.Run("legal separators and mixed case are accepted unchanged", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = "Blnk_prod-eu.1"

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected a legal prefix to be accepted, got %v", err)
		}
		if cnf.Kafka.TopicPrefix != "Blnk_prod-eu.1" {
			t.Errorf("A legal prefix must survive validation unchanged, got %q", cnf.Kafka.TopicPrefix)
		}
	})

	t.Run("the realistic accidents are normalised rather than refused", func(t *testing.T) {
		for _, configured := range []string{" blnk\n", ".blnk.", "\tblnk\r\n"} {
			cnf := kafkaEnabledConfig("localhost:9092")
			cnf.Kafka.TopicPrefix = configured

			if err := cnf.validateAndAddDefaults(); err != nil {
				t.Fatalf("Expected %q to be normalised rather than refused, got %v", configured, err)
			}
			if cnf.Kafka.TopicPrefix != "blnk" {
				t.Errorf("Expected %q to normalise to \"blnk\", got %q", configured, cnf.Kafka.TopicPrefix)
			}
		}
	})

	t.Run("a whitespace-only prefix falls back to the default and is stored", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = " . "

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected a blank prefix to resolve to the default, got %v", err)
		}
		if cnf.Kafka.TopicPrefix != defaultKafka.TopicPrefix {
			t.Errorf("Expected the stored prefix to be the default %q, got %q",
				defaultKafka.TopicPrefix, cnf.Kafka.TopicPrefix)
		}
	})

	t.Run("the prefix is validated even when no broker is configured", func(t *testing.T) {
		// A deployment can configure the prefix before the brokers. Refusing only when
		// brokers are present would let the illegal value sit unnoticed until the day
		// someone switched publishing on.
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.TopicPrefix = "blnk prod"

		if err := cnf.validateAndAddDefaults(); err == nil {
			t.Fatal("Expected the prefix to be validated independently of the broker list")
		}
	})
}

// TestValidateKafkaHistoricalTopicPrefixes_NormalisesTheAllowlistAndRefusesAnUnusableEntry
// covers KAFKA_HISTORICAL_TOPIC_PREFIXES.
//
// # Why the list has to be validated rather than merely trimmed
//
// Every prefix in it is treated as OWNED: writers are pre-created for its whole topic
// inventory, the publisher's ownership test admits it, and topic assurance keeps its topics
// present. That is what keeps rows captured before a KAFKA_TOPIC_PREFIX rename publishable
// after a restart — so a malformed entry is a silent failure of exactly the stranding the
// list exists to prevent: the operator declares the old namespace, the value is unusable,
// and the old rows still never drain. Refusing it at load, by name, is what makes the
// declaration mean something.
//
// # Why the normalisations are normalisations rather than refusals
//
// The list is meant to be DRAINED. A trailing comma, or the current prefix left in place
// after the rename completed, describes the same set of owned topics either way, so both are
// collapsed. Removing the current prefix in particular is what lets every consumer treat the
// field as "the prefixes BESIDES the configured one" and compose a duplicate-free inventory
// from current-plus-historical.
func TestValidateKafkaHistoricalTopicPrefixes_NormalisesTheAllowlistAndRefusesAnUnusableEntry(t *testing.T) {
	t.Run("the ordinary case is an empty list", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected a configuration with no historical prefix to load, got %v", err)
		}
		if cnf.Kafka.HistoricalTopicPrefixes != nil {
			t.Errorf("Expected no historical prefixes, got %v", cnf.Kafka.HistoricalTopicPrefixes)
		}
		if owned := cnf.Kafka.OwnedTopicPrefixes(); len(owned) != 1 || owned[0] != "blnk" {
			t.Errorf("Expected the owned set to be the configured prefix alone, got %v", owned)
		}
	})

	t.Run("entries are trimmed, deduplicated and stripped of the configured prefix", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = "acme"
		cnf.Kafka.HistoricalTopicPrefixes = []string{
			" blnk\n", ".legacy.", "", "   ", "blnk", "acme",
		}

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected the realistic accidents to be normalised, got %v", err)
		}

		want := []string{"blnk", "legacy"}
		if len(cnf.Kafka.HistoricalTopicPrefixes) != len(want) {
			t.Fatalf("Expected %v, got %v", want, cnf.Kafka.HistoricalTopicPrefixes)
		}
		for i, prefix := range want {
			if cnf.Kafka.HistoricalTopicPrefixes[i] != prefix {
				t.Errorf("Expected entry %d to be %q, got %q",
					i, prefix, cnf.Kafka.HistoricalTopicPrefixes[i])
			}
		}

		// The configured prefix leads the owned set, because that is where new events go.
		owned := cnf.Kafka.OwnedTopicPrefixes()
		wantOwned := []string{"acme", "blnk", "legacy"}
		if len(owned) != len(wantOwned) {
			t.Fatalf("Expected the owned set %v, got %v", wantOwned, owned)
		}
		for i, prefix := range wantOwned {
			if owned[i] != prefix {
				t.Errorf("Expected owned prefix %d to be %q, got %q", i, prefix, owned[i])
			}
		}
	})

	t.Run("a list of nothing but blanks resolves to no historical prefix", func(t *testing.T) {
		// KAFKA_HISTORICAL_TOPIC_PREFIXES="" and "," both parse into a non-empty slice
		// carrying nothing usable, and both must read as "none declared" rather than as a
		// list of blank namespaces.
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.HistoricalTopicPrefixes = []string{"", " ", "."}

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected an all-blank list to normalise away, got %v", err)
		}
		if cnf.Kafka.HistoricalTopicPrefixes != nil {
			t.Errorf("Expected no historical prefixes, got %v", cnf.Kafka.HistoricalTopicPrefixes)
		}
	})

	t.Run("an entry that cannot compose a legal topic name is fatal and names the variable", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = "acme"
		cnf.Kafka.HistoricalTopicPrefixes = []string{"blnk prod"}

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected an interior space in a historical prefix to fail the load")
		}
		if !strings.Contains(err.Error(), "KAFKA_HISTORICAL_TOPIC_PREFIXES") {
			t.Errorf("Expected the error to name the historical variable, got %q", err.Error())
		}
		if !strings.Contains(err.Error(), `' '`) {
			t.Errorf("Expected the offending character quoted, got %q", err.Error())
		}
	})

	t.Run("a control character in an entry is quoted rather than emitted", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = "acme"
		cnf.Kafka.HistoricalTopicPrefixes = []string{"blnk\nfake-log-record"}

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected a newline in a historical prefix to fail the load")
		}
		if strings.Contains(err.Error(), "\n") {
			t.Errorf("The error must not carry the raw control character: %q", err.Error())
		}
	})

	t.Run("more than the permitted number of distinct prefixes is fatal", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = "acme"
		cnf.Kafka.HistoricalTopicPrefixes = make([]string, 0, MaxHistoricalTopicPrefixes+1)
		for i := 0; i <= MaxHistoricalTopicPrefixes; i++ {
			cnf.Kafka.HistoricalTopicPrefixes = append(
				cnf.Kafka.HistoricalTopicPrefixes, fmt.Sprintf("gen%d", i))
		}

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected an over-long allowlist to fail the load: each prefix pre-creates a " +
				"writer per topic on every process start")
		}
		if !strings.Contains(err.Error(), "KAFKA_HISTORICAL_TOPIC_PREFIXES") {
			t.Errorf("Expected the error to name the variable, got %q", err.Error())
		}
	})

	t.Run("exactly the permitted number is accepted", func(t *testing.T) {
		cnf := kafkaEnabledConfig("localhost:9092")
		cnf.Kafka.TopicPrefix = "acme"
		for i := 0; i < MaxHistoricalTopicPrefixes; i++ {
			cnf.Kafka.HistoricalTopicPrefixes = append(
				cnf.Kafka.HistoricalTopicPrefixes, fmt.Sprintf("gen%d", i))
		}

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected the maximum allowlist to be accepted, got %v", err)
		}
		if len(cnf.Kafka.HistoricalTopicPrefixes) != MaxHistoricalTopicPrefixes {
			t.Errorf("Expected %d prefixes, got %d",
				MaxHistoricalTopicPrefixes, len(cnf.Kafka.HistoricalTopicPrefixes))
		}
	})

	t.Run("the allowlist is validated even when no broker is configured", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.HistoricalTopicPrefixes = []string{"blnk prod"}

		if err := cnf.validateAndAddDefaults(); err == nil {
			t.Fatal("Expected the allowlist to be validated independently of the broker list")
		}
	})

	t.Run("OwnedTopicPrefixes answers for an unvalidated configuration too", func(t *testing.T) {
		// Reached from a test or a tool that builds a KafkaConfig literal without running
		// the validate-and-default path. A hole in the list would be worse than a fallback:
		// the strictest available answer is the default prefix.
		unvalidated := KafkaConfig{HistoricalTopicPrefixes: []string{" ", "legacy", "legacy"}}

		owned := unvalidated.OwnedTopicPrefixes()
		want := []string{"blnk", "legacy"}
		if len(owned) != len(want) {
			t.Fatalf("Expected %v, got %v", want, owned)
		}
		for i, prefix := range want {
			if owned[i] != prefix {
				t.Errorf("Expected owned prefix %d to be %q, got %q", i, prefix, owned[i])
			}
		}
	})
}

// TestEventRetentionPeriod_ConvertsDaysAndRefusesANegativePeriod covers the conversion and the
// one input that would be catastrophic to honour.
//
// The configured value is DAYS and every consumer needs a duration, so the conversion is where
// an order-of-magnitude error hides — hours instead of days would delete rows an operator
// expected to keep for a month, irreversibly and with no error to notice. A NEGATIVE value is
// worse still: read literally it places the cutoff in the FUTURE, which makes every terminal
// row eligible including ones delivered seconds ago. It is almost certainly a typo, and the
// most destructive possible reading of a typo is not the one to take, so it is refused and
// retention is disabled instead.
func TestEventRetentionPeriod_ConvertsDaysAndRefusesANegativePeriod(t *testing.T) {
	for name, testCase := range map[string]struct {
		days     int
		want     time.Duration
		wantDays int
	}{
		"disabled by default":   {days: 0, want: 0, wantDays: 0},
		"a single day":          {days: 1, want: 24 * time.Hour, wantDays: 1},
		"thirty days":           {days: 30, want: 720 * time.Hour, wantDays: 30},
		"ninety days":           {days: 90, want: 2160 * time.Hour, wantDays: 90},
		"a negative period":     {days: -30, want: 0, wantDays: 0},
		"a negative single day": {days: -1, want: 0, wantDays: 0},
	} {
		t.Run(name, func(t *testing.T) {
			clearEventStreamingEnv(t)

			cnf := eventStreamingBaseConfig()
			cnf.Relay.EventRetentionDays = testCase.days
			if err := cnf.validateAndAddDefaults(); err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if cnf.Relay.EventRetentionDays != testCase.wantDays {
				t.Errorf("Expected Relay.EventRetentionDays to be %d, got %d",
					testCase.wantDays, cnf.Relay.EventRetentionDays)
			}
			if period := cnf.EventRetentionPeriod(); period != testCase.want {
				t.Errorf("Expected EventRetentionPeriod to be %s, got %s", testCase.want, period)
			}
		})
	}

	t.Run("a nil configuration reports retention as disabled rather than panicking", func(t *testing.T) {
		var cnf *Configuration
		if period := cnf.EventRetentionPeriod(); period != 0 {
			t.Errorf("Expected 0 for a nil configuration, got %s", period)
		}
	})
}

// TestEventRetentionDays_ResolvesFromBothEnvironmentVariableForms asserts the retention
// variable follows the same dual-name contract as the other eight.
//
// The bare RELAY_EVENT_RETENTION_DAYS is the documented name, and the BLNK_-prefixed form is
// the repository's own convention. Both must resolve, or an operator following either the
// documentation or the surrounding convention would find retention silently switched off —
// and the symptom of that is a table that just keeps growing.
func TestEventRetentionDays_ResolvesFromBothEnvironmentVariableForms(t *testing.T) {
	for name, variable := range map[string]string{
		"the documented bare name":             "RELAY_EVENT_RETENTION_DAYS",
		"the repository's prefixed convention": "BLNK_RELAY_EVENT_RETENTION_DAYS",
	} {
		t.Run(name, func(t *testing.T) {
			clearEventStreamingEnv(t)
			t.Setenv(variable, "45")

			cnf := eventStreamingBaseConfig()
			if err := applyEventStreamingEnvOverride(&cnf); err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if err := cnf.validateAndAddDefaults(); err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if cnf.Relay.EventRetentionDays != 45 {
				t.Errorf("Expected %s to set Relay.EventRetentionDays to 45, got %d",
					variable, cnf.Relay.EventRetentionDays)
			}
			if period := cnf.EventRetentionPeriod(); period != 45*24*time.Hour {
				t.Errorf("Expected a 45-day period, got %s", period)
			}
		})
	}
}

// TestEventRetentionPurgeCapacity_DefaultsAboveArrivalsAndResolvesFromBothEnvForms covers the
// two settings PERF-P23 introduced, and covers them as CAPACITY rather than as two integers.
//
// # Why the default value is the assertion
//
// Purge capacity used to be a compile-time constant of 100 batches of 1,000 rows — 100,000 rows
// an hour — whose own comment claimed it "overtakes any realistic arrival rate". At the rate
// this system is validated against, 500 events a second, rows arrive at 1,800,000 an hour:
// eighteen times faster. Capacity below arrivals does not slow growth, it permits it, and the
// configured retention period is then never actually enforced however short it is set. So the
// first subtest is arithmetic against that arrival rate, not a restatement of a literal — a
// future change that lowered either factor back under peak would fail it.
//
// # Why zero defaults and unbounded has its own value
//
// An unset int field IS zero, so "I did not configure this" and "I want no ceiling" cannot both
// be read from it. Zero is taken as unset and defaulted, because the bound is what stops the
// first sweep after retention is enabled from attempting an entire historical backlog in one
// pass beside a live relay — a deployment that never mentions the setting must keep that
// protection. Asking for no ceiling is spelled EventRetentionUnboundedSweep, and every negative
// normalises onto it so a reader downstream recognises one value rather than testing a sign.
//
// The batch SIZE has no such ambiguity and is simply defaulted: zero there would delete nothing
// while still reporting healthy sweeps. Retention is switched off by its period, in one place,
// and never by a capacity value.
func TestEventRetentionPurgeCapacity_DefaultsAboveArrivalsAndResolvesFromBothEnvForms(t *testing.T) {
	t.Run("the shipped defaults exceed the specified peak arrival rate", func(t *testing.T) {
		clearEventStreamingEnv(t)

		cnf := eventStreamingBaseConfig()
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if cnf.Relay.EventRetentionBatchSize != defaultRelay.EventRetentionBatchSize {
			t.Errorf("Expected Relay.EventRetentionBatchSize to default to %d, got %d",
				defaultRelay.EventRetentionBatchSize, cnf.Relay.EventRetentionBatchSize)
		}
		if cnf.Relay.EventRetentionMaxBatchesPerSweep != defaultRelay.EventRetentionMaxBatchesPerSweep {
			t.Errorf("Expected Relay.EventRetentionMaxBatchesPerSweep to default to %d, got %d",
				defaultRelay.EventRetentionMaxBatchesPerSweep, cnf.Relay.EventRetentionMaxBatchesPerSweep)
		}

		// One sweep an hour, so capacity per sweep IS capacity per hour.
		const peakRowsPerHour = 500 * 60 * 60

		capacity := cnf.Relay.EventRetentionMaxBatchesPerSweep * cnf.Relay.EventRetentionBatchSize
		if capacity <= peakRowsPerHour {
			t.Errorf(
				"Expected default purge capacity to EXCEED peak arrivals of %d rows/hour, got %d; "+
					"at or below the arrival rate the retention period is not enforced at all",
				peakRowsPerHour, capacity)
		}
	})

	t.Run("a non-positive batch size is defaulted because zero would delete nothing", func(t *testing.T) {
		for name, configured := range map[string]int{"zero": 0, "negative": -250} {
			t.Run(name, func(t *testing.T) {
				clearEventStreamingEnv(t)

				cnf := eventStreamingBaseConfig()
				cnf.Relay.EventRetentionBatchSize = configured
				if err := cnf.validateAndAddDefaults(); err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if cnf.Relay.EventRetentionBatchSize != defaultRelay.EventRetentionBatchSize {
					t.Errorf("Expected a %s batch size to be defaulted to %d, got %d",
						name, defaultRelay.EventRetentionBatchSize, cnf.Relay.EventRetentionBatchSize)
				}
			})
		}
	})

	t.Run("an unset ceiling is defaulted rather than read as unbounded", func(t *testing.T) {
		clearEventStreamingEnv(t)

		cnf := eventStreamingBaseConfig()
		cnf.Relay.EventRetentionDays = 30
		cnf.Relay.EventRetentionMaxBatchesPerSweep = 0
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if cnf.Relay.EventRetentionMaxBatchesPerSweep != DefaultEventRetentionMaxBatchesPerSweep {
			t.Errorf(
				"Expected an unset ceiling to default to %d, got %d; reading zero as unbounded "+
					"would silently remove the bound from every deployment that never sets it",
				DefaultEventRetentionMaxBatchesPerSweep, cnf.Relay.EventRetentionMaxBatchesPerSweep)
		}
	})

	t.Run("every negative normalises onto the unbounded sentinel", func(t *testing.T) {
		for name, configured := range map[string]int{
			"the sentinel itself": EventRetentionUnboundedSweep,
			"another negative":    -9,
		} {
			t.Run(name, func(t *testing.T) {
				clearEventStreamingEnv(t)

				cnf := eventStreamingBaseConfig()
				cnf.Relay.EventRetentionDays = 30
				cnf.Relay.EventRetentionMaxBatchesPerSweep = configured
				if err := cnf.validateAndAddDefaults(); err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if cnf.Relay.EventRetentionMaxBatchesPerSweep != EventRetentionUnboundedSweep {
					t.Errorf(
						"Expected %d to normalise onto EventRetentionUnboundedSweep (%d), got %d; "+
							"one recognisable value downstream beats a sign to test",
						configured, EventRetentionUnboundedSweep,
						cnf.Relay.EventRetentionMaxBatchesPerSweep)
				}
			})
		}
	})

	t.Run("both settings resolve from either environment variable form", func(t *testing.T) {
		for name, form := range map[string]struct {
			batchSize  string
			maxBatches string
		}{
			"the documented bare names": {
				batchSize:  "RELAY_EVENT_RETENTION_BATCH_SIZE",
				maxBatches: "RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP",
			},
			"the repository's prefixed convention": {
				batchSize:  "BLNK_RELAY_EVENT_RETENTION_BATCH_SIZE",
				maxBatches: "BLNK_RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP",
			},
		} {
			t.Run(name, func(t *testing.T) {
				clearEventStreamingEnv(t)
				t.Setenv(form.batchSize, "750")
				t.Setenv(form.maxBatches, "3000")

				cnf := eventStreamingBaseConfig()
				if err := applyEventStreamingEnvOverride(&cnf); err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if err := cnf.validateAndAddDefaults(); err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if cnf.Relay.EventRetentionBatchSize != 750 {
					t.Errorf("Expected %s to set the batch size to 750, got %d",
						form.batchSize, cnf.Relay.EventRetentionBatchSize)
				}
				if cnf.Relay.EventRetentionMaxBatchesPerSweep != 3000 {
					t.Errorf("Expected %s to set the ceiling to 3000, got %d",
						form.maxBatches, cnf.Relay.EventRetentionMaxBatchesPerSweep)
				}
			})
		}
	})
}

// TestWebhookConfig_AllowPrivateDestinationDefaultsToRefusing pins the SAFE default of the
// legacy transport's destination policy (SSRF-01).
//
// # Why this deserves its own test
//
// Every test that needs internal delivery sets this flag explicitly, so a change that made
// it default to true would break nothing and be caught by nothing — while silently opening
// loopback, RFC1918 and plain http on every deployment that never mentions it. The default is
// the security property; the flag is only the exception to it.
//
// It is asserted after validateAndAddDefaults rather than on a bare literal, because a
// default setter is exactly where such a change would be introduced.
func TestWebhookConfig_AllowPrivateDestinationDefaultsToRefusing(t *testing.T) {
	cnf := Configuration{
		ProjectName: "Test Project",
		DataSource:  DataSourceConfig{Dns: "some-dns"},
		Redis:       RedisConfig{Dns: "localhost:6379"},
		Notification: Notification{
			Webhook: WebhookConfig{Url: "https://hooks.example.com/blnk"},
		},
	}

	if err := cnf.validateAndAddDefaults(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cnf.Notification.Webhook.AllowPrivateDestination {
		t.Error("notification.webhook.allow_private_destination must default to false: an " +
			"operator who has not asserted that their destination is internal must not have " +
			"http, loopback and RFC1918 opened on their behalf")
	}

	t.Run("an explicit assertion survives the default setters", func(t *testing.T) {
		asserted := cnf
		asserted.Notification.Webhook.AllowPrivateDestination = true

		if err := asserted.validateAndAddDefaults(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !asserted.Notification.Webhook.AllowPrivateDestination {
			t.Error("a default setter must not overwrite an assertion the operator made")
		}
	})
}

// TestSetLogLevelDefaults_MakesTheDebugDiagnosticsReachable is the test for the defect that
// the event pipeline's designed diagnostics could not be switched on in a deployed binary.
//
// The relay's successful-publish line, the publisher's equivalent, the metrics collector's
// per-tick summary and the consumer-lag retirement notice are all emitted at debug on
// purpose — they are per-event or per-tick, and at 500 events a second a line saying "it
// worked" is volume rather than observability. Nothing in the codebase called
// logrus.SetLevel, though, so every built binary sat at logrus's default of info and those
// lines were unreachable without recompiling: a delivery investigation had only failure
// lines and batch counts to work from.
//
// Each sub-test below is one of the three outcomes the resolution has, and the third is the
// one with teeth: BLANK MUST NOT TOUCH THE LOGGER. This runs from validateAndAddDefaults,
// which MockConfig also calls, and several tests in the root package pin the level to
// capture a debug-only line. An unconditional SetLevel here would undo those pins from
// inside the configuration layer, and the failure would appear in an unrelated package.
func TestSetLogLevelDefaults_MakesTheDebugDiagnosticsReachable(t *testing.T) {
	t.Run("a stated level is applied and normalised", func(t *testing.T) {
		for stated, want := range map[string]logrus.Level{
			"debug":   logrus.DebugLevel,
			"DEBUG":   logrus.DebugLevel,
			" trace ": logrus.TraceLevel,
			"warn":    logrus.WarnLevel,
			"warning": logrus.WarnLevel,
			// "error", "fatal" and "panic" are deliberately absent. They are not
			// applied verbatim, because each of them suppresses the event relay's
			// mandatory per-attempt records; they are raised to the floor instead, and
			// TestSetLogLevelDefaults_KeepsMandatoryRecordsVisible covers them.
		} {
			t.Run(stated, func(t *testing.T) {
				pinLevel(t, logrus.InfoLevel)

				cnf := eventStreamingBaseConfig()
				cnf.LogLevel = stated

				if err := cnf.validateAndAddDefaults(); err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if logrus.GetLevel() != want {
					t.Errorf("Expected the logger at %s, got %s — a configured level that is not "+
						"applied leaves the pipeline's debug diagnostics as unreachable as they were "+
						"with no setting at all", want, logrus.GetLevel())
				}
				if cnf.LogLevel != want.String() {
					t.Errorf("Expected the field normalised to %q so it reports what the logger is "+
						"actually set to, got %q", want.String(), cnf.LogLevel)
				}
			})
		}
	})

	t.Run("an unparseable level warns and keeps the level in force", func(t *testing.T) {
		pinLevel(t, logrus.InfoLevel)

		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := eventStreamingBaseConfig()
		cnf.LogLevel = "verbose"

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("a typo in a diagnostic control must not stop the process starting, got %v", err)
		}

		if logrus.GetLevel() != logrus.InfoLevel {
			t.Errorf("Expected the level in force to be kept, got %s", logrus.GetLevel())
		}
		if cnf.LogLevel != logrus.InfoLevel.String() {
			t.Errorf("Expected the field to report the level actually in force rather than the "+
				"rejected text, got %q", cnf.LogLevel)
		}

		var warned bool
		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, "not a level logrus recognises") {
				warned = true
				if !strings.Contains(entry.Message, "BLNK_LOG_LEVEL") {
					t.Error("the warning must name the variable an operator has to correct")
				}
			}
		}
		if !warned {
			t.Error("an unparseable level must be reported: silently keeping the default is how an " +
				"operator concludes the pipeline has no diagnostics rather than that they mistyped")
		}
	})

	t.Run("a blank level fills the field and leaves the logger untouched", func(t *testing.T) {
		pinLevel(t, logrus.DebugLevel)

		cnf := eventStreamingBaseConfig()

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if logrus.GetLevel() != logrus.DebugLevel {
			t.Errorf("a configuration that states no level must not reach into the logger: the "+
				"root package pins the level to capture debug-only lines and MockConfig runs this "+
				"code, so an unconditional SetLevel breaks those tests from here. Got %s",
				logrus.GetLevel())
		}
		if cnf.LogLevel != DEFAULT_LOG_LEVEL {
			t.Errorf("Expected the field defaulted to %q so the effective level is readable, got %q",
				DEFAULT_LOG_LEVEL, cnf.LogLevel)
		}
	})

	t.Run("MockConfig resolves the level like any other setting", func(t *testing.T) {
		pinLevel(t, logrus.InfoLevel)
		// MockConfig publishes into the process-global ConfigStore, so without this the store
		// is left pointing at this subtest's fixture and the next Fetch() anywhere in the
		// package reads it.
		restoreConfigStore(t)

		cnf := eventStreamingBaseConfig()
		cnf.LogLevel = "debug"
		MockConfig(&cnf)

		if logrus.GetLevel() != logrus.DebugLevel {
			t.Errorf("Expected MockConfig to apply a stated level, got %s", logrus.GetLevel())
		}
	})
}

// AAP R-4 requires the event relay to log the attempt count and error reason on EVERY
// failed publish attempt. Those records are warnings, and logrus discards warnings
// whenever the logger sits at error, fatal or panic — so three of the seven configurable
// levels used to delete a mandatory audit trail, silently, with nothing in the log to
// say anything had been withheld.
//
// These assertions pin the floor that closes that gap. They are written against the
// EFFECTIVE level rather than against the configuration string alone, because the string
// is only a report: what decides whether the record survives is logrus.GetLevel().
func TestSetLogLevelDefaults_KeepsMandatoryRecordsVisible(t *testing.T) {
	pinLevel := func(t *testing.T, level logrus.Level) {
		t.Helper()

		previous := logrus.GetLevel()
		t.Cleanup(func() { logrus.SetLevel(previous) })
		logrus.SetLevel(level)
	}

	t.Run("a level that would suppress the relay's per-attempt records is raised", func(t *testing.T) {
		for _, stated := range []string{"error", "fatal", "panic", "ERROR", " panic "} {
			t.Run(stated, func(t *testing.T) {
				pinLevel(t, logrus.InfoLevel)

				cnf := eventStreamingBaseConfig()
				cnf.LogLevel = stated

				if err := cnf.validateAndAddDefaults(); err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if logrus.GetLevel() != logrus.WarnLevel {
					t.Errorf("Expected %q to be raised to warn so the relay's mandatory "+
						"per-attempt publish-failure records still emit, got %s",
						stated, logrus.GetLevel())
				}

				// The field must report the level in force, not the one asked for.
				// /metrics and support bundles read it, and a field claiming "error"
				// while the logger runs at warn misleads exactly the person trying to
				// work out why they can see more than they configured.
				if cnf.LogLevel != MINIMUM_LOG_LEVEL {
					t.Errorf("Expected the field to report the effective level %q, got %q",
						MINIMUM_LOG_LEVEL, cnf.LogLevel)
				}

				// The proof that matters: a warning emitted at this level is not
				// discarded. This is the R-4 record's severity, exercised directly.
				hook := logtest.NewGlobal()
				defer hook.Reset()

				logrus.Warn("a mandatory per-attempt record")

				if len(hook.AllEntries()) == 0 {
					t.Errorf("a warning was discarded at effective level %s, which means "+
						"the relay's mandatory per-attempt records would be lost",
						logrus.GetLevel())
				}
			})
		}
	})

	t.Run("the clamp announces itself", func(t *testing.T) {
		pinLevel(t, logrus.InfoLevel)

		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := eventStreamingBaseConfig()
		cnf.LogLevel = "error"

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Silently overriding an operator's setting is its own defect: the next person
		// to wonder why they are seeing warnings at "error" has nothing to read. The
		// warning must also name BOTH levels, so the override is legible without
		// consulting the source.
		var found bool
		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, "mandatory per-attempt") {
				found = true

				if entry.Data["requested_log_level"] != "error" {
					t.Errorf("Expected the announcement to name the requested level, got %v",
						entry.Data["requested_log_level"])
				}
				if entry.Data["effective_log_level"] != MINIMUM_LOG_LEVEL {
					t.Errorf("Expected the announcement to name the effective level, got %v",
						entry.Data["effective_log_level"])
				}
			}
		}

		if !found {
			t.Error("raising the configured level must be announced; a silent override " +
				"leaves an operator with no way to learn their setting was not honoured")
		}
	})

	t.Run("levels at or above the floor are untouched", func(t *testing.T) {
		// The floor must not become a ceiling. trace and debug are the levels the
		// LogLevel setting exists to make reachable, so clamping in the wrong direction
		// would defeat the feature it was added for while still passing every
		// assertion above.
		for stated, want := range map[string]logrus.Level{
			"warn":  logrus.WarnLevel,
			"info":  logrus.InfoLevel,
			"debug": logrus.DebugLevel,
			"trace": logrus.TraceLevel,
		} {
			t.Run(stated, func(t *testing.T) {
				pinLevel(t, logrus.InfoLevel)

				cnf := eventStreamingBaseConfig()
				cnf.LogLevel = stated

				if err := cnf.validateAndAddDefaults(); err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if logrus.GetLevel() != want {
					t.Errorf("Expected %q to be applied unchanged, got %s — the floor must "+
						"raise quiet levels, never lower verbose ones", stated, logrus.GetLevel())
				}
			})
		}
	})
}

// clampLogLevel's comparison direction is the one thing about it that can be wrong while
// still looking right, because logrus orders its levels with the QUIET end at zero.
// Reversing the test would clamp trace and debug away instead of error and panic.
func TestClampLogLevel_RaisesOnlyTheLevelsThatSuppressMandatoryRecords(t *testing.T) {
	// The constant is compared directly against the normalised LogLevel field, so it
	// has to be the spelling logrus itself reports. logrus.ParseLevel accepts both
	// "warn" and "warning" but String() only ever returns the latter, which makes
	// "warn" a constant that parses correctly and then never equals the field it is
	// meant to describe.
	t.Run("the constant is the spelling logrus reports", func(t *testing.T) {
		if got := minimumVisibleLogLevel().String(); got != MINIMUM_LOG_LEVEL {
			t.Errorf("MINIMUM_LOG_LEVEL is %q but the level it parses to reports %q; the "+
				"constant must match so it can be compared against a normalised field",
				MINIMUM_LOG_LEVEL, got)
		}
	})

	for _, tc := range []struct {
		requested logrus.Level
		want      logrus.Level
		clamped   bool
	}{
		{logrus.PanicLevel, logrus.WarnLevel, true},
		{logrus.FatalLevel, logrus.WarnLevel, true},
		{logrus.ErrorLevel, logrus.WarnLevel, true},
		{logrus.WarnLevel, logrus.WarnLevel, false},
		{logrus.InfoLevel, logrus.InfoLevel, false},
		{logrus.DebugLevel, logrus.DebugLevel, false},
		{logrus.TraceLevel, logrus.TraceLevel, false},
	} {
		t.Run(tc.requested.String(), func(t *testing.T) {
			got, clamped := clampLogLevel(tc.requested)

			if got != tc.want || clamped != tc.clamped {
				t.Errorf("clampLogLevel(%s) = (%s, %v), want (%s, %v)",
					tc.requested, got, clamped, tc.want, tc.clamped)
			}

			// Whatever comes out must be able to carry a warning, which is the whole
			// purpose of the floor.
			if got < logrus.WarnLevel {
				t.Errorf("clampLogLevel(%s) returned %s, which suppresses warnings",
					tc.requested, got)
			}
		})
	}
}

// TestLoadConfigFromFile_LogLevelResolvesFromTheEnvironment pins the deployment surface of
// the setting: an operator turning debug on does so with an environment variable, on a
// running deployment, without editing blnk.json.
//
// Both the file value and the environment value are exercised, and the environment must WIN,
// because that is the whole point of the variable — the file records what the deployment
// normally runs at and the variable is how an investigation temporarily overrides it.
func TestLoadConfigFromFile_LogLevelResolvesFromTheEnvironment(t *testing.T) {
	previous := logrus.GetLevel()
	t.Cleanup(func() { logrus.SetLevel(previous) })

	writeConfig := func(t *testing.T, level string) string {
		t.Helper()

		body := map[string]interface{}{
			"project_name": "Test Project",
			"data_source":  map[string]string{"dns": "some-dns"},
			"redis":        map[string]string{"dns": "localhost:6379"},
		}
		if level != "" {
			body["log_level"] = level
		}

		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("Unable to encode the configuration: %v", err)
		}

		path := t.TempDir() + "/blnk.json"
		if err := os.WriteFile(path, encoded, 0o600); err != nil {
			t.Fatalf("Unable to write the configuration: %v", err)
		}

		return path
	}

	t.Run("from the file", func(t *testing.T) {
		clearEventStreamingEnv(t)
		clearLogLevelEnv(t)
		restoreConfigStore(t)
		pinLevel(t, logrus.InfoLevel)

		if err := loadConfigFromFile(writeConfig(t, "debug")); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		cnf, err := Fetch()
		if err != nil {
			t.Fatalf("Expected the configuration to load, got %v", err)
		}
		if cnf.LogLevel != "debug" || logrus.GetLevel() != logrus.DebugLevel {
			t.Errorf("Expected log_level from blnk.json to be applied, got field %q and level %s",
				cnf.LogLevel, logrus.GetLevel())
		}
	})

	t.Run("the environment overrides the file", func(t *testing.T) {
		clearEventStreamingEnv(t)
		restoreConfigStore(t)
		pinLevel(t, logrus.InfoLevel)
		t.Setenv("BLNK_LOG_LEVEL", "trace")

		if err := loadConfigFromFile(writeConfig(t, "error")); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		cnf, err := Fetch()
		if err != nil {
			t.Fatalf("Expected the configuration to load, got %v", err)
		}
		if cnf.LogLevel != "trace" || logrus.GetLevel() != logrus.TraceLevel {
			t.Errorf("Expected BLNK_LOG_LEVEL to win over the file value, got field %q and level %s",
				cnf.LogLevel, logrus.GetLevel())
		}
	})

	t.Run("neither states one, so the shipped default stands", func(t *testing.T) {
		clearEventStreamingEnv(t)
		clearLogLevelEnv(t)
		restoreConfigStore(t)
		pinLevel(t, logrus.InfoLevel)

		if err := loadConfigFromFile(writeConfig(t, "")); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		cnf, err := Fetch()
		if err != nil {
			t.Fatalf("Expected the configuration to load, got %v", err)
		}
		if cnf.LogLevel != DEFAULT_LOG_LEVEL || logrus.GetLevel() != logrus.InfoLevel {
			t.Errorf("Expected %q and an unchanged logger, got field %q and level %s",
				DEFAULT_LOG_LEVEL, cnf.LogLevel, logrus.GetLevel())
		}
	})
}

// TestLogger_AppliesTheLevelBeforeTheConfigurationIsRead covers the window InitConfig opens:
// it calls logger() and THEN loads the file, and loading the file logs.
//
// Without the environment read inside logger(), the warnings emitted while a configuration is
// being validated would be filtered by the PREVIOUS level — which is exactly backwards for
// the run in which somebody has just turned debug on to find out what happens at start-up.
func TestLogger_AppliesTheLevelBeforeTheConfigurationIsRead(t *testing.T) {
	previous := logrus.GetLevel()
	t.Cleanup(func() { logrus.SetLevel(previous) })

	t.Run("a parseable variable takes effect immediately", func(t *testing.T) {
		pinLevel(t, logrus.InfoLevel)
		t.Setenv("BLNK_LOG_LEVEL", "debug")

		logger()

		if logrus.GetLevel() != logrus.DebugLevel {
			t.Errorf("Expected debug before any configuration was read, got %s", logrus.GetLevel())
		}
	})

	t.Run("an unparseable variable is left for the configuration layer to report", func(t *testing.T) {
		pinLevel(t, logrus.InfoLevel)
		t.Setenv("BLNK_LOG_LEVEL", "chatty")

		logger()

		if logrus.GetLevel() != logrus.InfoLevel {
			t.Errorf("Expected the level unchanged, got %s", logrus.GetLevel())
		}
	})

	t.Run("no variable changes nothing", func(t *testing.T) {
		pinLevel(t, logrus.WarnLevel)
		// Save-and-restore, not a bare Unsetenv: the ambient value belongs to whatever runs
		// next, and stripping it for the rest of the process is a leak in the other direction.
		clearLogLevelEnv(t)

		logger()

		if logrus.GetLevel() != logrus.WarnLevel {
			t.Errorf("Expected the level unchanged, got %s", logrus.GetLevel())
		}
	})
}

// TestResolveSearchCredential_NeverAssumesAPublicKeyInProduction is SEC-07's configuration half.
//
// # What was wrong
//
// setDefaultValues substituted DEFAULT_TYPESENSE_KEY — the literal "blnk-api-key" — whenever no
// key was configured, in every posture. That literal is published in this repository's compose
// files and README, so it is known to everyone, and it grants full access to the search
// collection holding indexed transaction, balance and identity records. A production deployment
// that had merely forgotten BLNK_TYPESENSE_KEY therefore authenticated with a public credential.
//
// The substitution also destroyed the evidence: once applied, the field was non-empty, so no
// later check could distinguish an operator's key from an invented one. That is why the fix is a
// change of DECISION SITE and not just a change of value.
//
// # Why three arms rather than "require it"
//
// Each arm is a different deployment and a single rule gets one of them wrong. The local posture
// must keep working — a local TypeSense is started with that very key and the whole test suite
// runs there. A secure deployment that does not use search at all must not be refused startup
// over a subsystem it never calls. A secure deployment that DOES use search must be refused,
// because the only alternatives are a public credential or a stop, and a stop is correct.
func TestResolveSearchCredential_NeverAssumesAPublicKeyInProduction(t *testing.T) {
	t.Run("an operator-supplied key is never touched, in either posture", func(t *testing.T) {
		for _, secure := range []bool{false, true} {
			cnf := eventStreamingBaseConfig()
			cnf.Server.Secure = secure
			cnf.TypeSense = TypeSenseConfig{Dns: "http://typesense:8108"}
			cnf.TypeSenseKey = "an-operators-own-key"

			if err := cnf.validateAndAddDefaults(); err != nil {
				t.Fatalf("secure=%v: expected no error, got %v", secure, err)
			}
			if cnf.TypeSenseKey != "an-operators-own-key" {
				t.Errorf("secure=%v: the supplied key must survive verbatim, got %q", secure, cnf.TypeSenseKey)
			}
		}
	})

	t.Run("the local posture keeps the historical default", func(t *testing.T) {
		// Unchanged behaviour, deliberately. The compose stack starts TypeSense with this key,
		// the makefile targets rely on it and every test in this repository runs here; refusing
		// would break all of them to protect a credential doing no work.
		cnf := eventStreamingBaseConfig()
		cnf.Server.Secure = false
		cnf.TypeSense = TypeSenseConfig{Dns: "http://localhost:8108"}

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.TypeSenseKey != DEFAULT_TYPESENSE_KEY {
			t.Errorf("Expected the local default %q, got %q", DEFAULT_TYPESENSE_KEY, cnf.TypeSenseKey)
		}
	})

	t.Run("a secure deployment using search is REFUSED rather than given the public key", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Server.Secure = true
		cnf.TypeSense = TypeSenseConfig{Dns: "http://typesense:8108"}

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected a refusal: a production deployment with search configured and no key " +
				"must not be handed a credential published in this repository")
		}

		// The message has to name the variable and say why, because the operator's next action is
		// to set it and they need to know the fallback was not merely missing but unsafe.
		for _, fragment := range []string{"BLNK_TYPESENSE_KEY", "secure mode", "BLNK_TYPESENSE_DNS"} {
			if !strings.Contains(err.Error(), fragment) {
				t.Errorf("Expected the refusal to mention %q, got: %v", fragment, err)
			}
		}

		// AND the public literal must not have been left in the field on the way out. A refused
		// configuration that still carries the credential would hand it to any caller that
		// ignored the error.
		if cnf.TypeSenseKey != "" {
			t.Errorf("Expected the key to stay empty on refusal, got %q", cnf.TypeSenseKey)
		}
	})

	t.Run("a secure deployment not using search starts, with no key assumed", func(t *testing.T) {
		// Search is optional in Blnk — an unset host means no indexing — so refusing here would
		// stop every production deployment that does not run search, which is a worse defect
		// than the one being fixed.
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := eventStreamingBaseConfig()
		cnf.Server.Secure = true

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error when search is not in use, got %v", err)
		}
		if cnf.TypeSenseKey != "" {
			t.Errorf("Expected no key to be assumed, got %q", cnf.TypeSenseKey)
		}

		warned := false
		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, "no search credential") {
				warned = true
			}
		}
		if !warned {
			t.Error("Expected the condition to be reported: silently empty and silently public are " +
				"equally hard to notice")
		}
	})

	t.Run("whitespace is not a credential", func(t *testing.T) {
		// A variable set to spaces is an operator who meant to supply a value and did not.
		// Accepting it would let the search client authenticate with " " and fail somewhere far
		// less legible than here.
		cnf := eventStreamingBaseConfig()
		cnf.Server.Secure = true
		cnf.TypeSense = TypeSenseConfig{Dns: "http://typesense:8108"}
		cnf.TypeSenseKey = "   "

		if err := cnf.validateAndAddDefaults(); err == nil {
			t.Error("Expected a whitespace-only key to be refused exactly like an absent one")
		}
	})
}

// TestSetupRateLimiting_DefaultsToAFiniteProductionSafeLimit is SEC-14.
//
// # What was wrong
//
// The shipped defaults were 5,000,000 requests per second with a burst of 10,000,000, PER
// CLIENT ADDRESS — tollbooth keys its limiter that way. No client can issue five million
// requests a second against one instance, so the limiter never engaged. Rate limiting was
// configured, was reported as configured, and did nothing: CWE-770, and worse than having no
// limiter at all because it reads as present in every review of the configuration.
//
// # What is asserted
//
// That the default is FINITE and reachable — which is the whole property — and that it sits
// above the throughput this project specifies for itself, so closing the security gap cannot
// throttle a deployment operating at the volume Blnk is built for. The relationship between
// burst and rate is asserted too, because this function applies the same 2× rule when only one
// of the pair is supplied, and two different ratios in one function is how the next reader
// concludes the numbers are arbitrary.
func TestSetupRateLimiting_DefaultsToAFiniteProductionSafeLimit(t *testing.T) {
	cnf := eventStreamingBaseConfig()

	if err := cnf.validateAndAddDefaults(); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if cnf.RateLimit.RequestsPerSecond == nil || cnf.RateLimit.Burst == nil {
		t.Fatal("Both halves must be defaulted: the middleware disables the limiter entirely when " +
			"either is nil, so a half-default is no limit at all")
	}

	rps := *cnf.RateLimit.RequestsPerSecond
	burst := *cnf.RateLimit.Burst

	if rps != DEFAULT_RATE_LIMIT_RPS || burst != DEFAULT_RATE_LIMIT_BURST {
		t.Errorf("Expected the declared defaults %v/%v, got %v/%v",
			DEFAULT_RATE_LIMIT_RPS, DEFAULT_RATE_LIMIT_BURST, rps, burst)
	}

	// THE REGRESSION GUARD. Stated as a ceiling rather than as an equality so that a future
	// tuning change is free, while a return to a number no client could ever reach is not.
	if rps > 100000 {
		t.Errorf("A per-client default of %v requests/second is not a limit: no client can reach it, "+
			"so the limiter never engages and the control exists on paper only", rps)
	}

	// AND it must not throttle the throughput the project specifies for itself. AAP V-1 fixes
	// 500 events per second sustained, and the k6 harness drives exactly that from one host —
	// which is one client address.
	const specifiedThroughput = 500.0
	if rps < specifiedThroughput {
		t.Errorf("A per-client default of %v requests/second is below the %v events/second this "+
			"project's own acceptance criterion states, so the security fix would fail the "+
			"throughput criterion", rps, specifiedThroughput)
	}

	if float64(burst) != 2*rps {
		t.Errorf("The burst must be twice the rate, matching the ratio this function already applies "+
			"when only one of the pair is supplied; got %v for a rate of %v", burst, rps)
	}

	t.Run("an operator's own values are never overridden", func(t *testing.T) {
		configuredRPS := 25.0
		configuredBurst := 50

		cnf := eventStreamingBaseConfig()
		cnf.RateLimit.RequestsPerSecond = &configuredRPS
		cnf.RateLimit.Burst = &configuredBurst

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if *cnf.RateLimit.RequestsPerSecond != configuredRPS || *cnf.RateLimit.Burst != configuredBurst {
			t.Errorf("Expected %v/%v to survive, got %v/%v", configuredRPS, configuredBurst,
				*cnf.RateLimit.RequestsPerSecond, *cnf.RateLimit.Burst)
		}
	})

	t.Run("a tighter limit than the default is honoured, which is what makes the default a default", func(t *testing.T) {
		// The point of asserting this: the fix must not have turned a default into a floor. An
		// operator whose client identity IS resolvable should be able to go far tighter than
		// 2,000, and nothing here may prevent it.
		tight := 10.0

		cnf := eventStreamingBaseConfig()
		cnf.RateLimit.RequestsPerSecond = &tight

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if *cnf.RateLimit.RequestsPerSecond != tight {
			t.Errorf("Expected the tight rate %v to survive, got %v", tight, *cnf.RateLimit.RequestsPerSecond)
		}
		if *cnf.RateLimit.Burst != 20 {
			t.Errorf("Expected the burst to be derived as twice the rate, got %v", *cnf.RateLimit.Burst)
		}
	})
}

// TestRelayConfig_RepairCapacityDefaultsAndResolves pins the capacity the two relay repair
// passes run at (PERF-M06).
//
// # What the numbers are for
//
// Two populations of outbox row are outside the publish claim's reach by design: rows whose
// retry budget is spent and whose `<topic>.dlt` write also failed, and rows whose Kafka leg
// finished and whose legacy webhook enqueue never succeeded. Both are EMPTY in normal operation
// and fill during an OUTAGE, all at once — 15 minutes at the 500 events per second acceptance
// rate is about 450,000 rows — so the capacity that clears them is a recovery-time property
// rather than a throughput one, and it needs its own settings.
//
// # Why each assertion is here
//
// A zero or negative in any of the three would silently disable the only path that ever
// revisits an event which reached no topic at all: no batch claims nothing, no per-tick bound
// chains nothing, and no concurrency waits on a semaphore permit that never exists. So all
// three default rather than being honoured, and there is deliberately no "off" value —
// switching repair off has no legitimate use, unlike the retention sweep, whose PERIOD of zero
// is the documented way to disable a destructive operation.
//
// Both environment name forms are asserted because the deployment contract publishes both: the
// bare names the requirement mandates, and the repository's own BLNK_ prefix.
func TestRelayConfig_RepairCapacityDefaultsAndResolves(t *testing.T) {
	t.Run("unset takes the shipped capacity", func(t *testing.T) {
		clearEventStreamingEnv(t)

		cnf := eventStreamingBaseConfig()
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if cnf.Relay.RepairBatchSize != DefaultRelayRepairBatchSize {
			t.Errorf("Expected an unset repair batch size to default to %d, got %d",
				DefaultRelayRepairBatchSize, cnf.Relay.RepairBatchSize)
		}
		if cnf.Relay.RepairMaxBatchesPerTick != DefaultRelayRepairMaxBatchesPerTick {
			t.Errorf("Expected an unset per-tick bound to default to %d, got %d",
				DefaultRelayRepairMaxBatchesPerTick, cnf.Relay.RepairMaxBatchesPerTick)
		}
		if cnf.Relay.RepairConcurrency != DefaultRelayRepairConcurrency {
			t.Errorf("Expected an unset repair concurrency to default to %d, got %d",
				DefaultRelayRepairConcurrency, cnf.Relay.RepairConcurrency)
		}
	})

	t.Run("the shipped capacity clears an outage backlog", func(t *testing.T) {
		// The arithmetic the defaults were chosen from, asserted so a future change to either
		// number has to face it. 25 x 100 = 2,500 rows a tick; the superseded fixed batch of 20
		// rows once per tick is what this replaced.
		perTick := DefaultRelayRepairMaxBatchesPerTick * DefaultRelayRepairBatchSize
		if perTick < 1000 {
			t.Errorf(
				"the shipped repair capacity is %d rows a tick, which cannot clear what an outage "+
					"produces: 15 minutes at 500 events a second leaves about 450,000 rows, and the "+
					"superseded 20 rows a tick took roughly six and a quarter hours",
				perTick)
		}
	})

	t.Run("a non-positive value defaults rather than disabling repair", func(t *testing.T) {
		for name, configured := range map[string]int{"zero": 0, "negative": -4} {
			t.Run(name, func(t *testing.T) {
				clearEventStreamingEnv(t)

				cnf := eventStreamingBaseConfig()
				cnf.Relay.RepairBatchSize = configured
				cnf.Relay.RepairMaxBatchesPerTick = configured
				cnf.Relay.RepairConcurrency = configured

				if err := cnf.validateAndAddDefaults(); err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if cnf.Relay.RepairBatchSize != DefaultRelayRepairBatchSize ||
					cnf.Relay.RepairMaxBatchesPerTick != DefaultRelayRepairMaxBatchesPerTick ||
					cnf.Relay.RepairConcurrency != DefaultRelayRepairConcurrency {
					t.Errorf(
						"Expected %d to default in all three, got batch=%d per_tick=%d concurrency=%d; "+
							"a deployment that stopped repairing would silently keep events that "+
							"reached no topic at all",
						configured, cnf.Relay.RepairBatchSize,
						cnf.Relay.RepairMaxBatchesPerTick, cnf.Relay.RepairConcurrency)
				}
			})
		}
	})

	t.Run("all three resolve from either environment variable form", func(t *testing.T) {
		for name, prefix := range map[string]string{
			"the documented bare names":            "",
			"the repository's prefixed convention": "BLNK_",
		} {
			t.Run(name, func(t *testing.T) {
				clearEventStreamingEnv(t)
				t.Setenv(prefix+"RELAY_REPAIR_BATCH_SIZE", "40")
				t.Setenv(prefix+"RELAY_REPAIR_MAX_BATCHES_PER_TICK", "9")
				t.Setenv(prefix+"RELAY_REPAIR_CONCURRENCY", "3")

				cnf := eventStreamingBaseConfig()
				if err := applyEventStreamingEnvOverride(&cnf); err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if err := cnf.validateAndAddDefaults(); err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if cnf.Relay.RepairBatchSize != 40 {
					t.Errorf("Expected %sRELAY_REPAIR_BATCH_SIZE to set 40, got %d",
						prefix, cnf.Relay.RepairBatchSize)
				}
				if cnf.Relay.RepairMaxBatchesPerTick != 9 {
					t.Errorf("Expected %sRELAY_REPAIR_MAX_BATCHES_PER_TICK to set 9, got %d",
						prefix, cnf.Relay.RepairMaxBatchesPerTick)
				}
				if cnf.Relay.RepairConcurrency != 3 {
					t.Errorf("Expected %sRELAY_REPAIR_CONCURRENCY to set 3, got %d",
						prefix, cnf.Relay.RepairConcurrency)
				}
			})
		}
	})
}
