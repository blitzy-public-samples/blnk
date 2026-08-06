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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
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

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	// t.Cleanup rather than a bare defer, and the error is reported: a temp file that
	// cannot be removed leaks into /tmp on every run, and silently discarding the reason
	// is what let it go unnoticed.
	t.Cleanup(func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Logf("unable to remove the temporary config file: %v", err)
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

	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

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

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	// t.Cleanup rather than a bare defer, and the error is reported: a temp file that
	// cannot be removed leaks into /tmp on every run, and silently discarding the reason
	// is what let it go unnoticed.
	t.Cleanup(func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Logf("unable to remove the temporary config file: %v", err)
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
// envconfig v1.4.0 derives a field's primary key by accumulating the prefix
// through every enclosing struct and appending the tag literal, then falls back
// to the bare tag literal as an alternate key consulted only when the primary is
// unset. KafkaConfig is nested under Configuration.Kafka, so its primary key is
// BLNK_KAFKA_<TAG> — BLNK_KAFKA_KAFKA_BROKERS — and its alternate is the bare,
// deployment-mandated KAFKA_BROKERS.
//
// The intuitive-looking BLNK_KAFKA_BROKERS is neither of those keys, and used to be
// read by nothing at all — a deployment that set it silently got default behaviour.
// applyPrefixedEnvAliases now resolves it explicitly, and with HIGHER precedence
// than the bare name, so all three forms below are live and each is asserted by
// TestLoadConfigFromFile_KafkaEnvNameForms. Every form is still cleared here,
// because a value leaking in from the surrounding environment would satisfy a later
// assertion for the wrong reason.
var eventStreamingEnvKeys = []string{
	"KAFKA_BROKERS", "BLNK_KAFKA_KAFKA_BROKERS", "BLNK_KAFKA_BROKERS",
	"KAFKA_TOPIC_PREFIX", "BLNK_KAFKA_KAFKA_TOPIC_PREFIX", "BLNK_KAFKA_TOPIC_PREFIX",
	"KAFKA_SASL_USER", "BLNK_KAFKA_KAFKA_SASL_USER", "BLNK_KAFKA_SASL_USER",
	"KAFKA_SASL_SECRET", "BLNK_KAFKA_KAFKA_SASL_SECRET", "BLNK_KAFKA_SASL_SECRET",
	"KAFKA_SASL_ADMIN_USER", "BLNK_KAFKA_KAFKA_SASL_ADMIN_USER", "BLNK_KAFKA_SASL_ADMIN_USER",
	"KAFKA_SASL_ADMIN_SECRET", "BLNK_KAFKA_KAFKA_SASL_ADMIN_SECRET", "BLNK_KAFKA_SASL_ADMIN_SECRET",
	"KAFKA_MIN_PARTITIONS", "BLNK_KAFKA_KAFKA_MIN_PARTITIONS", "BLNK_KAFKA_MIN_PARTITIONS",
	"KAFKA_REPLICATION_FACTOR", "BLNK_KAFKA_KAFKA_REPLICATION_FACTOR", "BLNK_KAFKA_REPLICATION_FACTOR",
	"KAFKA_TLS_ENABLED", "BLNK_KAFKA_KAFKA_TLS_ENABLED", "BLNK_KAFKA_TLS_ENABLED",
	"KAFKA_TLS_CA_FILE", "BLNK_KAFKA_KAFKA_TLS_CA_FILE", "BLNK_KAFKA_TLS_CA_FILE",
	"KAFKA_TLS_CERT_FILE", "BLNK_KAFKA_KAFKA_TLS_CERT_FILE", "BLNK_KAFKA_TLS_CERT_FILE",
	"KAFKA_TLS_KEY_FILE", "BLNK_KAFKA_KAFKA_TLS_KEY_FILE", "BLNK_KAFKA_TLS_KEY_FILE",
	"KAFKA_TLS_SERVER_NAME", "BLNK_KAFKA_KAFKA_TLS_SERVER_NAME", "BLNK_KAFKA_TLS_SERVER_NAME",
	"KAFKA_TLS_INSECURE_SKIP_VERIFY", "BLNK_KAFKA_KAFKA_TLS_INSECURE_SKIP_VERIFY", "BLNK_KAFKA_TLS_INSECURE_SKIP_VERIFY",
	"KAFKA_INSECURE_LOCAL_DEV", "BLNK_KAFKA_KAFKA_INSECURE_LOCAL_DEV", "BLNK_KAFKA_INSECURE_LOCAL_DEV",
	"KAFKA_ALLOW_PARTITION_GROWTH", "BLNK_KAFKA_KAFKA_ALLOW_PARTITION_GROWTH", "BLNK_KAFKA_ALLOW_PARTITION_GROWTH",
	"RELAY_MAX_RETRY_ATTEMPTS", "BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS", "BLNK_RELAY_MAX_RETRY_ATTEMPTS",
	"RELAY_RETRY_BASE_BACKOFF_MS", "BLNK_RELAY_RELAY_RETRY_BASE_BACKOFF_MS", "BLNK_RELAY_RETRY_BASE_BACKOFF_MS",
	"RELAY_RETRY_MAX_BACKOFF_MS", "BLNK_RELAY_RELAY_RETRY_MAX_BACKOFF_MS", "BLNK_RELAY_RETRY_MAX_BACKOFF_MS",
	"WEBHOOK_DEPRECATION_START_DATE", "BLNK_WEBHOOK_DEPRECATION_START_DATE",
	"WEBHOOK_DEPRECATION_SUNSET_DATE", "BLNK_WEBHOOK_DEPRECATION_SUNSET_DATE",
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

// TestLoadConfigFromFile_RefusesKafkaBrokersWithoutAWindow carries the required-window
// rule through the REAL load pipeline, not just validateAndAddDefaults.
//
// This is what a deployment actually experiences: brokers supplied by the environment,
// no window anywhere, and a process that refuses to start rather than one that runs
// with dual delivery it can never end.
func TestLoadConfigFromFile_RefusesKafkaBrokersWithoutAWindow(t *testing.T) {
	clearEventStreamingEnv(t)
	restoreConfigStore(t)

	configFile := writeTempEventConfigFile(t)
	t.Setenv("KAFKA_BROKERS", "broker-1:9092")

	err := loadConfigFromFile(configFile)
	if err == nil {
		t.Fatal("Expected loadConfigFromFile to refuse brokers with no dual-delivery window")
	}
	if !strings.Contains(err.Error(), "webhook_deprecation_sunset_date is required") {
		t.Errorf("Expected the error to say the sunset is required, got %q", err.Error())
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
// A dedicated producer principal is now preferred, and the fallback is reported so the
// caller can warn.
func TestProducerSASL(t *testing.T) {
	t.Run("a dedicated producer principal is preferred and is not the admin", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.SASLUser = "blnk-producer"
		cnf.Kafka.SASLSecret = "producer-secret"
		cnf.Kafka.SASLAdminUser = "blnk-admin"
		cnf.Kafka.SASLAdminSecret = "admin-secret"

		user, secret, usingAdmin := cnf.ProducerSASL()
		if user != "blnk-producer" || secret != "producer-secret" {
			t.Errorf("Expected the producer principal, got user %q", user)
		}
		if usingAdmin {
			t.Error("Expected usingAdmin to be false when a producer principal is configured")
		}
	})

	t.Run("the admin principal is the reported fallback", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.SASLAdminUser = "blnk-admin"
		cnf.Kafka.SASLAdminSecret = "admin-secret"

		user, secret, usingAdmin := cnf.ProducerSASL()
		if user != "blnk-admin" || secret != "admin-secret" {
			t.Errorf("Expected the admin principal as the fallback, got user %q", user)
		}
		if !usingAdmin {
			t.Error("Expected usingAdmin to be true so the caller can warn about excess privilege")
		}
	})

	t.Run("no credential at all yields no SASL", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()

		user, secret, usingAdmin := cnf.ProducerSASL()
		if user != "" || secret != "" || usingAdmin {
			t.Errorf("Expected no credentials, got user %q usingAdmin %t", user, usingAdmin)
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

	// No broker list and no credentials are ever invented. A shipped default for
	// either SASL field would be a credential baked into the binary.
	assertBrokerList(t, cnf.Kafka.Brokers, nil)
	if cnf.Kafka.SASLAdminUser != "" {
		t.Errorf("Expected Kafka.SASLAdminUser to stay empty, got '%s'", cnf.Kafka.SASLAdminUser)
	}
	if cnf.Kafka.SASLAdminSecret != "" {
		t.Errorf("Expected Kafka.SASLAdminSecret to stay empty, got a value of %d characters", len(cnf.Kafka.SASLAdminSecret))
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

// TestValidateAndAddDefaults_KafkaSASLPairIsFatalOnlyWithBrokers pins where the pair
// contract is enforced, which is as important as the contract itself.
//
// Two properties are in tension and both must hold. A half-configured credential has
// to stop a deployment that actually uses Kafka, because it cannot be honoured and
// fails much later as a wrong-password or authorization error. But it must NOT stop a
// deployment with no brokers, because nothing reads it there and the graceful
// degradation that lets Blnk run entirely without Kafka is a shipped guarantee — the
// .env.example that ships an empty KAFKA_BROKERS would otherwise fail to load the
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

// TestResolveWebhookDeprecationWindow covers the dual-delivery window, and above
// all that it FAILS CLOSED.
//
// The previous behaviour of this check was advisory: a malformed sunset date was
// warned about and then ignored, and an absent one was treated as "the sunset has
// not passed". Both defaults kept the deprecated HTTP webhook transport running
// indefinitely, and neither produced a failure anybody would notice — a single
// mis-typed environment variable silently cancelled the retirement of the transport
// this whole feature exists to replace. So the check now refuses the configuration,
// and every case below asserts the error rather than the warning.
//
// The one case that legitimately has no window is a deployment with NO Kafka
// brokers: there is nothing to migrate to, so there is nothing to describe.
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

	t.Run("a start alone derives a sunset exactly one window later", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.WebhookDeprecationStartDate = testWindowStart

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.WebhookDeprecationSunsetDate != testWindowSunset {
			t.Errorf("Expected the derived sunset to be %q, got %q", testWindowSunset, cnf.WebhookDeprecationSunsetDate)
		}

		// Asserted as arithmetic as well as as a literal, so that changing
		// WebhookDualDeliveryWindowDays cannot leave this test agreeing with itself.
		start, err := time.Parse(time.RFC3339, cnf.WebhookDeprecationStartDate)
		if err != nil {
			t.Fatalf("Expected the window start to parse, got %v", err)
		}
		sunset, err := time.Parse(time.RFC3339, cnf.WebhookDeprecationSunsetDate)
		if err != nil {
			t.Fatalf("Expected the sunset to parse, got %v", err)
		}
		if got := sunset.Sub(start); got != WebhookDualDeliveryWindowDays*24*time.Hour {
			t.Errorf("Expected the window to be exactly %d days, got %s", WebhookDualDeliveryWindowDays, got)
		}
	})

	t.Run("a matching start and sunset pair is accepted", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.WebhookDeprecationStartDate = testWindowStart
		cnf.WebhookDeprecationSunsetDate = testWindowSunset

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})

	// The exact-window rule. A window silently shortened to 14 days, or stretched to
	// 45, contradicts the deprecation notice subscribers were given, and nothing else
	// in the system can detect the discrepancy.
	t.Run("a start and sunset that are not exactly one window apart are refused", func(t *testing.T) {
		mismatched := []struct {
			name   string
			start  string
			sunset string
		}{
			{name: "too short by half", start: testWindowStart, sunset: "2026-08-20T00:00:00Z"},
			{name: "too long", start: testWindowStart, sunset: "2026-09-19T00:00:00Z"},
			{name: "one second short", start: testWindowStart, sunset: "2026-09-03T23:59:59Z"},
			{name: "one second long", start: testWindowStart, sunset: "2026-09-04T00:00:01Z"},
			{name: "sunset before start", start: testWindowSunset, sunset: testWindowStart},
		}

		for _, tc := range mismatched {
			t.Run(tc.name, func(t *testing.T) {
				cnf := eventStreamingBaseConfig()
				cnf.WebhookDeprecationStartDate = tc.start
				cnf.WebhookDeprecationSunsetDate = tc.sunset

				err := cnf.validateAndAddDefaults()
				if err == nil {
					t.Fatalf("Expected an error for the %s window %s..%s", tc.name, tc.start, tc.sunset)
				}
				if !strings.Contains(err.Error(), "exactly") {
					t.Errorf("Expected the error to state the exact-window rule, got %q", err.Error())
				}
			})
		}
	})

	// The finding this test exists for: a malformed date must NOT be ignored.
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
				t.Run("as the sunset", func(t *testing.T) {
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

				t.Run("as the start", func(t *testing.T) {
					cnf := eventStreamingBaseConfig()
					cnf.WebhookDeprecationStartDate = value

					err := cnf.validateAndAddDefaults()
					if err == nil {
						t.Fatalf("Expected an error for the malformed start %q", value)
					}
					if !strings.Contains(err.Error(), "webhook_deprecation_start_date") {
						t.Errorf("Expected the error to name the field, got %q", err.Error())
					}
				})
			})
		}
	})

	// The second half of the finding: an ABSENT window is just as fail-open as a
	// malformed one once Kafka is publishing, because dual delivery then has no end.
	t.Run("an absent window is refused once kafka brokers are configured", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.Brokers = []string{"broker-1:9092"}

		err := cnf.validateAndAddDefaults()
		if err == nil {
			t.Fatal("Expected an error when brokers are configured with no dual-delivery window")
		}
		if !strings.Contains(err.Error(), "webhook_deprecation_sunset_date is required") {
			t.Errorf("Expected the error to say the sunset is required, got %q", err.Error())
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

// TestValidateAndAddDefaults_RelayWindowWarnings covers the relay retry-window
// consistency check. Relay tuning is an operational knob: a bad value must degrade
// event delivery, never stop the server from starting, so every finding is a
// warning and validateAndAddDefaults still returns nil in every case below.
//
// The check runs after the defaults are applied, so it inspects effective values.
// That is why each case supplies non-zero values for the fields it is exercising —
// a zero would simply be replaced by its default before the check ever saw it.
func TestValidateAndAddDefaults_RelayWindowWarnings(t *testing.T) {
	clearEventStreamingEnv(t)

	const (
		baseAboveCapWarning = "relay retry_base_backoff_ms exceeds retry_max_backoff_ms"
		negativeBaseWarning = "relay retry_base_backoff_ms is negative"
		negativeCapWarning  = "relay retry_max_backoff_ms is negative"
		tooFewAttemptsWarn  = "relay max_retry_attempts is below 1"
	)

	cases := []struct {
		name     string
		relay    RelayConfig
		want     []string
		unwanted []string
	}{
		{
			name:     "a base backoff above the cap warns",
			relay:    RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 40000, RetryMaxBackoffMS: 30000},
			want:     []string{baseAboveCapWarning},
			unwanted: []string{negativeBaseWarning, negativeCapWarning, tooFewAttemptsWarn},
		},
		{
			name:     "a negative base backoff warns",
			relay:    RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: -1, RetryMaxBackoffMS: 30000},
			want:     []string{negativeBaseWarning},
			unwanted: []string{baseAboveCapWarning, negativeCapWarning, tooFewAttemptsWarn},
		},
		{
			// A negative cap is below the base as well, so both findings are
			// expected — the check reports every problem it sees rather than the
			// first one.
			name:     "a negative cap warns",
			relay:    RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 1000, RetryMaxBackoffMS: -1},
			want:     []string{negativeCapWarning, baseAboveCapWarning},
			unwanted: []string{negativeBaseWarning, tooFewAttemptsWarn},
		},
		{
			name:     "a retry count below one warns",
			relay:    RelayConfig{MaxRetryAttempts: -1, RetryBaseBackoffMS: 1000, RetryMaxBackoffMS: 30000},
			want:     []string{tooFewAttemptsWarn},
			unwanted: []string{baseAboveCapWarning, negativeBaseWarning, negativeCapWarning},
		},
		{
			name:     "the default window warns about nothing",
			relay:    RelayConfig{MaxRetryAttempts: 5, RetryBaseBackoffMS: 1000, RetryMaxBackoffMS: 30000},
			want:     nil,
			unwanted: []string{baseAboveCapWarning, negativeBaseWarning, negativeCapWarning, tooFewAttemptsWarn},
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

	// A bare fixture stays Kafka-less, which is what selects the no-op publisher.
	assertBrokerList(t, loaded.Kafka.Brokers, nil)
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

// TestValidateAndAddDefaults_KafkaGeometryAndPrefixWarnings covers the two load-time
// diagnostics for values that are silently corrected, or silently rejected, later on.
//
// Both exist because the correction happens somewhere the operator is not looking. A
// negative partition count is raised to the required minimum by topic assurance, and a
// prefix carrying an illegal character composes a topic name the broker refuses — in both
// cases at the moment Kafka is first used, which for a deployment with no event traffic in
// flight can be long after start-up and a long way from the variable that caused it.
//
// They stay WARNINGS: validateRequiredFields requires only the two DSNs, and a deployment
// that does not use Kafka must not be stopped from booting by a stray variable.
func TestValidateAndAddDefaults_KafkaGeometryAndPrefixWarnings(t *testing.T) {
	const (
		negativePartitionsWarning = "KAFKA_MIN_PARTITIONS is negative"
		negativeReplicationWarn   = "KAFKA_REPLICATION_FACTOR is negative"
		illegalPrefixWarning      = "KAFKA_TOPIC_PREFIX contains characters Kafka does not permit"
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
			unwanted: []string{negativeReplicationWarn, illegalPrefixWarning},
		},
		{
			name:     "a negative replication factor warns",
			kafka:    KafkaConfig{ReplicationFactor: -1},
			want:     []string{negativeReplicationWarn},
			unwanted: []string{negativePartitionsWarning, illegalPrefixWarning},
		},
		{
			name:     "an interior space in the prefix warns",
			kafka:    KafkaConfig{TopicPrefix: "with space"},
			want:     []string{illegalPrefixWarning},
			unwanted: []string{negativePartitionsWarning, negativeReplicationWarn},
		},
		{
			name:     "an interior slash in the prefix warns",
			kafka:    KafkaConfig{TopicPrefix: "tenant/one"},
			want:     []string{illegalPrefixWarning},
			unwanted: []string{negativePartitionsWarning, negativeReplicationWarn},
		},
		{
			// Trimmed by the topic composer, so reporting it would be noise about
			// something that is already handled correctly.
			name:     "surrounding whitespace and dots are not reported",
			kafka:    KafkaConfig{TopicPrefix: "  .blnk. \n"},
			want:     nil,
			unwanted: []string{illegalPrefixWarning},
		},
		{
			name:     "a legal prefix with every permitted character warns about nothing",
			kafka:    KafkaConfig{TopicPrefix: "blnk-2_prod.eu"},
			want:     nil,
			unwanted: []string{illegalPrefixWarning, negativePartitionsWarning, negativeReplicationWarn},
		},
		{
			name:     "an unset geometry warns about nothing",
			kafka:    KafkaConfig{},
			want:     nil,
			unwanted: []string{negativePartitionsWarning, negativeReplicationWarn, illegalPrefixWarning},
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
				t.Fatalf("Expected a Kafka geometry or prefix problem to warn, not to fail; got %v", err)
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
