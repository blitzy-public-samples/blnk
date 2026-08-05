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
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up after the test

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
	tmpFile.Close() // Close the file so loadConfigFromFile can open it

	// Set an environment variable to override the project name
	os.Setenv("BLNK_PROJECT_NAME", "Env Project")
	defer os.Unsetenv("BLNK_PROJECT_NAME") // Clean up after the test

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
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up after the test

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
	tmpFile.Close() // Close the file so InitConfig can open it

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
// deployment-mandated KAFKA_BROKERS. The intuitive-looking BLNK_KAFKA_BROKERS is
// neither key and is honoured by neither; it is listed here so that a stray value
// cannot silently influence a test, and it is pinned as a regression case by
// TestLoadConfigFromFile_KafkaEnvNameForms. WebhookDeprecationSunsetDate is a
// top-level field, so it accumulates no intermediate segment and both its bare
// and BLNK_-prefixed forms resolve.
var eventStreamingEnvKeys = []string{
	"KAFKA_BROKERS", "BLNK_KAFKA_KAFKA_BROKERS", "BLNK_KAFKA_BROKERS",
	"KAFKA_TOPIC_PREFIX", "BLNK_KAFKA_KAFKA_TOPIC_PREFIX", "BLNK_KAFKA_TOPIC_PREFIX",
	"KAFKA_SASL_ADMIN_USER", "BLNK_KAFKA_KAFKA_SASL_ADMIN_USER", "BLNK_KAFKA_SASL_ADMIN_USER",
	"KAFKA_SASL_ADMIN_SECRET", "BLNK_KAFKA_KAFKA_SASL_ADMIN_SECRET", "BLNK_KAFKA_SASL_ADMIN_SECRET",
	"KAFKA_MIN_PARTITIONS", "BLNK_KAFKA_KAFKA_MIN_PARTITIONS", "BLNK_KAFKA_MIN_PARTITIONS",
	"KAFKA_REPLICATION_FACTOR", "BLNK_KAFKA_KAFKA_REPLICATION_FACTOR", "BLNK_KAFKA_REPLICATION_FACTOR",
	"RELAY_MAX_RETRY_ATTEMPTS", "BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS", "BLNK_RELAY_MAX_RETRY_ATTEMPTS",
	"RELAY_RETRY_BASE_BACKOFF_MS", "BLNK_RELAY_RELAY_RETRY_BASE_BACKOFF_MS", "BLNK_RELAY_RETRY_BASE_BACKOFF_MS",
	"RELAY_RETRY_MAX_BACKOFF_MS", "BLNK_RELAY_RELAY_RETRY_MAX_BACKOFF_MS", "BLNK_RELAY_RETRY_MAX_BACKOFF_MS",
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

// writeTempEventConfigFile writes the minimal valid configuration to a temporary
// blnk.json and returns its path, removing the file when the test finishes.
//
// Tests use it so that they run through the real load pipeline — file decode,
// envconfig.Process("blnk", ...), then validateAndAddDefaults — because asserting
// against a hand-built struct would not exercise envconfig at all.
func writeTempEventConfigFile(t *testing.T) string {
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

	if err := json.NewEncoder(tmpFile).Encode(eventStreamingBaseConfig()); err != nil {
		t.Fatalf("Unable to write to temporary file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Unable to close temporary file: %v", err)
	}

	return tmpFile.Name()
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
		cnf := eventStreamingBaseConfig()
		cnf.Kafka.Brokers = []string{"broker-1:9092", "broker-2:9092"}
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

// TestLoadConfigFromFile_KafkaEnvNameForms proves that both the bare environment
// variable names the deployment contract mandates and the BLNK_-prefixed names the
// rest of this file uses resolve into the new configuration, and records which
// prefixed name is the real one.
//
// Read this before "correcting" the un-prefixed envconfig tags in config.go.
// envconfig builds a field's primary key by accumulating the prefix through every
// enclosing struct and appending the tag literal, then consults the bare tag
// literal as an alternate key only when the primary is unset. So:
//
//   - Configuration.Kafka contributes a segment, making the primary key for
//     Brokers BLNK_KAFKA_KAFKA_BROKERS and the alternate the mandated
//     KAFKA_BROKERS. The intuitive BLNK_KAFKA_BROKERS is neither key and is
//     honoured by neither — that is pinned below so nobody discovers it the hard
//     way in production.
//   - WebhookDeprecationSunsetDate is a top-level field, so it accumulates no
//     intermediate segment and both WEBHOOK_DEPRECATION_SUNSET_DATE and
//     BLNK_WEBHOOK_DEPRECATION_SUNSET_DATE resolve.
//
// Adding a BLNK_ prefix to a tag would only change which bare name is honoured and
// would break the mandated one. Each form is exercised in isolation — one key set
// per subtest, every other form unset — because the point is that each resolves on
// its own, and because two forms set at once would prove nothing about either.
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
			name:   "the intuitive BLNK_KAFKA_BROKERS is honoured by neither key",
			envKey: "BLNK_KAFKA_BROKERS",
			value:  "broker-5:9092",
			assert: func(t *testing.T, loaded *Configuration) {
				assertBrokerList(t, loaded.Kafka.Brokers, nil)
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
			name:   "the intuitive BLNK_RELAY_MAX_RETRY_ATTEMPTS is honoured by neither key",
			envKey: "BLNK_RELAY_MAX_RETRY_ATTEMPTS",
			value:  "3",
			assert: func(t *testing.T, loaded *Configuration) {
				if loaded.Relay.MaxRetryAttempts != 5 {
					t.Errorf("Expected Relay.MaxRetryAttempts to fall back to the default 5, got %d", loaded.Relay.MaxRetryAttempts)
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

			configFile := writeTempEventConfigFile(t)
			t.Setenv(tc.envKey, tc.value)

			// Going through loadConfigFromFile is the point: it is what runs
			// envconfig.Process("blnk", &cnf) over the decoded file.
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

			configFile := writeTempEventConfigFile(t)
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

// TestValidateAndAddDefaults_WebhookSunsetDateValidation covers the sunset-date
// check and, above all, that it is advisory.
//
// Non-fatal is a requirement rather than a preference, for three independent
// reasons that this test locks in:
//
//  1. MockConfig fails closed. It calls validateAndAddDefaults and, on error, logs
//     and returns without storing — so a fatal check would silently break every
//     test file that injects configuration through it, with no compile error to
//     show for it.
//  2. Fetch fails whenever ConfigStore is empty, and live code depends on it.
//  3. The sunset decision itself treats an unset or unparseable date as "the
//     sunset has not passed", so refusing to load the configuration would
//     contradict the only consumer of the field.
//
// Every subtest therefore asserts the absence of an error explicitly, not just the
// presence of the warning.
func TestValidateAndAddDefaults_WebhookSunsetDateValidation(t *testing.T) {
	clearEventStreamingEnv(t)

	// The stable part of the message; the full text also carries the offending
	// value and the expected layout as structured fields.
	const sunsetWarning = "webhook_deprecation_sunset_date is not a valid RFC3339 timestamp"

	t.Run("a valid rfc3339 date is preserved and warns about nothing", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		const sunset = "2026-09-04T00:00:00Z"
		cnf := eventStreamingBaseConfig()
		cnf.WebhookDeprecationSunsetDate = sunset

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.WebhookDeprecationSunsetDate != sunset {
			t.Errorf("Expected WebhookDeprecationSunsetDate to be '%s', got '%s'", sunset, cnf.WebhookDeprecationSunsetDate)
		}
		if _, err := time.Parse(time.RFC3339, cnf.WebhookDeprecationSunsetDate); err != nil {
			t.Errorf("Expected the preserved sunset date to parse as RFC3339, got %v", err)
		}
		if warnedAbout(hook, sunsetWarning) {
			t.Errorf("Did not expect a sunset date warning for the valid date '%s'", sunset)
		}
	})

	t.Run("a malformed date warns without failing", func(t *testing.T) {
		malformed := []struct {
			name  string
			value string
		}{
			{name: "day first with slashes", value: "04/09/2026"},
			{name: "not a date at all", value: "not-a-date"},
			{name: "date only, no time or offset", value: "2026-09-04"},
			{name: "time without an offset", value: "2026-09-04T00:00:00"},
		}

		for _, tc := range malformed {
			t.Run(tc.name, func(t *testing.T) {
				hook := logtest.NewGlobal()
				defer hook.Reset()

				cnf := eventStreamingBaseConfig()
				cnf.WebhookDeprecationSunsetDate = tc.value

				if err := cnf.validateAndAddDefaults(); err != nil {
					t.Fatalf("Expected no error for the malformed date '%s', got %v", tc.value, err)
				}
				if !warnedAbout(hook, sunsetWarning) {
					t.Errorf("Expected a warning naming the field and RFC3339 for '%s'", tc.value)
				}
				// The value is reported, never rewritten: the sunset helper is the
				// single place that decides what an unparseable date means.
				if cnf.WebhookDeprecationSunsetDate != tc.value {
					t.Errorf("Expected WebhookDeprecationSunsetDate to be left as '%s', got '%s'", tc.value, cnf.WebhookDeprecationSunsetDate)
				}
			})
		}
	})

	t.Run("an empty date is valid and warns about nothing", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := eventStreamingBaseConfig()
		cnf.WebhookDeprecationSunsetDate = ""

		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if cnf.WebhookDeprecationSunsetDate != "" {
			t.Errorf("Expected WebhookDeprecationSunsetDate to stay empty, got '%s'", cnf.WebhookDeprecationSunsetDate)
		}
		if warnedAbout(hook, sunsetWarning) {
			t.Errorf("Did not expect a sunset date warning for an unset date")
		}
	})

	// Nothing this feature adds may become required. The data source and Redis DNS
	// values remain the only two required fields, which is what keeps the existing
	// configuration literals across the test suite valid.
	t.Run("neither the sunset date nor any kafka field is required", func(t *testing.T) {
		cnf := eventStreamingBaseConfig()
		if cnf.WebhookDeprecationSunsetDate != "" || len(cnf.Kafka.Brokers) != 0 {
			t.Fatalf("Expected the base fixture to leave the event-streaming fields unset")
		}

		if err := cnf.validateRequiredFields(); err != nil {
			t.Errorf("Expected validateRequiredFields to accept a config with no event-streaming values, got %v", err)
		}
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Errorf("Expected no error, got %v", err)
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
