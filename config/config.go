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
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/sirupsen/logrus"
)

// Default constants
const (
	DEFAULT_PORT        = "5001"
	DEFAULT_CLEANUP_SEC = 10800 // 3 hours in seconds
	// DEFAULT_TYPESENSE_KEY is the api-key a LOCAL TypeSense is started with, and it is
	// applied only when secure mode is off. It is emphatically NOT a secret — it is
	// published in this repository's compose files and README — which is why
	// resolveSearchCredential refuses to substitute it in a production posture.
	DEFAULT_TYPESENSE_KEY = "blnk-api-key"
	// DEFAULT_RATE_LIMIT_RPS and DEFAULT_RATE_LIMIT_BURST are the PER-CLIENT request rate
	// a deployment that configures none is held to.
	DEFAULT_RATE_LIMIT_RPS     = 2000.0
	DEFAULT_RATE_LIMIT_BURST   = 4000
	DEFAULT_MONITORING_PORT    = "5004"
	DEFAULT_MAX_UPLOAD_SIZE_MB = 256 // caps reconciliation file uploads
	// DEFAULT_MAX_REQUEST_BODY_SIZE_MB caps non-upload request bodies so a large
	// POST can't exhaust memory before a handler (or the auth middleware) reads it.
	DEFAULT_MAX_REQUEST_BODY_SIZE_MB = 5
	// DEFAULT_UPLOAD_URL_TIMEOUT_SEC caps how long a URL-based reconciliation
	// upload may spend fetching the remote body, preventing a slow or stalled
	// upstream from hanging the handler.
	DEFAULT_UPLOAD_URL_TIMEOUT_SEC = 30
	// DEFAULT_LOG_LEVEL is the verbosity a deployment runs at when it states none, and it
	// is logrus's own default spelled out rather than a new choice: filling the field is
	// about making the effective level READABLE, not about changing it.
	DEFAULT_LOG_LEVEL = "info"

	// MINIMUM_LOG_LEVEL is the LEAST verbose level Blnk will actually run at, whatever a
	// deployment configures. A quieter request is honoured as far as this and no further.
	MINIMUM_LOG_LEVEL = "warning"
)

// Default values for different configurations
var (
	defaultTransaction = TransactionConfig{
		BatchSize:                  1000,
		MaxQueueSize:               1000,
		MaxWorkers:                 10,
		LockDuration:               5 * time.Minute,
		LockWaitTimeout:            3 * time.Second,
		IndexQueuePrefix:           "transactions",
		EnableCoalescing:           true,
		EnableQueuedChecks:         false,
		DisableBatchReferenceCheck: false,
	}

	defaultReconciliation = ReconciliationConfig{
		DefaultStrategy:  "one_to_one",
		ProgressInterval: 100,
		MaxRetries:       3,
		RetryDelay:       5 * time.Second,
	}

	defaultQueue = QueueConfig{
		TransactionQueue:                "new:transaction",
		WebhookQueue:                    "new:webhook",
		IndexQueue:                      "new:index",
		InflightExpiryQueue:             "new:inflight-expiry",
		InflightCommitQueue:             "new:inflight-commit",
		NumberOfQueues:                  20,
		EnableHotLane:                   false,
		HotQueueName:                    "hot_transactions",
		HotQueueConcurrency:             1,
		HotPairTTL:                      5 * time.Minute,
		HotPairLockContentionThreshold:  3,
		RejectLockContentionImmediately: false,
		MaxRetryAttempts:                5,
		MonitoringPort:                  DEFAULT_MONITORING_PORT,
		WebhookConcurrency:              20,
		TransactionWorkerConcurrency:    4,
	}

	defaultRedis = RedisConfig{
		PoolSize:     100,
		MinIdleConns: 20,
	}

	defaultDatabase = DataSourceConfig{
		MaxOpenConns:    50,
		MaxIdleConns:    25,
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	}

	// defaultKafka deliberately leaves Brokers and all four SASL fields zero-valued. An
	// empty broker list is a legitimate steady state, not an error: it selects the no-op
	// event publisher, reproducing the historic no-op-when-unconfigured contract so a
	// deployment without Kafka keeps working. The SASL fields are credentials and must
	// never carry a shipped default.
	defaultKafka = KafkaConfig{
		TopicPrefix:       "blnk",
		MinPartitions:     6,
		ReplicationFactor: 3,
		// Far above any plausible subscriber count for a single ledger deployment and far
		// below the point at which either cost it bounds — tick duration and exported series
		// — begins to matter. See KafkaConfig.MetricsSubscriberBudget.
		MetricsSubscriberBudget: 200,
	}

	// defaultRelay encodes the bounded exponential backoff schedule the requirement
	// mandates: five publish attempts and the delay sequence 1s, 2s, 4s, 8s, 16s, capped
	// at 30s.
	defaultRelay = RelayConfig{
		MaxRetryAttempts:   MaxRelayRetryAttempts,
		RetryBaseBackoffMS: 1000,
		RetryMaxBackoffMS:  30000,

		EventRetentionBatchSize:          DefaultEventRetentionBatchSize,
		EventRetentionMaxBatchesPerSweep: DefaultEventRetentionMaxBatchesPerSweep,

		RepairBatchSize:         DefaultRelayRepairBatchSize,
		RepairMaxBatchesPerTick: DefaultRelayRepairMaxBatchesPerTick,
		RepairConcurrency:       DefaultRelayRepairConcurrency,

		// Deliberately the same number as defaultKafka.MetricsSubscriberBudget, and kept in
		// step with blnk.DefaultSubscriberMetricsBudget, which is the value the collector
		// falls back to when it cannot read a configuration at all. These are two published
		// spellings of ONE ceiling; setRelayDefaults reconciles them onto a single value.
		SubscriberMetricsBudget: 200,
	}
)

var ConfigStore atomic.Value

type ServerConfig struct {
	SSL                  bool   `json:"ssl"                      envconfig:"BLNK_SERVER_SSL"`
	CertStoragePath      string `json:"cert_storage_path"        envconfig:"BLNK_CERT_STORAGE_PATH"`
	Secure               bool   `json:"secure"                   envconfig:"BLNK_SERVER_SECURE"`
	SecretKey            string `json:"secret_key"               envconfig:"BLNK_SERVER_SECRET_KEY"`
	Domain               string `json:"domain"                   envconfig:"BLNK_SERVER_SSL_DOMAIN"`
	Email                string `json:"ssl_email"                envconfig:"BLNK_SERVER_SSL_EMAIL"`
	Port                 string `json:"port"                     envconfig:"BLNK_SERVER_PORT"`
	MetricsBearerToken   string `json:"metrics_bearer_token"     envconfig:"BLNK_METRICS_BEARER_TOKEN"`
	MaxUploadSizeMB      int64  `json:"max_upload_size_mb"       envconfig:"BLNK_SERVER_MAX_UPLOAD_SIZE_MB"`
	MaxRequestBodySizeMB int64  `json:"max_request_body_size_mb" envconfig:"BLNK_SERVER_MAX_REQUEST_BODY_SIZE_MB"`

	// UploadWhitelist is a comma-separated list of exact hostnames permitted as
	// targets for URL-based reconciliation uploads (BLNK_UPLOAD_DOMAIN_WHITELIST).
	UploadDomainWhitelist string `json:"upload_whitelist"       envconfig:"BLNK_UPLOAD_DOMAIN_WHITELIST"`
	// UploadURLTimeoutSec caps the HTTP GET issued for a URL-based upload
	// (BLNK_UPLOAD_URL_TIMEOUT_SEC). Defaults to DEFAULT_UPLOAD_URL_TIMEOUT_SEC.
	UploadURLTimeoutSec int `json:"upload_url_timeout_sec" envconfig:"BLNK_UPLOAD_URL_TIMEOUT_SEC"`

	// TrustForwardedProto declares that every request reaching this process has passed
	// through a proxy — an ingress controller, a load balancer, a service mesh sidecar —
	// that TERMINATED TLS and that sets X-Forwarded-Proto, overwriting any value a client
	// supplied (BLNK_SERVER_TRUST_FORWARDED_PROTO).
	TrustForwardedProto bool `json:"trust_forwarded_proto" envconfig:"BLNK_SERVER_TRUST_FORWARDED_PROTO"`

	// AllowLoopbackCredentialIssuance permits POST /subscribers/{id}/kafka-credentials to
	// answer over a plaintext connection whose PEER is a loopback address
	// (BLNK_SERVER_ALLOW_LOOPBACK_CREDENTIAL_ISSUANCE). It is a LOCAL-DEVELOPMENT
	// convenience and nothing else.
	AllowLoopbackCredentialIssuance bool `json:"allow_loopback_credential_issuance" envconfig:"BLNK_SERVER_ALLOW_LOOPBACK_CREDENTIAL_ISSUANCE"`
	// TrustedProxies is a comma-separated list of CIDR blocks or bare IP literals whose
	// X-Forwarded-For and X-Real-IP headers may be BELIEVED (BLNK_SERVER_TRUSTED_PROXIES).
	TrustedProxies string `json:"trusted_proxies" envconfig:"BLNK_SERVER_TRUSTED_PROXIES"`
}

type DataSourceConfig struct {
	Dns             string        `json:"dns"                envconfig:"BLNK_DATA_SOURCE_DNS"`
	MaxOpenConns    int           `json:"max_open_conns"     envconfig:"BLNK_DATABASE_MAX_OPEN_CONNS"`
	MaxIdleConns    int           `json:"max_idle_conns"     envconfig:"BLNK_DATABASE_MAX_IDLE_CONNS"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime"  envconfig:"BLNK_DATABASE_CONN_MAX_LIFETIME"`
	ConnMaxIdleTime time.Duration `json:"conn_max_idle_time" envconfig:"BLNK_DATABASE_CONN_MAX_IDLE_TIME"`
}

type RedisConfig struct {
	Dns           string `json:"dns"             envconfig:"BLNK_REDIS_DNS"`
	SkipTLSVerify bool   `json:"skip_tls_verify" envconfig:"BLNK_REDIS_SKIP_TLS_VERIFY"`
	PoolSize      int    `json:"pool_size"       envconfig:"BLNK_REDIS_POOL_SIZE"`
	MinIdleConns  int    `json:"min_idle_conns"  envconfig:"BLNK_REDIS_MIN_IDLE_CONNS"`
}

type TypeSenseConfig struct {
	Dns string `json:"dns" envconfig:"BLNK_TYPESENSE_DNS"`
}

type AccountGenerationHttpService struct {
	Url     string `json:"url"`
	Timeout int    `json:"timeout"`
	Headers struct {
		Authorization string `json:"Authorization"`
	} `json:"headers"`
}
type AccountNumberGenerationConfig struct {
	EnableAutoGeneration bool                         `json:"enable_auto_generation"`
	HttpService          AccountGenerationHttpService `json:"http_service"`
}

type RateLimitConfig struct {
	RequestsPerSecond  *float64 `json:"requests_per_second"  envconfig:"BLNK_RATE_LIMIT_RPS"`
	Burst              *int     `json:"burst"                envconfig:"BLNK_RATE_LIMIT_BURST"`
	CleanupIntervalSec *int     `json:"cleanup_interval_sec" envconfig:"BLNK_RATE_LIMIT_CLEANUP_INTERVAL_SEC"`
}

type SlackWebhook struct {
	WebhookUrl string `json:"webhook_url" envconfig:"BLNK_SLACK_WEBHOOK_URL"`
}

type WebhookConfig struct {
	Url     string            `json:"url"     envconfig:"BLNK_WEBHOOK_URL"`
	Headers map[string]string `json:"headers" envconfig:"BLNK_WEBHOOK_HEADERS"`

	// AllowPrivateDestination asserts that the configured webhook destination is on a
	// network the operator owns, permitting http and permitting delivery to loopback and
	// private addresses.
	AllowPrivateDestination bool `json:"allow_private_destination" envconfig:"BLNK_WEBHOOK_ALLOW_PRIVATE_DESTINATION"`
}

type Notification struct {
	Slack   SlackWebhook  `json:"slack"`
	Webhook WebhookConfig `json:"webhook"`
}

type TransactionConfig struct {
	BatchSize                  int             `json:"batch_size"                    envconfig:"BLNK_TRANSACTION_BATCH_SIZE"`
	MaxQueueSize               int             `json:"max_queue_size"                envconfig:"BLNK_TRANSACTION_MAX_QUEUE_SIZE"`
	MaxWorkers                 int             `json:"max_workers"                   envconfig:"BLNK_TRANSACTION_MAX_WORKERS"`
	LockDuration               time.Duration   `json:"lock_duration"                 envconfig:"BLNK_TRANSACTION_LOCK_DURATION"`
	LockWaitTimeout            time.Duration   `json:"lock_wait_timeout"             envconfig:"BLNK_TRANSACTION_LOCK_WAIT_TIMEOUT"`
	IndexQueuePrefix           string          `json:"index_queue_prefix"            envconfig:"BLNK_TRANSACTION_INDEX_QUEUE_PREFIX"`
	EnableCoalescing           bool            `json:"enable_coalescing"             envconfig:"BLNK_TRANSACTION_ENABLE_COALESCING"`
	EnableQueuedChecks         bool            `json:"enable_queued_checks"          envconfig:"BLNK_TRANSACTION_ENABLE_QUEUED_CHECKS"`
	DisableBatchReferenceCheck bool            `json:"disable_batch_reference_check" envconfig:"BLNK_TRANSACTION_DISABLE_BATCH_REFERENCE_CHECK"`
	HashChain                  HashChainConfig `json:"hash_chain"`
}

type ReconciliationConfig struct {
	DefaultStrategy  string        `json:"default_strategy"  envconfig:"BLNK_RECONCILIATION_DEFAULT_STRATEGY"`
	ProgressInterval int           `json:"progress_interval" envconfig:"BLNK_RECONCILIATION_PROGRESS_INTERVAL"`
	MaxRetries       int           `json:"max_retries"       envconfig:"BLNK_RECONCILIATION_MAX_RETRIES"`
	RetryDelay       time.Duration `json:"retry_delay"       envconfig:"BLNK_RECONCILIATION_RETRY_DELAY"`
}

type QueueConfig struct {
	TransactionQueue                string        `json:"transaction_queue"                  envconfig:"BLNK_QUEUE_TRANSACTION"`
	WebhookQueue                    string        `json:"webhook_queue"                      envconfig:"BLNK_QUEUE_WEBHOOK"`
	IndexQueue                      string        `json:"index_queue"                        envconfig:"BLNK_QUEUE_INDEX"`
	InflightExpiryQueue             string        `json:"inflight_expiry_queue"              envconfig:"BLNK_QUEUE_INFLIGHT_EXPIRY"`
	InflightCommitQueue             string        `json:"inflight_commit_queue"              envconfig:"BLNK_QUEUE_INFLIGHT_COMMIT"`
	NumberOfQueues                  int           `json:"number_of_queues"                   envconfig:"BLNK_QUEUE_NUMBER_OF_QUEUES"`
	EnableHotLane                   bool          `json:"enable_hot_lane"                    envconfig:"BLNK_QUEUE_ENABLE_HOT_LANE"`
	HotQueueName                    string        `json:"hot_queue_name"                     envconfig:"BLNK_QUEUE_HOT_QUEUE_NAME"`
	HotQueueConcurrency             int           `json:"hot_queue_concurrency"              envconfig:"BLNK_QUEUE_HOT_QUEUE_CONCURRENCY"`
	HotPairTTL                      time.Duration `json:"hot_pair_ttl"                       envconfig:"BLNK_QUEUE_HOT_PAIR_TTL"`
	HotPairLockContentionThreshold  int           `json:"hot_pair_lock_contention_threshold" envconfig:"BLNK_QUEUE_HOT_PAIR_LOCK_CONTENTION_THRESHOLD"`
	RejectLockContentionImmediately bool          `json:"reject_lock_contention_immediately" envconfig:"BLNK_QUEUE_REJECT_LOCK_CONTENTION_IMMEDIATELY"`
	InsufficientFundRetries         bool          `json:"insufficient_fund_retries"          envconfig:"BLNK_QUEUE_INSUFFICIENT_FUND_RETRIES"`
	MaxRetryAttempts                int           `json:"max_retry_attempts"                 envconfig:"BLNK_QUEUE_MAX_RETRY_ATTEMPTS"`
	MonitoringPort                  string        `json:"monitoring_port"                    envconfig:"BLNK_QUEUE_MONITORING_PORT"`
	WebhookConcurrency              int           `json:"webhook_concurrency"                envconfig:"BLNK_QUEUE_WEBHOOK_CONCURRENCY"`
	TransactionWorkerConcurrency    int           `json:"transaction_worker_concurrency"     envconfig:"BLNK_QUEUE_TRANSACTION_WORKER_CONCURRENCY"`
}

type Configuration struct {
	ProjectName             string                        `json:"project_name"              envconfig:"BLNK_PROJECT_NAME"`
	BackupDir               string                        `json:"backup_dir"                envconfig:"BLNK_BACKUP_DIR"`
	AwsAccessKeyId          string                        `json:"aws_access_key_id"         envconfig:"BLNK_AWS_ACCESS_KEY_ID"`
	S3Endpoint              string                        `json:"s3_endpoint"               envconfig:"BLNK_S3_ENDPOINT"`
	AwsSecretAccessKey      string                        `json:"aws_secret_access_key"     envconfig:"BLNK_AWS_SECRET_ACCESS_KEY"`
	S3BucketName            string                        `json:"s3_bucket_name"            envconfig:"BLNK_S3_BUCKET_NAME"`
	S3Region                string                        `json:"s3_region"                 envconfig:"BLNK_S3_REGION"`
	Server                  ServerConfig                  `json:"server"`
	DataSource              DataSourceConfig              `json:"data_source"`
	Redis                   RedisConfig                   `json:"redis"`
	TypeSense               TypeSenseConfig               `json:"typesense"`
	TypeSenseKey            string                        `json:"type_sense_key"            envconfig:"BLNK_TYPESENSE_KEY"`
	TokenizationSecret      string                        `json:"tokenization_secret"       envconfig:"BLNK_TOKENIZATION_SECRET"`
	AccountNumberGeneration AccountNumberGenerationConfig `json:"account_number_generation"`
	Notification            Notification                  `json:"notification"`
	RateLimit               RateLimitConfig               `json:"rate_limit"`
	EnableTelemetry         bool                          `json:"enable_telemetry"          envconfig:"BLNK_ENABLE_TELEMETRY"`
	EnableObservability     bool                          `json:"enable_observability"      envconfig:"BLNK_ENABLE_OBSERVABILITY"`
	MonitoringDSN           string                        `json:"monitoring_dsn"            envconfig:"BLNK_MONITORING_DSN"`
	Transaction             TransactionConfig             `json:"transaction"`
	Reconciliation          ReconciliationConfig          `json:"reconciliation"`
	Queue                   QueueConfig                   `json:"queue"`
	Kafka                   KafkaConfig                   `json:"kafka"`
	Relay                   RelayConfig                   `json:"relay"`

	// LogLevel is the verbosity of the standard logger: one of trace, debug, info,
	// warn/warning, error, fatal or panic, matched case-insensitively by
	// logrus.ParseLevel.
	LogLevel string `json:"log_level" envconfig:"BLNK_LOG_LEVEL"`

	// WebhookDeprecationStartDate is the RFC3339 instant at which the dual-delivery window
	// OPENS. IT IS DERIVED, NOT CONFIGURED: it is always computed as
	// WebhookDeprecationSunsetDate minus exactly WebhookDualDeliveryWindowDays, and any
	// value present on input is overwritten by resolveWebhookDeprecationWindow.
	WebhookDeprecationStartDate string `json:"webhook_deprecation_start_date"`

	// A PUBLISHED CONTRACT VARIABLE, and the ONLY one describing the dual-delivery window.
	WebhookDeprecationSunsetDate string `json:"webhook_deprecation_sunset_date" envconfig:"WEBHOOK_DEPRECATION_SUNSET_DATE"`
}

// HashChainConfig controls the background hash-chainer that seals transaction
// history into a tamper-evident chain. It lives under transaction config and is
// disabled by default.
type HashChainConfig struct {
	Enabled       bool          `json:"enabled"        envconfig:"BLNK_TRANSACTION_HASHCHAIN_ENABLED"`
	PollInterval  time.Duration `json:"poll_interval"  envconfig:"BLNK_TRANSACTION_HASHCHAIN_POLL_INTERVAL"`
	BatchSize     int           `json:"batch_size"     envconfig:"BLNK_TRANSACTION_HASHCHAIN_BATCH_SIZE"`
	TrailingDelay time.Duration `json:"trailing_delay" envconfig:"BLNK_TRANSACTION_HASHCHAIN_TRAILING_DELAY"`
}

func (cnf *Configuration) RemoteMonitoringDSN() string {
	if strings.TrimSpace(cnf.MonitoringDSN) != "" {
		return cnf.MonitoringDSN
	}
	return ""
}

// decodeConfigFile layers the JSON configuration file over cnf when there is one to
// layer, and reports the reason there is not when there is not.
//
// THE ABSENT FILE IS NOT AN ERROR, and it never has been: an environment-only deployment
// is a first-class configuration mode, so a missing path is announced and skipped.
// Two OTHER shapes are treated the same way rather than as a decode failure, because
// neither carries any configuration and both are things a deployment does TO this path
// rather than mistakes in a file's contents:
//
//   - A DIRECTORY. Docker materialises a missing bind-mount source, and for a source with
//     no trailing slash it creates a directory. `./blnk.json:/blnk.json` against a clone —
//     where blnk.json is gitignored and therefore never present — left every process
//     started that way exiting on `read blnk.json: is a directory`, naming a path the
//     operator never created. os.Stat succeeds on a directory and so does os.Open, so
//     only an explicit check separates it from a real file.
//   - AN EMPTY FILE, including one holding nothing but whitespace, which the decoder
//     reports as io.EOF. A configuration file that was created but not yet written says
//     nothing about the deployment, and "EOF" says nothing about the file.
//
// Both are WARNED about with the remedy, not passed over in silence: the operator either
// intended file configuration and has to fix the path, or did not and can remove it.
//
// Parameters:
//   - file string: the configuration file path, from --config or its default.
//   - cnf *Configuration: the struct the file is decoded into. Left untouched when there
//     is nothing to decode.
//
// Returns:
//   - error: a stat failure other than absence, an open failure, or a JSON syntax error.
//     Never an error for an absent, empty or directory path.
func decodeConfigFile(file string, cnf *Configuration) error {
	info, err := os.Stat(file)
	switch {
	case errors.Is(err, os.ErrNotExist):
		logrus.Info("config json not passed, will use env variables")
		return nil
	case err != nil:
		// A path that exists but cannot be described - no execute permission on a parent
		// directory, an I/O error - is a genuine fault and is reported rather than read as
		// absence, because reading it as absence would silently discard the configuration
		// the operator believes is in force.
		return err
	case info.IsDir():
		logrus.WithField("path", file).Warn(
			"the configuration path is a DIRECTORY, not a file, so no file configuration was " +
				"loaded and this process is configured from its environment alone. A container " +
				"runtime creates a directory when it is asked to bind-mount a file that does not " +
				"exist, so this usually means a mount points at a path the host does not have. " +
				"Remove the directory, and either mount a real file there or configure this " +
				"process through the environment",
		)
		return nil
	}

	f, err := os.Open(file)
	if err != nil {
		return err
	}
	// CLOSED ON EVERY PATH. The handle used to be left open for the life of the process;
	// harmless once, and wrong in a test binary that loads configuration repeatedly.
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			logrus.WithError(closeErr).WithField("path", file).
				Warn("could not close the configuration file after reading it")
		}
	}()

	if err := json.NewDecoder(f).Decode(cnf); err != nil {
		if errors.Is(err, io.EOF) {
			logrus.WithField("path", file).Warn(
				"the configuration file is empty, so no file configuration was loaded and this " +
					"process is configured from its environment alone",
			)
			return nil
		}
		return err
	}

	return nil
}

func loadConfigFromFile(file string) error {
	var cnf Configuration
	if err := decodeConfigFile(file, &cnf); err != nil {
		return err
	}

	// override config from environment variables
	err := envconfig.Process("blnk", &cnf)
	if err != nil {
		return explainEnvProcessError(err)
	}

	// The BLNK_-prefixed aliases of the nested Kafka and relay variables, which
	// envconfig cannot reach on its own. Runs after Process so the prefixed form
	// wins, matching the precedence the top-level sunset variable already has.
	if err = applyPrefixedEnvAliases(&cnf); err != nil {
		return err
	}

	// Then let the conventional BLNK_-prefixed names for the nested Kafka and relay blocks
	// take effect. This runs between the environment overlay and the defaults so that the
	// precedence documented on eventStreamingEnvOverride holds, and so that
	// setKafkaDefaults and setRelayDefaults still see the final configured values.
	if err = applyEventStreamingEnvOverride(&cnf); err != nil {
		return err
	}

	err = cnf.validateAndAddDefaults()
	if err != nil {
		return err
	}

	ConfigStore.Store(&cnf)
	return err
}

// envAliasPrefix is the house prefix every other variable in this file answers to.
const envAliasPrefix = "BLNK_"

// explainEnvProcessError re-states an envconfig parse failure in terms of the variable
// the operator actually set.
func explainEnvProcessError(err error) error {
	var parseErr *envconfig.ParseError
	if !errors.As(err, &parseErr) {
		return err
	}

	// The reported variable IS the one that was set: nothing to explain.
	if _, reported := os.LookupEnv(parseErr.KeyName); reported {
		return err
	}

	// The names an operator may legitimately have used for this field, in the order
	// envconfig itself consults them: the house-prefixed alias applyPrefixedEnvAliases
	// adds, then the bare name the deployment contract mandates. The bare name is
	// recovered from the primary key rather than from a second table, so this cannot drift
	// out of step with the tags.
	bare := bareEnvNameFrom(parseErr.KeyName)
	if bare == "" {
		return err
	}

	for _, candidate := range []string{envAliasPrefix + bare, bare} {
		value, present := os.LookupEnv(candidate)
		if !present {
			continue
		}

		return fmt.Errorf(
			"%s must be a valid %s, got %q (envconfig reports this field under its nested "+
				"name %s): %w",
			candidate, parseErr.TypeName, value, parseErr.KeyName, err,
		)
	}

	return err
}

// bareEnvNameFrom recovers the tag literal from an accumulated envconfig primary key.
func bareEnvNameFrom(key string) string {
	trimmed, hadPrefix := strings.CutPrefix(key, envAliasPrefix)
	if !hadPrefix {
		return ""
	}

	for i, char := range trimmed {
		if char != '_' {
			continue
		}

		accumulated, remainder := trimmed[:i], trimmed[i+1:]
		if accumulated == "" {
			return ""
		}

		if strings.HasPrefix(remainder, accumulated+"_") {
			return remainder
		}
	}

	return ""
}

// applyPrefixedEnvAliases overlays the BLNK_-prefixed alias of every Kafka and relay
// environment variable onto an already-processed Configuration.
func applyPrefixedEnvAliases(cnf *Configuration) error {
	stringAliases := map[string]*string{
		"KAFKA_TOPIC_PREFIX":              &cnf.Kafka.TopicPrefix,
		"KAFKA_SASL_USER":                 &cnf.Kafka.SASLUser,
		"KAFKA_SASL_SECRET":               &cnf.Kafka.SASLSecret,
		"KAFKA_SASL_ADMIN_USER":           &cnf.Kafka.SASLAdminUser,
		"KAFKA_SASL_ADMIN_SECRET":         &cnf.Kafka.SASLAdminSecret,
		"KAFKA_TLS_CA_FILE":               &cnf.Kafka.TLS.CAFile,
		"KAFKA_TLS_CERT_FILE":             &cnf.Kafka.TLS.CertFile,
		"KAFKA_TLS_KEY_FILE":              &cnf.Kafka.TLS.KeyFile,
		"KAFKA_TLS_SERVER_NAME":           &cnf.Kafka.TLS.ServerName,
		"WEBHOOK_DEPRECATION_SUNSET_DATE": &cnf.WebhookDeprecationSunsetDate,
	}
	for name, target := range stringAliases {
		if value, ok := os.LookupEnv(envAliasPrefix + name); ok {
			*target = value
		}
	}

	intAliases := map[string]*int{
		"KAFKA_MIN_PARTITIONS":                        &cnf.Kafka.MinPartitions,
		"KAFKA_REPLICATION_FACTOR":                    &cnf.Kafka.ReplicationFactor,
		"RELAY_MAX_RETRY_ATTEMPTS":                    &cnf.Relay.MaxRetryAttempts,
		"RELAY_RETRY_BASE_BACKOFF_MS":                 &cnf.Relay.RetryBaseBackoffMS,
		"RELAY_RETRY_MAX_BACKOFF_MS":                  &cnf.Relay.RetryMaxBackoffMS,
		"RELAY_EVENT_RETENTION_DAYS":                  &cnf.Relay.EventRetentionDays,
		"RELAY_EVENT_RETENTION_BATCH_SIZE":            &cnf.Relay.EventRetentionBatchSize,
		"RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP": &cnf.Relay.EventRetentionMaxBatchesPerSweep,
		"EVENT_METRICS_SUBSCRIBER_BUDGET":             &cnf.Kafka.MetricsSubscriberBudget,
		"RELAY_SUBSCRIBER_METRICS_BUDGET":             &cnf.Relay.SubscriberMetricsBudget,
	}
	for name, target := range intAliases {
		key := envAliasPrefix + name
		value, ok := os.LookupEnv(key)
		if !ok {
			continue
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("%s must be an integer, got %q: %w", key, value, err)
		}
		*target = parsed
	}

	boolAliases := map[string]*bool{
		"KAFKA_TLS_ENABLED":              &cnf.Kafka.TLS.Enabled,
		"KAFKA_TLS_INSECURE_SKIP_VERIFY": &cnf.Kafka.TLS.InsecureSkipVerify,
		"KAFKA_INSECURE_LOCAL_DEV":       &cnf.Kafka.InsecureLocalDev,
		"KAFKA_ALLOW_PARTITION_GROWTH":   &cnf.Kafka.AllowPartitionGrowth,
		"KAFKA_ALLOW_ADMIN_PRODUCER":     &cnf.Kafka.AllowAdminProducer,
	}
	for name, target := range boolAliases {
		key := envAliasPrefix + name
		value, ok := os.LookupEnv(key)
		if !ok {
			continue
		}
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("%s must be a boolean, got %q: %w", key, value, err)
		}
		*target = parsed
	}

	// Comma-separated, matching how envconfig splits the bare KAFKA_BROKERS. The
	// entries are normalised later by setKafkaDefaults, so a value with spaces after
	// the commas behaves identically whichever name supplied it.
	if value, ok := os.LookupEnv(envAliasPrefix + "KAFKA_BROKERS"); ok {
		cnf.Kafka.Brokers = strings.Split(value, ",")
	}

	return nil
}

func InitConfig(configFile string) error {
	logger()
	return loadConfigFromFile(configFile)
}

func Fetch() (*Configuration, error) {
	config := ConfigStore.Load()
	c, ok := config.(*Configuration)
	if !ok {
		return nil, errors.New("config not loaded from file. Create a json file called blnk.json with your config ")
	}
	return c, nil
}

func (cnf *Configuration) validateAndAddDefaults() error {
	if err := cnf.validateRequiredFields(); err != nil {
		return err
	}

	cnf.setDefaultValues()
	cnf.trimWhitespace()
	cnf.setupRateLimiting()

	// AFTER trimWhitespace, so a key of nothing but spaces is treated as absent rather
	// than accepted as a credential, and BEFORE the secure-mode warning below, so a
	// production deployment that would authenticate to its search index with a publicly
	// known key is refused rather than warned about among other warnings.
	if err := cnf.resolveSearchCredential(); err != nil {
		return err
	}

	// IMMEDIATELY AFTER, and for the same reason: it is the other check whose failure
	// would leave a secret readable. resolveSearchCredential refuses a publicly known
	// search key; this refuses a forwarded-HTTPS declaration that would put a one-time
	// SASL password on a channel established by a header any caller can send. Both are
	// ahead of the secure-mode warning below so a production deployment is refused rather
	// than warned about among other warnings.
	if err := cnf.validateForwardedProtoTrust(); err != nil {
		return err
	}

	if !cnf.Server.Secure {
		logrus.Warn(
			"SECURITY: server.secure is false — API authentication is DISABLED. Do not use this configuration in production.",
		)
	}

	// Validate tokenization secret length (AES-256 requires 32 bytes)
	if len(cnf.TokenizationSecret) > 0 && len(cnf.TokenizationSecret) != 32 {
		logrus.Warn("tokenization secret should be 32 bytes for AES-256 encryption")
	}

	if err := cnf.resolveWebhookDeprecationWindow(); err != nil {
		return err
	}

	cnf.validateRelayRetryWindow()
	cnf.warnOnInsecureKafkaTransport()
	cnf.warnOnUnusableSubscriberBrokers()

	// AFTER setDefaultValues, so the default prefix has been filled and this validates
	// the value that will actually be used, and BEFORE the SASL check so a configuration
	// with two faults reports the one that would silently swallow events first.
	if err := cnf.validateKafkaTopicPrefix(); err != nil {
		return err
	}

	if err := cnf.validateKafkaSASLCredentials(); err != nil {
		return err
	}

	// AFTER the SASL pair check, so the identities validated here are ones that could
	// actually be presented, and last because it is the check whose failure is a
	// privilege-escalation risk rather than a connection failure.
	if err := cnf.validateKafkaPrincipalSeparation(); err != nil {
		return err
	}

	return nil
}

func (cnf *Configuration) validateRequiredFields() error {
	if cnf.DataSource.Dns == "" {
		return errors.New("data source DNS is required")
	}

	if cnf.Redis.Dns == "" {
		return errors.New("redis DNS is required")
	}

	return nil
}

func (cnf *Configuration) setDefaultValues() {
	// Project defaults
	if cnf.ProjectName == "" {
		cnf.ProjectName = "Blnk Server"
		logrus.Warn("project name is empty, setting default name")
	}

	// Server defaults
	if cnf.Server.Port == "" {
		cnf.Server.Port = DEFAULT_PORT
		logrus.WithField("port", DEFAULT_PORT).Warn("port not specified in config, setting default")
	}

	if cnf.Server.MaxUploadSizeMB <= 0 {
		cnf.Server.MaxUploadSizeMB = DEFAULT_MAX_UPLOAD_SIZE_MB
	}

	if cnf.Server.MaxRequestBodySizeMB <= 0 {
		cnf.Server.MaxRequestBodySizeMB = DEFAULT_MAX_REQUEST_BODY_SIZE_MB
	}

	if cnf.Server.UploadURLTimeoutSec <= 0 {
		cnf.Server.UploadURLTimeoutSec = DEFAULT_UPLOAD_URL_TIMEOUT_SEC
	}

	// THE SEARCH CREDENTIAL IS DELIBERATELY NOT DEFAULTED HERE.
	//
	// Set module defaults
	cnf.setLogLevelDefaults()
	cnf.setRedisDefaults()
	cnf.setDatabaseDefaults()
	cnf.setTransactionDefaults()
	cnf.setReconciliationDefaults()
	cnf.setQueueDefaults()
	cnf.setHashChainDefaults()
	cnf.setKafkaDefaults()
	cnf.setRelayDefaults()

	if cnf.EnableTelemetry {
		logrus.Info("telemetry enabled")
	} else {
		logrus.Info("telemetry disabled")
	}

	if cnf.EnableObservability {
		logrus.Info("observability enabled")
	} else {
		logrus.Info("observability disabled")
	}
}

func (cnf *Configuration) setHashChainDefaults() {
	// Enabled stays false unless explicitly turned on.
	if cnf.Transaction.HashChain.PollInterval <= 0 {
		cnf.Transaction.HashChain.PollInterval = 5 * time.Second
	}
	if cnf.Transaction.HashChain.BatchSize <= 0 {
		cnf.Transaction.HashChain.BatchSize = 1000
	}
	if cnf.Transaction.HashChain.TrailingDelay <= 0 {
		cnf.Transaction.HashChain.TrailingDelay = 30 * time.Second
	}
}

func (cnf *Configuration) setTransactionDefaults() {
	if cnf.Transaction.BatchSize == 0 {
		cnf.Transaction.BatchSize = defaultTransaction.BatchSize
	}
	if cnf.Transaction.MaxQueueSize == 0 {
		cnf.Transaction.MaxQueueSize = defaultTransaction.MaxQueueSize
	}
	if cnf.Transaction.MaxWorkers == 0 {
		cnf.Transaction.MaxWorkers = defaultTransaction.MaxWorkers
	}
	if cnf.Transaction.LockDuration == 0 {
		cnf.Transaction.LockDuration = defaultTransaction.LockDuration
	} else if cnf.Transaction.LockDuration > 0 && cnf.Transaction.LockDuration < time.Second {
		cnf.Transaction.LockDuration = cnf.Transaction.LockDuration * time.Second
	}
	if cnf.Transaction.LockWaitTimeout == 0 {
		cnf.Transaction.LockWaitTimeout = defaultTransaction.LockWaitTimeout
	} else if cnf.Transaction.LockWaitTimeout > 0 && cnf.Transaction.LockWaitTimeout < time.Second {
		cnf.Transaction.LockWaitTimeout = cnf.Transaction.LockWaitTimeout * time.Second
	}
	if !cnf.Transaction.EnableCoalescing {
		cnf.Transaction.EnableCoalescing = defaultTransaction.EnableCoalescing
	}
	if cnf.Transaction.IndexQueuePrefix == "" {
		cnf.Transaction.IndexQueuePrefix = defaultTransaction.IndexQueuePrefix
	}
}

func (cnf *Configuration) setReconciliationDefaults() {
	if cnf.Reconciliation.DefaultStrategy == "" {
		cnf.Reconciliation.DefaultStrategy = defaultReconciliation.DefaultStrategy
	}
	if cnf.Reconciliation.ProgressInterval == 0 {
		cnf.Reconciliation.ProgressInterval = defaultReconciliation.ProgressInterval
	}
	if cnf.Reconciliation.MaxRetries == 0 {
		cnf.Reconciliation.MaxRetries = defaultReconciliation.MaxRetries
	}
	if cnf.Reconciliation.RetryDelay == 0 {
		cnf.Reconciliation.RetryDelay = defaultReconciliation.RetryDelay
	}
}

func (cnf *Configuration) setQueueDefaults() {
	if cnf.Queue.TransactionQueue == "" {
		cnf.Queue.TransactionQueue = defaultQueue.TransactionQueue
	}
	if cnf.Queue.WebhookQueue == "" {
		cnf.Queue.WebhookQueue = defaultQueue.WebhookQueue
	}
	if cnf.Queue.IndexQueue == "" {
		cnf.Queue.IndexQueue = defaultQueue.IndexQueue
	}
	if cnf.Queue.InflightExpiryQueue == "" {
		cnf.Queue.InflightExpiryQueue = defaultQueue.InflightExpiryQueue
	}
	if cnf.Queue.InflightCommitQueue == "" {
		cnf.Queue.InflightCommitQueue = defaultQueue.InflightCommitQueue
	}
	if cnf.Queue.NumberOfQueues == 0 {
		cnf.Queue.NumberOfQueues = defaultQueue.NumberOfQueues
	}
	if cnf.Queue.HotQueueName == "" {
		cnf.Queue.HotQueueName = defaultQueue.HotQueueName
	}
	if cnf.Queue.HotQueueConcurrency == 0 {
		cnf.Queue.HotQueueConcurrency = defaultQueue.HotQueueConcurrency
	}
	if cnf.Queue.HotPairTTL == 0 {
		cnf.Queue.HotPairTTL = defaultQueue.HotPairTTL
	} else {
		cnf.Queue.HotPairTTL = cnf.Queue.HotPairTTL * time.Second
	}
	if cnf.Queue.HotPairLockContentionThreshold == 0 {
		cnf.Queue.HotPairLockContentionThreshold = defaultQueue.HotPairLockContentionThreshold
	}
	if cnf.Queue.MaxRetryAttempts == 0 {
		cnf.Queue.MaxRetryAttempts = defaultQueue.MaxRetryAttempts
	}
	if cnf.Queue.MonitoringPort == "" {
		cnf.Queue.MonitoringPort = defaultQueue.MonitoringPort
	}
	if cnf.Queue.WebhookConcurrency == 0 {
		cnf.Queue.WebhookConcurrency = defaultQueue.WebhookConcurrency
	}
	if cnf.Queue.TransactionWorkerConcurrency == 0 {
		cnf.Queue.TransactionWorkerConcurrency = defaultQueue.TransactionWorkerConcurrency
	}
}

// setLogLevelDefaults resolves LogLevel onto the standard logger.
func minimumVisibleLogLevel() logrus.Level {
	level, err := logrus.ParseLevel(MINIMUM_LOG_LEVEL)
	if err != nil {
		return logrus.WarnLevel
	}

	return level
}

// clampLogLevel raises a requested level to the floor that keeps mandatory records
// visible, reporting whether it had to.
func clampLogLevel(requested logrus.Level) (logrus.Level, bool) {
	floor := minimumVisibleLogLevel()
	if requested < floor {
		return floor, true
	}

	return requested, false
}

func (cnf *Configuration) setLogLevelDefaults() {
	raw := strings.TrimSpace(cnf.LogLevel)
	if raw == "" {
		cnf.LogLevel = DEFAULT_LOG_LEVEL

		return
	}

	level, err := logrus.ParseLevel(strings.ToLower(raw))
	if err != nil {
		logrus.WithField("log_level", raw).Warn(
			"the configured log level is not a level logrus recognises, so the current level is kept; " +
				"use one of trace, debug, info, warn, error, fatal or panic (BLNK_LOG_LEVEL, or " +
				"\"log_level\" in blnk.json)",
		)
		cnf.LogLevel = logrus.GetLevel().String()

		return
	}

	// A level quieter than the floor is honoured as far as the floor and no further. The
	// warning is emitted AFTER the level is applied, which is what guarantees it is itself
	// visible: it is a warning, and the level it is announcing is the one that makes
	// warnings emit. Announcing before applying could discard the announcement.
	effective, clamped := clampLogLevel(level)

	// The field records the level the process is RUNNING at, not the one it was asked for.
	cnf.LogLevel = effective.String()
	logrus.SetLevel(effective)

	if clamped {
		logrus.WithFields(logrus.Fields{
			"requested_log_level": level.String(),
			"effective_log_level": effective.String(),
		}).Warn(
			"the configured log level would suppress the event relay's mandatory per-attempt " +
				"publish-failure records, so it has been raised to the minimum Blnk runs at; " +
				"those records are the only evidence of why an event was retried or " +
				"dead-lettered, and discarding them is not a supported configuration " +
				"(BLNK_LOG_LEVEL, or \"log_level\" in blnk.json)",
		)
	}
}

func (cnf *Configuration) setRedisDefaults() {
	if cnf.Redis.PoolSize == 0 {
		cnf.Redis.PoolSize = defaultRedis.PoolSize
	}
	if cnf.Redis.MinIdleConns == 0 {
		cnf.Redis.MinIdleConns = defaultRedis.MinIdleConns
	}
}

func (cnf *Configuration) setDatabaseDefaults() {
	if cnf.DataSource.MaxOpenConns == 0 {
		cnf.DataSource.MaxOpenConns = defaultDatabase.MaxOpenConns
	}
	if cnf.DataSource.MaxIdleConns == 0 {
		cnf.DataSource.MaxIdleConns = defaultDatabase.MaxIdleConns
	}
	if cnf.DataSource.ConnMaxLifetime == 0 {
		cnf.DataSource.ConnMaxLifetime = defaultDatabase.ConnMaxLifetime
	}
	if cnf.DataSource.ConnMaxIdleTime == 0 {
		cnf.DataSource.ConnMaxIdleTime = defaultDatabase.ConnMaxIdleTime
	}
}

func (cnf *Configuration) trimWhitespace() {
	cnf.ProjectName = strings.TrimSpace(cnf.ProjectName)
	cnf.Server.Port = strings.TrimSpace(cnf.Server.Port)
	cnf.DataSource.Dns = strings.TrimSpace(cnf.DataSource.Dns)
	cnf.Redis.Dns = strings.TrimSpace(cnf.Redis.Dns)
}

// UploadDomainWhitelistHosts returns the parsed, trimmed, de-duplicated, lowercase
// list of exact hostnames permitted as targets for URL-based reconciliation
// uploads. Entries may be supplied as bare hostnames ("example.com") or full
// URLs ("https://example.com/path"); the hostname is extracted either way.
func (cnf *Configuration) UploadDomainWhitelistHosts() []string {
	raw := strings.TrimSpace(cnf.Server.UploadDomainWhitelist)
	if raw == "" {
		return nil
	}

	seen := make(map[string]struct{})
	hosts := make([]string, 0, 4)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		// Tolerate entries supplied as full URLs by extracting the hostname.
		var host string
		if strings.Contains(entry, "://") {
			if u, err := url.Parse(entry); err == nil {
				host = u.Hostname()
			}
		} else {
			host = entry
		}
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		hosts = append(hosts, host)
	}
	return hosts
}

// TrustedProxyCIDRs returns the parsed, trimmed, de-duplicated list of proxy addresses
// whose forwarded-for headers the HTTP layer may believe.
//
// Returns:
//   - []string: the proxy addresses, or nil to trust none.
func (cnf *Configuration) TrustedProxyCIDRs() []string {
	raw := strings.TrimSpace(cnf.Server.TrustedProxies)
	if raw == "" {
		return nil
	}

	seen := make(map[string]struct{})
	proxies := make([]string, 0, 4)

	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		if _, duplicate := seen[entry]; duplicate {
			continue
		}

		seen[entry] = struct{}{}
		proxies = append(proxies, entry)
	}

	if len(proxies) == 0 {
		return nil
	}

	return proxies
}

// ForwardedProtoTrustedPeers parses Server.TrustedProxies into the socket-peer matchers
// the forwarded-HTTPS channel is believed on, and reports whether what remains is
// usable.
//
// Returns:
//   - peers []*net.IPNet: the parsed matchers, bare IP literals widened to a
//     single-address network. Nil when nothing usable was declared.
//   - usable bool: whether at least one non-universal matcher was declared. False means
//     the forwarded-HTTPS channel establishes nothing, whatever the header says.
func (cnf *Configuration) ForwardedProtoTrustedPeers() (peers []*net.IPNet, usable bool) {
	declared := cnf.TrustedProxyCIDRs()
	if len(declared) == 0 {
		return nil, false
	}

	peers = make([]*net.IPNet, 0, len(declared))

	for _, entry := range declared {
		if network := parseTrustedPeerEntry(entry); network != nil {
			peers = append(peers, network)
		}
	}

	if len(peers) == 0 {
		return nil, false
	}

	return peers, true
}

// parseTrustedPeerEntry parses one trusted-proxy entry into a network matcher.
func parseTrustedPeerEntry(entry string) *net.IPNet {
	if _, network, err := net.ParseCIDR(entry); err == nil {
		if ones, _ := network.Mask.Size(); ones == 0 {
			// 0.0.0.0/0 and ::/0. Matching every peer is indistinguishable from having declared
			// no allowlist, except that it reads as though a decision was made.
			return nil
		}

		return network
	}

	address := net.ParseIP(entry)
	if address == nil {
		return nil
	}

	// A BARE IP IS A SINGLE-ADDRESS NETWORK, widened here rather than special-cased at the
	// comparison, so the matcher list has one shape and the caller has one loop.
	bits := net.IPv6len * 8
	if address.To4() != nil {
		bits = net.IPv4len * 8
	}

	return &net.IPNet{IP: address, Mask: net.CIDRMask(bits, bits)}
}

// TrustsForwardedProtoFrom reports whether X-Forwarded-Proto may be believed on a
// request whose socket peer is remoteAddr.
//
// Parameters:
//   - remoteAddr string: the peer as http.Request.RemoteAddr gives it, "host:port" or a
//     bare host. It must be the SOCKET peer: a value derived from X-Forwarded-For would
//     let a caller nominate the peer that authorises it.
//
// Returns:
//   - bool: true only when the declaration, a usable allowlist and a matching peer all
//     hold.
func (cnf *Configuration) TrustsForwardedProtoFrom(remoteAddr string) bool {
	if !cnf.Server.TrustForwardedProto {
		return false
	}

	peers, usable := cnf.ForwardedProtoTrustedPeers()
	if !usable {
		return false
	}

	address := peerAddressOf(remoteAddr)
	if address == nil {
		return false
	}

	for _, network := range peers {
		if network.Contains(address) {
			return true
		}
	}

	return false
}

// peerAddressOf extracts the IP from a socket peer address.
func peerAddressOf(remoteAddr string) net.IP {
	candidate := strings.TrimSpace(remoteAddr)
	if candidate == "" {
		return nil
	}

	if host, _, err := net.SplitHostPort(candidate); err == nil {
		candidate = host
	}

	if zone := strings.IndexByte(candidate, '%'); zone >= 0 {
		candidate = candidate[:zone]
	}

	return net.ParseIP(strings.Trim(candidate, "[]"))
}

// validateForwardedProtoTrust refuses, in secure mode, a deployment that has declared
// the forwarded-HTTPS channel without naming the proxies it may be believed from.
func (cnf *Configuration) validateForwardedProtoTrust() error {
	if !cnf.Server.TrustForwardedProto {
		return nil
	}

	if _, usable := cnf.ForwardedProtoTrustedPeers(); usable {
		return nil
	}

	declared := strings.TrimSpace(cnf.Server.TrustedProxies)

	if !cnf.Server.Secure {
		logrus.WithField("trusted_proxies_declared", declared != "").Warn(
			"SECURITY: BLNK_SERVER_TRUST_FORWARDED_PROTO is set but BLNK_SERVER_TRUSTED_PROXIES " +
				"names no usable proxy, so X-Forwarded-Proto would be believed from ANY peer. " +
				"Credential issuance therefore refuses the forwarded-HTTPS channel and answers " +
				"SUBSCRIBER_INSECURE_TRANSPORT. Set BLNK_SERVER_TRUSTED_PROXIES to the CIDR ranges " +
				"of the proxies that terminate TLS in front of this process. This is a warning " +
				"rather than a refusal only because secure mode is off.",
		)

		return nil
	}

	if declared == "" {
		return errors.New(
			"BLNK_SERVER_TRUST_FORWARDED_PROTO is set with secure mode enabled, but " +
				"BLNK_SERVER_TRUSTED_PROXIES is empty. The flag declares that a proxy in front of " +
				"this process owns X-Forwarded-Proto, and that declaration is what allows the " +
				"one-time SASL password from POST /subscribers/{id}/kafka-credentials onto a " +
				"channel this process cannot see for itself. With no allowlist the header is " +
				"believed from any peer, so a caller that reaches this process directly — a pod " +
				"IP, a port-forward, a second Service — can set it and be handed that password. " +
				"Set BLNK_SERVER_TRUSTED_PROXIES to the CIDR ranges of the proxies that terminate " +
				"TLS, or unset BLNK_SERVER_TRUST_FORWARDED_PROTO and terminate TLS in this process " +
				"with BLNK_SERVER_SSL",
		)
	}

	return fmt.Errorf(
		"BLNK_SERVER_TRUST_FORWARDED_PROTO is set with secure mode enabled, but "+
			"BLNK_SERVER_TRUSTED_PROXIES declares no usable proxy range. A universal range "+
			"(0.0.0.0/0 or ::/0) matches every peer, which is the same as declaring no allowlist "+
			"at all, and a malformed entry matches none. Blnk would then believe "+
			"X-Forwarded-Proto from any caller and hand a one-time SASL password to whoever "+
			"reached this process directly. Replace the value with the CIDR ranges of the "+
			"proxies you operate: %q",
		declared,
	)
}

func (cnf *Configuration) setupRateLimiting() {

	if cnf.RateLimit.RequestsPerSecond == nil && cnf.RateLimit.Burst == nil {
		defaultRPS := DEFAULT_RATE_LIMIT_RPS
		defaultBurst := DEFAULT_RATE_LIMIT_BURST

		cnf.RateLimit.RequestsPerSecond = &defaultRPS
		cnf.RateLimit.Burst = &defaultBurst

		logrus.WithFields(logrus.Fields{
			"rps":   defaultRPS,
			"burst": defaultBurst,
		}).Info(
			"rate limiting not configured, using defaults. These are PER-CLIENT limits: set " +
				"BLNK_RATE_LIMIT_RPS and BLNK_RATE_LIMIT_BURST if your callers legitimately exceed " +
				"them, and set BLNK_SERVER_TRUSTED_PROXIES so each caller is limited separately " +
				"rather than all of them sharing your proxy's address",
		)
	}

	if cnf.RateLimit.RequestsPerSecond != nil && cnf.RateLimit.Burst == nil {
		defaultBurst := 2 * int(*cnf.RateLimit.RequestsPerSecond)
		cnf.RateLimit.Burst = &defaultBurst
		logrus.WithField("burst", defaultBurst).Warn("rate limit burst not specified, setting default")
	}
	if cnf.RateLimit.RequestsPerSecond == nil && cnf.RateLimit.Burst != nil {
		defaultRPS := float64(*cnf.RateLimit.Burst) / 2
		cnf.RateLimit.RequestsPerSecond = &defaultRPS
		logrus.WithField("rps", defaultRPS).Warn("rate limit RPS not specified, setting default")
	}
	if cnf.RateLimit.CleanupIntervalSec == nil {
		defaultCleanup := DEFAULT_CLEANUP_SEC
		cnf.RateLimit.CleanupIntervalSec = &defaultCleanup
		logrus.WithField("cleanup_interval_sec", defaultCleanup).
			Warn("rate limit cleanup interval not specified, setting default")
	}
}

// MockConfig sets a mock configuration for testing purposes.
func MockConfig(mockConfig *Configuration) {
	err := mockConfig.validateAndAddDefaults()
	if err != nil {
		logrus.WithError(err).Error("error setting mock config")
		return
	}
	ConfigStore.Store(mockConfig)
}

// logger configures the standard logger before any configuration has been read.
func logger() {
	// Configure logrus defaults
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	raw, ok := os.LookupEnv(envAliasPrefix + "LOG_LEVEL")
	if !ok {
		return
	}

	if level, err := logrus.ParseLevel(strings.ToLower(strings.TrimSpace(raw))); err == nil {
		// Clamped here as well as in setLogLevelDefaults, and for the same reason: this is
		// the level in force for the whole of start-up, including the configuration load
		// itself. Applying an unclamped quiet level here would discard start-up warnings that
		// the later clamp can no longer bring back, since by then they have already been
		// emitted and dropped.
		effective, _ := clampLogLevel(level)
		logrus.SetLevel(effective)
	}
}

// resolveSearchCredential decides what the TypeSense API key becomes when the operator
// supplied none, and REFUSES the one case in which the historical answer was a
// hard-coded credential in a production deployment.
func (cnf *Configuration) resolveSearchCredential() error {
	// TrimSpace rather than a bare comparison: a variable set to whitespace is an operator
	// who meant to supply a value and did not, and treating it as a credential would let the
	// search client authenticate with " " and fail somewhere far less legible.
	if strings.TrimSpace(cnf.TypeSenseKey) != "" {
		return nil
	}

	if !cnf.Server.Secure {
		cnf.TypeSenseKey = DEFAULT_TYPESENSE_KEY

		return nil
	}

	if strings.TrimSpace(cnf.TypeSense.Dns) == "" {
		// Left EMPTY on purpose. An empty key with an empty host is an honest description of
		// a deployment that does not use search, and it is what keeps this from being an
		// invented credential sitting in memory waiting for a host to be configured later.
		logrus.Warn(
			"BLNK_TYPESENSE_KEY is not set and secure mode is enabled, so no search credential " +
				"has been assumed. No TypeSense host is configured either, so search is not in use " +
				"and this is not a fault. Set BLNK_TYPESENSE_KEY before configuring " +
				"BLNK_TYPESENSE_DNS: the public default this used to fall back to is not a secret.",
		)

		return nil
	}

	return errors.New(
		"BLNK_TYPESENSE_KEY is required when secure mode is enabled and a TypeSense host is " +
			"configured. It used to fall back to a built-in default, but that default is the " +
			"literal published in this repository's compose files and README, so it is known to " +
			"everyone and grants full access to the search collection holding indexed transaction, " +
			"balance and identity records. Set BLNK_TYPESENSE_KEY to the api-key your TypeSense " +
			"deployment was started with, or unset BLNK_TYPESENSE_DNS if you are not using search",
	)
}
