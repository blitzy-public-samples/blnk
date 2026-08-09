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
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/internal/logsafe"
)

// Default constants
const (
	DEFAULT_PORT        = "5001"
	DEFAULT_CLEANUP_SEC = 10800 // 3 hours in seconds
	// DEFAULT_TYPESENSE_KEY is the api-key a LOCAL TypeSense is started with, and it is
	// applied only when secure mode is off. It is emphatically NOT a secret — it is published
	// in this repository's compose files and README — which is why resolveSearchCredential
	// refuses to substitute it in a production posture. See SEC-07 there.
	DEFAULT_TYPESENSE_KEY = "blnk-api-key"
	// DEFAULT_RATE_LIMIT_RPS and DEFAULT_RATE_LIMIT_BURST are the PER-CLIENT request rate a
	// deployment that configures none is held to. SEC-14.
	//
	// # Why the previous numbers were not a limit
	//
	// They were 5,000,000 requests per second with a burst of 10,000,000. tollbooth keys its
	// limiter per client address, and no single client can issue five million requests a second
	// against one Blnk instance, so the limiter never engaged: rate limiting was configured,
	// reported as configured, and had no effect. That is CWE-770 — a control that exists on
	// paper only, which is worse than none, because it reads as present in every review.
	//
	// # Where these numbers come from
	//
	// 2,000 per second is FOUR TIMES the throughput the project's own acceptance criterion
	// states (AAP V-1: 500 events per second sustained), so it cannot throttle a deployment
	// operating at the volume Blnk is specified for, nor the k6 harness that measures it. The
	// burst is twice the rate, which is the same relationship this function already applies
	// when only one of the pair is supplied — so a reader finds one rule rather than two.
	//
	// # Why not tighter
	//
	// The limit is per CLIENT ADDRESS, and a ledger's callers are usually backend services
	// behind a proxy or NAT. Unless BLNK_SERVER_TRUSTED_PROXIES is set, every request keys to
	// the proxy's address, so a per-client limit behaves as a GLOBAL one — and a tight default
	// would then throttle an entire deployment rather than one abusive caller. Finite and
	// generous is the correct default; the operator tightens it once the client identity is
	// resolvable.
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
	// DEFAULT_LOG_LEVEL is the verbosity a deployment runs at when it states none,
	// and it is logrus's own default spelled out rather than a new choice: filling
	// the field is about making the effective level READABLE, not about changing it.
	//
	// Info is the right production level because the level below it is used by the
	// event pipeline for per-event diagnostics — one line per published event, which
	// is five hundred lines a second at the throughput this feature is built for.
	// Those lines exist to be turned on for an investigation, not to be shipped.
	DEFAULT_LOG_LEVEL = "info"

	// MINIMUM_LOG_LEVEL is the LEAST verbose level Blnk will actually run at,
	// whatever a deployment configures. A quieter request is honoured as far as this
	// and no further.
	//
	// # Why a floor exists at all
	//
	// One class of record in this codebase is mandatory rather than discretionary.
	// The event relay must log the attempt count and the error reason on EVERY failed
	// publish attempt — not only on the last one — because that per-attempt trail is
	// the only evidence of why an event was delayed or eventually dead-lettered, and
	// the retry schedule is otherwise invisible: a row that succeeds on its fourth
	// attempt looks identical afterwards to one that succeeded on its first.
	//
	// Those records are warnings, which is the correct severity for "this failed and
	// will be retried". logrus emits an entry only when the logger's level is at
	// least as verbose as the entry's, so a deployment setting error, fatal or panic
	// discards every one of them. The requirement and the configuration surface were
	// therefore in direct contradiction: the quiet levels did not reduce noise, they
	// deleted the audit trail, silently and with no indication in the logs that
	// anything had been withheld.
	//
	// # Why the floor rather than a separate sink
	//
	// The alternative is a dedicated logger whose level cannot be configured. It was
	// rejected: logrus fires hooks only AFTER the level check, so a second logger does
	// not escape level gating, it merely moves it — and it would take the mandatory
	// records out of the standard logger that every hook, formatter and log-capturing
	// test in this repository is attached to. The result would be a compliance record
	// that no existing tooling could see, which is a worse failure than the one being
	// fixed.
	//
	// # What is actually given up
	//
	// Exactly three settings — error, fatal and panic — and only their effect on
	// warnings. Every level from warn upwards behaves precisely as before, and warn
	// itself still suppresses the per-event info and debug diagnostics that make up
	// almost all of the pipeline's volume. A deployment wanting less than warn is
	// asking not to be told that its event delivery is failing.
	//
	// Spelled "warning" rather than "warn" because that is what logrus.Level.String()
	// returns, and this constant is compared against the normalised LogLevel field.
	// logrus.ParseLevel accepts both spellings, so an operator may write either.
	MINIMUM_LOG_LEVEL = "warning"
)

// WebhookDualDeliveryWindowDays is the exact length of the window in which Kafka
// publishing and legacy HTTP webhook delivery run side by side, in days.
//
// It is a constant rather than a configurable value because the length is part of
// the deprecation contract published to subscribers, not an operational knob: a
// deployment that quietly shortened it would retire a transport subscribers were
// told they had until a stated date to move off. Operators choose WHEN the window
// opens; they do not choose how long it lasts.
const WebhookDualDeliveryWindowDays = 30

// webhookDualDeliveryWindow is WebhookDualDeliveryWindowDays as a duration, used to
// derive the sunset instant from the window's start and to verify a pair of
// explicitly configured dates really is that far apart.
const webhookDualDeliveryWindow = WebhookDualDeliveryWindowDays * 24 * time.Hour

// WebhookDualDeliveryWindow returns the window length as a duration.
//
// The dual-delivery decision lives in package blnk, which needs the same span this
// package derives and validates against. Exporting the duration — rather than letting
// the caller multiply WebhookDualDeliveryWindowDays out for itself — keeps one
// definition of "exactly 30 days" for both the configuration that refuses a
// mis-stated window and the predicate that decides whether an event is inside one.
//
// Returns:
//   - time.Duration: WebhookDualDeliveryWindowDays expressed as a duration.
func WebhookDualDeliveryWindow() time.Duration {
	return webhookDualDeliveryWindow
}

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

	// defaultKafka deliberately leaves Brokers and all four SASL fields zero-valued.
	// An empty broker list is a legitimate steady state, not an error: it selects the
	// no-op event publisher, reproducing the historic no-op-when-unconfigured
	// contract so a deployment without Kafka keeps working. The SASL fields are
	// credentials and must never carry a shipped default. ReplicationFactor defaults
	// to 3 for production durability; a single-broker cluster must set it to 1
	// explicitly, which setKafkaDefaults preserves because it only fills zero values.
	//
	// TLS.Enabled and InsecureLocalDev are both false by default, which is not a
	// contradiction: it is the ONE combination that refuses to build a Kafka client
	// at all. A deployment must choose, in writing, between verified TLS and an
	// explicitly acknowledged local-dev plaintext connection — neither can be
	// arrived at by leaving a variable unset.
	defaultKafka = KafkaConfig{
		TopicPrefix:       "blnk",
		MinPartitions:     6,
		ReplicationFactor: 3,
		// Far above any plausible subscriber count for a single ledger deployment and far
		// below the point at which either cost it bounds — tick duration and exported series
		// — begins to matter. See KafkaConfig.MetricsSubscriberBudget.
		MetricsSubscriberBudget: 200,
	}

	// defaultRelay encodes the bounded exponential backoff schedule requirement R-4
	// mandates: five publish attempts and the delay sequence 1s, 2s, 4s, 8s, 16s,
	// capped at 30s.
	//
	// The relay produces and durably records EVERY ONE of those five delays — each is
	// stamped on the row's next_attempt_at by the same statement that decides whether
	// the budget is spent. Five attempts have four gaps between them, so the delays a
	// retried event actually WAITS are the first four, 15s in total, and the fifth is
	// recorded on the attempt that spends the budget rather than separating two
	// publishes. Raising MaxRetryAttempts is what turns it into a wait as well.
	//
	// The 30s cap is out of reach with these parameters, which is intentional: it
	// engages only when the configured base or attempt count is raised.
	defaultRelay = RelayConfig{
		MaxRetryAttempts:   MaxRelayRetryAttempts,
		RetryBaseBackoffMS: 1000,
		RetryMaxBackoffMS:  30000,

		EventRetentionBatchSize:          DefaultEventRetentionBatchSize,
		EventRetentionMaxBatchesPerSweep: DefaultEventRetentionMaxBatchesPerSweep,

		// Deliberately the same number as defaultKafka.MetricsSubscriberBudget, and kept in
		// step with blnk.DefaultSubscriberMetricsBudget, which is the value the collector
		// falls back to when it cannot read a configuration at all. These are two published
		// spellings of ONE ceiling; setRelayDefaults reconciles them onto a single value.
		SubscriberMetricsBudget: 200,
	}
)

// The shipped purge capacity, EXPORTED because the sweeper in the root package needs the same
// two numbers as its fallback for a configuration it cannot read, and two copies of a default
// are two numbers that can disagree. Which one a deployment ran at would then depend on
// start-up ordering, and nothing would report the difference.
//
// PERF-P23: 2,000 x 1,000 = 2,000,000 rows an hour, which EXCEEDS the 1,800,000 an hour that
// 500 events a second produces. Capacity below ingestion does not slow the table's growth, it
// permits it, and the retention period is then never actually enforced however short it is. So
// the default has to clear peak rather than approach it. See RelayConfig for the full
// arithmetic and for why the previous fixed ceiling of 100 batches failed it.
const (
	// DefaultEventRetentionBatchSize is how many rows one DELETE statement removes by default.
	// It is the lock-footprint factor: small keeps each statement's row locks and WAL
	// contribution light on a table the relay is concurrently claiming from.
	DefaultEventRetentionBatchSize = 1000

	// DefaultEventRetentionMaxBatchesPerSweep is how many such statements one sweep issues by
	// default. It is the throughput factor, and it is the safer of the two to raise.
	DefaultEventRetentionMaxBatchesPerSweep = 2000
)

// MaxRelayRetryAttempts is the CEILING on RELAY_MAX_RETRY_ATTEMPTS, not merely its
// default. A configured value above it is clamped down to it by setRelayDefaults.
//
// It is a hard bound rather than advice because the attempt number is an EXPORTED METRIC
// LABEL. blnk.events.publish.duration carries the attempt as an attribute and multiplies it
// by its bucket count, and blnk.events.publish.attempts.total is read per attempt, so the
// attempt domain is part of the observability contract: it is documented as 1..5 in
// internal/metrics, the alert queries select on attempt="1", and a deployment that set 5000
// here would silently mint 5000 label values and make the histogram the most expensive
// series in the exporter.
//
// Five is also the retry budget requirement itself, so clamping does not restrict any
// supported configuration — it only rejects one that was never valid. Clamping rather than
// refusing to start is deliberate and matches how every other bad value in this file is
// handled: a misconfigured relay must never stop the ledger from serving, so the value is
// corrected, the correction is logged loudly, and the process continues.
//
// The recording path in the root package bounds the label independently as well, collapsing
// anything above this into a single "over" bucket, so the domain stays closed even for a row
// whose per-row max_attempts was raised directly in the database.
const MaxRelayRetryAttempts = 5

// EventRetentionUnboundedSweep is the value of RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP
// that removes the per-sweep batch ceiling entirely. PERF-P23.
//
// It exists because zero cannot carry that meaning. An unset int field IS zero, so a zero
// ceiling is indistinguishable from a deployment that never mentioned the setting, and only
// one of those two readings can be honoured. The bound is taken as the safe reading — it is
// what stops the first sweep after retention is enabled from attempting the entire historical
// backlog in one pass on a table the relay is claiming from — so zero defaults and "no
// ceiling" needs a value of its own.
//
// A sweep with no ceiling is still bounded by the sweep timeout, so this means "delete as much
// as you can in one sweep" rather than "run for as long as it takes". It is the setting for a
// deliberate one-off catch-up, not a steady state.
const EventRetentionUnboundedSweep = -1

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
	// Empty/unset means deny-by-default: every URL upload is rejected. Listing a
	// bare IP literal or "localhost" is unsafe and defeats the SSRF guard.
	UploadDomainWhitelist string `json:"upload_whitelist"       envconfig:"BLNK_UPLOAD_DOMAIN_WHITELIST"`
	// UploadURLTimeoutSec caps the HTTP GET issued for a URL-based upload
	// (BLNK_UPLOAD_URL_TIMEOUT_SEC). Defaults to DEFAULT_UPLOAD_URL_TIMEOUT_SEC.
	UploadURLTimeoutSec int `json:"upload_url_timeout_sec" envconfig:"BLNK_UPLOAD_URL_TIMEOUT_SEC"`

	// TrustForwardedProto declares that every request reaching this process has
	// passed through a proxy — an ingress controller, a load balancer, a service
	// mesh sidecar — that TERMINATED TLS and that sets X-Forwarded-Proto,
	// overwriting any value a client supplied
	// (BLNK_SERVER_TRUST_FORWARDED_PROTO).
	//
	// It exists because one endpoint returns a secret: POST
	// /subscribers/{id}/kafka-credentials answers with a one-time SASL password,
	// and the request carrying it is the only chance to disclose that password to
	// anything watching the wire. When Blnk terminates TLS itself (SSL above) the
	// process can see that for itself. When TLS terminates in front of it — the
	// normal Kubernetes shape, where the hop from ingress to pod is plaintext — it
	// cannot, and X-Forwarded-Proto is a header any client can set, so believing it
	// unconditionally would let a caller assert its own confidentiality. This flag
	// is the deployment stating, once and explicitly, that the header is now under
	// the proxy's control and may be believed.
	//
	// Default false, which is deny-by-default: with no in-process TLS and no
	// declaration, credential issuance refuses with
	// SUBSCRIBER_INSECURE_TRANSPORT rather than putting a password on a channel
	// nobody has established as confidential. A loopback caller is still served,
	// because those bytes never leave the host.
	//
	// SETTING THIS WITHOUT SUCH A PROXY IS UNSAFE: it re-enables exactly the
	// disclosure the default prevents, and it does so silently. Nothing else in
	// Blnk's behaviour depends on it.
	TrustForwardedProto bool `json:"trust_forwarded_proto" envconfig:"BLNK_SERVER_TRUST_FORWARDED_PROTO"`
	// TrustedProxies is a comma-separated list of CIDR blocks or bare IP literals whose
	// X-Forwarded-For and X-Real-IP headers may be BELIEVED
	// (BLNK_SERVER_TRUSTED_PROXIES).
	//
	// Empty/unset means trust NOTHING, which is the safe default and the one this
	// repository's manifests deploy: the client address resolves to the peer that
	// actually opened the connection, which a caller cannot forge. It is deliberately
	// the same deny-by-default posture as UploadDomainWhitelist above.
	//
	// # Why the default has to be "trust nothing" rather than "trust everything"
	//
	// Gin's own default is to trust every proxy, which means it believes the leftmost
	// X-Forwarded-For value on any request. Any client can then choose the address that
	// appears in the access log and in anything built on it, so a log becomes evidence of
	// what the caller claimed rather than of what happened. Setting this to a real proxy
	// range is what makes a forwarded address trustworthy again.
	//
	// Set it ONLY to the addresses of proxies you operate. Listing 0.0.0.0/0 restores the
	// forgeable behaviour and defeats the point.
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
	// private addresses (SSRF-01).
	//
	// # Why a flag exists at all
	//
	// The legacy transport refuses internal destinations at dial time, which is what
	// stops a redirect or a rebound DNS answer from turning a webhook into a request
	// against Blnk's own Postgres, Redis, broker or cloud metadata endpoint. But two
	// entirely legitimate deployments are internal by nature: an on-premise install
	// delivering to https://webhooks.corp:8443 behind RFC1918, and a developer
	// delivering to a loopback sink. Refusing those outright would not make anyone safer,
	// it would make the deny-list something operators route around.
	//
	// # What it can and cannot open
	//
	// It is NOT a switch that turns the guard off, and it is deliberately not named as
	// one. It opens exactly the two tiers an operator can plausibly own — loopback and
	// private/ULA — and it opens http. It CANNOT open the ranges no configuration should
	// reach: link-local (169.254.169.254), the unspecified address, multicast, the
	// carrier-grade NAT range, the benchmarking and protocol-assignment ranges, or an
	// internal address smuggled through the NAT64 well-known prefix. Those stay refused
	// with this set to true. Redirects likewise stay refused unconditionally, because a
	// webhook POST has no legitimate reason to be redirected and following one is the
	// exact vector this closes.
	//
	// Default false. Enabling it logs a warning naming what it permitted, so the
	// assertion is visible in the log of any deployment that made it.
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

// KafkaConfig configures the Kafka event-publishing pipeline: the brokers the
// producer and admin client dial, the prefix every category and dead-letter topic
// is derived from, the SASL/SCRAM administrative principal used to provision
// subscriber credentials and ACLs, and the topic geometry applied when topics are
// created or grown.
//
// Note on the environment variable names: unlike every other struct in this file,
// these tags carry no BLNK_ prefix, because the deployment contract mandates the
// bare names (KAFKA_BROKERS, KAFKA_TOPIC_PREFIX, ...). Do NOT add one.
//
// Those bare names are honoured through envconfig's alternate-key fallback. For
// each field envconfig derives a primary key by accumulating the prefix through
// every enclosing struct, and uses the raw tag literal as an alternate key that is
// consulted only when the primary is unset. Because Configuration.Kafka is itself a
// prefix segment, the primary key envconfig derives here is BLNK_KAFKA_<TAG> — for
// example BLNK_KAFKA_KAFKA_BROKERS, not BLNK_KAFKA_BROKERS.
//
// BOTH FORMS RESOLVE ANYWAY. The house convention across this file is that a
// setting also answers to its BLNK_-prefixed name, and a deployment that writes
// BLNK_KAFKA_BROKERS out of habit must not silently select default behaviour. So
// after envconfig has run, the ordinary BLNK_-prefixed alias of EVERY variable in
// this struct and in RelayConfig is overlaid, and the prefixed form wins when both
// are set — the same precedence the top-level WebhookDeprecationSunsetDate already
// has, where envconfig itself provides it.
//
// TWO MECHANISMS DO THAT OVERLAY, split by type rather than by policy, and adding a
// field here means adding it to whichever one fits:
//
//   - eventStreamingEnvOverride, a flat struct envconfig processes a second time.
//     Everything it carries is resolved by the library, so a LIST splits on commas and
//     a number is parsed and reported by the library naming the variable. Lists belong
//     here; so do the contract variables.
//   - applyPrefixedEnvAliases, three explicit string/int/bool tables. Everything else.
//
// A field in NEITHER answers only to its bare name and to envconfig's own
// BLNK_KAFKA_<TAG> artefact, which is the gap this comment used to describe as covered
// when it was not.
//
// Brokers and the four SASL fields have no defaults by design — see defaultKafka.
//
// # THE DEPLOYMENT CONTRACT, AND WHAT IS AN ADDITION TO IT
//
// Requirement R-10 freezes the deployment contract at EIGHT environment variables. Four
// of them are declared in this struct and are marked below; the other four are
// RELAY_MAX_RETRY_ATTEMPTS, RELAY_RETRY_BASE_BACKOFF_MS and RELAY_RETRY_MAX_BACKOFF_MS
// in RelayConfig, and WEBHOOK_DEPRECATION_SUNSET_DATE on Configuration.
//
// Every other variable in this struct is a SUPPLEMENTARY SETTING. The distinction is
// recorded in the source rather than left to a document because the two have different
// standing: the eight are a contract with the deployment and must not change name,
// default or meaning, while a supplementary setting is Blnk's own and may be revised.
// Each supplementary setting below names the reason it exists, and none of them is
// required for a working deployment — every one has a safe default or a documented
// fallback, so the eight remain sufficient on their own.
//
// The supplementary settings, and why each is not merely convenience:
//
//   - KAFKA_SASL_USER / KAFKA_SASL_SECRET — the least-privilege producer principal.
//     Without them the publisher must authenticate as the administrator, which puts
//     topic creation, credential minting and ACL rewriting in the hands of the busiest
//     process in the deployment.
//   - KAFKA_TLS_* — transport security. SASL/SCRAM without TLS sends ledger and
//     identity payloads over a channel an observer can read.
//   - KAFKA_INSECURE_LOCAL_DEV — the single, explicit acknowledgement that lets the
//     local stack run without TLS, so that not having it is never the silent default.
//   - KAFKA_MIN_PARTITIONS / KAFKA_REPLICATION_FACTOR — topic geometry. AAP §0.5.2
//     sanctions both as fields, and §0.4.5 requires the replication factor in
//     particular to be configuration-driven: a single-broker local stack cannot
//     satisfy the production factor of 3, so hardcoding either value breaks one
//     environment or the other.
//   - KAFKA_ALLOW_PARTITION_GROWTH — the explicit consent adding partitions to a
//     non-empty topic requires, because doing so changes which partition a key lands
//     on and therefore breaks per-aggregate ordering for keys already in flight.
type KafkaConfig struct {
	// Brokers and TopicPrefix are R-10 CONTRACT VARIABLES.
	Brokers     []string `json:"brokers"      envconfig:"KAFKA_BROKERS"`
	TopicPrefix string   `json:"topic_prefix" envconfig:"KAFKA_TOPIC_PREFIX"`

	// HistoricalTopicPrefixes lists topic namespaces this deployment USED TO OWN and
	// must still be able to publish to. It is empty in every deployment that has never
	// renamed its namespace, which is almost all of them.
	//
	// # Why renaming the prefix needs this, and what happened without it
	//
	// An outbox row records its fully-resolved destination topic at INSERT time, so that
	// a committed event stays bound to the topic it was always meant for. Change
	// KAFKA_TOPIC_PREFIX from "blnk" to "acme" and the rows already in the table still
	// name blnk.transactions, while the publisher, the topic-assurance pass and the
	// ownership test all now speak of acme.transactions.
	//
	// A running process survived that, because it had pre-created a writer for every
	// blnk.* topic at start-up and served those rows from that inventory. A RESTART did
	// not: the new process pre-created acme.* only, and every stored blnk.* row then
	// failed the ownership test at writer resolution. Those rows are not lost — they stay
	// claimable and each failure names the topic — but nothing drains them, and the same
	// refusal strands the dead-letter write of a failing row and the replay of an already
	// dead-lettered one. A prefix rename plus a rolling restart is an ordinary
	// maintenance operation, and it silently stopped delivery of every event captured
	// before it.
	//
	// # What listing a prefix here does
	//
	// Every name in this list is treated as owned, exactly as TopicPrefix is: writers are
	// pre-created for its whole topic inventory, the ownership test admits it, topic
	// assurance keeps its topics present, and the metrics layer reports its topic names
	// verbatim instead of collapsing them to the "unowned" label. It does NOT change
	// where NEW events go — TopicPrefix alone decides that — so the list is drained
	// rather than accumulated: once no non-terminal and no replayable row names a prefix,
	// remove it.
	//
	// It is an explicit allowlist rather than a blanket "any prefix with a known
	// category" rule on purpose. That looser rule would also admit
	// "attacker.transactions", which has precisely the same shape as a real topic name,
	// and writer resolution is the one place a stored string turns into an outbound
	// connection carrying Blnk's own producer credentials. An operator naming the
	// namespaces this deployment actually owned is the only source that can tell the two
	// apart.
	//
	// Blank entries are dropped and duplicates — including a repeat of TopicPrefix — are
	// collapsed, so a trailing comma or a value left in place after the rename completed
	// is harmless. The count is bounded by MaxHistoricalTopicPrefixes, because each
	// prefix multiplies the pre-created writer inventory and an unbounded list would be a
	// memory cost paid on every process start for topics nothing writes to.
	HistoricalTopicPrefixes []string `json:"historical_topic_prefixes" envconfig:"KAFKA_HISTORICAL_TOPIC_PREFIXES"`

	// SubscriberBrokers is the SUBSCRIBER-FACING bootstrap list, and it is a DIFFERENT
	// LIST FROM Brokers rather than a convenience alias for it.
	//
	// Brokers is what Blnk itself dials, and inside a deployment that is an internal
	// address: "kafka:9092" on a compose network, a headless or ClusterIP Service name in
	// Kubernetes. Neither resolves for a subscriber outside the deployment, and Kafka
	// compounds it — a broker answers every client with the ADVERTISED address of the
	// listener the connection arrived on, so even an external address that does resolve
	// leads to a bootstrap that hands back internal ones. A subscriber must therefore be
	// given the addresses of the externally advertised listener, which only an operator
	// can know.
	//
	// So POST /subscribers/{id}/kafka-credentials reports this list when it is set. It is an
	// OPTIONAL OVERRIDE rather than a prerequisite: with it empty, issuance reports Brokers
	// and logs that it did. Requirement R-10 fixes the mandatory configuration surface at
	// eight variables, and this is not one of them — making it mandatory meant a deployment
	// satisfying the entire documented contract could run the relay and still receive 503
	// from every issuance request, which is a required endpoint disabled by an undocumented
	// setting.
	//
	// The warning is what the refusal used to be. An in-cluster subscriber needs no override
	// at all; an external one needs this variable, and both the start-up log and every
	// issuance say so while the endpoint keeps working. See SubscriberFacingBrokers.
	SubscriberBrokers []string `json:"subscriber_brokers" envconfig:"KAFKA_SUBSCRIBER_BROKERS"`

	// SUPPLEMENTARY (least privilege). SASLUser and SASLSecret are the STEADY-STATE
	// PRODUCER principal: the identity
	// the event publisher in the server and worker processes authenticates as. It
	// needs only Write and Describe on the topics Blnk owns.
	//
	// It is deliberately separate from the administrative principal below. A producer
	// process that authenticates as the administrator holds authority to create
	// topics, mint SCRAM credentials and rewrite ACLs, so compromising the busiest,
	// most exposed process in the deployment would hand over the whole cluster's
	// authorization state. Least privilege is the point: configure these two, and the
	// admin credentials never leave the provisioning path.
	//
	// There is NO fallback to the administrative principal. When these are empty and
	// the administrative pair is not, ProducerSASL reports adminOnly and the publisher
	// REFUSES to build, returning ErrProducerPrincipalRequired. That path used to
	// downgrade to the administrative pair and log an excess-privilege warning, which
	// recorded the problem without preventing it — and a warning nobody reads is how a
	// temporary allowance becomes the permanent configuration. See ProducerSASL.
	//
	// Both pairs empty is a different, legitimate case: a broker requiring no
	// authentication, for which no SASL mechanism is built and no error is raised.
	//
	// scripts/kafka-provision.sh creates this principal for the local stack and grants
	// it Write and Describe on the Blnk-owned topics only.
	SASLUser   string `json:"sasl_user"   envconfig:"KAFKA_SASL_USER"`
	SASLSecret string `json:"sasl_secret" envconfig:"KAFKA_SASL_SECRET"`

	// SASLAdminUser and SASLAdminSecret are R-10 CONTRACT VARIABLES, and are the
	// ADMINISTRATIVE principal: used only for topic assurance, subscriber SCRAM
	// credential provisioning, ACL grants and revocation, and the offset/lag reads
	// behind reconciliation. Keep them out of every process that only publishes.
	SASLAdminUser   string `json:"sasl_admin_user"   envconfig:"KAFKA_SASL_ADMIN_USER"`
	SASLAdminSecret string `json:"sasl_admin_secret" envconfig:"KAFKA_SASL_ADMIN_SECRET"`

	// SUPPLEMENTARY (topic geometry). Sanctioned as fields by AAP §0.5.2; the
	// replication factor must be configuration-driven per §0.4.5, because a
	// single-broker local stack cannot satisfy the production factor of 3.
	MinPartitions     int `json:"min_partitions"     envconfig:"KAFKA_MIN_PARTITIONS"`
	ReplicationFactor int `json:"replication_factor" envconfig:"KAFKA_REPLICATION_FACTOR"`

	// TLS carries the transport-security settings both the producer and the
	// administrative client dial with. See KafkaTLSConfig.
	TLS KafkaTLSConfig `json:"tls"`

	// InsecureLocalDev permits an UNENCRYPTED Kafka connection.
	//
	// Ledger events carry financial amounts and identity events carry names, email
	// addresses, phone numbers, addresses and dates of birth, so an unencrypted
	// broker connection exposes exactly the data this system exists to protect. Both
	// Kafka clients therefore REFUSE to dial without TLS unless this flag is
	// explicitly set, which is what makes plaintext an opt-in rather than the
	// accident of an unset variable.
	//
	// It exists because the local single-broker KRaft stack listens on
	// SASL_PLAINTEXT, and only for that. Setting it in production defeats the
	// protection; it is logged as a warning on every configuration load so that its
	// presence in a real deployment cannot go unnoticed.
	// SUPPLEMENTARY (explicit local-dev acknowledgement).
	InsecureLocalDev bool `json:"insecure_local_dev" envconfig:"KAFKA_INSECURE_LOCAL_DEV"`

	// AllowPartitionGrowth permits the topic-assurance pass to raise the partition
	// count of a topic that ALREADY HOLDS MESSAGES.
	//
	// Growing a live topic re-maps keys to partitions — a key hashed into partition 2
	// of six lands somewhere else out of twelve — so one aggregate's history is split
	// across two partitions and its events can be consumed out of order. That breaks
	// the per-aggregate ordering guarantee irreversibly for every key already
	// written. Assurance therefore refuses to grow a non-empty topic and reports it
	// instead, unless an operator has planned the migration and set this flag.
	//
	// An EMPTY topic is grown regardless of this flag: with no records written there
	// is no mapping to preserve.
	// SUPPLEMENTARY (explicit consent for a reordering-unsafe operation).
	AllowPartitionGrowth bool `json:"allow_partition_growth" envconfig:"KAFKA_ALLOW_PARTITION_GROWTH"`

	// MetricsSubscriberBudget caps how many registry rows ONE consumer-lag sweep examines.
	//
	// # Why it is a knob and not a constant
	//
	// The periodic collector measures each subscriber's lag with two broker round trips per
	// authorised topic, so an unbounded registry could make one tick outlast the collection
	// interval; and each subscriber-topic pair is an exported gauge series, so the registry's
	// size is also the series count. A budget is therefore necessary. Hard-coding it is not:
	// a deployment with more subscribers than the budget had every one beyond it measured on a
	// LATER tick at best, and the only remedy was a rebuild.
	//
	// The sweep resumes from a rotating cursor rather than restarting at the newest row, so a
	// registry larger than the budget is covered across successive ticks instead of leaving a
	// permanent hole. Raising this value is what compresses that coverage into a single tick —
	// set it above the registry size and every subscriber is measured every tick, which is what
	// blnk.kafka.consumer_lag_inventory_complete reports.
	//
	// Defaulted to 200 by setKafkaDefaults and clamped to MaxMetricsSubscriberBudget, because
	// the two costs it bounds are real: the value is not a preference about strictness but a
	// limit on tick duration and exported series.
	MetricsSubscriberBudget int `json:"metrics_subscriber_budget" envconfig:"EVENT_METRICS_SUBSCRIBER_BUDGET"`

	// AllowAdminProducer permits the event publisher to authenticate with the
	// ADMINISTRATIVE credentials when no dedicated producer principal is configured.
	//
	// The publisher's own principal is SASLUser/SASLSecret. When those are empty the
	// only other credential in the configuration is the administrative pair — the
	// principal that creates topics, alters SCRAM credentials and grants or revokes
	// ACLs. Publishing every ledger event as that principal makes a leaked producer
	// credential a full compromise of the cluster's authorization state rather than
	// the ability to publish events, and it erases the audit distinction between
	// routine publishing and administration.
	//
	// So the fallback is REFUSED by default: a deployment that has brokers but no
	// producer principal fails at start-up, naming KAFKA_SASL_USER and
	// KAFKA_SASL_SECRET, rather than quietly running at maximum privilege. This flag
	// is the deliberate, documented escape hatch for the one case that justifies it —
	// an existing deployment mid-upgrade that has not provisioned its producer
	// principal yet and must keep publishing while it does. It warns on every
	// publisher construction, because a compatibility path nobody is reminded of
	// becomes the permanent configuration.
	//
	// Default false. It is the least-privilege posture that has to be the default: an
	// unset variable must not be the thing standing between a deployment and running
	// its data plane as an administrator.
	AllowAdminProducer bool `json:"allow_admin_producer" envconfig:"KAFKA_ALLOW_ADMIN_PRODUCER"`
}

// KafkaTLSConfig configures the TLS client both Kafka transports use.
//
// Enabled turns TLS on. CAFile names a PEM bundle to verify the broker's
// certificate against, which is required whenever the broker presents a
// certificate signed by a private authority; leaving it empty uses the host trust
// store. CertFile and KeyFile supply a client certificate for mutual TLS and must
// be set together. ServerName overrides the name verified against the certificate,
// which is needed when brokers are reached through an address that does not match
// their advertised name.
//
// InsecureSkipVerify disables certificate verification entirely. It is a
// LAST-RESORT development switch: with it set, TLS still encrypts but no longer
// authenticates, so an interposed broker is indistinguishable from the real one.
// It is warned about on every configuration load.
type KafkaTLSConfig struct {
	// ALL SUPPLEMENTARY (transport security).
	Enabled            bool   `json:"enabled"              envconfig:"KAFKA_TLS_ENABLED"`
	CAFile             string `json:"ca_file"              envconfig:"KAFKA_TLS_CA_FILE"`
	CertFile           string `json:"cert_file"            envconfig:"KAFKA_TLS_CERT_FILE"`
	KeyFile            string `json:"key_file"             envconfig:"KAFKA_TLS_KEY_FILE"`
	ServerName         string `json:"server_name"          envconfig:"KAFKA_TLS_SERVER_NAME"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify" envconfig:"KAFKA_TLS_INSECURE_SKIP_VERIFY"`
}

// RelayConfig tunes the transactional-outbox relay that publishes event rows to
// Kafka. The three values define a bounded exponential backoff: the relay makes at
// most MaxRetryAttempts attempts, waiting RetryBaseBackoffMS before the first retry
// and doubling thereafter, never sleeping longer than RetryMaxBackoffMS. Both delay
// values are milliseconds and are used verbatim — they are never reinterpreted as
// another unit. Bad tuning is reported as a warning, never as a fatal error, so a
// misconfigured relay can never stop the server from starting.
//
// Env tags are un-prefixed for the same reason as KafkaConfig's, and both the bare
// RELAY_* names and the conventional BLNK_RELAY_* names resolve through the same
// mechanism; see the notes on KafkaConfig and eventStreamingEnvOverride.
type RelayConfig struct {
	// ALL THREE ARE R-10 CONTRACT VARIABLES, with the defaults the requirement fixes:
	// 5 attempts, a 1000 ms base delay and a 30000 ms ceiling. See defaultRelay.
	MaxRetryAttempts   int `json:"max_retry_attempts"    envconfig:"RELAY_MAX_RETRY_ATTEMPTS"`
	RetryBaseBackoffMS int `json:"retry_base_backoff_ms" envconfig:"RELAY_RETRY_BASE_BACKOFF_MS"`
	RetryMaxBackoffMS  int `json:"retry_max_backoff_ms"  envconfig:"RELAY_RETRY_MAX_BACKOFF_MS"`

	// EventRetentionDays is how long a DELIVERED or DEAD-LETTERED event row is kept
	// before it is deleted, and it is a data-protection control rather than a storage
	// tuning knob.
	//
	// WHAT THE OUTBOX HOLDS IS SENSITIVE. Each row's payload is the webhook body
	// verbatim: a transaction event carries amounts and balance identifiers, and an
	// identity event carries names, email addresses, phone numbers, postal addresses and
	// dates of birth. Once the event has been delivered none of that has any operational
	// value, so keeping it indefinitely turns a delivery buffer into an unbounded second
	// copy of the ledger's most sensitive data — without the access controls the primary
	// tables have around them, and with a blast radius that only grows.
	//
	// TWO POPULATIONS, TWO LIFECYCLES, and the distinction is what makes any finite value
	// here safe to set.
	//
	// A DISPATCHED row is a receipt: the event reached the broker, a subscriber has had it,
	// and after this period the row says nothing anyone needs. Age alone governs it.
	//
	// A DEAD-LETTERED row is the opposite — the record of an event NO SUBSCRIBER EVER
	// RECEIVED, together with the failure metadata explaining why and the bytes a replay is
	// driven from. It is NEVER deleted by this period, however old it is, and it keeps the
	// DeadLetterMessageStuck alert firing until the event actually reaches a subscriber. The
	// workflow that ends its life is REPLAY, through
	// POST /events/dead-letter/:event_id/replay: a re-publish the broker acknowledges makes
	// the row dispatched, and this period then applies to it as it does to any other
	// receipt.
	//
	// Every other state is still owed a delivery attempt and is never deleted however old
	// it is; a failed row in particular is excluded because its dead-letter write is still
	// owed, which makes this table the only copy of that event in existence.
	//
	// ZERO DISABLES RETENTION and is the default, deliberately. Deleting ledger-adjacent
	// records is a decision only an operator can take: a jurisdiction, an audit programme
	// or a legal hold may require a longer period than any default could guess, and a
	// default that silently deleted evidence would be worse than one that keeps too much.
	// So the mechanism ships switched off and the period is an explicit choice. The
	// resulting storage growth is observable through blnk.outbox.pending and the
	// statistics endpoint.
	EventRetentionDays int `json:"event_retention_days" envconfig:"RELAY_EVENT_RETENTION_DAYS"`

	// EventRetentionBatchSize is how many rows ONE delete statement removes. PERF-P23.
	//
	// This is the lock-footprint knob, not the throughput knob. Each statement takes row
	// locks and writes WAL for the rows it removes, and the sweeper runs beside a live relay
	// claiming from the same table, so a large single delete trades a brief pause for the
	// relay against fewer statements. The default keeps each statement small and reaches
	// throughput through the batch COUNT below instead, which is the safer of the two ways to
	// buy capacity.
	EventRetentionBatchSize int `json:"event_retention_batch_size" envconfig:"RELAY_EVENT_RETENTION_BATCH_SIZE"`

	// EventRetentionMaxBatchesPerSweep is how many delete statements one sweep may issue, and
	// with the batch size it is what sets the sweeper's CAPACITY. PERF-P23.
	//
	// # The arithmetic, because the previous default failed it
	//
	// Capacity is batches x batch size per sweep, and the sweep runs hourly, so:
	//
	//	rows per hour = EventRetentionMaxBatchesPerSweep x EventRetentionBatchSize
	//
	// The old fixed ceiling was 100 batches of 1,000 rows — 100,000 rows an hour — while the
	// throughput this system is specified for, 500 events a second, ARRIVES at 1,800,000 rows
	// an hour. A sweeper eighteen times slower than ingestion does not slow growth down; the
	// table grows unboundedly anyway and the retention period is never actually enforced. The
	// bound was documented as overtaking "any realistic arrival rate", which was true of the
	// deployments it was written for and false of the one the acceptance criteria describe.
	//
	// The default is therefore set ABOVE peak ingestion rather than below it, and it is
	// configurable so an operator whose ingestion is higher still can raise it, and one who
	// would rather protect a small database can lower it.
	//
	// # Zero means UNSET, and unbounded has its own value
	//
	// An unset int field is indistinguishable from a deliberate zero, so the two readings of
	// zero — "I did not configure this" and "I want no ceiling" — cannot both be honoured.
	// Zero is taken as UNSET and defaulted, because that is the safe reading: the per-sweep
	// bound is what stops the first sweep after retention is enabled from trying to delete an
	// entire historical backlog in one pass, holding locks on a table the relay is claiming
	// from, and a deployment that never mentions this setting must keep that protection.
	//
	// EventRetentionUnboundedSweep (-1) is the explicit way to ask for no ceiling, which is
	// what a deliberate one-off catch-up wants.
	EventRetentionMaxBatchesPerSweep int `json:"event_retention_max_batches_per_sweep" envconfig:"RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP"`

	// TWO SPELLINGS, ONE KNOB. This is the same ceiling as Kafka.MetricsSubscriberBudget:
	// RELAY_SUBSCRIBER_METRICS_BUDGET and EVENT_METRICS_SUBSCRIBER_BUDGET are both accepted
	// because both were published, and setRelayDefaults reconciles them so the two fields can
	// never disagree about the value the collector actually uses.
	// SubscriberMetricsBudget caps how many subscribers ONE metrics collection measures
	// consumer lag for, and it is a MONITORING COVERAGE control.
	//
	// # What the budget costs when it is reached
	//
	// A subscriber past the budget is not measured approximately — it gets NO
	// consumer-lag series at all. blnk_kafka_consumer_lag is simply absent for it, so the
	// SubscriberConsumerLagHigh rule has nothing to evaluate and the subscriber can fall
	// arbitrarily far behind while every dashboard reads clean. Silence and health become
	// indistinguishable, which is why the shortfall is published on
	// blnk.subscribers.lag_unmeasured and alerted on rather than only logged.
	//
	// # Why there is a budget at all
	//
	// It bounds two scarce things at once. Each subscriber costs an OffsetFetch and a
	// ListOffsets round trip per authorised topic, so an unbounded registry could make one
	// collection longer than the interval between collections. And each subscriber-topic
	// pair is a retained gauge series, so the registry's size is also the exported series
	// count. Neither cost is a reason to leave subscribers unmeasured silently; both are
	// reasons to make the ceiling an explicit, observable, operator-set number.
	//
	// ZERO TAKES THE DEFAULT of 200, which is far above any plausible subscriber count for
	// one ledger deployment. Raise it when blnk_subscribers_lag_unmeasured is non-zero and
	// the collection is comfortably inside its interval; that gauge and
	// blnk.subscribers.registered together say whether there is headroom.
	SubscriberMetricsBudget int `json:"subscriber_metrics_budget" envconfig:"RELAY_SUBSCRIBER_METRICS_BUDGET"`
}

// eventStreamingEnvOverride is how the CONVENTIONAL BLNK_-prefixed names for the
// nested Kafka and relay blocks are resolved. It exists because one envconfig pass
// cannot honour both name forms for the same field, and the deployment contract
// requires both.
//
// # Why a second, flat struct is necessary rather than a prefix on the tags
//
// envconfig derives a field's PRIMARY key by accumulating the prefix through every
// enclosing struct and appending the tag literal, then consults the bare tag literal
// as an ALTERNATE key only when the primary is unset. Configuration.Kafka is itself a
// prefix segment, so for Configuration.Kafka.Brokers — tagged KAFKA_BROKERS — the
// primary key is BLNK_KAFKA_KAFKA_BROKERS and the alternate is the mandated bare
// KAFKA_BROKERS. The conventional BLNK_KAFKA_BROKERS is neither key, so nesting alone
// silently drops it.
//
// Renaming the tags cannot fix that. A tag of BROKERS would make the primary
// BLNK_KAFKA_BROKERS and the alternate a bare BROKERS, honouring the convention but
// breaking the mandated KAFKA_BROKERS. Either way one form is lost, because a field
// has exactly one primary key and one alternate. Two forms therefore need two
// sources, which is what this struct is.
//
// This struct is FLAT, so envconfig accumulates no intermediate segment: for
// KafkaBrokers, tagged KAFKA_BROKERS, the primary key is BLNK_KAFKA_BROKERS and the
// alternate is KAFKA_BROKERS. Both forms resolve here, the library performs all
// parsing, and a malformed integer is reported by the library naming the exact
// variable that carried it.
//
// # Every field is a POINTER, and that is the whole mechanism
//
// envconfig skips a field whose variable is unset, leaving a pointer nil, and
// allocates one whose variable is set — even when the value is empty. Nil therefore
// means "not configured through this name" and non-nil means "configured, use it",
// which is exactly the distinction an overlay needs. A value struct could not tell an
// explicit empty value from an absent one and would erase a value the file or the bare
// name supplied.
//
// # The resulting precedence, highest first
//
//  1. BLNK_KAFKA_BROKERS / BLNK_RELAY_MAX_RETRY_ATTEMPTS — the conventional prefixed
//     name, this struct's primary key. Highest because it matches the prefixed
//     convention every other variable in this file follows.
//  2. KAFKA_BROKERS / RELAY_MAX_RETRY_ATTEMPTS — the bare name the deployment
//     contract mandates, this struct's alternate key.
//  3. BLNK_KAFKA_KAFKA_BROKERS / BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS — the key
//     envconfig derives from the nested struct. Still honoured, because it is what
//     the nested pass applies and removing it would break any deployment that found
//     it; lowest of the three because it is an artefact of the nesting rather than a
//     name anybody would choose.
//  4. The matching key under "kafka" or "relay" in blnk.json.
//  5. The defaults in defaultKafka and defaultRelay.
//
// Levels 1 to 3 order themselves without any lookup gymnastics: the nested pass runs
// first and applies level 3, this overlay runs second and applies level 1 or 2 over
// it, and within the overlay envconfig's own primary-before-alternate rule orders 1
// above 2.
//
// WebhookDeprecationSunsetDate is deliberately absent. It is a top-level field on
// Configuration, so it accumulates no intermediate segment and the nested pass already
// resolves both WEBHOOK_DEPRECATION_SUNSET_DATE and its BLNK_-prefixed form. Adding it
// here would be redundant.
type eventStreamingEnvOverride struct {
	KafkaBrokers *[]string `envconfig:"KAFKA_BROKERS"`
	// KafkaSubscriberBrokers belongs HERE rather than in applyPrefixedEnvAliases for one
	// mechanical reason: it is a LIST, and that function's three alias tables are typed
	// string, int and bool. Resolving it through this struct also means the library performs
	// the comma splitting, so the prefixed alias splits exactly as the bare name does
	// instead of through a second, hand-written parse.
	KafkaSubscriberBrokers *[]string `envconfig:"KAFKA_SUBSCRIBER_BROKERS"`
	KafkaTopicPrefix       *string   `envconfig:"KAFKA_TOPIC_PREFIX"`
	KafkaSASLAdminUser     *string   `envconfig:"KAFKA_SASL_ADMIN_USER"`
	KafkaSASLAdminSecret   *string   `envconfig:"KAFKA_SASL_ADMIN_SECRET"`
	KafkaMinPartitions     *int      `envconfig:"KAFKA_MIN_PARTITIONS"`
	KafkaReplicationFactor *int      `envconfig:"KAFKA_REPLICATION_FACTOR"`

	RelayMaxRetryAttempts                 *int `envconfig:"RELAY_MAX_RETRY_ATTEMPTS"`
	RelayRetryBaseBackoffMS               *int `envconfig:"RELAY_RETRY_BASE_BACKOFF_MS"`
	RelayRetryMaxBackoffMS                *int `envconfig:"RELAY_RETRY_MAX_BACKOFF_MS"`
	RelaySubscriberMetricsBudget          *int `envconfig:"RELAY_SUBSCRIBER_METRICS_BUDGET"`
	RelayEventRetentionDays               *int `envconfig:"RELAY_EVENT_RETENTION_DAYS"`
	RelayEventRetentionBatchSize          *int `envconfig:"RELAY_EVENT_RETENTION_BATCH_SIZE"`
	RelayEventRetentionMaxBatchesPerSweep *int `envconfig:"RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP"`
}

// applyEventStreamingEnvOverride resolves the Kafka and relay environment variables
// through eventStreamingEnvOverride and copies whatever was set onto cnf.
//
// It is called from loadConfigFromFile immediately after the nested envconfig pass and
// before validateAndAddDefaults, which is what produces the precedence documented on
// eventStreamingEnvOverride. It does not re-process Configuration, add a configuration
// source, or reorder the load pipeline: the pipeline remains file, then environment,
// then defaults and validation, then publish.
//
// Only non-nil fields are copied, so a name that was never set cannot erase a value
// the file or a lower-precedence name supplied. An explicitly EMPTY value is copied,
// because emptying a variable is how an operator turns a setting off — most visibly
// KAFKA_BROKERS, where empty is the supported "no Kafka configured" steady state that
// selects the no-op event publisher.
//
// Parameters:
//   - cnf *Configuration: the configuration being loaded, mutated in place.
//
// Returns:
//   - error: the library's own error when a value cannot be parsed into its field —
//     for example a non-numeric KAFKA_MIN_PARTITIONS. The message names the offending
//     variable. It never contains KAFKA_SASL_ADMIN_SECRET's value, because that field
//     is a string and a string conversion cannot fail.
func applyEventStreamingEnvOverride(cnf *Configuration) error {
	var override eventStreamingEnvOverride
	if err := envconfig.Process("blnk", &override); err != nil {
		return err
	}

	if override.KafkaBrokers != nil {
		cnf.Kafka.Brokers = *override.KafkaBrokers
	}
	if override.KafkaSubscriberBrokers != nil {
		cnf.Kafka.SubscriberBrokers = *override.KafkaSubscriberBrokers
	}
	if override.KafkaTopicPrefix != nil {
		cnf.Kafka.TopicPrefix = *override.KafkaTopicPrefix
	}
	if override.KafkaSASLAdminUser != nil {
		cnf.Kafka.SASLAdminUser = *override.KafkaSASLAdminUser
	}
	if override.KafkaSASLAdminSecret != nil {
		cnf.Kafka.SASLAdminSecret = *override.KafkaSASLAdminSecret
	}
	if override.KafkaMinPartitions != nil {
		cnf.Kafka.MinPartitions = *override.KafkaMinPartitions
	}
	if override.KafkaReplicationFactor != nil {
		cnf.Kafka.ReplicationFactor = *override.KafkaReplicationFactor
	}

	if override.RelayMaxRetryAttempts != nil {
		cnf.Relay.MaxRetryAttempts = *override.RelayMaxRetryAttempts
	}
	if override.RelayRetryBaseBackoffMS != nil {
		cnf.Relay.RetryBaseBackoffMS = *override.RelayRetryBaseBackoffMS
	}
	if override.RelayRetryMaxBackoffMS != nil {
		cnf.Relay.RetryMaxBackoffMS = *override.RelayRetryMaxBackoffMS
	}
	if override.RelaySubscriberMetricsBudget != nil {
		cnf.Relay.SubscriberMetricsBudget = *override.RelaySubscriberMetricsBudget
	}
	if override.RelayEventRetentionDays != nil {
		cnf.Relay.EventRetentionDays = *override.RelayEventRetentionDays
	}
	if override.RelayEventRetentionBatchSize != nil {
		cnf.Relay.EventRetentionBatchSize = *override.RelayEventRetentionBatchSize
	}
	if override.RelayEventRetentionMaxBatchesPerSweep != nil {
		cnf.Relay.EventRetentionMaxBatchesPerSweep = *override.RelayEventRetentionMaxBatchesPerSweep
	}

	return nil
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
	//
	// # Why this exists at all
	//
	// Several of the event pipeline's diagnostics are deliberately emitted at DEBUG,
	// because they are per-event and the pipeline is built for 500 events a second:
	// the successful-publish line in the relay and in the publisher, the metrics
	// collector's per-tick summary, and the consumer-lag series retirement notice.
	// Suppressing them by default is correct — a line per published event whose
	// content is "it worked" is not observability, it is volume. Having NO WAY TO
	// TURN THEM ON was the defect: nothing in the codebase called logrus.SetLevel, so
	// every deployed binary sat at logrus's default of info and a delivery
	// investigation had only failure lines and batch counts to work from. Those
	// diagnostics were unreachable without recompiling.
	//
	// # An unparseable value warns rather than refusing to start
	//
	// Deliberately different from WebhookDeprecationSunsetDate, which is fatal. A
	// mis-typed sunset silently keeps the deprecated transport alive, so the only
	// safe response is to refuse; a mis-typed log level silently keeps the SAFE
	// default, changes nothing about how traffic is served, and is announced by the
	// warning itself. Failing the load would turn a typo in a diagnostic control into
	// an outage.
	//
	// Blank means "leave the logger alone", which is info. That distinction matters
	// beyond tidiness: an unconditional SetLevel here would reach into every process
	// that loads a configuration, including a test that has pinned the level to
	// capture a debug line.
	LogLevel string `json:"log_level" envconfig:"BLNK_LOG_LEVEL"`

	// WebhookDeprecationStartDate is the RFC3339 instant at which the dual-delivery
	// window OPENS. IT IS DERIVED, NOT CONFIGURED: it is always computed as
	// WebhookDeprecationSunsetDate minus exactly WebhookDualDeliveryWindowDays, and
	// any value present on input is overwritten by resolveWebhookDeprecationWindow.
	//
	// It carries no envconfig tag on purpose. Requirement R-10 freezes the deployment
	// contract at eight environment variables, of which exactly one describes this
	// window — WEBHOOK_DEPRECATION_SUNSET_DATE. A second variable for the other end
	// was a genuine convenience and still a contract violation, and it bought nothing
	// that could not be derived: the window is exactly
	// WebhookDualDeliveryWindowDays long by definition, so either end determines the
	// other and the sunset is the end the requirement names.
	//
	// Deriving rather than accepting also removes a whole class of misconfiguration.
	// Two independently settable ends can disagree, which previously had to be
	// detected and refused; one settable end cannot, so "exactly 30 days" is a
	// property of the arithmetic instead of a rule that has to be enforced.
	//
	// The field remains exported because the window's opening instant is worth
	// reading — the migration documentation and the dual-delivery logging both state
	// it — and because a derived value nobody can see is a value nobody can check.
	WebhookDeprecationStartDate string `json:"webhook_deprecation_start_date"`

	// AN R-10 CONTRACT VARIABLE, and the ONLY one describing the dual-delivery window.
	// WebhookDeprecationSunsetDate is the RFC3339 instant at which the legacy HTTP
	// webhook transport is retired. Before it, Kafka publishing and legacy webhook
	// delivery run concurrently from the same outbox rows; from it onwards Kafka is
	// the only transport and the deprecated webhook routes answer 410 Gone.
	//
	// A VALUE THAT WILL NOT PARSE IS FATAL. An operator who states a retirement
	// instant and mis-types it would otherwise get the silent resolution "keep the
	// legacy behaviour", so a single typo keeps the deprecated, less protected of the
	// two transports alive indefinitely with nothing failing and nothing to notice.
	// Refusing to start is the only signal that cannot be overlooked, and it happens
	// before any traffic is served.
	//
	// IT IS REQUIRED WHENEVER KAFKA_BROKERS IS SET, and absent one the load FAILS. The
	// runtime's sunset predicate fails closed on a publishing deployment with no usable
	// window — it answers "the sunset has already passed", stopping dual delivery and
	// answering 410 Gone on the deprecated routes — so accepting the combination here
	// with a warning that promised the opposite let configuration and behaviour
	// disagree about the one decision R-12 is made of. See
	// resolveWebhookDeprecationWindow for the full reasoning.
	//
	// With NO brokers it stays optional and empty is the norm: there is no transport to
	// migrate to, so there is no window to describe, and AAP §0.7.2's graceful
	// degradation is untouched.
	//
	// The other end of the window is DERIVED from this one — see
	// WebhookDeprecationStartDate — so there is no second value to keep in step and
	// the window is exactly WebhookDualDeliveryWindowDays long by construction.
	//
	// Being a top-level field, it accumulates no intermediate prefix segment, so both
	// the mandated bare WEBHOOK_DEPRECATION_SUNSET_DATE and the house-convention
	// BLNK_WEBHOOK_DEPRECATION_SUNSET_DATE are honoured, the prefixed form winning
	// if both are set. KafkaConfig's nested fields get the same behaviour from
	// applyPrefixedEnvAliases.
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

// EventPublishingConfigured reports whether this deployment has at least one usable
// Kafka broker, and therefore whether the event pipeline captures anything at all.
//
// # Why the predicate lives on the configuration
//
// Two packages need this same answer and MUST NOT disagree about it. The root package
// decides whether to capture an event and whether the legacy transport is the one to
// use; the database package decides whether to write a balance-monitor handoff inside a
// ledger transaction. If those two answers ever differed, one of two silent faults
// would follow: handoffs written that nothing drains, or a movement whose monitors are
// evaluated by neither the handoff nor the post-commit path — an alert lost with nothing
// failing to say so.
//
// Putting the predicate here makes the disagreement unrepresentable rather than
// unlikely. The root package's eventPublishingConfigured delegates to it, and the
// database layer calls it directly.
//
// # What counts as configured
//
// A broker list containing only blank entries is NOT configured. A whitespace-only
// entry survives environment-variable splitting and list parsing but cannot be dialled,
// so treating it as a broker would arm the whole pipeline against an endpoint that
// cannot exist.
//
// Returns:
//   - bool: true when at least one broker address is non-blank. A nil receiver answers
//     false, because a process with no configuration cannot publish.
func (cnf *Configuration) EventPublishingConfigured() bool {
	if cnf == nil {
		return false
	}

	for _, broker := range cnf.Kafka.Brokers {
		if strings.TrimSpace(broker) != "" {
			return true
		}
	}

	return false
}

func loadConfigFromFile(file string) error {
	var cnf Configuration
	_, err := os.Stat(file)
	if err == nil {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		err = json.NewDecoder(f).Decode(&cnf)
		if err != nil {
			return err
		}

	} else if errors.Is(err, os.ErrNotExist) {
		logrus.Info("config json not passed, will use env variables")
	}

	// override config from environment variables
	err = envconfig.Process("blnk", &cnf)
	if err != nil {
		return explainEnvProcessError(err)
	}

	// The BLNK_-prefixed aliases of the nested Kafka and relay variables, which
	// envconfig cannot reach on its own. Runs after Process so the prefixed form
	// wins, matching the precedence the top-level sunset variable already has.
	if err = applyPrefixedEnvAliases(&cnf); err != nil {
		return err
	}

	// Then let the conventional BLNK_-prefixed names for the nested Kafka and relay
	// blocks take effect. This runs between the environment overlay and the defaults so
	// that the precedence documented on eventStreamingEnvOverride holds, and so that
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
// It is applied to the bare Kafka and relay names to form their ordinary aliases.
const envAliasPrefix = "BLNK_"

// explainEnvProcessError re-states an envconfig parse failure in terms of the variable
// the operator actually set.
//
// # The problem it solves
//
// envconfig derives a nested field's PRIMARY key by accumulating the prefix through every
// enclosing struct and treats the raw tag literal as an ALTERNATE. Configuration.Relay.
// MaxRetryAttempts, tagged RELAY_MAX_RETRY_ATTEMPTS, therefore has the primary key
// BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS and the alternate RELAY_MAX_RETRY_ATTEMPTS. The value
// is read from whichever is set, but *envconfig.ParseError always reports the PRIMARY.
//
// So an operator who sets the mandated bare name RELAY_MAX_RETRY_ATTEMPTS=five is told
// "assigning BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS to MaxRetryAttempts" — a variable name
// that appears nowhere in their configuration and that a search of it will not find. The
// failure is correct and it is loud; only the name is unhelpful, and the name is the one
// piece of information the operator needs.
//
// # What it does and, deliberately, does not do
//
// It rewrites the MESSAGE and nothing else. Which values are accepted is left entirely to
// envconfig, because that acceptance is subtle — integers are parsed with base 0, so
// "0x10" is 16, and no trimming is performed, so " 4 " is rejected — and a hand-written
// pre-flight check would have to reproduce it exactly or would silently change behaviour.
// The original error is wrapped, so errors.As still recovers the *envconfig.ParseError.
//
// The primary key is only rewritten when it is genuinely ABSENT from the environment and a
// name the operator could have set is present. When the primary itself is set, it is the
// variable at fault and it is already named correctly.
//
// Parameters:
//   - err error: the error returned by envconfig.Process. Never nil at the call site.
//
// Returns:
//   - error: err unchanged when it is not a parse failure or the reported key is the one
//     that was set; otherwise a wrapping error naming the variable that was set.
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
	// recovered from the primary key rather than from a second table, so this cannot
	// drift out of step with the tags.
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
//
// A nested primary key is "BLNK_" + every enclosing struct field name + "_" + the tag
// literal:
//
//	BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS      accumulated "BLNK_RELAY",     tag RELAY_MAX_RETRY_ATTEMPTS
//	BLNK_KAFKA_KAFKA_MIN_PARTITIONS          accumulated "BLNK_KAFKA",     tag KAFKA_MIN_PARTITIONS
//	BLNK_KAFKA_TLS_KAFKA_TLS_ENABLED         accumulated "BLNK_KAFKA_TLS", tag KAFKA_TLS_ENABLED
//
// Every struct this feature adds is NAMED AFTER the prefix its tags already carry, and
// that is what makes the literal recoverable rather than guessed: the accumulated segment
// is repeated as the head of the tag, so the split point is the position where the
// remainder begins with the segment that precedes it. The loop below walks the underscore
// boundaries and returns at the first such position, which handles a struct nested one or
// two levels deep without a table to keep in step with the tags.
//
// Anything that does not match that shape returns the empty string, so a field outside
// these structs is left to envconfig's own wording rather than being described by a guess.
//
// Parameters:
//   - key string: the primary key envconfig reported.
//
// Returns:
//   - string: the bare tag literal, or "" when the key is not one of the nested forms.
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
//
// # Why this function has to exist
//
// envconfig derives a field's primary key by accumulating the prefix through every
// enclosing struct and consults the raw tag literal only as an alternate key. For
// Configuration.Kafka.Brokers, tagged KAFKA_BROKERS, the primary key is therefore
// BLNK_KAFKA_KAFKA_BROKERS and the alternate is KAFKA_BROKERS. BLNK_KAFKA_BROKERS —
// the name a reader of this file would naturally write, because every other setting
// here is spelled that way — matches NEITHER, so it used to be read by nothing at
// all. A deployment that set it got default behaviour: no brokers, the no-op
// publisher, and no error anywhere to say why events were not being published.
//
// Rather than change the tags (which would break the mandated bare names) or flatten
// the struct (which would break the blnk.json shape), the aliases are applied here,
// explicitly and by name. Explicit beats reflective for a table this small: the set
// of variables is fixed by the deployment contract, and a reader can check the list
// against the two structs by eye.
//
// # Precedence
//
// The prefixed alias WINS over the bare name when both are set. That is the same
// precedence envconfig itself gives the top-level WebhookDeprecationSunsetDate,
// where the accumulated BLNK_ primary beats the bare alternate, so the behaviour is
// uniform across every variable this feature adds rather than depending on whether a
// given field happens to sit inside a nested struct.
//
// A malformed integer or boolean alias is an ERROR, not a silently ignored value:
// BLNK_RELAY_MAX_RETRY_ATTEMPTS=five must not resolve to the default 5 and leave an
// operator believing they had configured something.
//
// Returns:
//   - error: when a prefixed alias holds a value that cannot be parsed as the
//     field's type.
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

	// AFTER trimWhitespace, so a key of nothing but spaces is treated as absent rather than
	// accepted as a credential, and BEFORE the secure-mode warning below, so a production
	// deployment that would authenticate to its search index with a publicly known key is
	// refused rather than warned about among other warnings.
	if err := cnf.resolveSearchCredential(); err != nil {
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

// ReservedSubscriberPrincipalNamespace is the principal prefix Blnk reserves for the
// subscriber identities it mints.
//
// # It is a PINNED COPY, and it must equal model.SubscriberPrincipalNamespace
//
// This package cannot import the model package: model already depends on config, and an
// import here would close the cycle. So the value is restated, and a test in each package
// asserts the two are equal — which is the only thing that keeps them from drifting. If you
// change one, change the other; the test will tell you if you forget.
//
// It is exported so that test, and any operator tooling validating a deployment, can read the
// same value the check below applies.
const ReservedSubscriberPrincipalNamespace = "blnk-sub-"

// validateKafkaPrincipalSeparation refuses a Kafka identity configuration in which Blnk's own
// principals could be mistaken for — or could overwrite — a subscriber's.
//
// # The escalation this closes
//
// Blnk mints a subscriber principal by prefixing the subscriber's identifier with the reserved
// namespace, and issuing credentials for it performs a SCRAM UPSERT: an existing credential for
// that principal is REPLACED with a freshly generated password, which is then returned to the
// caller in the response body.
//
// Nothing previously stopped the administrative or producer username from living inside that
// same reserved namespace. A deployment whose KAFKA_SASL_ADMIN_USER was, say,
// "blnk-sub-admin" could therefore be made to rotate and hand out its own Kafka superuser
// credential: register a subscriber whose identifier derives that exact principal, call the
// credential endpoint, and the response contains a working administrative password. The same
// applies to the producer principal, which holds Write on every Blnk-owned topic.
//
// Two rules close it, and both are checked here:
//
//  1. NEITHER Blnk principal may sit inside the reserved subscriber namespace. The namespace is
//     documented as Blnk's own and is derived, not chosen, so an identity inside it is
//     reachable by an ordinary authorized API call.
//  2. The administrative and producer principals must be DISTINCT from each other. They exist
//     precisely to separate publishing from administration — one holds Write on the topics, the
//     other can mint credentials and grant ACLs — and collapsing them onto one identity gives
//     every process that only publishes the ability to provision. It also makes the excess
//     privilege invisible, because each pair looks correctly configured on its own.
//
// # Severity follows whether Kafka is in use, matching validateKafkaSASLCredentials
//
// With brokers configured this is FATAL: the credential endpoint is reachable and the
// escalation is live, so refusing to start is the only signal that cannot be overlooked. With
// no brokers configured nothing reads these identities and no credential can be issued, so the
// defect is a warning and start-up continues — the same posture the half-configured-pair check
// takes, for the same reason.
//
// Comparison is case-SENSITIVE and on the trimmed value, because Kafka principals are
// case-sensitive: "blnk-sub-x" and "BLNK-SUB-X" are two different identities at the broker, and
// folding case here would refuse a configuration that is in fact safe while telling the
// operator something untrue about why.
//
// Returns:
//   - error: non-nil only when a rule is broken on a deployment that has brokers configured.
//     No message contains a secret — only usernames, which are not secrets.
func (cnf *Configuration) validateKafkaPrincipalSeparation() error {
	admin := strings.TrimSpace(cnf.Kafka.SASLAdminUser)
	producer := strings.TrimSpace(cnf.Kafka.SASLUser)

	var faults []string

	if admin != "" && strings.HasPrefix(admin, ReservedSubscriberPrincipalNamespace) {
		faults = append(faults, fmt.Sprintf(
			"KAFKA_SASL_ADMIN_USER (%q) is inside the reserved subscriber principal namespace %q, so "+
				"issuing credentials for a subscriber that derives it would rotate and return the "+
				"administrative credential",
			admin, ReservedSubscriberPrincipalNamespace,
		))
	}

	if producer != "" && strings.HasPrefix(producer, ReservedSubscriberPrincipalNamespace) {
		faults = append(faults, fmt.Sprintf(
			"KAFKA_SASL_USER (%q) is inside the reserved subscriber principal namespace %q, so issuing "+
				"credentials for a subscriber that derives it would rotate and return the producer "+
				"credential",
			producer, ReservedSubscriberPrincipalNamespace,
		))
	}

	if admin != "" && admin == producer {
		faults = append(faults, fmt.Sprintf(
			"KAFKA_SASL_ADMIN_USER and KAFKA_SASL_USER are the same principal (%q); they must be "+
				"distinct, because one holds Write on every Blnk-owned topic and the other can mint "+
				"subscriber credentials and grant ACLs",
			admin,
		))
	}

	if len(faults) == 0 {
		return nil
	}

	reason := strings.Join(faults, "; ")

	if len(cnf.Kafka.Brokers) == 0 {
		logrus.WithField("reason", reason).Warn(
			"kafka: the configured Kafka principals are not separated as required. No broker is " +
				"configured, so no credential can be issued today and start-up continues; it must be " +
				"fixed before KAFKA_BROKERS is set",
		)

		return nil
	}

	return fmt.Errorf("kafka: reserved principal identities must be disjoint: %s", reason)
}

// resolveWebhookDeprecationWindow validates the dual-delivery window and derives its
// opening instant from its closing one.
//
// There is exactly ONE input: WebhookDeprecationSunsetDate. The window is
// WebhookDualDeliveryWindowDays long by definition, so the start is arithmetic rather
// than configuration, and "exactly 30 days" cannot be got wrong because there is no
// second value to disagree with. Requirement R-10 names the sunset and only the
// sunset, which is also why the other end carries no environment variable.
//
// The rules, in the order they are applied:
//
//  1. A sunset that will not parse as RFC3339 is a FATAL error, with or without
//     brokers. The date drives two security-relevant behaviours — whether the legacy
//     HTTP transport still runs, and whether the deprecated webhook management routes
//     still answer — and an operator who states a retirement instant and mis-types it
//     must not have it silently reinterpreted. Refusing to start is the only signal
//     that cannot be overlooked, and it happens before any traffic is served.
//
//  2. A BLANK SUNSET WITH BROKERS CONFIGURED IS ALSO FATAL, and this is the rule the
//     review's C-1 finding exists to install. It was previously a warning whose text
//     promised that "legacy HTTP webhook delivery will run alongside Kafka
//     INDEFINITELY" — and the runtime does the OPPOSITE. blnk.WebhookSunsetPassed
//     treats "publishing configured, window unusable" as fail-closed: the sunset has
//     ALREADY passed, so dual delivery stops and the deprecated webhook routes answer
//     410 Gone. One of the two had to give, and the runtime's reading is the safe one
//     — carrying on with a deprecated, less protected transport on the strength of an
//     unset variable is worse than retiring it loudly. So configuration now REFUSES
//     the combination outright, which is what event_sunset.go's own documentation
//     already claimed it did, and the fail-closed arm becomes the second line of
//     defence it is described as rather than a routine outcome.
//
//     Refusing costs nothing an operator cannot pay before serving traffic: the value
//     is one RFC3339 instant, and the error below names the variable, the window
//     length and how to compute it. What it buys is that "dual delivery is running"
//     and "the webhook routes still answer" can no longer disagree with the
//     configuration that described them.
//
//     This does NOT weaken AAP §0.7.2's graceful degradation, which is about
//     KAFKA_BROKERS being UNSET: with no brokers the publisher is the no-op, there is
//     no migration to describe, and case 3 below leaves the window empty without
//     complaint.
//
//  3. A blank sunset with NO brokers is silently accepted and clears the window.
//     Nothing has been mis-stated: there is no transport to migrate to, so there is no
//     window, and the deprecated routes keep answering exactly as they did before this
//     feature existed.
//
//  4. A parseable sunset. Used verbatim and normalised to RFC3339 in UTC, with the
//     start back-filled as sunset minus the window so both ends are available to
//     describe it.
//
// Returns:
//   - error: non-nil when the sunset date is present and unparseable, and when it is
//     absent while Kafka brokers are configured.
func (cnf *Configuration) resolveWebhookDeprecationWindow() error {
	rawSunset := strings.TrimSpace(cnf.WebhookDeprecationSunsetDate)

	if rawSunset == "" {
		// No date, no window. The start is cleared too, so a value that arrived from a
		// configuration file cannot survive as a window with only one end.
		cnf.WebhookDeprecationStartDate = ""
		cnf.WebhookDeprecationSunsetDate = ""

		if len(cnf.Kafka.Brokers) > 0 {
			return fmt.Errorf(
				"webhook_deprecation_sunset_date is required once kafka.brokers is set: the runtime "+
					"treats a Kafka deployment with no usable dual-delivery window as ALREADY past "+
					"the sunset, so legacy HTTP webhook delivery would stop and the deprecated "+
					"webhook management routes would answer 410 Gone — the opposite of continuing "+
					"dual delivery. Set WEBHOOK_DEPRECATION_SUNSET_DATE to the RFC3339 instant the "+
					"legacy transport retires, which opens the %d-day window %d days earlier "+
					"(for example: date -u -d '+%d days' +%%Y-%%m-%%dT%%H:%%M:%%SZ)",
				WebhookDualDeliveryWindowDays, WebhookDualDeliveryWindowDays, WebhookDualDeliveryWindowDays,
			)
		}

		return nil
	}

	sunset, err := time.Parse(time.RFC3339, rawSunset)
	if err != nil {
		return fmt.Errorf(
			"webhook_deprecation_sunset_date %q is not a valid RFC3339 instant (expected %s): %w",
			rawSunset, time.RFC3339, err,
		)
	}

	sunset = sunset.UTC()

	// DERIVED, never read as input. Whatever the field held is replaced, which is what
	// makes the window's length a property of this arithmetic rather than a rule.
	cnf.WebhookDeprecationStartDate = sunset.Add(-webhookDualDeliveryWindow).Format(time.RFC3339)
	cnf.WebhookDeprecationSunsetDate = sunset.Format(time.RFC3339)

	return nil
}

// warnOnInsecureKafkaTransport reports a Kafka client that is configured to give up
// a protection it would otherwise have.
//
// Neither case is an error here, because both are reachable on purpose: the local
// single-broker stack listens on SASL_PLAINTEXT, and a developer pointing at a
// broker with a self-signed certificate may legitimately skip verification for an
// afternoon. Both are refused at the point a client is actually built unless the
// insecure flag is set, so this function's job is only to make the setting's
// presence impossible to miss in a log an operator reads.
func (cnf *Configuration) warnOnInsecureKafkaTransport() {
	if len(cnf.Kafka.Brokers) == 0 {
		return
	}

	if cnf.Kafka.InsecureLocalDev {
		logrus.Warn(
			"SECURITY: kafka.insecure_local_dev is true — the Kafka connection may run WITHOUT TLS, " +
				"so ledger amounts and identity records (names, email addresses, phone numbers, " +
				"addresses, dates of birth) can travel in cleartext. This setting is for the local " +
				"single-broker stack only. Do not use it in production.",
		)
	}

	if cnf.Kafka.TLS.Enabled && cnf.Kafka.TLS.InsecureSkipVerify {
		logrus.Warn(
			"SECURITY: kafka.tls.insecure_skip_verify is true — the broker's certificate is NOT " +
				"verified, so TLS encrypts but no longer authenticates and an interposed broker is " +
				"indistinguishable from the real one. Configure kafka.tls.ca_file instead.",
		)
	}
}

// warnOnUnusableSubscriberBrokers reports at STARTUP which broker list subscribers will be given.
//
// KAFKA_SUBSCRIBER_BROKERS is an OPTIONAL OVERRIDE: without it credential issuance reports
// KAFKA_BROKERS, which is right when subscribers run inside the deployment and wrong when they
// do not. Only an operator knows which, so the absence cannot be an error and must not be
// silence either — an unnoticed internal address reaches the subscriber as an unexplained
// connection timeout days later, nowhere near the request that produced it.
//
// It is a WARNING for the same reason the insecure-transport notices are: the setting's absence
// changes what an endpoint reports, and that belongs in the log an operator reads at boot. It
// says nothing when no broker is configured, because then there is no Kafka and no issuance.
func (cnf *Configuration) warnOnUnusableSubscriberBrokers() {
	if len(cnf.Kafka.Brokers) == 0 {
		return
	}

	if _, advertised := cnf.Kafka.SubscriberFacingBrokers(); advertised {
		return
	}

	logrus.Warn(
		"KAFKA_SUBSCRIBER_BROKERS is not configured: POST /subscribers/{id}/kafka-credentials " +
			"will report the internal broker addresses Blnk dials (KAFKA_BROKERS), which is correct " +
			"only when subscribers run inside this deployment. Set it to the externally advertised " +
			"broker addresses subscribers connect to if they do not.",
	)
}

// validateKafkaSASLCredentials refuses a half-configured administrative SASL
// credential at configuration load, so the deployment contract fails where it is
// described rather than at the first broker dial.
//
// The severity depends on whether Kafka is in use. With brokers configured the pair is
// load-bearing and a half-configured one is fatal: an admin username with no secret
// cannot complete a SCRAM exchange, and a secret with no username would connect
// anonymously while the operator believed it was authenticating. With no brokers
// configured nothing reads the pair, so the defect is reported as a warning and
// start-up continues — a deployment that does not publish events must not be blocked
// by a credential it never uses.
//
// Returns:
//   - error: non-nil only for a half-configured pair on a deployment that has brokers
//     configured. The message never contains the secret.
func (cnf *Configuration) validateKafkaSASLCredentials() error {
	err := cnf.Kafka.ValidateSASLAdminCredentials()
	if err == nil {
		return nil
	}

	if len(cnf.Kafka.Brokers) == 0 {
		logrus.WithField("reason", err.Error()).Warn(
			"kafka: the administrative SASL credential is half-configured. No broker is configured, so " +
				"nothing reads it today and start-up continues; it must be fixed before KAFKA_BROKERS is set",
		)

		return nil
	}

	return err
}

// validateRelayRetryWindow advises on a relay retry window that cannot behave as
// intended. Every finding is a warning: relay tuning is an operational knob, and a
// bad value must degrade event delivery rather than prevent the server from
// starting. It runs after setDefaultValues, so it inspects effective values with
// defaults already applied.
func (cnf *Configuration) validateRelayRetryWindow() {
	if cnf.Relay.MaxRetryAttempts < 1 {
		logrus.WithField("max_retry_attempts", cnf.Relay.MaxRetryAttempts).Warn(
			"relay max_retry_attempts is below 1; events will not be retried before being dead-lettered",
		)
	}

	// CLAMPED rather than merely warned about. The attempt number is a metric attribute on a
	// histogram, so a configured budget above the ceiling widens that attribute's domain by
	// one value per attempt — see MaxRelayRetryAttempts. Reducing it here is what keeps the
	// domain closed at its declared eight values.
	if cnf.Relay.MaxRetryAttempts > MaxRelayRetryAttempts {
		logrus.WithFields(logrus.Fields{
			"max_retry_attempts": cnf.Relay.MaxRetryAttempts,
			"ceiling":            MaxRelayRetryAttempts,
		}).Warn(
			"relay max_retry_attempts exceeds the supported ceiling and has been reduced to it; " +
				"the attempt number is a bounded metric attribute and cannot be extended by configuration",
		)

		cnf.Relay.MaxRetryAttempts = MaxRelayRetryAttempts
	}

	if cnf.Relay.RetryBaseBackoffMS < 0 {
		logrus.WithField("retry_base_backoff_ms", cnf.Relay.RetryBaseBackoffMS).Warn(
			"relay retry_base_backoff_ms is negative; retries will not be delayed",
		)
	}

	if cnf.Relay.RetryMaxBackoffMS < 0 {
		logrus.WithField("retry_max_backoff_ms", cnf.Relay.RetryMaxBackoffMS).Warn(
			"relay retry_max_backoff_ms is negative; the backoff cap will not delay retries",
		)
	}

	if cnf.Relay.RetryBaseBackoffMS > cnf.Relay.RetryMaxBackoffMS {
		logrus.WithFields(logrus.Fields{
			"retry_base_backoff_ms": cnf.Relay.RetryBaseBackoffMS,
			"retry_max_backoff_ms":  cnf.Relay.RetryMaxBackoffMS,
		}).Warn(
			"relay retry_base_backoff_ms exceeds retry_max_backoff_ms; every retry will wait the capped maximum",
		)
	}
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

	// THE SEARCH CREDENTIAL IS DELIBERATELY NOT DEFAULTED HERE. SEC-07.
	//
	// It used to be, unconditionally, to the literal DEFAULT_TYPESENSE_KEY — and that
	// substitution is what made the misconfiguration invisible. Once it had run, TypeSenseKey
	// was non-empty, so no later check could tell a value the operator supplied from one this
	// code invented. resolveSearchCredential makes the decision instead, from
	// validateAndAddDefaults, where it can still see the difference and can refuse.
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
//
// It runs FIRST among the module default setters, so the level an operator asked for is
// already in force for the warnings the other setters emit. Resolving it last would mean
// the very lines that report a defaulted configuration were filtered by the previous
// level.
//
// # Three inputs, three outcomes
//
//   - A PARSEABLE value is applied and normalised, so the field afterwards reads exactly
//     what the logger is set to. logrus.ParseLevel accepts trace, debug, info, warn,
//     warning, error, fatal and panic, case-insensitively, and it is the only parser used
//     here: a second spelling of the level vocabulary would be a second thing to keep in
//     step with the library.
//
//   - An UNPARSEABLE value warns, names the accepted values, and leaves the logger as it
//     is. The field is then set to the level actually in force rather than to the rejected
//     text, because a configuration that reports a level the process is not running at is
//     worse than one that reports the truth. Refusing to start would turn a typo in a
//     diagnostic control into an outage; see the LogLevel field's documentation for why
//     that is the opposite trade-off from the sunset date's.
//
//   - BLANK leaves the logger UNTOUCHED and fills the field with the default. Not calling
//     SetLevel is the whole point rather than an optimisation: this function runs from
//     validateAndAddDefaults, which MockConfig also calls, and several tests pin the level
//     with logrus.SetLevel in order to capture a debug-only line. An unconditional
//     SetLevel here would silently undo those pins from inside the configuration layer.
//     logrus already defaults to info, so filling the field is a statement of fact.
//
// minimumVisibleLogLevel is MINIMUM_LOG_LEVEL as a logrus level.
//
// It is parsed from the constant rather than written as logrus.WarnLevel so that the
// name an operator sets and the level the code enforces cannot drift apart. Parsing a
// compile-time constant cannot fail; if it somehow did, warn is the answer anyway.
func minimumVisibleLogLevel() logrus.Level {
	level, err := logrus.ParseLevel(MINIMUM_LOG_LEVEL)
	if err != nil {
		return logrus.WarnLevel
	}

	return level
}

// clampLogLevel raises a requested level to the floor that keeps mandatory records
// visible, reporting whether it had to.
//
// logrus orders its levels from panic (0) to trace (6) and emits an entry only when the
// logger's level is numerically at least the entry's, so "quieter" means a SMALLER
// value and the floor is a minimum rather than a maximum. Getting that comparison
// backwards would clamp away trace and debug — the exact diagnostics the LogLevel field
// was added to make reachable — so the direction is asserted by
// TestClampLogLevel_RaisesOnlyTheLevelsThatSuppressMandatoryRecords.
//
// Parameters:
//   - requested: the level the deployment asked for.
//
// Returns:
//   - logrus.Level: the level that will actually be applied.
//   - bool: true when the request was quieter than the floor and has been raised.
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

	// A level quieter than the floor is honoured as far as the floor and no further.
	// The warning is emitted AFTER the level is applied, which is what guarantees it is
	// itself visible: it is a warning, and the level it is announcing is the one that
	// makes warnings emit. Announcing before applying could discard the announcement.
	effective, clamped := clampLogLevel(level)

	// The field records the level the process is RUNNING at, not the one it was asked
	// for. This follows the same rule as the unparseable branch above: a configuration
	// that reports a level the logger is not set to is worse than one that reports the
	// truth, and /metrics and support bundles read this field.
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

// MaxMetricsSubscriberBudget is the supported ceiling on how many subscribers one metrics
// collection tick may measure.
//
// The ceiling is real rather than cautious: each subscriber-topic pair is an exported gauge
// series, and each subscriber costs two broker round trips per authorised topic, so an
// unbounded budget makes a single tick unbounded in both duration and cardinality. A
// configured value above it is clamped and logged rather than rejected, because a
// misconfigured metrics budget must never stop the ledger from serving.
const MaxMetricsSubscriberBudget = 5000

// setKafkaDefaults fills only the unset Kafka topic-geometry values. Brokers,
// SASLAdminUser and SASLAdminSecret are never defaulted: an empty broker list
// selects the no-op event publisher, and the two SASL fields are credentials.
//
// Because every assignment is guarded on the zero value, an explicitly configured
// value always survives — in particular ReplicationFactor: 1, which a single-broker
// cluster requires and which must not be overwritten by the production default of 3.
func (cnf *Configuration) setKafkaDefaults() {
	if cnf.Kafka.TopicPrefix == "" {
		cnf.Kafka.TopicPrefix = defaultKafka.TopicPrefix
	}
	if cnf.Kafka.MinPartitions == 0 {
		cnf.Kafka.MinPartitions = defaultKafka.MinPartitions
	}
	if cnf.Kafka.ReplicationFactor == 0 {
		cnf.Kafka.ReplicationFactor = defaultKafka.ReplicationFactor
	}
	// The lag-sweep budget: defaulted when unset or nonsensical, and CLAMPED above.
	//
	// Clamped rather than refused, matching how every other bad value in this file is
	// handled — a misconfigured metrics budget must never stop the ledger from serving — and
	// the correction is logged loudly so it cannot be mistaken for the value taking effect.
	// The ceiling is real rather than cautious: each subscriber-topic pair is an exported
	// gauge series and each subscriber costs two broker round trips per topic, so a budget of
	// a million would make one collection tick unbounded in both time and cardinality.
	if cnf.Kafka.MetricsSubscriberBudget <= 0 {
		cnf.Kafka.MetricsSubscriberBudget = defaultKafka.MetricsSubscriberBudget
	}
	if cnf.Kafka.MetricsSubscriberBudget > MaxMetricsSubscriberBudget {
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Kafka.MetricsSubscriberBudget,
			"applied":    MaxMetricsSubscriberBudget,
		}).Warn(
			"EVENT_METRICS_SUBSCRIBER_BUDGET is above the supported ceiling and has been clamped; " +
				"each subscriber measured costs two broker round trips per authorised topic and one " +
				"exported gauge series per topic, so an unbounded budget makes a single collection " +
				"tick unbounded in both duration and cardinality",
		)
		cnf.Kafka.MetricsSubscriberBudget = MaxMetricsSubscriberBudget
	}
	cnf.Kafka.Brokers = normalizeBrokers(cnf.Kafka.Brokers)
	// NOT DEFAULTED TO Brokers, deliberately. Falling back would hand every subscriber
	// Blnk's internal broker addresses in a 200 response, which is the failure
	// SubscriberBrokers exists to prevent; issuance refuses instead. Normalised the same
	// way so that KAFKA_SUBSCRIBER_BROKERS="" and "," — both of which envconfig parses into
	// a non-empty slice carrying nothing usable — read as "not configured" rather than as a
	// list of blank endpoints.
	cnf.Kafka.SubscriberBrokers = normalizeBrokers(cnf.Kafka.SubscriberBrokers)

	// Credentials are trimmed here rather than in trimWhitespace because a stray
	// newline from an environment file turns a correct SASL username into one the
	// broker has never heard of, and the resulting authentication failure reads
	// exactly like a wrong password. trimWhitespace is a fixed list of long-standing
	// fields; extending it would change behaviour for those, so the Kafka fields are
	// normalised in their own domain setter instead.
	cnf.Kafka.SASLUser = strings.TrimSpace(cnf.Kafka.SASLUser)
	cnf.Kafka.SASLSecret = strings.TrimSpace(cnf.Kafka.SASLSecret)
	cnf.Kafka.SASLAdminUser = strings.TrimSpace(cnf.Kafka.SASLAdminUser)
	cnf.Kafka.SASLAdminSecret = strings.TrimSpace(cnf.Kafka.SASLAdminSecret)
	cnf.Kafka.TLS.CAFile = strings.TrimSpace(cnf.Kafka.TLS.CAFile)
	cnf.Kafka.TLS.CertFile = strings.TrimSpace(cnf.Kafka.TLS.CertFile)
	cnf.Kafka.TLS.KeyFile = strings.TrimSpace(cnf.Kafka.TLS.KeyFile)
	cnf.Kafka.TLS.ServerName = strings.TrimSpace(cnf.Kafka.TLS.ServerName)

	// A HALF-CONFIGURED pair is reported HERE, at load, and not only when a transport is
	// eventually built.
	//
	// The construction paths already refuse it — kafkaTransportCredentials validates before
	// it builds anything, so nothing half-authenticated can be dialled — but that failure
	// arrives whenever the publisher or the admin client is first needed, which for a
	// deployment with no Kafka work in flight can be long after start-up and a long way from
	// the variable that caused it. Naming the missing variable while the configuration is
	// being loaded is what turns "authentication failed" hours later into an actionable line
	// in the boot log.
	//
	// It is a WARNING and not a fatal error, deliberately. validateRequiredFields requires
	// only the two DSNs, and making a Kafka credential fatal would stop a server that does
	// not use Kafka at all from booting because of a stray variable. The construction path
	// remains the fail-closed one.
	if err := cnf.Kafka.ValidateSASLAdminCredentials(); err != nil {
		logrus.WithField("cause", logsafe.Cause(err)).Warn(
			"the Kafka administrative SASL credential is half-configured; topic assurance, " +
				"subscriber provisioning and the offset reads behind reconciliation will all be " +
				"refused rather than run unauthenticated",
		)
	}
	if err := ValidateSASLPair("producer", cnf.Kafka.SASLUser, cnf.Kafka.SASLSecret); err != nil {
		logrus.WithField("cause", logsafe.Cause(err)).Warn(
			"the Kafka producer SASL credential is half-configured; event publishing will be " +
				"refused rather than run unauthenticated",
		)
	}

	cnf.warnOnUnusableKafkaTopicGeometry()
}

// warnOnUnusableKafkaTopicGeometry reports a topic geometry that will be silently
// corrected at the point of use.
//
// # Why a negative value needs its own line
//
// Zero means "unset" and is replaced by the default just above, which is correct and needs
// no comment. A NEGATIVE value is different: it cannot have been intended, and the topic
// assurance path raises it to the required six partitions anyway. Without this warning the
// operator's stated number and the provisioned number differ with nothing anywhere saying
// so — and the same value would then be reported back by a status endpoint as though it had
// been honoured.
//
// A value below the six-partition floor but positive is warned about where it is applied,
// because that is where the floor lives and where the correction is made; only the negative
// case is invisible at that point, since a negative count reads as a paste error rather
// than as a layout choice.
//
// A negative replication factor is warned about for the same reason and is more dangerous:
// Kafka reads a negative replica count in a create request as "use the broker default", so
// a topic would be created successfully with a durability the operator never chose.
//
// It is a WARNING and not a fatal error, matching every other Kafka diagnostic here:
// validateRequiredFields requires only the two DSNs, and a deployment that does not use
// Kafka must not be stopped from booting by a stray variable.
func (cnf *Configuration) warnOnUnusableKafkaTopicGeometry() {
	if cnf.Kafka.MinPartitions < 0 {
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Kafka.MinPartitions,
			"will_use":   defaultKafka.MinPartitions,
		}).Warn(
			"KAFKA_MIN_PARTITIONS is negative, which cannot be provisioned; topic assurance will " +
				"raise it to the required minimum, so the configured value will not be the value " +
				"in effect",
		)
	}

	if cnf.Kafka.ReplicationFactor < 0 {
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Kafka.ReplicationFactor,
			"will_use":   defaultKafka.ReplicationFactor,
		}).Warn(
			"KAFKA_REPLICATION_FACTOR is negative; a negative replica count is read by Kafka as " +
				"'use the broker default', so topics would be created with a durability that was " +
				"never chosen",
		)
	}
}

// kafkaTopicNameCutset is the set of characters Kafka permits in a topic name:
// alphanumerics, dot, underscore and hyphen. Anything else is rejected by the broker.
const kafkaTopicNameCutset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"0123456789._-"

// MaxKafkaTopicNameLength is the longest topic name a Kafka broker will accept.
//
// It is Kafka's own limit, not a Blnk policy, and it bounds the COMPOSED name rather
// than the prefix: a broker refuses `CreateTopics` outright for anything longer, so a
// prefix that fits but whose composed `<prefix>.transactions.dlt` does not would pass
// configuration and then fail every topic assurance.
const MaxKafkaTopicNameLength = 249

// maxComposedTopicSuffixLength is the length of the LONGEST suffix event_topics.go
// appends to the prefix — ".transactions" plus the ".dlt" dead-letter sibling, 17
// characters. Reserving it here is what makes the prefix budget below correct for
// every name in the catalogue rather than only for the shortest one.
//
// Keep it in step with event_topics.go: a longer category name added there without a
// matching change here would let a prefix through that composes an over-long topic.
const maxComposedTopicSuffixLength = len(".transactions") + len(DeadLetterTopicSuffixForValidation)

// DeadLetterTopicSuffixForValidation duplicates event_topics.go's DeadLetterTopicSuffix
// so that this package can size the prefix budget without importing the root package,
// which imports this one. It is validated against the real constant by
// TestKafkaTopicPrefixBudgetMatchesTopicSuffix in the root package's tests.
const DeadLetterTopicSuffixForValidation = ".dlt"

// MaxKafkaTopicPrefixLength is the longest KAFKA_TOPIC_PREFIX that can compose a legal
// topic name for every category in the catalogue.
const MaxKafkaTopicPrefixLength = MaxKafkaTopicNameLength - maxComposedTopicSuffixLength

// MaxHistoricalTopicPrefixes bounds KAFKA_HISTORICAL_TOPIC_PREFIXES.
//
// The bound is a resource bound, not a taste one. Each declared prefix has a Kafka writer
// pre-created for every topic in its inventory — two per event category — on every process
// start, and those writers exist for the sole purpose of draining rows captured before a
// rename. Four is generous for what the list is for: a deployment that has renamed its
// topic namespace four times without ever draining the previous generation has an
// operational problem the allowlist cannot fix, and unbounded growth would let one stale
// environment variable multiply the writer inventory indefinitely.
const MaxHistoricalTopicPrefixes = 4

// validateKafkaTopicPrefix REFUSES a topic prefix that cannot compose a legal Kafka
// topic name.
//
// # Why this is fatal rather than a warning
//
// It used to warn and then use the value anyway. That is the worst of the three
// available behaviours, and not a conservative middle ground:
//
//   - EVENTS ARE LOST-IN-PLACE, not rejected. Producers keep capturing outbox rows
//     whose `topic` column names a topic the broker will never create, so every one of
//     them exhausts its retry budget and dead-letters — onto a dead-letter topic whose
//     name is equally illegal, so the dead-letter write fails too and the row sits in
//     `failed` forever. A ledger accepts mutations and silently stops notifying anyone.
//   - THE PREFIX REACHES A LOG LINE UNSANITISED. The warning interpolated the configured
//     value straight into a structured log message, so a prefix carrying newlines or
//     ANSI control sequences could forge log records — the classic log-injection sink,
//     reachable by anyone who can set an environment variable on the process.
//   - The broker's own refusal, which the warning deferred to, arrives at first Kafka
//     use. For a deployment with no event traffic at boot that can be hours later and a
//     long way from the variable that caused it.
//
// Refusing at load is therefore strictly better on every axis: nothing is captured that
// cannot be delivered, no attacker-influenced value is logged, and the failure names the
// variable at the moment it is read.
//
// # What is corrected and what is refused
//
// Leading and trailing whitespace and separators are stripped, and a blank prefix falls
// back to the default, so the realistic accidents — a trailing newline in an environment
// file, a lone dot — are accepted and normalised. That normalisation is applied to the
// stored value here, so what this function validates is exactly what event_topics.go will
// compose from.
//
// An INTERIOR character outside Kafka's set, or a prefix too long to leave room for the
// longest composed suffix, is refused. The message names the offending characters as Go
// quoted runes, so a control character is reported as `'\x00'` rather than emitted.
//
// Returns:
//   - error: nil when the prefix can compose legal topic names for every category.
func (cnf *Configuration) validateKafkaTopicPrefix() error {
	// The composer's own normalisation, applied to the STORED value so that the prefix
	// this validates is the prefix that will be used. topicPrefixFrom in event_topics.go
	// trims the same cutset; doing it here as well means the two cannot disagree.
	prefix := strings.Trim(cnf.Kafka.TopicPrefix, " \t\n\v\f\r.")
	if prefix == "" {
		// Blank resolves to the default downstream. Leaving the field as configured would
		// make the effective prefix depend on which code path read it, so it is set here.
		cnf.Kafka.TopicPrefix = defaultKafka.TopicPrefix
	} else {
		cnf.Kafka.TopicPrefix = prefix

		if err := validateKafkaTopicPrefixValue("KAFKA_TOPIC_PREFIX", prefix); err != nil {
			return err
		}
	}

	return cnf.validateKafkaHistoricalTopicPrefixes()
}

// validateKafkaTopicPrefixValue applies the length and character rules a topic prefix must
// satisfy, whatever variable it arrived in.
//
// It exists because KAFKA_TOPIC_PREFIX is no longer the only prefix a deployment declares:
// KAFKA_HISTORICAL_TOPIC_PREFIXES names the namespaces it used to own and must still be
// able to publish to, and a historical prefix composes topic names through exactly the same
// composer. Validating the two with one function is what stops a value being accepted in
// one variable and refused in the other.
//
// Parameters:
//   - variable string: the environment variable name to quote in any error, so the message
//     names the value an operator has to change.
//   - prefix string: the already-trimmed, non-empty prefix to check.
//
// Returns:
//   - error: nil when every topic composed from the prefix would be a legal Kafka topic
//     name; otherwise an error naming the variable and what is wrong with it.
func validateKafkaTopicPrefixValue(variable, prefix string) error {
	if len(prefix) > MaxKafkaTopicPrefixLength {
		return fmt.Errorf(
			"%s is %d characters, which is longer than the %d a topic prefix may "+
				"be: Kafka refuses any topic name over %d characters and the longest name composed "+
				"from the prefix is \"<prefix>.transactions%s\"",
			variable, len(prefix), MaxKafkaTopicPrefixLength, MaxKafkaTopicNameLength,
			DeadLetterTopicSuffixForValidation,
		)
	}

	seen := map[rune]struct{}{}
	var offenders []string
	for _, char := range prefix {
		if strings.ContainsRune(kafkaTopicNameCutset, char) {
			continue
		}
		if _, already := seen[char]; already {
			continue
		}
		seen[char] = struct{}{}
		// QuoteRune, never the raw rune: this string reaches an error message and from
		// there a log record, and the whole point of refusing the value is that it may
		// carry newlines or terminal control sequences.
		offenders = append(offenders, strconv.QuoteRune(char))
	}

	if len(offenders) == 0 {
		return nil
	}

	return fmt.Errorf(
		"%s contains %d character(s) Kafka does not permit in a topic name "+
			"(%s); only letters, digits, '.', '_' and '-' are legal, and every topic composed "+
			"from this prefix would be refused by the broker, so events would be captured and "+
			"never delivered",
		variable, len(offenders), strings.Join(offenders, ", "),
	)
}

// validateKafkaHistoricalTopicPrefixes normalises and checks the historical-prefix
// allowlist, and rewrites the field so every reader sees the normalised list.
//
// # Why it is validated rather than just trimmed
//
// Every prefix in this list is treated as OWNED: writers are pre-created for its whole
// topic inventory, the publisher's ownership test admits it, and topic assurance keeps its
// topics present. A malformed entry would therefore be a silent failure of exactly the
// stranding this list exists to prevent — the operator declares the old namespace, the
// value is unusable, and the old rows still never drain. Refusing it at load, by name, is
// what makes the declaration mean something.
//
// # What is normalised away rather than refused
//
// Blank entries and duplicates, including a repeat of TopicPrefix. Both are ordinary in an
// allowlist that is meant to be DRAINED: a trailing comma, or the current prefix left in
// place after a rename completed, describes the same set of owned topics either way. The
// current prefix is removed rather than kept so that every consumer can treat this list as
// "the prefixes BESIDES the configured one", which is what makes a topic inventory
// composed from current-plus-historical free of duplicates.
//
// Returns:
//   - error: non-nil when an entry could not compose legal topic names, or when more than
//     MaxHistoricalTopicPrefixes distinct prefixes are declared.
func (cnf *Configuration) validateKafkaHistoricalTopicPrefixes() error {
	if len(cnf.Kafka.HistoricalTopicPrefixes) == 0 {
		cnf.Kafka.HistoricalTopicPrefixes = nil

		return nil
	}

	current := strings.Trim(cnf.Kafka.TopicPrefix, " \t\n\v\f\r.")
	if current == "" {
		current = defaultKafka.TopicPrefix
	}

	seen := map[string]struct{}{current: {}}
	normalised := make([]string, 0, len(cnf.Kafka.HistoricalTopicPrefixes))

	for _, raw := range cnf.Kafka.HistoricalTopicPrefixes {
		prefix := strings.Trim(raw, " \t\n\v\f\r.")
		if prefix == "" {
			continue
		}
		if _, already := seen[prefix]; already {
			continue
		}

		if err := validateKafkaTopicPrefixValue("KAFKA_HISTORICAL_TOPIC_PREFIXES", prefix); err != nil {
			return err
		}

		seen[prefix] = struct{}{}
		normalised = append(normalised, prefix)
	}

	if len(normalised) > MaxHistoricalTopicPrefixes {
		return fmt.Errorf(
			"KAFKA_HISTORICAL_TOPIC_PREFIXES declares %d distinct prefixes, which is more than "+
				"the %d permitted: every prefix in the list has a writer pre-created for each of "+
				"its topics on every process start, so an unbounded list is memory spent on "+
				"topics nothing writes to. This list is meant to be DRAINED — remove a prefix "+
				"once no non-terminal and no replayable outbox row still names it",
			len(normalised), MaxHistoricalTopicPrefixes,
		)
	}

	if len(normalised) == 0 {
		cnf.Kafka.HistoricalTopicPrefixes = nil

		return nil
	}

	cnf.Kafka.HistoricalTopicPrefixes = normalised

	return nil
}

// ErrProducerPrincipalRequired reports that a deployment configured an ADMINISTRATIVE
// Kafka principal but no dedicated producer principal.
//
// It is exported so the publisher's construction path can classify the failure and so a
// test can assert on it with errors.Is rather than on message text.
var ErrProducerPrincipalRequired = errors.New(
	"a dedicated Kafka producer principal is required: set KAFKA_SASL_USER and " +
		"KAFKA_SASL_SECRET. The event publisher must not authenticate with " +
		"KAFKA_SASL_ADMIN_USER, which can create topics, mint SCRAM credentials and " +
		"rewrite ACLs, because publishing every ledger event as that principal turns a " +
		"leaked producer credential into full control of the cluster's authorization " +
		"state. Provision a producer principal with Write and Describe on the Blnk-owned " +
		"topics only",
)

// ProducerSASL returns the SASL identity the steady-state event publisher must
// authenticate as, and reports the one misconfiguration that has no safe answer.
//
// It exists so that "which credential does the producer use?" is answered in exactly
// one place. The publisher and the administrative client used to reach into the
// configuration separately, which is how the producer ended up authenticating as the
// administrator: nothing in either call site was wrong on its own, and no single
// place expressed the intent that they should differ.
//
// # There is NO fallback to the administrative principal (PRIV-01)
//
// This method used to return the administrative pair when no producer pair was set, so
// that an existing deployment kept publishing across an upgrade. That compatibility
// path was the vulnerability rather than a mitigation of it: the busiest process in the
// deployment held cluster-administration authority permanently, a leaked producer
// credential became a full compromise of the authorization model, and the broker's audit
// trail could not tell routine publishing from administration. A warning does not change
// any of that — it only records it — and a warning nobody reads is how a temporary
// allowance becomes the permanent configuration.
//
// So the administrative pair is never returned here. When it is the only pair configured,
// adminOnly is true and the caller must REFUSE to build a producer transport.
//
// Returns:
//   - user, secret string: the dedicated producer credentials. Both empty means no SASL
//     at all, which is legitimate on a broker that requires none.
//   - adminOnly bool: true when an administrative principal is configured and no producer
//     principal is. The caller must treat this as fatal, not as a credential to use.
func (cnf *Configuration) ProducerSASL() (user, secret string, adminOnly bool) {
	if cnf.Kafka.SASLUser != "" || cnf.Kafka.SASLSecret != "" {
		return cnf.Kafka.SASLUser, cnf.Kafka.SASLSecret, false
	}

	if cnf.Kafka.SASLAdminUser == "" && cnf.Kafka.SASLAdminSecret == "" {
		return "", "", false
	}

	return "", "", true
}

// SASLAdminCredentials is THE one reading of the ADMINISTRATIVE SASL/SCRAM
// credential, and every component that authenticates to a broker as the administrator
// must resolve it through this method rather than inspecting the two fields itself.
//
// # The contract
//
//	both empty         no SASL. The broker is reached over a PLAINTEXT (or plain TLS)
//	                   listener. This is a supported deployment, not a degraded one:
//	                   the local single-broker stack can run without SASL.
//	both set           SASL/SCRAM-SHA-512 as the named principal.
//	exactly one set    a misconfiguration. enabled is false so no half-authenticated
//	                   transport can be built, and ValidateSASLPair reports it by name.
//
// # Why this is a method rather than two field reads
//
// Before it existed, three components each invented their own reading of the same two
// values. The publisher enabled SASL on a non-empty username and ignored an empty
// secret. The admin client enabled it on a non-empty username, rejected a username
// without a secret, and silently ignored a secret without a username — so a
// deployment that set only KAFKA_SASL_ADMIN_SECRET connected as an anonymous
// principal while its operator believed it was authenticating. The provisioning
// scripts substituted the literal principal "admin" for an empty username, so the same
// configuration meant "authenticate as admin" to a script and "authenticate as nobody"
// to the service. One shared reading is what makes those three agree, and
// scripts/kafka-provision.sh names this method as the contract it mirrors.
//
// Both values are trimmed here as well as at load, because a Configuration assembled
// in a test or by a caller that bypassed validateAndAddDefaults must read the same way
// as one that went through it: a trailing newline or space from a secret store, a
// Kubernetes secret or a hand-edited .env would otherwise turn "unset" into a
// credential made of whitespace, which fails SASL preparation or authenticates as a
// principal nobody created.
//
// Returns:
//   - user string: the trimmed principal, empty when SASL is not configured.
//   - secret string: the trimmed secret, empty when SASL is not configured. Never log
//     this value.
//   - enabled bool: true only when BOTH values are present.
func (k KafkaConfig) SASLAdminCredentials() (user, secret string, enabled bool) {
	user = strings.TrimSpace(k.SASLAdminUser)
	secret = strings.TrimSpace(k.SASLAdminSecret)

	if user == "" || secret == "" {
		return "", "", false
	}

	return user, secret, true
}

// OwnedTopicPrefixes returns every topic namespace this deployment owns: the configured
// prefix first, then each historical prefix in declared order.
//
// # Why the order matters
//
// The configured prefix is where NEW events go, so it leads: a caller that composes a
// topic inventory from this list builds the live generation's names first, which is the
// order the provisioning script and the documentation both use, and a caller that is
// resolving one topic name finds the common case on the first comparison.
//
// # What it guarantees
//
// Non-empty, distinct, and free of blanks. A configuration that has not been through the
// validate-and-default path can still hold a blank prefix, and rather than return a list
// with a hole in it this falls back to the default prefix — the same answer every other
// prefix reader gives for an unconfigured deployment, and the STRICTEST one available,
// since an unconfigured caller must not accidentally widen the owned namespace.
//
// The returned slice is freshly allocated. The configuration is shared through an
// atomic.Value and read concurrently, so handing out the backing array would let a caller
// that appends to its result mutate what every later reader sees.
//
// Returns:
//   - []string: one or more distinct, non-blank prefixes, configured prefix first.
func (k KafkaConfig) OwnedTopicPrefixes() []string {
	current := strings.Trim(k.TopicPrefix, " \t\n\v\f\r.")
	if current == "" {
		current = defaultKafka.TopicPrefix
	}

	prefixes := make([]string, 0, 1+len(k.HistoricalTopicPrefixes))
	prefixes = append(prefixes, current)

	seen := map[string]struct{}{current: {}}
	for _, raw := range k.HistoricalTopicPrefixes {
		prefix := strings.Trim(raw, " \t\n\v\f\r.")
		if prefix == "" {
			continue
		}
		if _, already := seen[prefix]; already {
			continue
		}
		seen[prefix] = struct{}{}
		prefixes = append(prefixes, prefix)
	}

	return prefixes
}

// SubscriberFacingBrokers returns the bootstrap list to HAND TO A SUBSCRIBER, and reports
// whether that list is the operator's ADVERTISED one or the internal fallback.
//
// It exists so that the single question "what do I tell this subscriber to connect to?" has
// one answer in one place.
//
// # KAFKA_BROKERS is the fallback, and the fallback is REQUIRED rather than optional
//
// SubscriberBrokers used to be a hard prerequisite: absent it, issuance refused with 503 so
// that a subscriber could never be handed an address it cannot resolve. That made a NINTH
// variable mandatory for the credential endpoint, and requirement R-10 fixes the mandatory
// configuration surface at eight. A deployment satisfying the whole documented contract could
// therefore run the relay, publish every event, and still receive 503 from every issuance
// request — a required endpoint made unusable by a setting the contract never mentions.
//
// So the resolution order is: the advertised list when one is configured, the internal list
// otherwise, and "nothing configured" only when Kafka itself is unconfigured. The advertised
// list keeps its whole reason for existing — inside a deployment "kafka:9092" or a ClusterIP
// Service name does not resolve for an outside subscriber, and a broker answers every client
// with the ADVERTISED address of the listener the connection arrived on — but that reason is
// now carried by the second return value and by the warning the caller logs, rather than by
// refusing to answer at all. An operator whose subscribers run outside the deployment sees the
// warning at start-up and at every issuance; one whose subscribers run inside it needs no
// extra variable to make a documented endpoint work.
//
// The returned slice is a COPY. The configuration is shared through an atomic.Value and read
// concurrently, so handing out the backing array would let a caller that appends to its
// result mutate what every later reader sees.
//
// Returns:
//   - brokers []string: a copy of the list to report, nil only when neither
//     KAFKA_SUBSCRIBER_BROKERS nor KAFKA_BROKERS is configured.
//   - advertised bool: true when the list came from KAFKA_SUBSCRIBER_BROKERS. False with a
//     non-empty list means the internal bootstrap list is being reported, which callers must
//     warn about — it is correct only when subscribers run inside the deployment.
func (k KafkaConfig) SubscriberFacingBrokers() (brokers []string, advertised bool) {
	if normalized := normalizeBrokers(k.SubscriberBrokers); len(normalized) > 0 {
		brokers = make([]string, len(normalized))
		copy(brokers, normalized)

		return brokers, true
	}

	// THE REQUIRED FALLBACK. Empty here means no Kafka at all, which the caller answers with
	// the same "Kafka is not configured" refusal every other Kafka-dependent operation gives.
	normalized := normalizeBrokers(k.Brokers)
	if len(normalized) == 0 {
		return nil, false
	}

	brokers = make([]string, len(normalized))
	copy(brokers, normalized)

	return brokers, false
}

// ValidateSASLAdminCredentials reports a half-configured administrative credential.
//
// It is the administrative arm of ValidateSASLPair, named so that a caller holding only
// a KafkaConfig — the scripts' Go counterpart, a start-up check, a test — does not have
// to know which two fields to hand it.
//
// Returns:
//   - error: non-nil only for a half-configured pair. Both-empty and both-set are valid
//     and return nil.
func (k KafkaConfig) ValidateSASLAdminCredentials() error {
	return ValidateSASLPair("admin", k.SASLAdminUser, k.SASLAdminSecret)
}

// ValidateSASLPair rejects a half-configured SASL credential.
//
// One value without the other is always a mistake and never a mode: a username with
// no secret cannot complete a SCRAM exchange, and a secret with no username has
// nobody to present it as. Both used to be tolerated differently by the two Kafka
// clients — one built a mechanism from whatever it had, the other skipped SASL
// entirely — so the same misconfiguration produced an authentication failure in one
// process and a silently unauthenticated connection in the other. Validating in one
// exported place is what makes the two agree.
//
// Both empty is valid and means "no SASL".
//
// Parameters:
//   - role string: "producer" or "admin", used only to name the offending pair in the
//     error.
//   - user, secret string: the configured pair. The secret is never echoed.
//
// Returns:
//   - error: non-nil when exactly one of the two is set.
func ValidateSASLPair(role, user, secret string) error {
	user = strings.TrimSpace(user)
	secret = strings.TrimSpace(secret)

	userVar, secretVar := saslEnvNames(role)

	switch {
	case user == "" && secret == "":
		return nil
	case user == "":
		// The DANGEROUS half. Every component used to ignore a secret with no username and
		// connect anonymously while looking configured, so this arm is the reason the
		// function exists rather than an afterthought. The secret is never echoed.
		return fmt.Errorf(
			"kafka %s SASL: %s is set but %s is empty; SASL/SCRAM needs both. Without a principal "+
				"the secret cannot be used and the connection would be anonymous. Set the user, or "+
				"clear both to reach a broker that has no SASL listener", role, secretVar, userVar,
		)
	case secret == "":
		return fmt.Errorf(
			"kafka %s SASL: %s is set to %q but %s is empty; SASL/SCRAM needs both. Set the "+
				"secret, or clear both to reach a broker that has no SASL listener",
			role, userVar, user, secretVar,
		)
	default:
		return nil
	}
}

// saslEnvNames maps a role onto the two environment variables that configure it.
//
// The error messages name the VARIABLE an operator has to change, not the struct field or
// the role, because the variable is the only one of the three they can act on. An unknown
// role still produces something useful rather than an empty name — the role is a literal at
// every call site, so an unrecognised one is a programming slip and not an operator's
// problem to decode.
//
// Parameters:
//   - role string: "producer" or "admin".
//
// Returns:
//   - userVar, secretVar string: the environment variable names.
func saslEnvNames(role string) (userVar, secretVar string) {
	if role == "admin" {
		return "KAFKA_SASL_ADMIN_USER", "KAFKA_SASL_ADMIN_SECRET"
	}

	return "KAFKA_SASL_USER", "KAFKA_SASL_SECRET"
}

// normalizeBrokers trims surrounding whitespace from each broker address and drops
// empty entries, preserving the configured order.
//
// Normalization is required rather than tidy: envconfig splits a comma-separated
// value without trimming, so BLNK_KAFKA_BROKERS="a:9092, b:9092" yields the address
// " b:9092", which cannot be dialled, and it preserves the empty entries produced by
// consecutive or trailing commas, which this function discards.
//
// The function is idempotent — re-running it over its own output is a no-op, which
// matters because validateAndAddDefaults may be invoked more than once on the same
// Configuration. An empty or all-blank input yields an empty slice rather than an
// error, because "no brokers configured" is a supported deployment mode.
func normalizeBrokers(brokers []string) []string {
	if len(brokers) == 0 {
		return brokers
	}

	normalized := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			continue
		}
		normalized = append(normalized, broker)
	}
	return normalized
}

// setRelayDefaults fills the unset relay retry values and BOUNDS the attempt count.
//
// The two backoff values are milliseconds and are stored verbatim; unlike the queue's
// HotPairTTL they are never reinterpreted as another unit, so a configured 1000 stays 1000.
//
// The attempt count is treated differently from the two delays, because it is the only one
// of the three that leaves the process as an exported metric label, so it is CLAMPED as
// well as defaulted: anything above MaxRelayRetryAttempts is reduced to it, with a warning
// naming both numbers so the operator sees what was asked for and what is in force. See
// MaxRelayRetryAttempts for why the ceiling exists.
//
// Values below 1 are deliberately left alone here. They are not a label-domain problem —
// the recording path normalises any attempt number below 1 to 1 — and
// validateRelayRetryWindow already reports them as the operational warning they are, which
// keeps "no retries before dead-lettering" a visible choice rather than one this function
// silently overrides.
func (cnf *Configuration) setRelayDefaults() {
	switch {
	case cnf.Relay.MaxRetryAttempts == 0:
		cnf.Relay.MaxRetryAttempts = defaultRelay.MaxRetryAttempts
	case cnf.Relay.MaxRetryAttempts > MaxRelayRetryAttempts:
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Relay.MaxRetryAttempts,
			"maximum":    MaxRelayRetryAttempts,
			"variable":   "RELAY_MAX_RETRY_ATTEMPTS",
		}).Warn(
			"relay max_retry_attempts exceeds the supported maximum and is being clamped; " +
				"the attempt number is an exported metric label and its domain is fixed",
		)
		cnf.Relay.MaxRetryAttempts = MaxRelayRetryAttempts
	}

	if cnf.Relay.RetryBaseBackoffMS == 0 {
		cnf.Relay.RetryBaseBackoffMS = defaultRelay.RetryBaseBackoffMS
	}
	if cnf.Relay.RetryMaxBackoffMS == 0 {
		cnf.Relay.RetryMaxBackoffMS = defaultRelay.RetryMaxBackoffMS
	}

	// RETENTION IS NOT DEFAULTED, and the asymmetry with the three values above is the
	// point. Those three have a correct answer that requirement R-4 fixes, so an unset
	// value is filled in. A retention period has no correct answer this code can know —
	// it depends on jurisdiction, audit programme and any legal hold in force — and
	// getting it wrong deletes ledger-adjacent evidence. So zero is left as zero and
	// means "retention disabled", which is the only safe default for a destructive
	// operation.
	//
	// A NEGATIVE value is refused rather than honoured. Read literally it is a cutoff in
	// the FUTURE, which would delete every terminal row including ones delivered seconds
	// ago — the most destructive possible reading of what is almost certainly a typo.
	if cnf.Relay.EventRetentionDays < 0 {
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Relay.EventRetentionDays,
			"variable":   "RELAY_EVENT_RETENTION_DAYS",
		}).Warn(
			"relay event_retention_days is negative, which would place the retention cutoff in the " +
				"future and delete every delivered event; retention is being disabled instead",
		)
		cnf.Relay.EventRetentionDays = 0
	}

	// PURGE CAPACITY (PERF-P23). Unlike the retention PERIOD above, these two DO have a
	// correct answer this code can supply, so an unset value is filled in rather than left to
	// mean "off": a batch size of zero would delete nothing at all, which is a silently broken
	// sweeper rather than a disabled one. Retention is switched off by the period, in one
	// place, and these only decide how fast it is enforced.
	if cnf.Relay.EventRetentionBatchSize <= 0 {
		cnf.Relay.EventRetentionBatchSize = defaultRelay.EventRetentionBatchSize
	}

	// The batch CEILING distinguishes unset from unbounded, because an unset int is zero and
	// both readings of zero cannot be honoured. Zero DEFAULTS — the per-sweep bound is what
	// protects a live relay from a single sweep attempting an entire historical backlog, and a
	// deployment that never mentioned this setting must keep that protection. Asking for no
	// ceiling is spelled EventRetentionUnboundedSweep, and any negative is normalised onto it
	// so a reader downstream has one value to recognise rather than a sign to test.
	switch {
	case cnf.Relay.EventRetentionMaxBatchesPerSweep == 0:
		cnf.Relay.EventRetentionMaxBatchesPerSweep = defaultRelay.EventRetentionMaxBatchesPerSweep
	case cnf.Relay.EventRetentionMaxBatchesPerSweep < 0:
		cnf.Relay.EventRetentionMaxBatchesPerSweep = EventRetentionUnboundedSweep
	}

	// Reported when retention is ON, because capacity only matters if something is deleting,
	// and reported as ROWS PER HOUR rather than as its two factors — that is the number an
	// operator has to compare against their arrival rate, and making them multiply it
	// themselves is how the previous default went eighteen times under peak unnoticed.
	if cnf.Relay.EventRetentionDays > 0 {
		fields := logrus.Fields{
			"batch_size":  cnf.Relay.EventRetentionBatchSize,
			"max_batches": cnf.Relay.EventRetentionMaxBatchesPerSweep,
		}

		if cnf.Relay.EventRetentionMaxBatchesPerSweep > 0 {
			fields["purge_capacity_rows_per_hour"] =
				cnf.Relay.EventRetentionMaxBatchesPerSweep * cnf.Relay.EventRetentionBatchSize
		} else {
			fields["purge_capacity_rows_per_hour"] = "unbounded"
		}

		logrus.WithFields(fields).Info(
			"event outbox retention is enabled; compare the purge capacity against your event " +
				"arrival rate, because capacity below arrivals means the table grows without bound " +
				"however short the retention period is",
		)
	}
	// TWO PUBLISHED SPELLINGS OF ONE CEILING. RELAY_SUBSCRIBER_METRICS_BUDGET and
	// EVENT_METRICS_SUBSCRIBER_BUDGET both name how many registry rows ONE consumer-lag
	// sweep is allowed to measure. Both are honoured because both were documented, and
	// both are reconciled onto a single value HERE — after setKafkaDefaults has defaulted
	// and clamped its side — because two fields holding one knob is how the budget that
	// actually took effect comes to depend on which construction path ran: the collector
	// reads Kafka.MetricsSubscriberBudget directly, while the server's collector wiring
	// passes Relay.SubscriberMetricsBudget.
	//
	// A NEGATIVE value is refused rather than honoured. Read literally it would measure
	// nothing at all, which would leave every subscriber's lag unmeasured — the exact
	// condition the budget's own gauge exists to make visible, arrived at by
	// configuration instead of by scale.
	if cnf.Relay.SubscriberMetricsBudget < 0 {
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Relay.SubscriberMetricsBudget,
			"variable":   "RELAY_SUBSCRIBER_METRICS_BUDGET",
			"using":      defaultRelay.SubscriberMetricsBudget,
		}).Warn(
			"relay subscriber_metrics_budget is negative, which would leave every subscriber's " +
				"consumer lag unmeasured; the default is being used instead",
		)
		cnf.Relay.SubscriberMetricsBudget = 0
	}

	switch {
	case cnf.Relay.SubscriberMetricsBudget == 0:
		// Unset under this spelling: adopt whatever setKafkaDefaults settled on, which is
		// either the operator's EVENT_METRICS_SUBSCRIBER_BUDGET or the shipped default.
		cnf.Relay.SubscriberMetricsBudget = cnf.Kafka.MetricsSubscriberBudget
	case cnf.Kafka.MetricsSubscriberBudget != defaultKafka.MetricsSubscriberBudget &&
		cnf.Kafka.MetricsSubscriberBudget != cnf.Relay.SubscriberMetricsBudget:
		// Both spellings were set, to different values. Neither can be silently discarded,
		// so the disagreement is reported naming both variables, and the value the
		// collector reads directly wins so that the clamp it is subject to always applies.
		logrus.WithFields(logrus.Fields{
			"relay_subscriber_metrics_budget": cnf.Relay.SubscriberMetricsBudget,
			"event_metrics_subscriber_budget": cnf.Kafka.MetricsSubscriberBudget,
			"applied":                         cnf.Kafka.MetricsSubscriberBudget,
		}).Warn(
			"RELAY_SUBSCRIBER_METRICS_BUDGET and EVENT_METRICS_SUBSCRIBER_BUDGET name the same " +
				"consumer-lag sweep ceiling and were set to different values; " +
				"EVENT_METRICS_SUBSCRIBER_BUDGET has been applied to both, because that is the " +
				"value the metrics collector reads and the one the supported ceiling clamps",
		)
		cnf.Relay.SubscriberMetricsBudget = cnf.Kafka.MetricsSubscriberBudget
	default:
		// Set here and either agreeing with the other spelling or the only one set: this is
		// the value that must reach the collector, clamped by the same ceiling.
		if cnf.Relay.SubscriberMetricsBudget > MaxMetricsSubscriberBudget {
			logrus.WithFields(logrus.Fields{
				"configured": cnf.Relay.SubscriberMetricsBudget,
				"applied":    MaxMetricsSubscriberBudget,
				"variable":   "RELAY_SUBSCRIBER_METRICS_BUDGET",
			}).Warn(
				"relay subscriber_metrics_budget is above the supported ceiling and has been " +
					"clamped; each subscriber measured costs two broker round trips per authorised " +
					"topic and one exported gauge series per topic",
			)
			cnf.Relay.SubscriberMetricsBudget = MaxMetricsSubscriberBudget
		}
		cnf.Kafka.MetricsSubscriberBudget = cnf.Relay.SubscriberMetricsBudget
	}

}

// EventRetentionPeriod returns the configured retention period as a duration, and zero
// when retention is disabled.
//
// It exists so no caller multiplies days by hours itself. Two callers doing that
// arithmetic separately is how one of them ends up an order of magnitude out, and the
// consequence here is deleted ledger evidence rather than a wrong number on a dashboard.
//
// Returns:
//   - time.Duration: the retention period, or 0 when retention is disabled.
func (cnf *Configuration) EventRetentionPeriod() time.Duration {
	if cnf == nil || cnf.Relay.EventRetentionDays <= 0 {
		return 0
	}

	return time.Duration(cnf.Relay.EventRetentionDays) * 24 * time.Hour
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
// An empty/unset whitelist yields an empty slice, which the upload handler
// treats as deny-by-default.
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
// A NIL result means "trust nothing", and that is what an empty or unset value yields.
// The distinction is load-bearing: the router hands this straight to gin's
// SetTrustedProxies, where nil disables forwarded-header handling altogether and the
// client address becomes the connection's own peer. Returning an empty non-nil slice
// would not be the same thing to a future reader, so nil is returned deliberately.
//
// Entries are NOT validated here. gin parses them and reports a malformed entry, which
// is the only place that can say authoritatively what it accepts; validating twice would
// mean two definitions of a valid CIDR, and the stricter one would silently reject a
// value the router would have taken.
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
//
// The formatter is unconditional, as it always was. The LEVEL is read straight from the
// environment here, ahead of the configuration file, because InitConfig calls this and then
// loads the file — and loading the file logs. Without this, the warnings emitted while the
// configuration is being validated would be filtered by the previous level rather than by
// the one the operator asked for, which is exactly backwards for the run in which somebody
// has just turned debug on to find out what is happening at start-up.
//
// Only the environment is consulted, because that is the only source available yet. The
// value in blnk.json — and the environment again, since envconfig overlays it — is applied
// by setLogLevelDefaults once the configuration exists, and that later application is
// authoritative. An absent or unparseable variable is ignored in silence here: the
// configuration layer reports the same condition properly, with the accepted values named,
// and duplicating the warning at a point where nothing has been loaded would only make it
// look like two separate faults.
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
		// Clamped here as well as in setLogLevelDefaults, and for the same reason: this
		// is the level in force for the whole of start-up, including the configuration
		// load itself. Applying an unclamped quiet level here would discard start-up
		// warnings that the later clamp can no longer bring back, since by then they
		// have already been emitted and dropped.
		//
		// The clamp is silent at this point. Nothing has been loaded yet, and
		// setLogLevelDefaults reports the same condition properly once it can name both
		// the requested and the effective level; warning twice would read as two
		// separate faults.
		effective, _ := clampLogLevel(level)
		logrus.SetLevel(effective)
	}
}

// resolveSearchCredential decides what the TypeSense API key becomes when the operator
// supplied none, and REFUSES the one case in which the historical answer was a hard-coded
// credential in a production deployment. SEC-07.
//
// # What was wrong
//
// setDefaultValues substituted the literal DEFAULT_TYPESENSE_KEY — "blnk-api-key" —
// unconditionally, whenever no key was configured. That literal is not a secret by any
// measure: it is the key in this repository's own compose files, it is in every clone, and it
// is the value a local TypeSense is started with. So a production deployment that had simply
// forgotten BLNK_TYPESENSE_KEY authenticated to its search index — which holds indexed
// transaction, balance and identity records — with a credential anybody can read from GitHub.
// That is CWE-798, and the substitution is also what made it UNDETECTABLE: once it had run,
// the field was non-empty, so nothing downstream could distinguish a key the operator chose
// from one this function invented.
//
// # The three outcomes, and why it is not simply "require it"
//
// The posture is read from Server.Secure, which is this codebase's own production signal —
// the same flag whose falsity already announces that API authentication is disabled. The
// three arms are deliberately different, because a single rule gets one of them wrong:
//
//  1. NOT SECURE — the historical default is applied, exactly as before. This is the posture
//     of the compose stack, the makefile targets and the whole test suite, and a local
//     TypeSense is started with that very key, so refusing here would break every local
//     bring-up to protect a credential that is doing no work.
//  2. SECURE, and no TypeSense host configured — the key is left EMPTY and the condition is
//     reported. Search is optional in Blnk: an unset host means no indexing is in use, and
//     failing to start over a credential for a subsystem the deployment does not run would be
//     a worse defect than the one being fixed.
//  3. SECURE, with a TypeSense host configured — REFUSED. Search is genuinely in use, the
//     operator has supplied no key, and the only remaining options are to invent a public one
//     or to stop. Stopping is correct: the alternative silently indexes ledger data behind a
//     credential that grants anyone who knows it full access to the collection.
//
// Note what this does NOT do. It does not touch search behaviour, the TypeSense client or any
// indexing code — the AAP excludes that feature (§0.6.2) and none of its files are modified.
// It removes a hard-coded credential from the configuration layer, which §0.6.1 puts in scope.
//
// Returns:
//   - error: non-nil only in case 3, carrying the variable to set and the reason.
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
