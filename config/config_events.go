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
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/sirupsen/logrus"
)

// WebhookSunsetRetiredSentinel is the value WEBHOOK_DEPRECATION_SUNSET_DATE takes once
// there is no retirement instant left to state.
const WebhookSunsetRetiredSentinel = "retired"

// WebhookDualDeliveryWindowDays is the exact length of the window in which Kafka
// publishing and legacy HTTP webhook delivery run side by side, in days.
const WebhookDualDeliveryWindowDays = 30

// webhookDualDeliveryWindow is WebhookDualDeliveryWindowDays as a duration, used to
// derive the sunset instant from the window's start and to verify a pair of
// explicitly configured dates really is that far apart.
const webhookDualDeliveryWindow = WebhookDualDeliveryWindowDays * 24 * time.Hour

// WebhookDualDeliveryWindow returns the window length as a duration.
//
// Returns:
//   - time.Duration: WebhookDualDeliveryWindowDays expressed as a duration.
func WebhookDualDeliveryWindow() time.Duration {
	return webhookDualDeliveryWindow
}

// The shipped purge capacity, EXPORTED because the sweeper in the root package needs
// the same two numbers as its fallback for a configuration it cannot read, and two
// copies of a default are two numbers that can disagree. Which one a deployment ran at
// would then depend on start-up ordering, and nothing would report the difference.
const (
	// DefaultEventRetentionBatchSize is how many rows one DELETE statement removes by default.
	DefaultEventRetentionBatchSize = 1000

	// DefaultEventRetentionMaxBatchesPerSweep is how many such statements one sweep issues
	// by default. It is the throughput factor, and it is the safer of the two to raise.
	DefaultEventRetentionMaxBatchesPerSweep = 4000
)

// The shipped REPAIR capacity, EXPORTED for the same reason the retention capacity
// above is: the relay needs these numbers as its fallback for a configuration it cannot read,
// and two copies of a default are two numbers that can disagree about what a deployment ran at.
const (
	// DefaultRelayRepairBatchSize is how many rows one repair claim takes by default. It
	// matches the publish claim's batch size, because a repair row's cost is one Kafka write —
	// the same unit of work the publish batch is sized around.
	DefaultRelayRepairBatchSize = 100

	// DefaultRelayRepairMaxBatchesPerTick is how many repair claims one tick chains by default.
	DefaultRelayRepairMaxBatchesPerTick = 25

	// DefaultRelayRepairConcurrency is how many message-key groups of a repair batch are
	// written at once by default. The same 8 the publish loop uses, for the same reason:
	DefaultRelayRepairConcurrency = 8
)

// MaxRelayRetryAttempts is the CEILING on RELAY_MAX_RETRY_ATTEMPTS, not merely its
// default. A configured value above it is clamped down to it by setRelayDefaults.
const MaxRelayRetryAttempts = 5

// DEFAULT_KEY_SCOPE_ATTESTATION_TIMEOUT_MS bounds one call to the key-authorising
// component's control endpoint when KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TIMEOUT_MS is
// unset.
const DEFAULT_KEY_SCOPE_ATTESTATION_TIMEOUT_MS = 2000

// MaxKeyScopeAttestationTimeout caps a configured attestation timeout.
const MaxKeyScopeAttestationTimeout = 5 * time.Second

// EventRetentionUnboundedSweep is the value of
// RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP that removes the per-sweep batch ceiling
// entirely.
const EventRetentionUnboundedSweep = -1

// KafkaConfig configures the Kafka event-publishing pipeline: the brokers the producer
// and admin client dial, the prefix every category and dead-letter topic is derived
// from, the SASL/SCRAM administrative principal used to provision subscriber
// credentials and ACLs, and the topic geometry applied when topics are created or
// grown.
type KafkaConfig struct {
	// Brokers and TopicPrefix are the two published Kafka contract variables.
	Brokers     []string `json:"brokers"      envconfig:"KAFKA_BROKERS"`
	TopicPrefix string   `json:"topic_prefix" envconfig:"KAFKA_TOPIC_PREFIX"`

	// HistoricalTopicPrefixes lists topic namespaces this deployment USED TO OWN and must
	// still be able to publish to. It is empty in every deployment that has never renamed
	// its namespace, which is almost all of them.
	HistoricalTopicPrefixes []string `json:"historical_topic_prefixes" envconfig:"KAFKA_HISTORICAL_TOPIC_PREFIXES"`

	// SubscriberBrokers is the SUBSCRIBER-FACING bootstrap list, and it is a DIFFERENT
	// LIST FROM Brokers rather than a convenience alias for it.
	SubscriberBrokers []string `json:"subscriber_brokers" envconfig:"KAFKA_SUBSCRIBER_BROKERS"`

	// KeyScopeEnforcement names WHERE a subscriber's partition_key_prefix is enforced, and
	// it exists so that Blnk can never issue a credential whose declared key scope nothing
	// evaluates.
	KeyScopeEnforcement string `json:"key_scope_enforcement" envconfig:"KAFKA_KEY_SCOPE_ENFORCEMENT"`

	// KeyScopeGatewayBrokers is the bootstrap list a key-scoped subscriber is told to
	// dial, and it must be the ENFORCING component rather than the brokers.
	KeyScopeGatewayBrokers []string `json:"key_scope_gateway_brokers" envconfig:"KAFKA_KEY_SCOPE_GATEWAY_BROKERS"`

	// KeyScopeGatewayAttestationURL is the CONTROL endpoint of the key-authorising
	// component, and it is what turns the declaration above from a claim into a verified
	// fact.
	KeyScopeGatewayAttestationURL string `json:"key_scope_gateway_attestation_url" envconfig:"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL"`

	// KeyScopeGatewayAttestationToken is the bearer credential Blnk presents to the
	// control endpoint above.
	KeyScopeGatewayAttestationToken string `json:"key_scope_gateway_attestation_token" envconfig:"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TOKEN"`

	// KeyScopeGatewayAttestationTimeoutMS bounds a single attestation or revocation call.
	KeyScopeGatewayAttestationTimeoutMS int `json:"key_scope_gateway_attestation_timeout_ms" envconfig:"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TIMEOUT_MS"`

	// SubscriberSharedTopicAccess is the deployment ACKNOWLEDGING that a subscriber
	// granted a category topic reads every record on it — every ledger's, every other
	// subscriber's.
	SubscriberSharedTopicAccess bool `json:"subscriber_shared_topic_access" envconfig:"KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS"`

	// SubscriberInternalTopicAccess is the deployment ACKNOWLEDGING that a subscriber
	// granted the internal category topic — `<KAFKA_TOPIC_PREFIX>.system` — reads Blnk's
	// own error text verbatim, for the whole deployment, plus `ledger.created` and every
	// event type the catalogue does not yet recognise.
	SubscriberInternalTopicAccess bool `json:"subscriber_internal_topic_access" envconfig:"KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS"`

	// SUPPLEMENTARY (least privilege). SASLUser and SASLSecret are the STEADY-STATE
	// PRODUCER principal: the identity the event publisher authenticates as. It needs only
	// Write and Describe on the topics Blnk owns.
	SASLUser   string `json:"sasl_user"   envconfig:"KAFKA_SASL_USER"`
	SASLSecret string `json:"sasl_secret" envconfig:"KAFKA_SASL_SECRET"`

	// SASLAdminUser and SASLAdminSecret are published contract variables, and are the
	// ADMINISTRATIVE principal: used only for topic assurance, subscriber SCRAM credential
	// provisioning, ACL grants and revocation, and the offset/lag reads behind
	// reconciliation. Keep them out of every process that only publishes.
	SASLAdminUser   string `json:"sasl_admin_user"   envconfig:"KAFKA_SASL_ADMIN_USER"`
	SASLAdminSecret string `json:"sasl_admin_secret" envconfig:"KAFKA_SASL_ADMIN_SECRET"`

	// SUPPLEMENTARY (topic geometry). the replication factor must be configuration-driven
	// per §0.4.5, because a single-broker local stack cannot satisfy the production factor
	// of 3.
	MinPartitions     int `json:"min_partitions"     envconfig:"KAFKA_MIN_PARTITIONS"`
	ReplicationFactor int `json:"replication_factor" envconfig:"KAFKA_REPLICATION_FACTOR"`

	// TLS carries the transport-security settings both the producer and the
	// administrative client dial with. See KafkaTLSConfig.
	TLS KafkaTLSConfig `json:"tls"`

	// InsecureLocalDev permits an UNENCRYPTED Kafka connection.
	InsecureLocalDev bool `json:"insecure_local_dev" envconfig:"KAFKA_INSECURE_LOCAL_DEV"`

	// AllowPartitionGrowth permits the topic-assurance pass to raise the partition count
	// of a topic that ALREADY HOLDS MESSAGES.
	AllowPartitionGrowth bool `json:"allow_partition_growth" envconfig:"KAFKA_ALLOW_PARTITION_GROWTH"`

	// MetricsSubscriberBudget caps how many registry rows ONE consumer-lag sweep examines.
	MetricsSubscriberBudget int `json:"metrics_subscriber_budget" envconfig:"EVENT_METRICS_SUBSCRIBER_BUDGET"`

	// AllowAdminProducer permits the event publisher to authenticate with the
	// ADMINISTRATIVE credentials when no dedicated producer principal is configured.
	AllowAdminProducer bool `json:"allow_admin_producer" envconfig:"KAFKA_ALLOW_ADMIN_PRODUCER"`
}

// KafkaTLSConfig configures the TLS client both Kafka transports use.
type KafkaTLSConfig struct {
	// ALL SUPPLEMENTARY (transport security).
	Enabled            bool   `json:"enabled"              envconfig:"KAFKA_TLS_ENABLED"`
	CAFile             string `json:"ca_file"              envconfig:"KAFKA_TLS_CA_FILE"`
	CertFile           string `json:"cert_file"            envconfig:"KAFKA_TLS_CERT_FILE"`
	KeyFile            string `json:"key_file"             envconfig:"KAFKA_TLS_KEY_FILE"`
	ServerName         string `json:"server_name"          envconfig:"KAFKA_TLS_SERVER_NAME"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify" envconfig:"KAFKA_TLS_INSECURE_SKIP_VERIFY"`
}

// RelayConfig tunes the transactional-outbox relay that publishes event rows to Kafka.
type RelayConfig struct {
	// ALL THREE ARE PUBLISHED CONTRACT VARIABLES, with fixed defaults:
	MaxRetryAttempts   int `json:"max_retry_attempts"    envconfig:"RELAY_MAX_RETRY_ATTEMPTS"`
	RetryBaseBackoffMS int `json:"retry_base_backoff_ms" envconfig:"RELAY_RETRY_BASE_BACKOFF_MS"`
	RetryMaxBackoffMS  int `json:"retry_max_backoff_ms"  envconfig:"RELAY_RETRY_MAX_BACKOFF_MS"`

	// EventRetentionDays is how long a DISPATCHED event row is kept before it is deleted,
	// and it is a data-protection control rather than a storage tuning knob.
	EventRetentionDays int `json:"event_retention_days" envconfig:"RELAY_EVENT_RETENTION_DAYS"`

	// EventRetentionBatchSize is how many rows ONE delete statement removes.
	EventRetentionBatchSize int `json:"event_retention_batch_size" envconfig:"RELAY_EVENT_RETENTION_BATCH_SIZE"`

	// EventRetentionMaxBatchesPerSweep is how many delete statements one sweep may issue,
	// and with the batch size it is what sets the sweeper's CAPACITY: batches x batch size
	// per sweep, with the sweep running hourly.
	EventRetentionMaxBatchesPerSweep int `json:"event_retention_max_batches_per_sweep" envconfig:"RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP"`

	// TWO SPELLINGS, ONE KNOB, AND THIS IS THE CANONICAL ONE.
	SubscriberMetricsBudget int `json:"subscriber_metrics_budget" envconfig:"RELAY_SUBSCRIBER_METRICS_BUDGET"`

	// REPAIR CAPACITY: how fast the relay clears the two REPAIR backlogs, which is a
	// different question from how fast it publishes new events.
	RepairBatchSize int `json:"repair_batch_size" envconfig:"RELAY_REPAIR_BATCH_SIZE"`

	// RepairMaxBatchesPerTick is how many repair claims ONE tick chains before returning
	// to the publish loop. It is the throughput factor and the safer of the three to
	// raise.
	RepairMaxBatchesPerTick int `json:"repair_max_batches_per_tick" envconfig:"RELAY_REPAIR_MAX_BATCHES_PER_TICK"`

	// RepairConcurrency is how many message-key groups of one repair batch are written at
	// once, mirroring the publish loop's own concurrency.
	RepairConcurrency int `json:"repair_concurrency" envconfig:"RELAY_REPAIR_CONCURRENCY"`
}

// eventStreamingEnvOverride is how the CONVENTIONAL BLNK_-prefixed names for the nested
// Kafka and relay blocks are resolved. It exists because one envconfig pass cannot
// honour both name forms for the same field, and the deployment contract requires both.
//
// Renaming the tags cannot fix that. A tag of BROKERS would make the primary
// BLNK_KAFKA_BROKERS and the alternate a bare BROKERS, honouring the convention but
// breaking the mandated KAFKA_BROKERS.
//
// THE ORDER BELOW IS A DECISION, and it is the opposite of what a reader may expect from
// the feature's own design notes, which asserted that a single tag resolves the bare name
// FIRST and the BLNK_-prefixed name only as a fallback. That is not what
// kelseyhightower/envconfig v1.4.0 does for a NESTED struct: Process("blnk", …) derives
// BLNK_KAFKA_KAFKA_BROKERS for this field, never the bare KAFKA_BROKERS, so both name
// forms exist here only because this pass supplies them — and a pass that runs after
// Process necessarily takes precedence over it.
//
// Prefixed-wins is also the right way round on its own merits: BLNK_-prefixing is the
// convention every other variable in this file follows, an operator who sets the
// prefixed form has named this service explicitly, and the bare names collide with any
// other Kafka client sharing the environment. Both forms resolve either way, which is
// what the deployment contract requires; the precedence decides only which one wins when
// a deployment sets both to different values, and .env.example states it so that is not
// discovered by experiment.
//
//  1. BLNK_KAFKA_BROKERS / BLNK_RELAY_MAX_RETRY_ATTEMPTS — the conventional prefixed
//     name, this struct's primary key. Highest because it matches the prefixed
//     convention every other variable in this file follows.
//  2. KAFKA_BROKERS / RELAY_MAX_RETRY_ATTEMPTS — the bare name the deployment contract
//     mandates, this struct's alternate key.
//  3. BLNK_KAFKA_KAFKA_BROKERS / BLNK_RELAY_RELAY_MAX_RETRY_ATTEMPTS — the key
//     envconfig derives from the nested struct. Still honoured, because it is what the
//     nested pass applies and removing it would break any deployment that found it;
//     lowest of the three because it is an artefact of the nesting rather than a name
//     anybody would choose.
//  4. The matching key under "kafka" or "relay" in blnk.json.
//  5. The defaults in defaultKafka and defaultRelay.
type eventStreamingEnvOverride struct {
	KafkaBrokers *[]string `envconfig:"KAFKA_BROKERS"`
	// KafkaSubscriberBrokers belongs HERE rather than in applyPrefixedEnvAliases for one
	// mechanical reason: it is a LIST, and that function's three alias tables are typed
	// string, int and bool. Resolving it through this struct also means the library
	// performs the comma splitting, so the prefixed alias splits exactly as the bare name
	// does instead of through a second, hand-written parse.
	KafkaSubscriberBrokers *[]string `envconfig:"KAFKA_SUBSCRIBER_BROKERS"`
	// KafkaHistoricalTopicPrefixes is here for the same reason, and its absence was a real
	// defect rather than an omission of convenience. Both compose files forward
	// BLNK_KAFKA_HISTORICAL_TOPIC_PREFIXES and .env.example documents that either form
	// resolves, but with no field here the only names that reached the configuration were
	// the bare KAFKA_HISTORICAL_TOPIC_PREFIXES and the alias envconfig DERIVES for a
	// nested field, BLNK_KAFKA_KAFKA_HISTORICAL_TOPIC_PREFIXES. So the documented and
	// forwarded name was silently ignored, and the symptom is the worst kind this variable
	// has: every event captured under a previous KAFKA_TOPIC_PREFIX is refused at writer
	// resolution after the next restart, having been configured to be publishable.
	KafkaHistoricalTopicPrefixes *[]string `envconfig:"KAFKA_HISTORICAL_TOPIC_PREFIXES"`
	// KafkaKeyScopeGatewayBrokers is a list for the same mechanical reason, and it is
	// resolved here rather than in applyPrefixedEnvAliases so that the mode and the
	// gateway list cannot be resolved by different mechanisms — a deployment that set the
	// mode through one name and the list through the other would declare an enforcement
	// point with no addresses, which KeyScopeGateway reads as "not declared" and refuses
	// issuance on.
	KafkaKeyScopeGatewayBrokers *[]string `envconfig:"KAFKA_KEY_SCOPE_GATEWAY_BROKERS"`
	KafkaKeyScopeEnforcement    *string   `envconfig:"KAFKA_KEY_SCOPE_ENFORCEMENT"`
	// The attestation trio belongs here with the mode and the gateway list, and for the
	// same reason: enforcement is active only when the mode, the addresses AND the control
	// endpoint all resolve, so resolving one of them by a different mechanism from the
	// others is how a deployment ends up declaring an enforcement point it cannot reach —
	// which fails closed and refuses every key-scoped subscriber, on a configuration the
	// operator believes is complete.
	KafkaKeyScopeGatewayAttestationURL       *string `envconfig:"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL"`
	KafkaKeyScopeGatewayAttestationToken     *string `envconfig:"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TOKEN"`
	KafkaKeyScopeGatewayAttestationTimeoutMS *int    `envconfig:"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TIMEOUT_MS"`
	// KafkaSubscriberSharedTopicAccess is the whole-topic ACKNOWLEDGEMENT, and it is a
	// bool, so applyPrefixedEnvAliases could carry it. It is here instead so that every
	// variable governing the subscriber access model resolves through one mechanism: a
	// deployment that declared the acknowledgement by a name this struct honoured and the
	// mode by one it did not would get the widest behaviour from the narrowest-looking
	// configuration.
	KafkaSubscriberSharedTopicAccess *bool `envconfig:"KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS"`
	// KafkaSubscriberInternalTopicAccess is the internal-topic ACKNOWLEDGEMENT, and it is
	// here for the same reason as the acknowledgement above: every variable governing the
	// subscriber access model resolves through one mechanism, so a deployment cannot
	// declare one of them by a name this struct honours and another by a name it does not.
	KafkaSubscriberInternalTopicAccess *bool   `envconfig:"KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS"`
	KafkaTopicPrefix                   *string `envconfig:"KAFKA_TOPIC_PREFIX"`
	KafkaSASLAdminUser                 *string `envconfig:"KAFKA_SASL_ADMIN_USER"`
	KafkaSASLAdminSecret               *string `envconfig:"KAFKA_SASL_ADMIN_SECRET"`
	KafkaMinPartitions                 *int    `envconfig:"KAFKA_MIN_PARTITIONS"`
	KafkaReplicationFactor             *int    `envconfig:"KAFKA_REPLICATION_FACTOR"`

	RelayMaxRetryAttempts                 *int `envconfig:"RELAY_MAX_RETRY_ATTEMPTS"`
	RelayRetryBaseBackoffMS               *int `envconfig:"RELAY_RETRY_BASE_BACKOFF_MS"`
	RelayRetryMaxBackoffMS                *int `envconfig:"RELAY_RETRY_MAX_BACKOFF_MS"`
	RelaySubscriberMetricsBudget          *int `envconfig:"RELAY_SUBSCRIBER_METRICS_BUDGET"`
	RelayEventRetentionDays               *int `envconfig:"RELAY_EVENT_RETENTION_DAYS"`
	RelayEventRetentionBatchSize          *int `envconfig:"RELAY_EVENT_RETENTION_BATCH_SIZE"`
	RelayEventRetentionMaxBatchesPerSweep *int `envconfig:"RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP"`
	RelayRepairBatchSize                  *int `envconfig:"RELAY_REPAIR_BATCH_SIZE"`
	RelayRepairMaxBatchesPerTick          *int `envconfig:"RELAY_REPAIR_MAX_BATCHES_PER_TICK"`
	RelayRepairConcurrency                *int `envconfig:"RELAY_REPAIR_CONCURRENCY"`
}

// applyEventStreamingEnvOverride resolves the Kafka and relay environment variables
// through eventStreamingEnvOverride and copies whatever was set onto cnf.
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
	if override.KafkaHistoricalTopicPrefixes != nil {
		cnf.Kafka.HistoricalTopicPrefixes = *override.KafkaHistoricalTopicPrefixes
	}
	if override.KafkaKeyScopeGatewayBrokers != nil {
		cnf.Kafka.KeyScopeGatewayBrokers = *override.KafkaKeyScopeGatewayBrokers
	}
	if override.KafkaKeyScopeEnforcement != nil {
		cnf.Kafka.KeyScopeEnforcement = *override.KafkaKeyScopeEnforcement
	}
	if override.KafkaKeyScopeGatewayAttestationURL != nil {
		cnf.Kafka.KeyScopeGatewayAttestationURL = *override.KafkaKeyScopeGatewayAttestationURL
	}
	if override.KafkaKeyScopeGatewayAttestationToken != nil {
		cnf.Kafka.KeyScopeGatewayAttestationToken = *override.KafkaKeyScopeGatewayAttestationToken
	}
	if override.KafkaKeyScopeGatewayAttestationTimeoutMS != nil {
		cnf.Kafka.KeyScopeGatewayAttestationTimeoutMS = *override.KafkaKeyScopeGatewayAttestationTimeoutMS
	}
	if override.KafkaSubscriberSharedTopicAccess != nil {
		cnf.Kafka.SubscriberSharedTopicAccess = *override.KafkaSubscriberSharedTopicAccess
	}
	if override.KafkaSubscriberInternalTopicAccess != nil {
		cnf.Kafka.SubscriberInternalTopicAccess = *override.KafkaSubscriberInternalTopicAccess
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
	if override.RelayRepairBatchSize != nil {
		cnf.Relay.RepairBatchSize = *override.RelayRepairBatchSize
	}
	if override.RelayRepairMaxBatchesPerTick != nil {
		cnf.Relay.RepairMaxBatchesPerTick = *override.RelayRepairMaxBatchesPerTick
	}
	if override.RelayRepairConcurrency != nil {
		cnf.Relay.RepairConcurrency = *override.RelayRepairConcurrency
	}

	return nil
}

// EventPublishingConfigured reports whether this deployment has at least one usable
// Kafka broker, and therefore whether the event pipeline captures anything at all.
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

// ReservedSubscriberPrincipalNamespace is the principal prefix Blnk reserves for the
// subscriber identities it mints.
const ReservedSubscriberPrincipalNamespace = "blnk-sub-"

// validateKafkaPrincipalSeparation refuses a Kafka identity configuration in which
// Blnk's own principals could be mistaken for — or could overwrite — a subscriber's.
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
func (cnf *Configuration) resolveWebhookDeprecationWindow() error {
	rawSunset := strings.TrimSpace(cnf.WebhookDeprecationSunsetDate)

	// The sentinel, normalised to its canonical lower-case spelling so every reader
	// downstream compares against one value. The start is cleared with it: a window that
	// has closed has no opening instant left to describe.
	if strings.EqualFold(rawSunset, WebhookSunsetRetiredSentinel) {
		cnf.WebhookDeprecationStartDate = ""
		cnf.WebhookDeprecationSunsetDate = WebhookSunsetRetiredSentinel

		return nil
	}

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
			"webhook_deprecation_sunset_date %q is neither the literal %q nor a valid RFC3339 "+
				"instant (expected %s): %w",
			rawSunset, WebhookSunsetRetiredSentinel, time.RFC3339, err,
		)
	}

	sunset = sunset.UTC()

	// DERIVED, never read as input. Whatever the field held is replaced, which is what
	// makes the window's length a property of this arithmetic rather than a rule.
	cnf.WebhookDeprecationStartDate = sunset.Add(-webhookDualDeliveryWindow).Format(time.RFC3339)
	cnf.WebhookDeprecationSunsetDate = sunset.Format(time.RFC3339)

	return nil
}

// warnOnInsecureKafkaTransport reports a Kafka client that is configured to give up a
// protection it would otherwise have.
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

// warnOnUnusableSubscriberBrokers reports at STARTUP that credential issuance will
// refuse.
func (cnf *Configuration) warnOnUnusableSubscriberBrokers() {
	if len(cnf.Kafka.Brokers) == 0 {
		return
	}

	if _, advertised := cnf.Kafka.SubscriberFacingBrokers(); advertised {
		return
	}

	logrus.Warn(
		"KAFKA_SUBSCRIBER_BROKERS is not configured: POST /subscribers/{id}/kafka-credentials " +
			"will answer 503 SUBSCRIBER_BROKERS_NOT_CONFIGURED and issue nothing. Set it to the " +
			"externally advertised broker addresses subscribers connect to — the same value as " +
			"KAFKA_BROKERS when they run inside this deployment. Publishing is unaffected.",
	)
}

// validateKafkaSASLCredentials refuses a half-configured administrative SASL credential
// at configuration load, so the deployment contract fails where it is described rather
// than at the first broker dial.
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
// intended. Every finding is a warning: relay tuning is an operational knob, and a bad
// value must degrade event delivery rather than prevent the server from starting. It
// runs after setDefaultValues, so it inspects effective values with defaults already
// applied.
func (cnf *Configuration) validateRelayRetryWindow() {
	// An INVERTED window: a base delay above the cap. Both values are legitimate on their
	// own, so neither can be corrected without discarding something the operator asked
	// for. The relay applies the cap last, so every retry waits the capped maximum — the
	// slower schedule, bounded as configured.
	if cnf.Relay.RetryBaseBackoffMS > cnf.Relay.RetryMaxBackoffMS {
		logrus.WithFields(logrus.Fields{
			"retry_base_backoff_ms": cnf.Relay.RetryBaseBackoffMS,
			"retry_max_backoff_ms":  cnf.Relay.RetryMaxBackoffMS,
		}).Warn(
			"relay retry_base_backoff_ms exceeds retry_max_backoff_ms; every retry will wait the capped maximum",
		)
	}
}
