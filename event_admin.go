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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
)

// event_admin.go is the administrative half of the Kafka event pipeline. Four
// operations live here and nothing else:

// SubscriberSASLMechanism is the SASL mechanism every subscriber credential is
// provisioned with, and the value the credential endpoint reports to the subscriber.
const SubscriberSASLMechanism = "SCRAM-SHA-512"

// MinScramIterations is the smallest PBKDF2 iteration count Kafka accepts for a SCRAM
// credential. A lower value is rejected by the broker, so a requested count below this
// is raised to it rather than passed through to fail.
const MinScramIterations = 4096

// DefaultScramIterations is the iteration count used when a provisioning request does
// not specify one.
const DefaultScramIterations = 4096

// scramSaltLength is the length in bytes of the random salt generated per credential.
const scramSaltLength = 32

// MinSCRAMPasswordLength is the shortest secret this service will mint a credential
// from.
const MinSCRAMPasswordLength = 32

// MinSCRAMPasswordDistinctChars is the minimum number of distinct characters a secret
// must contain.
const MinSCRAMPasswordDistinctChars = 16

// MinTopicPartitions is the minimum number of partitions every event topic is created
// or grown to.
const MinTopicPartitions = 6

// ACLHostAny is the ACL host pattern that matches every client host.
const ACLHostAny = "*"

// kafkaPrincipalPrefix is the prefix Kafka requires on a principal name in an ACL
// binding: the SASL username "acme" is the principal "User:acme". Omitting it produces
// a binding that is accepted and never matches, so the grant looks present in
// kafka-acls output while every request from the subscriber is denied.
const kafkaPrincipalPrefix = "User:"

// kafkaAdminClientID identifies this client in broker logs and request metrics, so an
// operator reading broker-side logs can tell Blnk's administrative traffic apart from
// its producer traffic and from CLI tooling.
const kafkaAdminClientID = "blnk-event-admin"

// kafkaAdminRequestTimeout caps a single administrative round trip.
const kafkaAdminRequestTimeout = 10 * time.Second

// kafkaAdminDialTimeout bounds connection establishment, which includes the TCP
// handshake and the SASL/SCRAM negotiation's two round trips.
const kafkaAdminDialTimeout = 3 * time.Second

// kafkaAdminIdleTimeout is how long an unused administrative connection is kept open.
const kafkaAdminIdleTimeout = 30 * time.Second

// ErrKafkaAdminNotConfigured is returned by every administrative operation when no
// brokers are configured.
var ErrKafkaAdminNotConfigured = errors.New(
	"kafka admin: no brokers are configured (KAFKA_BROKERS is empty), so administrative operations are unavailable",
)

// kafkaAdminAPI is the exact subset of kafka-go's Client that this file uses.
type kafkaAdminAPI interface {
	CreateTopics(ctx context.Context, req *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error)
	CreatePartitions(ctx context.Context, req *kafka.CreatePartitionsRequest) (*kafka.CreatePartitionsResponse, error)
	CreateACLs(ctx context.Context, req *kafka.CreateACLsRequest) (*kafka.CreateACLsResponse, error)
	DeleteACLs(ctx context.Context, req *kafka.DeleteACLsRequest) (*kafka.DeleteACLsResponse, error)
	DescribeACLs(ctx context.Context, req *kafka.DescribeACLsRequest) (*kafka.DescribeACLsResponse, error)
	AlterUserScramCredentials(ctx context.Context, req *kafka.AlterUserScramCredentialsRequest) (*kafka.AlterUserScramCredentialsResponse, error)
	DescribeUserScramCredentials(ctx context.Context, req *kafka.DescribeUserScramCredentialsRequest) (*kafka.DescribeUserScramCredentialsResponse, error)
	Metadata(ctx context.Context, req *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
	OffsetFetch(ctx context.Context, req *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error)
	ListOffsets(ctx context.Context, req *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error)
}

// Compile-time proof that the seam is a faithful subset of the real client. If a
// kafka-go upgrade changes one of these signatures, this line fails the build here
// rather than at some call site with a confusing type error.
var _ kafkaAdminAPI = (*kafka.Client)(nil)

// KafkaAdmin is the administrative surface of the event pipeline, as its consumers see
// it.
type KafkaAdmin interface {
	// IsConfigured reports whether any broker is configured. False means every other
	// method returns ErrKafkaAdminNotConfigured.
	IsConfigured() bool

	// Brokers returns the configured bootstrap broker list, which the credential
	// endpoint hands to subscribers.
	Brokers() []string

	// EnsureTopics creates or grows every topic Blnk owns.
	EnsureTopics(ctx context.Context) (TopicAssuranceReport, error)

	// ProvisionSubscriberPrincipal mints a subscriber's SCRAM credential and binds
	// its ACLs.
	ProvisionSubscriberPrincipal(ctx context.Context, req SubscriberProvisioningRequest) (SubscriberProvisioningResult, error)

	// SubscriberCredentialExists reports whether a principal already holds a SCRAM
	// credential.
	SubscriberCredentialExists(ctx context.Context, principal string) (bool, error)

	// RevokeSubscriber removes a subscriber's ACL bindings and then its SCRAM credential,
	// ending its access at the broker. Call it BEFORE deleting the registry row: the row
	// is the only record of which principal and which bindings to remove.
	RevokeSubscriber(ctx context.Context, subscriber *model.EventSubscriber) error

	// RevokeSubscriberPrincipal deletes one principal's SCRAM credential. It is
	// idempotent, so it is safe to retry and safe to call on a principal that may not
	// exist.
	RevokeSubscriberPrincipal(ctx context.Context, principal string) error

	// PruneSubscriberAccess removes the Blnk-owned bindings a subscriber's RECORDED
	// authorization no longer implies, and creates nothing. It is the first of the three
	// steps an authorization change takes; see its documentation for why narrowing must
	// reach the broker BEFORE the registry records it.
	PruneSubscriberAccess(ctx context.Context, subscriber *model.EventSubscriber) (SubscriberACLReconciliation, error)

	// GrantSubscriberAccess creates the bindings a subscriber's recorded authorization
	// implies, and removes nothing. It is the last of those three steps, so a widening
	// reaches the broker only after the registry records it.
	GrantSubscriberAccess(ctx context.Context, subscriber *model.EventSubscriber) (SubscriberACLReconciliation, error)

	// AuthorizerActive reports whether the broker enforces ACLs at all.
	AuthorizerActive(ctx context.Context) (bool, error)

	// ConsumerLag measures how far a consumer group trails the end of the log.
	ConsumerLag(ctx context.Context, req ConsumerLagRequest) (ConsumerLagReport, error)

	// TopicEndOffsets measures the per-partition offset windows the zero-loss
	// reconciliation classifies each outbox row's stored coordinate against.
	TopicEndOffsets(ctx context.Context, since time.Time, topics ...string) (TopicOffsetReport, error)

	// Close releases the client's pooled connections.
	Close() error
	// CompensateProvisioning undoes the half-completed provisioning a deferred result
	// reported, revoking the credential and removing the bindings the failed attempt
	// attempted. It is the obligation that comes with asking for DeferCompensation.
	CompensateProvisioning(ctx context.Context, result SubscriberProvisioningResult) error
}

// KafkaAdminClient is the kafka-go-backed implementation of KafkaAdmin.
type KafkaAdminClient struct {
	// client is the administrative transport. It is nil, and only nil, when no
	// brokers are configured; every method tests that through the ready guard.
	client kafkaAdminAPI

	// transport is retained solely so Close can return its pooled connections. It is
	// nil when the client was injected by a test, or when unconfigured.
	transport *kafka.Transport

	// brokers is the normalised bootstrap list, returned to subscribers by the
	// credential endpoint.
	brokers []string

	// partitions is the partition count applied to every topic, already raised to
	// MinTopicPartitions.
	partitions int

	// replicationFactor is the replication factor applied to newly created topics, taken
	// from configuration and never defaulted to a literal here. Zero means unconfigured,
	// which EnsureTopics reports as an actionable error.
	replicationFactor int

	// allowPartitionGrowth permits raising the partition count of a topic that already
	// holds records.
	allowPartitionGrowth bool

	// cacheMu guards every memoised answer below. A plain Mutex rather than an RWMutex
	// because a read that misses has to write, so no path is purely a read.
	cacheMu sync.Mutex

	// authorizerProbe memoises whether the broker enforces ACLs.
	authorizerProbe cachedAuthorizerProbe

	// offsetSnapshots memoises the partition metadata and end offsets per topic set.
	offsetSnapshots map[string]*offsetSnapshot

	// offsetSnapshotTTL is how long a snapshot may be reused. Zero selects
	// defaultOffsetSnapshotTTL; a negative value disables caching entirely, which is what
	// a test asserting on raw round trips wants.
	offsetSnapshotTTL time.Duration

	// reservedPrincipals are the SASL identities this deployment uses for its OWN Kafka
	// access — the administrative principal and the producer principal — recorded here so
	// a subscriber provisioning can refuse to overwrite one.
	reservedPrincipals []string

	// now is the clock, injectable so cache expiry is testable without sleeping. Nil
	// means time.Now.
	now func() time.Time
}

// cachedAuthorizerProbe is a memoised answer to "does this broker enforce ACLs".
type cachedAuthorizerProbe struct {
	// active is the cached answer, meaningful only when checkedAt is non-zero.
	active bool

	// checkedAt is when the broker last answered. Zero means never asked.
	checkedAt time.Time
}

// offsetSnapshot is a cached view of the partition layout and the end offsets for one
// set of topics.
type offsetSnapshot struct {
	// partitions maps each topic to its partition IDs.
	partitions map[string][]int

	// bounds maps each topic and partition to its offset bounds.
	bounds map[string]map[int]partitionOffsetBounds

	// missing holds the requested topics the broker did not report.
	missing []string

	// takenAt is when the two reads completed.
	takenAt time.Time
}

// defaultOffsetSnapshotTTL is how long a partition/end-offset snapshot is reused.
const defaultOffsetSnapshotTTL = 5 * time.Second

// maxCachedOffsetSnapshots bounds how many distinct topic sets are remembered.
const maxCachedOffsetSnapshots = 64

// Compile-time proof that the concrete client implements the published interface.
var _ KafkaAdmin = (*KafkaAdminClient)(nil)

// NewKafkaAdmin builds the shared administrative client from configuration.
//
// Parameters:
//   - cnf *config.Configuration: the loaded configuration. A nil configuration is
//     treated as "no brokers", matching the unconfigured steady state.
//
// Returns:
//   - *KafkaAdminClient: a client that is never nil when err is nil, and which is safe
//     to share across goroutines.
//   - error: when the SASL credentials are internally inconsistent, the SCRAM mechanism
//     cannot be constructed, the TLS material is unreadable or invalid, or plaintext
//     would be used without the explicit local-dev acknowledgement.
func NewKafkaAdmin(cnf *config.Configuration) (*KafkaAdminClient, error) {
	if cnf == nil {
		logrus.Debug("kafka admin: configuration is not loaded; administrative operations are unavailable")

		return &KafkaAdminClient{}, nil
	}

	brokers := normalizeKafkaBrokers(cnf.Kafka.Brokers)
	if len(brokers) == 0 {
		logrus.Debug("kafka admin: no brokers are configured; administrative operations are unavailable")

		return &KafkaAdminClient{}, nil
	}

	transport, err := adminTransport(cnf.Kafka)
	if err != nil {
		return nil, err
	}

	admin := &KafkaAdminClient{
		client: &kafka.Client{
			Addr:      kafka.TCP(brokers...),
			Timeout:   kafkaAdminRequestTimeout,
			Transport: transport,
		},
		transport:            transport,
		brokers:              brokers,
		partitions:           resolveTopicPartitions(cnf.Kafka.MinPartitions),
		replicationFactor:    cnf.Kafka.ReplicationFactor,
		allowPartitionGrowth: cnf.Kafka.AllowPartitionGrowth,
		// The identities a subscriber credential must never be minted for.
		reservedPrincipals: reservedKafkaPrincipals(cnf.Kafka),
	}

	if admin.replicationFactor < 1 {
		// Reported here as well as rejected by EnsureTopics, so the misconfiguration is
		// visible at start-up rather than only when a topic is first provisioned.
		logrus.WithField("broker_count", len(brokers)).Warn(
			"kafka admin: KAFKA_REPLICATION_FACTOR is not configured; topic creation will be refused until it is " +
				"set (3 for a replicated production cluster, 1 for a single-broker stack)",
		)
	}

	logrus.WithFields(logrus.Fields{
		"broker_count":       len(brokers),
		"partitions":         admin.partitions,
		"replication_factor": admin.replicationFactor,
		"sasl":               transport.SASL != nil,
		"tls":                transport.TLS != nil,
	}).Debug("kafka admin: client constructed")

	return admin, nil
}

// adminTransport builds the connection pool the administrative client sends through.
func adminTransport(cfg config.KafkaConfig) (*kafka.Transport, error) {
	transport, err := NewKafkaTransport(cfg, KafkaTransportRoleAdmin)
	if err != nil {
		return nil, fmt.Errorf("kafka admin: %w", err)
	}

	// The shared builder sets the publisher's client id and timeouts; the administrative
	// client identifies itself separately in broker logs and gives up sooner.
	transport.ClientID = kafkaAdminClientID
	transport.DialTimeout = kafkaAdminDialTimeout
	transport.IdleTimeout = kafkaAdminIdleTimeout

	return transport, nil
}

// normalizeKafkaBrokers trims each entry and drops blanks.
func normalizeKafkaBrokers(brokers []string) []string {
	normalized := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		if trimmed := strings.TrimSpace(broker); trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}

	if len(normalized) == 0 {
		return nil
	}

	return normalized
}

// resolveTopicPartitions applies the MinTopicPartitions floor to a configured partition
// count.
func resolveTopicPartitions(configured int) int {
	if configured > maxTopicPartitions {
		logrus.WithFields(logrus.Fields{
			"configured": configured,
			"maximum":    maxTopicPartitions,
		}).Warn(
			"kafka admin: KAFKA_MIN_PARTITIONS is implausibly large and is being capped; a partition count this " +
				"high is a configuration error rather than a layout choice",
		)

		return maxTopicPartitions
	}

	if configured >= MinTopicPartitions {
		return configured
	}

	// Zero alone is silent, because zero means UNSET: the configuration defaults it and
	// nothing was overridden.
	if configured != 0 {
		logrus.WithFields(logrus.Fields{
			"configured": configured,
			"minimum":    MinTopicPartitions,
		}).Warn(
			"kafka admin: KAFKA_MIN_PARTITIONS is below the required minimum and is being raised; " +
				"fewer partitions than the minimum would cap how far a subscriber's consumer group can scale",
		)
	}

	return MinTopicPartitions
}

// IsConfigured reports whether this client can talk to a broker at all.
//
// Returns:
//   - bool: true when at least one broker is configured.
func (a *KafkaAdminClient) IsConfigured() bool {
	return a != nil && a.client != nil
}

// Brokers returns the configured bootstrap broker list.
//
// Returns:
//   - []string: a fresh slice the caller may mutate freely; nil when unconfigured.
func (a *KafkaAdminClient) Brokers() []string {
	if a == nil || len(a.brokers) == 0 {
		return nil
	}

	brokers := make([]string, len(a.brokers))
	copy(brokers, a.brokers)

	return brokers
}

// Close returns pooled connections to the operating system.
//
// Returns:
//   - error: always nil.
func (a *KafkaAdminClient) Close() error {
	if a == nil || a.transport == nil {
		return nil
	}

	a.transport.CloseIdleConnections()

	return nil
}

// ready is the guard every administrative operation runs before its first round trip.
func (a *KafkaAdminClient) ready(ctx context.Context) error {
	if !a.IsConfigured() {
		return ErrKafkaAdminNotConfigured
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("kafka admin: %w", err)
	}

	return nil
}
