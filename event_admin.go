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
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// event_admin.go is the administrative half of the Kafka event pipeline. Four
// operations live here and nothing else:
//
//  1. Topic assurance — create the topics Blnk publishes to, and grow the ones that
//     exist with too few partitions.
//  2. Subscriber provisioning — mint a SASL/SCRAM credential for a subscriber
//     principal and bind the least-privilege ACLs that are its access boundary.
//  3. Consumer lag — measure how far a subscriber's consumer group trails the end of
//     the log, in-process.
//  4. Topic end offsets — the broker-side half of the daily zero-loss reconciliation.

// SubscriberSASLMechanism is the SASL mechanism every subscriber credential is
// provisioned with, and the value the credential endpoint reports to the subscriber.
const SubscriberSASLMechanism = "SCRAM-SHA-512"

// MinScramIterations is the smallest PBKDF2 iteration count Kafka accepts for a SCRAM
// credential. A lower value is rejected by the broker, so a requested count below this
// is raised to it rather than passed through to fail.
const MinScramIterations = 4096

// DefaultScramIterations is the iteration count used when a provisioning request does
// not specify one.
//
// It matches the default in scripts/kafka-bootstrap.sh, so a credential minted through
// this file and one seeded by the bootstrap script are derived identically. Callers may
// raise it; they cannot lower it below MinScramIterations.
const DefaultScramIterations = 4096

// scramSaltLength is the length in bytes of the random salt generated per credential.
// RFC 8018 recommends at least 8 bytes; 32 is generous, costs nothing, and matches the
// SHA-512 security level the mechanism is derived at.
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
// Administrative traffic is bursty and infrequent — a provisioning call, then nothing
// for hours — so connections are not worth holding much longer than a request cycle.
const kafkaAdminIdleTimeout = 30 * time.Second

// ErrKafkaAdminNotConfigured is returned by every administrative operation when no
// brokers are configured.
var ErrKafkaAdminNotConfigured = errors.New(
	"kafka admin: no brokers are configured (KAFKA_BROKERS is empty), so administrative operations are unavailable",
)

// kafkaAdminAPI is the exact subset of kafka-go's Client that this file uses.
//
// *kafka.Client satisfies this interface as declared, with no adapter.
//
// It was excluded on the same "no destructive operations" reasoning as DeleteTopics,
// and that conflated two very different kinds of destruction. Deleting a TOPIC destroys
// committed events and cannot be undone.
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
//
// The subscriber service depends on this interface rather than on the concrete client
// so that credential issuance is testable without a broker, and so that a deployment
// with no Kafka can be handed an implementation whose methods fail fast.
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
	//
	// A consumer-lag sweep asks the same two questions for every subscriber — which
	// partitions exist, and where each one ends — and only the third, the group's
	// committed offsets, actually differs between them. Caching the shared pair turns an
	// N-subscriber sweep from 3N round trips into 1 metadata + 1 ListOffsets + N
	// OffsetFetch, on the metrics path, at whatever interval the gauge is refreshed.
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
//
// The end offset is the head of the log, and it moves. A cached one is therefore
// slightly behind, and the question is whether that can change an answer anybody acts
// on.
const defaultOffsetSnapshotTTL = 5 * time.Second

// maxCachedOffsetSnapshots bounds how many distinct topic sets are remembered.
//
// The cache is keyed by topic set, and different subscribers legitimately hold
// different grants, so the key space is influenced by registry rows an API client
// authors. A cache with a caller-influenced key space and no bound is a leak.
const maxCachedOffsetSnapshots = 64

// Compile-time proof that the concrete client implements the published interface.
var _ KafkaAdmin = (*KafkaAdminClient)(nil)

// NewKafkaAdmin builds the shared administrative client from configuration.
//
// Building a kafka-go Client is pure struct assembly: the Transport dials lazily on the
// first request. That property is load-bearing rather than incidental, because this
// constructor may be called from process start-up, where a broker that is slow or
// absent must not delay or fail the boot.
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
		// broker_count, not brokers: the value is a COUNT, and a field named for the list
		// would read as the endpoint list an operator could act on. The endpoints are
		// deliberately not logged — see publisherAuthMode's note on why a broker address list
		// is topology an error line does not need.
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
//
// The timeouts remain this client's own, because an administrative request has a
// different shape from a produce: it is rare, it is interactive, and it should give up
// sooner rather than hold a credential-issuance request open.
//
// Parameters:
//   - cfg config.KafkaConfig: the Kafka block, read for the administrative credential
//     pair and the TLS material.
//
// Returns:
//   - *kafka.Transport: never nil when err is nil.
//   - error: a half-configured credential pair, a credential SASL preparation rejects,
//     unreadable or invalid TLS material, or plaintext without the explicit local-dev
//     acknowledgement.
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
//
// The configuration loader already does this, but only on the validate-and-default
// path. A configuration published straight into the store — as tests do, and as any
// caller bypassing that path would — can still carry a stray empty element from a
// trailing comma in KAFKA_BROKERS.
//
// Parameters:
//   - brokers []string: the configured bootstrap list. May be nil.
//
// Returns:
//   - []string: a fresh slice with no blank entries, nil when nothing usable remains.
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
//
// Parameters:
//   - configured int: config.Kafka.MinPartitions. Zero means unset.
//
// Returns:
//   - int: the partition count to provision, never below MinTopicPartitions and never
//     above maxTopicPartitions.
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
// Callers use it to choose a response rather than to guess from an error: the
// credential endpoint answers service-unavailable when Kafka is not configured, and the
// statistics endpoint omits broker-side offsets instead of failing the whole request.
//
// Returns:
//   - bool: true when at least one broker is configured.
func (a *KafkaAdminClient) IsConfigured() bool {
	return a != nil && a.client != nil
}

// Brokers returns the configured bootstrap broker list.
//
// The credential endpoint returns this to a subscriber as the address to connect to, so
// it must be the same list this client dials — reading it from here rather than from
// configuration a second time is what guarantees that.
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
// kafka-go has no Client.Close; the connection pool belongs to the Transport, so
// closing idle connections there is the whole of it. It is safe to call on an
// unconfigured client and safe to call more than once, because a shared client is
// closed on the way out of a process that may have several shutdown paths.
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
//
// It enforces two properties the requirements state separately:
//
//   - An unconfigured client fails FAST. No dial is attempted, so a deployment without
//     Kafka gets an immediate, typed answer rather than a connection timeout.
//   - Context cancellation is honoured EVEN IF the deadline has already passed. Without
//     this check, an expired context would still perform a network round trip before
//     kafka-go noticed, which is exactly the behaviour a 5-second provisioning budget
//     cannot tolerate.
//
// Parameters:
//   - ctx context.Context: the caller's context.
//
// Returns:
//   - error: ErrKafkaAdminNotConfigured, or the wrapped context error (so errors.Is
//     against context.Canceled and context.DeadlineExceeded still works), or nil.
func (a *KafkaAdminClient) ready(ctx context.Context) error {
	if !a.IsConfigured() {
		return ErrKafkaAdminNotConfigured
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("kafka admin: %w", err)
	}

	return nil
}

// TopicAssurance is what happened to one topic during EnsureTopics.
//
// Every field is reported rather than merely logged, because topic geometry is a silent
// failure surface: a topic with one partition works, accepts messages and loses only
// the ordering guarantee, so an operator needs to be able to assert the geometry rather
// than infer it from the absence of an error.
type TopicAssurance struct {
	// Topic is the fully-qualified topic name, as resolved by event_topics.go.
	Topic string

	// Created is true when this run created the topic.
	Created bool

	// PartitionsBefore is the partition count found before this run, and 0 for a topic
	// this run created.
	PartitionsBefore int

	// PartitionsAfter is the partition count in force after this run.
	PartitionsAfter int

	// PartitionsAdded is true when this run grew an existing topic.
	PartitionsAdded bool

	// ShrinkRefused is true when the topic has MORE partitions than configured and was
	// deliberately left alone. See EnsureTopics for why shrinking is never attempted.
	ShrinkRefused bool

	// GrowthRefused is true when the topic has FEWER partitions than configured, already
	// holds records, and was therefore not grown.
	GrowthRefused bool

	// ReplicationFactor is the topic's OBSERVED minimum replica count across its
	// partitions, or the factor it was created with for a topic this run created. Zero
	// means the topic was not visible in metadata yet.
	ReplicationFactor int

	// ReplicationInadequate is true when the observed replica count is below the
	// configured KAFKA_REPLICATION_FACTOR.
	ReplicationInadequate bool
}

// TopicAssuranceReport is the outcome of one EnsureTopics call across every topic Blnk
// owns.
type TopicAssuranceReport struct {
	// Topics carries one entry per topic, in the canonical order AllTopicsWithDeadLetters
	// returns: every category topic, then each one's dead-letter siblings. The stable
	// order is what lets the report be diffed against the provisioning script line for
	// line.
	Topics []TopicAssurance

	// GrowthRefusedCount is how many topics needed partitions and were not grown because
	// they already hold records. A non-zero value is a geometry defect that needs a
	// planned migration, and EnsureTopics returns ErrPartitionGrowthRefused alongside it.
	GrowthRefusedCount int

	// Partitions is the partition count applied, after the MinTopicPartitions floor.
	Partitions int

	// ReplicationFactor is the factor applied to newly created topics, straight from
	// configuration.
	ReplicationFactor int

	// CreatedCount, GrownCount, UnchangedCount and ShrinkRefusedCount summarise Topics.
	// They are computed here rather than left to the caller so that a log line or an
	// operator response does not have to re-derive them.
	CreatedCount       int
	GrownCount         int
	UnchangedCount     int
	ShrinkRefusedCount int

	// CompletedAt is when the assurance finished.
	CompletedAt time.Time
}

// Lookup finds the assurance for one topic.
//
// Parameters:
//   - topic string: a fully-qualified topic name.
//
// Returns:
//   - TopicAssurance: the entry, or the zero value when absent.
//   - bool: whether the topic appears in the report.
func (r TopicAssuranceReport) Lookup(topic string) (TopicAssurance, bool) {
	for _, assurance := range r.Topics {
		if assurance.Topic == topic {
			return assurance, true
		}
	}

	return TopicAssurance{}, false
}

// TopicNames returns the topic names covered by the report, in canonical order.
//
// Returns:
//   - []string: a fresh slice, nil when the report is empty.
func (r TopicAssuranceReport) TopicNames() []string {
	if len(r.Topics) == 0 {
		return nil
	}

	names := make([]string, 0, len(r.Topics))
	for _, assurance := range r.Topics {
		names = append(names, assurance.Topic)
	}

	return names
}

// maxTopicPartitions bounds the configured partition count.
const maxTopicPartitions = 10000

// topicCreationOutcome records which topics a create pass actually created and which
// ones turned out to exist already.
type topicCreationOutcome struct {
	// created holds the topics this process created.
	created map[string]bool

	// raced holds topics that the metadata probe reported as absent but that the broker
	// answered TOPIC_ALREADY_EXISTS for. Another provisioner — a second server instance,
	// or scripts/kafka-provision.sh — created them in between. Their partition count is
	// unknown at that point and has to be re-probed before the grow pass can decide
	// anything.
	raced []string
}

// EnsureTopics creates every topic Blnk publishes to and grows any that exist with too
// few partitions.
//
// The operation is safe to run repeatedly and concurrently with itself. A topic that
// already exists is not an error: it is reported, logged at info, and then checked for
// partition count.
//
// Parameters:
//   - ctx context.Context: cancelled or expired before any round trip is attempted.
//
// Returns:
//   - TopicAssuranceReport: populated even when an error is returned, so a caller can
//     see how far the assurance got.
//   - error: ErrKafkaAdminNotConfigured when no broker is configured, an actionable
//     error when the replication factor is unconfigured, ErrPartitionGrowthRefused when
//     a non-empty topic needs growing, ErrReplicationFactorInadequate when an existing
//     topic is under-replicated, or a wrapped broker error.
func (a *KafkaAdminClient) EnsureTopics(ctx context.Context) (_ TopicAssuranceReport, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "ensure_topics")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	report := TopicAssuranceReport{CompletedAt: time.Now().UTC()}
	if err := a.ready(ctx); err != nil {
		return report, err
	}

	report.Partitions = a.partitions
	report.ReplicationFactor = a.replicationFactor

	if a.replicationFactor < 1 {
		return report, errors.New(
			"kafka admin: KAFKA_REPLICATION_FACTOR is not configured, so topics cannot be created; " +
				"set it to 3 on a replicated production cluster, or to 1 on a single-broker stack, " +
				"which cannot satisfy a higher factor",
		)
	}

	// The inventory comes from event_topics.go, never from literals here, so this
	// operation and scripts/kafka-provision.sh provision exactly the same topics under
	// whatever KAFKA_TOPIC_PREFIX is configured.
	desired := AllOwnedTopicsAcrossPrefixes()

	partitionsBefore, err := a.partitionCounts(ctx, desired)
	if err != nil {
		return report, err
	}

	outcome, err := a.createMissingTopics(ctx, desired, partitionsBefore)
	if err != nil {
		return report, err
	}

	if len(outcome.raced) > 0 {
		racedCounts, probeErr := a.partitionCounts(ctx, outcome.raced)
		if probeErr != nil {
			return report, probeErr
		}
		for topic, count := range racedCounts {
			partitionsBefore[topic] = count
		}
	}

	// Re-read as metadata rather than counts, because the replica sets are needed too and a
	// second read would observe a different moment from the one the counts came from.
	observed, err := a.topicMetadata(ctx, desired)
	if err != nil {
		return report, err
	}

	plan := a.planAssurance(desired, partitionsBefore, outcome)
	report.Topics = plan.assurances
	report.CreatedCount = plan.created
	report.UnchangedCount = plan.unchanged
	report.ShrinkRefusedCount = plan.shrinkRefused

	replicationErr := a.verifyReplication(report.Topics, observed)

	var growthErr error
	if len(plan.grow) > 0 {
		growable, refused, inspectErr := a.partitionGrowthDecision(ctx, plan.grow, observed)
		if inspectErr != nil {
			return report, inspectErr
		}

		if len(refused) > 0 {
			a.markGrowthRefused(report.Topics, refused)
			report.GrowthRefusedCount = len(refused)
			growthErr = growthRefusedError(refused, a.partitions)
		}

		if len(growable) > 0 {
			// The plan describes the topics as they are, so a failure here returns a report that
			// still says "one partition", not one that claims a growth that did not happen. The
			// entries are only updated once the broker has confirmed it.
			if growErr := a.growTopics(ctx, growable); growErr != nil {
				return report, growErr
			}

			a.markPartitionsGrown(report.Topics, growable)
			report.GrownCount = len(growable)
		}
	}

	report.CompletedAt = time.Now().UTC()

	// Any cached partition layout is now KNOWN to be wrong rather than merely old: a topic
	// was created or repartitioned. Dropping the snapshot here — rather than waiting for
	// the TTL — is what stops a lag measurement taken straight after provisioning from
	// reporting against a layout that no longer exists.
	if report.CreatedCount > 0 || report.GrownCount > 0 {
		a.InvalidateOffsetSnapshot()
	}

	logrus.WithFields(logrus.Fields{
		"topics":             len(report.Topics),
		"created":            report.CreatedCount,
		"grown":              report.GrownCount,
		"unchanged":          report.UnchangedCount,
		"shrink_refused":     report.ShrinkRefusedCount,
		"growth_refused":     report.GrowthRefusedCount,
		"partitions":         report.Partitions,
		"replication_factor": report.ReplicationFactor,
	}).Info("kafka admin: event topics assured")

	// Joined rather than short-circuited: an operator fixing a geometry problem needs to see
	// every geometry problem, not the first one in an arbitrary order.
	return report, errors.Join(replicationErr, growthErr)
}

// ErrPartitionGrowthRefused reports that a topic needs more partitions and already holds
// records, so growing it would re-map keys and split aggregate histories.
var ErrPartitionGrowthRefused = errors.New(
	"kafka admin: refusing to add partitions to a topic that already holds records",
)

// ErrReplicationFactorInadequate reports that an existing topic has fewer replicas than the
// configured replication factor.
var ErrReplicationFactorInadequate = errors.New(
	"kafka admin: an existing topic has fewer replicas than KAFKA_REPLICATION_FACTOR requires",
)

// verifyReplication records the observed replica count on each assurance entry and
// reports every topic that falls short of the configured factor.
//
// It records BEFORE it judges, so the report describes the cluster accurately whether
// or not an error is returned — which is what makes the error actionable: the operator
// reads the report to see which topics and how far short.
//
// Parameters:
//   - assurances []TopicAssurance: the report entries, mutated in place.
//   - observed map[string]observedTopicMetadata: the metadata read.
//
// Returns:
//   - error: wrapping ErrReplicationFactorInadequate and naming every short topic, or
//     nil.
func (a *KafkaAdminClient) verifyReplication(
	assurances []TopicAssurance,
	observed map[string]observedTopicMetadata,
) error {
	short := make([]string, 0, len(assurances))

	for i := range assurances {
		metadata, exists := observed[assurances[i].Topic]
		if !exists || metadata.minReplicas <= 0 {
			// The topic is not visible yet — created moments ago, or its leader is still being
			// assigned. Its factor was set by whoever created it and cannot be read now; the
			// next assurance pass reads it.
			continue
		}

		assurances[i].ReplicationFactor = metadata.minReplicas

		if metadata.minReplicas < a.replicationFactor {
			assurances[i].ReplicationInadequate = true
			short = append(short, fmt.Sprintf("%s (%d)", assurances[i].Topic, metadata.minReplicas))

			logrus.WithFields(logrus.Fields{
				"topic":             assurances[i].Topic,
				"observed_replicas": metadata.minReplicas,
				"configured_factor": a.replicationFactor,
				"consequence":       "events on this topic are lost if that broker is lost",
				"remedy":            "reassign partitions with kafka-reassign-partitions",
			}).Error(
				"kafka admin: existing topic is under-replicated relative to KAFKA_REPLICATION_FACTOR; " +
					"a replication factor cannot be raised by creating a topic that already exists",
			)
		}
	}

	if len(short) == 0 {
		return nil
	}

	return fmt.Errorf(
		"%w: %s each have fewer replicas than the configured factor %d. Raising the factor of an "+
			"existing topic requires a partition reassignment (kafka-reassign-partitions); it cannot be "+
			"done by re-running topic assurance",
		ErrReplicationFactorInadequate, strings.Join(short, ", "), a.replicationFactor,
	)
}

// partitionGrowthDecision splits the topics that need growing into those that may be
// grown and those that must not be.
//
// Parameters:
//   - ctx context.Context
//   - candidates []string: topics with fewer partitions than configured.
//   - observed map[string]observedTopicMetadata: the metadata read, for partition IDs.
//
// Returns:
//   - growable []string: topics safe to grow, in the candidates' order.
//   - refused []string: topics that hold records and must not be grown.
//   - error: a wrapped broker error from the offset read.
func (a *KafkaAdminClient) partitionGrowthDecision(
	ctx context.Context,
	candidates []string,
	observed map[string]observedTopicMetadata,
) (growable, refused []string, err error) {
	partitions := make(map[string][]int, len(candidates))
	for _, topic := range candidates {
		if metadata, exists := observed[topic]; exists && len(metadata.partitionIDs) > 0 {
			partitions[topic] = metadata.partitionIDs
		}
	}

	holding, err := a.topicsHoldingRecords(ctx, partitions)
	if err != nil {
		return nil, nil, err
	}

	growable = make([]string, 0, len(candidates))
	refused = make([]string, 0, len(candidates))

	for _, topic := range candidates {
		records, occupied := holding[topic]
		switch {
		case !occupied:
			growable = append(growable, topic)

		case a.allowPartitionGrowth:
			logrus.WithFields(logrus.Fields{
				"topic":      topic,
				"records":    records,
				"partitions": a.partitions,
			}).Warn(
				"kafka admin: growing a topic that holds records because KAFKA_ALLOW_PARTITION_GROWTH is " +
					"set. Keys already written will re-map to different partitions, so the per-aggregate " +
					"ordering of existing events is not preserved",
			)

			growable = append(growable, topic)

		default:
			refused = append(refused, topic)
		}
	}

	return growable, refused, nil
}

// markGrowthRefused records a refusal on the report entries.
//
// Parameters:
//   - assurances []TopicAssurance: the report entries, mutated in place.
//   - refused []string: the topics that were not grown.
func (a *KafkaAdminClient) markGrowthRefused(assurances []TopicAssurance, refused []string) {
	names := make(map[string]struct{}, len(refused))
	for _, topic := range refused {
		names[topic] = struct{}{}
	}

	for i := range assurances {
		if _, ok := names[assurances[i].Topic]; ok {
			assurances[i].GrowthRefused = true
		}
	}
}

// growthRefusedError builds the actionable message for a refused growth.
//
// Parameters:
//   - refused []string: the topics that were not grown.
//   - configured int: the partition count they fall short of.
//
// Returns:
//   - error: wrapping ErrPartitionGrowthRefused.
func growthRefusedError(refused []string, configured int) error {
	return fmt.Errorf(
		"%w: %s hold records and have fewer than the configured %d partitions. Adding partitions would "+
			"re-map keys and split existing aggregate histories across partitions, breaking per-aggregate "+
			"ordering irreversibly. Provision a correctly-shaped topic and migrate consumers to it, or set "+
			"KAFKA_ALLOW_PARTITION_GROWTH once that migration is planned",
		ErrPartitionGrowthRefused, strings.Join(refused, ", "), configured,
	)
}

// topicAssurancePlan is what planAssurance decided: the per-topic report entries as the
// topics stand right now, plus the list of topics that still need growing.
type topicAssurancePlan struct {
	assurances    []TopicAssurance
	grow          []string
	created       int
	unchanged     int
	shrinkRefused int
}

// planAssurance decides, per topic, what state it is in and whether it needs growing.
//
// It is a pure function of the probe results, which is what makes every branch —
// created, unchanged, under-partitioned, over-partitioned — reachable from a unit test
// with no broker.
//
// Parameters:
//   - desired []string: the topic inventory, in canonical order.
//   - partitionsBefore map[string]int: partition counts observed before the create
//     pass; a topic absent from the map did not exist.
//   - outcome topicCreationOutcome: what the create pass did.
//
// Returns:
//   - topicAssurancePlan: one assurance entry per desired topic, in the same order, the
//     topics to grow, and the summary counts.
func (a *KafkaAdminClient) planAssurance(
	desired []string,
	partitionsBefore map[string]int,
	outcome topicCreationOutcome,
) topicAssurancePlan {
	plan := topicAssurancePlan{assurances: make([]TopicAssurance, 0, len(desired))}

	for _, topic := range desired {
		assurance := TopicAssurance{Topic: topic, PartitionsAfter: a.partitions}

		existing, exists := partitionsBefore[topic]
		switch {
		case !exists:
			// Created by this run, or by a concurrent provisioner whose partitions the re-probe
			// could not yet see. Either way the topic now exists and its geometry was decided by
			// whoever created it.
			assurance.Created = outcome.created[topic]
			if assurance.Created {
				assurance.ReplicationFactor = a.replicationFactor
				plan.created++
			} else {
				plan.unchanged++
			}

		case existing < a.partitions:
			// Reported as it stands. markPartitionsGrown updates it once the broker has
			// actually added the partitions.
			assurance.PartitionsBefore = existing
			assurance.PartitionsAfter = existing
			plan.grow = append(plan.grow, topic)

		case existing > a.partitions:
			assurance.PartitionsBefore = existing
			assurance.PartitionsAfter = existing
			assurance.ShrinkRefused = true
			plan.shrinkRefused++

			logrus.WithFields(logrus.Fields{
				"topic":         topic,
				"partitions":    existing,
				"configured":    a.partitions,
				"action":        "left unchanged",
				"why_no_shrink": "kafka cannot reduce a partition count, and doing so would move keys between partitions",
			}).Warn(
				"kafka admin: topic has more partitions than configured; it is being left alone. Either raise " +
					"KAFKA_MIN_PARTITIONS to match the topic, or accept the wider layout — reducing partitions " +
					"is impossible in Kafka and would break per-aggregate ordering if it were not",
			)

		default:
			assurance.PartitionsBefore = existing
			plan.unchanged++
		}

		plan.assurances = append(plan.assurances, assurance)
	}

	return plan
}

// markPartitionsGrown records a confirmed growth on the report entries.
//
// It runs only after CreatePartitions has succeeded, which is what keeps the report
// honest: an entry claims PartitionsAdded if and only if the broker added them.
//
// Parameters:
//   - assurances []TopicAssurance: the report entries, mutated in place.
//   - grown []string: the topics the broker confirmed.
func (a *KafkaAdminClient) markPartitionsGrown(assurances []TopicAssurance, grown []string) {
	confirmed := make(map[string]struct{}, len(grown))
	for _, topic := range grown {
		confirmed[topic] = struct{}{}
	}

	for i := range assurances {
		if _, ok := confirmed[assurances[i].Topic]; !ok {
			continue
		}
		assurances[i].PartitionsAdded = true
		assurances[i].PartitionsAfter = a.partitions
	}
}

// createMissingTopics creates the topics the probe did not find, with the configured
// geometry.
//
// Parameters:
//   - ctx context.Context
//   - desired []string: the full inventory, in canonical order.
//   - partitionsBefore map[string]int: the probe result; keys are topics that exist.
//
// Returns:
//   - topicCreationOutcome: which topics were created and which raced.
//   - error: a wrapped transport error, an actionable message for
//     INVALID_REPLICATION_FACTOR, or a wrapped per-topic error.
func (a *KafkaAdminClient) createMissingTopics(
	ctx context.Context,
	desired []string,
	partitionsBefore map[string]int,
) (topicCreationOutcome, error) {
	outcome := topicCreationOutcome{created: make(map[string]bool, len(desired))}

	configs := make([]kafka.TopicConfig, 0, len(desired))
	for _, topic := range desired {
		if _, exists := partitionsBefore[topic]; exists {
			continue
		}
		configs = append(configs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     a.partitions,
			ReplicationFactor: a.replicationFactor,
		})
	}

	if len(configs) == 0 {
		return outcome, nil
	}

	response, err := a.client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: configs})
	if err != nil {
		return outcome, fmt.Errorf(
			"kafka admin: creating %d event topic(s) with %d partitions and replication factor %d: %w",
			len(configs), a.partitions, a.replicationFactor, err,
		)
	}

	for topic, topicErr := range response.Errors {
		switch {
		case topicErr == nil:
			outcome.created[topic] = true

			logrus.WithFields(logrus.Fields{
				"topic":              topic,
				"partitions":         a.partitions,
				"replication_factor": a.replicationFactor,
			}).Info("kafka admin: event topic created")

		case errors.Is(topicErr, kafka.TopicAlreadyExists):
			// Not an error. Another provisioner won the race; the grow pass will bring
			// the topic up to the configured partition count if it needs it.
			outcome.raced = append(outcome.raced, topic)

			logrus.WithField("topic", topic).Info(
				"kafka admin: event topic already exists; leaving it in place and checking its partition count",
			)

		case errors.Is(topicErr, kafka.InvalidReplicationFactor):
			return outcome, fmt.Errorf(
				"kafka admin: cannot create topic %q with replication factor %d — the cluster has fewer brokers "+
					"than that. Set KAFKA_REPLICATION_FACTOR to at most the broker count (1 for the "+
					"single-broker local stack): %w",
				topic, a.replicationFactor, topicErr,
			)

		default:
			return outcome, fmt.Errorf("kafka admin: creating topic %q: %w", topic, topicErr)
		}
	}

	// Sorted so that the re-probe request, and any log line derived from it, is
	// deterministic; response.Errors is a map and iterates in random order.
	sort.Strings(outcome.raced)

	return outcome, nil
}

// growTopics raises the partition count of existing topics to the configured value.
//
// Kafka's CreatePartitions takes the NEW TOTAL count, not a delta, which is why the
// configured value is sent verbatim.
//
// Parameters:
//   - ctx context.Context
//   - topics []string: topics known to have fewer partitions than configured.
//
// Returns:
//   - error: a wrapped transport error, or a wrapped per-topic error.
func (a *KafkaAdminClient) growTopics(ctx context.Context, topics []string) error {
	configs := make([]kafka.TopicPartitionsConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicPartitionsConfig{
			Name: topic,
			//nolint:gosec // resolveTopicPartitions bounds the count well inside int32.
			Count: int32(a.partitions),
		})
	}

	response, err := a.client.CreatePartitions(ctx, &kafka.CreatePartitionsRequest{Topics: configs})
	if err != nil {
		return fmt.Errorf("kafka admin: growing %d topic(s) to %d partitions: %w", len(configs), a.partitions, err)
	}

	for topic, topicErr := range response.Errors {
		switch {
		case topicErr == nil:
			logrus.WithFields(logrus.Fields{
				"topic":      topic,
				"partitions": a.partitions,
			}).Info("kafka admin: event topic partitions increased")

		case errors.Is(topicErr, kafka.InvalidPartitionNumber):
			logrus.WithFields(logrus.Fields{
				"topic":      topic,
				"partitions": a.partitions,
			}).Info(
				"kafka admin: topic already has at least the configured partition count; nothing to grow",
			)

		default:
			return fmt.Errorf(
				"kafka admin: growing topic %q to %d partitions: %w", topic, a.partitions, topicErr,
			)
		}
	}

	return nil
}

// topicPartitions probes which of the given topics exist and which partition IDs each
// has.
//
// It is the single metadata read in this file; topic assurance, consumer lag and the
// offset snapshot all go through it, so they cannot disagree about which topics exist.
//
// Parameters:
//   - ctx context.Context
//   - topics []string: the topics to probe.
//
// Returns:
//   - map[string][]int: ascending partition IDs per EXISTING topic.
//   - error: a wrapped transport error, or a per-topic error that is not simply
//     "unknown topic" or "leader not available" — a topic authorization failure, for
//     instance, which means the administrative principal lacks the grant it needs and
//     must be reported rather than mistaken for an absent topic.
func (a *KafkaAdminClient) topicPartitions(ctx context.Context, topics []string) (map[string][]int, error) {
	metadata, err := a.topicMetadata(ctx, topics)
	if err != nil {
		return nil, err
	}

	partitions := make(map[string][]int, len(metadata))
	for topic := range metadata {
		partitions[topic] = metadata[topic].partitionIDs
	}

	return partitions, nil
}

// observedTopicMetadata is what the broker says about one existing topic.
type observedTopicMetadata struct {
	// partitionIDs are the topic's partition IDs, ascending.
	partitionIDs []int

	// minReplicas is the SMALLEST replica-set size across the topic's partitions.
	minReplicas int
}

// topicMetadata probes which of the given topics exist, which partition IDs each has,
// and how many replicas the least-replicated partition of each has.
//
// It is the single metadata read in this file; topic assurance, consumer lag and the
// offset snapshot all go through it, so they cannot disagree about which topics exist.
//
// Parameters:
//   - ctx context.Context
//   - topics []string: the topics to probe.
//
// Returns:
//   - map[string]observedTopicMetadata: one entry per EXISTING topic.
//   - error: a wrapped transport error, or a per-topic error that is not simply
//     "unknown topic" or "leader not available".
func (a *KafkaAdminClient) topicMetadata(
	ctx context.Context,
	topics []string,
) (map[string]observedTopicMetadata, error) {
	response, err := a.client.Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		return nil, fmt.Errorf("kafka admin: reading topic metadata: %w", err)
	}

	partitions := make(map[string]observedTopicMetadata, len(response.Topics))
	for _, topic := range response.Topics {
		if len(topic.Partitions) > 0 {
			ids := make([]int, 0, len(topic.Partitions))
			minReplicas := -1
			for _, partition := range topic.Partitions {
				ids = append(ids, partition.ID)
				if replicas := len(partition.Replicas); minReplicas < 0 || replicas < minReplicas {
					minReplicas = replicas
				}
			}
			sort.Ints(ids)
			partitions[topic.Name] = observedTopicMetadata{partitionIDs: ids, minReplicas: minReplicas}

			continue
		}

		switch {
		case topic.Error == nil,
			errors.Is(topic.Error, kafka.UnknownTopicOrPartition),
			errors.Is(topic.Error, kafka.LeaderNotAvailable):
			// Absent, or newly created and not yet assigned a leader. Both are treated as "not
			// there yet"; the create pass turns the first into an existing topic and the second
			// into a harmless TOPIC_ALREADY_EXISTS.
			logrus.WithField("topic", topic.Name).Debug("kafka admin: topic not present yet")

		default:
			return nil, fmt.Errorf("kafka admin: reading metadata for topic %q: %w", topic.Name, topic.Error)
		}
	}

	return partitions, nil
}

// partitionCounts is topicMetadata reduced to a count per topic.
//
// Parameters:
//   - ctx context.Context
//   - topics []string: the topics to probe.
//
// Returns:
//   - map[string]int: partition count per existing topic.
//   - error: as topicMetadata.
func (a *KafkaAdminClient) partitionCounts(ctx context.Context, topics []string) (map[string]int, error) {
	partitions, err := a.topicPartitions(ctx, topics)
	if err != nil {
		return nil, err
	}

	counts := make(map[string]int, len(partitions))
	for topic, ids := range partitions {
		counts[topic] = len(ids)
	}

	return counts, nil
}

// topicsHoldingRecords reports which of the given topics currently hold at least one
// retained record.
//
// It is the test partition growth is gated on: growing an EMPTY topic re-maps nothing,
// while growing one that holds records re-maps keys to partitions and splits an
// aggregate's history irreversibly.
//
// Parameters:
//   - ctx context.Context
//   - partitions map[string][]int: the partitions to inspect, per topic.
//
// Returns:
//   - map[string]int64: retained record count per topic, only for topics holding
//     records.
//   - error: a wrapped transport error.
func (a *KafkaAdminClient) topicsHoldingRecords(
	ctx context.Context,
	partitions map[string][]int,
) (map[string]int64, error) {
	if len(partitions) == 0 {
		return nil, nil
	}

	bounds, err := a.offsetBounds(ctx, partitions)
	if err != nil {
		return nil, err
	}

	holding := make(map[string]int64, len(partitions))
	for topic, ids := range partitions {
		var records int64

		for _, id := range ids {
			bound, ok := bounds[topic][id]
			if !ok || bound.unavailable {
				// Unknown is treated as occupied. Guessing "empty" here would authorise a
				// growth that cannot be undone.
				records++

				continue
			}

			records += retainedRecords(bound.first, bound.end)
		}

		if records > 0 {
			holding[topic] = records
		}
	}

	return holding, nil
}

// missingTopics lists the requested topics the broker does not have.
//
// Parameters:
//   - requested []string: the topics asked for, in caller order.
//   - present map[string][]int: the probe result.
//
// Returns:
//   - []string: the absent topics in requested order, nil when all are present.
func missingTopics(requested []string, present map[string][]int) []string {
	missing := make([]string, 0, len(requested))
	for _, topic := range requested {
		if _, exists := present[topic]; !exists {
			missing = append(missing, topic)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	return missing
}

// normalizeTopicList trims a topic list and drops blanks and duplicates, preserving the
// caller's order.
//
// Parameters:
//   - topics []string: may be nil, may contain blanks and duplicates.
//
// Returns:
//   - []string: a fresh slice, nil when nothing usable remains.
func normalizeTopicList(topics []string) []string {
	normalized := make([]string, 0, len(topics))
	seen := make(map[string]struct{}, len(topics))

	for _, topic := range topics {
		trimmed := strings.TrimSpace(topic)
		if trimmed == "" {
			continue
		}
		if _, duplicate := seen[trimmed]; duplicate {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}

	if len(normalized) == 0 {
		return nil
	}

	return normalized
}

// RedactedSecretPlaceholder is what a SubscriberSecret renders as, in every form.
//
// A fixed, obviously-deliberate string rather than an empty value: an empty rendering
// reads as "there was no secret", which would make a leak and an absence look the same in
// a log line.
const RedactedSecretPlaceholder = "[REDACTED]"

// SubscriberSecret carries a generated SCRAM password in a form that cannot be printed,
// logged or serialised by accident.
//
// This type removes the possibility instead of documenting the rule:
//
//   - Format covers EVERY fmt verb, so %s, %v, %+v, %#v, %q and %x all render the
//     placeholder. Format is what fmt consults first, ahead of Stringer.
//   - MarshalJSON and MarshalText cover encoding/json and every library that uses the
//     text marshaller, so a struct carrying one can be serialised without exposing it.
//   - The value itself is unexported, so no package outside this one can read it at all.
type SubscriberSecret struct {
	// value is the plaintext. Unexported so that no other package can read it, and
	// deliberately not tagged: no tag is needed because the marshallers below are what
	// encoding/json consults.
	value string
}

// NewSubscriberSecret wraps a generated password.
//
// Parameters:
//   - password string: the plaintext, generated by the subscriber service.
//
// Returns:
//   - SubscriberSecret: a value that renders as RedactedSecretPlaceholder everywhere.
func NewSubscriberSecret(password string) SubscriberSecret {
	return SubscriberSecret{value: password}
}

// IsZero reports whether no secret is held. Used by validation, which must distinguish
// "absent" from "present but unusable" without reading the value.
func (s SubscriberSecret) IsZero() bool {
	return s.value == ""
}

// Len returns the secret's length in bytes.
//
// The length is safe to publish and is the one property worth reporting: it lets a test
// or an operator confirm a secret of the expected strength was generated without the
// value appearing anywhere.
func (s SubscriberSecret) Len() int {
	return len(s.value)
}

// String renders the placeholder, so a Stringer-aware caller cannot print the secret.
func (s SubscriberSecret) String() string {
	return RedactedSecretPlaceholder
}

// GoString renders the placeholder for %#v, which would otherwise print the struct
// literal including the unexported field's contents.
func (s SubscriberSecret) GoString() string {
	return "SubscriberSecret(" + RedactedSecretPlaceholder + ")"
}

// Format renders the placeholder for every fmt verb.
//
// Implementing fmt.Formatter rather than only fmt.Stringer is what closes %+v, %#v, %q
// and %x: fmt consults Formatter first and ignores Stringer entirely when it is present,
// so a single method covers verbs a Stringer does not reach.
func (s SubscriberSecret) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(RedactedSecretPlaceholder))
}

// MarshalJSON renders the placeholder as a JSON string.
//
// Returning the placeholder rather than an error is deliberate: an error would make any
// struct carrying a secret unserialisable, and a caller would work around that by
// copying the field out — which is the leak this type exists to prevent. A redacted
// value serialises cleanly and says plainly that something was withheld.
func (s SubscriberSecret) MarshalJSON() ([]byte, error) {
	return []byte(`"` + RedactedSecretPlaceholder + `"`), nil
}

// MarshalText renders the placeholder, covering encoding.TextMarshaler consumers.
func (s SubscriberSecret) MarshalText() ([]byte, error) {
	return []byte(RedactedSecretPlaceholder), nil
}

// reveal returns the plaintext. Unexported by design: only this package derives from it.
func (s SubscriberSecret) reveal() string {
	return s.value
}

// SubscriberProvisioningRequest is everything needed to turn a registry row into a real
// Kafka access boundary: one SCRAM credential plus a set of ACL bindings.
//
// SECURITY: Password is the only secret this package ever handles. It is converted into
// a salted PBKDF2 derivation and sent to the broker in that form; it is never logged,
// never returned, never placed in an error message and never persisted.
type SubscriberProvisioningRequest struct {
	// SubscriberID is the registry business key, carried for log correlation only. It
	// takes no part in the credential or the ACLs.
	SubscriberID string

	// Principal is the SASL/SCRAM username, which is the subscriber's Kafka principal
	// (model.EventSubscriber.KafkaPrincipal). Required.
	Principal string

	// Password is the generated secret. Required, and restricted to printable ASCII — see
	// validateSCRAMPassword for why that restriction is a correctness requirement rather
	// than a policy preference.
	Password SubscriberSecret

	// ConsumerGroupPrefix is the consumer group namespace the subscriber is granted Read
	// on, bound with a PREFIXED pattern so every group whose name starts with it is
	// covered. Normally the subscriber's consumer group ID. When empty, no group binding
	// is created and the subscriber cannot join a group at all, which is reported as a
	// warning.
	ConsumerGroupPrefix string

	// Topics is the exact set of topics the credential may read
	// (model.EventSubscriber.AuthorizedTopics). Each is bound with a LITERAL pattern. An
	// empty set is legitimate and means the principal can read nothing — the fail-closed
	// default of a freshly registered subscriber — and is reported as a warning rather
	// than silently widened.
	Topics []string

	// Iterations is the PBKDF2 iteration count. Zero selects DefaultScramIterations, and
	// any value below MinScramIterations is raised to it because the broker would
	// otherwise reject the credential outright.
	Iterations int

	// Host restricts the binding to one client host. Empty selects ACLHostAny, which is
	// the right default for subscribers on networks Blnk does not control.
	Host string

	// KeyScoped says that the registry row this request was built from declares a
	// partition-key prefix, and therefore that RECORD-LEVEL READ MUST BE WITHHELD.
	KeyScoped bool

	// DeferCompensation asks provisioning to REPORT a needed compensation instead of
	// performing it.
	DeferCompensation bool

	// DeclaredKeyScope carries the partition key prefix the REGISTRY ROW records, for the
	// sole purpose of CHECKING KeyScoped against the row it claims to come from. It is
	// never turned into a binding, because there is no binding to turn it into.
	DeclaredKeyScope string
}

// NewSubscriberProvisioningRequest maps a registry row and a freshly generated password
// onto a provisioning request.
//
// The two readings this replaced were both wrong, in opposite directions. Issuance once
// REFUSED any row carrying a prefix, which withheld the only credential that can exist
// for a state the registry is designed to hold.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the registry row.
//   - password string: the generated secret, owned and returned once by the subscriber
//     service.
//
// Returns:
//   - SubscriberProvisioningRequest: ready to pass to ProvisionSubscriberPrincipal.
func NewSubscriberProvisioningRequest(subscriber *model.EventSubscriber, password string) SubscriberProvisioningRequest {
	if subscriber == nil {
		return SubscriberProvisioningRequest{Password: NewSubscriberSecret(password)}
	}

	return SubscriberProvisioningRequest{
		SubscriberID:        subscriber.SubscriberID,
		Principal:           subscriber.KafkaPrincipal,
		Password:            NewSubscriberSecret(password),
		ConsumerGroupPrefix: subscriber.ConsumerGroupID,
		Topics:              subscriber.AuthorizedTopics,
		// Read from the SAME row as the topics, so a grant can never be built with one
		// subscriber's topic list and another's enforcement decision.
		KeyScoped: subscriber.RequiresGatewayDelivery(),
		// The prefix itself travels beside the flag it decided, for no purpose other than
		// letting validate confirm the two agree. Every binding is derived from KeyScoped;
		// nothing reads this value to grant anything.
		DeclaredKeyScope: subscriber.RequestedKeyScope(),
	}
}

// SubscriberProvisioningResult reports what was provisioned, for the caller's log line,
// response and tests.
//
// It carries NO secret, by construction. The password is not a field here and must
// never become one: this value is logged and may be serialised, and the credential
// itself is returned to the subscriber exactly once by the endpoint that generated it.
type SubscriberProvisioningResult struct {
	// SubscriberID echoes the request, for correlation.
	SubscriberID string

	// Principal is the principal the credential belongs to.
	Principal string

	// Mechanism is always SubscriberSASLMechanism.
	Mechanism string

	// Iterations is the PBKDF2 iteration count actually used, after the minimum was
	// applied.
	Iterations int

	// Topics is the normalised topic set the credential was granted Read and Describe
	// on.
	Topics []string

	// ConsumerGroupPrefix is the group namespace granted Read, empty when none was
	// requested.
	ConsumerGroupPrefix string

	// ACLBindings is how many bindings the subscriber's authorization implies, and so how
	// many the broker holds for it after a successful provisioning.
	ACLBindings int

	// ACLBindingsRemoved is how many OBSOLETE Blnk-owned bindings reconciliation deleted:
	// grants from an authorization this subscriber no longer has.
	ACLBindingsRemoved int

	// ForeignACLBindings is how many bindings on this principal Blnk does not provision
	// and deliberately did not touch — a hand-made grant, a DENY, a Write.
	ForeignACLBindings int

	// CompensationOwed reports that provisioning failed after the credential was written
	// and that the caller asked for the compensation to be DEFERRED, so it has not run.
	CompensationOwed bool

	// OwedBindings are the bindings the failed provisioning attempted, so a deferred
	// compensation deletes exactly those rather than everything the principal holds.
	OwedBindings []kafka.ACLEntry

	// ForeignACLBindingsGranting is how many of those foreign bindings GRANT access rather
	// than restricting it.
	ForeignACLBindingsGranting int

	// CredentialReplaced is true when the principal already held a SCRAM credential and
	// this call replaced it. Re-issuing is a supported operation, not an error, and this
	// flag is how an operator sees that an existing consumer's credential just stopped
	// working.
	CredentialReplaced bool

	// AuthorizerActive reports whether the broker confirmed that it enforces ACLs.
	AuthorizerActive bool

	// TopicRecordAccessGranted reports whether the bindings written include Read on the
	// subscriber's topics — that is, whether this principal may fetch records DIRECTLY
	// from the broker.
	TopicRecordAccessGranted bool

	// KeyScopeBoundaryVerified reports that a key-scoped subscriber's partition-key
	// boundary was CONFIRMED to be enforceable before this result was returned.
	KeyScopeBoundaryVerified bool

	// CredentialWritten reports whether a SCRAM credential is believed to exist at the
	// broker when this result was returned.
	CredentialWritten bool

	// Compensated reports that provisioning failed after the credential was written and
	// that the credential was then successfully revoked.
	Compensated bool

	// ProvisionedAt is when provisioning completed.
	ProvisionedAt time.Time
}

// ProvisionSubscriberPrincipal creates or replaces a subscriber's SCRAM credential and
// binds the ACLs that are its access boundary.
//
// The grant is exactly what a consumer needs and nothing more. There is deliberately NO
// cluster-wide Describe, NO wildcard or prefixed TOPIC pattern, and NO Write, Create,
// Delete or Alter of any kind.
//
// Parameters:
//   - ctx context.Context: honoured before every round trip.
//   - req SubscriberProvisioningRequest: the boundary to provision.
//
// Returns:
//   - SubscriberProvisioningResult: partially populated when an error is returned, so a
//     caller can tell whether the credential was written before the bindings failed.
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
func (a *KafkaAdminClient) ProvisionSubscriberPrincipal(
	ctx context.Context,
	req SubscriberProvisioningRequest,
) (_ SubscriberProvisioningResult, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "provision_subscriber_principal",
		hashedSubscriberSpanAttribute(req.SubscriberID))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	result := SubscriberProvisioningResult{
		SubscriberID: strings.TrimSpace(req.SubscriberID),
		Mechanism:    SubscriberSASLMechanism,
	}

	if err := a.ready(ctx); err != nil {
		return result, err
	}

	if err := req.validate(); err != nil {
		return result, err
	}

	principal := req.principal()
	iterations := req.iterations()
	topics := req.normalizedTopics()
	groupPrefix := req.consumerGroupPrefix()

	result.Principal = principal
	result.Iterations = iterations
	result.Topics = topics
	result.ConsumerGroupPrefix = groupPrefix

	// THE RESERVED-PRINCIPAL GATE, because a collision here means the SCRAM upsert below
	// would rotate one of this
	// deployment's OWN credentials and return it in the response. Configuration validation
	// already refuses such a deployment at start-up; this is the second gate, because
	// configuration can be reloaded and an admin client can be constructed from a
	// configuration this process never validated. It costs one slice comparison and it is
	// the difference between an authorized API call minting a subscriber credential and an
	// authorized API call minting a Kafka superuser one.
	if err := a.requirePrincipalNotReserved(principal); err != nil {
		return result, err
	}

	// BEFORE anything is written. A credential minted against a broker that does not
	// enforce ACLs — or one whose enforcement cannot be confirmed — has no boundary, so
	// this is a precondition rather than a diagnostic.
	if err := a.requireEnforcedAuthorizer(ctx, principal); err != nil {
		return result, err
	}
	result.AuthorizerActive = true

	// Informational only: the upsert below works whether or not a credential exists, so a
	// probe failure that is not a cancellation must not stop provisioning.
	existed, err := a.SubscriberCredentialExists(ctx, principal)
	switch {
	case err == nil:
		result.CredentialReplaced = existed
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return result, err
	default:
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(principal),
			"error_class":    kafkaErrorClassField("provision_subscriber_principal", err),
		}).Debug(
			"kafka admin: could not determine whether the principal already holds a credential; provisioning anyway",
		)
	}

	upsertion, err := deriveScramUpsertion(principal, req.Password.reveal(), iterations)
	if err != nil {
		return result, err
	}

	if err := a.upsertScramCredential(ctx, upsertion); err != nil {
		return result, err
	}
	result.CredentialWritten = true

	// RECONCILED, not merely created. The broker's Blnk-owned bindings for this principal
	// are made exactly what the row's authorization implies, so a topic removed from
	// authorized_topics since the last issuance has its Read and Describe bindings DELETED
	// here. Creating without deleting made a subscriber's broker-side grant the union of
	// every authorization it had ever held: narrowing the registry left the old access
	// live, and revocation could not find it either, because revocation derives what to
	// delete from the row that no longer names the topic.
	bindings := req.aclEntries()

	reconciliation, err := a.reconcileSubscriberACLs(ctx, principal, bindings)

	// A refusal here takes the compensation path below, so the credential just written is
	// revoked. Verifying after the response would be verifying nothing: the secret would
	// already be in the caller's hands.
	if err == nil {
		err = verifyKeyScopeBoundary(req, bindings, reconciliation, result.AuthorizerActive)
	}

	if err != nil {
		// the credential exists and its boundary does not. That combination is the one state
		// provisioning must never leave behind, because the principal can authenticate — so
		// it is compensated by revoking the credential before returning.
		if req.DeferCompensation {
			result.CompensationOwed = true
			result.OwedBindings = bindings

			return result, err
		}

		if cleanupErr := a.compensateFailedProvisioning(ctx, principal, bindings); cleanupErr == nil {
			result.CredentialWritten = false
			result.Compensated = true
		}

		return result, err
	}

	result.ACLBindings = len(bindings)
	result.ACLBindingsRemoved = reconciliation.Removed
	// Read back off the bindings that were actually reconciled, not off the request, so the
	// result describes the grant the broker now holds.
	result.TopicRecordAccessGranted = bindingsGrantTopicRead(bindings)
	// True only for a key-scoped request, and by this line the verification above has already
	// passed — provisioning refuses rather than reaching here unverified.
	result.KeyScopeBoundaryVerified = req.KeyScoped
	// Deny bindings only. A foreign ALLOW made reconcileSubscriberACLs return an error
	// above, so a successful provisioning is by construction one with no widening binding
	// on the principal — which is what makes the issuance response's enforced-access
	// declaration true rather than aspirational.
	result.ForeignACLBindings = len(reconciliation.Foreign())

	if len(topics) == 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash":     subscriberLogLabel(principal),
			"subscriber_id_hash": subscriberLogLabel(result.SubscriberID),
		}).Warn(
			"kafka admin: principal provisioned with no authorised topics, so it can read nothing. This is the " +
				"fail-closed default of a newly registered subscriber; grant topics on the subscriber before " +
				"issuing credentials it is expected to consume with",
		)
	}

	if groupPrefix == "" {
		logrus.WithFields(logrus.Fields{
			"principal_hash":     subscriberLogLabel(principal),
			"subscriber_id_hash": subscriberLogLabel(result.SubscriberID),
		}).Warn(
			"kafka admin: principal provisioned without a consumer group grant, so it cannot join a consumer " +
				"group; set the subscriber's consumer group before issuing credentials",
		)
	}

	result.ProvisionedAt = time.Now().UTC()

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":   subscriberLogLabel(result.SubscriberID),
		"principal_hash":       subscriberLogLabel(principal),
		"mechanism":            SubscriberSASLMechanism,
		"iterations":           iterations,
		"topics":               len(topics),
		"consumer_group_hash":  consumerGroupLogLabel(groupPrefix),
		"acl_bindings":         result.ACLBindings,
		"acl_bindings_removed": result.ACLBindingsRemoved,
		"foreign_acl_bindings": result.ForeignACLBindings,
		"foreign_acl_granting": result.ForeignACLBindingsGranting,
		"credential_replaced":  result.CredentialReplaced,
		"authorizer_active":    result.AuthorizerActive,
	}).Info("kafka admin: subscriber principal provisioned")

	return result, nil
}

// validate rejects a request that cannot produce a usable credential, or that asks for
// a boundary this service will not grant.
//
// The topic list is checked against the grantable allowlist for the same reason — see
// normalizedTopics — and the host pattern is checked because an unconstrained host
// string ends up inside an ACL binding.
//
// Returns:
//   - error: nil when the request can be provisioned exactly as asked.
func (r SubscriberProvisioningRequest) validate() error {
	subscriberID := strings.TrimSpace(r.SubscriberID)
	if subscriberID == "" {
		return errors.New(
			"kafka admin: a subscriber id is required to provision a credential; the principal and " +
				"consumer group namespace are derived from it and cannot be taken from the request",
		)
	}

	expectedPrincipal, err := model.CanonicalKafkaPrincipal(subscriberID)
	if err != nil {
		return fmt.Errorf("kafka admin: cannot derive a Kafka principal for this subscriber: %w", err)
	}

	// Compared EXACTLY, not after trimming. The values are quoted so a whitespace-only
	// difference is visible in the message.
	if r.Principal != expectedPrincipal {
		return fmt.Errorf(
			"kafka admin: refusing to provision principal %q for subscriber %q; the only principal "+
				"this subscriber may hold is %q, derived from its identifier. A principal supplied by a "+
				"caller is a request for an access boundary, not for a name",
			r.Principal, subscriberID, expectedPrincipal,
		)
	}

	expectedNamespace, err := model.CanonicalConsumerGroupNamespace(subscriberID)
	if err != nil {
		return fmt.Errorf("kafka admin: cannot derive a consumer group namespace for this subscriber: %w", err)
	}

	// The request carries the subscriber's consumer group, which must be a leaf INSIDE its
	// own namespace. The prefixed binding is then made over the namespace, never over the
	// supplied string, so a group chosen to overlap a sibling's namespace cannot be
	// granted.
	if group := strings.TrimSpace(r.ConsumerGroupPrefix); group != "" &&
		!model.IsInSubscriberGroupNamespace(group, expectedNamespace) {
		return fmt.Errorf(
			"kafka admin: refusing to grant consumer group %q to subscriber %q; it lies outside the "+
				"subscriber's own namespace %q, and a prefixed grant on it would reach another "+
				"subscriber's groups",
			group, subscriberID, expectedNamespace,
		)
	}

	if err := r.validateTopics(); err != nil {
		return err
	}

	if err := r.validateHost(); err != nil {
		return err
	}

	if err := r.validateKeyScope(); err != nil {
		return err
	}

	return validateSCRAMPassword(r.Password.reveal())
}

// validateKeyScope refuses a request that declares a key scope it does not narrow the
// grant for.
//
// It does not attempt to enforce the prefix, and it must not: the only binding shaped
// anything like a key scope is a PREFIXED topic pattern, which matches topic NAMES and
// would widen the grant to every topic sharing the string while appearing to narrow it.
//
// Returns:
//   - error: a refusal naming the prefix and the missing narrowing, otherwise nil.
func (r SubscriberProvisioningRequest) validateKeyScope() error {
	scope := strings.TrimSpace(r.DeclaredKeyScope)
	if scope == "" || r.KeyScoped {
		return nil
	}

	return fmt.Errorf(
		"kafka admin: refusing to provision subscriber %q, whose registry row records the partition "+
			"key prefix %q while this request asks for the ordinary grant; Kafka's authorizer has no "+
			"message-key dimension, so the credential would hold Read on every record of every "+
			"authorised topic and the registry would describe a narrower boundary than exists. Build "+
			"the request with NewSubscriberProvisioningRequest, which withholds record-level Read for "+
			"a key-scoped row, or clear the prefix to accept whole-topic access",
		strings.TrimSpace(r.SubscriberID), scope,
	)
}

// validateTopics refuses any topic outside the subscriber-grantable allowlist.
//
// The list arrives from the registry and ends up as the resource name of a LITERAL ACL
// binding, so whatever is in it is what the credential can read. Three classes have to
// be excluded and each for its own reason:
//
//   - "*" and other WILDCARDS, because Kafka's resource name "*" matches any resource: one
//     such entry turns a per-topic grant into a cluster-wide one.
//   - FOREIGN topics, because a grant over a topic Blnk does not own is a grant into
//     somebody else's data on a broker Blnk shares.
//   - DEAD-LETTER topics, because every DLT carries other subscribers' failed events
//     together with Blnk's own failure metadata, so it has no subscriber audience. That
//     exclusion is structural rather than listed, since SubscriberGrantableTopics composes
//     only "<prefix>.<category>" names and a ".dlt" name can never be one.
//   - THE INTERNAL CATEGORY TOPIC "<prefix>.system", UNLESS this deployment has declared
//     KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS. system.error's frozen payload renders Blnk's
//     error text verbatim and the category is the catalogue's catch-all, so the default
//     grantable set is the three tenant categories; model.SubscriberPrivilegedEventCategories
//     owns that decision, and the refusal below names the variable rather than the allowlist
//     when the name IS a category topic and only the acknowledgement is missing.
//
// Returns:
//   - error: naming the first offending topic, nil when every topic is grantable.
func (r SubscriberProvisioningRequest) validateTopics() error {
	for _, topic := range normalizeTopicList(r.Topics) {
		if !IsSubscriberGrantableTopic(topic) {
			if IsSubscriberPrivilegedTopic(topic) {
				return fmt.Errorf(
					"kafka admin: refusing to grant the internal category topic %q; it carries Blnk's "+
						"own error text verbatim and every event type the catalogue does not yet "+
						"recognise, so it is grantable only where the deployment has acknowledged that "+
						"by setting KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS. Set it, or grant only the "+
						"tenant category topics (%s)",
					topic, strings.Join(SubscriberGrantableTopics(), ", "),
				)
			}

			return fmt.Errorf(
				"kafka admin: refusing to grant topic %q; only Blnk-owned category topics may be "+
					"granted (%s). Dead-letter topics carry other subscribers' failed events together "+
					"with Blnk's failure metadata and have no subscriber audience",
				topic, strings.Join(SubscriberGrantableTopics(), ", "),
			)
		}

		if IsSubscriberPrivilegedTopic(topic) {
			logrus.WithFields(logrus.Fields{
				"subscriber_id_hash": hashLogIdentifier(strings.TrimSpace(r.SubscriberID)),
				"topic":              topic,
			}).Warn(
				"kafka admin: granting the internal category topic to a subscriber, which this " +
					"deployment has acknowledged with KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS. The " +
					"principal will read Blnk's own error text verbatim, for the whole deployment, " +
					"plus every event type the catalogue does not yet recognise",
			)
		}
	}

	return nil
}

// validateHost refuses a host pattern that is neither the explicit any-host wildcard
// nor a plausible single host.
//
// Returns:
//   - error: nil when the host is usable.
func (r SubscriberProvisioningRequest) validateHost() error {
	host := strings.TrimSpace(r.Host)
	if host == "" || host == ACLHostAny {
		return nil
	}

	for i := 0; i < len(host); i++ {
		if character := host[i]; character < '!' || character > '~' ||
			character == '*' || character == '?' || character == ',' {
			return fmt.Errorf(
				"kafka admin: refusing ACL host %q; a host restriction must be a single host, or %q "+
					"to allow any host. Wildcards, separators and non-printable characters are not accepted",
				host, ACLHostAny,
			)
		}
	}

	return nil
}

// principal returns the trimmed SASL username as SUPPLIED.
//
// It is trimmed only so that the comparison in validate reports a whitespace mismatch
// as a mismatch of NAMES rather than of invisible characters. Nothing downstream relies
// on trimming to make a value safe: validate has already established that this equals
// the derived principal exactly.
func (r SubscriberProvisioningRequest) principal() string {
	return strings.TrimSpace(r.Principal)
}

// boundPrincipal returns the principal every ACL binding is made over, DERIVED from the
// subscriber identifier.
func (r SubscriberProvisioningRequest) boundPrincipal() string {
	if derived, err := model.CanonicalKafkaPrincipal(strings.TrimSpace(r.SubscriberID)); err == nil {
		return derived
	}

	return r.principal()
}

// consumerGroupPrefix returns the PREFIXED ACL resource name for this subscriber's
// consumer group namespace, DERIVED from its identifier.
//
// Two decisions are combined here, and both matter:
//
//   - WHETHER to bind at all is taken from the request. A registry row with no consumer group
//     is a subscriber that has not been set up to consume, and it gets no group grant — the
//     fail-closed default, reported as a warning rather than silently widened.
//   - WHAT to bind is DERIVED and never taken from the request. validate has already confirmed
//     the recorded group lies inside this namespace; binding the derived namespace rather than
//     the recorded string is what makes an overlapping grant unreachable even if a future
//     caller stops going through validate.
func (r SubscriberProvisioningRequest) consumerGroupPrefix() string {
	if strings.TrimSpace(r.ConsumerGroupPrefix) == "" {
		return ""
	}

	namespace, err := model.CanonicalConsumerGroupNamespace(strings.TrimSpace(r.SubscriberID))
	if err != nil {
		return ""
	}

	return namespace
}

// host returns the ACL host pattern, defaulting to ACLHostAny.
func (r SubscriberProvisioningRequest) host() string {
	if host := strings.TrimSpace(r.Host); host != "" {
		return host
	}

	return ACLHostAny
}

// iterations returns the PBKDF2 iteration count to use, never below MinScramIterations.
//
// Raising rather than rejecting matters because the broker's own rejection of a low
// count arrives as an UNACCEPTABLE_CREDENTIAL error that reads like a bad password.
func (r SubscriberProvisioningRequest) iterations() int {
	if r.Iterations <= 0 {
		return DefaultScramIterations
	}

	if r.Iterations < MinScramIterations {
		logrus.WithFields(logrus.Fields{
			"requested": r.Iterations,
			"minimum":   MinScramIterations,
		}).Warn(
			"kafka admin: requested SCRAM iteration count is below the minimum Kafka accepts and is being raised",
		)

		return MinScramIterations
	}

	return r.Iterations
}

// normalizedTopics trims the topic list and drops blanks and duplicates, preserving the
// order the registry recorded.
//
// Duplicates would produce duplicate ACL bindings — harmless at the broker, but they
// would inflate the binding count an operator or a test reads, and they make the log
// line misreport the size of the grant.
//
// Returns:
//   - []string: a fresh slice, nil when nothing usable remains.
func (r SubscriberProvisioningRequest) normalizedTopics() []string {
	return normalizeTopicList(r.Topics)
}

// aclEntries builds the exact bindings that constitute the subscriber's access
// boundary.
//
// The bindings, and why each is what it is:
//
//	Topic  <each authorised topic>  LITERAL   Read      Allow  — consume the topic
//	Topic  <each authorised topic>  LITERAL   Describe  Allow  — see its partitions
//	Group  <consumer group prefix>  PREFIXED  Read      Allow  — join and commit
//
// Returns:
//   - []kafka.ACLEntry: the bindings, in a deterministic order — all bindings for the
//     first topic, then the second, and the group binding last.
func (r SubscriberProvisioningRequest) aclEntries() []kafka.ACLEntry {
	principal := kafkaPrincipalPrefix + r.boundPrincipal()
	host := r.host()
	topics := r.normalizedTopics()
	groupPrefix := r.consumerGroupPrefix()

	// The topic operations this grant is made of. Describe is always present; Read is present
	// only when the boundary the row describes is one the broker can keep in full.
	topicOperations := []kafka.ACLOperationType{kafka.ACLOperationTypeDescribe}
	if !r.KeyScoped {
		topicOperations = []kafka.ACLOperationType{
			kafka.ACLOperationTypeRead,
			kafka.ACLOperationTypeDescribe,
		}
	}

	entries := make([]kafka.ACLEntry, 0, len(topics)*len(topicOperations)+1)

	for _, topic := range topics {
		for _, operation := range topicOperations {
			entries = append(entries, kafka.ACLEntry{
				ResourceType:        kafka.ResourceTypeTopic,
				ResourceName:        topic,
				ResourcePatternType: kafka.PatternTypeLiteral,
				Principal:           principal,
				Host:                host,
				Operation:           operation,
				PermissionType:      kafka.ACLPermissionTypeAllow,
			})
		}
	}

	if groupPrefix != "" {
		entries = append(entries, kafka.ACLEntry{
			ResourceType:        kafka.ResourceTypeGroup,
			ResourceName:        groupPrefix,
			ResourcePatternType: kafka.PatternTypePrefixed,
			Principal:           principal,
			Host:                host,
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		})
	}

	if len(entries) == 0 {
		return nil
	}

	return entries
}

// validateSCRAMPassword enforces the one restriction the SCRAM derivation depends on.
//
// A SCRAM client normalises the password with SASLprep before proving knowledge of it,
// while the credential stored at the broker is derived from the bytes handed to
// AlterUserScramCredentials. For printable ASCII the two are identical, because
// SASLprep leaves that range untouched.
//
// Parameters:
//   - password string: the secret. Never echoed, in any branch.
//
// Returns:
//   - error: nil when the password is usable.
func validateSCRAMPassword(password string) error {
	if password == "" {
		return errors.New("kafka admin: a password is required to provision a SCRAM credential")
	}

	for i := 0; i < len(password); i++ {
		if character := password[i]; character < '!' || character > '~' {
			return errors.New(
				"kafka admin: the SCRAM password contains a character outside printable ASCII (0x21-0x7E). " +
					"SASL preparation would rewrite or reject it, producing a credential that cannot " +
					"authenticate; generate the secret from printable ASCII only",
			)
		}
	}

	if len(password) < MinSCRAMPasswordLength {
		return fmt.Errorf(
			"kafka admin: the SCRAM password is shorter than the %d character minimum. A subscriber "+
				"credential is generated, never chosen, and a Kafka SASL handshake has no rate limit or "+
				"lockout, so a short secret is guessable without restriction",
			MinSCRAMPasswordLength,
		)
	}

	distinct := make(map[byte]struct{}, len(password))
	for i := 0; i < len(password); i++ {
		distinct[password[i]] = struct{}{}
	}

	if len(distinct) < MinSCRAMPasswordDistinctChars {
		return fmt.Errorf(
			"kafka admin: the SCRAM password uses fewer than %d distinct characters, so it is long "+
				"without being unpredictable. Generate it from the printable ASCII alphabet rather than "+
				"padding a shorter value",
			MinSCRAMPasswordDistinctChars,
		)
	}

	return nil
}

// deriveScramUpsertion turns a plaintext password into the salted form the broker
// stores.
//
// Deriving client-side rather than sending a plaintext password is also strictly better
// for secret handling: the password itself never crosses the wire, and the broker
// stores only what it needs to verify a SCRAM proof.
//
// Parameters:
//   - principal string: the SASL username, used only in error messages.
//   - password string: the secret. Consumed here and never retained.
//   - iterations int: already normalised to at least MinScramIterations.
//
// Returns:
//   - kafka.UserScramCredentialsUpsertion: ready to send.
//   - error: when the salt cannot be generated or the derivation rejects its
//     parameters.
func deriveScramUpsertion(principal, password string, iterations int) (kafka.UserScramCredentialsUpsertion, error) {
	salt := make([]byte, scramSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return kafka.UserScramCredentialsUpsertion{}, fmt.Errorf(
			"kafka admin: generating a SCRAM salt for principal %q: %w", principal, err,
		)
	}

	saltedPassword, err := pbkdf2.Key(sha512.New, password, salt, iterations, sha512.Size)
	if err != nil {
		return kafka.UserScramCredentialsUpsertion{}, fmt.Errorf(
			"kafka admin: deriving the %s salted password for principal %q with %d iterations: %w",
			SubscriberSASLMechanism, principal, iterations, err,
		)
	}

	return kafka.UserScramCredentialsUpsertion{
		Name:           principal,
		Mechanism:      kafka.ScramMechanismSha512,
		Iterations:     iterations,
		Salt:           salt,
		SaltedPassword: saltedPassword,
	}, nil
}

// upsertScramCredential sends the credential and interprets the per-user result.
//
// Parameters:
//   - ctx context.Context
//   - upsertion kafka.UserScramCredentialsUpsertion: the derived credential.
//
// Returns:
//   - error: a wrapped transport error, a wrapped per-user error, or an error when the
//     broker answered about nobody at all — which would otherwise be read as success.
func (a *KafkaAdminClient) upsertScramCredential(
	ctx context.Context,
	upsertion kafka.UserScramCredentialsUpsertion,
) error {
	response, err := a.client.AlterUserScramCredentials(ctx, &kafka.AlterUserScramCredentialsRequest{
		Upsertions: []kafka.UserScramCredentialsUpsertion{upsertion},
	})
	if err != nil {
		return fmt.Errorf(
			"kafka admin: writing the %s credential for principal %q: %w",
			SubscriberSASLMechanism, upsertion.Name, err,
		)
	}

	for _, outcome := range response.Results {
		if outcome.User != upsertion.Name {
			continue
		}
		if outcome.Error != nil {
			return fmt.Errorf(
				"kafka admin: broker rejected the %s credential for principal %q: %w",
				SubscriberSASLMechanism, upsertion.Name, outcome.Error,
			)
		}

		return nil
	}

	return fmt.Errorf(
		"kafka admin: broker returned no result for principal %q when writing its %s credential, "+
			"so the credential cannot be assumed to exist",
		upsertion.Name, SubscriberSASLMechanism,
	)
}

// createACLBindings sends the bindings and interprets the per-binding results.
//
// Parameters:
//   - ctx context.Context
//   - principal string: used only for error context.
//   - bindings []kafka.ACLEntry: may be empty, in which case no request is sent.
//
// Returns:
//   - error: a wrapped transport error, or the first per-binding failure.
func (a *KafkaAdminClient) createACLBindings(ctx context.Context, principal string, bindings []kafka.ACLEntry) error {
	if len(bindings) == 0 {
		return nil
	}

	// the LAST gate, and the only unbypassable one. Three call sites reach this function —
	// provisioning's reconciliation, PruneSubscriberAccess/GrantSubscriberAccess, and
	// ReconcileSubscriberACLs — and a guard placed at any one of them leaves the other two
	// open. Placing it here means every ACL Blnk creates, on every path present or future,
	// has had its shape checked against the two shapes a subscriber grant is made of.
	if err := validateDesiredACLBindings(principal, bindings); err != nil {
		return err
	}

	response, err := a.client.CreateACLs(ctx, &kafka.CreateACLsRequest{ACLs: bindings})
	if err != nil {
		return fmt.Errorf("kafka admin: binding %d ACL(s) for principal %q: %w", len(bindings), principal, err)
	}

	// The results are positional, so an index identifies the binding that failed.
	for index, bindingErr := range response.Errors {
		if bindingErr == nil {
			continue
		}

		binding := kafka.ACLEntry{}
		if index < len(bindings) {
			binding = bindings[index]
		}

		return fmt.Errorf(
			"kafka admin: binding %s %s on %s %q for principal %q: %w",
			binding.PermissionType, binding.Operation, binding.ResourceType, binding.ResourceName, principal,
			bindingErr,
		)
	}

	return nil
}

// ---------------------------------------------------------------------------------------
// ACL RECONCILIATION

// SubscriberACLReconciliation reports what one reconciliation of a principal's bindings
// did.
type SubscriberACLReconciliation struct {
	// Principal is the bare SASL username the bindings belong to.
	Principal string

	// Desired is how many bindings the subscriber's recorded authorization implies.
	Desired int

	// Managed is how many Blnk-shaped bindings the broker held before reconciliation.
	Managed int

	// Created is how many bindings were added.
	Created int

	// Removed is how many surplus Blnk-shaped bindings were deleted.
	Removed int

	// ForeignAllow describes every binding on this principal that Blnk does not own and
	// that WIDENS its access — an ALLOW of a shape Blnk never provisions, or a binding
	// whose permission type the broker did not state definitively.
	ForeignAllow []string

	// ForeignDeny describes every foreign binding that can only NARROW the principal's
	// access: an explicit DENY.
	ForeignDeny []string
}

// Foreign returns every foreign binding, widening ones first.
//
// It exists for logging and counting, where the distinction does not matter and a
// single list reads better. Anything that ACTS on the difference must read ForeignAllow
// directly: collapsing the two is exactly the conflation that let a widening binding be
// reported with the same weight as a harmless DENY and then survive issuance.
//
// Returns:
//   - []string: a fresh slice; nil when there are none.
func (r SubscriberACLReconciliation) Foreign() []string {
	if len(r.ForeignAllow) == 0 && len(r.ForeignDeny) == 0 {
		return nil
	}

	foreign := make([]string, 0, len(r.ForeignAllow)+len(r.ForeignDeny))
	foreign = append(foreign, r.ForeignAllow...)
	foreign = append(foreign, r.ForeignDeny...)

	return foreign
}

// describeSubscriberACLs reads every ACL binding the broker currently holds for a
// principal.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - principal string: the bare SASL username. Required.
//
// Returns:
//   - []kafka.ACLEntry: every binding held for the principal, in the shape
//     createACLBindings and deleteACLBindings both speak.
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
func (a *KafkaAdminClient) describeSubscriberACLs(ctx context.Context, principal string) ([]kafka.ACLEntry, error) {
	if err := a.ready(ctx); err != nil {
		return nil, err
	}

	principal = strings.TrimSpace(principal)
	if principal == "" {
		return nil, errors.New("kafka admin: a principal is required to read its ACL bindings")
	}

	bound := kafkaPrincipalPrefix + principal

	response, err := a.client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{
		Filter: kafka.ACLFilter{
			ResourceTypeFilter:        kafka.ResourceTypeAny,
			ResourcePatternTypeFilter: kafka.PatternTypeAny,
			PrincipalFilter:           bound,
			Operation:                 kafka.ACLOperationTypeAny,
			PermissionType:            kafka.ACLPermissionTypeAny,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("kafka admin: reading the ACL bindings of principal %q: %w", principal, err)
	}

	if response.Error != nil {
		// SECURITY_DISABLED reaches here on a broker with no authorizer. It is returned as an
		// error rather than as "no bindings": reporting an empty grant would let a caller
		// conclude there was nothing to reconcile on precisely the broker where nothing is
		// enforced at all.
		return nil, fmt.Errorf("kafka admin: reading the ACL bindings of principal %q: %w",
			principal, response.Error)
	}

	bindings := make([]kafka.ACLEntry, 0, 8)
	for _, resource := range response.Resources {
		for _, description := range resource.ACLs {
			if description.Principal != bound {
				continue
			}

			bindings = append(bindings, kafka.ACLEntry{
				ResourceType:        resource.ResourceType,
				ResourceName:        resource.ResourceName,
				ResourcePatternType: resource.PatternType,
				Principal:           description.Principal,
				Host:                description.Host,
				Operation:           description.Operation,
				PermissionType:      description.PermissionType,
			})
		}
	}

	return bindings, nil
}

// blnkManagedACLBinding reports whether a binding is one Blnk itself provisions.
//
// Parameters:
//   - binding kafka.ACLEntry: the observed binding.
//
// Returns:
//   - bool: true when Blnk would have created a binding of this shape.
func blnkManagedACLBinding(binding kafka.ACLEntry) bool {
	if binding.PermissionType != kafka.ACLPermissionTypeAllow {
		return false
	}

	switch binding.ResourceType {
	case kafka.ResourceTypeTopic:
		return binding.ResourcePatternType == kafka.PatternTypeLiteral &&
			(binding.Operation == kafka.ACLOperationTypeRead ||
				binding.Operation == kafka.ACLOperationTypeDescribe)
	case kafka.ResourceTypeGroup:
		return binding.ResourcePatternType == kafka.PatternTypePrefixed &&
			binding.Operation == kafka.ACLOperationTypeRead
	default:
		return false
	}
}

// foreignACLBindingWidens reports whether a foreign binding can GRANT access, as
// opposed to only taking it away.
//
// Parameters:
//   - binding kafka.ACLEntry: a foreign binding.
//
// Returns:
//   - bool: false only for an explicit Deny; true for everything else.
func foreignACLBindingWidens(binding kafka.ACLEntry) bool {
	return binding.PermissionType != kafka.ACLPermissionTypeDeny
}

// reservedKafkaPrincipals returns the SASL usernames this deployment uses for its own
// Kafka access, trimmed and deduplicated, with blanks dropped.
//
// Parameters:
//   - kafka config.KafkaConfig: the loaded Kafka configuration.
//
// Returns:
//   - []string: the reserved usernames; nil when no SASL identity is configured.
func reservedKafkaPrincipals(kafkaConfig config.KafkaConfig) []string {
	candidates := []string{
		strings.TrimSpace(kafkaConfig.SASLAdminUser),
		strings.TrimSpace(kafkaConfig.SASLUser),
	}

	var reserved []string
	for _, candidate := range candidates {
		if candidate == "" || slices.Contains(reserved, candidate) {
			continue
		}

		reserved = append(reserved, candidate)
	}

	return reserved
}

// ErrSubscriberPrincipalReserved is returned when a subscriber's DERIVED principal
// collides with one of this deployment's own Kafka identities.
var ErrSubscriberPrincipalReserved = errors.New(
	"kafka admin: the subscriber's derived principal is one of this deployment's own Kafka " +
		"identities, so provisioning it would rotate and return that credential; KAFKA_SASL_USER " +
		"and KAFKA_SASL_ADMIN_USER must not sit inside the reserved 'blnk-sub-' namespace",
)

// requirePrincipalNotReserved refuses a principal that is one of the deployment's own.
//
// Comparison is exact and case-sensitive on the trimmed value, because Kafka principals
// are case-sensitive: "blnk-sub-x" and "BLNK-SUB-X" are two different identities at the
// broker, so folding case here would refuse a provisioning that is in fact safe.
//
// Parameters:
//   - principal string: the DERIVED subscriber principal, never a caller-supplied one.
//
// Returns:
//   - error: an error wrapping ErrSubscriberPrincipalReserved on a collision, else nil.
func (a *KafkaAdminClient) requirePrincipalNotReserved(principal string) error {
	principal = strings.TrimSpace(principal)
	if principal == "" || !slices.Contains(a.reservedPrincipals, principal) {
		return nil
	}

	logrus.WithField("principal_hash", subscriberLogLabel(principal)).Error(
		"kafka admin: refusing to provision a subscriber credential for a principal that is this " +
			"deployment's own Kafka identity; the SCRAM write would rotate that credential and the " +
			"response would return it. Move KAFKA_SASL_USER and KAFKA_SASL_ADMIN_USER outside the " +
			"reserved 'blnk-sub-' namespace",
	)

	return fmt.Errorf("%w (principal %q)",
		ErrSubscriberPrincipalReserved,
		sanitizeLogValue(principal, maxLoggedFilterLength),
	)
}

// ErrSubscriberForeignACLGrant is returned when a subscriber principal carries an ALLOW
// ACL binding Blnk did not provision.
//
// Nothing in the response said otherwise, and the only trace was one log line in the
// stream of a SUCCESSFUL request.
var ErrSubscriberForeignACLGrant = errors.New(
	"kafka admin: this subscriber principal carries ALLOW ACL bindings Blnk did not provision, " +
		"so its effective access is broader than the subscriber registry records and cannot be " +
		"stated; remove the foreign bindings with kafka-acls, or move them to a principal outside " +
		"the reserved subscriber namespace, and retry",
)

// refuseForeignACLGrant builds the fail-closed error for a principal carrying foreign
// ALLOW bindings.
//
// It wraps ErrSubscriberForeignACLGrant so a caller can classify it with errors.Is
// without matching on the message.
//
// Parameters:
//   - principal string: the bare SASL username.
//   - widening []string: the rendered foreign ALLOW bindings.
//
// Returns:
//   - error: an error wrapping ErrSubscriberForeignACLGrant.
func refuseForeignACLGrant(principal string, widening []string) error {
	return fmt.Errorf("%w (principal %q, %d binding(s): %s)",
		ErrSubscriberForeignACLGrant,
		sanitizeLogValue(principal, maxLoggedFilterLength),
		len(widening),
		strings.Join(widening, "; "),
	)
}

// classifyForeignACLBinding records one foreign binding on the report, in the list its
// permission type puts it in.
//
// Parameters:
//   - report *SubscriberACLReconciliation: the report being assembled.
//   - binding kafka.ACLEntry: a binding blnkManagedACLBinding has already rejected.
func classifyForeignACLBinding(report *SubscriberACLReconciliation, binding kafka.ACLEntry) {
	rendered := fmt.Sprintf("%s %s on %s %q (%s, host %s)",
		binding.PermissionType, binding.Operation, binding.ResourceType,
		sanitizeLogValue(binding.ResourceName, maxLoggedFilterLength),
		binding.ResourcePatternType, sanitizeLogValue(binding.Host, maxLoggedFilterLength))

	if foreignACLBindingWidens(binding) {
		report.ForeignAllow = append(report.ForeignAllow, rendered)

		return
	}

	report.ForeignDeny = append(report.ForeignDeny, rendered)
}

// aclBindingKey renders a binding as the tuple that identifies it, so two bindings can
// be compared as set members.
func aclBindingKey(binding kafka.ACLEntry) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s",
		binding.ResourceType, binding.ResourceName, binding.ResourcePatternType,
		binding.Principal, binding.Host, binding.Operation, binding.PermissionType)
}

// ErrSubscriberBindingShapeUnsupported is returned when a binding Blnk is about to
// CREATE is not one of the two shapes it owns.
var ErrSubscriberBindingShapeUnsupported = errors.New(
	"kafka admin: refusing to create an ACL binding outside the two shapes a subscriber grant is " +
		"made of (Read/Describe Allow on a LITERAL topic name, Read Allow on a PREFIXED consumer " +
		"group namespace)",
)

// validateDesiredACLBindings asserts that every binding about to be WRITTEN is one Blnk
// owns.
//
// Three properties are checked, and each corresponds to a way a subscriber grant could
// become broader than the registry records:
//
//   - THE SHAPE, via blnkManagedACLBinding. A PREFIXED topic pattern instead of a LITERAL one
//     is the dangerous edit — it silently converts "Read blnk.transactions" into "Read every
//     topic whose name starts with blnk.transactions", DLTs included — and a Cluster resource
//     or a Write/Alter operation would be worse still. This is also what forbids a Deny, which
//     Blnk never provisions.
//   - THE PRINCIPAL, which must be the one identity this reconciliation is for. A binding
//     carrying a different principal would grant one subscriber's topics to another's
//     credential, and reconciliation would never remove it because it describes by principal.
//   - THE RESOURCE NAME, which must be non-empty, whitespace-free at its edges, and not the
//     wildcard. Kafka reads the resource name "*" as matching EVERY resource, so a LITERAL
//     binding on "*" passes the shape check and is a cluster-wide grant; the registry's topic
//     allowlist refuses a wildcard at admission, and this is the same refusal at the last
//     moment before it would be written.
//
// Parameters:
//   - principal string: the bound principal, already prefixed as Kafka states it.
//   - desired []kafka.ACLEntry: the bindings aclEntries produced. Empty is valid and
//     means "authorised for nothing", which is a legitimate instruction.
//
// Returns:
//   - error: nil when every binding is owned; otherwise an error wrapping
//     ErrSubscriberBindingShapeUnsupported and naming the offending binding.
func validateDesiredACLBindings(principal string, desired []kafka.ACLEntry) error {
	expected := kafkaPrincipalPrefix + strings.TrimSpace(principal)

	for _, binding := range desired {
		rendered := fmt.Sprintf("%s %s on %s %q (%s)",
			binding.PermissionType, binding.Operation, binding.ResourceType,
			sanitizeLogValue(binding.ResourceName, maxLoggedFilterLength),
			binding.ResourcePatternType)

		if !blnkManagedACLBinding(binding) {
			return fmt.Errorf("%w: %s", ErrSubscriberBindingShapeUnsupported, rendered)
		}

		if binding.Principal != expected {
			return fmt.Errorf(
				"%w: %s names principal %q, but this reconciliation is for %q",
				ErrSubscriberBindingShapeUnsupported, rendered,
				sanitizeLogValue(binding.Principal, maxLoggedFilterLength),
				sanitizeLogValue(expected, maxLoggedFilterLength))
		}

		name := binding.ResourceName
		if name == "" || name != strings.TrimSpace(name) || name == ACLHostAny {
			return fmt.Errorf(
				"%w: %s names a resource that is blank, padded, or the wildcard %q, which Kafka "+
					"reads as matching every resource",
				ErrSubscriberBindingShapeUnsupported, rendered, ACLHostAny)
		}
	}

	return nil
}

// reconcileSubscriberACLs makes the broker's Blnk-owned bindings for a principal
// EXACTLY the desired set.
//
// Every partial failure must leave the principal with FEWER rights than the registry
// records, never more — the same rule RevokeSubscriber follows. Deleting first
// satisfies that: a reconciliation that dies between the two steps has removed access
// it was going to re-grant, which the next reconciliation restores, whereas creating
// first and failing to delete leaves live access the registry says is gone.
//
// A subscriber authorised for nothing is the fail-closed default of a fresh
// registration and the state a caller reaches by clearing the topic list. Reconciling
// to it removes every Blnk-owned binding, which is exactly what "authorised for
// nothing" has to mean at the broker.
//
// Parameters:
//   - ctx context.Context: cancels both round trips.
//   - principal string: the bare SASL username.
//   - desired []kafka.ACLEntry: the bindings the subscriber's recorded authorization
//     implies.
//
// Returns:
//   - SubscriberACLReconciliation: what was found, removed and created.
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
func (a *KafkaAdminClient) reconcileSubscriberACLs(
	ctx context.Context,
	principal string,
	desired []kafka.ACLEntry,
) (SubscriberACLReconciliation, error) {
	report := SubscriberACLReconciliation{
		Principal: strings.TrimSpace(principal),
		Desired:   len(desired),
	}

	// checked BEFORE the describe, so a widened desired set costs no round trip
	// and leaves the broker untouched.
	if err := validateDesiredACLBindings(report.Principal, desired); err != nil {
		return report, err
	}

	observed, err := a.describeSubscriberACLs(ctx, principal)
	if err != nil {
		return report, err
	}

	wanted := make(map[string]struct{}, len(desired))
	for _, binding := range desired {
		wanted[aclBindingKey(binding)] = struct{}{}
	}

	surplus := make([]kafka.ACLEntry, 0, len(observed))
	present := make(map[string]struct{}, len(observed))

	for _, binding := range observed {
		if !blnkManagedACLBinding(binding) {
			classifyForeignACLBinding(&report, binding)

			continue
		}

		report.Managed++

		key := aclBindingKey(binding)
		present[key] = struct{}{}

		if _, keep := wanted[key]; !keep {
			surplus = append(surplus, binding)
		}
	}

	if len(report.ForeignDeny) > 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(report.Principal),
			"bindings":       report.Foreign,
		}).Warn(
			"kafka admin: this principal carries DENY ACL bindings Blnk does not provision and will " +
				"not remove; they only narrow what the grant allows, so provisioning continues — but " +
				"the subscriber may read less than its authorised topics suggest",
		)
	}

	// REFUSED HERE, before anything is deleted or created, so a principal whose
	// effective grant Blnk cannot state is left exactly as it was found.
	if len(report.ForeignAllow) > 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(report.Principal),
			"bindings":       report.ForeignAllow,
		}).Error(
			"kafka admin: this principal carries ALLOW ACL bindings Blnk did not provision, so its " +
				"effective access is broader than the subscriber registry records; refusing to " +
				"reconcile or issue credentials until they are removed by hand",
		)

		return report, refuseForeignACLGrant(report.Principal, report.ForeignAllow)
	}

	if len(surplus) > 0 {
		if err := a.deleteACLBindings(ctx, report.Principal, surplus); err != nil {
			return report, fmt.Errorf(
				"kafka admin: removing %d obsolete ACL binding(s) from principal %q: %w",
				len(surplus), report.Principal, err,
			)
		}

		report.Removed = len(surplus)
	}

	missing := make([]kafka.ACLEntry, 0, len(desired))
	for _, binding := range desired {
		if _, already := present[aclBindingKey(binding)]; already {
			continue
		}

		missing = append(missing, binding)
	}

	if len(missing) > 0 {
		if err := a.createACLBindings(ctx, report.Principal, missing); err != nil {
			return report, err
		}

		report.Created = len(missing)
	}

	logrus.WithFields(logrus.Fields{
		"principal_hash": subscriberLogLabel(report.Principal),
		"desired":        report.Desired,
		"managed":        report.Managed,
		"created":        report.Created,
		"removed":        report.Removed,
		// Deny only: a widening binding cannot reach this line, because it returned above.
		"foreign_deny": len(report.ForeignDeny),
	}).Info("kafka admin: subscriber ACL bindings reconciled")

	return report, nil
}

// PruneSubscriberAccess removes every Blnk-owned binding the broker holds for a
// subscriber that its RECORDED authorization does not imply, and creates nothing.
//
// An authorization change has to be applied in three steps — prune at the broker,
// persist the row, then grant at the broker — because there is no transaction spanning
// Blnk and Kafka and only that order leaves every partial failure fail-closed:
//
//   - Pruning first means a NARROWING is already in force at the broker before the row claims
//     it is. If the persist then fails, the subscriber has lost access the registry still
//     records — recoverable by re-running the update, and safe in the meantime.
//   - Granting last means a WIDENING reaches the broker only after the row records it. If the
//     grant then fails, the subscriber has less access than the registry records — again
//     recoverable, again safe.
//
// Parameters:
//   - ctx context.Context: cancels the round trips.
//   - subscriber *model.EventSubscriber: the row carrying the authorization to converge
//     on.
//
// Returns:
//   - SubscriberACLReconciliation: what was found and removed. Created is always zero.
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
func (a *KafkaAdminClient) PruneSubscriberAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (_ SubscriberACLReconciliation, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "prune_subscriber_access", subscriberSpanAttribute(subscriber))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	desired, principal, err := subscriberDesiredBindings(subscriber)
	if err != nil {
		return SubscriberACLReconciliation{}, err
	}

	report := SubscriberACLReconciliation{Principal: principal, Desired: len(desired)}

	observed, err := a.describeSubscriberACLs(ctx, principal)
	if err != nil {
		return report, err
	}

	wanted := make(map[string]struct{}, len(desired))
	for _, binding := range desired {
		wanted[aclBindingKey(binding)] = struct{}{}
	}

	surplus := make([]kafka.ACLEntry, 0, len(observed))
	for _, binding := range observed {
		if !blnkManagedACLBinding(binding) {
			// CLASSIFIED AND REPORTED, NEVER REFUSED. This is the one operation on this surface
			// that must not fail closed on a foreign ALLOW binding, and the reason is the
			// direction it moves in: pruning only ever REMOVES access, so refusing it would
			// leave the subscriber with MORE access than the operator asked for — precisely the
			// outcome the refusal exists to prevent. The widening binding is surfaced on the
			// report so the caller and the log say so, and the next operation that would hand
			// out a credential or converge a widening is the one that refuses.
			classifyForeignACLBinding(&report, binding)

			continue
		}

		report.Managed++

		if _, keep := wanted[aclBindingKey(binding)]; !keep {
			surplus = append(surplus, binding)
		}
	}

	if len(report.ForeignAllow) > 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(principal),
			"bindings":       report.ForeignAllow,
		}).Error(
			"kafka admin: this principal carries ALLOW ACL bindings Blnk did not provision, so " +
				"narrowing its authorization does NOT narrow its effective access; the obsolete " +
				"Blnk-owned bindings were still removed, but the foreign grants must be removed by hand",
		)
	}

	if len(surplus) == 0 {
		return report, nil
	}

	if err := a.deleteACLBindings(ctx, principal, surplus); err != nil {
		return report, fmt.Errorf(
			"kafka admin: removing %d obsolete ACL binding(s) from principal %q: %w",
			len(surplus), principal, err,
		)
	}

	report.Removed = len(surplus)

	logrus.WithFields(logrus.Fields{
		"principal_hash": subscriberLogLabel(principal),
		"removed":        report.Removed,
		"desired":        report.Desired,
	}).Info("kafka admin: obsolete subscriber ACL bindings removed")

	return report, nil
}

// GrantSubscriberAccess creates the bindings a subscriber's recorded authorization
// implies, and removes nothing.
//
// Parameters:
//   - ctx context.Context: cancels the round trips.
//   - subscriber *model.EventSubscriber: the row carrying the authorization to grant.
//
// Returns:
//   - SubscriberACLReconciliation: what was found and created. Removed is always zero.
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
func (a *KafkaAdminClient) GrantSubscriberAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (_ SubscriberACLReconciliation, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "grant_subscriber_access", subscriberSpanAttribute(subscriber))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	desired, principal, err := subscriberDesiredBindings(subscriber)
	if err != nil {
		return SubscriberACLReconciliation{}, err
	}

	report := SubscriberACLReconciliation{Principal: principal, Desired: len(desired)}

	if len(desired) == 0 {
		// Nothing to grant. Reported rather than silently skipped, because "authorised for
		// nothing" is a real state and an operator reading the log should see it stated.
		logrus.WithField("principal_hash", subscriberLogLabel(principal)).Info(
			"kafka admin: the subscriber's recorded authorization grants nothing, so no ACL binding " +
				"was created",
		)

		return report, nil
	}

	observed, err := a.describeSubscriberACLs(ctx, principal)
	if err != nil {
		return report, err
	}

	present := make(map[string]struct{}, len(observed))
	for _, binding := range observed {
		present[aclBindingKey(binding)] = struct{}{}
		if blnkManagedACLBinding(binding) {
			report.Managed++

			continue
		}

		classifyForeignACLBinding(&report, binding)
	}

	// a WIDENING refuses, for the same reason issuance does. Converging a wider grant on a
	// principal whose effective access Blnk cannot state would report the registry and the
	// broker as agreeing when they demonstrably do not. Refusing leaves the subscriber
	// with the access it already had — less than the row now records, which is the
	// documented safe direction this whole three-step surface is built around, and which
	// the caller reports as retryable.
	if len(report.ForeignAllow) > 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(principal),
			"bindings":       report.ForeignAllow,
		}).Error(
			"kafka admin: this principal carries ALLOW ACL bindings Blnk did not provision, so its " +
				"effective access is broader than the subscriber registry records; refusing to grant " +
				"further access until they are removed by hand",
		)

		return report, refuseForeignACLGrant(principal, report.ForeignAllow)
	}

	if len(report.ForeignDeny) > 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(principal),
			"bindings":       report.ForeignDeny,
		}).Warn(
			"kafka admin: this principal carries DENY ACL bindings Blnk does not provision; they only " +
				"narrow the grant, so the requested bindings were still created",
		)
	}

	missing := make([]kafka.ACLEntry, 0, len(desired))
	for _, binding := range desired {
		if _, already := present[aclBindingKey(binding)]; already {
			continue
		}

		missing = append(missing, binding)
	}

	if len(missing) == 0 {
		return report, nil
	}

	if err := a.createACLBindings(ctx, principal, missing); err != nil {
		return report, err
	}

	report.Created = len(missing)

	logrus.WithFields(logrus.Fields{
		"principal_hash": subscriberLogLabel(principal),
		"created":        report.Created,
		"desired":        report.Desired,
	}).Info("kafka admin: subscriber ACL bindings granted")

	return report, nil
}

// subscriberDesiredBindings derives the bindings a registry row implies, and the
// principal they belong to.
//
// The request is validated for everything except the password, which plays no part in a
// binding: a row whose principal or consumer group is not derived from its identifier
// is refused here exactly as it would be at issuance.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row. Nil is refused.
//
// Returns:
//   - []kafka.ACLEntry: the desired bindings, possibly empty.
//   - string: the bare principal.
//   - error: a validation error when the row cannot yield a boundary.
func subscriberDesiredBindings(subscriber *model.EventSubscriber) ([]kafka.ACLEntry, string, error) {
	if subscriber == nil {
		return nil, "", errors.New(
			"kafka admin: a subscriber is required to reconcile its ACL bindings",
		)
	}

	request := NewSubscriberProvisioningRequest(subscriber, "")

	principal := request.boundPrincipal()
	if principal == "" {
		return nil, "", errors.New(
			"kafka admin: the subscriber has neither a derivable nor a recorded Kafka principal, " +
				"so its ACL bindings cannot be reconciled",
		)
	}

	if err := request.validateTopics(); err != nil {
		return nil, principal, err
	}

	if err := request.validateHost(); err != nil {
		return nil, principal, err
	}

	return request.aclEntries(), principal, nil
}

// SubscriberCredentialExists reports whether a principal already holds a SCRAM-SHA-512
// credential.
//
// A principal holding only a SHA-256 credential answers false: a SHA-256 credential
// cannot authenticate the SHA-512 mechanism Blnk standardises on, so for Blnk's
// purposes no credential exists.
//
// Parameters:
//   - ctx context.Context
//   - principal string: the SASL username.
//
// Returns:
//   - bool: true when a SHA-512 credential is present.
//   - error: ErrKafkaAdminNotConfigured, a validation error for a blank principal, or a
//     wrapped broker error.
func (a *KafkaAdminClient) SubscriberCredentialExists(ctx context.Context, principal string) (_ bool, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "describe_subscriber_credential")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	if err := a.ready(ctx); err != nil {
		return false, err
	}

	principal = strings.TrimSpace(principal)
	if principal == "" {
		return false, errors.New("kafka admin: a principal name is required to describe a SCRAM credential")
	}

	response, err := a.client.DescribeUserScramCredentials(ctx, &kafka.DescribeUserScramCredentialsRequest{
		Users: []kafka.UserScramCredentialsUser{{Name: principal}},
	})
	if err != nil {
		return false, fmt.Errorf("kafka admin: describing the SCRAM credential of principal %q: %w", principal, err)
	}

	if response.Error != nil {
		return false, fmt.Errorf(
			"kafka admin: broker refused to describe SCRAM credentials while looking up principal %q: %w",
			principal, response.Error,
		)
	}

	for _, outcome := range response.Results {
		if outcome.User != principal {
			continue
		}

		if outcome.Error != nil {
			if errors.Is(outcome.Error, kafka.ResourceNotFound) {
				return false, nil
			}

			return false, fmt.Errorf(
				"kafka admin: describing the SCRAM credential of principal %q: %w", principal, outcome.Error,
			)
		}

		for _, info := range outcome.CredentialInfos {
			if info.Mechanism == kafka.ScramMechanismSha512 {
				return true, nil
			}
		}
	}

	return false, nil
}

// AuthorizerActive reports whether the broker enforces ACLs.
//
// A KRaft broker started WITHOUT
// authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer accepts
// every ACL binding and applies none of them. Provisioning appears to succeed, the
// bindings are listable, and every principal can read everything.
//
// Parameters:
//   - ctx context.Context
//
// Returns:
//   - bool: true when the broker enforces ACLs, false when it explicitly reports
//     security disabled.
//   - error: ErrKafkaAdminNotConfigured, or a wrapped broker error when the question
//     could not be answered.
func (a *KafkaAdminClient) AuthorizerActive(ctx context.Context) (_ bool, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "describe_authorizer")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	if err := a.ready(ctx); err != nil {
		return false, err
	}

	// Every filter field left at its zero value matches anything: the name, principal and
	// host filters are nullable on the wire, and Any matches every pattern type, operation
	// and permission.
	response, err := a.client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{
		Filter: kafka.ACLFilter{
			ResourceTypeFilter:        kafka.ResourceTypeTopic,
			ResourcePatternTypeFilter: kafka.PatternTypeAny,
			Operation:                 kafka.ACLOperationTypeAny,
			PermissionType:            kafka.ACLPermissionTypeAny,
		},
	})
	if err != nil {
		return false, fmt.Errorf("kafka admin: probing whether the broker enforces ACLs: %w", err)
	}

	if response.Error != nil {
		if errors.Is(response.Error, kafka.SecurityDisabled) {
			return false, nil
		}

		return false, fmt.Errorf("kafka admin: probing whether the broker enforces ACLs: %w", response.Error)
	}

	return true, nil
}

// ErrAuthorizerNotEnforcing reports that the broker does not enforce ACLs, or that its
// enforcement could not be confirmed.
//
// It is a sentinel so the subscriber service, the API layer and a test all recognise
// the refusal without matching message text, and so the two distinct causes — no
// authorizer, and no answer — are provably one refusal.
var ErrAuthorizerNotEnforcing = errors.New(
	"kafka admin: the broker's ACL enforcement is not confirmed, so no subscriber credential may be issued",
)

// ErrForeignACLGrantsAccess reports that the principal carries ACL bindings Blnk did
// not create which grant it access wider than its recorded authorization, so no
// credential may be issued.
var ErrForeignACLGrantsAccess = errors.New(
	"kafka admin: the principal holds ACL bindings Blnk did not create that grant access beyond its " +
		"recorded authorization, so no subscriber credential may be issued",
)

// ErrSubscriberKeyScopeUnenforced reports that a subscriber recording a partition-key
// prefix could not be provisioned with a boundary that actually keeps it, so no
// credential may be issued.
//
// It is a sentinel for the same reasons its siblings are: the subscriber service, the
// API layer and a test all recognise the refusal without matching message text.
var ErrSubscriberKeyScopeUnenforced = errors.New(
	"kafka admin: a subscriber recording a partition-key prefix must be granted no record-level " +
		"Read, so that the declared key-authorising component is the only path its records can " +
		"take; the boundary could not be established, so no subscriber credential may be issued",
)

// bindingsGrantTopicRead reports whether a desired binding set includes Read on a
// TOPIC.
//
// Only ALLOW is considered. A DENY on a topic Read is a narrowing Blnk never provisions
// — and if an operator has added one, it takes access away rather than granting it, so
// it must not be read here as evidence of a grant.
//
// Parameters:
//   - bindings []kafka.ACLEntry: the desired set. Empty answers false, which is
//     correct: a principal granted nothing can read nothing.
//
// Returns:
//   - bool: true when any ALLOW Read on a Topic resource is present.
func bindingsGrantTopicRead(bindings []kafka.ACLEntry) bool {
	for _, binding := range bindings {
		if binding.ResourceType == kafka.ResourceTypeTopic &&
			binding.Operation == kafka.ACLOperationTypeRead &&
			binding.PermissionType == kafka.ACLPermissionTypeAllow {
			return true
		}
	}

	return false
}

// verifyKeyScopeBoundary is the check that a key-scoped subscriber's partition-key
// boundary is real before its credential can be returned.
//
// A request with NO key scope is verified vacuously: there is no key boundary to keep,
// the topic grant is the whole boundary, and the broker enforces all of it.
//
// Parameters:
//   - req SubscriberProvisioningRequest: the request, read for KeyScoped only.
//   - bindings []kafka.ACLEntry: the desired bindings that were reconciled.
//   - reconciliation SubscriberACLReconciliation: what reconciliation observed at the
//     broker.
//   - authorizerActive bool: whether the broker confirmed it enforces ACLs.
//
// Returns:
//   - error: nil when the boundary holds; otherwise an error wrapping
//     ErrSubscriberKeyScopeUnenforced naming which of the three facts failed.
func verifyKeyScopeBoundary(
	req SubscriberProvisioningRequest,
	bindings []kafka.ACLEntry,
	reconciliation SubscriberACLReconciliation,
	authorizerActive bool,
) error {
	if !req.KeyScoped {
		return nil
	}

	if bindingsGrantTopicRead(bindings) {
		return fmt.Errorf(
			"%w: the grant reconciled for principal %q includes Read on a topic, so the broker "+
				"would admit it to every record on that shared topic regardless of the recorded "+
				"partition-key prefix",
			ErrSubscriberKeyScopeUnenforced,
			sanitizeLogValue(reconciliation.Principal, maxLoggedFilterLength),
		)
	}

	if !authorizerActive {
		return fmt.Errorf(
			"%w: the broker's ACL enforcement is not confirmed for principal %q, so withholding "+
				"Read withholds nothing",
			ErrSubscriberKeyScopeUnenforced,
			sanitizeLogValue(reconciliation.Principal, maxLoggedFilterLength),
		)
	}

	if len(reconciliation.ForeignAllow) > 0 {
		return fmt.Errorf(
			"%w: principal %q carries %d ALLOW ACL binding(s) Blnk did not provision, which may "+
				"restore the record access this boundary withholds",
			ErrSubscriberKeyScopeUnenforced,
			sanitizeLogValue(reconciliation.Principal, maxLoggedFilterLength),
			len(reconciliation.ForeignAllow),
		)
	}

	return nil
}

// clock returns the time source, defaulting to time.Now.
//
// Cache expiry is the only thing in this file that depends on the passage of time, and
// a test that had to sleep to observe it would be both slow and flaky. Reading the
// clock through a field makes expiry directly assertable.
//
// Returns:
//   - time.Time: the current instant.
func (a *KafkaAdminClient) clock() time.Time {
	if a.now != nil {
		return a.now()
	}

	return time.Now()
}

// snapshotTTL returns the effective cache lifetime.
//
// Zero — the value a client built by any constructor in this file carries unless told
// otherwise — selects the default, so PRODUCTION GETS THE BENEFIT WITHOUT OPTING IN. A
// negative value disables caching, which is what a test asserting on raw round-trip
// counts wants and is deliberately not reachable from configuration.
//
// Returns:
//   - time.Duration: the lifetime; non-positive means caching is off.
func (a *KafkaAdminClient) snapshotTTL() time.Duration {
	if a.offsetSnapshotTTL == 0 {
		return defaultOffsetSnapshotTTL
	}

	return a.offsetSnapshotTTL
}

// WithOffsetSnapshotTTL sets how long a partition/end-offset snapshot may be reused.
//
// It follows the fluent configurator convention the outbox processors already use. A
// negative value disables caching entirely; zero restores the default.
//
// Parameters:
//   - ttl time.Duration: the lifetime. Negative disables caching, zero restores the
//     default.
//
// Returns:
//   - *KafkaAdminClient: the client, for chaining.
func (a *KafkaAdminClient) WithOffsetSnapshotTTL(ttl time.Duration) *KafkaAdminClient {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()

	a.offsetSnapshotTTL = ttl
	a.offsetSnapshots = nil
	a.authorizerProbe = cachedAuthorizerProbe{}

	return a
}

// InvalidateOffsetSnapshot drops every cached snapshot and the authorizer probe.
//
// It exists for the two cases where a cached answer is known to be wrong rather than merely
// old: immediately after topics are created or repartitioned, which changes the partition
// layout a snapshot describes, and in a test that wants the next read to go to the broker.
func (a *KafkaAdminClient) InvalidateOffsetSnapshot() {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()

	a.offsetSnapshots = nil
	a.authorizerProbe = cachedAuthorizerProbe{}
}

// offsetSnapshotKey builds the cache key for a topic set.
//
// Parameters:
//   - topics []string: the normalised topic list.
//
// Returns:
//   - string: the cache key.
func offsetSnapshotKey(topics []string) string {
	return strings.Join(topics, "\x00")
}

// partitionOffsetSnapshot returns the partition layout and end offsets for a topic set,
// from cache when it is fresh.
//
// A consumer-lag sweep over N subscribers asked the same two questions N times — which
// partitions exist, and where does each one end — because only the third question, the
// group's committed offsets, actually differs between subscribers. That made a sweep
// cost 3N round trips where 1 + 1 + N would do, and it did so on the metrics path, at
// whatever interval the gauge is refreshed.
//
// The two reads are cached TOGETHER, as one snapshot, because they must be consistent
// with each other: a partition present in the layout but absent from the bounds, or the
// reverse, would produce a lag figure computed from two different views of the cluster.
//
// A failure is returned and NOT cached, so a transient broker problem is re-asked on
// the next call instead of being remembered for the life of the TTL.
//
// Parameters:
//   - ctx context.Context: cancels either read.
//   - topics []string: the normalised, bounded topic list.
//
// Returns:
//   - *offsetSnapshot: never nil when the error is nil.
//   - error: a wrapped broker error from either read.
func (a *KafkaAdminClient) partitionOffsetSnapshot(
	ctx context.Context,
	topics []string,
) (*offsetSnapshot, error) {
	ttl := a.snapshotTTL()
	key := offsetSnapshotKey(topics)

	if ttl > 0 {
		a.cacheMu.Lock()
		cached, found := a.offsetSnapshots[key]
		a.cacheMu.Unlock()

		if found && a.clock().Sub(cached.takenAt) < ttl {
			return cached, nil
		}
	}

	partitions, err := a.topicPartitions(ctx, topics)
	if err != nil {
		// The partial result is still useful to the caller: it reports which topics were
		// missing, which is a caveat the lag report carries even on failure.
		return &offsetSnapshot{
			partitions: partitions,
			missing:    missingTopics(topics, partitions),
			takenAt:    a.clock(),
		}, err
	}

	snapshot := &offsetSnapshot{
		partitions: partitions,
		missing:    missingTopics(topics, partitions),
		takenAt:    a.clock(),
	}

	if len(partitions) > 0 {
		bounds, boundsErr := a.offsetBounds(ctx, partitions)
		if boundsErr != nil {
			return snapshot, boundsErr
		}
		snapshot.bounds = bounds
	}

	if ttl > 0 {
		a.cacheMu.Lock()
		// Dropped wholesale at the bound rather than evicted entry by entry: entries live for
		// seconds, so the next sweep repopulates exactly what it needs and precise eviction
		// would be bookkeeping for no benefit.
		if len(a.offsetSnapshots) >= maxCachedOffsetSnapshots {
			a.offsetSnapshots = nil
		}
		if a.offsetSnapshots == nil {
			a.offsetSnapshots = make(map[string]*offsetSnapshot, 8)
		}
		a.offsetSnapshots[key] = snapshot
		a.cacheMu.Unlock()
	}

	return snapshot, nil
}

// authorizerActiveCached answers "does this broker enforce ACLs" from a memoised probe.
//
// Whether a broker runs an authorizer comes from its own startup configuration, so the
// answer cannot change while the broker is up. Asking on every provisioning call
// therefore spent a serial DescribeACLs round trip — from the same five-second budget
// as the credential write and the ACL batch — to re-learn something already known.
//
// Parameters:
//   - ctx context.Context
//
// Returns:
//   - bool: true when the broker enforces ACLs.
//   - error: as AuthorizerActive.
func (a *KafkaAdminClient) authorizerActiveCached(ctx context.Context) (bool, error) {
	ttl := a.snapshotTTL()

	if ttl > 0 {
		a.cacheMu.Lock()
		probe := a.authorizerProbe
		a.cacheMu.Unlock()

		if !probe.checkedAt.IsZero() && a.clock().Sub(probe.checkedAt) < ttl {
			return probe.active, nil
		}
	}

	active, err := a.AuthorizerActive(ctx)
	if err != nil {
		return false, err
	}

	a.cacheMu.Lock()
	a.authorizerProbe = cachedAuthorizerProbe{active: active, checkedAt: a.clock()}
	a.cacheMu.Unlock()

	return active, nil
}

// requireEnforcedAuthorizer is the gate: no credential is written unless the broker has
// affirmatively confirmed that it enforces ACLs.
//
// Both failure modes are fatal, and the messages differ because the remedies do:
//
//   - NO AUTHORIZER. The broker must be restarted with the KRaft StandardAuthorizer. Until
//     then no boundary of any kind can be created on it.
//   - NO ANSWER. Usually the administrative principal lacks Describe on the cluster, or the
//     broker is unreachable. Unverifiable enforcement is treated exactly as absent
//     enforcement, because from the point of view of the credential about to be minted the
//     two are indistinguishable.
//
// Parameters:
//   - ctx context.Context: cancels the probe.
//   - principal string: included in the log line and the error for correlation.
//
// Returns:
//   - error: nil ONLY when the broker confirmed it enforces ACLs; otherwise wrapping
//     ErrAuthorizerNotEnforcing.
func (a *KafkaAdminClient) requireEnforcedAuthorizer(ctx context.Context, principal string) error {
	// The MEMOISED probe, not the raw one. Whether the broker enforces ACLs comes from its
	// own startup configuration, so asking on every provisioning call spends a serial
	// round trip out of the five-second budget to re-learn something already known. The
	// memo has a TTL and a failed probe is never cached, so the FAIL-CLOSED behaviour
	// below is unchanged: an unanswerable probe is still treated as an absent boundary.
	active, err := a.authorizerActiveCached(ctx)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(principal),
			"error_class":    kafkaErrorClassField("revoke_subscriber_principal", err),
		}).Error(
			"kafka admin: refusing to issue a subscriber credential because the broker's ACL enforcement " +
				"could not be confirmed. Confirm the broker runs " +
				"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer and that the " +
				"administrative principal is allowed to describe ACLs",
		)

		return fmt.Errorf(
			"%w: the enforcement probe for principal %q did not answer; an unverifiable boundary is "+
				"treated as an absent one. Grant the administrative principal Describe on the cluster, or "+
				"fix broker reachability, then retry",
			ErrAuthorizerNotEnforcing, principal,
		)
	}

	if !active {
		logrus.WithField("principal_hash", subscriberLogLabel(principal)).Error(
			"kafka admin: THE BROKER HAS NO AUTHORIZER CONFIGURED, so no credential was issued. ACL " +
				"bindings would be accepted and never applied, leaving every principal able to read every " +
				"topic including other subscribers' topics and the dead-letter topics. Start the broker with " +
				"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
		)

		return fmt.Errorf(
			"%w: the broker reports that security is disabled, so ACL bindings for principal %q would be "+
				"accepted and never enforced. Start the broker with "+
				"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
			ErrAuthorizerNotEnforcing, principal,
		)
	}

	return nil
}

// kafkaCleanupBudget bounds a compensating broker operation.
//
// It is a CEILING AND ONLY A CEILING, applying when no wall clock is in force.
// kafkaCleanupContext bounds the phase by the caller's SLA whenever there is one, so a
// compensation inside a five-second credential issuance never spends ten.
const kafkaCleanupBudget = kafkaAdminRequestTimeout

// kafkaCleanupContext derives the context a compensating broker operation runs on.
//
// A caller with no wall clock keeps the ten-second bound, which is the right size for
// two round trips against a broker that is refusing rather than hanging.
//
// Parameters:
//   - ctx context.Context: the caller's context, used for its values and its issuance
//     deadline.
//
// Returns:
//   - context.Context: a context detached from the caller's cancellation and bounded
//     either by the caller's wall clock or by kafkaCleanupBudget.
//   - context.CancelFunc: must be called, conventionally by defer.
func kafkaCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return subscriberPhaseContext(ctx, subscriberDurabilityReserve, kafkaCleanupBudget, true)
}

// compensateFailedProvisioning undoes a half-completed provisioning.
//
// Provisioning writes the SCRAM credential first and the ACL bindings second, because a
// binding for a principal that does not exist is inert while a credential without
// bindings is not: it AUTHENTICATES. On a broker whose default is deny that principal
// can read nothing, but it is a live, valid credential that was handed to a subscriber
// by a call that then reported failure — so the subscriber holds a secret nobody is
// tracking, and a later change to a default or a broad binding gives it reach.
//
// So the failure path revokes. Both directions are attempted, and independently: the
// bindings, because CreateACLs is not atomic across entries and some may have landed
// before the error; and the credential itself.
//
// EVERY FAILURE IS LOGGED, AND THE CREDENTIAL REVOCATION'S OUTCOME IS RETURNED. The
// caller still returns the ORIGINAL provisioning error — replacing it with a cleanup
// error would hide the cause — but it needs to know whether the broker was actually
// left clean, because "compensated" and "a live principal nobody is tracking" are the
// two states a caller must answer differently.
//
// Parameters:
//   - ctx context.Context: the provisioning context. It is used for its VALUES only —
//     its cancellation is deliberately not inherited, see above.
//   - principal string: the principal to revoke.
//   - bindings []kafka.ACLEntry: the bindings that were attempted, so exactly those are
//     deleted rather than everything the principal holds.
//
// Returns:
//   - error: nil only when the credential is confirmed revoked, so the broker holds no
//     principal that can authenticate; the revocation error otherwise.
func (a *KafkaAdminClient) compensateFailedProvisioning(
	ctx context.Context,
	principal string,
	bindings []kafka.ACLEntry,
) error {
	logger := logrus.WithField("principal_hash", subscriberLogLabel(principal))

	ctx, cancel := kafkaCleanupContext(ctx)
	defer cancel()

	if err := a.deleteACLBindings(ctx, principal, bindings); err != nil {
		withKafkaError(logger, "compensate_delete_acl_bindings", err).Error(
			"kafka admin: could not remove the ACL bindings of a failed provisioning; " +
				"remove them manually with kafka-acls before reissuing",
		)
	}

	if err := a.RevokeSubscriberPrincipal(ctx, principal); err != nil {
		withKafkaError(logger, "compensate_revoke_subscriber_principal", err).Error(
			"kafka admin: A SCRAM CREDENTIAL WAS WRITTEN AND COULD NOT BE REVOKED after provisioning " +
				"failed. The principal can authenticate and is not recorded in the registry. Delete it " +
				"manually: kafka-configs --alter --delete-config SCRAM-SHA-512 --entity-type users " +
				"--entity-name <principal>",
		)

		return err
	}

	logger.Warn(
		"kafka admin: provisioning failed after the credential was written; the credential and its " +
			"attempted ACL bindings have been revoked, so no unbounded principal was left behind",
	)

	return nil
}

// RevokeSubscriberPrincipal deletes a subscriber's SCRAM credential.
//
// The registry row was gone and the access was not: a deprovisioned subscriber kept
// consuming, and nothing in Blnk could see it any more.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - principal string: the SASL username to delete. Required.
//
// Returns:
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
func (a *KafkaAdminClient) RevokeSubscriberPrincipal(ctx context.Context, principal string) (err error) {
	ctx, span := startKafkaAdminSpan(ctx, "revoke_subscriber_credential")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	if err := a.ready(ctx); err != nil {
		return err
	}

	principal = strings.TrimSpace(principal)
	if principal == "" {
		return errors.New("kafka admin: a principal is required to revoke a SCRAM credential")
	}

	response, err := a.client.AlterUserScramCredentials(ctx, &kafka.AlterUserScramCredentialsRequest{
		Deletions: []kafka.UserScramCredentialsDeletion{{
			Name:      principal,
			Mechanism: kafka.ScramMechanismSha512,
		}},
	})
	if err != nil {
		return fmt.Errorf("kafka admin: deleting the %s credential for principal %q: %w",
			SubscriberSASLMechanism, principal, err)
	}

	for i := range response.Results {
		resultErr := response.Results[i].Error
		if resultErr == nil || errors.Is(resultErr, kafka.ResourceNotFound) {
			// Nothing to delete is the desired end state, so it is success. This is what makes
			// revocation safe to retry, which both operators and the compensation path depend
			// on.
			continue
		}

		return fmt.Errorf("kafka admin: deleting the %s credential for principal %q: %w",
			SubscriberSASLMechanism, response.Results[i].User, resultErr)
	}

	logrus.WithField("principal_hash", subscriberLogLabel(principal)).
		Info("kafka admin: subscriber SCRAM credential revoked")

	return nil
}

// RevokeSubscriber removes a subscriber's ACL bindings and then its SCRAM credential,
// ending its access at the broker.
//
// Parameters:
//   - ctx context.Context: cancels the requests.
//   - subscriber *model.EventSubscriber: the registry row. Nil is refused, because
//     there would be no principal to revoke.
//
// Returns:
//   - error: nil when the broker holds neither the bindings nor the credential.
func (a *KafkaAdminClient) RevokeSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (err error) {
	ctx, span := startKafkaAdminSpan(ctx, "revoke_subscriber_access", subscriberSpanAttribute(subscriber))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	if err := a.ready(ctx); err != nil {
		return err
	}

	if subscriber == nil {
		return errors.New("kafka admin: a subscriber is required to revoke its Kafka access")
	}

	// The password plays no part in a binding, so revocation builds the request without one.
	request := NewSubscriberProvisioningRequest(subscriber, "")
	principal := request.boundPrincipal()
	if principal == "" {
		return errors.New(
			"kafka admin: the subscriber has neither a derivable nor a recorded Kafka principal, " +
				"so there is nothing to revoke",
		)
	}

	bindingErr := a.deleteAllPrincipalBindings(ctx, principal)
	credentialErr := a.RevokeSubscriberPrincipal(ctx, principal)

	switch {
	case credentialErr != nil:
		if bindingErr != nil {
			logrus.WithFields(logrus.Fields{
				"principal_hash": subscriberLogLabel(principal),
				"error_class":    kafkaErrorClassField("revoke_subscriber_acl_bindings", bindingErr),
			}).Error(
				"kafka admin: removing a subscriber's ACL bindings also failed; both need manual attention",
			)
		}

		return credentialErr
	case bindingErr != nil:
		return bindingErr
	default:
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"principal_hash":     subscriberLogLabel(principal),
		}).Info("kafka admin: subscriber access revoked at the broker")

		return nil
	}
}

// ReconcileSubscriberACLs brings the broker's ACL bindings for one subscriber into line
// with the grant recorded on its registry row.
//
// A subscriber's authorization lives in two places: the authorized_topics column and
// the broker's ACL bindings. Only the first is a Blnk write.
//
// Parameters:
//   - ctx context.Context: cancels the broker round trips.
//   - subscriber *model.EventSubscriber: the registry row AFTER the change, whose
//     authorized_topics and consumer group describe the desired state.
//   - revokedTopics []string: the topics removed from the grant, whose bindings must
//     go.
//
// Returns:
//   - error: ErrKafkaAdminNotConfigured when no broker is configured; the broker's
//     error when a revocation or a creation failed.
func (a *KafkaAdminClient) ReconcileSubscriberACLs(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	revokedTopics []string,
) (err error) {
	ctx, span := startKafkaAdminSpan(ctx, "reconcile_subscriber_acls", subscriberSpanAttribute(subscriber))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	if err := a.ready(ctx); err != nil {
		return err
	}

	if subscriber == nil {
		return errors.New("kafka admin: a subscriber is required to reconcile its ACL bindings")
	}

	// No password: this operation binds and unbinds, it never mints.
	desired := NewSubscriberProvisioningRequest(subscriber, "")
	principal := desired.boundPrincipal()
	if principal == "" {
		return errors.New(
			"kafka admin: the subscriber has neither a derivable nor a recorded Kafka principal, " +
				"so there are no ACL bindings to reconcile",
		)
	}

	// The revoked bindings are built from a request carrying ONLY the removed topics, so
	// aclEntries — the single place a subscriber's binding shape is expressed — produces
	// the exact filters to delete. Restating the resource type, pattern type, host and
	// operation list here would be a second copy of that shape, and the two would drift.
	if revoked := normalizeTopicList(revokedTopics); len(revoked) > 0 {
		revocation := desired
		revocation.Topics = revoked
		// The consumer group namespace is NOT revoked: it is derived from the subscriber's
		// identity and survives every change to the topic list. Clearing it here would strip
		// the group binding on an ordinary topic edit and leave the subscriber unable to join
		// its own group.
		revocation.ConsumerGroupPrefix = ""

		if err := a.deleteACLBindings(ctx, principal, revocation.aclEntries()); err != nil {
			return fmt.Errorf(
				"kafka admin: revoking %d topic binding(s) for principal %q: %w",
				len(revoked), principal, err)
		}
	}

	bindings := desired.aclEntries()
	if len(bindings) == 0 {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"principal_hash":     subscriberLogLabel(principal),
			"revoked":            len(revokedTopics),
		}).Info(
			"kafka admin: subscriber ACL bindings reconciled; the grant is now empty, so the " +
				"principal can authenticate and read nothing",
		)

		return nil
	}

	if err := a.createACLBindings(ctx, principal, bindings); err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(principal),
		"granted":            len(desired.normalizedTopics()),
		"revoked":            len(normalizeTopicList(revokedTopics)),
		"acl_entries":        len(bindings),
	}).Info("kafka admin: subscriber ACL bindings reconciled with the registry grant")

	return nil
}

// deleteACLBindings removes exactly the bindings it is given.
//
// A binding that is already absent is not an error. Kafka reports zero matches, which
// is the desired end state, so this is idempotent and safe in a retry or a compensation
// path.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - principal string: used only for the log line; the filters carry their own
//     principal.
//   - bindings []kafka.ACLEntry: the bindings to remove. Empty is a no-op.
//
// Returns:
//   - error: a wrapped broker error, or the first per-filter error the broker reported.
func (a *KafkaAdminClient) deleteACLBindings(
	ctx context.Context,
	principal string,
	bindings []kafka.ACLEntry,
) error {
	if len(bindings) == 0 {
		return nil
	}

	filters := make([]kafka.DeleteACLsFilter, 0, len(bindings))
	for _, binding := range bindings {
		filters = append(filters, kafka.DeleteACLsFilter{
			ResourceTypeFilter:        binding.ResourceType,
			ResourceNameFilter:        binding.ResourceName,
			ResourcePatternTypeFilter: binding.ResourcePatternType,
			PrincipalFilter:           binding.Principal,
			HostFilter:                binding.Host,
			Operation:                 binding.Operation,
			PermissionType:            binding.PermissionType,
		})
	}

	response, err := a.client.DeleteACLs(ctx, &kafka.DeleteACLsRequest{Filters: filters})
	if err != nil {
		return fmt.Errorf("kafka admin: deleting %d ACL bindings for principal %q: %w",
			len(filters), principal, err)
	}

	removed := 0
	for i := range response.Results {
		if response.Results[i].Error != nil {
			return fmt.Errorf("kafka admin: deleting ACL bindings for principal %q: %w",
				principal, response.Results[i].Error)
		}

		removed += len(response.Results[i].MatchingACLs)
	}

	logrus.WithFields(logrus.Fields{
		"principal_hash": subscriberLogLabel(principal),
		"filters":        len(filters),
		"removed":        removed,
	}).Info("kafka admin: subscriber ACL bindings removed")

	return nil
}

// deleteAllPrincipalBindings removes EVERY ACL binding the broker holds for one
// principal, with a single match-any filter.
//
// It is the deprovisioning counterpart to deleteACLBindings, which deletes an
// enumerated set. The distinction is which of the two questions is being answered:
//
//   - deleteACLBindings answers "remove exactly these". Used by compensation, which is
//     unwinding bindings it just created, and by reconciliation, which is removing the surplus
//     it computed against the row. In both the principal SURVIVES, so a binding this process
//     did not create must be left alone.
//   - this function answers "remove everything this principal has". Used by revocation, where
//     the principal is being retired. The registry row names only the latest grant, but the
//     broker holds the union of every grant the subscriber ever had, so an enumerated delete
//     leaves the residue of every earlier one live with nothing left to describe it.
//
// THE PRINCIPAL CARRIES THE "User:" PREFIX, because that is how a binding stores it. A
// filter naming the bare SASL username matches nothing, and Kafka answers that with a
// success carrying an empty MatchingACLs — so the revocation would report clean while
// removing nothing at all.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - principal string: the BARE SASL username, as boundPrincipal returns it.
//
// Returns:
//   - error: nil when the broker holds no binding for the principal afterwards.
func (a *KafkaAdminClient) deleteAllPrincipalBindings(ctx context.Context, principal string) error {
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return errors.New("kafka admin: a principal is required to delete its ACL bindings")
	}

	// Applied here rather than expected from the caller. Every other filter field on this
	// request is a match-any sentinel, so the principal is the ONE exact term the delete
	// turns on — and the failure mode of getting it wrong is silent: Kafka answers an
	// unmatched filter with a success carrying an empty MatchingACLs, so the revocation
	// reports clean while every binding stays live.
	bound := kafkaPrincipalPrefix + principal

	response, err := a.client.DeleteACLs(ctx, &kafka.DeleteACLsRequest{
		Filters: []kafka.DeleteACLsFilter{{
			ResourceTypeFilter:        kafka.ResourceTypeAny,
			ResourceNameFilter:        "",
			ResourcePatternTypeFilter: kafka.PatternTypeAny,
			PrincipalFilter:           bound,
			HostFilter:                "",
			Operation:                 kafka.ACLOperationTypeAny,
			PermissionType:            kafka.ACLPermissionTypeAny,
		}},
	})
	if err != nil {
		return fmt.Errorf("kafka admin: deleting every ACL binding for principal %q: %w", bound, err)
	}

	removed := 0
	for i := range response.Results {
		if response.Results[i].Error != nil {
			return fmt.Errorf("kafka admin: deleting every ACL binding for principal %q: %w",
				bound, response.Results[i].Error)
		}

		removed += len(response.Results[i].MatchingACLs)
	}

	logrus.WithFields(logrus.Fields{
		"principal_hash": subscriberLogLabel(bound),
		"removed":        removed,
	}).Info("kafka admin: every ACL binding for the subscriber principal removed")

	return nil
}

// ConsumerLagRequest identifies the consumer group whose lag is to be measured.
type ConsumerLagRequest struct {
	// SubscriberID labels the measurement. It is used for the metric attribute and the log
	// line and takes no part in the computation, so a bare operational query may leave it
	// empty.
	SubscriberID string

	// GroupID is the consumer group to read committed offsets for. Required.
	GroupID string

	// Topics are the topics to measure. Normally the subscriber's authorised topics.
	// An empty list yields an empty report rather than an error — see ConsumerLag.
	Topics []string
}

// PartitionLag is the lag of one partition, with every input to the arithmetic
// retained.
type PartitionLag struct {
	// Topic and Partition identify the partition.
	Topic     string
	Partition int

	// CommittedOffset is the group's committed offset, or -1 when it has never committed
	// here. Committed says which of the two it is, so a caller never has to know that -1
	// is the sentinel.
	CommittedOffset int64
	Committed       bool

	// FirstOffset is the earliest offset still retained, which is above zero once
	// retention has deleted the head of the log. It is the baseline for a partition with
	// no commit.
	FirstOffset int64

	// EndOffset is the log end offset: the offset the next produced record will take.
	EndOffset int64

	// Lag is EndOffset minus the baseline, never negative.
	Lag int64

	// Unavailable is true when the broker could not report this partition's offsets, in
	// which case Lag is 0 and the partition contributes nothing. It is reported so a zero
	// caused by an unreadable partition is distinguishable from a zero caused by a
	// consumer that is keeping up.
	Unavailable bool
}

// TopicLag aggregates one topic's partitions.
type TopicLag struct {
	// Topic is the topic measured.
	Topic string

	// TotalLag is the sum of the partition lags, and the value published to the
	// consumer-lag gauge for this topic.
	TotalLag int64

	// Partitions carries the per-partition detail, in ascending partition order.
	Partitions []PartitionLag

	// PartitionsWithoutCommit counts partitions the group has never committed on. A number
	// equal to the partition count on a supposedly running consumer means the group is not
	// consuming this topic at all, which is a different fault from being behind.
	PartitionsWithoutCommit int

	// PartitionsUnavailable counts partitions whose offsets could not be read.
	PartitionsUnavailable int
}

// ConsumerLagReport is the outcome of one lag measurement.
type ConsumerLagReport struct {
	// SubscriberID and GroupID echo the request.
	SubscriberID string
	GroupID      string

	// TotalLag is the sum across every topic measured.
	TotalLag int64

	// Topics carries per-topic detail, in the order the request listed them.
	Topics []TopicLag

	// MissingTopics lists requested topics that do not exist on the broker. They
	// contribute no lag, and they are reported because a lag alert on a topic that
	// silently does not exist would read as permanently healthy.
	MissingTopics []string

	// MeasuredAt is when the measurement was taken. Committed offsets and end offsets are
	// read in two separate round trips, so under live traffic the figure is a snapshot of
	// two moments a few milliseconds apart, and a caller comparing it with anything else
	// needs to know when it was made.
	MeasuredAt time.Time
}

// LagByTopic reduces the report to the per-topic totals.
//
// Returns:
//   - map[string]int64: total lag keyed by topic, nil when nothing was measured.
func (r ConsumerLagReport) LagByTopic() map[string]int64 {
	if len(r.Topics) == 0 {
		return nil
	}

	lags := make(map[string]int64, len(r.Topics))
	for _, topic := range r.Topics {
		lags[topic.Topic] = topic.TotalLag
	}

	return lags
}

// LagSamples renders the report as inventory entries for the asynchronous consumer-lag
// gauges, one per topic measured.
//
// Returns:
//   - []metrics.ConsumerLagSample: one entry per measured topic, nil when nothing was
//     measured.
func (r ConsumerLagReport) LagSamples() []metrics.ConsumerLagSample {
	if len(r.Topics) == 0 {
		return nil
	}

	samples := make([]metrics.ConsumerLagSample, 0, len(r.Topics))
	for _, topicLag := range r.Topics {
		samples = append(samples, consumerLagSample(r.SubscriberID, r.GroupID, topicLag))
	}

	return samples
}

// ConsumerLag measures how far a consumer group trails the end of the log, in-process.
//
// Per partition the lag is the end offset minus a baseline, where the baseline is the
// committed offset when the group has one. Two cases need an explicit decision, and
// both are decided here rather than left to arithmetic:
//
//   - NO COMMITTED OFFSET. The group has never committed on this partition, which
//     OffsetFetch reports as -1. THE POLICY IS FULL LAG FROM THE EARLIEST RETAINED
//     OFFSET: the baseline becomes the partition's first offset, so the lag is every
//     record still on the log. This is deliberately not "unknown" and not zero. Zero
//     would make a subscriber that has never started look perfectly healthy, which is
//     the single most important thing this measurement has to catch; and using the
//     earliest RETAINED offset rather than zero avoids inventing lag for records
//     retention has already deleted.
//   - A COMMITTED OFFSET AT OR BEYOND THE END. Legitimately transient — offsets are read
//     in two round trips, and a commit can land in between — and the naive subtraction
//     would produce a NEGATIVE lag, which no gauge should ever carry and which would
//     drag a summed figure below the truth. It is clamped to zero.
//
// Parameters:
//   - ctx context.Context: honoured before every round trip.
//   - req ConsumerLagRequest: the group and topics to measure.
//
// Returns:
//   - ConsumerLagReport: per-partition, per-topic and total lag.
//   - error: ErrKafkaAdminNotConfigured, a validation error for a missing group, or a
//     wrapped broker error.
func (a *KafkaAdminClient) ConsumerLag(ctx context.Context, req ConsumerLagRequest) (_ ConsumerLagReport, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "consumer_lag", hashedSubscriberSpanAttribute(req.SubscriberID))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	report := ConsumerLagReport{
		SubscriberID: strings.TrimSpace(req.SubscriberID),
		GroupID:      strings.TrimSpace(req.GroupID),
		MeasuredAt:   time.Now().UTC(),
	}

	if err := a.ready(ctx); err != nil {
		return report, err
	}

	if report.GroupID == "" {
		return report, errors.New("kafka admin: a consumer group ID is required to measure consumer lag")
	}

	topics := normalizeTopicList(req.Topics)
	if len(topics) == 0 {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash":  subscriberLogLabel(report.SubscriberID),
			"consumer_group_hash": consumerGroupLogLabel(report.GroupID),
		}).Debug("kafka admin: no topics to measure consumer lag for")

		return report, nil
	}

	// The partition layout and the end offsets come from ONE memoised snapshot, because
	// they are the two questions every subscriber in a sweep asks identically — only the
	// committed offsets below actually differ. Reading them together also keeps them
	// CONSISTENT with each other: a partition present in the layout but absent from the
	// bounds, or the reverse, would produce a lag computed from two different views of the
	// cluster.
	snapshot, err := a.partitionOffsetSnapshot(ctx, topics)
	partitions := snapshot.partitions
	report.MissingTopics = snapshot.missing
	if err != nil {
		return report, err
	}

	if len(report.MissingTopics) > 0 {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash":  subscriberLogLabel(report.SubscriberID),
			"consumer_group_hash": consumerGroupLogLabel(report.GroupID),
			"topics":              report.MissingTopics,
		}).Warn("kafka admin: consumer lag was requested for topics that do not exist; they contribute no lag")
	}

	if len(partitions) == 0 {
		return report, nil
	}

	bounds := snapshot.bounds

	committed, err := a.committedOffsets(ctx, report.GroupID, partitions)
	if err != nil {
		return report, err
	}

	report.Topics = make([]TopicLag, 0, len(partitions))
	for _, topic := range topics {
		ids, exists := partitions[topic]
		if !exists {
			continue
		}

		topicLag := TopicLag{Topic: topic, Partitions: make([]PartitionLag, 0, len(ids))}
		for _, id := range ids {
			partitionLag := buildPartitionLag(
				topic, id,
				offsetBoundsFor(bounds, topic, id),
				committedOffsetFor(committed, topic, id),
			)

			topicLag.TotalLag += partitionLag.Lag
			// UNAVAILABLE FIRST: a partition nobody could read is not a partition without a
			// commit, and counting it in both would report the same fault twice under two
			// different diagnoses — one of which ("the group is not consuming this topic") sends
			// an operator to the consumer rather than to the broker.
			if partitionLag.Unavailable {
				topicLag.PartitionsUnavailable++
			} else if !partitionLag.Committed {
				topicLag.PartitionsWithoutCommit++
			}
			topicLag.Partitions = append(topicLag.Partitions, partitionLag)
		}

		report.TotalLag += topicLag.TotalLag
		report.Topics = append(report.Topics, topicLag)

		// A topic with unreadable partitions is reported LOUDLY, and this is the only place
		// the condition is stated per topic. The measurement continues — a partial total is
		// still the best diagnosis available and the caller receives it — but it must not be
		// mistaken for a complete one, so LagSamples marks it incomplete and the gauge the
		// alert reads does not receive it. See metrics.ConsumerLagUnmeasuredPartitions.
		if topicLag.PartitionsUnavailable > 0 {
			logrus.WithFields(logrus.Fields{
				"subscriber_id_hash":     subscriberLogLabel(report.SubscriberID),
				"consumer_group_hash":    consumerGroupLogLabel(report.GroupID),
				"topic":                  topic,
				"partitions_unavailable": topicLag.PartitionsUnavailable,
				"partitions":             len(topicLag.Partitions),
				"partial_lag":            topicLag.TotalLag,
			}).Warn(
				"kafka admin: some partitions could not be read, so this topic's lag is a LOWER BOUND and is " +
					"withheld from the consumer-lag gauge; the unmeasured-partition count is published instead " +
					"so the degraded measurement alerts rather than reading as healthy",
			)
		}
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":  subscriberLogLabel(report.SubscriberID),
		"consumer_group_hash": consumerGroupLogLabel(report.GroupID),
		"topics":              len(report.Topics),
		"total_lag":           report.TotalLag,
	}).Debug("kafka admin: consumer lag measured")

	return report, nil
}

// buildPartitionLag assembles one partition's entry from its offset bounds and
// committed offset.
//
// Lag is end offset minus a baseline, so it needs both. Both inputs are now treated the
// same way — no reading, no lag, and the topic is reported as incompletely measured so
// the degradation alerts.
//
// Parameters:
//   - topic string, partition int: the partition being described.
//   - bounds partitionOffsetBounds: the zero value is an unavailable partition, which
//     is what an absent map entry means.
//   - committed committedOffset: the group's committed-offset reading.
//
// Returns:
//   - PartitionLag: fully populated, with Lag never negative and zero whenever
//     Unavailable.
func buildPartitionLag(
	topic string,
	partition int,
	bounds partitionOffsetBounds,
	committed committedOffset,
) PartitionLag {
	unavailable := bounds.unavailable || committed.unavailable

	lag := PartitionLag{
		Topic:           topic,
		Partition:       partition,
		CommittedOffset: committed.offset,
		// An unavailable reading is not evidence of an absent commit: the group may well have
		// committed here and the broker simply did not say. Reporting Committed false for it
		// would put the partition in PartitionsWithoutCommit, which is read as "this consumer
		// is not consuming the topic at all".
		Committed:   !committed.unavailable && committed.offset >= 0,
		FirstOffset: bounds.first,
		EndOffset:   bounds.end,
		Unavailable: unavailable,
	}

	if !unavailable {
		lag.Lag = lagForPartition(committed.offset, bounds.first, bounds.end)
	}

	return lag
}

// lagForPartition is the whole lag arithmetic, isolated so it can be tested
// exhaustively against a table with no broker in sight.
//
// The rules, in the order they apply:
//
//  1. An end offset at or below zero means an empty partition (0) or an unreadable one
//     (-1). Neither can be trailed, so the lag is zero.
//  2. A committed offset below zero means no commit exists. The baseline becomes the
//     earliest RETAINED offset — the documented policy of full lag from the start of the
//     log — and a first offset that is itself unknown falls back to zero.
//  3. A baseline at or beyond the end offset means the group is caught up, or has
//     transiently committed past the end offset that was read a round trip earlier. The
//     lag is zero. THIS CLAMP IS WHAT MAKES A NEGATIVE LAG IMPOSSIBLE, which matters
//     because a negative contribution would pull a summed figure below the truth and
//     silence the very alert this number exists to raise.
//
// Parameters:
//   - committedOffset int64: the group's committed offset, negative when absent.
//   - firstOffset int64: the earliest retained offset, negative when unknown.
//   - endOffset int64: the log end offset.
//
// Returns:
//   - int64: the lag, always zero or greater.
func lagForPartition(committedOffset, firstOffset, endOffset int64) int64 {
	if endOffset <= 0 {
		return 0
	}

	baseline := committedOffset
	if baseline < 0 {
		baseline = firstOffset
		if baseline < 0 {
			baseline = 0
		}
	}

	if baseline >= endOffset {
		return 0
	}

	return endOffset - baseline
}

// offsetBoundsFor reads one partition's offset window out of the ListOffsets result.
//
// AN ABSENT ENTRY IS UNAVAILABLE, NOT EMPTY. Indexing the nested maps directly would
// yield the zero value — first 0, end 0, unavailable false — which reads as a
// legitimate empty partition and would be counted as a real zero.
//
// Parameters:
//   - bounds map[string]map[int]partitionOffsetBounds: the ListOffsets result, possibly
//     nil.
//   - topic string, partition int: the partition to look up.
//
// Returns:
//   - partitionOffsetBounds: the reading, or an explicitly unavailable window.
func offsetBoundsFor(
	bounds map[string]map[int]partitionOffsetBounds,
	topic string,
	partition int,
) partitionOffsetBounds {
	if perPartition, exists := bounds[topic]; exists {
		if bound, ok := perPartition[partition]; ok {
			return bound
		}
	}

	return partitionOffsetBounds{first: -1, end: -1, unavailable: true}
}

// committedOffsetFor reads one partition's committed-offset reading out of the fetch
// result.
//
// Parameters:
//   - committed map[string]map[int]committedOffset: the fetch result, possibly nil.
//   - topic string, partition int: the partition to look up.
//
// Returns:
//   - committedOffset: the reading. The zero-commit default when nothing was recorded.
func committedOffsetFor(
	committed map[string]map[int]committedOffset,
	topic string,
	partition int,
) committedOffset {
	if offsets, exists := committed[topic]; exists {
		if offset, ok := offsets[partition]; ok {
			return offset
		}
	}

	return committedOffset{offset: -1}
}

// consumerLagSample renders one topic's measurement as an inventory entry for the
// asynchronous consumer-lag gauges.
//
// Resolving here rather than at the call site is also what keeps the two gauges' label
// tuples identical to each other, since both are observed from this one sample.
//
// Parameters:
//   - subscriber, group string: the RAW values; each is resolved to a bounded label
//     here rather than by the caller, so no call site can bypass the bound.
//   - topicLag TopicLag: one topic's measurement, carrying both the total and the count
//     of partitions that could not be read.
//
// Returns:
//   - metrics.ConsumerLagSample: the entry to publish.
func consumerLagSample(subscriber, group string, topicLag TopicLag) metrics.ConsumerLagSample {
	return metrics.ConsumerLagSample{
		Subscriber:           subscriberLagLabel(subscriber),
		Group:                consumerGroupLagLabel(group),
		Topic:                topicLagLabel(topicLag.Topic),
		Lag:                  topicLag.TotalLag,
		LagComplete:          topicLag.PartitionsUnavailable == 0,
		UnmeasuredPartitions: topicLag.PartitionsUnavailable,
	}
}

// partitionOffsetBounds is one partition's offset window: the earliest offset still
// retained and the log end offset.
//
// The zero value is deliberately NOT a valid reading — first and end are both zero and
// unavailable is false — which is why every consumer of the map treats an absent entry
// as unavailable rather than as an empty partition.
type partitionOffsetBounds struct {
	// first is the earliest retained offset, -1 when unknown.
	first int64

	// end is the log end offset, -1 when unknown.
	end int64

	// unavailable is true when the broker reported an error for this partition or gave
	// no usable end offset.
	unavailable bool
}

// offsetBounds reads the first and end offset of every given partition in ONE round
// trip.
//
// Parameters:
//   - ctx context.Context
//   - partitions map[string][]int: partitions to read, from topicPartitions.
//
// Returns:
//   - map[string]map[int]partitionOffsetBounds: bounds per topic and partition.
//   - error: a wrapped transport error.
func (a *KafkaAdminClient) offsetBounds(
	ctx context.Context,
	partitions map[string][]int,
) (map[string]map[int]partitionOffsetBounds, error) {
	request := &kafka.ListOffsetsRequest{
		Topics:         make(map[string][]kafka.OffsetRequest, len(partitions)),
		IsolationLevel: kafka.ReadUncommitted,
	}

	for topic, ids := range partitions {
		requests := make([]kafka.OffsetRequest, 0, len(ids)*2)
		for _, id := range ids {
			requests = append(requests, kafka.FirstOffsetOf(id), kafka.LastOffsetOf(id))
		}
		request.Topics[topic] = requests
	}

	response, err := a.client.ListOffsets(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("kafka admin: reading partition offsets: %w", err)
	}

	bounds := make(map[string]map[int]partitionOffsetBounds, len(response.Topics))
	for topic, offsets := range response.Topics {
		perPartition := make(map[int]partitionOffsetBounds, len(offsets))

		for _, offset := range offsets {
			bound := partitionOffsetBounds{first: offset.FirstOffset, end: offset.LastOffset}

			if offset.Error != nil || offset.LastOffset < 0 {
				bound.unavailable = true

				entry := logrus.WithFields(logrus.Fields{
					"topic":      topic,
					"partition":  offset.Partition,
					"end_offset": offset.LastOffset,
				})
				if offset.Error != nil {
					entry = withKafkaError(entry, "read_partition_end_offsets", offset.Error)
				}
				entry.Warn(
					"kafka admin: could not read offsets for this partition; it is excluded from the measurement " +
						"rather than counted as zero",
				)
			}

			perPartition[offset.Partition] = bound
		}

		bounds[topic] = perPartition
	}

	return bounds, nil
}

// committedOffsets reads a consumer group's committed offsets.
//
// Parameters:
//   - ctx context.Context
//   - group string: the consumer group ID.
//   - partitions map[string][]int: the partitions to ask about.
//
// Returns:
//   - map[string]map[int]committedOffset: the reading per topic and partition.
//   - error: a wrapped transport or group error.
func (a *KafkaAdminClient) committedOffsets(
	ctx context.Context,
	group string,
	partitions map[string][]int,
) (map[string]map[int]committedOffset, error) {
	request := &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  make(map[string][]int, len(partitions)),
	}

	for topic, ids := range partitions {
		// Copied so the request cannot alias, and later mutate, the caller's slices.
		copied := make([]int, len(ids))
		copy(copied, ids)
		request.Topics[topic] = copied
	}

	response, err := a.client.OffsetFetch(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("kafka admin: fetching committed offsets for consumer group %q: %w", group, err)
	}

	if response.Error != nil {
		if errors.Is(response.Error, kafka.GroupIdNotFound) {
			// DEBUG, not Info. This is the NORMAL state of every subscriber that has been
			// provisioned but has not connected yet, and the periodic collector re-measures
			// every registered subscriber on every tick — so at Info one un-started subscriber
			// produced a line per tick, indefinitely, for a condition that is not a fault. That
			// volume is not free: it is what trains an operator to filter the logger out, and it
			// takes the genuine warnings from this file with it.
			logrus.WithField("consumer_group_hash", consumerGroupLogLabel(group)).Debug(
				"kafka admin: consumer group does not exist yet, so it has committed nothing; " +
					"its lag is the whole retained log",
			)

			return nil, nil
		}

		return nil, fmt.Errorf(
			"kafka admin: fetching committed offsets for consumer group %q: %w", group, response.Error,
		)
	}

	committed := make(map[string]map[int]committedOffset, len(response.Topics))
	for topic, offsets := range response.Topics {
		perPartition := make(map[int]committedOffset, len(offsets))

		for _, offset := range offsets {
			if offset.Error != nil {
				// RECORDED AS UNAVAILABLE, not skipped.
				logrus.WithFields(logrus.Fields{
					"consumer_group_hash": consumerGroupLogLabel(group),
					"topic":               topic,
					"partition":           offset.Partition,
					"error_class":         kafkaErrorClassField("fetch_committed_offsets", offset.Error),
				}).Warn(
					"kafka admin: the broker could not report this partition's committed offset, so its " +
						"lag is UNKNOWN and is withheld rather than scored as uncommitted; the topic is " +
						"reported as incompletely measured",
				)

				perPartition[offset.Partition] = committedOffset{offset: -1, unavailable: true}

				continue
			}

			perPartition[offset.Partition] = committedOffset{offset: offset.CommittedOffset}
		}

		committed[topic] = perPartition
	}

	return committed, nil
}

// committedOffset is ONE partition's committed-offset reading, and it exists to keep
// two facts apart that a bare int64 conflated.
type committedOffset struct {
	// offset is the group's committed offset, or -1 when it has none. Meaningless when
	// unavailable is true.
	offset int64

	// unavailable is true when the broker returned a per-partition error for this
	// partition. The partition then contributes no lag and makes its topic's measurement
	// incomplete.
	unavailable bool
}

// PartitionOffsetSnapshot is one partition's offset window at a point in time.
type PartitionOffsetSnapshot struct {
	// Partition is the partition ID.
	Partition int

	// FirstOffset is the earliest offset still retained: the INCLUSIVE lower bound of the
	// window this partition can currently serve.
	FirstOffset int64

	// EndOffset is the log end offset, one past the last record written, which also equals
	// the total number of records ever produced to the partition.
	EndOffset int64

	// Unavailable is true when the broker could not report this partition, in which case
	// both offsets are meaningless: the partition contributes nothing to the sums and is
	// omitted from PartitionIntervals entirely, so the rows on it are classified as
	// unmeasured rather than misread as beyond the log end.
	Unavailable bool

	// WindowStartOffset is the earliest offset whose record was written at or after the
	// report's WindowStart: the left-hand side of the windowed record count. It is -1 when
	// the partition holds nothing that recent, and 0 with WindowUnreadable set when the
	// window could not be resolved for this partition.
	WindowStartOffset int64

	// WindowUnreadable is true when the window-start offset could not be resolved, so this
	// partition contributes nothing to the windowed count and the count is incomplete.
	WindowUnreadable bool
}

// TopicOffsetSnapshot aggregates one topic's partitions.
type TopicOffsetSnapshot struct {
	// Topic is the topic measured.
	Topic string

	// Partitions carries the per-partition detail, in ascending partition order.
	Partitions []PartitionOffsetSnapshot

	// EndOffsetSum is the sum of the partitions' end offsets: every record ever published
	// to this topic BY ANY PRODUCER, whether or not it is still retained.
	EndOffsetSum int64

	// RetainedCount is the sum of end minus first across partitions: the records still on
	// the log. Like EndOffsetSum it is context rather than proof, and it is reported so
	// that the share of a topic's traffic the verdict accounts for is readable beside it.
	RetainedCount int64

	// PartitionsUnavailable counts partitions excluded from the sums.
	PartitionsUnavailable int

	// WindowRecordCount is how many records this topic accepted inside the report's
	// window: the sum over partitions of end offset minus window-start offset. It is the
	// figure the WINDOWED reconciliation compares the outbox against, and it is zero when
	// the report carries no window.
	WindowRecordCount int64

	// WindowTruncated is true when retention has removed records that were written inside
	// the window, so this topic's window count is a LOWER bound. It is detected rather
	// than assumed: a partition whose window-start offset equals its first retained
	// offset, on a partition that has already had records deleted, cannot rule out that
	// earlier records inside the window are gone.
	WindowTruncated bool

	// WindowPartitionsUnreadable counts partitions whose window-start offset could not be
	// resolved, so their records are missing from WindowRecordCount.
	WindowPartitionsUnreadable int
}

// TopicOffsetReport is the broker-side half of the zero-loss reconciliation.
//
// Its load-bearing content is the PER-PARTITION WINDOWS, reachable through
// PartitionIntervals(): each outbox row's stored coordinate is checked for membership
// in the window of its own partition, which is what makes the reconciliation a bounded
// mapping rather than a comparison of totals.
type TopicOffsetReport struct {
	// Topics carries per-topic detail, in the order the request listed them, or the
	// canonical inventory order when the request named no topics.
	Topics []TopicOffsetSnapshot

	// EndOffsetSum is the total number of RECORDS WRITTEN across every topic measured, and
	// RetainedCount how many of those the broker still holds.
	EndOffsetSum  int64
	RetainedCount int64

	// MissingTopics lists requested topics that do not exist on the broker.
	MissingTopics []string

	// PartitionsUnavailable counts partitions excluded from the totals across all
	// topics.
	PartitionsUnavailable int

	// MeasuredAt is when the snapshot was taken.
	MeasuredAt time.Time

	// WindowStart is the instant the windowed figures below are measured from, and the
	// zero value means the report carries no window.
	WindowStart time.Time

	// WindowRecordCount is how many records the broker accepted across every measured topic
	// inside the window. This is the figure a windowed reconciliation compares against.
	WindowRecordCount int64

	// WindowTruncated is true when retention has removed records written inside the window
	// on at least one partition, which makes WindowRecordCount a lower bound and the
	// verdict inconclusive: a short broker retention against a longer reconciliation
	// window is exactly the configuration that would otherwise report loss that never
	// happened.
	WindowTruncated bool

	// WindowPartitionsUnreadable counts partitions whose window-start offset could not be
	// resolved across all topics.
	WindowPartitionsUnreadable int
}

// EndOffsetsByTopic reduces the report to one end-offset sum per topic.
//
// This is the shape the outbox statistics endpoint reports, so the mapping lives here
// rather than being re-derived by every caller.
//
// Returns:
//   - map[string]int64: end-offset sum keyed by topic, nil when nothing was measured.
func (r TopicOffsetReport) EndOffsetsByTopic() map[string]int64 {
	if len(r.Topics) == 0 {
		return nil
	}

	offsets := make(map[string]int64, len(r.Topics))
	for _, topic := range r.Topics {
		offsets[topic.Topic] = topic.EndOffsetSum
	}

	return offsets
}

// Lookup finds one topic's snapshot.
//
// Parameters:
//   - topic string: a fully-qualified topic name.
//
// Returns:
//   - TopicOffsetSnapshot: the entry, or the zero value when absent.
//   - bool: whether the topic appears in the report.
func (r TopicOffsetReport) Lookup(topic string) (TopicOffsetSnapshot, bool) {
	for _, snapshot := range r.Topics {
		if snapshot.Topic == topic {
			return snapshot, true
		}
	}

	return TopicOffsetSnapshot{}, false
}

// TopicEndOffsets measures the PER-PARTITION OFFSET WINDOWS the daily zero-loss
// reconciliation classifies the outbox against.
//
// A surplus is therefore INDISTINGUISHABLE FROM COMPENSATED LOSS — ten redeliveries and
// ten lost events produce exactly the totals of a healthy pipeline — which is why the
// verdict now rests on the per-coordinate mapping and reports the sums only as context.
//
// Parameters:
//   - ctx context.Context: honoured before every round trip.
//   - topics ...string: optional topic names. Blanks and duplicates are dropped; an
//     entirely empty list selects the full inventory.
//
// Returns:
//   - TopicOffsetReport: the per-partition windows, the sums, and the caveats
//     (MissingTopics, PartitionsUnavailable) that say what could not be measured.
//   - error: ErrKafkaAdminNotConfigured, or a wrapped broker error.
func (a *KafkaAdminClient) TopicEndOffsets(
	ctx context.Context,
	since time.Time,
	topics ...string,
) (_ TopicOffsetReport, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "topic_end_offsets")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	report := TopicOffsetReport{MeasuredAt: time.Now().UTC()}
	if err := a.ready(ctx); err != nil {
		return report, err
	}

	requested := normalizeTopicList(topics)
	if len(requested) == 0 {
		// The inventory, from its single source of truth, so the reconciliation covers
		// exactly the topics the pipeline provisions and publishes to — across every owned
		// prefix, because the outbox rows this is reconciled against may name a namespace the
		// deployment has since renamed away from. Omitting a historical topic would leave its
		// dispatched rows counted with no offsets to match them, which reads as loss that did
		// not happen.
		requested = AllOwnedTopicsAcrossPrefixes()
	}

	partitions, err := a.topicPartitions(ctx, requested)
	report.MissingTopics = missingTopics(requested, partitions)
	if err != nil {
		return report, err
	}

	if len(partitions) == 0 {
		return report, nil
	}

	bounds, err := a.offsetBounds(ctx, partitions)
	if err != nil {
		return report, err
	}

	// The window's left-hand side, read only when one was asked for. A failure here does NOT
	// void the report: the cumulative figures are still valid and are what the diagnostic
	// reading wants, so the window is simply left unset and ReconcileAgainstOutbox declines to
	// call the comparison conclusive — which is the honest outcome of a window that could not
	// be measured.
	var windowStarts map[string]map[int]int64
	if !since.IsZero() {
		report.WindowStart = since.UTC()

		windowStarts, err = a.windowStartOffsets(ctx, partitions, since)
		if err != nil {
			kafkaErrorEntry("topic_window_start_offsets", err).Error(
				"kafka admin: the window-start offsets could not be read, so the reconciliation has no " +
					"bounded broker-side population and its verdict will be reported inconclusive",
			)

			report.WindowStart = time.Time{}
		}
	}

	report.Topics = make([]TopicOffsetSnapshot, 0, len(partitions))
	for _, topic := range requested {
		ids, exists := partitions[topic]
		if !exists {
			continue
		}

		snapshot := TopicOffsetSnapshot{Topic: topic, Partitions: make([]PartitionOffsetSnapshot, 0, len(ids))}
		for _, id := range ids {
			bound := offsetBoundsFor(bounds, topic, id)

			partitionSnapshot := PartitionOffsetSnapshot{
				Partition:         id,
				FirstOffset:       bound.first,
				EndOffset:         bound.end,
				WindowStartOffset: -1,
				Unavailable:       bound.unavailable,
			}

			if bound.unavailable {
				snapshot.PartitionsUnavailable++
			} else {
				snapshot.EndOffsetSum += bound.end
				snapshot.RetainedCount += retainedRecords(bound.first, bound.end)

				if !report.WindowStart.IsZero() {
					applyWindowToPartition(&partitionSnapshot, &snapshot, windowStarts, topic, id, bound)
				}
			}

			snapshot.Partitions = append(snapshot.Partitions, partitionSnapshot)
		}

		report.EndOffsetSum += snapshot.EndOffsetSum
		report.RetainedCount += snapshot.RetainedCount
		report.PartitionsUnavailable += snapshot.PartitionsUnavailable
		report.WindowRecordCount += snapshot.WindowRecordCount
		report.WindowPartitionsUnreadable += snapshot.WindowPartitionsUnreadable
		report.WindowTruncated = report.WindowTruncated || snapshot.WindowTruncated
		report.Topics = append(report.Topics, snapshot)
	}

	report.MeasuredAt = time.Now().UTC()

	logrus.WithFields(logrus.Fields{
		"topics":                 len(report.Topics),
		"end_offset_sum":         report.EndOffsetSum,
		"retained":               report.RetainedCount,
		"window_start":           report.WindowStart.Format(time.RFC3339),
		"window_records":         report.WindowRecordCount,
		"window_truncated":       report.WindowTruncated,
		"missing_topics":         len(report.MissingTopics),
		"partitions_unavailable": report.PartitionsUnavailable,
	}).Debug("kafka admin: topic end offsets read")

	return report, nil
}

// retainedRecords counts the records still on a partition's log.
//
// It is end minus first, clamped at zero. The clamp covers an empty partition whose
// first and end offsets are equal and above zero — the state of a partition every
// record of which has been deleted by retention — where the subtraction is already
// zero, and any transient reading where first exceeds end.
//
// Parameters:
//   - firstOffset int64: the earliest retained offset, negative when unknown.
//   - endOffset int64: the log end offset.
//
// Returns:
//   - int64: the retained record count, never negative.
func retainedRecords(firstOffset, endOffset int64) int64 {
	if endOffset <= 0 {
		return 0
	}

	first := firstOffset
	if first < 0 {
		first = 0
	}

	if first >= endOffset {
		return 0
	}

	return endOffset - first
}

// PartitionIntervals flattens the report into the measured windows the outbox audit is
// classified against.
//
// Returns:
//   - []model.PartitionOffsetInterval: one window per measured, available partition, in
//     report order.
func (r TopicOffsetReport) PartitionIntervals() []model.PartitionOffsetInterval {
	intervals := make([]model.PartitionOffsetInterval, 0, len(r.Topics))
	for _, topic := range r.Topics {
		for _, partition := range topic.Partitions {
			if partition.Unavailable {
				continue
			}

			intervals = append(intervals, model.PartitionOffsetInterval{
				Topic:       topic.Topic,
				Partition:   partition.Partition,
				FirstOffset: partition.FirstOffset,
				EndOffset:   partition.EndOffset,
			})
		}
	}

	return intervals
}

// OutboxReconciliation is the verdict of comparing the outbox against the broker.
type OutboxReconciliation struct {
	// TerminalEvents is how many outbox rows claim to have been published to the broker.
	TerminalEvents int64

	// CorroboratedEvents is how many of those rows name a record INSIDE the measured
	// [first, end) window of its partition — a record the broker can serve right now.
	CorroboratedEvents int64

	// UnconfirmedEvents claim a publication without naming any record at all.
	UnconfirmedEvents int64

	// UnmeasuredEvents name a topic or partition the measurement did not cover: a missing
	// topic, an unavailable partition, or a partition count that has since shrunk. Their
	// records may well be there; nothing in this measurement says so.
	UnmeasuredEvents int64

	// AgedOutEvents name an offset BELOW the retained window. The record was written and
	// Kafka retention has since deleted it.
	AgedOutEvents int64

	// BeyondEndEvents name an offset AT OR ABOVE the log end of their partition.
	BeyondEndEvents int64

	// DuplicatedRecords is how many corroborated rows share a coordinate with another row.
	DuplicatedRecords int64

	// MessagesWritten is how many records the broker has accepted across the measured
	// topics, from summed end offsets, and RecordsRetained how many of those it still
	// holds.
	MessagesWritten int64
	RecordsRetained int64

	// BlnkRecordShare is how many of the retained records this reconciliation attributed
	// to Blnk rows: CorroboratedEvents. Reported as its own field so the response states
	// plainly how much of a shared topic's traffic the verdict actually accounts for.
	BlnkRecordShare int64

	// LossDetected is true when specific records this outbox recorded are provably not on
	// the log: a row naming an offset at or above its partition's end.
	LossDetected bool

	// Conclusive reports whether the mapping accounted for EVERY retained claim.
	Conclusive bool

	// Caveats names, in plain words, every reason the result is inconclusive. Empty when
	// Conclusive is true.
	Caveats []string

	// CoveredFrom and CoveredTo bound the publication instants of the corroborated
	// population: the window a green verdict actually speaks about.
	CoveredFrom time.Time
	CoveredTo   time.Time

	// OldestTerminalAt is the earliest publication instant among all retained terminal
	// rows. Anything published before it has been pruned from the outbox and is outside
	// the reach of any verdict — which is why it is reported rather than left implicit.
	OldestTerminalAt time.Time

	// MeasuredAt is when the broker side was measured.
	MeasuredAt time.Time

	// WindowStart is the instant both sides were measured from, zero when the comparison was
	// whole-history.
	WindowStart time.Time

	// Overhead is MessagesWritten minus TerminalEvents: the redelivery, replay and
	// dead-letter copies. Its EXPECTED value is greater than or equal to zero, and a
	// healthy system's overhead is small but not zero.
	Overhead int64

	// Windowed reports whether both sides were bounded to a common window. Only a windowed
	// comparison can be conclusive — see the caveat ReconcileAgainstOutbox adds when it is
	// not — so this is the field to read before believing MessagesWritten or Overhead.
	Windowed bool
}

// Summary renders the verdict as one sentence for a log line or a runbook.
//
// Returns:
//   - string: the verdict, always naming the numbers it rests on so the sentence is
//     checkable against the fields.
func (r OutboxReconciliation) Summary() string {
	switch {
	case r.BeyondEndEvents > 0:
		// Stated ahead of the arithmetic because it is stronger evidence: an offset past the
		// end of a log cannot be explained by any amount of surplus.
		return fmt.Sprintf(
			"LOSS DETECTED: %d row(s) name a broker record at or beyond the end of the partition "+
				"they claim, so those records do not exist. That is only possible if the partition was "+
				"truncated or the topic was deleted and recreated beneath the ledger, or the events "+
				"were lost after being marked published",
			r.BeyondEndEvents,
		)
	case r.LossDetected:
		return fmt.Sprintf(
			"LOSS DETECTED: the broker holds %d record(s) inside the measured window against %d "+
				"outbox row(s) claiming a publication inside it, a shortfall of %d. Records are a "+
				"lower bound on events — every redelivery, replay and dead-letter copy adds one — so "+
				"a shortfall means rows claim a publication that never happened",
			r.MessagesWritten, r.TerminalEvents, -r.Overhead,
		)
	case !r.Conclusive:
		return fmt.Sprintf(
			"INCONCLUSIVE: %d of %d outbox rows were matched to a record inside the measured broker "+
				"windows, and the rest could not be (%s)",
			r.CorroboratedEvents, r.TerminalEvents, strings.Join(r.Caveats, "; "),
		)
	case r.TerminalEvents == 0:
		return "NO LOSS DETECTED: the outbox holds no rows claiming a publication, so there is " +
			"nothing to reconcile"
	case !r.Windowed:
		// THE SAME GREEN VERDICT, WITHOUT CALLING A CUMULATIVE TOTAL A SURPLUS.
		return fmt.Sprintf(
			"NO LOSS DETECTED: every one of the %d outbox rows published between %s and %s names the "+
				"distinct broker record it produced, inside the measured offset window of its own "+
				"partition, so no event this outbox still retains is missing from the broker. The "+
				"broker's %d record(s) are a CUMULATIVE total for the topics rather than a count "+
				"inside a shared window, so the difference against the row count is not a surplus "+
				"and no shortfall can be computed from it",
			r.CorroboratedEvents,
			r.CoveredFrom.Format(time.RFC3339),
			r.CoveredTo.Format(time.RFC3339),
			r.MessagesWritten,
		)
	default:
		// The SURPLUS is named, because a green verdict that only said "no loss detected"
		// left an operator unable to tell a healthy overhead from a shortfall that happened
		// to be hidden by one: what makes the surplus safe is that every row names the
		// distinct record it produced, so the extra records belong to redeliveries, replays
		// and dead-letter copies rather than to events nothing accounts for.
		return fmt.Sprintf(
			"NO LOSS DETECTED: every one of the %d outbox rows published between %s and %s names the "+
				"distinct broker record it produced, inside the measured offset window of its own "+
				"partition, so the %d record(s) of surplus are redelivery, replay and dead-letter "+
				"copies rather than unaccounted events, and no event this outbox still retains is "+
				"missing from the broker",
			r.CorroboratedEvents,
			r.CoveredFrom.Format(time.RFC3339),
			r.CoveredTo.Format(time.RFC3339),
			r.Overhead,
		)
	}
}

// ReconcileAgainstOutbox interprets an offset report against the outbox's interval
// audit.
//
// It is the ONLY sanctioned way to compare the two, and it exists so that the reasoning
// lives in one place rather than being re-derived — wrongly — by each caller.
//
// Parameters:
//   - report TopicOffsetReport: the broker-side measurement from TopicEndOffsets, whose
//     PartitionIntervals() must be the windows the audit was taken against.
//   - audit model.EventRecordIntervalAudit: the outbox-side classification from
//     AuditEventRecordsInIntervals.
//
// Returns:
//   - OutboxReconciliation: the verdict, always populated.
func ReconcileAgainstOutbox(
	report TopicOffsetReport,
	audit model.EventRecordIntervalAudit,
) OutboxReconciliation {
	// THE COMMON POPULATION. When both sides name a window the comparison is drawn INSIDE
	// it, because the cumulative sum counts a history the outbox no longer holds: a topic
	// that has accepted a million records over its life and a thousand inside the window
	// must reconcile against the rows the outbox holds for that window, or every
	// reconciliation reports a surplus that grows without bound. With no window on either
	// side the cumulative reading is the only one available, and the caveats say so.
	windowed := !report.WindowStart.IsZero() && !audit.WindowStart.IsZero()

	writtenRecords := report.EndOffsetSum
	if windowed {
		writtenRecords = report.WindowRecordCount
	}

	verdict := OutboxReconciliation{
		TerminalEvents:     audit.PublishedRows,
		CorroboratedEvents: audit.CorroboratedRows,
		UnconfirmedEvents:  audit.UnconfirmedRows,
		UnmeasuredEvents:   audit.UnmeasuredRows,
		AgedOutEvents:      audit.AgedOutRows,
		BeyondEndEvents:    audit.BeyondEndRows,
		DuplicatedRecords:  audit.DuplicatedRecords(),
		MessagesWritten:    writtenRecords,
		Windowed:           windowed,
		Overhead:           writtenRecords - audit.PublishedRows,
		RecordsRetained:    report.RetainedCount,
		BlnkRecordShare:    audit.CorroboratedRows,
		CoveredFrom:        audit.CorroboratedFrom,
		CoveredTo:          audit.CorroboratedTo,
		OldestTerminalAt:   audit.OldestTerminalAt,
		WindowStart:        report.WindowStart,
		MeasuredAt:         report.MeasuredAt,
	}

	// TWO INDEPENDENT SIGNALS, and either is enough.
	//
	// The first is unambiguous: an offset at or above the log end cannot be explained by
	// retention, by a redelivery or by another producer — the broker issued it, so the log
	// reached it once and does not now.
	//
	// The second is arithmetic and only available on a windowed comparison: records are a
	// LOWER BOUND on events, because a redelivery, a replay and a dead-letter copy each
	// append a record no row claims. So a surplus is normal and a SHORTFALL is not: fewer
	// records inside the window than rows claiming one inside it means rows claim a
	// publication that never happened.
	verdict.LossDetected = verdict.BeyondEndEvents > 0 || (windowed && verdict.Overhead < 0)

	caveats := make([]string, 0, 7)

	// AN UNWINDOWED COMPARISON IS NOT A CAVEAT, and the reason is worth stating because
	// the opposite is the intuitive answer.

	if verdict.BeyondEndEvents > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) name an offset at or beyond the end of their partition's log, which means the "+
				"partition was truncated or the topic was deleted and recreated after those records "+
				"were written",
			verdict.BeyondEndEvents,
		))
	}

	// THE CAVEAT THE COORDINATE MAPPING EXISTS FOR. A row claiming a publication it cannot
	// name a record for is not evidence of loss — the record may well be there — but it is
	// precisely what a surplus of redeliveries could be concealing, so no green verdict
	// may be reported while any remain.
	if verdict.UnconfirmedEvents > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) claim a publication without naming the broker record they produced, so they "+
				"cannot be matched against any measured window",
			verdict.UnconfirmedEvents,
		))
	}

	if verdict.UnmeasuredEvents > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) name a topic or partition this measurement did not cover, so their records "+
				"were neither confirmed nor ruled out",
			verdict.UnmeasuredEvents,
		))
	}

	// Retention is a statement about SPECIFIC EVENTS rather than a topic-wide subtraction.
	// A caveat keyed on anything at all having aged out of a shared topic — including
	// records Blnk never wrote — would be permanently on in any long-lived deployment and
	// would tell an operator nothing about their own events.
	if verdict.AgedOutEvents > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) name a record Kafka retention has already deleted, so the write is evidenced "+
				"by the stored offset but the record can no longer be read",
			verdict.AgedOutEvents,
		))
	}

	// Impossible while the partial unique index on the coordinate exists, which is why its
	// appearance is reported as a schema problem rather than absorbed.
	if verdict.DuplicatedRecords > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) name a broker record another row also names, which the unique index on "+
				"(kafka_topic, kafka_partition, kafka_offset) should make impossible; verify that "+
				"index still exists before trusting any reconciliation",
			verdict.DuplicatedRecords,
		))
	}

	if len(report.MissingTopics) > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d measured topic(s) do not exist on the broker (%s), so no window could be measured "+
				"for them",
			len(report.MissingTopics), strings.Join(report.MissingTopics, ", "),
		))
	}

	if report.PartitionsUnavailable > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d partition(s) did not report offsets, so no window could be measured for them",
			report.PartitionsUnavailable,
		))
	}

	// RETENTION IS ONLY A CAVEAT WHEN IT TRUNCATES THE MEASURED WINDOW.
	if report.WindowTruncated {
		caveats = append(caveats,
			"retention has removed records that were written inside the measured window, so the "+
				"broker-side count is a lower bound; shorten the reconciliation window or lengthen the "+
				"broker's retention so the window fits inside it",
		)
	}

	if report.WindowPartitionsUnreadable > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d partition(s) did not report a window-start offset, so the records they accepted inside "+
				"the window are missing from the total",
			report.WindowPartitionsUnreadable,
		))
	}

	verdict.Caveats = caveats
	verdict.Conclusive = len(caveats) == 0

	return verdict
}

// --------------------------------------------------------------------------- Event
// pipeline statistics — the orchestration behind GET /events/stats
//
// It lives here, in the root package, and not in the HTTP handler. Two costs followed.

// eventOffsetReadTimeout bounds the broker round trip a statistics read makes.
//
// It exists so that an unreachable broker DEGRADES the answer rather than holding the
// caller open: the outbox counts have already been read from PostgreSQL by the time the
// broker is dialled, and reporting them without the offsets is a valid, documented answer.
const eventOffsetReadTimeout = 10 * time.Second

// EventOffsetInclusion is how a statistics read wants the BROKER side treated — and,
// because the two are one decision, whether the DISPATCHED HISTORY is counted at all.
type EventOffsetInclusion int

const (
	// EventOffsetsSkipped makes no broker round trip at all and counts no dispatched
	// history: the answer is the exact unresolved inventory. It is the ZERO VALUE and the
	// posture every routine caller wants.
	EventOffsetsSkipped EventOffsetInclusion = iota

	// EventOffsetsBestEffort counts the dispatched history, reads the broker when one is
	// configured, and omits the broker-side fields on any failure, logging the reason. It
	// is the posture the daily reconciliation runbook relies on, and it must be asked for.
	EventOffsetsBestEffort

	// EventOffsetsRequired is EventOffsetsBestEffort except that a failure to read the
	// broker becomes a typed error, because the caller asked specifically for the half of
	// the reconciliation that failed.
	EventOffsetsRequired
)

// countsDispatchedHistory reports whether this posture counts the unbounded dispatched
// population as well as the unresolved inventory.
//
// Returns:
//   - bool: true for every posture that measures the broker side.
func (i EventOffsetInclusion) countsDispatchedHistory() bool {
	return i != EventOffsetsSkipped
}

// EventOutboxStatistics is the whole state of the event pipeline at one instant: the
// outbox side always, the broker side and the verdict when they could be measured.
//
// Reconciliation is nil unless BOTH sides were measured and at least one topic was
// actually covered. That is a documented absence rather than an error: with nothing
// measured there is nothing to compare, so no verdict is reported.
type EventOutboxStatistics struct {
	// GeneratedAt is when the outbox side was read, in UTC.
	GeneratedAt time.Time

	// CountsByStatus is the per-status aggregate, keyed by the model.EventOutboxStatus*
	// values. A status with no rows is ABSENT rather than present with a zero, because
	// GROUP BY only produces rows that exist — read it with the two-value form or accept
	// the zero value.
	CountsByStatus map[string]int64

	// UnreportedStatuses names any status the table holds that model.EventOutboxStatuses
	// does not know about, sorted.
	UnreportedStatuses []string

	// WindowStart is the instant the WINDOWED figures are measured from, and Window is its
	// length. They are reported rather than implied because only one figure here is
	// windowed — the dispatched count — and a reader who cannot see the interval cannot
	// tell a quiet day from a short window.
	WindowStart time.Time
	Window      time.Duration

	// DispatchedHistoryCounted reports whether the dispatched population was counted at
	// all.
	DispatchedHistoryCounted bool

	// Audit is the outbox side of the reconciliation, classified against the very
	// partition windows the broker reported. Meaningful only when AuditRead.
	Audit model.EventRecordIntervalAudit

	// AuditRead reports whether the audit was actually read. False both when the posture
	// skipped the broker side entirely and when the audit query failed under a best-effort
	// posture.
	AuditRead bool

	// Offsets is the broker-side measurement. Meaningful only when OffsetsRead.
	Offsets TopicOffsetReport

	// OffsetsRead reports whether the broker was actually read.
	OffsetsRead bool

	// Reconciliation is the verdict comparing the two sides, or nil when it could not
	// be produced.
	Reconciliation *OutboxReconciliation

	// ProducerAtomicity is the outstanding half of the two pre-recorded intents, or nil
	// when neither census could be read.
	ProducerAtomicity *ProducerAtomicityCensus
}

// eventStatisticsStore is the repository surface the statistics read needs, and
// deliberately no more of it.
//
// Two reads, both of them reads. A statistics call cannot claim, insert or transition
// anything, and depending on a two-method interface rather than on the whole
// IDataSource is what states that at the type level instead of in a comment.
type eventStatisticsStore interface {
	// CountUnresolvedEventOutbox returns the per-status aggregate for every NON-DISPATCHED
	// status, exact and unwindowed.
	CountUnresolvedEventOutbox(ctx context.Context) (map[string]int64, error)

	// CountEventOutboxByStatus returns the same aggregate PLUS the dispatched history. The
	// instant bounds the unbounded dispatched population only; every other status is
	// counted in full however short the window, because a row stuck for days must not
	// vanish from a one-day reading. It is read only when the broker side is being
	// measured, because the dispatched figure exists to be compared against the broker's
	// records.
	CountEventOutboxByStatus(ctx context.Context, since time.Time) (map[string]int64, error)

	// AuditEventRecordsInIntervals returns the outbox side of the zero-loss
	// reconciliation, classified against the readable offset windows the broker reported.
	// The windows are what make the two sides describe one population, so the audit is
	// taken AFTER the offsets are measured and against those very intervals.
	AuditEventRecordsInIntervals(
		ctx context.Context,
		intervals []model.PartitionOffsetInterval,
	) (model.EventRecordIntervalAudit, error)

	// CountBalanceMonitorHandoffByStatus and CountUnfinalizedBulkTransactionBatches are
	// the two PRE-RECORDED INTENT censuses, and they answer the one question the
	// per-status counts above cannot.
	CountBalanceMonitorHandoffByStatus(ctx context.Context) (map[string]int64, error)
	CountUnfinalizedBulkTransactionBatches(
		ctx context.Context,
		olderThan time.Duration,
	) (int64, *time.Time, error)
}

// ProducerAtomicityCensus is how much of the two pre-recorded intents is outstanding.
//
// It travels with the outbox counts rather than beside them because it answers the same
// question — whether every event that should exist does — and a reader who saw only the
// per-status counts would read a clean outbox as a clean pipeline while alerts and
// batch summaries were still owed.
type ProducerAtomicityCensus struct {
	// MonitorHandoffPending, MonitorHandoffProcessing, MonitorHandoffCompleted and
	// MonitorHandoffFailed are the handoff relay's four states, read straight from the
	// aggregate. A status with no rows is absent from that aggregate and reads as zero
	// here, which is the correct reading of "none in that state".
	//
	// FAILED IS THE NUMBER TO ACT ON: each one is a balance movement whose monitor
	// conditions were never judged, so any alert it should have produced does not exist
	// and never will without intervention.
	MonitorHandoffPending    int64
	MonitorHandoffProcessing int64
	MonitorHandoffCompleted  int64
	MonitorHandoffFailed     int64

	// UnfinalizedBatches counts asynchronous bulk batches that began and never reported an
	// outcome, past the grace period so batches still legitimately running are excluded,
	// and OldestUnfinalizedBatchAt is when the oldest of them began — nil when there are
	// none. The age is what separates a large batch still running from one that was
	// abandoned, so the count alone is not actionable and the pair is.
	UnfinalizedBatches       int64
	OldestUnfinalizedBatchAt *time.Time
}

// unfinalizedBulkBatchGrace is how long a bulk batch may run before an unfinalized
// coordinator row counts as outstanding.
const unfinalizedBulkBatchGrace = 15 * time.Minute

// EventOutboxStatistics assembles the statistics for this instance.
//
// The outbox counts are read FIRST and their failure is the only unconditional one:
// without them there is nothing to report at all. Everything after them is governed by
// the posture:
//
//	EventOffsetsSkipped   returns after the counts. No audit, no broker round trip.
//	EventOffsetsBestEffort logs and omits on an audit or offset failure. This is what
//	                      makes a deployment with no brokers answer 200 with the
//	                      counts, which is a legitimate steady state and not a fault.
//	EventOffsetsRequired  returns a typed error on either failure, because the caller
//	                      asked for the half that could not be produced.
//
// Parameters:
//   - ctx context.Context: cancels the queries and the broker round trip.
//   - inclusion EventOffsetInclusion: how the broker side is to be treated.
//
// Returns:
//   - EventOutboxStatistics: the statistics. Populated whenever err is nil; read
//     AuditRead and OffsetsRead before the fields they govern.
//   - error: a typed APIError — ErrInternalServer when the outbox cannot be read, and
//     ErrKafkaUnavailable when the broker was REQUIRED and could not be read.
const (
	defaultEventStatisticsWindow = 24 * time.Hour
	maxEventStatisticsWindow     = defaultEventStatisticsWindow
)

// normalizeEventStatisticsWindow bounds a requested window.
//
// Parameters:
//   - window time.Duration: the requested length. Zero means "unspecified".
//
// Returns:
//   - time.Duration: a positive duration no longer than the ceiling.
func normalizeEventStatisticsWindow(window time.Duration) time.Duration {
	if window <= 0 {
		return defaultEventStatisticsWindow
	}

	if window > maxEventStatisticsWindow {
		return maxEventStatisticsWindow
	}

	return window
}

func (b *Blnk) EventOutboxStatistics(
	ctx context.Context,
	inclusion EventOffsetInclusion,
	window time.Duration,
) (EventOutboxStatistics, error) {
	store, err := b.eventStatisticsStore()
	if err != nil {
		return EventOutboxStatistics{}, err
	}

	return eventOutboxStatistics(ctx, store, b.readEventTopicEndOffsets, inclusion, window)
}

// eventOutboxStatistics is the orchestration itself, with its two collaborators passed
// in.
//
// Parameters:
//   - ctx context.Context: cancels both reads.
//   - store eventStatisticsStore: the two repository reads. Must not be nil.
//   - readOffsets func: the broker-side measurement, taking the WINDOW START the outbox
//     side was counted from so both sides describe one interval.
//   - inclusion EventOffsetInclusion: how the broker side is to be treated.
//
// Returns:
//   - EventOutboxStatistics: as EventOutboxStatistics.
//   - error: as EventOutboxStatistics.
func eventOutboxStatistics(
	ctx context.Context,
	store eventStatisticsStore,
	readOffsets func(context.Context, time.Time) (TopicOffsetReport, error),
	inclusion EventOffsetInclusion,
	window time.Duration,
) (EventOutboxStatistics, error) {
	ctx, span := tracer.Start(ctx, "EventOutboxStatistics")
	defer span.End()

	if store == nil {
		err := apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The event outbox is unavailable because the service is not initialised",
			errEventStatisticsDataSourceMissing,
		)
		span.RecordError(err)

		return EventOutboxStatistics{}, err
	}

	if readOffsets == nil {
		// An absent reader is the same situation as an unconfigured broker, and reporting it
		// as that error rather than panicking is what keeps the two postures behaving
		// identically for a caller that has no broker at all.
		readOffsets = func(context.Context, time.Time) (TopicOffsetReport, error) {
			return TopicOffsetReport{}, ErrKafkaAdminNotConfigured
		}
	}

	// ONE WINDOW START, DERIVED ONCE, AND REPORTED. So the request did not get the whole
	// history: it got twenty-four hours of dispatched rows, described to the operator as
	// though it were everything, with no window on the response to say otherwise.
	windowStart := time.Now().UTC().Add(-normalizeEventStatisticsWindow(window))

	// WHICH AGGREGATE, decided by the posture and by nothing else. Skipped takes the
	// unresolved inventory alone — exact, unwindowed, bounded by outstanding work — and
	// the two broker-reading postures additionally count the dispatched history, because
	// that figure exists only to be compared against what the broker recorded. See
	// EventOffsetInclusion for why this is one decision rather than two flags.
	countsHistory := inclusion.countsDispatchedHistory()

	counts, err := readEventStatusCounts(ctx, store, windowStart, countsHistory)
	if err != nil {
		span.RecordError(err)

		return EventOutboxStatistics{}, err
	}

	statistics := EventOutboxStatistics{
		GeneratedAt:              time.Now().UTC(),
		CountsByStatus:           counts,
		UnreportedStatuses:       unreportedEventOutboxStatuses(counts),
		WindowStart:              windowStart,
		Window:                   normalizeEventStatisticsWindow(window),
		DispatchedHistoryCounted: countsHistory,
		// THE OWED EVENTS, read from PostgreSQL alongside the counts and never from the
		// broker, so the figure is present in every posture — including the one that skips
		// Kafka entirely and the one where the broker is unreachable, which is exactly when
		// an operator is asking what is outstanding.
		ProducerAtomicity: producerAtomicityCensus(ctx, store),
	}

	if len(statistics.UnreportedStatuses) > 0 {
		// Logged HERE as well as returned, because the caller may render it and may
		// not, and this is a schema-drift warning an operator needs to see either way.
		logrus.WithField("statuses", strings.Join(statistics.UnreportedStatuses, ", ")).Warn(
			"the event outbox holds rows in states nothing reports a count for, so the reported " +
				"per-status counts sum to less than the table's row count; teach the statistics " +
				"projection the new state before trusting the zero-loss reconciliation",
		)
	}

	span.SetAttributes(
		attribute.Int("event_outbox.statuses_reported", len(counts)),
		attribute.Int("event_outbox.statuses_unreported", len(statistics.UnreportedStatuses)),
		attribute.Bool("event_outbox.dispatched_history_counted", countsHistory),
	)

	if !countsHistory {
		return statistics, nil
	}

	// MEASURED FROM THE SAME INSTANT THE OUTBOX SIDE WAS COUNTED FROM. Two populations,
	// one verdict — and the verdict could still report itself CONCLUSIVE, because the
	// arithmetic that would have caught the mismatch (a shortfall of records against rows)
	// is only available when both sides name a window, and with the broker side unbounded
	// it was silently switched off. Passing the window start here is what makes the
	// comparison a comparison.
	report, err := readOffsets(ctx, windowStart)
	if err != nil {
		if inclusion == EventOffsetsRequired {
			// The driver error carries transport detail, which renders with broker addresses and
			// topology, so it is logged here and a fixed message is returned in its place.
			withLoggableCause(nil, err).Error(
				"the Kafka topic end offsets could not be read for a statistics request that " +
					"required them",
			)
			span.RecordError(err)

			return statistics, apierror.NewAPIError(
				apierror.ErrKafkaUnavailable,
				"The Kafka broker could not be read, so the topic end offsets this request required "+
					"are unavailable; retry once the broker recovers or omit include_offsets to "+
					"receive the outbox counts alone",
				errors.New("blnk: reading the event topic end offsets failed"),
			)
		}

		withLoggableCause(nil, err).Warn(
			"the Kafka topic end offsets could not be read, so the statistics report the " +
				"per-status counts alone; this is the expected result when no brokers are configured",
		)

		return statistics, nil
	}
	statistics.Offsets, statistics.OffsetsRead = report, true

	// The audit is taken against the windows THIS report measured, never against the whole
	// table: an audit and an offset total that describe different populations cannot be
	// compared, which is the defect the interval form exists to close.
	audit, err := store.AuditEventRecordsInIntervals(ctx, report.PartitionIntervals())
	if err != nil {
		if inclusion == EventOffsetsRequired {
			span.RecordError(err)

			return statistics, err
		}

		withLoggableCause(nil, err).Warn(
			"the event outbox audit could not be read, so the statistics report the per-status " +
				"counts and the measured offsets without the zero-loss verdict",
		)

		return statistics, nil
	}
	statistics.Audit, statistics.AuditRead = audit, true

	// With nothing measured there is nothing to compare against, so no verdict is
	// produced. A verdict computed over zero topics would report a clean bill of health it
	// never established.
	if len(report.Topics) > 0 {
		verdict := ReconcileAgainstOutbox(report, audit)
		statistics.Reconciliation = &verdict

		span.SetAttributes(
			attribute.Bool("event_outbox.reconciliation.conclusive", verdict.Conclusive),
			attribute.Bool("event_outbox.reconciliation.loss_detected", verdict.LossDetected),
		)
	}

	return statistics, nil
}

// readEventStatusCounts reads whichever per-status aggregate the posture calls for.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//   - store eventStatisticsStore: the repository seam. Must not be nil.
//   - windowStart time.Time: the instant the dispatched arm counts from.
//   - includeHistory bool: true to count the unbounded dispatched population as well.
//
// Returns:
//   - map[string]int64: counts by status. The `dispatched` key is present only when
//     includeHistory and the window actually holds dispatched rows.
//   - error: the repository's own typed error, unwrapped.
func readEventStatusCounts(
	ctx context.Context,
	store eventStatisticsStore,
	windowStart time.Time,
	includeHistory bool,
) (map[string]int64, error) {
	if includeHistory {
		return store.CountEventOutboxByStatus(ctx, windowStart)
	}

	return store.CountUnresolvedEventOutbox(ctx)
}

// producerAtomicityCensus reads the two pre-recorded intent censuses, and answers nil
// rather than zeros when either read fails.
//
// It is DEGRADING and not refusing, deliberately. The per-status counts and the broker
// comparison are the reconciliation's main line; a census read that fails must not deny
// an operator the rest of the statistics during the incident that broke it.
//
// Parameters:
//   - ctx context.Context: cancels both reads.
//   - store eventStatisticsStore: the repository surface. Must not be nil.
//
// Returns:
//   - *ProducerAtomicityCensus: the two censuses, or nil when either could not be read.
func producerAtomicityCensus(
	ctx context.Context,
	store eventStatisticsStore,
) *ProducerAtomicityCensus {
	handoffs, err := store.CountBalanceMonitorHandoffByStatus(ctx)
	if err != nil {
		withLoggableCause(nil, err).Warn(
			"the balance-monitor handoff census could not be read, so the statistics omit the " +
				"producer-atomicity figures rather than reporting zeros for them; an outstanding " +
				"handoff is a balance movement whose monitor conditions have not been judged, and " +
				"reporting zero would say the opposite",
		)

		return nil
	}

	batches, oldest, err := store.CountUnfinalizedBulkTransactionBatches(ctx, unfinalizedBulkBatchGrace)
	if err != nil {
		withLoggableCause(nil, err).Warn(
			"the unfinalized bulk-batch census could not be read, so the statistics omit the " +
				"producer-atomicity figures rather than reporting a partial answer",
		)

		return nil
	}

	// The four handoff states use the shared outbox status vocabulary rather than one of
	// their own — see model.BalanceMonitorHandoff.Status — so they are read through the
	// same constants the outbox counts are. A status with no rows is absent from the
	// aggregate and indexes to zero, which is the correct reading of "none in that state".
	census := &ProducerAtomicityCensus{
		MonitorHandoffPending:    handoffs[model.OutboxStatusPending],
		MonitorHandoffProcessing: handoffs[model.OutboxStatusProcessing],
		MonitorHandoffCompleted:  handoffs[model.OutboxStatusCompleted],
		MonitorHandoffFailed:     handoffs[model.OutboxStatusFailed],
		UnfinalizedBatches:       batches,
		OldestUnfinalizedBatchAt: oldest,
	}

	// LOGGED ONLY WHEN SOMETHING IS OWED, and at warn, because these two numbers are the
	// ones nothing else in the response can reveal: an unjudged monitor movement and an
	// abandoned batch are both events that will never exist without intervention.
	if census.MonitorHandoffFailed > 0 || census.UnfinalizedBatches > 0 {
		logrus.WithFields(logrus.Fields{
			"monitor_handoff_failed": census.MonitorHandoffFailed,
			"unfinalized_batches":    census.UnfinalizedBatches,
		}).Warn(
			"the event pipeline owes events that no outbox row represents: a failed monitor " +
				"handoff is a balance movement whose conditions were never judged, and an " +
				"unfinalized bulk batch never reported its outcome",
		)
	}

	return census
}

// errEventStatisticsDataSourceMissing is the cause recorded when statistics are asked
// for before a datasource exists. It is a start-up or programming fault rather than a
// caller error, so the cause is kept out of any response body.
var errEventStatisticsDataSourceMissing = errors.New(
	"blnk: event statistics require a datasource",
)

// eventStatisticsStore resolves the repository the statistics read goes through.
//
// It is guarded rather than dereferenced because NewBlnk(nil) is a supported
// construction in this codebase: the caller must receive a typed error it can render
// rather than a nil-pointer panic from inside a query.
//
// Returns:
//   - eventStatisticsStore: the repository, never nil when err is nil.
//   - error: a typed internal APIError when no datasource is available.
func (b *Blnk) eventStatisticsStore() (eventStatisticsStore, error) {
	unavailable := func() error {
		return apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The event outbox is unavailable because the service is not initialised",
			errEventStatisticsDataSourceMissing,
		)
	}

	if b == nil {
		return nil, unavailable()
	}

	datasource := b.GetDataSource()
	if datasource == nil {
		return nil, unavailable()
	}

	return datasource, nil
}

// unreportedEventOutboxStatuses names the statuses present in the aggregate that the
// state-machine enumeration does not know about, sorted.
//
// Driven from model.EventOutboxStatuses, the authoritative list, rather than from a
// list restated here — a second copy is how a state gets added to one and not the
// other.
//
// Parameters:
//   - counts map[string]int64: the per-status aggregate. May be nil.
//
// Returns:
//   - []string: the unknown statuses, sorted; nil when every status is known.
func unreportedEventOutboxStatuses(counts map[string]int64) []string {
	if len(counts) == 0 {
		return nil
	}

	known := model.EventOutboxStatuses()

	var unreported []string
	for status := range counts {
		if !slices.Contains(known, status) {
			unreported = append(unreported, status)
		}
	}
	if len(unreported) == 0 {
		return nil
	}

	// Map iteration order is unspecified, so the names are sorted to keep the warning —
	// and any test asserting on it — deterministic.
	slices.Sort(unreported)

	return unreported
}

// readEventTopicEndOffsets measures the broker side of the reconciliation.
//
// The admin client is built per call and closed before returning. That is the right
// trade for a rare, operator-triggered read and the wrong one in a loop: it costs one
// connection and one SASL handshake in exchange for this path owning no client
// lifecycle.
//
// Parameters:
//   - ctx context.Context: cancellation is inherited; the read is additionally bounded
//     by eventOffsetReadTimeout.
//   - since time.Time: the left-hand edge of the window the broker-side population is
//     bounded to.
//
// Returns:
//   - TopicOffsetReport: the per-topic detail and the sums.
//   - error: ErrKafkaAdminNotConfigured when no broker is configured, or the broker's
//     own wrapped error.
func (b *Blnk) readEventTopicEndOffsets(ctx context.Context, since time.Time) (TopicOffsetReport, error) {
	if b == nil {
		return TopicOffsetReport{}, ErrKafkaAdminNotConfigured
	}

	configuration := b.Config()
	if configuration == nil || !KafkaBrokersConfigured(configuration.Kafka.Brokers) {
		return TopicOffsetReport{}, ErrKafkaAdminNotConfigured
	}

	admin, err := NewKafkaAdmin(configuration)
	if err != nil {
		return TopicOffsetReport{}, err
	}
	defer func() {
		if closeErr := admin.Close(); closeErr != nil {
			withLoggableCause(nil, closeErr).Warn(
				"closing the Kafka admin client after reading the event topic end offsets failed",
			)
		}
	}()

	measurement, cancel := context.WithTimeout(ctx, eventOffsetReadTimeout)
	defer cancel()

	// No topic list is passed, so the full inventory is measured — see the note on
	// EventOutboxStatistics for why narrowing it would invalidate the verdict.
	return admin.TopicEndOffsets(measurement, since)
}

// ReconciliationBaseline is everything beyond the two counts that the verdict needs in
// order to be sound, gathered into one parameter.
type ReconciliationBaseline struct {
	// Purges is what retention has deleted from the outbox. Its Recorded field
	// distinguishes "nothing was purged" from "we cannot tell", and only the former
	// supports a conclusive verdict.
	Purges model.EventOutboxPurgeTotals

	// Coordinates is what the outbox claims about the broker, per partition. It is checked
	// against the report's live per-partition bounds, which is the step that turns the
	// reconciliation from an inference into a verification.
	Coordinates model.EventRecordCoordinateAudit
}

// TopicCatalogueReport is the EXISTENCE answer: which of the topics Blnk may write to
// the broker actually has.
//
// It is deliberately narrower than TopicAssuranceReport. That one is about GEOMETRY —
// how many partitions, what replication factor, what this run changed — and it is what
// an operator reads at boot.
type TopicCatalogueReport struct {
	// Expected is every topic Blnk may write to, across every owned prefix: each category
	// topic and each dead-letter sibling.
	Expected []string

	// Missing is the subset of Expected the broker does not have, in Expected's order.
	Missing []string

	// VerifiedAt is when the broker was read.
	VerifiedAt time.Time
}

// Complete reports whether the whole catalogue is present.
//
// Returns:
//   - bool: true when the broker has every expected topic.
func (r TopicCatalogueReport) Complete() bool {
	return len(r.Expected) > 0 && len(r.Missing) == 0
}

// VerifyTopicCatalogue reports which of the topics Blnk may write to are absent from
// the broker.
//
// Rows captured before a KAFKA_TOPIC_PREFIX rename name the previous generation's
// topics and the relay still publishes them, so those topics are ones it may need. See
// AllOwnedTopicsAcrossPrefixes.
//
// Parameters:
//   - ctx context.Context: bounds the metadata read.
//
// Returns:
//   - TopicCatalogueReport: populated on success. Expected is always set, even on
//     error, so a caller can report what it was looking for.
//   - error: ErrKafkaAdminNotConfigured when no broker is configured, or a wrapped
//     broker error.
func (a *KafkaAdminClient) VerifyTopicCatalogue(ctx context.Context) (TopicCatalogueReport, error) {
	report := TopicCatalogueReport{
		Expected:   AllOwnedTopicsAcrossPrefixes(),
		VerifiedAt: time.Now().UTC(),
	}

	if err := a.ready(ctx); err != nil {
		return report, err
	}

	present, err := a.topicPartitions(ctx, report.Expected)
	if err != nil {
		return report, err
	}

	report.Missing = missingTopics(report.Expected, present)
	report.VerifiedAt = time.Now().UTC()

	return report, nil
}

// TopicCatalogueGate answers whether the relay's destination topics exist, caching the
// answer once they do and rate-limiting how often it asks while they do not.
type TopicCatalogueGate struct {
	// verify performs one ensure-and-verify pass. It is a field so a test can drive the gate
	// without a broker; in production newCatalogueVerifier builds it from configuration.
	verify func(ctx context.Context) (TopicCatalogueReport, error)

	// now is the clock, replaceable in-package, following the relay's own now field.
	now func() time.Time

	mu       sync.Mutex
	verified bool

	// nextProbeAt is when the gate will next talk to the broker. Zero means "now".
	nextProbeAt time.Time

	// probeInterval is the current gap, grown on each failure up to the cap.
	probeInterval time.Duration
}

// NewTopicCatalogueGate builds the gate the server role hands to the relay.
//
// It performs NO I/O: the admin client is built per probe and closed again, because a
// probe happens at most every few seconds and holding an authenticated admin session
// open for the life of the process — for a check that stops running once it succeeds —
// would keep a SASL connection per broker for nothing.
//
// Parameters:
//   - cfg *config.Configuration: read for the broker list and the topic geometry.
//
// Returns:
//   - *TopicCatalogueGate: ready to be passed to WithCatalogueGate.
func NewTopicCatalogueGate(cfg *config.Configuration) *TopicCatalogueGate {
	return &TopicCatalogueGate{
		verify:        newCatalogueVerifier(cfg),
		now:           time.Now,
		probeInterval: defaultCatalogueProbeInterval,
	}
}

// newCatalogueVerifier returns the ensure-and-verify pass the gate runs.
//
// The order is verify-then-ensure-then-verify rather than ensure-then-verify, and that is what
// makes the gate affordable in a loop: the steady state costs ONE metadata read, and creation
// is attempted only when that read says something is actually absent.
func newCatalogueVerifier(cfg *config.Configuration) func(ctx context.Context) (TopicCatalogueReport, error) {
	return func(ctx context.Context) (TopicCatalogueReport, error) {
		admin, err := NewKafkaAdmin(cfg)
		if err != nil {
			return TopicCatalogueReport{Expected: AllOwnedTopicsAcrossPrefixes()}, err
		}
		defer func() {
			if closeErr := admin.Close(); closeErr != nil {
				withLoggableCause(nil, closeErr).Warn(
					"closing the Kafka admin client after a topic-catalogue probe failed",
				)
			}
		}()

		report, err := admin.VerifyTopicCatalogue(ctx)
		if err != nil || report.Complete() {
			return report, err
		}

		// Something is missing, so try to create it. The assurance error is deliberately NOT
		// returned in place of the verification: what the caller needs to know is whether the
		// catalogue is complete NOW, and the authority on that is the second verification
		// below. A failed assurance whose gap another process then filled must still open the
		// gate.
		if _, ensureErr := admin.EnsureTopics(ctx); ensureErr != nil {
			withLoggableCause(logrus.WithField("missing_topics", report.Missing), ensureErr).Warn(
				"the event topic catalogue is incomplete and assuring it failed; the relay will not " +
					"claim outbox rows until every destination topic exists, so nothing is published " +
					"and nothing spends its retry budget on a topic that cannot accept it",
			)
		}

		return admin.VerifyTopicCatalogue(ctx)
	}
}

// Ready reports whether the relay may claim work.
//
// It is safe for concurrent use and cheap once the catalogue has been verified: after
// the first success it takes a mutex and returns, with no broker contact ever again.
//
// Parameters:
//   - ctx context.Context: cancellation is respected; the probe is additionally bounded
//     by catalogueProbeTimeout.
//
// Returns:
//   - error: nil when every expected topic exists.
func (g *TopicCatalogueGate) Ready(ctx context.Context) error {
	if g == nil {
		return nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.verified {
		return nil
	}

	if g.verify == nil {
		return errors.New(
			"the event topic catalogue gate has no verifier, so the relay cannot confirm that its " +
				"destination topics exist",
		)
	}

	now := g.now()
	if !g.nextProbeAt.IsZero() && now.Before(g.nextProbeAt) {
		// Inside the backoff window. The condition was already reported by the probe that
		// opened the window, so this is silent — and it costs nothing, which is what lets the
		// relay consult the gate on every tick.
		return errCatalogueNotVerified
	}

	probe, cancel := context.WithTimeout(ctx, catalogueProbeTimeout)
	defer cancel()

	report, err := g.verify(probe)
	if err == nil && report.Complete() {
		g.verified = true
		logrus.WithField("topics", len(report.Expected)).Info(
			"every event topic and dead-letter sibling exists; the event outbox relay may claim rows",
		)

		return nil
	}

	g.backOff(now)

	if err != nil {
		withLoggableCause(logrus.WithFields(logrus.Fields{
			"next_probe_in": g.probeInterval.String(),
			"expected":      len(report.Expected),
		}), err).Warn(
			"could not verify that the event topic catalogue exists, so the event outbox relay is " +
				"NOT claiming rows. Nothing is lost — the rows stay pending and no attempt is spent " +
				"on a destination that may not accept it — but nothing is published either until " +
				"this succeeds",
		)

		return err
	}

	logrus.WithFields(logrus.Fields{
		"missing_topics": report.Missing,
		"expected":       len(report.Expected),
		"next_probe_in":  g.probeInterval.String(),
	}).Warn(
		"event topics are missing from the broker, so the event outbox relay is NOT claiming rows. " +
			"Publishing to a topic that does not exist would fail every attempt and then fail to " +
			"dead-letter for the same reason, spending each row's retry budget for nothing. Provision " +
			"the topics — `make kafka_provision`, or let the server's own assurance succeed — and the " +
			"relay resumes on its own",
	)

	return fmt.Errorf("%w: %s", errCatalogueIncomplete, strings.Join(report.Missing, ", "))
}

// Verified reports whether the gate has confirmed the catalogue. It exists for tests and for a
// caller that wants to log the gate's state without probing.
//
// Returns:
//   - bool: true once a probe has seen the whole catalogue.
func (g *TopicCatalogueGate) Verified() bool {
	if g == nil {
		return false
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	return g.verified
}

// backOff schedules the next probe, doubling the interval up to the cap.
//
// Called with the mutex held.
func (g *TopicCatalogueGate) backOff(now time.Time) {
	if g.probeInterval <= 0 {
		g.probeInterval = defaultCatalogueProbeInterval
	}

	g.nextProbeAt = now.Add(g.probeInterval)

	if next := g.probeInterval * 2; next <= maxCatalogueProbeInterval {
		g.probeInterval = next

		return
	}

	g.probeInterval = maxCatalogueProbeInterval
}

const (
	// defaultCatalogueProbeInterval is the shortest gap between two broker probes by the
	// topic-catalogue gate.
	defaultCatalogueProbeInterval = 5 * time.Second

	// maxCatalogueProbeInterval caps the gate's backoff.
	maxCatalogueProbeInterval = 60 * time.Second

	// catalogueProbeTimeout bounds one probe. A metadata read against a reachable broker
	// is milliseconds; this is generous enough for a loaded cluster and short enough that
	// a tick is never held up for long by an unreachable one.
	catalogueProbeTimeout = 10 * time.Second
)

var (
	// errCatalogueNotVerified is the silent refusal returned inside a backoff window. It
	// carries no detail because the detail was logged by the probe that opened the window.
	errCatalogueNotVerified = errors.New(
		"the event topic catalogue has not been verified yet, so the relay is not claiming rows",
	)

	// errCatalogueIncomplete reports that named topics are absent.
	errCatalogueIncomplete = errors.New("event topics are missing from the broker")
)

// CompensateProvisioning undoes the half-completed provisioning a deferred result
// describes.
//
// Parameters:
//   - ctx context.Context: used for its VALUES only; the round trips run on their own
//     bounded deadline, because a deferred compensation's caller is usually holding an
//     expired one.
//   - result SubscriberProvisioningResult: the deferred result, read for the principal
//     and the bindings it attempted.
//
// Returns:
//   - error: nil when nothing was owed or the credential is confirmed revoked; the
//     revocation's error otherwise.
func (a *KafkaAdminClient) CompensateProvisioning(
	ctx context.Context,
	result SubscriberProvisioningResult,
) error {
	if a == nil || !result.CompensationOwed || strings.TrimSpace(result.Principal) == "" {
		return nil
	}

	return a.compensateFailedProvisioning(ctx, result.Principal, result.OwedBindings)
}

// windowStartOffsets resolves, per partition, the earliest offset whose record was
// written at or after the given instant.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - partitions map[string][]int: the partitions to resolve, from topicPartitions.
//   - since time.Time: the window start.
//
// Returns:
//   - map[string]map[int]int64: the window-start offset per topic and partition, -1
//     where the partition holds nothing that recent, absent where it could not be read.
//   - error: a wrapped transport error.
func (a *KafkaAdminClient) windowStartOffsets(
	ctx context.Context,
	partitions map[string][]int,
	since time.Time,
) (map[string]map[int]int64, error) {
	request := &kafka.ListOffsetsRequest{
		Topics:         make(map[string][]kafka.OffsetRequest, len(partitions)),
		IsolationLevel: kafka.ReadUncommitted,
	}

	for topic, ids := range partitions {
		requests := make([]kafka.OffsetRequest, 0, len(ids))
		for _, id := range ids {
			requests = append(requests, kafka.TimeOffsetOf(id, since))
		}
		request.Topics[topic] = requests
	}

	response, err := a.client.ListOffsets(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("kafka admin: reading window-start offsets: %w", err)
	}

	starts := make(map[string]map[int]int64, len(response.Topics))
	for topic, offsets := range response.Topics {
		perPartition := make(map[int]int64, len(offsets))

		for _, offset := range offsets {
			if offset.Error != nil {
				logrus.WithFields(logrus.Fields{
					"topic":     topic,
					"partition": offset.Partition,
					"error":     sanitizeLogValue(offset.Error.Error(), maxLoggedErrorLength),
				}).Warn(
					"kafka admin: could not resolve the window-start offset for this partition; it is " +
						"excluded from the windowed reconciliation rather than counted as empty",
				)

				continue
			}

			// A timestamp request's answer arrives in the Offsets map rather than in FirstOffset
			// or LastOffset, which kafka-go reserves for the two sentinel timestamps. The map
			// holds one entry per timestamp asked about, and exactly one was asked about here;
			// the broker's "nothing that recent" answer is the offset -1, which is carried
			// through unchanged for the caller to interpret.
			resolved := int64(-1)
			for candidate := range offset.Offsets {
				resolved = candidate

				break
			}

			perPartition[offset.Partition] = resolved
		}

		starts[topic] = perPartition
	}

	return starts, nil
}

// windowStartOffsetFor reads one partition's window-start offset out of the result.
//
// AN ABSENT ENTRY IS UNREADABLE, NOT EMPTY — the same distinction offsetBoundsFor
// exists to preserve, and for the same reason: treating an unreadable partition as zero
// makes the broker side short and short looks exactly like loss.
//
// Parameters:
//   - starts map[string]map[int]int64: the result of windowStartOffsets, possibly nil.
//   - topic string, partition int: the partition to look up.
//
// Returns:
//   - int64: the window-start offset, or -1 when the partition holds nothing that
//     recent.
//   - bool: false when the partition could not be read.
func windowStartOffsetFor(starts map[string]map[int]int64, topic string, partition int) (int64, bool) {
	if perPartition, exists := starts[topic]; exists {
		if offset, ok := perPartition[partition]; ok {
			return offset, true
		}
	}

	return -1, false
}

// applyWindowToPartition folds one partition's windowed record count into its snapshot
// and its topic's totals.
//
// Parameters:
//   - partitionSnapshot *PartitionOffsetSnapshot: filled with the window-start offset.
//   - snapshot *TopicOffsetSnapshot: accumulates the topic's window figures.
//   - windowStarts map[string]map[int]int64: the window-start offsets read from the
//     broker.
//   - topic string, partition int: the partition being folded in.
//   - bound partitionOffsetBounds: its already-validated first and end offsets.
func applyWindowToPartition(
	partitionSnapshot *PartitionOffsetSnapshot,
	snapshot *TopicOffsetSnapshot,
	windowStarts map[string]map[int]int64,
	topic string,
	partition int,
	bound partitionOffsetBounds,
) {
	start, readable := windowStartOffsetFor(windowStarts, topic, partition)
	if !readable {
		partitionSnapshot.WindowUnreadable = true
		snapshot.WindowPartitionsUnreadable++

		return
	}

	partitionSnapshot.WindowStartOffset = start
	if start < 0 {
		// Every record predates the window: a real zero, not an absence.
		return
	}

	if start <= bound.first && bound.first > 0 {
		// The oldest record the partition still holds is already inside the window, so
		// retention may have removed earlier records that were inside it too.
		snapshot.WindowTruncated = true
	}

	if written := bound.end - start; written > 0 {
		snapshot.WindowRecordCount += written
	}
}
