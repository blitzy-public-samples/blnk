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
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/blnkfinance/blnk/config"
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
//
// # Everything here is native Go; nothing shells out
//
// kafka-go's Client exposes CreateTopics, CreatePartitions, CreateACLs, DescribeACLs,
// AlterUserScramCredentials, DescribeUserScramCredentials, Metadata, OffsetFetch and
// ListOffsets, and its sasl/scram sub-package supplies the client mechanism. The whole
// provisioning and measurement path is therefore reachable from Go, and application
// code MUST NOT invoke kafka-configs.sh, kafka-acls.sh or kafka-topics.sh. Shelling out
// from a server process would require the Kafka CLI (and a JVM) on every image, would
// lose typed error codes in favour of scraped stdout, and would make cancellation
// unimplementable.
//
// scripts/kafka-bootstrap.sh legitimately uses the CLI, and that is not a
// contradiction: it formats KRaft storage to seed the first SCRAM credential BEFORE any
// broker exists to talk to. There is no API to call at that point. Once a broker is
// running, this file is the only provisioning path.
//
// # Consumer lag is computed here rather than exported by a sidecar
//
// Lag is the difference between a consumer group's committed offsets (OffsetFetch) and
// the partition end offsets (ListOffsets). Computing it in-process is what makes the
// lag alert satisfiable with no external Kafka exporter in the deployment: the figure
// is published on Blnk's own /metrics endpoint through the gauge already declared in
// internal/metrics, and the alert rule reads it there.
//
// # What is deliberately NOT here
//
//   - No producing. Writers belong to the publisher, and the relay is the only thing
//     that writes event messages. An admin client that could also produce would make
//     it possible to bypass the outbox, which is the one thing the whole design exists
//     to prevent.
//   - No consuming, no consumer error handling, no subscriber-side dead-lettering.
//     Blnk publishes the `<topic>.dlt` naming convention (see event_topics.go) and
//     stops there; subscriber-side dead-lettering is explicitly the subscriber's own
//     concern.
//   - No SQL. The subscriber registry is persisted by database/event_subscriber.go.
//     This file talks to a broker and returns values; it never reads or writes a row.
//   - No topic deletion. DeleteTopics is reachable in the client library and is
//     deliberately not wrapped: no normal operation may destroy an event topic, and an
//     accidental call would discard undelivered events for every subscriber.
//   - No topic names. Every name comes from AllTopicsWithDeadLetters, which is the
//     single source of truth shared with scripts/kafka-provision.sh. A literal here
//     would ignore KAFKA_TOPIC_PREFIX and provision topics nothing publishes to.
//
// # Secret handling
//
// A subscriber's SASL password reaches this file as a parameter, is converted
// immediately into a salted PBKDF2 derivation, and is transmitted to the broker in
// that form only. It is never logged, never placed in an error message, never returned
// in a result and never persisted — mirroring the API-key posture, where the stored
// value is a bcrypt hash and the raw key is never kept. event_subscriber.go owns
// generation and the one-time return to the caller; database/event_subscriber.go
// stores only a non-reversible reference and the issuance instant.

// SubscriberSASLMechanism is the SASL mechanism every subscriber credential is
// provisioned with, and the value the credential endpoint reports to the subscriber.
//
// It is a constant rather than configuration on purpose. Kafka implements only
// SCRAM-SHA-256 and SCRAM-SHA-512, Blnk standardises on SHA-512, and
// scripts/kafka-bootstrap.sh fixes the same mechanism for the administrative principal
// it seeds. A configurable mechanism would let the broker's seeded credential and the
// provisioned subscriber credentials disagree, which fails at authentication time with
// an error that reads like a wrong password.
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

// MinTopicPartitions is the minimum number of partitions every event topic is created
// or grown to.
//
// Six is a requirement, not a preference: it is the floor the event-streaming design
// specifies so that a subscriber can scale its consumer group out to six members
// without any topic becoming the bottleneck. A configured KAFKA_MIN_PARTITIONS below
// this is raised to it, with a warning, because silently provisioning fewer partitions
// than required is invisible until a subscriber cannot scale.
const MinTopicPartitions = 6

// ACLHostAny is the ACL host pattern that matches every client host.
//
// Host-scoped ACLs are not used by default because subscribers consume from networks
// Blnk does not control and whose addresses change without notice; a host restriction
// there produces authorization failures that look exactly like a credential problem.
// Isolation comes from the principal, its topics and its consumer group, all of which
// are stable. A deployment that does control subscriber addressing may narrow the grant
// by setting SubscriberProvisioningRequest.Host.
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
//
// It is an upper bound, not a budget: kafka-go applies the shorter of this value and
// the caller's context deadline, so a credential issuance running under the 5-second
// provisioning budget is still bounded by that budget. The cap exists for callers that
// pass a context with no deadline at all, where an unreachable broker would otherwise
// hang the request indefinitely.
const kafkaAdminRequestTimeout = 10 * time.Second

// kafkaAdminDialTimeout bounds connection establishment, which includes the TCP
// handshake and the SASL/SCRAM negotiation's two round trips.
//
// It is deliberately well under the 5-second provisioning budget so that a dial to a
// broker that is not listening fails with time left to report a useful error, rather
// than consuming the whole budget and surfacing as a context deadline the caller cannot
// interpret.
const kafkaAdminDialTimeout = 3 * time.Second

// kafkaAdminIdleTimeout is how long an unused administrative connection is kept open.
// Administrative traffic is bursty and infrequent — a provisioning call, then nothing
// for hours — so connections are not worth holding much longer than a request cycle.
const kafkaAdminIdleTimeout = 30 * time.Second

// ErrKafkaAdminNotConfigured is returned by every administrative operation when no
// brokers are configured.
//
// An empty KAFKA_BROKERS is a legitimate steady state rather than a fault: it selects
// the no-op event publisher and reproduces the historic no-op-when-unconfigured
// contract, so a deployment with no Kafka keeps working. Construction therefore
// succeeds and performs no I/O, and it is the individual operations that fail — fast,
// with this error, and without dialling anything. Callers distinguish the case with
// errors.Is and can map it onto a service-unavailable response instead of guessing from
// a connection error.
var ErrKafkaAdminNotConfigured = errors.New(
	"kafka admin: no brokers are configured (KAFKA_BROKERS is empty), so administrative operations are unavailable",
)

// kafkaAdminAPI is the exact subset of kafka-go's Client that this file uses.
//
// It exists so the whole file is unit-testable with no broker: a fake implementation
// records the requests it is handed and returns canned responses, which is what lets
// tests assert the partition count, the replication factor, the ACL bindings and the
// SCRAM mechanism actually sent — the properties that have no runtime failure mode and
// so cannot be caught any other way. A wrong replication factor or a wildcard topic
// pattern is accepted happily by a broker; only an assertion on the outgoing request
// catches it.
//
// *kafka.Client satisfies this interface as declared, with no adapter.
//
// Adding a method here is how a new administrative capability arrives. Do NOT add
// DeleteTopics, DeleteACLs or any producing method: the first two make destructive
// operations reachable from server code, and the third would let event messages bypass
// the outbox.
type kafkaAdminAPI interface {
	CreateTopics(ctx context.Context, req *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error)
	CreatePartitions(ctx context.Context, req *kafka.CreatePartitionsRequest) (*kafka.CreatePartitionsResponse, error)
	CreateACLs(ctx context.Context, req *kafka.CreateACLsRequest) (*kafka.CreateACLsResponse, error)
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
// The subscriber service depends on this interface rather than on the concrete client so
// that credential issuance is testable without a broker, and so that a deployment with
// no Kafka can be handed an implementation whose methods fail fast.
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

	// AuthorizerActive reports whether the broker enforces ACLs at all.
	AuthorizerActive(ctx context.Context) (bool, error)

	// ConsumerLag measures how far a consumer group trails the end of the log.
	ConsumerLag(ctx context.Context, req ConsumerLagRequest) (ConsumerLagReport, error)

	// TopicEndOffsets reads the broker-side offsets the zero-loss reconciliation
	// compares outbox counts against.
	TopicEndOffsets(ctx context.Context, topics ...string) (TopicOffsetReport, error)

	// Close releases the client's pooled connections.
	Close() error
}

// KafkaAdminClient is the kafka-go-backed implementation of KafkaAdmin.
//
// It is constructed once per process and shared, exactly as the single HTTP client is:
// kafka-go's Client and Transport are safe for concurrent use and the Transport pools
// connections and caches cluster metadata, so a per-call client would re-dial and
// re-authenticate on every operation — several round trips of SASL negotiation to
// answer one question.
//
// All state is set at construction and never mutated afterwards, which is what makes
// sharing safe without a mutex. Topic NAMES are the one thing deliberately not
// captured: they are resolved per call from AllTopicsWithDeadLetters, so a
// configuration reload that renames the namespace is honoured without rebuilding the
// client, and there is exactly one place topics are named.
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

	// replicationFactor is the replication factor applied to newly created topics,
	// taken from configuration and never defaulted to a literal here. Zero means
	// unconfigured, which EnsureTopics reports as an actionable error.
	replicationFactor int
}

// Compile-time proof that the concrete client implements the published interface.
var _ KafkaAdmin = (*KafkaAdminClient)(nil)

// NewKafkaAdmin builds the shared administrative client from configuration.
//
// # It performs no I/O and cannot block
//
// Building a kafka-go Client is pure struct assembly: the Transport dials lazily on the
// first request. That property is load-bearing rather than incidental, because this
// constructor may be called from process start-up, where a broker that is slow or absent
// must not delay or fail the boot. A configuration with no brokers yields an
// UNCONFIGURED client — not an error and not a nil pointer — whose every operation
// returns ErrKafkaAdminNotConfigured without touching the network. That is what keeps a
// Kafka-less deployment working unchanged.
//
// # Authentication
//
// When KAFKA_SASL_ADMIN_USER is set, the transport authenticates with SCRAM-SHA-512
// built from it and KAFKA_SASL_ADMIN_SECRET. When it is unset, no SASL mechanism is
// attached, which is the correct behaviour for a PLAINTEXT broker: attaching one would
// fail the handshake against a listener that offers no mechanism. A user without a
// secret is the one genuinely broken combination and is the only case that returns an
// error, because it would otherwise surface later as an authentication failure that
// reads like a wrong password.
//
// The transport carries no TLS configuration, because none is configurable: the Kafka
// configuration surface has no TLS fields. SCRAM over SASL_PLAINTEXT is acceptable for
// the local single-broker stack; a production deployment terminates transport security
// at the broker's listener, and adding a client-side TLS option is a configuration
// change (a new field in KafkaConfig) rather than something to improvise here.
//
// # Geometry
//
// The partition count is raised to MinTopicPartitions when configuration asks for
// fewer, because six is a requirement. The replication factor is taken verbatim and is
// NEVER defaulted to a literal here: a single-broker cluster cannot satisfy 3 and topic
// creation fails outright, while hard-coding 1 would silently discard the durability
// requirement in production. An unset factor is reported at construction and rejected
// with an actionable message by EnsureTopics.
//
// Parameters:
//   - cnf *config.Configuration: the loaded configuration. A nil configuration is
//     treated as "no brokers", matching the unconfigured steady state.
//
// Returns:
//   - *KafkaAdminClient: a client that is never nil when err is nil, and which is safe
//     to share across goroutines.
//   - error: only when the SASL credentials are internally inconsistent or the SCRAM
//     mechanism cannot be constructed. Never for an absent broker list.
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

	transport, err := adminTransport(cnf.Kafka.SASLAdminUser, cnf.Kafka.SASLAdminSecret)
	if err != nil {
		return nil, err
	}

	admin := &KafkaAdminClient{
		client: &kafka.Client{
			Addr:      kafka.TCP(brokers...),
			Timeout:   kafkaAdminRequestTimeout,
			Transport: transport,
		},
		transport:         transport,
		brokers:           brokers,
		partitions:        resolveTopicPartitions(cnf.Kafka.MinPartitions),
		replicationFactor: cnf.Kafka.ReplicationFactor,
	}

	if admin.replicationFactor < 1 {
		// Reported here as well as rejected by EnsureTopics, so the misconfiguration is
		// visible at start-up rather than only when a topic is first provisioned.
		logrus.WithField("brokers", len(brokers)).Warn(
			"kafka admin: KAFKA_REPLICATION_FACTOR is not configured; topic creation will be refused until it is " +
				"set (3 for a replicated production cluster, 1 for a single-broker stack)",
		)
	}

	logrus.WithFields(logrus.Fields{
		"brokers":            len(brokers),
		"partitions":         admin.partitions,
		"replication_factor": admin.replicationFactor,
		"sasl":               transport.SASL != nil,
	}).Debug("kafka admin: client constructed")

	return admin, nil
}

// adminTransport builds the connection pool the administrative client sends through,
// including its SCRAM-SHA-512 authentication when SASL is configured.
//
// The mechanism is built here rather than returned to the caller so that the SASL
// interface type never has to be named outside kafka-go's own packages, keeping this
// file's kafka-go imports to the two the plan permits.
//
// SHA-512 is fixed rather than negotiated: it is the mechanism
// scripts/kafka-bootstrap.sh seeds the administrative principal with and the mechanism
// every subscriber credential is provisioned with, so a second choice here could only
// ever be a mismatch.
//
// Parameters:
//   - user string: KAFKA_SASL_ADMIN_USER. Empty selects no SASL at all, which is
//     correct for a PLAINTEXT listener.
//   - secret string: KAFKA_SASL_ADMIN_SECRET. Required when user is set.
//
// Returns:
//   - *kafka.Transport: never nil when err is nil.
//   - error: when a user is configured without a secret, or when the mechanism cannot
//     be constructed. Neither message contains the secret.
func adminTransport(user, secret string) (*kafka.Transport, error) {
	transport := &kafka.Transport{
		ClientID:    kafkaAdminClientID,
		DialTimeout: kafkaAdminDialTimeout,
		IdleTimeout: kafkaAdminIdleTimeout,
	}

	user = strings.TrimSpace(user)
	if user == "" {
		return transport, nil
	}

	if strings.TrimSpace(secret) == "" {
		return nil, errors.New(
			"kafka admin: KAFKA_SASL_ADMIN_USER is set but KAFKA_SASL_ADMIN_SECRET is empty; " +
				"SASL/SCRAM authentication needs both",
		)
	}

	mechanism, err := scram.Mechanism(scram.SHA512, user, secret)
	if err != nil {
		// The error from the SCRAM library can quote the credential it was given, so it
		// is deliberately NOT wrapped: only the fact of failure and the mechanism name
		// are reported.
		return nil, fmt.Errorf(
			"kafka admin: cannot build the %s mechanism for administrative user %q; "+
				"the username or secret contains characters SASL preparation rejects",
			SubscriberSASLMechanism, user,
		)
	}

	transport.SASL = mechanism

	return transport, nil
}

// normalizeKafkaBrokers trims each entry and drops blanks.
//
// The configuration loader already does this, but only on the validate-and-default path.
// A configuration published straight into the store — as tests do, and as any caller
// bypassing that path would — can still carry a stray empty element from a trailing
// comma in KAFKA_BROKERS. An empty element becomes the address ":9092" once kafka-go
// canonicalises it, which dials the local host and fails with an error that names no
// broker, so trimming here is what keeps the failure legible.
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
// Raising rather than rejecting is deliberate: six partitions is a requirement, and a
// deployment that asked for fewer still gets a correct topic. The warning is what stops
// the correction being silent, because a topic provisioned with too few partitions
// behaves perfectly until a subscriber tries to scale its consumer group past the
// partition count and discovers members sitting idle.
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

	if configured > 0 {
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
// Callers use it to choose a response rather than to guess from an error: the credential
// endpoint answers service-unavailable when Kafka is not configured, and the statistics
// endpoint omits broker-side offsets instead of failing the whole request.
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
// kafka-go has no Client.Close; the connection pool belongs to the Transport, so closing
// idle connections there is the whole of it. It is safe to call on an unconfigured
// client and safe to call more than once, because a shared client is closed on the way
// out of a process that may have several shutdown paths.
//
// Returns:
//   - error: always nil. The signature carries one so that KafkaAdmin can be closed
//     uniformly with the other resources a process shuts down, and so a future
//     implementation with a real failure mode does not change the interface.
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
// failure surface: a topic with one partition works, accepts messages and loses only the
// ordering guarantee, so an operator needs to be able to assert the geometry rather than
// infer it from the absence of an error.
type TopicAssurance struct {
	// Topic is the fully-qualified topic name, as resolved by event_topics.go.
	Topic string `json:"topic"`

	// Created is true when this run created the topic.
	Created bool `json:"created"`

	// PartitionsBefore is the partition count found before this run, and 0 for a topic
	// this run created.
	PartitionsBefore int `json:"partitions_before"`

	// PartitionsAfter is the partition count in force after this run.
	PartitionsAfter int `json:"partitions_after"`

	// PartitionsAdded is true when this run grew an existing topic.
	PartitionsAdded bool `json:"partitions_added"`

	// ShrinkRefused is true when the topic has MORE partitions than configured and was
	// deliberately left alone. See EnsureTopics for why shrinking is never attempted.
	ShrinkRefused bool `json:"shrink_refused"`

	// ReplicationFactor is the factor the topic was created with, and 0 for a topic
	// that already existed — its factor is a property of the existing topic and is not
	// altered by this operation.
	ReplicationFactor int `json:"replication_factor,omitempty"`
}

// TopicAssuranceReport is the outcome of one EnsureTopics call across every topic Blnk
// owns.
type TopicAssuranceReport struct {
	// Topics carries one entry per topic, in the canonical order
	// AllTopicsWithDeadLetters returns: the four category topics, then their four
	// dead-letter siblings. The stable order is what lets the report be diffed against
	// the provisioning script line for line.
	Topics []TopicAssurance `json:"topics"`

	// Partitions is the partition count applied, after the MinTopicPartitions floor.
	Partitions int `json:"partitions"`

	// ReplicationFactor is the factor applied to newly created topics, straight from
	// configuration.
	ReplicationFactor int `json:"replication_factor"`

	// CreatedCount, GrownCount, UnchangedCount and ShrinkRefusedCount summarise
	// Topics. They are computed here rather than left to the caller so that a log line
	// or an operator response does not have to re-derive them.
	CreatedCount       int `json:"created_count"`
	GrownCount         int `json:"grown_count"`
	UnchangedCount     int `json:"unchanged_count"`
	ShrinkRefusedCount int `json:"shrink_refused_count"`

	// CompletedAt is when the assurance finished.
	CompletedAt time.Time `json:"completed_at"`
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
//
// It guards two things at once: a fat-fingered KAFKA_MIN_PARTITIONS (a pasted number
// rather than a small integer) that a broker would spend a long time rejecting, and the
// int32 conversion the CreateTopics and CreatePartitions requests require — an unbounded
// int could wrap to a negative partition count, which Kafka reads as "unset" and answers
// with the broker default. Ten thousand partitions on one topic is already far beyond
// any sane event-streaming layout.
const maxTopicPartitions = 10000

// topicCreationOutcome records which topics a create pass actually created and which
// ones turned out to exist already.
type topicCreationOutcome struct {
	// created holds the topics this process created.
	created map[string]bool

	// raced holds topics that the metadata probe reported as absent but that the
	// broker answered TOPIC_ALREADY_EXISTS for. Another provisioner — a second server
	// instance, or scripts/kafka-provision.sh — created them in between. Their
	// partition count is unknown at that point and has to be re-probed before the grow
	// pass can decide anything.
	raced []string
}

// EnsureTopics creates every topic Blnk publishes to and grows any that exist with too
// few partitions.
//
// # Idempotent by design, because it runs on every start-up
//
// The operation is safe to run repeatedly and concurrently with itself. A topic that
// already exists is not an error: it is reported, logged at info, and then checked for
// partition count. A topic that appears between the metadata probe and the create — the
// TOPIC_ALREADY_EXISTS race, which two server instances starting together will hit — is
// re-probed and treated as pre-existing. That is what allows this to be wired into
// start-up unconditionally instead of behind a "first deploy only" flag nobody would
// remember to unset.
//
// # Geometry, and why the replication factor is never a literal
//
// The partition count is config.Kafka.MinPartitions raised to MinTopicPartitions. The
// replication factor is config.Kafka.ReplicationFactor, used verbatim: a single-broker
// KRaft cluster CANNOT satisfy a factor of 3 and rejects the creation outright with
// INVALID_REPLICATION_FACTOR, so a hard-coded 3 would make local bring-up impossible,
// while a hard-coded 1 would silently discard the durability requirement in production.
// An unconfigured factor is refused with an actionable message rather than guessed at.
//
// # Growing is allowed; shrinking is refused, never attempted
//
// A topic with too few partitions is grown with CreatePartitions, because a topic
// provisioned by hand or auto-created with one partition would otherwise cap how far a
// subscriber can scale.
//
// A topic with MORE partitions than configured is LEFT ALONE and reported. Kafka cannot
// reduce a partition count at all, so attempting it can only fail; but the deeper reason
// is that shrinking would be wrong even if it were possible. Per-aggregate ordering
// depends on a stable key-to-partition mapping, and the partition a key hashes to is a
// function of the partition count — change the count downwards and events for one ledger
// would start landing on a different partition from their predecessors, breaking the
// ordering guarantee for every key. Growing has the same hazard in principle, which is
// why the floor exists at all and why the configured count should be set once and left
// alone.
//
// Parameters:
//   - ctx context.Context: cancelled or expired before any round trip is attempted.
//
// Returns:
//   - TopicAssuranceReport: populated even when an error is returned, so a caller can
//     see how far the assurance got.
//   - error: ErrKafkaAdminNotConfigured when no broker is configured, an actionable
//     error when the replication factor is unconfigured, or a wrapped broker error.
func (a *KafkaAdminClient) EnsureTopics(ctx context.Context) (TopicAssuranceReport, error) {
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
	// operation and scripts/kafka-provision.sh provision exactly the same eight topics
	// under whatever KAFKA_TOPIC_PREFIX is configured.
	desired := AllTopicsWithDeadLetters()

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

	plan := a.planAssurance(desired, partitionsBefore, outcome)
	report.Topics = plan.assurances
	report.CreatedCount = plan.created
	report.UnchangedCount = plan.unchanged
	report.ShrinkRefusedCount = plan.shrinkRefused

	if len(plan.grow) > 0 {
		// The plan describes the topics as they are, so a failure here returns a report
		// that still says "one partition", not one that claims a growth that did not
		// happen. The entries are only updated once the broker has confirmed it.
		if growErr := a.growTopics(ctx, plan.grow); growErr != nil {
			return report, growErr
		}

		a.markPartitionsGrown(report.Topics, plan.grow)
		report.GrownCount = len(plan.grow)
	}

	report.CompletedAt = time.Now().UTC()

	logrus.WithFields(logrus.Fields{
		"topics":             len(report.Topics),
		"created":            report.CreatedCount,
		"grown":              report.GrownCount,
		"unchanged":          report.UnchangedCount,
		"shrink_refused":     report.ShrinkRefusedCount,
		"partitions":         report.Partitions,
		"replication_factor": report.ReplicationFactor,
	}).Info("kafka admin: event topics assured")

	return report, nil
}

// topicAssurancePlan is what planAssurance decided: the per-topic report entries as the
// topics stand right now, plus the list of topics that still need growing.
//
// The two are kept apart deliberately. The entries describe observed reality, so they can
// be returned alongside an error without ever claiming a change the broker did not make;
// the grow list is an intention, and it becomes reality in the entries only after
// CreatePartitions has confirmed it.
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
			// Created by this run, or by a concurrent provisioner whose partitions the
			// re-probe could not yet see. Either way the topic now exists and its
			// geometry was decided by whoever created it.
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
//   - error: a wrapped transport error, or a wrapped per-topic error. A topic another
//     provisioner grew first answers INVALID_PARTITIONS and is treated as success,
//     because the desired end state has been reached either way.
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
// Naming the topics explicitly is safe with respect to auto-creation: kafka-go never
// sets the metadata request's AllowAutoTopicCreation flag, so an unknown topic is
// reported as unknown rather than being created behind the caller's back with the
// broker's default single partition and default replication factor. That distinction
// matters — an accidentally auto-created topic would satisfy the existence check while
// silently having the wrong geometry.
//
// Parameters:
//   - ctx context.Context
//   - topics []string: the topics to probe.
//
// Returns:
//   - map[string][]int: ascending partition IDs per EXISTING topic. An absent key means
//     the topic does not exist, or exists but has no visible partitions yet. IDs are
//     sorted because the broker returns them in arbitrary order and every report built
//     from them has to be deterministic.
//   - error: a wrapped transport error, or a per-topic error that is not simply
//     "unknown topic" or "leader not available" — a topic authorization failure, for
//     instance, which means the administrative principal lacks the grant it needs and
//     must be reported rather than mistaken for an absent topic.
func (a *KafkaAdminClient) topicPartitions(ctx context.Context, topics []string) (map[string][]int, error) {
	response, err := a.client.Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		return nil, fmt.Errorf("kafka admin: reading topic metadata: %w", err)
	}

	partitions := make(map[string][]int, len(response.Topics))
	for _, topic := range response.Topics {
		if len(topic.Partitions) > 0 {
			ids := make([]int, 0, len(topic.Partitions))
			for _, partition := range topic.Partitions {
				ids = append(ids, partition.ID)
			}
			sort.Ints(ids)
			partitions[topic.Name] = ids

			continue
		}

		switch {
		case topic.Error == nil,
			errors.Is(topic.Error, kafka.UnknownTopicOrPartition),
			errors.Is(topic.Error, kafka.LeaderNotAvailable):
			// Absent, or newly created and not yet assigned a leader. Both are treated
			// as "not there yet"; the create pass turns the first into an existing topic
			// and the second into a harmless TOPIC_ALREADY_EXISTS.
			logrus.WithField("topic", topic.Name).Debug("kafka admin: topic not present yet")

		default:
			return nil, fmt.Errorf("kafka admin: reading metadata for topic %q: %w", topic.Name, topic.Error)
		}
	}

	return partitions, nil
}

// partitionCounts is topicPartitions reduced to a count per topic, which is all the
// topic-assurance path needs.
//
// Parameters:
//   - ctx context.Context
//   - topics []string: the topics to probe.
//
// Returns:
//   - map[string]int: partition count per existing topic.
//   - error: as topicPartitions.
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

// missingTopics lists the requested topics the broker does not have.
//
// They are reported rather than treated as an error, because a topic that does not exist
// contributes nothing to lag or to an offset sum and one mistyped name should not void a
// whole measurement. Reporting them is what stops the absence being invisible: a
// reconciliation or a lag alert built on a topic that silently does not exist would read
// as a permanently healthy zero.
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
// Order is preserved rather than sorted because it is meaningful: the topic inventory has
// a canonical order, and a subscriber's authorised topics are recorded in a deliberate
// one. Every report in this file is built by walking the normalised list, so the caller's
// order is the report's order.
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

// SubscriberProvisioningRequest is everything needed to turn a registry row into a real
// Kafka access boundary: one SCRAM credential plus a set of ACL bindings.
//
// SECURITY: Password is the only secret this package ever handles. It is converted into
// a salted PBKDF2 derivation and sent to the broker in that form; it is never logged,
// never returned, never placed in an error message and never persisted. Do not add a
// field that would carry it anywhere else, and do not log this struct as a whole — log
// the individual non-secret fields, as ProvisionSubscriberPrincipal does.
type SubscriberProvisioningRequest struct {
	// SubscriberID is the registry business key, carried for log correlation only. It
	// takes no part in the credential or the ACLs.
	SubscriberID string

	// Principal is the SASL/SCRAM username, which is the subscriber's Kafka principal
	// (model.EventSubscriber.KafkaPrincipal). Required.
	Principal string

	// Password is the generated secret. Required, and restricted to printable ASCII —
	// see validateSCRAMPassword for why that restriction is a correctness requirement
	// rather than a policy preference.
	Password string

	// ConsumerGroupPrefix is the consumer group namespace the subscriber is granted
	// Read on, bound with a PREFIXED pattern so every group whose name starts with it
	// is covered. Normally the subscriber's consumer group ID. When empty, no group
	// binding is created and the subscriber cannot join a group at all, which is
	// reported as a warning.
	ConsumerGroupPrefix string

	// Topics is the exact set of topics the credential may read
	// (model.EventSubscriber.AuthorizedTopics). Each is bound with a LITERAL pattern.
	// An empty set is legitimate and means the principal can read nothing — the
	// fail-closed default of a freshly registered subscriber — and is reported as a
	// warning rather than silently widened.
	Topics []string

	// Iterations is the PBKDF2 iteration count. Zero selects DefaultScramIterations,
	// and any value below MinScramIterations is raised to it because the broker would
	// otherwise reject the credential outright.
	Iterations int

	// Host restricts the binding to one client host. Empty selects ACLHostAny, which is
	// the right default for subscribers on networks Blnk does not control.
	Host string
}

// NewSubscriberProvisioningRequest maps a registry row and a freshly generated password
// onto a provisioning request.
//
// It exists so the mapping from registry columns to access boundary is written once. The
// three fields that constitute the boundary — the principal, the consumer group and the
// authorised topics — must all come from the same row, and a caller assembling the
// request by hand could quietly pair one subscriber's principal with another's topics.
//
// PartitionKeyPrefix is deliberately NOT mapped, and cannot be: Kafka's authorizer has
// no message-key dimension. There is no ACL that restricts a consumer to a slice of a
// topic by key, so the prefix is application-level metadata that the registry records
// and the credential response reports for the subscriber's own filtering. Pretending to
// enforce it here — by, say, binding a prefixed TOPIC pattern instead of a literal one —
// would be strictly worse than not enforcing it: it would widen the topic grant to every
// topic sharing that prefix while appearing to narrow it.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the registry row. A nil row yields a request
//     that fails validation, which is the correct outcome for a caller that did not load
//     one.
//   - password string: the generated secret, owned and returned once by the subscriber
//     service.
//
// Returns:
//   - SubscriberProvisioningRequest: ready to pass to ProvisionSubscriberPrincipal.
func NewSubscriberProvisioningRequest(subscriber *model.EventSubscriber, password string) SubscriberProvisioningRequest {
	if subscriber == nil {
		return SubscriberProvisioningRequest{Password: password}
	}

	return SubscriberProvisioningRequest{
		SubscriberID:        subscriber.SubscriberID,
		Principal:           subscriber.KafkaPrincipal,
		Password:            password,
		ConsumerGroupPrefix: subscriber.ConsumerGroupID,
		Topics:              subscriber.AuthorizedTopics,
	}
}

// SubscriberProvisioningResult reports what was provisioned, for the caller's log line,
// response and tests.
//
// It carries NO secret, by construction. The password is not a field here and must never
// become one: this value is logged and may be serialised, and the credential itself is
// returned to the subscriber exactly once by the endpoint that generated it.
type SubscriberProvisioningResult struct {
	// SubscriberID echoes the request, for correlation.
	SubscriberID string `json:"subscriber_id,omitempty"`

	// Principal is the principal the credential belongs to.
	Principal string `json:"principal"`

	// Mechanism is always SubscriberSASLMechanism.
	Mechanism string `json:"mechanism"`

	// Iterations is the PBKDF2 iteration count actually used, after the minimum was
	// applied.
	Iterations int `json:"iterations"`

	// Topics is the normalised topic set the credential was granted Read and Describe
	// on.
	Topics []string `json:"topics"`

	// ConsumerGroupPrefix is the group namespace granted Read, empty when none was
	// requested.
	ConsumerGroupPrefix string `json:"consumer_group_prefix,omitempty"`

	// ACLBindings is how many bindings were created.
	ACLBindings int `json:"acl_bindings"`

	// CredentialReplaced is true when the principal already held a SCRAM credential and
	// this call replaced it. Re-issuing is a supported operation, not an error, and this
	// flag is how an operator sees that an existing consumer's credential just stopped
	// working.
	CredentialReplaced bool `json:"credential_replaced"`

	// AuthorizerActive reports whether the broker appears to enforce ACLs. False means
	// the bindings were accepted and will not be enforced — see AuthorizerActive.
	AuthorizerActive bool `json:"authorizer_active"`

	// ProvisionedAt is when provisioning completed.
	ProvisionedAt time.Time `json:"provisioned_at"`
}

// ProvisionSubscriberPrincipal creates or replaces a subscriber's SCRAM credential and
// binds the ACLs that are its access boundary.
//
// # The two steps, in this order
//
//  1. AlterUserScramCredentials upserts a SCRAM-SHA-512 credential derived from the
//     supplied password with at least MinScramIterations iterations. An upsert makes
//     re-issuing well defined: a principal that already holds a credential gets a new
//     one, reported through CredentialReplaced, instead of an error.
//  2. CreateACLs binds Read and Describe on each authorised topic with a LITERAL
//     pattern, and Read on the consumer group namespace with a PREFIXED pattern. Kafka's
//     CreateAcls is idempotent, so re-provisioning the same boundary is a no-op.
//
// # Least privilege is the point, and it is tested
//
// The grant is exactly what a consumer needs and nothing more. There is deliberately NO
// cluster-wide Describe, NO wildcard or prefixed TOPIC pattern, and NO Write, Create,
// Delete or Alter of any kind. A subscriber therefore cannot read, list or describe a
// topic, a partition or a dead-letter topic outside its own grant, which is precisely
// what the subscriber-isolation acceptance criterion asserts.
//
// The prefixed pattern on the GROUP resource is the one intentional widening, and it
// widens only within the subscriber's own namespace: consumer groups are created by
// clients at will, so reserving "<group>*" lets a subscriber run several groups (a
// replay group beside its live one, say) without an administrative round trip, while
// still excluding every other subscriber's namespace.
//
// # ⚠️ ACLs are only ENFORCED when the broker runs an authorizer
//
// In KRaft mode a broker enforces ACLs only when it is started with
// authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer.
// WITHOUT IT, CreateACLs SUCCEEDS AND THE BINDINGS ARE NEVER APPLIED: every request
// from every principal is allowed, the bindings are visible in kafka-acls output, and an
// isolation test would pass while proving nothing at all. This method probes for the
// authorizer and logs a prominent warning when the probe says it is absent, so a
// misconfigured broker is loud rather than silent, and it reports the finding through
// SubscriberProvisioningResult.AuthorizerActive so a test or an operator can assert it.
//
// # Secret handling
//
// The password is used to derive a salted PBKDF2 value and is otherwise untouched. No
// log line, error message, result field or return value contains it. It is not persisted
// here and cannot be: this file executes no SQL.
//
// Parameters:
//   - ctx context.Context: honoured before every round trip. Credential issuance runs
//     under a 5-second budget, and each of the four possible round trips respects the
//     remaining time.
//   - req SubscriberProvisioningRequest: the boundary to provision.
//
// Returns:
//   - SubscriberProvisioningResult: partially populated when an error is returned, so a
//     caller can tell whether the credential was written before the bindings failed.
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
//     No error message contains the password.
func (a *KafkaAdminClient) ProvisionSubscriberPrincipal(
	ctx context.Context,
	req SubscriberProvisioningRequest,
) (SubscriberProvisioningResult, error) {
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

	// Informational only: the upsert below works whether or not a credential exists, so a
	// probe failure that is not a cancellation must not stop provisioning.
	existed, err := a.SubscriberCredentialExists(ctx, principal)
	switch {
	case err == nil:
		result.CredentialReplaced = existed
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return result, err
	default:
		logrus.WithError(err).WithField("principal", principal).Debug(
			"kafka admin: could not determine whether the principal already holds a credential; provisioning anyway",
		)
	}

	upsertion, err := deriveScramUpsertion(principal, req.Password, iterations)
	if err != nil {
		return result, err
	}

	if err := a.upsertScramCredential(ctx, upsertion); err != nil {
		return result, err
	}

	// Probed after the credential is written and before the bindings, so the warning is
	// emitted even if the binding call then fails.
	result.AuthorizerActive = a.warnIfAuthorizerInactive(ctx, principal)

	bindings := req.aclEntries()
	if err := a.createACLBindings(ctx, principal, bindings); err != nil {
		return result, err
	}
	result.ACLBindings = len(bindings)

	if len(topics) == 0 {
		logrus.WithFields(logrus.Fields{
			"principal":  principal,
			"subscriber": result.SubscriberID,
		}).Warn(
			"kafka admin: principal provisioned with no authorised topics, so it can read nothing. This is the " +
				"fail-closed default of a newly registered subscriber; grant topics on the subscriber before " +
				"issuing credentials it is expected to consume with",
		)
	}

	if groupPrefix == "" {
		logrus.WithFields(logrus.Fields{
			"principal":  principal,
			"subscriber": result.SubscriberID,
		}).Warn(
			"kafka admin: principal provisioned without a consumer group grant, so it cannot join a consumer " +
				"group; set the subscriber's consumer group before issuing credentials",
		)
	}

	result.ProvisionedAt = time.Now().UTC()

	logrus.WithFields(logrus.Fields{
		"subscriber":            result.SubscriberID,
		"principal":             principal,
		"mechanism":             SubscriberSASLMechanism,
		"iterations":            iterations,
		"topics":                len(topics),
		"consumer_group_prefix": groupPrefix,
		"acl_bindings":          result.ACLBindings,
		"credential_replaced":   result.CredentialReplaced,
		"authorizer_active":     result.AuthorizerActive,
	}).Info("kafka admin: subscriber principal provisioned")

	return result, nil
}

// validate rejects a request that cannot produce a usable credential.
//
// No message quotes the password, not even to say what is wrong with it beyond the rule
// it broke.
//
// Returns:
//   - error: nil when the request can be provisioned.
func (r SubscriberProvisioningRequest) validate() error {
	if r.principal() == "" {
		return errors.New(
			"kafka admin: a Kafka principal is required to provision a subscriber credential; " +
				"the subscriber's kafka_principal is empty",
		)
	}

	return validateSCRAMPassword(r.Password)
}

// principal returns the trimmed SASL username.
func (r SubscriberProvisioningRequest) principal() string {
	return strings.TrimSpace(r.Principal)
}

// consumerGroupPrefix returns the trimmed group namespace, empty when none was given.
func (r SubscriberProvisioningRequest) consumerGroupPrefix() string {
	return strings.TrimSpace(r.ConsumerGroupPrefix)
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
// would inflate the binding count a reviewer or a test reads, and they make the log line
// misreport the size of the grant.
//
// Returns:
//   - []string: a fresh slice, nil when nothing usable remains.
func (r SubscriberProvisioningRequest) normalizedTopics() []string {
	return normalizeTopicList(r.Topics)
}

// aclEntries builds the exact bindings that constitute the subscriber's access boundary.
//
// It is a pure function of the request so that the bindings can be asserted directly,
// without a broker: an ACL mistake has no runtime symptom on the Blnk side — the binding
// is accepted, and the consequence is either a subscriber that cannot read or, far worse,
// one that can read somebody else's topic.
//
// The bindings, and why each is what it is:
//
//	Topic  <each authorised topic>  LITERAL   Read      Allow  — consume the topic
//	Topic  <each authorised topic>  LITERAL   Describe  Allow  — see its partitions
//	Group  <consumer group prefix>  PREFIXED  Read      Allow  — join and commit
//
// Kafka's own implication rules make Read imply Describe on the same resource, so the
// topic Describe binding is technically redundant, and the group Read binding already
// implies the group Describe that FindCoordinator and OffsetFetch require. Describe is
// nonetheless requested explicitly on topics so that the grant is auditable from the
// binding list alone, without the reader having to know the implication table. The
// redundancy is deliberate and costs one binding per topic.
//
// Returns:
//   - []kafka.ACLEntry: the bindings, in a deterministic order — all bindings for the
//     first topic, then the second, and the group binding last. Nil only when there is
//     nothing at all to grant.
func (r SubscriberProvisioningRequest) aclEntries() []kafka.ACLEntry {
	principal := kafkaPrincipalPrefix + r.principal()
	host := r.host()
	topics := r.normalizedTopics()
	groupPrefix := r.consumerGroupPrefix()

	entries := make([]kafka.ACLEntry, 0, len(topics)*2+1)

	for _, topic := range topics {
		for _, operation := range []kafka.ACLOperationType{
			kafka.ACLOperationTypeRead,
			kafka.ACLOperationTypeDescribe,
		} {
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
// AlterUserScramCredentials. For printable ASCII the two are identical, because SASLprep
// leaves that range untouched. Outside it they can differ — a non-ASCII space is mapped,
// unassigned code points are rejected outright — and the result is a credential that
// authenticates for nobody, failing with a message indistinguishable from a wrong
// password. Restricting the input is how that class of bug is made impossible rather
// than merely unlikely; the subscriber service's generator produces exactly this
// alphabet.
//
// Space (0x20) is excluded along with the control characters: it is legal in SASLprep but
// survives round trips through shells, environment files and connection strings badly
// enough that it has no place in a generated secret.
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

	return nil
}

// deriveScramUpsertion turns a plaintext password into the salted form the broker stores.
//
// Kafka's AlterUserScramCredentials API takes a SALT and a SALTED PASSWORD, never a
// plaintext one, so the derivation happens here: PBKDF2-HMAC-SHA-512 over the password
// and a fresh 32-byte random salt, for the requested iteration count, producing a
// SHA-512-sized key. That is exactly Kafka's own ScramFormatter computation, which is
// what makes a credential minted here interchangeable with one created by
// kafka-configs.sh or seeded by scripts/kafka-bootstrap.sh.
//
// Deriving client-side rather than sending a plaintext password is also strictly better
// for secret handling: the password itself never crosses the wire, and the broker stores
// only what it needs to verify a SCRAM proof.
//
// Parameters:
//   - principal string: the SASL username, used only in error messages.
//   - password string: the secret. Consumed here and never retained.
//   - iterations int: already normalised to at least MinScramIterations.
//
// Returns:
//   - kafka.UserScramCredentialsUpsertion: ready to send.
//   - error: when the salt cannot be generated or the derivation rejects its parameters.
//     Neither case can contain the password.
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
//   - error: a wrapped transport error, or the first per-binding failure. Kafka's
//     CreateAcls is idempotent, so an already-present binding is not an error.
func (a *KafkaAdminClient) createACLBindings(ctx context.Context, principal string, bindings []kafka.ACLEntry) error {
	if len(bindings) == 0 {
		return nil
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

// SubscriberCredentialExists reports whether a principal already holds a SCRAM-SHA-512
// credential.
//
// It is what makes re-issuing well defined. Provisioning upserts, so a second issuance
// is legitimate — the subscriber lost its secret, or is rotating it — and this check lets
// the caller say so explicitly instead of the operation either failing or silently
// invalidating a working consumer's credential without comment.
//
// A principal holding only a SHA-256 credential answers false: a SHA-256 credential
// cannot authenticate the SHA-512 mechanism Blnk standardises on, so for Blnk's purposes
// no credential exists.
//
// Parameters:
//   - ctx context.Context
//   - principal string: the SASL username.
//
// Returns:
//   - bool: true when a SHA-512 credential is present.
//   - error: ErrKafkaAdminNotConfigured, a validation error for a blank principal, or a
//     wrapped broker error. An unknown principal is NOT an error; it is false.
func (a *KafkaAdminClient) SubscriberCredentialExists(ctx context.Context, principal string) (bool, error) {
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
// # Why this exists
//
// A KRaft broker started WITHOUT
// authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer accepts
// every ACL binding and applies none of them. Provisioning appears to succeed, the
// bindings are listable, and every principal can read everything. An isolation test run
// against such a broker passes without proving anything, which is the worst possible
// outcome: a security property that reports itself satisfied while absent.
//
// The probe is a DescribeACLs call with a match-anything filter. A broker with no
// authorizer answers SECURITY_DISABLED, which is the unambiguous signal; any other
// successful answer means an authorizer is present and enforcing.
//
// The probe requires the administrative principal to be allowed to describe ACLs. A
// permission failure is therefore returned as an error rather than as "inactive" — the
// distinction matters, because "I am not allowed to ask" is not the same as "nobody is
// being checked".
//
// Parameters:
//   - ctx context.Context
//
// Returns:
//   - bool: true when the broker enforces ACLs, false when it explicitly reports
//     security disabled.
//   - error: ErrKafkaAdminNotConfigured, or a wrapped broker error when the question
//     could not be answered.
func (a *KafkaAdminClient) AuthorizerActive(ctx context.Context) (bool, error) {
	if err := a.ready(ctx); err != nil {
		return false, err
	}

	// Every filter field left at its zero value matches anything: the name, principal
	// and host filters are nullable on the wire, and Any matches every pattern type,
	// operation and permission.
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

// warnIfAuthorizerInactive probes the authorizer and shouts if it is missing.
//
// It never fails provisioning. A broker that will not answer the question is a reason to
// log, not a reason to refuse a credential the operator asked for — and refusing would
// make Blnk unusable against a broker whose administrative principal simply lacks the
// DescribeACLs grant.
//
// Parameters:
//   - ctx context.Context
//   - principal string: included in the log line for correlation.
//
// Returns:
//   - bool: true only when the broker was asked and answered that it enforces ACLs.
func (a *KafkaAdminClient) warnIfAuthorizerInactive(ctx context.Context, principal string) bool {
	active, err := a.AuthorizerActive(ctx)
	if err != nil {
		logrus.WithError(err).WithField("principal", principal).Warn(
			"kafka admin: could not confirm that the broker enforces ACLs, so the isolation guarantee for this " +
				"principal is unverified. Confirm the broker runs " +
				"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer and that the " +
				"administrative principal may describe ACLs",
		)

		return false
	}

	if !active {
		logrus.WithField("principal", principal).Error(
			"kafka admin: THE BROKER HAS NO AUTHORIZER CONFIGURED. The ACLs just created were accepted and will " +
				"NOT be enforced: every principal can read every topic, including other subscribers' topics and " +
				"the dead-letter topics. Start the broker with " +
				"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
		)
	}

	return active
}

// ConsumerLagRequest identifies the consumer group whose lag is to be measured.
type ConsumerLagRequest struct {
	// SubscriberID labels the measurement. It is used for the metric attribute and the
	// log line and takes no part in the computation, so a bare operational query may
	// leave it empty.
	SubscriberID string

	// GroupID is the consumer group to read committed offsets for. Required.
	GroupID string

	// Topics are the topics to measure. Normally the subscriber's authorised topics.
	// An empty list yields an empty report rather than an error — see ConsumerLag.
	Topics []string
}

// PartitionLag is the lag of one partition, with every input to the arithmetic retained.
//
// The inputs are reported, not just the result, because lag is a derived number that
// operators routinely disbelieve: being able to see the committed offset and the end
// offset side by side is the difference between diagnosing a stalled consumer and
// arguing about the metric.
type PartitionLag struct {
	// Topic and Partition identify the partition.
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`

	// CommittedOffset is the group's committed offset, or -1 when it has never
	// committed here. Committed says which of the two it is, so a caller never has to
	// know that -1 is the sentinel.
	CommittedOffset int64 `json:"committed_offset"`
	Committed       bool  `json:"committed"`

	// FirstOffset is the earliest offset still retained, which is above zero once
	// retention has deleted the head of the log. It is the baseline for a partition with
	// no commit.
	FirstOffset int64 `json:"first_offset"`

	// EndOffset is the log end offset: the offset the next produced record will take.
	EndOffset int64 `json:"end_offset"`

	// Lag is EndOffset minus the baseline, never negative.
	Lag int64 `json:"lag"`

	// Unavailable is true when the broker could not report this partition's offsets, in
	// which case Lag is 0 and the partition contributes nothing. It is reported so a
	// zero caused by an unreadable partition is distinguishable from a zero caused by a
	// consumer that is keeping up.
	Unavailable bool `json:"unavailable,omitempty"`
}

// TopicLag aggregates one topic's partitions.
type TopicLag struct {
	// Topic is the topic measured.
	Topic string `json:"topic"`

	// TotalLag is the sum of the partition lags, and the value published to the
	// consumer-lag gauge for this topic.
	TotalLag int64 `json:"total_lag"`

	// Partitions carries the per-partition detail, in ascending partition order.
	Partitions []PartitionLag `json:"partitions"`

	// PartitionsWithoutCommit counts partitions the group has never committed on. A
	// number equal to the partition count on a supposedly running consumer means the
	// group is not consuming this topic at all, which is a different fault from being
	// behind.
	PartitionsWithoutCommit int `json:"partitions_without_commit"`

	// PartitionsUnavailable counts partitions whose offsets could not be read.
	PartitionsUnavailable int `json:"partitions_unavailable,omitempty"`
}

// ConsumerLagReport is the outcome of one lag measurement.
type ConsumerLagReport struct {
	// SubscriberID and GroupID echo the request.
	SubscriberID string `json:"subscriber_id,omitempty"`
	GroupID      string `json:"group_id"`

	// TotalLag is the sum across every topic measured.
	TotalLag int64 `json:"total_lag"`

	// Topics carries per-topic detail, in the order the request listed them.
	Topics []TopicLag `json:"topics"`

	// MissingTopics lists requested topics that do not exist on the broker. They
	// contribute no lag, and they are reported because a lag alert on a topic that
	// silently does not exist would read as permanently healthy.
	MissingTopics []string `json:"missing_topics,omitempty"`

	// MeasuredAt is when the measurement was taken. Committed offsets and end offsets
	// are read in two separate round trips, so under live traffic the figure is a
	// snapshot of two moments a few milliseconds apart, and a caller comparing it with
	// anything else needs to know when it was made.
	MeasuredAt time.Time `json:"measured_at"`
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

// ConsumerLag measures how far a consumer group trails the end of the log, in-process.
//
// # Why it is computed here
//
// Lag is the difference between the group's committed offsets, read with OffsetFetch, and
// the partition end offsets, read with ListOffsets. Computing it inside Blnk is what makes
// the lag alert satisfiable with nothing else deployed: the figure lands on the
// blnk.kafka.consumer_lag gauge, is scraped from Blnk's own /metrics endpoint, and the
// alert rule fires above 10,000 messages. No external Kafka lag exporter is part of the
// deployment, and this method is the reason none is needed. The threshold itself lives in
// the alert rule, not here — this method only produces the number.
//
// # The arithmetic, and the two ways it could go wrong
//
// Per partition the lag is the end offset minus a baseline, where the baseline is the
// committed offset when the group has one. Two cases need an explicit decision, and both
// are decided here rather than left to arithmetic:
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
// A partition whose offsets cannot be read is reported as unavailable and contributes
// nothing, rather than contributing a zero that would be indistinguishable from a healthy
// partition.
//
// # Empty topic list
//
// An empty list is not an error. A subscriber authorised for nothing has no lag, and the
// registry deliberately fails closed to exactly that state, so a metrics loop walking
// every subscriber must not be forced to special-case it. The report comes back empty and
// no metric is recorded.
//
// Parameters:
//   - ctx context.Context: honoured before every round trip.
//   - req ConsumerLagRequest: the group and topics to measure.
//
// Returns:
//   - ConsumerLagReport: per-partition, per-topic and total lag.
//   - error: ErrKafkaAdminNotConfigured, a validation error for a missing group, or a
//     wrapped broker error.
func (a *KafkaAdminClient) ConsumerLag(ctx context.Context, req ConsumerLagRequest) (ConsumerLagReport, error) {
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
			"subscriber": report.SubscriberID,
			"group":      report.GroupID,
		}).Debug("kafka admin: no topics to measure consumer lag for")

		return report, nil
	}

	partitions, err := a.topicPartitions(ctx, topics)
	report.MissingTopics = missingTopics(topics, partitions)
	if err != nil {
		return report, err
	}

	if len(report.MissingTopics) > 0 {
		logrus.WithFields(logrus.Fields{
			"subscriber": report.SubscriberID,
			"group":      report.GroupID,
			"topics":     report.MissingTopics,
		}).Warn("kafka admin: consumer lag was requested for topics that do not exist; they contribute no lag")
	}

	if len(partitions) == 0 {
		return report, nil
	}

	bounds, err := a.offsetBounds(ctx, partitions)
	if err != nil {
		return report, err
	}

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
			if !partitionLag.Committed {
				topicLag.PartitionsWithoutCommit++
			}
			if partitionLag.Unavailable {
				topicLag.PartitionsUnavailable++
			}
			topicLag.Partitions = append(topicLag.Partitions, partitionLag)
		}

		report.TotalLag += topicLag.TotalLag
		report.Topics = append(report.Topics, topicLag)

		recordConsumerLag(ctx, report.SubscriberID, report.GroupID, topic, topicLag.TotalLag)
	}

	logrus.WithFields(logrus.Fields{
		"subscriber": report.SubscriberID,
		"group":      report.GroupID,
		"topics":     len(report.Topics),
		"total_lag":  report.TotalLag,
	}).Debug("kafka admin: consumer lag measured")

	return report, nil
}

// buildPartitionLag assembles one partition's entry from its offset bounds and committed
// offset.
//
// Parameters:
//   - topic string, partition int: the partition being described.
//   - bounds partitionOffsetBounds: the zero value is an unavailable partition, which is
//     what an absent map entry means.
//   - committedOffset int64: the group's committed offset, or -1 when it has none.
//
// Returns:
//   - PartitionLag: fully populated, with Lag never negative.
func buildPartitionLag(topic string, partition int, bounds partitionOffsetBounds, committedOffset int64) PartitionLag {
	lag := PartitionLag{
		Topic:           topic,
		Partition:       partition,
		CommittedOffset: committedOffset,
		Committed:       committedOffset >= 0,
		FirstOffset:     bounds.first,
		EndOffset:       bounds.end,
		Unavailable:     bounds.unavailable,
	}

	if !bounds.unavailable {
		lag.Lag = lagForPartition(committedOffset, bounds.first, bounds.end)
	}

	return lag
}

// lagForPartition is the whole lag arithmetic, isolated so it can be tested exhaustively
// against a table with no broker in sight.
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
// yield the zero value — first 0, end 0, unavailable false — which reads as a legitimate
// empty partition and would be counted as a real zero. In a lag measurement that hides a
// backlog; in the zero-loss reconciliation it makes the broker side short and looks
// exactly like message loss. Every read goes through here so that distinction cannot be
// lost at a call site.
//
// An empty partition that the broker DID report is a different thing entirely: it comes
// back with end offset 0 and unavailable false, and is counted as the real zero it is.
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

// committedOffsetFor reads one partition's committed offset out of the fetch result.
//
// A partition the group has never committed on is absent from the map, and this returns
// the same -1 the broker uses for that case, so the caller has exactly one representation
// of "no commit" to reason about.
//
// Parameters:
//   - committed map[string]map[int]int64: the fetch result, possibly nil.
//   - topic string, partition int: the partition to look up.
//
// Returns:
//   - int64: the committed offset, or -1 when there is none.
func committedOffsetFor(committed map[string]map[int]int64, topic string, partition int) int64 {
	if offsets, exists := committed[topic]; exists {
		if offset, ok := offsets[partition]; ok {
			return offset
		}
	}

	return -1
}

// recordConsumerLag publishes one topic's lag on the shared gauge.
//
// The instrument is the one declared in internal/metrics and no other: a second
// instrument for the same measurement would produce two series with different names, and
// the alert rule reads exactly one of them. The attribute keys — subscriber, group, topic
// — are the labels the alert rule's description interpolates, so they are fixed by that
// contract rather than free.
//
// The nil guard costs nothing and keeps a measurement path from panicking a ledger
// process in a build where the instruments were never created.
//
// Parameters:
//   - ctx context.Context: carries the metric's exemplar context.
//   - subscriber, group, topic string: the gauge attributes.
//   - lag int64: the value, already guaranteed non-negative.
func recordConsumerLag(ctx context.Context, subscriber, group, topic string, lag int64) {
	if metrics.SubscriberConsumerLag == nil {
		return
	}

	metrics.SubscriberConsumerLag.Record(ctx, lag, otelmetric.WithAttributes(
		attribute.String("subscriber", subscriber),
		attribute.String("group", group),
		attribute.String("topic", topic),
	))
}

// partitionOffsetBounds is one partition's offset window: the earliest offset still
// retained and the log end offset.
//
// The zero value is deliberately NOT a valid reading — first and end are both zero and
// unavailable is false — which is why every consumer of the map treats an absent entry as
// unavailable rather than as an empty partition.
type partitionOffsetBounds struct {
	// first is the earliest retained offset, -1 when unknown.
	first int64

	// end is the log end offset, -1 when unknown.
	end int64

	// unavailable is true when the broker reported an error for this partition or gave
	// no usable end offset.
	unavailable bool
}

// offsetBounds reads the first and end offset of every given partition in ONE round trip.
//
// Both bounds are requested together because both are needed: the end offset is the
// right-hand side of every lag and of the reconciliation, and the first offset is the
// baseline for a partition with no committed offset. Asking for them separately would
// double the round trips and, worse, read them at two different moments.
//
// The isolation level is left at ReadUncommitted, which makes the end offset the log end
// offset — the same LOG-END-OFFSET an operator sees from kafka-consumer-groups, so the
// two figures agree. ReadCommitted would return the last stable offset instead; Blnk
// publishes non-transactionally so the two coincide today, but pinning the conventional
// definition keeps the numbers comparable if that ever changes.
//
// Parameters:
//   - ctx context.Context
//   - partitions map[string][]int: partitions to read, from topicPartitions.
//
// Returns:
//   - map[string]map[int]partitionOffsetBounds: bounds per topic and partition.
//   - error: a wrapped transport error. A per-partition failure is NOT an error: it is
//     marked unavailable and logged, so one offline partition cannot void a whole
//     measurement.
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
					entry = entry.WithError(offset.Error)
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
//   - map[string]map[int]int64: committed offset per topic and partition. A partition
//     the group has not committed on is absent, or present as the broker's own -1.
//   - error: a wrapped transport or group error. A group that does not exist is NOT an
//     error — it simply has no commits, which is the state of every subscriber that has
//     not started consuming yet, and the caller scores it as full lag.
func (a *KafkaAdminClient) committedOffsets(
	ctx context.Context,
	group string,
	partitions map[string][]int,
) (map[string]map[int]int64, error) {
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
			logrus.WithField("group", group).Info(
				"kafka admin: consumer group does not exist yet, so it has committed nothing; " +
					"its lag is the whole retained log",
			)

			return nil, nil
		}

		return nil, fmt.Errorf(
			"kafka admin: fetching committed offsets for consumer group %q: %w", group, response.Error,
		)
	}

	committed := make(map[string]map[int]int64, len(response.Topics))
	for topic, offsets := range response.Topics {
		perPartition := make(map[int]int64, len(offsets))

		for _, offset := range offsets {
			if offset.Error != nil {
				logrus.WithError(offset.Error).WithFields(logrus.Fields{
					"group":     group,
					"topic":     topic,
					"partition": offset.Partition,
				}).Debug("kafka admin: no committed offset available for this partition; treating it as uncommitted")

				continue
			}

			perPartition[offset.Partition] = offset.CommittedOffset
		}

		committed[topic] = perPartition
	}

	return committed, nil
}

// PartitionOffsetSnapshot is one partition's offset window at a point in time.
type PartitionOffsetSnapshot struct {
	// Partition is the partition ID.
	Partition int `json:"partition"`

	// FirstOffset is the earliest offset still retained.
	FirstOffset int64 `json:"first_offset"`

	// EndOffset is the log end offset, which equals the total number of records ever
	// produced to the partition. It is the figure the zero-loss reconciliation sums.
	EndOffset int64 `json:"end_offset"`

	// Unavailable is true when the broker could not report this partition, in which case
	// both offsets are meaningless and the partition contributes nothing to the sums.
	Unavailable bool `json:"unavailable,omitempty"`
}

// TopicOffsetSnapshot aggregates one topic's partitions.
type TopicOffsetSnapshot struct {
	// Topic is the topic measured.
	Topic string `json:"topic"`

	// Partitions carries the per-partition detail, in ascending partition order.
	Partitions []PartitionOffsetSnapshot `json:"partitions"`

	// EndOffsetSum is the sum of the partitions' end offsets: every record ever
	// published to this topic, whether or not it is still retained. This is the
	// reconciliation figure.
	EndOffsetSum int64 `json:"end_offset_sum"`

	// RetainedCount is the sum of end minus first across partitions: the records still
	// on the log. It is NOT the reconciliation figure — retention deletes records, so
	// this number legitimately falls below the outbox count — and it is reported so that
	// a discrepancy caused by retention can be told apart from one caused by loss.
	RetainedCount int64 `json:"retained_count"`

	// PartitionsUnavailable counts partitions excluded from the sums.
	PartitionsUnavailable int `json:"partitions_unavailable,omitempty"`
}

// TopicOffsetReport is the broker-side half of the zero-loss reconciliation.
//
// The reconciliation compares the outbox's own row counts against these offsets:
// dispatched plus dead-lettered rows should equal the summed end offsets of the category
// topics and their dead-letter siblings. The reconciliation is only valid when
// PartitionsUnavailable is zero and MissingTopics is empty — otherwise the right-hand
// side is short through unreadability rather than through loss, which is exactly the
// mistake this report is shaped to prevent.
type TopicOffsetReport struct {
	// Topics carries per-topic detail, in the order the request listed them, or the
	// canonical inventory order when the request named no topics.
	Topics []TopicOffsetSnapshot `json:"topics"`

	// EndOffsetSum and RetainedCount are the totals across every topic measured.
	EndOffsetSum  int64 `json:"end_offset_sum"`
	RetainedCount int64 `json:"retained_count"`

	// MissingTopics lists requested topics that do not exist on the broker.
	MissingTopics []string `json:"missing_topics,omitempty"`

	// PartitionsUnavailable counts partitions excluded from the totals across all
	// topics.
	PartitionsUnavailable int `json:"partitions_unavailable,omitempty"`

	// MeasuredAt is when the snapshot was taken.
	MeasuredAt time.Time `json:"measured_at"`
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

// TopicEndOffsets reads the broker-side offsets the daily zero-loss reconciliation
// compares outbox counts against.
//
// Summed end offsets are the count of every record ever published to a topic, so the
// reconciliation is: outbox rows marked dispatched, plus rows marked dead-lettered,
// should equal the summed end offsets of the category topics plus their dead-letter
// siblings. A shortfall on the broker side is a lost publish; a shortfall on the outbox
// side is a lost row.
//
// Called with no topics it measures the whole inventory — the four category topics and
// their four dead-letter siblings — which is what the reconciliation wants and what the
// statistics endpoint reports. Named topics are measured instead, for narrowing an
// investigation to one category.
//
// Parameters:
//   - ctx context.Context: honoured before every round trip.
//   - topics ...string: optional topic names. Blanks and duplicates are dropped; an
//     entirely empty list selects the full inventory.
//
// Returns:
//   - TopicOffsetReport: per-partition detail and the sums, plus the caveats
//     (MissingTopics, PartitionsUnavailable) that say whether the reconciliation may be
//     trusted.
//   - error: ErrKafkaAdminNotConfigured, or a wrapped broker error.
func (a *KafkaAdminClient) TopicEndOffsets(ctx context.Context, topics ...string) (TopicOffsetReport, error) {
	report := TopicOffsetReport{MeasuredAt: time.Now().UTC()}
	if err := a.ready(ctx); err != nil {
		return report, err
	}

	requested := normalizeTopicList(topics)
	if len(requested) == 0 {
		// The inventory, from its single source of truth, so the reconciliation covers
		// exactly the topics the pipeline provisions and publishes to.
		requested = AllTopicsWithDeadLetters()
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
				Partition:   id,
				FirstOffset: bound.first,
				EndOffset:   bound.end,
				Unavailable: bound.unavailable,
			}

			if bound.unavailable {
				snapshot.PartitionsUnavailable++
			} else {
				snapshot.EndOffsetSum += bound.end
				snapshot.RetainedCount += retainedRecords(bound.first, bound.end)
			}

			snapshot.Partitions = append(snapshot.Partitions, partitionSnapshot)
		}

		report.EndOffsetSum += snapshot.EndOffsetSum
		report.RetainedCount += snapshot.RetainedCount
		report.PartitionsUnavailable += snapshot.PartitionsUnavailable
		report.Topics = append(report.Topics, snapshot)
	}

	report.MeasuredAt = time.Now().UTC()

	logrus.WithFields(logrus.Fields{
		"topics":                 len(report.Topics),
		"end_offset_sum":         report.EndOffsetSum,
		"retained":               report.RetainedCount,
		"missing_topics":         len(report.MissingTopics),
		"partitions_unavailable": report.PartitionsUnavailable,
	}).Debug("kafka admin: topic end offsets read")

	return report, nil
}

// retainedRecords counts the records still on a partition's log.
//
// It is end minus first, clamped at zero. The clamp covers an empty partition whose first
// and end offsets are equal and above zero — the state of a partition every record of
// which has been deleted by retention — where the subtraction is already zero, and any
// transient reading where first exceeds end.
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
