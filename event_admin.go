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
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
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
// # The service boundary: every result and report here is INTERNAL
//
// SubscriberProvisioningResult, SubscriberProvisioningRequest, TopicAssurance(Report),
// ConsumerLagReport, TopicLag, PartitionLag, TopicOffsetReport, TopicOffsetSnapshot,
// PartitionOffsetSnapshot and OutboxReconciliation are values this package hands to its own
// callers. NONE of them is an HTTP response shape, and none carries a struct tag, so that
// nothing about them invites being handed to a JSON encoder and returned to a client.
//
// That is a boundary rather than a preference, for two reasons. First, api/model owns the
// API contract: api/model.KafkaCredentialsResponse is the single, authoritative shape for
// a credential issuance, and api/model.EventOutboxStatsResponse for the statistics
// endpoint. A second, tag-bearing shape reaching a client would mean two contracts for one
// endpoint, drifting independently and with no test pinning the one that shipped. Second,
// these values legitimately describe the INSIDE of the system — the PBKDF2 iteration
// count, how many ACL bindings were written, whether an existing credential was replaced,
// whether the broker's authorizer appears to be enforcing, per-partition offset windows.
// That is exactly what an operator's log line needs and exactly what a subscriber has no
// business being told, and telling it publishes a map of the security posture to whoever
// holds an API key.
//
// A handler that reports any of this therefore maps the fields it needs onto the api/model
// DTO explicitly, field by field, and everything it does not name stays inside.
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

// MinSCRAMPasswordLength is the shortest secret this service will mint a credential from.
//
// Thirty-two characters over the printable ASCII alphabet is far beyond what an unrated,
// unlocked SASL handshake can be attacked at. It is a floor on a GENERATED value rather than
// a human-chosen one, so it costs no usability: the issuing service's generator produces
// exactly this alphabet at or above this length.
const MinSCRAMPasswordLength = 32

// MinSCRAMPasswordDistinctChars is the minimum number of distinct characters a secret must
// contain.
//
// It exists because a length floor on its own accepts a long run of one character. Sixteen
// distinct characters out of thirty-two is met by a random draw over a 94-character alphabet
// with overwhelming probability, and is not met by a padded constant or a repeated pattern.
const MinSCRAMPasswordDistinctChars = 16

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
// DeleteTopics or any producing method: the first makes an irreversible, data-destroying
// operation reachable from server code, and the second would let event messages bypass the
// outbox.
//
// # AUTH-01: why DeleteACLs is now here, having been excluded
//
// It was excluded on the same "no destructive operations" reasoning as DeleteTopics, and that
// conflated two very different kinds of destruction. Deleting a TOPIC destroys committed
// events and cannot be undone. Deleting an ACL BINDING removes an authorization, and the
// authorization can be recreated from the registry row that describes it — the registry, not
// the broker, is the record of what a subscriber may read.
//
// Excluding it meant Blnk could grant access and never withdraw it. Reducing a subscriber's
// topics left the wider grant standing; deleting a subscriber left its whole boundary live
// with no registry row left to describe it. So the safe-looking omission produced the less
// safe system: a set of permissions that only ever grew.
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

	// RevokeSubscriber removes a subscriber's ACL bindings and then its SCRAM credential,
	// ending its access at the broker. Call it BEFORE deleting the registry row: the row
	// is the only record of which principal and which bindings to remove.
	RevokeSubscriber(ctx context.Context, subscriber *model.EventSubscriber) error

	// RevokeSubscriberPrincipal deletes one principal's SCRAM credential. It is
	// idempotent, so it is safe to retry and safe to call on a principal that may not
	// exist.
	RevokeSubscriberPrincipal(ctx context.Context, principal string) error

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

	// allowPartitionGrowth permits raising the partition count of a topic that already
	// holds records.
	//
	// It defaults to false, which is the safe direction: growing a live topic re-maps keys
	// to partitions and splits every existing aggregate's history irreversibly. Setting it
	// is an operator's statement that the ordering consequences have been planned for.
	allowPartitionGrowth bool

	// cacheMu guards every memoised answer below. A plain Mutex rather than an RWMutex
	// because a read that misses has to write, so no path is purely a read.
	cacheMu sync.Mutex

	// authorizerProbe memoises whether the broker enforces ACLs.
	//
	// Whether a broker runs an authorizer is fixed at broker startup — it comes from
	// authorizer.class.name in the broker's own configuration — so asking on every
	// provisioning call re-answers a question that cannot have changed, and does so from
	// the same five-second budget as the credential write and the ACL batch. The answer
	// is still re-checked periodically rather than once per process, because the cluster
	// a long-lived server is pointed at CAN be restarted with different settings
	// underneath it, and continuing to report "enforcing" would be reporting a security
	// property that has stopped being true.
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
	// defaultOffsetSnapshotTTL; a negative value disables caching entirely, which is
	// what a test asserting on raw round trips wants.
	offsetSnapshotTTL time.Duration

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

// offsetSnapshot is a cached view of the partition layout and the end offsets for one set
// of topics.
//
// It holds the ERROR-FREE result only. A failed probe is never cached: caching a failure
// would keep answering a transient broker problem long after it cleared, and the lag
// gauge would stay wrong for the life of the TTL rather than self-correcting on the next
// sweep.
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
// The end offset is the head of the log, and it moves. A cached one is therefore slightly
// behind, and the question is whether that can change an answer anybody acts on. It
// cannot: at the target rate of 500 events per second a few seconds of staleness is a few
// thousand records against a lag alert threshold of 10,000, and a subscriber close enough
// to that boundary for the difference to matter is already alerting on the next sweep. A
// few seconds is long enough to collapse one sweep's fan-out — which is the only thing
// this cache is for — and short enough that no operator reads a stale figure for long.
const defaultOffsetSnapshotTTL = 5 * time.Second

// maxCachedOffsetSnapshots bounds how many distinct topic sets are remembered.
//
// The cache is keyed by topic set, and different subscribers legitimately hold different
// grants, so the key space is influenced by registry rows an API client authors. A cache
// with a caller-influenced key space and no bound is a leak. When the bound is reached the
// cache is dropped wholesale rather than evicted entry by entry: entries live for seconds,
// so the next sweep repopulates exactly what it needs and precise eviction would be
// bookkeeping for no benefit.
const maxCachedOffsetSnapshots = 64

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
// # Authentication and transport security
//
// The transport is built by NewKafkaTransport — the SAME function the event publisher
// dials through — with the administrative role. That sharing is the fix for CRYPTO-01 and
// it is structural rather than tidy: this client and the publisher previously assembled
// their own transports with their own credential checks, so the two could disagree about
// whether TLS was required and about what counted as a valid credential pair. One of them
// being right was not enough, because the administrative client is the one that carries
// SCRAM credentials for OTHER principals across the wire.
//
// What that shared policy means here: TLS is the default posture and an unencrypted
// connection has to be asked for explicitly with KAFKA_INSECURE_LOCAL_DEV, certificate
// verification cannot be disabled outside that mode, and the SASL pair is validated
// centrally so a username without a secret is refused at construction rather than
// surfacing later as what looks like a wrong password. SCRAM-SHA-512 is fixed, matching
// what scripts/kafka-bootstrap.sh seeds and what every subscriber credential is
// provisioned with.
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
//   - error: when the SASL credentials are internally inconsistent, the SCRAM mechanism
//     cannot be constructed, the TLS material is unreadable or invalid, or plaintext
//     would be used without the explicit local-dev acknowledgement. Never for an absent
//     broker list.
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
	}

	if admin.replicationFactor < 1 {
		// Reported here as well as rejected by EnsureTopics, so the misconfiguration is
		// visible at start-up rather than only when a topic is first provisioned.
		// broker_count, not brokers: the value is a COUNT, and a field named for the
		// list would read as the endpoint list an operator could act on. The endpoints
		// are deliberately not logged — see publisherAuthMode's note on why a broker
		// address list is topology an error line does not need.
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
// # CRYPTO-01: one transport policy, not two
//
// This used to assemble its own transport and do its own credential checking, in parallel
// with the publisher doing the same. Two implementations of one security decision is one
// implementation too many: they disagreed about whether TLS was required and about what
// counted as a valid credential pair, and this is the client whose requests CARRY SCRAM
// CREDENTIALS FOR OTHER PRINCIPALS — a subscriber's password crosses the wire inside an
// AlterUserScramCredentials request. An unencrypted administrative connection therefore
// discloses not one deployment's data but every subscriber's credential as it is minted.
//
// It now delegates to NewKafkaTransport with the administrative role, so the TLS policy,
// the plaintext refusal, the verification requirement and the credential validation are
// literally the same code the publisher runs. The role selects the administrative
// principal (KAFKA_SASL_ADMIN_USER / KAFKA_SASL_ADMIN_SECRET) rather than the producer's.
//
// The timeouts remain this client's own, because an administrative request has a different
// shape from a produce: it is rare, it is interactive, and it should give up sooner rather
// than hold a credential-issuance request open.
//
// SCRAM-SHA-512 is fixed rather than negotiated: it is what scripts/kafka-bootstrap.sh
// seeds the administrative principal with and what every subscriber credential is
// provisioned with, so a second choice could only ever be a mismatch.
//
// Parameters:
//   - cfg config.KafkaConfig: the Kafka block, read for the administrative credential
//     pair and the TLS material.
//
// Returns:
//   - *kafka.Transport: never nil when err is nil.
//   - error: a half-configured credential pair, a credential SASL preparation rejects,
//     unreadable or invalid TLS material, or plaintext without the explicit local-dev
//     acknowledgement. No message contains the secret.
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

	// Zero alone is silent, because zero means UNSET: the configuration defaults it and
	// nothing was overridden. Every other below-floor value was stated by an operator and
	// is being changed, so it is reported — including a NEGATIVE one, which used to slip
	// through this branch and be corrected with nothing anywhere to show the configured
	// number was not the number in effect.
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
	//
	// It is a refusal rather than a failure: the topic works, and what it cannot offer is
	// the partition count the configuration asks for. Reaching that count is a planned
	// migration, because adding partitions to a live topic re-maps keys and splits
	// aggregate histories — see EnsureTopics.
	GrowthRefused bool

	// ReplicationFactor is the topic's OBSERVED minimum replica count across its
	// partitions, or the factor it was created with for a topic this run created. Zero
	// means the topic was not visible in metadata yet.
	//
	// The minimum is reported rather than an average because durability is decided by the
	// weakest partition.
	ReplicationFactor int

	// ReplicationInadequate is true when the observed replica count is below the
	// configured KAFKA_REPLICATION_FACTOR.
	//
	// It exists because this was previously invisible: the factor was applied to newly
	// created topics and discarded from the metadata of existing ones, so a topic at one
	// replica was reported as assured under a configuration asking for three.
	ReplicationInadequate bool
}

// TopicAssuranceReport is the outcome of one EnsureTopics call across every topic Blnk
// owns.
type TopicAssuranceReport struct {
	// Topics carries one entry per topic, in the canonical order
	// AllTopicsWithDeadLetters returns: every category topic, then each one's
	// dead-letter siblings. The stable order is what lets the report be diffed against
	// the provisioning script line for line.
	Topics []TopicAssurance

	// GrowthRefusedCount is how many topics needed partitions and were not grown because
	// they already hold records. A non-zero value is a geometry defect that needs a planned
	// migration, and EnsureTopics returns ErrPartitionGrowthRefused alongside it.
	GrowthRefusedCount int

	// Partitions is the partition count applied, after the MinTopicPartitions floor.
	Partitions int

	// ReplicationFactor is the factor applied to newly created topics, straight from
	// configuration.
	ReplicationFactor int

	// CreatedCount, GrownCount, UnchangedCount and ShrinkRefusedCount summarise
	// Topics. They are computed here rather than left to the caller so that a log line
	// or an operator response does not have to re-derive them.
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
// # TOPIC-01: growing an EMPTY topic is safe; growing a LIVE one destroys ordering
//
// A topic with too few partitions is grown with CreatePartitions ONLY WHILE IT IS EMPTY,
// because a topic provisioned by hand or auto-created with one partition would otherwise cap
// how far a subscriber can scale.
//
// Growth used to be unconditional, and that was the defect. The partition a key lands on is
// murmur2(key) mod partitionCount, so raising the count re-maps keys: a ledger that hashed
// into partition 2 of six lands somewhere else out of twelve, and its history is then split
// across two partitions with no ordering between them. Every key already written loses the
// per-aggregate ordering guarantee, permanently and unrecoverably — the events cannot be
// moved back.
//
// So a non-empty topic is REFUSED and reported rather than grown, and EnsureTopics returns
// ErrPartitionGrowthRefused so the refusal cannot be missed. Reaching six partitions on a live
// topic is a planned migration — provision a correctly-shaped topic, move consumers, drain the
// old one — not something a provisioning pass should do behind an operator's back.
// KAFKA_ALLOW_PARTITION_GROWTH exists for the operator who HAS planned that migration and
// wants the pass to perform the growth step.
//
// # TOPIC-01: the replication factor of an EXISTING topic is verified, not assumed
//
// The factor was previously applied to topics this pass CREATED and discarded from the
// metadata of topics that already existed, so a topic sitting at one replica was reported as
// assured under a configuration asking for three. The report said the durability requirement
// was met; the cluster did not meet it, and a single broker failure would have taken the
// events with it.
//
// The observed minimum replica count is now recorded on every assurance entry and compared
// against the configured factor. A shortfall returns ErrReplicationFactorInadequate. The
// MINIMUM across partitions is used because durability is decided by the weakest one.
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
//   - error: ErrKafkaAdminNotConfigured when no broker is configured, an actionable error
//     when the replication factor is unconfigured, ErrPartitionGrowthRefused when a
//     non-empty topic needs growing, ErrReplicationFactorInadequate when an existing topic
//     is under-replicated, or a wrapped broker error. The two geometry errors are joined
//     when both apply, and the report is fully populated alongside them.
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
	// operation and scripts/kafka-provision.sh provision exactly the same topics
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
			// The plan describes the topics as they are, so a failure here returns a report
			// that still says "one partition", not one that claims a growth that did not
			// happen. The entries are only updated once the broker has confirmed it.
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

// verifyReplication records the observed replica count on each assurance entry and reports
// every topic that falls short of the configured factor.
//
// It records BEFORE it judges, so the report describes the cluster accurately whether or not
// an error is returned — which is what makes the error actionable: the operator reads the
// report to see which topics and how far short.
//
// Parameters:
//   - assurances []TopicAssurance: the report entries, mutated in place.
//   - observed map[string]observedTopicMetadata: the metadata read.
//
// Returns:
//   - error: wrapping ErrReplicationFactorInadequate and naming every short topic, or nil.
func (a *KafkaAdminClient) verifyReplication(
	assurances []TopicAssurance,
	observed map[string]observedTopicMetadata,
) error {
	short := make([]string, 0, len(assurances))

	for i := range assurances {
		metadata, exists := observed[assurances[i].Topic]
		if !exists || metadata.minReplicas <= 0 {
			// The topic is not visible yet — created moments ago, or its leader is still
			// being assigned. Its factor was set by whoever created it and cannot be read
			// now; the next assurance pass reads it.
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

// partitionGrowthDecision splits the topics that need growing into those that may be grown
// and those that must not be.
//
// A topic is growable when it holds NO RECORDS, because there is then no key-to-partition
// mapping to preserve. It is also growable when an operator has set
// KAFKA_ALLOW_PARTITION_GROWTH, which is the explicit statement that the ordering
// consequences have been planned for — that path logs at warning level naming the record
// count it is about to re-map.
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
//
// The REPLICA COUNT is carried alongside the partition IDs because a topic's durability is
// as much a part of its geometry as its partition count, and it was previously read and
// thrown away — see EnsureTopics for what that cost.
type observedTopicMetadata struct {
	// partitionIDs are the topic's partition IDs, ascending.
	partitionIDs []int

	// minReplicas is the SMALLEST replica-set size across the topic's partitions.
	//
	// The minimum rather than an average or the first, because durability is decided by
	// the weakest partition: a topic with five partitions replicated three times and one
	// replicated once loses data when that one broker fails, and reporting three would
	// hide exactly the partition that matters.
	minReplicas int
}

// topicMetadata probes which of the given topics exist, which partition IDs each has, and
// how many replicas the least-replicated partition of each has.
//
// It is the single metadata read in this file; topic assurance, consumer lag and the offset
// snapshot all go through it, so they cannot disagree about which topics exist.
//
// Naming the topics explicitly is safe with respect to auto-creation: kafka-go never sets
// the metadata request's AllowAutoTopicCreation flag, so an unknown topic is reported as
// unknown rather than being created behind the caller's back with the broker's default
// single partition and default replication factor.
//
// Parameters:
//   - ctx context.Context
//   - topics []string: the topics to probe.
//
// Returns:
//   - map[string]observedTopicMetadata: one entry per EXISTING topic. An absent key means
//     the topic does not exist, or exists but has no visible partitions yet.
//   - error: a wrapped transport error, or a per-topic error that is not simply "unknown
//     topic" or "leader not available".
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
// while growing one that holds records re-maps keys to partitions and splits an aggregate's
// history irreversibly.
//
// A partition whose offsets the broker will not report is treated as NON-EMPTY. That is the
// safe direction: assuming empty on missing information is what would allow the destructive
// growth this check exists to prevent.
//
// Parameters:
//   - ctx context.Context
//   - partitions map[string][]int: the partitions to inspect, per topic.
//
// Returns:
//   - map[string]int64: retained record count per topic, only for topics holding records.
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

// RedactedSecretPlaceholder is what a SubscriberSecret renders as, in every form.
//
// A fixed, obviously-deliberate string rather than an empty value: an empty rendering
// reads as "there was no secret", which would make a leak and an absence look the same in
// a log line.
const RedactedSecretPlaceholder = "[REDACTED]"

// SubscriberSecret carries a generated SCRAM password in a form that cannot be printed,
// logged or serialised by accident.
//
// # Why a type rather than a convention
//
// The password was previously a plain string field on the provisioning request. Nothing
// in this file leaked it — that is asserted by tests — but the protection was a property
// of the code that happened to exist rather than of the value itself. A plain string
// field is carried into a log the moment anyone writes logrus.WithField("request", req),
// into an API response the moment the struct is embedded in one, and into a stack trace
// or a test failure message whenever %+v is used on anything containing it. Each of those
// is one ordinary line of code away, none of them fails, and the leak is permanent
// because logs are retained.
//
// This type removes the possibility instead of documenting the rule:
//
//   - Format covers EVERY fmt verb, so %s, %v, %+v, %#v, %q and %x all render the
//     placeholder. Format is what fmt consults first, ahead of Stringer.
//   - MarshalJSON and MarshalText cover encoding/json and every library that uses the
//     text marshaller, so a struct carrying one can be serialised without exposing it.
//   - The value itself is unexported, so no package outside this one can read it at all.
//
// The field on the request stays EXPORTED and typed as this struct, which is load-bearing
// and easy to get wrong: fmt can only call a field's methods when the field is exported
// (it needs CanInterface). An unexported field of a redacting type would be printed by
// %+v as its raw contents, so hiding the field would defeat the redaction rather than
// strengthen it.
//
// # Reading it back
//
// reveal is unexported, so the plaintext is reachable only from this package, where the
// derivation happens. Nothing returns it, and no exported accessor exists: the credential
// is handed to the subscriber exactly once by the endpoint that generated it, which holds
// the plaintext itself and never needs to read it back out of here.
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
	//
	// It is TYPED so that printing, logging or serialising this struct cannot expose it;
	// see SubscriberSecret. The field stays EXPORTED, which is load-bearing rather than
	// incidental: fmt can only call a field's methods when it can take its interface,
	// so an unexported field of a redacting type would be printed by %+v as its raw
	// contents and the redaction would be defeated by the very change meant to
	// strengthen it. Do not add a plain string field that would carry the plaintext.
	Password SubscriberSecret

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
		return SubscriberProvisioningRequest{Password: NewSubscriberSecret(password)}
	}

	return SubscriberProvisioningRequest{
		SubscriberID:        subscriber.SubscriberID,
		Principal:           subscriber.KafkaPrincipal,
		Password:            NewSubscriberSecret(password),
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

	// ACLBindings is how many bindings were created.
	ACLBindings int

	// CredentialReplaced is true when the principal already held a SCRAM credential and
	// this call replaced it. Re-issuing is a supported operation, not an error, and this
	// flag is how an operator sees that an existing consumer's credential just stopped
	// working.
	CredentialReplaced bool

	// AuthorizerActive reports whether the broker confirmed that it enforces ACLs.
	//
	// It is ALWAYS true on a successful return, because provisioning now refuses to write a
	// credential against a broker whose enforcement is not confirmed — see
	// requireEnforcedAuthorizer. It is retained as a field rather than dropped so that the
	// property is assertable from the result, and so a caller logging the result records the
	// fact rather than the assumption.
	AuthorizerActive bool

	// CredentialWritten reports whether the SCRAM credential reached the broker.
	//
	// It exists for the failure path: a caller handed an error needs to know whether a
	// credential now exists, because that is the difference between "retry" and "a live
	// principal is unaccounted for". It is false on a successful compensation, which is the
	// state the broker is actually left in.
	CredentialWritten bool

	// Compensated reports that provisioning failed after the credential was written and the
	// credential and its attempted bindings were revoked.
	//
	// True means the broker was left clean; the caller must not persist an issuance record,
	// and the secret it generated is dead. False alongside an error after CredentialWritten
	// means revocation itself failed and a principal needs manual attention — the log line
	// names it.
	Compensated bool

	// ProvisionedAt is when provisioning completed.
	ProvisionedAt time.Time
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
// # SEC-02: enforcement is verified BEFORE the credential is written, and failure is fatal
//
// In KRaft mode a broker enforces ACLs only when it is started with
// authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer.
// WITHOUT IT, CreateACLs SUCCEEDS AND THE BINDINGS ARE NEVER APPLIED: every request from
// every principal is allowed, the bindings are visible in kafka-acls output, and an isolation
// test would pass while proving nothing at all.
//
// The probe used to run AFTER the credential was written and was warning-only, so on such a
// broker this method minted a working credential, returned it, and reported success. The
// subscriber then held cluster-wide read access to every ledger topic, every dead-letter
// topic and every other subscriber's data — and the only trace was a log line in a stream
// nobody reads during a successful provisioning.
//
// Now the probe runs FIRST and it FAILS CLOSED. No credential is written, no binding is
// created and nothing is returned unless the broker has affirmatively answered that it
// enforces ACLs. Two distinct failures are both fatal:
//
//   - The broker reports SECURITY_DISABLED. There is no authorizer; a credential issued here
//     would have no boundary at all.
//   - The question cannot be answered — the administrative principal may not describe ACLs,
//     or the broker is unreachable. "I am not allowed to ask" is not "the answer is yes", and
//     issuing a credential whose isolation is unverifiable is the same exposure as issuing one
//     with no isolation. The remedy is operational and the error says so: grant the
//     administrative principal Describe on the cluster.
//
// The finding is still reported through SubscriberProvisioningResult.AuthorizerActive, which
// is now always true on a successful return — a property a test can assert directly.
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

	// SEC-02: BEFORE anything is written. A credential minted against a broker that does not
	// enforce ACLs — or one whose enforcement cannot be confirmed — has no boundary, so this
	// is a precondition rather than a diagnostic.
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
		logrus.WithError(err).WithField("principal", principal).Debug(
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

	bindings := req.aclEntries()
	if err := a.createACLBindings(ctx, principal, bindings); err != nil {
		// AUTH-01: the credential exists and its boundary does not. That combination is the
		// one state provisioning must never leave behind, because the principal can
		// authenticate — so it is compensated by revoking the credential before returning.
		a.compensateFailedProvisioning(ctx, principal, bindings)
		result.CredentialWritten = false
		result.Compensated = true

		return result, err
	}
	result.ACLBindings = len(bindings)

	if len(topics) == 0 {
		logrus.WithFields(logrus.Fields{
			"principal":  sanitizeLogValue(principal, maxLoggedFilterLength),
			"subscriber": sanitizeLogValue(result.SubscriberID, maxLoggedFilterLength),
		}).Warn(
			"kafka admin: principal provisioned with no authorised topics, so it can read nothing. This is the " +
				"fail-closed default of a newly registered subscriber; grant topics on the subscriber before " +
				"issuing credentials it is expected to consume with",
		)
	}

	if groupPrefix == "" {
		logrus.WithFields(logrus.Fields{
			"principal":  sanitizeLogValue(principal, maxLoggedFilterLength),
			"subscriber": sanitizeLogValue(result.SubscriberID, maxLoggedFilterLength),
		}).Warn(
			"kafka admin: principal provisioned without a consumer group grant, so it cannot join a consumer " +
				"group; set the subscriber's consumer group before issuing credentials",
		)
	}

	result.ProvisionedAt = time.Now().UTC()

	logrus.WithFields(logrus.Fields{
		"subscriber":            sanitizeLogValue(result.SubscriberID, maxLoggedFilterLength),
		"principal":             sanitizeLogValue(principal, maxLoggedFilterLength),
		"mechanism":             SubscriberSASLMechanism,
		"iterations":            iterations,
		"topics":                len(topics),
		"consumer_group_prefix": sanitizeLogValue(groupPrefix, maxLoggedFilterLength),
		"acl_bindings":          result.ACLBindings,
		"credential_replaced":   result.CredentialReplaced,
		"authorizer_active":     result.AuthorizerActive,
	}).Info("kafka admin: subscriber principal provisioned")

	return result, nil
}

// validate rejects a request that cannot produce a usable credential, or that asks for a
// boundary this service will not grant.
//
// # SEC-03: the boundary is DERIVED, and the request is checked against it
//
// The principal and the consumer group namespace are not names, they are the access
// boundary: the credential is minted for the principal and every ACL binding names it and
// the namespace. Accepting them from a caller — which is what merely trimming them amounted
// to — meant a request could ask for the principal or group "*", and Kafka treats the
// resource name "*" as matching ANY resource, so the binding became a cluster-wide grant. A
// namespace could also be chosen to OVERLAP another subscriber's, which is a cross-domain
// grant obtained through an ordinary authorized request.
//
// So both are derived from the subscriber's immutable identifier, and a supplied value is
// only ever COMPARED against the derived one. A mismatch is refused rather than corrected,
// because a caller that sent a different boundary asked for something it may not have and
// silently substituting the right answer would hide that.
//
// The topic list is checked against the grantable allowlist for the same reason — see
// normalizedTopics — and the host pattern is checked because an unconstrained host string
// ends up inside an ACL binding.
//
// No message quotes the password, not even to say what is wrong with it beyond the rule it
// broke.
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

	// Compared EXACTLY, not after trimming. A recorded principal that differs from the derived
	// one only by whitespace is the SEC-04 duplicate seen from this side: it looks like the
	// same identity, and a registry that holds it can hold two rows for one principal. The
	// values are quoted so a whitespace-only difference is visible in the message.
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
	// supplied string, so a group chosen to overlap a sibling's namespace cannot be granted.
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

	return validateSCRAMPassword(r.Password.reveal())
}

// validateTopics refuses any topic outside the subscriber-grantable allowlist.
//
// # SEC-03: an allowlist, not a shape check
//
// The list arrives from the registry and ends up as the resource name of a LITERAL ACL
// binding, so whatever is in it is what the credential can read. Three classes have to be
// excluded and each for its own reason:
//
//   - "*" and other WILDCARDS, because Kafka's resource name "*" matches any resource: one
//     such entry turns a per-topic grant into a cluster-wide one.
//   - FOREIGN topics, because a grant over a topic Blnk does not own is a grant into
//     somebody else's data on a broker Blnk shares.
//   - DEAD-LETTER and INTERNAL topics, because the DLTs carry failed events with Blnk's own
//     failure metadata and the system and quarantine categories carry Blnk's internal
//     diagnostics and uncatalogued payloads. None of those has a subscriber audience.
//
// IsSubscriberGrantableTopic is the single test for all three, so the API layer, this path
// and the provisioning script cannot disagree about what is grantable.
//
// Returns:
//   - error: naming the first offending topic, nil when every topic is grantable.
func (r SubscriberProvisioningRequest) validateTopics() error {
	for _, topic := range normalizeTopicList(r.Topics) {
		if !IsSubscriberGrantableTopic(topic) {
			return fmt.Errorf(
				"kafka admin: refusing to grant topic %q; only Blnk-owned subscriber-facing category "+
					"topics may be granted (%s). Dead-letter and internal topics carry Blnk's own "+
					"failure and diagnostic data and have no subscriber audience",
				topic, strings.Join(SubscriberGrantableTopics(), ", "),
			)
		}
	}

	return nil
}

// validateHost refuses a host pattern that is neither the explicit any-host wildcard nor a
// plausible single host.
//
// The value becomes the Host field of every ACL binding, where "*" means "from anywhere".
// That is the intended default, so it is accepted as an exact value — but a string that
// merely CONTAINS a wildcard, or carries whitespace or control characters, is refused: it
// would either widen the binding in a way nobody asked for or produce a binding that matches
// nothing while looking like a restriction.
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
// It is trimmed only so that the comparison in validate reports a whitespace mismatch as a
// mismatch of NAMES rather than of invisible characters. Nothing downstream relies on
// trimming to make a value safe: validate has already established that this equals the
// derived principal exactly.
func (r SubscriberProvisioningRequest) principal() string {
	return strings.TrimSpace(r.Principal)
}

// boundPrincipal returns the principal every ACL binding is made over, DERIVED from the
// subscriber identifier.
//
// Bindings are made over the derived name rather than the supplied one so that the grant and
// the credential are provably the same identity: validate refuses a mismatch, and deriving
// here means the bindings would still be correct even if a future caller reached aclEntries
// without going through validate.
//
// It falls back to the supplied principal only when derivation is impossible — an identifier
// that is not canonical — which provisioning refuses outright. The fallback exists for
// REVOCATION, which must be able to remove the bindings of a row written before these rules
// existed rather than refusing to clean it up.
func (r SubscriberProvisioningRequest) boundPrincipal() string {
	if derived, err := model.CanonicalKafkaPrincipal(strings.TrimSpace(r.SubscriberID)); err == nil {
		return derived
	}

	return r.principal()
}

// consumerGroupPrefix returns the PREFIXED ACL resource name for this subscriber's consumer
// group namespace, DERIVED from its identifier.
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
//
// An empty return therefore means either "no group recorded" or "the identifier cannot produce
// a namespace", and validate rejects the second before this is reached.
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
	principal := kafkaPrincipalPrefix + r.boundPrincipal()
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
// # PASS-01: strength is enforced here, at the boundary that mints the credential
//
// This used to check the alphabet and nothing else, so a ONE-CHARACTER password passed and
// was minted into a real, working SCRAM credential with Read on live ledger topics. There is
// no rate limit at a Kafka SASL handshake and no lockout, so a short secret is not "weak", it
// is open.
//
// Two rules replace that. A LENGTH FLOOR of MinSCRAMPasswordLength, because the secret is
// generated by the issuing service rather than chosen by a human — nothing legitimate is
// short, so the floor costs nobody anything and refuses everything that reached here by
// mistake. And a DISTINCT-CHARACTER FLOOR, because a length floor alone accepts a run of
// thirty-two identical characters, which is long and has almost no entropy; a generated
// secret over this alphabet exceeds the floor overwhelmingly, while a padded constant does
// not.
//
// Neither rule is a substitute for generating the secret properly, and neither can be: this
// function sees a string, not its provenance. They are the boundary check that makes a
// generator defect — or a caller passing a placeholder — impossible to mint.
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

// ErrAuthorizerNotEnforcing reports that the broker does not enforce ACLs, or that its
// enforcement could not be confirmed.
//
// It is a sentinel so the subscriber service, the API layer and a test all recognise the
// refusal without matching message text, and so the two distinct causes — no authorizer, and
// no answer — are provably one refusal.
var ErrAuthorizerNotEnforcing = errors.New(
	"kafka admin: the broker's ACL enforcement is not confirmed, so no subscriber credential may be issued",
)

// clock returns the time source, defaulting to time.Now.
//
// Cache expiry is the only thing in this file that depends on the passage of time, and a
// test that had to sleep to observe it would be both slow and flaky. Reading the clock
// through a field makes expiry directly assertable.
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
// negative value disables caching, which is what a test asserting on raw round-trip counts
// wants and is deliberately not reachable from configuration.
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
// The topics are joined with a byte that cannot occur in a Kafka topic name, so two
// different sets cannot produce one key by concatenation. Order is preserved rather than
// sorted: the lists reaching here are already normalised and come from a canonical order,
// so two callers asking about the same topics produce the same key, and a differently
// ordered list producing a second entry costs one extra snapshot rather than a wrong
// answer.
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
// # What this removes
//
// A consumer-lag sweep over N subscribers asked the same two questions N times — which
// partitions exist, and where does each one end — because only the third question, the
// group's committed offsets, actually differs between subscribers. That made a sweep cost
// 3N round trips where 1 + 1 + N would do, and it did so on the metrics path, at whatever
// interval the gauge is refreshed.
//
// The two reads are cached TOGETHER, as one snapshot, because they must be consistent with
// each other: a partition present in the layout but absent from the bounds, or the reverse,
// would produce a lag figure computed from two different views of the cluster.
//
// A failure is returned and NOT cached, so a transient broker problem is re-asked on the
// next call instead of being remembered for the life of the TTL.
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
		// Dropped wholesale at the bound rather than evicted entry by entry: entries live
		// for seconds, so the next sweep repopulates exactly what it needs and precise
		// eviction would be bookkeeping for no benefit.
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
// answer cannot change while the broker is up. Asking on every provisioning call therefore
// spent a serial DescribeACLs round trip — from the same five-second budget as the
// credential write and the ACL batch — to re-learn something already known.
//
// The memo has a TTL rather than being permanent, because a long-lived server can be
// pointed at a cluster that is restarted with different settings underneath it, and
// continuing to report "enforcing" would be reporting a security property that has stopped
// being true.
//
// A FAILED probe is not cached. A permission problem or a transient broker error must be
// re-asked next time; caching it would turn one bad answer into TTL-long silence about
// whether the isolation guarantee holds.
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

// requireEnforcedAuthorizer is the SEC-02 gate: no credential is written unless the broker has
// affirmatively confirmed that it enforces ACLs.
//
// It replaces a warning-only probe that ran AFTER the credential was written. The difference
// is the whole finding: a warning on a broker with no authorizer still left a working
// credential with cluster-wide read access in a subscriber's hands, whereas this refuses
// before anything exists to clean up.
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
	// memo has a TTL and a failed probe is never cached, so the FAIL-CLOSED behaviour below
	// is unchanged: an unanswerable probe is still treated as an absent boundary.
	active, err := a.authorizerActiveCached(ctx)
	if err != nil {
		logrus.WithError(err).WithField("principal", principal).Error(
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
		logrus.WithField("principal", principal).Error(
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

// compensateFailedProvisioning undoes a half-completed provisioning.
//
// # AUTH-01: a credential without its boundary must not survive the failure that created it
//
// Provisioning writes the SCRAM credential first and the ACL bindings second, because a
// binding for a principal that does not exist is inert while a credential without bindings is
// not: it AUTHENTICATES. On a broker whose default is deny that principal can read nothing,
// but it is a live, valid credential that was handed to a subscriber by a call that then
// reported failure — so the subscriber holds a secret nobody is tracking, and a later change
// to a default or a broad binding gives it reach.
//
// So the failure path revokes. Both directions are attempted, and independently: the
// bindings, because CreateACLs is not atomic across entries and some may have landed before
// the error; and the credential itself.
//
// Failures HERE are logged and not returned, deliberately. The caller is already returning
// the original error, which is the one that explains what happened, and replacing it with a
// cleanup error would hide the cause. What the log line guarantees is that an operator can
// find the principal that needs manual revocation, which is why it names it at error level.
//
// Parameters:
//   - ctx context.Context: the provisioning context. Note that a cancelled context cannot
//     revoke anything, which is itself logged.
//   - principal string: the principal to revoke.
//   - bindings []kafka.ACLEntry: the bindings that were attempted, so exactly those are
//     deleted rather than everything the principal holds.
func (a *KafkaAdminClient) compensateFailedProvisioning(
	ctx context.Context,
	principal string,
	bindings []kafka.ACLEntry,
) {
	logger := logrus.WithField("principal", principal)

	if err := a.deleteACLBindings(ctx, principal, bindings); err != nil {
		logger.WithError(err).Error(
			"kafka admin: could not remove the ACL bindings of a failed provisioning; " +
				"remove them manually with kafka-acls before reissuing",
		)
	}

	if err := a.RevokeSubscriberPrincipal(ctx, principal); err != nil {
		logger.WithError(err).Error(
			"kafka admin: A SCRAM CREDENTIAL WAS WRITTEN AND COULD NOT BE REVOKED after provisioning " +
				"failed. The principal can authenticate and is not recorded in the registry. Delete it " +
				"manually: kafka-configs --alter --delete-config SCRAM-SHA-512 --entity-type users " +
				"--entity-name <principal>",
		)

		return
	}

	logger.Warn(
		"kafka admin: provisioning failed after the credential was written; the credential and its " +
			"attempted ACL bindings have been revoked, so no unbounded principal was left behind",
	)
}

// RevokeSubscriberPrincipal deletes a subscriber's SCRAM credential.
//
// # AUTH-01: deletion is part of the lifecycle, not an afterthought
//
// The administrative contract used to expose creation and no removal at all, so reducing a
// subscriber's topics, or deleting the subscriber entirely, left the credential and its
// bindings live at the broker. The registry row was gone and the access was not: a
// deprovisioned subscriber kept consuming, and nothing in Blnk could see it any more.
//
// Deleting the credential is the operation that actually ends access, because it is what the
// SASL handshake checks. Removing bindings alone leaves a principal that can authenticate;
// removing the credential alone leaves inert bindings. Callers ending a subscriber's life
// should do both, and should do them BEFORE deleting the registry row — the row is the only
// record of which principal to revoke.
//
// It is IDEMPOTENT: deleting a credential that does not exist is reported by the broker as
// RESOURCE_NOT_FOUND, which is treated as success. That matters because revocation is
// retried by operators and by compensation paths, and a second attempt must not fail.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - principal string: the SASL username to delete. Required.
//
// Returns:
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
func (a *KafkaAdminClient) RevokeSubscriberPrincipal(ctx context.Context, principal string) error {
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
			// Nothing to delete is the desired end state, so it is success. This is what
			// makes revocation safe to retry, which both operators and the compensation
			// path depend on.
			continue
		}

		return fmt.Errorf("kafka admin: deleting the %s credential for principal %q: %w",
			SubscriberSASLMechanism, response.Results[i].User, resultErr)
	}

	logrus.WithField("principal", principal).Info("kafka admin: subscriber SCRAM credential revoked")

	return nil
}

// RevokeSubscriber removes a subscriber's ACL bindings and then its SCRAM credential, ending
// its access at the broker.
//
// # AUTH-01: deletion propagation, and why the order is fixed
//
// This is the operation a caller ending a subscriber's life must use, and it must run BEFORE
// the registry row is deleted — the row is the only record of which principal holds which
// grant, so deleting it first strands live broker state that nothing can any longer describe.
//
// The repository half it pairs with is Datasource.TakeEventSubscriber, which deletes the row
// and RETURNS it, so the value passed here is exactly the boundary that was removed rather
// than one re-read beforehand and possibly since changed. The full sequence a delete handler
// runs is: Take the row, Revoke with it here, and — if this fails — log the principal from
// the returned row so an operator can revoke by hand. Datasource.ClearSubscriberCredential
// is the corresponding call when only the credential is being revoked and the subscriber
// itself stays registered.
//
// The ORDER inside it is bindings first, credential second. Reversed, there is a window in
// which the principal cannot authenticate while its bindings still stand, and if the
// credential is ever recreated — by a retry, or by an operator — the old boundary is silently
// back in force. In this order the boundary goes first, so a partial failure always leaves the
// principal with FEWER rights rather than more.
//
// Both steps are attempted even when the first fails, so a binding-removal problem does not
// leave the credential live. The returned error names whichever step failed; when both fail
// the credential error is returned, because a principal that can still authenticate is the
// more serious of the two.
//
// The bindings removed are exactly those the registry row describes, derived the same way
// provisioning derived them. A subscriber whose topics were reduced before revocation may
// therefore still hold bindings for the topics it lost — which is why a topic reduction must
// itself go through a reconciliation rather than relying on eventual revocation.
//
// Parameters:
//   - ctx context.Context: cancels the requests.
//   - subscriber *model.EventSubscriber: the registry row. Nil is refused, because there
//     would be no principal to revoke.
//
// Returns:
//   - error: nil when the broker holds neither the bindings nor the credential.
func (a *KafkaAdminClient) RevokeSubscriber(ctx context.Context, subscriber *model.EventSubscriber) error {
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

	bindingErr := a.deleteACLBindings(ctx, principal, request.aclEntries())
	credentialErr := a.RevokeSubscriberPrincipal(ctx, principal)

	switch {
	case credentialErr != nil:
		if bindingErr != nil {
			logrus.WithError(bindingErr).WithField("principal", principal).Error(
				"kafka admin: removing a subscriber's ACL bindings also failed; both need manual attention",
			)
		}

		return credentialErr
	case bindingErr != nil:
		return bindingErr
	default:
		logrus.WithFields(logrus.Fields{
			"subscriber": strings.TrimSpace(subscriber.SubscriberID),
			"principal":  principal,
		}).Info("kafka admin: subscriber access revoked at the broker")

		return nil
	}
}

// deleteACLBindings removes exactly the bindings it is given.
//
// It deletes by an EXACT FILTER per binding — resource type, name, pattern type, principal,
// host, operation and permission all matched — rather than by a broad "everything for this
// principal" filter. A broad delete is the more convenient call and the more dangerous one: a
// filter wide enough to catch a subscriber's own bindings is wide enough to catch bindings an
// operator created by hand, and ACL deletion has no undo.
//
// A binding that is already absent is not an error. Kafka reports zero matches, which is the
// desired end state, so this is idempotent and safe in a retry or a compensation path.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - principal string: used only for the log line; the filters carry their own principal.
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
		"principal": principal,
		"filters":   len(filters),
		"removed":   removed,
	}).Info("kafka admin: subscriber ACL bindings removed")

	return nil
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
	Topic     string
	Partition int

	// CommittedOffset is the group's committed offset, or -1 when it has never
	// committed here. Committed says which of the two it is, so a caller never has to
	// know that -1 is the sentinel.
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
	// which case Lag is 0 and the partition contributes nothing. It is reported so a
	// zero caused by an unreadable partition is distinguishable from a zero caused by a
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

	// PartitionsWithoutCommit counts partitions the group has never committed on. A
	// number equal to the partition count on a supposedly running consumer means the
	// group is not consuming this topic at all, which is a different fault from being
	// behind.
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

	// MeasuredAt is when the measurement was taken. Committed offsets and end offsets
	// are read in two separate round trips, so under live traffic the figure is a
	// snapshot of two moments a few milliseconds apart, and a caller comparing it with
	// anything else needs to know when it was made.
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
			"subscriber": sanitizeLogValue(report.SubscriberID, maxLoggedFilterLength),
			"group":      sanitizeLogValue(report.GroupID, maxLoggedFilterLength),
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
			"subscriber": sanitizeLogValue(report.SubscriberID, maxLoggedFilterLength),
			"group":      sanitizeLogValue(report.GroupID, maxLoggedFilterLength),
			"topics":     report.MissingTopics,
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
		"subscriber": sanitizeLogValue(report.SubscriberID, maxLoggedFilterLength),
		"group":      sanitizeLogValue(report.GroupID, maxLoggedFilterLength),
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
// # Every label value is bounded here, at the last possible moment
//
// All three attribute values pass through a resolver that either recognises the value or
// substitutes a fixed token. This is the THIRD enforcement of the identifier contract —
// the repository validates before a write and the schema asserts a CHECK constraint — and
// it is the only one that covers a value which never came from the registry at all. An
// operator's ad-hoc lag query, a group id read back from the broker, or a future caller
// assembling a ConsumerLagRequest by hand all arrive here without having passed either of
// the other two gates.
//
// Two things would go wrong without it, and neither would fail loudly. A group named for
// its owner would export a customer's name into the monitoring system's retention, onto
// dashboards and into alert notifications. And because this is a GAUGE that a periodic
// collector re-records on every cycle, each distinct label tuple is a series held for as
// long as it is fed, so an unbounded value would make the series count a function of what
// callers send rather than of how many subscribers are registered.
//
// The nil guard costs nothing and keeps a measurement path from panicking a ledger
// process in a build where the instruments were never created.
//
// Parameters:
//   - ctx context.Context: carries the metric's exemplar context.
//   - subscriber, group, topic string: the RAW values; each is resolved to a bounded
//     label here rather than by the caller, so no call site can bypass the bound.
//   - lag int64: the value, already guaranteed non-negative.
func recordConsumerLag(ctx context.Context, subscriber, group, topic string, lag int64) {
	if metrics.SubscriberConsumerLag == nil {
		return
	}

	// Every attribute goes through its RESOLVER, and for two reasons that both matter.
	// It bounds the cardinality: the three values arrive from registry rows, so an
	// unconstrained one would mint a new time series on every collection cycle. And it is
	// what lets a stale series be CLEARED — the collector zeroes a series by writing to
	// the identical label tuple, so if it resolved labels differently from this function
	// it would zero a tuple nobody published and leave the real one standing at its last
	// reading for ever. See event_metrics_support.go.
	metrics.SubscriberConsumerLag.Record(ctx, lag, otelmetric.WithAttributes(
		attribute.String("subscriber", subscriberLagLabel(subscriber)),
		attribute.String("group", consumerGroupLagLabel(group)),
		attribute.String("topic", topicLagLabel(topic)),
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
			logrus.WithField("group", sanitizeLogValue(group, maxLoggedFilterLength)).Info(
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
					"group":     sanitizeLogValue(group, maxLoggedFilterLength),
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
	Partition int

	// FirstOffset is the earliest offset still retained.
	FirstOffset int64

	// EndOffset is the log end offset, which equals the total number of records ever
	// produced to the partition. It is the figure the zero-loss reconciliation sums.
	EndOffset int64

	// Unavailable is true when the broker could not report this partition, in which case
	// both offsets are meaningless and the partition contributes nothing to the sums.
	Unavailable bool
}

// TopicOffsetSnapshot aggregates one topic's partitions.
type TopicOffsetSnapshot struct {
	// Topic is the topic measured.
	Topic string

	// Partitions carries the per-partition detail, in ascending partition order.
	Partitions []PartitionOffsetSnapshot

	// EndOffsetSum is the sum of the partitions' end offsets: every record ever
	// published to this topic, whether or not it is still retained. This is the
	// reconciliation figure.
	EndOffsetSum int64

	// RetainedCount is the sum of end minus first across partitions: the records still
	// on the log. It is NOT the reconciliation figure — retention deletes records, so
	// this number legitimately falls below the outbox count — and it is reported so that
	// a discrepancy caused by retention can be told apart from one caused by loss.
	RetainedCount int64

	// PartitionsUnavailable counts partitions excluded from the sums.
	PartitionsUnavailable int
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
	Topics []TopicOffsetSnapshot

	// EndOffsetSum is the total number of RECORDS WRITTEN across every topic measured, and
	// RetainedCount how many of those the broker still holds.
	//
	// EndOffsetSum is NOT a count of events: it counts every redelivery, every replay and
	// every dead-letter copy as its own record. See TopicEndOffsets and
	// ReconcileAgainstOutbox for what may and may not be concluded from it.
	EndOffsetSum  int64
	RetainedCount int64

	// MissingTopics lists requested topics that do not exist on the broker.
	MissingTopics []string

	// PartitionsUnavailable counts partitions excluded from the totals across all
	// topics.
	PartitionsUnavailable int

	// MeasuredAt is when the snapshot was taken.
	MeasuredAt time.Time
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

// TopicEndOffsets reads the broker-side offsets the daily zero-loss reconciliation reads.
//
// # OBS-01: this is a LOWER BOUND on messages, not a count of events
//
// Summed end offsets count RECORDS WRITTEN, and the outbox counts EVENTS. Those are
// deliberately different numbers, and the reconciliation used to be documented as an
// equality between them, which cannot hold:
//
//   - A REDELIVERY writes a second record for one event. The relay can crash between a
//     successful publish and the row being marked dispatched, so the redelivery is a designed
//     behaviour of an at-least-once transport, not a fault.
//   - A REPLAY writes another record for an event that already has one, on purpose.
//   - A DEAD-LETTERED event has a record on its `.dlt` topic and its row counted once.
//   - RETENTION deletes records while their rows remain, so the end offset keeps climbing
//     while retained records fall.
//
// So messages >= events, always, and an equality check would report loss on a healthy system
// the first time anything was redelivered — the classic alert that gets muted, taking the
// real signal with it.
//
// What this number CAN establish is the direction that matters. Every dispatched or
// dead-lettered row must have produced at least one record, so:
//
//	messages <  events   ⇒  LOSS. Rows claim publication that never reached a broker.
//	messages >= events   ⇒  no loss detectable this way; the excess is the duplicate,
//	                        replay and dead-letter overhead, and it is expected.
//
// Proving the stronger property — that every event_id appears at least once — requires
// reading the topics and deduplicating on event_id, which needs a consumer. Blnk implements
// no consumer by design, so that check belongs to the audit procedure in
// docs/kafka-operations.md rather than to this method, and this method must not be presented
// as a substitute for it.
//
// Called with no topics it measures the whole inventory — every category topic and
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
//     trusted. Use ReconcileAgainstOutbox to interpret it rather than comparing the sums by
//     hand.
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

// OutboxReconciliation is the verdict of comparing the outbox against the broker.
//
// # OBS-01: a verdict, not an equality
//
// It exists because callers were left to compare a record count against an event count
// themselves, and the only comparison available — equality — is wrong. This type states what
// the comparison can and cannot establish, so no caller has to re-derive it and none can
// accidentally report "reconciled" from a number that was never going to match.
type OutboxReconciliation struct {
	// TerminalEvents is how many outbox rows claim to have been published: dispatched plus
	// dead-lettered. Each is counted exactly ONCE, which is the property the unique index on
	// event_id gives.
	TerminalEvents int64

	// MessagesWritten is how many records the broker has accepted across the measured
	// topics, from summed end offsets. It counts every redelivery, replay and dead-letter
	// copy separately.
	MessagesWritten int64

	// Overhead is MessagesWritten minus TerminalEvents: the redelivery, replay and
	// dead-letter copies. Its EXPECTED value is greater than or equal to zero, and a healthy
	// system's overhead is small but not zero.
	//
	// It is negative exactly when loss is detected, which is why the field is signed rather
	// than clamped: clamping would erase the only signal this reconciliation carries.
	Overhead int64

	// LossDetected is true when the broker holds FEWER records than the outbox has terminal
	// rows. That is unambiguous: rows claim a publication that no record corresponds to.
	//
	// False does NOT mean "proven no loss" — see Conclusive. It means no loss is detectable
	// by counting.
	LossDetected bool

	// Conclusive reports whether the count could be trusted at all.
	//
	// It is false when a measured topic was missing, when any partition's offsets were
	// unavailable, or when retention has deleted records — in each case the record count is
	// not a complete picture of what was written, so neither a shortfall nor a surplus proves
	// anything. A caller must not report a green reconciliation on an inconclusive result.
	Conclusive bool

	// Caveats names, in plain words, every reason the result is inconclusive. Empty when
	// Conclusive is true.
	Caveats []string

	// MeasuredAt is when the broker side was measured.
	MeasuredAt time.Time
}

// Summary renders the verdict as one sentence for a log line or a runbook.
//
// Returns:
//   - string: the verdict, always naming both numbers so the sentence is checkable.
func (r OutboxReconciliation) Summary() string {
	switch {
	case r.LossDetected:
		return fmt.Sprintf(
			"LOSS DETECTED: %d outbox rows are marked published but the broker holds only %d records "+
				"across the measured topics (%d missing). Every dispatched or dead-lettered row must have "+
				"produced at least one record",
			r.TerminalEvents, r.MessagesWritten, -r.Overhead,
		)
	case !r.Conclusive:
		return fmt.Sprintf(
			"INCONCLUSIVE: %d outbox rows against %d broker records, but the count cannot be trusted (%s)",
			r.TerminalEvents, r.MessagesWritten, strings.Join(r.Caveats, "; "),
		)
	default:
		return fmt.Sprintf(
			"NO LOSS DETECTED: %d outbox rows against %d broker records, %d of which are redelivery, "+
				"replay or dead-letter overhead. This establishes that nothing claims a publication that "+
				"did not happen; proving every event_id is present requires the audit consumer described "+
				"in docs/kafka-operations.md",
			r.TerminalEvents, r.MessagesWritten, r.Overhead,
		)
	}
}

// ReconcileAgainstOutbox interprets an offset report against the outbox's terminal row count.
//
// It is the ONLY sanctioned way to compare the two, and it exists so that the asymmetry is
// applied in one place: messages are a lower bound on events, never an equality, so the test
// is a DIRECTIONAL one and the surplus is expected rather than suspicious.
//
// Retention is treated as a caveat rather than folded into the arithmetic. A topic whose
// records have partly aged out has an end offset that still counts them, so the comparison
// remains valid in the direction that matters — but a reader must know that retention is in
// play before concluding anything about what is still consumable.
//
// Parameters:
//   - report TopicOffsetReport: the broker-side measurement from TopicEndOffsets.
//   - terminalEvents int64: dispatched plus dead-lettered outbox rows, from
//     CountEventOutboxByStatus. Each event counted once.
//
// Returns:
//   - OutboxReconciliation: the verdict, always populated.
func ReconcileAgainstOutbox(report TopicOffsetReport, terminalEvents int64) OutboxReconciliation {
	verdict := OutboxReconciliation{
		TerminalEvents:  terminalEvents,
		MessagesWritten: report.EndOffsetSum,
		Overhead:        report.EndOffsetSum - terminalEvents,
		MeasuredAt:      report.MeasuredAt,
	}

	verdict.LossDetected = verdict.Overhead < 0

	caveats := make([]string, 0, 3)
	if len(report.MissingTopics) > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d measured topic(s) do not exist on the broker (%s), so their records cannot be counted",
			len(report.MissingTopics), strings.Join(report.MissingTopics, ", "),
		))
	}

	if report.PartitionsUnavailable > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d partition(s) did not report offsets, so their records are missing from the total",
			report.PartitionsUnavailable,
		))
	}

	if report.RetainedCount < report.EndOffsetSum {
		caveats = append(caveats, fmt.Sprintf(
			"retention has removed %d record(s) that were written, so the broker no longer holds "+
				"everything the offsets count",
			report.EndOffsetSum-report.RetainedCount,
		))
	}

	verdict.Caveats = caveats
	verdict.Conclusive = len(caveats) == 0

	return verdict
}
