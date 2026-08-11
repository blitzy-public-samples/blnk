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
	// CompensateProvisioning undoes the half-completed provisioning a deferred result reported,
	// revoking the credential and removing the bindings the failed attempt attempted. It is the
	// obligation that comes with asking for DeferCompensation.
	CompensateProvisioning(ctx context.Context, result SubscriberProvisioningResult) error
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

	// reservedPrincipals are the SASL identities this deployment uses for its OWN Kafka
	// access — the administrative principal and the producer principal — recorded here so a
	// subscriber provisioning can refuse to overwrite one.
	//
	// SEC-05: provisioning performs a SCRAM UPSERT. Writing a credential for a principal that
	// is really Blnk's own would REPLACE that credential with a freshly generated password and
	// return it in the response body, handing an API caller either Write on every Blnk-owned
	// topic or the ability to mint credentials and grant ACLs. Configuration validation refuses
	// such a deployment at start-up, and this field is what lets the write path refuse it again
	// at the moment of the upsert — the two together, because configuration can be reloaded and
	// an admin client can be constructed from a configuration this process never validated.
	//
	// Empty when no SASL is configured, which is a broker requiring no authentication and so
	// has no privileged identity to protect.
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
		// SEC-05: the identities a subscriber credential must never be minted for.
		reservedPrincipals: reservedKafkaPrincipals(cnf.Kafka),
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
	// operation and scripts/kafka-provision.sh provision exactly the same topics
	// under whatever KAFKA_TOPIC_PREFIX is configured.
	//
	// EVERY OWNED PREFIX, not only the configured one. Outbox rows record their destination
	// at insert time, so a deployment that renamed its namespace still holds committed rows
	// naming the previous generation's topics — and each prefix an operator declares in
	// KAFKA_HISTORICAL_TOPIC_PREFIXES is a namespace the publisher will still write to.
	// Assuring only the live generation would leave those topics unprotected against having
	// been deleted, or absent entirely on a broker restored from elsewhere, and every publish
	// of the rows that name them would then either fail or silently auto-create a topic with
	// one partition and the wrong replication factor. Declaring a prefix and assuring it are
	// two halves of one decision.
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

	// KeyScoped says that the registry row this request was built from declares a
	// partition-key prefix, and therefore that RECORD-LEVEL READ MUST BE WITHHELD.
	//
	// It is a bool rather than the prefix itself, and that is deliberate: the prefix's
	// VALUE has no representation at the broker — Kafka's authorizer has no message-key
	// dimension — while its PRESENCE decides the shape of the grant. Carrying the value
	// here would invite somebody to look for a binding to put it in, and the only bindings
	// that could hold it are wrong: a PREFIXED topic pattern would widen the grant to every
	// topic sharing the prefix while appearing to narrow it.
	//
	// True produces topic Describe and consumer-group Read and NO topic Read, so every
	// fetch such a principal attempts is refused by the broker with any client from any
	// host. Its records reach it through the key-authorising component the deployment has
	// DECLARED in front of the brokers (KAFKA_KEY_SCOPE_ENFORCEMENT), which applies the prefix
	// before returning anything; Blnk ships no such component and serves no records itself, so
	// with none declared issuance refuses such a row outright. False produces the ordinary
	// grant, in which the topic list IS the boundary and the broker keeps all of it.
	//
	// It is set by NewSubscriberProvisioningRequest from the row, never by a caller
	// choosing a boundary: a request that could ask for record access on a key-scoped
	// subscriber would be a way to defeat the enforcement point through an ordinary
	// authorized call.
	KeyScoped bool

	// DeferCompensation asks provisioning to REPORT a needed compensation instead of
	// performing it. PERF-P09.
	//
	// The compensation is two administrative round trips on their own ten-second budget, and
	// they run on the failure path — which is reached, most often, because a five-second
	// provisioning budget has just expired. Performing them inline therefore adds their whole
	// duration to a response that has already run out of time, and requirement R-7's ceiling
	// is on the response.
	//
	// So a caller that can finish the work elsewhere sets this, and provisioning returns
	// CompensationOwed with the bindings to undo. The caller must then call
	// CompensateProvisioning — the credential HAS been written, and leaving it is the one
	// state AUTH-01 forbids. It defaults to false so every caller that does not opt in keeps
	// the inline, fully-compensated behaviour, which is what a CLI and a test want.
	DeferCompensation bool

	// DeclaredKeyScope carries the partition key prefix the REGISTRY ROW records, for the sole
	// purpose of CHECKING KeyScoped against the row it claims to come from. It is never turned
	// into a binding, because there is no binding to turn it into.
	//
	// # Why an unenforceable value is carried at all
	//
	// This layer is the last thing between a request and a live SCRAM credential with Read on
	// real ledger topics, and Kafka's authorizer has no message-key dimension: a row recording a
	// key prefix describes a boundary no ACL can express. What closes that gap is the NARROWING —
	// KeyScoped withholds record-level Read, so the credential can fetch nothing and the stream
	// gateway delivers the subscriber's records with the prefix applied before they leave the
	// process.
	//
	// The narrowing is therefore the only thing that makes such a request safe, and this field is
	// what makes its ABSENCE detectable here. NewSubscriberProvisioningRequest sets both from the
	// same row, so the pair is consistent by construction; a request that names a scope while
	// asking for the ordinary unnarrowed grant did not come from that constructor, and it is
	// exactly the shape that would defeat the enforcement point through an ordinary authorized
	// call. validate refuses it — which matters because this layer is exported and reachable from
	// any caller: a CLI, a repair script, a future endpoint.
	//
	// It is deliberately NOT mapped onto a prefixed TOPIC pattern, which is the mistake it exists
	// to prevent: a prefixed topic binding would WIDEN the grant to every topic sharing that
	// string while appearing to narrow it.
	//
	// Empty is the ordinary case and provisioning proceeds. See validate.
	DeclaredKeyScope string
}

// NewSubscriberProvisioningRequest maps a registry row and a freshly generated password
// onto a provisioning request.
//
// It exists so the mapping from registry columns to access boundary is written once. The
// three fields that constitute the boundary — the principal, the consumer group and the
// authorised topics — must all come from the same row, and a caller assembling the
// request by hand could quietly pair one subscriber's principal with another's topics.
//
// PartitionKeyPrefix's VALUE is deliberately not mapped, and cannot be: Kafka's authorizer
// has no message-key dimension, so there is no ACL that restricts a consumer to a slice of
// a topic by key. Pretending to enforce it here — by, say, binding a prefixed TOPIC pattern
// instead of a literal one — would be strictly worse than not enforcing it: it would widen
// the topic grant to every topic sharing that prefix while appearing to narrow it.
//
// Its PRESENCE, however, is mapped, onto KeyScoped, and that is what makes the boundary
// real. A row declaring a key scope is provisioned WITHOUT topic Read: it keeps Describe and
// its consumer-group namespace, and the broker refuses every record fetch it attempts. The
// records it is entitled to reach it through the key-authorising component the deployment has
// declared in front of the brokers, which applies model.EventSubscriber.HasKeyAccess's rule —
// a byte-exact prefix test on the record key — before returning anything. Blnk does not ship
// that component: with none declared, issuance for such a row is refused rather than narrowed.
//
// The two readings this replaced were both wrong, in opposite directions. Issuance once
// REFUSED any row carrying a prefix, which withheld the only credential that can exist for a
// state the registry is designed to hold. Then it granted whole-topic Read and DISCLOSED that
// the prefix was the consumer's to apply — accurate as a statement and absent as a boundary,
// since a client that ignores the obligation reads every other ledger's records on the shared
// category topic. Withholding record-level Read withholds exactly the access the prefix was
// meant to deny, and nothing else. See the access-model note on model.EventSubscriber.
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

	// ACLBindings is how many bindings the subscriber's authorization implies, and so how
	// many the broker holds for it after a successful provisioning.
	ACLBindings int

	// ACLBindingsRemoved is how many OBSOLETE Blnk-owned bindings reconciliation deleted:
	// grants from an authorization this subscriber no longer has.
	//
	// A non-zero value on an ordinary re-issue is the observable proof that narrowing the
	// registry actually reached the broker. Before reconciliation existed this was always
	// zero in effect, and a narrowed subscriber kept reading the topic it had lost.
	ACLBindingsRemoved int

	// ForeignACLBindings is how many bindings on this principal Blnk does not provision and
	// deliberately did not touch — a hand-made grant, a DENY, a Write.
	//
	// Reconciliation names each one rather than deleting it, because ACL deletion has no undo
	// and an operator's deliberate binding is not Blnk's to remove. Non-zero is therefore
	// informational on its own: see ForeignACLBindingsGranting for the count that decides
	// whether a credential may be issued at all.
	ForeignACLBindings int

	// CompensationOwed reports that provisioning failed after the credential was written and
	// that the caller asked for the compensation to be DEFERRED, so it has not run. PERF-P09.
	//
	// True obliges the caller to call CompensateProvisioning with this result: a credential
	// exists at the broker with no authorization boundary, which is the one state AUTH-01
	// forbids leaving behind. It is never true unless the request set DeferCompensation, and
	// it is mutually exclusive with Compensated — the compensation has either run here or been
	// handed back, never both.
	CompensationOwed bool

	// OwedBindings are the bindings the failed provisioning attempted, so a deferred
	// compensation deletes exactly those rather than everything the principal holds.
	//
	// Populated only alongside CompensationOwed. It carries no secret: an ACL entry names a
	// principal, a resource and an operation.
	OwedBindings []kafka.ACLEntry

	// ForeignACLBindingsGranting is how many of those foreign bindings GRANT access rather
	// than restricting it.
	//
	// It is ALWAYS zero on a successful return, because provisioning now refuses to issue a
	// credential to a principal whose effective permissions exceed its recorded authorization
	// — the same fail-closed shape as AuthorizerActive, and the same reason. It is retained as
	// a field rather than dropped so the property is assertable from the result, and so the
	// refusal path can report how many bindings caused it.
	ForeignACLBindingsGranting int

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

	// TopicRecordAccessGranted reports whether the bindings written include Read on the
	// subscriber's topics — that is, whether this principal may fetch records DIRECTLY from
	// the broker.
	//
	// False for a key-scoped subscriber, always, and that is the isolation boundary rather
	// than a detail: such a principal is granted Describe and its consumer-group namespace
	// and nothing else, so the broker refuses every fetch it attempts. Its records reach it
	// through the declared key-authorising component instead.
	//
	// It is reported rather than inferred so a caller's log line and the credential response
	// state what was actually written, and so a test can assert the grant shape from the
	// result instead of re-deriving it.
	TopicRecordAccessGranted bool

	// KeyScopeBoundaryVerified reports that a key-scoped subscriber's partition-key boundary
	// was CONFIRMED to be enforceable before this result was returned.
	//
	// It is true only when all three hold: the broker confirmed it enforces ACLs, the
	// bindings reconciled for this principal contain no topic Read, and reconciliation found
	// no foreign ALLOW binding widening the principal beyond them. Together those mean the
	// only path a record can take to this subscriber is the gateway that applies its prefix.
	//
	// Provisioning REFUSES rather than returning false for a key-scoped subscriber — see
	// ErrSubscriberKeyScopeUnenforced — so on a successful return this is true whenever the
	// request was key-scoped. It is retained as a field so the property is assertable from
	// the result, exactly as AuthorizerActive is, and so the refusal path can report it.
	//
	// It is false for a subscriber with NO key scope, because there is no key boundary to
	// verify: the topic grant is the whole boundary and the broker keeps it.
	KeyScopeBoundaryVerified bool

	// CredentialWritten reports whether a SCRAM credential is believed to exist at the
	// broker when this result was returned.
	//
	// It exists for the failure path: a caller handed an error needs to know whether a
	// credential now exists, because that is the difference between "retry" and "a live
	// principal is unaccounted for". It is cleared only when compensation CONFIRMED the
	// revocation, so it describes the state the broker is actually left in rather than the
	// step that ran.
	CredentialWritten bool

	// Compensated reports that provisioning failed after the credential was written and that
	// the credential was then successfully revoked.
	//
	// True means the broker was confirmed clean: the caller must not persist an issuance
	// record, and the secret it generated is dead. False alongside an error and
	// CredentialWritten means the revocation ALSO failed, so a principal that can
	// authenticate is unaccounted for and needs manual attention; the log line names it.
	// Removal of the attempted ACL bindings is best-effort and is deliberately NOT part of
	// this flag — inert bindings for a principal that no longer exists grant nothing.
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
// authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer. WITHOUT IT
// THERE IS NO BOUNDARY AT ALL: with no authorizer configured, every request from every
// principal is permitted, so a credential minted against such a broker reads every topic
// Blnk owns — and an isolation test run against it would pass while proving nothing.
//
// Such a broker also refuses the ACL administrative APIs themselves: Create, Delete and
// Describe all answer SECURITY_DISABLED, because there is no authorizer to answer them. That
// is what makes the state DETECTABLE rather than merely dangerous, and it is what the probe
// below reads — but it also means provisioning cannot succeed even partially: the bindings
// this method would create are refused outright, so proceeding would leave a working
// credential with no bindings and unrestricted access.
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

	// SEC-05: BEFORE the authorizer probe and before any write, because a collision here means
	// the SCRAM upsert below would rotate one of this deployment's OWN credentials and return
	// it in the response. Configuration validation already refuses such a deployment at
	// start-up; this is the second gate, because configuration can be reloaded and an admin
	// client can be constructed from a configuration this process never validated. It costs one
	// slice comparison and it is the difference between an authorized API call minting a
	// subscriber credential and an authorized API call minting a Kafka superuser one.
	if err := a.requirePrincipalNotReserved(principal); err != nil {
		return result, err
	}

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

	// AUTH-02: RECONCILED, not merely created. The broker's Blnk-owned bindings for this
	// principal are made exactly what the row's authorization implies, so a topic removed from
	// authorized_topics since the last issuance has its Read and Describe bindings DELETED
	// here. Creating without deleting made a subscriber's broker-side grant the union of every
	// authorization it had ever held: narrowing the registry left the old access live, and
	// revocation could not find it either, because revocation derives what to delete from the
	// row that no longer names the topic.
	bindings := req.aclEntries()

	reconciliation, err := a.reconcileSubscriberACLs(ctx, principal, bindings)

	// SEC-06: a key-scoped subscriber's boundary is VERIFIED before a password can be
	// returned, and the verdict is assigned to the SAME err the reconciliation reports through
	// — so a refusal here takes the AUTH-01 compensation path below and the credential that
	// was just written is revoked. Verifying after the response would be verifying nothing:
	// the secret would already be in the caller's hands.
	if err == nil {
		err = verifyKeyScopeBoundary(req, bindings, reconciliation, result.AuthorizerActive)
	}

	if err != nil {
		// AUTH-01: the credential exists and its boundary does not. That combination is the
		// one state provisioning must never leave behind, because the principal can
		// authenticate — so it is compensated by revoking the credential before returning.
		//
		// THE RESULT REPORTS WHAT COMPENSATION ACHIEVED, not that it ran. Only a revocation
		// the broker confirmed clears CredentialWritten and sets Compensated; a revocation
		// that itself failed leaves CredentialWritten true and Compensated false, which is
		// the state the broker is actually left in and the only signal that tells the caller
		// a live principal needs manual revocation.
		//
		// DEFERRED when the caller asked for it: the two round trips are handed back instead of
		// being added to a response whose budget has usually already expired. CredentialWritten
		// stays TRUE, because it is true — the credential exists and nothing has revoked it yet —
		// and the caller is obliged to run the compensation.
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
	// Deny bindings only. A foreign ALLOW made reconcileSubscriberACLs return an error above,
	// so a successful provisioning is by construction one with no widening binding on the
	// principal — which is what makes the issuance response's enforced-access declaration true
	// rather than aspirational.
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

	if err := r.validateKeyScope(); err != nil {
		return err
	}

	return validateSCRAMPassword(r.Password.reveal())
}

// validateKeyScope refuses a request that declares a key scope it does not narrow the grant for.
//
// # What is actually dangerous here
//
// Kafka's authorizer evaluates resources and has no message-key dimension, so no ACL can express
// a partition-key prefix. A credential minted with the ORDINARY grant for a row that records one
// therefore reads EVERY record on every granted topic — other ledgers' and other subscribers'
// included — while the row it came from says the subscriber may see only the records whose key
// carries one prefix. Disclosing the prefix to the holder does not close that gap either: a
// client-side filter is a convention, and the party asked to honour it is the one holding the
// credential.
//
// What closes it is KeyScoped: record-level Read is withheld, every fetch such a principal
// attempts is refused by the broker from any client on any host, and the subscriber's records
// reach it through the declared key-authorising component with the prefix applied first. So the
// request that must be refused is not the key-scoped one — it is the one that names a scope
// while asking for the unnarrowed grant.
//
// # Why the check is here as well as in the service
//
// NewSubscriberProvisioningRequest sets both fields from one row, so it cannot produce that pair.
// This function is exported, though, and reachable from any caller — a CLI, a repair script, a
// future endpoint — and a request assembled by hand is precisely how the enforcement point would
// be defeated through an ordinary authorized call. The service layer's own guard,
// requireProvisionableKeyScope, refuses a scope that NOTHING enforces before a secret exists;
// this one refuses a scope this request would not enforce, at the boundary that talks to the
// broker.
//
// # What it does not do
//
// It does not attempt to enforce the prefix, and it must not: the only binding shaped anything
// like a key scope is a PREFIXED topic pattern, which matches topic NAMES and would widen the
// grant to every topic sharing the string while appearing to narrow it.
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
//   - DEAD-LETTER topics, because every DLT carries other subscribers' failed events
//     together with Blnk's own failure metadata, so it has no subscriber audience. That
//     exclusion is structural rather than listed, since SubscriberGrantableTopics composes
//     only "<prefix>.<category>" names and a ".dlt" name can never be one.
//   - THE INTERNAL CATEGORY TOPIC "<prefix>.system", UNLESS this deployment has declared
//     KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS. system.error's frozen payload renders Blnk's
//     error text verbatim and the category is the catalogue's catch-all, so the default
//     grantable set is the four tenant categories; model.SubscriberPrivilegedEventCategories
//     owns that decision, and the refusal below names the variable rather than the allowlist
//     when the name IS a category topic and only the acknowledgement is missing.
//
// IsSubscriberGrantableTopic is the single test for all of them, so the API layer, this path
// and the provisioning script cannot disagree about what is grantable.
//
// A PRIVILEGED GRANT THAT PASSES IS LOGGED, at warning level, once per provisioning. It is
// legitimate — the deployment declared it and the subscriber asked for it — and it is also the
// one grant in the model that hands a tenant Blnk's own operational detail, so the binding
// that carries it should be visible in the log an operator reads afterwards rather than only
// in the registry row.
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
// Kafka's own implication rules make Read imply Describe on the same resource, so for a
// subscriber that HAS topic Read the Describe binding is technically redundant, and the group
// Read binding already implies the group Describe that FindCoordinator and OffsetFetch
// require. Describe is nonetheless requested explicitly on topics so that the grant is
// auditable from the binding list alone, without the reader having to know the implication
// table. The redundancy is deliberate and costs one binding per topic — and for a key-scoped
// subscriber it is not redundant at all, because Describe is then the ONLY topic operation
// granted.
//
// # KEY-SCOPED SUBSCRIBERS ARE GRANTED NO TOPIC READ, and that is the isolation boundary
//
// When the request is KeyScoped the Read binding is omitted from every topic. The row asks
// for a narrower boundary than a whole topic — records whose key carries a given prefix — and
// Kafka's authorizer cannot express it, so granting Read anyway would grant the whole topic
// while the registry, the response and the runbook all described something smaller. That is
// what this omission ends: such a principal can list its topics and their offsets, can join
// its own consumer group, and CANNOT FETCH A SINGLE RECORD, with any client, from any host.
//
// Its records reach it through the key-authorising component the deployment has declared in
// front of the brokers, which applies the recorded prefix before returning anything. The grant
// here and that component are the two halves of one boundary: withholding Read is what makes
// the declared component the only path, and the declaration is what stops the withholding from
// being a dead end — which is why issuance refuses a key-scoped row when nothing is declared,
// rather than provisioning a principal that can fetch nothing at all.
//
// THE DECLARATION IS VERIFIED BEFORE THIS FUNCTION EVER RUNS (SEC-01). Issuance binds the
// recorded prefix at that component's control endpoint over an authenticated call and requires
// it to confirm this exact principal against this exact prefix; a component that cannot be
// reached, or that confirms a wider prefix, refuses the issuance with
// SUBSCRIBER_KEY_SCOPE_UNATTESTED. So the omission below is never taken on the strength of two
// configuration values alone — see (*EventSubscriberService).attestKeyScope.
//
// # AND THE WHOLE-TOPIC SHAPE IS A DECLARED DECISION, not the shape reached by configuring
// nothing
//
// The Read-bearing shape above grants every record on each listed topic — every ledger's, and
// every other subscriber's. That is the mandated access model, correct for a single-tenant
// ledger or a trusted internal consumer, and it is the only shape Kafka can enforce for a
// subscriber that records no prefix. What it is no longer is a default: under a declared
// key-scoped model, issuance refuses a prefix-less subscriber outright with
// SUBSCRIBER_KEY_SCOPE_REQUIRED, and in secure mode with NO model declared it refuses with
// SUBSCRIBER_SHARED_TOPIC_ACCESS_UNACKNOWLEDGED until an operator has said which deployment
// this is. Both refusals are ordered ahead of provisioning, so this function only ever builds
// bindings for a shape somebody chose.
//
// The group binding is retained for such a subscriber even though it cannot consume from the
// broker, and deliberately: the namespace is RESERVED by that binding, so no other principal
// can take it, and clearing the row's key scope later restores direct consumption without
// having to re-reserve anything.
//
// THOSE TWO DIMENSIONS ARE THE WHOLE BOUNDARY, and that is why a request carrying a
// declared key scope never reaches this function: validate refuses it. A topic granted
// LITERAL Read is readable IN FULL, whatever key each record carries, so there is no
// binding here that could confine a subscriber to one ledger's records — and the boundary
// this list describes must be the same width as the registry row it came from.
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

	// AUTH-04: the LAST gate, and the only unbypassable one. Three call sites reach this
	// function — provisioning's reconciliation, PruneSubscriberAccess/GrantSubscriberAccess, and
	// ReconcileSubscriberACLs — and a guard placed at any one of them leaves the other two open.
	// Placing it here means every ACL Blnk creates, on every path present or future, has had its
	// shape checked against the two shapes a subscriber grant is made of.
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
// ACL RECONCILIATION — AUTH-02
//
// Provisioning used to CREATE bindings and never remove any, which made a subscriber's
// broker-side grant the UNION of every authorization it had ever held. Narrowing
// authorized_topics updated the registry and left the removed topic's Read and Describe
// bindings live at the broker: the registry said the access was gone, the subscriber kept
// consuming, and no request failed to say otherwise. Revocation could not clean it up either,
// because it derives the bindings to delete from the CURRENT row — the row that no longer
// names the topic.
//
// So the grant is RECONCILED rather than accumulated: the broker is read, the difference
// against the desired set is computed, and the surplus is deleted as well as the missing
// created.
//
// # What Blnk owns on a principal it minted, and what it will not touch
//
// Reconciliation converges exactly the shapes Blnk provisions — Allow Read/Describe on a
// LITERAL topic, and Allow Read on a PREFIXED group — and only on principals in its own
// 'blnk-sub-' namespace. Every other binding on such a principal is REPORTED AND LEFT ALONE:
// a DENY, a Write, a cluster or transactional-id resource, a prefixed topic pattern. Those are
// an operator's deliberate work, ACL deletion has no undo, and a reconciliation wide enough to
// tidy them is wide enough to delete something load-bearing that nobody remembers creating.
//
// The asymmetry is deliberate and is the safe direction: Blnk removes only what it would
// itself have created, and names anything else so an operator can decide.
//
// Leaving a foreign binding in place is NOT the same as accepting it, though, and the two were
// conflated. Reconciliation reports the binding and moves on; PROVISIONING then refuses to issue
// a credential while any foreign binding GRANTS access, because the boundary a credential is
// issued against is the one the broker will actually enforce — the union of Blnk's converged
// grant and everything else on the principal. A foreign DENY is exempt, since it can only
// tighten that union. See foreignACLBindingGrantsAccess and the refusal in
// ProvisionSubscriberPrincipal.
// ---------------------------------------------------------------------------------------

// SubscriberACLReconciliation reports what one reconciliation of a principal's bindings did.
//
// It is returned rather than only logged because the counts are what a caller reports and a
// test asserts: "narrowing this subscriber removed two bindings" is the observable fact that
// the registry and the broker now agree, and Foreign is how an operator learns that something
// outside Blnk's ownership is also granting this principal access.
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

	// ForeignAllow describes every binding on this principal that Blnk does not own and that
	// WIDENS its access — an ALLOW of a shape Blnk never provisions, or a binding whose
	// permission type the broker did not state definitively.
	//
	// A NON-EMPTY ForeignAllow MEANS THE EFFECTIVE GRANT IS BROADER THAN THE REGISTRY
	// RECORDS, and by an amount Blnk cannot describe. That is why it is separated from
	// ForeignDeny rather than counted alongside it: the two are opposite facts. A widening
	// binding invalidates every isolation statement Blnk would otherwise make about the
	// principal, so an operation that would hand out a credential or converge a widening
	// REFUSES while any are present — see reconcileSubscriberACLs.
	//
	// Blnk still does not delete them. ACL deletion has no undo and an operator's deliberate
	// binding is not Blnk's to remove; refusing names the problem and leaves the decision
	// where it belongs, which is the same asymmetry the rest of this surface follows.
	ForeignAllow []string

	// ForeignDeny describes every foreign binding that can only NARROW the principal's
	// access: an explicit DENY.
	//
	// These are reported and retained and are never fatal. A DENY subtracts from what the
	// ALLOW bindings grant, so its presence means the subscriber can read less than its grant
	// describes — which cannot be an isolation failure, and which an operator may well have
	// added on purpose. Refusing on one would block credential issuance for a principal that
	// is more restricted than Blnk requires.
	ForeignDeny []string
}

// Foreign returns every foreign binding, widening ones first.
//
// It exists for logging and counting, where the distinction does not matter and a single list
// reads better. Anything that ACTS on the difference must read ForeignAllow directly:
// collapsing the two is exactly the conflation that let a widening binding be reported with
// the same weight as a harmless DENY and then survive issuance.
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

// describeSubscriberACLs reads every ACL binding the broker currently holds for a principal.
//
// The filter is deliberately wide on every dimension except the principal: an empty resource
// name, an empty host and the Any pattern, operation and permission filters are all encoded as
// null on the wire and match anything, so what comes back is the principal's COMPLETE grant.
// That completeness is the point — a reconciliation that could only see the bindings it
// expected could never discover the ones it had to remove.
//
// The principal is re-checked in Go on every returned binding. The broker's own filter is
// exact, so this is belt and braces rather than necessity; it costs one comparison and it is
// what stops a broker or client quirk turning a reconciliation into somebody else's
// revocation.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - principal string: the bare SASL username. Required.
//
// Returns:
//   - []kafka.ACLEntry: every binding held for the principal, in the shape createACLBindings
//     and deleteACLBindings both speak.
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
// It is the ownership test the whole reconciliation rests on, so it is written as an
// allowlist of the two shapes aclEntries produces and nothing else. Anything wider would give
// reconciliation permission to delete an operator's work; anything narrower — matching on the
// topic name, say — would fail to recognise a binding from a previous authorization as Blnk's
// own, which is precisely the binding that has to be removed.
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

// foreignACLBindingWidens reports whether a foreign binding can GRANT access, as opposed to
// only taking it away.
//
// It is called only on bindings blnkManagedACLBinding has already rejected, so every input is
// a binding Blnk does not own. The question here is the different and security-critical one:
// does it make the principal's effective grant broader than the registry records?
//
// # Only an unambiguous DENY is treated as harmless
//
// Kafka's permission type has four values, and the two that are neither Allow nor Deny —
// Unknown and Any — are treated as WIDENING. That is deliberate and it is the fail-closed
// reading: a binding whose permission the broker did not state definitively cannot be shown to
// narrow anything, and "I could not tell" must never be recorded as "it is safe". Any is
// additionally a FILTER value rather than a binding value, so seeing it on a described binding
// means the client or broker returned something this code does not understand, which is exactly
// when guessing is worst.
//
// Parameters:
//   - binding kafka.ACLEntry: a foreign binding.
//
// Returns:
//   - bool: false only for an explicit Deny; true for everything else.
func foreignACLBindingWidens(binding kafka.ACLEntry) bool {
	return binding.PermissionType != kafka.ACLPermissionTypeDeny
}

// reservedKafkaPrincipals returns the SASL usernames this deployment uses for its own Kafka
// access, trimmed and deduplicated, with blanks dropped.
//
// It is the administrative principal and the producer principal — the two identities a
// subscriber credential must never be minted for. Both are read even when only one is
// configured, because "not configured" and "configured to the same thing" are different
// situations and only the first is safe to ignore.
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

// ErrSubscriberPrincipalReserved is returned when a subscriber's DERIVED principal collides
// with one of this deployment's own Kafka identities.
//
// # The escalation this closes
//
// Provisioning performs a SCRAM UPSERT: an existing credential for the principal is REPLACED
// with a freshly generated password, which the issuance response then returns. So a subscriber
// whose derived principal happens to equal the administrative or producer username does not get
// a new identity — it gets THAT identity's credential rotated and handed to the caller, along
// with either Write on every Blnk-owned topic or the ability to mint credentials and grant ACLs.
//
// Configuration validation refuses such a deployment at start-up. This is the second gate, and
// it exists because the first one is not sufficient on its own: configuration can be reloaded,
// and an admin client can be built from a configuration this process never validated. The check
// is cheap, it runs immediately before the write it protects, and a collision here is a
// deployment fault rather than a caller fault — so it is stated plainly rather than as a
// validation message aimed at whoever made the request.
var ErrSubscriberPrincipalReserved = errors.New(
	"kafka admin: the subscriber's derived principal is one of this deployment's own Kafka " +
		"identities, so provisioning it would rotate and return that credential; KAFKA_SASL_USER " +
		"and KAFKA_SASL_ADMIN_USER must not sit inside the reserved 'blnk-sub-' namespace",
)

// requirePrincipalNotReserved refuses a principal that is one of the deployment's own.
//
// Comparison is exact and case-sensitive on the trimmed value, because Kafka principals are
// case-sensitive: "blnk-sub-x" and "BLNK-SUB-X" are two different identities at the broker, so
// folding case here would refuse a provisioning that is in fact safe.
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

// ErrSubscriberForeignACLGrant is returned when a subscriber principal carries an ALLOW ACL
// binding Blnk did not provision.
//
// # Why this is fatal rather than a warning
//
// The whole isolation guarantee is "the principal may Read and Describe exactly these topics,
// and join exactly this consumer-group namespace". A foreign ALLOW binding is, by definition,
// access outside that set — and Blnk cannot say how much, because the binding may name any
// resource, pattern, host and operation the broker supports.
//
// It used to be detected, logged at warning level, and then provisioning continued: a
// credential was minted, the password was returned, and the response declared an enforced
// access boundary that the broker was not in fact enforcing. Nothing in the response said
// otherwise, and the only trace was one log line in the stream of a SUCCESSFUL request.
//
// Now the operation refuses. Nothing is deleted — ACL deletion has no undo and an operator's
// deliberate binding is not Blnk's to remove — so the remedy is operational and the message
// says so, and the caller's compensation revokes any credential that was already written.
var ErrSubscriberForeignACLGrant = errors.New(
	"kafka admin: this subscriber principal carries ALLOW ACL bindings Blnk did not provision, " +
		"so its effective access is broader than the subscriber registry records and cannot be " +
		"stated; remove the foreign bindings with kafka-acls, or move them to a principal outside " +
		"the reserved subscriber namespace, and retry",
)

// refuseForeignACLGrant builds the fail-closed error for a principal carrying foreign ALLOW
// bindings.
//
// The bindings are NAMED in the error, already sanitized and bounded by the caller, because the
// remedy is to remove them by hand and an operator who cannot see which ones they are has to go
// and describe the ACLs again. The principal is named for the same reason.
//
// It wraps ErrSubscriberForeignACLGrant so a caller can classify it with errors.Is without
// matching on the message.
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
// It is the single place a foreign binding is rendered, so every reader — the warning, the
// refusal error and any test — sees identical text for identical bindings. The resource name and
// the host are sanitized and length-bounded because both are operator-supplied strings that end
// up in a log line and in an error body, where an unbounded value with newlines in it can forge
// log structure.
//
// Parameters:
//   - report *SubscriberACLReconciliation: the report being assembled. Must not be nil.
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

// aclBindingKey renders a binding as the tuple that identifies it, so two bindings can be
// compared as set members.
//
// Every dimension the broker evaluates is included, HOST INCLUDED. A binding that differs only
// by host is a different grant — it admits a different set of clients — so treating the two as
// the same member would leave a stale host-scoped grant in place while reporting the set
// converged.
func aclBindingKey(binding kafka.ACLEntry) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s",
		binding.ResourceType, binding.ResourceName, binding.ResourcePatternType,
		binding.Principal, binding.Host, binding.Operation, binding.PermissionType)
}

// ErrSubscriberBindingShapeUnsupported is returned when a binding Blnk is about to CREATE is
// not one of the two shapes it owns.
//
// It exists because an ACL mistake is silent. The broker accepts any well-formed binding, so a
// widened one produces no error, no log and no failing request — the only symptom is a
// subscriber that can read somebody else's ledger, discovered by whoever notices first.
var ErrSubscriberBindingShapeUnsupported = errors.New(
	"kafka admin: refusing to create an ACL binding outside the two shapes a subscriber grant is " +
		"made of (Read/Describe Allow on a LITERAL topic name, Read Allow on a PREFIXED consumer " +
		"group namespace)",
)

// validateDesiredACLBindings asserts that every binding about to be WRITTEN is one Blnk owns.
//
// # Why the desired set is checked and not only the observed one
//
// blnkManagedACLBinding is the ownership allowlist, and reconciliation already applies it to
// what the broker REPORTS. That catches a widened binding one reconcile too late: the binding
// is created, it is live, and the next reconcile classifies it as a foreign ALLOW and refuses —
// so the failure mode of a widening bug was "grant the access, then jam the subscriber's
// provisioning permanently". Applying the same allowlist to the desired set inverts that: the
// widened binding is never written, and the operation fails before it touches the broker.
//
// Three properties are checked, and each corresponds to a way a subscriber grant could become
// broader than the registry records:
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
// It is deliberately NOT a check that the names equal the registry's topic list. That comparison
// belongs to admission, where the list is known, and repeating it here would need the row
// threaded through a function whose whole job is to be a pure guard on a slice of bindings.
//
// Parameters:
//   - principal string: the bound principal, already prefixed as Kafka states it.
//   - desired []kafka.ACLEntry: the bindings aclEntries produced. Empty is valid and means
//     "authorised for nothing", which is a legitimate instruction.
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

// reconcileSubscriberACLs makes the broker's Blnk-owned bindings for a principal EXACTLY the
// desired set.
//
// # The order is delete-then-create, and it is the safe one
//
// Every partial failure must leave the principal with FEWER rights than the registry records,
// never more — the same rule RevokeSubscriber follows. Deleting first satisfies that: a
// reconciliation that dies between the two steps has removed access it was going to re-grant,
// which the next reconciliation restores, whereas creating first and failing to delete leaves
// live access the registry says is gone.
//
// # An empty desired set is a legitimate instruction
//
// A subscriber authorised for nothing is the fail-closed default of a fresh registration and
// the state a caller reaches by clearing the topic list. Reconciling to it removes every
// Blnk-owned binding, which is exactly what "authorised for nothing" has to mean at the
// broker. It is NOT treated as "no work to do", because that reading is what let a narrowing
// to empty silently leave a full grant in place.
//
// Parameters:
//   - ctx context.Context: cancels both round trips.
//   - principal string: the bare SASL username.
//   - desired []kafka.ACLEntry: the bindings the subscriber's recorded authorization implies.
//     May be empty.
//
// Returns:
//   - SubscriberACLReconciliation: what was found, removed and created. Populated as far as
//     the operation got, even on error.
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

	// AUTH-04: checked BEFORE the describe, so a widened desired set costs no round trip and
	// leaves the broker untouched.
	//
	// It is NOT the authoritative gate — createACLBindings is, because it is the one function
	// that writes and every path reaches it. This one is here for a different reason: this
	// function DELETES surplus bindings before it creates the desired ones, so without an early
	// refusal a widened set would first strip the subscriber's live bindings and only then fail.
	// The end state would be safe — narrower than recorded, which is the direction every partial
	// failure here must fall — but the subscriber would lose working access to a request that was
	// never going to be applied.
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

	// AUTH-03: REFUSED HERE, before anything is deleted or created, so a principal whose
	// effective grant Blnk cannot state is left exactly as it was found.
	//
	// A foreign ALLOW binding is access outside the boundary the registry describes, by an
	// amount Blnk cannot bound. It used to be logged and then ignored: the reconciliation
	// reported success, the caller minted the credential, and the issuance response declared
	// an enforced boundary the broker was not enforcing. Refusing before the mutations means
	// the broker state is unchanged, and the caller's own compensation revokes the credential
	// it had already written — so no password can be returned under an unknown grant.
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

// PruneSubscriberAccess removes every Blnk-owned binding the broker holds for a subscriber
// that its RECORDED authorization does not imply, and creates nothing.
//
// # Why narrowing is a separate call from granting
//
// An authorization change has to be applied in three steps — prune at the broker, persist the
// row, then grant at the broker — because there is no transaction spanning Blnk and Kafka and
// only that order leaves every partial failure fail-closed:
//
//   - Pruning first means a NARROWING is already in force at the broker before the row claims
//     it is. If the persist then fails, the subscriber has lost access the registry still
//     records — recoverable by re-running the update, and safe in the meantime.
//   - Granting last means a WIDENING reaches the broker only after the row records it. If the
//     grant then fails, the subscriber has less access than the registry records — again
//     recoverable, again safe.
//
// Doing both around a single persist, or persisting first, makes one of those two cases
// fail-OPEN: live broker access that the registry says was revoked, which is the defect this
// whole surface exists to close.
//
// Parameters:
//   - ctx context.Context: cancels the round trips.
//   - subscriber *model.EventSubscriber: the row carrying the authorization to converge on.
//     Its principal and consumer group are DERIVED, never taken from the request.
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
			// CLASSIFIED AND REPORTED, NEVER REFUSED. This is the one operation on this
			// surface that must not fail closed on a foreign ALLOW binding, and the reason is
			// the direction it moves in: pruning only ever REMOVES access, so refusing it
			// would leave the subscriber with MORE access than the operator asked for —
			// precisely the outcome the refusal exists to prevent. The widening binding is
			// surfaced on the report so the caller and the log say so, and the next operation
			// that would hand out a credential or converge a widening is the one that refuses.
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

// GrantSubscriberAccess creates the bindings a subscriber's recorded authorization implies,
// and removes nothing.
//
// It is the third step of the three PruneSubscriberAccess documents, and it is idempotent:
// Kafka's CreateAcls accepts a binding that already exists, and the reconciliation skips the
// ones the broker already holds, so re-running it is a no-op rather than a duplicate.
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

	// AUTH-03: a WIDENING refuses, for the same reason issuance does. Converging a wider grant
	// on a principal whose effective access Blnk cannot state would report the registry and the
	// broker as agreeing when they demonstrably do not. Refusing leaves the subscriber with the
	// access it already had — less than the row now records, which is the documented safe
	// direction this whole three-step surface is built around, and which the caller reports as
	// retryable.
	//
	// Pruning is deliberately exempt: see PruneSubscriberAccess.
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

// subscriberDesiredBindings derives the bindings a registry row implies, and the principal
// they belong to.
//
// It goes through NewSubscriberProvisioningRequest so that the desired set is produced by the
// SAME code that provisioning binds — a second derivation here would be a second definition of
// the access boundary, and reconciliation would then converge on a set provisioning never
// creates, deleting and recreating the same bindings on every call.
//
// The request is validated for everything except the password, which plays no part in a
// binding: a row whose principal or consumer group is not derived from its identifier is
// refused here exactly as it would be at issuance.
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
func (a *KafkaAdminClient) AuthorizerActive(ctx context.Context) (_ bool, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "describe_authorizer")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

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

// ErrForeignACLGrantsAccess reports that the principal carries ACL bindings Blnk did not create
// which grant it access wider than its recorded authorization, so no credential may be issued.
//
// It is a sentinel for the same reasons as ErrAuthorizerNotEnforcing: the subscriber service,
// the API layer and a test all recognise the refusal without matching message text.
//
// It is a SIBLING of that error rather than a variation on it, because the two describe the two
// ways an authorization boundary can be absent. There, the broker would not enforce the
// bindings; here, the bindings themselves say more than the registry does. Either way the
// credential would authenticate a principal whose real permissions are not the ones Blnk
// recorded, which is the state credential issuance exists to prevent.
var ErrForeignACLGrantsAccess = errors.New(
	"kafka admin: the principal holds ACL bindings Blnk did not create that grant access beyond its " +
		"recorded authorization, so no subscriber credential may be issued",
)

// ErrSubscriberKeyScopeUnenforced reports that a subscriber recording a partition-key prefix
// could not be provisioned with a boundary that actually keeps it, so no credential may be
// issued.
//
// It is the third member of the family above, and it describes the third way an authorization
// boundary can be absent. With ErrAuthorizerNotEnforcing the broker would not enforce the
// bindings at all; with ErrForeignACLGrantsAccess the bindings themselves say more than the
// registry does; here the bindings Blnk itself was about to write would have granted
// record-level Read to a principal whose row asks for less than a whole topic.
//
// That combination is exactly the disclosure this release closes: whole-topic Read handed to a
// key-scoped subscriber lets it consume every other ledger's records on the shared category
// topic, whatever the response says about the prefix. A key-scoped subscriber is therefore
// granted Describe and its consumer-group namespace and no topic Read, and its records reach it
// through the declared key-authorising component, which applies the prefix before returning
// anything. If for any reason that shape cannot be established — the grant still carries Read,
// or the broker's enforcement is unconfirmed — issuance FAILS rather than returning a password
// whose isolation nobody verified.
//
// It is a sentinel for the same reasons its siblings are: the subscriber service, the API layer
// and a test all recognise the refusal without matching message text.
var ErrSubscriberKeyScopeUnenforced = errors.New(
	"kafka admin: a subscriber recording a partition-key prefix must be granted no record-level " +
		"Read, so that the declared key-authorising component is the only path its records can " +
		"take; the boundary could not be established, so no subscriber credential may be issued",
)

// bindingsGrantTopicRead reports whether a desired binding set includes Read on a TOPIC.
//
// It answers the one question that decides whether a principal can fetch records at all, and it
// is written over the bindings rather than over the request so that the answer describes what
// was, or is about to be, written at the broker. Deriving it from the request instead would make
// this a restatement of KeyScoped, and a restatement cannot catch the case it exists for: a
// binding set that does not match the decision the request asked for.
//
// Only ALLOW is considered. A DENY on a topic Read is a narrowing Blnk never provisions — and if
// an operator has added one, it takes access away rather than granting it, so it must not be
// read here as evidence of a grant.
//
// Parameters:
//   - bindings []kafka.ACLEntry: the desired set. Empty answers false, which is correct: a
//     principal granted nothing can read nothing.
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

// verifyKeyScopeBoundary is SEC-06: the check that a key-scoped subscriber's partition-key
// boundary is real before its credential can be returned.
//
// # Why a check at all, when aclEntries already omits the Read binding
//
// Because the omission is one line in a function several callers away from the response that
// promises the boundary, and the cost of that line regressing is silent: the credential would be
// issued, the response would declare the prefix enforced at the gateway, and the principal would
// hold whole-topic Read the whole time. This is the assertion that turns "the code that builds
// the grant is correct" into "the grant that was written is correct", made at the moment the
// password becomes returnable and against the bindings that were actually reconciled.
//
// # The three facts it requires, and why each is necessary
//
//   - THE BINDINGS CARRY NO TOPIC READ. Without this the broker admits the principal to the
//     whole shared topic and the gateway is one path among two.
//   - ACL ENFORCEMENT IS CONFIRMED. A broker with no authorizer permits every request from
//     every principal, so a grant that omits Read grants nothing less than one that includes
//     it. requireEnforcedAuthorizer has already refused such a broker before anything was
//     written; this re-reads the fact off the result so the verification stands on its own.
//   - NO FOREIGN ALLOW BINDING. reconcileSubscriberACLs refuses those outright, so this is
//     belt-and-braces — but a hand-made ALLOW Read on the topic would restore exactly the
//     access being withheld, and a check that ignored it would verify the wrong set.
//
// A request with NO key scope is verified vacuously: there is no key boundary to keep, the topic
// grant is the whole boundary, and the broker enforces all of it.
//
// Parameters:
//   - req SubscriberProvisioningRequest: the request, read for KeyScoped only.
//   - bindings []kafka.ACLEntry: the desired bindings that were reconciled.
//   - reconciliation SubscriberACLReconciliation: what reconciliation observed at the broker.
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
// It is the admin client's own per-request timeout, shared by the two round trips a
// compensation makes rather than granted to each: a broker that has stopped answering must cost
// the caller one budget, not one per call. Ten seconds is enough for two round trips against a
// broker that is refusing rather than hanging, which is the ordinary shape of the failure being
// compensated.
//
// It is a CEILING AND ONLY A CEILING, applying when no wall clock is in force.
// kafkaCleanupContext bounds the phase by the caller's SLA whenever there is one, so a
// compensation inside a five-second credential issuance never spends ten.
const kafkaCleanupBudget = kafkaAdminRequestTimeout

// kafkaCleanupContext derives the context a compensating broker operation runs on.
//
// It carries the caller's VALUES — trace context above all, so the cleanup appears under the
// span that caused it — and deliberately NOT the caller's cancellation. See
// compensateFailedProvisioning for the failure that made the distinction necessary: the
// commonest reason provisioning fails is its own deadline expiring, and a cleanup that
// inherited that deadline could never run on the occasion it exists for.
//
// # SLA-01: detached from cancellation, still inside the caller's wall clock
//
// Detaching from the cancellation is not licence to detach from the DEADLINE. "Fresh" once meant
// a brand-new TEN-second budget, and it was the single largest contributor to a five-second
// credential-issuance contract answering in twenty: an issuance whose broker call failed spent
// its own budget and then this one on top, with every individual timeout looking correct because
// they were merely serialised. Requirement R-7 states the ceiling over the RESPONSE, not over
// each call inside it. Detachment from CANCELLATION is what the cleanup needs; a new wall clock
// never was.
//
// So when the caller is operating under a wall clock — credential issuance is, and it is the only
// caller that makes a timed promise — this context is bounded by that clock's compensation phase
// instead. subscriberPhaseContext owns the arithmetic, and two parts of it matter here:
//
//   - THE DURABILITY RESERVE is subtracted, so the compensation cannot consume the slice held
//     back for writing the revocation obligation. A compensation that spent the last of the
//     budget would leave an exposure whose only record is a log line, which is the exposure the
//     obligation exists to remove.
//   - A FLOOR applies, because a zero-length context fails every call instantly: a compensation
//     reached slightly late would do nothing at all and report that it had tried. The floor is
//     small enough that it cannot meaningfully extend the response and large enough for one
//     round trip to a broker that is refusing rather than hanging.
//
// A caller with no wall clock keeps the ten-second bound, which is the right size for two round
// trips against a broker that is refusing rather than hanging.
//
// Neither bound makes this path the thing that guarantees cleanup. The service layer persists
// the revocation obligation BEFORE reaching here and finishes the revocation in the background
// when the budget is spent — see EventSubscriberService.recordFailure.
//
// Parameters:
//   - ctx context.Context: the caller's context, used for its values and its issuance deadline.
//
// Returns:
//   - context.Context: a context detached from the caller's cancellation and bounded either by
//     the caller's wall clock or by kafkaCleanupBudget.
//   - context.CancelFunc: must be called, conventionally by defer.
func kafkaCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return subscriberPhaseContext(ctx, subscriberDurabilityReserve, kafkaCleanupBudget, true)
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
// EVERY FAILURE IS LOGGED, AND THE CREDENTIAL REVOCATION'S OUTCOME IS RETURNED. The caller
// still returns the ORIGINAL provisioning error — replacing it with a cleanup error would hide
// the cause — but it needs to know whether the broker was actually left clean, because
// "compensated" and "a live principal nobody is tracking" are the two states a caller must
// answer differently. A binding-removal failure is logged and not returned: inert bindings for
// a principal that no longer exists grant nothing, so they are untidiness rather than access.
//
// # CLEAN-01: the cleanup runs on a FRESH deadline, not the caller's
//
// It used to inherit the provisioning context, and that context is the 5-second credential
// issuance budget — whose EXPIRY is one of the commonest reasons provisioning fails at all. So
// on the failure this function exists for, both round trips below were attempted against an
// already-cancelled context and returned immediately: nothing was revoked, `Compensated` was
// reported anyway, and the credential stayed live at the broker with nothing recording it. The
// compensation was busiest precisely when it could not work.
//
// A context detached from the caller's CANCELLATION — but not from its DEADLINE — makes the
// cleanup possible in exactly that case. It is BOUNDED rather than merely detached for the
// reason every other detached write in this codebase is: "finish what you owe" must not become
// "block the request indefinitely on a broker that has gone away". The two round trips share
// one window, so a failing broker costs the caller that window once, not twice.
//
// # AAP-02: the window is the REQUEST'S, not a fresh one
//
// Detaching used to mean a full ten seconds of its own here, on top of whatever the forward
// path had already spent — which is how a five-second endpoint came to answer in twenty. See
// kafkaCleanupContext: the absolute issuance deadline travels on the context as a value, is
// re-imposed here, and is rebased so a nested cleanup shares this window instead of opening
// another. A caller under no issuance deadline — a maintenance sweep, an explicit revocation —
// still gets the ten-second budget, because there is no endpoint bound to respect.
//
// Parameters:
//   - ctx context.Context: the provisioning context. It is used for its VALUES only — its
//     cancellation is deliberately not inherited, see above.
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
			// Nothing to delete is the desired end state, so it is success. This is what
			// makes revocation safe to retry, which both operators and the compensation
			// path depend on.
			continue
		}

		return fmt.Errorf("kafka admin: deleting the %s credential for principal %q: %w",
			SubscriberSASLMechanism, response.Results[i].User, resultErr)
	}

	logrus.WithField("principal_hash", subscriberLogLabel(principal)).
		Info("kafka admin: subscriber SCRAM credential revoked")

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
// # The deletion is BY PRINCIPAL, not by the current grant
//
// This used to delete exactly the bindings the registry row describes, derived the way
// provisioning derived them, on the reasoning that a broad filter could catch a binding an
// operator had made by hand. That reasoning holds where the principal SURVIVES — it is why
// compensation and reconciliation still delete precisely — and fails here, because the
// broker's bindings for a principal are the UNION OF EVERY GRANT IT HAS EVER HELD while the
// registry row describes only the latest. Deleting by the current grant therefore walks past
// every binding an earlier, wider grant left behind: a subscriber whose topics were narrowed
// and then deleted kept a live Read on the topic that was removed, with no row left anywhere
// to describe the access.
//
// The width is bounded by the principal itself. It is 'User:blnk-sub-<subscriber id>', a
// namespace Blnk issues and owns, so a principal-wide filter cannot reach another system's
// bindings — and every binding inside it belongs to a subscriber that is being deleted.
//
// Parameters:
//   - ctx context.Context: cancels the requests.
//   - subscriber *model.EventSubscriber: the registry row. Nil is refused, because there
//     would be no principal to revoke.
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
// # Why this exists at all
//
// A subscriber's authorization lives in two places: the authorized_topics column and the
// broker's ACL bindings. Only the first is a Blnk write. Narrowing the column and stopping
// there produced the worst possible outcome — the registry said the subscriber could read
// three topics, the broker still let it read four, and nothing anywhere disagreed out loud.
// Every reader of the registry, including the subscriber-isolation criterion, was reading a
// claim rather than the enforced boundary.
//
// # The order is not interchangeable
//
// REVOCATIONS FIRST, and their failure is fatal to the operation. A caller narrowing a
// grant must be able to treat success as "the removed topics are no longer readable", and
// the only way to keep that promise is to fail loudly when the broker refused. Additions
// come second because a failure there leaves the subscriber with FEWER rights than the
// registry claims, which over-restricts rather than over-permits — the safe direction.
//
// Reapplying the surviving bindings on every reconcile is deliberate: Kafka accepts an
// identical binding as a no-op, so a full desired-state create is idempotent, and it repairs
// a binding an earlier partial failure never wrote. Computing a precise additions-only diff
// would need a DescribeACLs round trip to be correct and would still be wrong the moment a
// binding was removed out of band.
//
// # It never touches the SCRAM credential
//
// Reconciling a grant is not a credential operation. The subscriber keeps authenticating
// with the secret it already holds — which is the entire point, since the alternative is
// forcing a credential rotation on every topic-list edit — so no password is needed and
// none is accepted.
//
// Parameters:
//   - ctx context.Context: cancels the broker round trips.
//   - subscriber *model.EventSubscriber: the registry row AFTER the change, whose
//     authorized_topics and consumer group describe the desired state.
//   - revokedTopics []string: the topics removed from the grant, whose bindings must go.
//     Empty means nothing was removed and only the desired state is reapplied.
//
// Returns:
//   - error: ErrKafkaAdminNotConfigured when no broker is configured; the broker's error
//     when a revocation or a creation failed. A revocation failure is returned before any
//     addition is attempted.
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
	// aclEntries — the single place a subscriber's binding shape is expressed — produces the
	// exact filters to delete. Restating the resource type, pattern type, host and operation
	// list here would be a second copy of that shape, and the two would drift.
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
		"principal_hash": subscriberLogLabel(principal),
		"filters":        len(filters),
		"removed":        removed,
	}).Info("kafka admin: subscriber ACL bindings removed")

	return nil
}

// deleteAllPrincipalBindings removes EVERY ACL binding the broker holds for one principal,
// with a single match-any filter.
//
// It is the deprovisioning counterpart to deleteACLBindings, which deletes an enumerated set.
// The distinction is which of the two questions is being answered:
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
// # The filter's shape is load-bearing in three places
//
// THE PRINCIPAL CARRIES THE "User:" PREFIX, because that is how a binding stores it. A filter
// naming the bare SASL username matches nothing, and Kafka answers that with a success
// carrying an empty MatchingACLs — so the revocation would report clean while removing
// nothing at all.
//
// THE RESOURCE NAME AND HOST ARE EMPTY STRINGS, not "*". Both fields are nullable in the
// protocol and an empty string encodes as null, which Kafka reads as match-any. A literal "*"
// is a resource NAMED "*", which matches only a binding on that name.
//
// THE COUNT IS REPORTED. Zero removed for a principal that was provisioned is worth seeing:
// it means either the bindings were already gone or the filter did not match, and the two are
// told apart by whether the credential was there.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - principal string: the BARE SASL username, as boundPrincipal returns it. The "User:"
//     prefix a binding stores is applied here, in one place, so no caller can forget it.
//     Required.
//
// Returns:
//   - error: nil when the broker holds no binding for the principal afterwards.
func (a *KafkaAdminClient) deleteAllPrincipalBindings(ctx context.Context, principal string) error {
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return errors.New("kafka admin: a principal is required to delete its ACL bindings")
	}

	// Applied here rather than expected from the caller. Every other filter field on this
	// request is a match-any sentinel, so the principal is the ONE exact term the delete turns
	// on — and the failure mode of getting it wrong is silent: Kafka answers an unmatched
	// filter with a success carrying an empty MatchingACLs, so the revocation reports clean
	// while every binding stays live.
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

// LagSamples renders the report as inventory entries for the asynchronous consumer-lag
// gauges, one per topic measured.
//
// It is the bridge between a measurement and its telemetry, and it deliberately does not
// publish: the observable gauges export the CURRENT INVENTORY, so only the periodic collector
// — which is the one caller that knows the whole registry — may publish, and it does so once
// per tick from the samples every subscriber contributed. An ad-hoc lag query therefore
// measures without perturbing what is exported, which under the previous synchronous gauge it
// could not do.
//
// A topic listed in MissingTopics produces NO sample. It does not exist on the broker, so
// there is no partition count to report as unmeasured and no lag to withhold; the condition is
// reported by ConsumerLag's warning and by the MissingTopics field, and the absence of a series
// is the honest telemetry for a topic that is absent.
//
// Returns:
//   - []metrics.ConsumerLagSample: one entry per measured topic, nil when nothing was measured.
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
			// different diagnoses — one of which ("the group is not consuming this topic")
			// sends an operator to the consumer rather than to the broker.
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

// buildPartitionLag assembles one partition's entry from its offset bounds and committed
// offset.
//
// # A partition is unavailable if EITHER side of the subtraction is unreadable
//
// Lag is end offset minus a baseline, so it needs both. An unreadable END OFFSET has always
// marked the partition unavailable; an unreadable COMMITTED OFFSET did not, and that asymmetry
// was the defect (OBS-21): the missing commit was scored as "never committed", which is a
// deliberate FULL-LAG policy, so an unmeasurable partition produced the largest possible number
// instead of no number at all. Both inputs are now treated the same way — no reading, no lag,
// and the topic is reported as incompletely measured so the degradation alerts.
//
// Parameters:
//   - topic string, partition int: the partition being described.
//   - bounds partitionOffsetBounds: the zero value is an unavailable partition, which is
//     what an absent map entry means.
//   - committed committedOffset: the group's committed-offset reading. Carries -1 for "never
//     committed" and a separate flag for "the broker would not say".
//
// Returns:
//   - PartitionLag: fully populated, with Lag never negative and zero whenever Unavailable.
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
		// An unavailable reading is not evidence of an absent commit: the group may well
		// have committed here and the broker simply did not say. Reporting Committed false
		// for it would put the partition in PartitionsWithoutCommit, which is read as "this
		// consumer is not consuming the topic at all".
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

// committedOffsetFor reads one partition's committed-offset reading out of the fetch result.
//
// An ABSENT entry means the group has never committed on the partition, and it resolves to
// the same -1 the broker uses for that case, with unavailable false — so the caller has exactly
// one representation of "no commit" to reason about. An entry that is present and marked
// unavailable means the broker refused to report it, which is a different fact; see
// committedOffset.
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
// It builds a value rather than recording one, which is the shape the instrument kind
// requires: SubscriberConsumerLag is an OBSERVABLE gauge, so a measurement is not written
// when it is taken but read from a published inventory at collection time. See
// metrics.SubscriberConsumerLag for why the synchronous gauge it replaced could not bound
// its own series count, and metrics.PublishConsumerLagInventory for who publishes.
//
// # Every label value is bounded here, at the last possible moment
//
// All three attribute values pass through a resolver that either recognises the value or
// substitutes a fixed token. This is the THIRD enforcement of the identifier contract —
// the repository validates before a write and the schema asserts a CHECK constraint — and
// it is the only one that covers a value which never came from the registry at all. An
// operator's ad-hoc lag query, a group id read back from the broker, or a future caller
// assembling a ConsumerLagRequest by hand all arrive here without having passed either of
// the other two gates. Without it, a consumer group named for its owner would export a
// customer's name into the monitoring system's retention, onto dashboards and into alert
// notifications.
//
// Resolving here rather than at the call site is also what keeps the two gauges' label
// tuples identical to each other, since both are observed from this one sample.
//
// # LagComplete is decided from the partition count, not from the total
//
// A topic with any unreadable partition has a TotalLag that is a LOWER BOUND, because an
// unavailable partition contributes zero. Marking the sample incomplete is what stops that
// number reaching the gauge the >10000 rule reads — see
// metrics.ConsumerLagUnmeasuredPartitions for why publishing it would resolve a firing
// alert with a figure known to be too small.
//
// Parameters:
//   - subscriber, group string: the RAW values; each is resolved to a bounded label here
//     rather than by the caller, so no call site can bypass the bound.
//   - topicLag TopicLag: one topic's measurement, carrying both the total and the count of
//     partitions that could not be read.
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
//   - map[string]map[int]committedOffset: the reading per topic and partition. A partition
//     the group has not committed on is absent, or present as the broker's own -1; a
//     partition the broker refused to report is present and marked UNAVAILABLE, which is a
//     different fact and must not be scored as an absent commit.
//   - error: a wrapped transport or group error. A group that does not exist is NOT an
//     error — it simply has no commits, which is the state of every subscriber that has
//     not started consuming yet, and the caller scores it as full lag.
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
			// every registered subscriber on every tick — so at Info one un-started
			// subscriber produced a line per tick, indefinitely, for a condition that is not
			// a fault. That volume is not free: it is what trains an operator to filter the
			// logger out, and it takes the genuine warnings from this file with it.
			//
			// Nothing is lost by demoting it, because the condition is already MONITORED
			// rather than merely logged. A group with no commits scores every partition at
			// full lag, so a subscriber that never starts consuming shows up as a rising
			// consumer-lag series and trips the >10000 rule — which is a signal an operator
			// receives rather than one they would have had to be reading logs to notice.
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
				// RECORDED AS UNAVAILABLE, not skipped (OBS-21).
				//
				// Skipping left the map entry absent, and an absent entry means "the group has
				// never committed here" — which the lag arithmetic deliberately scores as FULL
				// LAG FROM THE EARLIEST RETAINED OFFSET. So a partition the broker merely would
				// not report produced the whole retained log as lag, on a topic that stayed
				// marked COMPLETE, and that number went to the gauge SubscriberConsumerLagHigh
				// reads. A leader election on one partition of a healthy, caught-up consumer
				// could therefore page an operator with a six-figure lag that was never real.
				//
				// The two conditions are not the same fact and are no longer collapsed. "No
				// commit" is knowledge; "the broker refused" is the absence of knowledge, and
				// the honest handling of the second is to withhold this partition's lag, mark
				// the topic incompletely measured, and let ConsumerLagMeasurementDegraded carry
				// it — the rule that exists precisely so an unmeasurable subject alerts instead
				// of reading as a number.
				//
				// WARN rather than Debug for the same reason: this is a degraded measurement an
				// operator has to know about, not the routine state of an unstarted subscriber.
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

// committedOffset is ONE partition's committed-offset reading, and it exists to keep two
// facts apart that a bare int64 conflated.
//
// A group that has never committed on a partition and a broker that would not report the
// partition are different states with opposite correct treatments: the first is knowledge and
// scores as full lag from the earliest retained offset, so an unstarted consumer cannot read as
// healthy; the second is the ABSENCE of knowledge, and any number derived from it is invented.
// Representing both as -1 meant the second silently inherited the first's treatment.
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
	//
	// It is the EXCLUSIVE upper bound of the readable window, and that is how the
	// reconciliation uses it: a stored coordinate at or above it cannot be on the log, which
	// is only possible if the partition was truncated or the topic recreated. The SUM of
	// these offsets is reported as context and is not the verdict — see
	// ReconcileAgainstOutbox for why a whole-topic total cannot decide anything on a topic
	// Blnk may share.
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
	//
	// It is reported as context and is NOT the reconciliation figure. On a topic Blnk
	// shares it counts records Blnk never wrote, and it counts every redelivery, replay
	// and dead-letter copy separately, so it can exceed the number of Blnk events
	// arbitrarily without meaning anything.
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
	// the window, so this topic's window count is a LOWER bound. It is detected rather than
	// assumed: a partition whose window-start offset equals its first retained offset, on a
	// partition that has already had records deleted, cannot rule out that earlier records
	// inside the window are gone.
	WindowTruncated bool

	// WindowPartitionsUnreadable counts partitions whose window-start offset could not be
	// resolved, so their records are missing from WindowRecordCount.
	WindowPartitionsUnreadable int
}

// TopicOffsetReport is the broker-side half of the zero-loss reconciliation.
//
// # What it is for, and what it is not for
//
// Its load-bearing content is the PER-PARTITION WINDOWS, reachable through
// PartitionIntervals(): each outbox row's stored coordinate is checked for membership in the
// window of its own partition, which is what makes the reconciliation a bounded mapping
// rather than a comparison of totals.
//
// The sums are context. The reconciliation used to be an equality — or a directional
// inequality — between the summed end offsets and the outbox's row count, and that comparison
// is unsound however carefully it is read: the two sides share no baseline (outbox pruning
// shrinks one while the other only climbs), no readability guarantee (retention deletes
// records the end offset still counts), no topic incarnation (recreating a topic resets it)
// and no producer (foreign traffic inflates it). MissingTopics and PartitionsUnavailable now
// mean "no window could be measured here", so the rows on those partitions are reported as
// unmeasured rather than read as loss.
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

	// WindowStart is the instant the windowed figures below are measured from, and the
	// zero value means the report carries no window.
	//
	// # PERF-P05: why a windowless report cannot support a verdict
	//
	// End offsets are cumulative for the life of a topic and survive Kafka retention, while
	// the outbox's retention sweep deletes terminal rows. Compared over the whole history
	// the two sides therefore drift apart by exactly however much the outbox has forgotten,
	// in the direction the check tolerates — so a growing surplus is indistinguishable from
	// a growing amount of concealed loss. ReconcileAgainstOutbox refuses to call a
	// windowless comparison conclusive for that reason; the windowless spelling exists for
	// diagnostics, where the raw end offsets are the point.
	WindowStart time.Time

	// WindowRecordCount is how many records the broker accepted across every measured topic
	// inside the window. This is the figure a windowed reconciliation compares against.
	WindowRecordCount int64

	// WindowTruncated is true when retention has removed records written inside the window
	// on at least one partition, which makes WindowRecordCount a lower bound and the
	// verdict inconclusive: a short broker retention against a longer reconciliation window
	// is exactly the configuration that would otherwise report loss that never happened.
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
// # The windows are the measurement; the sums are context
//
// Each partition reports a half-open window [FirstOffset, EndOffset): the offsets the broker
// can currently serve. That window is what makes the reconciliation decidable, because
// membership of a stored coordinate in it is a fact about ONE record and is unaffected by
// anything else on the topic.
//
// The SUMS this method also returns must not be compared against the outbox's row count. That
// comparison was the reconciliation once, and it is unsound in four independent ways:
//
//   - A REDELIVERY writes a second record for one event. The relay can crash between a
//     successful publish and the row being marked dispatched, so the redelivery is a designed
//     behaviour of an at-least-once transport, not a fault. A REPLAY writes another on
//     purpose, and a DEAD-LETTERED event has a record on its `.dlt` sibling.
//   - FOREIGN TRAFFIC counts. Nothing about an end offset says which producer wrote the
//     record, so on a topic Blnk shares the sum is inflated by an unknown amount.
//   - RETENTION deletes records while their rows remain, so the sum asserts a record was
//     written that no consumer can now read.
//   - OUTBOX PRUNING removes rows while the sum only climbs, so the gap grows on its own.
//
// A surplus is therefore INDISTINGUISHABLE FROM COMPENSATED LOSS — ten redeliveries and ten
// lost events produce exactly the totals of a healthy pipeline — which is why the verdict now
// rests on the per-coordinate mapping and reports the sums only as context.
//
// Proving the stronger property — that the bytes at each coordinate are the event its row
// claims — requires reading the topics and matching on event_id, which needs a consumer. Blnk
// implements no consumer by design, so that check belongs to the audit procedure in
// docs/kafka-operations.md rather than to this method, and this method must not be presented
// as a substitute for it.
//
// Called with no topics it measures the whole inventory — every category topic and
// their dead-letter siblings — which is what the reconciliation wants and what the
// statistics endpoint reports. Named topics are measured instead, for narrowing an
// investigation to one category. Narrowing does not narrow the verdict: rows on the topics
// left out are classified as unmeasured, so the result is inconclusive rather than partial.
//
// Parameters:
//   - ctx context.Context: honoured before every round trip.
//   - topics ...string: optional topic names. Blanks and duplicates are dropped; an
//     entirely empty list selects the full inventory.
//
// Returns:
//   - TopicOffsetReport: the per-partition windows, the sums, and the caveats
//     (MissingTopics, PartitionsUnavailable) that say what could not be measured. Pass its
//     PartitionIntervals() to AuditEventRecordsInIntervals and interpret the pair with
//     ReconcileAgainstOutbox rather than comparing the sums by hand.
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
		// prefix, because the outbox rows this is reconciled against may name a namespace
		// the deployment has since renamed away from. Omitting a historical topic would
		// leave its dispatched rows counted with no offsets to match them, which reads as
		// loss that did not happen.
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

// PartitionIntervals flattens the report into the measured windows the outbox audit is
// classified against.
//
// Only AVAILABLE partitions are returned. An unavailable one is deliberately omitted rather
// than emitted with zeroed bounds, because a zero-width window would classify every row on
// that partition as beyond the log end — reporting truncation where the truth is only that
// the broker did not answer. Omitted partitions surface as unmeasured rows instead, which is
// what they are, and the report's own PartitionsUnavailable count says how many.
//
// Returns:
//   - []model.PartitionOffsetInterval: one window per measured, available partition, in
//     report order. Empty when nothing was measured.
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
//
// # A bounded mapping, not an equality
//
// It exists because callers were left to compare a record count against an event count
// themselves, and every available comparison of those two totals is wrong. The totals
// describe different populations — see model.PartitionOffsetInterval for the four ways they
// diverge — so this type carries the result of placing each row's OWN recorded coordinate
// inside the measured window of its own partition, plus the bounds of the window that
// placement covers. No caller has to re-derive the comparison, and none can report
// "reconciled" from a number that was never going to match.
type OutboxReconciliation struct {
	// TerminalEvents is how many outbox rows claim to have been published to the broker.
	//
	// It counts every row whose Kafka leg completed — dispatched AND webhook_pending, since a
	// webhook_pending row IS on its topic and what remains outstanding is the deprecated HTTP
	// leg — plus every dead-lettered row, whose record is on the dead-letter topic. Each is
	// counted exactly ONCE, which is the property the unique index on event_id gives.
	//
	// It counts rows THE OUTBOX STILL RETAINS. Pruning removes older rows, which is why
	// OldestTerminalAt is reported beside it: this verdict is a statement about that window
	// and about nothing earlier.
	TerminalEvents int64

	// CorroboratedEvents is how many of those rows name a record INSIDE the measured
	// [first, end) window of its partition — a record the broker can serve right now.
	//
	// It is the only population a green verdict may consist of, and it is strictly stronger
	// than the "names a coordinate" count it replaced: a coordinate that has aged out or whose
	// partition was not measured no longer counts as corroboration, because nothing available
	// today can confirm it.
	CorroboratedEvents int64

	// UnconfirmedEvents claim a publication without naming any record at all.
	//
	// # Why this is a caveat and not a curiosity
	//
	// A count-based verdict is directional — records are a lower bound on events, so a surplus
	// is expected — and its fatal weakness is that THE SURPLUS IS INDISTINGUISHABLE FROM
	// COMPENSATED LOSS. An unconfirmed row is precisely a claim nothing corroborates, so while
	// any exist the verdict cannot rule out that they are the losses a surplus is hiding.
	UnconfirmedEvents int64

	// UnmeasuredEvents name a topic or partition the measurement did not cover: a missing
	// topic, an unavailable partition, or a partition count that has since shrunk. Their
	// records may well be there; nothing in this measurement says so.
	UnmeasuredEvents int64

	// AgedOutEvents name an offset BELOW the retained window. The record was written and Kafka
	// retention has since deleted it.
	//
	// This is not loss — the write happened, and the offset proves it — but it is not
	// corroboration either, and a subscriber that had not consumed the record by then never
	// will. It is reported separately so that "retention is in play" is a quantified statement
	// about specific events rather than a blanket caveat derived from topic-wide offsets.
	AgedOutEvents int64

	// BeyondEndEvents name an offset AT OR ABOVE the log end of their partition.
	//
	// On an intact log this is impossible: the broker assigned that offset when it accepted
	// the write, so the end offset cannot since have fallen below it. It means the partition
	// was TRUNCATED or the topic DELETED AND RECREATED, and the records those rows name are
	// gone. This is treated as detected loss rather than as a caveat, because the specific
	// records Blnk recorded are provably no longer on the log.
	BeyondEndEvents int64

	// DuplicatedRecords is how many corroborated rows share a coordinate with another row.
	//
	// It should be zero always: one record is produced by one acknowledged write of one row,
	// and the partial unique index on the coordinate forbids two rows naming the same one. A
	// non-zero value therefore means that index is absent or has been dropped — so the field
	// exists to report a broken schema rather than to tolerate duplicates, and it makes the
	// verdict inconclusive for the same reason an unconfirmed row does: two rows sharing one
	// record's corroboration is exactly the double-counting the mapping removes.
	DuplicatedRecords int64

	// MessagesWritten is how many records the broker has accepted across the measured topics,
	// from summed end offsets, and RecordsRetained how many of those it still holds.
	//
	// Neither is the basis of the verdict any more, and that is the point of reporting them
	// separately: MessagesWritten counts every redelivery, every replay, every dead-letter
	// copy AND every record any other producer ever wrote to a shared topic, so it can be
	// arbitrarily larger than the number of Blnk events without meaning anything. They are
	// retained as context an operator reads beside the mapping, never as the proof.
	MessagesWritten int64
	RecordsRetained int64

	// BlnkRecordShare is how many of the retained records this reconciliation attributed to
	// Blnk rows: CorroboratedEvents. Reported as its own field so the response states plainly
	// how much of a shared topic's traffic the verdict actually accounts for.
	BlnkRecordShare int64

	// LossDetected is true when specific records this outbox recorded are provably not on the
	// log: a row naming an offset at or above its partition's end.
	//
	// False does NOT mean "proven no loss" — see Conclusive. It means no loss is provable from
	// the coordinates that were checkable.
	LossDetected bool

	// Conclusive reports whether the mapping accounted for EVERY retained claim.
	//
	// It is true only when every terminal row was placed inside a measured window, no two rows
	// shared a coordinate, every requested topic existed and every partition reported. A
	// caller must not report a green reconciliation on an inconclusive result.
	Conclusive bool

	// Caveats names, in plain words, every reason the result is inconclusive. Empty when
	// Conclusive is true.
	Caveats []string

	// CoveredFrom and CoveredTo bound the publication instants of the corroborated
	// population: the window a green verdict actually speaks about.
	CoveredFrom time.Time
	CoveredTo   time.Time

	// OldestTerminalAt is the earliest publication instant among all retained terminal rows.
	// Anything published before it has been pruned from the outbox and is outside the reach of
	// any verdict — which is why it is reported rather than left implicit.
	OldestTerminalAt time.Time

	// MeasuredAt is when the broker side was measured.
	MeasuredAt time.Time

	// WindowStart is the instant both sides were measured from, zero when the comparison was
	// whole-history.
	WindowStart time.Time

	// Overhead is MessagesWritten minus TerminalEvents: the redelivery, replay and
	// dead-letter copies. Its EXPECTED value is greater than or equal to zero, and a healthy
	// system's overhead is small but not zero.
	//
	// It is negative exactly when loss is detected, which is why the field is signed rather
	// than clamped: clamping would erase the only signal this reconciliation carries.
	Overhead int64

	// Windowed reports whether both sides were bounded to a common window. Only a windowed
	// comparison can be conclusive — see the caveat ReconcileAgainstOutbox adds when it is
	// not — so this is the field to read before believing MessagesWritten or Overhead.
	Windowed bool
}

// Summary renders the verdict as one sentence for a log line or a runbook.
//
// The green branch is deliberately the most heavily qualified of the three. It is the sentence
// an operator will paste into a compliance record, so it states the two independent grounds it
// rests on — the matched all-time baseline and the per-record verification — rather than
// asserting an unqualified "no loss", which the arithmetic alone was never able to support.
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
		//
		// The conclusion is unchanged and rests on the same evidence: every row names the distinct
		// record it produced, verified against its partition's live bounds. What differs is the
		// record TOTAL. With no common window the broker figure counts every record the topics have
		// ever accepted — a history the outbox no longer holds — so the difference between it and
		// the row count grows for the life of the topic and is not a surplus over anything. The
		// green sentence used to report it as one, which invited an operator to read a number in
		// the millions as unaccounted copies.
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
		// The SURPLUS is named, because a green verdict that only said "no loss detected" left an
		// operator unable to tell a healthy overhead from a shortfall that happened to be hidden
		// by one: what makes the surplus safe is that every row names the distinct record it
		// produced, so the extra records belong to redeliveries, replays and dead-letter copies
		// rather than to events nothing accounts for.
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

// ReconcileAgainstOutbox interprets an offset report against the outbox's interval audit.
//
// It is the ONLY sanctioned way to compare the two, and it exists so that the reasoning lives
// in one place rather than being re-derived — wrongly — by each caller.
//
// # What replaced the arithmetic, and why it had to be replaced
//
// It used to take a row count and conclude from `endOffsetSum - terminalRows >= 0`. That was
// unsound in four separate ways, each of which is enough on its own to make the verdict
// meaningless: the two sides had no common baseline (outbox pruning shrinks one while the
// other only climbs), no common readability guarantee (Kafka retention deletes records the
// end offset still counts), no common topic incarnation (recreating a topic resets its
// offsets), and no common producer (foreign traffic on a shared topic inflates the right side
// by an unknown amount). On top of all that, the surplus it tolerated was indistinguishable
// from compensated loss: ten redeliveries and ten lost events produce exactly the totals of a
// healthy pipeline.
//
// This function now interprets a MAPPING that is bounded per partition. Every retained row
// that claims a publication has been placed into exactly one bucket by
// AuditEventRecordsInIntervals, and the verdict is a reading of those buckets:
//
//	beyond the log end        ⇒ LOSS. The broker assigned that offset; the log no longer
//	                            reaches it, so the record is gone (truncation or recreation).
//	any other uncorroborated  ⇒ INCONCLUSIVE, itemised by reason.
//	all corroborated, distinct⇒ NO LOSS DETECTED, over the stated publication window.
//
// The summed end offsets are still reported, because an operator wants them, but they are no
// longer the proof. That is deliberate: on a shared topic they cannot be.
//
// Parameters:
//   - report TopicOffsetReport: the broker-side measurement from TopicEndOffsets, whose
//     PartitionIntervals() must be the windows the audit was taken against. Passing an audit
//     computed from different windows would produce a verdict about nothing.
//   - audit model.EventRecordIntervalAudit: the outbox-side classification from
//     AuditEventRecordsInIntervals. Each event counted once, by virtue of the unique index on
//     event_id.
//
// Returns:
//   - OutboxReconciliation: the verdict, always populated.
func ReconcileAgainstOutbox(
	report TopicOffsetReport,
	audit model.EventRecordIntervalAudit,
) OutboxReconciliation {
	// THE COMMON POPULATION. When both sides name a window the comparison is drawn INSIDE it,
	// because the cumulative sum counts a history the outbox no longer holds: a topic that has
	// accepted a million records over its life and a thousand inside the window must reconcile
	// against the rows the outbox holds for that window, or every reconciliation reports a
	// surplus that grows without bound. With no window on either side the cumulative reading is
	// the only one available, and the caveats say so.
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
	// The second is arithmetic and only available on a windowed comparison: records are a LOWER
	// BOUND on events, because a redelivery, a replay and a dead-letter copy each append a record
	// no row claims. So a surplus is normal and a SHORTFALL is not: fewer records inside the
	// window than rows claiming one inside it means rows claim a publication that never happened.
	verdict.LossDetected = verdict.BeyondEndEvents > 0 || (windowed && verdict.Overhead < 0)

	caveats := make([]string, 0, 7)

	// AN UNWINDOWED COMPARISON IS NOT A CAVEAT, and the reason is worth stating because the
	// opposite is the intuitive answer.
	//
	// A broker end offset counts every record a partition has ever accepted and never falls,
	// while the outbox side is a population retention deletes from and a request may bound. With
	// no window on both sides those two TOTALS describe different intervals, and the shortfall
	// arithmetic over them is meaningless — which is why LossDetected only consults Overhead when
	// windowed.
	//
	// But the totals are not what this verdict rests on. It rests on the per-row COORDINATE
	// MAPPING: every row claiming a publication either names a record verified against the live
	// bounds of the partition it names, or it is counted into UnconfirmedEvents, UnmeasuredEvents,
	// AgedOutEvents or BeyondEndEvents — each of which raises its own caveat below. So a fully
	// mapped population is accounted for event by event, with no window needed, and a population
	// that is not fully mapped is already inconclusive for a reason that names itself.
	//
	// Adding a blanket caveat here would therefore not catch a false green — there is none to
	// catch — and it WOULD destroy a deliberate property: retention on a topic Blnk shares with
	// another producer is permanently in the past, so an always-on caveat would report every such
	// deployment as inconclusive for ever, which is the noise this verdict was rewritten to
	// remove. What the unwindowed case does need is for the SURPLUS not to be described as a
	// windowed surplus, and Summary does that from the Windowed flag.

	if verdict.BeyondEndEvents > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) name an offset at or beyond the end of their partition's log, which means the "+
				"partition was truncated or the topic was deleted and recreated after those records "+
				"were written",
			verdict.BeyondEndEvents,
		))
	}

	// THE CAVEAT THE COORDINATE MAPPING EXISTS FOR. A row claiming a publication it cannot name
	// a record for is not evidence of loss — the record may well be there — but it is precisely
	// what a surplus of redeliveries could be concealing, so no green verdict may be reported
	// while any remain.
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

	// Retention is now a statement about SPECIFIC EVENTS rather than a topic-wide subtraction.
	// The old caveat fired whenever anything at all had aged out of a shared topic — including
	// records Blnk never wrote — so it was permanently on in any long-lived deployment and told
	// an operator nothing about their own events.
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
	//
	// It used to be one whenever the broker held fewer records than the offsets counted —
	// which is true of every healthy cluster with any retention policy at all, so the verdict
	// became permanently inconclusive the first time a segment was deleted and stayed that
	// way. That is not what retention does to this comparison: end offsets count deleted
	// records too, so a cumulative reading is unaffected and a WINDOWED reading is affected
	// only when records written INSIDE the window have been removed. That case is detected
	// per partition and reported here; anything else is normal operation and is reported as a
	// note on the report rather than as a reason to distrust the verdict.
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

// ---------------------------------------------------------------------------
// Event pipeline statistics — the orchestration behind GET /events/stats
//
// Everything below assembles ONE answer to "what state is the event pipeline in",
// from four collaborators: the per-status aggregate and the terminal-record audit
// from PostgreSQL, the topic end offsets from the broker, and the reconciliation
// verdict that compares the last two.
//
// It lives here, in the root package, and not in the HTTP handler. The handler used
// to orchestrate it directly — resolve the datasource, read the counts, decide
// whether the audit was needed, build a Kafka admin client, read the offsets,
// choose per-failure whether to degrade or refuse, and then reconcile — which put
// the sequencing rules and the failure policy of a five-step operation inside a
// function whose job is to translate HTTP. Two costs followed. The sequence was
// unreachable from anything that is not a Gin request, so the CLI and any test had
// to restate it; and each degradation decision was expressed as a `c.JSON` early
// return, so "which failures are tolerable" could only be read by tracing response
// writes.
//
// The handler now performs one call and maps the result onto its DTO.
// ---------------------------------------------------------------------------

// eventOffsetReadTimeout bounds the broker round trip a statistics read makes.
//
// It exists so that an unreachable broker DEGRADES the answer rather than holding the
// caller open: the outbox counts have already been read from PostgreSQL by the time the
// broker is dialled, and reporting them without the offsets is a valid, documented answer.
const eventOffsetReadTimeout = 10 * time.Second

// EventOffsetInclusion is how a statistics read wants the BROKER side treated — and, because
// the two are one decision, whether the DISPATCHED HISTORY is counted at all.
//
// The three postures exist because the broker half of the reconciliation is
// optional in a way the outbox half is not: a deployment with no brokers configured
// is a legitimate steady state, so an unreadable broker is ordinarily an enrichment
// that is absent rather than a failure. A caller who specifically asked for that
// half needs the opposite, and a caller who wants the counts cheaply needs neither.
//
// # PERF-M05/M02: why ONE posture governs both halves, and why Skipped is the zero value
//
// The dispatched count and the broker offsets are wanted by exactly the same caller and by no
// other: the only reason to know how many rows were dispatched in an interval is to compare it
// against what the broker recorded over that interval. Every other caller — the drain poll in
// the load harness, a health check, an operator asking what is outstanding — wants the
// unresolved inventory and nothing else.
//
// So the posture selects the whole reading rather than half of it. Skipped counts the
// unresolved inventory alone, which is bounded by operation and index-only; the other two
// additionally count the dispatched history, which at 500 events per second is 43.2 million
// index entries for a day. A separate `include_history` flag was the obvious alternative and
// was rejected: two flags that are always set together are two flags that can disagree, and
// the disagreement that matters here is the cheap-looking call that quietly does the expensive
// half.
//
// Skipped is the ZERO VALUE, so a Go caller that constructs the type without thinking and an
// HTTP caller that omits `include_offsets` get the same cheap reading. The previous zero value
// was best-effort, which meant the default was a broker round trip plus a history-sized count
// — paid, in the load harness, on every quiescence poll of the drain loop.
type EventOffsetInclusion int

const (
	// EventOffsetsSkipped makes no broker round trip at all and counts no dispatched
	// history: the answer is the exact unresolved inventory. It is the ZERO VALUE and the
	// posture every routine caller wants.
	EventOffsetsSkipped EventOffsetInclusion = iota

	// EventOffsetsBestEffort counts the dispatched history, reads the broker when one is
	// configured, and omits the broker-side fields on any failure, logging the reason. It is
	// the posture the daily reconciliation runbook relies on, and it must be asked for.
	EventOffsetsBestEffort

	// EventOffsetsRequired is EventOffsetsBestEffort except that a failure to read the
	// broker becomes a typed error, because the caller asked specifically for the half of
	// the reconciliation that failed.
	EventOffsetsRequired
)

// countsDispatchedHistory reports whether this posture counts the unbounded dispatched
// population as well as the unresolved inventory.
//
// It is a method rather than an inline comparison at the two places that need it so that the
// coupling stated on EventOffsetInclusion — that the dispatched count and the broker read are
// one decision — is enforced in one place. Two inline comparisons is how the response comes to
// claim a history figure that the query never produced.
//
// Returns:
//   - bool: true for every posture that measures the broker side.
func (i EventOffsetInclusion) countsDispatchedHistory() bool {
	return i != EventOffsetsSkipped
}

// EventOutboxStatistics is the whole state of the event pipeline at one instant: the
// outbox side always, the broker side and the verdict when they could be measured.
//
// # Read the two booleans before reading the fields they govern
//
// AuditRead and OffsetsRead exist because "measured and zero" and "not measured"
// are different answers that the numbers alone cannot distinguish, and confusing
// them is how a reconciliation reports a clean bill of health it never established.
// A response that presents an unmeasured broker side as though it were a measured
// empty one is exactly the failure the zero-loss criterion cannot survive.
//
// Reconciliation is nil unless BOTH sides were measured and at least one topic was
// actually covered. That is a documented absence rather than an error: with nothing
// measured there is nothing to compare, so no verdict is reported.
type EventOutboxStatistics struct {
	// GeneratedAt is when the outbox side was read, in UTC.
	GeneratedAt time.Time

	// CountsByStatus is the per-status aggregate, keyed by the
	// model.EventOutboxStatus* values. A status with no rows is ABSENT rather than
	// present with a zero, because GROUP BY only produces rows that exist — read it
	// with the two-value form or accept the zero value.
	CountsByStatus map[string]int64

	// UnreportedStatuses names any status the table holds that model.EventOutboxStatuses
	// does not know about, sorted.
	//
	// The status column deliberately permits values the code has not learned yet so the
	// state machine can be extended without a migration, which means a new state can
	// appear in the aggregate before any consumer's shape learns about it. Its rows
	// would then be missing from every reported total, and a short total is precisely
	// what a zero-loss reconciliation cannot tolerate. Surfacing the names here is what
	// lets a caller say so rather than silently under-report.
	UnreportedStatuses []string

	// WindowStart is the instant the WINDOWED figures are measured from, and Window is its
	// length. They are reported rather than implied because only one figure here is windowed —
	// the dispatched count — and a reader who cannot see the interval cannot tell a quiet day
	// from a short window.
	WindowStart time.Time
	Window      time.Duration

	// DispatchedHistoryCounted reports whether the dispatched population was counted at all.
	//
	// It is a companion flag for the same reason AuditRead and OffsetsRead are: an absent
	// `dispatched` key means "no rows in the window" and "never counted" indistinguishably,
	// because GROUP BY produces only rows that exist — and a reconciliation that reads the
	// second as the first concludes that nothing was published. False whenever the posture
	// skipped the broker side, which is the cheap reading every routine caller takes
	// (PERF-M05).
	DispatchedHistoryCounted bool

	// Audit is the outbox side of the reconciliation, classified against the very
	// partition windows the broker reported. Meaningful only when AuditRead.
	Audit model.EventRecordIntervalAudit

	// AuditRead reports whether the audit was actually read. False both when the
	// posture skipped the broker side entirely and when the audit query failed under a
	// best-effort posture.
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
	//
	// Nil is "not measured" and a zero-valued struct is "measured, nothing outstanding".
	// Reporting the second as the first — or the first as zeros — is the one confusion a
	// zero-loss check cannot survive, which is why this is a pointer rather than a value with
	// a companion flag: there is nothing to read if it is nil.
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
	// status, exact and unwindowed. It is what a EventOffsetsSkipped read answers with, and
	// it is the whole reading whenever the broker side is not being measured (PERF-M05):
	// its cost is set by how much work is outstanding rather than by how much history the
	// table holds.
	CountUnresolvedEventOutbox(ctx context.Context) (map[string]int64, error)

	// CountEventOutboxByStatus returns the same aggregate PLUS the dispatched history. The
	// instant bounds the unbounded dispatched population only; every other status is counted
	// in full however short the window, because a row stuck for days must not vanish from a
	// one-day reading. It is read only when the broker side is being measured, because the
	// dispatched figure exists to be compared against the broker's records.
	CountEventOutboxByStatus(ctx context.Context, since time.Time) (map[string]int64, error)

	// AuditEventRecordsInIntervals returns the outbox side of the zero-loss
	// reconciliation, classified against the readable offset windows the broker
	// reported. The windows are what make the two sides describe one population, so
	// the audit is taken AFTER the offsets are measured and against those very
	// intervals.
	AuditEventRecordsInIntervals(
		ctx context.Context,
		intervals []model.PartitionOffsetInterval,
	) (model.EventRecordIntervalAudit, error)

	// CountBalanceMonitorHandoffByStatus and CountUnfinalizedBulkTransactionBatches are the
	// two PRE-RECORDED INTENT censuses, and they answer the one question the per-status
	// counts above cannot.
	//
	// An event captured from an INTENT written atomically with its mutation — rather than as an
	// outbox row inside it — is OWED while that intent is outstanding, and no outbox row exists
	// for it yet. A reconciliation reading only the outbox would find it consistent while those
	// events were still pending.
	//
	// There are two such intents. A batch coordinator row is the ordinary route for every bulk
	// summary, because a summary belongs to no single member transaction. A balance-monitor
	// handoff is not: the atomic writers insert a monitor alert's canonical row inside the
	// mutation's own transaction, so the handoff census describes a finite, draining population
	// — rows written before that capture existed, and rows written by a process with no alert
	// capture registered.
	//
	// They are on this seam rather than left to a separate endpoint because they are part of
	// the same answer, and the statistics response declares a field for them.
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
// per-status counts would read a clean outbox as a clean pipeline while alerts and batch
// summaries were still owed.
//
// The zero value is a MEANINGFUL reading: nothing outstanding. That is why the statistics
// carry it as a pointer and leave it nil when a census could not be read, rather than
// reporting zeros for "we could not tell" — the one misreading a zero-loss check cannot
// afford.
type ProducerAtomicityCensus struct {
	// MonitorHandoffPending, MonitorHandoffProcessing, MonitorHandoffCompleted and
	// MonitorHandoffFailed are the handoff relay's four states, read straight from the
	// aggregate. A status with no rows is absent from that aggregate and reads as zero here,
	// which is the correct reading of "none in that state".
	//
	// FAILED IS THE NUMBER TO ACT ON: each one is a balance movement whose monitor conditions
	// were never judged, so any alert it should have produced does not exist and never will
	// without intervention.
	MonitorHandoffPending    int64
	MonitorHandoffProcessing int64
	MonitorHandoffCompleted  int64
	MonitorHandoffFailed     int64

	// UnfinalizedBatches counts asynchronous bulk batches that began and never reported an
	// outcome, past the grace period so batches still legitimately running are excluded, and
	// OldestUnfinalizedBatchAt is when the oldest of them began — nil when there are none.
	// The age is what separates a large batch still running from one that was abandoned, so
	// the count alone is not actionable and the pair is.
	UnfinalizedBatches       int64
	OldestUnfinalizedBatchAt *time.Time
}

// unfinalizedBulkBatchGrace is how long a bulk batch may run before an unfinalized
// coordinator row counts as outstanding.
//
// A batch legitimately takes time — it is asynchronous precisely because it may hold
// thousands of member transactions — so counting one the moment it starts would report the
// steady state as a residue and make the figure useless. Fifteen minutes is comfortably
// beyond any batch this ledger processes and well inside the fifteen-minute dead-letter
// alerting window an operator is already watching, so a genuinely abandoned batch surfaces on
// the same timescale as every other event-pipeline residue.
const unfinalizedBulkBatchGrace = 15 * time.Minute

// EventOutboxStatistics assembles the statistics for this instance.
//
// # The failure policy, in one place
//
// The outbox counts are read FIRST and their failure is the only unconditional one:
// without them there is nothing to report at all. Everything after them is governed
// by the posture:
//
//	EventOffsetsSkipped   returns after the counts. No audit, no broker round trip.
//	EventOffsetsBestEffort logs and omits on an audit or offset failure. This is what
//	                      makes a deployment with no brokers answer 200 with the
//	                      counts, which is a legitimate steady state and not a fault.
//	EventOffsetsRequired  returns a typed error on either failure, because the caller
//	                      asked for the half that could not be produced.
//
// The audit is read only when the broker side is going to be measured, because its
// only consumer is the comparison against the offsets.
//
// # Why the whole topic inventory is always measured
//
// No topic-narrowing parameter is offered, here or at the endpoint. The verdict
// compares the broker's records against EVERY outbox row that claims a publication,
// so measuring a subset of the topics would manufacture a shortfall and report loss
// that has not happened. Restricting the topics is only meaningful alongside a
// matching restriction on the outbox side, which no repository method offers.
//
// Parameters:
//   - ctx context.Context: cancels the queries and the broker round trip. The broker
//     read is additionally bounded by its own timeout, so an unreachable broker
//     degrades the answer rather than holding the caller open.
//   - inclusion EventOffsetInclusion: how the broker side is to be treated.
//
// Returns:
//   - EventOutboxStatistics: the statistics. Populated whenever err is nil; read
//     AuditRead and OffsetsRead before the fields they govern.
//   - error: a typed APIError — ErrInternalServer when the outbox cannot be read, and
//     ErrKafkaUnavailable when the broker was REQUIRED and could not be read.
//
// defaultEventStatisticsWindow is the window a statistics request gets when it names none, and
// maxEventStatisticsWindow is the longest one accepted.
//
// One day, because that is the period the zero-loss reconciliation runbook reconciles over and
// the period an operator asks "what happened today" about.
//
// # PERF-M05: the ceiling is the default, so the window may only be NARROWED
//
// It used to be a week, on the reasoning that a week is "long enough for an operator
// investigating something that started last weekend, short enough that the bounded queries stay
// bounded". The second half of that does not hold at the target rate. The window bounds exactly
// one figure — the exact COUNT of the dispatched population — and at 500 events per second a day
// of it is 43.2 million index entries while a week is 302.4 million. A count of 302 million
// entries is not servable inside any sane request timeout, so permitting it did not buy the
// operator a wider retrospective: it bought them a timeout, and the database the scan anyway.
//
// And a wider window bought very little even when it completed. Everything an operator acts on —
// the pending backlog, the rows in flight, the dead-letter inventory, the two repair legs — is
// counted exactly and for ALL TIME regardless of the window, and the zero-loss audit is bounded
// by outbox retention rather than by this parameter. The only figure that grew with the window
// was a historical one.
//
// So the ceiling is the daily reconciliation period, and the parameter's remaining job is to
// NARROW it: `?window=15m` for a twenty-minute incident is the case it exists for, and that case
// is unaffected. A retrospective longer than a day is served by the dead-letter inventory and
// the audit, both of which are retention-bounded and neither of which reads this window.
const (
	defaultEventStatisticsWindow = 24 * time.Hour
	maxEventStatisticsWindow     = defaultEventStatisticsWindow
)

// normalizeEventStatisticsWindow bounds a requested window.
//
// A non-positive request becomes the default, which is what a caller that named no window
// passes, and anything longer than the ceiling is clamped to it. Correcting rather than
// refusing is deliberate at this layer: the HTTP handler refuses an out-of-range value so the
// caller sees their mistake, and this is the floor under every other caller — a gauge, a
// runbook script, a future CLI — so that none of them can ask for an unbounded scan by
// accident.
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
// It is separated from the Blnk method for one reason: every branch below is a POLICY
// decision — which failures degrade the answer and which refuse it — and a policy that
// can only be exercised through a fully constructed service with a live PostgreSQL and a
// live broker is a policy whose branches go untested. The seam takes a two-method store
// and an offset-reader function, so each posture and each failure combination is
// reachable directly.
//
// Parameters:
//   - ctx context.Context: cancels both reads.
//   - store eventStatisticsStore: the two repository reads. Must not be nil.
//   - readOffsets func: the broker-side measurement, taking the WINDOW START the outbox side
//     was counted from so both sides describe one interval. A nil function is treated as an
//     unconfigured broker, which is the same degradation an unreachable one takes.
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
		// An absent reader is the same situation as an unconfigured broker, and reporting
		// it as that error rather than panicking is what keeps the two postures behaving
		// identically for a caller that has no broker at all.
		readOffsets = func(context.Context, time.Time) (TopicOffsetReport, error) {
			return TopicOffsetReport{}, ErrKafkaAdminNotConfigured
		}
	}

	// ONE WINDOW START, DERIVED ONCE, AND REPORTED. This used to pass the zero instant with a
	// comment saying it asked for the whole retained history — and the repository normalises a
	// zero instant to its own default precisely so that a caller which forgot the parameter
	// cannot scan a table gaining 43.2 million rows a day. So the request did not get the whole
	// history: it got twenty-four hours of dispatched rows, described to the operator as though
	// it were everything, with no window on the response to say otherwise.
	//
	// What the window does and does not bound is the repository's contract, restated here
	// because it is what makes a short window safe to ask for: every status EXCEPT dispatched is
	// counted exactly and in full whatever the window says — those are the populations an
	// operator acts on, and any of them can legitimately be older than any window — while
	// dispatched, the only one that grows without bound, is counted from here.
	windowStart := time.Now().UTC().Add(-normalizeEventStatisticsWindow(window))

	// WHICH AGGREGATE, decided by the posture and by nothing else (PERF-M05). Skipped takes the
	// unresolved inventory alone — exact, unwindowed, bounded by outstanding work — and the two
	// broker-reading postures additionally count the dispatched history, because that figure
	// exists only to be compared against what the broker recorded. See EventOffsetInclusion for
	// why this is one decision rather than two flags.
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
		// Kafka entirely and the one where the broker is unreachable, which is exactly when an
		// operator is asking what is outstanding.
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

	// MEASURED FROM THE SAME INSTANT THE OUTBOX SIDE WAS COUNTED FROM (V-2). The broker read
	// used to be cumulative: every record the topics had ever accepted, compared against an
	// outbox population bounded by a window. Two populations, one verdict — and the verdict
	// could still report itself CONCLUSIVE, because the arithmetic that would have caught the
	// mismatch (a shortfall of records against rows) is only available when both sides name a
	// window, and with the broker side unbounded it was silently switched off. Passing the
	// window start here is what makes the comparison a comparison.
	report, err := readOffsets(ctx, windowStart)
	if err != nil {
		if inclusion == EventOffsetsRequired {
			// DATA-01: the cause is a Kafka client error, which renders with broker
			// addresses and topology, so it is logged here and a fixed message is
			// returned in its place.
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

	// The audit is taken against the windows THIS report measured, never against the
	// whole table: an audit and an offset total that describe different populations
	// cannot be compared, which is the defect the interval form exists to close.
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
	// produced. A verdict computed over zero topics would report a clean bill of health
	// it never established.
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
// It exists so that the CHOICE is one expression in one place. The two aggregates return the
// same shape and differ only in what they cost and what they cover, which is exactly the
// situation in which a second call site drifts onto the expensive one — and the drift is
// invisible in review, because both lines read as "count the outbox by status".
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//   - store eventStatisticsStore: the repository seam. Must not be nil.
//   - windowStart time.Time: the instant the dispatched arm counts from. Ignored when the
//     dispatched arm is not being run.
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

// producerAtomicityCensus reads the two pre-recorded intent censuses, and answers nil rather
// than zeros when either read fails.
//
// # Why a failure omits the whole object
//
// The two censuses answer one question — how many events are OWED and not yet captured — and a
// partial answer to it is worse than none: a caller cannot tell "no monitor alerts are
// outstanding" from "the handoff table could not be read", and a zero-loss reconciliation that
// mistakes the second for the first reports a clean bill of health it never established. So a
// failure on either read yields nil, which the response renders as an ABSENT key rather than as
// zeros, and the cause is logged where an operator will see it.
//
// It is DEGRADING and not refusing, deliberately. The per-status counts and the broker
// comparison are the reconciliation's main line; a census read that fails must not deny an
// operator the rest of the statistics during the incident that broke it.
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

	// The four handoff states use the shared outbox status vocabulary rather than one of their
	// own — see model.BalanceMonitorHandoff.Status — so they are read through the same
	// constants the outbox counts are. A status with no rows is absent from the aggregate and
	// indexes to zero, which is the correct reading of "none in that state".
	census := &ProducerAtomicityCensus{
		MonitorHandoffPending:    handoffs[model.OutboxStatusPending],
		MonitorHandoffProcessing: handoffs[model.OutboxStatusProcessing],
		MonitorHandoffCompleted:  handoffs[model.OutboxStatusCompleted],
		MonitorHandoffFailed:     handoffs[model.OutboxStatusFailed],
		UnfinalizedBatches:       batches,
		OldestUnfinalizedBatchAt: oldest,
	}

	// LOGGED ONLY WHEN SOMETHING IS OWED, and at warn, because these two numbers are the ones
	// nothing else in the response can reveal: an unjudged monitor movement and an abandoned
	// batch are both events that will never exist without intervention.
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
// lifecycle. The relay and the metrics collector each hold their own long-lived
// client.
//
// A close failure is logged and never returned, matching how the server does it: the
// measurement is what the caller asked about, and reporting a connection-teardown
// problem as a failed read would send an operator looking for a broker fault that
// does not exist.
//
// An unconfigured broker is reported as ErrKafkaAdminNotConfigured WITHOUT building a
// client, so the reason appears in the caller's log rather than being obscured by an
// operation that refuses after dialling nothing.
//
// Parameters:
//   - ctx context.Context: cancellation is inherited; the read is additionally bounded
//     by eventOffsetReadTimeout.
//   - since time.Time: the left-hand edge of the window the broker-side population is
//     bounded to. The zero instant asks for the cumulative reading only, which is the
//     right request for a diagnostic and the wrong one for a verdict — a comparison
//     against a bounded outbox population needs a bounded broker population too.
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

// ReconciliationBaseline is everything beyond the two counts that the verdict needs in order
// to be sound, gathered into one parameter.
//
// # Why it is a required parameter rather than an option
//
// Both members answer a question the counting cannot: whether the two sides of the comparison
// cover the same interval, and whether the records the outbox names actually exist. A verdict
// computed without them is not a weaker verdict, it is an UNSOUND one — it can report a
// confident "no loss detected" while events are missing. Making it a parameter means a caller
// has to state what it knows, and a caller that knows nothing gets an inconclusive verdict
// rather than an optimistic one. That is the whole design: the zero value is the honest
// "unknown", because model.EventOutboxPurgeTotals.Recorded is false in it and an empty
// coordinate audit verifies nothing.
type ReconciliationBaseline struct {
	// Purges is what retention has deleted from the outbox. Its Recorded field distinguishes
	// "nothing was purged" from "we cannot tell", and only the former supports a conclusive
	// verdict.
	Purges model.EventOutboxPurgeTotals

	// Coordinates is what the outbox claims about the broker, per partition. It is checked
	// against the report's live per-partition bounds, which is the step that turns the
	// reconciliation from an inference into a verification.
	Coordinates model.EventRecordCoordinateAudit
}

// THE GO-SIDE CLAIM VERIFIER WAS RETIRED HERE, and its work is now done in SQL.
//
// It took the offset report and a per-partition coordinate audit and sorted every claimed
// offset into verified / unverifiable / missing by comparing it against that partition's live
// low and high water marks. AuditEventRecordsInIntervals now performs the same classification
// inside the database, over the SAME per-partition windows the offsets were measured in, and
// returns it as corroborated / aged-out / beyond-end / unmeasured row counts.
//
// The SQL form is not merely equivalent, it is stricter in the two ways that matter. It splits
// "could not be checked" into retention having removed a record the stored offset still
// evidences (aged out) and the measurement not having covered that topic or partition
// (unmeasured) — fusing them made every cluster with a retention policy permanently
// inconclusive. And because the audit and the offsets are drawn over one set of intervals, the
// two sides of the comparison describe the same population by construction rather than by two
// callers agreeing to pass matching arguments.
// TopicCatalogueReport is the answer to "does every topic the relay may need exist right now?"
//
// It is deliberately narrower than TopicAssuranceReport. That one is about GEOMETRY — how many
// partitions, what replication factor, what this run changed — and it is what an operator reads
// at boot. This one is about EXISTENCE, and it is what the relay's claim gate reads before it
// leases a single row: a topic that is missing cannot be published to, because auto-creation is
// disabled, and it cannot be dead-lettered to either.
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
//   - bool: true when the broker has every expected topic. False for an empty expectation
//     too, which is not reachable in production — event_topics.go always composes at least
//     one category per prefix — and must not read as "verified" if it ever became reachable.
func (r TopicCatalogueReport) Complete() bool {
	return len(r.Expected) > 0 && len(r.Missing) == 0
}

// VerifyTopicCatalogue reports which of the topics Blnk may write to are absent from the
// broker.
//
// # Why existence is checked separately from assurance
//
// EnsureTopics CREATES; this one only LOOKS. The distinction is what lets the relay's claim
// gate run on every poll: a metadata read is one round trip and mutates nothing, so it is
// affordable in a loop, whereas re-running creation continuously would be neither.
//
// # Why the dead-letter siblings are part of the answer
//
// A missing dead-letter topic is worse than a missing category topic, not better. The category
// topic's absence fails the publish, which retries; the dead-letter topic's absence fails the
// PRESERVATION of an event whose retry budget is already spent, and that event has then reached
// no topic at all. Both are in Expected for that reason, and Complete requires both.
//
// # Why every owned prefix is included
//
// Rows captured before a KAFKA_TOPIC_PREFIX rename name the previous generation's topics and
// the relay still publishes them, so those topics are ones it may need. See
// AllOwnedTopicsAcrossPrefixes.
//
// Parameters:
//   - ctx context.Context: bounds the metadata read.
//
// Returns:
//   - TopicCatalogueReport: populated on success. Expected is always set, even on error, so a
//     caller can report what it was looking for.
//   - error: ErrKafkaAdminNotConfigured when no broker is configured, or a wrapped broker
//     error. A returned error means the catalogue is UNKNOWN rather than incomplete, and a
//     caller must not treat it as verified.
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

// TopicCatalogueGate answers whether the relay's destination topics exist, caching the answer
// once they do and rate-limiting how often it asks while they do not.
//
// # What it is for
//
// Every write the relay makes goes to a Blnk-owned topic, and auto-creation is disabled, so a
// missing topic fails the publish, burns the row's retry budget one attempt at a time, and then
// fails the dead-letter write for the same reason — leaving the row failed with no dead-letter
// topic recorded. A boot against an unprovisioned broker could spend every pending row's budget
// that way. This gate is what stops the relay claiming a row it cannot deliver.
//
// # Why it can repair rather than only report
//
// The gate is consulted continuously, which makes it the natural place to CLOSE the gap it
// finds: when the catalogue is incomplete it runs topic assurance and verifies again. That is
// the recovery path for a broker that came up after the boot-time assurance had already run and
// failed. Assurance is only attempted when verification says something is missing, so the
// steady state is one cheap metadata read — and, after the first success, none at all.
//
// # Why success is latched
//
// Once the whole catalogue has been seen, it is not re-checked. A topic can be deleted from
// under a running deployment, but that is an operator action against a live ledger and it
// surfaces immediately as publish failures and dead letters, which are exactly the signals for
// it; whereas re-probing for ever would put a metadata read on the relay's path for the entire
// life of the process to detect something that does not normally happen. The latch is what
// makes the gate free in the case that matters.
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
// It performs NO I/O: the admin client is built per probe and closed again, because a probe
// happens at most every few seconds and holding an authenticated admin session open for the
// life of the process — for a check that stops running once it succeeds — would keep a SASL
// connection per broker for nothing.
//
// Parameters:
//   - cfg *config.Configuration: read for the broker list and the topic geometry. A nil
//     configuration, or one with no brokers, yields a gate that reports the condition and
//     refuses, which is correct: with no broker there is no catalogue and the relay is not
//     started in the first place.
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
// It is safe for concurrent use and cheap once the catalogue has been verified: after the first
// success it takes a mutex and returns, with no broker contact ever again.
//
// All reporting is done HERE rather than by the caller, because the caller is a 1-second poll
// loop and the gate is what knows whether this call actually probed anything. A refusal is
// logged once per probe, not once per tick.
//
// Parameters:
//   - ctx context.Context: cancellation is respected; the probe is additionally bounded by
//     catalogueProbeTimeout.
//
// Returns:
//   - error: nil when every expected topic exists. Otherwise the reason the relay must not
//     claim — a broker that could not be reached, or the list of topics that are absent.
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
	//
	// The relay polls every second, and the gate is consulted on every one of those ticks, so
	// without a floor an unprovisioned broker would be interrogated once per second for as
	// long as it stayed that way — and the log would carry one refusal per second with it.
	// Five seconds keeps recovery prompt on a compose stack or a rolling restart while
	// costing one metadata round trip per five ticks in the worst case, and none at all once
	// the catalogue is verified.
	defaultCatalogueProbeInterval = 5 * time.Second

	// maxCatalogueProbeInterval caps the gate's backoff.
	//
	// A broker that has been unprovisioned for ten minutes is very unlikely to fix itself in
	// the next five seconds, so the interval grows; but it is capped, because the whole point
	// of gating rather than refusing to start is that recovery happens without a human, and
	// an unbounded backoff would eventually make that indistinguishable from a restart.
	maxCatalogueProbeInterval = 60 * time.Second

	// catalogueProbeTimeout bounds one probe. A metadata read against a reachable broker is
	// milliseconds; this is generous enough for a loaded cluster and short enough that a tick
	// is never held up for long by an unreachable one.
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

// CompensateProvisioning undoes the half-completed provisioning a deferred result describes.
// PERF-P09.
//
// It is the other half of SubscriberProvisioningRequest.DeferCompensation: provisioning reports
// what it left behind, the caller finishes the work somewhere that is not a response path, and
// this performs exactly the same two round trips the inline compensation would have.
//
// # It is a no-op for anything that does not owe a compensation
//
// A result whose CompensationOwed is false has either compensated already or never written a
// credential, and revoking on either would destroy a live credential — a working one, in the
// re-issue case. So the flag is the gate, not the caller's memory of which branch it took.
//
// # Its outcome is the caller's to record
//
// Returning nil means the broker was CONFIRMED clean: no principal is left able to authenticate
// without a boundary. A non-nil error means one is, and compensateFailedProvisioning has already
// logged it at ERROR with the principal named and the manual remedy spelled out — so a caller
// that can do nothing useful with the error may drop it, and one that reports state must not.
//
// Parameters:
//   - ctx context.Context: used for its VALUES only; the round trips run on their own bounded
//     deadline, because a deferred compensation's caller is usually holding an expired one.
//   - result SubscriberProvisioningResult: the deferred result, read for the principal and the
//     bindings it attempted.
//
// Returns:
//   - error: nil when nothing was owed or the credential is confirmed revoked; the revocation's
//     error otherwise.
func (a *KafkaAdminClient) CompensateProvisioning(
	ctx context.Context,
	result SubscriberProvisioningResult,
) error {
	if a == nil || !result.CompensationOwed || strings.TrimSpace(result.Principal) == "" {
		return nil
	}

	return a.compensateFailedProvisioning(ctx, result.Principal, result.OwedBindings)
}

// windowStartOffsets resolves, per partition, the earliest offset whose record was written
// at or after the given instant.
//
// # PERF-P05: this is the broker half of the shared reconciliation window
//
// The zero-loss check compares outbox rows against broker records, and over the whole
// history the two cannot be compared at all: end offsets are cumulative for the life of a
// topic and are unaffected by Kafka retention, while the outbox's retention sweep DELETES
// terminal rows. So the outbox side shrinks, the broker side never does, and the "surplus"
// the verdict tolerates grows without bound until it can conceal any amount of loss. Bounding
// both sides to the same recent window is what makes the comparison mean something again.
//
// Kafka answers this natively: a ListOffsets request with a TIMESTAMP returns the first offset
// whose record timestamp is greater than or equal to it. Differencing that against the end
// offset gives exactly the number of records the partition accepted inside the window.
//
// # Its own round trip, deliberately
//
// It does not extend offsetBounds. That method serves the consumer-lag sweep as well, whose
// results are cached per topic set and read once per subscriber per collection interval;
// adding a timestamp request there would put a window into a cache keyed without one and would
// cost every lag sweep a third of a request more for a figure it never reads. One extra round
// trip on a periodic reconciliation read is the cheaper trade by a wide margin.
//
// # The two sentinels a caller must handle
//
//   - -1 means the broker has NO record at or after the instant, which is a legitimate and
//     common reading: every record the partition holds is older than the window. The window
//     count for that partition is zero.
//   - An absent entry means the partition could not be read, which is not the same thing and
//     must not be counted as zero. Callers distinguish the two.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - partitions map[string][]int: the partitions to resolve, from topicPartitions.
//   - since time.Time: the window start.
//
// Returns:
//   - map[string]map[int]int64: the window-start offset per topic and partition, -1 where the
//     partition holds nothing that recent, absent where it could not be read.
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

			// A timestamp request's answer arrives in the Offsets map rather than in
			// FirstOffset or LastOffset, which kafka-go reserves for the two sentinel
			// timestamps. The map holds one entry per timestamp asked about, and exactly one
			// was asked about here; the broker's "nothing that recent" answer is the offset
			// -1, which is carried through unchanged for the caller to interpret.
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
// AN ABSENT ENTRY IS UNREADABLE, NOT EMPTY — the same distinction offsetBoundsFor exists to
// preserve, and for the same reason: treating an unreadable partition as zero makes the broker
// side short and short looks exactly like loss.
//
// Parameters:
//   - starts map[string]map[int]int64: the result of windowStartOffsets, possibly nil.
//   - topic string, partition int: the partition to look up.
//
// Returns:
//   - int64: the window-start offset, or -1 when the partition holds nothing that recent.
//   - bool: false when the partition could not be read.
func windowStartOffsetFor(starts map[string]map[int]int64, topic string, partition int) (int64, bool) {
	if perPartition, exists := starts[topic]; exists {
		if offset, ok := perPartition[partition]; ok {
			return offset, true
		}
	}

	return -1, false
}

// applyWindowToPartition folds one partition's windowed record count into its snapshot and its
// topic's totals.
//
// # The three readings it separates, because collapsing any two of them would lie
//
//   - UNREADABLE. The window-start offset could not be resolved. The partition's records are
//     missing from the count, and the count must say so — treating it as zero would make the
//     broker side short, and short is what loss looks like.
//   - NOTHING THAT RECENT. The broker answered -1: every record the partition holds predates
//     the window. The contribution is a genuine zero.
//   - TRUNCATED. The window-start offset the broker returned is the partition's FIRST retained
//     offset, on a partition that has already had records deleted. The earliest record still
//     present is inside the window, so records that were also inside it may have been removed
//     by retention and the count is a lower bound. This is the reading that matters when the
//     broker's retention is shorter than the reconciliation window — the exact configuration
//     that would otherwise report loss that never happened.
//
// Parameters:
//   - partitionSnapshot *PartitionOffsetSnapshot: filled with the window-start offset.
//   - snapshot *TopicOffsetSnapshot: accumulates the topic's window figures.
//   - windowStarts map[string]map[int]int64: the window-start offsets read from the broker.
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
