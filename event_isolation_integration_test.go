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

//	"Credentials cannot read or list topics, partitions, or DLTs outside their
//	       ACL grant."
//
// That failure is the right outcome but a confusing one to debug, since it looks like
// an isolation defect in Blnk rather than a broker that was never enforcing anything.
// So the broker's configuration is PART OF THE CRITERION, and enforcement is
// established explicitly, twice, before any isolation assertion is read.
//
//  1. DECLARATIVELY, by asking the broker: KafkaAdminClient.AuthorizerActive probes
//     DescribeACLs and reports whether the broker answers SECURITY_DISABLED.
//  2. EMPIRICALLY, by trying it: a freshly provisioned principal must actually be
//     REFUSED on a topic outside its grant.
//
// ProvisionSubscriberPrincipal applies the same rule from the other side — it refuses
// to write a credential at all unless enforcement is confirmed — so an issuance that
// fails with ErrAuthorizerNotEnforcing is likewise surfaced here as the authorizer
// diagnostic rather than as an environment skip.
//
// The test SKIPS unless a broker is configured, so `go test -short ./...` — what `make
// test` runs, and what CI depends on passing with no broker — is unaffected. To
// actually run it:
//
//  1. Bring the local stack up: docker compose --profile kafka up -d kafka kafka-init
//     (or the equivalent for your stack; scripts/kafka-bootstrap.sh formats KRaft
//     storage with the bootstrap SCRAM admin credential, and scripts/kafka-provision.sh
//     creates the category topics and their .dlt siblings.)
//  2. Export the administrative credentials, which are the same variables the service
//     itself reads and are deliberately NOT hardcoded in this file:
//
//     export KAFKA_BROKERS=localhost:9092
//     export KAFKA_SASL_ADMIN_USER=...
//     export KAFKA_SASL_ADMIN_SECRET=...
//     export KAFKA_INSECURE_LOCAL_DEV=true   # the local broker listens SASL_PLAINTEXT
//
//  3. go test -run TestEventIsolation -count=1.
//
// The Kafka client here exists FOR VERIFICATION ONLY: Blnk ships no consumer library
// and no subscriber-side error handling or dead-lettering. Nothing here may be promoted
// into the package.
//
// The credential this test mints is RETURNED BY ISSUANCE AND NEVER PERSISTED: the
// registry keeps a non-reversible reference and an issuance timestamp, so once the
// issuance result is discarded the secret cannot be retrieved from Blnk at all. It
// lives only in this test's memory for the lifetime of the result value, and the
// accessor on that value may be read as often as the test needs — "issued once" is a
// statement about where the secret is stored, not a one-shot read.
//
// It is never logged, never written to a file or a fixture, and never placed in an
// assertion message or a failure operand: assertions about it are boolean predicates
// with secret-free messages rather than NotContains, because a failing NotContains
// prints both operands. The principal and its bindings are revoked in teardown so a
// leaked SCRAM user cannot accumulate in the broker's metadata log and make a later run
// pass for the wrong reason.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// Every identifier in this file carries the eventIsolation prefix.

// The environment variables this test reads. They are the SAME variables the service
// itself reads, so a machine that can run Blnk against Kafka can run this test with no
// extra configuration.
//
// The administrative secret is deliberately NOT defaulted.
const (
	eventIsolationBrokersEnv           = "KAFKA_BROKERS"
	eventIsolationSubscriberBrokersEnv = "KAFKA_SUBSCRIBER_BROKERS"
	eventIsolationTopicPrefixEnv       = "KAFKA_TOPIC_PREFIX"
	eventIsolationAdminUserEnv         = "KAFKA_SASL_ADMIN_USER"
	eventIsolationAdminSecretEnv       = "KAFKA_SASL_ADMIN_SECRET"
	eventIsolationReplicationFactorEnv = "KAFKA_REPLICATION_FACTOR"
	eventIsolationMinPartitionsEnv     = "KAFKA_MIN_PARTITIONS"
	eventIsolationTLSEnabledEnv        = "KAFKA_TLS_ENABLED"
	eventIsolationTLSCAFileEnv         = "KAFKA_TLS_CA_FILE"
	eventIsolationTLSServerNameEnv     = "KAFKA_TLS_SERVER_NAME"
	eventIsolationInsecureLocalDevEnv  = "KAFKA_INSECURE_LOCAL_DEV"
)

// Timeouts. Every broker operation in this file runs under an explicit deadline,
// because the failure mode worth designing against is not a refusal — a refusal is the
// answer the test wants — but a broker that accepts the connection and then stops
// answering.
const (
	// eventIsolationSetupTimeout bounds registration, issuance and the enforcement probes.
	// It is generously larger than SubscriberCredentialIssuanceBudget so that a budget
	// overrun is observed and asserted rather than masked by this deadline.
	eventIsolationSetupTimeout = 30 * time.Second

	// eventIsolationOperationTimeout bounds one authorization probe. A broker answers an
	// authorization decision from memory, so anything approaching this is a fault.
	eventIsolationOperationTimeout = 15 * time.Second

	// eventIsolationTeardownTimeout bounds revocation. Teardown runs on a fresh context
	// rather than the test's, because the test context is usually already cancelled by the
	// time cleanup runs and a cancelled context cannot revoke anything.
	eventIsolationTeardownTimeout = 20 * time.Second

	// eventIsolationDialTimeout bounds the reachability probe and every client dial.
	eventIsolationDialTimeout = 5 * time.Second

	// eventIsolationFetchWait bounds how long a fetch waits for records. The fetches here
	// assert authorization, not delivery, so they must not block for data.
	eventIsolationFetchWait = 500 * time.Millisecond

	// eventIsolationSettleTimeout bounds every wait for BROKER STATE TO CATCH UP, as
	// opposed to a wait for an answer.
	//
	// A KRaft broker holds SCRAM credentials and group coordination in its metadata log,
	// and both become usable slightly after the administrative call that created them
	// returns. Three things in this file were racing that gap and failing on a COLD broker
	// while passing on a warm one — the worst possible shape, because CI always starts
	// cold:
	//
	//   - The FIRST authenticated request made with a just-minted credential answered [58]
	//     SASL Authentication Failed.
	//   - The first consumer-group request answered [16] Not Coordinator For Group,
	//     because with auto.create.topics.enable=false the internal __consumer_offsets
	//     topic does not exist until some client performs group activity, and provisioning
	//     creates only the eight blnk.* topics.
	//   - Teardown read the credential list immediately after revoking and reported a LEAK
	//     that the same run's own log showed had been revoked.
	eventIsolationSettleTimeout = 30 * time.Second

	// eventIsolationSettleInterval is how often a settle wait re-probes.
	eventIsolationSettleInterval = 250 * time.Millisecond
)

// eventIsolationMaxFetchBytes caps a verification fetch. The bytes are never inspected —
// only the authorization outcome is — so this is small on purpose.
const eventIsolationMaxFetchBytes = 8 * 1024

// eventIsolationEnvironment is the resolved Kafka environment this test runs against.
type eventIsolationEnvironment struct {
	// kafka is the configuration block handed to NewKafkaAdmin and published to the
	// configuration store so that topic naming resolves the same prefix the broker was
	// provisioned with.
	kafka config.KafkaConfig

	// brokerAddress is the first bootstrap address, used for the reachability probe and
	// as the verification client's Addr.
	brokerAddress string
}

// eventIsolationResolveEnvironment builds the Kafka configuration from the environment,
// or explains what is missing.
//
// It returns a REASON rather than calling t.Skip itself so that the caller decides
// whether an absent value is a skip or a failure.
func eventIsolationResolveEnvironment(t *testing.T) (eventIsolationEnvironment, string, bool) {
	t.Helper()

	brokers := eventIsolationSplitBrokers(os.Getenv(eventIsolationBrokersEnv))
	if len(brokers) == 0 {
		return eventIsolationEnvironment{}, fmt.Sprintf(
			"%s is not set, so there is no Kafka broker to prove subscriber isolation against",
			eventIsolationBrokersEnv,
		), false
	}

	// THE SUBSCRIBER-FACING LIST IS A SEPARATE REQUIREMENT, and it is deliberately NOT
	// defaulted to the list above.
	subscriberBrokers := eventIsolationSplitBrokers(os.Getenv(eventIsolationSubscriberBrokersEnv))
	if len(subscriberBrokers) == 0 {
		return eventIsolationEnvironment{}, fmt.Sprintf(
			"%s is not set, so no subscriber-facing broker list exists and credential issuance "+
				"refuses by design. For the local stack it is the broker's external listener, the "+
				"same value .env.example and docker-compose.yaml carry",
			eventIsolationSubscriberBrokersEnv,
		), false
	}

	adminUser := strings.TrimSpace(os.Getenv(eventIsolationAdminUserEnv))
	adminSecret := os.Getenv(eventIsolationAdminSecretEnv)
	if adminUser == "" || adminSecret == "" {
		return eventIsolationEnvironment{}, fmt.Sprintf(
			"%s and %s must both be set: provisioning a subscriber principal, granting its ACLs and "+
				"probing whether the broker enforces them are all administrative operations",
			eventIsolationAdminUserEnv, eventIsolationAdminSecretEnv,
		), false
	}

	tlsEnabled := eventIsolationBoolEnv(eventIsolationTLSEnabledEnv, false)

	kafkaConfig := config.KafkaConfig{
		Brokers:           brokers,
		SubscriberBrokers: subscriberBrokers,
		TopicPrefix:       strings.TrimSpace(os.Getenv(eventIsolationTopicPrefixEnv)),
		SASLAdminUser:     adminUser,
		SASLAdminSecret:   adminSecret,
		MinPartitions:     eventIsolationIntEnv(eventIsolationMinPartitionsEnv, MinTopicPartitions),
		ReplicationFactor: eventIsolationIntEnv(eventIsolationReplicationFactorEnv, 1),
		TLS: config.KafkaTLSConfig{
			Enabled:    tlsEnabled,
			CAFile:     strings.TrimSpace(os.Getenv(eventIsolationTLSCAFileEnv)),
			ServerName: strings.TrimSpace(os.Getenv(eventIsolationTLSServerNameEnv)),
		},
		// NewKafkaTransport refuses to dial in the clear unless this is set, which is the
		// right default for production and exactly what the local single-broker KRaft stack
		// needs waived: it listens on SASL_PLAINTEXT and nothing else. Defaulting it to true
		// ONLY when TLS is off keeps the waiver scoped to the case that needs it, and an
		// operator running against a TLS broker never sees it applied.
		InsecureLocalDev: eventIsolationBoolEnv(eventIsolationInsecureLocalDevEnv, !tlsEnabled),
	}

	return eventIsolationEnvironment{
		kafka:         kafkaConfig,
		brokerAddress: brokers[0],
	}, "", true
}

// eventIsolationSplitBrokers parses a comma-separated bootstrap list, dropping blanks.
//
// A trailing comma in KAFKA_BROKERS otherwise yields an empty element, which kafka-go
// canonicalises to ":9092" and then dials on the local host — a failure that names no
// broker.
func eventIsolationSplitBrokers(raw string) []string {
	brokers := make([]string, 0, 1)
	for _, candidate := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}

	if len(brokers) == 0 {
		return nil
	}

	return brokers
}

// eventIsolationBoolEnv reads a boolean environment variable with a fallback.
//
// An unparseable value falls back rather than failing: this is test scaffolding reading
// an operator's shell, and a typo in KAFKA_TLS_ENABLED should not be reported as an
// isolation defect.
func eventIsolationBoolEnv(name string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}

	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}

	return parsed
}

// eventIsolationIntEnv reads a positive integer environment variable with a fallback.
func eventIsolationIntEnv(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return fallback
	}

	return parsed
}

// eventIsolationPublishConfiguration publishes cnf to config.ConfigStore and restores
// whatever was there when the test finishes.
func eventIsolationPublishConfiguration(t *testing.T, cnf *config.Configuration) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}

		config.ConfigStore.Store(&config.Configuration{})
	})

	config.ConfigStore.Store(cnf)
}

// eventIsolationKeyScopeGatewayBrokers is the address a declared key-authorising
// component listens on, for the tests that need one declared.
//
// TWO PROPERTIES MATTER. It is UNROUTABLE, because no probe here dials it — Blnk ships no
// component to run there, so all a test can assert is which address a credential NAMES.
// And it is DISTINCT from env.kafka.Brokers, because config.KafkaConfig.KeyScopeGateway
// reads a gateway list equal to the broker list as NOT enforcing.
var eventIsolationKeyScopeGatewayBrokers = []string{"keyscope-gateway.invalid:9095"}

// eventIsolationDeclareKeyScopeGateway republishes the fixture's configuration with
// key-scope enforcement DECLARED, and returns the gateway list it declared.
//
// Issuance REFUSES a subscriber recording a partition_key_prefix unless the deployment
// has declared a component that authorises record keys, because Blnk ships none and
// serves no records itself: the only credentials it could otherwise mint are one
// carrying whole-topic Read beside a prefix nothing applies, or one that can fetch
// nothing at all. So a test that needs a key-scoped principal to EXIST at the broker
// declares one here first.
//
// Parameters:
//   - t *testing.T: for the helper marker and the configuration restore.
//   - fixture *eventIsolationFixture: supplies the environment's Kafka configuration,
//     which is republished with only the two key-scope fields added.
//
// Returns:
//   - []string: the declared gateway bootstrap list, which is what a key-scoped
//     credential must report in place of the subscriber-facing brokers.
func eventIsolationDeclareKeyScopeGateway(t *testing.T, fixture *eventIsolationFixture) []string {
	t.Helper()

	gateway, _ := eventIsolationDeclareKeyScopeGatewayWithDouble(t, fixture)

	return gateway
}

// eventIsolationDeclareKeyScopeGatewayWithDouble is the same declaration, returning the
// CONTROL double as well so a test can assert what Blnk registered at it.
//
// A mode and a distinct bootstrap list are assertions a deployment makes about itself,
// and any address satisfies them.
//
// Parameters:
//   - t *testing.T: for the helper marker, the double's lifetime and the configuration
//     restore.
//   - fixture *eventIsolationFixture: supplies the environment's Kafka configuration.
//
// Returns:
//   - []string: the declared gateway bootstrap list.
//   - *keyScopeGatewayDouble: the control component, for assertions about the binding
//     it holds.
func eventIsolationDeclareKeyScopeGatewayWithDouble(
	t *testing.T,
	fixture *eventIsolationFixture,
) ([]string, *keyScopeGatewayDouble) {
	t.Helper()

	double, endpoint := newKeyScopeGatewayDouble(t)

	declared := fixture.env.kafka
	declared.KeyScopeEnforcement = config.KeyScopeEnforcementBrokerGateway
	declared.KeyScopeGatewayBrokers = append([]string(nil), eventIsolationKeyScopeGatewayBrokers...)
	declared.KeyScopeGatewayAttestationURL = endpoint
	declared.KeyScopeGatewayAttestationToken = double.token

	require.NotEqual(t, declared.Brokers, declared.KeyScopeGatewayBrokers,
		"the declared gateway must be DISTINCT from the broker list, or KeyScopeGateway reads it as "+
			"no declaration and issuance refuses anyway")

	eventIsolationPublishConfiguration(t, &config.Configuration{Kafka: declared})

	gateway, active := declared.KeyScopeGateway()
	require.True(t, active,
		"the republished configuration must report an ACTIVE enforcement point, or every key-scoped "+
			"issuance below refuses and the tests assert the wrong thing")

	return gateway, double
}

// eventIsolationRequireBroker skips the test when no broker answers a TCP dial.
//
// It is a plain dial rather than a Kafka round trip on purpose: this question is only
// "is there something listening?", and answering it without SASL keeps an unreachable
// broker distinguishable from a broker that rejects the administrative credentials.
func eventIsolationRequireBroker(t *testing.T, address string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", address, eventIsolationDialTimeout)
	if err != nil {
		t.Skipf(
			"no Kafka broker is listening on %s (%v); bring the stack up with "+
				"`docker compose --profile kafka up -d kafka kafka-init` before running the subscriber-isolation test",
			address, err,
		)
	}

	require.NoError(t, conn.Close(), "closing the broker reachability probe")
}

// eventIsolationRequireAuthorizer FAILS the test when the broker does not enforce ACLs.
//
// A skip would say "this machine cannot answer the question", and that is the wrong
// answer: a broker that answers but authorises everything makes every isolation
// assertion below pass vacuously. An unreachable broker and an unenforcing one are
// therefore both fatal.
func eventIsolationRequireAuthorizer(t *testing.T, ctx context.Context, admin *KafkaAdminClient) {
	t.Helper()

	active, err := admin.AuthorizerActive(ctx)
	if err != nil {
		t.Fatalf(
			"THE BROKER'S ACL ENFORCEMENT COULD NOT BE CONFIRMED, so subscriber isolation cannot be "+
				"proven and this test refuses to report a pass: %v\n"+
				"An unverifiable boundary is treated exactly as an absent one. Either the administrative "+
				"principal (%s) is not allowed to describe ACLs on the cluster, or the broker is "+
				"unreachable. Grant it Describe on the cluster, or fix reachability, then re-run.",
			err, eventIsolationAdminUserEnv,
		)
	}

	if !active {
		t.Fatal(
			"THE BROKER REPORTS THAT SECURITY IS DISABLED: it has no authorizer configured, so ACL " +
				"bindings are accepted and never applied. Every principal can read every topic, " +
				"including every other subscriber's topics and all the dead-letter topics, and every " +
				"isolation assertion below would pass while proving nothing.\n" +
				"Start the broker with " +
				"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer — " +
				"docker-compose.yaml and docker-compose.dev.yaml set it for exactly this reason, and " +
				"scripts/kafka-bootstrap.sh warns when the configuration it formats does not.",
		)
	}
}

// eventIsolationRequireTopics skips the test when the topics it needs are not
// provisioned.
//
// It runs AFTER the authorizer check, and that order is deliberate: a provisioning gap
// must never be able to mask a broker with no enforcement.
func eventIsolationRequireTopics(
	t *testing.T,
	ctx context.Context,
	admin *KafkaAdminClient,
	topics []string,
) {
	t.Helper()

	// A zero instant asks for end offsets only: this probe cares whether the topics EXIST,
	// not how much of their content is recent, and asking for a window would add a
	// ListOffsets round trip whose answer nothing here reads.
	report, err := admin.TopicEndOffsets(ctx, time.Time{}, topics...)
	if err != nil {
		t.Skipf(
			"the Kafka topics this test needs could not be read (%v); run scripts/kafka-provision.sh "+
				"to create %s and their .dlt siblings",
			err, strings.Join(topics, ", "),
		)
	}

	present := report.EndOffsetsByTopic()
	missing := make([]string, 0, len(topics))
	for _, topic := range topics {
		if _, ok := present[topic]; !ok {
			missing = append(missing, topic)
		}
	}

	if len(missing) > 0 {
		t.Skipf(
			"the Kafka topics %s are not provisioned; run scripts/kafka-provision.sh "+
				"(or `docker compose --profile kafka up -d kafka-init`) before running the subscriber-isolation test",
			strings.Join(missing, ", "),
		)
	}
}

// eventIsolationStore is an in-memory eventSubscriberStore.
//
// The property under test lives entirely at the BROKER — does the authorizer refuse a
// principal the operations its grant does not cover?
type eventIsolationStore struct {
	mu   sync.Mutex
	rows map[string]model.EventSubscriber

	// fences mirrors the provisioning_token / provisioning_until pair the real table
	// carries. It is a SEPARATE map rather than two fields on the row because the
	// repository deliberately does not project those columns into model.EventSubscriber —
	// that struct is serialised into API responses, and a live claim token in a response
	// body would be an internal lock handed to a caller.
	fences map[string]eventIsolationFence

	// obligations mirrors the five settlement columns, and is a separate map for the same
	// reason fences is: they are operational bookkeeping the repository deliberately does
	// not project into model.EventSubscriber, because that struct is serialised into API
	// responses.
	obligations map[string]eventIsolationObligation
}

// eventIsolationObligation is one subscriber's outstanding broker-side settlement work.
type eventIsolationObligation struct {
	grantPendingAt      time.Time
	credentialCleanupAt time.Time
	attempts            int
	lastError           string
	lastAttemptAt       time.Time
}

// outstanding reports whether anything is owed.
func (o eventIsolationObligation) outstanding() bool {
	return !o.grantPendingAt.IsZero() || !o.credentialCleanupAt.IsZero()
}

// eventIsolationFence is one live provisioning claim: the token it is held under and the
// instant it expires.
type eventIsolationFence struct {
	token string
	until time.Time
}

// eventIsolationNewStore builds an empty registry.
func eventIsolationNewStore() *eventIsolationStore {
	return &eventIsolationStore{
		rows:        make(map[string]model.EventSubscriber),
		fences:      make(map[string]eventIsolationFence),
		obligations: make(map[string]eventIsolationObligation),
	}
}

// eventIsolationSubscriberNotFound is the typed error a missing row reports.
func eventIsolationSubscriberNotFound(subscriberID string) error {
	return apierror.NewAPIError(
		apierror.ErrSubscriberNotFound,
		"Event subscriber not found",
		fmt.Errorf("event isolation store: no subscriber %q is registered", subscriberID),
	)
}

func (s *eventIsolationStore) clone(row model.EventSubscriber) *model.EventSubscriber {
	copied := row
	copied.AuthorizedTopics = append([]string(nil), row.AuthorizedTopics...)

	return &copied
}

// CreateEventSubscriber registers a subscriber and returns the stored row.
func (s *eventIsolationStore) CreateEventSubscriber(
	_ context.Context,
	subscriber *model.EventSubscriber,
) (*model.EventSubscriber, error) {
	if subscriber == nil || strings.TrimSpace(subscriber.SubscriberID) == "" {
		return nil, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"A subscriber with a business key is required",
			errors.New("event isolation store: subscriber is nil or has no subscriber_id"),
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.rows[subscriber.SubscriberID]; exists {
		return nil, apierror.NewAPIError(
			apierror.ErrConflict,
			"A subscriber with that identifier is already registered",
			fmt.Errorf("event isolation store: subscriber %q already exists", subscriber.SubscriberID),
		)
	}

	row := *subscriber
	row.AuthorizedTopics = append([]string(nil), subscriber.AuthorizedTopics...)
	row.ID = int64(len(s.rows) + 1)
	now := time.Now().UTC()
	row.CreatedAt = now
	row.UpdatedAt = now
	s.rows[row.SubscriberID] = row

	return s.clone(row), nil
}

// GetEventSubscriberByID reads one subscriber by its business key.
func (s *eventIsolationStore) GetEventSubscriberByID(
	_ context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	row, ok := s.rows[strings.TrimSpace(subscriberID)]
	if !ok {
		return nil, eventIsolationSubscriberNotFound(subscriberID)
	}

	return s.clone(row), nil
}

// CountEventSubscribers counts the seeded rows. Nothing in this file reads it — the isolation
// assertions address subscribers by key — but the store must satisfy the whole seam.
func (s *eventIsolationStore) CountEventSubscribers(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return int64(len(s.rows)), nil
}

// ListAndCountEventSubscribers answers the page and the total from one observation of the
// fake's state, under a single lock acquisition, matching the repository's single snapshot.
func (s *eventIsolationStore) ListAndCountEventSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, int64, error) {
	page, err := s.ListEventSubscribers(ctx, query)
	if err != nil {
		return model.SubscriberPage{}, 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return page, int64(len(s.rows)), nil
}

// ListEventSubscribers pages the registry. Ordering is unspecified here because nothing in
// this file depends on it; the isolation assertions address subscribers by key.
func (s *eventIsolationStore) ListEventSubscribers(
	_ context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows := make([]model.EventSubscriber, 0, len(s.rows))
	for _, row := range s.rows {
		rows = append(rows, *s.clone(row))
	}

	page := model.SubscriberPage{Subscribers: rows}
	if query.Limit > 0 && query.Limit < len(rows) {
		page.Subscribers = rows[:query.Limit]
		page.HasMore = true
	}

	return page, nil
}

// UpdateEventSubscriber replaces the mutable columns of an existing row, under the
// caller's provisioning claim and only while the row is not tombstoned for
// deregistration.
//
// Both predicates are reproduced rather than simplified.
func (s *eventIsolationStore) UpdateEventSubscriber(
	_ context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
) (*model.EventSubscriber, error) {
	if subscriber == nil {
		return nil, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"A subscriber is required",
			errors.New("event isolation store: subscriber is nil"),
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriber.SubscriberID, fenceToken, true); err != nil {
		return nil, err
	}

	existing := s.rows[strings.TrimSpace(subscriber.SubscriberID)]

	row := *subscriber
	row.AuthorizedTopics = append([]string(nil), subscriber.AuthorizedTopics...)
	row.ID = existing.ID
	row.CreatedAt = existing.CreatedAt
	row.UpdatedAt = time.Now().UTC()
	s.rows[row.SubscriberID] = row

	// The STORED row, matching RETURNING: the caller must never answer with the copy it handed in,
	// because that copy carries the updated_at it read before this write.
	stored := row

	return &stored, nil
}

// TakeEventSubscriber removes a subscriber and returns the row it removed, so the caller
// still holds the principal and topics that broker-side revocation needs.
func (s *eventIsolationStore) TakeEventSubscriber(
	_ context.Context,
	subscriberID string,
	fenceToken string,
) (*model.EventSubscriber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return nil, err
	}

	key := strings.TrimSpace(subscriberID)
	row := s.rows[key]

	delete(s.rows, key)
	delete(s.fences, key)

	return s.clone(row), nil
}

// RecordSubscriberCredentialIfUnchanged persists an issuance only while the row still
// holds the reference observed before provisioning.
func (s *eventIsolationStore) RecordSubscriberCredentialIfUnchanged(
	_ context.Context,
	subscriberID string,
	expected *string,
	credentialReference string,
	issuedAt time.Time,
	fenceToken string,
) error {
	if err := model.ValidateCredentialReference(credentialReference); err != nil {
		return apierror.NewAPIError(
			apierror.ErrInvalidInput,
			"Credential reference must be a reference derived by model.DeriveCredentialReference",
			nil,
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return err
	}

	key := strings.TrimSpace(subscriberID)
	row := s.rows[key]

	if !eventIsolationReferencesMatch(row.CredentialReference, expected) {
		return apierror.NewAPIError(
			apierror.ErrConflict,
			"The subscriber's credential changed while this issuance was in flight",
			fmt.Errorf(
				"event isolation store: subscriber %q no longer holds the expected credential reference",
				key,
			),
		)
	}

	reference := credentialReference
	stamped := issuedAt
	row.CredentialReference = &reference
	row.CredentialIssuedAt = &stamped
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	return nil
}

// ClearSubscriberCredential returns a row to the "registered, not yet provisioned" state,
// under the caller's provisioning claim so a stale owner cannot blank a newer issuance's record.
func (s *eventIsolationStore) ClearSubscriberCredential(
	_ context.Context,
	subscriberID, fenceToken string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return err
	}

	key := strings.TrimSpace(subscriberID)
	row := s.rows[key]

	row.CredentialReference = nil
	row.CredentialIssuedAt = nil
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	// One statement, one fact: the row's credential columns and the cleanup marker both describe
	// whether Blnk records a credential it has not settled.
	s.dischargeCredentialCleanupLocked(key)

	return nil
}

// dischargeCredentialCleanupLocked clears the cleanup marker, resetting the counters only when
// nothing else remains owed. Callers must hold the mutex.
func (s *eventIsolationStore) dischargeCredentialCleanupLocked(key string) {
	obligation, ok := s.obligations[key]
	if !ok {
		return
	}

	obligation.credentialCleanupAt = time.Time{}

	if obligation.grantPendingAt.IsZero() {
		obligation.attempts = 0
		obligation.lastError = ""
	}

	s.obligations[key] = obligation
}

// RecordSubscriberGrantReconcilePending marks a pending broker-side grant reconciliation,
// keeping the first instant.
func (s *eventIsolationStore) RecordSubscriberGrantReconcilePending(
	_ context.Context,
	subscriberID string,
	pendingAt time.Time,
	fenceToken string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return err
	}

	key := strings.TrimSpace(subscriberID)
	obligation := s.obligations[key]

	if obligation.grantPendingAt.IsZero() {
		if pendingAt.IsZero() {
			pendingAt = time.Now()
		}

		obligation.grantPendingAt = pendingAt.UTC()
	}

	s.obligations[key] = obligation

	return nil
}

// ClearSubscriberGrantReconcilePending discharges the grant-reconciliation marker.
func (s *eventIsolationStore) ClearSubscriberGrantReconcilePending(
	_ context.Context,
	subscriberID, fenceToken string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return err
	}

	key := strings.TrimSpace(subscriberID)
	obligation := s.obligations[key]
	obligation.grantPendingAt = time.Time{}

	if obligation.credentialCleanupAt.IsZero() {
		obligation.attempts = 0
		obligation.lastError = ""
	}

	s.obligations[key] = obligation

	return nil
}

// RecordSubscriberCredentialCleanupPending marks a pending credential cleanup, keeping the first
// instant.
func (s *eventIsolationStore) RecordSubscriberCredentialCleanupPending(
	_ context.Context,
	subscriberID string,
	pendingAt time.Time,
	fenceToken string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return err
	}

	key := strings.TrimSpace(subscriberID)
	obligation := s.obligations[key]

	if obligation.credentialCleanupAt.IsZero() {
		if pendingAt.IsZero() {
			pendingAt = time.Now()
		}

		obligation.credentialCleanupAt = pendingAt.UTC()
	}

	s.obligations[key] = obligation

	return nil
}

// GetSubscriberSettlementObligation reads what one subscriber owes.
func (s *eventIsolationStore) GetSubscriberSettlementObligation(
	_ context.Context,
	subscriberID string,
) (model.SubscriberSettlementObligation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	if _, ok := s.rows[key]; !ok {
		return model.SubscriberSettlementObligation{}, eventIsolationSubscriberNotFound(subscriberID)
	}

	obligation := s.obligations[key]

	return model.SubscriberSettlementObligation{
		SubscriberID:             key,
		GrantReconcilePending:    !obligation.grantPendingAt.IsZero(),
		CredentialCleanupPending: !obligation.credentialCleanupAt.IsZero(),
		Attempts:                 obligation.attempts,
		LastError:                obligation.lastError,
	}, nil
}

// ListSubscriberSettlementObligations returns the outstanding obligations, oldest attempt first.
func (s *eventIsolationStore) ListSubscriberSettlementObligations(
	_ context.Context,
	limit int,
	notBefore time.Time,
) ([]model.SubscriberSettlementObligation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if limit <= 0 {
		limit = 50
	}

	obligations := make([]model.SubscriberSettlementObligation, 0, len(s.obligations))

	for key, obligation := range s.obligations {
		if !obligation.outstanding() || len(obligations) >= limit {
			continue
		}

		if !notBefore.IsZero() && !obligation.lastAttemptAt.IsZero() &&
			!obligation.lastAttemptAt.Before(notBefore) {
			continue
		}

		obligations = append(obligations, model.SubscriberSettlementObligation{
			SubscriberID:             key,
			GrantReconcilePending:    !obligation.grantPendingAt.IsZero(),
			CredentialCleanupPending: !obligation.credentialCleanupAt.IsZero(),
			Attempts:                 obligation.attempts,
			LastError:                obligation.lastError,
		})
	}

	return obligations, nil
}

// MarkSubscriberSettlementAttempt records a settlement attempt without discharging anything.
func (s *eventIsolationStore) MarkSubscriberSettlementAttempt(
	_ context.Context,
	subscriberID string,
	attemptedAt time.Time,
	failure string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	obligation := s.obligations[key]

	if attemptedAt.IsZero() {
		attemptedAt = time.Now()
	}

	obligation.attempts++
	obligation.lastAttemptAt = attemptedAt.UTC()
	obligation.lastError = failure
	s.obligations[key] = obligation

	return nil
}

// MarkSubscriberMigrated stamps the instant a subscriber completed its move to Kafka.
// RecordSubscriberWebhookURL writes the URL and clears migrated_at, as the repository does.
func (s *eventIsolationStore) RecordSubscriberWebhookURL(
	_ context.Context,
	subscriberID, webhookURL string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return eventIsolationSubscriberNotFound(subscriberID)
	}

	recorded := webhookURL
	row.WebhookURL = &recorded
	row.MigratedAt = nil
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	return nil
}

// ClearSubscriberWebhookURL forgets the URL and leaves migrated_at alone.
func (s *eventIsolationStore) ClearSubscriberWebhookURL(
	_ context.Context,
	subscriberID string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return eventIsolationSubscriberNotFound(subscriberID)
	}

	row.WebhookURL = nil
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	return nil
}

func (s *eventIsolationStore) MarkSubscriberMigrated(
	_ context.Context,
	subscriberID string,
	migratedAt time.Time,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return eventIsolationSubscriberNotFound(subscriberID)
	}

	stamped := migratedAt
	row.MigratedAt = &stamped
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	return nil
}

// CompleteSubscriberWebhookMigration forgets the legacy endpoint AND stamps the migration
// instant together, which is the atomicity the cutover endpoint depends on.
func (s *eventIsolationStore) CompleteSubscriberWebhookMigration(
	_ context.Context,
	subscriberID string,
	migratedAt time.Time,
) (*model.EventSubscriber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return nil, eventIsolationSubscriberNotFound(subscriberID)
	}

	stamped := migratedAt
	row.WebhookURL = nil
	row.MigratedAt = &stamped
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	return s.clone(row), nil
}

// PurgeMigratedSubscriberWebhookURLs erases the legacy URL of every subscriber that
// migrated strictly before the cut-off.
func (s *eventIsolationStore) PurgeMigratedSubscriberWebhookURLs(
	_ context.Context,
	migratedBefore time.Time,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var purged int64
	for key, row := range s.rows {
		if row.WebhookURL == nil || row.MigratedAt == nil || !row.MigratedAt.Before(migratedBefore) {
			continue
		}

		row.WebhookURL = nil
		row.UpdatedAt = time.Now().UTC()
		s.rows[key] = row
		purged++
	}

	return purged, nil
}

// MarkSubscriberRevocationPending stamps the revocation tombstone and returns the row.
//
// The repository keeps the FIRST instant on a re-mark (COALESCE), because the value an
// operator needs is how long the revocation has been outstanding rather than the age of
// the last attempt.
//
// Parameters:
//   - _ context.Context: unused; the store is local.
//   - subscriberID string: the business key.
//   - pendingAt time.Time: the instant to record on the FIRST marking.
//   - fenceToken string: the provisioning claim the caller holds.
//
// Returns:
//   - *model.EventSubscriber: the marked row, carrying the principal and topics to
//     revoke.
//   - error: ErrSubscriberNotFound when absent, ErrConflict when the claim is not the
//     caller's.
func (s *eventIsolationStore) MarkSubscriberRevocationPending(
	_ context.Context,
	subscriberID string,
	pendingAt time.Time,
	fenceToken string,
) (*model.EventSubscriber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// requireActive is FALSE: re-marking an already-tombstoned row is a deregistration retry,
	// and finishing one is the whole reason the tombstone survives a failed revocation.
	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return nil, err
	}

	key := strings.TrimSpace(subscriberID)
	row := s.rows[key]

	if row.RevocationPendingAt == nil {
		stamped := pendingAt
		row.RevocationPendingAt = &stamped
	}

	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	return s.clone(row), nil
}

// ClaimSubscriberForProvisioning fences a subscriber for one issuance or revocation.
//
// The two refusal branches are kept distinct exactly as the repository keeps them: a
// subscriber that was never registered reports ErrSubscriberNotFound, and one that
// exists under a live claim reports ErrConflict.
//
// Parameters:
//   - _ context.Context: unused; the store is local.
//   - subscriberID string: the business key.
//   - lease time.Duration: how long the claim is held. Non-positive falls back to the
//     service's fence lease, matching the repository's own fallback.
//
// Returns:
//   - string: the token the claim is held under.
//   - error: ErrSubscriberNotFound when absent, ErrConflict when already claimed.
func (s *eventIsolationStore) ClaimSubscriberForProvisioning(
	_ context.Context,
	subscriberID string,
	lease time.Duration,
) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	if _, ok := s.rows[key]; !ok {
		return "", eventIsolationSubscriberNotFound(subscriberID)
	}

	if lease <= 0 {
		lease = SubscriberProvisioningFenceLease
	}

	now := time.Now().UTC()
	if held, ok := s.fences[key]; ok && held.until.After(now) {
		return "", apierror.NewAPIError(
			apierror.ErrConflict,
			"Another credential operation for this subscriber is already in progress",
			fmt.Errorf("event isolation store: subscriber %q is fenced until %s", subscriberID, held.until),
		)
	}

	token := uuid.NewString()
	s.fences[key] = eventIsolationFence{token: token, until: now.Add(lease)}

	row := s.rows[key]
	row.UpdatedAt = now
	s.rows[key] = row

	return token, nil
}

func (s *eventIsolationStore) ReleaseSubscriberProvisioningFence(
	_ context.Context,
	subscriberID string,
	token string,
) error {
	if strings.TrimSpace(token) == "" {
		return apierror.NewAPIError(
			apierror.ErrInvalidInput,
			"A provisioning claim token is required to release the fence",
			errors.New("event isolation store: no claim token was supplied"),
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// THE SHARED PREDICATE, which is token AND LEASE. Each of these three sites tested the
	// token alone, so an EXPIRED claim was accepted as held — and the production
	// statements carry `provisioning_until > NOW()` in the write itself, so the fake was
	// modelling a permission the database does not grant.
	key := strings.TrimSpace(subscriberID)
	if err := s.requireFenceLocked(subscriberID, token, "releasing the provisioning claim"); err != nil {
		return err
	}

	delete(s.fences, key)

	return nil
}

// RenewSubscriberProvisioningFence extends a claim the caller still holds.
//
// It is CONDITIONAL and it does NOT re-claim, exactly as the repository's is.
//
// Parameters:
//   - _ context.Context: unused; the store is local.
//   - subscriberID string: the business key.
//   - token string: the token the claim was taken under.
//   - lease time.Duration: how much longer the claim is held FROM NOW.
//
// Returns:
//   - error: ErrInvalidInput for a missing token, ErrSubscriberNotFound when absent,
//     ErrConflict when the claim is no longer the caller's.
func (s *eventIsolationStore) RenewSubscriberProvisioningFence(
	_ context.Context,
	subscriberID string,
	token string,
	lease time.Duration,
) error {
	if strings.TrimSpace(token) == "" {
		return apierror.NewAPIError(
			apierror.ErrInvalidInput,
			"A provisioning claim token is required to renew the fence",
			errors.New("event isolation store: no claim token was supplied"),
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	if _, ok := s.rows[key]; !ok {
		return eventIsolationSubscriberNotFound(subscriberID)
	}

	if err := s.requireFenceLocked(subscriberID, token, "renewing the provisioning claim"); err != nil {
		return err
	}

	held := s.fences[key]

	if lease <= 0 {
		lease = SubscriberProvisioningFenceLease
	}

	held.until = time.Now().UTC().Add(lease)
	s.fences[key] = held

	return nil
}

// fencedWriteGuardLocked reproduces the WHERE clause every fenced write carries.
//
// s.mu must already be held.
//
// Parameters:
//   - subscriberID string: the business key.
//   - token string: the claim the caller presented.
//   - requireActive bool: true for a write that must refuse a tombstoned row.
//
// Returns:
//   - error: nil when the write may proceed.
func (s *eventIsolationStore) fencedWriteGuardLocked(
	subscriberID, token string,
	requireActive bool,
) error {
	if strings.TrimSpace(token) == "" {
		return apierror.NewAPIError(
			apierror.ErrInvalidInput,
			"A provisioning claim token is required to modify this subscriber",
			errors.New("event isolation store: a fenced write was attempted with no claim token"),
		)
	}

	key := strings.TrimSpace(subscriberID)

	row, ok := s.rows[key]
	if !ok {
		return eventIsolationSubscriberNotFound(subscriberID)
	}

	if err := s.requireFenceLocked(subscriberID, token, "applying a fenced write"); err != nil {
		return err
	}

	if requireActive && row.RevocationPendingAt != nil {
		return apierror.NewAPIError(
			// The code the real datasource answers with for this miss; see the same branch in
			// database.fencedWriteMissError.
			apierror.ErrSubscriberDeprovisioning,
			"This subscriber is being deregistered, so its access model can no longer be changed",
			fmt.Errorf("event isolation store: subscriber %q carries a revocation tombstone", subscriberID),
		)
	}

	return nil
}

// eventIsolationReferencesMatch compares a stored credential reference with the one an
// issuance observed before it started.
//
// Two nils match — that is the first-issuance case — and a nil on one side only does
// not, which is the race the conditional write exists to catch.
func eventIsolationReferencesMatch(stored, expected *string) bool {
	switch {
	case stored == nil && expected == nil:
		return true
	case stored == nil || expected == nil:
		return false
	default:
		return *stored == *expected
	}
}

// Compile-time proof that the in-memory registry satisfies the seam the subscriber service
// requires. A signature drift in eventSubscriberStore fails the build on this line, which
// names the contract, rather than at the constructor call below.
var _ eventSubscriberStore = (*eventIsolationStore)(nil)

// eventIsolationFixture is one prepared run: a broker whose enforcement has been
// confirmed, an administrative client, the registry service wired to it, and the topic
// names the assertions address.
type eventIsolationFixture struct {
	env     eventIsolationEnvironment
	admin   *KafkaAdminClient
	service *EventSubscriberService
	store   *eventIsolationStore

	// grantable is every category topic, in the order EventTopics composes them. The
	// fixture asserts there are at least two so that one can be granted and another
	// deliberately withheld.
	grantable []string
}

// eventIsolationSetup performs the whole guard sequence and returns a prepared fixture.
//
//  1. SHORT MODE. `make test` runs `go test -short ./...` and CI depends on it passing
//     with no broker in sight, so short mode skips before anything dials.
//  2. CONFIGURATION. Absent broker or administrative credentials is an environment gap:
//     skip, naming the variable.
//  3. REACHABILITY. Nothing listening is an environment gap: skip, naming the address.
//  4. ENFORCEMENT. A broker that does not enforce ACLs is a FAILURE, and it is checked
//     BEFORE topics so that a provisioning gap can never mask it.
//  5. TOPICS. Unprovisioned topics is an environment gap: skip, naming the script.
func eventIsolationSetup(t *testing.T) (*eventIsolationFixture, context.Context) {
	t.Helper()

	if testing.Short() {
		t.Skip(
			"skipping the Kafka subscriber-isolation integration test in short mode: it requires a " +
				"live broker running the KRaft StandardAuthorizer",
		)
	}

	env, reason, ok := eventIsolationResolveEnvironment(t)
	if !ok {
		t.Skipf("skipping the Kafka subscriber-isolation integration test: %s", reason)
	}

	eventIsolationRequireBroker(t, env.brokerAddress)

	// Published before the administrative client is built, because the topic-naming layer
	// reads the prefix from here and the fixture's topic names must be the ones the broker
	// was provisioned with.
	eventIsolationPublishConfiguration(t, &config.Configuration{Kafka: env.kafka})

	admin, err := NewKafkaAdmin(&config.Configuration{Kafka: env.kafka})
	require.NoError(t, err,
		"building the administrative Kafka client from the environment; check the %s/%s pair and the TLS settings",
		eventIsolationAdminUserEnv, eventIsolationAdminSecretEnv)
	require.True(t, admin.IsConfigured(),
		"the administrative Kafka client reports no configured broker even though %s is set",
		eventIsolationBrokersEnv)
	t.Cleanup(func() {
		if closeErr := admin.Close(); closeErr != nil {
			t.Logf("closing the administrative Kafka client: %v", closeErr)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), eventIsolationSetupTimeout)
	t.Cleanup(cancel)

	eventIsolationRequireAuthorizer(t, ctx, admin)

	grantable := SubscriberGrantableTopics()
	require.GreaterOrEqual(t, len(grantable), 2,
		"subscriber isolation needs at least two grantable category topics so that one can be "+
			"granted and another withheld; SubscriberGrantableTopics returned %v", grantable)

	// Every topic the assertions touch must exist, proven by the administrative client:
	// the category topics on both sides of the grant boundary and their dead-letter
	// siblings. Proving existence here is what turns a later "unknown topic" answer to a
	// subscriber into evidence of non-disclosure rather than evidence of an empty broker.
	required := make([]string, 0, len(grantable)*2)
	for _, topic := range grantable {
		required = append(required, topic, DLTFor(topic))
	}

	eventIsolationRequireTopics(t, ctx, admin, required)

	store := eventIsolationNewStore()
	service := NewEventSubscriberService(store, admin)
	t.Cleanup(func() {
		// The administrative client was INJECTED, so the service does not own it and does
		// not close it; this call releases only what the service itself holds.
		if closeErr := service.Close(); closeErr != nil {
			t.Logf("closing the event subscriber service: %v", closeErr)
		}
	})

	return &eventIsolationFixture{
		env:       env,
		admin:     admin,
		service:   service,
		store:     store,
		grantable: grantable,
	}, ctx
}

// eventIsolationPrincipal is one provisioned subscriber: its registry row, the credential
// it was issued, and a Kafka client that authenticates AS IT.
type eventIsolationPrincipal struct {
	subscriber *model.EventSubscriber
	credential SubscriberCredential

	// client speaks to the broker as this principal. It exists FOR VERIFICATION ONLY —
	// see the scope-boundary note at the top of this file. Blnk ships no consumer.
	client *kafka.Client

	// issuanceDuration is how long IssueSubscriberCredential took, so the 5-second budget
	// of the requirement is assertable.
	issuanceDuration time.Duration
}

// eventIsolationProvision registers a subscriber with a NARROW grant and issues its
// credential through the real production path.
//
// Calling KafkaAdminClient.ProvisionSubscriberPrincipal directly would test the ACL
// arithmetic and skip everything a subscriber's boundary depends on: the grant
// validation that refuses a dead-letter topic or a wildcard, the principal and
// consumer-group derivation, the enforcement gate, the five-second budget, and the
// compensation that revokes a credential whose bindings failed. So issuance goes
// through EventSubscriberService.IssueSubscriberCredential.
func eventIsolationProvision(
	t *testing.T,
	ctx context.Context,
	fixture *eventIsolationFixture,
	label string,
	grant []string,
	advisoryKeyPrefix ...string,
) *eventIsolationPrincipal {
	t.Helper()

	subscriberID := eventIsolationSubscriberID(label)

	registration := SubscriberRegistration{
		SubscriberID:     subscriberID,
		Name:             fmt.Sprintf("event isolation probe (%s)", label),
		AuthorizedTopics: grant,
	}

	// Variadic so every existing call site is unchanged. Only the advisory-prefix test supplies
	// one, and it supplies it in order to prove the value changes nothing at the broker.
	if len(advisoryKeyPrefix) > 0 {
		registration.PartitionKeyPrefix = &advisoryKeyPrefix[0]
	}

	subscriber, err := fixture.service.RegisterSubscriber(ctx, registration)
	require.NoError(t, err, "registering subscriber %q with grant %v", subscriberID, grant)
	require.NotNil(t, subscriber, "RegisterSubscriber returned no row for %q", subscriberID)

	// Teardown is registered BEFORE issuance, not after. Issuance can fail after the
	// credential reached the broker but before this function returns, and a credential
	// that authenticates with nobody tracking it is exactly what must not survive a failed
	// run.
	t.Cleanup(func() { eventIsolationTeardown(t, fixture, subscriber) })

	started := time.Now()
	credential, err := fixture.service.IssueSubscriberCredential(ctx, subscriberID)
	elapsed := time.Since(started)

	if err != nil {
		// The one failure mode that must be reported as the authorizer diagnostic rather than
		// as an ordinary provisioning error: ProvisionSubscriberPrincipal refuses to write a
		// credential against a broker whose ACL enforcement is not confirmed. It is the same
		// finding eventIsolationRequireAuthorizer reports, arriving from the other side, and
		// it is still a failure and still never a skip.
		if errors.Is(err, ErrAuthorizerNotEnforcing) {
			t.Fatalf(
				"THE BROKER REFUSED TO ISSUE A SUBSCRIBER CREDENTIAL because its ACL enforcement is not "+
					"confirmed, so subscriber isolation cannot be proven: %v\n"+
					"Start the broker with "+
					"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer.",
				err,
			)
		}

		require.NoError(t, err, "issuing a Kafka credential for subscriber %q", subscriberID)
	}

	client := eventIsolationClient(t, fixture.env, credential)

	// The credential exists in the broker's metadata log; it is not necessarily USABLE
	// yet. Waiting for it here, once, is what keeps every assertion in every test from
	// having to know that — and the wait is registered AFTER `elapsed` is taken, so the
	// five-second issuance budget is still measured on issuance alone.
	eventIsolationAwaitAuthenticable(t, ctx, subscriberID, client)

	return &eventIsolationPrincipal{
		subscriber:       subscriber,
		credential:       credential,
		client:           client,
		issuanceDuration: elapsed,
	}
}

// eventIsolationAwaitAuthenticable blocks until a freshly minted credential can
// authenticate.
//
// AlterUserScramCredentials returns once the credential is in the KRaft metadata log,
// and the broker's authenticator picks it up a moment later.
func eventIsolationAwaitAuthenticable(
	t *testing.T,
	ctx context.Context,
	subscriberID string,
	client *kafka.Client,
) {
	t.Helper()

	deadline := time.Now().Add(eventIsolationSettleTimeout)
	started := time.Now()

	for {
		err := eventIsolationBoundedProbe(ctx, func(attemptCtx context.Context) error {
			_, probeErr := client.Metadata(attemptCtx, &kafka.MetadataRequest{})

			return probeErr
		})
		if !eventIsolationIsCredentialPropagating(err) {
			// Any other outcome, error or not, means authentication is settled: the broker
			// either accepted the credential or refused the request for a reason that has
			// nothing to do with it. The decision is named rather than inlined because it is the
			// ONE error code propagation explains, and reading it as "is this still
			// propagating?" is what keeps a future reader from widening it to every SASL failure
			// — which would turn a genuinely unusable credential into a timeout.
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf(
				"the credential just issued for subscriber %q still cannot AUTHENTICATE after %s, so "+
					"issuance reported success and produced an unusable credential: %v\n"+
					"This is not a propagation delay at this point — it is the credential itself.",
				subscriberID, time.Since(started).Round(time.Millisecond), err,
			)
		}

		select {
		case <-ctx.Done():
			t.Fatalf(
				"the context was cancelled while waiting for subscriber %q's new credential to become "+
					"usable: %v", subscriberID, ctx.Err(),
			)
		case <-time.After(eventIsolationSettleInterval):
		}
	}
}

// eventIsolationSubscriberID mints a per-run subscriber identifier.
//
// The value has to satisfy model.CanonicalizeSubscriberIdentifier — lowercase ASCII
// letters, digits, underscore and hyphen, first character alphanumeric — because the
// Kafka principal and the consumer group are derived from it byte for byte.
func eventIsolationSubscriberID(label string) string {
	var safe strings.Builder
	for _, character := range strings.ToLower(label) {
		switch {
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9':
			safe.WriteRune(character)
		default:
			safe.WriteByte('-')
		}
	}

	return fmt.Sprintf("iso-%s-%s", safe.String(), uuid.NewString())
}

// eventIsolationClient builds a Kafka client that authenticates as the given
// subscriber.
//
// This is the only place in the repository that authenticates as a SUBSCRIBER, and it
// exists FOR VERIFICATION ONLY.
func eventIsolationClient(
	t *testing.T,
	env eventIsolationEnvironment,
	credential SubscriberCredential,
) *kafka.Client {
	t.Helper()

	require.Equal(t, SubscriberSASLMechanism, credential.Mechanism,
		"the credential names an unexpected SASL mechanism")

	// The secret is read once, handed straight to the mechanism, and not retained anywhere
	// else in this function. A failure here reports only the username.
	mechanism, err := scram.Mechanism(scram.SHA512, credential.Username, credential.Password())
	require.NoError(t, err, "preparing SCRAM-SHA-512 for principal %q", credential.Username)

	transport := &kafka.Transport{
		SASL:        mechanism,
		DialTimeout: eventIsolationDialTimeout,
		ClientID:    fmt.Sprintf("blnk-isolation-probe-%s", credential.SubscriberID),
	}

	tlsConfig, err := eventIsolationTLSConfig(env.kafka)
	require.NoError(t, err, "building the verification client's TLS configuration")
	transport.TLS = tlsConfig

	client := &kafka.Client{
		Addr:      kafka.TCP(env.kafka.Brokers...),
		Timeout:   eventIsolationOperationTimeout,
		Transport: transport,
	}

	t.Cleanup(transport.CloseIdleConnections)

	return client
}

// eventIsolationAdminClient builds a kafka-go client authenticated as the
// ADMINISTRATOR.
//
// It exists for exactly one job the product's KafkaAdminClient has no reason to do:
// deleting the consumer groups this fixture's assertions create.
func eventIsolationAdminClient(t *testing.T, env eventIsolationEnvironment) *kafka.Client {
	t.Helper()

	mechanism, err := scram.Mechanism(
		scram.SHA512, env.kafka.SASLAdminUser, env.kafka.SASLAdminSecret,
	)
	require.NoError(t, err, "preparing SCRAM-SHA-512 for the administrative principal")

	transport := &kafka.Transport{
		SASL:        mechanism,
		DialTimeout: eventIsolationDialTimeout,
		ClientID:    "blnk-isolation-teardown",
	}

	tlsConfig, err := eventIsolationTLSConfig(env.kafka)
	require.NoError(t, err, "building the administrative client's TLS configuration")
	transport.TLS = tlsConfig

	t.Cleanup(transport.CloseIdleConnections)

	return &kafka.Client{
		Addr:      kafka.TCP(env.kafka.Brokers...),
		Timeout:   eventIsolationOperationTimeout,
		Transport: transport,
	}
}

// eventIsolationTeardown revokes a provisioned principal at the broker and removes its
// registry row.
//
// A SCRAM credential and its ACL bindings are broker state, not process state, so
// nothing about the test process exiting removes them.
func eventIsolationTeardown(
	t *testing.T,
	fixture *eventIsolationFixture,
	subscriber *model.EventSubscriber,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), eventIsolationTeardownTimeout)
	defer cancel()

	// DeregisterSubscriber revokes at the broker and then removes the row, which is the
	// production path and therefore the one worth exercising. A subscriber that was never
	// successfully registered is already absent and reports not-found, which is the
	// desired end state rather than a problem.
	if _, err := fixture.service.DeregisterSubscriber(ctx, subscriber.SubscriberID); err != nil &&
		!isSubscriberNotFoundError(err) {
		// The FIRST attempt failing is logged rather than failed, because there is a second
		// one and a principal removed by the fallback has not leaked.
		t.Logf("teardown: deregistering subscriber %q failed, falling back to a direct broker "+
			"revocation (not yet a leak): %v", subscriber.SubscriberID, err)

		// Fall back to a direct broker revocation. The registry row is local and
		// disposable; the broker credential is not, so it gets a second attempt.
		if revokeErr := fixture.admin.RevokeSubscriber(ctx, subscriber); revokeErr != nil {
			// BOTH attempts failed, so a SCRAM credential and its ACLs are left on a broker
			// every other test in this package shares, with no registry row recording them. That
			// is a leak this test caused, and it must not be able to report success: the next
			// isolation run asserts that a principal outside its grant cannot read, and an
			// abandoned principal with live ACLs is exactly what makes that assertion pass or
			// fail for the wrong reason.
			t.Errorf("teardown: LEAKED the Kafka principal %q — both DeregisterSubscriber and "+
				"the direct RevokeSubscriber fallback failed, so a credential and its ACLs "+
				"remain on the shared broker with no registry row recording them: %v",
				subscriber.KafkaPrincipal, revokeErr)
		}
	}

	// The consumer groups the assertions created, before the credential check, because
	// deleting a group is authorized as the ADMINISTRATOR and does not depend on the
	// subscriber's credential still existing.
	eventIsolationDeleteSubscriberGroups(t, ctx, fixture, subscriber)

	// POLLED, not read once. Revocation is applied to the KRaft metadata log and the
	// credential list catches up a moment later, so reading it immediately reported a LEAK
	// in the same run whose own log said "subscriber SCRAM credential revoked" — an
	// accusation against the product for something that had not happened.
	exists, err := eventIsolationAwaitCredentialRevoked(ctx, fixture, subscriber.KafkaPrincipal)
	if err != nil {
		// Being UNABLE TO VERIFY is not the same as being clean, and it must not be reported
		// as clean. If the credential list cannot be read, this run cannot say whether it
		// left a principal on the shared broker — and "we could not tell" is the state in
		// which a leak silently survives into the next run's isolation assertions.
		t.Errorf("teardown: could not confirm principal %q was revoked, so this run cannot "+
			"establish that it left no credential on the shared broker: %v",
			subscriber.KafkaPrincipal, err)

		return
	}

	if exists {
		t.Errorf(
			"teardown LEAKED a live Kafka principal: %q still holds a SCRAM credential %s after "+
				"revocation. Revoke it by hand — a leaked principal accumulates in the broker's "+
				"metadata log and a stale grant can make a later isolation run pass for the wrong "+
				"reason",
			subscriber.KafkaPrincipal, eventIsolationSettleTimeout,
		)
	}
}

// eventIsolationAwaitCredentialRevoked polls the broker until the principal holds no
// SCRAM credential, or the settle window elapses.
//
// Returns:
//   - bool: whether the credential still exists.
//   - error: a describe failure, in which case the boolean is meaningless.
func eventIsolationAwaitCredentialRevoked(
	ctx context.Context,
	fixture *eventIsolationFixture,
	principal string,
) (bool, error) {
	deadline := time.Now().Add(eventIsolationSettleTimeout)

	for {
		exists, err := fixture.admin.SubscriberCredentialExists(ctx, principal)
		if err != nil || !exists || time.Now().After(deadline) {
			return exists, err
		}

		select {
		case <-ctx.Done():
			return exists, ctx.Err()
		case <-time.After(eventIsolationSettleInterval):
		}
	}
}

// eventIsolationDeleteSubscriberGroups removes every consumer group inside a
// subscriber's own namespace, and reports a leak if any survives.
//
// It matters for the same reason a leftover principal does: a namespace left behind is
// state a later run inherits.
func eventIsolationDeleteSubscriberGroups(
	t *testing.T,
	ctx context.Context,
	fixture *eventIsolationFixture,
	subscriber *model.EventSubscriber,
) {
	t.Helper()

	namespace, err := SubscriberConsumerGroupNamespace(subscriber.SubscriberID)
	if err != nil {
		// Without the namespace no group can be found, so this run cannot clean up at all and
		// cannot tell whether it left anything behind. That is a failure, not a note.
		t.Errorf("teardown: could not derive the consumer group namespace of %q, so this run's "+
			"consumer groups cannot be found or removed: %v", subscriber.SubscriberID, err)

		return
	}

	client := eventIsolationAdminClient(t, fixture.env)

	groups, err := eventIsolationGroupsUnder(ctx, client, namespace)
	if err != nil {
		t.Errorf("teardown: could not list the consumer groups under %q, so this run cannot "+
			"establish that it left none behind: %v", namespace, err)

		return
	}

	if len(groups) == 0 {
		return
	}

	// The request failing does NOT skip the confirmation below. The two are separate
	// obligations: whether the delete succeeded, and whether the groups are gone.
	response, err := client.DeleteGroups(ctx, &kafka.DeleteGroupsRequest{
		Addr:     client.Addr,
		GroupIDs: groups,
	})
	if err != nil {
		t.Errorf("teardown: deleting the consumer groups under %q failed: %v", namespace, err)
	} else {
		for group, groupErr := range response.Errors {
			if groupErr != nil {
				t.Errorf("teardown: deleting consumer group %q failed: %v", group, groupErr)
			}
		}
	}

	// Confirmed rather than assumed, and as an error rather than a log line: a group this
	// fixture created and did not remove is exactly the residue the check above refuses to
	// tolerate for a principal.
	remaining, err := eventIsolationGroupsUnder(ctx, client, namespace)
	if err != nil {
		t.Errorf("teardown: could not confirm the consumer groups under %q were deleted, so a "+
			"leak here would go unreported: %v", namespace, err)

		return
	}

	assert.Emptyf(t, remaining,
		"teardown LEAKED consumer groups inside %q: %v. The assertions in this file create a group "+
			"by committing an offset to it, so every one of them is this fixture's to remove; left "+
			"behind they accumulate in the broker's metadata log run after run",
		namespace, remaining)
}

// eventIsolationGroupsUnder returns the ids of every consumer group whose name begins with
// namespace.
func eventIsolationGroupsUnder(
	ctx context.Context,
	client *kafka.Client,
	namespace string,
) ([]string, error) {
	listCtx, cancel := context.WithTimeout(ctx, eventIsolationOperationTimeout)
	defer cancel()

	response, err := client.ListGroups(listCtx, &kafka.ListGroupsRequest{Addr: client.Addr})
	if err != nil {
		return nil, err
	}

	if response == nil {
		return nil, errors.New("the broker returned no ListGroups response")
	}

	if response.Error != nil {
		return nil, response.Error
	}

	matched := []string{}
	for _, group := range response.Groups {
		if strings.HasPrefix(group.GroupID, namespace) {
			matched = append(matched, group.GroupID)
		}
	}

	return matched, nil
}

// eventIsolationTLSConfig mirrors the transport security posture the service dials
// with.
func eventIsolationTLSConfig(cfg config.KafkaConfig) (*tls.Config, error) {
	if !cfg.TLS.Enabled {
		if !cfg.InsecureLocalDev {
			return nil, fmt.Errorf(
				"%s is false, so the verification client would dial in the clear; set %s=true to "+
					"acknowledge that this is a local development broker",
				eventIsolationTLSEnabledEnv, eventIsolationInsecureLocalDevEnv,
			)
		}

		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: cfg.TLS.ServerName,
	}

	if cfg.TLS.CAFile == "" {
		return tlsConfig, nil
	}

	pem, err := os.ReadFile(cfg.TLS.CAFile)
	if err != nil {
		return nil, fmt.Errorf("reading %s %q: %w", eventIsolationTLSCAFileEnv, cfg.TLS.CAFile, err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s %q contains no usable certificate", eventIsolationTLSCAFileEnv, cfg.TLS.CAFile)
	}

	tlsConfig.RootCAs = pool

	return tlsConfig, nil
}

// eventIsolationTopicOutcome is the result of one topic-scoped probe: the error the
// request itself returned, and the error the broker attached to the topic or partition
// inside a successful response.
type eventIsolationTopicOutcome struct {
	// requestErr is what the client call returned.
	requestErr error

	// brokerErr is the error the broker reported inside a successful response, at the
	// topic or partition level.
	brokerErr error
}

// any returns whichever error is present, preferring the broker's own code.
//
// The broker's code is the more specific and more trustworthy of the two: kafka-go
// wraps a request-level failure in its own prose, while a per-partition code is the
// authorizer's verdict verbatim.
func (o eventIsolationTopicOutcome) any() error {
	if o.brokerErr != nil {
		return o.brokerErr
	}

	return o.requestErr
}

// allowed reports that the broker raised no objection at all.
func (o eventIsolationTopicOutcome) allowed() bool {
	return o.requestErr == nil && o.brokerErr == nil
}

// eventIsolationListOffsets asks the broker for a topic's partition offsets as the
// given principal.
//
// It is the operation whose refusal is UNAMBIGUOUS.
func eventIsolationListOffsets(
	ctx context.Context,
	client *kafka.Client,
	topic string,
) (eventIsolationTopicOutcome, []kafka.PartitionOffsets) {
	ctx, cancel := context.WithTimeout(ctx, eventIsolationOperationTimeout)
	defer cancel()

	response, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			topic: {kafka.FirstOffsetOf(0), kafka.LastOffsetOf(0)},
		},
	})

	outcome := eventIsolationTopicOutcome{requestErr: err}
	if response == nil {
		return outcome, nil
	}

	disclosed := make([]kafka.PartitionOffsets, 0, len(response.Topics[topic]))
	for _, offsets := range response.Topics[topic] {
		if offsets.Error != nil {
			if outcome.brokerErr == nil {
				outcome.brokerErr = offsets.Error
			}

			continue
		}

		disclosed = append(disclosed, offsets)
	}

	return outcome, disclosed
}

// eventIsolationFetch attempts to read records from a topic as the given principal.
//
// It complements eventIsolationListOffsets by exercising the operation a real consumer
// performs. See that function's note on why its error is the weaker of the two signals.
func eventIsolationFetch(
	ctx context.Context,
	client *kafka.Client,
	topic string,
	offset int64,
) eventIsolationTopicOutcome {
	ctx, cancel := context.WithTimeout(ctx, eventIsolationOperationTimeout)
	defer cancel()

	response, err := client.Fetch(ctx, &kafka.FetchRequest{
		Topic:     topic,
		Partition: 0,
		Offset:    offset,
		MinBytes:  1,
		MaxBytes:  eventIsolationMaxFetchBytes,
		MaxWait:   eventIsolationFetchWait,
	})

	outcome := eventIsolationTopicOutcome{requestErr: err}
	if response != nil {
		outcome.brokerErr = response.Error

		// The record set holds no assertion value here — only the authorization outcome does
		// — but kafka-go's documentation asks the caller to close it, and leaving it open
		// would hold the connection's buffer for the rest of the run. The reader is declared
		// as a bare RecordReader, so closability is discovered rather than assumed; a refused
		// request returns one that is not closeable at all.
		if closer, ok := response.Records.(io.Closer); ok && closer != nil {
			if closeErr := closer.Close(); closeErr != nil && outcome.brokerErr == nil {
				outcome.brokerErr = closeErr
			}
		}
	}

	return outcome
}

// eventIsolationProduce attempts to WRITE one record as the given principal.
//
// Subscribers are read-only: ProvisionSubscriberPrincipal grants Read and Describe and
// nothing else, so no Write binding exists for any topic.
func eventIsolationProduce(
	ctx context.Context,
	client *kafka.Client,
	topic string,
) eventIsolationTopicOutcome {
	ctx, cancel := context.WithTimeout(ctx, eventIsolationOperationTimeout)
	defer cancel()

	response, err := client.Produce(ctx, &kafka.ProduceRequest{
		Topic:        topic,
		Partition:    0,
		RequiredAcks: kafka.RequireAll,
		Records: kafka.NewRecordReader(kafka.Record{
			Key:   kafka.NewBytes([]byte("blnk-isolation-probe")),
			Value: kafka.NewBytes([]byte("this write must be refused")),
		}),
	})

	outcome := eventIsolationTopicOutcome{requestErr: err}
	if response == nil {
		return outcome
	}

	outcome.brokerErr = response.Error
	// A broker on Produce v8 or above reports per-record failures separately from the
	// response-level error, so a refusal recorded only there would otherwise be missed.
	for _, recordErr := range response.RecordErrors {
		if recordErr != nil && outcome.brokerErr == nil {
			outcome.brokerErr = recordErr
		}
	}

	return outcome
}

// eventIsolationOffsetFetch reads a consumer group's committed offsets as the given
// principal.
func eventIsolationOffsetFetch(
	ctx context.Context,
	client *kafka.Client,
	group string,
	topic string,
) error {
	return eventIsolationAwaitGroupCoordinator(ctx, func(attemptCtx context.Context) error {
		response, err := client.OffsetFetch(attemptCtx, &kafka.OffsetFetchRequest{
			GroupID: group,
			Topics:  map[string][]int{topic: {0}},
		})
		if err != nil {
			return err
		}

		if response == nil {
			return errors.New("the broker returned no OffsetFetch response")
		}

		return response.Error
	})
}

// eventIsolationAwaitGroupCoordinator runs a consumer-group probe, retrying only while
// the broker says it has no coordinator to answer with.
//
// The three retried codes are statements about the broker's own state, never about this
// principal's rights:
//
//   - [14] GroupLoadInProgress — the coordinator is loading its state.
//   - [15] GroupCoordinatorNotAvailable — __consumer_offsets has no leader yet, or does
//     not exist at all.
//   - [16] NotCoordinatorForGroup — this broker is not the coordinator for this group.
func eventIsolationAwaitGroupCoordinator(ctx context.Context, probe func(context.Context) error) error {
	deadline := time.Now().Add(eventIsolationSettleTimeout)

	for {
		err := eventIsolationBoundedProbe(ctx, probe)
		if !eventIsolationIsCoordinatorUnready(err) || time.Now().After(deadline) {
			return err
		}

		select {
		case <-ctx.Done():
			return err
		case <-time.After(eventIsolationSettleInterval):
		}
	}
}

// eventIsolationBoundedProbe runs one probe under the operation deadline.
func eventIsolationBoundedProbe(ctx context.Context, probe func(context.Context) error) error {
	attemptCtx, cancel := context.WithTimeout(ctx, eventIsolationOperationTimeout)
	defer cancel()

	return probe(attemptCtx)
}

// eventIsolationIsCoordinatorUnready reports whether err says the broker has no group
// coordinator ready, rather than saying anything about authorization.
func eventIsolationIsCoordinatorUnready(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, kafka.GroupLoadInProgress) ||
		errors.Is(err, kafka.GroupCoordinatorNotAvailable) ||
		errors.Is(err, kafka.NotCoordinatorForGroup)
}

// eventIsolationOffsetCommit commits an offset to a consumer group as the given
// principal.
func eventIsolationOffsetCommit(
	ctx context.Context,
	client *kafka.Client,
	group string,
	topic string,
) error {
	return eventIsolationAwaitGroupCoordinator(ctx, func(attemptCtx context.Context) error {
		response, err := client.OffsetCommit(attemptCtx, &kafka.OffsetCommitRequest{
			GroupID:      group,
			GenerationID: -1,
			MemberID:     "",
			Topics: map[string][]kafka.OffsetCommit{
				topic: {{Partition: 0, Offset: 0}},
			},
		})
		if err != nil {
			return err
		}

		if response == nil {
			return errors.New("the broker returned no OffsetCommit response")
		}

		for _, partitions := range response.Topics {
			for _, partition := range partitions {
				if partition.Error != nil {
					return partition.Error
				}
			}
		}

		return nil
	})
}

// eventIsolationVisibleTopics lists every topic the broker is willing to disclose to
// the given principal.
//
// Kafka filters a full metadata response by Describe authorization rather than refusing
// it, so an unauthorized principal receives a SHORTER LIST instead of an error.
func eventIsolationVisibleTopics(ctx context.Context, client *kafka.Client) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, eventIsolationOperationTimeout)
	defer cancel()

	response, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, err
	}

	if response == nil {
		return nil, errors.New("the broker returned no Metadata response")
	}

	visible := make([]string, 0, len(response.Topics))
	for _, topic := range response.Topics {
		// A topic carrying an error is one the broker named but refused to describe, which
		// is not disclosure of its contents and must not count as visible.
		if topic.Error != nil {
			continue
		}

		visible = append(visible, topic.Name)
	}

	return visible, nil
}

// eventIsolationDescribeACLs attempts an ADMINISTRATIVE list as the given principal.
func eventIsolationDescribeACLs(ctx context.Context, client *kafka.Client) error {
	ctx, cancel := context.WithTimeout(ctx, eventIsolationOperationTimeout)
	defer cancel()

	response, err := client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{
		Filter: kafka.ACLFilter{
			ResourceTypeFilter:        kafka.ResourceTypeTopic,
			ResourcePatternTypeFilter: kafka.PatternTypeAny,
			Operation:                 kafka.ACLOperationTypeAny,
			PermissionType:            kafka.ACLPermissionTypeAny,
		},
	})
	if err != nil {
		return err
	}

	if response == nil {
		return errors.New("the broker returned no DescribeACLs response")
	}

	return response.Error
}

// eventIsolationAssertAuthorizationError requires that err is the broker refusing on
// authorization grounds, and specifically not something that merely looks like a
// refusal.
//
// A non-nil error proves nothing on its own.
func eventIsolationAssertAuthorizationError(
	t *testing.T,
	operation string,
	err error,
	expected ...error,
) {
	t.Helper()

	require.Error(t, err,
		"%s WAS ALLOWED. The broker raised no objection to an operation outside the subscriber's ACL "+
			"grant, which means the grant is not being enforced and the isolation criterion is not met",
		operation)

	for _, code := range expected {
		if errors.Is(err, code) {
			return
		}
	}

	names := make([]string, 0, len(expected))
	for _, code := range expected {
		names = append(names, code.Error())
	}

	t.Fatalf(
		"%s failed, but NOT with a recognised refusal: %v\n"+
			"Expected one of: %s.\n"+
			"This matters because a connection failure, a SASL failure or a timeout would also be "+
			"non-nil here while proving nothing about the ACL grant — the test would be passing for the "+
			"wrong reason. Confirm the broker is healthy and that the principal's credential is live.",
		operation, err, strings.Join(names, "; "),
	)
}

// eventIsolationAssertReadDenied requires the broker's DIRECT authorization verdict on
// a read outside the grant.
//
// It is deliberately the strictest assertion in this file: nothing but
// TOPIC_AUTHORIZATION_FAILED is accepted.
func eventIsolationAssertReadDenied(t *testing.T, topic string, outcome eventIsolationTopicOutcome) {
	t.Helper()

	eventIsolationAssertAuthorizationError(
		t,
		fmt.Sprintf("reading the offsets of topic %q outside the subscriber's grant", topic),
		outcome.any(),
		kafka.TopicAuthorizationFailed,
	)
}

// eventIsolationAssertTopicUndiscoverable requires that a consumer-shaped operation
// could not even locate the topic.
//
// Kafka does not tell a principal that a topic it may not describe exists.
func eventIsolationAssertTopicUndiscoverable(t *testing.T, operation string, err error) {
	t.Helper()

	eventIsolationAssertAuthorizationError(
		t,
		operation,
		err,
		protocol.ErrNoTopic,
		kafka.UnknownTopicOrPartition,
		kafka.TopicAuthorizationFailed,
	)
}

// eventIsolationAssertGroupDenied requires that a consumer-group operation was refused.
//
// Only GROUP_AUTHORIZATION_FAILED is accepted.
func eventIsolationAssertGroupDenied(t *testing.T, operation string, err error) {
	t.Helper()

	eventIsolationAssertAuthorizationError(t, operation, err, kafka.GroupAuthorizationFailed)
}

// eventIsolationAssertAllowed requires that an operation INSIDE the grant succeeded.
//
// Every refusal above would also be reported by a principal whose credential does not
// work at all, or by a broker that refuses everything. A file containing only negative
// assertions can therefore pass while testing nothing but a broken credential.
func eventIsolationAssertAllowed(t *testing.T, operation string, err error) {
	t.Helper()

	require.NoError(t, err,
		"%s was REFUSED. The subscriber must be able to do everything inside its own grant; a "+
			"credential that can do nothing would satisfy every refusal asserted elsewhere in this "+
			"file while proving nothing about the boundary",
		operation)
}

// eventIsolationRequireEnforcement is the EMPIRICAL enforcement probe.
//
// AuthorizerActive asks the broker to describe its own configuration.
//
// So enforcement is also proven by DOING it: this principal, on this topic outside its
// grant, must actually be refused.
func eventIsolationRequireEnforcement(
	t *testing.T,
	ctx context.Context,
	principal *eventIsolationPrincipal,
	ungranted string,
) {
	t.Helper()

	outcome, partitions := eventIsolationListOffsets(ctx, principal.client, ungranted)
	if !outcome.allowed() {
		return
	}

	t.Fatalf(
		"ACL ENFORCEMENT IS NOT IN EFFECT: principal %q was granted only %v, yet the broker disclosed "+
			"%d partition(s) of %q with no error.\n"+
			"Every isolation assertion in this file would now pass while proving NOTHING, which is why "+
			"this is a failure and not a skip. The usual cause is a broker started without "+
			"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer, which in "+
			"KRaft mode accepts ACL bindings and never applies them. Other causes worth checking: the "+
			"principal is in the broker's super.users list, allow.everyone.if.no.acl.found is true, or a "+
			"wildcard binding grants every topic.",
		principal.credential.Username, principal.credential.AuthorizedTopics, len(partitions), ungranted,
	)
}

// eventIsolationGrantSplit divides the grantable category topics into the one topic a
// principal is authorised for and the ones it is deliberately not.
//
// A grant of exactly one topic is what gives the test something to prove.
func eventIsolationGrantSplit(fixture *eventIsolationFixture) (string, []string) {
	return fixture.grantable[0], append([]string(nil), fixture.grantable[1:]...)
}

// TestEventIsolation_SubscriberCredentialIsRefusedEverythingOutsideItsGrant is the
// subscriber-isolation guarantee, asserted against a live broker.
func TestEventIsolation_SubscriberCredentialIsRefusedEverythingOutsideItsGrant(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, withheld := eventIsolationGrantSplit(fixture)
	principal := eventIsolationProvision(t, ctx, fixture, "narrow", []string{granted})

	// The grant is what it was asked to be. If provisioning quietly widened it, everything
	// below would be testing a different boundary from the one under review.
	require.Equal(t, []string{granted}, principal.credential.AuthorizedTopics,
		"the issued credential's grant is not the narrow grant that was requested")

	// EMPIRICAL enforcement, before anything else is concluded from a refusal.
	eventIsolationRequireEnforcement(t, ctx, principal, withheld[0])

	t.Run("reading a category topic outside the grant is refused", func(t *testing.T) {
		for _, topic := range withheld {
			outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, topic)
			eventIsolationAssertReadDenied(t, topic, outcome)
			assert.Empty(t, disclosed,
				"the broker disclosed partition offsets for %q, which is outside the grant", topic)

			// The same denial through the operation a real consumer performs. Its refusal
			// arrives as non-disclosure rather than as the direct verdict; see
			// eventIsolationAssertTopicUndiscoverable for why that is the stronger answer and
			// not a weaker assertion.
			eventIsolationAssertTopicUndiscoverable(t,
				fmt.Sprintf("fetching records from topic %q outside the subscriber's grant", topic),
				eventIsolationFetch(ctx, principal.client, topic, 0).any())
		}
	})

	t.Run("reading a dead-letter topic outside the grant is refused", func(t *testing.T) {
		// Both the dead-letter sibling of a WITHHELD category and the sibling of the GRANTED
		// one. The second is the case worth being explicit about: a grant on
		// blnk.transactions must not carry blnk.transactions.dlt with it.
		deadLetterTopics := []string{DLTFor(granted)}
		for _, topic := range withheld {
			deadLetterTopics = append(deadLetterTopics, DLTFor(topic))
		}

		for _, topic := range deadLetterTopics {
			require.True(t, IsDeadLetterTopic(topic), "%q is not a dead-letter topic name", topic)
			require.NotContains(t, principal.credential.AuthorizedTopics, topic,
				"the credential unexpectedly names a dead-letter topic in its grant")

			outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, topic)
			eventIsolationAssertReadDenied(t, topic, outcome)
			assert.Empty(t, disclosed,
				"the broker disclosed partition offsets for the dead-letter topic %q", topic)

			eventIsolationAssertTopicUndiscoverable(t,
				fmt.Sprintf("fetching records from the dead-letter topic %q", topic),
				eventIsolationFetch(ctx, principal.client, topic, 0).any())
		}
	})

	t.Run("listing topics discloses nothing outside the grant", func(t *testing.T) {
		visible, err := eventIsolationVisibleTopics(ctx, principal.client)
		require.NoError(t, err, "listing the topics visible to principal %q", principal.credential.Username)

		// Kafka filters a metadata listing by Describe authorization rather than refusing
		// it, so the assertion is about absence from the answer.
		for _, topic := range withheld {
			assert.NotContains(t, visible, topic,
				"the topic listing disclosed %q, which is outside the grant", topic)
			assert.NotContains(t, visible, DLTFor(topic),
				"the topic listing disclosed the dead-letter topic %q", DLTFor(topic))
		}

		assert.NotContains(t, visible, DLTFor(granted),
			"the topic listing disclosed %q; a category grant must not carry its dead-letter sibling",
			DLTFor(granted))

		// Nothing Blnk owns may appear other than the granted topic itself. Stated as a
		// whole-inventory sweep rather than topic by topic so that a category added to the
		// catalogue later is covered without this test being edited.
		for _, topic := range AllTopicsWithDeadLetters() {
			if topic == granted {
				continue
			}

			assert.NotContains(t, visible, topic,
				"the topic listing disclosed the Blnk-owned topic %q, which is outside the grant", topic)
		}
	})

	t.Run("describing the cluster's authorization state is refused", func(t *testing.T) {
		// A subscriber that could enumerate ACLs would learn every other subscriber's
		// topics and group namespaces by name, so the grant carries no cluster Describe.
		eventIsolationAssertAuthorizationError(
			t,
			"describing the cluster's ACLs",
			eventIsolationDescribeACLs(ctx, principal.client),
			kafka.ClusterAuthorizationFailed,
		)
	})

	t.Run("using a consumer group outside its namespace is refused", func(t *testing.T) {
		namespace, err := SubscriberConsumerGroupNamespace(principal.subscriber.SubscriberID)
		require.NoError(t, err, "deriving the subscriber's consumer group namespace")

		foreign := []string{
			// A plausible Blnk-issued group belonging to a different subscriber.
			model.SubscriberPrincipalNamespace + "someone-else" +
				model.SubscriberGroupTerminator + model.SubscriberDefaultGroupLeaf,
			// A group that shares this subscriber's namespace as a strict PREFIX of its own name
			// rather than sitting inside it. "blnk-sub-<id>" and "blnk-sub-<id>x" look alike and
			// are different namespaces; a binding that matched the second would be reserving
			// more than it should.
			strings.TrimSuffix(namespace, model.SubscriberGroupTerminator) + "x.default",
			// An arbitrary group outside the Blnk namespace entirely.
			"blnk-isolation-unrelated-group",
		}

		for _, group := range foreign {
			require.NotEqual(t, principal.credential.ConsumerGroupID, group,
				"the foreign group fixture accidentally names the subscriber's own group")

			eventIsolationAssertGroupDenied(t,
				fmt.Sprintf("reading offsets of consumer group %q outside the subscriber's namespace", group),
				eventIsolationOffsetFetch(ctx, principal.client, group, granted))

			eventIsolationAssertGroupDenied(t,
				fmt.Sprintf("committing an offset to consumer group %q outside the subscriber's namespace", group),
				eventIsolationOffsetCommit(ctx, principal.client, group, granted))
		}
	})

	t.Run("writing is refused even on a granted topic", func(t *testing.T) {
		// Subscribers are read-only by construction: the provisioning grant is Read and
		// Describe on each authorised topic and Read on the group namespace. No Write binding
		// is created for any topic, so a subscriber cannot forge a ledger event on the very
		// topic it is entitled to consume.
		eventIsolationAssertAuthorizationError(
			t,
			fmt.Sprintf("writing to the granted topic %q", granted),
			eventIsolationProduce(ctx, principal.client, granted).any(),
			kafka.TopicAuthorizationFailed,
		)

		for _, topic := range withheld {
			eventIsolationAssertTopicUndiscoverable(t,
				fmt.Sprintf("writing to the ungranted topic %q", topic),
				eventIsolationProduce(ctx, principal.client, topic).any())
		}
	})
}

// TestEventIsolation_SubscriberCredentialIsAllowedEverythingInsideItsGrant is the
// POSITIVE half of the isolation proof, and it is not decoration.
//
// A file of refusals alone would pass against a credential that does not work at all,
// or against a broker refusing every request for an unrelated reason.
func TestEventIsolation_SubscriberCredentialIsAllowedEverythingInsideItsGrant(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, withheld := eventIsolationGrantSplit(fixture)
	principal := eventIsolationProvision(t, ctx, fixture, "granted", []string{granted})

	// Even the POSITIVE test gates on enforcement. Without it, "the credential can do
	// everything inside its grant" is a green result on a broker where it can do
	// everything everywhere — true, reassuring, and worthless.
	eventIsolationRequireEnforcement(t, ctx, principal, withheld[0])

	t.Run("the granted topic is readable", func(t *testing.T) {
		outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading offsets of the granted topic %q", granted), outcome.any())
		require.NotEmpty(t, disclosed,
			"the broker disclosed no partitions of the granted topic %q", granted)

		// Read from the topic's actual first offset rather than zero: a topic whose head has
		// been truncated by retention answers OFFSET_OUT_OF_RANGE at zero, which would be a
		// spurious failure with nothing to do with authorization.
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("fetching records from the granted topic %q", granted),
			eventIsolationFetch(ctx, principal.client, granted, disclosed[0].FirstOffset).any())
	})

	t.Run("the granted topic's partitions are describable", func(t *testing.T) {
		visible, err := eventIsolationVisibleTopics(ctx, principal.client)
		require.NoError(t, err, "listing the topics visible to the principal")
		assert.Contains(t, visible, granted,
			"the granted topic %q is not visible to the principal that was granted Describe on it", granted)
	})

	t.Run("its own consumer group is usable", func(t *testing.T) {
		group := principal.credential.ConsumerGroupID
		require.NotEmpty(t, group, "the credential carries no consumer group id")

		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading offsets of its own consumer group %q", group),
			eventIsolationOffsetFetch(ctx, principal.client, group, granted))

		eventIsolationAssertAllowed(t,
			fmt.Sprintf("committing an offset to its own consumer group %q", group),
			eventIsolationOffsetCommit(ctx, principal.client, group, granted))
	})

	t.Run("any group inside its own namespace is usable", func(t *testing.T) {
		// The group binding uses a PREFIXED pattern, which is the one intentional widening in
		// the grant: a subscriber can stand up a replay group beside its live one without an
		// administrative round trip. This asserts the widening actually works — and the
		// companion test asserts it widens only within the subscriber's own namespace.
		namespace, err := SubscriberConsumerGroupNamespace(principal.subscriber.SubscriberID)
		require.NoError(t, err, "deriving the subscriber's consumer group namespace")

		replayGroup := namespace + "replay"
		require.NotEqual(t, principal.credential.ConsumerGroupID, replayGroup,
			"the replay group fixture accidentally names the subscriber's default group")

		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading offsets of %q inside its own group namespace", replayGroup),
			eventIsolationOffsetFetch(ctx, principal.client, replayGroup, granted))

		eventIsolationAssertAllowed(t,
			fmt.Sprintf("committing an offset to %q inside its own group namespace", replayGroup),
			eventIsolationOffsetCommit(ctx, principal.client, replayGroup, granted))
	})
}

// TestEventIsolation_SubscribersCannotReachEachOthersTopicsOrGroupNamespaces proves the
// boundary holds BETWEEN subscribers, which is the tenancy property that matters
// operationally.
//
// Two principals are provisioned independently, on disjoint grants.
func TestEventIsolation_SubscribersCannotReachEachOthersTopicsOrGroupNamespaces(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	first, second := fixture.grantable[0], fixture.grantable[1]

	alpha := eventIsolationProvision(t, ctx, fixture, "alpha", []string{first})
	beta := eventIsolationProvision(t, ctx, fixture, "beta", []string{second})

	require.NotEqual(t, alpha.credential.Username, beta.credential.Username,
		"the two principals must be distinct broker identities")

	eventIsolationRequireEnforcement(t, ctx, alpha, second)
	eventIsolationRequireEnforcement(t, ctx, beta, first)

	t.Run("neither can read the other's topic", func(t *testing.T) {
		outcome, _ := eventIsolationListOffsets(ctx, alpha.client, second)
		eventIsolationAssertReadDenied(t, second, outcome)

		outcome, _ = eventIsolationListOffsets(ctx, beta.client, first)
		eventIsolationAssertReadDenied(t, first, outcome)
	})

	t.Run("neither can see the other's topic in a listing", func(t *testing.T) {
		visibleToAlpha, err := eventIsolationVisibleTopics(ctx, alpha.client)
		require.NoError(t, err, "listing the topics visible to the first principal")
		assert.Contains(t, visibleToAlpha, first, "the first principal cannot see its own topic")
		assert.NotContains(t, visibleToAlpha, second,
			"the first principal can see the second's topic %q", second)

		visibleToBeta, err := eventIsolationVisibleTopics(ctx, beta.client)
		require.NoError(t, err, "listing the topics visible to the second principal")
		assert.Contains(t, visibleToBeta, second, "the second principal cannot see its own topic")
		assert.NotContains(t, visibleToBeta, first,
			"the second principal can see the first's topic %q", first)
	})

	t.Run("neither can use the other's consumer group namespace", func(t *testing.T) {
		alphaNamespace, err := SubscriberConsumerGroupNamespace(alpha.subscriber.SubscriberID)
		require.NoError(t, err, "deriving the first principal's group namespace")
		betaNamespace, err := SubscriberConsumerGroupNamespace(beta.subscriber.SubscriberID)
		require.NoError(t, err, "deriving the second principal's group namespace")

		// Both the other's default group and an arbitrary leaf inside the other's namespace.
		// The leaf is the assertion that the prefixed pattern reserves the whole namespace
		// for its owner rather than merely naming one group.
		eventIsolationAssertGroupDenied(t,
			"the second principal reading the first principal's default consumer group",
			eventIsolationOffsetFetch(ctx, beta.client, alpha.credential.ConsumerGroupID, second))

		eventIsolationAssertGroupDenied(t,
			"the second principal reading a leaf inside the first principal's group namespace",
			eventIsolationOffsetFetch(ctx, beta.client, alphaNamespace+"replay", second))

		eventIsolationAssertGroupDenied(t,
			"the second principal committing an offset to the first principal's consumer group",
			eventIsolationOffsetCommit(ctx, beta.client, alpha.credential.ConsumerGroupID, second))

		eventIsolationAssertGroupDenied(t,
			"the first principal reading the second principal's default consumer group",
			eventIsolationOffsetFetch(ctx, alpha.client, beta.credential.ConsumerGroupID, first))

		eventIsolationAssertGroupDenied(t,
			"the first principal committing an offset to a leaf inside the second principal's namespace",
			eventIsolationOffsetCommit(ctx, alpha.client, betaNamespace+"replay", first))
	})
}

// TestEventIsolation_CredentialIssuanceReturnsTheSecretOnceWithinTheBudget covers the
// issuance properties the requirement states, against a real broker.
//
// The unit tests already drive these through a fake provisioner.
func TestEventIsolation_CredentialIssuanceReturnsTheSecretOnceWithinTheBudget(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, _ := eventIsolationGrantSplit(fixture)
	principal := eventIsolationProvision(t, ctx, fixture, "issuance", []string{granted})

	t.Run("issuance completes inside the five-second budget", func(t *testing.T) {
		assert.LessOrEqual(t,
			principal.issuanceDuration, SubscriberCredentialIssuanceBudget,
			"issuing a Kafka credential took %s, over the %s budget requirement R-7 sets",
			principal.issuanceDuration, SubscriberCredentialIssuanceBudget)
	})

	t.Run("the credential describes a usable connection", func(t *testing.T) {
		credential := principal.credential

		assert.Equal(t, principal.subscriber.SubscriberID, credential.SubscriberID)
		assert.Equal(t, SubscriberSASLMechanism, credential.Mechanism)
		assert.Equal(t, principal.subscriber.KafkaPrincipal, credential.Username)
		assert.Equal(t, []string{granted}, credential.AuthorizedTopics)
		assert.Equal(t, principal.subscriber.ConsumerGroupID, credential.ConsumerGroupID)
		// THE SUBSCRIBER-FACING LIST, and asserting the other one would be a defect in this
		// test rather than in the service.
		assert.Equal(t, fixture.env.kafka.SubscriberBrokers, credential.Brokers,
			"the credential must hand back the SUBSCRIBER-FACING broker list (%s), not the "+
				"addresses the admin client dialled (%s)",
			eventIsolationSubscriberBrokersEnv, eventIsolationBrokersEnv)
		assert.Equal(t, strings.Join(fixture.env.kafka.SubscriberBrokers, ","), credential.BrokerEndpoint,
			"and the single-string rendering must describe the same list, or a client configured "+
				"from it reaches a different endpoint than one configured from the array")
		if !slices.Equal(fixture.env.kafka.Brokers, fixture.env.kafka.SubscriberBrokers) {
			assert.NotEqual(t, fixture.env.kafka.Brokers, credential.Brokers,
				"the two lists are configured differently here precisely so that reporting the "+
					"internal one cannot pass")
		}
		assert.False(t, credential.IssuedAt.IsZero(), "the credential records no issuance instant")
		assert.NotEmpty(t, credential.Fingerprint, "the credential carries no reference fingerprint")
		assert.False(t, credential.Replaced,
			"a first issuance for a brand-new principal must not report replacing a credential")
	})

	t.Run("the broker holds a SCRAM-SHA-512 credential for the principal", func(t *testing.T) {
		exists, err := fixture.admin.SubscriberCredentialExists(ctx, principal.credential.Username)
		require.NoError(t, err, "describing the SCRAM credential of %q", principal.credential.Username)
		assert.True(t, exists,
			"the broker holds no SCRAM-SHA-512 credential for %q after a successful issuance",
			principal.credential.Username)
	})

	t.Run("the secret is strong enough to be worth minting", func(t *testing.T) {
		// The LENGTH is asserted, never the value. A SASL handshake has no rate limit and
		// no lockout, so a short secret is not weak — it is open.
		assert.GreaterOrEqual(t, principal.credential.PasswordLength(), MinSCRAMPasswordLength,
			"the generated secret is shorter than the %d-character floor", MinSCRAMPasswordLength)
	})

	t.Run("the secret is returned by issuance and persisted nowhere", func(t *testing.T) {
		secret := principal.credential.Password()
		require.NotEmpty(t, secret, "the credential returned no plaintext secret")

		// Every assertion below is written as a boolean predicate with a message that does
		// NOT interpolate the secret. assert.NotContains would print both operands on
		// failure, which would put the plaintext in the test output — the exact disclosure
		// this whole posture exists to prevent.
		require.False(t, strings.Contains(fmt.Sprintf("%v", principal.credential), secret),
			"rendering the credential with %%v disclosed the plaintext secret")
		require.False(t, strings.Contains(fmt.Sprintf("%+v", principal.credential), secret),
			"rendering the credential with %%+v disclosed the plaintext secret")
		require.False(t, strings.Contains(fmt.Sprintf("%#v", principal.credential), secret),
			"rendering the credential with %%#v disclosed the plaintext secret")
		require.False(t, strings.Contains(fmt.Sprintf("%s", principal.credential), secret),
			"rendering the credential with %%s disclosed the plaintext secret")
		require.False(t, strings.Contains(fmt.Sprintf("%q", principal.credential), secret),
			"rendering the credential with %%q disclosed the plaintext secret")

		encoded, err := json.Marshal(principal.credential)
		require.NoError(t, err, "marshalling the credential")
		require.False(t, strings.Contains(string(encoded), secret),
			"marshalling the credential to JSON disclosed the plaintext secret")

		logged := fmt.Sprintf("%v", principal.credential.LogFields())
		require.False(t, strings.Contains(logged, secret),
			"the credential's log fields disclosed the plaintext secret")

		// The registry keeps a non-reversible reference and an issuance instant, and nothing
		// that could be authenticated with. Once the issuance result above is discarded there
		// is no route by which Blnk can produce the secret again; a lost password can only be
		// replaced.
		stored, err := fixture.service.GetSubscriber(ctx, principal.subscriber.SubscriberID)
		require.NoError(t, err, "reading back the registry row")
		require.NotNil(t, stored.CredentialReference,
			"the registry recorded no credential reference for a successful issuance")
		require.NotNil(t, stored.CredentialIssuedAt,
			"the registry recorded no issuance instant for a successful issuance")
		require.False(t, strings.Contains(*stored.CredentialReference, secret),
			"the persisted credential reference contains the plaintext secret")

		row, err := json.Marshal(stored)
		require.NoError(t, err, "marshalling the registry row")
		require.False(t, strings.Contains(string(row), secret),
			"the persisted registry row contains the plaintext secret")

		// The reference is a digest of the principal and the secret, so it must be
		// reproducible from them and must differ for a different secret. That is what makes
		// it a reference rather than a stored password.
		expected, err := model.DeriveCredentialReference(principal.credential.Username, secret)
		require.NoError(t, err, "deriving the expected credential reference")
		assert.Equal(t, expected, *stored.CredentialReference,
			"the persisted reference is not the one derived from this principal and secret")
		assert.Equal(t, model.CredentialFingerprint(expected), principal.credential.Fingerprint,
			"the credential's fingerprint does not match the persisted reference")
	})
}

// TestEventIsolation_RevokedCredentialCanNoLongerReachTheBroker proves that revocation
// ends access, which is both the last state of the isolation boundary and the guarantee
// this file's own teardown depends on.
//
// A grant that cannot be withdrawn is not a boundary.
func TestEventIsolation_RevokedCredentialCanNoLongerReachTheBroker(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, _ := eventIsolationGrantSplit(fixture)
	principal := eventIsolationProvision(t, ctx, fixture, "revoked", []string{granted})

	// Access before revocation, so that losing it afterwards is attributable to the
	// revocation and not to a credential that never worked.
	outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, granted)
	eventIsolationAssertAllowed(t,
		fmt.Sprintf("reading the granted topic %q before revocation", granted), outcome.any())
	require.NotEmpty(t, disclosed, "the broker disclosed no partitions before revocation")

	require.NoError(t,
		fixture.service.RevokeSubscriberCredential(ctx, principal.subscriber.SubscriberID),
		"revoking the subscriber's Kafka credential")

	t.Run("the broker no longer holds the principal's credential", func(t *testing.T) {
		exists, err := fixture.admin.SubscriberCredentialExists(ctx, principal.credential.Username)
		require.NoError(t, err, "describing the SCRAM credential of %q after revocation",
			principal.credential.Username)
		assert.False(t, exists,
			"the broker still holds a SCRAM credential for the revoked principal %q",
			principal.credential.Username)
	})

	t.Run("the registry no longer claims a credential", func(t *testing.T) {
		stored, err := fixture.service.GetSubscriber(ctx, principal.subscriber.SubscriberID)
		require.NoError(t, err, "reading back the registry row after revocation")
		assert.Nil(t, stored.CredentialReference,
			"the registry still records a credential reference for a revoked subscriber")
		assert.Nil(t, stored.CredentialIssuedAt,
			"the registry still records an issuance instant for a revoked subscriber")
	})

	t.Run("the revoked credential can no longer authenticate", func(t *testing.T) {
		// A revoked SCRAM credential fails at the SASL handshake rather than at the
		// authorizer, so this is deliberately NOT asserted through
		// eventIsolationAssertAuthorizationError: the expected code is
		// SASL_AUTHENTICATION_FAILED, and accepting a topic-authorization code here would
		// mean the credential still authenticates.
		outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, granted)
		require.False(t, outcome.allowed(),
			"the revoked credential still reads the topic it was granted; revocation did not take effect")
		assert.Empty(t, disclosed, "the broker disclosed partitions to a revoked credential")

		// Two refusals are both correct here, and which one arrives depends on whether the
		// client's pooled connection outlived the revocation. On a NEW connection the SASL
		// handshake fails, because the credential is gone.
		eventIsolationAssertAuthorizationError(t,
			fmt.Sprintf("reading the granted topic %q with a revoked credential", granted),
			outcome.any(),
			kafka.SASLAuthenticationFailed,
			kafka.TopicAuthorizationFailed,
			protocol.ErrNoTopic,
			kafka.UnknownTopicOrPartition,
		)
	})
}

// TestEventIsolation_AKeyScopedSubscriberIsProvisionedAndNamesItsGatewayEndpoint covers
// the REGISTRY AND ISSUANCE path for a key-scoped subscriber against a real broker:
// that the credential exists, that it names the declared component rather than the
// brokers, and that a prefix may be recorded on a subscriber that already holds a
// credential.
//
// The credential was granted whole-topic Read and the response said the prefix was the
// consumer's own business — every word true, and still unrestricted access to a shared
// stream, because the party asked to filter was the party holding the credential.
//
// Two claims, and only a real authorizer can settle either:
//
//  1. The credential exists, discloses its scope, and names the DECLARED COMPONENT as
//     its endpoint.
//  2. Subscriber isolation still holds for this principal. A key-scoped subscriber is
//     refused every topic, dead-letter topic, listing and consumer group outside its
//     grant exactly as a subscriber with no prefix is, because the two broker-enforced
//     dimensions are untouched by any of this.
func TestEventIsolation_AKeyScopedSubscriberIsProvisionedAndNamesItsGatewayEndpoint(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, withheld := eventIsolationGrantSplit(fixture)
	keyPrefix := "ldg_" + uuid.NewString()

	// A DECLARED ENFORCEMENT POINT FIRST, because issuance refuses a key-scoped row
	// without one and every assertion below is about a credential that exists. The refusal
	// itself is TestEventIsolation_AKeyScopedSubscriberIsRefusedWhereNoGatewayIsDeclared's
	// subject.
	gateway := eventIsolationDeclareKeyScopeGateway(t, fixture)

	// Provisioned through the SHARED helper, with the key prefix supplied.
	principal := eventIsolationProvision(t, ctx, fixture, "keyscope", []string{granted}, keyPrefix)

	require.True(t, principal.subscriber.DeclaresKeyScope(),
		"the fixture must actually be in the state under test")
	require.Equal(t, model.KeyScopeEnforcementGateway,
		principal.subscriber.KeyScopeEnforcement(),
		"and the row must report where that scope is enforced")

	// EMPIRICAL enforcement, before anything is concluded from a refusal below.
	eventIsolationRequireEnforcement(t, ctx, principal, withheld[0])

	t.Run("the credential exists and discloses its key scope", func(t *testing.T) {
		assert.NotEmpty(t, principal.credential.Password(),
			"a secret must have been minted; the refusal this replaced generated none")
		assert.Equal(t, keyPrefix, principal.credential.PartitionKeyPrefix,
			"the subscriber must RECEIVE the scope it is expected to apply, in the same response as "+
				"the credential it qualifies")
		assert.Equal(t, model.KeyScopeEnforcementGateway,
			principal.credential.KeyScopeEnforcement,
			"and it must be told WHERE that scope is enforced, or the prefix reads as a broker boundary")
		assert.Equal(t, gateway, principal.credential.Brokers,
			"AND THE ENDPOINT IS THE DECLARED COMPONENT, not the subscriber-facing brokers: a "+
				"credential declaring key-scoped isolation beside an address that bypasses the thing "+
				"enforcing it is the substitution this pairing exists to prevent")

		exists, existsErr := fixture.admin.SubscriberCredentialExists(ctx, principal.subscriber.KafkaPrincipal)
		require.NoError(t, existsErr, "describing the SCRAM credential of %q",
			principal.subscriber.KafkaPrincipal)
		assert.True(t, exists,
			"principal %q holds no credential at the broker, so it can consume nothing at all",
			principal.subscriber.KafkaPrincipal)

		stored, readErr := fixture.service.GetSubscriber(ctx, principal.subscriber.SubscriberID)
		require.NoError(t, readErr)
		require.NotNil(t, stored.CredentialReference, "the issuance must be recorded")
		require.NotNil(t, stored.PartitionKeyPrefix,
			"and the recorded scope survives issuance rather than being cleared to permit it")
		assert.Equal(t, keyPrefix, *stored.PartitionKeyPrefix)
	})

	t.Run("the granted topic stays DESCRIBABLE, which is all this proves", func(t *testing.T) {
		// A DESCRIBE-ONLY ASSERTION, said so in the name because the probe cannot support a
		// wider one: ListOffsets resolves metadata and offsets and never fetches a record, so
		// it cannot show that "the credential reads the granted topic in FULL". Under the
		// shipped grant such a claim is also false — a key-scoped principal holds no topic
		// Read at all — so a subtest making it would pass while asserting the opposite of the
		// boundary.
		outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("describing the granted topic %q as a key-scoped principal", granted),
			outcome.any())
		assert.NotEmpty(t, disclosed,
			"the granted topic must remain describable for a key-scoped subscriber exactly as for an "+
				"unscoped one: the prefix narrows RECORD access, and a subscriber that cannot see its "+
				"own offsets cannot measure its lag")
	})

	t.Run("V-5 still holds: everything outside the grant is refused", func(t *testing.T) {
		// The two BROKER-enforced dimensions are untouched by the key scope, and this is the
		// assertion that says so. It would fail if the prefix were ever mapped onto a
		// prefixed TOPIC binding in an attempt to honour it.
		for _, topic := range withheld {
			outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, topic)
			eventIsolationAssertReadDenied(t, topic, outcome)
			assert.Empty(t, disclosed,
				"the broker disclosed partition offsets for %q, which is outside the grant", topic)

			eventIsolationAssertTopicUndiscoverable(t,
				fmt.Sprintf("fetching records from topic %q outside a key-scoped subscriber's grant", topic),
				eventIsolationFetch(ctx, principal.client, topic, 0).any())
		}

		// Including the dead-letter sibling of the GRANTED topic, which holds the payloads an
		// operator is still triaging and is never grantable.
		for _, topic := range append([]string{DLTFor(granted)}, DLTFor(withheld[0])) {
			require.True(t, IsDeadLetterTopic(topic), "%q is not a dead-letter topic name", topic)
			require.NotContains(t, principal.credential.AuthorizedTopics, topic,
				"the credential unexpectedly names a dead-letter topic in its grant")

			outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, topic)
			eventIsolationAssertReadDenied(t, topic, outcome)
			assert.Empty(t, disclosed,
				"the broker disclosed partition offsets for the dead-letter topic %q", topic)
		}

		visible, err := eventIsolationVisibleTopics(ctx, principal.client)
		require.NoError(t, err, "listing the topics visible to principal %q",
			principal.credential.Username)
		for _, topic := range AllTopicsWithDeadLetters() {
			if topic == granted {
				continue
			}

			assert.NotContains(t, visible, topic,
				"the topic listing disclosed the Blnk-owned topic %q, which is outside the grant", topic)
		}

		// And the group dimension, which is a different resource type and therefore a
		// separate probe.
		eventIsolationAssertGroupDenied(t,
			"joining a consumer group outside a key-scoped subscriber's namespace",
			eventIsolationOffsetFetch(ctx, principal.client, "blnk-isolation-unrelated-group", granted))
	})

	t.Run("recording a key prefix on a provisioned subscriber is accepted", func(t *testing.T) {
		// THE REVERSE ORDER, which is accepted rather than refused. An operator who decides on
		// a key scope after the credential exists must be able to record that decision;
		// refusing leaves them with nowhere to put it and no route to the state except revoking
		// a working credential, destroying a secret that cannot be recovered in response to a
		// request that never mentioned revocation.
		latePrefix := "ldg_late_" + uuid.NewString()
		updated, updateErr := fixture.service.UpdateSubscriber(ctx, principal.subscriber.SubscriberID,
			SubscriberUpdate{PartitionKeyPrefix: &latePrefix})
		require.NoError(t, updateErr,
			"recording a key scope on a provisioned subscriber is a decision an operator is "+
				"allowed to record")
		require.NotNil(t, updated.PartitionKeyPrefix)
		assert.Equal(t, latePrefix, *updated.PartitionKeyPrefix)

		after, afterErr := fixture.admin.SubscriberCredentialExists(ctx, principal.subscriber.KafkaPrincipal)
		require.NoError(t, afterErr)
		assert.True(t, after,
			"the update must not revoke the credential the row already holds")

		allowed, disclosed := eventIsolationListOffsets(ctx, principal.client, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading the granted topic %q after recording a late key prefix", granted),
			allowed.any())
		assert.NotEmpty(t, disclosed,
			"the credential must still read what it could read before the update")

		stillDenied, stillDisclosed := eventIsolationListOffsets(ctx, principal.client, withheld[0])
		eventIsolationAssertReadDenied(t, withheld[0], stillDenied)
		assert.Empty(t, stillDisclosed,
			"and it must still be refused everything outside its grant: recording a key scope "+
				"moves no ACL in either direction")
	})
}

// The fixture that mints a principal guarantees both the principal and its subscriber are
// non-nil, so every site reads the subscriber id directly rather than through a
// nil-tolerant accessor that would suggest a nil is reachable.
func TestEventIsolation_NarrowingASubscribersGrantWithdrawsItAtTheBroker(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	require.GreaterOrEqual(t, len(fixture.grantable), 2,
		"this test needs two grantable topics: one to keep and one to withdraw")

	kept := fixture.grantable[0]
	withdrawn := fixture.grantable[1]

	principal := eventIsolationProvision(t, ctx, fixture, "narrowed", []string{kept, withdrawn})

	// Both topics readable to begin with, so losing one afterwards is attributable to the
	// narrowing rather than to a grant that never worked.
	for _, topic := range []string{kept, withdrawn} {
		outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, topic)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading %q under the WIDE grant", topic), outcome.any())
		require.NotEmpty(t, disclosed, "the broker disclosed no partitions of %q", topic)
	}

	// The narrowing itself, through the production authorization-update path.
	updated, err := fixture.service.UpdateSubscriber(ctx, principal.subscriber.SubscriberID,
		SubscriberUpdate{AuthorizedTopics: []string{kept}})
	require.NoError(t, err, "narrowing the subscriber's authorized topics")
	require.Equal(t, []string{kept}, updated.AuthorizedTopics)

	t.Run("the withdrawn topic is no longer reachable", func(t *testing.T) {
		// A NEW client, because a pooled connection carries no ACL state but the broker
		// re-authorizes every request, so either client would do — a fresh one just removes
		// the question.
		client := eventIsolationClient(t, fixture.env, principal.credential)

		outcome, disclosed := eventIsolationListOffsets(ctx, client, withdrawn)
		require.False(t, outcome.allowed(),
			"THE WITHDRAWN TOPIC %q IS STILL READABLE: narrowing the registry did not reach the "+
				"broker, so the subscriber keeps consuming a topic the registry says it lost", withdrawn)
		assert.Empty(t, disclosed,
			"the broker disclosed partitions of a topic the subscriber is no longer authorised for")

		eventIsolationAssertReadDenied(t, withdrawn, outcome)
	})

	t.Run("the retained topic is untouched", func(t *testing.T) {
		// Reconciliation must remove the SURPLUS and nothing else. A narrowing that revoked
		// everything would pass the assertion above while breaking the subscriber.
		client := eventIsolationClient(t, fixture.env, principal.credential)

		outcome, disclosed := eventIsolationListOffsets(ctx, client, kept)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading the retained topic %q after the narrowing", kept), outcome.any())
		assert.NotEmpty(t, disclosed)
	})

	t.Run("re-widening restores access", func(t *testing.T) {
		// The third step of the update — grant — reaching the broker, proven the same way.
		rewidened, updateErr := fixture.service.UpdateSubscriber(ctx,
			principal.subscriber.SubscriberID,
			SubscriberUpdate{AuthorizedTopics: []string{kept, withdrawn}})
		require.NoError(t, updateErr)
		require.Len(t, rewidened.AuthorizedTopics, 2)

		client := eventIsolationClient(t, fixture.env, principal.credential)

		outcome, disclosed := eventIsolationListOffsets(ctx, client, withdrawn)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading %q after re-widening the grant", withdrawn), outcome.any())
		assert.NotEmpty(t, disclosed)
	})
}

// TestEventIsolation_DeregistrationRevokesBeforeItForgetsThePrincipal is the revoke-before-delete rule
// against a real broker.
//
// Here the whole sequence runs against a real broker and the end state is checked from
// the broker's side: the credential is gone, and nothing can be read with it.
func TestEventIsolation_DeregistrationRevokesBeforeItForgetsThePrincipal(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, _ := eventIsolationGrantSplit(fixture)
	principal := eventIsolationProvision(t, ctx, fixture, "deregistered", []string{granted})

	outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, granted)
	eventIsolationAssertAllowed(t,
		fmt.Sprintf("reading the granted topic %q before deregistration", granted), outcome.any())
	require.NotEmpty(t, disclosed)

	removed, err := fixture.service.DeregisterSubscriber(ctx, principal.subscriber.SubscriberID)
	require.NoError(t, err, "deregistering the subscriber")
	require.NotNil(t, removed)
	assert.True(t, removed.IsRevocationPending(),
		"the returned row must carry the tombstone the revocation was performed under, which is "+
			"what makes an interrupted deregistration recoverable")

	t.Run("the broker holds no credential", func(t *testing.T) {
		exists, existsErr := fixture.admin.SubscriberCredentialExists(ctx, principal.credential.Username)
		require.NoError(t, existsErr)
		assert.False(t, exists,
			"principal %q still holds a credential after deregistration: its access outlived the "+
				"registry row that named it", principal.credential.Username)
	})

	t.Run("the registry row is gone", func(t *testing.T) {
		_, readErr := fixture.service.GetSubscriber(ctx, principal.subscriber.SubscriberID)
		require.Error(t, readErr)
		assert.True(t, isSubscriberNotFoundError(readErr),
			"a confirmed revocation must remove the row rather than leaving it tombstoned")
	})

	t.Run("nothing can be read with the deregistered credential", func(t *testing.T) {
		client := eventIsolationClient(t, fixture.env, principal.credential)

		outcome, disclosed := eventIsolationListOffsets(ctx, client, granted)
		require.False(t, outcome.allowed(),
			"the deregistered credential still reads %q", granted)
		assert.Empty(t, disclosed)

		eventIsolationAssertAuthorizationError(t,
			fmt.Sprintf("reading %q with a deregistered credential", granted),
			outcome.any(),
			kafka.SASLAuthenticationFailed,
			kafka.TopicAuthorizationFailed,
			protocol.ErrNoTopic,
			kafka.UnknownTopicOrPartition,
		)
	})
}

// requireFenceLocked reproduces the repository's ownership predicate for a fenced
// write.
//
// The real writes carry `AND provisioning_token = $n AND provisioning_until > NOW()`
// inside the statement, so a caller whose lease lapsed is refused rather than racing. A
// fake that ignored the token would let every fence test pass while the production
// predicate was absent, which is the one thing these tests exist to rule out.
//
// Parameters:
//   - subscriberID string: the row the write targets.
//   - token string: the claim token the write carried.
//   - operation string: named in the lost-fence error, matching the repository's
//     wording.
//
// Returns:
//   - error: ErrInvalidInput for a blank token, the lost-fence conflict when the claim
//     is not the caller's, nil otherwise.
func (s *eventIsolationStore) requireFenceLocked(subscriberID, token, operation string) error {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"A provisioning claim token is required for this operation", nil)
	}

	key := strings.TrimSpace(subscriberID)

	held, ok := s.fences[key]
	if !ok || held.token != trimmed || !held.until.After(time.Now().UTC()) {
		// THE CLIENT MESSAGE database.fencedWriteMissError produces, with the marker in the
		// DETAIL because subscriberFenceWasLost matches on it: a fake that reported a generic
		// conflict would exercise the wrong branch of every compensation path, and one that
		// reported a message the repository no longer returns would pass here and fail
		// against the database. The release and renewal statements answer with the same
		// sentence up to its final clause, so this one string serves all three sites.
		return apierror.NewAPIError(apierror.ErrConflict,
			"The subscriber provisioning claim is no longer held by this caller, so the change was not applied",
			fmt.Errorf("subscriber %q: the provisioning claim was no longer held while %s, so the "+
				"write was refused", subscriberID, operation))
	}

	return nil
}

// MarkSubscriberCredentialOrphaned records a credential the service could neither record nor
// revoke. UNFENCED, exactly as the repository is: it is reached when the claim may already have
// lapsed, and conditioning it would lose the marker in the case that produces it.
func (s *eventIsolationStore) MarkSubscriberCredentialOrphaned(
	_ context.Context,
	subscriberID string,
	orphanedAt time.Time,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		// A row deleted mid-issuance is not an error: the deregistration revoked the principal
		// on its way out, which is one of the documented causes.
		return nil
	}

	if row.CredentialOrphanedAt == nil {
		stamped := orphanedAt
		row.CredentialOrphanedAt = &stamped
	}
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	return nil
}

// MarkSubscriberRevocationFailed records that the broker REFUSED the most recent revocation.
// Unfenced for the same reason, and it does not keep the first instant.
func (s *eventIsolationStore) MarkSubscriberRevocationFailed(
	_ context.Context,
	subscriberID string,
	failedAt time.Time,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return nil
	}

	stamped := failedAt
	row.RevocationFailedAt = &stamped
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	return nil
}

// eventIsolationIsCredentialPropagating reports whether err is the one failure a
// credential that has just been written to the metadata log explains on its own.
//
// [58] SASL Authentication Failed is that failure and the only one.
//
// Nothing else belongs here, and the exclusions are the point rather than an omission:
//
//   - A transport error, a dial failure or a timeout says the broker could not be
//     reached.
//   - An authorization refusal says the credential authenticated and the request was
//     denied.
func eventIsolationIsCredentialPropagating(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, kafka.SASLAuthenticationFailed)
}

// WHERE THE KEY-SCOPE COVERAGE LIVES, in four places and none of it optional. Every
// name below is a function in this file, so a reader can reach the coverage rather than
// looking for it:
//
//   - TestEventIsolation_AKeyScopedSubscriberIsProvisionedAndItsKeyBoundaryIsEnforced,
//     directly below, is the RECORD-ACCESS proof: it requires the broker to REFUSE this
//     principal's fetch with TopicAuthorizationFailed, and it covers replacing and
//     clearing the prefix.
//   - TestEventIsolation_AKeyScopedSubscriberIsProvisionedAndNamesItsGatewayEndpoint,
//     above, covers the registry and issuance path, the gateway endpoint substitution
//     and the Describe dimension.
//   - TestEventIsolation_AKeyScopedSubscriberIsRefusedWhereNoGatewayIsDeclared covers
//     the shipped default: no component declared, no credential minted, nothing left at
//     the broker.
//   - TestEventIsolation_AKeyScopeIsBoundAtTheDeclaredComponentBeforeAnyCredentialExists
//     covers the attestation ordering, and the api/model validation tests plus
//     TestSubscribersAPI_RecordsAndReturnsTheKeyScope in the api package cover the
//     response projection in both deployments.

// TestEventIsolation_AKeyScopedSubscriberIsProvisionedAndItsKeyBoundaryIsEnforced is
// The stated-scope contract against a REAL broker, stated as a property
// of the running system.
//
// It first asserted a REFUSAL: a subscriber recording a partition key prefix was
// unprovisionable, and the test proved no credential existed at the broker for it.
//
// It then asserted the opposite and asserted it truthfully: the credential was issued
// with Read on the whole topic, and this test PROVED the prefix was not a boundary by
// fetching the partition from offset zero with a prefix no record carried.
//
// The grant itself is narrower, and the broker is the thing that proves it. A
// key-scoped subscriber is provisioned with Describe and NO Read on its authorised
// topics, so:
//
//  1. The credential IS issued, DOES authenticate, and carries the recorded prefix.
//  2. Describe still works on the granted topic and is still refused on every withheld
//     one.
//  3. A FETCH of the granted topic is REFUSED — TOPIC_AUTHORIZATION_FAILED from the
//     authorizer itself.
//  4. The transition is reversible and observable in both directions.
func TestEventIsolation_AKeyScopedSubscriberIsProvisionedAndItsKeyBoundaryIsEnforced(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, withheld := eventIsolationGrantSplit(fixture)
	subscriberID := eventIsolationSubscriberID("keyscope")

	// A DECLARED ENFORCEMENT POINT FIRST. Without one, issuance refuses this row outright
	// and the broker-side properties below could not be observed at all — see
	// TestEventIsolation_AKeyScopedSubscriberIsRefusedWhereNoGatewayIsDeclared for that
	// refusal.
	eventIsolationDeclareKeyScopeGateway(t, fixture)

	// A prefix no record on the shared topic can carry. It is arbitrary now that the
	// broker refuses the fetch outright — but keeping it unrelated costs nothing and keeps
	// the fixture honest about what a prefix is.
	keyPrefix := "ldg_" + uuid.NewString()
	subscriber, err := fixture.service.RegisterSubscriber(ctx, SubscriberRegistration{
		SubscriberID:       subscriberID,
		Name:               "event isolation probe (key scope)",
		AuthorizedTopics:   []string{granted},
		PartitionKeyPrefix: &keyPrefix,
	})
	require.NoError(t, err, "recording a key prefix must be accepted at registration")
	require.NotNil(t, subscriber)
	t.Cleanup(func() { eventIsolationTeardown(t, fixture, subscriber) })

	require.True(t, subscriber.RequiresGatewayDelivery(),
		"the fixture must actually be in the state under test")
	require.False(t, subscriber.GrantsBrokerRecordAccess(),
		"and the row must declare the narrower grant, or the broker assertions below would be "+
			"testing a subscriber the registry describes differently")

	credential, err := fixture.service.IssueSubscriberCredential(ctx, subscriberID)
	require.NoError(t, err,
		"A RECORDED KEY PREFIX MUST NOT WITHDRAW THE CREDENTIAL CAPABILITY. The prefix narrows the "+
			"grant the credential carries; it is not a reason to withhold the credential, which is "+
			"the dead end this replaced")
	require.NotEmpty(t, credential.Password(), "a real secret is returned, exactly once")

	assert.Equal(t, keyPrefix, credential.PartitionKeyPrefix,
		"and it travels WITH the credential, so the response states the boundary that was applied")

	assert.Equal(t, []string{granted}, credential.AuthorizedTopics,
		"the grant is the narrow grant that was requested; a widened one would make everything "+
			"below test a different boundary")

	t.Run("the broker really holds the credential", func(t *testing.T) {
		exists, existsErr := fixture.admin.SubscriberCredentialExists(ctx, subscriber.KafkaPrincipal)
		require.NoError(t, existsErr, "describing the SCRAM credential of %q",
			subscriber.KafkaPrincipal)
		assert.True(t, exists,
			"principal %q holds NO credential after a successful issuance: the response returned a "+
				"password its holder cannot authenticate with, which is the dead end this replaced "+
				"wearing a success status", subscriber.KafkaPrincipal)
	})

	t.Run("the registry records the credential and keeps the prefix", func(t *testing.T) {
		stored, readErr := fixture.service.GetSubscriber(ctx, subscriberID)
		require.NoError(t, readErr)
		require.NotNil(t, stored.CredentialReference,
			"a successful issuance records the reference")
		require.NotNil(t, stored.CredentialIssuedAt)
		require.NotNil(t, stored.PartitionKeyPrefix,
			"and it does NOT erase the prefix: the operator's stated intent survives issuance")
		assert.Equal(t, keyPrefix, *stored.PartitionKeyPrefix)
		assert.True(t, stored.RequiresGatewayDelivery(),
			"so every subsequent read of this subscriber declares gateway delivery")
	})

	principalClient := eventIsolationClient(t, fixture.env, credential)

	// EMPIRICAL enforcement, before anything is concluded from a refusal below.
	eventIsolationRequireEnforcement(t, ctx, &eventIsolationPrincipal{
		credential: credential,
		client:     principalClient,
	}, withheld[0])

	t.Run("topic isolation is unaffected by the key scope", func(t *testing.T) {
		// The DESCRIBE dimension must hold exactly as it does for a subscriber with no
		// prefix. A key-scoped subscriber that lost Describe as well could no longer read its
		// own topics' offsets, which is what a consumer needs to measure its lag — so
		// withholding it would narrow more than the prefix asks for.
		allowed, disclosed := eventIsolationListOffsets(ctx, principalClient, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("describing the granted topic %q as a key-scoped subscriber", granted),
			allowed.any())
		assert.NotEmpty(t, disclosed,
			"the granted topic must remain describable: the prefix narrows RECORD access, not "+
				"metadata, and a subscriber that cannot see its own offsets cannot measure lag")

		for _, topic := range withheld {
			outcome, leaked := eventIsolationListOffsets(ctx, principalClient, topic)
			eventIsolationAssertReadDenied(t, topic, outcome)
			assert.Empty(t, leaked,
				"the broker disclosed partition offsets for %q, which is outside the grant", topic)
		}
	})

	t.Run("the key boundary IS enforced: the broker refuses this principal's records", func(t *testing.T) {
		// THE ASSERTION THAT MATTERS, and the one that was inverted before.
		//
		// The principal holds Describe on this topic, so metadata resolves and the request
		// reaches the authorizer's decision on the read itself — which is refused, because a
		// key-scoped subscriber is granted no Read binding at all. That refusal is the
		// boundary: it does not depend on which client the subscriber uses, on the prefix it
		// was told, or on it choosing to filter anything.
		outcome := eventIsolationFetch(ctx, principalClient, granted, 0)

		eventIsolationAssertAuthorizationError(
			t,
			fmt.Sprintf(
				"fetching records from %q with a KEY-SCOPED credential (declared prefix %q)",
				granted, keyPrefix),
			outcome.any(),
			kafka.TopicAuthorizationFailed,
		)

		assert.False(t, outcome.allowed(),
			"A KEY-SCOPED SUBSCRIBER MUST NOT REACH THE RAW PARTITION. An allowed fetch here is the "+
				"vulnerability this test exists to catch: the principal would be reading every record "+
				"on a topic it shares with other ledgers and other subscribers, and no response "+
				"field, projection or runbook could prevent it. Its own records are delivered by the "+
				"key-authorising component the deployment declared, which applies the prefix per record")
	})

	// REPLACING one prefix with another, which is the edit that moves nothing.
	t.Run("replacing the key prefix changes no boundary", func(t *testing.T) {
		before, beforeErr := fixture.admin.SubscriberCredentialExists(ctx, subscriber.KafkaPrincipal)
		require.NoError(t, beforeErr)
		require.True(t, before, "the premise is a subscriber that already holds a live credential")

		latePrefix := "ldg_late_" + uuid.NewString()
		updated, updateErr := fixture.service.UpdateSubscriber(ctx, subscriberID, SubscriberUpdate{
			PartitionKeyPrefix: &latePrefix,
		})
		require.NoError(t, updateErr,
			"swapping one prefix for another must be accepted, and must not depend on the broker: "+
				"there is no binding for it to move")
		require.NotNil(t, updated.PartitionKeyPrefix)
		assert.Equal(t, latePrefix, *updated.PartitionKeyPrefix)

		stored, readErr := fixture.service.GetSubscriber(ctx, subscriberID)
		require.NoError(t, readErr)
		require.NotNil(t, stored.PartitionKeyPrefix,
			"the registry records what the operator asked for")
		assert.Equal(t, latePrefix, *stored.PartitionKeyPrefix)
		require.NotNil(t, stored.CredentialReference,
			"and the working credential is left alone")

		after, afterErr := fixture.admin.SubscriberCredentialExists(ctx, subscriber.KafkaPrincipal)
		require.NoError(t, afterErr)
		assert.True(t, after,
			"the update must leave the credential at the broker untouched")

		// The boundary itself, re-observed. All three answers must be the same as before the edit.
		allowed, disclosed := eventIsolationListOffsets(ctx, principalClient, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("describing the granted topic %q after the prefix replacement", granted),
			allowed.any())
		assert.NotEmpty(t, disclosed,
			"the credential must still describe what it could describe before the update")

		eventIsolationAssertAuthorizationError(
			t,
			fmt.Sprintf("fetching %q after the prefix replacement", granted),
			eventIsolationFetch(ctx, principalClient, granted, 0).any(),
			kafka.TopicAuthorizationFailed,
		)

		outcome, leaked := eventIsolationListOffsets(ctx, principalClient, withheld[0])
		eventIsolationAssertReadDenied(t, withheld[0], outcome)
		assert.Empty(t, leaked,
			"and must still be refused %q: replacing a prefix widens nothing", withheld[0])
	})

	t.Run("clearing the key prefix re-grants record access at the broker", func(t *testing.T) {
		// THE WIDENING DIRECTION, and the reason this cycle is worth running end to end: it
		// is the same claim as point 3 read backwards. If clearing did NOT restore the fetch,
		// the narrowing would be indistinguishable from a broken grant — a subscriber that
		// could never read anything would satisfy every refusal above while proving nothing.
		//
		// WHILE THE KEY-SCOPED MODEL IS DECLARED, CLEARING IS REFUSED. It is the one path
		// that turns a live key-scoped principal into a whole-topic reader with issuance
		// never running again, so the deployment's own declaration is what stands in the way
		// — and the refusal is asserted here rather than assumed, because the widening below
		// would otherwise look like proof that clearing is always available.
		cleared := ""
		refusedUpdate, refusedErr := fixture.service.UpdateSubscriber(ctx, subscriberID, SubscriberUpdate{
			PartitionKeyPrefix: &cleared,
		})
		require.Error(t, refusedErr,
			"a deployment declaring key-scoped subscriber access must not let a subscriber be "+
				"widened out of that model by an ordinary update")
		assert.Nil(t, refusedUpdate)

		var refusal apierror.APIError
		require.ErrorAs(t, refusedErr, &refusal)
		assert.Equal(t, apierror.ErrSubscriberKeyScopeRequired, refusal.Code)

		stillScoped, readErr := fixture.service.GetSubscriber(ctx, subscriberID)
		require.NoError(t, readErr)
		require.NotNil(t, stillScoped.PartitionKeyPrefix,
			"and the refusal applied nothing: a half-applied clearing would leave the registry "+
				"describing whole-topic access it had just declined to grant")

		// THE REMEDY THE REFUSAL NAMES, exercised: the deployment stops declaring the
		// key-scoped model. That is a deployment-level decision rather than a per-subscriber
		// opt-out, which is the whole point — the model holds for every subscriber or for
		// none.
		eventIsolationPublishConfiguration(t, &config.Configuration{Kafka: fixture.env.kafka})
		_, stillDeclared := fixture.env.kafka.KeyScopeGateway()
		require.False(t, stillDeclared,
			"the republished configuration must declare nothing, or the clearing below is refused "+
				"again and this subtest proves neither half")

		updated, updateErr := fixture.service.UpdateSubscriber(ctx, subscriberID, SubscriberUpdate{
			PartitionKeyPrefix: &cleared,
		})
		require.NoError(t, updateErr,
			"and with the model no longer declared, clearing is an ordinary authorization update: "+
				"it is how a subscriber returns to direct consumption")
		require.Nil(t, updated.PartitionKeyPrefix,
			"a present empty string CLEARS the column rather than storing an empty prefix")
		assert.False(t, updated.RequiresGatewayDelivery())
		assert.True(t, updated.GrantsBrokerRecordAccess())
		require.NotNil(t, updated.CredentialReference,
			"and the credential survives: it is the GRANT that widens, not the identity")

		reissued, issueErr := fixture.service.IssueSubscriberCredential(ctx, subscriberID)
		require.NoError(t, issueErr)
		require.NotEmpty(t, reissued.Password())
		assert.Empty(t, reissued.PartitionKeyPrefix,
			"and the reissued credential declares no key scope, because the row records none")

		// The whole cycle — register with a prefix, issue, replace it, clear it, reissue — proven
		// at the broker rather than only in the registry.
		client := eventIsolationClient(t, fixture.env, reissued)

		outcome, disclosed := eventIsolationListOffsets(ctx, client, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("describing the granted topic %q after clearing the key prefix", granted),
			outcome.any())
		assert.NotEmpty(t, disclosed)

		eventIsolationAssertAllowed(t,
			fmt.Sprintf(
				"FETCHING %q after clearing the key prefix, which is what proves the earlier "+
					"refusal was the key scope and not a broken grant", granted),
			eventIsolationFetch(ctx, client, granted, 0).any())

		// And the topic boundary is untouched by the widening: clearing a prefix restores record
		// access to the topics the subscriber was granted, never to any other.
		refused, leaked := eventIsolationListOffsets(ctx, client, withheld[0])
		eventIsolationAssertReadDenied(t, withheld[0], refused)
		assert.Empty(t, leaked,
			"clearing a key scope must not reach outside the subscriber's topic grant")
	})
}

// TestEventIsolation_AKeyScopedSubscriberIsRefusedWhereNoGatewayIsDeclared is the isolation guarantee's
// fail-closed half against a REAL broker: the shipped configuration, and the credential
// that is never minted.
//
// event_subscriber_test.go proves the refusal is returned.
func TestEventIsolation_AKeyScopedSubscriberIsRefusedWhereNoGatewayIsDeclared(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, _ := eventIsolationGrantSplit(fixture)
	subscriberID := eventIsolationSubscriberID("keyscope-refused")

	// THE SHIPPED CONFIGURATION, asserted rather than assumed: the fixture publishes
	// env.kafka unchanged, which carries no key-scope declaration.
	_, active := fixture.env.kafka.KeyScopeGateway()
	require.False(t, active,
		"this test's premise is that NOTHING authorises record keys; a fixture that declared a "+
			"component would make the assertions below vacuous")

	keyPrefix := "ldg_" + uuid.NewString()
	subscriber, err := fixture.service.RegisterSubscriber(ctx, SubscriberRegistration{
		SubscriberID:       subscriberID,
		Name:               "event isolation probe (key scope refused)",
		AuthorizedTopics:   []string{granted},
		PartitionKeyPrefix: &keyPrefix,
	})
	require.NoError(t, err,
		"REGISTRATION is not the refusal: an operator must be able to record the intended boundary "+
			"before standing up the component that keeps it")
	require.NotNil(t, subscriber)
	t.Cleanup(func() { eventIsolationTeardown(t, fixture, subscriber) })

	credential, err := fixture.service.IssueSubscriberCredential(ctx, subscriberID)
	require.Error(t, err,
		"ISSUANCE IS the refusal: with nothing applying the prefix, every credential this service "+
			"could mint is either wider than the row describes or unable to fetch anything")
	assert.Zero(t, credential.PasswordLength(),
		"and no secret may exist, because a refusal that generated one created the thing it declined "+
			"to return")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberKeyScopeUnenforced, apiErr.Code,
		"the typed refusal, so an operator tells it from a broker outage")
	assert.Contains(t, err.Error(), "KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway",
		"naming the remedy that REALISES the recorded intent, which is the one an operator wanting "+
			"key scoping needs and the one the message used to omit entirely")
	assert.Contains(t, err.Error(), "clear the partition key prefix",
		"and the remedy that abandons it")
	assert.Contains(t, err.Error(), "narrow the subscriber's authorized topics",
		"and the enforceable alternative")

	// THE PROPERTY ONLY THE BROKER CAN ANSWER. Nothing authenticates as this principal.
	exists, existsErr := fixture.admin.SubscriberCredentialExists(ctx, subscriber.KafkaPrincipal)
	require.NoError(t, existsErr, "describing the SCRAM credential of %q", subscriber.KafkaPrincipal)
	assert.False(t, exists,
		"principal %q HOLDS A CREDENTIAL AT THE BROKER after a refused issuance: the refusal minted "+
			"and failed to clean up, so a principal nobody was handed a password for can authenticate",
		subscriber.KafkaPrincipal)

	// AND THE ROW RECORDS NO ISSUANCE, so a later read cannot present the subscriber as provisioned.
	stored, readErr := fixture.service.GetSubscriber(ctx, subscriberID)
	require.NoError(t, readErr)
	assert.Nil(t, stored.CredentialReference,
		"a refused issuance must record no credential reference")
	require.NotNil(t, stored.PartitionKeyPrefix,
		"and it must not clear the prefix as a way of making itself succeed")
	assert.Equal(t, keyPrefix, *stored.PartitionKeyPrefix)

	// THE REMEDY WORKS, which is what separates a refusal from a dead end. Clearing the prefix
	// makes the identical call succeed, against the same deployment and the same broker.
	cleared := ""
	widened, updateErr := fixture.service.UpdateSubscriber(ctx, subscriberID, SubscriberUpdate{
		PartitionKeyPrefix: &cleared,
	})
	require.NoError(t, updateErr, "clearing a prefix is the remedy the refusal names and is never itself refused")
	require.Nil(t, widened.PartitionKeyPrefix)

	issued, issueErr := fixture.service.IssueSubscriberCredential(ctx, subscriberID)
	require.NoError(t, issueErr,
		"and the remedy has to WORK: a refusal whose named remedy still fails is a dead end wearing "+
			"an instruction")
	assert.NotEmpty(t, issued.Password())
	assert.Equal(t, fixture.env.kafka.SubscriberBrokers, issued.Brokers,
		"and a prefix-less subscriber gets the subscriber-facing broker list, never a gateway "+
			"this deployment never declared")
}

// TestEventIsolation_AKeyScopeIsBoundAtTheDeclaredComponentBeforeAnyCredentialExists is
// The attestation rule's end-to-end half, against a REAL broker and a REAL control endpoint.
//
// event_keyscope_gateway_test.go proves the client speaks the contract.
// event_subscriber_test.go proves the service refuses when the contract is not
// satisfied. Neither can answer the question an operator actually has: when Blnk
// declares a subscriber key-scoped, IS THERE A COMPONENT HOLDING THAT BINDING, and is
// the broker-side state consistent with it?
//
//  1. ATTESTED. The binding the component holds is the principal and the exact stored
//     prefix, it was registered before any secret existed, and the credential that came
//     back authenticates against the broker while naming the component rather than a
//     broker address.
//  2. UNATTESTED. A component that answers for a WIDER prefix than the row records —
//     the truncating proxy, which is the dangerous misbehaviour because the subscriber
//     is told it is isolated — must produce a refusal that leaves NOTHING at the
//     broker.
func TestEventIsolation_AKeyScopeIsBoundAtTheDeclaredComponentBeforeAnyCredentialExists(t *testing.T) {
	t.Run("attested: the component holds the exact binding and the credential names it", func(t *testing.T) {
		fixture, ctx := eventIsolationSetup(t)

		granted, _ := eventIsolationGrantSplit(fixture)
		subscriberID := eventIsolationSubscriberID("keyscope-attested")
		keyPrefix := "ldg_" + uuid.NewString()

		gateway, double := eventIsolationDeclareKeyScopeGatewayWithDouble(t, fixture)

		subscriber, err := fixture.service.RegisterSubscriber(ctx, SubscriberRegistration{
			SubscriberID:       subscriberID,
			Name:               "event isolation probe (key scope attested)",
			AuthorizedTopics:   []string{granted},
			PartitionKeyPrefix: &keyPrefix,
		})
		require.NoError(t, err)
		require.NotNil(t, subscriber)
		t.Cleanup(func() { eventIsolationTeardown(t, fixture, subscriber) })

		require.Zero(t, double.requestCount(http.MethodPost),
			"the precondition: nothing has been bound by registration alone, so the assertion below "+
				"is about what ISSUANCE did")

		credential, err := fixture.service.IssueSubscriberCredential(ctx, subscriberID)
		require.NoError(t, err,
			"an attesting component must not block issuance: the declaration is satisfied")
		require.NotEmpty(t, credential.Password())

		held, bound := double.heldPrefix(subscriber.KafkaPrincipal)
		require.True(t, bound,
			"THE COMPONENT MUST HOLD A BINDING FOR THIS PRINCIPAL. Without one the credential's "+
				"declared key boundary rests on nothing, which is the state SEC-01 named")
		assert.Equal(t, keyPrefix, held,
			"and it must hold the EXACT stored prefix, byte for byte: a trimmed or normalised prefix "+
				"selects a different set of records than the registry describes")
		assert.Equal(t, "Bearer "+double.token, double.authorizationHeader(),
			"the call must be authenticated, or anything on the network can register bindings and "+
				"thereby decide what a subscriber sees")

		assert.Equal(t, gateway, credential.Brokers,
			"the credential names the component, never a broker address that would route around it")
		assert.Equal(t, model.KeyScopeEnforcementGateway, credential.KeyScopeEnforcement)

		// AND THE BROKER AGREES THE PRINCIPAL EXISTS, which is what makes this an end-to-end
		// assertion rather than two independent ones.
		exists, existsErr := fixture.admin.SubscriberCredentialExists(ctx, subscriber.KafkaPrincipal)
		require.NoError(t, existsErr, "describing the SCRAM credential of %q", subscriber.KafkaPrincipal)
		assert.True(t, exists,
			"principal %q must hold a credential at the broker: the attestation permitted the "+
				"issuance, so the issuance must have completed", subscriber.KafkaPrincipal)
	})

	t.Run("unattested: a wider prefix is refused and leaves nothing at the broker", func(t *testing.T) {
		fixture, ctx := eventIsolationSetup(t)

		granted, _ := eventIsolationGrantSplit(fixture)
		subscriberID := eventIsolationSubscriberID("keyscope-unattested")
		keyPrefix := "ldg_" + uuid.NewString()

		_, double := eventIsolationDeclareKeyScopeGatewayWithDouble(t, fixture)
		// THE TRUNCATING PROXY. It answers 200, for the right principal, with a prefix that
		// matches more keys than the row records.
		double.prefixOverride = "ldg_"

		subscriber, err := fixture.service.RegisterSubscriber(ctx, SubscriberRegistration{
			SubscriberID:       subscriberID,
			Name:               "event isolation probe (key scope unattested)",
			AuthorizedTopics:   []string{granted},
			PartitionKeyPrefix: &keyPrefix,
		})
		require.NoError(t, err,
			"registration is never the refusal: an operator records the intended boundary before "+
				"the component that keeps it is correct")
		require.NotNil(t, subscriber)
		t.Cleanup(func() { eventIsolationTeardown(t, fixture, subscriber) })

		credential, err := fixture.service.IssueSubscriberCredential(ctx, subscriberID)
		require.Error(t, err,
			"a component enforcing a WIDER boundary than the registry records must not be accepted: "+
				"the subscriber would be told it is isolated while reading a superset")
		assert.Zero(t, credential.PasswordLength())

		var apiErr apierror.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, apierror.ErrSubscriberKeyScopeUnattested, apiErr.Code,
			"the typed refusal, distinct from the no-component one so an operator knows the "+
				"component answered and answered wrongly")

		// THE PROPERTY ONLY THE BROKER CAN ANSWER.
		exists, existsErr := fixture.admin.SubscriberCredentialExists(ctx, subscriber.KafkaPrincipal)
		require.NoError(t, existsErr, "describing the SCRAM credential of %q", subscriber.KafkaPrincipal)
		assert.False(t, exists,
			"principal %q HOLDS A CREDENTIAL AT THE BROKER after an unattested issuance: the "+
				"attestation is ordered before the mint precisely so that this cannot happen",
			subscriber.KafkaPrincipal)

		stored, readErr := fixture.service.GetSubscriber(ctx, subscriberID)
		require.NoError(t, readErr)
		assert.Nil(t, stored.CredentialReference,
			"and no issuance may be recorded for a credential that does not exist")
	})
}

// There is no test of a Blnk-hosted read path for a key-scoped subscriber, because
// there is no such path.
