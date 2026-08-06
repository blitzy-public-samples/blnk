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

// This file owns ONE acceptance criterion and nothing else: subscriber isolation.
//
//	V-5 — "Credentials cannot read or list topics, partitions, or DLTs outside their
//	       ACL grant."
//
// It proves that by provisioning a real Kafka principal through the real issuance path
// and then, using that principal's OWN SASL/SCRAM credentials, attempting every read,
// list, group and write operation that its grant does not cover — and requiring the
// broker to refuse each one with a genuine authorization error.
//
// # THE MOST IMPORTANT THING IN THIS FILE
//
// This test is only meaningful against a broker configured with the KRaft
// StandardAuthorizer. In KRaft mode a broker started WITHOUT
//
//	authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
//
// ACCEPTS EVERY ACL BINDING AND APPLIES NONE OF THEM. Provisioning succeeds, the
// bindings are listable with kafka-acls, and every principal can read every topic —
// including every other subscriber's topics and all the dead-letter topics. An
// isolation test run against such a broker passes every assertion below while proving
// nothing whatsoever, which is the worst possible outcome for a security property: it
// reports itself satisfied precisely when it is absent.
//
// So the broker's configuration is PART OF THE CRITERION, and this file treats it that
// way. Enforcement is established twice, and a failure of either is a TEST FAILURE with
// an explicit diagnostic — never a skip and never a silent pass:
//
//  1. DECLARATIVELY, by asking the broker: KafkaAdminClient.AuthorizerActive probes
//     DescribeACLs and reports whether the broker answers SECURITY_DISABLED.
//  2. EMPIRICALLY, by trying it: a freshly provisioned principal must actually be
//     REFUSED on a topic outside its grant. A broker that allows that read has no
//     enforcement regardless of what it claims, and eventIsolationRequireEnforcement
//     fails the test with the reason spelled out.
//
// ProvisionSubscriberPrincipal applies the same rule from the other side — it refuses to
// write a credential at all unless enforcement is confirmed (its SEC-02 gate) — so an
// issuance that fails with ErrAuthorizerNotEnforcing is likewise surfaced here as the
// authorizer diagnostic rather than as an environment skip.
//
// docker-compose.yaml (line 317) and docker-compose.dev.yaml (line 322) both set
// authorizer.class.name to the KRaft StandardAuthorizer, and scripts/kafka-bootstrap.sh
// warns when the broker configuration it is pointed at does not. All three exist for
// exactly the reason above.
//
// # How to run it
//
// The test SKIPS unless a broker is configured, so `go test -short ./...` — what
// `make test` runs, and what CI depends on passing with no broker at all — is unaffected.
// To actually run it:
//
//  1. Bring the local stack up:            docker compose up -d kafka kafka-init
//     (or the equivalent for your stack; scripts/kafka-bootstrap.sh formats KRaft
//     storage with the bootstrap SCRAM admin credential, and
//     scripts/kafka-provision.sh creates the category topics and their .dlt siblings.)
//  2. Export the administrative credentials, which are the same variables the service
//     itself reads (AAP R-10) and are deliberately NOT hardcoded in this file:
//
//     export KAFKA_BROKERS=localhost:9092
//     export KAFKA_SASL_ADMIN_USER=...
//     export KAFKA_SASL_ADMIN_SECRET=...
//     export KAFKA_INSECURE_LOCAL_DEV=true   # the local broker listens SASL_PLAINTEXT
//
//  3. go test -run TestEventIsolation -count=1 .
//
// Missing broker, missing administrative credentials or unprovisioned topics are
// reported as skips that NAME what is absent. A broker that is present but does not
// enforce ACLs is reported as a FAILURE. The two are never conflated: the first means
// "this machine cannot answer the question", the second means "the answer is no".
//
// # Run it PACKAGE-SCOPED, not as part of `go test ./...`
//
// Note the `.` in step 3, and keep it. Those exported KAFKA_* variables are read by
// envconfig for the whole process, so with them set the config and cmd package tests fail
// — they assert on configuration loaded from a file, and envconfig overrides it from the
// environment. That is a pre-existing property of how this repository loads configuration
// and has nothing to do with this test; it just means the two invocations do not mix.
//
// The documented full-suite invocation exports only BLNK_TYPESENSE_DNS, and under it every
// test in this file skips — which is the intended behaviour, not a gap. Run the whole suite
// that way, and run this file explicitly, package-scoped, when you want the isolation
// criterion checked.
//
// # Scope boundary
//
// The Kafka client here exists FOR VERIFICATION ONLY. Blnk ships no consumer library
// and no subscriber-side error handling or dead-lettering — that is an explicit MUST NOT
// of the requirement (AAP §0.1.2, §0.6.2). Nothing in this file may be promoted into the
// package: it is a probe that asserts what the broker refuses, not a consumer anybody is
// meant to use.
//
// Deliberately NOT here, because other files own them:
//
//   - per-aggregate ordering (V-6)          → event_ordering_integration_test.go
//   - crash recovery (V-7)                  → event_recovery_integration_test.go
//   - dual delivery (V-8) and replay (V-9)  → event_dual_delivery_test.go,
//     event_replay_fidelity_test.go
//   - the HTTP credential endpoint and its master-key gate
//     → api/subscribers_api_test.go
//
// # Secret handling
//
// The credential this test mints is returned exactly once, by design. It is never
// logged, never written to a file or a fixture, and never placed in an assertion message
// or a failure operand — assertions about it are written as a boolean predicate with a
// secret-free message rather than as NotContains, because a failing NotContains prints
// both of its operands and would put the secret in the test output. The principal and
// its ACL bindings are revoked in teardown so a leaked SCRAM user cannot accumulate in
// the broker's metadata log and make a later run pass for the wrong reason.
//
// # No user rules were provided for this project
//
// review_rules reports none, so no project rule governs this file. It is held instead to
// the enterprise baseline the AAP anchors to the repository's own conventions: tests
// beside the source they exercise, package blnk, Test<Subject>_<Behaviour> naming,
// testify assertions, a short-mode guard as lineage_integration_test.go uses, the
// Apache-2.0 header above, and the secret-handling posture described just above.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
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
//
// package blnk is one namespace shared by the whole root package and by its sibling
// integration tests, several of which are authored alongside this one. A helper called
// something as tempting as `provision` or `subscriberClient` would collide the moment a
// neighbour reached for the same obvious name, and a redeclaration breaks the build for
// every test in the package rather than just this file.

// The environment variables this test reads. They are the SAME variables the service
// itself reads (AAP R-10), so a machine that can run Blnk against Kafka can run this
// test with no extra configuration.
//
// The administrative secret is deliberately NOT defaulted. A plausible-looking default
// in a committed test file is a credential in the repository, and the fact that it only
// works against a local broker today is not a property anybody can rely on tomorrow. An
// absent secret is an environment gap and is reported as a skip that names the variable.
const (
	eventIsolationBrokersEnv           = "KAFKA_BROKERS"
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

// Timeouts. Every broker operation in this file runs under an explicit deadline, because
// the failure mode worth designing against is not a refusal — a refusal is the answer the
// test wants — but a broker that accepts the connection and then stops answering. Without
// a deadline that hangs the whole package until the go test timeout kills it with no
// diagnostic at all.
const (
	// eventIsolationSetupTimeout bounds registration, issuance and the enforcement
	// probes. It is generously larger than SubscriberCredentialIssuanceBudget so that a
	// budget overrun is observed and asserted rather than masked by this deadline.
	eventIsolationSetupTimeout = 30 * time.Second

	// eventIsolationOperationTimeout bounds one authorization probe. A broker answers an
	// authorization decision from memory, so anything approaching this is a fault.
	eventIsolationOperationTimeout = 15 * time.Second

	// eventIsolationTeardownTimeout bounds revocation. Teardown runs on a fresh context
	// rather than the test's, because the test context is usually already cancelled by
	// the time cleanup runs and a cancelled context cannot revoke anything.
	eventIsolationTeardownTimeout = 20 * time.Second

	// eventIsolationDialTimeout bounds the reachability probe and every client dial.
	eventIsolationDialTimeout = 5 * time.Second

	// eventIsolationFetchWait bounds how long a fetch waits for records. The fetches here
	// assert authorization, not delivery, so they must not block for data.
	eventIsolationFetchWait = 500 * time.Millisecond
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
// whether an absent value is a skip or a failure. That distinction is the whole point of
// the guard sequence in eventIsolationSetup: "no broker" and "a broker that does not
// enforce ACLs" must never be reported the same way.
//
// Parameters:
//   - t *testing.T: used only for t.Helper, so failures point at the caller.
//
// Returns:
//   - eventIsolationEnvironment: the resolved environment; zero-valued when ok is false.
//   - string: a human-readable reason naming the missing variable; empty when ok.
//   - bool: whether a usable environment was resolved.
func eventIsolationResolveEnvironment(t *testing.T) (eventIsolationEnvironment, string, bool) {
	t.Helper()

	brokers := eventIsolationSplitBrokers(os.Getenv(eventIsolationBrokersEnv))
	if len(brokers) == 0 {
		return eventIsolationEnvironment{}, fmt.Sprintf(
			"%s is not set, so there is no Kafka broker to prove subscriber isolation against",
			eventIsolationBrokersEnv,
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
		// right default for production and exactly what the local single-broker KRaft
		// stack needs waived: it listens on SASL_PLAINTEXT and nothing else. Defaulting
		// it to true ONLY when TLS is off keeps the waiver scoped to the case that needs
		// it, and an operator running against a TLS broker never sees it applied.
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
// broker. Dropping blanks here is what keeps "not configured" distinguishable from
// "configured with something unusable".
//
// Parameters:
//   - raw string: the environment value.
//
// Returns:
//   - []string: the non-blank addresses, nil when none remain.
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
//
// Parameters:
//   - name string: the variable to read.
//   - fallback bool: the value to use when unset or unparseable.
//
// Returns:
//   - bool: the resolved value.
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
//
// Parameters:
//   - name string: the variable to read.
//   - fallback int: the value to use when unset, unparseable or non-positive.
//
// Returns:
//   - int: the resolved value.
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
//
// The topic-naming layer reads the prefix from the process-global configuration store on
// every call, so the names this test asserts on are only the names the broker was
// provisioned with if the store agrees. Publishing is therefore not a convenience — it is
// what makes TopicForCategory return the right topic.
//
// The restore matters just as much. config.ConfigStore is a process-global atomic.Value
// shared with every other test in the package, so leaving a Kafka-configured state behind
// would change unrelated tests' behaviour depending on run order. It follows the same
// save-and-restore shape event_outbox_test.go established, including publishing an empty
// configuration when nothing was there before: atomic.Value cannot be reset to nil, and
// an empty configuration is strictly closer to the original state than this one.
//
// Parameters:
//   - t *testing.T: the test whose cleanup restores the store.
//   - cnf *config.Configuration: the configuration to publish.
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

// eventIsolationRequireBroker skips the test when no broker answers a TCP dial.
//
// It is a plain dial rather than a Kafka round trip on purpose: this question is only
// "is there something listening?", and answering it without SASL keeps an unreachable
// broker distinguishable from a broker that rejects the administrative credentials. The
// first is an environment gap and skips; the second is a real misconfiguration and fails
// later, when an administrative call reports it.
//
// Parameters:
//   - t *testing.T: skipped when nothing is listening.
//   - address string: the bootstrap address to probe.
func eventIsolationRequireBroker(t *testing.T, address string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", address, eventIsolationDialTimeout)
	if err != nil {
		t.Skipf(
			"no Kafka broker is listening on %s (%v); bring the stack up with "+
				"`docker compose up -d kafka kafka-init` before running the subscriber-isolation test",
			address, err,
		)
	}

	require.NoError(t, conn.Close(), "closing the broker reachability probe")
}

// eventIsolationRequireAuthorizer FAILS the test when the broker does not enforce ACLs.
//
// # Why this is a failure and not a skip
//
// A skip says "this machine cannot answer the question". That is true of a missing broker
// and it is emphatically NOT true here: the broker answered, and the answer was that
// nothing is being checked. Skipping would file the most dangerous possible finding — a
// deployment whose subscriber boundaries do not exist — under "not run", where nobody
// looks. Failing puts it in front of the person who can fix it, with the remedy in the
// message.
//
// Both failure modes are fatal and for the same reason. SECURITY_DISABLED means there is
// no boundary. A probe that cannot be answered means the boundary is unverifiable, and
// from the point of view of the credential this test is about to mint, unverifiable and
// absent are indistinguishable — so they are treated identically, exactly as
// KafkaAdminClient.requireEnforcedAuthorizer treats them.
//
// Parameters:
//   - t *testing.T: failed when enforcement is not confirmed.
//   - ctx context.Context: bounds the probe.
//   - admin *KafkaAdminClient: the administrative client to probe with.
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

// eventIsolationRequireTopics skips the test when the topics it needs are not provisioned.
//
// It runs AFTER the authorizer check, and that order is deliberate: a provisioning gap
// must never be able to mask a broker with no enforcement. If the topic check came first,
// a stack whose topics were missing AND whose authorizer was absent would report the
// harmless problem and stay silent about the dangerous one.
//
// Existence is established with the ADMINISTRATIVE client, which is also what makes the
// non-disclosure assertions later in this file meaningful: when a subscriber is told a
// topic does not exist, this call has already proven that it does.
//
// Parameters:
//   - t *testing.T: skipped when a topic is absent.
//   - ctx context.Context: bounds the metadata read.
//   - admin *KafkaAdminClient: the administrative client.
//   - topics []string: the topics that must exist.
func eventIsolationRequireTopics(
	t *testing.T,
	ctx context.Context,
	admin *KafkaAdminClient,
	topics []string,
) {
	t.Helper()

	report, err := admin.TopicEndOffsets(ctx, topics...)
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
				"(or `docker compose up -d kafka-init`) before running the subscriber-isolation test",
			strings.Join(missing, ", "),
		)
	}
}

// eventIsolationStore is an in-memory eventSubscriberStore.
//
// # Why the registry is in memory while the broker is real
//
// The property under test lives entirely at the BROKER: does the authorizer refuse a
// principal the operations its grant does not cover? Postgres contributes nothing to that
// answer, and the registry's own persistence is already covered by
// database/event_subscriber_test.go.
//
// It also actively gets in the way. database.GetDBConnection memoises its connection in a
// sync.Once, so the DSN the whole package uses is decided by whichever test constructed a
// datasource first — which makes a real datasource here depend on run order, in a file
// whose entire value is that its result is trustworthy. And a registry row that outlives a
// failed run is a second thing to clean up in a test whose teardown already has to reach
// into a broker's metadata log.
//
// So the real path this test exercises is the one that matters: issuance goes through
// EventSubscriberService.IssueSubscriberCredential, which mints the SCRAM credential and
// binds the ACLs through KafkaAdminClient exactly as production does. Only the row
// underneath is local.
//
// The contract is mirrored faithfully rather than approximated, because
// IssueSubscriberCredential branches on it: a missing subscriber must report
// apierror.ErrSubscriberNotFound (which isSubscriberNotFoundError matches), and a
// credential reference that changed under an in-flight issuance must report
// apierror.ErrConflict rather than silently overwriting. Both are the same codes
// database/event_subscriber.go returns.
type eventIsolationStore struct {
	mu   sync.Mutex
	rows map[string]model.EventSubscriber
}

// eventIsolationNewStore builds an empty registry.
//
// Returns:
//   - *eventIsolationStore: a ready store.
func eventIsolationNewStore() *eventIsolationStore {
	return &eventIsolationStore{rows: make(map[string]model.EventSubscriber)}
}

// eventIsolationSubscriberNotFound is the typed error a missing row reports.
//
// Parameters:
//   - subscriberID string: the business key that was not found.
//
// Returns:
//   - error: an apierror carrying ErrSubscriberNotFound.
func eventIsolationSubscriberNotFound(subscriberID string) error {
	return apierror.NewAPIError(
		apierror.ErrSubscriberNotFound,
		"Event subscriber not found",
		fmt.Errorf("event isolation store: no subscriber %q is registered", subscriberID),
	)
}

// clone returns a defensive copy of a row.
//
// The registry hands out pointers, and a caller that mutated the slice inside one would be
// editing the store through the back door. Copying the slice header's contents as well as
// the struct is what makes the store behave like a database rather than like a shared map.
//
// Parameters:
//   - row model.EventSubscriber: the stored row.
//
// Returns:
//   - *model.EventSubscriber: an independent copy.
func (s *eventIsolationStore) clone(row model.EventSubscriber) *model.EventSubscriber {
	copied := row
	copied.AuthorizedTopics = append([]string(nil), row.AuthorizedTopics...)

	return &copied
}

// CreateEventSubscriber registers a subscriber and returns the stored row.
//
// Parameters:
//   - _ context.Context: unused; the store is local.
//   - subscriber *model.EventSubscriber: the row to store.
//
// Returns:
//   - *model.EventSubscriber: the stored row.
//   - error: a validation error for a nil or unkeyed subscriber, or a conflict for a
//     duplicate business key.
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
//
// Parameters:
//   - _ context.Context: unused.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the stored row.
//   - error: ErrSubscriberNotFound when absent.
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

// ListEventSubscribers pages the registry. Ordering is unspecified here because nothing in
// this file depends on it; the isolation assertions address subscribers by key.
//
// Parameters:
//   - _ context.Context: unused.
//   - limit int: maximum rows, non-positive means all.
//   - offset int: rows to skip.
//
// Returns:
//   - []model.EventSubscriber: the page.
//   - error: always nil.
func (s *eventIsolationStore) ListEventSubscribers(
	_ context.Context,
	limit, offset int,
) ([]model.EventSubscriber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows := make([]model.EventSubscriber, 0, len(s.rows))
	for _, row := range s.rows {
		rows = append(rows, *s.clone(row))
	}

	if offset >= len(rows) {
		return nil, nil
	}

	rows = rows[offset:]
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}

	return rows, nil
}

// UpdateEventSubscriber replaces the mutable columns of an existing row.
//
// Parameters:
//   - _ context.Context: unused.
//   - subscriber *model.EventSubscriber: the replacement row.
//
// Returns:
//   - error: ErrSubscriberNotFound when the row is absent.
func (s *eventIsolationStore) UpdateEventSubscriber(
	_ context.Context,
	subscriber *model.EventSubscriber,
) error {
	if subscriber == nil {
		return apierror.NewAPIError(
			apierror.ErrGenValidation,
			"A subscriber is required",
			errors.New("event isolation store: subscriber is nil"),
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.rows[strings.TrimSpace(subscriber.SubscriberID)]
	if !ok {
		return eventIsolationSubscriberNotFound(subscriber.SubscriberID)
	}

	row := *subscriber
	row.AuthorizedTopics = append([]string(nil), subscriber.AuthorizedTopics...)
	row.ID = existing.ID
	row.CreatedAt = existing.CreatedAt
	row.UpdatedAt = time.Now().UTC()
	s.rows[row.SubscriberID] = row

	return nil
}

// TakeEventSubscriber removes a subscriber and returns the row it removed, so the caller
// still holds the principal and topics that broker-side revocation needs.
//
// Parameters:
//   - _ context.Context: unused.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the removed row.
//   - error: ErrSubscriberNotFound when absent.
func (s *eventIsolationStore) TakeEventSubscriber(
	_ context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return nil, eventIsolationSubscriberNotFound(subscriberID)
	}

	delete(s.rows, key)

	return s.clone(row), nil
}

// RecordSubscriberCredentialIfUnchanged persists an issuance only while the row still
// holds the reference observed before provisioning.
//
// The conditional is the point, and it is reproduced rather than simplified: it is what
// makes a concurrent re-issue report a conflict instead of overwriting the record of the
// secret that actually works. The reference is validated the way the repository validates
// it, so a caller that passed a raw secret in this argument by mistake is refused here as
// well — and, as there, the offending value is never echoed.
//
// Parameters:
//   - _ context.Context: unused.
//   - subscriberID string: the business key.
//   - expected *string: the reference observed before provisioning; nil for a first
//     issuance.
//   - credentialReference string: the non-reversible reference to persist.
//   - issuedAt time.Time: when the issuance happened.
//
// Returns:
//   - error: ErrSubscriberNotFound, ErrConflict, or a validation error.
func (s *eventIsolationStore) RecordSubscriberCredentialIfUnchanged(
	_ context.Context,
	subscriberID string,
	expected *string,
	credentialReference string,
	issuedAt time.Time,
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

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return eventIsolationSubscriberNotFound(subscriberID)
	}

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

// ClearSubscriberCredential returns a row to the "registered, not yet provisioned" state.
//
// Parameters:
//   - _ context.Context: unused.
//   - subscriberID string: the business key.
//
// Returns:
//   - error: ErrSubscriberNotFound when absent.
func (s *eventIsolationStore) ClearSubscriberCredential(_ context.Context, subscriberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return eventIsolationSubscriberNotFound(subscriberID)
	}

	row.CredentialReference = nil
	row.CredentialIssuedAt = nil
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	return nil
}

// MarkSubscriberMigrated stamps the instant a subscriber completed its move to Kafka.
//
// Parameters:
//   - _ context.Context: unused.
//   - subscriberID string: the business key.
//   - migratedAt time.Time: the instant to stamp.
//
// Returns:
//   - error: ErrSubscriberNotFound when absent.
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

// PurgeMigratedSubscriberWebhookURLs erases the legacy URL of every subscriber that
// migrated strictly before the cut-off.
//
// Parameters:
//   - _ context.Context: unused.
//   - migratedBefore time.Time: the exclusive cut-off.
//
// Returns:
//   - int64: how many rows changed.
//   - error: always nil.
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

// eventIsolationReferencesMatch compares a stored credential reference with the one an
// issuance observed before it started.
//
// Two nils match — that is the first-issuance case — and a nil on one side only does not,
// which is the race the conditional write exists to catch.
//
// Parameters:
//   - stored *string: the reference currently on the row.
//   - expected *string: the reference the caller observed.
//
// Returns:
//   - bool: whether the row is unchanged.
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

	// grantable is the subscriber-facing category topics, in the order EventTopics
	// composes them. The fixture asserts there are at least two so that one can be
	// granted and another deliberately withheld.
	grantable []string
}

// eventIsolationSetup performs the whole guard sequence and returns a prepared fixture.
//
// # The order of these checks is the safety property
//
//  1. SHORT MODE. `make test` runs `go test -short ./...` and CI depends on it passing with
//     no broker in sight, so short mode skips before anything dials.
//  2. CONFIGURATION. Absent broker or administrative credentials is an environment gap:
//     skip, naming the variable.
//  3. REACHABILITY. Nothing listening is an environment gap: skip, naming the address.
//  4. ENFORCEMENT. A broker that does not enforce ACLs is a FAILURE, and it is checked
//     BEFORE topics so that a provisioning gap can never mask it.
//  5. TOPICS. Unprovisioned topics is an environment gap: skip, naming the script.
//
// Parameters:
//   - t *testing.T: skipped or failed according to the sequence above.
//
// Returns:
//   - *eventIsolationFixture: a prepared fixture; the function does not return when the
//     test is skipped or failed.
//   - context.Context: the setup context, already bounded and cancelled on cleanup.
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
	// of requirement R-7 is assertable.
	issuanceDuration time.Duration
}

// eventIsolationProvision registers a subscriber with a NARROW grant and issues its
// credential through the real production path.
//
// # Why the real path and not a hand-rolled admin call
//
// Calling KafkaAdminClient.ProvisionSubscriberPrincipal directly would test the ACL
// arithmetic and skip everything around it: the grant validation that refuses a
// dead-letter topic or a wildcard, the principal and consumer-group derivation, the
// enforcement gate, the five-second budget, and the compensation that revokes a credential
// whose bindings failed. Those are the parts a subscriber's boundary actually depends on in
// production, so the test goes through EventSubscriberService.IssueSubscriberCredential and
// exercises all of them.
//
// # Why the identifier is unique per run
//
// A Kafka SCRAM credential and its ACL bindings live in the broker's metadata log, which
// outlives the test process. A fixed identifier would collide between concurrent runs and,
// worse, would let a binding left behind by an earlier run satisfy a later run's
// assertions — a test passing on someone else's grant. The identifier is UUID-derived, so
// every run provisions a principal that has never existed before.
//
// Parameters:
//   - t *testing.T: failed on any provisioning error.
//   - ctx context.Context: bounds registration and issuance.
//   - fixture *eventIsolationFixture: the prepared run.
//   - label string: a short human tag, folded into the identifier for triage.
//   - grant []string: the topics to authorise. Deliberately narrow.
//
// Returns:
//   - *eventIsolationPrincipal: the provisioned principal, with a client that
//     authenticates as it.
func eventIsolationProvision(
	t *testing.T,
	ctx context.Context,
	fixture *eventIsolationFixture,
	label string,
	grant []string,
) *eventIsolationPrincipal {
	t.Helper()

	subscriberID := eventIsolationSubscriberID(label)

	subscriber, err := fixture.service.RegisterSubscriber(ctx, SubscriberRegistration{
		SubscriberID:     subscriberID,
		Name:             fmt.Sprintf("event isolation probe (%s)", label),
		AuthorizedTopics: grant,
	})
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
		// The one failure mode that must be reported as the authorizer diagnostic rather
		// than as an ordinary provisioning error: ProvisionSubscriberPrincipal refuses to
		// write a credential against a broker whose ACL enforcement is not confirmed. It
		// is the same finding eventIsolationRequireAuthorizer reports, arriving from the
		// other side, and it is still a failure and still never a skip.
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

	return &eventIsolationPrincipal{
		subscriber:       subscriber,
		credential:       credential,
		client:           eventIsolationClient(t, fixture.env, credential),
		issuanceDuration: elapsed,
	}
}

// eventIsolationSubscriberID mints a per-run subscriber identifier.
//
// The value has to satisfy model.CanonicalizeSubscriberIdentifier — lowercase ASCII
// letters, digits, underscore and hyphen, first character alphanumeric — because the Kafka
// principal and the consumer group are derived from it byte for byte. A UUID's canonical
// form is already lowercase hex and hyphens, so it qualifies as-is; the label is lowercased
// and stripped of anything else so a caller cannot accidentally introduce a character the
// derivation refuses.
//
// Parameters:
//   - label string: a short human tag.
//
// Returns:
//   - string: a canonical, unique subscriber identifier.
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

// eventIsolationClient builds a Kafka client that authenticates as the given subscriber.
//
// This is the only place in the repository that authenticates as a SUBSCRIBER, and it
// exists FOR VERIFICATION ONLY. Production never does this: Blnk publishes as its own
// producer principal and administers as its administrative principal, and subscribers
// connect from their own systems with their own consumers. Nothing here is a consumer
// library, and per the requirement's MUST NOT it must not become one.
//
// The transport is assembled directly rather than through NewKafkaTransport because that
// helper resolves the PRODUCER or ADMINISTRATIVE credential pair from configuration by
// role, and there is deliberately no role for "a subscriber" — a subscriber's secret is
// never in Blnk's configuration. The TLS posture is kept identical to the administrative
// client's so the test does not accidentally probe a different security posture from the
// one the service uses.
//
// Parameters:
//   - t *testing.T: failed when the SASL mechanism cannot be prepared.
//   - env eventIsolationEnvironment: the resolved environment.
//   - credential SubscriberCredential: the issued credential.
//
// Returns:
//   - *kafka.Client: a client bound to this principal.
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

// eventIsolationTeardown revokes a provisioned principal at the broker and removes its
// registry row.
//
// # Why this is not optional
//
// A SCRAM credential and its ACL bindings are broker state, not process state, so nothing
// about the test process exiting removes them. Left behind they accumulate in the metadata
// log run after run, and — far worse — a stale binding from an earlier run can satisfy a
// later run's assertions, which means a leak does not merely litter: it can make this test
// pass while proving nothing.
//
// It runs on a FRESH context. By the time cleanup runs the test's own context is usually
// cancelled, and a cancelled context revokes nothing.
//
// Failures are logged rather than failing the test, with one exception: a principal whose
// credential is still present after revocation is reported as an error, because that is the
// leak this function exists to prevent and a silent log line would let it recur.
//
// Parameters:
//   - t *testing.T: used for reporting.
//   - fixture *eventIsolationFixture: the prepared run.
//   - subscriber *model.EventSubscriber: the row to revoke and remove.
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
	// successfully registered is already absent and reports not-found, which is the desired
	// end state rather than a problem.
	if _, err := fixture.service.DeregisterSubscriber(ctx, subscriber.SubscriberID); err != nil &&
		!isSubscriberNotFoundError(err) {
		t.Logf("teardown: deregistering subscriber %q: %v", subscriber.SubscriberID, err)

		// Fall back to a direct broker revocation. The registry row is local and
		// disposable; the broker credential is not, so it gets a second attempt.
		if revokeErr := fixture.admin.RevokeSubscriber(ctx, subscriber); revokeErr != nil {
			t.Logf("teardown: revoking principal %q at the broker: %v",
				subscriber.KafkaPrincipal, revokeErr)
		}
	}

	exists, err := fixture.admin.SubscriberCredentialExists(ctx, subscriber.KafkaPrincipal)
	if err != nil {
		t.Logf("teardown: confirming principal %q was revoked: %v", subscriber.KafkaPrincipal, err)

		return
	}

	if exists {
		t.Errorf(
			"teardown LEAKED a live Kafka principal: %q still holds a SCRAM credential. Revoke it by "+
				"hand — a leaked principal accumulates in the broker's metadata log and a stale grant "+
				"can make a later isolation run pass for the wrong reason",
			subscriber.KafkaPrincipal,
		)
	}
}

// eventIsolationTLSConfig mirrors the transport security posture the service dials with.
//
// The verification client must probe the SAME listener under the SAME posture as the
// administrative client, or a refusal could be a TLS artefact rather than an authorization
// decision. The rule is the one config.KafkaConfig documents: plaintext only when TLS is
// off AND local development has been acknowledged.
//
// Parameters:
//   - cfg config.KafkaConfig: the resolved Kafka block.
//
// Returns:
//   - *tls.Config: a verified configuration, or nil for explicitly-permitted plaintext.
//   - error: TLS disabled without the local-dev acknowledgement, or unreadable CA material.
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

// eventIsolationTopicOutcome is the result of one topic-scoped probe: the error the request
// itself returned, and the error the broker attached to the topic or partition inside a
// successful response.
//
// Both halves are needed because Kafka answers an authorization decision at whichever level
// the API carries it. ListOffsets returns HTTP-200-shaped success with a PER-PARTITION
// error code; Fetch can fail at the request level; Produce carries a response-level error.
// Collapsing them would silently drop the very code the assertion is about.
type eventIsolationTopicOutcome struct {
	// requestErr is what the client call returned.
	requestErr error

	// brokerErr is the error the broker reported inside a successful response, at the
	// topic or partition level.
	brokerErr error
}

// any returns whichever error is present, preferring the broker's own code.
//
// The broker's code is the more specific and more trustworthy of the two: kafka-go wraps a
// request-level failure in its own prose, while a per-partition code is the authorizer's
// verdict verbatim.
//
// Returns:
//   - error: the broker error, else the request error, else nil.
func (o eventIsolationTopicOutcome) any() error {
	if o.brokerErr != nil {
		return o.brokerErr
	}

	return o.requestErr
}

// allowed reports that the broker raised no objection at all.
//
// Returns:
//   - bool: true when neither the request nor the broker reported an error.
func (o eventIsolationTopicOutcome) allowed() bool {
	return o.requestErr == nil && o.brokerErr == nil
}

// eventIsolationListOffsets asks the broker for a topic's partition offsets as the given
// principal.
//
// # Why ListOffsets is the read-denial primitive in this file
//
// It is the operation whose refusal is UNAMBIGUOUS. Reading offsets requires Describe on the
// topic, and a principal without it gets TOPIC_AUTHORIZATION_FAILED attached to the
// partition — the authorizer's verdict, stated in the response, with no room to
// misinterpret.
//
// A Fetch would be the more obvious choice and is a weaker instrument: kafka-go resolves the
// partition leader from metadata first, and Kafka deliberately answers metadata for an
// unauthorized topic with UNKNOWN_TOPIC_OR_PARTITION so as not to disclose that the topic
// exists. The read is refused either way, but the error that surfaces is "topic not found",
// which is indistinguishable from a broker where the topic genuinely is not there. Both
// probes are used below, and the fixture proves with the administrative client that every
// topic exists first, which is what turns non-disclosure into evidence.
//
// Parameters:
//   - ctx context.Context: bounds the request.
//   - client *kafka.Client: the principal's client.
//   - topic string: the topic to probe.
//
// Returns:
//   - eventIsolationTopicOutcome: the request-level and broker-level errors.
//   - []kafka.PartitionOffsets: only the partitions the broker actually DISCLOSED. A
//     partition it named while attaching an error to it is a refusal, not disclosure, and is
//     deliberately excluded — Kafka answers a refused ListOffsets by echoing the requested
//     partition with the error code and sentinel -1 offsets, so counting those as disclosed
//     would make an empty-result assertion impossible to write correctly.
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
//
// Parameters:
//   - ctx context.Context: bounds the request.
//   - client *kafka.Client: the principal's client.
//   - topic string: the topic to read.
//   - offset int64: the offset to read from.
//
// Returns:
//   - eventIsolationTopicOutcome: the request-level and broker-level errors.
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

		// The record set holds no assertion value here — only the authorization outcome
		// does — but kafka-go's documentation asks the caller to close it, and leaving it
		// open would hold the connection's buffer for the rest of the run. The reader is
		// declared as a bare RecordReader, so closability is discovered rather than
		// assumed; a refused request returns one that is not closeable at all.
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
// nothing else, so no Write binding exists for any topic. The probe targets a topic INSIDE
// the grant on purpose. On a granted topic the principal has Describe, so metadata resolves
// and the broker gets as far as evaluating the write itself, which it refuses with
// TOPIC_AUTHORIZATION_FAILED. Aimed at an ungranted topic the request would die earlier in
// metadata resolution and prove less.
//
// The record is never delivered — that is the point — so producing here cannot pollute a
// topic that sibling tests share.
//
// Parameters:
//   - ctx context.Context: bounds the request.
//   - client *kafka.Client: the principal's client.
//   - topic string: the topic to write to.
//
// Returns:
//   - eventIsolationTopicOutcome: the request-level and broker-level errors.
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
//
// Group authorization is a separate resource type from topic authorization, so it needs its
// own probe: a subscriber's grant reserves its own group NAMESPACE with a prefixed pattern
// and nothing beside it, and this is how "beside it" is tested.
//
// Parameters:
//   - ctx context.Context: bounds the request.
//   - client *kafka.Client: the principal's client.
//   - group string: the consumer group to read.
//   - topic string: a topic to ask about.
//
// Returns:
//   - error: the group-level error the broker reported, or the request error.
func eventIsolationOffsetFetch(
	ctx context.Context,
	client *kafka.Client,
	group string,
	topic string,
) error {
	ctx, cancel := context.WithTimeout(ctx, eventIsolationOperationTimeout)
	defer cancel()

	response, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
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
}

// eventIsolationOffsetCommit commits an offset to a consumer group as the given principal.
//
// Committing is the write half of group access and is authorized separately from reading, so
// a grant that leaked it would let one subscriber move another's consumer position — a
// denial-of-service against a tenant that no topic-level check would catch. GenerationID is
// -1 and MemberID empty, which is the simple-consumer form and needs no group membership.
//
// Parameters:
//   - ctx context.Context: bounds the request.
//   - client *kafka.Client: the principal's client.
//   - group string: the consumer group to commit to.
//   - topic string: the topic whose offset is committed.
//
// Returns:
//   - error: the partition-level error the broker reported, or the request error.
func eventIsolationOffsetCommit(
	ctx context.Context,
	client *kafka.Client,
	group string,
	topic string,
) error {
	ctx, cancel := context.WithTimeout(ctx, eventIsolationOperationTimeout)
	defer cancel()

	response, err := client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
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
}

// eventIsolationVisibleTopics lists every topic the broker is willing to disclose to the
// given principal.
//
// Kafka filters a full metadata response by Describe authorization rather than refusing it,
// so an unauthorized principal receives a SHORTER LIST instead of an error. That makes this
// the right shape for the list-operation half of the criterion: the assertion is about what
// is absent from the answer, not about an error code.
//
// Parameters:
//   - ctx context.Context: bounds the request.
//   - client *kafka.Client: the principal's client.
//
// Returns:
//   - []string: the topic names the broker disclosed.
//   - error: the request error, when the listing could not be obtained at all.
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
//
// Describing ACLs requires Describe on the CLUSTER, which a subscriber's grant does not
// include — deliberately, because a principal that could enumerate the authorization state
// could read off every other subscriber's topics and group namespaces by name even without
// being able to consume them. It is the cluster-scoped list denial, and the broker states it
// plainly as CLUSTER_AUTHORIZATION_FAILED.
//
// Parameters:
//   - ctx context.Context: bounds the request.
//   - client *kafka.Client: the principal's client.
//
// Returns:
//   - error: the error the broker reported, or nil when the listing was allowed.
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
// authorization grounds, and specifically not something that merely looks like a refusal.
//
// # Why the error KIND is asserted and not just its presence
//
// A non-nil error proves nothing on its own. A connection refused because the broker went
// away, a SASL handshake that failed because the credential was revoked, a request that
// timed out — every one of those is a non-nil error on an operation the principal was in
// fact authorized to perform, and a test that accepted them would report isolation while the
// broker enforced nothing. So the assertion is positive and narrow: the error must carry one
// of the codes the Kafka protocol reserves for an authorization decision.
//
// Parameters:
//   - t *testing.T: failed when the error is absent or of the wrong kind.
//   - operation string: what was attempted, for the failure message.
//   - err error: the error to classify.
//   - expected ...error: the acceptable refusals. Normally kafka.Error protocol codes;
//     protocol.ErrNoTopic is also permitted where the refusal reaches the client as
//     non-disclosure, and the callers that allow it say why.
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

// eventIsolationAssertReadDenied requires the broker's DIRECT authorization verdict on a
// read outside the grant.
//
// It is deliberately the strictest assertion in this file: nothing but
// TOPIC_AUTHORIZATION_FAILED is accepted. That is exactly what the authorizer attaches to
// the partition of a ListOffsets request from a principal without Describe on the topic, so
// there is no reason to accept anything looser and every reason not to — a laxer expectation
// is how a test starts passing on a timeout or a stale connection.
//
// Use it with eventIsolationListOffsets. The consumer-shaped probes go through
// eventIsolationAssertTopicUndiscoverable instead, because their refusal legitimately
// arrives in a different shape.
//
// Parameters:
//   - t *testing.T: failed when the read was allowed or refused for the wrong reason.
//   - topic string: the topic that must be unreadable.
//   - outcome eventIsolationTopicOutcome: the probe result.
func eventIsolationAssertReadDenied(t *testing.T, topic string, outcome eventIsolationTopicOutcome) {
	t.Helper()

	eventIsolationAssertAuthorizationError(
		t,
		fmt.Sprintf("reading the offsets of topic %q outside the subscriber's grant", topic),
		outcome.any(),
		kafka.TopicAuthorizationFailed,
	)
}

// eventIsolationAssertTopicUndiscoverable requires that a consumer-shaped operation could not
// even locate the topic.
//
// # Why this refusal has a different shape, and why it is still the boundary
//
// Kafka does not tell a principal that a topic it may not describe exists. Metadata for such
// a topic comes back as UNKNOWN_TOPIC_OR_PARTITION — deliberate NON-DISCLOSURE, so that a
// probing client cannot enumerate the names of topics it has no access to. Since kafka-go
// resolves a partition leader from metadata before it can fetch or produce, a Fetch or a
// Produce fails at that resolution step and surfaces the client-side protocol.ErrNoTopic
// rather than the authorizer's verdict.
//
// Accepting that WOULD be a real weakening if the topic might genuinely be absent, because
// then "topic not found" would mean "there was nothing there to protect". That hole is closed
// before any assertion runs: eventIsolationRequireTopics proves with the ADMINISTRATIVE
// client that every topic named in this file exists, and skips the test when one does not.
// Given that proof, a principal being told the topic does not exist is the strongest form the
// boundary takes — the broker refused to admit its existence at all.
//
// The direct verdict is accepted too, because a broker or client version that surfaces it
// instead is equally correct and equally a refusal.
//
// Parameters:
//   - t *testing.T: failed when the operation was allowed or refused for the wrong reason.
//   - operation string: what was attempted, for the failure message.
//   - err error: the error to classify.
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
// Only GROUP_AUTHORIZATION_FAILED is accepted. Unlike a topic, a consumer group needs no
// non-disclosure treatment — groups are created by clients at will, so their names carry no
// secret and Kafka states the refusal directly. Accepting anything looser here would allow a
// coordinator-not-available or an unknown-group answer to stand in for a refusal.
//
// Parameters:
//   - t *testing.T: failed when the operation was allowed or refused for the wrong reason.
//   - operation string: what was attempted, for the failure message.
//   - err error: the error to classify.
func eventIsolationAssertGroupDenied(t *testing.T, operation string, err error) {
	t.Helper()

	eventIsolationAssertAuthorizationError(t, operation, err, kafka.GroupAuthorizationFailed)
}

// eventIsolationAssertAllowed requires that an operation INSIDE the grant succeeded.
//
// # Why the positive assertions are load-bearing
//
// Every refusal above would also be reported by a principal whose credential does not work
// at all, or by a broker that refuses everything. A file containing only negative
// assertions can therefore pass while testing nothing but a broken credential. These
// assertions are what fix the test's meaning: the same credential that is refused
// everything outside its grant is demonstrably accepted everywhere inside it, so the
// refusals can only be the grant boundary.
//
// Parameters:
//   - t *testing.T: failed when the operation was refused.
//   - operation string: what was attempted, for the failure message.
//   - err error: the error, expected nil.
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
// # Why a second check, when AuthorizerActive already answered
//
// AuthorizerActive asks the broker to describe its own configuration. That is the right
// first question and it is not the last one, because it establishes only that an authorizer
// is loaded — not that this principal's bindings are the ones being applied. A broker with a
// permissive super-user list, an accidental wildcard binding, an authorizer that failed to
// load its metadata, or simply an ACL grant that turned out broader than intended would all
// answer "active" and then allow the read anyway.
//
// So enforcement is also proven by DOING it: this principal, on this topic outside its
// grant, must actually be refused. If it is allowed, every assertion that follows would pass
// vacuously, so the test stops here with the reason stated rather than continuing to a green
// result that means nothing.
//
// Parameters:
//   - t *testing.T: failed when the ungranted read is allowed.
//   - ctx context.Context: bounds the probe.
//   - principal *eventIsolationPrincipal: the provisioned principal.
//   - ungranted string: a topic the principal was NOT granted.
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
// A grant of exactly one topic is what gives the test something to prove. A subscriber
// authorised for everything has no boundary to test, and a subscriber authorised for nothing
// would be refused everything for the trivial reason that it holds no bindings at all —
// which is why the positive assertions matter as much as the negative ones.
//
// Parameters:
//   - fixture *eventIsolationFixture: the prepared run, whose grantable list is already
//     asserted to hold at least two topics.
//
// Returns:
//   - string: the topic to grant.
//   - []string: the category topics to withhold.
func eventIsolationGrantSplit(fixture *eventIsolationFixture) (string, []string) {
	return fixture.grantable[0], append([]string(nil), fixture.grantable[1:]...)
}

// TestEventIsolation_SubscriberCredentialIsRefusedEverythingOutsideItsGrant is acceptance
// criterion V-5.
//
// It provisions a real principal with a deliberately narrow grant — ONE category topic, no
// dead-letter topics — and then requires the broker to refuse it, on authorization grounds,
// every read, every list and every write outside that grant.
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
			// eventIsolationAssertTopicUndiscoverable for why that is the stronger answer
			// and not a weaker assertion.
			eventIsolationAssertTopicUndiscoverable(t,
				fmt.Sprintf("fetching records from topic %q outside the subscriber's grant", topic),
				eventIsolationFetch(ctx, principal.client, topic, 0).any())
		}
	})

	t.Run("reading a dead-letter topic outside the grant is refused", func(t *testing.T) {
		// Both the dead-letter sibling of a WITHHELD category and the sibling of the
		// GRANTED one. The second is the case worth being explicit about: a grant on
		// blnk.transactions must not carry blnk.transactions.dlt with it. Dead-letter
		// topics hold the events that failed to publish, so a leaked DLT grant exposes
		// exactly the payloads an operator is still triaging.
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
			// A group that shares this subscriber's namespace as a strict PREFIX of its
			// own name rather than sitting inside it. "blnk-sub-<id>" and
			// "blnk-sub-<id>x" look alike and are different namespaces; a binding that
			// matched the second would be reserving more than it should.
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
		// Describe on each authorised topic and Read on the group namespace. No Write
		// binding is created for any topic, so a subscriber cannot forge a ledger event on
		// the very topic it is entitled to consume.
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

// TestEventIsolation_SubscriberCredentialIsAllowedEverythingInsideItsGrant is the other half
// of criterion V-5, and it is not decoration.
//
// A file of refusals alone would pass against a credential that does not work at all, or
// against a broker refusing every request for an unrelated reason. This test pins the
// meaning of those refusals: the same credential is demonstrably accepted for every
// operation inside its grant, so what it is refused elsewhere can only be the boundary.
func TestEventIsolation_SubscriberCredentialIsAllowedEverythingInsideItsGrant(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, withheld := eventIsolationGrantSplit(fixture)
	principal := eventIsolationProvision(t, ctx, fixture, "granted", []string{granted})

	// Even the POSITIVE test gates on enforcement. Without it, "the credential can do
	// everything inside its grant" is a green result on a broker where it can do everything
	// everywhere — true, reassuring, and worthless. The gate keeps that reading impossible.
	eventIsolationRequireEnforcement(t, ctx, principal, withheld[0])

	t.Run("the granted topic is readable", func(t *testing.T) {
		outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading offsets of the granted topic %q", granted), outcome.any())
		require.NotEmpty(t, disclosed,
			"the broker disclosed no partitions of the granted topic %q", granted)

		// Read from the topic's actual first offset rather than zero: a topic whose head
		// has been truncated by retention answers OFFSET_OUT_OF_RANGE at zero, which would
		// be a spurious failure with nothing to do with authorization.
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
		// The group binding uses a PREFIXED pattern, which is the one intentional widening
		// in the grant: a subscriber can stand up a replay group beside its live one
		// without an administrative round trip. This asserts the widening actually works —
		// and the companion test asserts it widens only within the subscriber's own
		// namespace.
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
// Two principals are provisioned independently, on disjoint grants. Each must be refused the
// other's topic and the other's consumer group namespace — the latter being the assertion
// that the prefixed group pattern RESERVES a namespace rather than sharing one.
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

		// Both the other's default group and an arbitrary leaf inside the other's
		// namespace. The leaf is the assertion that the prefixed pattern reserves the whole
		// namespace for its owner rather than merely naming one group.
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
// issuance properties requirement R-7 states, against a real broker.
//
// The unit tests already drive these through a fake provisioner. What only a real broker can
// establish is that four genuine administrative round trips — the enforcement probe, the
// credential describe, the credential upsert and the ACL creation — complete inside the
// five-second budget, and that the credential the broker actually minted is SCRAM-SHA-512
// at the required iteration count.
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
		assert.Equal(t, fixture.env.kafka.Brokers, credential.Brokers,
			"the credential must hand back the broker list the admin client actually dialled")
		assert.Equal(t, strings.Join(fixture.env.kafka.Brokers, ","), credential.BrokerEndpoint)
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

	t.Run("the secret is returned once and exists nowhere else", func(t *testing.T) {
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

		// The registry keeps a non-reversible reference and an issuance instant, and
		// nothing that could be authenticated with. There is no route by which the secret
		// can be read a second time; a lost password can only be replaced.
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

// TestEventIsolation_RevokedCredentialCanNoLongerReachTheBroker proves that revocation ends
// access, which is both the last state of the isolation boundary and the guarantee this
// file's own teardown depends on.
//
// # Why it belongs here
//
// A grant that cannot be withdrawn is not a boundary. And a teardown that did not really
// revoke would leave live principals in the broker's metadata log where a stale binding from
// an earlier run could satisfy a later run's assertions — a leak that does not merely litter
// but can make this whole file pass for the wrong reason. This test asserts the mechanism
// every other test's cleanup relies on.
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
		// handshake fails, because the credential is gone. On a connection that was already
		// authenticated the handshake never repeats, so the request reaches the authorizer
		// and is refused on the deleted bindings instead — which is itself worth having
		// asserted, because it shows revocation ends access on established connections and
		// not merely on future ones.
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
