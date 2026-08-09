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
// ACCEPTS EVERY ACL BINDING AND APPLIES NONE OF THEM. Provisioning succeeds, the bindings are
// listable with kafka-acls, and every principal can read every topic — including every other
// subscriber's topics and all the dead-letter topics.
//
// WHAT THAT DOES TO A TEST DEPENDS ENTIRELY ON WHAT THE TEST ASSERTS. A test that provisions a
// principal and then checks that the expected bindings EXIST passes vacuously on such a broker:
// the bindings are all there, and the test never asks whether they do anything. The assertions
// below are not of that kind. They assert REFUSALS — a principal reading outside its grant must be
// denied — so an unenforcing broker makes them FAIL rather than pass.
//
// That failure is the right outcome but a confusing one to debug, since it looks like an isolation
// defect in Blnk rather than a broker that was never enforcing anything. So the broker's
// configuration is PART OF THE CRITERION, and enforcement is established explicitly, twice, before
// any isolation assertion is read. A failure of either is a TEST FAILURE naming the broker as the
// cause — never a skip and never a silent pass:
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
// docker-compose.yaml and docker-compose.dev.yaml both set authorizer.class.name to the KRaft
// StandardAuthorizer in the server.properties their kafka service renders — grep for
// authorizer.class.name rather than for a line number, which shifts — and
// scripts/kafka-bootstrap.sh warns when the broker configuration it is pointed at does not.
// All three exist for exactly the reason above.
//
// Note that those two services now sit behind the "kafka" Compose profile, so bringing the
// broker up is `docker compose --profile kafka up -d kafka kafka-init` rather than a bare
// `docker compose up`. Without the profile there is no broker and this test skips.
//
// # How to run it
//
// The test SKIPS unless a broker is configured, so `go test -short ./...` — what `make test` runs,
// and what CI depends on passing with no broker — is unaffected. To actually run it:
//
//  1. Bring the local stack up:            docker compose --profile kafka up -d kafka kafka-init
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
// A missing broker, missing administrative credentials or unprovisioned topics are reported as
// skips that NAME what is absent; a broker that is present but does not enforce ACLs is reported as
// a FAILURE. The first means "this machine cannot answer the question", the second "the answer is
// no".
//
// KEEP THE `.` IN STEP 3. Those exported KAFKA_* variables are read by envconfig for the whole
// process, so with them set the config and cmd package tests fail — they assert on configuration
// loaded from a file, which envconfig then overrides. That is a pre-existing property of how this
// repository loads configuration; it just means the two invocations do not mix. Under the
// documented full-suite invocation every test here skips, which is intended rather than a gap.
//
// # Scope boundary
//
// The Kafka client here exists FOR VERIFICATION ONLY: Blnk ships no consumer library and no
// subscriber-side error handling or dead-lettering, which is an explicit MUST NOT of the
// requirement (AAP §0.1.2, §0.6.2). Nothing here may be promoted into the package.
//
// Owned elsewhere: ordering (V-6) by event_ordering_integration_test.go, crash recovery (V-7) by
// event_recovery_integration_test.go, dual delivery and replay (V-8, V-9) by
// event_dual_delivery_test.go and event_replay_fidelity_test.go, and the HTTP credential endpoint
// with its master-key gate by api/subscribers_api_test.go.
//
// # Secret handling
//
// The credential this test mints is RETURNED BY ISSUANCE AND NEVER PERSISTED: the registry keeps a
// non-reversible reference and an issuance timestamp, so once the issuance result is discarded the
// secret cannot be retrieved from Blnk at all. It lives only in this test's memory for the
// lifetime of the result value, and the accessor on that value may be read as often as the test
// needs — "issued once" is a statement about where the secret is stored, not a one-shot read.
//
// It is never logged, never written to a file or a fixture, and never placed in an assertion
// message or a failure operand: assertions about it are boolean predicates with secret-free
// messages rather than NotContains, because a failing NotContains prints both operands. The
// principal and its bindings are revoked in teardown so a leaked SCRAM user cannot accumulate in
// the broker's metadata log and make a later run pass for the wrong reason.
//
// # Conventions this file follows
//
// The repository's own: tests beside the source they exercise, package blnk,
// Test<Subject>_<Behaviour> naming, testify assertions, a short-mode guard as
// lineage_integration_test.go uses, the Apache-2.0 header above, and the secret-handling posture
// described just above.

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

	// eventIsolationSettleTimeout bounds every wait for BROKER STATE TO CATCH UP, as opposed
	// to a wait for an answer.
	//
	// A KRaft broker holds SCRAM credentials and group coordination in its metadata log, and
	// both become usable slightly after the administrative call that created them returns.
	// Three things in this file were racing that gap and failing on a COLD broker while
	// passing on a warm one — the worst possible shape, because CI always starts cold:
	//
	//   - The FIRST authenticated request made with a just-minted credential answered
	//     [58] SASL Authentication Failed.
	//   - The first consumer-group request answered [16] Not Coordinator For Group, because
	//     with auto.create.topics.enable=false the internal __consumer_offsets topic does
	//     not exist until some client performs group activity, and provisioning creates only
	//     the eight blnk.* topics.
	//   - Teardown read the credential list immediately after revoking and reported a LEAK
	//     that the same run's own log showed had been revoked.
	//
	// It is deliberately much larger than the propagation actually takes. Nothing waits the
	// whole window unless something is wrong, and when something IS wrong the diagnostic is
	// the same either way — so the cost of being generous is zero and the cost of being tight
	// is a flake.
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
// whether an absent value is a skip or a failure. That distinction is the whole point of
// the guard sequence in eventIsolationSetup: "no broker" and "a broker that does not
// enforce ACLs" must never be reported the same way.
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
	//
	// A credential names the addresses the subscriber will connect to, and those are the
	// broker's EXTERNAL listener — which in a real deployment is not the internal bootstrap
	// list the relay uses. Issuance therefore refuses when it is unset rather than falling
	// back, so that a deployment cannot hand out its internal addresses by omission. This
	// fixture must not paper over that: defaulting it here would make the test pass while
	// the production refusal it depends on went unexercised, and the day the refusal broke
	// nothing would notice. It is set by .env.example and by both compose files for the
	// local stack, so a machine that can run Blnk against Kafka already has it.
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
func eventIsolationRequireTopics(
	t *testing.T,
	ctx context.Context,
	admin *KafkaAdminClient,
	topics []string,
) {
	t.Helper()

	// A zero instant asks for end offsets only: this probe cares whether the topics EXIST,
	// not how much of their content is recent, and asking for a window would add a
	// ListOffsets round trip whose answer nothing here reads (PERF-P05).
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
// The property under test lives entirely at the BROKER — does the authorizer refuse a principal the
// operations its grant does not cover? — so Postgres contributes nothing, and the registry's own
// persistence is covered by database/event_subscriber_test.go. It would also get in the way:
// database.GetDBConnection memoises its connection in a sync.Once, so a real datasource here would
// make this file depend on which test constructed one first, and a registry row outliving a failed
// run is a second thing to clean up in a teardown that already reaches into a metadata log.
//
// Issuance still goes through the real EventSubscriberService.IssueSubscriberCredential, which mints
// the SCRAM credential and binds the ACLs through KafkaAdminClient as production does; only the row
// underneath is local. The store contract is mirrored faithfully because that path branches on it: a
// missing subscriber must report apierror.ErrSubscriberNotFound, and a credential reference that
// changed under an in-flight issuance must report apierror.ErrConflict rather than overwriting —
// the same codes database/event_subscriber.go returns.
type eventIsolationStore struct {
	mu   sync.Mutex
	rows map[string]model.EventSubscriber

	// fences mirrors the provisioning_token / provisioning_until pair the real table
	// carries. It is a SEPARATE map rather than two fields on the row because the
	// repository deliberately does not project those columns into
	// model.EventSubscriber — that struct is serialised into API responses, and a live
	// claim token in a response body would be an internal lock handed to a caller.
	fences map[string]eventIsolationFence

	// obligations mirrors the five settlement columns, and is a separate map for the same
	// reason fences is: they are operational bookkeeping the repository deliberately does not
	// project into model.EventSubscriber, because that struct is serialised into API responses.
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

// seedLegacyKeyScope WAS RETIRED HERE. It wrote a partition_key_prefix straight onto a stored row,
// bypassing validation, so a test could model a subscriber that already carried a key scope.
//
// That was only necessary while recording a key scope was REFUSED: with no way to ask for one, a
// row holding one could only be manufactured. Registration now issues the scope and discloses that
// the broker does not enforce it, so a test that needs a key-scoped subscriber registers one — and
// a fixture built the way production builds it is worth more than one built behind its back.
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

// UpdateEventSubscriber replaces the mutable columns of an existing row, under the caller's
// provisioning claim and only while the row is not tombstoned for deregistration.
//
// Both predicates are reproduced rather than simplified. They are what stop a caller whose
// lease expired from overwriting the authorization a new owner reconciled with the broker, and
// what stop an update from WIDENING the topic set of a subscriber whose revocation is already in
// flight — and a fake that omitted them would let a test asserting either property pass against
// a store that does not enforce it.
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
//
// The conditional is the point, and it is reproduced rather than simplified: it is what
// makes a concurrent re-issue report a conflict instead of overwriting the record of the
// secret that actually works. The reference is validated the way the repository validates
// it, so a caller that passed a raw secret in this argument by mistake is refused here as
// well — and, as there, the offending value is never echoed.
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
// operator needs is how long the revocation has been outstanding rather than the age of the
// last attempt. This mirror does the same, so a test that retries a deregistration sees the
// same timestamp it saw the first time.
//
// Parameters:
//   - _ context.Context: unused; the store is local.
//   - subscriberID string: the business key.
//   - pendingAt time.Time: the instant to record on the FIRST marking.
//   - fenceToken string: the provisioning claim the caller holds.
//
// Returns:
//   - *model.EventSubscriber: the marked row, carrying the principal and topics to revoke.
//   - error: ErrSubscriberNotFound when absent, ErrConflict when the claim is not the caller's.
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
// subscriber that was never registered reports ErrSubscriberNotFound, and one that exists
// under a live claim reports ErrConflict. Collapsing them would send a caller looking for a
// race that did not happen.
//
// An EXPIRED claim is not a conflict. That is what stops one crashed issuance from fencing a
// subscriber permanently, and it is the property the fence's whole design rests on.
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

	// THE SHARED PREDICATE, which is token AND LEASE. Each of these three sites tested the token
	// alone, so an EXPIRED claim was accepted as held — and the production statements carry
	// `provisioning_until > NOW()` in the write itself, so the fake was modelling a permission the
	// database does not grant.
	key := strings.TrimSpace(subscriberID)
	if err := s.requireFenceLocked(subscriberID, token, "releasing the provisioning claim"); err != nil {
		return err
	}

	delete(s.fences, key)

	return nil
}

// RenewSubscriberProvisioningFence extends a claim the caller still holds.
//
// It is CONDITIONAL and it does NOT re-claim, exactly as the repository's is. A caller whose
// lease was taken over must learn it no longer owns the subscriber rather than be handed it
// back, because the new owner is mid-flight against the broker.
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
			apierror.ErrConflict,
			"This subscriber is being deregistered, so its access model can no longer be changed",
			fmt.Errorf("event isolation store: subscriber %q carries a revocation tombstone", subscriberID),
		)
	}

	return nil
}

// eventIsolationReferencesMatch compares a stored credential reference with the one an
// issuance observed before it started.
//
// Two nils match — that is the first-issuance case — and a nil on one side only does not,
// which is the race the conditional write exists to catch.
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

	// grantable is every category topic, in the order EventTopics
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

// eventIsolationProvision registers a subscriber with a NARROW grant and issues its credential
// through the real production path.
//
// Calling KafkaAdminClient.ProvisionSubscriberPrincipal directly would test the ACL arithmetic and
// skip everything a subscriber's boundary depends on: the grant validation that refuses a
// dead-letter topic or a wildcard, the principal and consumer-group derivation, the enforcement
// gate, the five-second budget, and the compensation that revokes a credential whose bindings
// failed. So issuance goes through EventSubscriberService.IssueSubscriberCredential.
//
// The identifier is UUID-derived because a SCRAM credential and its bindings live in the broker's
// metadata log, which outlives the test process: a fixed identifier would collide between
// concurrent runs and could let a binding left behind by an earlier run satisfy a later run's
// assertions — a test passing on someone else's grant.
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

	client := eventIsolationClient(t, fixture.env, credential)

	// The credential exists in the broker's metadata log; it is not necessarily USABLE yet.
	// Waiting for it here, once, is what keeps every assertion in every test from having to
	// know that — and the wait is registered AFTER `elapsed` is taken, so the five-second
	// issuance budget is still measured on issuance alone.
	eventIsolationAwaitAuthenticable(t, ctx, subscriberID, client)

	return &eventIsolationPrincipal{
		subscriber:       subscriber,
		credential:       credential,
		client:           client,
		issuanceDuration: elapsed,
	}
}

// eventIsolationAwaitAuthenticable blocks until a freshly minted credential can authenticate.
//
// # What was wrong
//
// AlterUserScramCredentials returns once the credential is in the KRaft metadata log, and the
// broker's authenticator picks it up a moment later. The first authenticated request a test made
// with a new credential therefore answered [58] SASL Authentication Failed on a COLD broker —
// and the failure landed on the POSITIVE half of an assertion, the half that establishes the
// credential works at all, so it read as "isolation is broken" when the credential was merely
// new. It passed on a warm broker, which is the shape that makes CI red and a developer's
// machine green.
//
// # Why only error 58 is retried, and why a timeout is a FAILURE and not a skip
//
// [58] is the one code that propagation explains. Anything else — an authorization refusal, a
// transport error, a broker that is not there — returns immediately, so nothing about the
// boundary under test is softened.
//
// And if the window elapses the credential genuinely does not work: the issuance path reported
// success and produced something that cannot authenticate, which is a defect in exactly the
// thing requirement R-7 promises. So this fails, loudly, naming the principal.
//
// The probe is Metadata with no topics, which every principal may issue: it is an
// AUTHENTICATION probe, and using anything that could also be refused on AUTHORIZATION grounds
// would conflate the two.
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
			// nothing to do with it. The decision is named rather than inlined because it is
			// the ONE error code propagation explains, and reading it as "is this still
			// propagating?" is what keeps a future reader from widening it to every SASL
			// failure — which would turn a genuinely unusable credential into a timeout.
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
// letters, digits, underscore and hyphen, first character alphanumeric — because the Kafka
// principal and the consumer group are derived from it byte for byte. A UUID's canonical
// form is already lowercase hex and hyphens, so it qualifies as-is; the label is lowercased
// and stripped of anything else so a caller cannot accidentally introduce a character the
// derivation refuses.
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

// eventIsolationAdminClient builds a kafka-go client authenticated as the ADMINISTRATOR.
//
// It exists for exactly one job the product's KafkaAdminClient has no reason to do: deleting the
// consumer groups this fixture's assertions create. Blnk never deletes a subscriber's groups —
// a group belongs to the consumer, not to the ledger — so adding the capability to
// event_admin.go to serve a test would put an operation in production code that production has
// no caller for. The raw client keeps it where it belongs.
//
// The transport mirrors eventIsolationClient's exactly, including the TLS posture, so an
// administrative operation cannot succeed or fail for a transport reason a subscriber operation
// would not have hit.
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
		// The FIRST attempt failing is logged rather than failed, because there is a second
		// one and a principal removed by the fallback has not leaked.
		t.Logf("teardown: deregistering subscriber %q failed, falling back to a direct broker "+
			"revocation (not yet a leak): %v", subscriber.SubscriberID, err)

		// Fall back to a direct broker revocation. The registry row is local and
		// disposable; the broker credential is not, so it gets a second attempt.
		if revokeErr := fixture.admin.RevokeSubscriber(ctx, subscriber); revokeErr != nil {
			// BOTH attempts failed, so a SCRAM credential and its ACLs are left on a broker
			// every other test in this package shares, with no registry row recording them.
			// That is a leak this test caused, and it must not be able to report success:
			// the next isolation run asserts that a principal outside its grant cannot read,
			// and an abandoned principal with live ACLs is exactly what makes that assertion
			// pass or fail for the wrong reason.
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
	// credential list catches up a moment later, so reading it immediately reported a LEAK in
	// the same run whose own log said "subscriber SCRAM credential revoked" — an accusation
	// against the product for something that had not happened. An independent
	// kafka-configs --describe afterwards showed nothing leaked.
	//
	// The polarity matters: this waits for the credential to be GONE and reports a leak only if
	// it is still there when the window elapses. A credential that really did leak is therefore
	// still reported, just one settle window later.
	exists, err := eventIsolationAwaitCredentialRevoked(ctx, fixture, subscriber.KafkaPrincipal)
	if err != nil {
		// Being UNABLE TO VERIFY is not the same as being clean, and it must not be reported as
		// clean. If the credential list cannot be read, this run cannot say whether it left a
		// principal on the shared broker — and "we could not tell" is the state in which a leak
		// silently survives into the next run's isolation assertions.
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

// eventIsolationAwaitCredentialRevoked polls the broker until the principal holds no SCRAM
// credential, or the settle window elapses.
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

// eventIsolationDeleteSubscriberGroups removes every consumer group inside a subscriber's own
// namespace, and reports a leak if any survives.
//
// # Why the fixture owns this
//
// The assertions in this file COMMIT OFFSETS to prove a subscriber may use its own group
// namespace, and committing an offset to a simple consumer group CREATES that group at the
// broker. Teardown revoked the principal, its ACLs and its registry row but never the groups, so
// a broker accumulated a `<principal>.default` and a `<principal>.replay` per run — 42 after
// eight runs, and every one of them a durable entry in the metadata log.
//
// It matters for the same reason the file already argues about principals: a namespace left
// behind is state a later run can pass on. It is also simply the checkpoint's requirement that
// cleanup removes topics, groups AND rows.
//
// # Why it deletes by NAMESPACE rather than by a recorded list
//
// The namespace is derived from the subscriber id and the group terminator, and the subscriber
// id is unique per run, so the namespace contains exactly this run's groups and nothing else.
// Listing and filtering by it therefore also collects a group a FUTURE assertion in this file
// creates, which a hand-maintained list of two names would not — and a cleanup that silently
// stops covering new state is worse than none.
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
	// obligations: whether the delete succeeded, and whether the groups are gone. A broker that
	// refused the request but had already removed the groups is clean, and a request that
	// returned without error but left one is not.
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

// eventIsolationTLSConfig mirrors the transport security posture the service dials with.
//
// The verification client must probe the SAME listener under the SAME posture as the
// administrative client, or a refusal could be a TLS artefact rather than an authorization
// decision. The rule is the one config.KafkaConfig documents: plaintext only when TLS is
// off AND local development has been acknowledged.
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

// eventIsolationAwaitGroupCoordinator runs a consumer-group probe, retrying only while the
// broker says it has no coordinator to answer with.
//
// # Why retrying here cannot weaken any assertion
//
// The three retried codes are statements about the broker's own state, never about this
// principal's rights:
//
//   - [14] GroupLoadInProgress — the coordinator is loading its state.
//   - [15] GroupCoordinatorNotAvailable — __consumer_offsets has no leader yet, or does not
//     exist at all. On a freshly provisioned broker it does not: auto.create.topics.enable is
//     false and kafka-provision.sh creates only the eight blnk.* topics, so the internal topic
//     is materialised by the first client that needs group coordination.
//   - [16] NotCoordinatorForGroup — this broker is not the coordinator for this group.
//
// An authorization decision is [30] GroupAuthorizationFailed, which is NOT in that set and
// therefore returns on the first attempt, unchanged and immediately. So a refusal is still a
// refusal, an allow is still an allow, and the only thing that changes is that a cold broker
// gets the moment it needs to elect a coordinator instead of failing the criterion.
//
// # Why not materialise the coordinator during setup instead
//
// A setup-time warm-up would have to perform group activity as some principal, and doing it as
// the ADMINISTRATOR would create the internal topic on a broker where the test then asserts
// what a SUBSCRIBER can see. Retrying at the probe keeps the fixture's footprint to exactly the
// groups the assertions name.
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

// eventIsolationOffsetCommit commits an offset to a consumer group as the given principal.
//
// Committing is the write half of group access and is authorized separately from reading, so
// a grant that leaked it would let one subscriber move another's consumer position — a
// denial-of-service against a tenant that no topic-level check would catch. GenerationID is
// -1 and MemberID empty, which is the simple-consumer form and needs no group membership.
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

// eventIsolationVisibleTopics lists every topic the broker is willing to disclose to the
// given principal.
//
// Kafka filters a full metadata response by Describe authorization rather than refusing it,
// so an unauthorized principal receives a SHORTER LIST instead of an error. That makes this
// the right shape for the list-operation half of the criterion: the assertion is about what
// is absent from the answer, not about an error code.
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
		// THE SUBSCRIBER-FACING LIST, and asserting the other one was a defect in this test
		// rather than in the service.
		//
		// A credential names the addresses the SUBSCRIBER will dial, which are the broker's
		// external listener — KAFKA_SUBSCRIBER_BROKERS. KAFKA_BROKERS is what Blnk's own admin
		// client and relay dial, and inside a deployment those are internal names that do not
		// resolve for a subscriber; Kafka makes it worse, because a broker answers each client
		// with the advertised address of the listener the connection arrived on, so even a
		// reachable internal bootstrap hands back internal names for the partition leaders.
		// subscriberFacingBrokers therefore reports the external list whenever one is
		// configured, and falls back to the internal list with a warning when it is not — the
		// fallback keeps the endpoint usable on the eight required variables alone, and the
		// warning is what tells an operator with external subscribers to set the override.
		//
		// This assertion named fixture.env.kafka.Brokers, so it only held while the two lists
		// happened to be equal, and it failed the moment they were configured differently — the
		// exact split the separate variable exists for, and the one .env.example ships. The
		// failure was in the acceptance suite that guards subscriber isolation, which is the
		// worst place for a false alarm: it trains a reader to discount a red V-5 run.
		//
		// The fixture keeps the two values DIFFERENT on purpose (see
		// eventIsolationResolveEnvironment), so this assertion now fails if the service ever
		// reports the internal list — which is the property worth having covered.
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

		// The registry keeps a non-reversible reference and an issuance instant, and nothing that
		// could be authenticated with. Once the issuance result above is discarded there is no
		// route by which Blnk can produce the secret again; a lost password can only be replaced.
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

// TestEventIsolation_ASubscriberRecordingAKeyPrefixIsProvisionedAndDisclosed states the key-scope
// access model as a property of the RUNNING SYSTEM: the prefix is a consumer-side filtering
// contract, the broker-enforced boundary is unchanged by it, and neither the credential nor the
// row is withheld because of it.
//
// # What this replaced, and why
//
// This test used to assert the opposite: that a subscriber recording a partition_key_prefix was
// REFUSED a credential, that no credential existed at the broker, and that recording a prefix on
// a provisioned subscriber was refused too. The concern behind it was real — Kafka's authorizer
// has no message-key dimension, so a registry row carrying a prefix must never be readable as
// "the broker confines this subscriber to those keys" — and the remedy was the wrong one.
//
// A subscriber that is refused a credential CONSUMES NOTHING. That is not a narrower boundary,
// it is the absence of one, and it left the credential endpoint unable to do the thing it exists
// for while the access model defines a subscriber's boundary as its topics, its consumer group
// AND its partition-key prefix. So the refusal is gone — from issuance, from update and from the
// schema, in sql/1781248930.sql — and what replaces it is disclosure: the credential carries the
// prefix together with the statement that its enforcement is consumer-side.
//
// # What is asserted here, and why the broker is required for it
//
// Two claims, and only a real authorizer can settle either:
//
//  1. The prefix grants and withholds NOTHING at the broker. The principal reads its granted
//     topic in full — every partition, whatever the keys on it — which is what makes
//     "consumer_side" an honest label rather than a hedge.
//  2. Acceptance criterion V-5 still holds for this principal. A key-scoped subscriber is refused
//     every topic, dead-letter topic, listing and consumer group outside its grant exactly as a
//     subscriber with no prefix is, because the two broker-enforced dimensions are untouched by
//     any of this. This is the assertion that would break if a future change tried to "honour"
//     the prefix by binding a PREFIXED topic pattern, which would widen the grant to every topic
//     sharing that prefix while appearing to narrow it.
func TestEventIsolation_ASubscriberRecordingAKeyPrefixIsProvisionedAndDisclosed(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, withheld := eventIsolationGrantSplit(fixture)
	keyPrefix := "ldg_" + uuid.NewString()

	// Provisioned through the SHARED helper, with the key prefix supplied. That the helper's
	// require.NoError on issuance now passes is itself the headline result: the same call
	// against the previous behaviour failed there.
	principal := eventIsolationProvision(t, ctx, fixture, "keyscope", []string{granted}, keyPrefix)

	require.True(t, principal.subscriber.DeclaresKeyScope(),
		"the fixture must actually be in the state under test")
	require.Equal(t, model.KeyScopeEnforcementConsumerSide,
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
		assert.Equal(t, model.KeyScopeEnforcementConsumerSide,
			principal.credential.KeyScopeEnforcement,
			"and it must be told WHERE that scope is enforced, or the prefix reads as a broker boundary")

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

	t.Run("the key scope withholds nothing at the broker", func(t *testing.T) {
		// THE HONEST HALF. The credential reads the granted topic in FULL: both offset ends
		// of the partition it probes are disclosed, and the broker never consults a key.
		// This is what consumer-side enforcement means, and it is asserted rather than
		// documented because a subscriber that assumed otherwise would build a tenancy
		// boundary on it.
		outcome, disclosed := eventIsolationListOffsets(ctx, principal.client, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading the granted topic %q as a key-scoped principal", granted),
			outcome.any())
		assert.NotEmpty(t, disclosed,
			"a key-scoped credential must read its granted topic exactly as an unscoped one does; "+
				"the prefix is not an ACL and nothing at the broker evaluates it")
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
		// THE REVERSE ORDER, which used to be refused as the second half of the same policy.
		// An operator who decides on a key scope after the credential exists must be able to
		// record that decision; refusing left them with nowhere to put it and no route to the
		// state except revoking a working credential, destroying a secret that cannot be
		// recovered in response to a request that never mentioned revocation.
		//
		// What must NOT change is the boundary, and that is what this asserts against the
		// broker: same credential, same allowed read, same refusals.
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

// subscriberIDOf WAS RETIRED HERE. It was a nil-safe accessor for a principal's subscriber id, and
// the fixture that mints a principal requires both the principal and its subscriber to be non-nil
// before returning it — so every one of the fifteen sites that reads the field reads it directly,
// and a nil-tolerant accessor beside them suggested a nil is reachable when it is not.
// TestEventIsolation_NarrowingASubscribersGrantWithdrawsItAtTheBroker is AUTH-02 against a real
// broker.
//
// # The defect
//
// Provisioning CREATED bindings and removed none, so a subscriber's broker-side grant was the
// union of every authorization it had ever held. Narrowing authorized_topics updated the
// registry and left the dropped topic's Read and Describe bindings live: the registry said the
// access was gone, the subscriber kept consuming, and no request failed to say otherwise.
//
// This is the only place that property can be proven for what it is. The admin-tier tests assert
// that the right ACL requests are sent; only a real authorizer can answer whether the withdrawn
// topic has actually become unreachable to a credential that could read it a moment earlier.
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

// TestEventIsolation_DeregistrationRevokesBeforeItForgetsThePrincipal is AUTH-01 against a real
// broker.
//
// Deregistration used to delete the registry row and then revoke, so a failed revocation left a
// principal that kept authenticating and kept reading — with the only record of WHICH principal
// that was having just been deleted. Here the whole sequence runs against a real broker and the
// end state is checked from the broker's side: the credential is gone, and nothing can be read
// with it.
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

// requireFenceLocked reproduces the repository's ownership predicate for a fenced write.
//
// The real writes carry `AND provisioning_token = $n AND provisioning_until > NOW()` inside the
// statement, so a caller whose lease lapsed is refused rather than racing. A fake that ignored
// the token would let every fence test pass while the production predicate was absent, which is
// the one thing these tests exist to rule out.
//
// The caller must already hold s.mu.
//
// Parameters:
//   - subscriberID string: the row the write targets.
//   - token string: the claim token the write carried.
//   - operation string: named in the lost-fence error, matching the repository's wording.
//
// Returns:
//   - error: ErrInvalidInput for a blank token, the lost-fence conflict when the claim is not
//     the caller's, nil otherwise.
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
		// reported a message the repository no longer returns would pass here and fail against
		// the database. The release and renewal statements answer with the same sentence up to
		// its final clause, so this one string serves all three sites.
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

// eventIsolationIsCredentialPropagating reports whether err is the one failure a credential that
// has just been written to the metadata log explains on its own.
//
// [58] SASL Authentication Failed is that failure and the only one. AlterUserScramCredentials
// returns when the credential is in the log, and the broker's authenticator loads it a moment
// later, so a handshake in that gap is refused with 58 by a broker that is working correctly.
//
// Nothing else belongs here, and the exclusions are the point rather than an omission:
//
//   - A transport error, a dial failure or a timeout says the broker could not be reached. The
//     fixture already proved it could be reached at setup, so this is an environment fault and
//     must be reported, not waited out.
//   - An authorization refusal says the credential authenticated and the request was denied. That
//     is a settled credential and a different question, and the probe is chosen so it cannot
//     happen.
//
// Widening this set is how a wait stops proving anything: whatever is retried here is, by
// construction, an outcome the caller will never be told about.
func eventIsolationIsCredentialPropagating(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, kafka.SASLAuthenticationFailed)
}

// TestEventIsolation_AKeyScopedSubscriberIsRefusedAndUnprovisionable IS GONE, and its
// absence is the point.
//
// It asserted that recording a partition_key_prefix was refused with 409
// SUBSCRIBER_ISOLATION_UNENFORCEABLE and that such a row could never be provisioned. That
// refusal implemented no part of AAP R-7's third scope — it withheld the CREDENTIAL
// instead, so a key-scoped subscriber could not consume at all — and it has been replaced
// by issuing the scope and DISCLOSING that the broker does not enforce it. The typed code
// is retired rather than left unraisable.
//
// What replaces the coverage: TestEventIsolation_AKeyScopedSubscriberIsProvisionedAndTold
// (below, if present) and the api/model validation tests for the value itself, plus
// TestSubscribersAPI_RecordsAndReturnsTheKeyScope in the api package, which asserts the
// prefix comes back beside partition_key_prefix_enforced=false.

// TestEventIsolation_AKeyScopedSubscriberIsProvisionedAndItsKeyBoundaryIsNotEnforced is C-02
// against a REAL broker, and it is the finding's resolution stated as a property of the running
// system.
//
// # What used to be asserted here, and why it was wrong
//
// This test used to assert a REFUSAL: a subscriber recording a partition key prefix was
// unprovisionable, and the test proved that no credential existed at the broker for it. The
// reasoning was that a topic-level credential is wider than a key-scoped row appears to describe,
// so refusing was the only fail-closed answer.
//
// The reasoning skipped a step. A refusal is fail-closed only when a narrower grant exists to
// insist upon, and none does: Kafka's authorizer names five resource types — Topic, Group,
// Cluster, TransactionalId and DelegationToken — and not one is a message key, while a topic per
// key space is excluded outright. So the refusal did not withhold a wider credential pending a
// narrower one. It withheld the ONLY credential that can exist, permanently, for a state the
// registry is explicitly designed to hold — turning a mandatory endpoint into a dead end.
//
// # What is asserted instead
//
// Three things, in this order, and the third is the one that could not be faked:
//
//  1. The credential IS issued, DOES authenticate, and carries the recorded prefix, so the
//     mandatory capability exists for every registry state the schema can hold.
//  2. The dimensions Kafka DOES enforce still hold exactly as they do for every other
//     subscriber — the granted topic is readable, every withheld topic is refused, and the group
//     namespace is reserved. A key-scoped subscriber is not a weaker subscriber.
//  3. The credential is admitted to the RAW PARTITION STREAM of its granted topic from offset
//     zero. Its declared prefix is a freshly generated ledger identifier that no record on that
//     shared topic carries, so an allowed fetch from the beginning is the observable proof that
//     the broker applied no key narrowing whatever: the principal reads the partition, not a
//     key-filtered view of it.
//
// Point 3 is deliberately an authorization observation rather than a record-level one. Proving it
// by producing one in-prefix and one out-of-prefix record and reading both back would need Write
// access to a category topic that sibling test runs share, and this file's discipline is that no
// probe leaves records behind. The authorization answer is sufficient and stronger than it looks:
// a broker that filtered by key would have to refuse or truncate the fetch, and it does neither.
//
// It runs against the real broker because every one of the three is a claim about what the
// broker's authorizer does, and a double could only report what it was told.
func TestEventIsolation_AKeyScopedSubscriberIsProvisionedAndItsKeyBoundaryIsNotEnforced(t *testing.T) {
	fixture, ctx := eventIsolationSetup(t)

	granted, withheld := eventIsolationGrantSplit(fixture)
	subscriberID := eventIsolationSubscriberID("keyscope")

	// A prefix no record on the shared topic can carry. That is what makes point 3 above
	// conclusive: if the broker narrowed by key, the subscriber would see nothing at all.
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

	require.True(t, subscriber.RequiresClientSideKeyFiltering(),
		"the fixture must actually be in the state under test")

	credential, err := fixture.service.IssueSubscriberCredential(ctx, subscriberID)
	require.NoError(t, err,
		"A RECORDED KEY PREFIX MUST NOT WITHDRAW THE CREDENTIAL CAPABILITY. Kafka can express no "+
			"narrower grant, so refusing here does not defer issuance until a safer credential is "+
			"available — it withholds the only credential that can ever exist for this row")
	require.NotEmpty(t, credential.Password(), "a real secret is returned, exactly once")

	assert.Equal(t, keyPrefix, credential.PartitionKeyPrefix,
		"and it travels WITH the credential, so the response can state whose obligation applying "+
			"it is rather than leaving the holder to assume the broker did it")

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
		assert.True(t, stored.RequiresClientSideKeyFiltering(),
			"so every subsequent read of this subscriber declares the client-side obligation")
	})

	principalClient := eventIsolationClient(t, fixture.env, credential)

	// EMPIRICAL enforcement, before anything is concluded from a refusal below.
	eventIsolationRequireEnforcement(t, ctx, &eventIsolationPrincipal{
		credential: credential,
		client:     principalClient,
	}, withheld[0])

	t.Run("topic isolation is unaffected by the key scope", func(t *testing.T) {
		// The dimension Kafka DOES enforce must hold exactly as it does for a subscriber with no
		// prefix. A key-scoped subscriber that was quietly granted less — or more — would make
		// the prefix change a boundary it has no business changing.
		allowed, disclosed := eventIsolationListOffsets(ctx, principalClient, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading the granted topic %q as a key-scoped subscriber", granted),
			allowed.any())
		assert.NotEmpty(t, disclosed,
			"the granted topic must be readable: the prefix narrows records, not topics")

		for _, topic := range withheld {
			outcome, leaked := eventIsolationListOffsets(ctx, principalClient, topic)
			eventIsolationAssertReadDenied(t, topic, outcome)
			assert.Empty(t, leaked,
				"the broker disclosed partition offsets for %q, which is outside the grant", topic)
		}
	})

	t.Run("the key boundary is NOT enforced, and that is why the response declares it", func(t *testing.T) {
		// THE ASSERTION THAT MATTERS, and the reason the response's
		// client_side_key_filtering_required flag exists rather than a comment.
		//
		// keyPrefix is a fresh identifier, so no record on this shared topic carries it. A broker
		// enforcing the prefix would therefore have to refuse this fetch or return an empty view
		// of the partition. It does neither: the principal is admitted to the partition from
		// offset zero, which is every record on it regardless of key.
		outcome := eventIsolationFetch(ctx, principalClient, granted, 0)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf(
				"fetching %q from offset 0 with a credential whose row declares the unrelated key "+
					"prefix %q", granted, keyPrefix),
			outcome.any())

		assert.True(t, outcome.allowed(),
			"THE PREFIX IS NOT A BOUNDARY. This fetch reads the partition from the beginning while "+
				"the subscriber's row declares a prefix no record here carries, so the broker "+
				"plainly applies no key narrowing. Anything that reports this subscriber as "+
				"confined to its prefix — a response field, a registry projection or a runbook — "+
				"states a boundary that does not exist, which is exactly why issuance declares the "+
				"narrowing as the holder's own obligation instead")
	})

	// THE REVERSE ORDER: hold a credential, then record a prefix.
	//
	// This used to be refused, on the reasoning that it produced a row describing a boundary the
	// credential did not have. But the danger was never in the ROW — it was in a response that
	// echoed a prefix without saying who enforced it, and that is what changed. The grant itself
	// is identical either side of this update, because there is no key dimension for it to move.
	//
	// So what is asserted is that the update changes the REGISTRY and NOT the boundary: same
	// principal, same credential, same access a moment later. Only a real broker can answer that
	// last part.
	t.Run("recording a key prefix on a provisioned subscriber changes no boundary", func(t *testing.T) {
		before, beforeErr := fixture.admin.SubscriberCredentialExists(ctx, subscriber.KafkaPrincipal)
		require.NoError(t, beforeErr)
		require.True(t, before, "the premise is a subscriber that already holds a live credential")

		latePrefix := "ldg_late_" + uuid.NewString()
		updated, updateErr := fixture.service.UpdateSubscriber(ctx, subscriberID, SubscriberUpdate{
			PartitionKeyPrefix: &latePrefix,
		})
		require.NoError(t, updateErr,
			"recording a key prefix on a provisioned subscriber must be accepted: it changes the "+
				"registry's statement of intent and no broker-side grant, so refusing it protects "+
				"nothing and blocks an ordinary operator action")
		require.NotNil(t, updated.PartitionKeyPrefix)
		assert.Equal(t, latePrefix, *updated.PartitionKeyPrefix)

		stored, readErr := fixture.service.GetSubscriber(ctx, subscriberID)
		require.NoError(t, readErr)
		require.NotNil(t, stored.PartitionKeyPrefix,
			"the registry records what the operator asked for")
		assert.Equal(t, latePrefix, *stored.PartitionKeyPrefix)
		require.NotNil(t, stored.CredentialReference,
			"and the working credential is left alone: there is no wider access to take away")

		after, afterErr := fixture.admin.SubscriberCredentialExists(ctx, subscriber.KafkaPrincipal)
		require.NoError(t, afterErr)
		assert.True(t, after,
			"the update must leave the credential at the broker untouched")

		// And the access itself is unchanged: still allowed on the granted topic, still refused
		// on the withheld ones. An update that quietly widened or narrowed the boundary on the
		// strength of a field the broker cannot read would be its own defect.
		allowed, disclosed := eventIsolationListOffsets(ctx, principalClient, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading the granted topic %q after the late key-prefix update", granted),
			allowed.any())
		assert.NotEmpty(t, disclosed,
			"the credential must still read what it could read before the update")

		outcome, leaked := eventIsolationListOffsets(ctx, principalClient, withheld[0])
		eventIsolationAssertReadDenied(t, withheld[0], outcome)
		assert.Empty(t, leaked,
			"and must still be refused %q: recording a prefix widens nothing", withheld[0])
	})

	t.Run("clearing the key prefix removes the obligation and keeps the credential", func(t *testing.T) {
		// Clearing is how an operator accepts topic-level scope and stops the registry implying
		// otherwise. It no longer RESTORES provisionability — issuance never stopped working — so
		// what it must do is take the declared obligation away without disturbing the credential.
		cleared := ""
		updated, updateErr := fixture.service.UpdateSubscriber(ctx, subscriberID, SubscriberUpdate{
			PartitionKeyPrefix: &cleared,
		})
		require.NoError(t, updateErr,
			"clearing the prefix must never be refused; it is the documented way to make the row "+
				"describe the access that exists")
		require.Nil(t, updated.PartitionKeyPrefix,
			"a present empty string CLEARS the column rather than storing an empty prefix")
		assert.False(t, updated.RequiresClientSideKeyFiltering())
		require.NotNil(t, updated.CredentialReference,
			"and the credential survives the repair")

		reissued, issueErr := fixture.service.IssueSubscriberCredential(ctx, subscriberID)
		require.NoError(t, issueErr)
		require.NotEmpty(t, reissued.Password())
		assert.Empty(t, reissued.PartitionKeyPrefix,
			"and the reissued credential declares no obligation, because the row records none")

		// The reissued credential works on the topic it was granted, so the whole cycle —
		// register with a prefix, issue, record another, clear, reissue — is proven end to end
		// rather than only at the registry.
		client := eventIsolationClient(t, fixture.env, reissued)
		outcome, disclosed := eventIsolationListOffsets(ctx, client, granted)
		eventIsolationAssertAllowed(t,
			fmt.Sprintf("reading the granted topic %q after clearing the key prefix", granted),
			outcome.any())
		assert.NotEmpty(t, disclosed)
	})
}
