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

// Crash recovery for the transactional event outbox and its Kafka relay — acceptance
// the acceptance criterion, "no duplicate and no lost events after a mid-batch relay restart".
//
// # STATE THE GUARANTEE HONESTLY BEFORE READING AN ASSERTION
//
// THREE FACTS, and they are not the same fact:
//
//  1. THE OUTBOX GUARANTEES THE ROW SURVIVES, and event_id carries a unique index so one id is
//     recorded at most once. How the row and the ledger mutation relate depends on the capture:
//     an IN-TRANSACTION capture (PublishEventInTx, the atomic writers) commits both together, so
//     neither can exist without the other; the STANDALONE capture the domain call sites use
//     commits the row afterwards, so a crash between the two can leave a mutation with no event.
//     Recapture is idempotent only where the id is DERIVED from the mutation
//     (model.EventIdentityFor); the repeatable event types that take a random id would be
//     recorded again under a new id.
//  2. Kafka delivery is AT-LEAST-ONCE. The relay publishes, the broker acknowledges, and
//     only then is the row marked dispatched — see processRow, which documents that window
//     in the code. A relay that dies inside it has published the event without recording
//     that it did, so once the lease expires the row is re-claimed and PUBLISHED AGAIN.
//     That duplicate is correct behaviour, not a defect: the alternative ordering (mark
//     first, publish second) loses events instead, and nothing can recover a lost event.
//  3. The duplicate is therefore suppressed AT THE SUBSCRIBER, on event_id, which is the
//     documented idempotency key (model.LedgerEvent.EventID and docs/event-streaming.md
//     both state that obligation).
//
// So this file asserts NO LOSS, and NO DUPLICATE AT THE CONSUMER'S IDEMPOTENCY BOUNDARY — after
// de-duplicating on event_id. It deliberately does NOT assert that the broker received each
// message exactly once: that promise is not made, and such an assertion would fail on a CORRECT
// implementation the moment a crash landed inside the publish-to-mark window, which is the window
// this file exists to exercise. Please do not "strengthen" it into that.
//
// What is asserted instead, and what each assertion buys:
//
//   - Every seeded event reaches the transport at least once (no loss).
//   - De-duplicating the delivered event_ids reproduces the seeded set EXACTLY — no gaps
//     and no strangers (no loss, no spurious extras).
//   - Every seeded outbox row reaches a terminal state, dispatched or dead_lettered, with
//     its lease and claim token released. A row parked in `processing` behind a lease that
//     never expires, or holding attempts >= max_attempts with no dead-letter record, is
//     permanently unclaimable — an event that is neither delivered nor visible to an
//     operator — and that is the failure mode these tests are built to catch.
//   - A lease is honoured while it is live and released when it expires, in both
//     directions, because that lease IS the recovery mechanism.
//   - A batch that takes LONGER than its own lease keeps it, and no second instance
//     republishes those rows. That assertion is deliberately stronger than the ones above:
//     no crash is staged and no lease is allowed to lapse, so no event may be published
//     twice BEFORE any idempotency filtering at all.
//   - An event whose dead-letter write failed is retried until it is preserved. A spent
//     retry budget with no dead-letter record is the one failure with nothing further to
//     fall back to, so it is recovered rather than abandoned.
//   - attempts survives a restart: it neither resets (which would let a permanently
//     failing event retry for ever) nor jumps (which would dead-letter a healthy event).
//   - FOR UPDATE SKIP LOCKED lets a second relay instance make progress on other rows
//     while one row is held, without either instance processing the same row.
//
// # HOW TO RUN
//
// These tests are skipped by `go test -short ./...`, which is what `make test` and CI run,
// so the default suite stays green with no infrastructure at all. To run them:
//
//	docker compose --profile kafka up -d postgres kafka kafka-init  # broker, topics, DLTs, principals
//	go run ./cmd migrate up                          # creates blnk.event_outbox
//	go test -run 'TestEventRecovery' -count=1 .
//
// Environment: BLNK_DATA_SOURCE_DNS (the DSN holding blnk.event_outbox) and KAFKA_BROKERS both
// default to the local compose stack. The live-delivery test additionally needs the SCRAM pair
// KAFKA_SASL_ADMIN_USER and KAFKA_SASL_ADMIN_SECRET, which are never hardcoded here — it skips
// when the secret is absent, naming what is missing.
//
//	BLNK_DATA_SOURCE_DNS     PostgreSQL DSN holding blnk.event_outbox
//	                         (default postgres://postgres:password@localhost:5432/blnk?sslmode=disable)
//	KAFKA_BROKERS            broker list for the live-delivery test (default localhost:9092)
//	KAFKA_SASL_ADMIN_USER    administrative SCRAM principal — used ONLY to read topic end
//	                         offsets in the live-delivery test
//	KAFKA_SASL_ADMIN_SECRET  its secret. NEVER hardcoded here; the live-delivery test
//	                         skips when it is absent
//	KAFKA_SASL_USER          the PRODUCER principal the publisher authenticates as. Blnk
//	                         refuses to publish as the administrator, so this is required
//	                         in addition to the administrative pair
//	KAFKA_SASL_SECRET        its secret, likewise never hardcoded
//
// Each test names exactly what is missing when it skips, so a skip is a shopping list
// rather than a mystery.
//
// # WHAT IS NOT HERE
//
// Ordering is event_ordering_integration_test.go, subscriber isolation is
// event_isolation_integration_test.go, dual-delivery payload equality is
// event_dual_delivery_test.go and replay fidelity is
// event_replay_fidelity_test.go. This file asserts none of them, so a failure here
// points at recovery and nothing else.
package blnk

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/model"
)

// testSettlementLease held the production hand-off window of thirty seconds. It is RETIRED,
// because this file deliberately does not use that period: recoveryDeadLetterHandoffLease is the
// hand-off lease these tests pass, shortened to two seconds so that waiting for it to lapse is an
// assertion rather than a stall, and recoveryHandOffRaceLease is the ninety-second one used where
// the window must NOT lapse mid-assertion. Both say so in full below. Wiring a thirty-second
// constant in here would have quietly reversed those two decisions.

const (
	// recoveryEventType is a real event string from the catalogue, chosen because it routes
	// to blnk.transactions — a topic the provisioning script creates. An unrecognised type
	// would route to the internal system topic rather than the category under test, so the
	// live-delivery test would fail for a reason that has nothing to do with recovery.
	recoveryEventType = "transaction.applied"

	// recoveryDefaultPostgresDSN is the local compose stack's DSN, and it is the same
	// literal the rest of the integration suite uses (see lineage_integration_test.go) so
	// one running stack serves every test.
	recoveryDefaultPostgresDSN = "postgres://postgres:password@localhost:5432/blnk?sslmode=disable"

	// recoveryDefaultRedisDNS is never dialled. config.validateRequiredFields demands a
	// Redis DSN before it will apply defaults, and these tests build the relay's
	// collaborators directly rather than through NewBlnk, so nothing ever connects to it.
	recoveryDefaultRedisDNS = "localhost:6379"

	// recoveryDefaultBroker is the local compose stack's single KRaft broker.
	recoveryDefaultBroker = "localhost:9092"

	// recoveryProbeTimeout bounds the reachability probe. It is short on purpose: a skip
	// for missing infrastructure must be instant, not a wait.
	recoveryProbeTimeout = 750 * time.Millisecond

	// recoveryLease is the claim lease these tests give the relay, and it is deliberately
	// far shorter than the production default of 30 seconds.
	//
	// The lease IS the recovery latency: rows a dead relay was holding become claimable
	// again this long after it stopped. Thirty seconds is right for production and would
	// make every restart assertion here a thirty-second wait, so it is shortened rather
	// than worked around — the mechanism under test is unchanged, only its period.
	recoveryLease = 2 * time.Second

	// recoveryDeadLetterHandoffLease is the lease a terminal transition holds the row under
	// while its dead-letter write is owed. It is the same short period as recoveryLease and for
	// the same reason: it is the delay before the dead-letter repair pass may adopt a row whose
	// owner died mid-hand-off, so the production default of 30 seconds would turn an assertion
	// into a wait.
	//
	// It is DELIBERATELY NON-ZERO. A zero lease resolves to an instant that has already passed,
	// which would make the retained claim token advisory rather than exclusive and would let a
	// second worker dead-letter the same event.
	recoveryDeadLetterHandoffLease = 2 * time.Second

	// recoveryHandOffRaceLease is the hand-off lease used by the test that races the repair
	// pass against a dead-letter write in flight, and it is deliberately LONG.
	//
	// Every other lease here is short because the test is waiting for it to expire. This one is
	// the opposite: the assertion is that the window is CLOSED while it holds, so the window
	// must not be able to lapse mid-assertion. Ninety seconds is far longer than the handful of
	// database round trips between the hand-off and the last assertion, so a slow or loaded
	// host cannot turn a correct build into a failure.
	recoveryHandOffRaceLease = 90 * time.Second

	// recoveryHeldLease is the lease taken by tests that must OBSERVE a row while it is still
	// held — the "not re-claimable while the lease is live" side of the contract, and the
	// crash reproduced by publishing without marking. It is longer than recoveryLease so the
	// observation cannot race the expiry on a loaded machine, and it is the wait those tests
	// then pay to see the row come back.
	recoveryHeldLease = 5 * time.Second

	// recoveryNoExpiryLease is the production default, used by the two-relay test so that
	// nothing can expire while it runs. That is what makes "no row was processed by both" a
	// statement about FOR UPDATE SKIP LOCKED rather than about how fast the machine is.
	recoveryNoExpiryLease = 30 * time.Second

	// recoveryLeaseOverrun is how long a batch is deliberately held PAST its lease by the
	// renewal test, and it is a multiple of the lease rather than a duration in its own right
	// so the two cannot drift apart.
	//
	// Three leases is what makes the assertion unambiguous: a lease taken once and never
	// renewed would have expired twice over by the time it is read back, so a lease still
	// live at the end of the overrun was renewed. Nothing about the number is tuning.
	recoveryLeaseOverrun = 3 * recoveryLease

	// recoveryOversizedBatch is a claim limit larger than any backlog seeded here.
	//
	// It is what a test uses when it needs ONE claim, under ONE token, to hold everything it
	// seeded — regardless of how many other runs' rows share the table and sort ahead of
	// them. A batch sized to the backlog would take only part of it whenever a stranger row
	// was older, and the test would then be asserting about a fraction of its own rows.
	recoveryOversizedBatch = 200

	// recoverySpentBudget is the retry budget given to rows that must reach the dead-letter
	// hand-off. One attempt spends it, so the row gets there on its first publish failure
	// instead of after the production schedule's 1s + 2s + 4s + 8s of backoff.
	recoverySpentBudget = 1

	// recoveryPollInterval keeps the relay responsive without changing what it does.
	recoveryPollInterval = 100 * time.Millisecond

	// recoveryMaxAttempts matches config's RELAY_MAX_RETRY_ATTEMPTS default, so a row's own
	// budget is the production budget.
	recoveryMaxAttempts = 5

	// recoveryPublishesInFlight is how many publishes must be in flight when the crash lands.
	//
	// It is what turns "the interruption probably hit a publish" into "it did". Without it the
	// crash is a race between the test's cancel and the relay's next publish, so the in-flight
	// failure path — record the attempt, release the lease, schedule the retry — is exercised on
	// some runs and not others, and an assertion about it is then a flake rather than a check.
	// Four is comfortably under the relay's default concurrency of eight and under the rows left
	// in every batch these tests seed.
	recoveryPublishesInFlight = 4

	// recoveryDrainTimeout bounds the wait for the backlog to reach terminal states. It is
	// generous because a shared development database may be serving other work, and a
	// timeout here reports the exact outbox state rather than hanging.
	recoveryDrainTimeout = 90 * time.Second

	// recoveryGateTimeout bounds the wait for the relay to reach the interruption point.
	recoveryGateTimeout = 45 * time.Second

	// recoveryConsumeTimeout bounds a single read from the topic. Generous, because the first
	// read of a partition also dials the broker and negotiates SASL — but bounded, so a broker
	// that stops delivering fails with the partition and offset it stopped at rather than
	// hanging the suite.
	recoveryConsumeTimeout = 20 * time.Second

	// recoveryConsumerMaxBytes bounds one fetch. The envelope cap is 768 KiB, so this holds
	// several messages per fetch without an unbounded buffer.
	recoveryConsumerMaxBytes = 10 << 20

	// recoveryStateInterval is how often the outbox is re-read while waiting.
	recoveryStateInterval = 200 * time.Millisecond

	// recoveryGoroutineSettle is how long the goroutine count is allowed to fall back to
	// its baseline after teardown. Stop joins the relay's worker synchronously, but the
	// connection pool and OpenTelemetry may retire goroutines a moment later.
	recoveryGoroutineSettle = 5 * time.Second

	// recoveryGoroutineTolerance allows for connection-pool goroutines created while the
	// relay was running and not yet retired. A real leak is one goroutine per batch
	// worker over many batches, so it is orders of magnitude larger than this.
	recoveryGoroutineTolerance = 8
)

// The two transport refusals these tests inject. They are sentinels rather than fmt.Errorf
// calls so that an assertion can name the exact failure it expects to see travel all the way
// into a row's last_error and out again as a dead-letter message's error_reason — which is
// how the repair path is shown to report what ENDED the event's retry budget rather than
// describing the repair.
var (
	// errRecoveryTransportRefused is a retryable publish failure: the shape of a broker that
	// is reachable and not accepting writes.
	errRecoveryTransportRefused = errors.New("the recovery test's Kafka transport refused the publish")

	// errRecoveryDeadLetterRefused is a failed dead-letter WRITE, which is the failure that
	// strands a row at 'failed' with no dlt_topic — the limbo the repair pass exists to end.
	errRecoveryDeadLetterRefused = errors.New("the recovery test's dead-letter transport refused the write")
)

// ---------------------------------------------------------------------------
// Infrastructure guarding
// ---------------------------------------------------------------------------

// recoverySkipUnlessIntegration skips in short mode.
//
// `make test` runs `go test -short ./...` and CI depends on it passing with no broker and
// no database, so every test in this file must be invisible to it.
func recoverySkipUnlessIntegration(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip(
			"skipping the event outbox crash-recovery integration tests in short mode: they need a " +
				"live PostgreSQL with blnk.event_outbox migrated (and, for the live-delivery test, a " +
				"Kafka broker). Run them with: go test -run 'TestEventRecovery' -count=1 .",
		)
	}
}

// recoveryEnv reads an environment variable, falling back to the local stack's value.
func recoveryEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}

	return fallback
}

// recoveryPostgresDSN resolves the outbox database.
func recoveryPostgresDSN() string {
	return recoveryEnv("BLNK_DATA_SOURCE_DNS", recoveryDefaultPostgresDSN)
}

// recoveryBrokers resolves the broker list for the live-delivery test.
func recoveryBrokers() []string {
	raw := recoveryEnv("KAFKA_BROKERS", recoveryDefaultBroker)

	brokers := make([]string, 0, 2)
	for _, candidate := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}

	return brokers
}

// recoveryDatabaseAddress extracts host:port from a DSN so it can be probed cheaply.
//
// It returns an empty string for a DSN it cannot decompose — a libpq key/value string, for
// instance — rather than guessing. The caller then skips the probe and lets the real
// connection attempt report the problem, which is the honest outcome: a probe that cannot
// run must not be turned into a verdict.
func recoveryDatabaseAddress(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Host == "" {
		return ""
	}

	host := parsed.Host
	if !strings.Contains(host, ":") {
		host += ":5432"
	}

	return host
}

// recoveryRequireReachable skips the test unless a TCP connection to address succeeds,
// naming what is missing and how to supply it.
func recoveryRequireReachable(t *testing.T, what, address, remedy string) {
	t.Helper()

	if strings.TrimSpace(address) == "" {
		t.Skipf("skipping: no address is configured for %s. %s", what, remedy)
	}

	conn, err := net.DialTimeout("tcp", address, recoveryProbeTimeout)
	if err != nil {
		t.Skipf("skipping: %s is not reachable at %s (%v). %s", what, address, err, remedy)
	}

	if closeErr := conn.Close(); closeErr != nil {
		t.Logf("closing the %s reachability probe: %v", what, closeErr)
	}
}

// recoveryKafkaCredentials returns the SCRAM principal the live-delivery test authenticates
// with, and whether it is available.
//
// NOTHING IS DEFAULTED HERE. A secret literal in a committed test is a secret in the
// repository whatever it happens to unlock today, so the credential comes from the
// environment or the test skips.
func recoveryKafkaCredentials() (string, string, bool) {
	user := strings.TrimSpace(os.Getenv("KAFKA_SASL_ADMIN_USER"))
	secret := strings.TrimSpace(os.Getenv("KAFKA_SASL_ADMIN_SECRET"))

	return user, secret, user != "" && secret != ""
}

// recoveryProducerCredentials returns the SCRAM principal the PUBLISHER authenticates with,
// and whether it is available.
//
// This is a SECOND, DELIBERATELY DISTINCT principal from the administrative one above, and
// the separation is the point rather than an inconvenience. The administrative credential can
// create topics, alter SCRAM credentials and manage ACLs; steady-state publishing needs none
// of that, so Blnk refuses to publish as the administrator and requires a producer principal
// with Write and Describe on the Blnk-owned topics. A test that supplied only the admin pair
// would not be exercising a configuration any deployment is allowed to run.
//
// Nothing is defaulted, for the same reason as above: the local stack's producer pair is
// provisioned by scripts/kafka-provision.sh and read from the environment, never from a
// literal in a committed file.
func recoveryProducerCredentials() (string, string, bool) {
	user := strings.TrimSpace(os.Getenv("KAFKA_SASL_USER"))
	secret := strings.TrimSpace(os.Getenv("KAFKA_SASL_SECRET"))

	return user, secret, user != "" && secret != ""
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// recoveryConfiguration builds the configuration these tests run under.
//
// A nil kafka argument means "no brokers", which is the right shape for every test that
// injects its own publisher: with brokers configured, config additionally requires a
// webhook sunset date and a complete administrative SASL credential, and demanding either
// of a test that never opens a socket would be noise.
func recoveryConfiguration(dsn string, kafka *config.KafkaConfig) *config.Configuration {
	conf := &config.Configuration{
		ProjectName: "blnk-event-recovery-test",
		DataSource: config.DataSourceConfig{
			Dns: dsn,
		},
		Redis: config.RedisConfig{
			Dns: recoveryDefaultRedisDNS,
		},
		Kafka: config.KafkaConfig{
			TopicPrefix: DefaultTopicPrefix,
		},
		// The production retry schedule, unchanged: base 1s, doubling, capped at 30s, five
		// attempts. The interruption in these tests costs a row at most one attempt, so the
		// real schedule is affordable and the row states asserted are the production ones.
		Relay: config.RelayConfig{
			MaxRetryAttempts:   recoveryMaxAttempts,
			RetryBaseBackoffMS: 1000,
			RetryMaxBackoffMS:  30000,
		},
		// Left empty deliberately: EnqueueLegacyWebhookDelivery returns immediately when no
		// webhook URL is configured, so the legacy leg cannot fire even if a future change
		// altered the sunset decision. Dual delivery is event_dual_delivery_test.go's subject.
		Notification: config.Notification{},
	}

	if kafka != nil {
		conf.Kafka = *kafka
		// Required by config once brokers are present, and a PAST instant on purpose: the
		// sunset having passed is what keeps the relay's dual-delivery branch shut.
		conf.WebhookDeprecationSunsetDate = time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	}

	return conf
}

// recoveryStoreConfiguration publishes conf for the duration of one test and restores
// whatever was there before.
//
// The store is global state that TopicForEvent, the persistence layer's topic-ownership
// check and the sunset decision all read, so a test that left its own configuration behind
// would change the behaviour of whatever ran next.
//
// The read-back is not ceremony. config.MockConfig LOGS a validation failure and returns
// without storing anything, so a configuration this file got wrong would leave the previous
// one in place and the test would then exercise it — passing or failing for reasons that
// have nothing to do with the code under test. Asserting the store holds this very pointer
// converts that into an immediate, legible failure.
func recoveryStoreConfiguration(t *testing.T, conf *config.Configuration) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}

		config.ConfigStore.Store(&config.Configuration{})
	})

	config.MockConfig(conf)

	stored, err := config.Fetch()
	require.NoError(t, err, "the test configuration must be readable through config.Fetch")
	require.Same(t, conf, stored,
		"config.MockConfig swallows a validation failure, so the store must be verified: the test "+
			"configuration was rejected and the previous one is still in force")
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

// recoveryFixture owns everything one recovery test needs: the outbox database, the
// configuration in force, a run tag that keeps this run's rows separable from every other
// run's, and the cleanup that removes them again.
type recoveryFixture struct {
	t    *testing.T
	ds   *database.Datasource
	conf *config.Configuration

	// runID tags every row this run seeds. It is UUID-derived so repeated runs — and
	// concurrent runs against one shared development database — cannot collide, which is
	// what makes `-count=5` and a busy shared stack both safe.
	runID string

	// topic is where recoveryEventType routes under the configured prefix.
	topic string
}

// newRecoveryFixture prepares the outbox database, or skips naming what is missing.
//
// # Why the datasource is built here rather than through database.NewDataSource
//
// database.GetDBConnection memoises ONE datasource per process behind a sync.Once, so
// whichever test in this binary calls it first fixes the connection for all of them and any
// DSN passed later is silently ignored. This file therefore opens its own pool through the
// exported ConnectDB and wraps it in a Datasource value. The wrapped value is the same type
// production uses and every method exercised here reads only Conn, so nothing is faked —
// the test simply owns its connection and closes it deterministically.
func newRecoveryFixture(t *testing.T, kafka *config.KafkaConfig) *recoveryFixture {
	t.Helper()

	recoverySkipUnlessIntegration(t)

	dsn := recoveryPostgresDSN()
	if address := recoveryDatabaseAddress(dsn); address != "" {
		recoveryRequireReachable(t, "PostgreSQL", address,
			"Start it with `docker compose up -d postgres`, or point BLNK_DATA_SOURCE_DNS at a reachable database.")
	}

	conf := recoveryConfiguration(dsn, kafka)
	recoveryStoreConfiguration(t, conf)

	db, err := database.ConnectDB(conf.DataSource)
	if err != nil {
		t.Skipf("skipping: could not connect to the outbox database at %s (%v). "+
			"Start it with `docker compose up -d postgres`, or set BLNK_DATA_SOURCE_DNS.", dsn, err)
	}

	fixture := &recoveryFixture{
		t:     t,
		ds:    &database.Datasource{Conn: db},
		conf:  conf,
		runID: strings.ReplaceAll(uuid.NewString(), "-", "")[:12],
		topic: TopicForEvent(recoveryEventType),
	}

	// Registered before the row cleanup so that cleanup — which runs in reverse order —
	// still has a live pool to delete through.
	t.Cleanup(func() {
		if closeErr := fixture.ds.Close(); closeErr != nil {
			t.Logf("closing the recovery test connection pool: %v", closeErr)
		}
	})

	fixture.requireOutboxMigrated()

	// EXCLUSIVE USE OF blnk.event_outbox for the duration of this test. See
	// lockEventOutboxTier: this tier claims from the whole table and another package's live
	// tier retires the whole table, so the two cannot run at once.
	lockEventOutboxTier(t, fixture.ds.Conn)

	t.Cleanup(fixture.deleteSeededRows)

	t.Logf("event recovery run %s: outbox at %s, topic %s", fixture.runID, dsn, fixture.topic)

	return fixture
}

// ---------------------------------------------------------------------------
// Exclusive use of the shared blnk.event_outbox
//
// Two test tiers in two different PACKAGES work the same table, and neither can be scoped to
// its own rows:
//
//   - This tier and the ordering tier CLAIM. ClaimPendingEventOutbox takes the oldest pending
//     rows in the table, whoever wrote them, because that is what the relay does in production
//     and a claim narrowed to a test's own rows would not be the code under test.
//   - database/event_outbox_test.go's quiesceEventOutbox RETIRES every claim-visible row that
//     is not its own, and then asserts there are none, so that its claim assertions read a
//     table it controls.
//
// Run in the same process they would still collide, and they do not run in the same process:
// `go test ./...` runs one package's binary per CPU, so an in-process mutex is not even
// available. What was observed is exactly what the two descriptions predict — the root tier
// timing out claiming rows the database tier had retired ("ANOTHER PROCESS is draining or
// purging the same outbox", the diagnostic this tier already prints), and the database tier's
// straggler assertion tripping on a row the root tier inserted a moment after the retire. It
// failed 3/3 at default parallelism and passed 2/2 under -p 1, which is why CI only escapes it
// by happening to pass -p 1.
//
// A POSTGRES ADVISORY LOCK is the one mechanism that reaches across processes without changing
// the production claim query, and it is held in the same database whose table is being
// protected — so it cannot get out of step with what it guards. It serialises the tiers instead
// of letting them corrupt each other; the total time is what -p 1 already costs.
// ---------------------------------------------------------------------------

// eventOutboxTierLockKey identifies the advisory lock the live outbox tiers share.
//
// It is an arbitrary constant, and it MUST BE THE SAME VALUE in database/event_outbox_test.go —
// two different keys are two different locks and would protect nothing. Both sides name each
// other so a future change to one is not made without the other.
const eventOutboxTierLockKey int64 = 0x424c4e4b4f5542 // "BLNKOUB"

// eventOutboxTierLockBudget bounds how long a tier waits for the lock.
//
// It is longer than the slowest live test in either package and shorter than `go test`'s
// ten-minute default timeout, so a genuinely stuck holder is reported by NAME here rather than
// as a package-level timeout with no explanation.
const eventOutboxTierLockBudget = 8 * time.Minute

// eventOutboxTierLockPoll is how often the lock is re-attempted.
const eventOutboxTierLockPoll = 200 * time.Millisecond

// eventOutboxTierLock holds the process's side of the advisory lock.
//
// The DEDICATED CONNECTION is the load-bearing part. A Postgres advisory lock belongs to a
// SESSION, and database/sql hands out an arbitrary pooled connection per statement, so taking
// the lock on a pool would release it the moment that connection was recycled — a lock that
// looks held and is not. MaxOpenConns(1) pins one session for the lock's whole lifetime.
//
// The refcount makes acquisition REENTRANT within the process. Tests in a package are serial,
// so in practice one holder at a time — but a test that built two fixtures would otherwise take
// the lock twice on two sessions and deadlock against itself, which is a far worse failure than
// the one being fixed.
var eventOutboxTierLock struct {
	mu       sync.Mutex
	holders  int
	conn     *sql.DB
	acquired bool
}

// lockEventOutboxTier takes exclusive use of blnk.event_outbox until the test finishes.
//
// Parameters:
//   - t *testing.T: the test, for cleanup registration and for failing when the lock cannot be
//     taken.
//   - pool *sql.DB: a pool on the database holding the outbox; only its DSN is used, because
//     the lock needs a session of its own.
func lockEventOutboxTier(t *testing.T, pool *sql.DB) {
	t.Helper()

	require.NotNil(t, pool, "the outbox tier lock needs a live pool to derive its session from")

	eventOutboxTierLock.mu.Lock()
	defer eventOutboxTierLock.mu.Unlock()

	if eventOutboxTierLock.holders == 0 {
		acquireEventOutboxTierLock(t, pool)
	}

	eventOutboxTierLock.holders++

	t.Cleanup(releaseEventOutboxTierLock)
}

// acquireEventOutboxTierLock opens the lock session and blocks until the lock is held.
func acquireEventOutboxTierLock(t *testing.T, pool *sql.DB) {
	t.Helper()

	conf, err := config.Fetch()
	require.NoError(t, err, "the outbox tier lock needs the configured DSN")

	conn, err := sql.Open("postgres", conf.DataSource.Dns)
	require.NoError(t, err, "opening the outbox tier lock session")

	// ONE session, for the reason in the type's comment.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	conn.SetConnMaxLifetime(0)

	deadline := time.Now().Add(eventOutboxTierLockBudget)
	for {
		var held bool
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		queryErr := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`,
			eventOutboxTierLockKey).Scan(&held)
		cancel()

		if queryErr != nil {
			_ = conn.Close()
			require.NoError(t, queryErr, "taking the outbox tier advisory lock")
		}

		if held {
			break
		}

		if time.Now().After(deadline) {
			_ = conn.Close()
			t.Fatalf(
				"another live outbox tier has held the blnk.event_outbox advisory lock (%d) for %s. "+
					"The root and database live tiers claim from and retire the whole table, so they take "+
					"this lock to run one at a time; a holder this long means a test in another package "+
					"is stuck rather than slow",
				eventOutboxTierLockKey, eventOutboxTierLockBudget,
			)
		}

		time.Sleep(eventOutboxTierLockPoll)
	}

	eventOutboxTierLock.conn = conn
	eventOutboxTierLock.acquired = true
}

// releaseEventOutboxTierLock drops one hold and, when it was the last, the lock itself.
func releaseEventOutboxTierLock() {
	eventOutboxTierLock.mu.Lock()
	defer eventOutboxTierLock.mu.Unlock()

	if eventOutboxTierLock.holders == 0 {
		return
	}

	eventOutboxTierLock.holders--
	if eventOutboxTierLock.holders > 0 || !eventOutboxTierLock.acquired {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Unlocked explicitly AND the session closed. Closing alone would release it — a session
	// ending drops its advisory locks — but an explicit unlock releases it at a known instant
	// rather than whenever the pool decides to close the connection.
	if _, err := eventOutboxTierLock.conn.ExecContext(ctx,
		`SELECT pg_advisory_unlock($1)`, eventOutboxTierLockKey); err != nil {
		logrus.WithError(err).Warn("releasing the event outbox tier advisory lock")
	}

	if err := eventOutboxTierLock.conn.Close(); err != nil {
		logrus.WithError(err).Warn("closing the event outbox tier advisory lock session")
	}

	eventOutboxTierLock.conn = nil
	eventOutboxTierLock.acquired = false
}

// requireOutboxMigrated skips unless blnk.event_outbox exists, because "table does not
// exist" deserves the migration command rather than a scan error.
func (f *recoveryFixture) requireOutboxMigrated() {
	f.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var migrated bool
	if err := f.ds.Conn.QueryRowContext(ctx,
		`SELECT to_regclass('blnk.event_outbox') IS NOT NULL`).Scan(&migrated); err != nil {
		f.t.Skipf("skipping: could not inspect the outbox schema (%v). Apply migrations with `go run ./cmd migrate up`.", err)
	}

	if !migrated {
		f.t.Skip("skipping: blnk.event_outbox does not exist. Apply migrations with `go run ./cmd migrate up`.")
	}
}

// keyPrefix is the partition-key prefix every row of this run carries. It is how the run's
// rows are selected for inspection and cleanup without touching anybody else's.
func (f *recoveryFixture) keyPrefix() string {
	return "blnk-recovery-" + f.runID + "-"
}

// partitionKey is the key of the i-th seeded row.
//
// EVERY SEEDED ROW GETS ITS OWN KEY, which is required rather than tidy: the claim admits
// at most one row per partition key at a time — that is half of the ordering guarantee — so
// rows sharing a key would be claimed strictly one at a time and a "batch" of a hundred
// could never form. Ordering itself is asserted in event_ordering_integration_test.go.
func (f *recoveryFixture) partitionKey(index int) string {
	return fmt.Sprintf("%s%05d", f.keyPrefix(), index)
}

// seed inserts count claimable outbox rows carrying the production retry budget, and returns
// their event ids in insertion order.
func (f *recoveryFixture) seed(ctx context.Context, count int) []string {
	f.t.Helper()

	return f.seedWithBudget(ctx, count, recoveryMaxAttempts)
}

// seedWithBudget inserts count claimable outbox rows whose retry budget is maxAttempts, and
// returns their event ids in insertion order.
//
// The rows are inserted through the production repository method, so they carry exactly the
// shape the capture path produces: a real legacy webhook envelope as the payload, a
// Blnk-owned topic, schema version 1, and the run's own partition key.
//
// The budget is a parameter because a test about what happens AFTER the budget is spent should
// not pay the production schedule's 1s + 2s + 4s + 8s of backoff to get there. Nothing else
// about the row changes, so the state the row arrives in is the production state.
func (f *recoveryFixture) seedWithBudget(ctx context.Context, count, maxAttempts int) []string {
	f.t.Helper()

	occurredAt := time.Now().UTC()
	eventIDs := make([]string, 0, count)

	for index := 0; index < count; index++ {
		key := f.partitionKey(index)

		payload, err := json.Marshal(NewWebhook{
			Event: recoveryEventType,
			Payload: map[string]interface{}{
				"transaction_id": key,
				"status":         "APPLIED",
				"amount":         1250,
				"currency":       "USD",
			},
		})
		require.NoError(f.t, err, "marshalling the seeded webhook envelope")

		row := &model.EventOutbox{
			EventID:       model.NewEventID(),
			EventType:     recoveryEventType,
			AggregateID:   key,
			PartitionKey:  key,
			LedgerID:      key,
			Topic:         f.topic,
			SchemaVersion: model.SchemaVersionV1,
			Payload:       payload,
			// Ascending by one millisecond so the claim's FIFO ordering has something to be
			// FIFO about, and so a diagnostic reads in insertion order.
			OccurredAt:  occurredAt.Add(time.Duration(index) * time.Millisecond),
			MaxAttempts: maxAttempts,
		}

		require.NoErrorf(f.t, f.ds.InsertEventOutbox(ctx, row), "seeding outbox row %d", index)
		require.NotZerof(f.t, row.ID, "seeded outbox row %d must come back with its surrogate key", index)

		eventIDs = append(eventIDs, row.EventID)
	}

	f.t.Logf("seeded %d event outbox rows for run %s", len(eventIDs), f.runID)

	return eventIDs
}

// deleteSeededRows removes this run's rows.
//
// Terminal rows would be harmless to leave, but non-terminal ones would not: a row left
// pending is claimable for ever, so the next relay any test starts would pick it up. The
// delete is scoped by the run's partition-key prefix, so no other run's rows are touched —
// and no topic is deleted, because sibling tests share the broker.
func (f *recoveryFixture) deleteSeededRows() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := f.ds.Conn.ExecContext(ctx,
		`DELETE FROM blnk.event_outbox WHERE starts_with(partition_key, $1)`, f.keyPrefix())
	if err != nil {
		// FAILS the test. This file's own comment above states why the rows matter — a row left
		// pending is claimable for ever, so the next relay any test starts picks it up — and a
		// logged line does not stop the run that caused it from reporting success.
		f.t.Errorf("could not clean up the seeded outbox rows of run %s: %v", f.runID, err)
	} else if affected, countErr := result.RowsAffected(); countErr != nil {
		f.t.Logf("cleaned up the seeded outbox rows of run %s (count unavailable: %v)",
			f.runID, countErr)
	} else {
		f.t.Logf("cleaned up %d seeded outbox rows for run %s", affected, f.runID)
	}

	f.verifySeededRowsGone()
}

// verifySeededRowsGone asserts this run left no outbox rows behind.
//
// Checked even when the delete errored, because "the statement succeeded" and "the table is
// clean" are different claims and only the second one matters to the next test.
func (f *recoveryFixture) verifySeededRowsGone() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var remaining int
	if err := f.ds.Conn.QueryRowContext(ctx,
		`SELECT count(*) FROM blnk.event_outbox WHERE starts_with(partition_key, $1)`,
		f.keyPrefix(),
	).Scan(&remaining); err != nil {
		f.t.Errorf("could not confirm the seeded outbox rows of run %s were removed: %v",
			f.runID, err)

		return
	}

	assert.Zerof(f.t, remaining,
		"recovery run %s left %d seeded outbox rows behind under key prefix %q. A pending row "+
			"stays claimable, so the next relay to start publishes it and some later test's "+
			"assertions are made against rows it never seeded.",
		f.runID, remaining, f.keyPrefix())
}

// relay builds a relay over this fixture's database and the supplied publisher.
//
// The instance is assembled in-package rather than through NewBlnk because the relay needs
// exactly three things from the container — configuration, the datasource and the publisher
// — and building the rest would drag in Redis and asynq for no assertion. The sunset
// decision is pinned so the dual-delivery branch is never taken; the configuration also
// carries a past sunset date and no webhook URL, so the legacy leg is shut twice over.
func (f *recoveryFixture) relay(publisher TopicEventPublisher) *EventRelayProcessor {
	f.t.Helper()

	processor := NewEventRelayProcessor(&Blnk{
		config:     f.conf,
		datasource: f.ds,
		events:     publisher,
	})
	// The window is pinned CLOSED so the dual-delivery branch is never taken, and closed
	// rather than merely absent so the relay is still startable: a window that has not
	// opened yet is a misconfiguration the startup obstacle refuses.
	processor.dualDeliveryActive = func(time.Time) bool { return false }
	processor.windowState = func(time.Time) WebhookWindowState { return WebhookWindowClosed }

	require.NoError(f.t, processor.startupObstacle(),
		"the relay must be startable: this is the assembly, not the behaviour under test")

	return processor.
		WithPollInterval(recoveryPollInterval).
		WithLockDuration(recoveryLease)
}

// ---------------------------------------------------------------------------
// Outbox state inspection
// ---------------------------------------------------------------------------

// recoveryRowState is one seeded row's relay-state machine, read back from the database.
//
// Every column is projected as a NON-NULL scalar — `locked_until IS NOT NULL`, COALESCE and
// so on — so the scan needs no nullable wrappers and each field answers one question
// directly: is a lease held, is it still live, is a claim token outstanding, was the row
// dispatched, is there a dead-letter record.
type recoveryRowState struct {
	eventID      string
	status       string
	attempts     int
	maxAttempts  int
	leaseHeld    bool
	leaseLive    bool
	claimToken   string
	dispatched   bool
	dltTopic     string
	lastError    string
	retryPending bool
}

// terminal reports whether the row has finished: dispatched, or preserved on its
// dead-letter topic.
func (s recoveryRowState) terminal() bool {
	return model.IsTerminalEventOutboxStatus(s.status)
}

// unclaimable reports the failure mode this file exists to catch: a row that can never be
// claimed again and never reached a terminal state, so its event is neither delivered nor
// visible in the dead-letter inventory.
//
// Two shapes qualify, and both are silent in production:
//
//   - a live lease over a non-terminal row once every relay has stopped — nothing will
//     release it except the clock, and if locked_until were never advanced, nothing at all;
//   - a spent retry budget (attempts >= max_attempts) with no dead-letter record, which the
//     claim predicate excludes for ever because it admits only attempts < max_attempts.
func (s recoveryRowState) unclaimable() bool {
	if s.terminal() {
		return false
	}

	return s.leaseLive || s.attempts >= s.maxAttempts
}

// recoveryOutboxStateQuery reads this run's rows in claim order.
const recoveryOutboxStateQuery = `
	SELECT event_id,
	       status,
	       attempts,
	       max_attempts,
	       locked_until IS NOT NULL             AS lease_held,
	       COALESCE(locked_until > NOW(), FALSE) AS lease_live,
	       COALESCE(claim_token, '')             AS claim_token,
	       dispatched_at IS NOT NULL             AS dispatched,
	       COALESCE(dlt_topic, '')               AS dlt_topic,
	       COALESCE(last_error, '')              AS last_error,
	       next_attempt_at > NOW()               AS retry_pending
	FROM blnk.event_outbox
	WHERE starts_with(partition_key, $1)
	ORDER BY occurred_at ASC, id ASC
`

// snapshot reads the current state of every row this run seeded.
//
// It is ONE query rather than one per event, because it is called in a polling loop and a
// hundred and fifty round trips per poll would make the test's own latency the thing being
// measured.
func (f *recoveryFixture) snapshot(ctx context.Context) []recoveryRowState {
	f.t.Helper()

	rows, err := f.ds.Conn.QueryContext(ctx, recoveryOutboxStateQuery, f.keyPrefix())
	require.NoError(f.t, err, "reading back the seeded outbox rows")

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			f.t.Logf("closing the outbox state cursor: %v", closeErr)
		}
	}()

	states := make([]recoveryRowState, 0, 64)
	for rows.Next() {
		var state recoveryRowState
		require.NoError(f.t, rows.Scan(
			&state.eventID,
			&state.status,
			&state.attempts,
			&state.maxAttempts,
			&state.leaseHeld,
			&state.leaseLive,
			&state.claimToken,
			&state.dispatched,
			&state.dltTopic,
			&state.lastError,
			&state.retryPending,
		), "scanning an outbox row state")

		states = append(states, state)
	}

	require.NoError(f.t, rows.Err(), "iterating the seeded outbox rows")

	return states
}

// recoveryLeaseState is one claimed row's lease as the database records it: who holds it,
// until when, and whether it is still live.
//
// It exists alongside recoveryRowState because the two answer different questions.
// recoveryRowState asks "is a lease held, and is it live", which is all the terminal-state
// contract needs. A RENEWAL, though, is only observable as the expiry MOVING FORWARD under an
// unchanged claim token — so the timestamp itself, and the token it belongs to, are what a
// renewal assertion has to read.
type recoveryLeaseState struct {
	eventID    string
	status     string
	claimToken string
	expiry     time.Time
	live       bool
}

// recoveryHeldLeaseQuery reads this run's rows that some worker currently holds a claim on.
//
// claim_token IS NOT NULL is the definition of "held": every terminal transition clears the
// token, and the ordinary claim stamps it, so the predicate selects exactly the rows a batch
// still owns — which is exactly the set a heartbeat is renewing.
const recoveryHeldLeaseQuery = `
	SELECT event_id,
	       status,
	       claim_token,
	       COALESCE(locked_until, TIMESTAMPTZ 'epoch') AS lease_expiry,
	       COALESCE(locked_until > NOW(), FALSE)       AS lease_live
	FROM blnk.event_outbox
	WHERE starts_with(partition_key, $1)
	  AND claim_token IS NOT NULL
	ORDER BY occurred_at ASC, id ASC
`

// heldLeases returns this run's currently claimed rows, keyed by event id.
func (f *recoveryFixture) heldLeases(ctx context.Context) map[string]recoveryLeaseState {
	f.t.Helper()

	rows, err := f.ds.Conn.QueryContext(ctx, recoveryHeldLeaseQuery, f.keyPrefix())
	require.NoError(f.t, err, "reading back the leases held over the seeded outbox rows")

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			f.t.Logf("closing the outbox lease cursor: %v", closeErr)
		}
	}()

	leases := make(map[string]recoveryLeaseState, 64)
	for rows.Next() {
		var lease recoveryLeaseState
		require.NoError(f.t, rows.Scan(
			&lease.eventID,
			&lease.status,
			&lease.claimToken,
			&lease.expiry,
			&lease.live,
		), "scanning an outbox lease")

		leases[lease.eventID] = lease
	}

	require.NoError(f.t, rows.Err(), "iterating the leases held over the seeded outbox rows")

	return leases
}

// recoveryClaimTokens returns the distinct claim tokens across a set of held leases.
//
// One token means one claim, which is what "the whole backlog is held by a single batch"
// reduces to — and what makes a later comparison against the same token meaningful.
func recoveryClaimTokens(leases map[string]recoveryLeaseState) []string {
	seen := make(map[string]struct{}, len(leases))
	tokens := make([]string, 0, len(leases))

	for _, lease := range leases {
		if _, ok := seen[lease.claimToken]; ok {
			continue
		}

		seen[lease.claimToken] = struct{}{}
		tokens = append(tokens, lease.claimToken)
	}

	return tokens
}

// recoveryDescribeStates renders a compact diagnostic: how many rows sit in each status, and
// a few examples of the ones that have not finished. It is what turns a timeout from "it
// hung" into "forty rows are still processing behind a live lease".
func recoveryDescribeStates(states []recoveryRowState) string {
	byStatus := map[string]int{}
	examples := make([]string, 0, 3)

	for _, state := range states {
		byStatus[state.status]++

		if state.terminal() || len(examples) == cap(examples) {
			continue
		}

		examples = append(examples, fmt.Sprintf(
			"{event %s status=%s attempts=%d/%d lease_held=%t lease_live=%t token=%t retry_pending=%t last_error=%q}",
			state.eventID, state.status, state.attempts, state.maxAttempts,
			state.leaseHeld, state.leaseLive, state.claimToken != "", state.retryPending, state.lastError))
	}

	summary := fmt.Sprintf("%d rows %v", len(states), byStatus)
	if len(examples) == 0 {
		return summary
	}

	return summary + "; unfinished: " + strings.Join(examples, ", ")
}

// waitForTerminalStates blocks until every seeded row has finished, and fails with the exact
// outbox state — never a bare timeout — when it does not.
func (f *recoveryFixture) waitForTerminalStates(ctx context.Context, expected int, timeout time.Duration) []recoveryRowState {
	f.t.Helper()

	deadline := time.Now().Add(timeout)

	for {
		states := f.snapshot(ctx)

		terminal := 0
		for _, state := range states {
			if state.terminal() {
				terminal++
			}
		}

		if len(states) == expected && terminal == expected {
			return states
		}

		if time.Now().After(deadline) {
			f.t.Fatalf("timed out after %s waiting for the %d seeded events of run %s to reach a terminal "+
				"state (dispatched or dead_lettered): %s",
				timeout, expected, f.runID, recoveryDescribeStates(states))

			return states
		}

		time.Sleep(recoveryStateInterval)
	}
}

// awaitRow blocks until one seeded row satisfies a predicate, and returns the state that did.
//
// It reports the whole run's outbox state on a timeout rather than a bare deadline, for the
// same reason waitForTerminalStates does: "the row never reached that shape" is not actionable,
// whereas "it is still processing behind a live lease" is. The description names the shape in
// the failure message, so a timeout reads as a sentence.
//
// Parameters:
//   - ctx context.Context: cancels the reads.
//   - eventID string: the row to watch.
//   - satisfied func(recoveryRowState) bool: the shape being waited for.
//   - timeout time.Duration: how long to wait.
//   - description string: what the shape means, for the failure message.
//
// Returns:
//   - recoveryRowState: the state that satisfied the predicate.
func (f *recoveryFixture) awaitRow(
	ctx context.Context,
	eventID string,
	satisfied func(recoveryRowState) bool,
	timeout time.Duration,
	description string,
) recoveryRowState {
	f.t.Helper()

	deadline := time.Now().Add(timeout)

	for {
		states := f.snapshot(ctx)

		for _, state := range states {
			if state.eventID == eventID && satisfied(state) {
				return state
			}
		}

		if time.Now().After(deadline) {
			f.t.Fatalf("timed out after %s waiting for event %s of run %s to %s: %s",
				timeout, eventID, f.runID, description, recoveryDescribeStates(states))

			return recoveryRowState{}
		}

		time.Sleep(recoveryStateInterval)
	}
}

// assertEveryRowFinishedCleanly is the terminal-state contract, asserted row by row.
func (f *recoveryFixture) assertEveryRowFinishedCleanly(states []recoveryRowState, expected int) {
	f.t.Helper()

	f.assertEveryRowReleasedItsClaim(states, expected)

	dispatched := 0
	for _, state := range states {
		if state.status == model.EventOutboxStatusDispatched {
			dispatched++
		}
	}

	assert.Equalf(f.t, expected, dispatched,
		"every seeded event must end up dispatched: the publisher only ever failed while the relay's "+
			"context was cancelled, and one cancellation cannot spend a five-attempt budget. %s",
		recoveryDescribeStates(states))
}

// assertEveryRowWasPreserved is assertEveryRowFinishedCleanly's dead-letter counterpart: the
// same released-claim contract, for a run whose events were GIVEN UP ON rather than delivered.
//
// The two are separate assertions rather than one lenient one on purpose. "Every row finished"
// is far too weak a statement for either test to rest on: a run that should have dispatched
// everything and dead-lettered one row instead has lost an event as surely as if the row had
// been deleted, and a run whose dead-letter write was supposed to be repaired has proved
// nothing if the row merely dispatched.
//
// Parameters:
//   - states []recoveryRowState: the rows read back after the run.
//   - expected int: how many rows the run seeded.
//   - dltTopic string: the dead-letter topic every row must have been preserved on.
func (f *recoveryFixture) assertEveryRowWasPreserved(
	states []recoveryRowState,
	expected int,
	dltTopic string,
) {
	f.t.Helper()

	f.assertEveryRowReleasedItsClaim(states, expected)

	for _, state := range states {
		assert.Equalf(f.t, model.EventOutboxStatusDeadLettered, state.status,
			"event %s must have been preserved on its dead-letter topic, got status %q. %s",
			state.eventID, state.status, recoveryDescribeStates(states))

		assert.Equalf(f.t, dltTopic, state.dltTopic,
			"event %s must record the dead-letter topic it was preserved on", state.eventID)
	}
}

// assertEveryRowReleasedItsClaim is the terminal-state contract both of the above rest on:
// every seeded row finished, none of them finished in a shape nothing can act on again, and
// none of them is still holding the lease or the token it worked under.
//
// Parameters:
//   - states []recoveryRowState: the rows read back after the run.
//   - expected int: how many rows the run seeded.
func (f *recoveryFixture) assertEveryRowReleasedItsClaim(states []recoveryRowState, expected int) {
	f.t.Helper()

	require.Len(f.t, states, expected, "every seeded row must still be present in the outbox")

	for _, state := range states {
		assert.Truef(f.t, state.terminal(),
			"event %s must have finished, got status %q (attempts %d/%d, lease_live=%t)",
			state.eventID, state.status, state.attempts, state.maxAttempts, state.leaseLive)

		assert.Falsef(f.t, state.unclaimable(),
			"event %s is permanently unclaimable: status=%q attempts=%d/%d lease_live=%t dlt_topic=%q. "+
				"Such a row is neither delivered nor visible to an operator",
			state.eventID, state.status, state.attempts, state.maxAttempts, state.leaseLive, state.dltTopic)

		assert.Falsef(f.t, state.leaseHeld,
			"event %s reached %q but still holds a lease; MarkEventDispatched must clear locked_until",
			state.eventID, state.status)
		assert.Emptyf(f.t, state.claimToken,
			"event %s reached %q but still carries a claim token; a terminal row is nobody's to hold",
			state.eventID, state.status)

		if state.status == model.EventOutboxStatusDispatched {
			assert.Truef(f.t, state.dispatched,
				"event %s is dispatched, so dispatched_at must be stamped", state.eventID)
		}

		if state.status == model.EventOutboxStatusDeadLettered {
			assert.NotEmptyf(f.t, state.dltTopic,
				"event %s is dead-lettered, so the dead-letter topic it was preserved on must be recorded",
				state.eventID)
		}
	}
}

// mine keeps only the rows this run seeded.
//
// A relay is process-global: it claims whatever is due, so a claim on a shared development
// database legitimately returns other runs' rows as well. Filtering here is what keeps every
// assertion about this run and this run only.
func (f *recoveryFixture) mine(rows []model.EventOutbox) []model.EventOutbox {
	filtered := make([]model.EventOutbox, 0, len(rows))
	for _, row := range rows {
		if strings.HasPrefix(row.PartitionKey, f.keyPrefix()) {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

// claimMine claims until want of this run's rows are in hand, and returns them carrying the
// claim tokens their claims issued.
//
// It claims in batches rather than in one oversized claim so that the number of other runs'
// rows it leases along the way stays bounded. Those rows are leased and then left, exactly
// as a relay that was stopped would leave them, and they return to the claimable set when
// the lease expires — the same self-healing path this file is testing.
func (f *recoveryFixture) claimMine(ctx context.Context, want int, lease time.Duration) []model.EventOutbox {
	f.t.Helper()

	claimedMine := make([]model.EventOutbox, 0, want)
	deadline := time.Now().Add(30 * time.Second)

	for len(claimedMine) < want {
		batch, err := f.ds.ClaimPendingEventOutbox(ctx, 100, lease)
		require.NoError(f.t, err, "claiming a batch of outbox rows")

		claimedMine = append(claimedMine, f.mine(batch)...)

		if len(claimedMine) >= want {
			break
		}

		if time.Now().After(deadline) {
			present := f.snapshot(ctx)
			f.t.Fatalf("timed out claiming %d of run %s's outbox rows; got %d, and the last claim returned %d "+
				"rows. Run state: %s. When the seeded rows are missing or already finished, ANOTHER PROCESS is "+
				"draining or purging the same outbox — these tests need a database no other relay is working",
				want, f.runID, len(claimedMine), len(batch), recoveryDescribeStates(present))
		}

		if len(batch) == 0 {
			time.Sleep(recoveryStateInterval)
		}
	}

	return claimedMine
}

// inDeadLetterInventory reports whether an event appears in the dead-letter inventory the
// operator API pages through, covering both `failed` and `dead_lettered`.
//
// It pages rather than reading the first page, because a shared database holds other runs'
// dead-lettered events and this run's row is not guaranteed to be on page one. It pages by
// CURSOR, which is what the operator API does now — an offset walk would have cost
// more per page the deeper this run's row happened to sit.
func (f *recoveryFixture) inDeadLetterInventory(ctx context.Context, eventID string) bool {
	f.t.Helper()

	const (
		pageSize = 200
		maxPages = 10
	)

	var cursor *model.DeadLetterCursor

	for range maxPages {
		page, err := f.ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
			Limit:  pageSize,
			Cursor: cursor,
		})
		require.NoError(f.t, err, "paging the dead-letter inventory")

		for _, row := range page.Entries {
			if row.EventID == eventID {
				return true
			}
		}

		if !page.HasMore || page.NextCursor == nil {
			return false
		}

		cursor = page.NextCursor
	}

	return false
}

// awaitDeliveries blocks until a publisher has recorded at least count deliveries of this
// run's events, and fails with the outbox state rather than hanging when it does not.
func (f *recoveryFixture) awaitDeliveries(publisher *recoveryPublisher, count int, timeout time.Duration) {
	f.t.Helper()

	deadline := time.Now().Add(timeout)

	for {
		delivered, strangers, failures := publisher.counts()
		if delivered >= count {
			return
		}

		if time.Now().After(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			f.t.Fatalf("publisher %q delivered %d of the %d events expected within %s "+
				"(%d deliveries for other runs, %d failures): %s",
				publisher.name, delivered, count, timeout, strangers, failures,
				recoveryDescribeStates(f.snapshot(ctx)))

			return
		}

		time.Sleep(recoveryStateInterval)
	}
}

// ---------------------------------------------------------------------------
// The instrumented publisher
// ---------------------------------------------------------------------------

// recoveryPublisher stands where the broker stands, and records what reached it.
//
// It is a full TopicEventPublisher, so the relay is the REAL relay driving its real publish path —
// nothing about claiming, marking, backoff or dead-lettering is stubbed. Only the transport is
// observable, which buys an EXACT RECORD of every delivery (so "no loss" and "duplicates
// de-duplicate to the seeded set" are set comparisons rather than inferences) and a DETERMINISTIC
// INTERRUPTION POINT: after the configured number of deliveries every later publish blocks until
// released or cancelled.
//
// The freeze reports itself only once a REQUIRED NUMBER OF PUBLISHES ARE PARKED in it. Signalling
// on the delivery count alone would leave the crash a race between the test's cancel and the
// relay's next publish — sometimes landing on in-flight publishes, sometimes not. Waiting for the
// workers to arrive puts the crash inside the publish-to-mark window on every run, which is where
// the outbox row and the broker disagree, and a sleep-and-hope would usually miss it entirely
// since a fast publisher drains 150 rows in milliseconds.
//
// A delegate may be supplied, in which case every publish is forwarded to it — that is how the
// live-broker test keeps the same instrumentation while writing to a real topic.
type recoveryPublisher struct {
	// name distinguishes instances in diagnostics, which matters for the two-relay test.
	name string

	// delegate is the real publisher, or nil to record without a broker.
	delegate TopicEventPublisher

	// watched is the set of event ids this run seeded. Everything else is another run's
	// work, counted separately and never asserted on.
	watched map[string]struct{}

	mu         sync.Mutex
	published  []string
	strangers  int
	failures   int
	refuseAll  bool
	gateAt     int
	gateParked int
	gateCount  int
	parked     int
	peakParked int
	hold       chan struct{}

	gate      chan struct{}
	gateOnce  sync.Once
	holdOnce  sync.Once
	closeOnce sync.Once
}

// recoveryPublisher must satisfy the full publisher contract, or the relay would not adopt
// it and would refuse to start.
var _ TopicEventPublisher = (*recoveryPublisher)(nil)

// newRecoveryPublisher builds a publisher watching the supplied event ids.
func newRecoveryPublisher(name string, delegate TopicEventPublisher, watched []string) *recoveryPublisher {
	set := make(map[string]struct{}, len(watched))
	for _, eventID := range watched {
		set[eventID] = struct{}{}
	}

	return &recoveryPublisher{
		name:      name,
		delegate:  delegate,
		watched:   set,
		published: make([]string, 0, len(watched)),
		gate:      make(chan struct{}),
	}
}

// freezeAfter arms the interruption point: once deliveries watched deliveries have been
// recorded, every subsequent publish blocks until release is called or the publishing context
// is cancelled — and the gate reported by awaitFreeze closes once inFlight of those blocked
// publishes have arrived.
//
// inFlight is the guarantee, not a hint. It is how many publishes are certain to be in flight
// when the test cancels the context, so the relay's in-flight failure path — record the
// attempt, release the lease, schedule the retry — is exercised on every run instead of
// whenever the scheduler happens to cooperate. Size it below the relay's concurrency and below
// the rows remaining in the batch, or the wait can never be satisfied.
//
// Call it before the relay starts.
func (p *recoveryPublisher) freezeAfter(deliveries, inFlight int) *recoveryPublisher {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.gateAt = deliveries
	p.gateParked = inFlight
	p.hold = make(chan struct{})

	return p
}

// refuseEveryPublish makes every publish fail as a RETRYABLE transport failure.
//
// It is how a test drives a row all the way to the dead-letter hand-off: retryable is the
// honest classification of a broker that will not accept a write, and it is the classification
// that makes the relay spend the row's retry budget rather than giving up on the first attempt.
// Pair it with a one-attempt budget — see seedWithBudget — so the hand-off happens immediately.
//
// Call it before the relay starts.
func (p *recoveryPublisher) refuseEveryPublish() *recoveryPublisher {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.refuseAll = true

	return p
}

// release unblocks a frozen publisher. It is idempotent, so it is safe both on the restart
// path and in a cleanup that runs after a failed assertion — without it, a failure between
// the freeze and the restart would leave relay workers blocked for ever.
func (p *recoveryPublisher) release() {
	p.holdOnce.Do(func() {
		p.mu.Lock()
		hold := p.hold
		p.mu.Unlock()

		if hold != nil {
			close(hold)
		}
	})
}

// awaitFreeze blocks until the interruption point is reached with the required number of
// publishes parked in it, and fails loudly rather than hanging when it is not.
func (p *recoveryPublisher) awaitFreeze(t *testing.T, timeout time.Duration) (int, int) {
	t.Helper()

	select {
	case <-p.gate:
	case <-time.After(timeout):
		delivered, strangers, failures := p.counts()

		p.mu.Lock()
		parked, required := p.parked, p.gateParked
		p.mu.Unlock()

		t.Fatalf("publisher %q never reached its interruption point within %s: %d watched deliveries, "+
			"%d other runs' deliveries, %d failures, %d of the %d required publishes parked. Either the relay is "+
			"not draining the outbox, or the batch has fewer rows left than the freeze requires in flight",
			p.name, timeout, delivered, strangers, failures, parked, required)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.gateCount, p.peakParked
}

// Publish satisfies the mandated minimal contract by submitting an envelope-only request,
// exactly as the real publishers do.
func (p *recoveryPublisher) Publish(ctx context.Context, event model.LedgerEvent) error {
	_, err := p.PublishToTopic(ctx, PublishRequest{Event: event})

	return err
}

// PublishToTopic records one delivery, honouring cancellation and the freeze.
func (p *recoveryPublisher) PublishToTopic(ctx context.Context, req PublishRequest) (PublishResult, error) {
	result := PublishResult{
		Status:       model.PublishStatusDispatched,
		EventID:      req.Event.EventID,
		EventType:    req.Event.EventType,
		Topic:        req.Topic,
		PartitionKey: req.Key,
		Attempt:      req.Attempt,
		MaxAttempts:  req.MaxAttempts,
		Purpose:      req.Purpose,
	}

	// The freeze is checked BEFORE the cancellation check, because that ordering is what
	// makes the interruption deterministic: the workers park here, and the test's cancel is
	// what wakes them.
	if err := p.awaitRelease(ctx); err != nil {
		return p.failed(result, err), err
	}

	if err := ctx.Err(); err != nil {
		// A cancelled context is how a crash reaches the transport, and a publish that did
		// not happen must be reported as a failure. Reporting it as dispatched would let the
		// relay mark a row delivered that no broker ever saw — the one bug that would turn
		// this suite's "no loss" assertion into a false negative.
		return p.failed(result, err), err
	}

	if p.refusesEverything() {
		// Checked before the delegate, so a refusing publisher never reaches a real broker.
		return p.failed(result, errRecoveryTransportRefused), errRecoveryTransportRefused
	}

	if p.delegate != nil {
		delegated, err := p.delegate.PublishToTopic(ctx, req)
		if err != nil {
			p.recordFailure()

			return delegated, err
		}

		result = delegated
	}

	p.record(req.Event.EventID)

	return result, nil
}

// Close releases the delegate, if any. It is idempotent.
func (p *recoveryPublisher) Close() error {
	var err error
	p.closeOnce.Do(func() {
		if p.delegate != nil {
			err = p.delegate.Close()
		}
	})

	return err
}

// awaitRelease implements the freeze, and reports the gate open once enough publishes are
// parked in it — see freezeAfter for why the parked count, not the delivery count, is what the
// waiting test is told about.
func (p *recoveryPublisher) awaitRelease(ctx context.Context) error {
	p.mu.Lock()
	frozen := p.gateAt > 0 && len(p.published) >= p.gateAt
	hold := p.hold

	reached := false
	if frozen && hold != nil {
		p.parked++
		if p.parked > p.peakParked {
			p.peakParked = p.parked
		}

		if p.parked >= p.gateParked {
			reached = true
			if p.gateCount == 0 {
				p.gateCount = len(p.published)
			}
		}
	}
	p.mu.Unlock()

	if !frozen || hold == nil {
		return nil
	}

	if reached {
		p.gateOnce.Do(func() { close(p.gate) })
	}

	defer func() {
		p.mu.Lock()
		p.parked--
		p.mu.Unlock()
	}()

	select {
	case <-hold:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// record notes one delivery, separating this run's events from every other run's.
//
// It does NOT open the gate. Reaching the delivery count only arms the freeze; the gate opens
// in awaitRelease once the required number of publishes have actually parked in it, which is
// what makes the interruption reproducible.
func (p *recoveryPublisher) record(eventID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.watched[eventID]; ok {
		p.published = append(p.published, eventID)

		return
	}

	p.strangers++
}

// refusesEverything reports whether refuseEveryPublish was called.
func (p *recoveryPublisher) refusesEverything() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.refuseAll
}

// recordFailure notes one failed delivery.
func (p *recoveryPublisher) recordFailure() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.failures++
}

// failed decorates a result as a retryable transport failure, which is what the relay reads
// to decide between another attempt and the dead-letter hand-off.
func (p *recoveryPublisher) failed(result PublishResult, cause error) PublishResult {
	p.recordFailure()

	result.Status = model.PublishStatusRetrying
	result.Retryable = true
	result.Transient = true
	result.Err = cause

	return result
}

// deliveries returns the watched event ids in delivery order, duplicates included — the
// duplicates are the point.
func (p *recoveryPublisher) deliveries() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.published...)
}

// counts returns watched deliveries, other runs' deliveries, and failures.
func (p *recoveryPublisher) counts() (int, int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.published), p.strangers, p.failures
}

// ---------------------------------------------------------------------------
// The dead-letter transport
// ---------------------------------------------------------------------------

// recoveryDeadLetterWriter is a dead-letter transport that REFUSES EVERY WRITE until it is
// released, and records the messages it accepts afterwards.
//
// It stands in for the one failure that strands an event with nowhere left to go: the row has
// spent its retry budget, so no further publish will be attempted, and the dead-letter write
// that was supposed to preserve it did not happen. Only the raw Kafka write is substituted —
// the message composition, the routing to `<topic>.dlt` and the transition that records the
// terminal state are the production ones, running against the real database — so what the
// repair is proved against is the real state machine and not a model of it.
type recoveryDeadLetterWriter struct {
	mu       sync.Mutex
	attempts int
	written  []kafka.Message
	released bool
}

var _ deadLetterMessageWriter = (*recoveryDeadLetterWriter)(nil)

// WriteMessages counts every attempt and, once released, keeps what it was handed.
func (w *recoveryDeadLetterWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.attempts++

	if !w.released {
		return errRecoveryDeadLetterRefused
	}

	w.written = append(w.written, msgs...)

	return nil
}

// release lets subsequent writes succeed. It is idempotent, so a cleanup may call it after a
// test already has.
func (w *recoveryDeadLetterWriter) release() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.released = true
}

// attemptCount is how many dead-letter writes have been attempted, accepted or refused. A
// count that keeps CLIMBING while the transport refuses is the observable proof that the
// repair pass is finding the row again.
func (w *recoveryDeadLetterWriter) attemptCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.attempts
}

// messages returns the dead-letter messages that were accepted.
func (w *recoveryDeadLetterWriter) messages() []kafka.Message {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]kafka.Message(nil), w.written...)
}

// recoveryBarrierDeadLetterWriter is a dead-letter transport that HOLDS ITS FIRST WRITE OPEN
// until it is told to proceed, and accepts every write.
//
// It exists to make an in-flight dead-letter write observable. The duplicate-dead-letter race
// this file covers occupies the interval between the exhaustion arm handing a row's
// dead-letter write to one worker and that worker completing it — an interval with no natural
// duration, so a test that merely ran two workers concurrently would almost always miss it.
// Blocking inside WriteMessages pins the interval open for as long as the assertions need,
// which turns a probabilistic race into a deterministic one.
//
// It differs from recoveryDeadLetterWriter deliberately: that one REFUSES writes to strand a
// row, this one DELAYS one to keep a hand-off in progress. Refusing would settle the row and
// close the very window under test.
type recoveryBarrierDeadLetterWriter struct {
	mu       sync.Mutex
	attempts int
	written  []kafka.Message

	// entered is closed when the first write arrives, so a test can wait for the hand-off to
	// be genuinely in flight rather than sleeping and hoping.
	entered   chan struct{}
	enterOnce sync.Once

	// proceed is closed to let the held write complete. Closing is idempotent through
	// proceedOnce, so a cleanup may release a writer a test already released.
	proceed     chan struct{}
	proceedOnce sync.Once
}

var _ deadLetterMessageWriter = (*recoveryBarrierDeadLetterWriter)(nil)

// newRecoveryBarrierDeadLetterWriter builds a writer whose first write blocks.
func newRecoveryBarrierDeadLetterWriter() *recoveryBarrierDeadLetterWriter {
	return &recoveryBarrierDeadLetterWriter{
		entered: make(chan struct{}),
		proceed: make(chan struct{}),
	}
}

// WriteMessages records the attempt, announces that it has arrived, and waits for permission
// before accepting the message.
//
// The lock is NOT held across the wait. Holding it would make attemptCount block on the
// barrier too, and the test's proof that the SECOND worker never wrote depends on being able
// to read the count while the first write is still parked here.
func (w *recoveryBarrierDeadLetterWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	w.attempts++
	w.mu.Unlock()

	w.enterOnce.Do(func() { close(w.entered) })

	select {
	case <-w.proceed:
	case <-ctx.Done():
		return ctx.Err()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.written = append(w.written, msgs...)

	return nil
}

// awaitEntered blocks until a dead-letter write has arrived at the barrier, and fails rather
// than hanging when none does.
func (w *recoveryBarrierDeadLetterWriter) awaitEntered(t *testing.T, timeout time.Duration) {
	t.Helper()

	select {
	case <-w.entered:
	case <-time.After(timeout):
		require.FailNow(t, "no dead-letter write reached the transport",
			"the hand-off must be IN FLIGHT before the race can be observed; nothing arrived within %s", timeout)
	}
}

// release lets the held write complete. It is idempotent.
func (w *recoveryBarrierDeadLetterWriter) release() {
	w.proceedOnce.Do(func() { close(w.proceed) })
}

// attemptCount is how many dead-letter writes have been attempted, including one parked at the
// barrier. It is the count that has to be exactly one.
func (w *recoveryBarrierDeadLetterWriter) attemptCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.attempts
}

// messages returns the dead-letter messages that were accepted.
func (w *recoveryBarrierDeadLetterWriter) messages() []kafka.Message {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]kafka.Message(nil), w.written...)
}

// deadLetterService builds the production dead-letter service over this fixture's database,
// with writes going to the supplied transport instead of a broker.
//
// Parameters:
//   - publisher TopicEventPublisher: the publisher replays would go through.
//   - writer deadLetterMessageWriter: the transport dead-letter writes are handed to.
//
// Returns:
//   - *EventDeadLetterService: the real service, wired to the real repository.
func (f *recoveryFixture) deadLetterService(
	publisher TopicEventPublisher,
	writer deadLetterMessageWriter,
) *EventDeadLetterService {
	f.t.Helper()

	return NewEventDeadLetterService(f.ds, publisher).
		withTransport(publisher, func(string) (deadLetterMessageWriter, error) {
			return writer, nil
		})
}

// recoveryDeadLetterEnvelope is the part of a dead-letter message these tests read back: the
// event it preserves and the failure metadata appended beside it.
//
// The envelope's other four LedgerEvent keys are asserted byte-for-byte in
// event_replay_fidelity_test.go, so they are deliberately not re-asserted here.
type recoveryDeadLetterEnvelope struct {
	EventID         string                `json:"event_id"`
	FailureMetadata model.FailureMetadata `json:"failure_metadata"`
}

// recoveryDecodeDeadLetters decodes accepted dead-letter messages, keyed by event id.
func recoveryDecodeDeadLetters(t *testing.T, msgs []kafka.Message) map[string]model.FailureMetadata {
	t.Helper()

	preserved := make(map[string]model.FailureMetadata, len(msgs))

	for index, msg := range msgs {
		var envelope recoveryDeadLetterEnvelope
		require.NoErrorf(t, json.Unmarshal(msg.Value, &envelope),
			"decoding dead-letter message %d: %s", index, string(msg.Value))
		require.NotEmptyf(t, envelope.EventID,
			"dead-letter message %d must carry the event id it preserves", index)

		preserved[envelope.EventID] = envelope.FailureMetadata
	}

	return preserved
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

// recoveryUnique de-duplicates while preserving first-seen order. This is the consumer's
// idempotency boundary expressed in three lines: the set of event ids a subscriber that
// suppresses on event_id would end up having processed.
func recoveryUnique(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))

	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}

		seen[value] = struct{}{}
		unique = append(unique, value)
	}

	return unique
}

// recoveryRepeated returns the values delivered more than once, with their delivery counts.
func recoveryRepeated(values []string) map[string]int {
	counts := make(map[string]int, len(values))
	for _, value := range values {
		counts[value]++
	}

	repeated := map[string]int{}
	for value, count := range counts {
		if count > 1 {
			repeated[value] = count
		}
	}

	return repeated
}

// recoveryIntersection returns the values present in both slices.
func recoveryIntersection(left, right []string) []string {
	inLeft := make(map[string]struct{}, len(left))
	for _, value := range left {
		inLeft[value] = struct{}{}
	}

	shared := make([]string, 0, 4)
	for _, value := range right {
		if _, ok := inLeft[value]; ok {
			shared = append(shared, value)
		}
	}

	return recoveryUnique(shared)
}

// recoveryCountUnfinished counts rows that have not reached a terminal state.
func recoveryCountUnfinished(states []recoveryRowState) int {
	unfinished := 0
	for _, state := range states {
		if !state.terminal() {
			unfinished++
		}
	}

	return unfinished
}

// recoveryRequireNoGoroutineLeak asserts the relay's goroutines were joined.
//
// Stop joins the loop synchronously, so the count must return to its baseline; the
// tolerance and the settling window exist only for the connection pool and the
// OpenTelemetry machinery, which may retire a goroutine a moment later. A real leak is one
// goroutine per batch worker per batch, so it is far larger than the tolerance.
func recoveryRequireNoGoroutineLeak(t *testing.T, baseline int) {
	t.Helper()

	deadline := time.Now().Add(recoveryGoroutineSettle)

	for {
		current := runtime.NumGoroutine()
		if current <= baseline+recoveryGoroutineTolerance {
			t.Logf("goroutines settled at %d against a baseline of %d", current, baseline)

			return
		}

		if time.Now().After(deadline) {
			stacks := make([]byte, 1<<16)
			written := runtime.Stack(stacks, true)
			t.Errorf("goroutines did not settle after teardown: %d against a baseline of %d "+
				"(tolerance %d). Stop must join every worker it started.\n%s",
				current, baseline, recoveryGoroutineTolerance, stacks[:written])

			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// the mid-batch restart
// ---------------------------------------------------------------------------

// TestEventRecovery_MidBatchRestartLosesNoEventsAndDuplicatesAreDedupableByEventID is
// the acceptance criterion.
//
// It interrupts the relay in the MIDDLE of a claimed batch, restarts it, and asserts the two halves
// separately: nothing was lost, and after de-duplicating on event_id the delivered set is exactly
// the seeded set. It does NOT assert the broker saw each message once — see the file comment.
//
// The interruption is a CONTEXT CANCELLATION, not a Stop: Stop is a graceful drain that lets the
// claimed batch finish, so stopping the relay would assert against an orderly shutdown while
// claiming to test a crash. Cancellation abandons the batch as a killed process does.
func TestEventRecovery_MidBatchRestartLosesNoEventsAndDuplicatesAreDedupableByEventID(t *testing.T) {
	fixture := newRecoveryFixture(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// MORE THAN ONE CLAIM at the house batch size of 100, so the interruption necessarily
	// leaves behind both rows that were claimed but not published and rows that were never
	// claimed at all. Those are two different recovery paths — a lease that must expire, and
	// a backlog that must simply be picked up — and a batch of fewer than 100 would exercise
	// only the first.
	const seededEvents = 150
	const freezeAfter = 20

	seeded := fixture.seed(ctx, seededEvents)
	publisher := newRecoveryPublisher("crashed-relay", nil, seeded).
		freezeAfter(freezeAfter, recoveryPublishesInFlight)

	baseline := runtime.NumGoroutine()

	crashCtx, crash := context.WithCancel(ctx)
	first := fixture.relay(publisher)

	// Registered immediately: a failed assertion between the freeze and the restart must not
	// leave relay workers parked in the publisher for ever.
	t.Cleanup(func() {
		publisher.release()
		crash()
		first.Stop()
	})

	require.NoError(t, first.Start(crashCtx),
		"the relay refused to start; the returned obstacle names the missing precondition")
	require.True(t, first.IsRunning(), "the relay must be running before it can be interrupted")

	frozenAt, parkedAtCrash := publisher.awaitFreeze(t, recoveryGateTimeout)
	require.GreaterOrEqual(t, parkedAtCrash, recoveryPublishesInFlight,
		"the crash must land on publishes that are actually in flight")

	crash()
	first.Stop()
	require.False(t, first.IsRunning(), "Stop must join the relay's worker goroutine")

	deliveredAtCrash := len(publisher.deliveries())
	interrupted := fixture.snapshot(ctx)
	unfinishedAtCrash := recoveryCountUnfinished(interrupted)

	leasedAtCrash := 0
	for _, state := range interrupted {
		if !state.terminal() && state.leaseHeld {
			leasedAtCrash++
		}
	}

	t.Logf("interrupted with %d of %d seeded events delivered (freeze armed at %d, tripped at %d with %d "+
		"publishes in flight); %s",
		deliveredAtCrash, seededEvents, freezeAfter, frozenAt, parkedAtCrash, recoveryDescribeStates(interrupted))

	// THE MID-BATCH PROOF. Both bounds matter: nothing delivered means the relay never got
	// going and the restart recovers a backlog rather than a crash, and everything delivered
	// means the run finished cleanly before the interruption and this test proved nothing.
	require.Positive(t, deliveredAtCrash,
		"the relay delivered nothing before the interruption, so nothing was recovered from a crash")
	require.Less(t, deliveredAtCrash, seededEvents,
		"the interruption must land MID-batch; every event was already delivered, so this run proved nothing")
	require.Positive(t, unfinishedAtCrash,
		"the interruption must leave unfinished rows behind, or there is nothing for the restart to recover")
	assert.Positive(t, leasedAtCrash,
		"the interruption must abandon rows that were claimed but not published; their lease expiring is the "+
			"recovery mechanism under test")

	// THE RESTART. A fresh processor stands in for a fresh process — but the SAME publisher,
	// because the record of what the transport has already seen has to survive the restart or
	// a duplicate could not be detected at all.
	publisher.release()

	restartCtx, stopRestart := context.WithCancel(ctx)
	second := fixture.relay(publisher)
	t.Cleanup(func() {
		stopRestart()
		second.Stop()
	})

	require.NoError(t, second.Start(restartCtx),
		"the restarted relay refused to start; the returned obstacle names the missing precondition")
	require.True(t, second.IsRunning(), "the restarted relay must be running")

	states := fixture.waitForTerminalStates(ctx, seededEvents, recoveryDrainTimeout)

	second.Stop()
	stopRestart()
	require.False(t, second.IsRunning(), "Stop must join the restarted relay's worker goroutine")

	// Every row finished, none left permanently unclaimable.
	fixture.assertEveryRowFinishedCleanly(states, seededEvents)

	delivered := publisher.deliveries()
	unique := recoveryUnique(delivered)
	repeated := recoveryRepeated(delivered)
	_, strangers, failures := publisher.counts()

	t.Logf("delivered %d messages covering %d distinct events; %d event ids were delivered more than once, "+
		"%d deliveries belonged to other runs, %d publish attempts failed",
		len(delivered), len(unique), len(repeated), strangers, failures)

	// NO LOSS AND NO STRANGERS, at the consumer's idempotency boundary.
	assert.ElementsMatch(t, seeded, unique,
		"de-duplicating the delivered event ids must reproduce the seeded set EXACTLY: a missing id is a lost "+
			"event, and an extra id is an event nobody recorded")
	require.GreaterOrEqual(t, len(delivered), len(seeded),
		"every seeded event must have been delivered at least once")

	// Duplicates are permitted, and when they happen they must be DETECTABLE — that is what
	// makes the subscriber's documented obligation to suppress on event_id actionable rather
	// than theoretical.
	for eventID, count := range repeated {
		assert.Containsf(t, seeded, eventID,
			"only seeded events may appear in this run's delivery record; %s was delivered %d times", eventID, count)
		assert.Greaterf(t, count, 1, "%s is listed as repeated, so it must have more than one delivery", eventID)
	}

	assert.GreaterOrEqual(t, failures, recoveryPublishesInFlight,
		"every publish that was in flight when the context died must be reported as failed; a publish that did not "+
			"happen must never be recorded as delivered")

	// attempts must SURVIVE the restart — neither reset (which would let a permanently failing
	// event retry for ever) nor inflated (which would dead-letter a healthy event early).
	retried := 0
	for _, state := range states {
		if state.attempts > 0 {
			retried++
		}

		assert.Lessf(t, state.attempts, state.maxAttempts,
			"event %s is healthy and must not have been driven to the edge of its retry budget by the restart "+
				"(attempts %d of %d)", state.eventID, state.attempts, state.maxAttempts)
	}

	// One recorded attempt per interrupted publish, and it must still be there after the row was
	// re-claimed and delivered. The bound is "at least the publishes that were in flight" rather
	// than an exact count, because a row whose lease had also expired would have lost its claim
	// and so could not record — which is correct behaviour, not a miscount.
	assert.GreaterOrEqualf(t, retried, 1,
		"the %d publishes interrupted in flight must have recorded a failed attempt that survived the restart; "+
			"a counter that resets across a restart lets a permanently failing event retry for ever", failures)

	recoveryRequireNoGoroutineLeak(t, baseline)
}

// TestEventRecovery_PublishedButUnmarkedRowIsRepublishedAndTheDuplicateDedupesByEventID
// reproduces the at-least-once window ON PURPOSE, which is the only way to test it
// deterministically.
//
// The window is the gap between the broker acknowledging a publish and the outbox row being
// marked dispatched. A relay that dies inside it has delivered the event without recording
// that it did. This test claims a row, publishes it, and then simply never marks it — a
// crash, expressed exactly — and asserts the three things that must follow:
//
//   - the row is recovered rather than stranded, because its lease expires;
//   - the event is therefore delivered a SECOND time, which is correct and not a defect;
//   - de-duplicating on event_id recovers the original set exactly, and the duplicate is
//     visible as a repeated event_id rather than silently indistinguishable.
//
// The redelivery must also cost no retry budget: the publish succeeded both times, so nothing
// failed and attempts must stay at zero.
func TestEventRecovery_PublishedButUnmarkedRowIsRepublishedAndTheDuplicateDedupesByEventID(t *testing.T) {
	fixture := newRecoveryFixture(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const seededEvents = 3

	seeded := fixture.seed(ctx, seededEvents)
	publisher := newRecoveryPublisher("at-least-once", nil, seeded)

	// A long lease so the assertions below observe the row while it is still held. It is the
	// same mechanism as production's thirty seconds, only shorter.
	claimed := fixture.claimMine(ctx, seededEvents, recoveryHeldLease)
	require.Len(t, claimed, seededEvents, "every seeded row must be claimable")

	for _, row := range claimed {
		require.NotEmptyf(t, row.ClaimToken, "the claim must stamp a token on event %s", row.EventID)

		result, err := publisher.PublishToTopic(ctx, PublishRequest{
			Event: model.LedgerEvent{
				EventID:       row.EventID,
				EventType:     row.EventType,
				AggregateID:   row.AggregateID,
				OccurredAt:    row.OccurredAt,
				Payload:       row.Payload,
				SchemaVersion: row.SchemaVersion,
			},
			Topic:       row.Topic,
			Key:         row.PartitionKey,
			Attempt:     row.Attempts + 1,
			MaxAttempts: row.MaxAttempts,
			Purpose:     PublishPurposeOriginal,
		})
		require.NoErrorf(t, err, "publishing event %s", row.EventID)
		require.Truef(t, result.Dispatched(), "event %s must be reported dispatched", row.EventID)

		// AND NOTHING IS MARKED. This is the crash: the transport has the event, the row does
		// not say so, and no code path will ever be told.
	}

	require.Len(t, publisher.deliveries(), seededEvents, "each event must have been delivered once so far")

	held := fixture.snapshot(ctx)
	require.Len(t, held, seededEvents)
	for _, state := range held {
		assert.Equalf(t, model.EventOutboxStatusProcessing, state.status,
			"event %s must still be processing: it was claimed and never marked", state.eventID)
		assert.Truef(t, state.leaseHeld, "event %s must still hold the lease of its lost claim", state.eventID)
		assert.Truef(t, state.leaseLive,
			"event %s must still be INSIDE its lease here: a claim that took no live lease would leave the row "+
				"claimable while it was being published, which duplicates every event under normal operation",
			state.eventID)
		assert.NotEmptyf(t, state.claimToken, "event %s must still carry the claim token of its lost claim", state.eventID)
	}

	// The lease expiring is the recovery. The relay needs no knowledge of the crash.
	relay := fixture.relay(publisher)
	t.Cleanup(relay.Stop)
	require.NoError(t, relay.Start(ctx),
		"the relay refused to start; the returned obstacle names the missing precondition")

	states := fixture.waitForTerminalStates(ctx, seededEvents, recoveryDrainTimeout)
	relay.Stop()

	fixture.assertEveryRowFinishedCleanly(states, seededEvents)

	delivered := publisher.deliveries()
	repeated := recoveryRepeated(delivered)

	t.Logf("each of the %d events was delivered %d times in total; %d event ids show a duplicate",
		seededEvents, len(delivered), len(repeated))

	assert.GreaterOrEqual(t, len(delivered), 2*seededEvents,
		"each event must be delivered again after its lease expired: the first delivery was never recorded, so "+
			"a republish is the only outcome that does not lose it")
	assert.ElementsMatch(t, seeded, recoveryUnique(delivered),
		"de-duplicating on event_id must recover the ORIGINAL set exactly; that is what makes the subscriber's "+
			"documented idempotency obligation actionable")
	assert.Len(t, repeated, seededEvents,
		"every event's duplicate must be detectable as a repeated event_id")

	for _, state := range states {
		assert.Zerof(t, state.attempts,
			"event %s was published successfully both times, so the redelivery must have cost no retry budget "+
				"(attempts %d)", state.eventID, state.attempts)
		assert.Emptyf(t, state.lastError, "event %s never failed, so no error must be recorded", state.eventID)
	}
}

// ---------------------------------------------------------------------------
// Lease semantics — the recovery mechanism itself
// ---------------------------------------------------------------------------

// TestEventRecovery_ClaimedRowIsReclaimableOnlyAfterItsLeaseExpires exercises BOTH sides of
// the lease, because each side prevents a different disaster.
//
//   - While the lease is LIVE the row must not be claimable. Otherwise two relay instances
//     publish the same event at the same time, every event is duplicated under normal
//     operation, and both instances then race to record the outcome.
//   - Once the lease has EXPIRED the row must be claimable. Otherwise a crashed relay's
//     in-flight rows are stranded for ever: never delivered, never dead-lettered, and
//     invisible in the dead-letter inventory. This is the whole of crash recovery.
//
// It also asserts the fencing that makes the second side safe: the token from the lost claim
// can no longer move the row, so a worker that wakes up after its lease expired cannot
// overwrite the state of the instance that has since taken over.
func TestEventRecovery_ClaimedRowIsReclaimableOnlyAfterItsLeaseExpires(t *testing.T) {
	fixture := newRecoveryFixture(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const seededEvents = 4

	fixture.seed(ctx, seededEvents)

	firstClaim := fixture.claimMine(ctx, seededEvents, recoveryHeldLease)
	require.Len(t, firstClaim, seededEvents, "every seeded row must be claimable")

	firstTokens := make(map[string]string, seededEvents)
	firstAttempted := make(map[string]time.Time, seededEvents)
	lastAttempted := make(map[string]time.Time, seededEvents)
	leaseEnd := time.Now()

	for _, row := range firstClaim {
		require.NotEmptyf(t, row.ClaimToken, "the claim must stamp a token on event %s", row.EventID)
		require.NotNilf(t, row.LockedUntil, "the claim must take a lease on event %s", row.EventID)
		require.NotNilf(t, row.FirstAttemptedAt, "the claim must stamp first_attempted_at on event %s", row.EventID)
		require.NotNilf(t, row.LastAttemptedAt, "the claim must stamp last_attempted_at on event %s", row.EventID)

		assert.Equalf(t, model.EventOutboxStatusProcessing, row.Status,
			"a claimed row must be processing, not %q", row.Status)
		assert.Zerof(t, row.Attempts, "a claim must not count as an attempt for event %s", row.EventID)

		firstTokens[row.EventID] = row.ClaimToken
		firstAttempted[row.EventID] = *row.FirstAttemptedAt
		lastAttempted[row.EventID] = *row.LastAttemptedAt

		if row.LockedUntil.After(leaseEnd) {
			leaseEnd = *row.LockedUntil
		}
	}

	// SIDE ONE: not claimable while the lease is live.
	blocked, err := fixture.ds.ClaimPendingEventOutbox(ctx, 100, recoveryLease)
	require.NoError(t, err, "a claim must not fail merely because everything of ours is held")
	assert.Empty(t, fixture.mine(blocked),
		"a row whose lease is still live must NOT be claimable; two relay instances publishing one event "+
			"concurrently is exactly what the lease exists to prevent")

	// SIDE TWO: claimable once the lease has expired, which is how a crashed relay's rows come
	// back. The wait is the lease, plus a small margin for clock granularity.
	time.Sleep(time.Until(leaseEnd.Add(500 * time.Millisecond)))

	secondClaim := fixture.claimMine(ctx, seededEvents, recoveryHeldLease)
	require.Len(t, secondClaim, seededEvents,
		"every row of the abandoned claim must be claimable again once its lease has expired; a row that is not "+
			"is stranded for ever")

	for _, row := range secondClaim {
		assert.NotEqualf(t, firstTokens[row.EventID], row.ClaimToken,
			"the re-claim of event %s must issue a NEW token, or the fencing that stops a stale worker writing "+
				"would be meaningless", row.EventID)
		assert.Zerof(t, row.Attempts,
			"re-claiming event %s after a lease expiry must not consume retry budget: no attempt was recorded, "+
				"only abandoned", row.EventID)

		require.NotNilf(t, row.FirstAttemptedAt, "first_attempted_at must survive the re-claim of event %s", row.EventID)
		assert.WithinDurationf(t, firstAttempted[row.EventID], *row.FirstAttemptedAt, time.Millisecond,
			"first_attempted_at must stay pinned to the FIRST claim of event %s; it bounds the retry window the "+
				"dead-letter metadata reports", row.EventID)

		require.NotNilf(t, row.LastAttemptedAt, "last_attempted_at must be stamped on the re-claim of event %s", row.EventID)
		assert.Truef(t, row.LastAttemptedAt.After(lastAttempted[row.EventID]),
			"last_attempted_at must move with every claim of event %s", row.EventID)
	}

	// FENCING: the lost claim's token must no longer be able to move the row, and the new one
	// must. Without this, a worker whose lease expired could mark an event dispatched that the
	// instance which took over had not yet published.
	stale := secondClaim[0]
	staleErr := fixture.ds.MarkEventDispatched(ctx, stale.ID, firstTokens[stale.EventID], model.BrokerRecord{})
	require.Error(t, staleErr,
		"the token of a lost claim must be refused: a stale worker must not be able to mark a row the new owner holds")
	assert.Contains(t, strings.ToLower(staleErr.Error()), "claim",
		"the refusal must say the claim was lost rather than fail opaquely: %v", staleErr)

	for _, row := range secondClaim {
		require.NoErrorf(t, fixture.ds.MarkEventDispatched(ctx, row.ID, row.ClaimToken, model.BrokerRecord{}),
			"the CURRENT claim token must be able to finish event %s", row.EventID)
	}

	states := fixture.snapshot(ctx)
	fixture.assertEveryRowFinishedCleanly(states, seededEvents)
}

// TestEventRecovery_ABatchThatOutlivesItsLeaseKeepsItAndIsNotRepublished is the other half of
// the lease contract, and the half that is easy to get wrong: a lease must not expire under a
// relay that is STILL HOLDING the rows it covers.
//
// # The defect it fails on
//
// The lease is taken once, when the batch is claimed. A batch can legitimately take longer than
// it: at the shipped defaults a claim takes a hundred rows and publishes eight at a time, so it
// runs in thirteen waves and one wave can occupy the writer's entire produce timeout. The rows
// in the later waves therefore had their lease expire BEFORE their publish was even attempted,
// while this relay still held them and still intended to publish them. A second instance then
// claimed and published exactly those rows, this one published them again afterwards, and every
// transition this one attempted failed as a lost claim. The topic got duplicates and neither
// process logged a defect — a silent doubling of every event under a backlog, which is when it
// matters most.
//
// # Why it is not tested by simply asserting a duplicate never happens
//
// Duplicates are LEGITIMATE on this pipeline: the file comment explains that a crash inside the
// publish-to-mark window republishes, and that the duplicate is suppressed at the subscriber on
// event_id. So the assertion here is deliberately stronger than the one those tests make. No
// crash is staged, nothing is cancelled, and no lease is allowed to lapse — so a duplicate here
// has no legitimate source, and NO EVENT MAY BE PUBLISHED TWICE BEFORE ANY IDEMPOTENCY
// FILTERING AT ALL. That is what makes this a test of the heartbeat rather than of the
// subscriber's obligation.
//
// # The shape
//
// Alpha claims the whole backlog in one batch under one token and then cannot finish it: its
// publisher parks, and its concurrency of one means the rest of the batch waits behind the
// parked publish. The batch is then held for three times its own lease — long enough that a
// lease taken once and never renewed would have expired twice over — while beta polls
// throughout. The assertions are that alpha's lease is still live under the SAME token with a
// LATER expiry, that beta delivered nothing of this run's, and that once alpha is released every
// event was published exactly once between them.
func TestEventRecovery_ABatchThatOutlivesItsLeaseKeepsItAndIsNotRepublished(t *testing.T) {
	fixture := newRecoveryFixture(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const seededEvents = 12

	seeded := fixture.seed(ctx, seededEvents)

	// Frozen after one delivery with one publish parked, which is all a concurrency of one can
	// have in flight — and enough, because what the lease is protecting is the rows QUEUED
	// BEHIND that publish, not the publish itself.
	alphaPublisher := newRecoveryPublisher("relay-alpha", nil, seeded).freezeAfter(1, 1)
	betaPublisher := newRecoveryPublisher("relay-beta", nil, seeded)

	baseline := runtime.NumGoroutine()

	// Both relays keep the fixture's SHORT lease, unlike the concurrency test which pins the
	// production one: here the lease expiring is precisely the hazard, so it must be short
	// enough that the test can outlive it in seconds.
	alpha := fixture.relay(alphaPublisher).
		WithBatchSize(recoveryOversizedBatch).
		WithConcurrency(1)
	beta := fixture.relay(betaPublisher).
		WithBatchSize(recoveryOversizedBatch)

	t.Cleanup(func() {
		alphaPublisher.release()
		alpha.Stop()
		beta.Stop()
	})

	require.NoError(t, alpha.Start(ctx), "alpha refused to start; the returned obstacle names why")

	_, parked := alphaPublisher.awaitFreeze(t, recoveryGateTimeout)
	require.GreaterOrEqual(t, parked, 1,
		"alpha must be parked inside a publish, holding a claimed batch it cannot finish")

	held := fixture.heldLeases(ctx)
	require.GreaterOrEqualf(t, len(held), 2,
		"alpha must be holding more of this run's rows than it has published: the batch limit is %d "+
			"against %d seeded rows, so one claim takes them all and the concurrency of one leaves the "+
			"rest queued. Held: %d",
		recoveryOversizedBatch, seededEvents, len(held))

	tokens := recoveryClaimTokens(held)
	require.Lenf(t, tokens, 1,
		"the whole backlog must be held under ONE claim token, so that renewing that token renews the "+
			"whole batch; got %d tokens", len(tokens))

	for eventID, lease := range held {
		require.Truef(t, lease.live, "event %s must be claimed under a live lease at the freeze", eventID)
		require.Equalf(t, model.EventOutboxStatusProcessing, lease.status,
			"event %s must be in flight at the freeze", eventID)
	}

	// Beta now polls for the whole overrun. Every poll is a claim attempt against rows whose
	// lease alpha is renewing, and every one of them must come back with nothing of ours.
	require.NoError(t, beta.Start(ctx), "beta refused to start; the returned obstacle names why")

	t.Logf("holding alpha's batch of %d rows for %s, which is %.0f times its own %s lease",
		len(held), recoveryLeaseOverrun, float64(recoveryLeaseOverrun)/float64(recoveryLease), recoveryLease)
	time.Sleep(recoveryLeaseOverrun)

	renewed := fixture.heldLeases(ctx)

	for eventID, before := range held {
		after, stillHeld := renewed[eventID]
		require.Truef(t, stillHeld,
			"event %s lost its claim while alpha was still holding it: after %s — %.0f leases — the "+
				"heartbeat must have kept it. A row released here is republished by another instance "+
				"while this one is still publishing it",
			eventID, recoveryLeaseOverrun, float64(recoveryLeaseOverrun)/float64(recoveryLease))

		assert.Equalf(t, before.claimToken, after.claimToken,
			"event %s must still be held by the SAME claim: a different token means the row was "+
				"re-claimed by somebody, which is the duplicate this test exists to rule out", eventID)

		assert.Truef(t, after.expiry.After(before.expiry),
			"event %s must have had its lease EXTENDED: it expired at %s and still expires at %s, so "+
				"nothing renewed it",
			eventID, before.expiry.UTC().Format(time.RFC3339Nano), after.expiry.UTC().Format(time.RFC3339Nano))

		assert.Truef(t, after.live,
			"event %s must still be under a live lease after %s: its expiry is %s and now is %s",
			eventID, recoveryLeaseOverrun,
			after.expiry.UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	}

	betaDelivered, betaStrangers, betaFailures := betaPublisher.counts()
	t.Logf("beta polled throughout the overrun and delivered %d of this run's events "+
		"(%d other runs', %d failures)", betaDelivered, betaStrangers, betaFailures)
	require.Zerof(t, betaDelivered,
		"beta must not publish a single row alpha is still holding: %d of this run's events were "+
			"published by a second instance while the first had them in flight", betaDelivered)

	alphaPublisher.release()

	states := fixture.waitForTerminalStates(ctx, seededEvents, recoveryDrainTimeout)

	alpha.Stop()
	beta.Stop()

	fixture.assertEveryRowFinishedCleanly(states, seededEvents)

	everyDelivery := append(append([]string(nil), alphaPublisher.deliveries()...), betaPublisher.deliveries()...)

	// NO DUPLICATE BEFORE IDEMPOTENCY FILTERING. Unlike the restart tests, this run staged no
	// crash and lapsed no lease, so a repeated publish has no legitimate explanation.
	duplicated := recoveryRepeated(everyDelivery)
	assert.Emptyf(t, duplicated,
		"no event may be published twice when no lease was ever allowed to lapse — a subscriber's "+
			"event_id filter is the last line of defence, not the first. Duplicated: %v", duplicated)

	// AND NOTHING WAS LOST while the batch was held.
	assert.ElementsMatch(t, seeded, recoveryUnique(everyDelivery),
		"every seeded event must have reached the transport exactly once between the two instances")

	recoveryRequireNoGoroutineLeak(t, baseline)
}

// TestEventRecovery_AttemptsSurviveRestartAndBoundTheRetryBudget walks one row through its
// entire retry budget through the repository, so the attempt counter is OBSERVED at every step
// rather than inferred from a relay's timing.
//
// Two opposite failures are being ruled out, and both are silent:
//
//   - attempts RESETTING across a re-claim, which turns a permanently undeliverable event into
//     an infinite retry loop that never reaches a dead-letter topic;
//   - attempts JUMPING, which dead-letters a healthy event that merely survived a restart.
//
// It then asserts the two properties that make a spent budget safe: the row leaves the
// claimable set for good, and it is still VISIBLE in the dead-letter inventory — because a row
// with a spent budget and no dead-letter record is the permanently unclaimable failure mode
// this file exists to catch.
func TestEventRecovery_AttemptsSurviveRestartAndBoundTheRetryBudget(t *testing.T) {
	fixture := newRecoveryFixture(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	seeded := fixture.seed(ctx, 1)
	eventID := seeded[0]

	const retryAfter = 300 * time.Millisecond

	recordedAttempts := 0
	exhaustionToken := ""
	rowID := int64(0)

	for attempt := 1; attempt <= recoveryMaxAttempts; attempt++ {
		claimed := fixture.claimMine(ctx, 1, recoveryLease)
		require.Lenf(t, claimed, 1, "the row must be claimable for attempt %d", attempt)

		row := claimed[0]
		rowID = row.ID

		require.Equalf(t, recordedAttempts, row.Attempts,
			"attempt %d must see the attempt counter exactly as the previous failure left it: a reset would let a "+
				"permanently failing event retry for ever, and a jump would dead-letter a healthy one", attempt)

		// terminal=false, because this fixture is simulating a TRANSIENT transport failure:
		// the whole point is that the budget bounds the retries, so declaring the failure
		// permanent would exhaust the row on attempt one and the boundary would go untested.
		// The trailing argument is the dead-letter hand-off lease. It only governs the
		// exhaustion arm, which the final attempt in this loop does take, so a real duration is
		// supplied rather than zero: on that attempt the row keeps its claim token, and the
		// lease is what keeps the token exclusive until the dead-letter write lands.
		outcome, err := fixture.ds.MarkEventFailed(ctx, row.ID, row.ClaimToken,
			fmt.Sprintf("recovery test: simulated transport failure %d", attempt), retryAfter, false,
			recoveryDeadLetterHandoffLease)
		require.NoErrorf(t, err, "recording failed attempt %d", attempt)
		require.Equalf(t, attempt, outcome.Attempts,
			"each recorded failure must advance the counter by exactly one (attempt %d)", attempt)

		recordedAttempts = outcome.Attempts

		if attempt < recoveryMaxAttempts {
			require.Falsef(t, outcome.Exhausted, "the budget must not be spent at attempt %d of %d", attempt, recoveryMaxAttempts)
			require.Equalf(t, model.EventOutboxStatusPending, outcome.Status,
				"a retryable failure must return the row to the claimable set (attempt %d)", attempt)

			// THE BACKOFF IS PERSISTED, not slept: an immediate claim must refuse the row because
			// it is not due yet. Without this the configured schedule — four waits of 1s, 2s, 4s
			// and 8s between the five attempts — collapses into consecutive attempts against a
			// broker that has barely begun to fail.
			early, claimErr := fixture.ds.ClaimPendingEventOutbox(ctx, 100, recoveryLease)
			require.NoError(t, claimErr)
			assert.Emptyf(t, fixture.mine(early),
				"the persisted next_attempt_at must keep the row out of the claimable set until it is due (attempt %d)",
				attempt)

			time.Sleep(retryAfter + 200*time.Millisecond)

			continue
		}

		require.True(t, outcome.Exhausted, "the budget must be reported spent on the final attempt")
		require.Equal(t, model.EventOutboxStatusFailed, outcome.Status,
			"an exhausted row is failed, and only the dead-letter writer may call it dead_lettered")
		require.NotEmpty(t, outcome.ClaimToken,
			"the exhaustion arm must retain the claim token, or two workers could both dead-letter the event")

		exhaustionToken = outcome.ClaimToken
	}

	// A spent budget must END the retry loop.
	exhausted, err := fixture.ds.ClaimPendingEventOutbox(ctx, 100, recoveryLease)
	require.NoError(t, err)
	assert.Empty(t, fixture.mine(exhausted),
		"a row whose retry budget is spent must not be claimable again; the claim predicate admits only "+
			"attempts < max_attempts")

	failed, err := fixture.ds.GetEventByID(ctx, eventID)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, model.EventOutboxStatusFailed, failed.Status)
	assert.Equal(t, recoveryMaxAttempts, failed.Attempts, "every attempt of the budget must be recorded")
	assert.NotEmpty(t, failed.LastError, "the failure reason must be stored for an operator to triage from")

	// ...and it must be VISIBLE while it waits for its dead-letter write, which is why the
	// inventory covers `failed` as well as `dead_lettered`.
	assert.True(t, fixture.inDeadLetterInventory(ctx, eventID),
		"an event with a spent budget must appear in the dead-letter inventory even before its dead-letter write "+
			"lands, or it is invisible to an operator")

	// The hand-off completes the failure path and makes the row terminal.
	metadata, err := json.Marshal(map[string]interface{}{
		"original_topic": failed.Topic,
		"error_reason":   failed.LastError,
		"attempt_count":  failed.Attempts,
	})
	require.NoError(t, err)
	require.NoError(t, fixture.ds.MarkEventDeadLettered(ctx, rowID, exhaustionToken, DLTFor(failed.Topic), metadata, model.BrokerRecord{}),
		"the worker that spent the last attempt holds the token, so it must be able to complete the hand-off")

	states := fixture.snapshot(ctx)
	require.Len(t, states, 1)
	assert.Equal(t, model.EventOutboxStatusDeadLettered, states[0].status)
	assert.Equal(t, DLTFor(failed.Topic), states[0].dltTopic,
		"the dead-letter topic the event was preserved on must be recorded")
	assert.True(t, states[0].terminal(), "a dead-lettered row has finished")
	assert.False(t, states[0].unclaimable(),
		"a dead-lettered row is terminal and recorded, so it is not the unclaimable failure mode")
}

// TestEventRecovery_ConcurrentRelaysShareTheBacklogWithoutDoubleProcessing is the FOR UPDATE
// SKIP LOCKED property, which is what makes running more than one relay instance safe — and
// therefore what makes a restart safe before the old instance has finished dying.
//
// The rendezvous is deterministic rather than timing-based. Relay alpha is frozen after ONE
// delivery while holding a claimed batch, so its rows are demonstrably locked; relay beta must
// then drain everything else. A test that merely started two relays and hoped both got work
// would usually see the first one chain through the entire backlog in its first tick and would
// then assert nothing about concurrency at all.
//
// The lease here is the production default, long enough that nothing can expire mid-test, so
// "no row was processed by both" is a statement about SKIP LOCKED rather than about timing.
func TestEventRecovery_ConcurrentRelaysShareTheBacklogWithoutDoubleProcessing(t *testing.T) {
	fixture := newRecoveryFixture(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const seededEvents = 60
	const betaProgress = 10

	seeded := fixture.seed(ctx, seededEvents)

	alphaPublisher := newRecoveryPublisher("relay-alpha", nil, seeded).freezeAfter(1, 1)
	betaPublisher := newRecoveryPublisher("relay-beta", nil, seeded)

	baseline := runtime.NumGoroutine()

	alpha := fixture.relay(alphaPublisher).
		WithBatchSize(5).
		WithConcurrency(1).
		WithLockDuration(recoveryNoExpiryLease)
	beta := fixture.relay(betaPublisher).
		WithBatchSize(20).
		WithLockDuration(recoveryNoExpiryLease)

	t.Cleanup(func() {
		alphaPublisher.release()
		alpha.Stop()
		beta.Stop()
	})

	require.NoError(t, alpha.Start(ctx), "alpha refused to start; the returned obstacle names why")

	_, alphaParked := alphaPublisher.awaitFreeze(t, recoveryGateTimeout)
	require.GreaterOrEqual(t, alphaParked, 1,
		"alpha must be holding a claimed batch it cannot finish before beta is asked to work around it")

	// Alpha now holds a claimed batch it cannot finish. Beta must still make progress.
	require.NoError(t, beta.Start(ctx), "beta refused to start; the returned obstacle names why")
	fixture.awaitDeliveries(betaPublisher, betaProgress, recoveryGateTimeout)

	betaWhileAlphaHeld, _, _ := betaPublisher.counts()
	t.Logf("beta delivered %d events while alpha held a claimed batch it could not finish", betaWhileAlphaHeld)
	require.GreaterOrEqual(t, betaWhileAlphaHeld, betaProgress,
		"a second relay instance must make progress on other rows while one instance holds a locked batch; "+
			"without SKIP LOCKED it would block behind alpha's claim")

	alphaPublisher.release()

	states := fixture.waitForTerminalStates(ctx, seededEvents, recoveryDrainTimeout)

	alpha.Stop()
	beta.Stop()

	fixture.assertEveryRowFinishedCleanly(states, seededEvents)

	alphaDelivered := recoveryUnique(alphaPublisher.deliveries())
	betaDelivered := recoveryUnique(betaPublisher.deliveries())

	t.Logf("alpha delivered %d events, beta delivered %d, of %d seeded",
		len(alphaDelivered), len(betaDelivered), seededEvents)

	assert.Positive(t, len(alphaDelivered), "alpha must have published the rows it claimed")
	assert.Positive(t, len(betaDelivered), "beta must have published the rest")

	// NONE SKIPPED: between them the two instances covered the whole backlog.
	assert.ElementsMatch(t, seeded, recoveryUnique(append(append([]string(nil), alphaDelivered...), betaDelivered...)),
		"the two instances together must cover every seeded event; a gap means a row was skipped by both")

	// NONE PROCESSED BY BOTH: no lease expired, so an overlap could only come from two
	// instances claiming the same row.
	overlap := recoveryIntersection(alphaDelivered, betaDelivered)
	assert.Emptyf(t, overlap,
		"no row may be processed by both instances while their leases are live; overlap: %v", overlap)

	recoveryRequireNoGoroutineLeak(t, baseline)
}

// ---------------------------------------------------------------------------
// Dead-letter preservation — recovering the last resort
// ---------------------------------------------------------------------------

// TestEventRecovery_ADeadLetterWriteThatFailedIsRetriedUntilTheEventIsPreserved covers the one
// recovery path that has no retry budget behind it, because there is nothing further to fall
// back to.
//
// # The limbo it ends
//
// When a row spends its retry budget the relay hands it to the dead-letter writer, which writes
// the event to its `<topic>.dlt` sibling and only then records the terminal state. If that WRITE
// fails — no transport, a broker outage, a topic that does not exist yet — the row is left at
// 'failed' with dlt_topic still NULL, and that was a dead end in three directions at once: the
// ordinary claim predicate admits only attempts < max_attempts, so it will never take the row
// again; replay accepts only 'dead_lettered', so an operator cannot re-drive it; and the worker
// holding the claim token had already moved on. The event existed ONLY as that row. It appeared
// in the dead-letter inventory — the listing covers 'failed' as well as 'dead_lettered' precisely
// so it would — and absolutely nothing in the system would ever act on it again.
//
// # What is real here and what is substituted
//
// Only the raw Kafka write is substituted. The exhaustion arm, the claim that finds the stranded
// row, the message composition, the routing and the transition that records preservation are all
// the production code paths running against the real database, which is the point of asserting
// this here rather than only against a fake store: the repair depends on a claim predicate that
// deliberately admits a status the ordinary claim excludes, and that is a property of SQL.
//
// # Why the reproduction is asserted before the repair
//
// The limbo state is asserted while the transport is still refusing, because a test that only
// checked the end state would pass just as happily against a build where the first dead-letter
// write never failed at all — proving nothing about the repair. The refusal is held open rather
// than counted down for the same reason: the window would otherwise be a race.
func TestEventRecovery_ADeadLetterWriteThatFailedIsRetriedUntilTheEventIsPreserved(t *testing.T) {
	fixture := newRecoveryFixture(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const seededEvents = 2

	seeded := fixture.seedWithBudget(ctx, seededEvents, recoverySpentBudget)

	publisher := newRecoveryPublisher("relay-dead-letter", nil, seeded).refuseEveryPublish()
	writer := &recoveryDeadLetterWriter{}

	baseline := runtime.NumGoroutine()

	relay := fixture.relay(publisher).WithBatchSize(recoveryOversizedBatch)
	// The production service over the production repository, with the broker replaced. Assigned
	// after construction because NewEventRelayProcessor builds its own from the publisher, which
	// would resolve to no transport at all here and fail the write for the wrong reason.
	relay.deadLetters = fixture.deadLetterService(publisher, writer)

	t.Cleanup(func() {
		writer.release()
		relay.Stop()
	})

	require.NoError(t, relay.Start(ctx),
		"the relay refused to start; the returned obstacle names the missing precondition")

	// STEP 1 — the hazard is reproduced. Every row spends its budget on its first publish and
	// its dead-letter write is refused, so each one lands in the limbo described above.
	for _, eventID := range seeded {
		state := fixture.awaitRow(ctx, eventID, func(state recoveryRowState) bool {
			return state.status == model.EventOutboxStatusFailed && state.dltTopic == ""
		}, recoveryGateTimeout, "spend its retry budget with its dead-letter write refused")

		require.Truef(t, state.unclaimable(),
			"event %s must be in the state this test exists to recover from — a spent budget "+
				"(%d/%d) with no dead-letter record — or the repair is not being exercised at all",
			eventID, state.attempts, state.maxAttempts)

		assert.Containsf(t, state.lastError, errRecoveryTransportRefused.Error(),
			"event %s must record WHY its budget was spent: that reason is what the dead-letter "+
				"metadata has to report when the preservation is finally retried", eventID)
	}

	// STEP 2 — the relay keeps coming back for it. More write attempts than there are rows can
	// only come from the repair pass re-claiming rows the ordinary claim will never admit again.
	require.Eventuallyf(t, func() bool {
		return writer.attemptCount() > seededEvents
	}, recoveryGateTimeout, recoveryStateInterval,
		"the relay must retry the dead-letter write of a row whose preservation failed: %d rows were "+
			"handed off and only %d writes have been attempted, so nothing is re-claiming them",
		seededEvents, writer.attemptCount())

	t.Logf("the dead-letter transport refused %d writes before being released", writer.attemptCount())

	// STEP 3 — with the transport back, the repair completes through the ordinary transition.
	writer.release()

	states := fixture.waitForTerminalStates(ctx, seededEvents, recoveryDrainTimeout)

	relay.Stop()

	fixture.assertEveryRowWasPreserved(states, seededEvents, DLTFor(fixture.topic))

	// The inventory an operator triages from must still list every one of them, now with a
	// dead-letter record behind it rather than nothing at all.
	for _, state := range states {
		assert.Truef(t, fixture.inDeadLetterInventory(ctx, state.eventID),
			"event %s must remain in the dead-letter inventory after being preserved", state.eventID)
	}

	// The metadata must report WHAT ENDED THE EVENT'S RETRY BUDGET, not the repair. The repair
	// rebuilds the cause from the row's own last_error precisely so that an operator reading the
	// dead-letter message learns why the event failed rather than that it was recovered.
	preserved := recoveryDecodeDeadLetters(t, writer.messages())

	for _, eventID := range seeded {
		metadata, ok := preserved[eventID]
		require.Truef(t, ok,
			"event %s must have reached its dead-letter topic; preserved: %d of %d",
			eventID, len(preserved), seededEvents)

		assert.Containsf(t, metadata.ErrorReason, errRecoveryTransportRefused.Error(),
			"the dead-letter metadata of event %s must report the publish failure that spent its "+
				"budget, not the dead-letter write that was retried", eventID)
		assert.Equalf(t, fixture.topic, metadata.OriginalTopic,
			"the dead-letter metadata of event %s must name the topic a replay has to send it back to",
			eventID)
		assert.Equalf(t, recoverySpentBudget, metadata.AttemptCount,
			"the dead-letter metadata of event %s must report the attempts the database recorded",
			eventID)
	}

	recoveryRequireNoGoroutineLeak(t, baseline)
}

// TestEventRecovery_ARepairPassRacingTheHandOffProducesExactlyOneDeadLetterWrite is the
// duplicate-dead-letter race, driven deterministically against the real table.
//
// # The defect this covers
//
// Two statements hand a dead-letter write to one worker: MarkEventFailed's exhaustion arm and
// MarkEventPermanentlyFailed. Both RETAIN the row's claim token, and the retained token is
// documented as what stops two workers each putting a copy of one event on one `<topic>.dlt`
// topic. Both also used to RELEASE the row's lease in the same statement — `locked_until =
// NULL` — on the stated reasoning that a failed row is outside the claimable set.
//
// It is not. claimFailedEventOutboxForDeadLetter, the repair pass this file already covers,
// admits exactly `status = failed AND dlt_topic IS NULL AND (locked_until IS NULL OR
// locked_until < NOW())`. A released lease satisfies that predicate IMMEDIATELY, so the row
// whose dead-letter write had just been handed to one worker was re-claimable by the very next
// poll of any other relay instance — with a FRESH token, which is what makes the second write
// pass its own MarkEventDeadLettered. The retained token did not prevent the duplicate; it only
// decided which of the two workers got to record it. One event, two copies on one dead-letter
// topic, and an operator replaying from an inventory that now double-counts.
//
// # Why the assertion is a WRITE count and not a database state
//
// The duplicate lands in KAFKA, not in PostgreSQL. Whichever worker loses the
// MarkEventDeadLettered race reports a lost claim and logs it — but its message is already on
// the topic. So the property is counted at the transport: exactly one write attempt, ever, for
// one event. A test that asserted only the final row state would have passed against the
// defect.
//
// # Why the window is held open rather than raced for
//
// The interval between the hand-off and the write completing has no natural duration, so two
// workers started together would miss it almost every time and the test would be
// non-deterministic in the direction that matters — passing while the defect is present. The
// barrier transport parks the first write inside WriteMessages, which makes the interval last
// exactly as long as the assertions need.
//
// The second worker is driven through recoverUnpreservedDeadLetters, the production caller, and
// the raw claim is exercised beside it: the pass proves the relay does not adopt the row, and
// the claim proves the SQL predicate is why.
func TestEventRecovery_ARepairPassRacingTheHandOffProducesExactlyOneDeadLetterWrite(t *testing.T) {
	fixture := newRecoveryFixture(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// One row with a one-attempt budget: its first failure spends the budget, which is the
	// transition that hands the dead-letter write to the worker recording it.
	seeded := fixture.seedWithBudget(ctx, 1, recoverySpentBudget)
	eventID := seeded[0]

	claimed := fixture.claimMine(ctx, 1, recoveryHandOffRaceLease)
	require.Len(t, claimed, 1)

	row := claimed[0]
	require.NotEmpty(t, row.ClaimToken, "the claim must issue a token; the hand-off is expressed through it")

	// WORKER A spends the budget. The exhaustion arm must retain the token AND hold the lease.
	outcome, err := fixture.ds.MarkEventFailed(ctx, row.ID, row.ClaimToken,
		errRecoveryTransportRefused.Error(), 0, false, recoveryHandOffRaceLease)
	require.NoError(t, err)
	require.True(t, outcome.Exhausted, "a one-attempt budget must be spent by one failure")
	require.Equal(t, model.EventOutboxStatusFailed, outcome.Status)
	require.NotEmpty(t, outcome.ClaimToken,
		"the exhaustion arm must hand the dead-letter write to this worker by retaining its token")

	// The row worker A carries into the dead-letter write is the one the database now holds, so
	// the service composes its message and takes its transition from real state.
	handedOff, err := fixture.ds.GetEventByID(ctx, eventID)
	require.NoError(t, err)
	require.NotNil(t, handedOff)
	require.Equal(t, outcome.ClaimToken, handedOff.ClaimToken)

	// THE LEASE IS THE PRECONDITION OF THE WHOLE TEST, so it is asserted rather than assumed: a
	// released lease here means the fix under test is absent and every assertion below would be
	// measuring the wrong thing.
	preRace := fixture.snapshot(ctx)
	require.Len(t, preRace, 1)
	require.True(t, preRace[0].leaseHeld,
		"the exhaustion arm must HOLD a lease over a row whose dead-letter write it just handed off")
	require.True(t, preRace[0].leaseLive,
		"and that lease must still be live: an already-expired lease is the race, not protection from it")

	// Worker A's dead-letter write, parked inside the transport.
	barrier := newRecoveryBarrierDeadLetterWriter()
	t.Cleanup(barrier.release)

	publisherA := newRecoveryPublisher("hand-off-owner", nil, seeded).refuseEveryPublish()
	serviceA := fixture.deadLetterService(publisherA, barrier)

	handOff := make(chan error, 1)

	go func() {
		_, dltErr := serviceA.DeadLetter(ctx, *handedOff, errRecoveryTransportRefused)
		handOff <- dltErr
	}()

	barrier.awaitEntered(t, recoveryGateTimeout)

	// ------------------------------------------------------------------
	// THE RACE, with worker A's write demonstrably in flight.
	// ------------------------------------------------------------------

	// The production caller first. Its transport is released, so if the repair claimed the row
	// it would write immediately and the count below would be 1 rather than 0.
	publisherB := newRecoveryPublisher("repair-pass", nil, seeded).refuseEveryPublish()
	writerB := &recoveryDeadLetterWriter{}
	writerB.release()

	relayB := fixture.relay(publisherB)
	relayB.deadLetters = fixture.deadLetterService(publisherB, writerB)

	repaired := relayB.recoverUnpreservedDeadLetters(ctx)
	assert.Zero(t, repaired,
		"the repair pass must not adopt a row whose dead-letter write is still owed by a live worker: "+
			"adopting it stamps a fresh token, and the fresh token is what lets a SECOND copy of one "+
			"event onto one .dlt topic")
	assert.Zero(t, writerB.attemptCount(),
		"and it must therefore not have written anything: this count is the duplicate, and it is the "+
			"assertion that fails when the hand-off lease is released")

	// The predicate underneath it, so a future change that made the pass skip the row for some
	// other reason cannot silently replace the protection being asserted.
	contended, err := fixture.ds.ClaimFailedEventOutboxForDeadLetter(ctx, recoveryOversizedBatch, time.Minute)
	require.NoError(t, err)
	assert.Emptyf(t, fixture.mine(contended),
		"the repair CLAIM must exclude the row on the lease alone: its predicate admits failed rows "+
			"with no dead-letter record whose locked_until is NULL or past, so the held lease is the "+
			"only thing standing between this event and a duplicate (claim returned %d rows)",
		len(contended))

	// ------------------------------------------------------------------
	// The hand-off completes. Exactly one write, and it is worker A's.
	// ------------------------------------------------------------------
	barrier.release()
	require.NoError(t, <-handOff,
		"the worker holding the token must be able to complete the hand-off it was given")

	assert.Equal(t, 1, barrier.attemptCount(),
		"exactly one dead-letter write may ever be attempted for one event")
	assert.Len(t, barrier.messages(), 1,
		"and exactly one message may reach the dead-letter topic")
	assert.Zero(t, writerB.attemptCount(),
		"the repair pass must still have written nothing after the hand-off completed")

	preserved := recoveryDecodeDeadLetters(t, barrier.messages())
	metadata, ok := preserved[eventID]
	require.Truef(t, ok, "the one dead-letter message must preserve event %s", eventID)
	assert.Equal(t, fixture.topic, metadata.OriginalTopic,
		"the preserved message must name the topic a replay sends it back to")

	// The terminal transition releases what the hand-off held. Left held, the row would sit
	// un-repairable until the clock passed it — harmless here, because dlt_topic is now set and
	// the repair predicate excludes it, but the release is what keeps those two defences
	// independent.
	states := fixture.snapshot(ctx)
	require.Len(t, states, 1)
	assert.Equal(t, model.EventOutboxStatusDeadLettered, states[0].status,
		"the event must end preserved on its dead-letter topic")
	assert.Equal(t, DLTFor(fixture.topic), states[0].dltTopic)
	assert.False(t, states[0].leaseHeld,
		"the dead-letter transition must release the lease it was protected by; nothing is owed any more")
	assert.Empty(t, states[0].claimToken,
		"and it must release the token: a terminal row belongs to nobody")

	// And nothing comes back for it. This is the second, independent defence: dlt_topic is no
	// longer NULL, so the repair predicate excludes the row whatever its lease says.
	after, err := fixture.ds.ClaimFailedEventOutboxForDeadLetter(ctx, recoveryOversizedBatch, time.Minute)
	require.NoError(t, err)
	assert.Empty(t, fixture.mine(after),
		"a preserved row must never be re-claimed: repairing it again would rewrite its dead-letter "+
			"record and put another copy on the topic on every poll for ever")
}

// TestEventRecovery_EventIDIsUniqueInTheOutbox asserts the index that makes event_id a
// trustworthy idempotency key.
//
// The index bounds recapture only as far as the ID is stable. A capture whose id is DERIVED from
// the mutation collides here and is refused; one that mints a random id — the repeatable event
// types do — enters as a distinct row that no consumer-side de-duplication can collapse. That is
// why the derived-id rule in model.EventIdentityFor matters as much as this index does.
func TestEventRecovery_EventIDIsUniqueInTheOutbox(t *testing.T) {
	fixture := newRecoveryFixture(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	seeded := fixture.seed(ctx, 1)

	original, err := fixture.ds.GetEventByID(ctx, seeded[0])
	require.NoError(t, err)
	require.NotNil(t, original)

	payload, err := json.Marshal(NewWebhook{
		Event:   recoveryEventType,
		Payload: map[string]interface{}{"transaction_id": "a second recording of one event"},
	})
	require.NoError(t, err)

	duplicate := &model.EventOutbox{
		EventID:       seeded[0],
		EventType:     recoveryEventType,
		AggregateID:   fixture.partitionKey(99999),
		PartitionKey:  fixture.partitionKey(99999),
		Topic:         fixture.topic,
		SchemaVersion: model.SchemaVersionV1,
		Payload:       payload,
		OccurredAt:    time.Now().UTC(),
		MaxAttempts:   recoveryMaxAttempts,
	}

	insertErr := fixture.ds.InsertEventOutbox(ctx, duplicate)
	require.Error(t, insertErr,
		"a second row carrying an existing event_id must be REFUSED; event_id is the subscriber's idempotency key")
	assert.Contains(t, insertErr.Error(), "already exists",
		"the refusal must be reported as a conflict rather than an opaque driver error: %v", insertErr)

	states := fixture.snapshot(ctx)
	assert.Len(t, states, 1, "exactly one row may exist for one event id")

	survivor, err := fixture.ds.GetEventByID(ctx, seeded[0])
	require.NoError(t, err)
	require.NotNil(t, survivor)
	assert.Equal(t, original.ID, survivor.ID, "the original row must be untouched by the refused insert")
	assert.JSONEq(t, string(original.Payload), string(survivor.Payload),
		"the refused insert must not have overwritten the recorded event body")
}

// ---------------------------------------------------------------------------
// The same crash, against a live broker
// ---------------------------------------------------------------------------

// TestEventRecovery_MidBatchRestartDeliversEveryEventToKafka repeats the crash and restart
// against a REAL broker and then reads the topic back, so "delivered" means a message a
// consumer can actually see rather than a call the relay believes succeeded.
//
// It is deliberately narrow. The deterministic assertions about row states, lease expiry and
// the attempt counter belong to the tests above, which need no broker and therefore run
// wherever PostgreSQL is available. This one answers the single question they cannot: after an
// interruption and a restart, is every seeded event ON THE TOPIC?
//
// The topic is read by OFFSET DELTA — the end offsets are captured before seeding and again
// after the drain, and only that window is consumed — because the topics are shared with every
// other test on the stack. Consumed ids outside this run's seeded set are other runs' events
// and are filtered out rather than asserted on. NO TOPIC IS CREATED OR DELETED here for the
// same reason.
func TestEventRecovery_MidBatchRestartDeliversEveryEventToKafka(t *testing.T) {
	recoverySkipUnlessIntegration(t)

	user, secret, credentialsPresent := recoveryKafkaCredentials()
	if !credentialsPresent {
		t.Skip("skipping the live-broker recovery test: KAFKA_SASL_ADMIN_USER and KAFKA_SASL_ADMIN_SECRET " +
			"must be set for the test to authenticate. The local stack's values are printed by the Kafka " +
			"provisioning script; no credential is defaulted in this test.")
	}

	// The publisher authenticates as its OWN principal, never as the administrator, so the
	// producer pair is required in addition to the administrative one. Skipping rather than
	// failing keeps the suite green on a machine that has a broker but has not provisioned a
	// producer, while still refusing to run the test in a configuration no deployment may use.
	producerUser, producerSecret, producerPresent := recoveryProducerCredentials()
	if !producerPresent {
		t.Skip("skipping the live-broker recovery test: KAFKA_SASL_USER and KAFKA_SASL_SECRET must be set. " +
			"Blnk refuses to publish as the administrative principal, so a dedicated producer principal with " +
			"Write and Describe on the Blnk-owned topics is required; scripts/kafka-provision.sh creates one " +
			"for the local stack.")
	}

	brokers := recoveryBrokers()
	require.NotEmpty(t, brokers, "KAFKA_BROKERS resolved to nothing")
	for _, broker := range brokers {
		recoveryRequireReachable(t, "the Kafka broker", broker,
			"Start it with `docker compose --profile kafka up -d kafka kafka-init` — the two services sit "+
				"behind the \"kafka\" profile, so a bare `docker compose up` starts neither — or point "+
				"KAFKA_BROKERS at a reachable broker.")
	}

	fixture := newRecoveryFixture(t, &config.KafkaConfig{
		Brokers:     brokers,
		TopicPrefix: DefaultTopicPrefix,
		// Two principals, and which one is used is decided by the ROLE of the client being
		// built: the admin client below resolves the administrative pair, the publisher
		// resolves the producer pair. Supplying both is what lets one configuration drive
		// both halves of this test exactly as a deployment does.
		SASLUser:        producerUser,
		SASLSecret:      producerSecret,
		SASLAdminUser:   user,
		SASLAdminSecret: secret,
		MinPartitions:   6,
		// One, because the local stack is a single broker: a replication factor of three is
		// the production value and cannot be satisfied here. Nothing in this test creates a
		// topic, so the value only has to be valid.
		ReplicationFactor: 1,
		// The local broker speaks SASL/SCRAM over plaintext, which the transport refuses
		// unless this acknowledgement is present.
		InsecureLocalDev: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	admin, err := NewKafkaAdmin(fixture.conf)
	require.NoError(t, err, "building the Kafka admin client")
	require.True(t, admin.IsConfigured(), "the admin client must be configured against the broker list")

	t.Cleanup(func() {
		if closeErr := admin.Close(); closeErr != nil {
			t.Logf("closing the Kafka admin client: %v", closeErr)
		}
	})

	before := fixture.topicEndOffsets(ctx, admin)

	transport, err := NewEventPublisher(fixture.conf)
	require.NoError(t, err, "building the Kafka event publisher")
	require.False(t, IsNoopEventPublisher(transport),
		"the live-broker test must resolve the real publisher; the no-op would report every publish dispatched "+
			"without sending anything")

	topical, isTopical := transport.(TopicEventPublisher)
	require.True(t, isTopical, "the publisher must accept a destination topic")

	// One claim's worth is 20 here, so 60 rows leave two further claims behind — the same
	// mid-batch shape as the deterministic test, at a size that is quick against a real broker.
	const seededEvents = 60
	const batchSize = 20
	const freezeAfter = 8

	seeded := fixture.seed(ctx, seededEvents)

	publisher := newRecoveryPublisher("kafka-relay", topical, seeded).
		freezeAfter(freezeAfter, recoveryPublishesInFlight)
	t.Cleanup(func() {
		if closeErr := publisher.Close(); closeErr != nil {
			t.Logf("closing the Kafka event publisher: %v", closeErr)
		}
	})

	crashCtx, crash := context.WithCancel(ctx)
	first := fixture.relay(publisher).WithBatchSize(batchSize)

	t.Cleanup(func() {
		publisher.release()
		crash()
		first.Stop()
	})

	require.NoError(t, first.Start(crashCtx),
		"the relay refused to start; the returned obstacle names the missing precondition")
	require.True(t, first.IsRunning(), "the relay must be running before it can be interrupted")

	_, parkedAtCrash := publisher.awaitFreeze(t, recoveryGateTimeout)
	require.GreaterOrEqual(t, parkedAtCrash, recoveryPublishesInFlight,
		"the crash must land on publishes that are actually in flight to the broker")

	crash()
	first.Stop()

	deliveredAtCrash := len(publisher.deliveries())
	t.Logf("interrupted with %d of %d seeded events acknowledged by the broker", deliveredAtCrash, seededEvents)
	require.Positive(t, deliveredAtCrash, "the relay must have published something before the interruption")
	require.Less(t, deliveredAtCrash, seededEvents,
		"the interruption must land mid-batch; every event was already published, so this run proved nothing")

	publisher.release()

	restartCtx, stopRestart := context.WithCancel(ctx)
	second := fixture.relay(publisher).WithBatchSize(batchSize)
	t.Cleanup(func() {
		stopRestart()
		second.Stop()
	})

	require.NoError(t, second.Start(restartCtx),
		"the restarted relay refused to start; the returned obstacle names the missing precondition")

	states := fixture.waitForTerminalStates(ctx, seededEvents, recoveryDrainTimeout)

	second.Stop()
	stopRestart()

	fixture.assertEveryRowFinishedCleanly(states, seededEvents)

	after := fixture.topicEndOffsets(ctx, admin)
	advance := recoveryOffsetAdvance(before, after)
	t.Logf("%s advanced by %d messages across the run", fixture.topic, advance)
	assert.GreaterOrEqualf(t, advance, int64(seededEvents),
		"the topic must have advanced by at least the %d seeded events", seededEvents)

	consumed := fixture.consumeEventIDs(ctx, brokers, user, secret, before, after)
	mine := recoveryUnique(recoveryOnlyKnown(consumed, seeded))

	t.Logf("read %d messages from the offset window, %d of them this run's", len(consumed), len(mine))

	// THE END-TO-END GUARANTEE: every seeded event is on the topic, and de-duplicating on
	// event_id yields exactly the seeded set. A duplicate on the wire is permitted — see the
	// file comment — and is why the comparison is made against the de-duplicated set.
	assert.ElementsMatch(t, seeded, mine,
		"every seeded event must be readable from %s after the interruption and restart; a missing id is a lost "+
			"event", fixture.topic)
}

// topicEndOffsets reads the current end offset of every partition of this run's topic.
//
// The snapshot cache is invalidated first: it exists so the metrics path can sweep many
// subscribers cheaply, and a cached value here would report offsets from before the run.
func (f *recoveryFixture) topicEndOffsets(ctx context.Context, admin *KafkaAdminClient) map[int]int64 {
	f.t.Helper()

	admin.InvalidateOffsetSnapshot()

	// A zero instant asks for end offsets only: this reads where the log HEAD is,
	// and a window would add a round trip whose answer nothing here reads.
	report, err := admin.TopicEndOffsets(ctx, time.Time{}, f.topic)
	if err != nil {
		f.t.Skipf("skipping: could not read the end offsets of %s (%v). Provision the topics with "+
			"`docker compose up kafka-init`.", f.topic, err)
	}

	for _, missing := range report.MissingTopics {
		if missing == f.topic {
			f.t.Skipf("skipping: topic %s does not exist on the broker. Provision it with "+
				"`docker compose up kafka-init`.", f.topic)
		}
	}

	offsets := make(map[int]int64, 8)
	for _, snapshot := range report.Topics {
		if snapshot.Topic != f.topic {
			continue
		}

		for _, partition := range snapshot.Partitions {
			if partition.Unavailable {
				continue
			}

			offsets[partition.Partition] = partition.EndOffset
		}
	}

	require.NotEmptyf(f.t, offsets, "topic %s reported no readable partitions", f.topic)

	return offsets
}

// consumeEventIDs reads the offset window [before, after) of every partition and returns the
// event ids it found, in the order they were read.
//
// It reads by PARTITION AND EXPLICIT OFFSET rather than joining a consumer group: the window
// is known exactly, so there is nothing to coordinate, no group offsets to commit and nothing
// left behind on the broker for the next test to trip over. Every read is bounded by a
// deadline, so a broker that stops delivering fails with the partition and offset it stopped
// at instead of hanging the suite.
func (f *recoveryFixture) consumeEventIDs(
	ctx context.Context,
	brokers []string,
	user, secret string,
	before, after map[int]int64,
) []string {
	f.t.Helper()

	mechanism, err := scram.Mechanism(scram.SHA512, user, secret)
	require.NoError(f.t, err, "building the SASL/SCRAM mechanism for the consumer")

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
	}

	eventIDs := make([]string, 0, 128)

	for partition, start := range before {
		end, present := after[partition]
		if !present || end <= start {
			continue
		}

		eventIDs = append(eventIDs, f.consumePartition(ctx, brokers, dialer, partition, start, end)...)
	}

	return eventIDs
}

// consumePartition reads one partition's window and returns the event ids of the envelopes it
// contains.
func (f *recoveryFixture) consumePartition(
	ctx context.Context,
	brokers []string,
	dialer *kafka.Dialer,
	partition int,
	start, end int64,
) []string {
	f.t.Helper()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     f.topic,
		Partition: partition,
		MinBytes:  1,
		MaxBytes:  recoveryConsumerMaxBytes,
		MaxWait:   500 * time.Millisecond,
		Dialer:    dialer,
	})

	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			f.t.Logf("closing the reader for %s partition %d: %v", f.topic, partition, closeErr)
		}
	}()

	require.NoErrorf(f.t, reader.SetOffset(start),
		"seeking %s partition %d to offset %d", f.topic, partition, start)

	eventIDs := make([]string, 0, int(end-start))

	for offset := start; offset < end; {
		readCtx, cancelRead := context.WithTimeout(ctx, recoveryConsumeTimeout)
		message, readErr := reader.ReadMessage(readCtx)
		cancelRead()

		if readErr != nil {
			f.t.Errorf("reading %s partition %d at offset %d (%d of %d messages in the window): %v",
				f.topic, partition, offset, len(eventIDs), end-start, readErr)

			break
		}

		offset = message.Offset + 1

		var envelope model.LedgerEvent
		if unmarshalErr := json.Unmarshal(message.Value, &envelope); unmarshalErr != nil {
			f.t.Errorf("the message at %s partition %d offset %d is not a LedgerEvent envelope: %v",
				f.topic, partition, message.Offset, unmarshalErr)

			continue
		}

		eventIDs = append(eventIDs, envelope.EventID)
	}

	return eventIDs
}

// recoveryOffsetAdvance sums how far every partition moved between two snapshots.
func recoveryOffsetAdvance(before, after map[int]int64) int64 {
	var advance int64
	for partition, end := range after {
		start, present := before[partition]
		if !present {
			start = 0
		}

		if end > start {
			advance += end - start
		}
	}

	return advance
}

// recoveryOnlyKnown keeps the values that belong to the allowed set. It is how another run's
// events, which legitimately share these topics, are excluded from this run's assertions.
func recoveryOnlyKnown(values, allowed []string) []string {
	permitted := make(map[string]struct{}, len(allowed))
	for _, value := range allowed {
		permitted[value] = struct{}{}
	}

	kept := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := permitted[value]; ok {
			kept = append(kept, value)
		}
	}

	return kept
}
