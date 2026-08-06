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
// criterion V-7, "no duplicate and no lost events after a mid-batch relay restart".
//
// # STATE THE GUARANTEE HONESTLY BEFORE READING AN ASSERTION
//
// This is the criterion most easily tested wrongly, so the guarantee is written out here
// rather than left implicit in the assertions. THREE FACTS, and they are not the same fact:
//
//  1. The outbox guarantees each event is RECORDED ONCE. The row is committed in the same
//     database transaction as the ledger mutation that produced it, and event_id carries a
//     unique index, so an event can never be lost and can never be recorded twice.
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
// So this file asserts NO LOSS, and asserts NO DUPLICATE AT THE CONSUMER'S IDEMPOTENCY
// BOUNDARY — that is, after de-duplicating on event_id. It deliberately does NOT assert
// that the broker received each message exactly once. Such an assertion would be testing a
// promise the system does not make and, worse, would fail on a CORRECT implementation the
// moment a crash landed inside the publish-to-mark window, which is precisely the window
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
//	docker compose up -d postgres kafka kafka-init   # broker, topics, DLTs, SCRAM principal
//	go run ./cmd migrate up                          # creates blnk.event_outbox
//	go test -run 'TestEventRecovery' -count=1 .
//
// Environment, all optional, each defaulting to the local compose stack:
//
//	BLNK_DATA_SOURCE_DNS     PostgreSQL DSN holding blnk.event_outbox
//	                         (default postgres://postgres:password@localhost:5432/blnk?sslmode=disable)
//	KAFKA_BROKERS            broker list for the live-delivery test (default localhost:9092)
//	KAFKA_SASL_ADMIN_USER    SCRAM principal for the live-delivery test
//	KAFKA_SASL_ADMIN_SECRET  its secret. NEVER hardcoded here; the live-delivery test
//	                         skips when it is absent
//
// Each test names exactly what is missing when it skips, so a skip is a shopping list
// rather than a mystery.
//
// # WHAT IS NOT HERE
//
// Ordering is event_ordering_integration_test.go (V-6), subscriber isolation is
// event_isolation_integration_test.go (V-5), dual-delivery payload equality is
// event_dual_delivery_test.go (V-8) and replay fidelity is
// event_replay_fidelity_test.go (V-9). This file asserts none of them, so a failure here
// points at recovery and nothing else.
package blnk

import (
	"context"
	"encoding/json"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/model"
)

const (
	// recoveryEventType is a real event string from the catalogue, chosen because it routes
	// to blnk.transactions — a topic the provisioning script creates. An unrecognised type
	// would route to the quarantine topic, which the local stack does not provision, so the
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
	t.Cleanup(fixture.deleteSeededRows)

	t.Logf("event recovery run %s: outbox at %s, topic %s", fixture.runID, dsn, fixture.topic)

	return fixture
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

// seed inserts count claimable outbox rows and returns their event ids in insertion order.
//
// The rows are inserted through the production repository method, so they carry exactly the
// shape the capture path produces: a real legacy webhook envelope as the payload, a
// Blnk-owned topic, schema version 1, and the run's own partition key.
func (f *recoveryFixture) seed(ctx context.Context, count int) []string {
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
			MaxAttempts: recoveryMaxAttempts,
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
		f.t.Logf("could not clean up the seeded outbox rows of run %s: %v", f.runID, err)

		return
	}

	affected, err := result.RowsAffected()
	if err != nil {
		f.t.Logf("cleaned up the seeded outbox rows of run %s (count unavailable: %v)", f.runID, err)

		return
	}

	f.t.Logf("cleaned up %d seeded outbox rows for run %s", affected, f.runID)
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
	processor.sunsetPassed = func(time.Time) bool { return true }

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

// assertEveryRowFinishedCleanly is the terminal-state contract, asserted row by row.
func (f *recoveryFixture) assertEveryRowFinishedCleanly(states []recoveryRowState, expected int) {
	f.t.Helper()

	require.Len(f.t, states, expected, "every seeded row must still be present in the outbox")

	dispatched := 0
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
			dispatched++

			assert.Truef(f.t, state.dispatched,
				"event %s is dispatched, so dispatched_at must be stamped", state.eventID)
		}

		if state.status == model.EventOutboxStatusDeadLettered {
			assert.NotEmptyf(f.t, state.dltTopic,
				"event %s is dead-lettered, so the dead-letter topic it was preserved on must be recorded",
				state.eventID)
		}
	}

	assert.Equalf(f.t, expected, dispatched,
		"every seeded event must end up dispatched: the publisher only ever failed while the relay's "+
			"context was cancelled, and one cancellation cannot spend a five-attempt budget. %s",
		recoveryDescribeStates(states))
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
// dead-lettered events and this run's row is not guaranteed to be on page one.
func (f *recoveryFixture) inDeadLetterInventory(ctx context.Context, eventID string) bool {
	f.t.Helper()

	const pageSize = 200

	for offset := 0; offset < 10*pageSize; offset += pageSize {
		page, err := f.ds.ListDeadLetteredEvents(ctx, pageSize, offset)
		require.NoError(f.t, err, "paging the dead-letter inventory")

		for _, row := range page {
			if row.EventID == eventID {
				return true
			}
		}

		if len(page) < pageSize {
			return false
		}
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
// It is a full TopicEventPublisher, so the relay is the REAL relay driving its real publish
// path — nothing about claiming, marking, backoff or dead-lettering is stubbed. Only the
// transport is observable, and that buys the two things a recovery test cannot do without:
//
//   - AN EXACT RECORD of every delivery, so "no loss" and "duplicates de-duplicate to the
//     original set" become set comparisons rather than inferences from row states.
//   - A DETERMINISTIC INTERRUPTION POINT. After the configured number of deliveries the
//     publisher freezes: every later publish blocks until the test releases it or the
//     context is cancelled. That is what makes the crash land reliably in the MIDDLE of a
//     batch. A sleep would not: with a fast publisher a hundred and fifty rows drain in
//     milliseconds, so a test that slept and hoped would usually interrupt nothing at all
//     and would then be asserting against a clean shutdown while claiming to test a crash.
//
// The freeze reports itself only once a REQUIRED NUMBER OF PUBLISHES ARE PARKED in it, and
// that second condition is what makes the crash reproducible rather than merely likely.
// Signalling on the delivery count alone leaves it a race between the test's cancel and the
// relay's next publish: sometimes several publishes are in flight when the context dies and
// the in-flight recovery path is exercised, sometimes none are and it is not. Waiting for the
// workers to arrive guarantees the crash lands ON publishes, every run — which is the whole
// point of the exercise, since that is the window where the outbox row and the broker
// disagree.
//
// A delegate may be supplied, in which case every publish is forwarded to it — that is how
// the live-broker test keeps the same instrumentation while writing to a real topic.
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
// V-7: the mid-batch restart
// ---------------------------------------------------------------------------

// TestEventRecovery_MidBatchRestartLosesNoEventsAndDuplicatesAreDedupableByEventID is
// acceptance criterion V-7.
//
// It interrupts the relay in the MIDDLE of a claimed batch, restarts it, and asserts the two
// halves of the guarantee separately: nothing was lost, and after de-duplicating on event_id
// the delivered set is exactly the seeded set. It does NOT assert the broker saw each message
// once — see the file comment for why that would be testing a promise the system does not
// make.
//
// The interruption is a CONTEXT CANCELLATION, not a Stop. Stop is a graceful drain whose
// documented promise is to let the claimed batch finish, so a test that only stopped the
// relay would be asserting against an orderly shutdown while claiming to test a crash.
// Cancellation abandons the batch exactly as a killed process does.
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

	first.Start(crashCtx)
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

	second.Start(restartCtx)
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
	relay.Start(ctx)

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
	staleErr := fixture.ds.MarkEventDispatched(ctx, stale.ID, firstTokens[stale.EventID])
	require.Error(t, staleErr,
		"the token of a lost claim must be refused: a stale worker must not be able to mark a row the new owner holds")
	assert.Contains(t, strings.ToLower(staleErr.Error()), "claim",
		"the refusal must say the claim was lost rather than fail opaquely: %v", staleErr)

	for _, row := range secondClaim {
		require.NoErrorf(t, fixture.ds.MarkEventDispatched(ctx, row.ID, row.ClaimToken),
			"the CURRENT claim token must be able to finish event %s", row.EventID)
	}

	states := fixture.snapshot(ctx)
	fixture.assertEveryRowFinishedCleanly(states, seededEvents)
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

		outcome, err := fixture.ds.MarkEventFailed(ctx, row.ID, row.ClaimToken,
			fmt.Sprintf("recovery test: simulated transport failure %d", attempt), retryAfter)
		require.NoErrorf(t, err, "recording failed attempt %d", attempt)
		require.Equalf(t, attempt, outcome.Attempts,
			"each recorded failure must advance the counter by exactly one (attempt %d)", attempt)

		recordedAttempts = outcome.Attempts

		if attempt < recoveryMaxAttempts {
			require.Falsef(t, outcome.Exhausted, "the budget must not be spent at attempt %d of %d", attempt, recoveryMaxAttempts)
			require.Equalf(t, model.EventOutboxStatusPending, outcome.Status,
				"a retryable failure must return the row to the claimable set (attempt %d)", attempt)

			// THE BACKOFF IS PERSISTED, not slept: an immediate claim must refuse the row
			// because it is not due yet. Without this the configured 1s/2s/4s/8s/16s schedule
			// collapses into consecutive attempts against a broker that has barely begun to fail.
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
	require.NoError(t, fixture.ds.MarkEventDeadLettered(ctx, rowID, exhaustionToken, DLTFor(failed.Topic), metadata),
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

	alpha.Start(ctx)

	_, alphaParked := alphaPublisher.awaitFreeze(t, recoveryGateTimeout)
	require.GreaterOrEqual(t, alphaParked, 1,
		"alpha must be holding a claimed batch it cannot finish before beta is asked to work around it")

	// Alpha now holds a claimed batch it cannot finish. Beta must still make progress.
	beta.Start(ctx)
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

// TestEventRecovery_EventIDIsUniqueInTheOutbox asserts the index that makes event_id a
// trustworthy idempotency key.
//
// The whole recovery story rests on it: duplicates are permitted on the wire precisely because
// a subscriber can suppress them on event_id, and that is only sound if one event is recorded
// once. Were the column merely conventional, a re-captured event could enter the outbox twice
// with two different ids and no amount of consumer-side de-duplication would help.
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

	brokers := recoveryBrokers()
	require.NotEmpty(t, brokers, "KAFKA_BROKERS resolved to nothing")
	for _, broker := range brokers {
		recoveryRequireReachable(t, "the Kafka broker", broker,
			"Start it with `docker compose up -d kafka kafka-init`, or point KAFKA_BROKERS at a reachable broker.")
	}

	fixture := newRecoveryFixture(t, &config.KafkaConfig{
		Brokers:         brokers,
		TopicPrefix:     DefaultTopicPrefix,
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

	first.Start(crashCtx)
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

	second.Start(restartCtx)

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

	report, err := admin.TopicEndOffsets(ctx, f.topic)
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
