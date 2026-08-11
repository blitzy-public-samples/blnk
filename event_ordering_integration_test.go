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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/cache"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file proves acceptance criterion V-6: EVENTS SHARING AN AGGREGATE ID ARE CONSUMED
// IN PUBLISH ORDER.
//
// # The two mechanisms it exercises, jointly
//
// Ordering is not delivered by one component. Kafka orders messages inside a PARTITION and
// nowhere else, so the guarantee only exists when both of the following hold, and this test
// is the only place they are checked together against real infrastructure:
//
//  1. THE MESSAGE KEY IS THE LEDGER ID, hashed with a stable balancer. Every event for one
//     aggregate therefore lands on one partition. event_publisher.go keys each message with
//     the outbox row's partition key — which PrepareEventOutbox stores as the ledger id
//     whenever the event has one, honouring requirement R-6 — and balances it with
//     kafka.Murmur2Balancer. This test re-computes the expected partition with that same
//     balancer and asserts the broker put the message exactly there, so a failure points at
//     the key or the balancer rather than at "ordering, somewhere".
//  2. THE RELAY DRAINS THE OUTBOX IN OCCURRENCE ORDER. ClaimPendingEventOutbox orders
//     candidates by occurred_at ascending and refuses any row whose partition key still has
//     an earlier pending or processing row, so one aggregate's events are published one at a
//     time, oldest first, no matter how many relay instances or how much concurrency is in
//     play.
//
// # How to run it
//
// It is an INTEGRATION test: it needs PostgreSQL with the migrations applied and a Kafka
// broker with the Blnk topics provisioned. Both come from the local stack:
//
//	docker compose --profile kafka up -d postgres kafka kafka-init
//	                                                 # storage with the bootstrap SCRAM admin
//	                                                 # credential; scripts/kafka-provision.sh
//	                                                 # then creates the topics and their DLTs
//	blnk migrate up                                  # creates blnk.event_outbox
//
//	export KAFKA_BROKERS=localhost:9092
//	export KAFKA_SASL_ADMIN_USER=admin               # the VERIFICATION consumer's principal
//	export KAFKA_SASL_ADMIN_SECRET=...               # never hard-coded here; read from the env
//	export KAFKA_SASL_USER=blnk-producer             # the PRODUCER principal the publisher
//	export KAFKA_SASL_SECRET=...                     # authenticates as. Blnk refuses to publish
//	                                                 # as the administrator, so this pair is
//	                                                 # required whenever the admin pair is set
//	export KAFKA_INSECURE_LOCAL_DEV=true             # the local broker is SASL_PLAINTEXT
//	export BLNK_DATA_SOURCE_DNS=postgres://postgres:password@localhost:5432/blnk?sslmode=disable
//
//	go test -run TestEventOrdering -v .
//	go test -run TestEventOrdering -count=5 .        # an ordering test must not be flaky
//
// TWO PRINCIPALS, NOT ONE, and they are not interchangeable. The publisher authenticates as
// the least-privileged PRODUCER principal, holding Write and Describe and nothing else; the
// verification consumer at the bottom of this file authenticates as the ADMINISTRATIVE one,
// because reading a topic needs Read and the producer deliberately does not have it. Handing
// the administrative pair to the publisher is refused outright rather than downgraded — see
// config.ErrProducerPrincipalRequired — so a run that exports only the administrative pair
// SKIPS here with that as the stated reason instead of failing several hundred lines later.
// scripts/kafka-provision.sh mints both principals and prints where it wrote each secret.
//
// Every one of those variables is OPTIONAL except KAFKA_BROKERS, which is what selects the
// broker and therefore what decides whether this test runs at all. With it unset — the
// state of `make test`, of `go test -short ./...` and of CI — the test SKIPS and says what
// is missing. That is deliberate: the repository's test suite must stay green on a machine
// with no broker, so this file never fails for want of infrastructure. The Kafka acceptance
// job in .github/workflows/go.yml is what stops a skip from hiding: it exports every variable
// above and FAILS if any test in this selection skips rather than runs.
//
// # The consumer in this file is a VERIFICATION INSTRUMENT, not product code
//
// Blnk ships NO consumer: subscribers consume the topics themselves with their own client,
// and subscriber-side error handling and dead-lettering are explicitly out of scope for
// this feature — Blnk publishes the `<topic>.dlt` naming convention and stops there. The
// fetch loop below exists only because the acceptance criterion is phrased in terms of what
// a consumer OBSERVES, and it is confined to this test and its integration siblings. Nothing
// here may be promoted into the product.
//
// It reads with kafka.Client.Fetch against explicit per-partition offsets rather than with a
// consumer group, and that choice is what makes an ordering assertion trustworthy: a fetch
// returns a partition's records in offset order with no group coordination, no rebalance and
// no committed-offset state to leak between runs.
//
// # A note on partition counts
//
// Growing a topic's partition count RE-MAPS keys to partitions: a key that hashed into
// partition 2 of six will hash somewhere else out of twelve, so one aggregate's history ends
// up split across two partitions and its events can then be consumed out of order. Kafka
// cannot shrink a partition count, so the damage is not reversible. That is why topic
// assurance grows a topic only when it is empty or when an operator has explicitly planned
// the migration, and why this test asserts the partition each aggregate landed on rather
// than merely that its events arrived.
//
// # What this file deliberately does NOT assert
//
//   - NO GLOBAL TOTAL ORDER across aggregates. Kafka does not provide one; asserting it
//     would be both wrong and flaky.
//   - No retry, backoff or dead-lettering behaviour — event_relay_test.go and
//     event_dlt_test.go own those.
//   - No ACL, credential or subscriber-isolation behaviour — event_isolation_integration_test.go
//     owns that.
//   - No dual-delivery payload comparison — event_dual_delivery_test.go owns that.
//
// # Running against a shared stack
//
// A relay drains the WHOLE outbox table, so running this test also publishes any unrelated
// pending rows that happen to be there, and a relay somebody else is running may publish
// this test's rows. Neither disturbs the assertions: the claim's per-partition-key predicate
// serialises a key across every concurrent instance, and the topic and key travel on the row
// itself — so whichever relay publishes a row, it goes to the same place in the same order.
// Every assertion below is filtered to this run's own events by a per-run identifier for
// exactly that reason.
//
// One shared-stack condition is NOT survivable, and it is detected rather than suffered: if
// another process LEASES this run's rows — a relay with a long lock duration, or a test that
// simulates a crashed relay by writing locked_until into the future — then the claim correctly
// refuses them, this test's own relay has nothing to drain, and no verdict about ordering can
// be reached. leasedElsewhere identifies that state exactly, by measuring the lease against the
// lock duration this test pins on its own relay, and the test SKIPS with the offending rows
// printed instead of reporting a failure it cannot attribute. On a dedicated stack the
// condition cannot arise.

const (
	// orderingAggregateCount is how many distinct aggregates (ledgers, and therefore
	// partition keys) the test publishes for.
	//
	// Twelve against a six-partition topic is chosen so the events provably span MORE THAN
	// ONE PARTITION. A single-partition run would satisfy every ordering assertion
	// vacuously — Kafka orders a lone partition by construction — so the test would prove
	// nothing about keying at all. Twelve independent keys landing on one partition of six
	// has a probability of 6 * (1/6)^12, about three in a billion, and the test asserts the
	// spread explicitly rather than assuming it.
	orderingAggregateCount = 12

	// orderingEventsPerAggregate is how many events each aggregate emits. Dozens rather
	// than a pair: two events per key can come out in the right order by luck, and a
	// relay that round-robins a batch across workers needs a queue deep enough for the
	// race to surface.
	orderingEventsPerAggregate = 24

	// orderingRelayBatchSize is deliberately larger than orderingAggregateCount. The claim
	// returns at most one row per partition key, so the batch is never filled by this
	// test's own rows and the relay is left free to also drain whatever else is pending —
	// which is what a real relay does and therefore what the test should run against.
	orderingRelayBatchSize = 64

	// orderingRelayPollInterval is far shorter than the production default of one second.
	// One event per aggregate is publishable per claim, so a 24-deep backlog needs 24
	// claims; at the production interval that is 24 seconds of waiting for a property that
	// has nothing to do with the poll interval.
	orderingRelayPollInterval = 40 * time.Millisecond

	// orderingRelayLockDuration is the production lease, pinned EXPLICITLY rather than
	// inherited. It is the yardstick leasedElsewhere measures against: a lease on one of this
	// run's rows that extends further into the future than this cannot have been taken by the
	// relay this test started, which is how foreign interference is told apart from a relay
	// that is failing to claim.
	orderingRelayLockDuration = 30 * time.Second

	// relayContentionLease is the lease the two-relay contention test parks its earlier row
	// under, and it is deliberately absurd.
	//
	// THIRTY MINUTES, against a test that finishes in seconds. The lease is not modelling a
	// realistic relay; it is removing an alternative explanation. The blocked row must stay
	// blocked because an EARLIER ROW FOR ITS PARTITION KEY HAS NOT SETTLED, and a row whose
	// lease expired mid-observation would become claimable for a completely different reason —
	// the reclaim path — producing a green run that proves nothing, or a red one that indicts
	// the ordering guard for the reclaim's behaviour. Absurd is what makes expiry impossible
	// rather than merely unlikely.
	//
	// It is deliberately NOT the relays' own lock duration: those stay on the pinned production
	// lease so that leasedElsewhere can still tell this test's leases from a foreign process's,
	// which is the diagnosis requireAllDispatched depends on.
	relayContentionLease = 30 * time.Minute

	// relayContentionObservation is how long the blocked row is watched while both relays poll.
	//
	// Long enough to cover many poll intervals — at the 40ms interval above, dozens of claims
	// per relay — because the property is "never claimed", and a window shorter than one poll
	// would assert only that nothing had happened yet. Short enough that the test stays a test:
	// the guard is enforced by a single SQL predicate, so a violation is immediate rather than
	// gradual, and waiting longer buys no additional confidence.
	relayContentionObservation = 2 * time.Second

	// relayContentionPollInterval is how often the blocked row's state is re-read during that
	// window. Frequent enough that a TRANSIENT claim — one relay taking the row and another
	// path putting it back — is caught rather than missed between two sparse reads.
	relayContentionPollInterval = 25 * time.Millisecond

	// orderingDispatchTimeout bounds the wait for every published row to reach its
	// dispatched terminal state.
	//
	// FOUR MINUTES, NOT NINETY SECONDS, AND THE MARGIN IS DELIBERATE. On a healthy stack this
	// whole test finishes in about a second, so the bound is patience rather than a budget —
	// it exists only so a stuck relay reports the outbox state instead of hanging until the
	// test binary is killed. Ninety seconds was not patience enough under `go test -race`,
	// which CI runs: the detector instruments every publish and every consume, and on a
	// contended machine the 288 events of a full run genuinely need longer than that. A
	// timeout that trips on a slow-but-correct run reports a defect that is not one, which is
	// worse than a stuck run taking four minutes to say so — and the diagnostic it prints is
	// what makes a real stall diagnosable either way.
	orderingDispatchTimeout = 4 * time.Minute

	// orderingConsumeTimeout bounds the fetch loop. Exceeding it FAILS the test with the
	// offsets reached and the count observed, rather than hanging until the go test binary
	// is killed and reports nothing useful. Four minutes for the same reason as the dispatch
	// bound above.
	orderingConsumeTimeout = 4 * time.Minute

	// orderingFetchMaxWait is how long one fetch parks on an empty partition before
	// returning. It is the loop's pacing: short enough that six empty partitions cost a
	// second and a half, long enough that the loop is not a busy spin.
	orderingFetchMaxWait = 250 * time.Millisecond

	// orderingFetchMaxBytes bounds one fetch response. It comfortably exceeds
	// orderingAggregateCount * orderingEventsPerAggregate small envelopes, so the whole run
	// can arrive in few round trips.
	orderingFetchMaxBytes = 8 << 20

	// orderingDialTimeout bounds the reachability probe that decides whether the test runs
	// or skips. A skip must be quick: this cost is paid by every `go test ./...` on a
	// machine where the stack is down.
	orderingDialTimeout = 3 * time.Second

	// orderingClientTimeout bounds one metadata, offset or fetch round trip made by the
	// verification client, so a broker that accepts a connection and then stops answering
	// surfaces as a failed request rather than as a hung test.
	orderingClientTimeout = 15 * time.Second

	// orderingClaimRows and orderingClaimBatchSize drive the claim-order subtest. Six rows
	// claimed three at a time is what puts the claim's LIMIT under pressure, which is the
	// only condition under which the candidate ORDER BY is observable at all.
	orderingClaimRows      = 6
	orderingClaimBatchSize = 3

	// orderingClaimLease is the lease the claim-order subtest takes. It is short because
	// the subtest marks every row it claims dispatched immediately, and a long lease would
	// only widen the window in which an abandoned row looks stuck.
	orderingClaimLease = 5 * time.Second

	// orderingClaimBackdateDays backdates the claim subtest's rows by a year.
	//
	// This is what puts the claim's LIMIT under pressure ON THIS TEST'S OWN ROWS. Candidates
	// are ordered by occurred_at ascending, so a batch smaller than the candidate set returns
	// the OLDEST rows — and backdating makes those provably this test's, on a table that may
	// hold unrelated pending rows from anything else running against the same database.
	orderingClaimBackdateDays = 365

	// orderingClaimAggregateBase keeps the claim subtest's synthetic transaction identifiers
	// clear of the delivery subtest's aggregate numbering, so a row is attributable to one
	// subtest at a glance.
	orderingClaimAggregateBase = 90

	// The divergent-key test's own dimensions (PERF-C02).
	//
	// orderingDivergentRows is how many events of ONE ledger it seeds. Six is enough that a
	// reordering shows up as a permutation rather than as a coin flip, and small enough that
	// two relays polling every 40ms drain it in well under a second.
	orderingDivergentRows = 6

	// orderingDivergentAggregateBase keeps this test's synthetic transaction identifiers clear
	// of both the delivery subtest's numbering and orderingClaimAggregateBase, so a row is
	// still attributable to one test at a glance.
	orderingDivergentAggregateBase = 70

	// orderingDivergentRelayCount is how many relay instances run against the seeded rows at
	// once. TWO is the whole point: the defect this test exists for is invisible to one relay,
	// because a single process groups its claimed batch by the effective key before publishing
	// and so serialises the rows itself. Only separate processes claiming concurrently can
	// publish two rows of one Kafka partition at the same time.
	orderingDivergentRelayCount = 2

	// orderingDivergentProbeLease is the lease the claim-level probe takes.
	//
	// Deliberately short: the probe claims to observe WHAT the claim admits, then leaves the
	// rows for the relays. A production-length lease would make the test wait 30 seconds for
	// the probe's own rows to become claimable again.
	orderingDivergentProbeLease = 300 * time.Millisecond

	// orderingLedgerMintAttempts bounds the search for a ledger id set that spans more than
	// one partition. One attempt fails with probability 6 * (1/6)^12 — about three in a
	// billion — so this exists to keep an astronomically rare draw from failing a correct
	// implementation, not because a retry is expected.
	orderingLedgerMintAttempts = 8

	// orderingInterleaveSeed fixes the publish interleaving. A FIXED seed rather than a
	// random one: an ordering failure must be reproducible from the log alone, and a
	// different interleaving on every run would make one flake impossible to re-create.
	orderingInterleaveSeed = 0x5EED1CE5EED1CE5

	// orderingFallbackPostgresDSN is the local development datasource, identical to the one
	// lineage_integration_test.go uses. It is a fallback only: BLNK_DATA_SOURCE_DNS wins
	// when it is set. The credential in it is the docker-compose development password, not
	// a secret — the local broker credentials, which are genuinely secret-shaped, are read
	// from the environment and never appear in this file.
	orderingFallbackPostgresDSN = "postgres://postgres:password@localhost:5432/blnk?sslmode=disable"

	// orderingFallbackRedisDNS is the local development Redis. NewBlnk builds an asynq
	// client, so a Redis address is required to construct the service container even though
	// this test never enqueues anything.
	orderingFallbackRedisDNS = "localhost:6379"

	// The three payload keys the test stamps onto each transaction's metadata. The sequence
	// is read back off the CONSUMED message, so the assertion is made against what a
	// subscriber would actually see rather than against test-local bookkeeping.
	orderingMetaRun       = "ordering_run"
	orderingMetaAggregate = "ordering_aggregate"
	orderingMetaSequence  = "ordering_sequence"

	// orderingTransactionStatus is the transaction status the published events describe. The
	// event NAME is derived from it with model.EventTypeForTransactionStatus rather than
	// spelled out, exactly as the transaction execution path derives it, so the test cannot
	// drift from the status-to-event mapping it is supposed to be exercising.
	orderingTransactionStatus = "APPLIED"

	// orderingDispatchPollInterval is how often the dispatched-status wait re-reads the rows
	// it is still waiting for.
	orderingDispatchPollInterval = 100 * time.Millisecond

	// orderingDiagnosticRows caps how many rows a failure message dumps. A dump of every row
	// of a 288-event run is a wall of text nobody reads; a handful plus the total is what
	// makes the message diagnosable.
	orderingDiagnosticRows = 5
)

// orderingFixture is the live-infrastructure handle for one test run: the configuration that
// was published to config.ConfigStore, the datasource, the Blnk service container that owns
// the shared Kafka publisher, the verification client, the topic under test and its
// partitions, and the per-run identifier every assertion filters on.
type orderingFixture struct {
	cfg  *config.Configuration
	ds   database.IDataSource
	blnk *Blnk
	// pool is the same connection this fixture's datasource wraps, kept as the concrete type
	// so cleanup can issue the one statement IDataSource has no method for: deleting this
	// run's own rows. Adding a purge to the repository interface to serve a test would put an
	// operation in production code that production has no caller for.
	pool       *sql.DB
	client     *kafka.Client
	topic      string
	partitions []int
	runID      string
}

// orderingSlot is one entry in the publish schedule: which aggregate emits, and which
// position in that aggregate's own sequence the event occupies. The schedule is a merge of
// every aggregate's sequence, so slots for one aggregate are scattered through it while
// their sequence numbers stay strictly increasing.
type orderingSlot struct {
	aggregate int
	sequence  int
}

// orderingEnvelope mirrors the canonical LedgerEvent wire shape. It is declared here, in the
// test, rather than reusing model.LedgerEvent: decoding into an INDEPENDENT struct means the
// assertions check the JSON a subscriber receives, and a rename or a retagged field in the
// model would be caught rather than silently followed.
type orderingEnvelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	AggregateID   string          `json:"aggregate_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
	SchemaVersion int             `json:"schema_version"`
}

// orderingPayload is the two-key legacy webhook body the payload field carries verbatim:
// the event name under "event" and the domain object under "data". Only the fields this test
// reads are declared.
type orderingPayload struct {
	Event string `json:"event"`
	Data  struct {
		TransactionID string `json:"transaction_id"`
		Reference     string `json:"reference"`
		MetaData      struct {
			Run       string `json:"ordering_run"`
			Aggregate string `json:"ordering_aggregate"`
			Sequence  int    `json:"ordering_sequence"`
		} `json:"meta_data"`
	} `json:"data"`
}

// orderingObservation is one message read back off the topic, carrying the two facts the
// broker adds — the partition it was written to and the offset it occupies — alongside the
// decoded envelope and the message key.
type orderingObservation struct {
	partition int
	offset    int64
	key       string
	envelope  orderingEnvelope
	payload   orderingPayload
}

// orderingEnvOr reads an environment variable, falling back when it is unset or blank.
//
// Parameters:
//   - key string: the variable to read.
//   - fallback string: the value to use when the variable carries nothing usable.
//
// Returns:
//   - string: the trimmed variable value, or the fallback.
func orderingEnvOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}

	return fallback
}

// orderingReplicationFactor resolves the replication factor the fixture assures topics with.
//
// It defaults to ONE rather than to the production three because the factor a test should ask
// for is the factor its broker can honour. A single-node development broker cannot place three
// replicas, so asking for three guarantees a refusal on every topic — and the refusal is not
// even actionable from a test, since raising the factor of an existing topic requires a
// partition reassignment. Reading the variable still allows a multi-broker cluster to be
// exercised at its real factor.
//
// Returns:
//   - int: KAFKA_REPLICATION_FACTOR when it parses as a positive integer, otherwise 1.
func orderingReplicationFactor() int {
	raw := strings.TrimSpace(os.Getenv("KAFKA_REPLICATION_FACTOR"))
	if raw == "" {
		return 1
	}

	factor, err := strconv.Atoi(raw)
	if err != nil || factor < 1 {
		return 1
	}

	return factor
}

// orderingBrokersFromEnv parses KAFKA_BROKERS the way the configuration loader does: a
// comma-separated list, trimmed, with empty entries dropped.
//
// The trimming matters rather than being defensive. "host-a:9092, host-b:9092" yields an
// entry with a leading space that would be dialled verbatim and fail, and a trailing comma
// yields an empty entry — so KAFKA_BROKERS=" " must resolve to "no brokers", which is what
// makes the skip below fire instead of the test failing on an unusable address.
//
// Returns:
//   - []string: the usable broker addresses, empty when none were configured.
func orderingBrokersFromEnv() []string {
	raw := os.Getenv("KAFKA_BROKERS")
	brokers := make([]string, 0, strings.Count(raw, ",")+1)

	for _, candidate := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}

	return brokers
}

// orderingHostFromDSN extracts the host:port a PostgreSQL DSN points at, so the test can
// probe reachability before doing any work.
//
// A DSN it cannot parse yields the empty string, which the caller treats as "cannot probe"
// and lets through — the datasource will then produce the real, specific error. Guessing is
// worse than not probing.
//
// Parameters:
//   - dsn string: the datasource DSN.
//
// Returns:
//   - string: the host:port to dial, or "" when it could not be determined.
func orderingHostFromDSN(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Host == "" {
		return ""
	}

	if parsed.Port() == "" {
		return net.JoinHostPort(parsed.Hostname(), "5432")
	}

	return parsed.Host
}

// orderingRelayOutboxColumns are the blnk.event_outbox columns the relay path READS that a
// database migrated to an earlier revision of this feature will not have.
//
// claim_token is what makes a claim's ownership provable across relay instances, and
// kafka_dispatched_at is what lets the two delivery legs reach their terminal state
// independently during the dual-delivery window. Every claim the relay issues names them, so
// a database missing either one answers the claim with SQLSTATE 42703 and nothing is ever
// dispatched — which would otherwise surface as an ordering failure rather than as the schema
// problem it is.
var orderingRelayOutboxColumns = []string{"claim_token", "kafka_dispatched_at"}

// orderingSkipUnlessOutboxSchemaIsCurrent skips the test when the connection the datasource
// handed back does not carry this feature's migrations.
//
// # Why a table-exists probe is not enough
//
// The probe above asks whether blnk.event_outbox is QUERYABLE, and a database migrated to an
// earlier revision of this feature answers yes: the table is there, and only two columns the
// relay's claim names are missing. The claim then fails with "column \"claim_token\" does
// not exist" on every tick, the rows stay pending, and the test spends its whole drain budget
// before failing with hundreds of unreadable rows — a report that describes the symptom and
// hides the cause.
//
// # What the message reports, and why
//
// The database and port the connection actually landed on, alongside the DSN that was asked
// for. The fixture owns its pool, so the two agree — but a DSN is a string and a `blnk`
// database on one port is easily mistaken for the `blnk` database on another when several are
// running, which is exactly the situation a partially-migrated schema arises in. Naming both
// makes the remedy unambiguous about WHICH database to migrate.
//
// Parameters:
//   - t *testing.T: the test. Skipped, never failed — an unmigrated database is missing
//     infrastructure, which is how the probe above already treats it, and the Kafka acceptance
//     job fails on any skip so this cannot quietly stop proving anything.
//   - ds database.IDataSource: the datasource just opened.
//   - dsn string: the DSN this fixture ASKED for, reported alongside what it actually got.
func orderingSkipUnlessOutboxSchemaIsCurrent(t *testing.T, ds database.IDataSource, dsn string) {
	t.Helper()

	// Only the concrete datasource exposes the pool, and the probe is a diagnostic rather than
	// a requirement: a fake or wrapped implementation is left to the assertions below.
	source, exposesPool := ds.(*database.Datasource)
	if !exposesPool || source.Conn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), orderingClientTimeout)
	defer cancel()

	// Reported, not asserted on. inet_server_port() is NULL over a unix socket, hence COALESCE.
	var connectedDatabase string
	var connectedPort int
	if err := source.Conn.QueryRowContext(ctx,
		"SELECT current_database(), COALESCE(inet_server_port(), 0)",
	).Scan(&connectedDatabase, &connectedPort); err != nil {
		t.Skipf(
			"event ordering integration test: the datasource is not answering queries (%v). Start "+
				"PostgreSQL and apply the migrations with `blnk migrate up`",
			err,
		)
	}

	// THE COLUMN LIST IS BOUND, not spelled a second time in the SQL. It was, and the two
	// spellings drifted: the statement asked after two names that are STATUS LITERALS rather
	// than columns, so the count was always zero, the comparison below always failed, and
	// every Kafka-backed assertion in this file reported a skip on a perfectly migrated
	// database. A skip is the one outcome that looks like success, so the drift survived.
	var present int
	if err := source.Conn.QueryRowContext(ctx,
		"SELECT count(*) FROM information_schema.columns "+
			"WHERE table_schema = 'blnk' AND table_name = 'event_outbox' "+
			"AND column_name = ANY($1)",
		pq.Array(orderingRelayOutboxColumns),
	).Scan(&present); err != nil {
		t.Skipf(
			"event ordering integration test: blnk.event_outbox's columns are not readable (%v). "+
				"Apply the migrations with `blnk migrate up`",
			err,
		)
	}

	if present != len(orderingRelayOutboxColumns) {
		t.Skipf(
			"event ordering integration test: blnk.event_outbox is missing %d of the %v columns the "+
				"relay's claim names, so no row could ever be dispatched and this test would spend "+
				"its whole drain budget before failing. The connection is on database %q at port %d; "+
				"this fixture asked for %q. Apply the migrations to THAT database with "+
				"`blnk migrate up`",
			len(orderingRelayOutboxColumns)-present, orderingRelayOutboxColumns,
			connectedDatabase, connectedPort, dsn,
		)
	}
}

// orderingSkipUnlessProducerPrincipal skips the test when the environment carries an
// ADMINISTRATIVE Kafka credential but no dedicated PRODUCER credential.
//
// That combination is refused by the product, not merely discouraged: the event publisher
// returns config.ErrProducerPrincipalRequired rather than authenticating as a principal that
// can create topics, mint SCRAM credentials and rewrite ACLs. So a run configured that way
// cannot build a service container at all, and without this guard the whole file fails at
// NewBlnk with an error about credentials — which reads as a product defect when it is an
// unfinished environment.
//
// The predicate mirrors config.Configuration.ProducerSASL exactly: a producer pair that is
// entirely absent, with at least one administrative value present. Neither pair set is a
// legitimate unauthenticated broker and is left to run; both set is the provisioned stack and
// is what this file is meant to exercise.
//
// Parameters:
//   - t *testing.T: the test. Skipped, never failed: an incomplete environment is missing
//     infrastructure, exactly like an unreachable broker, and the CI job that exports both
//     pairs fails on any skip so this cannot quietly stop proving anything.
func orderingSkipUnlessProducerPrincipal(t *testing.T) {
	t.Helper()

	producerConfigured := strings.TrimSpace(os.Getenv("KAFKA_SASL_USER")) != "" ||
		strings.TrimSpace(os.Getenv("KAFKA_SASL_SECRET")) != ""
	adminConfigured := strings.TrimSpace(os.Getenv("KAFKA_SASL_ADMIN_USER")) != "" ||
		strings.TrimSpace(os.Getenv("KAFKA_SASL_ADMIN_SECRET")) != ""

	if adminConfigured && !producerConfigured {
		t.Skip(
			"event ordering integration test: the environment carries KAFKA_SASL_ADMIN_USER/" +
				"KAFKA_SASL_ADMIN_SECRET but no KAFKA_SASL_USER/KAFKA_SASL_SECRET. The event " +
				"publisher REFUSES to authenticate as the administrative principal, so no service " +
				"container can be built. Run scripts/kafka-provision.sh, which mints the dedicated " +
				"producer principal, then export KAFKA_SASL_USER and KAFKA_SASL_SECRET",
		)
	}
}

// orderingSkipUnlessReachable skips the test when a dependency cannot be dialled.
//
// The skip is CLEAN by construction: it runs before any topic, row, publisher or goroutine
// exists, so there is nothing half-built to leave behind. The message names the component,
// the address and the variable that selects it, because "skipped" with no reason is
// indistinguishable from a test that silently stopped covering anything.
//
// Parameters:
//   - t *testing.T: the test.
//   - address string: the host:port to dial. An empty address is not probed.
//   - what string: the human name of the dependency, for the skip message.
//   - variable string: the environment variable that selects it.
func orderingSkipUnlessReachable(t *testing.T, address, what, variable string) {
	t.Helper()

	if address == "" {
		return
	}

	conn, err := net.DialTimeout("tcp", address, orderingDialTimeout)
	if err != nil {
		t.Skipf(
			"event ordering integration test: %s at %s (%s) is not reachable: %v. "+
				"Bring the local stack up and provision the Kafka topics — see the header of "+
				"event_ordering_integration_test.go",
			what, address, variable, err,
		)

		return
	}

	if closeErr := conn.Close(); closeErr != nil {
		t.Logf("closing the %s reachability probe failed: %v", what, closeErr)
	}
}

// orderingConfiguration builds the configuration this test publishes to config.ConfigStore.
//
// Everything Kafka-related comes from the ENVIRONMENT and nothing is hard-coded, which is
// both a security posture — no credential literal in the repository — and what lets the same
// test run against the local SASL_PLAINTEXT broker and against a TLS broker without an edit.
//
// InsecureLocalDev mirrors KAFKA_TLS_ENABLED: TLS off with no acknowledgement is the one
// combination that refuses to build a Kafka client at all, so a test that left both unset
// would fail at construction with a configuration error instead of running. Setting it
// alongside "TLS is off" states, in one place, that this is the local development broker.
//
// Parameters:
//   - brokers []string: the resolved broker list, already non-empty.
//   - dsn string: the datasource DSN.
//
// Returns:
//   - *config.Configuration: the configuration to hand to config.MockConfig.
func orderingConfiguration(brokers []string, dsn string) *config.Configuration {
	tlsEnabled := strings.EqualFold(strings.TrimSpace(os.Getenv("KAFKA_TLS_ENABLED")), "true")

	// A webhook deprecation window is MANDATORY once brokers are configured: a deployment
	// publishing to Kafka with no sunset would run the deprecated HTTP transport indefinitely
	// alongside it, and the loader refuses that outright. So the window is stated here even
	// though this file asserts nothing about deprecation or dual delivery.
	//
	// THE SUNSET IS THE ONLY END THAT MAY BE SUPPLIED, and the fallback below therefore derives
	// the SUNSET rather than the start. config.Configuration.resolveWebhookDeprecationWindow
	// takes exactly one input — the sunset — and OVERWRITES the start with "sunset minus the
	// window", so a configuration carrying only a start reaches the loader as a blank sunset
	// with brokers configured, which it refuses outright: "webhook_deprecation_sunset_date is
	// required once kafka.brokers is set". This fixture previously back-filled the START, so
	// following this file's own documented run recipe — which exports no deprecation variable
	// at all — produced a hard failure at config.Fetch rather than a running test. Deriving the
	// sunset is what makes the documented recipe work, and it cannot contradict the start
	// because the start is arithmetic the loader performs.
	//
	// One day INSIDE the window rather than exactly on its edge, so the derived start lands in
	// the past and the run is unambiguously within the dual-delivery window — the state a
	// deployment is in today, and therefore the one the relay should be exercised in. The
	// legacy leg is a no-op either way, since no notification webhook URL is configured.
	// NO START DATE IS READ. WebhookDeprecationStartDate is derived by the loader from the
	// sunset, and any value supplied on input is overwritten, so reading an environment
	// variable for it would look like a supported override and be silently discarded. The
	// retired WEBHOOK_DEPRECATION_START_DATE key is not consulted anywhere.
	deprecationSunset := strings.TrimSpace(os.Getenv("WEBHOOK_DEPRECATION_SUNSET_DATE"))
	if deprecationSunset == "" {
		deprecationSunset = time.Now().UTC().
			Add(time.Duration(config.WebhookDualDeliveryWindowDays-1) * 24 * time.Hour).
			Format(time.RFC3339)
	}

	return &config.Configuration{
		Redis:      config.RedisConfig{Dns: orderingEnvOr("BLNK_REDIS_DNS", orderingFallbackRedisDNS)},
		DataSource: config.DataSourceConfig{Dns: dsn},
		// Test-scoped queue names. Nothing is enqueued — the legacy webhook leg no-ops
		// because no notification URL is configured — but naming them keeps this run off
		// the queues a local worker may be draining.
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_ordering_test",
			IndexQueue:       "index_queue_ordering_test",
			TransactionQueue: "transaction_queue_ordering_test",
			NumberOfQueues:   1,
		},
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     30 * time.Second,
			IndexQueuePrefix: "ordering_test_index",
		},
		Kafka: config.KafkaConfig{
			Brokers:     brokers,
			TopicPrefix: strings.TrimSpace(os.Getenv("KAFKA_TOPIC_PREFIX")),
			// The replication factor is read from the environment and defaults to ONE, which
			// is the only factor a single-broker development stack can satisfy. Leaving it at
			// the production default of three made topic assurance report every one of the ten
			// topics as under-replicated on every run — twenty error lines that were pure
			// noise, since a factor cannot be raised by re-running assurance and the test's own
			// broker has one node. The relay proceeds either way, so this changes nothing about
			// what is under test; it stops the fixture manufacturing a fault it then ignores.
			ReplicationFactor: orderingReplicationFactor(),
			SASLUser:          strings.TrimSpace(os.Getenv("KAFKA_SASL_USER")),
			SASLSecret:        os.Getenv("KAFKA_SASL_SECRET"),
			SASLAdminUser:     strings.TrimSpace(os.Getenv("KAFKA_SASL_ADMIN_USER")),
			SASLAdminSecret:   os.Getenv("KAFKA_SASL_ADMIN_SECRET"),
			TLS: config.KafkaTLSConfig{
				Enabled:    tlsEnabled,
				CAFile:     strings.TrimSpace(os.Getenv("KAFKA_TLS_CA_FILE")),
				CertFile:   strings.TrimSpace(os.Getenv("KAFKA_TLS_CERT_FILE")),
				KeyFile:    strings.TrimSpace(os.Getenv("KAFKA_TLS_KEY_FILE")),
				ServerName: strings.TrimSpace(os.Getenv("KAFKA_TLS_SERVER_NAME")),
			},
			InsecureLocalDev: !tlsEnabled,
		},
		WebhookDeprecationSunsetDate: deprecationSunset,
		// Relay retry settings are left unset on purpose so config.MockConfig fills in the
		// production defaults (five attempts, 1s base, 30s cap). The schedule is not under
		// test here; running with anything else would mean the relay this test drives is not
		// the relay a deployment runs.
	}
}

// newOrderingFixture guards on infrastructure and then builds everything the test drives.
//
// The guard order is deliberate — short mode, then configuration, then reachability — so the
// cheapest reason to skip is found first and no dependency is dialled on a `-short` run.
//
// Parameters:
//   - t *testing.T: the test. Skipped, never failed, when infrastructure is absent.
//
// Returns:
//   - *orderingFixture: the ready fixture. Every resource it holds is released through
//     t.Cleanup, so a failure part-way through leaks nothing.
func newOrderingFixture(t *testing.T) *orderingFixture {
	t.Helper()

	if testing.Short() {
		t.Skip(
			"event ordering integration test: skipped in -short mode, which is how `make test` " +
				"and CI run. It needs a live Kafka broker and PostgreSQL; see the header of " +
				"event_ordering_integration_test.go for how to run it",
		)
	}

	brokers := orderingBrokersFromEnv()
	if len(brokers) == 0 {
		t.Skip(
			"event ordering integration test: KAFKA_BROKERS is unset or carries no usable " +
				"address, so there is no broker to publish to or consume from. Bring up the " +
				"compose stack, provision the topics with scripts/kafka-provision.sh, then " +
				"export KAFKA_BROKERS=localhost:9092",
		)
	}

	// A cluster that authenticates needs a PRODUCER principal, not just an administrative one.
	// Blnk refuses to publish as the administrator — that principal can create topics, alter
	// SCRAM credentials and manage ACLs, so a leaked producer credential would compromise the
	// cluster's authorization state — and construction fails outright rather than quietly
	// falling back. Skipping here turns that into a shopping list instead of an obscure failure
	// several hundred lines into the run.
	//
	// Through the named helper rather than inline: this check existed twice, once here and once
	// as orderingSkipUnlessProducerPrincipal, and the helper's condition is the more complete of
	// the two — it also catches an environment carrying only the administrative SECRET, or only
	// the producer secret, which the inline copy read as fully configured.
	orderingSkipUnlessProducerPrincipal(t)

	dsn := orderingEnvOr("BLNK_DATA_SOURCE_DNS", orderingFallbackPostgresDSN)
	orderingSkipUnlessReachable(t, orderingHostFromDSN(dsn), "PostgreSQL", "BLNK_DATA_SOURCE_DNS")

	for _, broker := range brokers {
		orderingSkipUnlessReachable(t, broker, "the Kafka broker", "KAFKA_BROKERS")
	}

	// The configuration is published BEFORE anything reads it. The broker list, the topic
	// prefix and the SASL credentials all reach the publisher, the topic resolver and the
	// outbox validator through config.Fetch rather than as parameters.
	previous, _ := config.ConfigStore.Load().(*config.Configuration)
	config.MockConfig(orderingConfiguration(brokers, dsn))
	t.Cleanup(func() {
		// Restored so a test running after this one sees the configuration it set up itself.
		// An atomic.Value cannot be emptied, so a process that had no configuration before
		// keeps this one — which is exactly what every other test in this package already
		// does by storing its own.
		if previous != nil {
			config.ConfigStore.Store(previous)
		}
	})

	cnf, err := config.Fetch()
	require.NoError(t, err,
		"the mocked configuration is not readable through config.Fetch. config.MockConfig only "+
			"stores a configuration that passes validation and merely LOGS the reason when it does "+
			"not, so the rejected field is named in the log line just above this failure")
	require.NotEmpty(t, cnf.Kafka.Brokers,
		"config.MockConfig dropped the broker list; NewBlnk would then select the no-op publisher")

	// # The connection pool is opened HERE rather than through database.NewDataSource
	//
	// database.GetDBConnection memoises ONE datasource per process behind a sync.Once, so
	// whichever test in this binary connects first fixes the pool for every test after it and
	// any DSN passed later is silently ignored. Several tests in this package hardcode
	// postgres://…@localhost:5432/blnk, so in a whole-package run this fixture would be handed
	// THAT server no matter what BLNK_DATA_SOURCE_DNS says — and it would then seed hundreds of
	// rows into a database it never named, drain none of them, and report the symptom rather
	// than the cause.
	//
	// So this file owns its connection, exactly as event_recovery_integration_test.go does and
	// for the same reason. Nothing is faked by it: ConnectDB is the exported production
	// connector, database.Datasource is the production type, and the cache is the one
	// GetDBConnection itself builds — the test simply refuses to share a pool it cannot address.
	pool, err := database.ConnectDB(cnf.DataSource)
	require.NoError(t, err, "could not open a connection pool at the configured DSN")

	sharedCache, err := cache.NewCache()
	require.NoError(t, err,
		"could not build the cache the datasource carries in production; it needs the configured Redis")

	ds := &database.Datasource{Conn: pool, Cache: sharedCache}
	t.Cleanup(func() {
		// sql.DB.Close is idempotent, so this stands even though closing the Blnk instance
		// below may already have released the same pool.
		assert.NoError(t, ds.Close(), "the ordering test's connection pool should close cleanly")
	})

	// The one thing database.NewDataSource does beyond connecting, reproduced verbatim so this
	// fixture's connection behaves exactly like a production one.
	searchPathCtx, cancelSearchPath := context.WithTimeout(context.Background(), orderingClientTimeout)
	defer cancelSearchPath()

	_, err = pool.ExecContext(searchPathCtx, "SET search_path TO blnk")
	require.NoError(t, err, "could not set the blnk search path on the pool")

	// blnk.event_outbox is created by a migration, so its absence means the schema was never
	// migrated — a missing dependency exactly like an unreachable broker or an unprovisioned
	// topic, and reported the same way. Without this probe the same condition surfaces as an
	// opaque insert failure a few hundred rows into the run.
	probeCtx, cancelProbe := context.WithTimeout(context.Background(), orderingClientTimeout)
	defer cancelProbe()

	// A zero instant takes the repository's default count window (PERF-P04). This is a
	// reachability probe, so any answer at all is the answer it wants.
	if _, probeErr := ds.CountEventOutboxByStatus(probeCtx, time.Time{}); probeErr != nil {
		t.Skipf(
			"event ordering integration test: blnk.event_outbox is not queryable (%v). Apply the "+
				"migrations with `blnk migrate up` before running this test",
			probeErr,
		)
	}

	// The table existing is not the same as the table being CURRENT, and the difference is a
	// ninety-second false failure. Checked here, one line after the table probe, because both
	// answer the same question — is this database ready — and a reader looking for that question
	// should find both answers together.
	orderingSkipUnlessOutboxSchemaIsCurrent(t, ds, dsn)

	instance, err := NewBlnk(ds)
	require.NoError(t, err, "could not build the Blnk service container")
	t.Cleanup(func() {
		// Closing the instance flushes and releases the publisher's writers together with
		// their pooled broker connections and SASL sessions. Skipping it leaks a connection
		// pool per run.
		assert.NoError(t, instance.Close(), "the Blnk instance should close cleanly")
	})

	require.False(t, IsNoopEventPublisher(instance.events),
		"the service container resolved the NO-OP event publisher, which reports every publish "+
			"as dispatched without sending anything: the whole test would pass vacuously. "+
			"Check KAFKA_BROKERS and the SASL settings")

	// The verification client authenticates as the ADMINISTRATIVE principal. Reading a topic
	// needs Read, and the steady-state producer principal is deliberately granted only Write
	// and Describe, so the producer credential cannot consume. Building the transport with
	// the production constructor means the test inherits the product's TLS and credential
	// policy rather than re-implementing SASL here.
	transport, err := NewKafkaTransport(cnf.Kafka, KafkaTransportRoleAdmin)
	require.NoError(t, err, "could not build the Kafka transport for the verification consumer")
	t.Cleanup(transport.CloseIdleConnections)

	fixture := &orderingFixture{
		cfg:  cnf,
		ds:   ds,
		pool: pool,
		blnk: instance,
		client: &kafka.Client{
			Addr:      kafka.TCP(brokers...),
			Timeout:   orderingClientTimeout,
			Transport: transport,
		},
		// The topic is resolved through the production resolver, so a run with
		// KAFKA_TOPIC_PREFIX set reads the topic the publisher actually writes to.
		topic: TopicForEvent(model.EventTypeTransactionApplied),
		// UUID-derived, so repeated runs — `-count=5` — and concurrent runs against one
		// shared stack cannot see each other's events.
		runID: strings.ReplaceAll(model.GenerateUUIDWithSuffix("ord"), "-", ""),
	}
	fixture.partitions = fixture.discoverPartitions(t)

	// EXCLUSIVE USE OF blnk.event_outbox for the duration of this test. This tier starts a
	// relay, and a relay claims the oldest pending rows in the whole table — so it drains
	// another package's live fixtures, and another package's quiesce retires its own rows out
	// from under it. lockEventOutboxTier in event_recovery_integration_test.go documents the
	// collision and why an advisory lock is what fixes it.
	lockEventOutboxTier(t, pool)

	// Registered AFTER the lock, so it runs BEFORE the lock is released: cleanup is LIFO, and
	// deleting this run's rows while still holding the table is what stops the next tier from
	// seeing them at all.
	t.Cleanup(func() { fixture.deleteSeededRows(t) })

	t.Logf("event ordering fixture ready: brokers=%v topic=%s partitions=%v run=%s",
		brokers, fixture.topic, fixture.partitions, fixture.runID)

	return fixture
}

// deleteSeededRows removes the outbox rows this run wrote.
//
// # Why a fixture that only writes terminal rows still has to clean up
//
// This tier left every row it created behind — 24 per aggregate, hundreds per run, thousands
// after a few runs. Three costs follow, and the third is the one that actually broke a run:
//
//  1. The tier gets slower and more order-sensitive as the table grows, because every claim
//     reads past the residue.
//  2. A row left non-terminal is claimable for ever, so the next relay any test starts picks it
//     up and reports work it did not seed.
//  3. A DISPATCHED row records the broker coordinate it was written to, under a unique index on
//     (kafka_topic, kafka_partition, kafka_offset). Reset the broker — which the local stack
//     does, and which any operator does with `docker compose down -v` — and topic offsets
//     restart at zero, so the next run's publishes collide with the stale rows' coordinates and
//     the relay cannot mark them dispatched. That was observed as 165 rows stuck in `processing`
//     and a four-minute timeout, with the real cause three commands earlier.
//
// Scoped by this run's transaction-id prefix, so no other run's rows are touched, and no topic
// is deleted because sibling tests share the broker.
func (f *orderingFixture) deleteSeededRows(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), orderingClientTimeout)
	defer cancel()

	result, err := f.pool.ExecContext(ctx,
		`DELETE FROM blnk.event_outbox WHERE starts_with(aggregate_id, $1)`,
		orderingTransactionIDPrefix(f.runID))
	if err != nil {
		// FAILS the test rather than logging. A row this run seeded and could not delete is
		// left in a shared table: a non-terminal one is claimable for ever, so the next relay
		// any test starts picks it up and that test's assertions are made against rows it
		// never created. A green test that leaves that behind reports success for the run and
		// hands the failure to somebody else, in a different file, with no way back to here.
		t.Errorf("could not clean up the outbox rows of ordering run %s: %v", f.runID, err)
	} else if affected, countErr := result.RowsAffected(); countErr != nil {
		t.Logf("cleaned up the outbox rows of ordering run %s (count unavailable: %v)",
			f.runID, countErr)
	} else {
		t.Logf("cleaned up %d outbox rows for ordering run %s", affected, f.runID)
	}

	// The delete reporting success is not the same as the rows being gone, so the state is
	// CONFIRMED rather than assumed. This also catches the case the delete cannot: a row whose
	// aggregate id does not carry the prefix the cleanup matches on, which the delete would
	// report as a cheerful zero.
	f.verifySeededRowsGone(t)
}

// verifySeededRowsGone asserts this run left nothing behind in the shared outbox.
//
// Separate from the delete so that a cleanup which errored is still checked: the two answer
// different questions — whether the statement succeeded, and whether the table is clean.
func (f *orderingFixture) verifySeededRowsGone(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), orderingClientTimeout)
	defer cancel()

	var remaining int
	if err := f.pool.QueryRowContext(ctx,
		`SELECT count(*) FROM blnk.event_outbox WHERE starts_with(aggregate_id, $1)`,
		orderingTransactionIDPrefix(f.runID),
	).Scan(&remaining); err != nil {
		t.Errorf("could not confirm the outbox rows of ordering run %s were removed: %v",
			f.runID, err)

		return
	}

	assert.Zerof(t, remaining,
		"ordering run %s left %d outbox rows behind. They are in a table every other test "+
			"shares, and a non-terminal one stays claimable, so the next relay to start will "+
			"publish them as if they were its own.", f.runID, remaining)
}

// orderingTransactionIDPrefix is the prefix every transaction id this run mints begins with,
// and therefore the prefix of every aggregate id its events carry.
//
// It is derived from the same format string orderingTransaction uses, so the two cannot drift
// into a cleanup that silently matches nothing.
func orderingTransactionIDPrefix(runID string) string {
	return fmt.Sprintf("txn_%s_", runID)
}

// discoverPartitions reads the topic's partition ids from broker metadata.
//
// A topic that does not exist SKIPS rather than fails: automatic topic creation is off on
// purpose, so an absent topic means the stack was never provisioned, which is a missing
// dependency exactly like an unreachable broker.
//
// Parameters:
//   - t *testing.T: the test.
//
// Returns:
//   - []int: the partition ids in ascending order, which is the order the balancer's
//     modulo indexes into.
func (f *orderingFixture) discoverPartitions(t *testing.T) []int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), orderingClientTimeout)
	defer cancel()

	response, err := f.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{f.topic}})
	require.NoError(t, err, "could not read metadata for topic %s", f.topic)

	for _, topic := range response.Topics {
		if topic.Name != f.topic {
			continue
		}

		if topic.Error != nil || len(topic.Partitions) == 0 {
			t.Skipf(
				"event ordering integration test: topic %s is not available on the broker (%v). "+
					"Provision the topics with scripts/kafka-provision.sh before running this test",
				f.topic, topic.Error,
			)

			return nil
		}

		ids := make([]int, 0, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			ids = append(ids, partition.ID)
		}
		sort.Ints(ids)

		require.Greater(t, len(ids), 1,
			"topic %s has %d partition(s): with a single partition every ordering assertion "+
				"below holds by construction and proves nothing about keying. Provision the "+
				"topic with the required minimum of 6 partitions",
			f.topic, len(ids))

		return ids
	}

	t.Skipf("event ordering integration test: the broker returned no metadata for topic %s", f.topic)

	return nil
}

// endOffsets records where every partition of the topic ENDS before the test publishes.
//
// The fetch loop starts from these offsets, which is what lets the test run against a topic
// that already holds unrelated traffic — including messages another run left behind — without
// reading a single one of them.
//
// Parameters:
//   - t *testing.T: the test.
//
// Returns:
//   - map[int]int64: the next offset to read for each partition.
func (f *orderingFixture) endOffsets(t *testing.T) map[int]int64 {
	t.Helper()

	requests := make([]kafka.OffsetRequest, 0, len(f.partitions))
	for _, partition := range f.partitions {
		requests = append(requests, kafka.LastOffsetOf(partition))
	}

	ctx, cancel := context.WithTimeout(context.Background(), orderingClientTimeout)
	defer cancel()

	response, err := f.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{f.topic: requests},
	})
	require.NoError(t, err, "could not list the end offsets of topic %s", f.topic)

	offsets := make(map[int]int64, len(f.partitions))
	for _, partition := range response.Topics[f.topic] {
		require.NoError(t, partition.Error, "the broker reported an error for %s/%d", f.topic, partition.Partition)
		offsets[partition.Partition] = partition.LastOffset
	}
	require.Len(t, offsets, len(f.partitions),
		"the broker reported offsets for %d of %d partitions of %s", len(offsets), len(f.partitions), f.topic)

	return offsets
}

// expectedPartitionFor predicts where a key lands, using the SAME balancer the publisher
// writes with.
//
// This is what turns "the events arrived in order" into evidence about the KEY. If the
// message key stopped being the ledger id — becoming the event id, say — the observed
// partition would no longer match the partition this function predicts for the ledger, and
// the failure would name the key rather than leaving ordering as the only symptom.
//
// Parameters:
//   - key string: the partition key, which for this test is a ledger id.
//
// Returns:
//   - int: the partition id the stable hash resolves the key to.
func (f *orderingFixture) expectedPartitionFor(key string) int {
	return kafka.Murmur2Balancer{}.Balance(kafka.Message{Topic: f.topic, Key: []byte(key)}, f.partitions...)
}

// decodeRecord turns one fetched record into an observation, reporting whether it belongs to
// this run.
//
// A record that cannot be decoded is SKIPPED rather than failed. The topic is shared, so a
// message this test cannot parse cannot be attributed to it either; it is logged so that a
// systematic decoding problem is still visible, and a message of this run that failed to
// decode surfaces anyway as a shortfall in the consume loop's count.
//
// Parameters:
//   - t *testing.T: the test, for diagnostic logging only.
//   - partition int: the partition the record came from.
//   - record *kafka.Record: the fetched record. Its Key and Value are copied out, since they
//     are only valid until the next ReadRecord call.
//
// Returns:
//   - orderingObservation: the decoded observation.
//   - bool: true when the record belongs to this run and was fully decoded.
func (f *orderingFixture) decodeRecord(t *testing.T, partition int, record *kafka.Record) (orderingObservation, bool) {
	t.Helper()

	key, err := kafka.ReadAll(record.Key)
	if err != nil {
		t.Logf("could not read the key of %s/%d offset %d: %v", f.topic, partition, record.Offset, err)

		return orderingObservation{}, false
	}

	value, err := kafka.ReadAll(record.Value)
	if err != nil {
		t.Logf("could not read the value of %s/%d offset %d: %v", f.topic, partition, record.Offset, err)

		return orderingObservation{}, false
	}

	var envelope orderingEnvelope
	if err = json.Unmarshal(value, &envelope); err != nil {
		t.Logf("%s/%d offset %d does not decode as a ledger event envelope: %v", f.topic, partition, record.Offset, err)

		return orderingObservation{}, false
	}

	var payload orderingPayload
	if err = json.Unmarshal(envelope.Payload, &payload); err != nil {
		t.Logf("the payload of event %s at %s/%d offset %d does not decode: %v",
			envelope.EventID, f.topic, partition, record.Offset, err)

		return orderingObservation{}, false
	}

	if payload.Data.MetaData.Run != f.runID {
		return orderingObservation{}, false
	}

	return orderingObservation{
		partition: partition,
		offset:    record.Offset,
		key:       string(key),
		envelope:  envelope,
		payload:   payload,
	}, true
}

// fetchPartition reads whatever one partition has at or after an offset.
//
// Records arrive in OFFSET ORDER, which is the whole reason the test reads with Fetch rather
// than through a consumer group: the sequence returned here is the sequence the partition
// holds, with nothing in between to reorder it.
//
// Parameters:
//   - t *testing.T: the test.
//   - partition int: the partition to read.
//   - offset int64: the offset to read from.
//
// Returns:
//   - []orderingObservation: this run's records from the response, in offset order.
//   - int64: the offset to continue from.
func (f *orderingFixture) fetchPartition(t *testing.T, partition int, offset int64) ([]orderingObservation, int64) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), orderingClientTimeout)
	defer cancel()

	response, err := f.client.Fetch(ctx, &kafka.FetchRequest{
		Topic:     f.topic,
		Partition: partition,
		Offset:    offset,
		MinBytes:  1,
		MaxBytes:  orderingFetchMaxBytes,
		MaxWait:   orderingFetchMaxWait,
	})
	require.NoError(t, err, "fetching %s/%d from offset %d failed", f.topic, partition, offset)
	require.NoError(t, response.Error, "the broker rejected the fetch of %s/%d from offset %d", f.topic, partition, offset)

	cursor := offset
	observations := make([]orderingObservation, 0, orderingEventsPerAggregate)

	for {
		record, readErr := response.Records.ReadRecord()
		if errors.Is(readErr, io.EOF) || record == nil {
			break
		}
		require.NoError(t, readErr, "reading a record from %s/%d failed", f.topic, partition)

		// A fetch may return a batch that STARTS BEFORE the requested offset, so records
		// already accounted for are dropped rather than counted twice.
		if record.Offset < offset {
			continue
		}
		cursor = record.Offset + 1

		if observation, ok := f.decodeRecord(t, partition, record); ok {
			observations = append(observations, observation)
		}
	}

	return observations, cursor
}

// consumeRun reads until every event of this run has been observed, or fails on timeout.
//
// Duplicates are EXPECTED and tolerated. Delivery is at-least-once by design: a relay that
// stops between a successful publish and marking its row dispatched republishes the row when
// the lease expires, and the duplicate is suppressed at the subscriber on event_id, which is
// the documented idempotency key. The first copy of each event is kept, because its position
// in the partition is the one that reflects the publish under test.
//
// Parameters:
//   - t *testing.T: the test.
//   - from map[int]int64: the per-partition offsets to start reading at.
//   - want int: how many distinct events of this run to wait for.
//
// Returns:
//   - []orderingObservation: exactly want observations, in the order they were read. The FIRST
//     appearance of each event, deduplicated on event_id.
//   - int: how many redeliveries were swallowed to produce that slice. Returned rather than
//     only logged because a caller asserting on at-least-once behaviour cannot recover the
//     number from the deduplicated slice — it is zero there by construction — and a test that
//     reported it from the slice would claim "no duplicates" on a run that had them.
func (f *orderingFixture) consumeRun(
	t *testing.T,
	from map[int]int64,
	want int,
) ([]orderingObservation, int) {
	t.Helper()

	cursors := make(map[int]int64, len(from))
	for partition, offset := range from {
		cursors[partition] = offset
	}

	deadline := time.Now().Add(orderingConsumeTimeout)
	seen := make(map[string]struct{}, want)
	observed := make([]orderingObservation, 0, want)
	duplicates := 0

	for len(seen) < want && time.Now().Before(deadline) {
		for _, partition := range f.partitions {
			batch, cursor := f.fetchPartition(t, partition, cursors[partition])
			cursors[partition] = cursor

			for _, observation := range batch {
				if _, repeated := seen[observation.envelope.EventID]; repeated {
					duplicates++

					continue
				}

				seen[observation.envelope.EventID] = struct{}{}
				observed = append(observed, observation)
			}

			if len(seen) >= want {
				break
			}
		}
	}

	if duplicates > 0 {
		// Not a failure: at-least-once permits it. Logged because a large number would say
		// something about the relay's lease handling that an operator would want to know.
		t.Logf("observed %d duplicate delivery(ies), which at-least-once permits; deduplicated on event_id", duplicates)
	}

	require.Len(t, observed, want,
		"timed out after %s waiting for this run's events: observed %d of %d on %s, "+
			"partition cursors %v. The relay publishes one event per aggregate per claim, so a "+
			"shortfall means rows were not claimed, not published, or not decodable.%s",
		orderingConsumeTimeout, len(observed), want, f.topic, cursors,
		f.describeTopicSharing())

	return observed, duplicates
}

// describeTopicSharing reports whether this topic is shared with anything else, for the
// shortfall message to include.
//
// It exists because of a diagnosis that cost real time and would have cost it again. A
// shortfall here has two causes that look identical — an event the relay never published, and
// an event published to a topic another process is also driving — and the second one is the
// likely one whenever the topic prefix is left at its default while parallel workspaces share
// one broker. Sibling traffic does not corrupt the count directly, since every record is
// filtered on this run's marker, but it does share the partitions and the fetch budget, and a
// run competing with a heavy neighbour can fail to observe a record that is genuinely there.
// Naming the prefix in the failure turns "the relay lost an event" into a question the reader
// can answer in one command.
//
// Returns:
//   - string: a leading-newline note about the topic namespace, or the empty string when the
//     prefix is already clone-scoped.
func (f *orderingFixture) describeTopicSharing() string {
	prefix := "(unreadable)"
	if cnf, err := config.Fetch(); err == nil {
		prefix = cnf.Kafka.TopicPrefix
	}

	return fmt.Sprintf(
		"\n\nThe topic namespace in use is %q (topic %s). If a broker is shared with other "+
			"workspaces, set KAFKA_TOPIC_PREFIX to a value unique to this one and provision it "+
			"with scripts/kafka-provision.sh, so this run's partitions carry only its own "+
			"traffic. Verify with: kafka-topics --list",
		prefix, f.topic,
	)
}

// orderingEventName is the event this test publishes, derived from the transaction status the
// way the transaction execution path derives it.
//
// Returns:
//   - string: the event name, "transaction.applied" for an applied transaction.
func orderingEventName() string {
	return model.EventTypeForTransactionStatus(orderingTransactionStatus)
}

// orderingNextRandom advances a deterministic 64-bit linear congruential generator.
//
// A generator written out here rather than math/rand, because the interleaving must be
// IDENTICAL on every run, on every platform and under every Go release: an ordering failure
// has to be reproducible from the log alone, and a schedule that shuffled differently each
// time would make a single flake impossible to re-create. The constants are Knuth's MMIX
// pair; only the high bits are used, since an LCG's low bits are weak.
//
// Parameters:
//   - state uint64: the current state.
//
// Returns:
//   - uint64: the next state.
func orderingNextRandom(state uint64) uint64 {
	return state*6364136223846793005 + 1442695040888963407
}

// orderingInterleave builds the publish schedule: a merge of every aggregate's own sequence.
//
// The result is INTERLEAVED rather than grouped — A1, B1, A2, C1, B2, A3, … — and irregularly
// so, not a round robin. That matters for what the test can conclude: if each aggregate's
// events were published contiguously, a pipeline that merely preserved the global publish
// order would satisfy every per-aggregate assertion without ever routing by key, and the
// test would pass while proving nothing. Interleaving means the per-aggregate order is a
// SUBSEQUENCE of the publish order, recoverable only if the events are actually partitioned
// by aggregate.
//
// Within one aggregate the sequence numbers are strictly increasing and gapless, which is
// what makes the consumption-side assertion a simple equality against 1..perAggregate.
//
// Parameters:
//   - aggregates int: how many aggregates take part.
//   - perAggregate int: how many events each aggregate emits.
//   - seed uint64: the generator seed. A fixed seed yields a fixed schedule.
//
// Returns:
//   - []orderingSlot: the publish schedule, aggregates*perAggregate entries long.
func orderingInterleave(aggregates, perAggregate int, seed uint64) []orderingSlot {
	issued := make([]int, aggregates)

	live := make([]int, 0, aggregates)
	for aggregate := 0; aggregate < aggregates; aggregate++ {
		live = append(live, aggregate)
	}

	schedule := make([]orderingSlot, 0, aggregates*perAggregate)
	state := seed

	for len(live) > 0 {
		state = orderingNextRandom(state)
		pick := int((state >> 33) % uint64(len(live)))
		aggregate := live[pick]

		issued[aggregate]++
		schedule = append(schedule, orderingSlot{aggregate: aggregate, sequence: issued[aggregate]})

		if issued[aggregate] == perAggregate {
			live = append(live[:pick], live[pick+1:]...)
		}
	}

	return schedule
}

// orderingTransaction builds the payload object for one event.
//
// It is a real *model.Transaction, which is the type the transaction execution call site
// passes, so the event travels the same derivation path as production: the aggregate id comes
// from the transaction id, and the event id is derived from it, which is what gives every
// event of this run a distinct identity that the outbox's unique index accepts.
//
// The run identifier, the aggregate and the sequence number are carried in the transaction's
// METADATA — a map the model already exposes for exactly this kind of caller-supplied context
// — so the consumption-side assertions read the sequence off the message a subscriber would
// receive rather than off test-local bookkeeping.
//
// Parameters:
//   - runID string: this run's identifier.
//   - ledgerID string: the aggregate's ledger id, which becomes the partition key.
//   - aggregate int: the aggregate's index, for readable identifiers.
//   - sequence int: the event's 1-based position in its aggregate's sequence.
//
// Returns:
//   - *model.Transaction: the payload object.
func orderingTransaction(runID, ledgerID string, aggregate, sequence int) *model.Transaction {
	return &model.Transaction{
		TransactionID: fmt.Sprintf("txn_%s_a%02d_s%03d", runID, aggregate, sequence),
		Reference:     fmt.Sprintf("ref_%s_a%02d_s%03d", runID, aggregate, sequence),
		Source:        fmt.Sprintf("bln_%s_a%02d_source", runID, aggregate),
		Destination:   fmt.Sprintf("bln_%s_a%02d_destination", runID, aggregate),
		Amount:        float64(sequence),
		Precision:     100,
		Currency:      "USD",
		Status:        orderingTransactionStatus,
		CreatedAt:     time.Now().UTC(),
		MetaData: map[string]interface{}{
			orderingMetaRun:       runID,
			orderingMetaAggregate: ledgerID,
			orderingMetaSequence:  sequence,
		},
	}
}

// publishRun captures the whole schedule through the PRODUCTION capture path for a
// transaction event: prepareTransactionEventOutbox, the function the single-transaction
// execution path calls immediately before it hands the row to the atomic writer.
//
// WithEventLedgerID is what makes this test about requirement R-6. A transaction payload
// carries no ledger of its own, so the ledger is supplied by the caller, and a supplied
// ledger becomes both the row's ledger_id and its PARTITION KEY. That single value is then
// the Kafka message key, which is why the observed key can be asserted to equal the ledger id.
//
// THIS HARNESS SUPPLYING IT PROVES NOTHING ABOUT PRODUCTION, and it must not be read as
// doing so: it seeds many ledgers directly rather than executing many transactions. That
// production supplies the ledger — buildTransactionExecutionWork resolving it from the
// loaded balances — is proved separately and without a broker by
// TestEventOrdering_ProductionCaptureKeysTransactionEventsByLedgerID. The two together are
// the requirement: production keys by ledger, and a ledger-keyed stream is consumed in
// order.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - t *testing.T: the test.
//   - ledgers []string: the ledger id of each aggregate, indexed by aggregate number.
//   - schedule []orderingSlot: the publish schedule.
//
// Returns:
//   - []string: the event id of every published event, in publish order. They are DERIVED
//     rather than read back, and the consumption-side assertions check the derivation against
//     what actually arrived.
func (f *orderingFixture) publishRun(
	ctx context.Context,
	t *testing.T,
	ledgers []string,
	schedule []orderingSlot,
) []string {
	t.Helper()

	eventIDs := make([]string, 0, len(schedule))

	for _, slot := range schedule {
		ledgerID := ledgers[slot.aggregate]
		transaction := orderingTransaction(f.runID, ledgerID, slot.aggregate, slot.sequence)

		// One ledger, a different balance pair per event. The balances are what production
		// reads the ledger from, and their ids are what a payload-derived key would have
		// used instead.
		sourceBalance := &model.Balance{
			BalanceID: fmt.Sprintf("bln_%s_a%02d_s%03d_source", f.runID, slot.aggregate, slot.sequence),
			LedgerID:  ledgerID,
		}
		destinationBalance := &model.Balance{
			BalanceID: fmt.Sprintf("bln_%s_a%02d_s%03d_destination", f.runID, slot.aggregate, slot.sequence),
			LedgerID:  ledgerID,
		}

		row, err := f.blnk.prepareTransactionEventOutbox(ctx, transaction, sourceBalance, destinationBalance)
		require.NoError(t, err,
			"preparing event %d of aggregate %d (transaction %s) failed",
			slot.sequence, slot.aggregate, transaction.TransactionID)
		require.NotNil(t, row,
			"event publishing must be configured for this fixture, or there is nothing to order")

		eventName := row.EventType
		require.Equal(t, orderingEventName(), eventName,
			"the production path must derive the event name from the transaction status")
		require.Equal(t, ledgerID, row.LedgerID,
			"the production path must record the ledger it resolved from the balances")
		require.Equal(t, ledgerID, row.PartitionKey,
			"and that ledger must be the ordering key, not the source balance id: keying on the balance would split one ledger's events across partitions")

		require.NoError(t, f.ds.InsertEventOutbox(ctx, row),
			"persisting the prepared event row for transaction %s failed", transaction.TransactionID)

		eventIDs = append(eventIDs, model.DeriveEventID(transaction.TransactionID, eventName, model.SchemaVersionV1))
	}

	return eventIDs
}

// requireAllDispatched waits until every row of this run has reached its dispatched terminal
// state, which is the last step of the path under test: claim, publish, mark.
//
// It polls rather than sleeping a fixed time, and it re-reads only the rows it is still
// waiting for, so the common case costs one round of reads.
//
// A timeout is CLASSIFIED rather than reported as one undifferentiated failure. There are two
// materially different reasons the rows might not have moved, and calling them both "the relay
// did not dispatch" would be wrong half the time — see leasedElsewhere.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - t *testing.T: the test.
//   - eventIDs []string: every event id this run captured.
func (f *orderingFixture) requireAllDispatched(ctx context.Context, t *testing.T, eventIDs []string) {
	t.Helper()

	outstanding := make([]string, len(eventIDs))
	copy(outstanding, eventIDs)

	deadline := time.Now().Add(orderingDispatchTimeout)
	lastReason := "no row was read"

	for len(outstanding) > 0 && time.Now().Before(deadline) {
		remaining := make([]string, 0, len(outstanding))

		for _, eventID := range outstanding {
			row, err := f.ds.GetEventByID(ctx, eventID)
			if err != nil {
				lastReason = fmt.Sprintf("reading event %s failed: %v", eventID, err)
				remaining = append(remaining, eventID)

				continue
			}

			if row.Status != model.EventOutboxStatusDispatched {
				lastReason = fmt.Sprintf("event %s is %s after %d attempt(s): %s",
					eventID, row.Status, row.Attempts, row.LastError)
				remaining = append(remaining, eventID)

				continue
			}
		}

		outstanding = remaining
		if len(outstanding) > 0 {
			time.Sleep(orderingDispatchPollInterval)
		}
	}

	if len(outstanding) == 0 {
		return
	}

	if f.leasedElsewhere(ctx, outstanding) {
		t.Skipf(
			"event ordering integration test: all %d of this run's outbox rows are held by a lease "+
				"that this test's own relay could not have taken (longer than its %s lock duration, "+
				"with no attempt recorded), so another process has taken the work away and no verdict "+
				"about ordering can be reached. Run against an isolated stack.%s",
			len(outstanding), orderingRelayLockDuration,
			f.describeEventRows(ctx, outstanding, orderingDiagnosticRows),
		)

		return
	}

	t.Fatalf(
		"%d of %d outbox rows were still not dispatched after %s; last reason: %s%s",
		len(outstanding), len(eventIDs), orderingDispatchTimeout, lastReason,
		f.describeEventRows(ctx, outstanding, orderingDiagnosticRows),
	)
}

// leasedElsewhere reports whether EVERY row given is held by a lease this test's own relay
// could not have taken.
//
// It exists because a stalled run has two entirely different causes and they demand opposite
// responses:
//
//   - THE RELAY IS NOT CLAIMING. Rows sit with no lease at all — locked_until is null — or with
//     recorded attempts. That is a defect, and the test must fail.
//   - ANOTHER PROCESS HOLDS THE ROWS. The outbox is a shared table and any relay instance
//     drains all of it, so a second process — or a test simulating a crashed relay by leasing
//     rows — can hold this run's rows for as long as its own lease lasts. The claim then
//     correctly refuses them, which is the designed behaviour, not a fault. This test cannot
//     reach a verdict about ordering in that state, so it skips and says so rather than
//     reporting a failure it cannot attribute.
//
// The two are told apart EXACTLY, not heuristically: this test pins its relay's lock duration,
// so a lease reaching beyond now plus that duration is provably not its own, and a row with no
// recorded attempt was never even tried.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - eventIDs []string: the rows that failed to reach a terminal state.
//
// Returns:
//   - bool: true only when every row is foreign-leased. False on the first row that is not —
//     including any row that cannot be read, since an unreadable row is not evidence of
//     anything.
func (f *orderingFixture) leasedElsewhere(ctx context.Context, eventIDs []string) bool {
	if len(eventIDs) == 0 {
		return false
	}

	horizon := time.Now().Add(orderingRelayLockDuration)

	for _, eventID := range eventIDs {
		row, err := f.ds.GetEventByID(ctx, eventID)
		if err != nil {
			return false
		}

		if row.Attempts != 0 || row.LockedUntil == nil || !row.LockedUntil.After(horizon) {
			return false
		}
	}

	return true
}

// mintLedgers creates the aggregates' ledger ids and their predicted partitions, insisting
// that the set spans MORE THAN ONE partition.
//
// The insistence is the point. Every ordering assertion in this file is trivially satisfied
// by a single partition, because a partition is ordered by construction — so a run whose keys
// all hashed to one partition would pass while proving nothing about keying at all. The ids
// are random, so the set is re-drawn rather than the test failing on a draw that says nothing
// about the implementation.
//
// Parameters:
//   - t *testing.T: the test.
//   - count int: how many ledger ids to mint.
//
// Returns:
//   - []string: the ledger ids, indexed by aggregate number.
//   - map[string]int: the partition the stable hash resolves each ledger id to.
func (f *orderingFixture) mintLedgers(t *testing.T, count int) ([]string, map[string]int) {
	t.Helper()

	for attempt := 1; attempt <= orderingLedgerMintAttempts; attempt++ {
		ledgers := make([]string, 0, count)
		expected := make(map[string]int, count)
		distinct := make(map[int]struct{}, count)

		for index := 0; index < count; index++ {
			ledgerID := model.GenerateUUIDWithSuffix("ldg")
			partition := f.expectedPartitionFor(ledgerID)

			ledgers = append(ledgers, ledgerID)
			expected[ledgerID] = partition
			distinct[partition] = struct{}{}
		}

		if len(distinct) > 1 {
			return ledgers, expected
		}

		t.Logf("attempt %d drew %d ledger ids that all hash to one partition; re-drawing", attempt, count)
	}

	t.Fatalf("could not mint %d ledger ids spanning more than one of the %d partitions of %s after %d attempts",
		count, len(f.partitions), f.topic, orderingLedgerMintAttempts)

	return nil, nil
}

// orderingScheduleSwitches counts how often the publish schedule changes aggregate between
// consecutive slots. It is the measure of how interleaved the schedule is, and it is asserted
// so that a change to the generator cannot quietly turn the schedule into per-aggregate runs
// and weaken every assertion that depends on it.
//
// Parameters:
//   - schedule []orderingSlot: the publish schedule.
//
// Returns:
//   - int: the number of adjacent pairs belonging to different aggregates.
func orderingScheduleSwitches(schedule []orderingSlot) int {
	switches := 0
	for index := 1; index < len(schedule); index++ {
		if schedule[index].aggregate != schedule[index-1].aggregate {
			switches++
		}
	}

	return switches
}

// orderingRequireRelayGoroutineGone asserts that no goroutine is still running the relay.
//
// Stop closes the stop channel and then WAITS on the processor's WaitGroup, so by the time it
// returns its loop goroutine — and the per-partition-key publish goroutines that loop spawned,
// which processBatch waits for — must all have finished. Scanning the goroutine dump for the
// processor's frames tests that directly, which is both exact and unaffected by whatever else
// happens to be running in the process; a goroutine count would be neither.
//
// Parameters:
//   - t *testing.T: the test.
func orderingRequireRelayGoroutineGone(t *testing.T) {
	t.Helper()

	// Grown until the dump fits, because a truncated dump could hide the very frame this is
	// looking for and would then report a leak as clean.
	size := 64 << 10
	var dump []byte

	for {
		buffer := make([]byte, size)
		written := runtime.Stack(buffer, true)

		if written < size {
			dump = buffer[:written]

			break
		}

		size *= 2
	}

	assert.NotContains(t, string(dump), "EventRelayProcessor",
		"a goroutine is still executing the event outbox relay after Stop() returned. Stop closes "+
			"the stop channel and then waits on the WaitGroup, so a surviving frame means the "+
			"worker was not joined")

	t.Logf("goroutines after the relay stopped: %d", runtime.NumGoroutine())
}

// TestEventOrdering_EventsSharingAnAggregateAreConsumedInPublishOrder is acceptance criterion
// V-6.
//
// The two subtests are the two mechanisms the criterion rests on, checked in the order they
// occur in the pipeline and sharing one fixture:
//
//  1. Delivery. Events are captured through PublishEvent, drained by a real
//     EventRelayProcessor and read back off the topic, and each aggregate's events must arrive
//     on ONE partition — the one the stable hash of its ledger id resolves to — in the order
//     they were published.
//  2. Claiming. With the topic drained and the relay stopped, rows are seeded with an
//     insertion order that deliberately contradicts their occurrence order, and the claim must
//     still hand them over oldest first.
//
// They are subtests of one function rather than two top-level tests because the second needs
// what the first leaves behind: an outbox with nothing of this run's still pending, which is
// what lets a small batch put the claim's LIMIT under pressure on this test's own rows.
func TestEventOrdering_EventsSharingAnAggregateAreConsumedInPublishOrder(t *testing.T) {
	fixture := newOrderingFixture(t)

	t.Run("kafka delivery preserves per-aggregate publish order", func(t *testing.T) {
		orderingAssertDeliveryPreservesPerAggregateOrder(t, fixture)
	})

	t.Run("the outbox claim yields occurrence order, not insertion order", func(t *testing.T) {
		orderingAssertClaimYieldsOccurrenceOrder(t, fixture)
	})
}

// relayContentionParkEarlierRow puts a row into the state a relay that claimed it and then
// stalled leaves behind: processing, leased far into the future, and never dispatched.
//
// # Why this is written with SQL rather than through ClaimPendingEventOutbox
//
// A real claim would be more faithful if it could be aimed, and it cannot. The claim is FIFO
// over the WHOLE table and the outbox is shared — with sibling workspaces, with a running
// server, with this run's own earlier tests — so a batch-of-one claim returns whatever row is
// oldest, which is very often not this test's. Looping until the wanted row appears would drag
// every other pending row through a lease as a side effect, and the state that arrives is
// identical either way. The subject under test is the CLAIM'S per-partition-key guard, not the
// route by which a predecessor came to be in flight.
//
// Parameters:
//   - t *testing.T: the test.
//   - f *orderingFixture: the fixture, for its connection pool.
//   - eventID string: the row to park.
//
// Returns:
//   - string: the claim token written, so the release can prove it is undoing its own work.
func relayContentionParkEarlierRow(t *testing.T, f *orderingFixture, eventID string) string {
	t.Helper()

	token := "relay-contention-" + f.runID

	result, err := f.pool.ExecContext(context.Background(), `
		UPDATE blnk.event_outbox
		SET status = 'processing',
			locked_until = NOW() + $2::interval,
			claim_token = $3,
			first_attempted_at = COALESCE(first_attempted_at, NOW()),
			last_attempted_at = NOW()
		WHERE event_id = $1
	`, eventID, relayContentionLease.String(), token)
	require.NoError(t, err, "parking the earlier row of the contended aggregate failed")

	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected,
		"exactly the earlier row must be parked; parking none makes the observation below "+
			"vacuous and parking more would block aggregates this test does not own")

	row, err := f.ds.GetEventByID(context.Background(), eventID)
	require.NoError(t, err, "re-reading the parked row failed")
	require.Equal(t, model.EventOutboxStatusProcessing, row.Status,
		"the premise is a predecessor IN FLIGHT; without it the later row is legitimately "+
			"claimable and nothing below means anything")
	require.NotNil(t, row.LockedUntil)
	require.Truef(t, row.LockedUntil.After(time.Now().Add(relayContentionObservation)),
		"the parked lease must outlast the observation window, or the row becomes claimable "+
			"through expiry rather than staying blocked by the ordering guard. lease until %s",
		row.LockedUntil)

	return token
}

// relayContentionReleaseEarlierRow returns the parked row to pending so the relays can publish
// it normally, undoing exactly what relayContentionParkEarlierRow did.
//
// The token is presented in the predicate rather than only the event id: a release that matched
// on the id alone would also undo a claim a REAL relay had taken in the meantime, and this
// function's whole premise is that no relay could.
func relayContentionReleaseEarlierRow(t *testing.T, f *orderingFixture, eventID, token string) {
	t.Helper()

	result, err := f.pool.ExecContext(context.Background(), `
		UPDATE blnk.event_outbox
		SET status = 'pending',
			locked_until = NULL,
			claim_token = NULL,
			attempts = 0,
			next_attempt_at = NOW()
		WHERE event_id = $1 AND claim_token = $2
	`, eventID, token)
	require.NoError(t, err, "releasing the parked row failed")

	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected,
		"the parked row must still carry this test's own claim token: if it does not, something "+
			"else took the row while it was leased half an hour into the future, and the "+
			"observation above was measuring that instead of the ordering guard")
}

// relayContentionStartRelay starts one relay processor on the production lease and registers its
// shutdown.
//
// Returns:
//   - *EventRelayProcessor: the started processor.
func relayContentionStartRelay(t *testing.T, f *orderingFixture, ctx context.Context, label string) *EventRelayProcessor {
	t.Helper()

	relay := NewEventRelayProcessor(f.blnk).
		WithPollInterval(orderingRelayPollInterval).
		WithBatchSize(orderingRelayBatchSize).
		WithLockDuration(orderingRelayLockDuration)

	relayCtx, cancelRelay := context.WithCancel(ctx)
	t.Cleanup(func() {
		relay.Stop()
		cancelRelay()
	})

	require.NoErrorf(t, relay.Start(relayCtx),
		"relay %s refused to start; the returned obstacle names the missing precondition", label)
	require.Truef(t, relay.IsRunning(),
		"relay %s reported no obstacle yet is not running", label)

	return relay
}

// TestEventOrdering_TwoRelaysContendingForOneAggregateCannotOvertakeEachOther is the
// DISTRIBUTED half of acceptance criterion V-6.
//
// # What the single-relay test could not establish
//
// TestEventOrdering_EventsSharingAnAggregateAreConsumedInPublishOrder proves two things about
// one process: that a batch is published in occurrence order, and that the message key routes an
// aggregate to one partition. Both are properties of a single relay draining a backlog, and
// neither says anything about the deployment that actually runs — two or more server replicas,
// each with its own relay, polling the same outbox table. In that deployment the ordering
// guarantee cannot come from anything inside one process, because no process can see what
// another has claimed. It comes from ONE SQL PREDICATE: a row is a claim candidate only while no
// earlier row for its partition key is still pending or processing.
//
// Delete that predicate and the single-relay test still passes in full — one relay claiming a
// batch still publishes it in order — while the deployment silently loses per-aggregate ordering
// the moment a second replica exists. That is the gap this test closes, and it is why the
// arrangement is two relays rather than a larger backlog.
//
// # The arrangement
//
// One aggregate, so both events share one partition key and the guard is the only thing that can
// order them. Two events. The earlier is parked IN FLIGHT under an absurd lease, which is what a
// relay that claimed a row and then stalled — GC pause, slow broker, lost pod — leaves behind.
// Two relays then poll the table for two seconds.
//
//   - THE LATER ROW MUST NOT MOVE. Not claimed, not leased, not published. A relay that took it
//     would publish the aggregate's second event before its first, and no consumer could recover
//     the order afterwards because the records would be in the partition the wrong way round.
//   - THEN THE PREDECESSOR SETTLES, and the same two relays must publish both, in order, with
//     the earlier record at the lower offset.
//
// The second half is also the first half's non-vacuity proof, and that is why they are one test
// rather than two. "Nothing was published for two seconds" is exactly what a pair of relays that
// never started, or that could not reach the broker, would also produce; the only way to know the
// silence meant refusal is to remove the reason for it and watch the same relays deliver.
func TestEventOrdering_TwoRelaysContendingForOneAggregateCannotOvertakeEachOther(t *testing.T) {
	fixture := newOrderingFixture(t)
	ctx := context.Background()

	// ONE aggregate, minted directly rather than through mintLedgers: that helper requires the
	// ids it draws to span more than one partition, which is exactly right for the delivery test
	// and unsatisfiable for a single key. Contention on ONE partition key is the whole subject
	// here, so a second ledger would add only a stream that is trivially independent.
	ledgerID := model.GenerateUUIDWithSuffix("ldg")
	ledgers := []string{ledgerID}
	contendedPartition := fixture.expectedPartitionFor(ledgerID)
	require.Containsf(t, fixture.partitions, contendedPartition,
		"the contended ledger must hash to a partition the topic actually has; %s resolved to %d "+
			"against %v", ledgerID, contendedPartition, fixture.partitions)
	t.Logf("contended aggregate: ledger %s on %s/%d", ledgerID, fixture.topic, contendedPartition)

	// Recorded before anything is captured, so the fetch below reads this run alone.
	startOffsets := fixture.endOffsets(t)

	eventIDs := fixture.publishRun(ctx, t, ledgers, []orderingSlot{
		{aggregate: 0, sequence: 0},
		{aggregate: 0, sequence: 1},
	})
	require.Len(t, eventIDs, 2, "the contended aggregate must have exactly a predecessor and a successor")

	earlier, later := eventIDs[0], eventIDs[1]
	require.NotEqual(t, earlier, later, "the two events must be distinct rows")

	token := relayContentionParkEarlierRow(t, fixture, earlier)

	first := relayContentionStartRelay(t, fixture, ctx, "A")
	second := relayContentionStartRelay(t, fixture, ctx, "B")

	t.Run("the successor is neither claimed nor published while its predecessor is in flight", func(t *testing.T) {
		deadline := time.Now().Add(relayContentionObservation)
		observations := 0

		for time.Now().Before(deadline) {
			row, err := fixture.ds.GetEventByID(ctx, later)
			require.NoError(t, err, "reading the blocked row failed")
			observations++

			require.Equalf(t, model.EventOutboxStatusPending, row.Status,
				"THE SUCCESSOR WAS CLAIMED while its predecessor was still in flight, so its "+
					"record can reach the partition first and the aggregate's order is lost "+
					"irrecoverably. A row is a claim candidate only while no earlier row for its "+
					"partition key is pending or processing — see claimPendingEventOutboxQuery. "+
					"row: status=%s attempts=%d token=%q", row.Status, row.Attempts, row.ClaimToken)
			require.Emptyf(t, row.ClaimToken,
				"a claim token on the blocked row means some relay leased it: %q", row.ClaimToken)
			require.Nilf(t, row.LockedUntil,
				"a lease on the blocked row means some relay claimed it: %v", row.LockedUntil)
			require.Zero(t, row.Attempts,
				"an attempt against the blocked row means a relay tried to publish it")

			time.Sleep(relayContentionPollInterval)
		}

		require.Greaterf(t, observations, 1,
			"the blocked row was read %d time(s): the observation window must span several poll "+
				"intervals or it asserts only that nothing had happened yet", observations)

		// AND NOTHING REACHED THE BROKER. The row state is the mechanism; this is the
		// consequence, and it is asserted separately because a relay that published without
		// recording the dispatch would satisfy the assertions above and still have broken the
		// order on the topic.
		records, _ := fixture.fetchPartition(t, contendedPartition, startOffsets[contendedPartition])
		require.Emptyf(t, records,
			"%d record(s) of this run reached %s/%d while the aggregate's earlier event was "+
				"still in flight. Whichever relay wrote them overtook its predecessor.",
			len(records), fixture.topic, contendedPartition)

		require.True(t, first.IsRunning(), "relay A must still be running, or the silence above is its absence")
		require.True(t, second.IsRunning(), "relay B must still be running, or the silence above is its absence")
	})

	t.Run("once the predecessor settles both are published in order", func(t *testing.T) {
		relayContentionReleaseEarlierRow(t, fixture, earlier, token)

		// The SAME two relays, still running, now do the work. That is what makes the silence
		// above a refusal rather than an absence.
		fixture.requireAllDispatched(ctx, t, eventIDs)

		observed, _ := fixture.consumeRun(t, startOffsets, len(eventIDs))
		require.Len(t, observed, 2)

		first.Stop()
		second.Stop()
		assert.False(t, first.IsRunning(), "IsRunning must report false once Stop has returned")
		assert.False(t, second.IsRunning(), "IsRunning must report false once Stop has returned")

		bySequence := make(map[int]orderingObservation, len(observed))
		for _, observation := range observed {
			require.Equal(t, ledgerID, observation.key,
				"every record of the contended aggregate must be keyed by its ledger")
			bySequence[observation.payload.Data.MetaData.Sequence] = observation
		}
		require.Len(t, bySequence, 2, "both sequences must be present exactly once")

		predecessor, successor := bySequence[0], bySequence[1]

		assert.Equal(t, predecessor.partition, successor.partition,
			"one partition key must resolve to one partition, or ordering is not even definable")
		assert.Equal(t, contendedPartition, predecessor.partition,
			"and it must be the partition the stable hash of the ledger resolves to, or the key "+
				"reaching the broker is not the key this test parked and released")
		assert.Lessf(t, predecessor.offset, successor.offset,
			"THE PREDECESSOR MUST OCCUPY THE LOWER OFFSET. Two relays published these records and "+
				"the partition is the only record of their order, so a successor at a lower offset "+
				"is a reordering no consumer can undo. sequence 0 at %s/%d offset %d, sequence 1 at "+
				"%s/%d offset %d",
			fixture.topic, predecessor.partition, predecessor.offset,
			fixture.topic, successor.partition, successor.offset)

		orderingRequireRelayGoroutineGone(t)
	})
}

// TestEventOrdering_ProductionCaptureKeysTransactionEventsByLedgerID is the proof that
// requirement R-6's partitioning dimension is supplied by PRODUCTION CODE and not by a test.
//
// # Why this test exists separately from the Kafka one
//
// The delivery subtest above proves that a message keyed by ledger id is consumed in
// per-aggregate order. It could not prove where that key came from, because its harness
// supplied the ledger itself — and for a while nothing in production did. Every real
// transaction event was therefore keyed on its SOURCE BALANCE, which preserves per-balance
// ordering rather than per-ledger ordering and puts a value that is not a ledger id in the
// row's ledger column, while a test that injected the option reported the requirement as met.
//
// So this drives prepareTransactionEventOutbox — the SINGLE place a transaction's event row is
// built, reached both by the atomic writer's caller before the write and by the post-commit
// fallback — and asserts what it produces. It needs no broker and no database: preparing a row
// is a marshal and no I/O, which is what lets this run everywhere rather than only where the
// integration fixture is reachable.
func TestEventOrdering_ProductionCaptureKeysTransactionEventsByLedgerID(t *testing.T) {
	// RESTORED WHEN THIS TEST ENDS. config.ConfigStore is a process-global atomic.Value, so the
	// broker list, topic prefix and sunset date installed below outlive this function and are
	// read by every later test in the package that calls config.Fetch — including the ones that
	// assert the UNCONFIGURED posture, which would then fail inside a test that never mentions
	// Kafka. Restoring is the established idiom here; see storeLegacyWebhookConfiguration in
	// webhooks_test.go and the outbox fixture's own store helper.
	previousConfiguration := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previousConfiguration != nil {
			config.ConfigStore.Store(previousConfiguration)

			return
		}

		// Nothing was installed before this test, so the store is returned to a configuration
		// that carries nothing rather than left holding this one. An empty value cannot be
		// written back into an atomic.Value, which is why this stores a zero Configuration.
		config.ConfigStore.Store(&config.Configuration{})
	})

	// Kafka brokers make event capture configured, and the deprecation window has to be
	// stated alongside them: the loader refuses brokers without one, so MockConfig would
	// silently store nothing and the assertions below would fail for a configuration
	// reason rather than a keying one. No broker is contacted — preparing a row performs
	// no I/O at all.
	config.MockConfig(&config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "postgres://localhost:5432/blnk?sslmode=disable"},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		Kafka:      config.KafkaConfig{Brokers: []string{"localhost:9092"}, TopicPrefix: "blnk"},
		// The SUNSET is the only end that is configurable, and it is REQUIRED alongside
		// brokers: the loader refuses a publishing deployment with no usable window, and
		// MockConfig runs the same validation, so omitting it would store nothing and every
		// assertion below would fail for a configuration reason rather than a keying one.
		// The start is derived from it as sunset minus the 30-day window.
		WebhookDeprecationSunsetDate: time.Now().UTC().Add(30 * 24 * time.Hour).Format(time.RFC3339),
	})

	loaded, err := config.Fetch()
	require.NoError(t, err, "the mocked configuration must publish, or nothing below is exercising event capture")
	require.NotEmpty(t, loaded.Kafka.Brokers, "event capture is only configured when brokers are")

	instance := &Blnk{}
	ctx := context.Background()

	ledgerID := model.GenerateUUIDWithSuffix("ldg")
	otherLedgerID := model.GenerateUUIDWithSuffix("ldg")

	sourceBalance := &model.Balance{
		BalanceID: model.GenerateUUIDWithSuffix("bln"),
		LedgerID:  ledgerID,
		Currency:  "USD",
	}
	destinationBalance := &model.Balance{
		BalanceID: model.GenerateUUIDWithSuffix("bln"),
		LedgerID:  otherLedgerID,
		Currency:  "USD",
	}

	transaction := &model.Transaction{
		TransactionID: model.GenerateUUIDWithSuffix("txn"),
		Reference:     "ref_" + model.GenerateUUIDWithSuffix("ord"),
		Source:        sourceBalance.BalanceID,
		Destination:   destinationBalance.BalanceID,
		Amount:        10,
		Precision:     100,
		PreciseAmount: big.NewInt(1000),
		Currency:      "USD",
		Status:        StatusApplied,
		CreatedAt:     time.Now().UTC(),
	}

	work, skipped := instance.buildTransactionExecutionWork(ctx, transaction, sourceBalance, destinationBalance)
	require.False(t, skipped, "a non-zero-amount transaction must produce persistable work")
	require.NotNil(t, work.transaction, "the work item must carry the finalised transaction")

	eventOutbox, err := instance.prepareTransactionEventOutbox(ctx, work.transaction, sourceBalance, destinationBalance)
	require.NoError(t, err, "a well-formed transaction payload must serialise")
	require.NotNil(t, eventOutbox,
		"the production execution path must PREPARE the transaction's event before the write; a nil row here means the event is captured after the commit again, outside the mutation's transaction")

	assert.Equal(t, ledgerID, eventOutbox.PartitionKey,
		"the Kafka message key must be the LEDGER id, which is requirement R-6's partitioning dimension; the source balance id keyed by the old fallback is what this asserts against")
	assert.Equal(t, ledgerID, eventOutbox.LedgerID,
		"and the row's ledger column must record that same authoritative ledger rather than NULL or a balance id")
	assert.NotEqual(t, sourceBalance.BalanceID, eventOutbox.PartitionKey,
		"a balance id as the key is the exact defect this test exists to catch")
	assert.Equal(t, "transaction.applied", eventOutbox.EventType,
		"the event string is still the status-derived one, unchanged by the keying")
	assert.Equal(t, model.EventOutboxStatusPending, eventOutbox.Status,
		"nothing is dispatched until the relay has published it; preparation only ever yields a pending row")

	t.Run("the ledger resolution prefers the source balance and falls back to the destination", func(t *testing.T) {
		assert.Equal(t, ledgerID, transactionLedgerID(sourceBalance, destinationBalance),
			"the source ledger wins, matching what the transaction queue already shards on")
		assert.Equal(t, otherLedgerID, transactionLedgerID(nil, destinationBalance),
			"a credit-only transaction is keyed by its destination's ledger")
		assert.Equal(t, otherLedgerID, transactionLedgerID(&model.Balance{LedgerID: "  "}, destinationBalance),
			"a blank source ledger is not a ledger: it would hash to its own partition and split one ledger's events across two")
		assert.Empty(t, transactionLedgerID(nil, nil),
			"a rejected transaction has no balances, and an empty result leaves the payload-derived key in place rather than inventing one")
	})
}

// orderingAssertDeliveryPreservesPerAggregateOrder drives the FULL path — PublishEvent, a
// pending outbox row, a real relay claiming and publishing it, the row marked dispatched — and
// then reads the topic back.
//
// Nothing is short-circuited, and that is deliberate: publishing straight to a writer would
// test kafka-go rather than Blnk, and the property under test lives in the join between the
// claim and the message key, which only exists when the outbox is in the path.
//
// Parameters:
//   - t *testing.T: the subtest.
//   - f *orderingFixture: the live fixture.
func orderingAssertDeliveryPreservesPerAggregateOrder(t *testing.T, f *orderingFixture) {
	ctx := context.Background()

	ledgers, expectedPartitions := f.mintLedgers(t, orderingAggregateCount)

	// Recorded BEFORE anything is published, so the fetch loop reads this run and nothing
	// that was on the topic beforehand.
	startOffsets := f.endOffsets(t)

	schedule := orderingInterleave(orderingAggregateCount, orderingEventsPerAggregate, orderingInterleaveSeed)
	require.Len(t, schedule, orderingAggregateCount*orderingEventsPerAggregate,
		"the schedule must hold every aggregate's every event exactly once")
	require.Greater(t, orderingScheduleSwitches(schedule), len(schedule)/2,
		"the publish schedule must genuinely interleave the aggregates: if each aggregate's events "+
			"were published contiguously, a pipeline that merely preserved the global publish order "+
			"would satisfy every assertion below without ever routing by key")

	eventIDs := f.publishRun(ctx, t, ledgers, schedule)
	t.Logf("captured %d events for %d aggregates in blnk.event_outbox", len(eventIDs), orderingAggregateCount)

	// The relay starts AFTER the whole backlog exists. That is the arrangement most likely to
	// expose a reordering defect: the first claims are full, so one batch carries many distinct
	// partition keys and is published across several workers at once — and a relay that split a
	// batch round-robin across those workers, instead of grouping it by partition key, would
	// reorder here and nowhere else.
	relay := NewEventRelayProcessor(f.blnk).
		WithPollInterval(orderingRelayPollInterval).
		WithBatchSize(orderingRelayBatchSize).
		WithLockDuration(orderingRelayLockDuration)

	relayCtx, cancelRelay := context.WithCancel(ctx)
	t.Cleanup(func() {
		// Idempotent, and registered before Start so a failure between here and the explicit
		// Stop below cannot leave the loop running against a publisher the fixture is about to
		// close.
		relay.Stop()
		cancelRelay()
	})

	require.NoError(t, relay.Start(relayCtx),
		"the relay refused to start; the returned obstacle names the missing precondition")
	require.True(t, relay.IsRunning(),
		"the relay reported no obstacle yet is not running")

	f.requireAllDispatched(ctx, t, eventIDs)
	observed, _ := f.consumeRun(t, startOffsets, len(eventIDs))

	relay.Stop()
	assert.False(t, relay.IsRunning(), "IsRunning must report false once Stop has returned")
	orderingRequireRelayGoroutineGone(t)

	orderingAssertObservations(t, f, ledgers, expectedPartitions, observed)
}

// orderingAssertObservations is where the criterion is decided.
//
// Parameters:
//   - t *testing.T: the subtest.
//   - f *orderingFixture: the fixture, for the topic and partition inventory.
//   - ledgers []string: the ledger id of each aggregate.
//   - expectedPartitions map[string]int: the partition the stable hash resolves each ledger to.
//   - observed []orderingObservation: every message of this run, in the order it was read.
func orderingAssertObservations(
	t *testing.T,
	f *orderingFixture,
	ledgers []string,
	expectedPartitions map[string]int,
	observed []orderingObservation,
) {
	t.Helper()

	eventName := orderingEventName()

	// Grouped by the ledger the PAYLOAD names rather than by the message key. If the key were
	// ever something else, that must be reported as a wrong key — grouping by the key would
	// instead split each aggregate into groups of one and leave a count mismatch as the only
	// symptom.
	byLedger := make(map[string][]orderingObservation, len(ledgers))
	partitionsSeen := make(map[int]struct{}, len(f.partitions))

	for _, observation := range observed {
		// An EMPTY key is balanced across partitions instead of being pinned to one, so it
		// destroys ordering with nothing in the message to show it.
		require.NotEmpty(t, observation.key,
			"event %s arrived with an empty message key: an unkeyed message is spread across "+
				"partitions, so per-aggregate ordering cannot hold for it",
			observation.envelope.EventID)

		ledgerID := observation.payload.Data.MetaData.Aggregate
		byLedger[ledgerID] = append(byLedger[ledgerID], observation)
		partitionsSeen[observation.partition] = struct{}{}
	}

	require.Len(t, byLedger, orderingAggregateCount,
		"expected events for %d aggregates, got %d distinct aggregates on %s",
		orderingAggregateCount, len(byLedger), f.topic)

	expectedSequence := make([]int, 0, orderingEventsPerAggregate)
	for sequence := 1; sequence <= orderingEventsPerAggregate; sequence++ {
		expectedSequence = append(expectedSequence, sequence)
	}

	for index, ledgerID := range ledgers {
		observations := byLedger[ledgerID]
		require.Len(t, observations, orderingEventsPerAggregate,
			"aggregate %d (ledger %s) produced %d of %d events", index, ledgerID, len(observations), orderingEventsPerAggregate)

		partition := observations[0].partition
		sequences := make([]int, 0, len(observations))
		offsets := make([]int64, 0, len(observations))

		for _, observation := range observations {
			// MECHANISM 1, in two assertions: the key IS the ledger id, and every event of the
			// aggregate therefore shares one partition.
			assert.Equal(t, ledgerID, observation.key,
				"event %s was keyed with %q instead of its ledger id: partitioning by ledger is "+
					"what pins an aggregate to one partition (requirement R-6)",
				observation.envelope.EventID, observation.key)
			require.Equal(t, partition, observation.partition,
				"aggregate %s was split across partitions %d and %d, so Kafka orders its events in "+
					"two independent sequences and no consumer can recover the publish order",
				ledgerID, partition, observation.partition)

			// The envelope a subscriber actually receives, checked field by field so that a
			// change to the wire contract cannot pass as an ordering test.
			assert.Equal(t, eventName, observation.envelope.EventType, "event %s carries the wrong event type", observation.envelope.EventID)
			assert.Equal(t, model.SchemaVersionV1, observation.envelope.SchemaVersion, "event %s carries the wrong schema version", observation.envelope.EventID)
			assert.Equal(t, observation.payload.Data.TransactionID, observation.envelope.AggregateID,
				"event %s must name the transaction it describes as its aggregate id", observation.envelope.EventID)
			assert.Equal(t, model.DeriveEventID(observation.envelope.AggregateID, eventName, model.SchemaVersionV1), observation.envelope.EventID,
				"the event id must be derived from the mutation identity, so a retried capture cannot admit a second row for one business event")
			assert.Equal(t, eventName, observation.payload.Event,
				"the payload must be the two-key webhook body verbatim, with the event name under \"event\"")

			sequences = append(sequences, observation.payload.Data.MetaData.Sequence)
			offsets = append(offsets, observation.offset)
		}

		assert.Equal(t, expectedPartitions[ledgerID], partition,
			"aggregate %s landed on partition %d, but the stable hash of its ledger id resolves to "+
				"partition %d: the message key is not the ledger id, or the balancer is not the "+
				"stable hash the subscriber contract assumes",
			ledgerID, partition, expectedPartitions[ledgerID])

		// THE CRITERION: this aggregate's events were consumed in the order they were
		// published. Only this aggregate's own subsequence is asserted — the publish schedule
		// interleaves the aggregates and Kafka provides no order between partitions, so a
		// global order is neither expected nor asserted.
		assert.Equal(t, expectedSequence, sequences,
			"aggregate %s was consumed out of publish order on partition %d: got %v",
			ledgerID, partition, sequences)
		assert.IsIncreasing(t, offsets,
			"aggregate %s must occupy strictly increasing offsets within its partition", ledgerID)

		t.Logf("aggregate %2d ledger %s -> partition %d, offsets %d..%d, %d events in publish order",
			index, ledgerID, partition, offsets[0], offsets[len(offsets)-1], len(observations))
	}

	// Without this the whole subtest could pass on a single-partition topic, where ordering is
	// guaranteed by construction and the key is irrelevant.
	require.Greater(t, len(partitionsSeen), 1,
		"every event of this run landed on one partition (%v), so the ordering assertions above "+
			"hold by construction and prove nothing about keying",
		partitionsSeen)

	t.Logf("this run spanned %d of the %d partitions of %s", len(partitionsSeen), len(f.partitions), f.topic)
}

// orderingAssertClaimYieldsOccurrenceOrder proves the second mechanism: the relay drains the
// outbox by OCCURRENCE, not by insertion.
//
// The distinction is invisible in the happy path, where rows are inserted in the order they
// occurred, and it is exactly what breaks per-aggregate ordering when it goes wrong — a
// mutation applied under contention, a coalesced batch, or any path that writes rows in an
// order the events did not happen in. So the rows here are inserted in an order that
// deliberately contradicts their occurred_at values, and they are BACKDATED so that a batch
// smaller than the seeded set puts the claim's LIMIT under pressure on this test's own rows
// even when the table holds unrelated pending work.
//
// Each round marks what it claimed dispatched before the next claim, which is what makes the
// second round return the NEXT three rows rather than the same three, and what leaves the
// table with nothing of this subtest still pending.
//
// Parameters:
//   - t *testing.T: the subtest.
//   - f *orderingFixture: the live fixture.
func orderingAssertClaimYieldsOccurrenceOrder(t *testing.T, f *orderingFixture) {
	ctx := context.Background()
	eventName := orderingEventName()

	base := time.Now().UTC().AddDate(0, 0, -orderingClaimBackdateDays).Truncate(time.Second)

	rows := make([]*model.EventOutbox, orderingClaimRows)
	for position := range rows {
		ledgerID := model.GenerateUUIDWithSuffix("ldg")
		transaction := orderingTransaction(f.runID, ledgerID, orderingClaimAggregateBase+position, position+1)

		row, err := f.blnk.PrepareEventOutbox(ctx, NewWebhook{
			Event:   eventName,
			Payload: transaction,
		}, WithEventLedgerID(ledgerID))
		require.NoError(t, err, "preparing the outbox row for position %d failed", position)
		require.NotNil(t, row,
			"PrepareEventOutbox returned no row, which means event publishing is not configured")

		// The ONLY field the test sets by hand. Everything else on the row is what the
		// production capture path produced, so the claim is exercised against a real row.
		row.OccurredAt = base.Add(time.Duration(position) * time.Second)
		rows[position] = row
	}

	// The insertion order contradicts the occurrence order, so the two cannot be confused: ids
	// ascend as rows are inserted, and this inserts the fourth-oldest row first.
	insertionOrder := []int{3, 1, 5, 0, 4, 2}
	require.Len(t, insertionOrder, orderingClaimRows, "the insertion order must cover every seeded row")

	for _, position := range insertionOrder {
		require.NoError(t, f.ds.InsertEventOutbox(ctx, rows[position]),
			"inserting the outbox row for position %d failed", position)
	}

	require.Greater(t, rows[0].ID, rows[3].ID,
		"the seeding failed to make insertion order disagree with occurrence order, so this "+
			"subtest could not tell one from the other")

	expected := make([]string, 0, orderingClaimRows)
	seeded := make(map[string]struct{}, orderingClaimRows)
	for _, row := range rows {
		expected = append(expected, row.EventID)
		seeded[row.EventID] = struct{}{}
	}

	claimed := make([]string, 0, orderingClaimRows)

	for round := 0; round*orderingClaimBatchSize < orderingClaimRows; round++ {
		// THE PREMISE HAS TO HOLD BEFORE THE CLAIM MEANS ANYTHING.
		//
		// This subtest asserts that a batch of three over six backdated rows hands back a
		// PREFIX of the occurrence order, and that claim is only about this claim: it presumes
		// nothing else takes these rows. On a database shared with the rest of the package
		// that presumption can fail — the subtest above runs a real relay against the same
		// table, and a relay whose final tick overlaps this seeding leases rows out from under
		// it. The observable symptom is a batch that skips positions rather than one that
		// reorders them, which is exactly what a foreign lease produces and exactly what
		// nothing in the claim query is doing wrong.
		//
		// So the precondition is checked and a violation SKIPS. Failing there would report a
		// defect that is not one, while a skip states plainly that the premise could not be
		// established — and a skip cannot pass vacuously. The FIFO property itself is
		// separately and hermetically asserted in database/event_outbox_test.go against a
		// quiesced table, so nothing is lost when this guard trips.
		// Only the rows this subtest has NOT yet claimed. Earlier rounds deliberately leave
		// their rows dispatched, so probing the whole seeded set would report this subtest's
		// own progress as a foreign hold and skip every run after the first round.
		start := round * orderingClaimBatchSize
		if foreign := f.foreignlyClaimedSeededRows(ctx, t, expected[start:]); foreign != "" {
			t.Skipf(
				"a concurrent claimer holds seeded rows before claim %d, so this claim would be "+
					"asserted against a table this subtest did not shape: %s",
				round+1, foreign)
		}

		batch, err := f.ds.ClaimPendingEventOutbox(ctx, orderingClaimBatchSize, orderingClaimLease)
		require.NoError(t, err, "claiming batch %d failed", round+1)

		mine := make([]model.EventOutbox, 0, orderingClaimBatchSize)
		for _, row := range batch {
			if _, ok := seeded[row.EventID]; ok {
				mine = append(mine, row)
			}
		}

		want := expected[start : start+orderingClaimBatchSize]

		got := make([]string, 0, len(mine))
		for _, row := range mine {
			got = append(got, row.EventID)
		}

		require.Equal(t, want, got,
			"claim %d did not return the oldest %d seeded rows, oldest first.\n"+
				"Candidates are ordered by occurred_at ascending and the returned batch is re-sorted "+
				"the same way, so a batch of %d over %d rows backdated %d days must hand back a prefix "+
				"of the occurrence order.\nThe seeded rows now read:%s\n"+
				"(if another relay instance is draining blnk.event_outbox against this database it may "+
				"have claimed these rows first; run against an isolated stack to rule that out)",
			round+1, orderingClaimBatchSize, orderingClaimBatchSize, orderingClaimRows,
			orderingClaimBackdateDays, f.describeEventRows(ctx, expected, orderingClaimRows))

		for _, row := range mine {
			assert.Equal(t, model.EventOutboxStatusProcessing, row.Status,
				"a claimed row must be processing while its lease is held")
			require.NotEmpty(t, row.ClaimToken,
				"a claimed row must carry the claim token every subsequent transition is conditional on")

			require.NoError(t, f.ds.MarkEventDispatched(ctx, row.ID, row.ClaimToken, model.BrokerRecord{}),
				"marking event %s dispatched failed", row.EventID)
			claimed = append(claimed, row.EventID)
		}
	}

	assert.Equal(t, expected, claimed,
		"every seeded row must be claimed exactly once, in occurrence order")
	f.requireAllDispatched(ctx, t, expected)

	t.Logf("claimed %d rows backdated %d days in occurrence order, against an insertion order of %v",
		len(claimed), orderingClaimBackdateDays, insertionOrder)
}

// describeEventRows renders the current database state of a set of events for a failure
// message.
//
// Every stall this file can report has more than one possible cause — a broken ORDER BY versus
// another process having claimed the rows, a relay that is not claiming versus one whose rows
// are leased elsewhere — and the assertion alone cannot separate them. The id, occurrence time,
// status, attempt count, lease and claim token separate them at a glance, which is the
// difference between a diagnosable failure and a re-run.
//
// foreignlyClaimedSeededRows reports whether any of the given rows has already left the
// pending state, which means something other than the caller has claimed it.
//
// It exists to separate a claim-ordering DEFECT from a shared-table ACCIDENT. Both surface as
// a batch that is not the expected prefix, and only one of them is worth failing a build for.
// A row this subtest seeded and has not yet claimed must be pending with no lease; anything
// else — processing under another claim token, already dispatched, or leased into the future —
// says a concurrent claimer got there first.
//
// It returns a DESCRIPTION rather than a boolean so the skip message names what it found. An
// empty string means the premise holds.
//
// Parameters:
//   - ctx context.Context: the context for the query.
//   - t *testing.T: fails the test if the probe itself cannot run — an unreadable table is a
//     real failure, not a reason to skip.
//   - eventIDs []string: the seeded events that should still be pending and unleased.
//
// Returns:
//   - string: a human-readable account of the first few foreign holds, or "" when there are
//     none.
func (f *orderingFixture) foreignlyClaimedSeededRows(ctx context.Context, t *testing.T, eventIDs []string) string {
	t.Helper()

	if len(eventIDs) == 0 {
		return ""
	}

	var report strings.Builder

	for _, eventID := range eventIDs {
		row, err := f.ds.GetEventByID(ctx, eventID)
		require.NoError(t, err, "probing seeded event %s for a foreign claim must succeed", eventID)
		require.NotNil(t, row, "seeded event %s must still be in the outbox", eventID)

		if row.Status == model.EventOutboxStatusPending && row.ClaimToken == "" {
			continue
		}

		if report.Len() > 0 {
			report.WriteString("; ")
		}

		report.WriteString(fmt.Sprintf("%s is %s", eventID, row.Status))
		if row.ClaimToken != "" {
			report.WriteString(fmt.Sprintf(" under claim token %q", row.ClaimToken))
		}
	}

	return report.String()
}

// The output is CAPPED. Dumping all 288 rows of a full run produces a wall of text nobody
// reads, so a handful of rows plus the total is what actually gets looked at.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - eventIDs []string: the events to describe, in occurrence order.
//   - limit int: the maximum number of rows to render.
//
// Returns:
//   - string: one indented line per rendered event, ready to embed in a message.
func (f *orderingFixture) describeEventRows(ctx context.Context, eventIDs []string, limit int) string {
	shown := eventIDs
	if limit > 0 && len(shown) > limit {
		shown = shown[:limit]
	}

	lines := make([]string, 0, len(shown)+1)

	for _, eventID := range shown {
		row, err := f.ds.GetEventByID(ctx, eventID)
		if err != nil {
			lines = append(lines, fmt.Sprintf("  %s: unreadable (%v)", eventID, err))

			continue
		}

		lease := "none"
		if row.LockedUntil != nil {
			lease = row.LockedUntil.UTC().Format(time.RFC3339)
		}

		lines = append(lines, fmt.Sprintf(
			"  %s id=%d occurred_at=%s status=%s attempts=%d locked_until=%s claim_token=%q",
			row.EventID, row.ID, row.OccurredAt.UTC().Format(time.RFC3339),
			row.Status, row.Attempts, lease, row.ClaimToken,
		))
	}

	if len(shown) < len(eventIDs) {
		lines = append(lines, fmt.Sprintf("  … and %d more", len(eventIDs)-len(shown)))
	}

	return "\n" + strings.Join(lines, "\n")
}

// ---------------------------------------------------------------------------
// Production wiring (Q4-28)
// ---------------------------------------------------------------------------

// orderingProducerWiring is one production call site and the ledger it must supply.
//
// The delivery test above asserts that events sharing a ledger arrive in publish order,
// and it earns that assertion by calling PublishEvent with WithEventLedgerID — because a
// supplied ledger becomes the outbox row's partition key and therefore the Kafka message
// key, and a stable key is what pins an aggregate to one partition.
//
// Which means the test's assurance rests entirely on PRODUCTION doing the same thing. If a
// producer stopped supplying the ledger, its events would fall back to the next-best
// partition key, spread across partitions, and lose their ordering guarantee — while this
// file's delivery test carried on passing, because it supplies the ledger itself.
type orderingProducerWiring struct {
	// file is the production source read verbatim.
	file string

	// what names the emitting path, so a failure says which producer regressed rather
	// than which line number moved.
	what string

	// supplies is the exact WithEventLedgerID call expected in that file.
	supplies string
}

// orderingProducerWirings is every production path that emits an event for an aggregate
// whose ledger is knowable at the call site.
//
// The list is deliberately explicit rather than derived from a grep for the option: a
// derived list would shrink silently as producers were removed, which is the regression it
// exists to catch.
//
// Two production emitters are ABSENT and their absence is correct, not an omission:
//
//   - identity.go emits identity.created, and an identity belongs to no ledger. Its
//     partition key falls back to the aggregate, which is the identity itself — the
//     strongest key available and stable for that aggregate.
//   - transaction_bulk.go emits bulk_transaction.<status> for a BATCH, which spans
//     transactions in different ledgers. There is no single ledger to name, and inventing
//     one would key a batch event to a ledger it only partly concerns.
//
// cmd/workers.go's rejection handler is likewise absent: it publishes through the same
// transaction path this list already covers, from a transaction whose ledger it does not
// resolve independently.
var orderingProducerWirings = []orderingProducerWiring{
	{
		file: "transaction_execution.go",
		what: "transaction execution (the status-derived event)",
		// Both capture paths in that file — the atomic builder and the post-commit fallback —
		// resolve the ledger through this one call, which is why the snippet names the resolver
		// rather than a local variable: a producer that stopped supplying the ledger would have
		// to remove this call, and one that started resolving it differently would too.
		supplies: "WithEventLedgerID(transactionLedgerID(sourceBalance, destinationBalance))",
	},
	{
		file:     "ledger.go",
		what:     "ledger creation (ledger.created)",
		supplies: "WithEventLedgerID(created.LedgerID)",
	},
	{
		file:     "balance.go",
		what:     "balance creation (balance.created)",
		supplies: "WithEventLedgerID(created.LedgerID)",
	},
	{
		file:     "balance.go",
		what:     "balance monitoring (balance.monitor)",
		supplies: "WithEventLedgerID(updatedBalance.LedgerID)",
	},
}

// TestEventOrdering_ProductionSuppliesTheLedgerAtEveryKnowableCallSite is a STATIC
// assertion about the wiring the delivery test above depends on.
//
// It reads no broker and no database, so it runs in every environment including short
// mode and CI without infrastructure — which matters, because the test it protects skips
// without a broker and would then protect nothing.
//
// WHY A SOURCE-TEXT ASSERTION RATHER THAN A BEHAVIOURAL ONE. The property is "the
// production call site passes this option", and the only way to observe that behaviourally
// is to drive each producer end to end with a real ledger, a real balance and a real
// database, then read the partition key back off the row — five integration tests to prove
// one line each, every one of which skips without infrastructure. Reading the source
// proves the same thing unconditionally. It is coarse: renaming the local variable holding
// the ledger id breaks this test without breaking the code. That is the intended
// trade — the failure message says exactly what to update, and a test that goes red when
// the wiring it describes changes is doing its job.
func TestEventOrdering_ProductionSuppliesTheLedgerAtEveryKnowableCallSite(t *testing.T) {
	for _, wiring := range orderingProducerWirings {
		source, err := os.ReadFile(wiring.file)
		require.NoErrorf(t, err, "reading the production source %s", wiring.file)

		assert.Containsf(t, string(source), wiring.supplies,
			"%s (%s) MUST supply the ledger with %q.\n\n"+
				"Without it the event's partition key falls back to the aggregate, its events "+
				"spread across partitions, and the per-aggregate ordering guarantee of "+
				"requirement R-6 is lost for that producer — while "+
				"TestEventOrdering_EventsSharingAnAggregateAreConsumedInPublishOrder keeps "+
				"passing, because it supplies the ledger itself and would be proving the test "+
				"harness rather than the system.\n\n"+
				"If the call site legitimately changed shape, update orderingProducerWirings in "+
				"this file to match — and if a producer legitimately has no ledger, move it to "+
				"the documented exceptions above rather than deleting the row.",
			wiring.what, wiring.file, wiring.supplies)
	}
}

// TestEventOrdering_ProductionStartsTheRelayThatDrainsTheOutbox asserts the OTHER half of
// the wiring this file's delivery test stands in for.
//
// PublishEvent only writes a pending outbox row; nothing reaches a broker until a relay
// claims that row. The delivery test starts a relay itself, so it would pass unchanged
// against a deployment whose server role started none — and on such a deployment every
// ledger mutation would be captured correctly and no subscriber would ever receive
// anything, which is the most expensive way for a passing test suite to be wrong.
//
// cmd/server_test.go asserts the server role's own wiring in detail — the call, the
// deferred stop, and topic assurance ordered before it. This asserts only that the relay
// constructor is reached from the server command at all, from the perspective of the file
// that depends on it, so that deleting that wiring fails HERE too and the failure names
// the ordering guarantee it costs.
func TestEventOrdering_ProductionStartsTheRelayThatDrainsTheOutbox(t *testing.T) {
	source, err := os.ReadFile("cmd/server.go")
	require.NoError(t, err, "reading cmd/server.go")

	body := string(source)

	assert.Contains(t, body, "blnk.NewEventRelayProcessor(instance)",
		"the server role MUST construct the event relay. Without it blnk.event_outbox is "+
			"written and never drained: every event is captured, none is delivered, and the "+
			"delivery test in this file still passes because it starts a relay of its own.")

	assert.Contains(t, body, "relay.Start(ctx)",
		"constructing the relay is not enough — it must be started. See cmd/server_test.go "+
			"for the full assertion, including that topic assurance precedes it.")
}

// ---------------------------------------------------------------------------
// The published partition-key contract must describe the keying the code performs
//
// The static assertion above proves PRODUCTION supplies the ledger, so transaction, balance,
// monitor and ledger events are all keyed on a LEDGER ID. docs/event-streaming.md is what a
// subscriber reads to decide how to shard its consumer group and what ordering it may rely
// on, and it said the opposite: a section headed "The key is not simply the ledger id" told
// readers that for "the highest-volume event type in the system — transactions — that is a
// balance id, not a ledger id", and a table repeated it row by row.
//
// That inversion is not a cosmetic one. A consumer built on it sizes its group by balance
// cardinality, assumes two transactions against one balance are mutually ordered across
// ledgers (they are not), and does not expect a ledger's whole event stream to arrive through
// a single partition (it does). The description of the fallback had been promoted to the rule,
// and only prose can carry that mistake — which is why the guard below reads the prose.
// ---------------------------------------------------------------------------

// invertedPartitionKeyClaims are the phrasings that present the payload-derived fallback as
// the partitioning rule. Each was published.
var invertedPartitionKeyClaims = []string{
	"The key is not simply",
	"that is a **balance** id, not a ledger id",
	"A transaction keys on its source balance",
}

// TestEventOrdering_PublishedKeyContractStatesLedgerKeyingAsTheRule guards the subscriber-
// facing description of the key against the wiring the test above pins.
//
// It reads no broker and no database, so unlike the delivery test it runs everywhere —
// which matters, because a document that contradicts the system is wrong in every
// environment, not only in one that has Kafka.
func TestEventOrdering_PublishedKeyContractStatesLedgerKeyingAsTheRule(t *testing.T) {
	published, err := os.ReadFile("docs/event-streaming.md")
	require.NoError(t, err, "reading the published event documentation")
	text := string(published)

	require.Contains(t, text, "### The key is the ledger id wherever a ledger exists",
		"the published partitioning section must state ledger keying as the RULE. "+
			"docs/kafka-operations.md links to it by anchor, so renaming the heading breaks "+
			"that link as well as the contract.")

	for _, claim := range invertedPartitionKeyClaims {
		// Falsef rather than NotContains so a failure prints the sentence to fix rather
		// than the whole document.
		assert.Falsef(t, strings.Contains(text, claim),
			"docs/event-streaming.md presents the payload-derived FALLBACK as the "+
				"partitioning rule with %q.\n\n"+
				"Every ledger-scoped producer supplies the ledger through WithEventLedgerID "+
				"(see orderingProducerWirings in this file), and a supplied ledger becomes the "+
				"partition key — so transaction, balance, monitor and ledger events are keyed "+
				"on a LEDGER ID. Only identity.created, bulk_transaction.*, system.error and a "+
				"rejected transaction reach the fallback. A consumer built on the inverted "+
				"claim shards on the wrong cardinality and assumes ordering it does not have.",
			claim)
	}

	// The classes that genuinely have no ledger must still be named, or a reader is told
	// the rule is universal and is surprised by the four cases where it is not.
	for _, unkeyed := range []string{"identity.created", "bulk_transaction.*", "system.error", "transaction.rejected"} {
		assert.Containsf(t, text, unkeyed,
			"the published section must name %s as reaching the fallback, since the ledger "+
				"rule does not apply to it", unkeyed)
	}
}

// TestEventOrdering_OperationsGuideAgreesWithThePublishedKeyContract closes the last place the
// inverted claim was repeated.
//
// docs/kafka-operations.md justified recording the subscriber partition-key prefix rather than
// deriving it, and its justification rested on the same false premise — that a key is "a
// balance id rather than a ledger id". The conclusion happens to be right for a different
// reason (a key comes from Blnk's aggregate namespace, never from a subscriber identifier), so
// the error survived being read as correct. Two documents disagreeing about the key is how an
// operator ends up setting a prefix that matches nothing.
func TestEventOrdering_OperationsGuideAgreesWithThePublishedKeyContract(t *testing.T) {
	operations, err := os.ReadFile("docs/kafka-operations.md")
	require.NoError(t, err, "reading the operations runbook")
	text := string(operations)

	assert.Falsef(t, strings.Contains(text, "is a *balance* id rather than a ledger id"),
		"docs/kafka-operations.md repeats the inverted key claim. The partition key is the "+
			"LEDGER ID for every ledger-scoped event; the reason a prefix cannot be derived "+
			"from the subscriber is that keys come from Blnk's aggregate namespace, not that "+
			"they are balance ids.")

	assert.Contains(t, text, "event-streaming.md#the-key-is-the-ledger-id-wherever-a-ledger-exists",
		"the runbook must link to the corrected section, so the two documents cannot describe "+
			"the key differently")
}

// TestEventOrdering_DivergentStoredKeysStaySerialisedAcrossRelayReplicas is the proof for
// PERF-C02: that the value the DATABASE serialises dispatch on is the value KAFKA partitions
// on, on the rows where the schema lets the two disagree.
//
// # The condition this test creates, and why it is a real one
//
// A row carries two key columns. ledger_id is the authoritative ledger; partition_key is "where
// does this message go" and is NOT NULL. The publish path resolves the message key as
// model.EffectivePartitionKey — the ledger where a row has one, the stored partition key where
// it does not — because requirement R-6 partitions by ledger id.
//
// On a row captured today the two AGREE, because PrepareEventOutbox derives the partition key
// from the ledger whenever a ledger is known. The schema permits them to DIVERGE, and two
// populations actually do: a row written before the ledger was threaded through its call site,
// whose partition_key holds a source balance while ledger_id holds the ledger; and a row whose
// partition_key was derived from the payload before the ledger was resolved. This test seeds
// exactly that: one ledger, and stored partition keys that differ per row.
//
// # Why the delivery subtest above cannot see the defect
//
// Two independent mechanisms serialise same-key rows, and they fail differently:
//
//   - groupEventRowsByPartitionKey groups a CLAIMED BATCH by the effective key and publishes
//     each group with one goroutine. That holds WITHIN one process whatever the SQL does, so a
//     single relay publishes these rows in order even with a claim that admits all of them.
//   - The claim's earlier-same-key exclusion is what holds ACROSS processes. It is the only
//     mechanism that does.
//
// So a claim serialising on the wrong key reorders nothing until a second replica exists — which
// is why this test runs two relays, and why the existing single-relay subtest passed throughout
// the period the defect was live.
//
// # What is asserted
//
// The claim-level probe is the deterministic detector: with the rows seeded, ONE claim wide
// enough for all of them must hand back at most one, because at most one row per effective key
// may be in flight. Against the superseded predicate it handed back every row, since their
// stored keys all differ.
//
// The delivery assertion is the consequence: two relays, run concurrently over the same rows,
// must produce messages that all land on the ledger's ONE partition at strictly increasing
// offsets in occurrence order. That is acceptance criterion V-6 under the condition R-6's
// keying creates.
func TestEventOrdering_DivergentStoredKeysStaySerialisedAcrossRelayReplicas(t *testing.T) {
	fixture := newOrderingFixture(t)
	ctx := context.Background()

	// ONE ledger, so every row hashes to one partition and any reordering is observable as a
	// permutation of one sequence rather than being spread across independent partitions.
	ledgerID := model.GenerateUUIDWithSuffix("ldg")
	expectedPartition := fixture.expectedPartitionFor(ledgerID)

	startOffsets := fixture.endOffsets(t)

	rows, eventIDs := fixture.seedDivergentKeyRows(ctx, t, ledgerID)

	t.Run("one claim admits at most one row of the ledger", func(t *testing.T) {
		orderingAssertClaimSerialisesDivergentKeys(ctx, t, fixture, ledgerID, eventIDs)
	})

	t.Run("two concurrent relays publish them in occurrence order", func(t *testing.T) {
		orderingAssertConcurrentRelaysPreserveOrder(
			ctx, t, fixture, ledgerID, expectedPartition, rows, eventIDs, startOffsets)
	})
}

// seedDivergentKeyRows captures orderingDivergentRows events for ONE ledger and stores them with
// DIFFERENT partition keys, which is the condition PERF-C02 is about.
//
// The rows are produced by the production capture path — PrepareEventOutbox with
// WithEventLedgerID — so everything about them except the one column under test is what
// production writes. Only partition_key is then overwritten, and only to a value production
// itself used to write there: the transaction's source balance id.
//
// They are BACKDATED, for the same reason the claim-order subtest backdates its rows: candidates
// are claimed oldest first, so backdating puts this test's rows at the head of the claimable set
// on a table that may hold unrelated pending work.
//
// Parameters:
//   - ctx context.Context: the context for the inserts.
//   - t *testing.T: the test.
//   - ledgerID string: the one ledger every seeded row belongs to.
//
// Returns:
//   - []*model.EventOutbox: the seeded rows, in occurrence order.
//   - []string: their event ids, in the same order.
func (f *orderingFixture) seedDivergentKeyRows(
	ctx context.Context,
	t *testing.T,
	ledgerID string,
) ([]*model.EventOutbox, []string) {
	t.Helper()

	eventName := orderingEventName()
	base := time.Now().UTC().AddDate(0, 0, -orderingClaimBackdateDays).Truncate(time.Second)

	rows := make([]*model.EventOutbox, 0, orderingDivergentRows)
	eventIDs := make([]string, 0, orderingDivergentRows)

	for sequence := 1; sequence <= orderingDivergentRows; sequence++ {
		transaction := orderingTransaction(f.runID, ledgerID, orderingDivergentAggregateBase, sequence)

		row, err := f.blnk.PrepareEventOutbox(ctx, NewWebhook{
			Event:   eventName,
			Payload: transaction,
		}, WithEventLedgerID(ledgerID))
		require.NoError(t, err, "preparing the outbox row for sequence %d failed", sequence)
		require.NotNil(t, row,
			"PrepareEventOutbox returned no row, which means event publishing is not configured")

		require.Equal(t, ledgerID, row.LedgerID,
			"the capture path must record the supplied ledger, or this test is not seeding the "+
				"condition it claims to")
		require.Equal(t, ledgerID, row.PartitionKey,
			"the capture path must key by the ledger today; this test overwrites that below to "+
				"recreate the divergence the schema still permits, and a capture path that had "+
				"stopped agreeing would make the overwrite meaningless")

		// THE DIVERGENCE, and the only field this test changes. A per-row balance id is exactly
		// what the payload-derived key used to be, so the stored keys differ from each other and
		// from the ledger while ledger_id — and therefore the Kafka message key — stays the same
		// for every row.
		row.PartitionKey = fmt.Sprintf("bln_%s_d%03d_source", f.runID, sequence)
		row.OccurredAt = base.Add(time.Duration(sequence) * time.Second)

		require.NotEqual(t, row.PartitionKey, row.EffectiveKey(),
			"the seeded row must publish under a key different from its stored one, or the "+
				"divergence this test exists for is not present")
		require.Equal(t, ledgerID, row.EffectiveKey(),
			"every seeded row must publish under the one ledger, or they do not share a Kafka "+
				"partition and nothing about ordering follows")

		require.NoError(t, f.ds.InsertEventOutbox(ctx, row),
			"inserting the divergent-key row for sequence %d failed", sequence)

		rows = append(rows, row)
		eventIDs = append(eventIDs, row.EventID)
	}

	t.Logf("seeded %d rows for ledger %s with %d distinct stored partition keys",
		len(rows), ledgerID, len(rows))

	return rows, eventIDs
}

// orderingAssertClaimSerialisesDivergentKeys is the deterministic detector for PERF-C02.
//
// One claim, wide enough to take every seeded row, must hand back AT MOST ONE of them: the
// claim admits a candidate only when no earlier row sharing its EFFECTIVE key is still pending
// or processing, and all of these rows share one. A claim serialising on the stored column
// returns all six, because all six stored keys differ.
//
// It asserts "at most one" rather than "exactly one" because the row it would return may
// legitimately be held by something else on a shared database — the delivery subtests in this
// package run real relays against the same table. Zero is therefore not a failure; two is,
// under every circumstance, because no concurrent claimer can cause a claim to return two rows
// of one key.
//
// The lease it takes is short and the rows are left pending, because the delivery assertion
// that follows needs them.
//
// Parameters:
//   - ctx context.Context: the context for the claim.
//   - t *testing.T: the subtest.
//   - f *orderingFixture: the live fixture.
//   - ledgerID string: the ledger every seeded row belongs to, for the failure message.
//   - eventIDs []string: the seeded event ids.
func orderingAssertClaimSerialisesDivergentKeys(
	ctx context.Context,
	t *testing.T,
	f *orderingFixture,
	ledgerID string,
	eventIDs []string,
) {
	t.Helper()

	seeded := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		seeded[eventID] = struct{}{}
	}

	batch, err := f.ds.ClaimPendingEventOutbox(ctx, len(eventIDs)*2, orderingDivergentProbeLease)
	require.NoError(t, err, "the probe claim failed")

	mine := make([]string, 0, len(eventIDs))
	keys := make([]string, 0, len(eventIDs))
	for _, row := range batch {
		if _, ok := seeded[row.EventID]; !ok {
			continue
		}

		mine = append(mine, row.EventID)
		keys = append(keys, fmt.Sprintf("%s(stored %s)", row.EffectiveKey(), row.PartitionKey))
	}

	require.LessOrEqual(t, len(mine), 1,
		"one claim returned %d rows of ledger %s at once: %v.\n"+
			"Every one of these rows publishes under the ledger as its Kafka message key, so they "+
			"share ONE partition — and the claim must therefore admit at most one of them at a "+
			"time, across every relay instance. Returning several means the earlier-same-key "+
			"exclusion is comparing a different key from the one the publisher uses: the rows' "+
			"STORED partition keys all differ (%v) while their EFFECTIVE keys are all %s. Two "+
			"relay replicas can then hold two rows of one partition and append them in either "+
			"order.",
		len(mine), ledgerID, mine, keys, ledgerID)

	// Released rather than left leased, so the relays that follow do not wait out the probe's
	// lease. Any row it did claim is returned to the claimable set with its retry budget
	// untouched — the claim spends no attempt.
	f.releaseProbeLease(ctx, t, mine)

	t.Logf("the probe claim admitted %d of %d seeded rows, which is the serialisation the "+
		"ordering guarantee rests on", len(mine), len(eventIDs))
}

// releaseProbeLease returns rows the probe claim leased to the claimable set immediately.
//
// It clears status, the lease and the claim token in one statement rather than through the
// repository, because no production caller needs to un-claim a row and adding the operation to
// the interface to serve a test would put an unused transition in production code. The lease
// would expire on its own in orderingDivergentProbeLease; releasing it explicitly is what keeps
// the assertion that follows independent of that timing.
//
// Parameters:
//   - ctx context.Context: the context for the update.
//   - t *testing.T: the subtest.
//   - eventIDs []string: the rows the probe claimed. Empty is a no-op.
func (f *orderingFixture) releaseProbeLease(ctx context.Context, t *testing.T, eventIDs []string) {
	t.Helper()

	if len(eventIDs) == 0 {
		return
	}

	for _, eventID := range eventIDs {
		if _, err := f.pool.ExecContext(ctx,
			`UPDATE blnk.event_outbox
			    SET status = 'pending', locked_until = NULL, claim_token = NULL
			  WHERE event_id = $1 AND status = 'processing'`,
			eventID,
		); err != nil {
			// Reported rather than fatal: the lease expires on its own, so the run can still
			// complete — it just waits.
			t.Logf("could not release the probe lease on %s (%v); the relays will wait for it to expire",
				eventID, err)
		}
	}
}

// orderingAssertConcurrentRelaysPreserveOrder runs orderingDivergentRelayCount relays over the
// seeded rows at once and asserts the topic received them in occurrence order.
//
// Two relays over one *Blnk is what two replicas look like FROM THE DATABASE: two independent
// claim loops, each taking its own lease and its own claim token, with nothing but the claim's
// predicate keeping them from holding two rows of one Kafka partition simultaneously. They share
// the process's Kafka writers, exactly as two pods share nothing — which is immaterial here,
// because the writers are safe for concurrent use and the ordering question is decided before a
// write is issued.
//
// Parameters:
//   - ctx context.Context: the parent context.
//   - t *testing.T: the subtest.
//   - f *orderingFixture: the live fixture.
//   - ledgerID string: the ledger every seeded row belongs to.
//   - expectedPartition int: the partition the stable hash resolves that ledger to.
//   - rows []*model.EventOutbox: the seeded rows, in occurrence order.
//   - eventIDs []string: their event ids, in the same order.
//   - startOffsets map[int]int64: the topic's end offsets before anything was published.
func orderingAssertConcurrentRelaysPreserveOrder(
	ctx context.Context,
	t *testing.T,
	f *orderingFixture,
	ledgerID string,
	expectedPartition int,
	rows []*model.EventOutbox,
	eventIDs []string,
	startOffsets map[int]int64,
) {
	t.Helper()

	relays := make([]*EventRelayProcessor, 0, orderingDivergentRelayCount)
	relayCtx, cancelRelays := context.WithCancel(ctx)

	t.Cleanup(func() {
		for _, relay := range relays {
			relay.Stop()
		}
		cancelRelays()
	})

	for instance := 0; instance < orderingDivergentRelayCount; instance++ {
		relay := NewEventRelayProcessor(f.blnk).
			WithPollInterval(orderingRelayPollInterval).
			WithBatchSize(orderingRelayBatchSize).
			WithLockDuration(orderingRelayLockDuration)

		require.NoErrorf(t, relay.Start(relayCtx),
			"relay instance %d refused to start; the returned obstacle names the missing precondition",
			instance)
		require.Truef(t, relay.IsRunning(),
			"relay instance %d reported no obstacle yet is not running",
			instance)

		relays = append(relays, relay)
	}

	f.requireAllDispatched(ctx, t, eventIDs)
	observed, _ := f.consumeRun(t, startOffsets, len(eventIDs))

	for _, relay := range relays {
		relay.Stop()
		assert.False(t, relay.IsRunning(), "IsRunning must report false once Stop has returned")
	}
	orderingRequireRelayGoroutineGone(t)

	// Only this test's own events. The topic is shared with every other run against this
	// broker, and consumeRun reads whatever arrived after the start offsets.
	sequenceByEventID := make(map[string]int, len(rows))
	for index, row := range rows {
		sequenceByEventID[row.EventID] = index + 1
	}

	sequences := make([]int, 0, len(rows))
	offsets := make([]int64, 0, len(rows))

	for _, observation := range observed {
		sequence, mine := sequenceByEventID[observation.envelope.EventID]
		if !mine {
			continue
		}

		assert.Equal(t, ledgerID, observation.key,
			"event %s was keyed with %q instead of its ledger id: the stored partition key must "+
				"never reach the wire as the message key on a row that carries a ledger",
			observation.envelope.EventID, observation.key)
		require.Equal(t, expectedPartition, observation.partition,
			"event %s landed on partition %d instead of %d, so these rows did not share a "+
				"partition and no ordering conclusion follows",
			observation.envelope.EventID, observation.partition, expectedPartition)

		sequences = append(sequences, sequence)
		offsets = append(offsets, observation.offset)
	}

	require.Len(t, sequences, len(rows),
		"expected all %d seeded events on %s/%d, observed %d",
		len(rows), f.topic, expectedPartition, len(sequences))

	expectedSequence := make([]int, 0, len(rows))
	for sequence := 1; sequence <= len(rows); sequence++ {
		expectedSequence = append(expectedSequence, sequence)
	}

	// THE CRITERION. consumeRun reads each partition forward from the start offset, so the
	// order of `sequences` is the order the partition holds — the order a subscriber sees.
	assert.Equal(t, expectedSequence, sequences,
		"ledger %s was appended to partition %d out of occurrence order: got %v at offsets %v.\n"+
			"Every one of these rows carries a DIFFERENT stored partition key and the SAME ledger, "+
			"so they are the rows on which the claim's key and the publisher's key must agree. Out "+
			"of order here means two relay instances held two rows of one partition at once.",
		ledgerID, expectedPartition, sequences, offsets)
	assert.IsIncreasing(t, offsets,
		"ledger %s must occupy strictly increasing offsets within partition %d",
		ledgerID, expectedPartition)

	t.Logf("%d relays drained %d divergent-key rows of ledger %s to partition %d at offsets %d..%d, in occurrence order",
		orderingDivergentRelayCount, len(rows), ledgerID, expectedPartition, offsets[0], offsets[len(offsets)-1])
}
