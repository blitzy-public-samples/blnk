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

// Acceptance criterion V-2 — ZERO MESSAGE LOSS — measured end to end, live.
//
// # Why this file exists
//
// V-2 is the criterion with the most machinery behind it and, until now, the least joined-up
// evidence. Three pieces were each well covered on their own:
//
//   - The PostgreSQL side. database/event_outbox_test.go drives real rows into every terminal
//     state and checks the census and the interval audit against a real table.
//   - The broker side. event_admin_test.go measures end offsets and partition windows against
//     a fake Kafka client that answers scripted offsets.
//   - The interpretation. ReconcileAgainstOutbox is exercised over hand-built reports and
//     hand-built audits, which is how its every branch is reached.
//
// What no test did was put a REAL record on a REAL broker, record the coordinate the broker
// actually assigned into a REAL outbox row, measure that partition's REAL window, and let the
// production reconciliation read the two together. Every one of those seams is where the two
// halves can disagree while each half is individually correct — a coordinate stored with the
// wrong partition, an interval computed exclusive where the audit reads it inclusive, an
// off-by-one at the log end — and each of those defects produces exactly the reassuring green
// verdict this criterion exists to make impossible.
//
// So this file joins them. It publishes through the production publisher, stores through the
// production repository transitions, measures through the production admin client, and asserts
// the production verdict. Nothing here is scripted: every number comes from PostgreSQL or from
// Kafka.
//
// # Why it owns a database
//
// The audit's population is every row in blnk.event_outbox that claims a publication — the
// whole table, deliberately, because a reconciliation narrowed to a subset would be a
// reconciliation of nothing. A CONCLUSIVE verdict therefore requires that every such row be
// accounted for, which cannot be arranged on a table shared with other suites, other clones and
// previous runs: one stale coordinate from a topic that has since been recreated is enough to
// turn the verdict into a permanent LOSS DETECTED that says nothing about the code.
//
// The fixture therefore creates its own database, migrates it with the embedded migrations, and
// drops it afterwards. That is the only arrangement in which "conclusive" is a statement about
// the implementation rather than about the tidiness of a shared table — and it is also why this
// suite cannot collide with the tier lock the other live outbox suites share.
package blnk

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// zeroLossEventType is the event type every fixture row carries.
//
// A transaction event, so TopicForEvent routes it to blnk.transactions — an OWNED topic, which
// is required rather than convenient: validateEventOutboxEntry refuses a stored destination
// outside the namespaces this deployment owns, so a fixture could not name an invented topic
// even if isolation would have been easier that way.
const zeroLossEventType = "transaction.applied"

// zeroLossEvents is how many events are published and reconciled.
//
// Small on purpose. The property is a MAPPING — every row names the record it produced — and
// the mapping either holds for one row or for none; volume adds runtime against a real broker
// without adding evidence. Six is enough to spread across partitions under the keyed balancer
// and therefore to catch a partition recorded from the wrong field.
const zeroLossEvents = 6

// zeroLossBudget bounds the live statistics projection.
//
// Generous rather than tight: it covers six publishes, two full-inventory offset reads across
// eight topics and two audits against a real database, and a timeout that fires part-way through
// would report as a reconciliation failure rather than as the slow broker it is.
const zeroLossBudget = 90 * time.Second

// zeroLossStatisticsWindow is the window the statistics projection is asked for.
//
// One hour, which is far longer than the run and therefore includes every row this fixture
// writes. The window bounds only the DISPATCHED population — the one status that grows without
// bound — so a value shorter than the run would drop rows from the census while the audit still
// classified them, and the mismatch would look like loss.
const zeroLossStatisticsWindow = time.Hour

// zeroLossFixture owns one disposable database, a live admin client and a live publisher.
type zeroLossFixture struct {
	t     *testing.T
	ds    database.Datasource
	conf  *config.Configuration
	admin *KafkaAdminClient
	// publisher is the real Kafka publisher, as the relay uses it.
	publisher TopicEventPublisher
	// topic is where zeroLossEventType routes under the configured prefix.
	topic string
	// runID tags the rows, so a failure can be traced in the database before it is dropped.
	runID string
}

// newZeroLossFixture builds the live end-to-end fixture, or skips naming exactly what is
// missing.
//
// The skips are deliberate and each names the command that fixes it: this suite is part of the
// Kafka acceptance job, where it must RUN, and it is also part of `go test ./...` on a developer
// machine that may have neither a broker nor a provisioned producer. The acceptance job's
// fail-on-skip gate is what makes those skips safe — a skip there is a job failure.
func newZeroLossFixture(t *testing.T) *zeroLossFixture {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping the live zero-loss reconciliation: it needs a PostgreSQL it can create a " +
			"database in and a Kafka broker it can publish to. Run it with: " +
			"go test -run 'TestZeroLoss' -count=1 .")
	}

	adminUser, adminSecret, adminPresent := recoveryKafkaCredentials()
	if !adminPresent {
		t.Skip("skipping the live zero-loss reconciliation: KAFKA_SASL_ADMIN_USER and " +
			"KAFKA_SASL_ADMIN_SECRET must be set so the offset measurement can authenticate. " +
			"scripts/kafka-provision.sh prints the local stack's values; no credential is " +
			"defaulted here.")
	}

	producerUser, producerSecret, producerPresent := recoveryProducerCredentials()
	if !producerPresent {
		t.Skip("skipping the live zero-loss reconciliation: KAFKA_SASL_USER and KAFKA_SASL_SECRET " +
			"must be set. Blnk refuses to publish as the administrative principal, so the publish " +
			"half of this test needs a dedicated producer principal.")
	}

	brokers := recoveryBrokers()
	require.NotEmpty(t, brokers, "KAFKA_BROKERS resolved to nothing")
	for _, broker := range brokers {
		recoveryRequireReachable(t, "the Kafka broker", broker,
			"Start it with `docker compose --profile kafka up -d kafka kafka-init` — both services "+
				"sit behind the \"kafka\" profile — or point KAFKA_BROKERS at a reachable broker.")
	}

	base := recoveryPostgresDSN()
	if address := recoveryDatabaseAddress(base); address != "" {
		recoveryRequireReachable(t, "PostgreSQL", address,
			"Start it with `docker compose up -d postgres`, or point BLNK_DATA_SOURCE_DNS at a "+
				"reachable database.")
	}

	runID := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	dsn := createZeroLossDatabase(t, base, runID)

	conf := recoveryConfiguration(dsn, &config.KafkaConfig{
		Brokers:     brokers,
		TopicPrefix: DefaultTopicPrefix,
		// Two principals, because the two halves of this test are two ROLES: the publisher
		// authenticates as the producer and the offset measurement as the administrator, which
		// is exactly how a deployment is configured.
		SASLUser:        producerUser,
		SASLSecret:      producerSecret,
		SASLAdminUser:   adminUser,
		SASLAdminSecret: adminSecret,
		MinPartitions:   6,
		// One replica: the local stack is a single broker. Nothing here creates a topic, so the
		// value need only be valid.
		ReplicationFactor: 1,
		// The local broker speaks SASL/SCRAM over plaintext, which the transport refuses without
		// this acknowledgement.
		InsecureLocalDev: true,
	})
	recoveryStoreConfiguration(t, conf)

	db, err := database.ConnectDB(conf.DataSource)
	require.NoErrorf(t, err, "connecting to the disposable reconciliation database at %s", dsn)
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("closing the zero-loss connection pool: %v", closeErr)
		}
	})

	admin, err := NewKafkaAdmin(conf)
	require.NoError(t, err, "building the Kafka admin client")
	require.True(t, admin.IsConfigured(),
		"the admin client must be configured against the broker list, or the offsets this test "+
			"reconciles against would be measured from nothing")
	t.Cleanup(func() {
		if closeErr := admin.Close(); closeErr != nil {
			t.Logf("closing the Kafka admin client: %v", closeErr)
		}
	})

	transport, err := NewEventPublisher(conf)
	require.NoError(t, err, "building the Kafka event publisher")
	require.False(t, IsNoopEventPublisher(transport),
		"this test must resolve the REAL publisher; the no-op reports every publish dispatched "+
			"without sending anything, and would corroborate rows against records that do not exist")
	topical, isTopical := transport.(TopicEventPublisher)
	require.True(t, isTopical, "the publisher must report the coordinate it wrote to")
	t.Cleanup(func() {
		if closeErr := topical.Close(); closeErr != nil {
			t.Logf("closing the Kafka event publisher: %v", closeErr)
		}
	})

	fixture := &zeroLossFixture{
		t:         t,
		ds:        database.Datasource{Conn: db},
		conf:      conf,
		admin:     admin,
		publisher: topical,
		topic:     TopicForEvent(zeroLossEventType),
		runID:     runID,
	}

	t.Logf("zero-loss run %s: outbox at %s, topic %s", runID, dsn, fixture.topic)

	return fixture
}

// createZeroLossDatabase creates a disposable database beside the configured one and returns a
// DSN pointing at it, migrated and empty.
//
// # Why a whole database rather than a marker
//
// Every other live suite here scopes itself with a marker on the rows it wrote. That works for
// a question about specific rows and cannot work for this one: the reconciliation's verdict is a
// property of the WHOLE table, so a foreign row is not noise to be filtered but a fact the
// verdict must account for. The only way to assert "conclusive" is to own every row in it.
//
// Parameters:
//   - t *testing.T: owns the database's lifetime; it is dropped in cleanup.
//   - base string: the configured DSN, used for the server address and credentials.
//   - runID string: makes the name unique per run, so concurrent clones cannot collide.
//
// Returns:
//   - string: a DSN for the new database, with blnk's migrations applied.
func createZeroLossDatabase(t *testing.T, base, runID string) string {
	t.Helper()

	parsed, err := url.Parse(base)
	require.NoErrorf(t, err, "the configured DSN %q must be a URL so a sibling database can be named", base)

	// Lower-case and unquoted: PostgreSQL folds an unquoted identifier to lower case, and a
	// name that needed quoting here would need quoting in the DROP as well.
	name := "blnk_zeroloss_" + strings.ToLower(runID)

	control, err := sql.Open("postgres", base)
	require.NoError(t, err, "opening the control connection that creates the database")
	defer func() {
		if closeErr := control.Close(); closeErr != nil {
			t.Logf("closing the control connection: %v", closeErr)
		}
	}()
	require.NoError(t, control.Ping(), "the control connection must reach PostgreSQL")

	// A LEFTOVER FROM A CRASHED RUN IS REMOVED FIRST. The name carries a fresh run id so a
	// collision is all but impossible, and "all but" is not a reason to fail on the second
	// attempt after a machine was interrupted mid-test.
	_, err = control.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`)
	require.NoErrorf(t, err, "clearing any leftover database %s", name)

	// CREATE DATABASE cannot run inside a transaction, which is why this is a bare Exec and
	// not part of any surrounding unit of work.
	_, err = control.Exec(`CREATE DATABASE ` + name)
	require.NoErrorf(t, err,
		"creating the disposable database %s. This test needs a PostgreSQL role that may create "+
			"databases: the reconciliation verdict is a property of the whole outbox table, so it "+
			"cannot be asserted on a shared one", name)

	// Registered immediately after creation, so a failure anywhere below still drops it. FORCE
	// terminates any connection this test left behind rather than failing on it.
	t.Cleanup(func() {
		dropper, openErr := sql.Open("postgres", base)
		if openErr != nil {
			t.Errorf("could not open a connection to drop %s: %v", name, openErr)

			return
		}
		defer func() { _ = dropper.Close() }()

		if _, dropErr := dropper.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`); dropErr != nil {
			t.Errorf("could not drop the disposable database %s: %v", name, dropErr)
		}
	})

	parsed.Path = "/" + name
	dsn := parsed.String()

	// THE PRODUCTION MIGRATIONS, from the embedded filesystem the binary ships. Applying a
	// hand-written schema here would make this test's table a different table from the one
	// production reconciles, which is the one thing it cannot afford.
	target, err := sql.Open("postgres", dsn)
	require.NoErrorf(t, err, "opening %s to migrate it", name)
	defer func() {
		if closeErr := target.Close(); closeErr != nil {
			t.Logf("closing the migration connection: %v", closeErr)
		}
	}()

	migrate.SetSchema("blnk")
	applied, err := migrate.Exec(target, "postgres",
		migrate.EmbedFileSystemMigrationSource{FileSystem: SQLFiles, Root: "sql"}, migrate.Up)
	require.NoErrorf(t, err, "applying the embedded migrations to %s", name)
	require.Positivef(t, applied, "no migration was applied to %s, so blnk.event_outbox does not exist", name)

	return dsn
}

// publishAndRecord publishes one event through the production publisher and stores an outbox
// row carrying the coordinate the BROKER assigned, exactly as the relay does.
//
// The two halves are what make this the joined path: the coordinate is not chosen by the test.
// It is read out of the publish result, which read it out of the broker's acknowledgement, and
// it is written through MarkEventDispatched, which is the only production route by which a row
// comes to claim a publication.
//
// Parameters:
//   - ctx context.Context: cancels the publish and the transitions.
//   - index int: distinguishes the fixture's aggregate and partition key.
//
// Returns:
//   - model.BrokerRecord: the coordinate the broker assigned and the row now names.
func (f *zeroLossFixture) publishAndRecord(ctx context.Context, index int) model.BrokerRecord {
	f.t.Helper()

	aggregate := fmt.Sprintf("zl-%s-agg-%d", f.runID, index)
	event := model.LedgerEvent{
		EventID:       uuid.NewString(),
		EventType:     zeroLossEventType,
		AggregateID:   aggregate,
		OccurredAt:    time.Now().UTC(),
		SchemaVersion: model.SchemaVersionV1,
		Payload: json.RawMessage(`{"event":"` + zeroLossEventType +
			`","data":{"transaction_id":"` + aggregate + `","status":"APPLIED"}}`),
	}

	result, err := f.publisher.PublishToTopic(ctx, PublishRequest{
		Event: event,
		Topic: f.topic,
		Key:   aggregate,
	})
	require.NoErrorf(f.t, err, "publishing fixture %d to %s", index, f.topic)
	require.Truef(f.t, result.Record.Confirmed(),
		"the broker must acknowledge fixture %d with a coordinate; an unconfirmed record would "+
			"leave the row claiming a publication it cannot name, which is the UNCONFIRMED bucket "+
			"rather than the corroborated one", index)
	require.Equalf(f.t, f.topic, result.Record.Topic,
		"the acknowledged coordinate must name the topic that was published to")

	entry := &model.EventOutbox{
		EventID:       event.EventID,
		EventType:     event.EventType,
		AggregateID:   aggregate,
		PartitionKey:  aggregate,
		LedgerID:      "zl-" + f.runID + "-ldg",
		Topic:         f.topic,
		SchemaVersion: model.SchemaVersionV1,
		Payload:       event.Payload,
		OccurredAt:    event.OccurredAt,
		MaxAttempts:   5,
	}
	require.NoErrorf(f.t, f.ds.InsertEventOutbox(ctx, entry), "storing fixture %d", index)

	claimed, err := f.ds.ClaimPendingEventOutbox(ctx, 1, time.Minute)
	require.NoErrorf(f.t, err, "claiming fixture %d", index)
	require.Lenf(f.t, claimed, 1, "fixture %d must be claimable", index)
	require.Equal(f.t, entry.EventID, claimed[0].EventID)

	require.NoErrorf(f.t,
		f.ds.MarkEventDispatched(ctx, claimed[0].ID, claimed[0].ClaimToken, result.Record),
		"recording the broker coordinate for fixture %d", index)

	return result.Record
}

// reconcile runs the whole live join: measure the broker, audit the outbox against exactly the
// windows that measurement produced, and interpret the pair with the production verdict.
//
// The intervals are taken from the report rather than computed here, which is the invariant the
// interval form of the audit exists to enforce: an audit taken against different windows
// produces a verdict about nothing.
func (f *zeroLossFixture) reconcile(ctx context.Context) (TopicOffsetReport, model.EventRecordIntervalAudit, OutboxReconciliation) {
	f.t.Helper()

	report, err := f.admin.TopicEndOffsets(ctx, time.Time{}, f.topic)
	require.NoErrorf(f.t, err, "measuring the end offsets of %s", f.topic)

	measured := make([]string, 0, len(report.Topics))
	for _, snapshot := range report.Topics {
		measured = append(measured, snapshot.Topic)
	}
	require.Containsf(f.t, measured, f.topic,
		"the measurement must cover %s; a row on an unmeasured topic is neither confirmed nor "+
			"ruled out and can never be part of a conclusive verdict", f.topic)
	require.Emptyf(f.t, report.MissingTopics,
		"%s must exist on the broker. Provision it with `docker compose --profile kafka up -d "+
			"kafka-init` or scripts/kafka-provision.sh", f.topic)

	audit, err := f.ds.AuditEventRecordsInIntervals(ctx, report.PartitionIntervals())
	require.NoError(f.t, err, "auditing the outbox against the measured partition windows")

	return report, audit, ReconcileAgainstOutbox(report, audit)
}

// TestZeroLoss_TheOutboxCensusAndTheBrokerOffsetsReconcileOverRealRecords is acceptance
// criterion V-2, joined.
//
// # The two outcomes, and why both are required
//
// A reconciliation that can only return one answer is not a reconciliation. Asserting the green
// verdict alone would pass against a function that returned "no loss detected" unconditionally —
// which is the single most dangerous defect this criterion can have, because it is invisible in
// every healthy run and silent in the one that matters. So the same live pieces are used twice:
//
//  1. CONCLUSIVE. Six events are published to a real topic, each row records the coordinate the
//     broker actually assigned, and the verdict must be conclusive with every row corroborated.
//  2. LOSS DETECTED. One further row claims a coordinate at the end of its partition's log — a
//     record the broker has not written and, by construction, does not have. The verdict must
//     flip, name the row, and say why.
//
// The second is the honest form of a loss because it is the form a loss actually takes. A row
// claiming an offset the log does not reach is what a truncated partition, a recreated topic, or
// an event lost after being marked published leaves behind, and it is the one signal the
// arithmetic cannot explain away as redelivery overhead.
func TestZeroLoss_TheOutboxCensusAndTheBrokerOffsetsReconcileOverRealRecords(t *testing.T) {
	fixture := newZeroLossFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// THE BASELINE IS PROVABLY EMPTY, which is what owning the database buys and what makes
	// every count below an equality rather than a delta.
	_, emptyAudit, emptyVerdict := fixture.reconcile(ctx)
	require.Zerof(t, emptyAudit.PublishedRows,
		"the disposable database must start with no row claiming a publication; it holds %d, so "+
			"the migrations ran against the wrong database", emptyAudit.PublishedRows)
	assert.False(t, emptyVerdict.LossDetected,
		"an outbox with nothing in it cannot have lost anything")
	assert.Contains(t, emptyVerdict.Summary(), "nothing to reconcile",
		"and the verdict must say so plainly rather than reporting a green result it did not measure")

	records := make([]model.BrokerRecord, 0, zeroLossEvents)
	for index := range zeroLossEvents {
		records = append(records, fixture.publishAndRecord(ctx, index))
	}

	// The keyed balancer must have spread the fixtures, or a partition read from the wrong field
	// would be indistinguishable from a correct one — every row would name partition 0 and every
	// window measured would be partition 0's.
	partitions := make(map[int]struct{}, len(records))
	for _, record := range records {
		partitions[record.Partition] = struct{}{}
	}
	t.Logf("zero-loss fixtures landed on %d partition(s) of %s", len(partitions), fixture.topic)

	t.Run("every published row is corroborated against the record it produced", func(t *testing.T) {
		report, audit, verdict := fixture.reconcile(ctx)

		require.Equalf(t, int64(zeroLossEvents), audit.PublishedRows,
			"the census must see exactly the rows this test published.\naudit: %+v", audit)
		assert.Equalf(t, int64(zeroLossEvents), audit.CorroboratedRows,
			"every row must be matched to a record inside its own partition's measured window. "+
				"This is the assertion that fails when the stored partition and the measured window "+
				"come from different fields, or when the interval is computed exclusive at one end "+
				"and read inclusive at the other.\naudit: %+v\nintervals: %+v",
			audit, report.PartitionIntervals())
		assert.Zerof(t, audit.UnconfirmedRows,
			"a row that claims a publication without naming a record would mean MarkEventDispatched "+
				"discarded the coordinate the broker acknowledged")
		assert.Zero(t, audit.UnmeasuredRows, "every fixture is on the measured topic")
		assert.Zerof(t, audit.BeyondEndRows,
			"a coordinate the broker itself assigned cannot be beyond the end of the log it "+
				"assigned it in; if it is, the stored offset is not the offset that was returned")
		assert.Zero(t, audit.AgedOutRows,
			"the fixtures were written seconds ago and cannot have aged out of retention")
		assert.Equalf(t, audit.CorroboratedRows, audit.DistinctCorroboratedRecords,
			"each row must name a DISTINCT record. Two rows sharing one coordinate is the "+
				"double-counting the unique index forbids, and it is how a genuine shortfall hides "+
				"behind an apparent surplus")

		assert.Falsef(t, verdict.LossDetected,
			"no loss may be reported: every row names a record verified against the live bounds of "+
				"the partition it claims.\nverdict: %s", verdict.Summary())
		assert.Truef(t, verdict.Conclusive,
			"the verdict must be CONCLUSIVE. An inconclusive result here means some row could not "+
				"be placed, and the caveats say which.\ncaveats: %v\nverdict: %s",
			verdict.Caveats, verdict.Summary())
		assert.Equal(t, int64(zeroLossEvents), verdict.TerminalEvents)
		assert.Equal(t, int64(zeroLossEvents), verdict.CorroboratedEvents)
		assert.Contains(t, verdict.Summary(), "NO LOSS DETECTED",
			"the summary is what an operator pastes into a compliance record, so it must state the "+
				"conclusion in words and not only in a boolean")

		// The broker's own reading is reported alongside, and it is a CUMULATIVE total for a topic
		// this stack shares. It must not be mistaken for a per-window count, and it must not be
		// what the verdict rests on — which is exactly why the surplus is not read as a shortfall.
		assert.Positivef(t, report.EndOffsetSum,
			"the measured topic must report a positive end offset sum after six records were "+
				"written to it")
		assert.GreaterOrEqual(t, report.EndOffsetSum, int64(zeroLossEvents),
			"the log must hold at least the records this test wrote")
	})

	t.Run("a row naming a record the log does not reach is reported as loss", func(t *testing.T) {
		report, _, _ := fixture.reconcile(ctx)

		intervals := report.PartitionIntervals()
		require.NotEmpty(t, intervals, "the measurement must produce at least one partition window")

		// AT the end offset, not past it by an arbitrary amount. The end offset is the position
		// the NEXT record will take, so it is precisely the first coordinate that does not exist —
		// which makes this the boundary case rather than an obviously absurd value, and the
		// boundary is where an off-by-one lives.
		target := intervals[0]
		phantom := model.BrokerRecord{
			Topic:     target.Topic,
			Partition: target.Partition,
			Offset:    target.EndOffset,
		}

		aggregate := fmt.Sprintf("zl-%s-phantom", fixture.runID)
		entry := &model.EventOutbox{
			EventID:       uuid.NewString(),
			EventType:     zeroLossEventType,
			AggregateID:   aggregate,
			PartitionKey:  aggregate,
			LedgerID:      "zl-" + fixture.runID + "-ldg",
			Topic:         fixture.topic,
			SchemaVersion: model.SchemaVersionV1,
			Payload: json.RawMessage(`{"event":"` + zeroLossEventType +
				`","data":{"transaction_id":"` + aggregate + `","status":"APPLIED"}}`),
			OccurredAt:  time.Now().UTC(),
			MaxAttempts: 5,
		}
		require.NoError(t, fixture.ds.InsertEventOutbox(ctx, entry))

		claimed, err := fixture.ds.ClaimPendingEventOutbox(ctx, 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		require.Equal(t, entry.EventID, claimed[0].EventID)
		require.NoError(t,
			fixture.ds.MarkEventDispatched(ctx, claimed[0].ID, claimed[0].ClaimToken, phantom),
			"the row must be recordable: the repository stores the coordinate it is given, and "+
				"detecting that it cannot exist is the reconciliation's job rather than the write's")

		_, audit, verdict := fixture.reconcile(ctx)

		require.Equalf(t, int64(1), audit.BeyondEndRows,
			"exactly the phantom row must be classified beyond the end of its partition's log.\n"+
				"phantom: %s\naudit: %+v", phantom.String(), audit)
		assert.Equal(t, int64(zeroLossEvents), audit.CorroboratedRows,
			"the six genuine rows must still be corroborated; a phantom must not contaminate them")
		assert.Equal(t, int64(zeroLossEvents+1), audit.PublishedRows)

		assert.Truef(t, verdict.LossDetected,
			"THE VERDICT MUST FLIP. A row claiming an offset the log does not reach is the one "+
				"signal that cannot be explained by retention, by a redelivery or by another "+
				"producer — the broker issued that offset, so the log reached it once and does not "+
				"now.\nverdict: %s", verdict.Summary())
		assert.Equal(t, int64(1), verdict.BeyondEndEvents)
		assert.Contains(t, verdict.Summary(), "LOSS DETECTED",
			"the summary must lead with the conclusion")
		assert.Containsf(t, strings.Join(verdict.Caveats, " "),
			"beyond the end",
			"the caveats must name the reason, so an operator knows to look for a truncated "+
				"partition or a recreated topic rather than at the ledger.\ncaveats: %v",
			verdict.Caveats)
		assert.False(t, verdict.Conclusive,
			"a verdict reporting loss is not a conclusive clean bill of health")
	})
}

// TestZeroLoss_TheStatisticsProjectionIsAssembledFromLivePostgresAndLiveKafka is the other half
// of the V-2 join: the ORCHESTRATION an operator actually reads, rather than its two inputs.
//
// # Why the reconciliation test above is not enough
//
// That test calls the three collaborators itself — measure the broker, audit the outbox against
// exactly those windows, interpret the pair — which proves the collaborators agree. It says
// nothing about the code that SEQUENCES them, and that code owns every policy decision the
// endpoint's answer depends on: which instant both sides are measured from, whether the audit is
// taken at all, which failures degrade the answer and which refuse it, and whether the verdict is
// reported or withheld. Those decisions are exercised exhaustively against fakes elsewhere in
// event_admin_test.go — TestEventOutboxStatistics_ReadsTheOutboxFirstAndTheBrokerAsAnEnrichment,
// TestEventOutboxStatistics_ProducesAVerdictOnlyWhenBothSidesWereMeasured and
// TestEventOutboxStatistics_ComparesOneCommonPopulation among them — because a policy reachable
// only through a live PostgreSQL and a live broker is a policy whose branches go untested.
//
// What no fake can establish is that the real repository, the real broker read and the real
// verdict COMPOSE: that the counts the statistics report come from the same rows the audit
// classifies, that the intervals the broker measured are the intervals the audit was taken
// against, and that a verdict assembled from all three says what the two halves separately say.
// This test is that composition, and it is the reason it lives beside the fixture that owns a
// disposable database rather than beside the fake-driven cases.
//
// # Both directions, again
//
// A projection that can only report health is worse than none, so the same live pieces answer
// twice: once where every row names a record the broker can serve, and once where a row claims a
// publication it cannot name at all. The second is the honest shape of a relay that crashed
// between the write and the acknowledgement, and it must withdraw the verdict's confidence
// rather than average the row away.
func TestZeroLoss_TheStatisticsProjectionIsAssembledFromLivePostgresAndLiveKafka(t *testing.T) {
	fixture := newZeroLossFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), zeroLossBudget)
	defer cancel()

	// The whole inventory, exactly as the production reader does it. Narrowing to one topic would
	// leave rows on any other topic UNMEASURED rather than corroborated, which is a caveat the
	// endpoint must never manufacture for itself — see the note on b.readEventTopicEndOffsets.
	readOffsets := func(ctx context.Context, since time.Time) (TopicOffsetReport, error) {
		return fixture.admin.TopicEndOffsets(ctx, since)
	}

	t.Run("a fully corroborated outbox yields a conclusive verdict", func(t *testing.T) {
		for index := 0; index < zeroLossEvents; index++ {
			fixture.publishAndRecord(ctx, index)
		}

		statistics, err := eventOutboxStatistics(
			ctx, fixture.ds, readOffsets, EventOffsetsRequired, zeroLossStatisticsWindow,
		)
		require.NoError(t, err, "both sides are live and healthy, so the required posture must succeed")

		// THE POSTURE FLAGS BEFORE THE FIGURES THEY GOVERN. "Measured and zero" and "not
		// measured" are different answers, and reading the second as the first is how a clean
		// bill of health gets reported from a measurement that never happened.
		require.True(t, statistics.OffsetsRead,
			"the broker side must be recorded as measured, or every figure below describes an "+
				"absence rather than a reading")
		require.True(t, statistics.AuditRead,
			"the audit must be recorded as taken; without it there is nothing to place the "+
				"broker's offsets against")
		require.NotNil(t, statistics.Reconciliation,
			"a verdict must be produced when both sides were measured and a topic was covered")

		// THE COUNTS COME FROM THE OWNED TABLE, so they are exact rather than a lower bound.
		assert.Equal(t, int64(zeroLossEvents),
			statistics.CountsByStatus[model.EventOutboxStatusDispatched],
			"every row this test recorded is dispatched, and the census must find exactly those")
		assert.Empty(t, statistics.UnreportedStatuses,
			"the projection must recognise every status present in the table; an unreported one "+
				"means the census sums to less than the table holds")
		assert.Equal(t, zeroLossStatisticsWindow, statistics.Window,
			"the window must be reported as requested, because every dispatched count is relative to it")
		assert.False(t, statistics.WindowStart.IsZero(),
			"the instant both sides were measured from must be reported, or the counts describe "+
				"an interval the reader cannot name")

		verdict := statistics.Reconciliation
		assert.Equal(t, int64(zeroLossEvents), verdict.TerminalEvents,
			"every row claiming a publication must be counted once, which is what the unique "+
				"event_id index guarantees")
		assert.Equal(t, int64(zeroLossEvents), verdict.CorroboratedEvents,
			"each row names a coordinate the broker acknowledged, so each must be placed INSIDE "+
				"the measured window of its own partition")
		assert.Zero(t, verdict.UnconfirmedEvents,
			"no row may claim a publication it cannot name")
		assert.Zero(t, verdict.BeyondEndEvents)
		assert.Zero(t, verdict.UnmeasuredEvents,
			"the full inventory was measured, so no row may sit on a partition nothing covered")
		assert.False(t, verdict.LossDetected)
		assert.Truef(t, verdict.Conclusive,
			"with every row corroborated inside a measured window there is nothing left to "+
				"qualify.\nsummary: %s\ncaveats: %v", verdict.Summary(), verdict.Caveats)
		assert.Contains(t, verdict.Summary(), "NO LOSS DETECTED")

		// THE TWO SIDES DESCRIBE ONE POPULATION. The audit the orchestration took must be the
		// audit of the intervals the report it just measured produced — the invariant the
		// interval form of the query exists to enforce — so the offsets it recorded must cover
		// the topic the rows are on.
		measured := make([]string, 0, len(statistics.Offsets.Topics))
		for _, snapshot := range statistics.Offsets.Topics {
			measured = append(measured, snapshot.Topic)
		}
		assert.Contains(t, measured, fixture.topic,
			"the measurement the verdict rests on must cover the topic the rows name")
		assert.Equal(t, int64(zeroLossEvents), statistics.Audit.CorroboratedRows,
			"the audit the projection carries must be the one the verdict was drawn from")
	})

	t.Run("a row that cannot name its record withdraws the verdict's confidence", func(t *testing.T) {
		// A ROW THAT IS DISPATCHED AND NAMES NOTHING. This is what a relay that crashed between
		// a successful publish and its bookkeeping leaves behind, and it is the population a
		// surplus of redeliveries could be hiding: arithmetically it is indistinguishable from a
		// healthy row, so the verdict must refuse to conclude rather than count it as fine.
		aggregate := fmt.Sprintf("zl-%s-unconfirmed", fixture.runID)
		entry := &model.EventOutbox{
			EventID:       uuid.NewString(),
			EventType:     zeroLossEventType,
			AggregateID:   aggregate,
			PartitionKey:  aggregate,
			LedgerID:      "zl-" + fixture.runID + "-ldg",
			Topic:         fixture.topic,
			SchemaVersion: model.SchemaVersionV1,
			Payload: json.RawMessage(`{"event":"` + zeroLossEventType +
				`","data":{"transaction_id":"` + aggregate + `","status":"APPLIED"}}`),
			OccurredAt:  time.Now().UTC(),
			MaxAttempts: 5,
		}
		require.NoError(t, fixture.ds.InsertEventOutbox(ctx, entry))

		claimed, err := fixture.ds.ClaimPendingEventOutbox(ctx, 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		require.Equal(t, entry.EventID, claimed[0].EventID)

		// The UNCONFIRMED record: no topic, no partition, no offset. The repository accepts it
		// because a publish whose acknowledgement was lost is a real outcome; classifying it is
		// the reconciliation's job, not the write's.
		require.NoError(t,
			fixture.ds.MarkEventDispatched(ctx, claimed[0].ID, claimed[0].ClaimToken, model.BrokerRecord{}),
			"a dispatched row with no coordinate must be recordable, or this state could not arise")

		statistics, err := eventOutboxStatistics(
			ctx, fixture.ds, readOffsets, EventOffsetsRequired, zeroLossStatisticsWindow,
		)
		require.NoError(t, err, "an unconfirmed row is a caveat, never a failure of the read")
		require.NotNil(t, statistics.Reconciliation)

		verdict := statistics.Reconciliation
		assert.Equal(t, int64(1), verdict.UnconfirmedEvents,
			"exactly the row with no coordinate may be unconfirmed")
		assert.Equal(t, int64(zeroLossEvents), verdict.CorroboratedEvents,
			"the corroborated rows must be unaffected; one unconfirmed row does not discredit them")
		assert.Falsef(t, verdict.Conclusive,
			"WHILE ANY ROW CANNOT BE PLACED, THE VERDICT IS INCONCLUSIVE. Those rows are exactly "+
				"what a surplus of redeliveries could be masking, so a green answer here would be "+
				"the masking defect itself.\nsummary: %s", verdict.Summary())
		assert.False(t, verdict.LossDetected,
			"an unplaceable row is not evidence of loss; only a coordinate beyond the log end is")
		assert.Contains(t, verdict.Summary(), "INCONCLUSIVE")
		assert.NotEmpty(t, verdict.Caveats,
			"the reason must be stated: an operator cannot act on a withdrawn verdict that does "+
				"not say what withdrew it")
	})
}
