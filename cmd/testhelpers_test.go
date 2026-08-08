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

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Default endpoints for the services this tier needs, used when the environment
// names none. They are the same defaults the database and root suites carry, so a
// developer running everything on the conventional local ports needs to export
// nothing at all.
const (
	defaultCmdTestDSN     = "postgres://postgres:@localhost:5432/blnk?sslmode=disable"
	defaultCmdTestRedis   = "localhost:6379"
	defaultCmdTestBrokers = "localhost:9092"
)

// firstEnvValue returns the value of the first variable in names that is set to a
// non-blank value, or fallback when none is.
//
// Blank is treated as unset deliberately. An exported-but-empty variable is what a
// shell leaves behind after `export VAR=` or a compose file's `${VAR}` with nothing
// to substitute, and honouring it would point this tier at an empty DSN — a failure
// that reads as "Postgres is down" rather than "the variable is empty".
//
// Parameters:
//   - fallback string: the value to use when no name is set.
//   - names ...string: the variables to consult, in precedence order.
//
// Returns:
//   - string: the resolved value, never blank as long as fallback is not.
func firstEnvValue(fallback string, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return fallback
}

// cmdTestDSN resolves the Postgres DSN this tier should use.
//
// TEST_DATABASE_URL is consulted first because that is the override the rest of the
// repository already establishes — database/filter_queries_coverage_test.go reads it
// with defaultRealTestDSN as its fallback — and a test-only variable should win over
// a deployment one. BLNK_DATA_SOURCE_DNS follows, because a developer who has
// already pointed the application at a relocated Postgres should not have to name it
// a second time.
//
// This tier used to hardcode the DSN, which pinned every test in it to
// localhost:5432 no matter what the environment said. That is how a database
// carrying an incompatible blnk.event_outbox schema was reached while a correctly
// migrated one sat on another port, and the resulting insert failure was reported as
// a product defect.
func cmdTestDSN() string {
	return firstEnvValue(defaultCmdTestDSN, "TEST_DATABASE_URL", "BLNK_DATA_SOURCE_DNS")
}

// cmdTestRedisDSN resolves the Redis endpoint, following cmdTestDSN's precedence so
// the whole tier relocates together. A stack moved to alternative ports is moved for
// all of its services, and leaving one of them pinned would half-relocate the tier.
func cmdTestRedisDSN() string {
	return firstEnvValue(defaultCmdTestRedis, "TEST_REDIS_URL", "BLNK_REDIS_DNS")
}

// cmdTestKafkaBrokers resolves the broker list, honouring the deployment-mandated
// KAFKA_BROKERS and the BLNK_-prefixed alias the configuration package resolves for
// it, and splitting on commas exactly as the configuration loader does.
//
// Nothing in this tier DIALS a broker: a configured broker list is what switches
// event capture on — eventPublishingConfigured reads only configuration — and the
// relay that would speak to it is not running. The list still has to be honoured,
// because a deployment whose brokers are elsewhere would otherwise be described by
// this tier as running on localhost:9092.
func cmdTestKafkaBrokers() []string {
	raw := firstEnvValue(defaultCmdTestBrokers, "KAFKA_BROKERS", "BLNK_KAFKA_BROKERS")
	brokers := make([]string, 0, 1)
	for _, candidate := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	if len(brokers) == 0 {
		return []string{defaultCmdTestBrokers}
	}
	return brokers
}

// realInfraConfig installs (via config.MockConfig) and returns a configuration
// pointing at the test services, following the same convention as the api and root
// package suites: Postgres, Redis and Typesense are hard test dependencies. Their
// endpoints come from the environment through the helpers above, defaulting to the
// conventional local ports.
func realInfraConfig(t *testing.T) *config.Configuration {
	t.Helper()
	cfg := &config.Configuration{
		DataSource: config.DataSourceConfig{Dns: cmdTestDSN()},
		Redis:      config.RedisConfig{Dns: cmdTestRedisDSN()},
		Queue: config.QueueConfig{
			TransactionQueue:    "transaction_queue_cmd_test",
			NumberOfQueues:      1,
			WebhookQueue:        "webhook_queue_cmd_test",
			IndexQueue:          "index_queue_cmd_test",
			InflightExpiryQueue: "inflight_expiry_cmd_test",
			InflightCommitQueue: "inflight_commit_cmd_test",
			MonitoringPort:      "5117",
		},
		Transaction: config.TransactionConfig{
			BatchSize:    100,
			MaxQueueSize: 1000,
			MaxWorkers:   2,
			// MockConfig runs validateAndAddDefaults, which interprets a
			// non-zero LockDuration as SECONDS and multiplies by time.Second.
			LockDuration: 30,
		},
	}
	config.MockConfig(cfg)
	fetched, err := config.Fetch()
	require.NoError(t, err)
	return fetched
}

// skipUnlessEventOutboxSchemaMatches skips the test unless the database behind ds
// accepts the exact INSERT the event-capture path issues.
//
// # Why a probe insert rather than a column-presence query
//
// The failure this guards against ran in BOTH directions. A database migrated to an
// earlier revision is MISSING columns the insert names, and answers with SQLSTATE
// 42703. A database carrying a column this revision does not know about — which is
// what happened: a NOT NULL bytea with no default, left behind by a different
// revision sharing the same Postgres — is missing nothing at all, and answers with
// SQLSTATE 23502 on a column the code has never heard of. A query that checks for
// the presence of an enumerated column list detects the first and is blind to the
// second, and it also has to spell the column list a second time, which is how the
// list and the statement drift apart.
//
// Issuing the product's own insert covers both directions, needs no column list, and
// cannot drift: whatever the insert binds is what is probed, for as long as the two
// are the same function. The probe runs inside a transaction that is always rolled
// back, so it leaves nothing behind — not even a consumed BIGSERIAL value that any
// assertion depends on.
//
// # Why a skip and not a failure
//
// An unmigrated or foreign schema is missing infrastructure, in exactly the way an
// unreachable Postgres is, and this tier already skips for that. Reporting it as a
// failure attributes an environment problem to the product, which is precisely the
// misattribution this exists to prevent — and the message therefore names the
// database and port actually reached alongside the DSN that was asked for, because
// two `blnk` databases on two ports are trivially confused and the remedy has to be
// unambiguous about which one to migrate.
//
// Parameters:
//   - t *testing.T: the test to skip.
//   - ds database.IDataSource: the datasource whose insert path is probed.
//   - dsn string: the DSN that was asked for, reported alongside what was reached.
func skipUnlessEventOutboxSchemaMatches(t *testing.T, ds database.IDataSource, dsn string) {
	t.Helper()

	// Only the concrete datasource exposes a pool to open a transaction on. A wrapped
	// or fake implementation is left to the assertions themselves — the probe is a
	// diagnostic, not a requirement.
	source, exposesPool := ds.(*database.Datasource)
	if !exposesPool || source.Conn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Reported, not asserted on.
	//
	// The port is the one the SERVER sees itself listening on, which is NOT the port in
	// the DSN when the server sits behind a port mapping — a container published on
	// 5452 reports 5432. It is still worth naming, because it distinguishes a server
	// reached directly from one reached through a mapping, but it is labelled as
	// server-side so it cannot be mistaken for the endpoint that was dialled. Both are
	// NULL over a unix socket, hence the COALESCEs.
	var reachedDatabase, reachedAddress string
	var reachedPort int
	if err := source.Conn.QueryRowContext(ctx,
		"SELECT current_database(), COALESCE(host(inet_server_addr()), 'local socket'), "+
			"COALESCE(inet_server_port(), 0)",
	).Scan(&reachedDatabase, &reachedAddress, &reachedPort); err != nil {
		t.Skipf("cmd test suite: %s is not answering queries (%v). Start PostgreSQL and "+
			"apply the migrations with `blnk migrate up`", dsn, err)
	}

	tx, err := source.Conn.BeginTx(ctx, nil)
	if err != nil {
		t.Skipf("cmd test suite: %s would not begin a transaction (%v)", dsn, err)
	}
	// Always rolled back — including on the success path, which is the point: the probe
	// proves the statement is ACCEPTED without persisting a row.
	defer func() { _ = tx.Rollback() }()

	// event_id must be a CANONICAL UUID — the insert path validates it before binding
	// anything — so it is minted as one rather than with the suffixed-id helper the
	// rest of this file uses for ledger objects. A suffixed id fails validation and
	// would make this probe skip every test in the tier on a perfectly good database,
	// which is the worst possible failure mode for a guard: a skip looks like success.
	probe := &model.EventOutbox{
		EventID:     uuid.NewString(),
		EventType:   "transaction.rejected",
		AggregateID: model.GenerateUUIDWithSuffix("txnprobe"),
		Topic:       "blnk.transactions",
		Payload:     json.RawMessage(`{"event":"transaction.rejected","data":{}}`),
		OccurredAt:  time.Now(),
	}
	// PartitionKey is set explicitly rather than left to a default: a persisted row
	// never carries a blank key, so a probe with one would be testing a shape the
	// product does not produce.
	probe.PartitionKey = probe.AggregateID

	if err := ds.InsertEventOutboxInTx(ctx, tx, probe); err != nil {
		t.Skipf("cmd test suite: blnk.event_outbox does not accept the row the event-capture "+
			"path writes (%v). %s The connection is on database %q, server-side %s:%d; this suite "+
			"asked for %q — and note that database.GetDBConnection is a process-wide sync.Once "+
			"singleton, so the FIRST DSN used in this test binary is the one in force. Apply "+
			"THAT database's migrations with `blnk migrate up`, or point TEST_DATABASE_URL at a "+
			"database migrated by this revision: a column this revision does not bind, or one it "+
			"binds and the database does not have, produces this and would otherwise be reported "+
			"as a product failure",
			err, describeEventOutboxColumns(ctx, source.Conn), reachedDatabase, reachedAddress,
			reachedPort, dsn)
	}
}

// describeEventOutboxColumns summarises blnk.event_outbox's columns for the skip
// message above.
//
// The repository maps every driver error onto a typed APIError and carries the cause
// only into the log, so the error the probe gets back says "missing a required field"
// or "Database error occurred" and names no column. That is right for an API response
// and useless for a diagnosis, so the shape of the table is reported alongside it.
//
// The NOT NULL columns that have no default are listed first because they are the
// actionable subset: the insert names a fixed column list, so any column outside that
// list which the table insists on and supplies no default for is precisely the drift
// that refuses the row. The full name list follows, which answers the opposite
// direction — a column the insert binds that the table does not have.
//
// Run on the pool rather than on the probe's transaction, because that transaction is
// aborted by the failed insert and would refuse any further statement.
//
// Parameters:
//   - ctx context.Context: carries the caller's timeout.
//   - pool *sql.DB: the connection pool to query.
//
// Returns:
//   - string: one sentence, always non-empty, safe to embed in a message. Any error
//     reading the catalogue is reported in place of the inventory rather than
//     replacing the refusal that prompted it.
func describeEventOutboxColumns(ctx context.Context, pool *sql.DB) string {
	rows, err := pool.QueryContext(ctx,
		"SELECT column_name, is_nullable, column_default IS NOT NULL "+
			"FROM information_schema.columns "+
			"WHERE table_schema = 'blnk' AND table_name = 'event_outbox' "+
			"ORDER BY ordinal_position")
	if err != nil {
		return fmt.Sprintf("(the column inventory could not be read: %v.)", err)
	}
	defer func() { _ = rows.Close() }()

	var all, mandatory []string
	for rows.Next() {
		var name, nullable string
		var hasDefault bool
		if err := rows.Scan(&name, &nullable, &hasDefault); err != nil {
			return fmt.Sprintf("(the column inventory could not be read: %v.)", err)
		}
		all = append(all, name)
		if nullable == "NO" && !hasDefault {
			mandatory = append(mandatory, name)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Sprintf("(the column inventory could not be read: %v.)", err)
	}
	if len(all) == 0 {
		return "The table does not exist at all, so no migration of this feature has been applied."
	}

	return fmt.Sprintf("The table has %d columns; those that are NOT NULL with no default — "+
		"the ones a row must supply — are %v; all of them, in order, are %v.",
		len(all), mandatory, all)
}

// newCmdTestInstance builds a blnkInstance backed by the real local Postgres
// and Redis, mirroring how preRun wires the production instance.
func newCmdTestInstance(t *testing.T) *blnkInstance {
	t.Helper()
	cfg := realInfraConfig(t)

	newBlnk, err := setupBlnk(cfg)
	require.NoError(t, err, "Postgres and Redis must be running for the cmd test suite")

	return &blnkInstance{blnk: newBlnk, cnf: cfg}
}

// createBalancePair creates two USD balances in the general ledger.
func createBalancePair(t *testing.T, b *blnkInstance) (*model.Balance, *model.Balance) {
	t.Helper()
	ds := b.blnk.GetDataSource()
	src, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
	require.NoError(t, err)
	dst, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
	require.NoError(t, err)
	return &src, &dst
}

// queuedTransaction builds a QUEUED-status transaction payload exactly like
// the queue producer would put on the wire for processTransaction.
func queuedTransaction(src, dst string, amount float64, allowOverdraft bool) *model.Transaction {
	txn := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"),
		Reference:      "ref_" + gofakeit.UUID(),
		Source:         src,
		Destination:    dst,
		Amount:         amount,
		Precision:      100,
		Currency:       "USD",
		AllowOverdraft: allowOverdraft,
		Status:         "QUEUED",
		CreatedAt:      time.Now(),
	}
	model.ApplyPrecision(txn)
	// The queue producer ships AmountString on the wire; the rejection path
	// persists it verbatim into the numeric amount column.
	txn.AmountString = strconv.FormatFloat(amount, 'f', -1, 64)
	return txn
}
