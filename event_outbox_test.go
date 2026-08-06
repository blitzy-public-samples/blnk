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

// event_outbox_test.go exists for ONE decisive reason: to prove that the bytes
// PrepareEventOutbox stores in blnk.event_outbox.payload are BYTE-IDENTICAL to the HTTP
// body the legacy webhook transport has always POSTed.
//
// That single property is what three separate guarantees rest on:
//
//   - Dual delivery. For the 30-day window the relay publishes the claimed outbox row to
//     Kafka AND enqueues the legacy webhook task from that same row. Their payloads are
//     equal because they are the same bytes, and this file is where "the same bytes" is
//     actually checked.
//   - Subscriber compatibility. The payload is the whole two-key {"event": ..., "data":
//     ...} object, not just the inner "data", so an existing subscriber's body parser
//     keeps working when only the transport changes.
//   - Replay fidelity. A dead-lettered event is replayed by re-publishing the stored
//     bytes rather than by re-marshalling a struct, so byte equality at rest is what
//     makes byte equality on replay possible.
//
// # Why the comparisons are on raw bytes
//
// Every payload assertion below compares []byte (or its string form) and NEVER an
// unmarshalled map. json.Unmarshal into a map[string]interface{} discards key ORDER and
// renormalises numbers, and key order is precisely what the dual-delivery equality
// criterion depends on. A map comparison would keep passing while the guarantee was
// broken — the worst possible failure mode for a test whose whole purpose is to detect
// exactly that. The same reasoning drives the exact-value style throughout: this is a
// money-adjacent path held to the repository's mutation-testing bar, so assertions
// compare exact bytes and exact values rather than merely non-empty ones.
//
// # Scope: what this file deliberately does NOT cover
//
//   - No Kafka import and no broker. Everything here runs with no infrastructure, which
//     mirrors the file under test: event_outbox.go talks to no broker either.
//   - No retry, backoff or dead-letter assertions. Those belong to event_relay_test.go
//     and event_dlt_test.go, which act on rows this file's subject creates.
//   - No SQL-statement coverage beyond what go-sqlmock needs to identify the statement
//     the two insert paths issue. database/event_outbox_test.go owns the repository.
//   - No assertions about blnk.lineage_outbox or the lineage relay. The two outboxes are
//     separate tables served by separate relays and are deliberately not merged.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
)

// ---------------------------------------------------------------------------
// Fixture identifiers and instants
// ---------------------------------------------------------------------------

// The identifiers below are fixed literals rather than generated values so that every
// expected partition key and aggregate id in this file can also be written as a literal.
// A derived expectation would agree with the implementation whatever the implementation
// did; a literal one is a second, independent statement of the contract.
const (
	// outboxLedgerID is the ledger every fixture belongs to. It is the expected Kafka
	// message key for ledger and balance events.
	outboxLedgerID = "ldg_outbox_fixture"
	// outboxSourceBalanceID is the source balance of the transaction fixtures, and
	// therefore their expected message key: transaction keying prefers the source
	// balance so that Kafka partitioning agrees with the transaction queue's own
	// sharding.
	outboxSourceBalanceID = "bln_outbox_source"
	// outboxDestinationBalanceID is the destination balance, used to prove the second
	// step of the transaction key preference.
	outboxDestinationBalanceID = "bln_outbox_destination"
	// outboxTransactionID is the transaction fixtures' aggregate, and the last resort
	// of the transaction key preference.
	outboxTransactionID = "txn_outbox_fixture"
	// outboxIdentityID is the identity fixture's aggregate and message key: an identity
	// is not scoped to a ledger in this model, so it is its own aggregate.
	outboxIdentityID = "idt_outbox_fixture"
	// outboxMonitorID is the balance monitor's own id — its aggregate, but NOT its key.
	outboxMonitorID = "mon_outbox_fixture"
	// outboxBatchID is the bulk transaction batch, which is both aggregate and key for
	// the bulk_transaction.<status> family.
	outboxBatchID = "bulk_outbox_fixture"
)

// outboxFixedInstant is the single timestamp every fixture payload carries.
//
// It is fixed, UTC and sub-second precise for three reasons: byte-equality comparisons
// need a payload that does not change between the expected and the actual marshal; the
// nanosecond component proves that time values survive the payload verbatim at
// RFC3339Nano precision; and a literal expected payload string can be written for it.
var outboxFixedInstant = time.Date(2026, time.March, 14, 15, 9, 26, 535897932, time.UTC)

// outboxSampleLedger returns the payload the post-ledger-creation actions pass for
// "ledger.created": a *model.Ledger.
//
// A fresh value is built on every call so that no test can mutate a fixture another
// test depends on.
func outboxSampleLedger() *model.Ledger {
	return &model.Ledger{
		LedgerID:  outboxLedgerID,
		Name:      "Outbox Fixture Ledger",
		CreatedAt: outboxFixedInstant,
		MetaData:  map[string]interface{}{"region": "eu-west-1"},
	}
}

// outboxSampleIdentity returns the payload the post-identity-creation actions pass for
// "identity.created": a *model.Identity.
func outboxSampleIdentity() *model.Identity {
	return &model.Identity{
		IdentityID:   outboxIdentityID,
		IdentityType: "individual",
		FirstName:    "Ada",
		LastName:     "Lovelace",
		EmailAddress: "ada@example.com",
		Nationality:  "GB",
		CreatedAt:    outboxFixedInstant,
		MetaData:     map[string]interface{}{"tier": "gold"},
	}
}

// outboxSampleBalance returns the payload the post-balance-creation actions pass for
// "balance.created": a *model.Balance.
//
// The big.Int amounts are populated because they marshal as JSON numbers rather than
// strings, and a payload that carries none of them would not exercise that encoding at
// all.
func outboxSampleBalance() *model.Balance {
	return &model.Balance{
		Balance:               big.NewInt(125000),
		Version:               7,
		InflightBalance:       big.NewInt(0),
		CreditBalance:         big.NewInt(200000),
		InflightCreditBalance: big.NewInt(0),
		DebitBalance:          big.NewInt(75000),
		InflightDebitBalance:  big.NewInt(0),
		LedgerID:              outboxLedgerID,
		IdentityID:            outboxIdentityID,
		BalanceID:             outboxSourceBalanceID,
		Currency:              "USD",
		CreatedAt:             outboxFixedInstant,
		MetaData:              map[string]interface{}{"purpose": "settlement"},
	}
}

// outboxSampleBalanceMonitor returns the payload the balance monitor check passes for
// "balance.monitor".
//
// It is returned BY VALUE, not as a pointer, because that is exactly how balance.go
// passes it. The distinction is not cosmetic: the derivation type switch has separate
// arms for the pointer and the value form, and a fixture that used the wrong one would
// leave the arm the producer actually reaches untested.
func outboxSampleBalanceMonitor() model.BalanceMonitor {
	return model.BalanceMonitor{
		MonitorID:   outboxMonitorID,
		BalanceID:   outboxSourceBalanceID,
		Description: "alert when the settlement balance runs low",
		CallBackURL: "https://example.com/monitor-callback",
		CreatedAt:   outboxFixedInstant,
		Condition: model.AlertCondition{
			Value:        1000,
			Precision:    100,
			PreciseValue: big.NewInt(100000),
			Field:        "balance",
			Operator:     "<",
		},
	}
}

// outboxSampleTransaction returns the payload transaction execution passes for the seven
// status-derived events: a *model.Transaction carrying the given status.
func outboxSampleTransaction(status string) *model.Transaction {
	return &model.Transaction{
		PreciseAmount: big.NewInt(1502500),
		Amount:        15025,
		Precision:     100,
		TransactionID: outboxTransactionID,
		Source:        outboxSourceBalanceID,
		Destination:   outboxDestinationBalanceID,
		Reference:     "ref_outbox_fixture",
		Currency:      "USD",
		Description:   "outbox fixture transaction",
		Status:        status,
		Hash:          "b6d0f1a1c4e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8",
		CreatedAt:     outboxFixedInstant,
		MetaData:      map[string]interface{}{"channel": "api"},
	}
}

// outboxSampleTransactionValue returns the same transaction BY VALUE, which is how the
// worker's rejection handler passes it for "transaction.rejected".
func outboxSampleTransactionValue(status string) model.Transaction {
	return *outboxSampleTransaction(status)
}

// outboxSampleBulkPayload reproduces, exactly, the map that
// sendBulkTransactionWebhook builds for the bulk_transaction.<status> family.
//
// The two conditionals are deliberately restated here rather than imported, so this
// file is an independent statement of the three shapes the family can take:
//
//	status != "failed"                 → transaction_count is present
//	status == "failed" && errorMsg != "" → error is present
//	status == "failed" && errorMsg == "" → neither is present
func outboxSampleBulkPayload(status, errorMsg string, transactionCount int) map[string]interface{} {
	payload := map[string]interface{}{
		"batch_id":  outboxBatchID,
		"status":    status,
		"timestamp": outboxFixedInstant,
	}

	if status != "failed" {
		payload["transaction_count"] = transactionCount
	}

	if status == "failed" && errorMsg != "" {
		payload["error"] = errorMsg
	}

	return payload
}

// outboxSampleSystemErrorPayload reproduces the map notification.NotifyError hands to
// the registered webhook sender for "system.error": exactly two keys, neither of which
// identifies an aggregate of any kind. That absence is the reason the partition-key
// fallback chain exists.
func outboxSampleSystemErrorPayload() map[string]interface{} {
	return map[string]interface{}{
		"error": "balance monitor lookup failed",
		"time":  outboxFixedInstant,
	}
}

// ---------------------------------------------------------------------------
// The event catalogue: every event string Blnk emits, with literal expectations
// ---------------------------------------------------------------------------

// outboxEventFixture is one row of the catalogue: an event exactly as a producer emits
// it, paired with every value PrepareEventOutbox must derive from it.
//
// The topic, aggregate id and partition key are LITERALS. Deriving them from
// TopicForEvent or from the derivation functions themselves would make the table agree
// with the implementation no matter what the implementation said, including after a
// category was renamed or a key preference reordered.
type outboxEventFixture struct {
	// name identifies the subtest.
	name string
	// eventType is the event string as the producer spells it.
	eventType string
	// payload is what the producer passes, in the SAME form — pointer where the producer
	// passes a pointer, value where it passes a value.
	payload interface{}
	// topic is the fully-qualified destination topic, with the default "blnk" prefix.
	topic string
	// aggregateID is the entity the event is ABOUT: what a consumer groups by.
	aggregateID string
	// partitionKey is the expected ledger_id column — the Kafka message key, and
	// therefore the partition, and therefore the ordering guarantee.
	partitionKey string
}

// outboxEventCatalogueSize is the number of event types Blnk emits: thirteen.
//
// It is a CONTRACTUAL figure. The coverage requirement is that every event type which
// reached the legacy webhook sender reaches Kafka, with zero exceptions, and thirteen is
// what an exhaustive sweep of the producer call sites found: seven transaction lifecycle
// events from getEventFromStatus, the runtime-composed bulk transaction family counted
// once, two balance events, one identity event, one ledger event, and the system error
// raised through the registered webhook-sender indirection.
//
// Spelling it as an independent number is what stops the catalogue silently shrinking.
// A fourteenth event type is welcome, but it must arrive with a fixture row here.
const outboxEventCatalogueSize = 13

// outboxBulkEventPrefix is the fixed prefix of the bulk transaction family. Its names
// are composed at runtime from the batch status, so the family is identified by prefix
// and counted once in the catalogue.
const outboxBulkEventPrefix = "bulk_transaction."

// outboxEventVocabulary is the thirteen catalogue keys, written out by hand.
//
// The bulk family appears as its bare prefix because there is no exhaustive literal for
// an open-ended suffix set. Everything else is the literal event string.
var outboxEventVocabulary = []string{
	"transaction.queued",
	"transaction.applied",
	"transaction.scheduled",
	"transaction.inflight",
	"transaction.void",
	"transaction.rejected",
	"transaction.unknown",
	outboxBulkEventPrefix,
	"ledger.created",
	"identity.created",
	"balance.created",
	"balance.monitor",
	"system.error",
}

// outboxVocabularyKey collapses a concrete event string onto its catalogue key, so that
// the three bulk_transaction fixtures count as the one family they are.
func outboxVocabularyKey(eventType string) string {
	if strings.HasPrefix(eventType, outboxBulkEventPrefix) {
		return outboxBulkEventPrefix
	}

	return eventType
}

// outboxEventFixtures returns the complete catalogue: all thirteen event types Blnk
// emits, with the bulk transaction family represented by all three of its payload
// shapes.
//
// Fresh payloads are built on every call, so a test that mutates one cannot affect
// another.
func outboxEventFixtures() []outboxEventFixture {
	return []outboxEventFixture{
		// The seven status-derived transaction events. All seven come from
		// getEventFromStatus and all seven key on the SOURCE balance, which is what
		// keeps one transaction's whole lifecycle — queued, then inflight, then applied
		// — on a single partition and therefore in order.
		{
			name:         "transaction.queued",
			eventType:    "transaction.queued",
			payload:      outboxSampleTransaction(StatusQueued),
			topic:        "blnk.transactions",
			aggregateID:  outboxTransactionID,
			partitionKey: outboxSourceBalanceID,
		},
		{
			name:         "transaction.applied",
			eventType:    "transaction.applied",
			payload:      outboxSampleTransaction(StatusApplied),
			topic:        "blnk.transactions",
			aggregateID:  outboxTransactionID,
			partitionKey: outboxSourceBalanceID,
		},
		{
			name:         "transaction.scheduled",
			eventType:    "transaction.scheduled",
			payload:      outboxSampleTransaction(StatusScheduled),
			topic:        "blnk.transactions",
			aggregateID:  outboxTransactionID,
			partitionKey: outboxSourceBalanceID,
		},
		{
			name:         "transaction.inflight",
			eventType:    "transaction.inflight",
			payload:      outboxSampleTransaction(StatusInflight),
			topic:        "blnk.transactions",
			aggregateID:  outboxTransactionID,
			partitionKey: outboxSourceBalanceID,
		},
		{
			name:         "transaction.void",
			eventType:    "transaction.void",
			payload:      outboxSampleTransaction(StatusVoid),
			topic:        "blnk.transactions",
			aggregateID:  outboxTransactionID,
			partitionKey: outboxSourceBalanceID,
		},
		{
			// The worker's rejection handler passes the transaction BY VALUE, which is
			// the only producer that does. The value arm of the derivation type switch
			// exists for this call site alone.
			name:         "transaction.rejected (payload by value)",
			eventType:    "transaction.rejected",
			payload:      outboxSampleTransactionValue(StatusRejected),
			topic:        "blnk.transactions",
			aggregateID:  outboxTransactionID,
			partitionKey: outboxSourceBalanceID,
		},
		{
			// transaction.unknown is reachable, not hypothetical: getEventFromStatus has
			// no case for the COMMIT status, so a committed inflight transaction falls
			// through to it. That is pre-existing behaviour, preserved deliberately so
			// the dual-delivery payload comparison is not disturbed by an unrelated
			// change to the event vocabulary.
			name:         "transaction.unknown (COMMIT falls through)",
			eventType:    "transaction.unknown",
			payload:      outboxSampleTransaction(StatusCommit),
			topic:        "blnk.transactions",
			aggregateID:  outboxTransactionID,
			partitionKey: outboxSourceBalanceID,
		},

		// The bulk transaction family, in all three shapes its producer can build. The
		// batch is the aggregate and the key, because a batch's progress events only
		// make sense read in order relative to one another.
		{
			name:         "bulk_transaction.applied (success, with transaction_count)",
			eventType:    "bulk_transaction.applied",
			payload:      outboxSampleBulkPayload("applied", "", 42),
			topic:        "blnk.transactions",
			aggregateID:  outboxBatchID,
			partitionKey: outboxBatchID,
		},
		{
			name:         "bulk_transaction.failed (with error)",
			eventType:    "bulk_transaction.failed",
			payload:      outboxSampleBulkPayload("failed", "insufficient funds on bln_outbox_source", 42),
			topic:        "blnk.transactions",
			aggregateID:  outboxBatchID,
			partitionKey: outboxBatchID,
		},
		{
			name:         "bulk_transaction.failed (empty error message)",
			eventType:    "bulk_transaction.failed",
			payload:      outboxSampleBulkPayload("failed", "", 42),
			topic:        "blnk.transactions",
			aggregateID:  outboxBatchID,
			partitionKey: outboxBatchID,
		},

		// The two balance events. Both route to the balances topic, but they key
		// differently: a balance belongs to a ledger, whereas a monitor carries no
		// ledger field at all and keys on the balance it watches.
		{
			name:         "balance.created",
			eventType:    "balance.created",
			payload:      outboxSampleBalance(),
			topic:        "blnk.balances",
			aggregateID:  outboxSourceBalanceID,
			partitionKey: outboxLedgerID,
		},
		{
			name:         "balance.monitor (payload by value)",
			eventType:    "balance.monitor",
			payload:      outboxSampleBalanceMonitor(),
			topic:        "blnk.balances",
			aggregateID:  outboxMonitorID,
			partitionKey: outboxSourceBalanceID,
		},

		// The identity event. An identity is its own aggregate.
		{
			name:         "identity.created",
			eventType:    "identity.created",
			payload:      outboxSampleIdentity(),
			topic:        "blnk.identities",
			aggregateID:  outboxIdentityID,
			partitionKey: outboxIdentityID,
		},

		// The two events that motivate the fourth category. Neither belongs to the
		// transactions, balances or identities topic, and the coverage requirement
		// forbids dropping them.
		{
			name:         "ledger.created",
			eventType:    "ledger.created",
			payload:      outboxSampleLedger(),
			topic:        "blnk.system",
			aggregateID:  outboxLedgerID,
			partitionKey: outboxLedgerID,
		},
		{
			// system.error has no aggregate of any kind, so both the aggregate id and
			// the partition key fall back to the event type. That gives the error
			// stream one partition and therefore a total order, which is what an error
			// consumer wants, and it keeps the key non-empty.
			name:         "system.error (no aggregate)",
			eventType:    "system.error",
			payload:      outboxSampleSystemErrorPayload(),
			topic:        "blnk.system",
			aggregateID:  "system.error",
			partitionKey: "system.error",
		},
	}
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

// outboxPublishingConfiguration returns a configuration in which event publishing is
// enabled by the Kafka transport alone.
//
// Only Kafka.Brokers is populated. The legacy webhook URL is deliberately left empty so
// that nothing in this file can accidentally depend on the legacy transport being
// configured, and no test can accidentally POST anywhere.
//
// Relay.MaxRetryAttempts is left unset on purpose: an unset value is what a
// configuration that never passed through the defaulting path carries, and the retry
// budget test asserts the default is applied at row construction rather than assumed.
func outboxPublishingConfiguration() *config.Configuration {
	return &config.Configuration{
		Kafka: config.KafkaConfig{Brokers: []string{"localhost:9092"}},
	}
}

// outboxStoreConfiguration publishes cnf to config.ConfigStore and restores whatever was
// there before once the test finishes.
//
// config.ConfigStore is a process-global atomic.Value, so leaking a Kafka-configured
// state out of one test produces confusing failures in unrelated ones. The restore is
// registered with t.Cleanup so it runs even when a test fails or calls t.Fatal.
//
// It writes to the store DIRECTLY rather than through config.MockConfig, matching the
// approach the sunset, topic and legacy webhook tests take. MockConfig runs
// validateAndAddDefaults, which refuses to store a configuration lacking a data-source
// and Redis DSN — the value would be silently dropped — and which would also apply the
// very defaults several tests here need to observe as absent. Storing directly is not a
// workaround: it reproduces exactly the situation the production code must survive.
func outboxStoreConfiguration(t *testing.T, cnf *config.Configuration) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		// Nothing was published before this test. Leaving this test's configuration in
		// place could influence later ones, so publish an empty configuration, which is
		// strictly closer to the original state.
		config.ConfigStore.Store(&config.Configuration{})
	})

	config.ConfigStore.Store(cnf)
}

// newOutboxBlnk builds the smallest Blnk that PrepareEventOutbox and publishEvent
// actually need: a cached configuration and a datasource. ds may be nil, which is a
// supported construction the no-datasource contract covers.
//
// The configuration is BOTH cached on the instance and published to the store, and both
// are necessary. The instance's copy is what (*Blnk).Config returns, and it supplies the
// broker list and the retry budget. The store's copy is what topic naming reads, because
// TopicPrefix resolves through the configuration seam rather than through any instance.
// Setting only one of the two would leave the row half-configured.
//
// The struct is built directly rather than through NewBlnk so that no Redis client,
// asynq client, queue, search client or hook manager is constructed: none of them is on
// the path under test, and none of them should be able to make this file's outcome
// depend on infrastructure.
func newOutboxBlnk(t *testing.T, cnf *config.Configuration, ds database.IDataSource) *Blnk {
	t.Helper()

	outboxStoreConfiguration(t, cnf)

	return &Blnk{config: cnf, datasource: ds}
}

// outboxSpyDatasource records which outbox insert was called, with which transaction and
// which row, and is the harness for every path-selection assertion below.
//
// # Why a spy rather than testify expectations
//
// The two insert methods are overridden so that the arguments — the *sql.Tx in particular —
// NEVER reach testify's recorded-call list. testify formats every recorded argument with
// fmt.Sprintf("%v") whenever it diffs a call, which reflects over the whole *sql.Tx
// including its unexported fields. database/sql concurrently mutates those fields from the
// goroutine it starts per transaction (awaitDone, which calls rollback when the
// transaction's context completes), so handing a live transaction to a testify mock is a
// genuine, detector-visible data race in any test built that way. Recording the pointer
// here and comparing it directly is race-free and, as a bonus, lets the assertions inspect
// the whole captured row rather than squeeze a verdict through a boolean matcher.
//
// The embedded *mocks.MockDataSource supplies the rest of database.IDataSource and carries
// NO expectations, which is itself an assertion: any other datasource method this path
// touched would panic rather than pass unnoticed.
type outboxSpyDatasource struct {
	*mocks.MockDataSource

	// guard protects every field below. The producers call PublishEvent from goroutines,
	// so the spy has to be safe for concurrent use for the concurrency tests to mean
	// anything.
	guard sync.Mutex
	// standaloneRows are the rows passed to InsertEventOutbox, in call order.
	standaloneRows []*model.EventOutbox
	// inTxRows are the rows passed to InsertEventOutboxInTx, in call order.
	inTxRows []*model.EventOutbox
	// inTxTransactions are the transactions passed alongside them, in the same order.
	inTxTransactions []*sql.Tx
	// insertErr is returned by both methods, so a persistence failure can be simulated.
	insertErr error
}

// newOutboxSpyDatasource returns a spy wired for use as the Blnk datasource.
func newOutboxSpyDatasource() *outboxSpyDatasource {
	return &outboxSpyDatasource{MockDataSource: new(mocks.MockDataSource)}
}

// InsertEventOutbox records a standalone insert.
func (s *outboxSpyDatasource) InsertEventOutbox(_ context.Context, e *model.EventOutbox) error {
	s.guard.Lock()
	defer s.guard.Unlock()

	s.standaloneRows = append(s.standaloneRows, e)

	return s.insertErr
}

// InsertEventOutboxInTx records an in-transaction insert, keeping the transaction it was
// given so its identity can be asserted.
func (s *outboxSpyDatasource) InsertEventOutboxInTx(_ context.Context, tx *sql.Tx, e *model.EventOutbox) error {
	s.guard.Lock()
	defer s.guard.Unlock()

	s.inTxTransactions = append(s.inTxTransactions, tx)
	s.inTxRows = append(s.inTxRows, e)

	return s.insertErr
}

// standalone returns a copy of the rows inserted outside any transaction.
func (s *outboxSpyDatasource) standalone() []*model.EventOutbox {
	s.guard.Lock()
	defer s.guard.Unlock()

	rows := make([]*model.EventOutbox, len(s.standaloneRows))
	copy(rows, s.standaloneRows)

	return rows
}

// inTx returns a copy of the rows inserted inside a transaction, with the transactions they
// were inserted with.
func (s *outboxSpyDatasource) inTx() ([]*model.EventOutbox, []*sql.Tx) {
	s.guard.Lock()
	defer s.guard.Unlock()

	rows := make([]*model.EventOutbox, len(s.inTxRows))
	copy(rows, s.inTxRows)
	transactions := make([]*sql.Tx, len(s.inTxTransactions))
	copy(transactions, s.inTxTransactions)

	return rows, transactions
}

// assertWroteNothing asserts neither insert path was taken. It is the mechanical form of
// "the event was not captured".
func (s *outboxSpyDatasource) assertWroteNothing(t *testing.T) {
	t.Helper()

	standalone := s.standalone()
	inTxRows, _ := s.inTx()

	assert.Empty(t, standalone, "no standalone insert may have been issued")
	assert.Empty(t, inTxRows, "no in-transaction insert may have been issued")
}

// newOutboxSQLDatasource returns a real database.Datasource over a go-sqlmock connection,
// together with the mock controller and the *sql.DB the caller needs to open a
// transaction.
//
// This is the harness for the two insert paths. It exercises the genuine repository
// implementation, so the statement that reaches the driver — and the bytes bound to it —
// are the real ones rather than a mock's idea of them.
//
// Cache is left nil because the outbox insert path does not touch it; a cache here would
// require a Redis client and would make this file depend on infrastructure it has no
// business needing.
func newOutboxSQLDatasource(t *testing.T) (database.IDataSource, *sql.DB, sqlmock.Sqlmock) {
	t.Helper()

	db, controller, err := sqlmock.New()
	require.NoError(t, err, "the stub database connection must open")

	t.Cleanup(func() {
		assert.NoError(t, controller.ExpectationsWereMet(),
			"every expected statement must have been issued, and no unexpected one")
		_ = db.Close()
	})

	return &database.Datasource{Conn: db}, db, controller
}

// outboxLegacyWebhookBody marshals event exactly as SendWebhook marshals it to build the
// HTTP body: json.Marshal of the WHOLE NewWebhook value, both keys.
//
// This function is the reference side of every byte-equality assertion in this file. It
// is one line on purpose — the moment it does anything cleverer than SendWebhook does,
// it stops being a reference.
func outboxLegacyWebhookBody(t *testing.T, event NewWebhook) []byte {
	t.Helper()

	body, err := json.Marshal(event)
	require.NoError(t, err, "the legacy webhook body must marshal")

	return body
}

// outboxTopLevelKeys returns the keys of a JSON object IN DOCUMENT ORDER.
//
// A map cannot be used for this: unmarshalling into one loses the very ordering the
// dual-delivery byte-equality guarantee depends on. Streaming the tokens keeps the order
// the bytes actually carry, which is what lets a test assert that the payload is
// {"event": ..., "data": ...} in that sequence and nothing else.
func outboxTopLevelKeys(t *testing.T, raw []byte) []string {
	t.Helper()

	decoder := json.NewDecoder(strings.NewReader(string(raw)))

	opening, err := decoder.Token()
	require.NoError(t, err, "the payload must begin with a JSON token")
	require.Equal(t, json.Delim('{'), opening, "the payload must be a JSON object")

	var keys []string
	for decoder.More() {
		token, tokenErr := decoder.Token()
		require.NoError(t, tokenErr, "each object key must be readable")

		key, ok := token.(string)
		require.True(t, ok, "each object key must be a string, got %T", token)
		keys = append(keys, key)

		// Consume the value. Decoding into json.RawMessage skips the whole value,
		// nested objects and arrays included, without interpreting it.
		var value json.RawMessage
		require.NoError(t, decoder.Decode(&value), "each object value must be readable")
	}

	closing, err := decoder.Token()
	require.NoError(t, err, "the payload must end with a JSON token")
	require.Equal(t, json.Delim('}'), closing, "the payload object must be closed")

	return keys
}

// outboxDataObject returns the raw bytes of the payload's inner "data" value, without
// interpreting them.
//
// json.RawMessage is used rather than interface{} so the inner object's own byte
// sequence — and therefore its key order — survives for comparison.
func outboxDataObject(t *testing.T, raw []byte) json.RawMessage {
	t.Helper()

	var envelope struct {
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(raw, &envelope), "the payload must be a webhook envelope")

	return envelope.Data
}

// outboxEnvelopePrefix is the exact byte prefix a stored payload must carry for the given
// event name: `{"event":"<name>","data":`.
//
// It is composed by marshalling the name rather than by string concatenation with quotes,
// so an event name needing JSON escaping still produces a correct expectation.
func outboxEnvelopePrefix(t *testing.T, eventName string) string {
	t.Helper()

	encoded, err := json.Marshal(eventName)
	require.NoError(t, err, "the event name must marshal")

	return `{"event":` + string(encoded) + `,"data":`
}

// ---------------------------------------------------------------------------
// The payload-preservation guarantee
// ---------------------------------------------------------------------------

// TestPrepareEventOutbox_PayloadIsByteIdenticalToTheLegacyWebhookBody is THE test this
// file exists for.
//
// For every one of the thirteen event types Blnk emits, it builds the NewWebhook a
// producer builds, marshals it exactly as SendWebhook marshals the HTTP body, and asserts
// that the bytes PrepareEventOutbox put in the payload column are the same bytes.
//
// The comparison is on []byte and on its string form, never on an unmarshalled map. A map
// comparison would ignore key order, and key order is exactly what the dual-delivery
// equality criterion turns on: the relay publishes the Kafka message and enqueues the
// legacy webhook task from this one stored row, so if these bytes were merely
// "equivalent" rather than identical, the guarantee would already be broken here.
func TestPrepareEventOutbox_PayloadIsByteIdenticalToTheLegacyWebhookBody(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
			event := NewWebhook{Event: fixture.eventType, Payload: fixture.payload}

			// The reference: the body the legacy HTTP transport would have POSTed.
			expected := outboxLegacyWebhookBody(t, event)

			row := blnk.PrepareEventOutbox(context.Background(), event)
			require.NotNil(t, row, "a configured deployment must capture %s", fixture.eventType)

			assert.Equal(t, expected, []byte(row.Payload),
				"the stored payload must be the legacy webhook body byte for byte")
			assert.Equal(t, string(expected), string(row.Payload),
				"byte equality restated as text, so a failure is readable")
			assert.Len(t, []byte(row.Payload), len(expected),
				"the stored payload must be neither truncated nor padded")
		})
	}
}

// TestPrepareEventOutbox_PayloadIsTheTwoKeyWebhookEnvelope pins the resolution of "the
// payload matches today's webhook body field-for-field" to its strongest reading: the
// WHOLE two-key object travels, both keys, in the order the legacy body carries them.
//
// Carrying only the inner "data" object would force every existing subscriber to rewrite
// its body parser, which is the opposite of preserving the payload. This test is what
// makes that mistake impossible to land quietly: it asserts the key set, the key order,
// the event name at the outer level and the inner object's own bytes.
func TestPrepareEventOutbox_PayloadIsTheTwoKeyWebhookEnvelope(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
			event := NewWebhook{Event: fixture.eventType, Payload: fixture.payload}

			row := blnk.PrepareEventOutbox(context.Background(), event)
			require.NotNil(t, row)

			assert.Equal(t, []string{"event", "data"}, outboxTopLevelKeys(t, []byte(row.Payload)),
				"the payload must be the two-key webhook envelope, event first, and nothing else")
			assert.True(t, strings.HasPrefix(string(row.Payload), outboxEnvelopePrefix(t, fixture.eventType)),
				"the payload must open with %s, got %s",
				outboxEnvelopePrefix(t, fixture.eventType), string(row.Payload))

			// The inner object must be the producer's payload verbatim, so a subscriber
			// reading only "data" sees precisely what the HTTP body carried there.
			expectedData, err := json.Marshal(fixture.payload)
			require.NoError(t, err)
			assert.Equal(t, string(expectedData), string(outboxDataObject(t, []byte(row.Payload))),
				`the "data" value must be the producer's payload object, unaltered`)
		})
	}
}

// TestPrepareEventOutbox_LedgerCreatedPayloadIsExactlyTheDocumentedObject is the
// hand-written cross-check.
//
// Every other payload assertion in this file compares against json.Marshal of the same
// value, which proves the two agree but not what either one looks like. This test writes
// the expected bytes out in full for one event, so the file also states — in literal,
// reviewable form — that a stored payload really is
// {"event":"ledger.created","data":{...}} and not some reshaped variant that happens to
// match a reshaped expectation.
func TestPrepareEventOutbox_LedgerCreatedPayloadIsExactlyTheDocumentedObject(t *testing.T) {
	const expected = `{"event":"ledger.created","data":{"ledger_id":"ldg_outbox_fixture",` +
		`"name":"Outbox Fixture Ledger","created_at":"2026-03-14T15:09:26.535897932Z",` +
		`"meta_data":{"region":"eu-west-1"}}}`

	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
		Event:   "ledger.created",
		Payload: outboxSampleLedger(),
	})
	require.NotNil(t, row)

	assert.Equal(t, expected, string(row.Payload),
		"the stored payload must be exactly the documented two-key object")
}

// TestPrepareEventOutbox_BulkTransactionPayloadShapes covers all three shapes the
// bulk_transaction family's payload can take, because the producer builds it
// conditionally and each branch is a different set of keys on the wire.
//
// The key sets are asserted explicitly rather than inferred. A regression that always
// included "error", or that dropped "transaction_count", would still round-trip through
// json.Marshal and would still be byte-equal to a comparison built from the same faulty
// map — only an independent statement of the expected keys catches it.
func TestPrepareEventOutbox_BulkTransactionPayloadShapes(t *testing.T) {
	const failureReason = "insufficient funds on bln_outbox_source"

	shapes := []struct {
		name      string
		eventType string
		payload   map[string]interface{}
		// dataKeys is the expected key set of the inner object, in the order a marshalled
		// Go map produces: sorted.
		dataKeys []string
	}{
		{
			name:      "success carries transaction_count and no error",
			eventType: "bulk_transaction.applied",
			payload:   outboxSampleBulkPayload("applied", "", 42),
			dataKeys:  []string{"batch_id", "status", "timestamp", "transaction_count"},
		},
		{
			name:      "failure with a reason carries error and no transaction_count",
			eventType: "bulk_transaction.failed",
			payload:   outboxSampleBulkPayload("failed", failureReason, 42),
			dataKeys:  []string{"batch_id", "error", "status", "timestamp"},
		},
		{
			name:      "failure with an empty reason carries neither",
			eventType: "bulk_transaction.failed",
			payload:   outboxSampleBulkPayload("failed", "", 42),
			dataKeys:  []string{"batch_id", "status", "timestamp"},
		},
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
			event := NewWebhook{Event: shape.eventType, Payload: shape.payload}

			row := blnk.PrepareEventOutbox(context.Background(), event)
			require.NotNil(t, row)

			assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(row.Payload),
				"the stored payload must be the legacy webhook body byte for byte")
			assert.Equal(t, shape.dataKeys, outboxTopLevelKeys(t, outboxDataObject(t, []byte(row.Payload))),
				"the inner object must carry exactly the keys this shape defines")

			// The batch is the aggregate for every shape, so a batch's progress events
			// stay grouped and ordered however the batch ended.
			assert.Equal(t, outboxBatchID, row.AggregateID)
			assert.Equal(t, outboxBatchID, row.LedgerID)
			assert.Equal(t, "blnk.transactions", row.Topic)
		})
	}
}

// TestPrepareEventOutbox_CoversEveryEmittedEventType is the completeness guard on the
// catalogue above.
//
// The coverage requirement is absolute — every event type that reached the legacy webhook
// sender must reach Kafka — so a fixture table that had quietly lost a row would leave a
// real event type unproven while every remaining test still passed. Comparing the table
// against an independently written vocabulary of thirteen keys is what makes adding a
// fourteenth event type without a fixture a test failure rather than a silent gap.
func TestPrepareEventOutbox_CoversEveryEmittedEventType(t *testing.T) {
	require.Len(t, outboxEventVocabulary, outboxEventCatalogueSize,
		"the hand-written vocabulary must name all thirteen event types")

	covered := make(map[string]int, outboxEventCatalogueSize)
	for _, fixture := range outboxEventFixtures() {
		covered[outboxVocabularyKey(fixture.eventType)]++
	}

	for _, eventType := range outboxEventVocabulary {
		assert.Positive(t, covered[eventType],
			"event type %q must have a fixture: every emitted event type is published, with zero exceptions",
			eventType)
	}

	assert.Len(t, covered, outboxEventCatalogueSize,
		"the fixtures must cover the thirteen event types and introduce no unknown one")

	// The bulk family is the only entry represented by more than one fixture, because it
	// is the only one whose payload shape varies.
	assert.Equal(t, 3, covered[outboxBulkEventPrefix],
		"the bulk transaction family must be represented by all three of its payload shapes")
}

// ---------------------------------------------------------------------------
// The canonical envelope fields
// ---------------------------------------------------------------------------

// outboxUUIDSampleSize is how many rows the identifier test builds.
//
// event_id doubles as the subscriber idempotency key, so a collision is not a cosmetic
// defect — it would make a duplicate look like a redelivery of a different event, or make
// a genuinely new event look like a duplicate and be discarded. A few hundred draws will
// not prove a generator sound on its own, but combined with parsing every value it does
// catch the failure modes that matter in practice: a constant, a counter reset, a value
// derived from the payload, or a truncated string.
const outboxUUIDSampleSize = 512

// TestPrepareEventOutbox_EventIDIsAUniqueUUID asserts the identifier is a real,
// canonically formatted UUID and that it is fresh on every call.
func TestPrepareEventOutbox_EventIDIsAUniqueUUID(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
	event := NewWebhook{Event: "ledger.created", Payload: outboxSampleLedger()}

	seen := make(map[string]struct{}, outboxUUIDSampleSize)
	for i := 0; i < outboxUUIDSampleSize; i++ {
		row := blnk.PrepareEventOutbox(context.Background(), event)
		require.NotNil(t, row)

		parsed, err := uuid.Parse(row.EventID)
		require.NoError(t, err, "event_id must be a valid UUID, got %q", row.EventID)
		assert.Equal(t, row.EventID, parsed.String(),
			"event_id must be in the canonical lower-case hyphenated form")
		assert.Len(t, row.EventID, 36, "a canonical UUID is 36 characters")

		_, duplicate := seen[row.EventID]
		require.False(t, duplicate,
			"event_id must be unique: it is the subscriber idempotency key (collision at draw %d)", i)
		seen[row.EventID] = struct{}{}
	}

	assert.Len(t, seen, outboxUUIDSampleSize, "every draw must have produced a distinct event_id")
}

// TestPrepareEventOutbox_EventTypeMirrorsTheInnerEventName asserts the event name is
// hoisted to the envelope exactly.
//
// The duplication between the envelope and the payload is deliberate: it lets subscribers,
// the relay and SQL-side triage route and filter without parsing the payload at all. It is
// only useful if the two agree, which is what this asserts.
func TestPrepareEventOutbox_EventTypeMirrorsTheInnerEventName(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
				Event:   fixture.eventType,
				Payload: fixture.payload,
			})
			require.NotNil(t, row)

			assert.Equal(t, fixture.eventType, row.EventType,
				"event_type must be the event string the producer passed")

			var envelope struct {
				Event string `json:"event"`
			}
			require.NoError(t, json.Unmarshal(row.Payload, &envelope))
			assert.Equal(t, row.EventType, envelope.Event,
				"the envelope's event_type and the payload's event name must agree")
		})
	}
}

// TestPrepareEventOutbox_EventTypeIsTrimmedWhileThePayloadStaysVerbatim pins the one place
// where the envelope and the payload legitimately differ.
//
// The envelope trims the event name so that routing, filtering and topic resolution are
// not defeated by a stray space. The payload is NOT re-derived from the trimmed value —
// it is the marshalled NewWebhook exactly as the producer handed it over — because the
// payload's whole contract is that it equals the byte sequence the legacy transport would
// have sent, spacing included.
func TestPrepareEventOutbox_EventTypeIsTrimmedWhileThePayloadStaysVerbatim(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
	event := NewWebhook{Event: "  ledger.created\t", Payload: outboxSampleLedger()}

	row := blnk.PrepareEventOutbox(context.Background(), event)
	require.NotNil(t, row)

	assert.Equal(t, "ledger.created", row.EventType, "event_type must be trimmed")
	assert.Equal(t, "blnk.system", row.Topic, "the trimmed name must be what routing sees")
	assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(row.Payload),
		"the payload must remain the legacy body byte for byte, untrimmed")
	assert.Contains(t, string(row.Payload), `"event":"  ledger.created\t"`,
		"the payload must carry the producer's exact event string")
}

// TestPrepareEventOutbox_SchemaVersionIsV1 asserts the envelope version is the integer 1.
//
// The value reaches subscribers on the wire, where it is what a consumer branches on to
// decide whether it understands the envelope shape. Asserting the exact integer — not
// merely that it is non-zero — is what makes an accidental bump, or a zero left by a
// missing assignment, a test failure.
func TestPrepareEventOutbox_SchemaVersionIsV1(t *testing.T) {
	require.Equal(t, 1, model.SchemaVersionV1,
		"the initial schema version is 1; a change here is a subscriber-facing contract change")

	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
				Event:   fixture.eventType,
				Payload: fixture.payload,
			})
			require.NotNil(t, row)

			assert.Equal(t, model.SchemaVersionV1, row.SchemaVersion)
			assert.Equal(t, 1, row.SchemaVersion)
		})
	}
}

// TestPrepareEventOutbox_OccurredAtIsUTCAndRoundTripsThroughRFC3339 asserts the timestamp
// is present, is the current instant in UTC, and survives the wire without loss.
//
// occurred_at is not decoration: the relay claims rows in ascending occurred_at order, so
// it is half of the ordering guarantee — the message key picks the partition, and this
// picks the sequence within it. UTC matters because the value is rendered as RFC3339 on
// the wire and must be unambiguous wherever the process runs.
//
// The round-trip is asserted at RFC3339Nano rendering, which is what time.Time's own JSON
// encoding produces. Rendering at second precision would silently discard the fractional
// part, and two events in the same second would then be indistinguishable to a consumer
// ordering by this field.
func TestPrepareEventOutbox_OccurredAtIsUTCAndRoundTripsThroughRFC3339(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	before := time.Now().UTC().Add(-time.Second)
	row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
		Event:   "transaction.applied",
		Payload: outboxSampleTransaction(StatusApplied),
	})
	require.NotNil(t, row)
	after := time.Now().UTC().Add(time.Second)

	require.False(t, row.OccurredAt.IsZero(), "occurred_at must be set at construction")
	assert.False(t, row.OccurredAt.Before(before), "occurred_at must not predate the call")
	assert.False(t, row.OccurredAt.After(after), "occurred_at must not postdate the call")
	assert.Equal(t, time.UTC, row.OccurredAt.Location(),
		"occurred_at must be UTC so its RFC3339 rendering is unambiguous")

	rendered := row.OccurredAt.Format(time.RFC3339Nano)
	parsed, err := time.Parse(time.RFC3339, rendered)
	require.NoError(t, err, "the rendered instant must be RFC3339-parseable")
	assert.True(t, parsed.Equal(row.OccurredAt),
		"the RFC3339 round-trip must be lossless: %s parsed back as %s", rendered, parsed)
	assert.Equal(t, rendered, parsed.Format(time.RFC3339Nano),
		"re-rendering the parsed instant must reproduce the same text")

	// The same round-trip through the encoding the wire actually uses.
	encoded, err := json.Marshal(row.OccurredAt)
	require.NoError(t, err)
	var decoded time.Time
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.True(t, decoded.Equal(row.OccurredAt),
		"occurred_at must survive JSON encoding without loss")
	assert.Equal(t, `"`+rendered+`"`, string(encoded),
		"time.Time's JSON encoding is RFC3339 with the fractional part preserved")
}

// TestPrepareEventOutbox_TopicIsResolvedFromTheEventType asserts the destination topic
// recorded on the row, for every one of the thirteen event types.
//
// Two assertions are made per fixture, and both are needed. The literal expectation is an
// independent statement of the routing contract, so a renamed category or a changed
// separator fails here. The equality with TopicForEvent proves the row is resolved through
// the same routing function the rest of the pipeline uses, so the two can never disagree.
func TestPrepareEventOutbox_TopicIsResolvedFromTheEventType(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
				Event:   fixture.eventType,
				Payload: fixture.payload,
			})
			require.NotNil(t, row)

			assert.Equal(t, fixture.topic, row.Topic,
				"%s must be recorded against %s", fixture.eventType, fixture.topic)
			assert.Equal(t, TopicForEvent(fixture.eventType), row.Topic,
				"the row's topic must be the one the routing function resolves")
			assert.NotEmpty(t, row.Topic, "a row with no topic could never be published")
		})
	}
}

// TestPrepareEventOutbox_TopicIsResolvedOnceAtConstruction asserts the topic follows the
// configured namespace prefix and is FROZEN on the row.
//
// Recording the resolved name rather than re-deriving it at publish time is what keeps a
// stored row replayable to its original destination: a dead-lettered event written before
// KAFKA_TOPIC_PREFIX changed must still be replayable to the topic it was meant for, not
// to a newly-named one no consumer has subscribed to yet.
func TestPrepareEventOutbox_TopicIsResolvedOnceAtConstruction(t *testing.T) {
	configuration := outboxPublishingConfiguration()
	configuration.Kafka.TopicPrefix = "acme"
	blnk := newOutboxBlnk(t, configuration, nil)

	row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
		Event:   "balance.created",
		Payload: outboxSampleBalance(),
	})
	require.NotNil(t, row)
	assert.Equal(t, "acme.balances", row.Topic, "the configured prefix must name the topic")

	// Re-publish a different prefix. The row already built must not follow it.
	config.ConfigStore.Store(&config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:     []string{"localhost:9092"},
			TopicPrefix: "renamed",
		},
	})
	assert.Equal(t, "acme.balances", row.Topic,
		"the recorded topic must not change when the configuration does")
}

// TestPrepareEventOutbox_MaxAttemptsFollowsTheRelayConfiguration asserts the retry budget
// stamped on the row.
//
// The budget is stamped rather than read by the relay at publish time so that a
// configuration change never retroactively alters rows already in flight, and so an
// operator can extend the budget for one stuck event without restarting anything.
//
// A non-positive configured value is REPLACED, not honoured. Zero would mean "never
// attempt", which strands the row: the relay would have no attempts to spend, so the event
// would neither publish nor dead-letter, and nothing would surface the fact.
func TestPrepareEventOutbox_MaxAttemptsFollowsTheRelayConfiguration(t *testing.T) {
	budgets := []struct {
		name       string
		configured int
		expected   int
	}{
		{name: "unset falls back to the default of five", configured: 0, expected: 5},
		{name: "a configured budget is honoured", configured: 9, expected: 9},
		{name: "a single attempt is honoured", configured: 1, expected: 1},
		{name: "a negative budget falls back to the default", configured: -3, expected: 5},
	}

	for _, budget := range budgets {
		t.Run(budget.name, func(t *testing.T) {
			configuration := outboxPublishingConfiguration()
			configuration.Relay.MaxRetryAttempts = budget.configured
			blnk := newOutboxBlnk(t, configuration, nil)

			row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
				Event:   "identity.created",
				Payload: outboxSampleIdentity(),
			})
			require.NotNil(t, row)

			assert.Equal(t, budget.expected, row.MaxAttempts)
			assert.Positive(t, row.MaxAttempts,
				"a non-positive budget would strand the row: neither published nor dead-lettered")
		})
	}

	assert.Equal(t, 5, defaultEventMaxAttempts,
		"the default retry budget is five, agreeing with RELAY_MAX_RETRY_ATTEMPTS and the column default")
}

// TestPrepareEventOutbox_StatusIsPendingAndTheRelayStateIsFresh asserts the row enters the
// relay state machine at its start.
//
// Status is set explicitly at construction even though the repository layer and the column
// default would both supply it, so the in-memory row the caller holds agrees with the row
// that lands in the table. The relay's claim query selects on this exact value, so a row
// carrying anything else would simply never be picked up.
func TestPrepareEventOutbox_StatusIsPendingAndTheRelayStateIsFresh(t *testing.T) {
	require.Equal(t, "pending", model.EventOutboxStatusPending,
		"the pending status is the literal the claim query selects on")

	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
				Event:   fixture.eventType,
				Payload: fixture.payload,
			})
			require.NotNil(t, row)

			assert.Equal(t, model.EventOutboxStatusPending, row.Status)
			assert.Equal(t, "pending", row.Status)
			assert.Zero(t, row.Attempts, "a freshly built row has made no attempt")
			assert.Empty(t, row.LastError, "a freshly built row has no failure recorded")
			assert.False(t, row.WebhookDispatched,
				"the dual-delivery marker is set by the relay, never at construction")
			assert.Zero(t, row.ID, "the surrogate key is assigned by the database, not here")
			assert.Nil(t, row.DispatchedAt)
			assert.Nil(t, row.FirstAttemptedAt)
			assert.Nil(t, row.LastAttemptedAt)
			assert.Nil(t, row.LockedUntil)
			assert.Empty(t, row.DLTTopic, "a row is not dead-lettered at construction")
			assert.Empty(t, row.FailureMetadata)
		})
	}
}

// ---------------------------------------------------------------------------
// The partition key: ledger_id, and therefore ordering
// ---------------------------------------------------------------------------

// TestPrepareEventOutbox_LedgerIDIsTheDocumentedPartitionKey asserts the derived key for
// every payload type, against a literal expectation.
//
// This is the highest-consequence value the file under test computes. The key is hashed by
// a stable balancer to pick a partition, so every event sharing a key lands on one
// partition and is consumed in publish order, while events with different keys carry no
// ordering relationship whatsoever. Per-aggregate ordering is therefore a property of THIS
// derivation, not of the broker and not of the relay.
//
// The expectations are literals rather than calls into the derivation, so a reordered key
// preference — source before destination, say, or ledger before balance — fails here
// instead of silently moving a partition assignment.
func TestPrepareEventOutbox_LedgerIDIsTheDocumentedPartitionKey(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
				Event:   fixture.eventType,
				Payload: fixture.payload,
			})
			require.NotNil(t, row)

			assert.Equal(t, fixture.partitionKey, row.LedgerID,
				"%s must be keyed on %s", fixture.eventType, fixture.partitionKey)
			assert.NotEmpty(t, row.LedgerID,
				"an empty key lets Kafka scatter the event round-robin and destroys ordering silently")
		})
	}
}

// TestPrepareEventOutbox_AggregateIDIsTheEventSubject asserts the aggregate identifier for
// every payload type.
//
// aggregate_id is deliberately distinct from the partition key: the key controls
// partitioning and therefore ordering, whereas this names the entity the event is ABOUT
// and is what a consumer groups by. For some event types they coincide — a ledger event is
// keyed on and about the same ledger — and for others they differ, most visibly the
// balance monitor, which is ABOUT the monitor that fired but keyed on the balance it
// watches so that every alert for that balance stays ordered.
func TestPrepareEventOutbox_AggregateIDIsTheEventSubject(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
				Event:   fixture.eventType,
				Payload: fixture.payload,
			})
			require.NotNil(t, row)

			assert.Equal(t, fixture.aggregateID, row.AggregateID,
				"%s must be recorded against aggregate %s", fixture.eventType, fixture.aggregateID)
			assert.NotEmpty(t, row.AggregateID,
				"aggregate_id is NOT NULL in the schema and is what consumers group by")
		})
	}

	// The one fixture where subject and key legitimately differ, called out explicitly so
	// a change that collapsed the two concepts into one cannot pass unnoticed.
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
	monitorRow := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
		Event:   "balance.monitor",
		Payload: outboxSampleBalanceMonitor(),
	})
	require.NotNil(t, monitorRow)
	assert.Equal(t, outboxMonitorID, monitorRow.AggregateID, "the monitor is the subject")
	assert.Equal(t, outboxSourceBalanceID, monitorRow.LedgerID, "the watched balance is the key")
	assert.NotEqual(t, monitorRow.AggregateID, monitorRow.LedgerID,
		"subject and partition key are different concepts and must not be conflated")
}

// TestPrepareEventOutbox_LedgerIDFallbackChain pins every step of the documented fallback,
// in order.
//
// The chain exists for exactly one reason: the key must ALWAYS be present and always
// deterministic. Kafka treats a null or empty key as "any partition", so an unkeyed event
// is scattered round-robin and its ordering relative to its siblings is lost with nothing
// in the data to show that it happened. Each case below is a real payload shape a producer
// can present, and each one must still yield a usable key.
func TestPrepareEventOutbox_LedgerIDFallbackChain(t *testing.T) {
	fallbacks := []struct {
		name        string
		eventType   string
		payload     interface{}
		key         string
		aggregateID string
	}{
		{
			// Step 1 of the transaction preference: the source balance, which is what
			// the transaction queue itself shards on, so Kafka partitioning and queue
			// sharding agree.
			name:        "a transaction keys on its source balance",
			eventType:   "transaction.applied",
			payload:     outboxSampleTransaction(StatusApplied),
			key:         outboxSourceBalanceID,
			aggregateID: outboxTransactionID,
		},
		{
			// Step 2: a credit-only transaction has no source.
			name:      "a credit-only transaction keys on its destination balance",
			eventType: "transaction.applied",
			payload: func() *model.Transaction {
				transaction := outboxSampleTransaction(StatusApplied)
				transaction.Source = ""

				return transaction
			}(),
			key:         outboxDestinationBalanceID,
			aggregateID: outboxTransactionID,
		},
		{
			// Step 3: a multi-source parent whose own source and destination are unset.
			name:      "a transaction with neither balance keys on itself",
			eventType: "transaction.applied",
			payload: func() *model.Transaction {
				transaction := outboxSampleTransaction(StatusApplied)
				transaction.Source = ""
				transaction.Destination = ""

				return transaction
			}(),
			key:         outboxTransactionID,
			aggregateID: outboxTransactionID,
		},
		{
			// Whitespace-only identifiers are treated as absent. They must be, because
			// " " and "" hash to different partitions, so honouring a stray space would
			// split one aggregate's events across two partitions.
			name:      "whitespace-only identifiers are treated as absent",
			eventType: "transaction.applied",
			payload: func() *model.Transaction {
				transaction := outboxSampleTransaction(StatusApplied)
				transaction.Source = "   "
				transaction.Destination = "\t"

				return transaction
			}(),
			key:         outboxTransactionID,
			aggregateID: outboxTransactionID,
		},
		{
			// A balance whose ledger is not populated on the payload.
			name:      "a balance with no ledger keys on itself",
			eventType: "balance.created",
			payload: func() *model.Balance {
				balance := outboxSampleBalance()
				balance.LedgerID = ""

				return balance
			}(),
			key:         outboxSourceBalanceID,
			aggregateID: outboxSourceBalanceID,
		},
		{
			// BalanceMonitor carries no ledger field at all, and the balance it watches
			// is the closest stable aggregate. With that gone, the monitor keys on
			// itself.
			name:      "a monitor with no balance keys on itself",
			eventType: "balance.monitor",
			payload: func() model.BalanceMonitor {
				monitor := outboxSampleBalanceMonitor()
				monitor.BalanceID = ""

				return monitor
			}(),
			key:         outboxMonitorID,
			aggregateID: outboxMonitorID,
		},
		{
			// system.error has neither an aggregate nor a ledger, so the key falls back
			// to the event type. That gives the error stream a single partition and
			// therefore a total order, which is what an error consumer wants.
			name:        "system.error falls back to its event type",
			eventType:   "system.error",
			payload:     outboxSampleSystemErrorPayload(),
			key:         "system.error",
			aggregateID: "system.error",
		},
		{
			// A bulk event whose batch id is missing: the map arm finds nothing, so the
			// event type carries it.
			name:      "a bulk event with no batch_id falls back to its event type",
			eventType: "bulk_transaction.failed",
			payload: map[string]interface{}{
				"status":    "failed",
				"timestamp": outboxFixedInstant,
			},
			key:         "bulk_transaction.failed",
			aggregateID: "bulk_transaction.failed",
		},
		{
			// A batch id of the wrong type is not stringified into nonsense such as
			// "%!s(int=7)"; it is treated as absent.
			name:      "a non-string batch_id is treated as absent",
			eventType: "bulk_transaction.applied",
			payload: map[string]interface{}{
				"batch_id":  7,
				"status":    "applied",
				"timestamp": outboxFixedInstant,
			},
			key:         "bulk_transaction.applied",
			aggregateID: "bulk_transaction.applied",
		},
		{
			// A typed nil pointer marshals to "null", so there is nothing to derive
			// from. It must not panic, and the event must still be keyed.
			name:        "a nil transaction pointer falls back to its event type",
			eventType:   "transaction.void",
			payload:     (*model.Transaction)(nil),
			key:         "transaction.void",
			aggregateID: "transaction.void",
		},
		{
			name:        "a nil balance pointer falls back to its event type",
			eventType:   "balance.created",
			payload:     (*model.Balance)(nil),
			key:         "balance.created",
			aggregateID: "balance.created",
		},
		{
			name:        "a nil ledger pointer falls back to its event type",
			eventType:   "ledger.created",
			payload:     (*model.Ledger)(nil),
			key:         "ledger.created",
			aggregateID: "ledger.created",
		},
		{
			name:        "a nil identity pointer falls back to its event type",
			eventType:   "identity.created",
			payload:     (*model.Identity)(nil),
			key:         "identity.created",
			aggregateID: "identity.created",
		},
		{
			name:        "a nil monitor pointer falls back to its event type",
			eventType:   "balance.monitor",
			payload:     (*model.BalanceMonitor)(nil),
			key:         "balance.monitor",
			aggregateID: "balance.monitor",
		},
		{
			// An absent payload of any other shape. The event type is still a
			// deterministic key, so these events stay ordered amongst themselves.
			name:        "an unrecognised payload falls back to its event type",
			eventType:   "ledger.created",
			payload:     nil,
			key:         "ledger.created",
			aggregateID: "ledger.created",
		},
	}

	for _, fallback := range fallbacks {
		t.Run(fallback.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			var row *model.EventOutbox
			require.NotPanics(t, func() {
				row = blnk.PrepareEventOutbox(context.Background(), NewWebhook{
					Event:   fallback.eventType,
					Payload: fallback.payload,
				})
			}, "no payload shape may panic the derivation: it runs on the ledger write path")
			require.NotNil(t, row)

			assert.Equal(t, fallback.key, row.LedgerID)
			assert.Equal(t, fallback.aggregateID, row.AggregateID)
			assert.NotEmpty(t, row.LedgerID, "the key must never be empty")
			assert.NotEmpty(t, row.AggregateID, "aggregate_id is NOT NULL in the schema")
		})
	}
}

// TestPrepareEventOutbox_UnkeyableEventGetsTheSentinelKey covers the terminal fallback.
//
// It is reached only by a zero-valued NewWebhook — no aggregate of any kind AND no event
// type to fall back to. Routing those to one fixed sentinel keeps them ordered amongst
// themselves rather than scattered, and the distinctive value makes them trivial to find
// in the table with `WHERE ledger_id = 'blnk.unkeyed'`.
func TestPrepareEventOutbox_UnkeyableEventGetsTheSentinelKey(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	var row *model.EventOutbox
	require.NotPanics(t, func() {
		row = blnk.PrepareEventOutbox(context.Background(), NewWebhook{})
	})
	require.NotNil(t, row, "even a zero-valued event is captured rather than dropped")

	assert.Equal(t, unkeyedEventPartitionKey, row.LedgerID)
	assert.Equal(t, "blnk.unkeyed", row.LedgerID,
		"the sentinel is a literal an operator can search the outbox for")
	assert.Equal(t, unkeyedEventPartitionKey, row.AggregateID,
		"aggregate_id inherits the key once every payload-derived candidate is exhausted")
	assert.Empty(t, row.EventType, "the event type really is empty; nothing was invented")
	assert.Equal(t, "blnk.system", row.Topic,
		"an unrecognised event type is routed to the catch-all rather than stranded")
	assert.Equal(t, `{"event":"","data":null}`, string(row.Payload),
		"the payload is still the two-key envelope, faithfully describing an empty event")
}

// TestPrepareEventOutbox_LedgerIDIsStableWithinAnAggregate is the ordering guarantee
// expressed as a property rather than as a table.
//
// Two things have to hold for per-aggregate ordering to survive: every event belonging to
// one aggregate must derive the SAME key, so they share a partition; and events belonging
// to different aggregates must derive DIFFERENT keys, so one busy aggregate cannot
// serialise the whole topic behind it.
func TestPrepareEventOutbox_LedgerIDIsStableWithinAnAggregate(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	keyFor := func(eventType string, payload interface{}) string {
		row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
			Event:   eventType,
			Payload: payload,
		})
		require.NotNil(t, row)

		return row.LedgerID
	}

	t.Run("one transaction's whole lifecycle shares a key", func(t *testing.T) {
		// queued, then inflight, then applied for one transaction: three different event
		// types, one aggregate, and therefore one partition. Source is stable across the
		// lifecycle, which is what makes this true.
		queued := keyFor("transaction.queued", outboxSampleTransaction(StatusQueued))
		inflight := keyFor("transaction.inflight", outboxSampleTransaction(StatusInflight))
		applied := keyFor("transaction.applied", outboxSampleTransaction(StatusApplied))

		assert.Equal(t, queued, inflight, "a transaction's events must never be reorderable")
		assert.Equal(t, inflight, applied, "a transaction's events must never be reorderable")
		assert.Equal(t, outboxSourceBalanceID, queued)
	})

	t.Run("repeated derivation is deterministic", func(t *testing.T) {
		first := keyFor("balance.created", outboxSampleBalance())
		second := keyFor("balance.created", outboxSampleBalance())

		assert.Equal(t, first, second,
			"the same payload must always derive the same key, or ordering is a coin toss")
	})

	t.Run("different aggregates derive different keys", func(t *testing.T) {
		otherTransaction := outboxSampleTransaction(StatusApplied)
		otherTransaction.Source = "bln_outbox_other_source"

		assert.NotEqual(t,
			keyFor("transaction.applied", outboxSampleTransaction(StatusApplied)),
			keyFor("transaction.applied", otherTransaction),
			"transactions on different source balances must be free to progress independently")

		otherLedger := outboxSampleLedger()
		otherLedger.LedgerID = "ldg_outbox_other"

		assert.NotEqual(t,
			keyFor("ledger.created", outboxSampleLedger()),
			keyFor("ledger.created", otherLedger),
			"different ledgers must derive different keys")

		otherIdentity := outboxSampleIdentity()
		otherIdentity.IdentityID = "idt_outbox_other"

		assert.NotEqual(t,
			keyFor("identity.created", outboxSampleIdentity()),
			keyFor("identity.created", otherIdentity),
			"different identities must derive different keys")
	})

	t.Run("balances of one ledger share that ledger's key", func(t *testing.T) {
		secondBalance := outboxSampleBalance()
		secondBalance.BalanceID = "bln_outbox_second"

		assert.Equal(t,
			keyFor("balance.created", outboxSampleBalance()),
			keyFor("balance.created", secondBalance),
			"a ledger's balance events must be ordered against each other")
	})

	t.Run("one batch's progress events share a key", func(t *testing.T) {
		assert.Equal(t,
			keyFor("bulk_transaction.inflight", outboxSampleBulkPayload("inflight", "", 3)),
			keyFor("bulk_transaction.applied", outboxSampleBulkPayload("applied", "", 3)),
			"a batch's progress must be readable in order")
	})
}

// ---------------------------------------------------------------------------
// The no-op-when-unconfigured contract
// ---------------------------------------------------------------------------

// TestPrepareEventOutbox_ReturnsNilWhenPublishingIsNotConfigured asserts the contract that
// protects every existing deployment and the whole existing test suite.
//
// SendWebhook returns nil the moment it sees an empty webhook URL, which is why Blnk runs
// perfectly well with no notification sink and why tests that construct NewBlnk(nil) work
// with no broker and no HTTP endpoint anywhere in sight. A publisher that errored, blocked
// or captured rows nobody could ever publish would break all of that.
//
// Brokers are checked for a non-BLANK entry rather than merely for a non-empty slice.
// KAFKA_BROKERS is parsed as a comma-separated list, so KAFKA_BROKERS="" and
// KAFKA_BROKERS="," both yield a slice that is non-empty and carries nothing usable;
// treating those as configured would fill the outbox with rows no relay could publish.
func TestPrepareEventOutbox_ReturnsNilWhenPublishingIsNotConfigured(t *testing.T) {
	unconfigured := []struct {
		name          string
		configuration *config.Configuration
	}{
		{
			name:          "no transport at all",
			configuration: &config.Configuration{},
		},
		{
			name: "a broker list of one empty string",
			configuration: &config.Configuration{
				Kafka: config.KafkaConfig{Brokers: []string{""}},
			},
		},
		{
			name: "a broker list of blanks, as a trailing comma yields",
			configuration: &config.Configuration{
				Kafka: config.KafkaConfig{Brokers: []string{" ", "", "\t"}},
			},
		},
		{
			name: "an empty broker list",
			configuration: &config.Configuration{
				Kafka: config.KafkaConfig{Brokers: []string{}},
			},
		},
		{
			name: "a whitespace-only webhook URL",
			configuration: &config.Configuration{
				Notification: config.Notification{
					Webhook: config.WebhookConfig{Url: "   "},
				},
			},
		},
		{
			name: "topic geometry configured but no transport",
			configuration: &config.Configuration{
				Kafka: config.KafkaConfig{TopicPrefix: "blnk", MinPartitions: 6, ReplicationFactor: 3},
				Relay: config.RelayConfig{MaxRetryAttempts: 5},
			},
		},
	}

	for _, scenario := range unconfigured {
		t.Run(scenario.name, func(t *testing.T) {
			datasource := newOutboxSpyDatasource()
			blnk := newOutboxBlnk(t, scenario.configuration, datasource)
			event := NewWebhook{Event: "transaction.applied", Payload: outboxSampleTransaction(StatusApplied)}

			assert.Nil(t, blnk.PrepareEventOutbox(context.Background(), event),
				"an unconfigured deployment captures nothing")
			assert.NoError(t, blnk.PublishEvent(context.Background(), event),
				"not being configured is not a failure of the mutation the caller just performed")
			assert.NoError(t, blnk.PublishEventInTx(context.Background(), nil, event),
				"the in-transaction entry point must honour the same contract")

			datasource.assertWroteNothing(t)
		})
	}
}

// TestPrepareEventOutbox_IsConfiguredByEitherTransport asserts the positive half of the
// contract: EITHER transport being configured is enough to capture the event.
//
// That is deliberate rather than lax. The outbox row feeds both transports — during the
// dual-delivery window the relay publishes it to Kafka and enqueues the legacy webhook task
// from the same row — so a deployment mid-migration with only a webhook URL still needs its
// rows captured, and a deployment past the sunset with only brokers obviously does too.
func TestPrepareEventOutbox_IsConfiguredByEitherTransport(t *testing.T) {
	configured := []struct {
		name          string
		configuration *config.Configuration
	}{
		{
			name: "Kafka brokers only",
			configuration: &config.Configuration{
				Kafka: config.KafkaConfig{Brokers: []string{"localhost:9092"}},
			},
		},
		{
			name: "several brokers",
			configuration: &config.Configuration{
				Kafka: config.KafkaConfig{Brokers: []string{"broker-1:9092", "broker-2:9092"}},
			},
		},
		{
			name: "a blank entry alongside a real broker",
			configuration: &config.Configuration{
				Kafka: config.KafkaConfig{Brokers: []string{"", "localhost:9092"}},
			},
		},
		{
			name: "the legacy webhook URL only, as a deployment mid-migration has",
			configuration: &config.Configuration{
				Notification: config.Notification{
					Webhook: config.WebhookConfig{Url: "https://example.com/webhooks"},
				},
			},
		},
		{
			name: "both transports, as the dual-delivery window has",
			configuration: &config.Configuration{
				Kafka: config.KafkaConfig{Brokers: []string{"localhost:9092"}},
				Notification: config.Notification{
					Webhook: config.WebhookConfig{Url: "https://example.com/webhooks"},
				},
			},
		},
	}

	for _, scenario := range configured {
		t.Run(scenario.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, scenario.configuration, nil)

			row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
				Event:   "transaction.applied",
				Payload: outboxSampleTransaction(StatusApplied),
			})
			require.NotNil(t, row, "a configured deployment must capture the event")
			assert.Equal(t, "transaction.applied", row.EventType)
		})
	}
}

// TestPrepareEventOutbox_ReturnsNilOnAnUnmarshalablePayload asserts that a malformed payload
// is a logged non-event, never an error and never a panic.
//
// A MALFORMED PAYLOAD MUST NEVER TAKE DOWN A LEDGER WRITE. Propagating an error here would
// abort the enclosing ledger transaction and reject a financially valid mutation because of
// a notification defect, which is the wrong trade in a ledger by a wide margin. Panicking
// would be worse still: these entry points are called from goroutines spawned by
// post-action hooks, where a panic takes down the process rather than surfacing as an error.
//
// This mirrors PrepareLineageOutbox's handling of the same situation exactly.
func TestPrepareEventOutbox_ReturnsNilOnAnUnmarshalablePayload(t *testing.T) {
	unmarshalable := []struct {
		name    string
		payload interface{}
	}{
		{name: "a channel", payload: make(chan int)},
		{name: "a function", payload: func() {}},
		{name: "a channel nested in a map", payload: map[string]interface{}{"sink": make(chan struct{})}},
		{
			name: "a channel nested in a payload field",
			payload: func() *model.Ledger {
				ledger := outboxSampleLedger()
				ledger.MetaData = map[string]interface{}{"sink": make(chan int)}

				return ledger
			}(),
		},
	}

	for _, scenario := range unmarshalable {
		t.Run(scenario.name, func(t *testing.T) {
			hook := logtest.NewGlobal()
			defer hook.Reset()

			datasource := newOutboxSpyDatasource()
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)
			event := NewWebhook{Event: "ledger.created", Payload: scenario.payload}

			var row *model.EventOutbox
			require.NotPanics(t, func() {
				row = blnk.PrepareEventOutbox(context.Background(), event)
			}, "a payload defect must not panic the ledger write path")
			assert.Nil(t, row, "an unmarshalable payload yields no row")

			var publishErr error
			require.NotPanics(t, func() {
				publishErr = blnk.PublishEvent(context.Background(), event)
			})
			assert.NoError(t, publishErr,
				"the failure must not propagate: it would abort a financially valid mutation")

			datasource.assertWroteNothing(t)

			// Silence would be the real defect: the event is genuinely lost, so an
			// operator has to be able to see that it happened and for which event.
			var logged bool
			for _, entry := range hook.AllEntries() {
				if entry.Level == logrus.ErrorLevel &&
					strings.Contains(entry.Message, "failed to marshal event outbox payload") &&
					strings.Contains(entry.Message, "ledger.created") {
					logged = true

					break
				}
			}
			assert.True(t, logged,
				"the marshal failure must be logged at error level, naming the event type")
		})
	}
}

// ---------------------------------------------------------------------------
// Persistence: which insert path, and what reaches the driver
// ---------------------------------------------------------------------------

// TestPublishEvent_UsesTheStandaloneInsertWithoutATransaction asserts the path selection
// for a caller that has no ledger transaction to enrol in.
//
// ledger.created, identity.created, balance.created, balance.monitor,
// bulk_transaction.<status> and system.error all arrive this way. They still belong in the
// outbox so they get the same durable retry, dead-letter and replay treatment as every
// other event; there is simply no wider transaction to join.
func TestPublishEvent_UsesTheStandaloneInsertWithoutATransaction(t *testing.T) {
	datasource := newOutboxSpyDatasource()
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)
	event := NewWebhook{Event: "ledger.created", Payload: outboxSampleLedger()}

	require.NoError(t, blnk.PublishEvent(context.Background(), event))

	standalone := datasource.standalone()
	require.Len(t, standalone, 1, "exactly one standalone insert must have been issued")
	assert.Equal(t, "ledger.created", standalone[0].EventType)
	assert.Equal(t, "blnk.system", standalone[0].Topic)
	assert.Equal(t, outboxLedgerID, standalone[0].LedgerID)
	assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(standalone[0].Payload),
		"the row handed to the repository must carry the legacy body byte for byte")

	inTxRows, _ := datasource.inTx()
	assert.Empty(t, inTxRows, "no transaction was supplied, so none may be used")
}

// TestPublishEventInTx_UsesTheInTransactionInsertWithTheCallersTransaction asserts the
// transactional-outbox guarantee at its narrowest.
//
// The caller has already begun a transaction and applied its mutation; passing that same
// *sql.Tx here ties the event's fate to it. The assertion is on the POINTER IDENTITY of the
// transaction, not merely on one having been supplied: an insert performed inside some
// other transaction would commit independently of the mutation and would defeat the whole
// point.
func TestPublishEventInTx_UsesTheInTransactionInsertWithTheCallersTransaction(t *testing.T) {
	_, db, controller := newOutboxSQLDatasource(t)
	controller.ExpectBegin()
	transaction, err := db.Begin()
	require.NoError(t, err, "the caller's transaction must open")
	controller.ExpectRollback()
	defer func() { _ = transaction.Rollback() }()

	datasource := newOutboxSpyDatasource()
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)
	event := NewWebhook{Event: "transaction.applied", Payload: outboxSampleTransaction(StatusApplied)}

	require.NoError(t, blnk.PublishEventInTx(context.Background(), transaction, event))

	inTxRows, transactions := datasource.inTx()
	require.Len(t, inTxRows, 1, "exactly one in-transaction insert must have been issued")
	require.Len(t, transactions, 1)
	assert.Same(t, transaction, transactions[0],
		"the insert must use the CALLER'S transaction, so the event and the mutation commit together")
	assert.Equal(t, "transaction.applied", inTxRows[0].EventType)
	assert.Equal(t, "blnk.transactions", inTxRows[0].Topic)
	assert.Equal(t, outboxSourceBalanceID, inTxRows[0].LedgerID)
	assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(inTxRows[0].Payload),
		"the row handed to the repository must carry the legacy body byte for byte")

	assert.Empty(t, datasource.standalone(),
		"a transaction was supplied, so the standalone path must not be taken")
}

// TestPublishEventInTx_NilTransactionFallsBackToTheStandaloneInsert asserts the convenience
// the two entry points share: a nil transaction is accepted and routed to the standalone
// path, so a caller holding a conditionally-open transaction needs no branch of its own.
func TestPublishEventInTx_NilTransactionFallsBackToTheStandaloneInsert(t *testing.T) {
	datasource := newOutboxSpyDatasource()
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)
	event := NewWebhook{Event: "identity.created", Payload: outboxSampleIdentity()}

	require.NoError(t, blnk.PublishEventInTx(context.Background(), nil, event))

	standalone := datasource.standalone()
	require.Len(t, standalone, 1, "a nil transaction must route to the standalone insert")
	assert.Equal(t, "identity.created", standalone[0].EventType)

	inTxRows, _ := datasource.inTx()
	assert.Empty(t, inTxRows, "a nil transaction must never be handed to the in-transaction insert")
}

// TestPublishEvent_IssuesTheOutboxInsertWithThePayloadBytes drives the real repository over
// a stubbed driver, so the statement and the bound values asserted here are the ones that
// would reach PostgreSQL.
//
// The payload argument is compared as EXACT BYTES. This is the byte-equality guarantee
// followed all the way to the SQL boundary: it is not enough for the in-memory row to hold
// the legacy body if something between the row and the driver re-marshals it.
//
// event_id and occurred_at are matched loosely because they are generated at construction;
// every other value is exact, including the resolved topic, the pending status and the
// retry budget.
func TestPublishEvent_IssuesTheOutboxInsertWithThePayloadBytes(t *testing.T) {
	datasource, _, controller := newOutboxSQLDatasource(t)
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)
	event := NewWebhook{Event: "balance.created", Payload: outboxSampleBalance()}

	expectedPayload := outboxLegacyWebhookBody(t, event)

	controller.ExpectQuery("INSERT INTO blnk.event_outbox").
		WithArgs(
			sqlmock.AnyArg(), // event_id: a fresh UUID per row
			"balance.created",
			outboxSourceBalanceID, // aggregate_id: the balance the event is about
			outboxLedgerID,        // ledger_id: the partition key
			"blnk.balances",
			model.SchemaVersionV1,
			expectedPayload,  // the legacy webhook body, byte for byte
			sqlmock.AnyArg(), // occurred_at: the domain instant, stamped at construction
			model.EventOutboxStatusPending,
			defaultEventMaxAttempts,
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(4242)))

	require.NoError(t, blnk.PublishEvent(context.Background(), event))
}

// TestPublishEvent_OpensNoTransactionOfItsOwn asserts an absence, and it is a load-bearing
// one.
//
// The standalone path must issue its insert on the connection pool. If it opened a
// transaction instead, the event would commit independently of anything the caller was
// doing, which is the exact failure the outbox exists to prevent — and callers that DO hold
// a transaction would end up with their event outside it.
//
// The stub expects one query and nothing else, so an unexpected Begin fails the statement,
// surfaces as a returned error and fails this test.
func TestPublishEvent_OpensNoTransactionOfItsOwn(t *testing.T) {
	datasource, _, controller := newOutboxSQLDatasource(t)
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

	controller.ExpectQuery("INSERT INTO blnk.event_outbox").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)))

	require.NoError(t, blnk.PublishEvent(context.Background(), NewWebhook{
		Event:   "system.error",
		Payload: outboxSampleSystemErrorPayload(),
	}), "the standalone insert must succeed without any transaction being opened")
}

// TestPublishEventInTx_IssuesTheInsertInsideTheCallersTransaction asserts the same statement
// is issued between the caller's Begin and Commit, so the event and the mutation share one
// transaction boundary.
func TestPublishEventInTx_IssuesTheInsertInsideTheCallersTransaction(t *testing.T) {
	datasource, db, controller := newOutboxSQLDatasource(t)
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)
	event := NewWebhook{Event: "transaction.queued", Payload: outboxSampleTransaction(StatusQueued)}

	expectedPayload := outboxLegacyWebhookBody(t, event)

	controller.ExpectBegin()
	controller.ExpectQuery("INSERT INTO blnk.event_outbox").
		WithArgs(
			sqlmock.AnyArg(),
			"transaction.queued",
			outboxTransactionID,
			outboxSourceBalanceID,
			"blnk.transactions",
			model.SchemaVersionV1,
			expectedPayload,
			sqlmock.AnyArg(), // occurred_at: the domain instant, stamped at construction
			model.EventOutboxStatusPending,
			defaultEventMaxAttempts,
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(99)))
	controller.ExpectCommit()

	transaction, err := db.Begin()
	require.NoError(t, err)

	require.NoError(t, blnk.PublishEventInTx(context.Background(), transaction, event))
	require.NoError(t, transaction.Commit(),
		"the event commits with the mutation, or not at all")
}

// TestPublishEvent_ReturnsThePersistenceError asserts the error is RETURNED rather than
// swallowed, on both paths.
//
// Error handling has to match SendWebhook's so that no call site needs adjusting: the
// producer sites route this error to notification.NotifyError exactly as they routed the
// webhook enqueue's error. Swallowing it would leave a failed capture invisible.
func TestPublishEvent_ReturnsThePersistenceError(t *testing.T) {
	sentinel := errors.New("outbox insert rejected by the database")

	t.Run("the standalone path returns the error verbatim", func(t *testing.T) {
		datasource := newOutboxSpyDatasource()
		datasource.insertErr = sentinel
		blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

		err := blnk.PublishEvent(context.Background(), NewWebhook{
			Event:   "ledger.created",
			Payload: outboxSampleLedger(),
		})
		require.Error(t, err, "a persistence failure must reach the caller")
		assert.ErrorIs(t, err, sentinel, "the error must arrive unwrapped and unaltered")
		assert.Len(t, datasource.standalone(), 1, "the insert must genuinely have been attempted")
	})

	t.Run("the in-transaction path returns the error verbatim", func(t *testing.T) {
		_, db, controller := newOutboxSQLDatasource(t)
		controller.ExpectBegin()
		transaction, beginErr := db.Begin()
		require.NoError(t, beginErr)
		controller.ExpectRollback()
		defer func() { _ = transaction.Rollback() }()

		datasource := newOutboxSpyDatasource()
		datasource.insertErr = sentinel
		blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

		err := blnk.PublishEventInTx(context.Background(), transaction, NewWebhook{
			Event:   "transaction.applied",
			Payload: outboxSampleTransaction(StatusApplied),
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, sentinel)

		inTxRows, transactions := datasource.inTx()
		require.Len(t, inTxRows, 1, "the insert must genuinely have been attempted")
		assert.Same(t, transaction, transactions[0],
			"the failed attempt must still have used the caller's transaction")
	})

	t.Run("a driver failure on the real repository is reported", func(t *testing.T) {
		datasource, _, controller := newOutboxSQLDatasource(t)
		blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

		controller.ExpectQuery("INSERT INTO blnk.event_outbox").
			WillReturnError(errors.New("connection reset by peer"))

		err := blnk.PublishEvent(context.Background(), NewWebhook{
			Event:   "balance.monitor",
			Payload: outboxSampleBalanceMonitor(),
		})
		require.Error(t, err, "a driver failure must not be silently discarded")
	})
}

// TestPublishEvent_DoesNotPanicWithoutADatasource asserts the two degenerate receivers a
// real deployment and the existing test suite both produce.
//
// NewBlnk(nil) is a supported construction — the legacy webhook tests use it — so there is
// genuinely nowhere to persist to, and that must be a logged warning rather than a crash. A
// nil receiver must not panic either: these methods are called from goroutines spawned by
// post-action hooks, where a panic takes down the process instead of surfacing as an error.
func TestPublishEvent_DoesNotPanicWithoutADatasource(t *testing.T) {
	event := NewWebhook{Event: "balance.created", Payload: outboxSampleBalance()}

	t.Run("a nil datasource is a warning, not a crash", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

		var err error
		require.NotPanics(t, func() { err = blnk.PublishEvent(context.Background(), event) })
		assert.NoError(t, err)

		require.NotPanics(t, func() { err = blnk.PublishEventInTx(context.Background(), nil, event) })
		assert.NoError(t, err)

		// The row is still built, which is what gives the warning an event identity an
		// operator can act on: "some event was dropped" is not a diagnosable message.
		var warned bool
		for _, entry := range hook.AllEntries() {
			if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "no datasource") {
				assert.Equal(t, "balance.created", entry.Data["event_type"],
					"the warning must name the event type that was dropped")
				assert.Equal(t, "blnk.balances", entry.Data["topic"])
				assert.NotEmpty(t, entry.Data["event_id"], "the warning must name the event id")
				warned = true

				break
			}
		}
		assert.True(t, warned,
			"a dropped event must be visible: with publishing configured, this means events are being lost")
	})

	t.Run("the datasource really is absent, as NewBlnk(nil) leaves it", func(t *testing.T) {
		// NewBlnk(nil) — the construction the legacy webhook tests use — is what makes a
		// nil datasource a supported reality rather than a hypothetical. It is NOT called
		// here, deliberately: NewBlnk builds a Redis client whose constructor pings the
		// server and fails if it cannot reach it, and the payload guarantee this file
		// proves has to hold with no infrastructure at all.
		//
		// Nothing is lost by building the struct instead. For this code path the two
		// constructions are indistinguishable — publishEvent reads exactly two fields, the
		// cached configuration and the datasource, and NewBlnk(nil) leaves the datasource
		// nil and the configuration set, which is precisely the shape asserted below.
		blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

		require.Nil(t, blnk.GetDataSource(),
			"the instance under test must carry no datasource, exactly as NewBlnk(nil) leaves it")

		var err error
		require.NotPanics(t, func() { err = blnk.PublishEvent(context.Background(), event) })
		assert.NoError(t, err)

		// The row is still constructible, so nothing about the absent datasource is
		// allowed to disturb the payload contract.
		row := blnk.PrepareEventOutbox(context.Background(), event)
		require.NotNil(t, row)
		assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(row.Payload))
	})

	t.Run("a nil receiver is a no-op", func(t *testing.T) {
		outboxStoreConfiguration(t, outboxPublishingConfiguration())

		var blnk *Blnk

		var err error
		require.NotPanics(t, func() { err = blnk.PublishEvent(context.Background(), event) },
			"a nil receiver must not panic: post-action hooks call this from goroutines")
		assert.NoError(t, err)

		require.NotPanics(t, func() { err = blnk.PublishEventInTx(context.Background(), nil, event) })
		assert.NoError(t, err)

		// The row is still constructible from the configuration store alone, which is what
		// the nil-receiver branch of the configuration read exists to provide.
		var row *model.EventOutbox
		require.NotPanics(t, func() { row = blnk.PrepareEventOutbox(context.Background(), event) })
		require.NotNil(t, row)
		assert.Equal(t, "blnk.balances", row.Topic)
	})
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// outboxConcurrentWriters and outboxEventsPerWriter size the concurrency test.
//
// The shape matters more than the size: several goroutines each publishing several events
// is exactly how the producers behave, since the post-action hooks in ledger.go,
// identity.go, balance.go and transaction_execution.go all call into this path from a
// goroutine they spawn per action.
const (
	outboxConcurrentWriters = 32
	outboxEventsPerWriter   = 8
)

// TestPublishEvent_IsSafeWhenCalledConcurrently asserts the entry point is safe to call
// from many goroutines at once, and that every call still produces a distinct event id.
//
// Run under -race this covers the data-race question. The uniqueness assertion covers a
// subtler failure: an identifier generated from shared mutable state would still be unique
// single-threaded and would collide only under concurrency, and a collision on event_id is a
// correctness bug because it is the subscriber idempotency key.
func TestPublishEvent_IsSafeWhenCalledConcurrently(t *testing.T) {
	datasource := newOutboxSpyDatasource()
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

	failures := make(chan error, outboxConcurrentWriters*outboxEventsPerWriter)

	var writers sync.WaitGroup
	for writer := 0; writer < outboxConcurrentWriters; writer++ {
		writers.Add(1)

		go func(writer int) {
			defer writers.Done()

			for event := 0; event < outboxEventsPerWriter; event++ {
				transaction := outboxSampleTransaction(StatusApplied)
				// A distinct source per writer, so the goroutines also exercise
				// concurrent derivation of DIFFERENT partition keys rather than all
				// hitting one.
				transaction.Source = fmt.Sprintf("bln_outbox_concurrent_%d", writer)
				transaction.TransactionID = fmt.Sprintf("txn_outbox_concurrent_%d_%d", writer, event)

				if err := blnk.PublishEvent(context.Background(), NewWebhook{
					Event:   "transaction.applied",
					Payload: transaction,
				}); err != nil {
					failures <- err
				}
			}
		}(writer)
	}
	writers.Wait()
	close(failures)

	for err := range failures {
		assert.NoError(t, err, "no concurrent publish may fail")
	}

	rows := datasource.standalone()
	require.Len(t, rows, outboxConcurrentWriters*outboxEventsPerWriter,
		"every concurrent publish must have reached the outbox exactly once")

	inTxRows, _ := datasource.inTx()
	assert.Empty(t, inTxRows, "no transaction was supplied by any writer")

	unique := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		require.NotNil(t, row)
		assert.NotEmpty(t, row.LedgerID, "no concurrently published row may lose its partition key")

		_, duplicate := unique[row.EventID]
		assert.False(t, duplicate, "event_id %q was issued twice under concurrency", row.EventID)
		unique[row.EventID] = struct{}{}
	}
	assert.Len(t, unique, len(rows), "every concurrently issued event_id must be distinct")
}

// TestPrepareEventOutbox_IsSafeWhenCalledConcurrently covers pure row construction under
// concurrency, with no datasource involved at all.
//
// Separating it from the publish test isolates the question: if the publish test failed
// under -race, this says whether the race is in construction or in persistence.
func TestPrepareEventOutbox_IsSafeWhenCalledConcurrently(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
	fixtures := outboxEventFixtures()

	var (
		guard sync.Mutex
		rows  []*model.EventOutbox
	)

	var writers sync.WaitGroup
	for writer := 0; writer < outboxConcurrentWriters; writer++ {
		writers.Add(1)

		go func(writer int) {
			defer writers.Done()

			fixture := fixtures[writer%len(fixtures)]
			row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
				Event:   fixture.eventType,
				Payload: fixture.payload,
			})

			guard.Lock()
			rows = append(rows, row)
			guard.Unlock()
		}(writer)
	}
	writers.Wait()

	guard.Lock()
	defer guard.Unlock()

	require.Len(t, rows, outboxConcurrentWriters)

	unique := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		require.NotNil(t, row, "row %d must have been built", index)
		assert.Equal(t, model.EventOutboxStatusPending, row.Status)
		assert.NotEmpty(t, row.LedgerID, "no concurrently built row may lose its partition key")
		unique[row.EventID] = struct{}{}
	}
	assert.Len(t, unique, len(rows), "concurrent construction must still produce distinct event ids")
}

// ---------------------------------------------------------------------------
// Structural invariant
// ---------------------------------------------------------------------------

// TestEventOutboxSource_ImportsNoKafkaClient pins the invariant the file under test states
// about itself: nothing on the producer's path talks to a broker.
//
// It matters because the alternative is silently plausible. A producer that published
// inline would compile, would pass a happy-path test against a running broker, and would
// then block a ledger write on broker availability — defeating the transactional outbox it
// exists to provide and coupling the ledger's availability to Kafka's. Only the relay may
// hold a writer.
//
// The path is relative because `go test` runs with the package directory as its working
// directory, which keeps this assertion self-contained.
func TestEventOutboxSource_ImportsNoKafkaClient(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "event_outbox.go", nil, parser.ImportsOnly)
	require.NoError(t, err, "event_outbox.go must be parseable to assert its imports")

	for _, imported := range parsed.Imports {
		path, unquoteErr := strconv.Unquote(imported.Path.Value)
		require.NoError(t, unquoteErr, "every import path must be a valid string literal")

		assert.NotContains(t, strings.ToLower(path), "kafka",
			"event_outbox.go must not import a Kafka client: a producer that published inline "+
				"would block a ledger write on broker availability")
	}
}

// TestEventOutboxTestSource_ImportsNoKafkaClient holds THIS file to the same standard.
//
// The payload contract has to be provable with no infrastructure, because that is the only
// way it stays provable in every environment the suite runs in. A broker dependency here
// would make the single most important assertion in the event pipeline conditional on a
// container being up.
func TestEventOutboxTestSource_ImportsNoKafkaClient(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "event_outbox_test.go", nil, parser.ImportsOnly)
	require.NoError(t, err, "event_outbox_test.go must be parseable to assert its imports")

	for _, imported := range parsed.Imports {
		path, unquoteErr := strconv.Unquote(imported.Path.Value)
		require.NoError(t, unquoteErr, "every import path must be a valid string literal")

		assert.NotContains(t, strings.ToLower(path), "kafka",
			"the payload-equivalence proof must need no broker")
	}
}

// ---------------------------------------------------------------------------
// Configuration hygiene
// ---------------------------------------------------------------------------

// TestPrepareEventOutbox_LeaksNoConfigurationBetweenTests asserts the harness itself is
// well behaved.
//
// config.ConfigStore is a process-global atomic.Value. Every test above publishes to it, so
// if the restore were not registered with t.Cleanup, a Kafka-configured state would leak
// into unrelated tests in this package and cause failures far from their cause. This test
// publishes a recognisable configuration through a subtest and then asserts the store no
// longer carries it — which is only true if the cleanup ran.
func TestPrepareEventOutbox_LeaksNoConfigurationBetweenTests(t *testing.T) {
	const sentinelPrefix = "outbox-leak-sentinel"

	before := config.ConfigStore.Load()

	t.Run("a subtest publishes a recognisable configuration", func(t *testing.T) {
		configuration := outboxPublishingConfiguration()
		configuration.Kafka.TopicPrefix = sentinelPrefix
		blnk := newOutboxBlnk(t, configuration, nil)

		row := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
			Event:   "identity.created",
			Payload: outboxSampleIdentity(),
		})
		require.NotNil(t, row)
		require.Equal(t, sentinelPrefix+".identities", row.Topic,
			"the sentinel configuration must genuinely be in force inside the subtest")
	})

	assert.Equal(t, before, config.ConfigStore.Load(),
		"the configuration store must be restored exactly as the subtest found it")
	assert.NotEqual(t, sentinelPrefix, TopicPrefix(),
		"no test may leave its own topic prefix in the process-global configuration store")
}
