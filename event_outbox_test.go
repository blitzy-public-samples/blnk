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
//   - Dual delivery. For the 30-day window the relay publishes the claimed outbox row
//     to Kafka AND enqueues the legacy webhook task from that same row.
//   - Subscriber compatibility. The payload is the whole two-key {"event": ..., "data":
//     ...} object, not just the inner "data", so an existing subscriber's body parser
//     keeps working when only the transport changes.
//   - Replay fidelity. A dead-lettered event is replayed by re-publishing the stored
//     bytes rather than by re-marshalling a struct, so byte equality at rest is what
//     makes byte equality on replay possible.
//
//   - No Kafka import and no broker. Everything here runs with no infrastructure, which
//     mirrors the file under test: event_outbox.go talks to no broker either.
//   - No retry, backoff or dead-letter assertions. Those belong to event_relay_test.go
//     and event_dlt_test.go, which act on rows this file's subject creates.
//   - No SQL-statement coverage beyond what go-sqlmock needs to identify the statement
//     the two insert paths issue. database/event_outbox_test.go owns the repository.
//   - No assertions about blnk.lineage_outbox or the lineage relay. The two outboxes
//     are separate tables served by separate relays and are deliberately not merged.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math/big"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/apierror"
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
	// therefore their expected message key: transaction keying prefers the source balance
	// so that Kafka partitioning agrees with the transaction queue's own sharding.
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
// passes it.
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

// outboxSampleBulkPayload reproduces, exactly, the map that sendBulkTransactionWebhook
// builds for the bulk_transaction.<status> family.
//
// The two conditionals are deliberately restated here rather than imported, so this
// file is an independent statement of the three shapes the family can take:
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
// The topic, aggregate id and partition key are LITERALS.
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
	// partitionKey is the expected partition_key column — the Kafka message key, and
	// therefore the partition, and therefore the ordering guarantee. It is a DIFFERENT
	// column from ledger_id, which records the authoritative ledger and takes no part in
	// partitioning; several shapes below have a partition key and no ledger at all.
	partitionKey string
}

// outboxEventCatalogueSize is the number of event types Blnk emits: thirteen.
//
// It is a CONTRACTUAL figure.
//
// Spelling it as an independent number is what stops the catalogue silently shrinking.
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
func outboxEventFixtures() []outboxEventFixture {
	return []outboxEventFixture{
		// The seven status-derived transaction events. All seven come from getEventFromStatus
		// and all seven key on the SOURCE balance, which is what keeps one transaction's
		// whole lifecycle — queued, then inflight, then applied — on a single partition and
		// therefore in order.
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
			// The worker's rejection handler passes the transaction BY VALUE, which is the only
			// producer that does. The value arm of the derivation type switch exists for this
			// call site alone.
			name:         "transaction.rejected (payload by value)",
			eventType:    "transaction.rejected",
			payload:      outboxSampleTransactionValue(StatusRejected),
			topic:        "blnk.transactions",
			aggregateID:  outboxTransactionID,
			partitionKey: outboxSourceBalanceID,
		},
		{
			// transaction.unknown is reachable, not hypothetical: getEventFromStatus has no case
			// for the COMMIT status, so a committed inflight transaction falls through to it.
			// That is pre-existing behaviour, preserved deliberately so the dual-delivery
			// payload comparison is not disturbed by an unrelated change to the event
			// vocabulary.
			name:         "transaction.unknown (COMMIT falls through)",
			eventType:    "transaction.unknown",
			payload:      outboxSampleTransaction(StatusCommit),
			topic:        "blnk.transactions",
			aggregateID:  outboxTransactionID,
			partitionKey: outboxSourceBalanceID,
		},

		// The bulk transaction family, in all three shapes its producer can build. The batch
		// is the aggregate and the key, because a batch's progress events only make sense
		// read in order relative to one another.
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

		// The two balance events. Both route to the balances topic, but they key differently:
		// a balance belongs to a ledger, whereas a monitor carries no ledger field at all and
		// keys on the balance it watches.
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

		// The two events that belong to none of the three categories the requirement names,
		// and that the coverage requirement forbids dropping. They do NOT share the one
		// category beyond the three the requirements name: the published catalogue puts
		// ledger.created and system.error together on blnk.system, so a subscriber reaches
		// ledger.created only through the privileged grant of that topic — see
		// model.EventCategorySystem.
		{
			name:         "ledger.created",
			eventType:    "ledger.created",
			payload:      outboxSampleLedger(),
			topic:        "blnk.system",
			aggregateID:  outboxLedgerID,
			partitionKey: outboxLedgerID,
		},
		{
			// system.error has no aggregate of any kind, so both the aggregate id and the
			// partition key fall back to the event type. That gives the error stream one
			// partition and therefore a total order, which is what an error consumer wants, and
			// it keeps the key non-empty.
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
// Only Kafka.Brokers is populated.
func outboxPublishingConfiguration() *config.Configuration {
	return &config.Configuration{
		Kafka: config.KafkaConfig{Brokers: []string{"localhost:9092"}},
	}
}

// outboxStoreConfiguration publishes cnf to config.ConfigStore and restores whatever
// was there before once the test finishes.
//
// config.ConfigStore is a process-global atomic.Value, so leaking a Kafka-configured
// state out of one test produces confusing failures in unrelated ones.
func outboxStoreConfiguration(t *testing.T, cnf *config.Configuration) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		// Nothing was published before this test. Leaving this test's configuration in place
		// could influence later ones, so publish an empty configuration, which is strictly
		// closer to the original state.
		config.ConfigStore.Store(&config.Configuration{})
	})

	config.ConfigStore.Store(cnf)
}

// newOutboxBlnk builds the smallest Blnk that PrepareEventOutbox and publishEvent
// actually need: a cached configuration and a datasource. ds may be nil, which is a
// supported construction the no-datasource contract covers.
func mustPrepareEventOutbox(t *testing.T, b *Blnk, event NewWebhook, options ...EventOption) *model.EventOutbox {
	t.Helper()

	row, err := b.PrepareEventOutbox(context.Background(), event, options...)
	require.NoError(t, err, "preparing the outbox row for %q must not fail", event.Event)
	require.NotNil(t, row, "publishing must be configured for this test, so a row is expected")

	return row
}

// requireAPIErrorCode asserts an error carries a specific typed code.
//
// The code is what callers and handlers switch on, so asserting the message alone would
// let the code change silently underneath every consumer of it.
func requireAPIErrorCode(t *testing.T, err error, want apierror.ErrorCode) {
	t.Helper()

	require.Error(t, err)

	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		assert.Equal(t, want, apiErr.Code)
		return
	}

	var apiErrPtr *apierror.APIError
	if errors.As(err, &apiErrPtr) && apiErrPtr != nil {
		assert.Equal(t, want, apiErrPtr.Code)
		return
	}

	require.Failf(t, "not an APIError", "expected a typed APIError, got %T: %v", err, err)
}

func newOutboxBlnk(t *testing.T, cnf *config.Configuration, ds database.IDataSource) *Blnk {
	t.Helper()

	outboxStoreConfiguration(t, cnf)

	return &Blnk{config: cnf, datasource: ds}
}

// outboxLegacyQueueName is the webhook queue the legacy-transport cases enqueue onto.
//
// It matches the literal the existing webhook tests use, so the two files describe the same
// queue rather than two plausible-looking ones.
const outboxLegacyQueueName = "webhook_queue"

// outboxLegacyWebhookURL is the endpoint the legacy-transport cases configure.
//
// The .invalid TLD is reserved by RFC 2606 and never resolves, which is deliberate:
// these tests assert what is ENQUEUED, and a URL that could resolve would leave open
// the possibility of an outbound request escaping the test.
const outboxLegacyWebhookURL = "http://webhook.invalid/blnk"

// outboxLegacyWebhookConfiguration returns the PRE-MIGRATION steady state: a webhook
// URL, a queue to enqueue onto, and no Kafka broker anywhere.
//
// This is the configuration the overwhelming majority of existing deployments are in,
// and the one every deployment is in before it opts into Kafka.
//
// Parameters:
//   - redisAddress string: the address of the Redis the asynq client enqueues into.
//
// Returns:
//   - *config.Configuration: a webhook-only configuration.
func outboxLegacyWebhookConfiguration(redisAddress string) *config.Configuration {
	return &config.Configuration{
		Redis: config.RedisConfig{Dns: redisAddress},
		Queue: config.QueueConfig{WebhookQueue: outboxLegacyQueueName, NumberOfQueues: 1},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{Url: outboxLegacyWebhookURL},
		},
	}
}

// newOutboxLegacyBlnk builds a Blnk whose asynq client enqueues into a real
// (in-process) Redis, which is what makes the legacy leg OBSERVABLE rather than merely
// unerroring.
//
// Parameters:
//   - t *testing.T: the test, used for the Redis lifecycle and the client's cleanup.
//   - cnf *config.Configuration: the configuration to cache and publish.
//   - ds database.IDataSource: the datasource, which the legacy path must never reach.
//
// Returns:
//   - *Blnk: an instance whose SendWebhook enqueues into cnf.Redis.Dns.
func newOutboxLegacyBlnk(t *testing.T, cnf *config.Configuration, ds database.IDataSource) *Blnk {
	t.Helper()

	outboxStoreConfiguration(t, cnf)

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: cnf.Redis.Dns})
	t.Cleanup(func() { _ = client.Close() })

	return &Blnk{config: cnf, datasource: ds, asynqClient: client}
}

// pendingLegacyTasks reads the legacy webhook queue and is safe to call from any
// goroutine.
//
// It deliberately takes no *testing.T.
//
// It is for use from the TEST GOROUTINE.
//
// Parameters:
//   - t *testing.T: the test, used for fatal failures.
//   - redisAddress string: the Redis the tasks were enqueued into.
//
// Returns:
//   - []*asynq.TaskInfo: the pending tasks, in queue order. Empty when nothing was
//     enqueued.
func outboxPendingLegacyTasks(t *testing.T, redisAddress string) []*asynq.TaskInfo {
	t.Helper()

	tasks, err := pendingLegacyTasks(redisAddress)
	require.NoError(t, err, "the legacy webhook queue must be readable for this assertion to mean anything")

	return tasks
}

// pendingLegacyTasks reads the legacy webhook queue and RETURNS its error, taking no
// *testing.T at all.
//
// testify evaluates an assert.Never or require.Eventually condition repeatedly and ON
// ITS OWN GOROUTINE, and it stops WAITING for those goroutines once the window closes —
// a straggler can still be mid-call after the test function has returned. Two things
// then go wrong if the condition can fail the test:
//
//   - t.Cleanup has already torn the miniredis server down, so the straggler's read
//     fails; and
//   - reporting that failure from a goroutine after the test completed is not a test
//     failure but a PANIC ("Fail in goroutine after … has completed") that takes the
//     whole package down, naming a test that had in fact passed.
//
// Parameters:
//   - redisAddress string: the Redis the tasks were enqueued into.
//
// Returns:
//   - []*asynq.TaskInfo: the pending tasks, in queue order. Empty when nothing was
//     enqueued.
//   - error: any failure other than the queue never having existed.
func pendingLegacyTasks(redisAddress string) ([]*asynq.TaskInfo, error) {
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: redisAddress})
	defer func() { _ = inspector.Close() }()

	tasks, err := inspector.ListPendingTasks(outboxLegacyQueueName)
	if errors.Is(err, asynq.ErrQueueNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return tasks, nil
}

// countPendingLegacyTasks is pendingLegacyTasks for a polled condition: it answers with
// a count and folds an unreadable queue into -1, which no expected count can equal.
//
// Parameters:
//   - redisAddress string: the Redis the tasks were enqueued into.
//
// Returns:
//   - int: the number of pending tasks, or -1 when the queue could not be read.
func countPendingLegacyTasks(redisAddress string) int {
	tasks, err := pendingLegacyTasks(redisAddress)
	if err != nil {
		return -1
	}

	return len(tasks)
}

// outboxSpyDatasource records which outbox insert was called, with which transaction
// and which row, and is the harness for every path-selection assertion below.
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

	// recordInsertDeadlines opts into recording the DEADLINE each insert ran under.
	recordInsertDeadlines bool
	// standaloneDeadlineValues are the deadlines standalone inserts ran under, in call
	// order. A nil entry means the context carried none, which is the defect.
	standaloneDeadlineValues []*time.Time
	// inTxDeadlineValues are the deadlines in-transaction inserts ran under, in call
	// order. A nil entry here is CORRECT: the deadline on that path belongs to the
	// caller's transaction.
	inTxDeadlineValues []*time.Time
}

// contextDeadline extracts a context's deadline as a nil-able value, so "carried no
// deadline" is representable rather than indistinguishable from the zero time.
func contextDeadline(ctx context.Context) *time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil
	}

	return &deadline
}

// newOutboxSpyDatasource returns a spy wired for use as the Blnk datasource.
func newOutboxSpyDatasource() *outboxSpyDatasource {
	return &outboxSpyDatasource{MockDataSource: new(mocks.MockDataSource)}
}

// InsertEventOutbox records a standalone insert.
func (s *outboxSpyDatasource) InsertEventOutbox(ctx context.Context, e *model.EventOutbox) error {
	s.guard.Lock()
	defer s.guard.Unlock()

	s.standaloneRows = append(s.standaloneRows, e)
	if s.recordInsertDeadlines {
		s.standaloneDeadlineValues = append(s.standaloneDeadlineValues, contextDeadline(ctx))
	}

	return s.insertErr
}

// InsertEventOutboxInTx records an in-transaction insert, keeping the transaction it was
// given so its identity can be asserted.
func (s *outboxSpyDatasource) InsertEventOutboxInTx(ctx context.Context, tx *sql.Tx, e *model.EventOutbox) error {
	s.guard.Lock()
	defer s.guard.Unlock()

	s.inTxTransactions = append(s.inTxTransactions, tx)
	s.inTxRows = append(s.inTxRows, e)
	if s.recordInsertDeadlines {
		s.inTxDeadlineValues = append(s.inTxDeadlineValues, contextDeadline(ctx))
	}

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

// assertWroteNothing asserts neither insert path was taken. It is the mechanical form
// of "the event was not captured".
//
// Parameters:
//   - t *testing.T: the test to fail.
//   - reasons ...string: optional context appended to the failure message.
func (s *outboxSpyDatasource) assertWroteNothing(t *testing.T, reasons ...string) {
	t.Helper()

	standalone := s.standalone()
	inTxRows, _ := s.inTx()

	context := strings.Join(reasons, "; ")
	if context != "" {
		context = ": " + context
	}

	assert.Empty(t, standalone, "no standalone insert may have been issued"+context)
	assert.Empty(t, inTxRows, "no in-transaction insert may have been issued"+context)
}

// newOutboxSQLDatasource returns a real database.Datasource over a go-sqlmock
// connection, together with the mock controller and the *sql.DB the caller needs to
// open a transaction.
//
// This is the harness for the two insert paths.
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

// outboxLegacyWebhookBody marshals event exactly as SendWebhook marshals it to build
// the HTTP body: json.Marshal of the WHOLE NewWebhook value, both keys.
//
// This function is the reference side of every byte-equality assertion in this file.
func outboxLegacyWebhookBody(t *testing.T, event NewWebhook) []byte {
	t.Helper()

	body, err := json.Marshal(event)
	require.NoError(t, err, "the legacy webhook body must marshal")

	return body
}

// outboxEnvelopeCarrying matches the event_raw bound value: the canonical envelope,
// whose payload member must be the given bytes VERBATIM.
type outboxEnvelopeCarrying struct {
	// payload is the legacy webhook body the envelope must splice in unaltered.
	payload []byte
}

// Match reports whether value is a canonical envelope carrying the expected payload
// bytes.
//
// Parameters:
//   - value driver.Value: the bound value sqlmock observed.
//
// Returns:
//   - bool: true only for a valid JSON object that declares all six envelope members
//     and whose payload member is byte-equal to the expected body.
func (m outboxEnvelopeCarrying) Match(value driver.Value) bool {
	raw, isBytes := value.([]byte)
	if !isBytes || len(raw) == 0 {
		return false
	}

	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		return false
	}

	for _, name := range []string{
		"event_id", "event_type", "aggregate_id", "occurred_at", "payload", "schema_version",
	} {
		if _, present := members[name]; !present {
			return false
		}
	}

	// json.RawMessage keeps the member's bytes exactly as they appeared, which is what makes
	// this a byte comparison rather than a document comparison.
	return string(members["payload"]) == string(m.payload)
}

// outboxTopLevelKeys returns the keys of a JSON object IN DOCUMENT ORDER.
//
// A map cannot be used for this: unmarshalling into one loses the very ordering the
// dual-delivery byte-equality guarantee depends on.
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

// outboxEnvelopePrefix is the exact byte prefix a stored payload must carry for the
// given event name: `{"event":"<name>","data":`.
//
// It is composed by marshalling the name rather than by string concatenation with
// quotes, so an event name needing JSON escaping still produces a correct expectation.
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
// The comparison is on []byte and on its string form, never on an unmarshalled map.
func TestPrepareEventOutbox_PayloadIsByteIdenticalToTheLegacyWebhookBody(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
			event := NewWebhook{Event: fixture.eventType, Payload: fixture.payload}

			// The reference: the body the legacy HTTP transport would have POSTed.
			expected := outboxLegacyWebhookBody(t, event)

			row := mustPrepareEventOutbox(t, blnk, event)
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
func TestPrepareEventOutbox_PayloadIsTheTwoKeyWebhookEnvelope(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
			event := NewWebhook{Event: fixture.eventType, Payload: fixture.payload}

			row := mustPrepareEventOutbox(t, blnk, event)
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
// value, which proves the two agree but not what either one looks like.
func TestPrepareEventOutbox_LedgerCreatedPayloadIsExactlyTheDocumentedObject(t *testing.T) {
	const expected = `{"event":"ledger.created","data":{"ledger_id":"ldg_outbox_fixture",` +
		`"name":"Outbox Fixture Ledger","created_at":"2026-03-14T15:09:26.535897932Z",` +
		`"meta_data":{"region":"eu-west-1"}}}`

	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	row := mustPrepareEventOutbox(t, blnk, NewWebhook{
		Event:   "ledger.created",
		Payload: outboxSampleLedger(),
	})
	require.NotNil(t, row)

	assert.Equal(t, expected, string(row.Payload),
		"the stored payload must be exactly the documented two-key object")
}

// TestPrepareEventOutbox_SpanWithholdsTheFinancialIdentifiers is the tracing
// guard.
func TestPrepareEventOutbox_SpanWithholdsTheFinancialIdentifiers(t *testing.T) {
	const (
		ledgerID    = "ldg_span_disclosure_probe"
		balanceID   = "bln_span_disclosure_probe"
		aggregateID = balanceID
	)

	// Through the package's shared span facility rather than by installing a provider
	// here. OpenTelemetry delegates the global tracer exactly once, so a provider
	// installed directly reached transaction.go's package-level tracer only if this test
	// happened to be the FIRST span test to run: it passed at -count=1 in declaration
	// order and recorded nothing at -count=2 or under any -shuffle seed that ran the
	// bulk-capture span test first.
	recorder := recordingTracerProvider(t)

	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	row := mustPrepareEventOutbox(t, blnk, NewWebhook{
		Event:   "balance.created",
		Payload: &model.Balance{BalanceID: balanceID, LedgerID: ledgerID, Currency: "USD"},
	}, WithEventLedgerID(ledgerID))

	// The row itself still carries every identifier: this is a TELEMETRY redaction, not a
	// reduction in what is durably recorded. The outbox row is the authoritative place an
	// operator looks up what an event belonged to, and it must remain complete.
	require.Equal(t, ledgerID, row.LedgerID, "the row must still record the ledger")
	require.Equal(t, ledgerID, row.PartitionKey,
		"a supplied ledger is the partition key, so the fixture exercises the key that used to be exported")
	require.Equal(t, aggregateID, row.AggregateID, "the row must still record the aggregate")

	spans := recorder.Ended()
	require.NotEmpty(t, spans, "the recorder must have captured the PrepareEventOutbox span, or nothing below is exercised")

	prepared := 0
	partitionKeyHashes := []string{}

	for _, span := range spans {
		attributeSets := [][]attribute.KeyValue{span.Attributes()}
		for _, event := range span.Events() {
			attributeSets = append(attributeSets, event.Attributes)

			assert.NotContains(t, event.Name, ledgerID, "a span event name must not carry the ledger either")
			assert.NotContains(t, event.Name, aggregateID)

			if event.Name != "Event outbox entry prepared" {
				continue
			}

			prepared++
			for _, kv := range event.Attributes {
				if kv.Key == "event.partition_key_hash" {
					partitionKeyHashes = append(partitionKeyHashes, kv.Value.AsString())
				}
			}
		}

		assert.NotContains(t, span.Name(), ledgerID, "a span name must not carry the ledger")

		for _, set := range attributeSets {
			for _, kv := range set {
				// Value.Emit renders every attribute type this span can carry, which is
				// what the leak assertions below need to inspect.
				rendered := kv.Value.Emit()
				assert.NotContains(t, rendered, ledgerID,
					"attribute %q exports the ledger id in the clear", kv.Key)
				assert.NotContains(t, rendered, balanceID,
					"attribute %q exports the balance id — the aggregate and the partition key — in the clear", kv.Key)
			}
		}
	}

	require.Equal(t, 1, prepared, "exactly one prepared-row span event is expected for one prepared row")
	require.Len(t, partitionKeyHashes, 1, "the partition key must still be reported, as a hash")

	assert.Equal(t, hashLogIdentifier(row.PartitionKey), partitionKeyHashes[0],
		"the hash must be the same projection the dead-letter log fields use, so a span and a log line about one event correlate")
	assert.NotEmpty(t, partitionKeyHashes[0], "a hash of a non-empty key must be non-empty, or correlation is impossible")
	assert.NotEqual(t, row.PartitionKey, partitionKeyHashes[0], "the hash must not be the identifier itself")
}

// TestPrepareEventOutbox_BulkTransactionPayloadShapes covers all three shapes the
// bulk_transaction family's payload can take, because the producer builds it
// conditionally and each branch is a different set of keys on the wire.
//
// The key sets are asserted explicitly rather than inferred.
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

			row := mustPrepareEventOutbox(t, blnk, event)
			require.NotNil(t, row)

			assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(row.Payload),
				"the stored payload must be the legacy webhook body byte for byte")
			assert.Equal(t, shape.dataKeys, outboxTopLevelKeys(t, outboxDataObject(t, []byte(row.Payload))),
				"the inner object must carry exactly the keys this shape defines")

			// The batch is the aggregate for every shape, so a batch's progress events
			// stay grouped and ordered however the batch ended.
			assert.Equal(t, outboxBatchID, row.AggregateID)
			assert.Equal(t, outboxBatchID, row.PartitionKey)
			assert.Equal(t, "blnk.transactions", row.Topic)
		})
	}
}

// TestPrepareEventOutbox_CoversEveryEmittedEventType is the completeness guard on the
// catalogue above.
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
// defect — it would make a duplicate look like a redelivery of a different event, or
// make a genuinely new event look like a duplicate and be discarded. A few hundred
// draws will not prove a generator sound on its own, but combined with parsing every
// value it does catch the failure modes that matter in practice: a constant, a counter
// reset, a value derived from the payload, or a truncated string.
const outboxUUIDSampleSize = 512

// TestPrepareEventOutbox_EventIDIsACanonicalUUIDAndStableForOneMutation asserts the
// identifier is a real, canonically formatted UUID and that its STABILITY matches the
// event's nature.
//
// event_id carries two contracts at once: it is the subscriber's idempotency key, and
// it is the unique index that makes the outbox's write side exactly-once. A freshly
// random id per preparation satisfies neither for a RETRY.
func TestPrepareEventOutbox_EventIDIsACanonicalUUIDAndStableForOneMutation(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
	event := NewWebhook{Event: "ledger.created", Payload: outboxSampleLedger()}

	first := mustPrepareEventOutbox(t, blnk, event)
	require.NotNil(t, first)

	parsed, err := uuid.Parse(first.EventID)
	require.NoError(t, err, "event_id must be a valid UUID, got %q", first.EventID)
	assert.Equal(t, first.EventID, parsed.String(),
		"event_id must be in the canonical lower-case hyphenated form")
	assert.Len(t, first.EventID, 36, "a canonical UUID is 36 characters")

	for i := 0; i < outboxUUIDSampleSize; i++ {
		repeat := mustPrepareEventOutbox(t, blnk, event)
		require.NotNil(t, repeat)
		require.Equal(t, first.EventID, repeat.EventID,
			"re-preparing the SAME mutation must derive the SAME id, or a retried mutation is delivered twice (draw %d)", i)
	}

	// A different ledger is a different event.
	otherLedger := outboxSampleLedger()
	otherLedger.LedgerID = "ldg_a_different_one"
	other := mustPrepareEventOutbox(t, blnk, NewWebhook{Event: "ledger.created", Payload: otherLedger})
	require.NotNil(t, other)
	assert.NotEqual(t, first.EventID, other.EventID,
		"two events about different ledgers must never share an id")

	// The same transaction at two points in its life is two events.
	transaction := outboxSampleTransaction(StatusQueued)
	queued := mustPrepareEventOutbox(t, blnk, NewWebhook{Event: "transaction.queued", Payload: transaction})
	applied := mustPrepareEventOutbox(t, blnk, NewWebhook{Event: "transaction.applied", Payload: transaction})
	require.NotNil(t, queued)
	require.NotNil(t, applied)
	assert.NotEqual(t, queued.EventID, applied.EventID,
		"two event types for one transaction must differ, or the second is suppressed as a duplicate of the first")
}

// TestPrepareEventOutbox_EventIDStaysFreshForARepEATABLEEvent is the other half of the
// identity contract, and it is the half that is easy to get catastrophically wrong.
//
// Determinism is CORRECT only for an event describing a mutation that happens once.
// Applying it to a repeatable event would collapse every later occurrence into a
// duplicate the unique index rejects, and the pipeline would stop delivering those
// events with no error anywhere at all:
//
//   - balance.monitor fires every time its condition is met. A derived id would deliver
//     the first alert and silently discard every one after it — the exact opposite of
//     what a monitor is for.
//   - system.error is emitted per occurrence. Two identical messages a second apart are
//     two events an operator needs to see twice.
func TestPrepareEventOutbox_EventIDStaysFreshForARepeatableEvent(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	for _, testCase := range []struct {
		name  string
		event NewWebhook
	}{
		{
			name:  "a balance monitor fires repeatedly",
			event: NewWebhook{Event: "balance.monitor", Payload: outboxSampleBalanceMonitor()},
		},
		{
			name:  "a system error is emitted per occurrence",
			event: NewWebhook{Event: "system.error", Payload: outboxSampleSystemErrorPayload()},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			seen := make(map[string]struct{}, outboxUUIDSampleSize)
			for i := 0; i < outboxUUIDSampleSize; i++ {
				row := mustPrepareEventOutbox(t, blnk, testCase.event)
				require.NotNil(t, row)

				parsed, err := uuid.Parse(row.EventID)
				require.NoError(t, err, "event_id must be a valid UUID, got %q", row.EventID)
				assert.Equal(t, row.EventID, parsed.String(),
					"event_id must be in the canonical lower-case hyphenated form")

				_, duplicate := seen[row.EventID]
				require.False(t, duplicate,
					"a repeatable event must get a FRESH id every time, or every occurrence after the first is silently discarded (collision at draw %d)", i)
				seen[row.EventID] = struct{}{}
			}

			assert.Len(t, seen, outboxUUIDSampleSize,
				"every occurrence must have produced a distinct event_id")
		})
	}
}

// TestPrepareEventOutbox_EventTypeMirrorsTheInnerEventName asserts the event name is
// hoisted to the envelope exactly.
func TestPrepareEventOutbox_EventTypeMirrorsTheInnerEventName(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
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

// TestPrepareEventOutbox_EventTypeIsTrimmedWhileThePayloadStaysVerbatim pins the one
// place where the envelope and the payload legitimately differ.
//
// The envelope trims the event name so that routing, filtering and topic resolution are
// not defeated by a stray space.
func TestPrepareEventOutbox_EventTypeIsTrimmedWhileThePayloadStaysVerbatim(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
	event := NewWebhook{Event: "  ledger.created\t", Payload: outboxSampleLedger()}

	row := mustPrepareEventOutbox(t, blnk, event)
	require.NotNil(t, row)

	assert.Equal(t, "ledger.created", row.EventType, "event_type must be trimmed")
	assert.Equal(t, "blnk.system", row.Topic, "the trimmed name must be what routing sees")
	assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(row.Payload),
		"the payload must remain the legacy body byte for byte, untrimmed")
	assert.Contains(t, string(row.Payload), `"event":"  ledger.created\t"`,
		"the payload must carry the producer's exact event string")
}

// TestPrepareEventOutbox_SchemaVersionIsV1 asserts the envelope version is the integer
// 1.
//
// The value reaches subscribers on the wire, where it is what a consumer branches on to
// decide whether it understands the envelope shape.
func TestPrepareEventOutbox_SchemaVersionIsV1(t *testing.T) {
	require.Equal(t, 1, model.SchemaVersionV1,
		"the initial schema version is 1; a change here is a subscriber-facing contract change")

	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
				Event:   fixture.eventType,
				Payload: fixture.payload,
			})
			require.NotNil(t, row)

			assert.Equal(t, model.SchemaVersionV1, row.SchemaVersion)
			assert.Equal(t, 1, row.SchemaVersion)
		})
	}
}

// TestPrepareEventOutbox_OccurredAtIsUTCAndRoundTripsThroughRFC3339 asserts the
// timestamp is present, is the current instant in UTC, and survives the wire without
// loss.
func TestPrepareEventOutbox_OccurredAtIsUTCAndRoundTripsThroughRFC3339(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	before := time.Now().UTC().Add(-time.Second)
	row := mustPrepareEventOutbox(t, blnk, NewWebhook{
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
// Two assertions are made per fixture, and both are needed: the literal expectation is an
// independent statement of the routing contract, and the equality with TopicForEvent
// proves the row was resolved through the same function the rest of the pipeline uses.
func TestPrepareEventOutbox_TopicIsResolvedFromTheEventType(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
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

// TestPrepareEventOutbox_TopicIsResolvedOnceAtConstruction asserts the topic follows
// the configured namespace prefix and is FROZEN on the row.
func TestPrepareEventOutbox_TopicIsResolvedOnceAtConstruction(t *testing.T) {
	configuration := outboxPublishingConfiguration()
	configuration.Kafka.TopicPrefix = "acme"
	blnk := newOutboxBlnk(t, configuration, nil)

	row := mustPrepareEventOutbox(t, blnk, NewWebhook{
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

// TestPrepareEventOutbox_MaxAttemptsFollowsTheRelayConfiguration asserts the retry
// budget stamped on the row.
//
// A non-positive configured value is REPLACED, not honoured.
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

			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
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

// TestPrepareEventOutbox_StatusIsPendingAndTheRelayStateIsFresh asserts the row enters
// the relay state machine at its start.
func TestPrepareEventOutbox_StatusIsPendingAndTheRelayStateIsFresh(t *testing.T) {
	require.Equal(t, "pending", model.EventOutboxStatusPending,
		"the pending status is the literal the claim query selects on")

	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
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

// TestPrepareEventOutbox_PartitionKeyIsTheDocumentedDerivation asserts the derived key
// for every payload type, against a literal expectation.
//
// This is the highest-consequence value the file under test computes.
func TestPrepareEventOutbox_PartitionKeyIsTheDocumentedDerivation(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
				Event:   fixture.eventType,
				Payload: fixture.payload,
			})
			require.NotNil(t, row)

			assert.Equal(t, fixture.partitionKey, row.PartitionKey,
				"%s must be keyed on %s", fixture.eventType, fixture.partitionKey)
			assert.NotEmpty(t, row.PartitionKey,
				"an empty key lets Kafka scatter the event round-robin and destroys ordering silently")
		})
	}
}

// TestPrepareEventOutbox_AggregateIDIsTheEventSubject asserts the aggregate identifier
// for every payload type.
func TestPrepareEventOutbox_AggregateIDIsTheEventSubject(t *testing.T) {
	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
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
	monitorRow := mustPrepareEventOutbox(t, blnk, NewWebhook{
		Event:   "balance.monitor",
		Payload: outboxSampleBalanceMonitor(),
	})
	require.NotNil(t, monitorRow)
	assert.Equal(t, outboxMonitorID, monitorRow.AggregateID, "the monitor is the subject")
	assert.Equal(t, outboxSourceBalanceID, monitorRow.PartitionKey, "the watched balance is the key")
	assert.Empty(t, monitorRow.LedgerID,
		"model.BalanceMonitor carries NO ledger, so the ledger column stays NULL rather than being filled with the balance id")
	assert.NotEqual(t, monitorRow.AggregateID, monitorRow.PartitionKey,
		"subject and partition key are different concepts and must not be conflated")
}

// TestPrepareEventOutbox_PartitionKeyFallbackChain pins every step of the documented
// fallback, in order.
//
// The chain exists for exactly one reason: the key must ALWAYS be present and always
// deterministic.
func TestPrepareEventOutbox_PartitionKeyFallbackChain(t *testing.T) {
	fallbacks := []struct {
		name        string
		eventType   string
		payload     interface{}
		key         string
		aggregateID string
	}{
		{
			// Step 1 of the transaction preference: the source balance, which is what the
			// transaction queue itself shards on, so Kafka partitioning and queue sharding
			// agree.
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
			// Whitespace-only identifiers are treated as absent. They must be, because " " and
			// "" hash to different partitions, so honouring a stray space would split one
			// aggregate's events across two partitions.
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
			// BalanceMonitor carries no ledger field at all, and the balance it watches is the
			// closest stable aggregate. With that gone, the monitor keys on itself.
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
			// system.error has neither an aggregate nor a ledger, so the key falls back to the
			// event type. That gives the error stream a single partition and therefore a total
			// order, which is what an error consumer wants.
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
			var prepareErr error
			require.NotPanics(t, func() {
				row, prepareErr = blnk.PrepareEventOutbox(context.Background(), NewWebhook{
					Event:   fallback.eventType,
					Payload: fallback.payload,
				})
			}, "no payload shape may panic the derivation: it runs on the ledger write path")
			require.NoError(t, prepareErr)
			require.NotNil(t, row)

			assert.Equal(t, fallback.key, row.PartitionKey)
			assert.Equal(t, fallback.aggregateID, row.AggregateID)
			assert.NotEmpty(t, row.PartitionKey, "the key must never be empty")
			assert.NotEmpty(t, row.AggregateID, "aggregate_id is NOT NULL in the schema")
		})
	}
}

// TestPrepareEventOutbox_UnkeyableEventIsRefused pins the replacement for the removed
// "blnk.unkeyed" sentinel.
//
// Only a zero-valued NewWebhook can reach it: every catalogued event type carries a
// declared key dimension and every real payload yields at least the type.
func TestPrepareEventOutbox_UnkeyableEventIsRefused(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	var row *model.EventOutbox
	var prepareErr error
	require.NotPanics(t, func() {
		row, prepareErr = blnk.PrepareEventOutbox(context.Background(), NewWebhook{})
	}, "the refusal must be an error, not a panic: this runs on the ledger write path")

	require.Error(t, prepareErr, "an event with no key must not be captured")
	assert.Nil(t, row, "no row may be handed back for an event that cannot be published in order")

	var apiErr apierror.APIError
	require.ErrorAs(t, prepareErr, &apiErr, "the refusal is a typed API error, not a bare error")
	assert.Equal(t, apierror.ErrEventKeyUnresolvable, apiErr.Code,
		"the code is what tells a caller this is a key problem rather than a storage failure")
	assert.Equal(t, http.StatusInternalServerError, apierror.StatusForCode(apiErr.Code),
		"an unkeyable event is a producer defect in this service, so it is a 500 and not a 400")
}

// TestPrepareEventOutbox_KeyDimensionIsDeclaredForEveryCataloguedEventType is the partitioning
// declaration contract.
//
// model.KeyDimensionForEventType declares what a type's key is SUPPOSED to be, and
// PrepareEventOutbox compares the dimension it achieved against it.
func TestPrepareEventOutbox_KeyDimensionIsDeclaredForEveryCataloguedEventType(t *testing.T) {
	declared := model.EventKeyDimensionsByType()

	for _, eventType := range model.CataloguedEventTypes() {
		dimension, present := declared[eventType]
		assert.Truef(t, present,
			"%q is a catalogued event type, so it must declare a key dimension; without one the "+
				"R-6 comparison in PrepareEventOutbox cannot fail for it", eventType)
		assert.NotEmptyf(t, dimension, "%q declares an empty key dimension", eventType)
	}

	// The bulk family is composed at runtime and is therefore matched by prefix rather than
	// declared. Its dimension is the batch, which is an aggregate.
	assert.Equal(t, model.EventKeyDimensionAggregate,
		model.KeyDimensionForEventType("bulk_transaction.applied"),
		"a batch is a runtime grouping whose members may span ledgers, so the batch is its own unit")

	// An event type nobody has declared must not report a ledger miss for every occurrence.
	assert.Equal(t, model.EventKeyDimensionAggregate,
		model.KeyDimensionForEventType("something.nobody.declared"),
		"an undeclared type is aggregate-dimensioned so it cannot produce a false R-6 miss")
}

// TestPrepareEventOutbox_EveryCategoryReachesItsDeclaredKeyDimension walks the whole
// catalogue and asserts the key each event type actually gets.
func TestPrepareEventOutbox_EveryCategoryReachesItsDeclaredKeyDimension(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	ledgerID := "ldg_dimension"

	cases := []struct {
		name        string
		eventType   string
		payload     interface{}
		options     []EventOption
		wantKey     string
		wantLedger  string
		wantDeclare model.EventKeyDimension
	}{
		{
			name:        "a transaction event supplied with its ledger reaches the ledger dimension",
			eventType:   "transaction.applied",
			payload:     &model.Transaction{TransactionID: "txn_1", Source: "bal_src", Destination: "bal_dst"},
			options:     []EventOption{WithEventLedgerID(ledgerID)},
			wantKey:     ledgerID,
			wantLedger:  ledgerID,
			wantDeclare: model.EventKeyDimensionLedger,
		},
		{
			name:        "a balance carries its own ledger, so no option is needed",
			eventType:   "balance.created",
			payload:     &model.Balance{BalanceID: "bal_1", LedgerID: ledgerID},
			wantKey:     ledgerID,
			wantLedger:  ledgerID,
			wantDeclare: model.EventKeyDimensionLedger,
		},
		{
			name:        "a monitor alert is keyed on the watched balance's ledger, supplied by the check",
			eventType:   "balance.monitor",
			payload:     model.BalanceMonitor{MonitorID: "mon_1", BalanceID: "bal_1"},
			options:     []EventOption{WithEventLedgerID(ledgerID)},
			wantKey:     ledgerID,
			wantLedger:  ledgerID,
			wantDeclare: model.EventKeyDimensionLedger,
		},
		{
			name:        "a ledger event is about the ledger itself",
			eventType:   "ledger.created",
			payload:     &model.Ledger{LedgerID: ledgerID},
			wantKey:     ledgerID,
			wantLedger:  ledgerID,
			wantDeclare: model.EventKeyDimensionLedger,
		},
		{
			name:        "an identity is its own aggregate and has no ledger",
			eventType:   "identity.created",
			payload:     &model.Identity{IdentityID: "idt_1"},
			wantKey:     "idt_1",
			wantLedger:  "",
			wantDeclare: model.EventKeyDimensionAggregate,
		},
		{
			name:      "a bulk batch summary is keyed on the batch",
			eventType: "bulk_transaction.applied",
			payload: map[string]interface{}{
				"batch_id": "bulk_1", "status": "applied", "transaction_count": 3,
			},
			wantKey:     "bulk_1",
			wantLedger:  "",
			wantDeclare: model.EventKeyDimensionAggregate,
		},
		{
			name:        "system.error has no aggregate at all and keys on its type",
			eventType:   "system.error",
			payload:     map[string]interface{}{"error": "boom"},
			wantKey:     "system.error",
			wantLedger:  "",
			wantDeclare: model.EventKeyDimensionEventType,
		},
		{
			name:        "THE MISS: a rejected transaction has no balances, so its key falls to the source",
			eventType:   "transaction.rejected",
			payload:     &model.Transaction{TransactionID: "txn_2", Source: "bal_src"},
			wantKey:     "bal_src",
			wantLedger:  "",
			wantDeclare: model.EventKeyDimensionLedger,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
				Event:   testCase.eventType,
				Payload: testCase.payload,
			}, testCase.options...)
			require.NotNil(t, row)

			assert.Equal(t, testCase.wantKey, row.PartitionKey, "the Kafka message key")
			assert.Equal(t, testCase.wantLedger, row.LedgerID,
				"ledger_id records the AUTHORITATIVE ledger and stays NULL when the event has none; "+
					"a fabricated ledger is worse than an absent one")
			assert.Equal(t, testCase.wantDeclare,
				model.KeyDimensionForEventType(testCase.eventType),
				"the declared dimension is what the capture compares against")
			assert.NotEmpty(t, row.PartitionKey, "no event may reach Kafka without a key")
			assert.NotEmpty(t, row.AggregateID, "aggregate_id is NOT NULL in the schema")
		})
	}
}

// TestPrepareEventOutbox_PartitionKeyIsStableWithinAnAggregate is the ordering
// guarantee expressed as a property rather than as a table.
func TestPrepareEventOutbox_PartitionKeyIsStableWithinAnAggregate(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	keyFor := func(eventType string, payload interface{}) string {
		row := mustPrepareEventOutbox(t, blnk, NewWebhook{
			Event:   eventType,
			Payload: payload,
		})
		require.NotNil(t, row)

		return row.PartitionKey
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

// TestPrepareEventOutbox_ReturnsNilWhenPublishingIsNotConfigured asserts the contract
// that protects every existing deployment and the whole existing test suite.
//
// Brokers are checked for a non-BLANK entry rather than merely for a non-empty slice.
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

			unconfiguredRow, unconfiguredErr := blnk.PrepareEventOutbox(context.Background(), event)
			assert.Nil(t, unconfiguredRow, "an unconfigured deployment captures nothing")
			assert.NoError(t, unconfiguredErr,
				"being unconfigured is the ONE nil-nil case: it is a no-op, not a failure")
			assert.NoError(t, blnk.PublishEvent(context.Background(), event),
				"not being configured is not a failure of the mutation the caller just performed")
			assert.NoError(t, blnk.PublishEventInTx(context.Background(), nil, event),
				"the in-transaction entry point must honour the same contract")

			datasource.assertWroteNothing(t)
		})
	}
}

// TestPrepareEventOutbox_IsConfiguredByKafkaBrokers asserts the positive half of the
// contract: the outbox captures an event when, and only when, KAFKA IS CONFIGURED.
//
// The outbox is only a destination while something drains it, and the relay refuses to
// run without Kafka — handed the no-op publisher it would report every publish as
// dispatched and retire the entire outbox having sent nothing. A webhook-only
// deployment that captured rows therefore accumulated them and delivered none of them,
// silently, because the producers had stopped calling SendWebhook themselves.
func TestPrepareEventOutbox_IsConfiguredByKafkaBrokers(t *testing.T) {
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

			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
				Event:   "transaction.applied",
				Payload: outboxSampleTransaction(StatusApplied),
			})
			require.NotNil(t, row, "a configured deployment must capture the event")
			assert.Equal(t, "transaction.applied", row.EventType)
		})
	}

	t.Run("a legacy webhook URL alone captures nothing", func(t *testing.T) {
		datasource := newOutboxSpyDatasource()
		blnk := newOutboxBlnk(t, &config.Configuration{
			Notification: config.Notification{
				Webhook: config.WebhookConfig{Url: "https://example.com/webhooks"},
			},
		}, datasource)

		row, err := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
			Event:   "transaction.applied",
			Payload: outboxSampleTransaction(StatusApplied),
		})
		require.NoError(t, err, "having no broker is a steady state, not a failure")
		assert.Nil(t, row,
			"with no Kafka there is no relay to claim the row, so capturing one would strand it "+
				"forever while the configured webhook went unsent")

		datasource.assertWroteNothing(t)
	})
}

// TestPublishEvent_DeliversOverTheLegacyTransportWhenKafkaIsAbsent is the other half of
// Having established that a webhook-only deployment captures nothing, this
// states what it does INSTEAD.
func TestPublishEvent_DeliversOverTheLegacyTransportWhenKafkaIsAbsent(t *testing.T) {
	newLegacyOnlyInstanceWithSunset := func(t *testing.T, webhookURL, sunset string) (*Blnk, *outboxSpyDatasource) {
		t.Helper()

		redisServer := miniredis.RunT(t)
		datasource := newOutboxSpyDatasource()

		outboxStoreConfiguration(t, &config.Configuration{
			Redis: config.RedisConfig{Dns: redisServer.Addr()},
			Queue: config.QueueConfig{
				WebhookQueue:   "webhook_queue_legacy_only_test",
				IndexQueue:     "index_queue_legacy_only_test",
				NumberOfQueues: 1,
			},
			Notification: config.Notification{
				Webhook: config.WebhookConfig{Url: webhookURL},
			},
			WebhookDeprecationSunsetDate: sunset,
		})

		instance, err := NewBlnk(datasource)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, instance.Close()) })
		require.True(t, IsNoopEventPublisher(instance.events),
			"no brokers must select the no-op publisher, which is what makes the relay refuse to "+
				"run and therefore what makes the legacy path the only delivery route")

		return instance, datasource
	}

	// No sunset: the ordinary state of a webhook-only deployment that has not scheduled a
	// retirement. With no Kafka transport the sunset resolves to "not passed", so the
	// legacy path keeps working exactly as it did before this feature existed.
	newLegacyOnlyInstance := func(t *testing.T, webhookURL string) (*Blnk, *outboxSpyDatasource) {
		t.Helper()

		return newLegacyOnlyInstanceWithSunset(t, webhookURL, "")
	}

	event := NewWebhook{Event: "transaction.applied", Payload: outboxSampleTransaction(StatusApplied)}

	t.Run("the legacy task is enqueued and no outbox row is written", func(t *testing.T) {
		instance, datasource := newLegacyOnlyInstance(t, "https://example.com/webhooks")

		require.NoError(t, instance.PublishEvent(context.Background(), event))

		datasource.assertWroteNothing(t)

		// The enqueue is observed on the queue itself rather than inferred from the absence
		// of an error, because "returned nil" is exactly what the broken behaviour did too.
		inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: instance.Config().Redis.Dns})
		t.Cleanup(func() { assert.NoError(t, inspector.Close()) })

		info, err := inspector.GetQueueInfo("webhook_queue_legacy_only_test")
		require.NoError(t, err,
			"the webhook queue must exist, which it only does once something was enqueued onto it")
		assert.Equal(t, 1, info.Size,
			"exactly one legacy delivery task must be queued: this is the delivery a webhook-only "+
				"deployment had before the outbox existed and must still have")
	})

	t.Run("no webhook URL and no brokers stays a complete no-op", func(t *testing.T) {
		instance, datasource := newLegacyOnlyInstance(t, "")

		require.NoError(t, instance.PublishEvent(context.Background(), event),
			"a deployment with no notification sink at all is a legitimate steady state, exactly "+
				"as SendWebhook returning nil on an empty URL has always made it")

		datasource.assertWroteNothing(t)

		inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: instance.Config().Redis.Dns})
		t.Cleanup(func() { assert.NoError(t, inspector.Close()) })

		info, err := inspector.GetQueueInfo("webhook_queue_legacy_only_test")
		if err == nil {
			assert.Zero(t, info.Size, "nothing may be enqueued when nothing is configured")
		}
	})

	t.Run("the in-transaction entry point delivers nothing and reports success", func(t *testing.T) {
		instance, datasource := newLegacyOnlyInstance(t, "https://example.com/webhooks")

		// A non-nil transaction is what a caller holding an open ledger transaction passes. A
		// real one is unnecessary — nothing dereferences it on this path — and opening one
		// would make this test depend on a database it has no business needing.
		require.NoError(t, instance.PublishEventInTx(context.Background(), new(sql.Tx), event),
			"an in-transaction capture on a Kafka-less deployment is a documented no-op, not a failure")

		datasource.assertWroteNothing(t)

		inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: instance.Config().Redis.Dns})
		t.Cleanup(func() { assert.NoError(t, inspector.Close()) })

		info, err := inspector.GetQueueInfo("webhook_queue_legacy_only_test")
		if err == nil {
			assert.Zero(t, info.Size,
				"asynq is backed by Redis and cannot join a PostgreSQL transaction, so enqueuing "+
					"here would deliver a webhook for a mutation that may still roll back; the "+
					"caller's post-commit path is what delivers it")
		}
	})

	// THE SUNSET GATE on this path. The legacy-only
	// branch compared nothing against the retirement instant, so a deployment that had
	// scheduled the HTTP transport's retirement — and passed it — kept delivering over it,
	// while the relay and the 410 guard on a Kafka deployment both treated it as gone.
	t.Run("nothing is enqueued once the configured sunset has passed", func(t *testing.T) {
		past := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
		instance, datasource := newLegacyOnlyInstanceWithSunset(t, "https://example.com/webhooks", past)

		require.True(t, WebhookSunsetPassedNow(),
			"the fixture must actually be past the sunset, or the assertion below proves nothing")

		require.NoError(t, instance.PublishEvent(context.Background(), event),
			"a retired transport is not an error: the event is simply no longer owed to it")

		datasource.assertWroteNothing(t)

		inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: instance.Config().Redis.Dns})
		t.Cleanup(func() { assert.NoError(t, inspector.Close()) })

		info, err := inspector.GetQueueInfo("webhook_queue_legacy_only_test")
		if err == nil {
			assert.Zero(t, info.Size,
				"the legacy HTTP transport is retired, so no delivery may be enqueued onto it")
		}
	})

	// ... and the boundary on the other side: a sunset still in the future leaves the
	// transport running. Asserted so the gate cannot be satisfied by refusing everything.
	t.Run("a sunset still in the future leaves the legacy transport working", func(t *testing.T) {
		future := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
		instance, datasource := newLegacyOnlyInstanceWithSunset(t, "https://example.com/webhooks", future)

		require.False(t, WebhookSunsetPassedNow(), "the fixture must be inside the window")
		require.NoError(t, instance.PublishEvent(context.Background(), event))

		datasource.assertWroteNothing(t)

		inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: instance.Config().Redis.Dns})
		t.Cleanup(func() { assert.NoError(t, inspector.Close()) })

		info, err := inspector.GetQueueInfo("webhook_queue_legacy_only_test")
		require.NoError(t, err)
		assert.Equal(t, 1, info.Size,
			"before the sunset the legacy delivery is still owed and must still be enqueued")
	})
}

// TestPrepareEventOutbox_ReturnsNilOnAnUnmarshalablePayload asserts that a malformed
// payload is a logged non-event, never an error and never a panic.
//
// A MALFORMED PAYLOAD MUST NEVER TAKE DOWN A LEDGER WRITE.
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
			var prepareErr error
			require.NotPanics(t, func() {
				row, prepareErr = blnk.PrepareEventOutbox(context.Background(), event)
			}, "a payload defect must not panic the ledger write path")
			assert.Nil(t, row, "an unmarshalable payload yields no row")

			// THE BEHAVIOUR THIS TEST NOW GUARDS, and it is the reverse of what it asserted
			// before.
			require.Error(t, prepareErr,
				"a payload that cannot be serialised must be reported, not swallowed: a swallowed one is a lost event no mechanism can find")
			requireAPIErrorCode(t, prepareErr, apierror.ErrInternalServer)

			var publishErr error
			require.NotPanics(t, func() {
				publishErr = blnk.PublishEvent(context.Background(), event)
			})
			require.Error(t, publishErr,
				"the failure must reach the caller so an in-transaction mutation rolls back rather than committing without its event")

			datasource.assertWroteNothing(t)

			// Silence would be the real defect: the event is genuinely lost, so an
			// operator has to be able to see that it happened and for which event.
			var logged bool
			for _, entry := range hook.AllEntries() {
				if entry.Level == logrus.ErrorLevel &&
					strings.Contains(entry.Message, "payload could not be marshaled") {
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

// TestPublishEvent_UsesTheStandaloneInsertWithoutATransaction asserts the path
// selection for a caller that has no ledger transaction to enrol in.
//
// The standalone path is for events that accompany NO MUTATION OF THEIR OWN. Three
// kinds reach it:
//
//   - balance.monitor, which reports that a condition was met and changes nothing;
//   - bulk_transaction.<status>, a batch SUMMARY, whose per-transaction mutations have
//     each already committed under their own transaction — there is no batch-spanning
//     transaction for it to join;
//   - system.error, which describes a failure rather than a write.
func TestPublishEvent_UsesTheStandaloneInsertWithoutATransaction(t *testing.T) {
	datasource := newOutboxSpyDatasource()
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)
	event := NewWebhook{Event: "ledger.created", Payload: outboxSampleLedger()}

	require.NoError(t, blnk.PublishEvent(context.Background(), event))

	standalone := datasource.standalone()
	require.Len(t, standalone, 1, "exactly one standalone insert must have been issued")
	assert.Equal(t, "ledger.created", standalone[0].EventType)
	assert.Equal(t, "blnk.system", standalone[0].Topic)
	assert.Equal(t, outboxLedgerID, standalone[0].PartitionKey)
	assert.Equal(t, outboxLedgerID, standalone[0].LedgerID,
		"a balance payload DOES carry a ledger, so the ledger column is populated as well as the key")
	assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(standalone[0].Payload),
		"the row handed to the repository must carry the legacy body byte for byte")

	inTxRows, _ := datasource.inTx()
	assert.Empty(t, inTxRows, "no transaction was supplied, so none may be used")
}

// TestPublishEvent_WebhookOnlyDeploymentStillDeliversOverTheLegacyTransport is the
// other half of TestPrepareEventOutbox_IsConfiguredByKafkaBrokers, and together they
// describe the whole of what a deployment without a broker does.
//
// Such a deployment has only the legacy transport, so PublishEvent routes it straight
// down that one. The four cases below are the four states the two transports can be in, and each
// pins a different half of the decision:
//
//   - webhook only — the legacy enqueue happens and NO row is captured, because a row
//     nobody drains is worse than no row at all;
//   - both configured — the outbox wins and there is no publish-time enqueue, because
//     during the dual-delivery window the relay drives BOTH legs from the one claimed
//     row, which is what makes their bytes identical;
//   - neither configured — nothing at all, which is the no-op-when-unconfigured
//     contract;
//   - a webhook URL with no queue client — a typed error, because that deployment asked
//     for a transport it has not got and dropping the event silently is how it would
//     never find out.
func TestPublishEvent_WebhookOnlyDeploymentStillDeliversOverTheLegacyTransport(t *testing.T) {
	event := NewWebhook{Event: "transaction.applied", Payload: outboxSampleTransaction(StatusApplied)}

	t.Run("a webhook URL and no broker delivers over the legacy transport", func(t *testing.T) {
		redisServer := miniredis.RunT(t)
		datasource := newOutboxSpyDatasource()
		instance := newOutboxLegacyBlnk(t, outboxLegacyWebhookConfiguration(redisServer.Addr()), datasource)

		require.NoError(t, instance.PublishEvent(context.Background(), event),
			"a webhook-only deployment must deliver the event, not fail on it")

		tasks := outboxPendingLegacyTasks(t, redisServer.Addr())
		require.Len(t, tasks, 1,
			"exactly one legacy delivery must be enqueued; this is the delivery that silently stopped happening")
		assert.Equal(t, outboxLegacyQueueName, tasks[0].Type,
			"the task type and the queue name are deliberately the same string: the mux dispatches on the type")
		assert.Equal(t, outboxLegacyQueueName, tasks[0].Queue,
			"the task must land on the configured webhook queue, whose mux has the handler for it")
		assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(tasks[0].Payload),
			"the enqueued body must be the legacy two-key envelope byte for byte, exactly as the producers sent it before")

		datasource.assertWroteNothing(t,
			"no relay can drain this deployment's outbox, so capturing a row here would strand it forever")
	})

	t.Run("a configured broker keeps the outbox as the destination", func(t *testing.T) {
		redisServer := miniredis.RunT(t)
		cnf := outboxLegacyWebhookConfiguration(redisServer.Addr())
		cnf.Kafka = config.KafkaConfig{Brokers: []string{"localhost:9092"}}

		datasource := newOutboxSpyDatasource()
		instance := newOutboxLegacyBlnk(t, cnf, datasource)

		require.NoError(t, instance.PublishEvent(context.Background(), event))

		standalone := datasource.standalone()
		require.Len(t, standalone, 1,
			"with a broker configured the event belongs in the outbox, whichever other transports are also configured")
		assert.Equal(t, "transaction.applied", standalone[0].EventType)

		assert.Empty(t, outboxPendingLegacyTasks(t, redisServer.Addr()),
			"the legacy leg is the relay's to drive from the claimed row; enqueuing here as well would deliver the event twice")
	})

	t.Run("neither transport configured is a no-op", func(t *testing.T) {
		redisServer := miniredis.RunT(t)
		cnf := outboxLegacyWebhookConfiguration(redisServer.Addr())
		cnf.Notification.Webhook.Url = ""

		datasource := newOutboxSpyDatasource()
		instance := newOutboxLegacyBlnk(t, cnf, datasource)

		require.NoError(t, instance.PublishEvent(context.Background(), event),
			"being unconfigured is a legitimate steady state, not a failure of the mutation the caller just performed")

		assert.Empty(t, outboxPendingLegacyTasks(t, redisServer.Addr()),
			"with no webhook URL there is nowhere to deliver to")
		datasource.assertWroteNothing(t, "with no broker there is nothing to capture for")
	})

	t.Run("a webhook URL with no queue client is reported rather than dropped", func(t *testing.T) {
		datasource := newOutboxSpyDatasource()
		// Built WITHOUT an asynq client on purpose: this is the shape a half-wired
		// deployment has, and the event has nowhere to go.
		instance := newOutboxBlnk(t, outboxLegacyWebhookConfiguration("127.0.0.1:6379"), datasource)

		err := instance.PublishEvent(context.Background(), event)
		requireAPIErrorCode(t, err, apierror.ErrInternalServer)
		assert.Contains(t, err.Error(), "queue client",
			"the error must name what is missing, or an operator cannot act on it")

		datasource.assertWroteNothing(t, "the outbox is not a fallback for a missing queue client")
	})
}

// TestPublishEventInTx_UsesTheInTransactionInsertWithTheCallersTransaction asserts the
// transactional-outbox guarantee at its narrowest.
//
// The caller has already begun a transaction and applied its mutation; passing that
// same *sql.Tx here ties the event's fate to it.
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
	assert.Equal(t, outboxSourceBalanceID, inTxRows[0].PartitionKey)
	assert.Empty(t, inTxRows[0].LedgerID,
		"model.Transaction has NO ledger field, so the ledger column is NULL: a source balance id in a column called ledger_id is exactly the conflation this split removed")
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

// TestPublishEvent_IssuesTheOutboxInsertWithThePayloadBytes drives the real repository
// over a stubbed driver, so the statement and the bound values asserted here are the
// ones that would reach PostgreSQL.
//
// The payload argument is compared as EXACT BYTES.
func TestPublishEvent_IssuesTheOutboxInsertWithThePayloadBytes(t *testing.T) {
	datasource, _, controller := newOutboxSQLDatasource(t)
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)
	event := NewWebhook{Event: "balance.created", Payload: outboxSampleBalance()}

	expectedPayload := outboxLegacyWebhookBody(t, event)

	// partition_key and ledger_id are SEPARATE bound values, and this is the one place the
	// distinction is visible all the way at the SQL boundary. A balance payload is the
	// case where both are populated and they happen to be the SAME value — the balance
	// belongs to that ledger, so keying by the ledger co-locates its balance events — but
	// they are still two columns carrying two different facts.
	controller.ExpectQuery("INSERT INTO blnk.event_outbox").
		WithArgs(
			sqlmock.AnyArg(), // event_id: a fresh UUID per row
			"balance.created",
			outboxSourceBalanceID, // aggregate_id: the balance the event is about
			outboxLedgerID,        // partition_key: the routing key
			outboxLedgerID,        // ledger_id: the authoritative ledger, which a balance does carry
			"blnk.balances",
			model.SchemaVersionV1,
			expectedPayload, // payload: the legacy webhook body, byte for byte
			// payload_raw: the SAME bytes, bound from the SAME slice. Asserting the value twice
			// is what pins the mechanism rather than merely the outcome — if a future edit
			// re-marshalled for one of the two body columns, this expectation would fail here at
			// the SQL boundary rather than silently producing a JSONB-normalised replay much
			// later.
			expectedPayload,
			// event_raw: the canonical envelope, with that same body spliced in unaltered. It is
			// written ONCE here and read by every later transport, so the bytes bound at this
			// boundary are the bytes a subscriber eventually receives — which is why the payload
			// member is compared exactly rather than the whole column being waved through.
			outboxEnvelopeCarrying{payload: expectedPayload},
			sqlmock.AnyArg(), // occurred_at: the domain instant, stamped at construction
			model.EventOutboxStatusPending,
			defaultEventMaxAttempts,
			// The two TRACE-CONTEXT columns, bound as SQL NULL because this capture runs on a
			// background context with no active span. That is the common production shape — a
			// CLI mutation, a worker-initiated rejection, observability disabled — and binding
			// nil rather than '' is what keeps "no trace was recorded" a single spelling that
			// the columns' CHECK constraints and any query filtering on them both agree about.
			nil,
			nil,
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(4242)))

	require.NoError(t, blnk.PublishEvent(context.Background(), event))
}

// TestPublishEvent_OpensNoTransactionOfItsOwn asserts an absence, and it is a
// load-bearing one.
//
// The standalone path must issue its insert on the connection pool.
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
			// partition_key: the source balance. This test supplies NO ledger, so the row
			// carries the payload-derived fallback; every production transaction capture
			// supplies the ledger and is keyed on that instead.
			outboxSourceBalanceID,
			// ledger_id: SQL NULL. model.Transaction HAS NO LEDGER FIELD, so there is no ledger
			// to record — and binding the source balance id here, as the single combined column
			// used to, put a balance id in a column called ledger_id where everything downstream
			// read it as a ledger.
			nil,
			"blnk.transactions",
			model.SchemaVersionV1,
			expectedPayload, // payload
			expectedPayload, // payload_raw: the same slice bound into both body columns
			// event_raw: the canonical envelope wrapping that same body verbatim.
			outboxEnvelopeCarrying{payload: expectedPayload},
			sqlmock.AnyArg(), // occurred_at: the domain instant, stamped at construction
			model.EventOutboxStatusPending,
			defaultEventMaxAttempts,
			// The two TRACE-CONTEXT columns, bound as SQL NULL because this capture runs on a
			// background context with no active span. That is the common production shape — a
			// CLI mutation, a worker-initiated rejection, observability disabled — and binding
			// nil rather than '' is what keeps "no trace was recorded" a single spelling that
			// the columns' CHECK constraints and any query filtering on them both agree about.
			nil,
			nil,
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

	t.Run("the failure line correlates by event id and hashes the aggregate", func(t *testing.T) {
		// This is a FAILURE path a broker outage or a database incident can make high-volume,
		// and the aggregate id is a ledger, balance, transaction or identity id — a financial
		// identifier naming whose money the event is about. It is hashed rather than printed:
		// event_id already identifies the event uniquely, so correlation loses nothing, and
		// the token still shows that several failures share one aggregate, which is all the
		// identifier was contributing.
		hook := logtest.NewGlobal()
		defer hook.Reset()

		datasource := newOutboxSpyDatasource()
		datasource.insertErr = sentinel
		blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

		require.Error(t, blnk.PublishEvent(context.Background(), NewWebhook{
			Event:   "transaction.applied",
			Payload: outboxSampleTransaction(StatusApplied),
		}))

		var reported bool
		for _, entry := range hook.AllEntries() {
			if entry.Level != logrus.ErrorLevel ||
				!strings.Contains(entry.Message, "failed to record event in the outbox") {
				continue
			}
			reported = true

			assert.Equal(t, hashLogIdentifier(outboxTransactionID), entry.Data["aggregate_id_hash"],
				"the aggregate must be present as a stable token, so failures can still be grouped")
			assert.NotContains(t, entry.Data, "aggregate_id",
				"the plaintext identifier must be gone, not merely accompanied by the hash")
			assert.NotEmpty(t, entry.Data["event_id"], "the line must identify the event that was lost")
			assert.Equal(t, "transaction.applied", entry.Data["event_type"])

			for field, value := range entry.Data {
				text, ok := value.(string)
				if !ok {
					continue
				}
				assert.NotContains(t, text, outboxTransactionID,
					"field %q must not carry the aggregate id in the clear", field)
				assert.NotContains(t, text, outboxSourceBalanceID,
					"field %q must not carry the ledger id in the clear either", field)
			}
		}
		require.True(t, reported,
			"a failed capture must be logged; silence would make a genuinely lost event invisible")
	})
}

// TestPublishEvent_DoesNotPanicWithoutADatasource asserts the two degenerate receivers
// a real deployment and the existing test suite both produce.
//
// NewBlnk(nil) is a supported construction — the legacy webhook tests use it — so a nil
// datasource must never CRASH.
func TestPublishEvent_DoesNotPanicWithoutADatasource(t *testing.T) {
	event := NewWebhook{Event: "balance.created", Payload: outboxSampleBalance()}

	t.Run("a nil datasource is a reported error, not a crash", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

		var err error
		require.NotPanics(t, func() { err = blnk.PublishEvent(context.Background(), event) })
		requireAPIErrorCode(t, err, apierror.ErrInternalServer)

		require.NotPanics(t, func() { err = blnk.PublishEventInTx(context.Background(), nil, event) })
		requireAPIErrorCode(t, err, apierror.ErrInternalServer)

		// The row is still built, which is what gives the log line an event identity an
		// operator can act on: "some event was dropped" is not a diagnosable message.
		var warned bool
		for _, entry := range hook.AllEntries() {
			if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, "no datasource") {
				assert.Equal(t, "balance.created", entry.Data["event_type"],
					"the log line must name the event type that was dropped")
				assert.Equal(t, "blnk.balances", entry.Data["topic"])
				assert.NotEmpty(t, entry.Data["event_id"], "the log line must name the event id")
				warned = true

				break
			}
		}
		assert.True(t, warned,
			"a dropped event must be visible: with publishing configured, this means events are being lost")
	})

	t.Run("the datasource really is absent, as NewBlnk(nil) leaves it", func(t *testing.T) {
		// NewBlnk(nil) — the construction the legacy webhook tests use — is what makes a nil
		// datasource a supported reality rather than a hypothetical. It is NOT called here,
		// deliberately: NewBlnk builds a Redis client whose constructor pings the server and
		// fails if it cannot reach it, and the payload guarantee this file proves has to hold
		// with no infrastructure at all.
		blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

		require.Nil(t, blnk.GetDataSource(),
			"the instance under test must carry no datasource, exactly as NewBlnk(nil) leaves it")

		// A CONFIGURED PUBLISHER WITH NO DATASOURCE IS AN ERROR, and this is the reverse of
		// what this test asserted before.
		var err error
		require.NotPanics(t, func() { err = blnk.PublishEvent(context.Background(), event) })
		require.Error(t, err,
			"publishing configured with no datasource drops every event; reporting success hides that from every caller")
		requireAPIErrorCode(t, err, apierror.ErrInternalServer)

		// The row is still constructible, so nothing about the absent datasource is
		// allowed to disturb the payload contract.
		row, prepareErr := blnk.PrepareEventOutbox(context.Background(), event)
		require.NoError(t, prepareErr)
		require.NotNil(t, row)
		assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(row.Payload))
	})

	t.Run("a nil receiver is a no-op", func(t *testing.T) {
		outboxStoreConfiguration(t, outboxPublishingConfiguration())

		var blnk *Blnk

		// A nil receiver must not PANIC — that is the property under test, and it matters
		// because the post-action hooks call these from goroutines where a panic takes the
		// process down rather than surfacing as an error. It is reported as an error for the
		// same reason the nil-datasource case is: publishing is configured and the event is
		// being dropped.
		var err error
		require.NotPanics(t, func() { err = blnk.PublishEvent(context.Background(), event) },
			"a nil receiver must not panic: post-action hooks call this from goroutines")
		require.Error(t, err, "a nil instance cannot capture the event, and saying so is the honest answer")

		require.NotPanics(t, func() { err = blnk.PublishEventInTx(context.Background(), nil, event) })
		require.Error(t, err, "the in-transaction entry point must honour the same contract")

		// The row is still constructible from the configuration store alone, which is what
		// the nil-receiver branch of the configuration read exists to provide.
		var row *model.EventOutbox
		var prepareErr error
		require.NotPanics(t, func() { row, prepareErr = blnk.PrepareEventOutbox(context.Background(), event) })
		require.NoError(t, prepareErr)
		require.NotNil(t, row)
		assert.Equal(t, "blnk.balances", row.Topic)
	})
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// outboxConcurrentWriters and outboxEventsPerWriter size the concurrency test.
const (
	outboxConcurrentWriters = 32
	outboxEventsPerWriter   = 8
)

// TestPublishEvent_IsSafeWhenCalledConcurrently asserts the entry point is safe to call
// from many goroutines at once, and that every call still produces a distinct event id.
//
// Run under -race this covers the data-race question.
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
				// A distinct source per writer, so the goroutines also exercise concurrent
				// derivation of DIFFERENT partition keys rather than all hitting one.
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
		assert.NotEmpty(t, row.PartitionKey, "no concurrently published row may lose its partition key")

		_, duplicate := unique[row.EventID]
		assert.False(t, duplicate, "event_id %q was issued twice under concurrency", row.EventID)
		unique[row.EventID] = struct{}{}
	}
	assert.Len(t, unique, len(rows), "every concurrently issued event_id must be distinct")
}

// TestPrepareEventOutbox_IsSafeWhenCalledConcurrently covers pure row construction
// under concurrency, with no datasource involved at all.
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
			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
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

	// The identity assertion is per FIXTURE, not per row, because a derived id is a pure
	// function of the event: two writers preparing the same fixture must agree on the id,
	// and two writers preparing different fixtures must not. Asserting "every row is
	// distinct" would now be asserting the opposite of the idempotency contract — while
	// still catching nothing about concurrency, since the derivation reads no shared
	// state.
	byFixture := make(map[string]map[string]struct{}, len(fixtures))
	for index, row := range rows {
		require.NotNil(t, row, "row %d must have been built", index)
		assert.Equal(t, model.EventOutboxStatusPending, row.Status)
		assert.NotEmpty(t, row.PartitionKey, "no concurrently built row may lose its partition key")
		assert.NotEmpty(t, row.EventID, "no concurrently built row may lose its event id")

		if byFixture[row.EventType] == nil {
			byFixture[row.EventType] = make(map[string]struct{}, 1)
		}
		byFixture[row.EventType][row.EventID] = struct{}{}
	}

	for eventType, ids := range byFixture {
		if model.EventTypeIsRepeatable(eventType) {
			// A repeatable event gets a fresh id per occurrence, so every writer that
			// produced one must have produced a different id.
			assert.Len(t, ids, countRowsOfType(rows, eventType),
				"%s repeats, so concurrent construction must produce one distinct id per occurrence", eventType)

			continue
		}

		assert.Len(t, ids, 1,
			"%s is derived from the mutation, so every concurrent preparation of the same mutation must agree on one id", eventType)
	}
}

// countRowsOfType counts the prepared rows carrying one event type.
func countRowsOfType(rows []*model.EventOutbox, eventType string) int {
	count := 0
	for _, row := range rows {
		if row != nil && row.EventType == eventType {
			count++
		}
	}

	return count
}

// ---------------------------------------------------------------------------
// Structural invariant
// ---------------------------------------------------------------------------

// TestEventOutboxSource_ImportsNoKafkaClient pins the invariant the file under test
// states about itself: nothing on the producer's path talks to a broker.
//
// It matters because the alternative is silently plausible.
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
// The payload contract has to be provable with no infrastructure, because that is the
// only way it stays provable in every environment the suite runs in.
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
// config.ConfigStore is a process-global atomic.Value.
func TestPrepareEventOutbox_LeaksNoConfigurationBetweenTests(t *testing.T) {
	const sentinelPrefix = "outbox-leak-sentinel"

	before := config.ConfigStore.Load()

	t.Run("a subtest publishes a recognisable configuration", func(t *testing.T) {
		configuration := outboxPublishingConfiguration()
		configuration.Kafka.TopicPrefix = sentinelPrefix
		blnk := newOutboxBlnk(t, configuration, nil)

		row := mustPrepareEventOutbox(t, blnk, NewWebhook{
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

// ---------------------------------------------------------------------------------------
// The size ceiling on the wire. The rest of this file proves what the stored payload IS.
// ---------------------------------------------------------------------------------------

// sizeLimitPublisher builds a real Kafka-backed publisher without contacting a broker.
func sizeLimitPublisher(t *testing.T) *kafkaPublisher {
	t.Helper()

	publisher, err := newKafkaPublisher([]string{"localhost:9092"}, config.KafkaConfig{
		Brokers:          []string{"localhost:9092"},
		TopicPrefix:      DefaultTopicPrefix,
		InsecureLocalDev: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Close() })

	return publisher
}

// sizedLedgerEvent builds a valid event whose payload is exactly payloadBytes long.
//
// The payload is a JSON string of filler, so it is genuinely valid JSON — an invalid payload
// would fail marshalling first and the test would pass for the wrong reason.
func sizedLedgerEvent(payloadBytes int) model.LedgerEvent {
	// Two bytes of the payload are the quotes around the filler.
	filler := strings.Repeat("x", payloadBytes-2)

	return model.LedgerEvent{
		EventID:       uuid.NewString(),
		EventType:     "transaction.applied",
		AggregateID:   "txn_size_limit",
		OccurredAt:    time.Now().UTC(),
		Payload:       json.RawMessage(`"` + filler + `"`),
		SchemaVersion: model.SchemaVersionV1,
	}
}

// TestPublishToTopic_RefusesAnOversizedEnvelopeAsPermanent is the guard on the wire.
//
// Persistence validates the PAYLOAD it is handed, which is the right place to reject a
// caller's oversized data.
//
// The failure must be PERMANENT.
func TestPublishToTopic_RefusesAnOversizedEnvelopeAsPermanent(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	publisher := sizeLimitPublisher(t)
	event := sizedLedgerEvent(model.MaxEventMessageBytes)

	result, err := publisher.PublishToTopic(context.Background(), PublishRequest{Event: event})
	require.Error(t, err, "an envelope over the ceiling must be refused")

	assert.ErrorIs(t, err, ErrEventMessageTooLarge,
		"the refusal must be recognisable without matching message text")
	assert.Equal(t, model.PublishStatusRetrying, result.Status,
		"the per-attempt outcome vocabulary is the three values requirement R-3 names, so a failed "+
			"attempt reports retrying whatever its classification. It must NOT report dead_lettered "+
			"— nothing has been written to a `.dlt` sibling at this point, and for an oversized "+
			"event the strictly larger dead-letter copy may never be writable at all, so claiming "+
			"it here would count a preservation that never happened. The PERMANENT distinction is "+
			"carried on the result's classification fields and is asserted immediately below")
	assert.True(t, result.Classified,
		"and the publisher must record that it REACHED a verdict, because that marker is what "+
			"lets PermanentFailure end this event's life early while an unclassified failure from "+
			"some other implementation still gets its retry budget")
	assert.False(t, result.Transient,
		"the verdict itself: a message that can never fit is not a transient condition")
	assert.False(t, result.Retryable,
		"and nothing further will be tried for it, whatever budget the row states")
	assert.True(t, result.PermanentFailure(),
		"the relay reads this predicate to take the row straight to its terminal state: an "+
			"oversized message must not spend five broker round trips proving it cannot shrink")
	assert.False(t, result.Dispatched(), "nothing was published")
	assert.False(t, result.Transient,
		"an oversized message is permanent: no retry and no broker state can make it fit")
	assert.False(t, IsTransientPublishError(err),
		"and the relay must read the same classification from the error it is handed")
	assert.Contains(t, err.Error(), strconv.Itoa(model.MaxEventMessageBytes),
		"the operator must be told the limit, not just that one was exceeded")
}

// TestPublishToTopic_AcceptsAnEnvelopeInsideTheCeiling is the other side of the
// boundary.
//
// A ceiling that also rejected ordinary events would be a availability defect dressed
// as a safety check, so the largest payload that fits must still be accepted.
func TestPublishToTopic_AcceptsAnEnvelopeInsideTheCeiling(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	publisher := sizeLimitPublisher(t)

	// Sized so the whole envelope — the payload plus its five sibling members — lands
	// EXACTLY on the ceiling, which is the largest message the check accepts.
	event := sizedLedgerEvent(1024)
	probe, err := event.CanonicalBytes()
	require.NoError(t, err, "the probe envelope must serialise before its overhead can be measured")

	overhead := len(probe) - len(event.Payload)
	// Two of the payload's bytes are the quotes around the filler, as in sizedLedgerEvent.
	event.Payload = json.RawMessage(`"` + strings.Repeat("x", model.MaxEventMessageBytes-overhead-2) + `"`)

	atCeiling, err := event.CanonicalBytes()
	require.NoError(t, err)
	require.Len(t, atCeiling, model.MaxEventMessageBytes,
		"the fixture must sit exactly on the ceiling, or it is not testing the boundary")

	// The write is expected to fail — there is no broker this test may rely on — and the
	// deadline is what keeps that failure fast and identical whether or not something is
	// listening on the port. What matters is only WHICH failure: any transport or deadline
	// error is fine, the size refusal is not.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err = publisher.PublishToTopic(ctx, PublishRequest{Event: event}); err != nil {
		assert.NotErrorIs(t, err, ErrEventMessageTooLarge,
			"an envelope inside the ceiling must not be refused for its size")
	}
}

// TestPublishToTopic_RefusesATopicOutsideTheOwnedNamespace is the guard reached through
// the publish entry point rather than through writerFor directly.
//
// This is the realistic shape of the attack: the destination arrives on a
// PublishRequest built from a stored row, so what matters is that the whole path
// refuses it, not merely that an internal helper would have.
func TestPublishToTopic_RefusesATopicOutsideTheOwnedNamespace(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	publisher := sizeLimitPublisher(t)

	result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
		Event: model.LedgerEvent{
			EventID:       uuid.NewString(),
			EventType:     "transaction.applied",
			AggregateID:   "txn_foreign_topic",
			OccurredAt:    time.Now().UTC(),
			Payload:       json.RawMessage(`{"event":"transaction.applied","data":{}}`),
			SchemaVersion: model.SchemaVersionV1,
		},
		Topic: "attacker.transactions",
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTopicNotOwned)
	assert.False(t, result.Transient,
		"a destination Blnk may not write to is permanent: no retry makes the write legitimate")
	assert.False(t, IsTransientPublishError(err))
}

// ---------------------------------------------------------------------------
// The mock must not hide the event rows it is handed
// ---------------------------------------------------------------------------

// TestMockDataSource_CapturesTheEventRowsHandedToTheAtomicWriters closes the last half
// of the coverage gap this closes: database/mocks/repo_mocks.go dropping the variadic event
// rows so no test could see them.
//
// That design decision is exactly what makes this test necessary.
func TestMockDataSource_CapturesTheEventRowsHandedToTheAtomicWriters(t *testing.T) {
	transaction := &model.Transaction{TransactionID: "txn_0f6e2c8a", Status: "APPLIED"}

	firstRow := &model.EventOutbox{
		EventID:      "0f6e2c8a-1b4d-4e9f-8a7c-2d5b6e3f1a9c",
		EventType:    "transaction.applied",
		AggregateID:  transaction.TransactionID,
		PartitionKey: "ldg_4b1e7c30",
		Topic:        "blnk.transactions",
		Payload:      json.RawMessage(`{"event":"transaction.applied","data":{}}`),
	}
	secondRow := &model.EventOutbox{
		EventID:      "1a7c2d5b-6e3f-4a9c-8f6e-2c8a1b4d4e9f",
		EventType:    "transaction.queued",
		AggregateID:  "txn_1a7c2d5b",
		PartitionKey: "ldg_4b1e7c30",
		Topic:        "blnk.transactions",
		Payload:      json.RawMessage(`{"event":"transaction.queued","data":{}}`),
	}

	t.Run("the single-transaction writer", func(t *testing.T) {
		datasource := new(mocks.MockDataSource)

		// FIVE arguments, deliberately: this is the shape every existing expectation in the
		// repository uses, and it must keep matching while the sixth variadic parameter is
		// populated. If someone forwards the variadic into m.Called, this expectation stops
		// matching and the call panics — which is the regression this arrangement exists to
		// prevent, caught here rather than in an unrelated benchmark.
		datasource.On("RecordTransactionWithBalancesAndOutbox",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(transaction, nil)

		stored, err := datasource.RecordTransactionWithBalancesAndOutbox(
			context.Background(), transaction, nil, nil, nil, firstRow, secondRow)
		require.NoError(t, err)
		require.NotNil(t, stored)

		captured := datasource.CapturedEventOutboxes()
		require.Len(t, captured, 2, "both rows handed to the writer must be visible")
		assert.Equal(t, firstRow.EventID, captured[0].EventID)
		assert.Equal(t, secondRow.EventID, captured[1].EventID,
			"the order rows were passed in must be preserved, because it is the order they "+
				"would be inserted and therefore claimed")
		assert.Equal(t, "blnk.transactions", captured[0].Topic)
		assert.Equal(t, "ldg_4b1e7c30", captured[0].PartitionKey)

		datasource.AssertExpectations(t)
	})

	t.Run("the batch writer", func(t *testing.T) {
		datasource := new(mocks.MockDataSource)
		datasource.On("RecordTransactionsWithBalancesAndOutboxes",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.Transaction{transaction}, nil)

		_, err := datasource.RecordTransactionsWithBalancesAndOutboxes(
			context.Background(), []*model.Transaction{transaction}, nil, nil, nil, firstRow)
		require.NoError(t, err)

		require.Len(t, datasource.CapturedEventOutboxes(), 1)
		datasource.AssertExpectations(t)
	})

	t.Run("a nil row is not recorded", func(t *testing.T) {
		// A nil row is what the atomic writers legitimately receive when publishing is
		// unconfigured, and recording it would put a nil into the slice that every reader of
		// the accessor would then have to guard against.
		datasource := new(mocks.MockDataSource)
		datasource.On("RecordTransactionWithBalancesAndOutbox",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(transaction, nil)

		_, err := datasource.RecordTransactionWithBalancesAndOutbox(
			context.Background(), transaction, nil, nil, nil, nil)
		require.NoError(t, err)

		assert.Empty(t, datasource.CapturedEventOutboxes(),
			"a nil row must be skipped rather than recorded")
	})

	t.Run("no rows at all leaves the record empty", func(t *testing.T) {
		// The out-of-scope callers that pass no event rows at all must keep working, and
		// must not appear to have passed one.
		datasource := new(mocks.MockDataSource)
		datasource.On("RecordTransactionWithBalancesAndOutbox",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(transaction, nil)

		_, err := datasource.RecordTransactionWithBalancesAndOutbox(
			context.Background(), transaction, nil, nil, nil)
		require.NoError(t, err)

		assert.Empty(t, datasource.CapturedEventOutboxes())
		datasource.AssertExpectations(t)
	})

	t.Run("the record can be reset for a reused mock", func(t *testing.T) {
		datasource := new(mocks.MockDataSource)
		datasource.On("RecordTransactionWithBalancesAndOutbox",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(transaction, nil)

		_, err := datasource.RecordTransactionWithBalancesAndOutbox(
			context.Background(), transaction, nil, nil, nil, firstRow)
		require.NoError(t, err)
		require.Len(t, datasource.CapturedEventOutboxes(), 1)

		datasource.ResetCapturedEventOutboxes()
		assert.Empty(t, datasource.CapturedEventOutboxes(),
			"a test that drives the same mock twice must be able to separate the two runs")
	})

	t.Run("the accessor returns a copy", func(t *testing.T) {
		// Otherwise a caller mutating the returned slice would corrupt the record, and the
		// second assertion in a test would be reading something the first assertion wrote.
		datasource := new(mocks.MockDataSource)
		datasource.On("RecordTransactionWithBalancesAndOutbox",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(transaction, nil)

		_, err := datasource.RecordTransactionWithBalancesAndOutbox(
			context.Background(), transaction, nil, nil, nil, firstRow)
		require.NoError(t, err)

		captured := datasource.CapturedEventOutboxes()
		require.Len(t, captured, 1)
		captured[0] = nil

		again := datasource.CapturedEventOutboxes()
		require.Len(t, again, 1)
		assert.NotNil(t, again[0], "the accessor must hand out a copy, not the record itself")
	})
}

// TestWithEventLedgerID_SuppliesWhatThePayloadCannotYield covers the one option
// PrepareEventOutbox accepts.
//
// It records the ledger AND it becomes the partition key.
func TestWithEventLedgerID_SuppliesWhatThePayloadCannotYield(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
	event := NewWebhook{Event: "transaction.queued", Payload: outboxSampleTransaction(StatusQueued)}

	t.Run("without it a transaction event has no ledger and is keyed on its source balance", func(t *testing.T) {
		row := mustPrepareEventOutbox(t, blnk, event)
		require.NotNil(t, row)

		assert.Empty(t, row.LedgerID,
			"a transaction payload carries no ledger, and a fabricated one is worse than none")
		assert.Equal(t, outboxSourceBalanceID, row.PartitionKey,
			"the fallback key is the source balance — the same identifier the transaction queue "+
				"shards on, though not the same partition, since the two hash different values")
	})

	t.Run("with it the ledger is recorded and becomes the key", func(t *testing.T) {
		row := mustPrepareEventOutbox(t, blnk, event, WithEventLedgerID(outboxLedgerID))
		require.NotNil(t, row)

		assert.Equal(t, outboxLedgerID, row.LedgerID,
			"the supplied ledger is the authoritative one and must be recorded as such")
		assert.Equal(t, outboxLedgerID, row.PartitionKey,
			"requirement R-6 partitions by ledger id, and a supplied ledger takes the same precedence a derived one already takes")
	})

	t.Run("a blank or whitespace-only value is ignored rather than stored", func(t *testing.T) {
		// A whitespace key hashes to a different partition from an empty one, so storing it
		// would split one ledger's events across two partitions — the precise failure the
		// option exists to prevent.
		for _, blank := range []string{"", "   ", "\t\n"} {
			row := mustPrepareEventOutbox(t, blnk, event, WithEventLedgerID(blank))
			require.NotNil(t, row)

			assert.Empty(t, row.LedgerID, "%q must read as absence", blank)
			assert.Equal(t, outboxSourceBalanceID, row.PartitionKey,
				"%q must leave the derived key alone", blank)
		}
	})

	t.Run("the last non-blank option wins and a nil option is skipped", func(t *testing.T) {
		row := mustPrepareEventOutbox(t, blnk, event,
			WithEventLedgerID("ldg_first"), nil, WithEventLedgerID(outboxLedgerID))
		require.NotNil(t, row, "a nil option must not panic: the list is assembled at call sites that may build it conditionally")

		assert.Equal(t, outboxLedgerID, row.LedgerID)
	})

	t.Run("it does not override a ledger the payload already carries correctly", func(t *testing.T) {
		// A balance payload yields its own ledger, and the two agree in every real call.
		// Asserting the supplied value still wins is what keeps the precedence rule ONE rule
		// rather than one per payload type.
		balanceEvent := NewWebhook{Event: "balance.created", Payload: outboxSampleBalance()}

		derived := mustPrepareEventOutbox(t, blnk, balanceEvent)
		require.NotNil(t, derived)
		assert.Equal(t, outboxLedgerID, derived.LedgerID, "a balance payload yields its ledger unaided")

		supplied := mustPrepareEventOutbox(t, blnk, balanceEvent, WithEventLedgerID("ldg_explicit"))
		require.NotNil(t, supplied)
		assert.Equal(t, "ldg_explicit", supplied.LedgerID,
			"the caller is the authority on which ledger the mutation belonged to")
		assert.Equal(t, "ldg_explicit", supplied.PartitionKey)
	})

	t.Run("the aggregate id is unaffected", func(t *testing.T) {
		// aggregate_id answers "what is this event about" and the option answers "where does
		// it belong". Letting the option move the aggregate would change what a consumer
		// groups by, which is not what supplying a ledger states.
		without := mustPrepareEventOutbox(t, blnk, event)
		with := mustPrepareEventOutbox(t, blnk, event, WithEventLedgerID(outboxLedgerID))
		require.NotNil(t, without)
		require.NotNil(t, with)

		assert.Equal(t, without.AggregateID, with.AggregateID,
			"the option states the ledger, not the subject")
		assert.Equal(t, outboxTransactionID, with.AggregateID)
	})

	t.Run("the event id is unaffected, so an opted-in call site stays idempotent", func(t *testing.T) {
		// The derived id is a function of the mutation identity, the event type and the
		// schema version. If the ledger entered it, wiring the option at a call site would
		// change every future event id for events already delivered, and a subscriber's
		// idempotency store would stop recognising a replayed event.
		without := mustPrepareEventOutbox(t, blnk, event)
		with := mustPrepareEventOutbox(t, blnk, event, WithEventLedgerID(outboxLedgerID))
		require.NotNil(t, without)
		require.NotNil(t, with)

		assert.Equal(t, without.EventID, with.EventID,
			"supplying the ledger must not change the idempotency key of an event already delivered")
	})
}

// TestPrepareTransactionEventOutbox_CapturesTheEventForTheAtomicWriter is the test for
// the transaction family: it asserts that the event row exists BEFORE persistence and
// therefore has something to commit inside.
func TestPrepareTransactionEventOutbox_CapturesTheEventForTheAtomicWriter(t *testing.T) {
	const ledgerID = "ldg_9c1f4a20"

	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	sourceBalance := &model.Balance{BalanceID: "bln_source_9c1f", LedgerID: ledgerID, Currency: "USD"}
	destinationBalance := &model.Balance{BalanceID: "bln_dest_9c1f", LedgerID: ledgerID, Currency: "USD"}

	newTransaction := func() *model.Transaction {
		return &model.Transaction{
			TransactionID: "txn_9c1f4a20",
			Source:        sourceBalance.BalanceID,
			Destination:   destinationBalance.BalanceID,
			Amount:        25,
			PreciseAmount: big.NewInt(2500),
			Precision:     100,
			Currency:      "USD",
			Status:        StatusQueued,
			CreatedAt:     time.Now().UTC(),
		}
	}

	prepare := func(t *testing.T, instance *Blnk, source, destination *model.Balance) (*model.EventOutbox, *model.Transaction) {
		t.Helper()

		// The work builder runs first because it is what finalises the status, and the event
		// name is derived from the FINAL status. Preparing from the pre-update transaction
		// would announce transaction.queued for a transaction that committed as APPLIED.
		work, skip := instance.buildTransactionExecutionWork(context.Background(), newTransaction(), source, destination)
		require.False(t, skip, "a non-zero amount must be persisted")

		row, err := instance.prepareTransactionEventOutbox(context.Background(), work.transaction, source, destination)
		require.NoError(t, err, "a well-formed transaction payload must serialise")

		return row, work.transaction
	}

	t.Run("the row is built before persistence", func(t *testing.T) {
		row, transaction := prepare(t, blnk, sourceBalance, destinationBalance)
		require.NotNil(t, row,
			"the event row must be prepared BEFORE persistence; a nil row here means the event "+
				"can only be captured after the commit, which is what requirement R-2 forbids")

		assert.Equal(t, getEventFromStatus(transaction.Status), row.EventType,
			"the event name must be derived from the FINAL status, after updateTransactionDetails")
		assert.Equal(t, transaction.TransactionID, row.AggregateID)
		assert.Equal(t, model.EventOutboxStatusPending, row.Status)
	})

	t.Run("the ledger is resolved from the balances, not supplied by the caller", func(t *testing.T) {
		row, _ := prepare(t, blnk, sourceBalance, destinationBalance)
		require.NotNil(t, row)

		assert.Equal(t, ledgerID, row.LedgerID,
			"a transaction payload carries no ledger, so the capture path must take it from the "+
				"loaded source balance; an empty value is requirement R-6 unsatisfied at the producer")
		assert.Equal(t, ledgerID, row.PartitionKey,
			"and it must become the partition key, because that is the Kafka message key")
		assert.Equal(t, ledgerID, PublishRequestFromOutbox(*row, 1).Key,
			"end to end: the message a subscriber receives must be keyed by the ledger")
	})

	t.Run("the destination ledger is used when the source names none", func(t *testing.T) {
		row, _ := prepare(t, blnk, &model.Balance{BalanceID: sourceBalance.BalanceID}, destinationBalance)
		require.NotNil(t, row)

		assert.Equal(t, ledgerID, row.PartitionKey,
			"a transfer whose source balance carries no ledger must still be keyed by the ledger "+
				"the destination names, rather than falling through to a balance id")
	})

	t.Run("a zero-amount transaction is never persisted, so nothing is prepared for it", func(t *testing.T) {
		transaction := newTransaction()
		transaction.PreciseAmount = big.NewInt(0)

		work, skip := blnk.buildTransactionExecutionWork(context.Background(), transaction, sourceBalance, destinationBalance)
		require.True(t, skip, "a zero-amount transaction is discarded rather than persisted")

		// The skip arm returns before the persistence step that prepares the event, so no row
		// is ever built: there is no mutation for an event to describe, and capturing one
		// would announce a transaction that does not exist.
		assert.Nil(t, work.outbox,
			"nothing at all is prepared for a transaction that is not persisted")
	})

	t.Run("no brokers configured captures nothing and fails nothing", func(t *testing.T) {
		unconfigured := newOutboxBlnk(t, &config.Configuration{}, nil)

		row, _ := prepare(t, unconfigured, sourceBalance, destinationBalance)
		assert.Nil(t, row,
			"the no-op-when-unconfigured contract inherited from SendWebhook must survive: with no "+
				"transport there is nothing to capture and nothing to fail")
	})
}

// TestPersistSingleTransactionExecutionWork_HandsTheEventRowToTheWriter proves the
// second half of same-transaction capture for the transaction family: the prepared row
// actually reaches the
// writer that commits it.
//
// The row is NOT injected.
func TestPersistSingleTransactionExecutionWork_HandsTheEventRowToTheWriter(t *testing.T) {
	transaction := &model.Transaction{
		TransactionID: "txn_5b0d7e14",
		Source:        "bln_source_5b0d",
		Destination:   "bln_dest_5b0d",
		PreciseAmount: big.NewInt(2500),
		Precision:     100,
		Currency:      "USD",
		Status:        StatusApplied,
		CreatedAt:     time.Now().UTC(),
	}
	sourceBalance := &model.Balance{BalanceID: transaction.Source, LedgerID: "ldg_5b0d7e14", Currency: "USD"}
	destinationBalance := &model.Balance{BalanceID: transaction.Destination, LedgerID: "ldg_5b0d7e14", Currency: "USD"}

	datasource := new(mocks.MockDataSource)
	datasource.On("RecordTransactionWithBalancesAndOutbox",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(transaction, nil)
	// The persistence path now resolves each balance's monitors BEFORE the write, so that
	// a threshold alert commits in the same transaction as the movement that crossed it.
	// No monitor is configured here, so no alert row is produced and this test still
	// asserts exactly one event row — see
	// TestPersistSingleTransactionExecutionWork_CommitsMonitorAlertsWithTheMovement for
	// the case where one is.
	datasource.On("GetBalanceMonitors", mock.Anything).Return([]model.BalanceMonitor{}, nil)

	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

	persisted, eventCaptured, capturedMonitors, err := blnk.persistSingleTransactionExecutionWork(context.Background(), queuedBatchPostCommitWork{
		transaction:        transaction,
		sourceBalance:      sourceBalance,
		destinationBalance: destinationBalance,
	})
	require.NoError(t, err)
	assert.True(t, capturedMonitors.covers(sourceBalance.BalanceID),
		"both balances were EVALUATED before the write, so the post-commit check must skip them: "+
			"it would re-read the same monitor list and re-evaluate the same conditions against the "+
			"same values, and conclude nothing new at the cost of a read per balance")
	assert.True(t, capturedMonitors.covers(destinationBalance.BalanceID))
	assert.False(t, capturedMonitors.holds(sourceBalance.BalanceID, "mon_absent"),
		"no monitor was configured, so no CROSSING was captured — covered and captured are "+
			"different facts and this test is the one that keeps them apart")
	require.NotNil(t, persisted.transaction)
	assert.True(t, eventCaptured,
		"the writer committed the event, so the post-commit hook must be told not to capture it "+
			"again; a second capture is refused by the unique index and reported as a failure of a "+
			"transaction that in fact succeeded")

	captured := datasource.CapturedEventOutboxes()
	require.Len(t, captured, 1,
		"the prepared event row must reach the atomic writer; preparing it and not passing it "+
			"loses exactly as many events as capturing after the commit did")
	assert.Equal(t, "transaction.applied", captured[0].EventType)
	assert.Equal(t, sourceBalance.LedgerID, captured[0].LedgerID)

	datasource.AssertExpectations(t)
}

// TestPersistSingleTransactionExecutionWork_CommitsMonitorAlertsWithTheMovement is the
// test for balance.monitor, and it closes that finding critical case.
//
//   - The alert row REACHES THE WRITER, alongside the transaction event.
//   - It is keyed on the LEDGER, not on the monitored balance. model.BalanceMonitor
//     carries no ledger, so without the explicit option the row would key on the
//     balance and store NULL — the requirement partitions by ledger id.
//   - The transaction event is still present and still exactly one. The cardinality
//     invariant that refuses two mutation-describing rows for one transaction must not
//     have been widened into one that refuses nothing.
//   - The covered monitor is REPORTED BACK, because that set is the only thing stopping
//     the post-commit path from publishing the same alert a second time. A
//     balance.monitor id is a fresh UUID by design, so no subscriber-side idempotency
//     could collapse a duplicate pair.
func TestPersistSingleTransactionExecutionWork_CommitsMonitorAlertsWithTheMovement(t *testing.T) {
	transaction := &model.Transaction{
		TransactionID: "txn_f03a1c77",
		Source:        "bln_source_f03a",
		Destination:   "bln_dest_f03a",
		PreciseAmount: big.NewInt(5000),
		Precision:     100,
		Currency:      "USD",
		Status:        StatusApplied,
		CreatedAt:     time.Now().UTC(),
	}
	// The balances carry the values the writer is about to persist, which is the state
	// model.UpdateBalances has already produced by the time persistence is reached. The
	// condition below is evaluated against exactly these values.
	sourceBalance := &model.Balance{
		BalanceID: transaction.Source,
		LedgerID:  "ldg_f03a1c77",
		Currency:  "USD",
		Balance:   big.NewInt(-9000),
	}
	destinationBalance := &model.Balance{
		BalanceID: transaction.Destination,
		LedgerID:  "ldg_f03a1c77",
		Currency:  "USD",
		Balance:   big.NewInt(9000),
	}

	// One monitor, on the SOURCE balance only, whose condition the post-movement value
	// meets. Scoping it to one balance is deliberate: it proves the captured set names the
	// monitor that actually fired rather than every monitor the transaction touched.
	firing := model.BalanceMonitor{
		MonitorID: "mon_f03a1c77",
		BalanceID: sourceBalance.BalanceID,
		Condition: model.AlertCondition{
			Field:        "balance",
			Operator:     "<",
			PreciseValue: big.NewInt(-5000),
		},
	}
	require.True(t, firing.CheckCondition(sourceBalance),
		"the fixture must actually satisfy its condition, or this test would pass by capturing nothing")

	datasource := new(mocks.MockDataSource)
	datasource.On("GetBalanceMonitors", sourceBalance.BalanceID).
		Return([]model.BalanceMonitor{firing}, nil)
	datasource.On("GetBalanceMonitors", destinationBalance.BalanceID).
		Return([]model.BalanceMonitor{}, nil)
	datasource.On("RecordTransactionWithBalancesAndOutbox",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(transaction, nil)

	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

	_, eventCaptured, capturedMonitors, err := blnk.persistSingleTransactionExecutionWork(
		context.Background(), queuedBatchPostCommitWork{
			transaction:        transaction,
			sourceBalance:      sourceBalance,
			destinationBalance: destinationBalance,
		})
	require.NoError(t, err)
	assert.True(t, eventCaptured)

	captured := datasource.CapturedEventOutboxes()
	require.Len(t, captured, 2,
		"the writer must receive BOTH the transaction event and the alert the movement triggered; "+
			"one row means the alert is back outside the transaction and the loss window is back with it")

	byType := make(map[string]*model.EventOutbox, len(captured))
	for _, row := range captured {
		byType[row.EventType] = row
	}

	transactionEvent, ok := byType["transaction.applied"]
	require.True(t, ok, "the transaction's own event must still be captured; rows: %v", byType)
	assert.Equal(t, sourceBalance.LedgerID, transactionEvent.LedgerID)

	alert, ok := byType[model.EventTypeBalanceMonitor]
	require.True(t, ok, "the balance.monitor alert must be in the same write; rows: %v", byType)
	assert.Equal(t, sourceBalance.LedgerID, alert.LedgerID,
		"the alert must be keyed on the LEDGER: model.BalanceMonitor carries no ledger, so without "+
			"the explicit option this column is NULL and R-6's per-ledger ordering is lost")
	assert.Contains(t, string(alert.Payload), firing.MonitorID,
		"the payload must be the monitor object the legacy transport received, unchanged")

	assert.True(t, capturedMonitors.holds(sourceBalance.BalanceID, firing.MonitorID),
		"exactly the crossing that fired must be reported back, so the post-commit path skips it "+
			"and publishes nothing twice. A balance.monitor id is a fresh UUID by design, so no "+
			"subscriber-side idempotency on event_id could collapse a duplicate pair")
	assert.False(t, capturedMonitors.holds(destinationBalance.BalanceID, firing.MonitorID),
		"and the crossing is keyed on BOTH halves: one balance can carry several monitors and one "+
			"monitor names one balance, so a balance-only key would let a sibling's crossing be "+
			"mistaken for this one and dropped by both routes")
	assert.True(t, capturedMonitors.covers(destinationBalance.BalanceID),
		"the destination's monitors were read and evaluated too — it simply had none that fired, "+
			"which is a skip rather than a capture")

	datasource.AssertExpectations(t)
}

// TestPersistSingleTransactionExecutionWork_FallsBackWhenMonitorsCannotBeReadRatherThanRefusing
// is the other half of the failure direction.
//
// A monitor lookup that fails must NOT refuse the transaction, and the reason is a
// scope boundary rather than a preference.
func TestPersistSingleTransactionExecutionWork_FallsBackWhenMonitorsCannotBeReadRatherThanRefusing(t *testing.T) {
	transaction := &model.Transaction{
		TransactionID: "txn_f03b2d88",
		Source:        "bln_source_f03b",
		Destination:   "bln_dest_f03b",
		PreciseAmount: big.NewInt(1500),
		Precision:     100,
		Currency:      "USD",
		Status:        StatusApplied,
		CreatedAt:     time.Now().UTC(),
	}
	sourceBalance := &model.Balance{BalanceID: transaction.Source, LedgerID: "ldg_f03b2d88", Currency: "USD", Balance: big.NewInt(-100)}
	destinationBalance := &model.Balance{BalanceID: transaction.Destination, LedgerID: "ldg_f03b2d88", Currency: "USD", Balance: big.NewInt(100)}

	monitorErr := errors.New("monitor lookup unavailable")

	datasource := new(mocks.MockDataSource)
	datasource.On("GetBalanceMonitors", sourceBalance.BalanceID).
		Return([]model.BalanceMonitor(nil), monitorErr)
	datasource.On("GetBalanceMonitors", destinationBalance.BalanceID).
		Return([]model.BalanceMonitor{}, nil)
	datasource.On("RecordTransactionWithBalancesAndOutbox",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(transaction, nil)

	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

	_, eventCaptured, capturedMonitors, err := blnk.persistSingleTransactionExecutionWork(
		context.Background(), queuedBatchPostCommitWork{
			transaction:        transaction,
			sourceBalance:      sourceBalance,
			destinationBalance: destinationBalance,
		})

	require.NoError(t, err,
		"a movement whose monitors cannot be read must still commit: this change substitutes a "+
			"transport and may not turn a monitor-store outage into a ledger outage")
	assert.True(t, eventCaptured,
		"the transaction's own event was captured with the write, which the monitor read has no "+
			"bearing on")

	assert.False(t, capturedMonitors.covers(sourceBalance.BalanceID),
		"THE BALANCE WHOSE MONITORS COULD NOT BE READ MUST NOT BE CLAIMED AS COVERED. Claiming it "+
			"would suppress the post-commit evaluation as well, so a crossing would be examined by "+
			"neither route — strictly worse than the loss window this path exists to narrow")
	assert.True(t, capturedMonitors.covers(destinationBalance.BalanceID),
		"and degradation is per balance, not per transaction: the sibling whose read succeeded is "+
			"still covered, so one unreadable monitor set does not send the whole write back")

	captured := datasource.CapturedEventOutboxes()
	require.Len(t, captured, 1,
		"exactly the transaction event, and no alert: no monitor was evaluated for the source, so "+
			"no crossing could be captured for it")
	assert.Equal(t, "transaction.applied", captured[0].EventType)

	datasource.AssertExpectations(t)
}

// TestPersistSingleTransactionExecutionWork_RefusesToCommitAnUncapturableEvent is the
// A mutation whose event cannot be prepared must not be written at all.
//
// The only way preparation can fail is a payload that will not serialise, which is a
// producer defect.
//
// The unserialisable payload is a channel in the transaction's metadata. encoding/json
// refuses it, which is exactly the failure the production code can encounter, and it is
// reached without any database or broker being involved.
func TestPersistSingleTransactionExecutionWork_RefusesToCommitAnUncapturableEvent(t *testing.T) {
	transaction := &model.Transaction{
		TransactionID: "txn_c3f1a208",
		Source:        "bln_source_c3f1",
		Destination:   "bln_dest_c3f1",
		PreciseAmount: big.NewInt(1000),
		Precision:     100,
		Currency:      "USD",
		Status:        StatusApplied,
		CreatedAt:     time.Now().UTC(),
		MetaData:      map[string]interface{}{"unserialisable": make(chan struct{})},
	}
	sourceBalance := &model.Balance{BalanceID: transaction.Source, LedgerID: "ldg_c3f1a208", Currency: "USD"}
	destinationBalance := &model.Balance{BalanceID: transaction.Destination, LedgerID: "ldg_c3f1a208", Currency: "USD"}

	// No expectation is registered on the writer AT ALL. testify fails an unexpected call, so
	// this is what proves the mutation was never attempted rather than merely unobserved.
	datasource := new(mocks.MockDataSource)
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

	_, eventCaptured, capturedMonitors, err := blnk.persistSingleTransactionExecutionWork(context.Background(), queuedBatchPostCommitWork{
		transaction:        transaction,
		sourceBalance:      sourceBalance,
		destinationBalance: destinationBalance,
	})

	require.Error(t, err,
		"a transaction whose event could not be prepared must be refused, not committed with the "+
			"event silently dropped")
	assert.False(t, eventCaptured, "nothing was written, so nothing was captured")
	assert.Empty(t, capturedMonitors,
		"and no monitor alert was captured either: the transaction event is prepared first, so the "+
			"refusal happens before any monitor is even resolved")
	assert.Empty(t, datasource.CapturedEventOutboxes(),
		"the atomic writer must not have been reached")

	datasource.AssertExpectations(t)
}

// TestRejectTransaction_CommitsTheRejectionEventWithTheRejection is the test for the
// rejection path, which is the one transaction-recording caller with an event to
// capture and no balance movement to enrol it beside.
//
// AND IT PINS THE ORDERING DOMAIN.
func TestRejectTransaction_CommitsTheRejectionEventWithTheRejection(t *testing.T) {
	transaction := &model.Transaction{
		TransactionID: "txn_2e8b3f70",
		Source:        "bln_source_2e8b",
		Destination:   "bln_dest_2e8b",
		PreciseAmount: big.NewInt(1000),
		Currency:      "USD",
		Status:        StatusQueued,
		CreatedAt:     time.Now().UTC(),
	}

	const rejectionLedgerID = "ldg_reject_2e8b"

	datasource := new(mocks.MockDataSource)
	datasource.On("RecordTransaction", mock.Anything, mock.Anything).Return(transaction, nil)
	// The one lookup the ledger resolution makes. It is the SOURCE balance, matching
	// transactionLedgerID in transaction_execution.go so the two producers of one
	// transaction's events cannot disagree about which balance names the ledger.
	datasource.On("GetBalanceByIDLite", transaction.Source).Return(&model.Balance{
		BalanceID: transaction.Source,
		LedgerID:  rejectionLedgerID,
	}, nil)

	// A real Blnk rather than the bare struct newOutboxBlnk builds: RejectTransaction runs
	// the post-commit actions, which index through the queue, so the instance needs a
	// queue and a Redis to reach. miniredis is what the legacy webhook tests already use
	// for this.
	redisServer := miniredis.RunT(t)
	cnf := outboxPublishingConfiguration()
	// NewBlnk constructs the real Kafka publisher, which refuses to dial an unencrypted
	// broker unless a deployment says so in writing. This test never contacts a broker —
	// capture only writes an outbox row — so the acknowledgement is what lets the
	// publisher be constructed.
	cnf.Kafka.InsecureLocalDev = true
	cnf.Redis = config.RedisConfig{Dns: redisServer.Addr()}
	cnf.Queue = config.QueueConfig{WebhookQueue: "webhook_queue", IndexQueue: "index_queue", NumberOfQueues: 1}
	outboxStoreConfiguration(t, cnf)

	blnk, err := NewBlnk(datasource)
	require.NoError(t, err)

	rejected, err := blnk.RejectTransaction(context.Background(), transaction, "insufficient funds")
	require.NoError(t, err)
	require.NotNil(t, rejected)
	assert.Equal(t, StatusRejected, rejected.Status)

	captured := datasource.CapturedEventOutboxes()
	require.Len(t, captured, 1,
		"the rejection event must be handed to RecordTransaction so it commits with the rejection")
	assert.Equal(t, "transaction.rejected", captured[0].EventType,
		"the event name must come from getEventFromStatus rather than a literal, so it cannot "+
			"drift from the status-to-event table")
	assert.Equal(t, transaction.TransactionID, captured[0].AggregateID)
	assert.Equal(t, rejectionLedgerID, captured[0].PartitionKey,
		"the rejection event must be keyed on the LEDGER, resolved from the source balance, so it "+
			"shares a partition with the rest of this transaction's lifecycle events. Keying on the "+
			"source balance instead — which is what this call site did before it looked the ledger "+
			"up — puts one transaction's events on two partitions, and Kafka orders only within one")
	assert.Equal(t, rejectionLedgerID, captured[0].LedgerID,
		"and the ledger column must record it too, so the event is groupable by ledger by the daily "+
			"reconciliation and by any consumer")

	datasource.AssertExpectations(t)
}

// TestRejectTransaction_StillRejectsWhenTheLedgerCannotBeResolved is the other half of
// the ledger resolution, and the more important half.
func TestRejectTransaction_StillRejectsWhenTheLedgerCannotBeResolved(t *testing.T) {
	transaction := &model.Transaction{
		TransactionID: "txn_no_ledger_5a1c",
		Source:        "bln_missing_source",
		Destination:   "bln_missing_destination",
		PreciseAmount: big.NewInt(500),
		Currency:      "USD",
		Status:        StatusQueued,
		CreatedAt:     time.Now().UTC(),
	}

	datasource := new(mocks.MockDataSource)
	datasource.On("RecordTransaction", mock.Anything, mock.Anything).Return(transaction, nil)
	// BOTH balances are unresolvable, which is what forces the fallback: the resolution tries the
	// source and then the destination, so failing only the source would still find a ledger.
	datasource.On("GetBalanceByIDLite", transaction.Source).
		Return((*model.Balance)(nil), errors.New("event outbox test: no such balance"))
	datasource.On("GetBalanceByIDLite", transaction.Destination).
		Return((*model.Balance)(nil), errors.New("event outbox test: no such balance"))

	redisServer := miniredis.RunT(t)
	cnf := outboxPublishingConfiguration()
	cnf.Kafka.InsecureLocalDev = true
	cnf.Redis = config.RedisConfig{Dns: redisServer.Addr()}
	cnf.Queue = config.QueueConfig{WebhookQueue: "webhook_queue", IndexQueue: "index_queue", NumberOfQueues: 1}
	outboxStoreConfiguration(t, cnf)

	blnk, err := NewBlnk(datasource)
	require.NoError(t, err)

	rejected, err := blnk.RejectTransaction(context.Background(), transaction, "no such balance")
	require.NoError(t, err,
		"a balance lookup that fails must NEVER be the reason a rejection goes unrecorded")
	require.NotNil(t, rejected)
	assert.Equal(t, StatusRejected, rejected.Status)

	captured := datasource.CapturedEventOutboxes()
	require.Len(t, captured, 1, "the rejection event is still captured with the rejection")
	assert.Equal(t, transaction.Source, captured[0].PartitionKey,
		"and the key falls back to the documented payload-derived chain — the source balance — "+
			"rather than being empty, which would let Kafka scatter the event round-robin")
	assert.Empty(t, captured[0].LedgerID,
		"the ledger column stays NULL rather than carrying a fabricated value: no ledger was "+
			"resolved, and inventing one would corrupt every consumer grouping by ledger")

	datasource.AssertExpectations(t)
}

// TestEventOutboxSource_HoldsTheRelocatedPayloadContract pins WHERE NewWebhook is
// declared, which no behavioural test can express: the payload behaviour is covered
// exhaustively above, but the declaration must not migrate back into webhooks.go, the
// file the sunset deletes. Several non-test files depend on this type, so a declaration
// inside the deleted transport would take the payload contract down with it — and the
// terminal release is performed weeks later, by someone reading a checklist rather than
// this file.
//
// The invariant is asserted in both directions:
//
//   - It IS declared in event_outbox.go, exactly once. A revert or a bad merge that
//     dropped the declaration would otherwise surface only as a build failure
//     elsewhere.
//   - It is NOT declared in webhooks.go. Two declarations in package blnk do not
//     compile, so this is the assertion that fails if someone restores the old
//     declaration.
//   - The JSON tags are still `event` and `data`. They are the bytes every subscriber
//     parses, on either transport, and a rename here is a silent break of every parser
//     written against the HTTP era.
func TestEventOutboxSource_HoldsTheRelocatedPayloadContract(t *testing.T) {
	path := filepath.Join(moduleRootDir(t), "event_outbox.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
	require.NoError(t, err, "event_outbox.go must be parseable to assert its structure")

	declarations := 0
	var fields *ast.StructType

	for _, declaration := range parsed.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}

		for _, spec := range generic.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "NewWebhook" {
				continue
			}

			declarations++
			if structType, ok := typeSpec.Type.(*ast.StructType); ok {
				fields = structType
			}
		}
	}

	require.Equal(t, 1, declarations,
		"NewWebhook must be declared exactly once in event_outbox.go: its marshaled form IS the payload every LedgerEvent carries, so it must outlive the HTTP transport")
	require.NotNil(t, fields, "NewWebhook must still be a struct type")

	assert.NotContains(t, readRepoFile(t, "webhooks.go"), "type NewWebhook struct",
		"webhooks.go must no longer declare NewWebhook; the relocation is STEP 1 of the sunset procedure and a second declaration in package blnk breaks the build")

	tags := make([]string, 0, len(fields.Fields.List))
	for _, field := range fields.Fields.List {
		require.NotNil(t, field.Tag, "every NewWebhook field must carry an explicit JSON tag")
		tags = append(tags, field.Tag.Value)
	}

	assert.Equal(t, []string{"`json:\"event\"`", "`json:\"data\"`"}, tags,
		"the two JSON tags ARE the wire contract, in this order: {\"event\": <string>, \"data\": <object>}. Renaming one, or dropping the outer envelope in favour of the inner data object, breaks byte-equivalence between the two transports and every subscriber parser written against the HTTP era")
}
