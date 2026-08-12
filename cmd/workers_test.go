package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/blnkfinance/blnk/config"
	"github.com/hibiken/asynq"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasReachedMaxRetryAttempt(t *testing.T) {
	cfg := &config.Configuration{
		Queue: config.QueueConfig{
			MaxRetryAttempts: 5,
		},
	}

	tests := []struct {
		name       string
		retryCount int
		want       bool
	}{
		{name: "below max", retryCount: 4, want: false},
		{name: "at max", retryCount: 5, want: true},
		{name: "above max", retryCount: 6, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasReachedMaxRetryAttempt(cfg, tt.retryCount); got != tt.want {
				t.Fatalf("hasReachedMaxRetryAttempt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShouldRejectLockContentionImmediately(t *testing.T) {
	cfg := &config.Configuration{
		Queue: config.QueueConfig{
			RejectLockContentionImmediately: true,
		},
	}

	if !shouldRejectLockContentionImmediately(cfg, fmt.Errorf("failed to acquire lock: already held")) {
		t.Fatal("expected lock contention to reject immediately when enabled")
	}

	if shouldRejectLockContentionImmediately(cfg, fmt.Errorf("random transient database error")) {
		t.Fatal("expected non-lock error not to reject immediately")
	}

	cfg.Queue.RejectLockContentionImmediately = false
	if shouldRejectLockContentionImmediately(cfg, fmt.Errorf("failed to acquire lock: already held")) {
		t.Fatal("expected lock contention not to reject immediately when disabled")
	}
}

func mockConfig(t *testing.T) *config.Configuration {
	t.Helper()
	config.MockConfig(&config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "postgres://x:x@localhost/x"},
		Redis:      config.RedisConfig{Dns: "redis://localhost:6379"},
	})
	cfg, err := config.Fetch()
	if err != nil {
		t.Fatalf("config.Fetch: %v", err)
	}
	return cfg
}

// Fix 1: InflightCommitQueue must have a non-empty default so the producer
// doesn't silently enqueue to a "" queue name when the env var is unset.
func TestInflightCommitQueueDefault(t *testing.T) {
	cfg := mockConfig(t)
	if cfg.Queue.InflightCommitQueue == "" {
		t.Fatal("InflightCommitQueue has no default — producer will enqueue to empty queue name and jobs are lost")
	}
}

// Fix 2: initializeQueues must include InflightCommitQueue so the asynq server
// polls it. Without this, tasks move from scheduled→pending and sit there forever.
func TestInflightCommitQueuePolled(t *testing.T) {
	cfg := mockConfig(t)
	queues := initializeQueues()
	if _, ok := queues[cfg.Queue.InflightCommitQueue]; !ok {
		t.Fatalf("initializeQueues() missing %q — asynq server will never dequeue inflight commit jobs", cfg.Queue.InflightCommitQueue)
	}
}

// Fix 3: initializeTaskHandlers must register a handler for InflightCommitQueue.
// Without this, asynq archives every task with "handler not found".
func TestInflightCommitHandlerRegistered(t *testing.T) {
	cfg := mockConfig(t)
	b := &blnkInstance{cnf: cfg}
	mux := asynq.NewServeMux()
	initializeTaskHandlers(b, mux)

	payload, _ := json.Marshal("dummy-txn-id")
	task := asynq.NewTask(cfg.Queue.InflightCommitQueue, payload)
	noHandlerErr := fmt.Sprintf("asynq: no handler found for task %q", cfg.Queue.InflightCommitQueue)

	func() {
		defer func() { _ = recover() }() // nil blnk panics inside handler body — that's fine, it means handler was found
		if err := mux.ProcessTask(context.Background(), task); err != nil && err.Error() == noHandlerErr {
			t.Fatalf("no handler registered for %q — tasks will be archived unprocessed", cfg.Queue.InflightCommitQueue)
		}
	}()
}

// TestHandleTransactionRejection_CapturesTheEventExactlyOnce is the worker half of the
// rejection atomicity repair, and it asserts the consequence an operator actually saw.
//
// So the assertions are: the handler reports success, and the event exists exactly
// once.
func TestHandleTransactionRejection_CapturesTheEventExactlyOnce(t *testing.T) {
	cfg := realInfraConfig(t)
	// A configured BROKER is what makes the capture path active. Without it the rejection
	// captures nothing at all and this test would pass for the wrong reason.
	//
	// A webhook URL is deliberately NOT used for this: capture is tied to Kafka because
	// the outbox is only a destination when a relay can drain it, and the relay refuses to
	// run without brokers — see eventPublishingConfigured. A webhook-only deployment
	// therefore writes no row and is served by the legacy transport directly, which is a
	// different behaviour with its own coverage.
	cfg.Kafka.Brokers = cmdTestKafkaBrokers()
	// The publisher refuses a plaintext broker unless the deployment says out loud that it
	// is a development one. Saying so is correct here — nothing is dialled, and the
	// alternative would be a TLS configuration this test has no use for.
	cfg.Kafka.InsecureLocalDev = true

	newBlnk, err := setupBlnk(cfg)
	require.NoError(t, err, "Postgres and Redis must be running for the cmd test suite")
	instance := &blnkInstance{blnk: newBlnk, cnf: cfg}

	// The assertions below read blnk.event_outbox, so a database whose event_outbox does
	// not match what the capture path writes cannot answer them. That is an environment
	// problem and is reported as one: without this guard a foreign schema surfaces as "the
	// rejection event must exist exactly once" or as a raw pq constraint violation
	// returned by the handler, both of which read as a defect in the handler.
	skipUnlessEventOutboxSchemaMatches(t, newBlnk.GetDataSource(), cfg.DataSource.Dns)

	source, destination := createBalancePair(t, instance)

	txn := queuedTransaction(source.BalanceID, destination.BalanceID, 100, false)

	err = handleTransactionRejection(context.Background(), instance, txn,
		errors.New("insufficient funds"))
	require.NoError(t, err,
		"the handler must report success once the transaction is durably rejected; returning the "+
			"duplicate-event conflict made asynq retry an already-rejected transaction")

	db, err := sql.Open("postgres", cfg.DataSource.Dns)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var events int
	require.NoError(t, db.QueryRow(`
		SELECT COUNT(*) FROM blnk.event_outbox
		WHERE aggregate_id = $1 AND event_type = 'transaction.rejected'
	`, txn.TransactionID).Scan(&events))

	assert.Equal(t, 1, events,
		"the rejection event must exist exactly once: none means the rejection was recorded with "+
			"nothing announcing it, and two means the duplicate this handler used to produce")

	var status string
	require.NoError(t, db.QueryRow(`
		SELECT status FROM blnk.transactions WHERE transaction_id = $1
	`, txn.TransactionID).Scan(&status))
	assert.Equal(t, "REJECTED", status,
		"and the transaction itself must be recorded as rejected, so the event above cannot be "+
			"passing for a mutation that never happened")
}
