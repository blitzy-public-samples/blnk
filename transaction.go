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
	"math/big"
	"sync"

	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/semaphore"
)

var tracer = otel.Tracer("blnk.transactions")

const (
	StatusQueued    = "QUEUED"
	StatusApplied   = "APPLIED"
	StatusScheduled = "SCHEDULED"
	StatusRejected  = "REJECTED"
)

var asyncBulkSemaphore = semaphore.NewWeighted(100) // max 100 concurrent
var asyncTxnSemaphore = semaphore.NewWeighted(20)   // max 20 concurrent async transaction processors

// balanceMonitorSem bounds the number of concurrent balance-monitor checks
// spawned by post-commit work across all transactions.
var balanceMonitorSem = make(chan struct{}, 32)

// postTransactionActionSem bounds the number of concurrent post-transaction action
// goroutines across all transactions.
//
// # What was unbounded, and what it cost
//
// postTransactionActions spawned one goroutine per transaction with nothing limiting
// how many could exist at once. That was survivable while the goroutine only enqueued
// an index batch into Redis, and stopped being survivable once it also had to reach the
// database: at 500 transactions per second against a database that has begun to stall,
// the arrival rate is fixed and the completion rate is not, so goroutines accumulate
// without limit, each holding a connection request, until the process is killed for
// memory. Nothing in the logs says why, because nothing failed.
//
// # Why acquiring in the caller is the point
//
// The permit is taken BEFORE the goroutine is spawned, so a saturated pool is felt by
// the producer as backpressure rather than absorbed as unbounded queueing. That is
// exactly what balanceMonitorSem above already does for monitor checks, and the reason
// is the same: the only safe response to work arriving faster than it can be completed
// is to slow the arrivals down.
//
// # Why 64
//
// The work behind one permit is one Redis enqueue and, on the paths that could not
// capture their event inside a ledger transaction, one indexed INSERT — single-digit
// milliseconds together on a healthy system, so 64 in flight clears far more than the
// 500 events per second requirement V-1 asks for. It is deliberately larger than
// balanceMonitorSem's 32 because a monitor check is a read that can be deferred, while
// this work carries an event capture that must not queue behind unrelated reads.
var postTransactionActionSem = make(chan struct{}, 64)

const (
	maxQueuedCoalescingBatchSize = 10000
)

type queuedCoalescingScope string

const (
	queuedCoalescingScopePair        queuedCoalescingScope = "pair"
	queuedCoalescingScopeSource      queuedCoalescingScope = "source"
	queuedCoalescingScopeDestination queuedCoalescingScope = "destination"
)

type queuedBatchPostCommitWork struct {
	transaction        *model.Transaction
	sourceBalance      *model.Balance
	destinationBalance *model.Balance
	outbox             *model.LineageOutbox

	// eventOutbox is the transaction's ledger event, prepared BEFORE the write and
	// inserted INSIDE the same database transaction as the balance updates. That is
	// what makes the event and the mutation commit or roll back together, which is
	// the whole of the transactional-outbox guarantee: with a post-commit insert
	// instead, a crash between the commit and the insert loses the event with nothing
	// anywhere able to detect it.
	//
	// It is nil in exactly two cases, and both are legitimate:
	//
	//   - Event publishing is not configured for Kafka, so there is nothing to
	//     capture; the legacy transport is used directly instead. See PublishEvent.
	//   - The transaction is not persisted through an atomic writer at all — a
	//     rejection, for instance, which is recorded by RecordTransaction.
	//
	// eventCaptured, not this field, is what the post-commit hook branches on: a nil
	// row with eventCaptured false means the event still has to be published after
	// the commit, whereas a nil row with eventCaptured true would be a contradiction.
	eventOutbox *model.EventOutbox

	// eventCaptured reports that the event row above was handed to an atomic writer,
	// so the post-commit hook must NOT publish it a second time.
	//
	// Two separate fields rather than a nil check because "no row to insert" and "the
	// row was inserted" must be distinguishable: the first still needs a post-commit
	// publish, the second must not have one, and a single nilable field cannot say
	// which of the two it is once the row has been consumed.
	eventCaptured bool
}

type queuedBatchPersistResult struct {
	orderedBalances []*model.Balance
	postCommitWork  []queuedBatchPostCommitWork
}

type transactionExecutionMode string

const (
	transactionExecutionModeSingle         transactionExecutionMode = "single"
	transactionExecutionModeQueuedBatch    transactionExecutionMode = "queued_batch"
	transactionExecutionModeHotQueuedBatch transactionExecutionMode = "hot_queued_batch"
)

type transactionExecutionPlan struct {
	mode        transactionExecutionMode
	transaction *model.Transaction
}

type transactionExecutionResult struct {
	mode        transactionExecutionMode
	transaction *model.Transaction
}

// getTxns is a function type that retrieves a batch of transactions based on the parent transaction ID, batch size, and offset.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The ID of the parent transaction.
// - batchSize int: The number of transactions to retrieve in a batch.
// - offset int64: The offset for pagination.
//
// Returns:
// - []*model.Transaction: A slice of pointers to the retrieved Transaction models.
// - error: An error if the transactions could not be retrieved.
type getTxns func(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error)

// transactionWorker is a function type that processes transactions from a job channel and sends the results to a results channel.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - jobs <-chan *model.Transaction: A channel from which transactions are received for processing.
// - results chan<- BatchJobResult: A channel to which the results of the processing are sent.
// - wg *sync.WaitGroup: A wait group to synchronize the completion of the worker.
// - amount *big.Int: The amount to be processed in the transaction.
type transactionWorker func(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount *big.Int)

// BatchJobResult represents the result of processing a transaction in a batch job.
//
// Fields:
// - Txn *model.Transaction: A pointer to the processed Transaction model.
// - Error error: An error if the transaction could not be processed.
type BatchJobResult struct {
	Txn   *model.Transaction
	Error error
}
