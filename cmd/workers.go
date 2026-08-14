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
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/middleware"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/hotpairs"
	"github.com/blnkfinance/blnk/internal/metrics"
	redis_db "github.com/blnkfinance/blnk/internal/redis-db"
	"github.com/blnkfinance/blnk/internal/search"
	trace "github.com/blnkfinance/blnk/internal/traces"
	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
)

// indexData represents the data structure used for indexing data in the system.
type indexData struct {
	Collection string                 `json:"collection"`
	Payload    map[string]interface{} `json:"payload"`
}

// processTransaction processes a transaction received from the Redis queue.
func (b *blnkInstance) processTransaction(ctx context.Context, t *asynq.Task) error {
	ctx, span := otel.Tracer("blnk.transactions.worker").Start(ctx, "Process Transaction From Redis Queue")
	defer span.End()

	startTime := time.Now()

	var txn model.Transaction
	if err := json.Unmarshal(t.Payload(), &txn); err != nil {
		logrus.Error(err)
		return err
	}

	handled, err := b.blnk.TryRecordQueuedTransactionBatch(ctx, &txn)
	if b.cnf.Queue.EnableHotLane && t.Type() == b.cnf.Queue.HotQueueName {
		handled, err = b.blnk.TryRecordQueuedTransactionBatchForHotLane(ctx, &txn)
	}
	if err != nil {
		logrus.WithError(err).Warnf("coalesced processing attempt failed for transaction %s", txn.TransactionID)
	}
	if handled {
		metrics.QueueProcessingDuration.Record(ctx, time.Since(startTime).Seconds(),
			otelmetric.WithAttributes(attribute.String("result", "success")),
		)
		return nil
	}
	_, err = b.blnk.ProcessQueuedTransaction(ctx, &txn, b.cnf.Queue.EnableHotLane && t.Type() == b.cnf.Queue.HotQueueName)
	if err != nil {
		// ALREADY APPLIED IS THE EXPECTED OUTCOME HERE, NOT A SYSTEM FAULT.
		//
		// A duplicate-reference error on a QUEUED transaction means this exact transaction
		// has already been written, and the two ways that happens are both routine:
		//
		//  1. Coalescing. A leader builds a batch out of its queued siblings and applies
		//     all of them in one commit. Every follower in that batch still has its own
		//     task, and each one arrives here to find its work already done. One batch of
		//     2,000 children therefore produces up to 2,000 arrivals on this branch.
		//  2. At-least-once delivery. asynq redelivers a task whose handler had already
		//     committed — after a worker restart, a lost ack or a lease expiry.
		//
		// Neither is a fault, and the correct response to both is to acknowledge the task,
		// which is what returning nil does.
		//
		// This branch used to call notification.NotifyError, and that is what turned a
		// routine deduplication into an incident. NotifyError writes one ERROR line per
		// occurrence AND publishes a system.error event — which, since events became
		// outbox-backed, means a row in blnk.event_outbox for the relay to publish. So
		// every already-applied follower cost an operator-facing error and a unit of load
		// on the very pipeline the ledger's own events flow through. Measured under a
		// coalescing load: 1,763 error lines and 1,366 pending system.error events, none of
		// which described anything wrong.
		//
		// The outcome is still observable — it is recorded on the same histogram that
		// counts successful tasks, under its own result label, so a genuine change in the
		// rate of already-applied arrivals remains visible without paging anyone.
		if blnk.IsDuplicateReferenceError(err) {
			logrus.WithFields(logrus.Fields{
				"transaction_id": txn.TransactionID,
				"reference":      txn.Reference,
			}).Info("Transaction was already applied; acknowledging without reprocessing")
			metrics.QueueProcessingDuration.Record(ctx, time.Since(startTime).Seconds(),
				otelmetric.WithAttributes(attribute.String("result", "already_applied")),
			)

			return nil
		}

		if strings.Contains(strings.ToLower(err.Error()), "insufficient funds") {
			if !b.cnf.Queue.InsufficientFundRetries {
				return handleTransactionRejection(ctx, b, &txn, err)
			}

			retryCount, _ := asynq.GetRetryCount(ctx)
			if hasReachedMaxRetryAttempt(b.cnf, retryCount) {
				logrus.WithFields(logrus.Fields{
					"transaction_id": txn.TransactionID,
					"retry_count":    retryCount,
					"max_retries":    b.cnf.Queue.MaxRetryAttempts,
				}).Warn("Transaction reached max retry attempts; rejecting with final processing error")
				return handleTransactionRejection(ctx, b, &txn, err)
			}

			logrus.Infof("Insufficient funds for transaction %s, retry attempt %d/%d",
				txn.TransactionID, retryCount, b.cnf.Queue.MaxRetryAttempts)
			metrics.WorkerRetriesTotal.Add(ctx, 1,
				otelmetric.WithAttributes(attribute.String("reason", "insufficient_funds")),
			)
			return err // This will trigger a retry
		}

		if strings.Contains(strings.ToLower(err.Error()), "transaction exceeds overdraft limit") {
			return handleTransactionRejection(ctx, b, &txn, err)
		}

		if shouldRejectLockContentionImmediately(b.cnf, err) {
			logrus.WithFields(logrus.Fields{
				"transaction_id": txn.TransactionID,
				"error":          err.Error(),
			}).Warn("Rejecting transaction immediately due to lock contention policy")
			return handleTransactionRejection(ctx, b, &txn, err)
		}

		retryCount, _ := asynq.GetRetryCount(ctx)
		if hasReachedMaxRetryAttempt(b.cnf, retryCount) {
			logrus.WithFields(logrus.Fields{
				"transaction_id": txn.TransactionID,
				"retry_count":    retryCount,
				"max_retries":    b.cnf.Queue.MaxRetryAttempts,
				"error":          err.Error(),
			}).Warn("Transaction reached max retry attempts; rejecting with final processing error")
			return handleTransactionRejection(ctx, b, &txn, err)
		}

		logrus.Infof("Transaction %s pushed back for retry due to error: %v", txn.TransactionID, err)
		metrics.WorkerRetriesTotal.Add(ctx, 1,
			otelmetric.WithAttributes(attribute.String("reason", "other")),
		)
		return err
	}

	logrus.Infof("Transaction %s processed successfully", txn.TransactionID)
	metrics.QueueProcessingDuration.Record(ctx, time.Since(startTime).Seconds(),
		otelmetric.WithAttributes(attribute.String("result", "success")),
	)
	return nil
}

// handleTransactionRejection rejects a transaction that has exhausted its retries or
// hit a terminal processing error.
func handleTransactionRejection(ctx context.Context, b *blnkInstance, txn *model.Transaction, err error) error {
	_, rejectErr := b.blnk.RejectTransaction(ctx, txn, err.Error())

	return rejectErr
}

func hasReachedMaxRetryAttempt(cfg *config.Configuration, retryCount int) bool {
	if cfg == nil || cfg.Queue.MaxRetryAttempts <= 0 {
		return false
	}
	return retryCount >= cfg.Queue.MaxRetryAttempts
}

func shouldRejectLockContentionImmediately(cfg *config.Configuration, err error) bool {
	if cfg == nil || !cfg.Queue.RejectLockContentionImmediately {
		return false
	}
	return hotpairs.IsLockContentionError(err)
}

// queueBacklogInterval is how often queue depths are republished. Backlog is a slow signal
// — the alert over it dwells for minutes — so this is deliberately unhurried: each tick is
// one Redis round trip per queue, and paying that every second would add load to the very
// component under observation.
const queueBacklogInterval = 15 * time.Second

// queueBacklogCollector publishes the depth of every asynq queue as a gauge.
//
// WHY: A QUEUE THAT CANNOT KEEP PACE HAS NO SYMPTOM UNTIL SOMETHING ELSE BREAKS.
//
// The index queue reached 609,673 pending tasks during a sustained load run, draining at
// 276/s against 550/s arriving. Nothing reported it. The asynqmon dashboard would have shown
// it to anyone who happened to open the dashboard, and no alert could fire because no series
// existed to alert on — so the imbalance was found by a load test rather than by the system
// that was living through it.
//
// Depth alone is the right signal here, and it is worth being precise about why: an arrival
// rate and a drain rate are both derivable from it, but the QUESTION an operator has is
// "is work accumulating", and accumulation is exactly what depth measures directly. The
// alert over this gauge dwells long enough that an ordinary burst — which is what a queue is
// FOR — passes without complaint.
type queueBacklogCollector struct {
	inspector *asynq.Inspector
	interval  time.Duration
	stop      chan struct{}
	done      chan struct{}
}

// newQueueBacklogCollector builds a collector against the same Redis the workers consume
// from. It returns nil when Redis cannot be addressed, because a worker that cannot reach
// Redis has a louder problem than its missing gauges.
func newQueueBacklogCollector(conf *config.Configuration) *queueBacklogCollector {
	redisOption, err := redis_db.ParseRedisURL(conf.Redis.Dns, conf.Redis.SkipTLSVerify)
	if err != nil {
		logrus.Errorf("queue backlog collector disabled; could not parse Redis URL: %v", err)

		return nil
	}

	return &queueBacklogCollector{
		inspector: asynq.NewInspector(asynq.RedisClientOpt{
			Addr:      redisOption.Addr,
			Password:  redisOption.Password,
			DB:        redisOption.DB,
			TLSConfig: redisOption.TLSConfig,
		}),
		interval: queueBacklogInterval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start begins publishing depths until Stop is called or ctx is cancelled.
func (q *queueBacklogCollector) Start(ctx context.Context) {
	if q == nil {
		return
	}

	go func() {
		defer close(q.done)

		ticker := time.NewTicker(q.interval)
		defer ticker.Stop()

		// Publish immediately, so a backlog that is already there on startup is visible
		// without waiting out a first interval.
		q.collect(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-q.stop:
				return
			case <-ticker.C:
				q.collect(ctx)
			}
		}
	}()
}

// Stop halts collection and waits for the loop to exit.
func (q *queueBacklogCollector) Stop() {
	if q == nil {
		return
	}

	close(q.stop)
	<-q.done

	if err := q.inspector.Close(); err != nil {
		logrus.Errorf("queue backlog collector: closing inspector: %v", err)
	}
}

// collect publishes one reading per queue per waiting state.
//
// Failures are logged and dropped rather than retried: the next tick is 15 seconds away and
// carries a fresh reading, so a transient Redis error costs one sample. Nothing here is
// allowed to interfere with task processing.
func (q *queueBacklogCollector) collect(ctx context.Context) {
	queues, err := q.inspector.Queues()
	if err != nil {
		logrus.Warnf("queue backlog collector: listing queues: %v", err)

		return
	}

	for _, name := range queues {
		info, infoErr := q.inspector.GetQueueInfo(name)
		if infoErr != nil {
			logrus.Warnf("queue backlog collector: reading queue %q: %v", name, infoErr)

			continue
		}

		// The four states are reported separately because they call for different actions.
		// Pending is work that has arrived and not been started — the accumulation the
		// alert watches. Active is work in flight. Retry and scheduled are work deferred to
		// a future time, which looks like a backlog on a dashboard but is not one: a
		// scheduled task is waiting for its clock, not for a worker.
		for state, depth := range map[string]int{
			"pending":   info.Pending,
			"active":    info.Active,
			"retry":     info.Retry,
			"scheduled": info.Scheduled,
		} {
			metrics.QueueBacklog.Record(ctx, int64(depth),
				otelmetric.WithAttributes(
					attribute.String("queue", name),
					attribute.String("state", state),
				),
			)
		}
	}
}

// searchIndexer holds the one TypeSense client a worker process needs, and remembers
// whether the collection schema has been established.
//
// WHY THIS EXISTS: A PER-TASK SETUP COST THAT THE INDEX QUEUE CANNOT AFFORD.
//
// Both index handlers used to construct a client and call EnsureCollectionsExist on EVERY
// task. That call is not a cached lookup — it issues a create for each of the five
// collections and then upserts the default general ledger, so six HTTP round trips ran
// before the task's own single write. Worse, each task built its own client, so each of
// those seven requests opened a fresh connection: nothing was pooled or reused across
// tasks, and a per-client circuit breaker never accumulated enough history to be useful.
//
// Measured against an IDLE local TypeSense, the schema work alone cost 10.66ms per task.
// The index queue is fed once per indexed write, so at the validated 550 events/second the
// arrival rate is 550/s while the drain rate is bounded by that setup: with this server's
// twenty goroutines split three-to-one in the webhook queue's favour, roughly five slots
// serve indexing, giving about 333/s before the real write is even attempted. The observed
// drain was 276/s against 550/s arrivals — a net gain of ~274/s that grew the queue without
// bound and peaked at 609,673 pending tasks.
//
// Reuse makes the schema work O(1) per PROCESS instead of O(1) per TASK, and lets the
// underlying HTTP client pool connections across every task the process ever runs.
//
// WHY NOT sync.Once: a Once that ran during a TypeSense outage would record the failure
// permanently, and every subsequent task would then write into collections that were never
// created — for the life of the process. Success is what is latched here, not the attempt,
// so a failed assurance is retried by the next task. The client itself is cached
// unconditionally, because constructing one performs no I/O and cannot fail.
type searchIndexer struct {
	mu      sync.Mutex
	client  *search.TypesenseClient
	ensured bool
}

// clientFor returns the process-wide TypeSense client with its schema assured.
//
// The lock is held across the assurance deliberately: the point is that exactly one task
// does that work while the others wait for it, rather than all of them racing to create
// the same five collections. Once ensured is latched, the critical section is a pointer
// read and a boolean test.
func (s *searchIndexer) clientFor(ctx context.Context, apiKey string, hosts []string) (*search.TypesenseClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client == nil {
		s.client = search.NewTypesenseClient(apiKey, hosts)
	}

	if !s.ensured {
		if err := s.client.EnsureCollectionsExist(ctx); err != nil {
			return nil, err
		}

		s.ensured = true
	}

	return s.client, nil
}

// indexData indexes data into TypeSense for searchability.
func (b *blnkInstance) indexData(ctx context.Context, t *asynq.Task) error {
	if b.cnf.TypeSense.Dns == "" {
		return nil
	}

	var data indexData

	// Unmarshal the indexing data from the task payload.
	if err := json.Unmarshal(t.Payload(), &data); err != nil {
		logrus.Error(err)
		return err
	}

	collection := data.Collection
	payload := data.Payload

	// The process-wide client, with its schema established once rather than per task.
	newSearch, err := b.searchIndex.clientFor(ctx, b.cnf.TypeSenseKey, []string{b.cnf.TypeSense.Dns})
	if err != nil {
		logrus.Errorf("Failed to ensure collections exist: %v", err)
		return err
	}

	// Handle the notification and send the payload to the collection for indexing.
	err = newSearch.HandleNotification(ctx, collection, payload)
	if err != nil {
		logrus.Error("Error indexing data", err)
		return err
	}

	logrus.Infof(" [*] Data indexed: %s", collection)
	return nil
}

// indexBatchData indexes a batch of items into TypeSense in dependency order.
func (b *blnkInstance) indexBatchData(ctx context.Context, t *asynq.Task) error {
	if b.cnf.TypeSense.Dns == "" {
		return nil
	}

	var batch search.IndexBatch

	// Unmarshal the batch data from the task payload.
	if err := json.Unmarshal(t.Payload(), &batch); err != nil {
		logrus.Error(err)
		return err
	}

	// The same process-wide client the single-document handler uses, for the same reason.
	newSearch, err := b.searchIndex.clientFor(ctx, b.cnf.TypeSenseKey, []string{b.cnf.TypeSense.Dns})
	if err != nil {
		logrus.Errorf("Failed to ensure collections exist: %v", err)
		return err
	}

	// Handle the batch notification - indexes dependencies first, then primary.
	err = newSearch.HandleBatchNotification(ctx, &batch)
	if err != nil {
		logrus.Errorf("Error indexing batch %s: %v", batch.ID, err)
		return err
	}

	logrus.Infof(" [*] Batch indexed: %s (deps: %d)", batch.ID, len(batch.Dependencies))
	return nil
}

func (b *blnkInstance) processInflightCommit(ctx context.Context, t *asynq.Task) error {
	var p blnk.InflightActionPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil || p.TransactionID == "" {
		// Legacy payload: a bare JSON string transaction ID from a scheduled
		// auto-commit enqueued before this change. Treat it as a full commit.
		var txnID string
		if serr := json.Unmarshal(t.Payload(), &txnID); serr != nil || txnID == "" {
			logrus.WithError(serr).Error("failed to unmarshal inflight action payload")
			return serr
		}
		p = blnk.InflightActionPayload{TransactionID: txnID, Action: blnk.InflightActionCommit}
	}

	action := p.Action
	if action == "" {
		action = blnk.InflightActionCommit
	}

	amount := big.NewInt(0)
	if p.PreciseAmount != "" {
		if parsed, ok := new(big.Int).SetString(p.PreciseAmount, 10); ok {
			amount = parsed
		}
	}

	retryable, err := b.blnk.RunInflightActionByParent(ctx, p.TransactionID, action, amount, p.ActionID)
	if err != nil && retryable {
		return err // transient failure; asynq will retry
	}
	if err != nil {
		// Non-retryable failure: ack so the task doesn't poison-loop. The
		// service layer already logged the per-leg detail.
		logrus.WithError(err).WithField("transaction_id", p.TransactionID).Error("inflight action failed permanently; acking task")
	}
	return nil
}

// processInflightExpiry handles the expiry of inflight transactions.
func (b *blnkInstance) processInflightExpiry(cxt context.Context, t *asynq.Task) error {
	var txnID string
	// Unmarshal the transaction ID from the task payload.
	if err := json.Unmarshal(t.Payload(), &txnID); err != nil {
		logrus.Error(err)
		return err
	}

	// Void the inflight transaction by its ID.
	_, err := b.blnk.VoidInflightTransaction(cxt, txnID)
	if err != nil {
		return err
	}

	logrus.Printf(" [*] Inflight Transaction Expired %s", txnID)
	return nil
}

func initializeQueues() map[string]int {
	cfg, err := config.Fetch()
	if err != nil {
		logrus.Errorf("Error fetching config, using defaults: %v", err)
		return nil
	}

	queues := make(map[string]int)
	queues[cfg.Queue.InflightExpiryQueue] = 1
	queues[cfg.Queue.InflightCommitQueue] = 1

	for i := 1; i <= cfg.Queue.NumberOfQueues; i++ {
		queueName := fmt.Sprintf("%s_%d", cfg.Queue.TransactionQueue, i)
		queues[queueName] = 1
	}
	return queues
}

func initializeHotQueues() map[string]int {
	cfg, err := config.Fetch()
	if err != nil {
		logrus.Errorf("Error fetching config, using defaults: %v", err)
		return nil
	}
	if !cfg.Queue.EnableHotLane {
		return nil
	}

	return map[string]int{
		cfg.Queue.HotQueueName: 1,
	}
}

func initializeWebhookQueues() map[string]int {
	cfg, err := config.Fetch()
	if err != nil {
		logrus.Errorf("Error fetching config, using defaults: %v", err)
		return nil
	}

	queues := make(map[string]int)
	queues[cfg.Queue.WebhookQueue] = 3
	queues[cfg.Queue.IndexQueue] = 1

	return queues
}

func initializeWebhookWorkerServer(conf *config.Configuration, queues map[string]int) (*asynq.Server, error) {
	redisOption, err := redis_db.ParseRedisURL(conf.Redis.Dns, conf.Redis.SkipTLSVerify)
	if err != nil {
		return nil, fmt.Errorf("error parsing Redis URL: %v", err)
	}

	return asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:      redisOption.Addr,
			Password:  redisOption.Password,
			DB:        redisOption.DB,
			TLSConfig: redisOption.TLSConfig,
			PoolSize:  conf.Redis.PoolSize,
		},
		asynq.Config{
			Concurrency:     conf.Queue.WebhookConcurrency,
			Queues:          queues,
			ShutdownTimeout: 30 * time.Second,
		},
	), nil
}

func initializeWorkerServer(conf *config.Configuration, queues map[string]int) (*asynq.Server, error) {
	redisOption, err := redis_db.ParseRedisURL(conf.Redis.Dns, conf.Redis.SkipTLSVerify)
	if err != nil {
		return nil, fmt.Errorf("error parsing Redis URL: %v", err)
	}

	return asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:      redisOption.Addr,
			Password:  redisOption.Password,
			DB:        redisOption.DB,
			TLSConfig: redisOption.TLSConfig,
			PoolSize:  conf.Redis.PoolSize,
		},
		asynq.Config{
			Concurrency:     conf.Queue.TransactionWorkerConcurrency,
			Queues:          queues,
			ShutdownTimeout: 30 * time.Second,
		},
	), nil
}

func initializeHotWorkerServer(conf *config.Configuration, queues map[string]int) (*asynq.Server, error) {
	redisOption, err := redis_db.ParseRedisURL(conf.Redis.Dns, conf.Redis.SkipTLSVerify)
	if err != nil {
		return nil, fmt.Errorf("error parsing Redis URL: %v", err)
	}

	return asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:      redisOption.Addr,
			Password:  redisOption.Password,
			DB:        redisOption.DB,
			TLSConfig: redisOption.TLSConfig,
			PoolSize:  conf.Redis.PoolSize,
		},
		asynq.Config{
			Concurrency:     conf.Queue.HotQueueConcurrency,
			Queues:          queues,
			ShutdownTimeout: 30 * time.Second,
		},
	), nil
}

func initializeTaskHandlers(b *blnkInstance, mux *asynq.ServeMux) {
	cfg, err := config.Fetch()
	if err != nil {
		logrus.Errorf("Error fetching config, using defaults: %v", err)
		return
	}

	for i := 1; i <= cfg.Queue.NumberOfQueues; i++ {
		queueName := fmt.Sprintf("%s_%d", cfg.Queue.TransactionQueue, i)
		mux.HandleFunc(queueName, b.processTransaction)
	}
	if cfg.Queue.EnableHotLane {
		mux.HandleFunc(cfg.Queue.HotQueueName, b.processTransaction)
	}
	mux.HandleFunc(cfg.Queue.InflightExpiryQueue, b.processInflightExpiry)
	mux.HandleFunc(cfg.Queue.InflightCommitQueue, b.processInflightCommit)
}

func initializeWebhookTaskHandlers(b *blnkInstance, mux *asynq.ServeMux) {
	cfg, err := config.Fetch()
	if err != nil {
		logrus.Errorf("Error fetching config, using defaults: %v", err)
		return
	}

	// FOUR HANDLERS ON ONE MUX, AND ONLY THE FIRST BELONGS TO THE LEGACY WEBHOOK
	// TRANSPORT.
	mux.HandleFunc(cfg.Queue.WebhookQueue, b.blnk.ProcessWebhook)
	mux.HandleFunc("new:hook_execution", b.blnk.Hooks.ProcessHookTask)
	mux.HandleFunc(cfg.Queue.IndexQueue, b.indexData)
	mux.HandleFunc("new:index:batch", b.indexBatchData)
}

// workerCommands defines the "workers" command to start worker processes.
func workerCommands(b *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workers",
		Short: "start blnk workers",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			conf, err := config.Fetch()
			if err != nil {
				logrus.Fatal("Error fetching config:", err)
			}

			if err := runWorkers(ctx, b, conf); err != nil {
				logrus.Fatal(err)
			}
		},
	}

	return cmd
}

// runWorkers starts the transaction, hot-lane (optional), and webhook worker
// servers plus the monitoring HTTP server, then blocks until ctx is canceled
// and shuts everything down gracefully. Extracted from the cobra Run closure
// so the full worker lifecycle is testable; startup failures are returned to
// the caller, which keeps the process-exit decision at the command layer.
func runWorkers(ctx context.Context, b *blnkInstance, conf *config.Configuration) error {
	phClient, shutdown, err := initializeTelemetryAndObservability(context.Background(), conf)
	if err != nil {
		return err
	}
	if shutdown != nil {
		defer func() {
			tctx, cancel := context.WithTimeout(context.Background(), telemetryFlushTimeout)
			defer cancel()
			if err := shutdown(tctx); err != nil {
				logrus.Errorf("Error during shutdown: %v", err)
			}
		}()
	}
	if phClient != nil {
		defer func() { _ = phClient.Close() }()
	}

	srv, hotSrv, webhookSrv, mux, webhookMux, err := setupWorkerServers(b, conf)
	if err != nil {
		return err
	}

	monitoringSrv := startMonitoringServer(conf)

	if err := srv.Start(mux); err != nil {
		return fmt.Errorf("could not start transaction worker server: %w", err)
	}
	if hotSrv != nil {
		if err := hotSrv.Start(mux); err != nil {
			return fmt.Errorf("could not start hot transaction worker server: %w", err)
		}
	}
	if err := webhookSrv.Start(webhookMux); err != nil {
		return fmt.Errorf("could not start webhook worker server: %w", err)
	}

	recoveryProcessor := blnk.NewQueuedTransactionRecoveryProcessor(b.blnk)
	recoveryProcessor.Start(ctx)

	// Queue depths, so an arrival rate that outruns a drain rate is visible while it is
	// still only an imbalance. See queueBacklogCollector.
	backlogCollector := newQueueBacklogCollector(conf)
	backlogCollector.Start(ctx)

	logrus.Info("Workers started.")

	// Wait for SIGINT/SIGTERM (or test-driven context cancellation).
	<-ctx.Done()

	logrus.Info("Shutdown signal received. Shutting down...")

	recoveryProcessor.Stop()
	backlogCollector.Stop()

	if monitoringSrv != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := monitoringSrv.Shutdown(sctx); err != nil {
			logrus.Errorf("monitoring shutdown error: %v", err)
		}
	}

	webhookSrv.Shutdown()
	if hotSrv != nil {
		hotSrv.Shutdown()
	}
	srv.Shutdown()

	// Close the service container, and only now that every worker server has stopped.
	if err := b.blnk.Close(); err != nil {
		logrus.WithError(err).Error(
			"closing the service container reported an error; the asynq client or a scheduled " +
				"cleanup may not have shut down cleanly. This role holds no event publisher, so a " +
				"captured event is unaffected: it is already committed to blnk.event_outbox and is " +
				"published by the relay in the server role",
		)
	}

	logrus.Info("Shutdown complete.")
	return nil
}

func setupWorkerServers(b *blnkInstance, conf *config.Configuration) (*asynq.Server, *asynq.Server, *asynq.Server, *asynq.ServeMux, *asynq.ServeMux, error) {
	queues := initializeQueues()
	hotQueues := initializeHotQueues()
	webhookQueues := initializeWebhookQueues()

	srv, err := initializeWorkerServer(conf, queues)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	var hotSrv *asynq.Server
	if conf.Queue.EnableHotLane && len(hotQueues) > 0 {
		hotSrv, err = initializeHotWorkerServer(conf, hotQueues)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
	}

	webhookSrv, err := initializeWebhookWorkerServer(conf, webhookQueues)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	mux := asynq.NewServeMux()
	initializeTaskHandlers(b, mux)

	webhookMux := asynq.NewServeMux()
	initializeWebhookTaskHandlers(b, webhookMux)

	return srv, hotSrv, webhookSrv, mux, webhookMux, nil
}

func startMonitoringServer(conf *config.Configuration) *http.Server {
	redisOption, _ := redis_db.ParseRedisURL(conf.Redis.Dns, conf.Redis.SkipTLSVerify)
	asynqmonHandler := asynqmon.New(asynqmon.Options{
		RootPath: "/monitoring",
		RedisConnOpt: asynq.RedisClientOpt{
			Addr:      redisOption.Addr,
			Password:  redisOption.Password,
			DB:        redisOption.DB,
			TLSConfig: redisOption.TLSConfig,
			PoolSize:  conf.Redis.PoolSize,
		},
	})

	monitoringMux := http.NewServeMux()

	monitoringMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status": "UP", "service": "worker"}`)
	})

	monitoringMux.Handle("/monitoring/", middleware.MetricsAuthHandler(conf.Server.Secure, conf.Server.MetricsBearerToken, asynqmonHandler))
	if h := trace.MetricsHandler(); h != nil {
		monitoringMux.Handle("/metrics", middleware.MetricsAuthHandler(conf.Server.Secure, conf.Server.MetricsBearerToken, h))
	}

	monitoringAddr := fmt.Sprintf(":%s", conf.Queue.MonitoringPort)

	// Bounded with exactly the same limits as the API listener, through the shared helper
	// rather than a second opinion written out here. This listener is the one more likely
	// to be forgotten and the less likely to be behind an ingress that would bound it
	// anyway — it exists to serve the asynqmon dashboard and /metrics to an operator or a
	// scraper, so it is reached directly.
	srv := hardenHTTPServer(&http.Server{
		Addr:    monitoringAddr,
		Handler: monitoringMux,
	})

	go func() {
		logrus.Infof("Worker monitoring server listening on %s (health: /health, dashboard: /monitoring)", monitoringAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Fatalf("could not start monitoring server: %v", err)
		}
	}()

	return srv
}
