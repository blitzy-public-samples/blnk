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
	"embed"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/hibiken/asynq"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/cache"
	"github.com/blnkfinance/blnk/internal/hooks"
	"github.com/blnkfinance/blnk/internal/hotpairs"
	"github.com/blnkfinance/blnk/internal/notification"
	redis_db "github.com/blnkfinance/blnk/internal/redis-db"
	"github.com/blnkfinance/blnk/internal/search"
	"github.com/blnkfinance/blnk/internal/tokenization"

	"github.com/blnkfinance/blnk/model"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Blnk represents the main struct for the Blnk application.
type Blnk struct {
	queue       *Queue
	search      *search.TypesenseClient
	redis       redis.UniversalClient
	asynqClient *asynq.Client
	datasource  database.IDataSource
	bt          *model.BalanceTracker
	tokenizer   *tokenization.TokenizationService
	httpClient  *http.Client
	events      EventPublisher // Kafka-backed ledger event publisher; the no-op implementation when no brokers are configured.
	Hooks       hooks.HookManager
	config      *config.Configuration
	cache       cache.Cache
	hotPairs    *hotpairs.Manager

	// kafkaAdminMu guards kafkaAdmin, which is resolved on first use rather than at
	// construction.
	kafkaAdminMu sync.Mutex

	// kafkaAdmin is the PROCESS-WIDE Kafka administrative client.
	kafkaAdmin *KafkaAdminClient

	// background tracks compensating work scheduled off a response path.
	background sync.WaitGroup

	// legacyWebhookNow is the clock ProcessWebhook evaluates the webhook sunset against.
	legacyWebhookNow func() time.Time
}

// legacyWebhookClock returns the clock the legacy transport reads, defaulting to the
// wall clock.
//
// Nil-guarded rather than assigned in NewBlnk, because the harness in
// event_dual_delivery_test.go assembles a Blnk literal directly to keep TypeSense, the
// hook manager and the cache off the path under test. A field that had to be
// initialised by a constructor would be nil there and panic.
//
// Returns:
//   - time.Time: the instant to evaluate the sunset against.
func (b *Blnk) legacyWebhookClock() time.Time {
	if b == nil || b.legacyWebhookNow == nil {
		return time.Now()
	}

	return b.legacyWebhookNow()
}

const (
	GeneralLedgerID = "general_ledger_id"
)

//go:embed sql/*.sql
var SQLFiles embed.FS

// initializeRedisClients sets up both the Redis client and Asynq client
func initializeRedisClients(config *config.Configuration) (redis.UniversalClient, *asynq.Client, error) {
	redisClient, err := redis_db.NewRedisClient([]string{config.Redis.Dns}, config.Redis.SkipTLSVerify, &redis_db.PoolConfig{
		PoolSize:     config.Redis.PoolSize,
		MinIdleConns: config.Redis.MinIdleConns,
	})
	if err != nil {
		return nil, nil, err
	}

	redisOption, err := redis_db.ParseRedisURL(config.Redis.Dns, config.Redis.SkipTLSVerify)
	if err != nil {
		return nil, nil, err
	}

	asynqClient := asynq.NewClient(asynq.RedisClientOpt{
		Addr:      redisOption.Addr,
		Password:  redisOption.Password,
		DB:        redisOption.DB,
		TLSConfig: redisOption.TLSConfig,
		PoolSize:  config.Redis.PoolSize,
	})

	return redisClient.Client(), asynqClient, nil
}

// closeInitializedEventPublisher releases the publisher initializeEventPublisher built.
//
// It exists for the ONE construction error path in NewBlnk that now sits after the
// publisher: the publisher is built first, deliberately, so that a configuration
// refusal unwinds nothing — but a later failure must still not drop it. A Kafka-backed
// publisher owns one writer per topic, and every writer holds a shared transport with a
// connection pool and a background goroutine behind it.
//
// Parameters:
//   - publisher EventPublisher: the publisher to close. May be nil, and need not be
//     closeable.
func closeInitializedEventPublisher(publisher EventPublisher) {
	if publisher == nil {
		return
	}

	closer, closeable := publisher.(io.Closer)
	if !closeable {
		return
	}

	if err := closer.Close(); err != nil {
		withLoggableCause(nil, err).Warn(
			"blnk: closing the event publisher after a failed initialization; its connections are " +
				"released when the process exits",
		)
	}
}

// initializeTokenizationService creates and configures the tokenization service
func initializeTokenizationService(config *config.Configuration) *tokenization.TokenizationService {
	if config.TokenizationSecret == "" {
		return tokenization.NewTokenizationService(nil)
	}

	key := []byte(config.TokenizationSecret)
	return tokenization.NewTokenizationService(key)
}

// initializeHTTPClient creates and configures the HTTP client for webhook requests
func initializeHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		// The redirect refusal and the dial guard are the two halves of the legacy
		// transport's destination policy: a 3xx must not be able to walk a delivery onto an
		// address the URL check approved of, and a hostname must be judged by what it
		// actually resolves to rather than by how it is spelled. See the destination-guard
		// section of webhooks.go.
		CheckRedirect: refuseLegacyWebhookRedirect,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
				Control:   guardLegacyWebhookDial,
			}).DialContext,
		},
	}
}

// ProcessRole is which of Blnk's process roles a service container is being built for.
//
// It exists for ONE decision — whether this process builds a Kafka PRODUCER — and that
// decision is a least-privilege boundary rather than an optimisation.
//
// Only the server role publishes to Kafka. The relay is the sole thing that turns an
// outbox row into a Kafka message, and cmd/server.go is the only process that starts
// it, alongside the event metrics collector and the subscriber provisioning path.
type ProcessRole string

const (
	// ProcessRoleServer is the API server, which also hosts the event outbox relay, the
	// event metrics collector and the retention sweeper. It is the ONLY role that
	// publishes to Kafka, and therefore the only one that builds a producer.
	ProcessRoleServer ProcessRole = "server"

	// ProcessRoleWorker is the asynq worker: transaction processing, transaction hooks,
	// search indexing and, during the dual-delivery window, legacy webhook delivery.
	ProcessRoleWorker ProcessRole = "worker"

	// ProcessRoleTool is a one-shot command — migrate, verify-chain — that neither serves
	// requests nor drains a queue. It publishes nothing and builds no producer.
	ProcessRoleTool ProcessRole = "tool"
)

// PublishesEvents reports whether this role produces Kafka messages and therefore needs
// a real publisher.
//
// Returns:
//   - bool: true only for ProcessRoleServer.
func (r ProcessRole) PublishesEvents() bool {
	return r == ProcessRoleServer
}

// initializeEventPublisher creates and configures the Kafka event publisher for this
// process: ONE instance, built once and shared for the process lifetime, in the same
// shape as initializeHTTPClient.
//
// The check is made HERE rather than at the call sites so there is one place where
// "this process may write to Kafka" is decided, and it precedes the configuration read
// so a misconfigured credential cannot fail a role that would never have used it.
//
// Parameters:
//   - configuration *config.Configuration: the loaded configuration.
//   - role ProcessRole: the process role being built. Anything other than
//     ProcessRoleServer selects the no-op unconditionally.
//
// Returns:
//   - EventPublisher: the Kafka-backed publisher when this role publishes AND brokers
//     are configured, otherwise the no-op.
//   - error: non-nil only when a configured broker list's transport, credentials or TLS
//     material cannot be assembled securely — and therefore only for a publishing role.
func initializeEventPublisher(configuration *config.Configuration, role ProcessRole) (EventPublisher, error) {
	if !role.PublishesEvents() {
		// Debug rather than info: this is the normal, correct state for the role and it is
		// reported on every worker start-up. The condition an operator needs to notice is a
		// role that DOES publish and cannot, which NewEventPublisher reports itself.
		logrus.WithField("role", string(role)).Debug(
			"this process role does not publish ledger events, so no Kafka producer is built; " +
				"events are still captured in the outbox and published by the relay in the server role",
		)

		return NewNoopEventPublisher(), nil
	}

	publisher, err := NewEventPublisher(configuration)
	if err != nil {
		return nil, err
	}

	// Unreachable today — NewEventPublisher returns the no-op rather than nil for every
	// unconfigured case — and asserted anyway so the field's invariant is established
	// HERE, at the wiring site that owns it. Close and the relay both read b.events, and
	// "never nil" is far cheaper to guarantee once than to re-check at each use.
	if publisher == nil {
		return NewNoopEventPublisher(), nil
	}

	return publisher, nil
}

// NewBlnk initializes a new instance of Blnk with the provided database datasource. It
// fetches the configuration, initializes Redis client, balance tracker, queue, and
// search client.
//
// It builds the SERVER role, which is the publishing one. That is the compatible
// default: every existing caller and the whole test suite behaves exactly as before.
//
// Parameters:
// - db database.IDataSource: The datasource for database operations.
//
// Returns:
// - *Blnk: A pointer to the newly created Blnk instance.
// - error: An error if any of the initialization steps fail.
func NewBlnk(db database.IDataSource) (*Blnk, error) {
	return NewBlnkForRole(db, ProcessRoleServer)
}

// NewBlnkForRole initializes a Blnk instance for a named process role.
//
// It is NewBlnk with the one role-dependent decision made explicit: whether this
// process builds a Kafka producer. Everything else — the datasource, Redis, asynq, the
// balance tracker, the hot-pair manager, the queue, TypeSense, the hook manager, the
// tokenizer, the HTTP client and the cache — is identical in every role, because every
// role can serve every other part of the service container.
//
// Parameters:
//   - db database.IDataSource: the datasource for database operations.
//   - role ProcessRole: which process is being built. Only ProcessRoleServer receives a
//     real event publisher.
//
// Returns:
//   - *Blnk: the service container.
//   - error: a configuration or construction failure.
func NewBlnkForRole(db database.IDataSource, role ProcessRole) (*Blnk, error) {
	configuration, err := config.Fetch()
	if err != nil {
		return nil, err
	}

	// THE EVENT PUBLISHER IS BUILT FIRST, BEFORE ANY POOLED RESOURCE, so that a refusal
	// here cannot leak one. Closing pooled resources on that error path would also work;
	// ordering removes the class rather than one instance of it, because the publisher
	// validates PURE CONFIGURATION and opens no connection — there is nothing to unwind if
	// it refuses, and no future resource added between here and there can reintroduce the
	// leak.
	eventPublisher, err := initializeEventPublisher(configuration, role)
	if err != nil {
		return nil, err
	}

	redisClient, asynqClient, err := initializeRedisClients(configuration)
	if err != nil {
		// The publisher above owns per-topic writers, so it is closed rather than dropped:
		// each writer holds a shared transport that keeps a connection pool and a background
		// goroutine. The close error is LOGGED rather than returned, because the Redis
		// failure is what the caller needs to read.
		closeInitializedEventPublisher(eventPublisher)

		return nil, err
	}

	bt := NewBalanceTracker()
	hotPairManager := hotpairs.NewManager(redisClient, hotpairs.Config{
		Enabled:                 configuration.Queue.EnableHotLane,
		HotQueueName:            configuration.Queue.HotQueueName,
		HotPairTTL:              configuration.Queue.HotPairTTL,
		LockContentionThreshold: configuration.Queue.HotPairLockContentionThreshold,
	})
	newQueue := NewQueue(configuration, asynqClient)
	newSearch := search.NewTypesenseClient(configuration.TypeSenseKey, []string{configuration.TypeSense.Dns})
	hookManager := hooks.NewHookManager(redisClient, asynqClient)
	tokenizer := initializeTokenizationService(configuration)
	httpClient := initializeHTTPClient()

	newCache := cache.NewCacheWithClient(redisClient)

	b := &Blnk{
		datasource:  db,
		bt:          bt,
		queue:       newQueue,
		redis:       redisClient,
		asynqClient: asynqClient,
		search:      newSearch,
		tokenizer:   tokenizer,
		httpClient:  httpClient,
		events:      eventPublisher,
		Hooks:       hookManager,
		config:      configuration,
		cache:       newCache,
		hotPairs:    hotPairManager,
	}

	// PRODUCER CALL SITE FOR system.error, and the only one that is an indirection rather
	// than a direct emission. internal/notification cannot import this package, so it
	// holds a registered WebhookSender instead and NotifyError calls through it.
	//
	// The payload is FORWARDED VERBATIM and must stay that way. NotifyError builds the
	// LEGACY system.error body — exactly {"error", "time"}, the two keys the HTTP push has
	// always carried — and its classification, typed code and correlation id go to the log
	// line instead.
	notification.RegisterWebhookSender(func(event string, payload interface{}) error {
		capture, cancel := context.WithTimeout(context.Background(), systemErrorCaptureBudget)
		defer cancel()

		return b.PublishEvent(capture, NewWebhook{
			Event:   event,
			Payload: payload,
		})
	})

	// SAME-TRANSACTION CAPTURE FOR THE COALESCED BATCH, and the second indirection in this
	// constructor, for the same structural reason as the one above: the database package
	// cannot import this one, so it holds a registered capture instead.
	database.RegisterTransactionEventCapture(
		func(ctx context.Context, txn *model.Transaction, ledgerID string) (*model.EventOutbox, error) {
			return b.PrepareEventOutbox(ctx, NewWebhook{
				Event:   getEventFromStatus(txn.Status),
				Payload: txn,
			}, WithEventLedgerID(ledgerID))
		})

	// SAME-TRANSACTION CAPTURE FOR BALANCE MONITOR ALERTS, and the third indirection in
	// this constructor, for the same structural reason as the two above.
	database.RegisterBalanceMonitorAlertCapture(
		func(ctx context.Context, balance *model.Balance, monitor model.BalanceMonitor) (*model.EventOutbox, error) {
			return b.prepareBalanceMonitorAlertRow(ctx, balance, monitor)
		})

	return b, nil
}

// KafkaAdmin returns the process-wide Kafka administrative client, building it on first
// use.
//
// It performs NO I/O: like the publisher's constructor, it assembles a transport and
// dials nothing.
//
// Returns:
//   - *KafkaAdminClient: the shared client, never nil when the error is nil.
//   - error: when the configuration cannot be read, or the SASL credentials, the TLS
//     material or the plaintext acknowledgement make a secure transport impossible.
func (b *Blnk) KafkaAdmin() (*KafkaAdminClient, error) {
	if b == nil {
		return nil, errors.New("blnk: no instance, so no Kafka administrative client")
	}

	b.kafkaAdminMu.Lock()
	defer b.kafkaAdminMu.Unlock()

	if b.kafkaAdmin != nil {
		return b.kafkaAdmin, nil
	}

	configuration := b.config
	if configuration == nil {
		fetched, err := config.Fetch()
		if err != nil {
			return nil, err
		}

		configuration = fetched
	}

	admin, err := NewKafkaAdmin(configuration)
	if err != nil {
		return nil, err
	}

	b.kafkaAdmin = admin

	return b.kafkaAdmin, nil
}

// scheduleBackgroundWork runs a compensating task off the caller's goroutine and tracks
// it so Close can wait for it.
//
// Work whose outcome no response depends on, and only that: releasing a provisioning
// fence, revoking a credential whose issuance failed, clearing a registry record that
// no longer describes anything.
//
// Parameters:
//   - task func(): the work to run. Nil is ignored.
func (b *Blnk) scheduleBackgroundWork(task func()) {
	if b == nil || task == nil {
		return
	}

	b.background.Add(1)

	go func() {
		defer b.background.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				logrus.WithField("panic", recovered).Error(
					"blnk: a scheduled background cleanup panicked; the work it owed did not complete, " +
						"and whatever it was compensating is described by the log lines around this one",
				)
			}
		}()

		task()
	}()
}

// waitForBackgroundWork blocks until every scheduled cleanup has finished or the grace
// period expires, and reports which happened.
//
// Parameters:
//   - grace time.Duration: how long to wait. Non-positive waits not at all.
//
// Returns:
//   - bool: true when every task finished within the grace period.
func (b *Blnk) waitForBackgroundWork(grace time.Duration) bool {
	if b == nil {
		return true
	}

	drained := make(chan struct{})

	go func() {
		b.background.Wait()
		close(drained)
	}()

	if grace <= 0 {
		select {
		case <-drained:
			return true
		default:
			return false
		}
	}

	timer := time.NewTimer(grace)
	defer timer.Stop()

	select {
	case <-drained:
		return true
	case <-timer.C:
		return false
	}
}

// backgroundWorkDrainGrace bounds how long Close waits for scheduled cleanups.
//
// Two cleanup budgets. One is enough for a single task that is going to succeed; two leaves room
// for the compensation-then-release pair a failed issuance schedules together, which is the
// longest chain anything schedules.
const backgroundWorkDrainGrace = 10 * time.Second

// systemErrorCaptureBudget bounds the single outbox insert that captures a
// system.error.
//
// It exists because the notifier that reaches that insert has no context of its own:
// the WebhookSender signature carries none, and NotifyError runs detached from any
// request. Something has to supply the deadline, and the sender registered in NewBlnk
// is the only place that can.
const systemErrorCaptureBudget = 10 * time.Second

// Close properly closes all connections and resources used by the Blnk instance.
//
// The event publisher is released first. Its writers share a transport holding pooled
// broker connections, and closing a writer flushes whatever it has batched, so skipping
// it would both leak the connections and lose events that were accepted but not yet
// produced.
//
// Returns:
//   - error: the joined close errors, or nil when everything closed cleanly.
func (b *Blnk) Close() error {
	// SCHEDULED CLEANUPS FIRST, and before anything they depend on is released. They hold
	// a provisioning fence and reach the broker through the administrative client closed
	// below, so draining them afterwards would mean draining them into a closed transport
	// — the cleanup would fail on shutdown, which is precisely when its residue is least
	// likely to be noticed.
	if !b.waitForBackgroundWork(backgroundWorkDrainGrace) {
		logrus.Warn(
			"blnk: shutting down with scheduled cleanups still running after the drain grace period; " +
				"a credential revocation or fence release may not have completed. The preceding log " +
				"lines name anything that was outstanding, and a held fence expires with its lease",
		)
	}

	var publisherErr error
	if publisher, ok := b.events.(TopicEventPublisher); ok {
		publisherErr = publisher.Close()
	}

	// The process-wide administrative client. Closed here and only here: every
	// subscriber service that borrowed it treats it as not owned, so no request path can take
	// the transport away from the next one.
	var adminErr error

	b.kafkaAdminMu.Lock()
	admin := b.kafkaAdmin
	b.kafkaAdmin = nil
	b.kafkaAdminMu.Unlock()

	if admin != nil {
		adminErr = admin.Close()
	}

	var asynqErr error
	if b.asynqClient != nil {
		asynqErr = b.asynqClient.Close()
	}

	err := errors.Join(publisherErr, adminErr, asynqErr)

	// LOGGED, because the defect this method's invocation fixes was that nothing invoked
	// it A release step that leaves no trace is one an operator cannot confirm ran, and
	// the symptom of it not running — Kafka writer goroutines and broker connections
	// surviving the process's own shutdown sequence — is not visible from outside either.
	// One line at info makes the shutdown sequence readable end to end; the failure case
	// is a warning rather than an error because the process is going away regardless and
	// the kernel closes the sockets, so this reports what leaked its own way out rather
	// than a fault anyone can act on now.
	if err != nil {
		logrus.WithError(err).Warn(
			"blnk: releasing service resources reported errors; the process is exiting anyway, so " +
				"these describe what was not closed cleanly rather than work still to do",
		)
	} else {
		logrus.Info("blnk: service resources released")
	}

	return err
}

// Config returns the cached configuration for the Blnk instance.
// Falls back to config.Fetch() if not initialized (for backward compatibility with tests).
func (b *Blnk) Config() *config.Configuration {
	if b.config != nil {
		return b.config
	}
	cfg, err := config.Fetch()
	if err != nil {
		return &config.Configuration{}
	}
	return cfg
}

func (b *Blnk) GetSearchClient() *search.TypesenseClient {
	return b.search
}

func (b *Blnk) GetDataSource() database.IDataSource {
	return b.datasource
}
