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
	"net/http"
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
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

// initializeEventPublisher creates and configures the Kafka event publisher for this
// process, in the same shape as initializeHTTPClient does for the legacy transport: ONE
// instance, built once here and shared for the process lifetime.
//
// That sharing is the point rather than an economy. The publisher owns one kafka.Writer
// per topic over a single transport, so every writer draws on one connection pool and one
// SASL session per broker. Building a publisher per publish would give each event an empty
// partition-metadata cache and an empty connection pool, so it would pay a metadata round
// trip plus a TCP and SASL handshake before it could produce anything.
//
// # An empty broker list is a legitimate steady state, not an error
//
// With no brokers configured this returns the NO-OP publisher and a NIL ERROR. It never
// reports a problem, because there is none: a Blnk deployment has always been able to run
// with no notification sink, exactly as SendWebhook returns nil without enqueuing when no
// webhook URL is configured (webhooks.go). Every deployment that does not run Kafka, and
// every existing test that constructs a Blnk instance with nothing but a Redis DSN, must
// keep working unchanged — so a whitespace-only entry left by a stray separator in an
// environment file resolves the same way as an unset KAFKA_BROKERS.
//
// # It performs no I/O
//
// Nothing here dials a broker, resolves a name, fetches metadata, or blocks, on either
// path. NewEventPublisher documents and enforces that property; writers connect lazily on
// their first write. It is load-bearing: NewBlnk runs on every process's startup path and
// throughout the existing test suite, so a network call, a delay or an error here would
// make both depend on a broker being reachable.
//
// The one error it can surface comes from preparing the SASL/SCRAM credentials or the TLS
// material for a broker list that IS configured — a malformed credential, unreadable TLS
// material, or plaintext without the explicit local-development acknowledgement. That is a
// fatal misconfiguration and is returned rather than downgraded to the no-op, because
// silently publishing nothing is the failure mode the loud error exists to prevent. The
// error is propagated unwrapped, matching initializeRedisClients above.
//
// # Ownership
//
// The returned publisher belongs to the Blnk instance that holds it, and Close releases it.
// A process that also wants it reachable from a request handler donates this same instance
// with SetSharedEventPublisher rather than letting a second one be built; a donated
// publisher is explicitly not owned by the shared slot, so it is closed here and only here.
//
// Parameters:
//   - configuration *config.Configuration: the loaded configuration. May be nil, which
//     selects the no-op.
//
// Returns:
//   - EventPublisher: the Kafka-backed publisher when brokers are configured, otherwise the
//     no-op. Never nil when the error is nil.
//   - error: non-nil only when a configured broker list cannot be dialled securely or its
//     credentials cannot be prepared.
func initializeEventPublisher(configuration *config.Configuration) (EventPublisher, error) {
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

// NewBlnk initializes a new instance of Blnk with the provided database datasource.
// It fetches the configuration, initializes Redis client, balance tracker, queue, and search client.
//
// Parameters:
// - db database.IDataSource: The datasource for database operations.
//
// Returns:
// - *Blnk: A pointer to the newly created Blnk instance.
// - error: An error if any of the initialization steps fail.
func NewBlnk(db database.IDataSource) (*Blnk, error) {
	configuration, err := config.Fetch()
	if err != nil {
		return nil, err
	}

	redisClient, asynqClient, err := initializeRedisClients(configuration)
	if err != nil {
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
	eventPublisher, err := initializeEventPublisher(configuration)
	if err != nil {
		return nil, err
	}

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
	// than a direct emission. internal/notification cannot import this package, so it holds
	// a registered WebhookSender instead and NotifyError calls through it.
	//
	// Only the CLOSURE BODY changes here: the event now goes to the transactional outbox
	// instead of straight onto the legacy webhook queue, so system.error gets the same
	// durable retry, dead-letter and replay treatment as every other event, and — during
	// the dual-delivery window — is delivered over both transports from that one row. The
	// closure's signature, notification.WebhookSender and NotifyError's signature are all
	// untouched, which is why capturing the event costs the notification package no import
	// churn at all.
	//
	// context.Background() is deliberate. WebhookSender supplies no context and must not
	// grow one, and NotifyError already runs this on its own goroutine detached from any
	// request, so there is no caller context to inherit and no deadline to respect.
	//
	// The payload is FORWARDED VERBATIM and must stay that way. NotifyError has already
	// sanitized it — the raw error text is logged against a correlation id rather than put
	// on the wire — so re-deriving or enriching it here would undo that deliberately and
	// invisibly.
	//
	// COUPLING TO WATCH — notification.NotifyError guards this call on the event transports
	// being configured (`len(conf.Kafka.Brokers) > 0 || conf.Notification.Webhook.Url != ""`
	// in internal/notification/notification.go). Registering the sender here is therefore
	// necessary but not sufficient: were that guard to test the webhook URL alone, a
	// Kafka-only deployment would never emit system.error and event coverage would be
	// incomplete with nothing failing to say so.
	notification.RegisterWebhookSender(func(event string, payload interface{}) error {
		return b.PublishEvent(context.Background(), NewWebhook{
			Event:   event,
			Payload: payload,
		})
	})

	return b, nil
}

// Close properly closes all connections and resources used by the Blnk instance.
//
// The event publisher is released first. Its writers hold pooled broker connections and a
// SASL session per broker, and closing a writer flushes whatever it has batched, so
// skipping it would both leak the connections and lose events that were accepted but not
// yet produced. Both publisher implementations are idempotent and nil-safe on Close, so
// this stays as simple as the nil-guarded asynq close it sits beside — the no-op publisher
// releases nothing because it acquired nothing.
//
// The type assertion is how the narrow mandated EventPublisher contract, which has no
// Close, reaches the fuller TopicEventPublisher one that does. Both implementations satisfy
// it; a publisher that does not is skipped rather than failing shutdown, and an unset field
// yields ok == false because a nil interface matches no type.
//
// Errors are JOINED rather than short-circuited so that a failing publisher cannot leave
// the asynq client open, and vice versa. errors.Join reports nil when every close succeeded,
// so a caller sees exactly what it saw before this: the asynq client's error, or nil.
//
// Returns:
//   - error: the joined close errors, or nil when everything closed cleanly.
func (b *Blnk) Close() error {
	var publisherErr error
	if publisher, ok := b.events.(TopicEventPublisher); ok {
		publisherErr = publisher.Close()
	}

	var asynqErr error
	if b.asynqClient != nil {
		asynqErr = b.asynqClient.Close()
	}

	return errors.Join(publisherErr, asynqErr)
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
