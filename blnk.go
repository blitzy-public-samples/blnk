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
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

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

	// legacyWebhookNow is the clock ProcessWebhook evaluates the webhook sunset against.
	//
	// nil in every production construction, where it resolves to time.Now. It exists so a
	// test can pin the sunset boundary to the nanosecond, in exactly the shape
	// EventRelayProcessor already uses for the same purpose — and only the CLOCK is
	// injectable, never the predicate. WebhookSunsetPassed remains the single decision
	// point both the relay and the handler read, so the two cannot disagree about the rule
	// even while a test disagrees with the wall clock about the hour.
	legacyWebhookNow func() time.Time
}

// legacyWebhookClock returns the clock the legacy transport reads, defaulting to the wall
// clock.
//
// Nil-guarded rather than assigned in NewBlnk, because the harness in
// event_dual_delivery_test.go assembles a Blnk literal directly to keep TypeSense, the hook
// manager and the cache off the path under test. A field that had to be initialised by a
// constructor would be nil there and panic.
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

// closeInitializedRedisClients releases the two pooled clients initializeRedisClients built.
//
// It exists for the construction error paths in NewBlnk. Both clients own descriptors and
// background goroutines from the moment they are created, so an error exit that returns without
// closing them leaks a pair per attempt — and the attempts that reach such an exit are exactly
// the ones a supervised process repeats, because they are configuration failures.
//
// Failures are LOGGED, never returned. The caller is already on its way out with the error the
// operator has to read, and replacing that with "closing redis failed" would hide the real
// problem behind cleanup noise. A nil client is skipped, so the helper is safe to call from any
// point after construction.
//
// Parameters:
//   - redisClient redis.UniversalClient: the client to close. May be nil.
//   - asynqClient *asynq.Client: the client to close. May be nil.
func closeInitializedRedisClients(redisClient redis.UniversalClient, asynqClient *asynq.Client) {
	if asynqClient != nil {
		if err := asynqClient.Close(); err != nil {
			logrus.WithError(err).Warn(
				"blnk: closing the asynq client after a failed initialization; its connections are " +
					"released when the process exits",
			)
		}
	}

	if redisClient != nil {
		if err := redisClient.Close(); err != nil {
			logrus.WithError(err).Warn(
				"blnk: closing the redis client after a failed initialization; its connections are " +
					"released when the process exits",
			)
		}
	}
}

// closeInitializedEventPublisher releases the publisher initializeEventPublisher built.
//
// It exists for the ONE construction error path in NewBlnk that now sits after the publisher:
// the publisher is built first, deliberately, so that a configuration refusal unwinds nothing —
// but a later failure must still not drop it. A Kafka-backed publisher owns one writer per
// topic, and every writer holds a shared transport with a connection pool and a background
// goroutine behind it.
//
// Failures are LOGGED, never returned, for the same reason closeInitializedRedisClients logs
// its own: the caller is already on its way out with the error the operator has to read. A nil
// publisher is skipped, and the no-op publisher's Close is a no-op, so this is safe on every
// path.
//
// The parameter is the MANDATED narrow interface rather than TopicEventPublisher, matching
// what initializeEventPublisher returns, and Close is reached by assertion. That keeps the
// helper correct for a publisher supplied by a test double that implements only Publish:
// there is then nothing to close, and nothing to panic over either.
//
// Parameters:
//   - publisher EventPublisher: the publisher to close. May be nil, and need not be closeable.
func closeInitializedEventPublisher(publisher EventPublisher) {
	if publisher == nil {
		return
	}

	closer, closeable := publisher.(io.Closer)
	if !closeable {
		return
	}

	if err := closer.Close(); err != nil {
		logrus.WithError(err).Warn(
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

// initializeEventPublisher creates and configures the Kafka event publisher for this
// process: ONE instance, built once and shared for the process lifetime, in the same shape
// as initializeHTTPClient.
//
// The sharing is load-bearing. The publisher owns one kafka.Writer per topic over a SINGLE
// SHARED TRANSPORT, so every writer draws on that transport's connection pool and its one
// authentication and TLS configuration, and connections are established lazily per broker
// and then reused. Building a publisher per publish would give each event an empty
// partition-metadata cache and an empty pool, paying a metadata round trip plus a TCP and
// SASL handshake before it could produce anything.
//
// # An empty broker list is a legitimate steady state, not an error
//
// With no brokers configured this returns the NO-OP publisher and a NIL ERROR: a Blnk
// deployment has always been able to run with no notification sink, exactly as SendWebhook
// returns nil without enqueuing when no webhook URL is configured. Every deployment that
// does not run Kafka, and every existing test that constructs a Blnk instance with nothing
// but a Redis DSN, keeps working — so a whitespace-only entry left by a stray separator
// resolves the same way as an unset KAFKA_BROKERS.
//
// # It performs no I/O
//
// Nothing here dials a broker, resolves a name, fetches metadata or blocks, on either path;
// writers connect on their first write. NewBlnk runs on every process's startup path and
// throughout the test suite, so a network call, a delay or an error here would make both
// depend on a broker being reachable.
//
// The one error it can surface comes from ASSEMBLING the transport for a broker list that
// IS configured — a malformed credential, unreadable TLS material, or plaintext without the
// explicit local-development acknowledgement. That is a fatal misconfiguration and is
// returned rather than downgraded to the no-op, because silently publishing nothing is the
// failure mode the loud error exists to prevent. It is propagated unwrapped, matching
// initializeRedisClients above.
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
//   - error: non-nil only when a configured broker list's transport, credentials or TLS
//     material cannot be assembled securely.
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

	// THE EVENT PUBLISHER IS BUILT FIRST, BEFORE ANY POOLED RESOURCE. RES-01.
	//
	// The Redis client and the asynq client below are both POOLED CONNECTION OWNERS: each
	// holds file descriptors and a background goroutine set from the moment it is built.
	// Constructing them first and validating the publisher afterwards leaked BOTH on every
	// failed construction, and this is not a theoretical path — initializeEventPublisher
	// fails on a malformed CA bundle, a SASL credential SASLprep rejects, or TLS disabled
	// without the local-development acknowledgement, all of which are configuration mistakes
	// that a supervised process retries. Each retry leaked another pair, so a misconfigured
	// deployment exhausted descriptors rather than failing cleanly on the first attempt.
	//
	// Closing them on that error path would also have worked. Ordering is the better fix
	// because it removes the class rather than one instance of it: the publisher validates
	// PURE CONFIGURATION and opens no connection, so there is nothing to unwind if it
	// refuses, and no future resource added between here and there can reintroduce the leak.
	eventPublisher, err := initializeEventPublisher(configuration)
	if err != nil {
		return nil, err
	}

	redisClient, asynqClient, err := initializeRedisClients(configuration)
	if err != nil {
		// The publisher above owns per-topic writers, so it is closed rather than dropped:
		// each writer holds a shared transport that keeps a connection pool and a background
		// goroutine. The close error is LOGGED rather than returned, because the Redis failure
		// is what the caller needs to read.
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
	// than a direct emission. internal/notification cannot import this package, so it holds
	// a registered WebhookSender instead and NotifyError calls through it.
	//
	// Routing it through PublishEvent is what gives system.error the same durable retry,
	// dead-letter and replay treatment as every other event type, and — during the
	// dual-delivery window — delivery over both transports from one outbox row.
	// notification.WebhookSender and NotifyError's signature are untouched, so capturing
	// this event costs the notification package no import churn.
	//
	// context.Background() is deliberate: WebhookSender supplies no context and must not
	// grow one, and NotifyError already runs this on its own goroutine detached from any
	// request, so there is no caller context to inherit and no deadline to respect.
	//
	// The payload is FORWARDED VERBATIM and must stay that way. NotifyError builds the
	// LEGACY system.error body — exactly {"error", "time"}, the two keys the HTTP push has
	// always carried — and its classification, typed code and correlation id go to the log
	// line instead. Re-deriving or enriching the payload here would change the schema a
	// subscriber parses while looking like a harmless addition, which is the one thing the
	// payload-preservation guarantee forbids.
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
// The event publisher is released first. Its writers share a transport holding pooled
// broker connections, and closing a writer flushes whatever it has batched, so skipping it
// would both leak the connections and lose events that were accepted but not yet produced.
// Both publisher implementations are idempotent and nil-safe on Close, so this stays as
// simple as the nil-guarded asynq close it sits beside — the no-op publisher releases
// nothing because it acquired nothing.
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
