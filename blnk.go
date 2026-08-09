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

	// kafkaAdmin is the PROCESS-WIDE Kafka administrative client. PERF-P10.
	//
	// Nil until the first operation that genuinely needs it. Every subscriber-management
	// request used to build one of its own and close it again, paying a transport, a TCP
	// connection per broker, a two-round-trip SASL/SCRAM handshake and a cold metadata cache
	// before it could send its first administrative request — inside a five-second budget.
	// One client for the process pays that once. See KafkaAdmin.
	kafkaAdmin *KafkaAdminClient

	// background tracks compensating work scheduled off a response path. PERF-P09.
	//
	// It exists so that Close can WAIT for that work: scheduling a cleanup is only legitimate
	// if a graceful shutdown still finishes it, and a WaitGroup is what turns "it runs in a
	// goroutine" into "it ran".
	background sync.WaitGroup

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

// closeInitializedEventPublisher releases the publisher initializeEventPublisher built.
//
// It exists for the ONE construction error path in NewBlnk that now sits after the publisher:
// the publisher is built first, deliberately, so that a configuration refusal unwinds nothing —
// but a later failure must still not drop it. A Kafka-backed publisher owns one writer per
// topic, and every writer holds a shared transport with a connection pool and a background
// goroutine behind it.
//
// Failures are LOGGED, never returned: the caller is already on its way out with the error the
// operator has to read, and replacing that with "closing the publisher failed" would hide the
// real problem behind cleanup noise. A nil publisher is skipped, and the no-op publisher's
// Close is a no-op, so this is safe on every path.
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
// # Why the constructor needs to know
//
// It exists for ONE decision — whether this process builds a Kafka PRODUCER — and that
// decision is a least-privilege boundary rather than an optimisation.
//
// Only the server role publishes to Kafka. The relay is the sole thing that turns an outbox
// row into a Kafka message, and cmd/server.go is the only process that starts it, alongside
// the event metrics collector and the subscriber provisioning path. Every other role writes
// outbox rows and nothing else.
//
// Building the producer everywhere anyway is what this type prevents, and the cost was not
// theoretical. The worker received KAFKA_SASL_USER and KAFKA_SASL_SECRET and built one
// kafka.Writer per owned topic over an authenticated transport — standing WRITE authority on
// every Blnk topic, including the dead-letter siblings, held by a process with no code path
// that produces a message. A compromised worker could forge any ledger event onto any topic,
// and a subscriber cannot tell a forged event from a real one because both arrive with valid
// producer credentials. It also made a broker Blnk did not need for that role into something
// the role's start-up depended on.
//
// # What a non-publishing role loses, and what it keeps
//
// It loses only the producer. Event CAPTURE is untouched, because capture is gated on
// KAFKA_BROKERS being configured (eventPublishingConfigured reads configuration, not the
// publisher), so a worker still enrols every event in its ledger transaction exactly as
// before and the relay in the server role publishes those rows. What the worker no longer
// holds is a credential and a set of connections it never used.
type ProcessRole string

const (
	// ProcessRoleServer is the API server, which also hosts the event outbox relay, the
	// event metrics collector and the retention sweeper. It is the ONLY role that publishes
	// to Kafka, and therefore the only one that builds a producer.
	//
	// It is the default: NewBlnk resolves to this role, so every existing caller — including
	// the whole test suite — behaves exactly as it did.
	ProcessRoleServer ProcessRole = "server"

	// ProcessRoleWorker is the asynq worker: transaction processing, transaction hooks,
	// search indexing and, during the dual-delivery window, legacy webhook delivery.
	//
	// It CAPTURES events (transaction.rejected reaches the outbox through
	// RejectTransaction's post-transaction actions) and publishes none, so it builds no
	// producer and needs no broker credential.
	ProcessRoleWorker ProcessRole = "worker"

	// ProcessRoleTool is a one-shot command — migrate, verify-chain — that neither serves
	// requests nor drains a queue. It publishes nothing and builds no producer.
	ProcessRoleTool ProcessRole = "tool"
)

// PublishesEvents reports whether this role produces Kafka messages and therefore needs a
// real publisher.
//
// The test is an ALLOWLIST rather than a denylist: only the server role publishes, so a role
// added later without a decision recorded here is treated as non-publishing. That is the
// direction least-privilege has to fail in — a new role that silently acquired write
// authority over every ledger topic is exactly the outcome this type exists to prevent.
//
// Returns:
//   - bool: true only for ProcessRoleServer.
func (r ProcessRole) PublishesEvents() bool {
	return r == ProcessRoleServer
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
// # It is ROLE-AWARE, and a non-publishing role gets the no-op
//
// Only ProcessRoleServer publishes — see ProcessRole. For every other role this returns the
// no-op publisher WITHOUT reading the broker list, the SASL pair or the TLS material, so that
// role holds no producer credential, opens no broker connection and does not fail to start
// because a broker is unreachable. Event capture is unaffected: it is gated on
// KAFKA_BROKERS through eventPublishingConfigured, never on which publisher this returned.
//
// The check is made HERE rather than at the call sites so there is one place where "this
// process may write to Kafka" is decided, and it precedes the configuration read so a
// misconfigured credential cannot fail a role that would never have used it.
//
// Parameters:
//   - configuration *config.Configuration: the loaded configuration. May be nil, which
//     selects the no-op.
//   - role ProcessRole: the process role being built. Anything other than
//     ProcessRoleServer selects the no-op unconditionally.
//
// Returns:
//   - EventPublisher: the Kafka-backed publisher when this role publishes AND brokers are
//     configured, otherwise the no-op. Never nil when the error is nil.
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

// NewBlnk initializes a new instance of Blnk with the provided database datasource.
// It fetches the configuration, initializes Redis client, balance tracker, queue, and search client.
//
// It builds the SERVER role, which is the publishing one. That is the compatible default:
// every existing caller and the whole test suite behaves exactly as before. A process that is
// not the server — the asynq worker, or a one-shot command — should call NewBlnkForRole so it
// is not handed a Kafka producer it never uses; see ProcessRole for why that matters.
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
// It is NewBlnk with the one role-dependent decision made explicit: whether this process
// builds a Kafka producer. Everything else — the datasource, Redis, asynq, the balance
// tracker, the hot-pair manager, the queue, TypeSense, the hook manager, the tokenizer, the
// HTTP client and the cache — is identical in every role, because every role can serve every
// other part of the service container.
//
// Parameters:
//   - db database.IDataSource: the datasource for database operations. May be nil; several
//     tests construct an instance that way and every event path is nil-guarded.
//   - role ProcessRole: which process is being built. Only ProcessRoleServer receives a real
//     event publisher.
//
// Returns:
//   - *Blnk: the service container.
//   - error: a configuration or construction failure.
func NewBlnkForRole(db database.IDataSource, role ProcessRole) (*Blnk, error) {
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
	eventPublisher, err := initializeEventPublisher(configuration, role)
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

	// SAME-TRANSACTION CAPTURE FOR THE COALESCED BATCH (requirement R-2), and the second
	// indirection in this constructor, for the same structural reason as the one above: the
	// database package cannot import this one, so it holds a registered capture instead.
	//
	// The coalescing path commits many transactions in one database transaction and assembles
	// that call without event rows. Its file belongs to the frozen transaction pipeline, so the
	// rows cannot be added at the call site; the writer derives them through this callback
	// instead, inside the same transaction as the balance updates it is committing.
	//
	// The row is built by the SAME code the direct paths use — getEventFromStatus for the event
	// name, the transaction itself as the payload, PrepareEventOutbox for the envelope — so an
	// event captured for a coalesced transaction is indistinguishable from one captured for a
	// singly-executed one. That identity is what makes the two paths interchangeable to a
	// subscriber, and it is why the ledger arrives as a parameter: the writer resolves it from
	// the balance set by the same source-then-destination rule transactionLedgerID applies, so
	// both paths key the event on the same partition.
	//
	// Returning (nil, nil) when publishing is unconfigured is PrepareEventOutbox's own
	// behaviour and is preserved deliberately: the writer then inserts nothing and a
	// broker-less deployment commits exactly the rows it always did.
	database.RegisterTransactionEventCapture(
		func(ctx context.Context, txn *model.Transaction, ledgerID string) (*model.EventOutbox, error) {
			return b.PrepareEventOutbox(ctx, NewWebhook{
				Event:   getEventFromStatus(txn.Status),
				Payload: txn,
			}, WithEventLedgerID(ledgerID))
		})

	return b, nil
}

// KafkaAdmin returns the process-wide Kafka administrative client, building it on first use.
// PERF-P10.
//
// # Why one client, for the process
//
// Administrative work — provisioning a subscriber's SCRAM credential, binding or pruning its
// ACLs, revoking it, assuring topics, measuring consumer lag — reaches the broker over an
// authenticated connection whose setup is not free: a transport, a TCP connection per broker, a
// SASL/SCRAM handshake whose proof is PBKDF2-derived over two round trips, and a metadata cache
// that starts empty. Every subscriber-management request used to pay all of it, because the
// Blnk wrappers build a subscriber service per request and close it afterwards, and the service
// built its own client. Credential issuance has a five-second budget and was spending a
// measurable part of it on setup that this process had already done. One shared client pays it
// once and lets the broker keep its authorization decisions warm too.
//
// # Why LAZY, and why only success is remembered
//
// Construction reads TLS material from disk and validates the ADMINISTRATIVE SASL pair, which
// the publisher's producer role does not always validate — so building eagerly in NewBlnk would
// newly refuse to start a deployment that publishes happily and never issues a credential.
// Resolving on first use keeps that a request-time 503, exactly as it is today.
//
// A failure is NOT cached: the causes are external and fixable — an unreadable CA bundle, a
// secret that has not been mounted yet — and a permanently poisoned accessor would require a
// restart to recover from something that no longer applies. Each caller therefore pays one
// construction attempt while the configuration is broken, and the first success is shared by
// everything afterwards.
//
// It performs NO I/O: like the publisher's constructor, it assembles a transport and dials
// nothing.
//
// Returns:
//   - *KafkaAdminClient: the shared client, never nil when the error is nil. It is safe for
//     concurrent use and must NOT be closed by the caller — Close owns it.
//   - error: when the configuration cannot be read, or the SASL credentials, the TLS material
//     or the plaintext acknowledgement make a secure transport impossible.
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

// scheduleBackgroundWork runs a compensating task off the caller's goroutine and tracks it so
// Close can wait for it. PERF-P09.
//
// # What belongs here
//
// Work whose outcome no response depends on, and only that: releasing a provisioning fence,
// revoking a credential whose issuance failed, clearing a registry record that no longer
// describes anything. Each is bounded by its own deadline, each logs its own outcome, and each
// used to be performed inline — which is how a five-second endpoint came to answer in ten on
// success and twenty-five on failure.
//
// # Why it is tracked rather than fired and forgotten
//
// A cleanup that a process exit can silently cancel is not a cleanup; it is a comment. The
// WaitGroup lets Close drain outstanding tasks, so a graceful shutdown finishes the revocation
// of a credential nobody is tracking instead of abandoning it. A hard kill still abandons it,
// which is why every one of these tasks also leaves a durable trace — a leased fence that
// expires, a logged principal, a registry record that reads as over-reporting access.
//
// A panic inside a task is recovered and logged rather than taking the process down: these run
// after a response has been written, so there is no caller to attribute the failure to and no
// reason for one cleanup's defect to end a healthy server.
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

// waitForBackgroundWork blocks until every scheduled cleanup has finished or the grace period
// expires, and reports which happened.
//
// A bounded wait rather than an unbounded one, for the reason every detached write in this
// codebase is bounded: "finish what you owe" must not become "refuse to shut down because a
// broker stopped answering". A wait that times out is reported so the caller can say so, since
// the tasks still outstanding at that point are the ones whose residue an operator may have to
// deal with by hand.
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
	// SCHEDULED CLEANUPS FIRST, and before anything they depend on is released (PERF-P09).
	// They hold a provisioning fence and reach the broker through the administrative client
	// closed below, so draining them afterwards would mean draining them into a closed
	// transport — the cleanup would fail on shutdown, which is precisely when its residue is
	// least likely to be noticed.
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

	// The process-wide administrative client (PERF-P10). Closed here and only here: every
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

	// LOGGED, because the defect this method's invocation fixes was that nothing invoked it
	// (PERF-P17). A release step that leaves no trace is one an operator cannot confirm ran,
	// and the symptom of it not running — Kafka writer goroutines and broker connections
	// surviving the process's own shutdown sequence — is not visible from outside either. One
	// line at info makes the shutdown sequence readable end to end; the failure case is a
	// warning rather than an error because the process is going away regardless and the
	// kernel closes the sockets, so this reports what leaked its own way out rather than a
	// fault anyone can act on now.
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
