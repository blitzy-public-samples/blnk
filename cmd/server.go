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
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api"
	"github.com/blnkfinance/blnk/api/middleware"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/logsafe"
	"github.com/blnkfinance/blnk/internal/search"
	trace "github.com/blnkfinance/blnk/internal/traces"
	"github.com/caddyserver/certmagic"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/posthog/posthog-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// eventTopicAssuranceTimeout bounds the Kafka topic assurance performed at start-up.
const eventTopicAssuranceTimeout = 15 * time.Second

// serverShutdownTimeout bounds the graceful drain: in-flight requests are given this
// long to finish before the listener is closed and the process moves on. A clean stop is
// the ordinary outcome rather than the exceptional one.
const serverShutdownTimeout = 20 * time.Second

// telemetryFlushTimeout bounds the final span and metric flush on the way out. Long
// enough that a collector on the far side of a slow link still receives the last batch,
// and short enough that nobody waiting on the process would notice.
const telemetryFlushTimeout = 10 * time.Second

// Bounds on the HTTP server itself, every one stated explicitly so the whole exposure is
// visible in one place rather than inherited from a library default.
const (
	// httpReadHeaderTimeout bounds how long a peer may take to send its request headers.
	httpReadHeaderTimeout = 15 * time.Second

	// httpReadTimeout bounds the whole request read, headers and body together. Sized for a
	// legitimate upload over a slow link rather than for an idle attacker, which the header
	// bound above catches far sooner.
	httpReadTimeout = 5 * time.Minute

	// httpWriteTimeout bounds the response write, sized for a streamed export.
	httpWriteTimeout = 5 * time.Minute

	// httpIdleTimeout bounds a kept-alive connection between requests. Comfortably longer
	// than a browser's or client pool's reuse interval, so this reclaims abandoned sockets
	// without forcing healthy clients to reconnect.
	httpIdleTimeout = 120 * time.Second

	// httpMaxHeaderBytes bounds the header block. Stated explicitly rather than left to
	// net/http's default, so the limit is visible beside the other bounds and a change to the
	// library default cannot silently change Blnk's exposure.
	httpMaxHeaderBytes = 1 << 20
)

// Retry pacing for start-up topic assurance. Every attempt is logged, because a broker
// that never becomes reachable must be visible in the log rather than not be noticed for
// hours.
const (
	eventTopicAssuranceRetryBase = 5 * time.Second
	eventTopicAssuranceRetryMax  = 60 * time.Second
)

// Leadership timings for the event maintenance work.
const (
	// eventMaintenanceRetryInterval is how often a replica that is NOT the leader tries
	// again.
	eventMaintenanceRetryInterval = 30 * time.Second

	// eventMaintenanceVerifyInterval is how often the leader re-confirms with the database
	// that it is still the leader. See EventMaintenanceLease.StillHeld for why believing it is
	// not sufficient.
	eventMaintenanceVerifyInterval = 30 * time.Second

	// eventMaintenanceLeaseOpTimeout bounds one acquire, verify or release.
	eventMaintenanceLeaseOpTimeout = 5 * time.Second
)

// resolveCertStoragePath returns the configured certificate storage path,
// falling back to the default location when unset.
func resolveCertStoragePath(conf config.ServerConfig) string {
	if conf.CertStoragePath == "" {
		return "/var/lib/blnk/certs"
	}
	return conf.CertStoragePath
}

// resolveTLSDomains returns the certificate domains, defaulting to localhost
// when no domain is configured.
func resolveTLSDomains(conf config.ServerConfig) []string {
	if conf.Domain == "" {
		return []string{"localhost"}
	}
	return []string{conf.Domain}
}

/*
serveTLS starts an HTTPS server with TLS enabled using CertMagic for automatic certificate management.
It accepts a gin.Engine instance as the router and a ServerConfig struct for server configurations.
If no domain is specified, the server will default to running on localhost.
*/
func serveTLS(r *gin.Engine, conf config.ServerConfig) error {
	// Configure CertMagic's ACME (Automatic Certificate Management Environment) for automatic TLS
	certmagic.DefaultACME.Agreed = true      // Agree to ACME TOS
	certmagic.DefaultACME.Email = conf.Email // Set email for certificate recovery/notifications
	cfg := certmagic.NewDefault()

	cfg.Storage = &certmagic.FileStorage{Path: resolveCertStoragePath(conf)}

	// Define domain(s) for the certificate
	if conf.Domain == "" {
		logrus.Error("No domain specified, defaulting to localhost")
	}
	domains := resolveTLSDomains(conf)

	// Manage TLS certificates for the specified domains
	if err := cfg.ManageSync(context.Background(), domains); err != nil {
		return err
	}

	// Create and configure the HTTPS server.
	server := hardenHTTPServer(&http.Server{
		Addr:      ":" + conf.Port, // Server address and port
		Handler:   r,               // Handler for HTTP requests (gin router)
		TLSConfig: cfg.TLSConfig(), // TLS configuration from CertMagic
	})

	logrus.Errorf("Starting HTTPS server on %s\n", conf.Port)
	// Start the HTTPS server with automatic certificate management.
	if err := server.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("failed to start the HTTPS server on port %s: %w", conf.Port, err)
	}

	return nil
}

/*
migrateTypeSenseSchema ensures that the necessary TypeSense schema is migrated for all required collections.
It takes a TypesenseClient and a context as parameters.
This function loops through the predefined collections and migrates their schema in TypeSense.
*/
func migrateTypeSenseSchema(ctx context.Context, t *search.TypesenseClient) error {
	// Define the collections to migrate schema for
	collections := []string{"ledgers", "balances", "transactions", "identities", "reconciliations"}

	// Migrate schema for each collection
	for _, c := range collections {
		err := t.MigrateTypeSenseSchema(ctx, c)
		if err != nil {
			return err // Return if an error occurs during migration
		}
	}
	return nil
}

func getOrCreateHeartbeatID() string {
	return getOrCreateHeartbeatIDAt("./heartbeat.db")
}

// getOrCreateHeartbeatIDAt persists a stable heartbeat UUID in a SQLite file
// at the given path, creating it on first use. Any storage failure falls
// back to a fresh (non-persistent) UUID.
func getOrCreateHeartbeatIDAt(path string) string {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		logrus.Errorf("Failed to open SQLite DB: %v", err)
		return uuid.New().String() // fallback to temp UUID
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)`)
	if err != nil {
		logrus.Errorf("Failed to create config table: %v", err)
		return uuid.New().String()
	}

	var heartbeatID string
	err = db.QueryRow(`SELECT value FROM config WHERE key = 'heartbeat_id'`).Scan(&heartbeatID)
	if err == sql.ErrNoRows {
		heartbeatID = uuid.New().String()
		_, err = db.Exec(`INSERT INTO config (key, value) VALUES (?, ?)`, "heartbeat_id", heartbeatID)
		if err != nil {
			logrus.Errorf("Failed to insert heartbeat_id: %v", err)
		}
	} else if err != nil {
		logrus.Errorf("Failed to read heartbeat_id: %v", err)
		return uuid.New().String()
	}

	return heartbeatID
}

// sendHeartbeat initializes and maintains a periodic heartbeat to PostHog
func sendHeartbeat(client posthog.Client, heartbeatID string) {
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for range ticker.C {
			if err := client.Enqueue(posthog.Capture{
				DistinctId: heartbeatID,
				Event:      "server_heartbeat",
				Timestamp:  time.Now().UTC(),
				Properties: map[string]interface{}{
					"timestamp": time.Now().UTC(),
				},
			}); err != nil {
				logrus.Errorf("Failed to send heartbeat: %v", err)
			}
		}
	}()
}

func healthCheckHandler(c *gin.Context) {
	cfg, err := config.Fetch()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "DOWN", "reason": "config unavailable"})
		return
	}

	ds, err := database.GetDBConnection(cfg)
	if err != nil || ds == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "DOWN", "reason": "database unreachable"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	if err := ds.Conn.PingContext(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "DOWN", "reason": "database ping failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "UP"})
}

func initializeRouter(b *blnkInstance) *gin.Engine {
	router := api.NewAPI(b.blnk).Router()
	router.GET("/health", healthCheckHandler)
	if h := trace.MetricsHandler(); h != nil {
		cfg, _ := config.Fetch()
		var secure bool
		var token string
		if cfg != nil {
			secure = cfg.Server.Secure
			token = cfg.Server.MetricsBearerToken
		}
		router.GET("/metrics", middleware.MetricsAuth(secure, token), gin.WrapH(h))
	}
	return router
}

func initializeOpenTelemetry(ctx context.Context, monitoringDSN string) (func(context.Context) error, error) {
	shutdown, err := trace.SetupOTelSDK(ctx, "BLNK", monitoringDSN)
	if err != nil {
		return nil, fmt.Errorf("error setting up OTel SDK: %w", err)
	}
	return shutdown, nil
}

func initializeTypeSense(ctx context.Context, cfg *config.Configuration) (*search.TypesenseClient, error) {
	if cfg.TypeSense.Dns == "" {
		logrus.Warn("TypeSense DNS not configured. Search functionality will be disabled.")
		return nil, nil
	}

	newSearch := search.NewTypesenseClient(cfg.TypeSenseKey, []string{cfg.TypeSense.Dns})

	err := retryWithBackoff(ctx, 5, 2*time.Second, func() error {
		if err := newSearch.EnsureCollectionsExist(ctx); err != nil {
			return err
		}
		return migrateTypeSenseSchema(ctx, newSearch)
	})
	if err != nil {
		return nil, err
	}
	return newSearch, nil
}

// retryWithBackoff runs fn up to attempts times with exponential backoff
// starting at baseDelay. It stops early when the context is canceled and
// returns the last error when all attempts fail.
func retryWithBackoff(ctx context.Context, attempts int, baseDelay time.Duration, fn func() error) error {
	retryDelay := baseDelay
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}

		logrus.Errorf("TypeSense initialization failed (attempt %d/%d): %v. Retrying in %v...", i+1, attempts, err, retryDelay)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
			retryDelay *= 2
		}
	}
	return fmt.Errorf("failed to initialize TypeSense after %d attempts: %v", attempts, err)
}

func initializePostHog() (posthog.Client, string) {
	client, _ := posthog.NewWithConfig("phc_XbsHF5iBSnPiTA96gl7xygazrwBa0r2Ut4vEHoBHNiG",
		posthog.Config{Endpoint: "https://us.i.posthog.com"})
	heartbeatID := getOrCreateHeartbeatID()
	sendHeartbeat(client, heartbeatID)
	return client, heartbeatID
}

// startServer serves the API and blocks until the signal context is cancelled or the
// listener fails.
func startServer(ctx context.Context, router *gin.Engine, port string) error {
	server := newHTTPServer(router, port)

	// A listener that fails on its OWN — the port is taken, the socket cannot be bound —
	// must NOT be reported with logrus.Fatalf from inside this goroutine. That is os.Exit
	// from a goroutine, so every deferred stop above it would be skipped: the relay never
	// stopped, the scheduled cleanups never drained, the database pool never closed. The
	// failure travels out on this channel instead.
	listenErr := make(chan error, 1)

	go func() {
		logrus.Infof("Server started on port %s", port)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- err
		}

		close(listenErr)
	}()

	// The context, adapted to the drain's wait.
	stopping := make(chan os.Signal, 1)

	go func() {
		<-ctx.Done()
		stopping <- syscall.SIGTERM
	}()

	return gracefulShutdown(server, stopping, listenErr, serverShutdownTimeout)
}

// newHTTPServer builds the API HTTP server for the given router and port.
func newHTTPServer(router *gin.Engine, port string) *http.Server {
	return hardenHTTPServer(&http.Server{
		Addr:    ":" + port,
		Handler: router,
	})
}

// gracefulShutdown blocks until either a signal arrives on quit or the listener fails,
// then shuts the server down, giving outstanding requests up to timeout to complete.
func gracefulShutdown(
	server *http.Server,
	quit <-chan os.Signal,
	listenErr <-chan error,
	timeout time.Duration,
) error {
	var failure error

	select {
	case <-quit:
		logrus.Info("Shutting down server...")
	case err, open := <-listenErr:
		if open && err != nil {
			// NAMED, so an operator reading the exit status knows the process did not merely
			// receive a signal. Wrapped rather than replaced, so the listener's own error stays
			// identifiable with errors.Is.
			failure = fmt.Errorf("api listener failed: %w", err)
			withLoggableCause(nil, err).Error(
				"the API listener stopped with an error, so the server is shutting down; every " +
					"background processor is stopped in order on the way out",
			)
		} else {
			logrus.Info("The API listener closed; shutting down server...")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logrus.Errorf("Server forced to shutdown: %v", err)

		if failure != nil {
			return failure
		}

		return err
	}

	if failure != nil {
		return failure
	}

	logrus.Info("Server exited gracefully")

	return nil
}

// withLoggableCause attaches a dependency error to a log entry in the two renderings an
// operator needs, and it is the ONLY way this file should put an error into a line.
func withLoggableCause(entry *logrus.Entry, err error) *logrus.Entry {
	if entry == nil {
		entry = logrus.NewEntry(logrus.StandardLogger())
	}

	if err == nil {
		return entry
	}

	entry = entry.WithField("cause", logsafe.Cause(err))

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		entry = entry.WithField("cause_verbatim", logsafe.CauseVerbatim(err))
	}

	return entry
}

// startEventMetricsCollector starts the periodic collector that maintains the event
// pipeline's gauges — the outbox backlog, the dead-letter age, outstanding subscriber
// revocations, subscriber consumer lag, and the collector's own collection health — and
// returns the function that stops it and releases what it opened.
func startEventMetricsCollector(
	ctx context.Context,
	instance *blnk.Blnk,
	cfg *config.Configuration,
) func() {
	// The age gauge is read from the outbox table and resolves no publisher, so this
	// service needs no broker. It is closed by the returned cleanup all the same, because
	// EventDeadLetters documents the caller as its owner.
	deadLetters := instance.EventDeadLetters()

	// Declared as the interface and assigned only on success: a nil *KafkaAdminClient
	// placed in an interface field is a non-nil interface holding a nil pointer, which
	// passes every nil guard and then panics on first use.
	var admin blnk.KafkaAdmin
	kafkaAdmin, err := blnk.NewKafkaAdmin(cfg)
	if err != nil {
		// The bounded class rather than the client's own words, which render with the
		// broker list, the listener addresses and the administrative principal. The raw
		// text goes to the trace-level diagnostic sink beside it.
		logrus.WithField("error_class", blnk.KafkaErrorClass(err)).Warn(
			"event metrics: the Kafka admin client could not be built, so subscriber consumer lag will " +
				"not be measured; the outbox backlog and dead-letter age gauges are unaffected",
		)
		blnk.LogKafkaDiagnostic("event_metrics_new_kafka_admin", err)
	} else {
		admin = kafkaAdmin
	}

	// THE MEASUREMENT BUDGET IS CONFIGURED, not left at the collector's default.
	collector := blnk.NewBlnkEventMetricsCollector(instance, deadLetters, admin).
		WithSubscriberBudget(cfg.Relay.SubscriberMetricsBudget)
	collector.Start(ctx)

	return func() {
		collector.Stop()

		if admin != nil {
			if closeErr := admin.Close(); closeErr != nil {
				logrus.WithField("error_class", blnk.KafkaErrorClass(closeErr)).
					Warn("event metrics: closing the Kafka admin client failed")
				blnk.LogKafkaDiagnostic("event_metrics_close_kafka_admin", closeErr)
			}
		}

		if closeErr := deadLetters.Close(); closeErr != nil {
			logrus.WithField("error_class", blnk.KafkaErrorClass(closeErr)).
				Warn("event metrics: closing the dead-letter service failed")
			blnk.LogKafkaDiagnostic("event_metrics_close_dead_letter_service", closeErr)
		}
	}
}

// startEventRelay assures the Kafka topics exist and starts the transactional event
// outbox relay, returning the function that stops it and the startup obstacle, if any.
func startEventRelay(
	ctx context.Context,
	instance *blnk.Blnk,
	cfg *config.Configuration,
) (func(), error) {
	if cfg == nil || !blnk.KafkaBrokersConfigured(cfg.Kafka.Brokers) {
		logrus.Info(
			"no Kafka brokers are configured, so the event outbox relay is not started; ledger events " +
				"are delivered over the legacy webhook transport, which is the documented steady state " +
				"for a deployment that has not migrated",
		)

		return func() {}, nil
	}

	stopAssurance := assureEventTopics(ctx, instance, cfg)
	reportStrandedTopicPrefixes(ctx, instance)
	probeKeyScopeGateway(ctx, cfg)

	// THE CLAIM GATE. Assurance above is allowed to fail and be stepped past, because the
	// usual cause is a broker that is not listening yet and refusing to start the relay
	// would turn that into an outage needing a human. This is the other half of that
	// decision: the relay starts, so recovery is automatic, but it CLAIMS NOTHING until
	// every destination topic is known to exist.
	relay := blnk.NewEventRelayProcessor(instance).
		WithCatalogueGate(blnk.NewTopicCatalogueGate(cfg))
	startErr := relay.Start(ctx)

	stop := func() {
		relay.Stop()
		stopAssurance()
	}

	if startErr != nil {
		// WRAPPED with what the operator has to decide, because the obstacle alone says what
		// is wrong and not what it costs. Every remedy is one of these two, and both are
		// configuration: open the window, or stop asking for Kafka.
		return stop, fmt.Errorf(
			"the event outbox relay refused to start while KAFKA_BROKERS is configured, so every "+
				"captured ledger event would stay undelivered by BOTH transports — no Kafka publish "+
				"and no legacy webhook, because the relay is what enqueues the legacy leg during the "+
				"dual-delivery window. Refusing to serve rather than accumulating undeliverable rows. "+
				"Fix the configuration this names, or unset KAFKA_BROKERS to run on the legacy "+
				"transport alone: %w",
			startErr,
		)
	}

	return stop, nil
}

// reportStrandedTopicPrefixes names, at start-up, any topic namespace that outbox rows
// still name and this deployment no longer owns.
func probeKeyScopeGateway(ctx context.Context, cfg *config.Configuration) {
	gateway, err := blnk.NewKeyScopeGatewayClient(cfg)
	if err != nil {
		// Nothing declared. Silence here is correct: config validation warns when the mode is
		// declared without a usable endpoint, and a deployment that declares neither has nothing
		// to be told.
		return
	}

	probeCtx, cancel := context.WithTimeout(ctx, cfg.Kafka.KeyScopeAttestationTimeout())
	defer cancel()

	if err := gateway.Health(probeCtx); err != nil {
		logrus.WithFields(logrus.Fields{
			"gateway_endpoint": gateway.Endpoint(),
			"error_class":      blnk.KafkaErrorClass(err),
		}).Warn(
			"the declared key-scope enforcement gateway did not pass its start-up health probe, so " +
				"credential issuance for every subscriber recording a partition_key_prefix will be " +
				"REFUSED with SUBSCRIBER_KEY_SCOPE_UNATTESTED until it does. Event publishing and the " +
				"relay are unaffected. Check the component is running, that " +
				"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL names its control endpoint, and that " +
				"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TOKEN is the credential it expects",
		)

		return
	}

	logrus.WithField("gateway_endpoint", gateway.Endpoint()).Info(
		"the declared key-scope enforcement gateway answered its start-up health probe and reports " +
			"that it enforces record-key scopes; key-scoped subscriber credentials will be attested " +
			"against it at issuance",
	)
}

func reportStrandedTopicPrefixes(ctx context.Context, instance *blnk.Blnk) {
	if instance == nil {
		return
	}

	stranded, err := instance.AuditStrandedTopicPrefixes(ctx)
	if err != nil {
		// A WARNING and not an ERROR: the audit failing tells us nothing about whether a
		// prefix is stranded, and reporting a measurement failure at the severity reserved
		// for the finding itself is how an operator learns to filter both out.
		withLoggableCause(nil, err).Warn(
			"could not audit the event outbox for stranded topic prefixes; if KAFKA_TOPIC_PREFIX was " +
				"changed recently, verify that every namespace the outbox still names is listed in " +
				"KAFKA_HISTORICAL_TOPIC_PREFIXES",
		)

		return
	}

	for _, finding := range stranded {
		logrus.WithFields(logrus.Fields{
			"stranded_prefix":    finding.Prefix,
			"stranded_topics":    finding.Topics,
			"undelivered_rows":   finding.UndeliveredRows,
			"replayable_rows":    finding.ReplayableRows,
			"oldest_occurred_at": finding.OldestOccurredAt.Format(time.RFC3339),
			"owned_prefixes":     blnk.OwnedTopicPrefixes(),
			"remedy": "add " + finding.Prefix + " to KAFKA_HISTORICAL_TOPIC_PREFIXES and restart, " +
				"then remove it once these rows have drained",
		}).Error(
			"event outbox rows name a topic namespace this deployment no longer owns, so they cannot " +
				"be published, dead-lettered or replayed. The events are safe in the table and nothing " +
				"is lost, but nothing is delivering them either",
		)
	}
}

// assureEventTopics creates or grows the event topics to the configured geometry,
// logging what it did and what it could not do.
func assureEventTopics(ctx context.Context, instance *blnk.Blnk, cfg *config.Configuration) func() {
	if cfg == nil || !blnk.KafkaBrokersConfigured(cfg.Kafka.Brokers) {
		logrus.Debug(
			"no Kafka brokers are configured, so there are no event topics to assure; the relay is not " +
				"started either, which startEventRelay reports once at info",
		)

		return func() {}
	}

	// ONE SYNCHRONOUS ATTEMPT FIRST, so that on a healthy cluster the topics exist before
	// the relay's first publish — the ordering the relay depends on, since auto-creation
	// is disabled and a publish to a missing topic fails, retries, and then fails to
	// dead-letter because the .dlt topic is missing for the same reason.
	if attemptEventTopicAssurance(ctx, instance) == nil {
		return func() {}
	}

	// IT FAILED, SO KEEP TRYING. On a first deploy that is the ordinary case rather than
	// an edge case — the broker's StatefulSet and the server's Deployment come up
	// together, so the server frequently wins — and the consequence was permanent: the
	// topics were never created, so every event failed to publish and then failed to
	// dead-letter, and the only remedy was to restart the pod. Retrying converts a
	// permanent outage into a delay.
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		retryEventTopicAssurance(ctx, stop, instance)
	}()

	var once sync.Once

	return func() {
		once.Do(func() { close(stop) })
		<-done
	}
}

// attemptEventTopicAssurance makes one bounded assurance pass.
func attemptEventTopicAssurance(ctx context.Context, instance *blnk.Blnk) error {
	admin, err := instance.KafkaAdmin()
	if err != nil {
		withLoggableCause(nil, err).Error(
			"the Kafka admin client could not be built, so the event topics were not assured; the relay " +
				"still starts and publishes, but a topic that does not exist — or one whose partition " +
				"count or replication factor is wrong — will not be corrected until this succeeds",
		)

		return err
	}
	// NOT CLOSED HERE. Blnk.KafkaAdmin returns the container's own cached client, so this
	// function borrows it rather than owning it — and closing a borrowed client is worse
	// than leaking one: the retry loop calls this again after a failure, the
	// credential-issuance endpoint and the metrics collector share the same handle, and
	// every one of them would then be working against a closed connection. The container
	// closes it on shutdown.

	assurance, cancel := context.WithTimeout(ctx, eventTopicAssuranceTimeout)
	defer cancel()

	report, err := admin.EnsureTopics(assurance)
	if err != nil {
		// The report is populated even on failure, so how far assurance got is reported
		// alongside the reason it stopped.
		withLoggableCause(logrus.WithFields(logrus.Fields{
			"created":            report.CreatedCount,
			"grown":              report.GrownCount,
			"unchanged":          report.UnchangedCount,
			"growth_refused":     report.GrowthRefusedCount,
			"partitions":         report.Partitions,
			"replication_factor": report.ReplicationFactor,
		}), err).Error(
			"assuring the Kafka event topics failed; the relay still starts, so events publish to " +
				"whatever topics exist and dead-letter what they cannot reach",
		)

		return err
	}

	logrus.WithFields(logrus.Fields{
		"topics":             len(report.Topics),
		"created":            report.CreatedCount,
		"grown":              report.GrownCount,
		"unchanged":          report.UnchangedCount,
		"partitions":         report.Partitions,
		"replication_factor": report.ReplicationFactor,
	}).Info("Kafka event topics assured")

	return nil
}

// retryEventTopicAssurance retries assurance with capped exponential backoff until it
// succeeds.
func retryEventTopicAssurance(ctx context.Context, stop <-chan struct{}, instance *blnk.Blnk) {
	backoff := eventTopicAssuranceRetryBase

	for attempt := 2; ; attempt++ {
		if !waitForEventMaintenance(ctx, stop, backoff) {
			logrus.Warn(
				"the server is shutting down with the Kafka event topics still unassured; whatever " +
					"topics exist are what the relay published to",
			)

			return
		}

		logrus.WithFields(logrus.Fields{
			"attempt": attempt,
			"backoff": backoff.String(),
		}).Info("retrying assurance of the Kafka event topics")

		err := attemptEventTopicAssurance(ctx, instance)
		if err == nil {
			logrus.WithField("attempt", attempt).Info(
				"the Kafka event topics were assured on retry; the relay's publishes now have the " +
					"topics they need",
			)

			return
		}

		// A GEOMETRY REFUSAL IS NOT TRANSIENT, so this stops instead of restating it for
		// ever. A topic that already holds records and needs more partitions refuses
		// identically on every attempt, and an inadequate replication factor is a property of
		// the cluster: neither is something another attempt can achieve, and growing a live
		// topic is a planned migration rather than a retry. Retrying would print the same
		// refusal on a widening schedule for the life of the process, which buries the one
		// line an operator has to act on under copies of itself.
		if errors.Is(err, blnk.ErrPartitionGrowthRefused) ||
			errors.Is(err, blnk.ErrReplicationFactorInadequate) {
			withLoggableCause(logrus.WithField("attempt", attempt), err).Error(
				"assuring the Kafka event topics was REFUSED for a reason no retry can change, so " +
					"assurance stops here; the relay keeps publishing to whatever topics exist. " +
					"Correct the topic geometry deliberately — growing a topic that holds records " +
					"re-maps its partition keys and is a planned migration — or set the replication " +
					"factor this cluster can satisfy",
			)

			return
		}

		backoff *= 2
		if backoff > eventTopicAssuranceRetryMax {
			backoff = eventTopicAssuranceRetryMax
		}
	}
}

// Renamed from initializeObservability to better reflect its purpose
func initializeTelemetryAndObservability(ctx context.Context, cfg *config.Configuration) (posthog.Client, func(context.Context) error, error) {
	var phClient posthog.Client
	var tracingShutdown func(context.Context) error = func(context.Context) error { return nil }
	var err error

	// Initialize tracing if observability is enabled
	if cfg.EnableObservability {
		tracingShutdown, err = initializeOpenTelemetry(ctx, cfg.RemoteMonitoringDSN())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize tracing: %w", err)
		}
	}

	// Initialize PostHog if telemetry is enabled
	if cfg.EnableTelemetry {
		phClient, _ = initializePostHog()
	}

	return phClient, tracingShutdown, nil
}

/*
serverCommands returns the Cobra command responsible for starting the Blnk server.
It sets up the API routes, traces, and TypeSense client before launching the server.
*/
// serverCommands builds the `start` command, which runs the server role.
//
// The command layer owns the SIGNAL and the EXIT STATUS; runServer owns the lifecycle.
// That is the same split cmd/workers.go uses, and it is what makes the lifecycle
// testable.
//
// Parameters:
//   - b *blnkInstance: the service container the role serves from.
//
// Returns:
//   - *cobra.Command: the `start` command.
func serverCommands(b *blnkInstance) *cobra.Command {
	// Bound to --require-kafka below and read by RunE. Declared here rather than in a package
	// variable so a second `start` command built for a test carries its own flag state.
	var requireKafka bool

	// Define the `start` command for starting the server
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start blnk server", // Short description of the command
		// RunE, not Run, and that is the whole of the lifecycle fix at this level.
		RunE: func(cmd *cobra.Command, args []string) error {
			// ONE signal context for the whole process. Everything that has to stop when this
			// process is asked to stop reads this context and nothing else: every background
			// processor, and the HTTP listener inside runServer.
			ctx, stopSignals := signal.NotifyContext(
				context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stopSignals()

			// The Kafka requirement is checked BEFORE the listener binds and before any
			// background processor starts, so a deployment that asked for the relay and would
			// not have got one fails fast instead of serving.
			if requireKafka {
				if err := requireKafkaBrokersConfigured(); err != nil {
					return err
				}
			}

			return runServer(ctx, b)
		},
	}

	// --require-kafka EXISTS SO THAT THE DECISION IS MADE BY THE CODE THAT KNOWS.
	cmd.Flags().BoolVar(&requireKafka, "require-kafka", false,
		"refuse to start unless this deployment's own configuration resolves to at least one "+
			"Kafka broker, so a server that would silently run without the event relay fails "+
			"to start instead")

	return cmd
}

// requireKafkaBrokersConfigured refuses to start the server role when the effective
// configuration resolves to no Kafka broker.
func requireKafkaBrokersConfigured() error {
	cfg, err := config.Fetch()
	if err != nil {
		return fmt.Errorf("--require-kafka: the configuration could not be read: %w", err)
	}

	if cfg != nil && blnk.KafkaBrokersConfigured(cfg.Kafka.Brokers) {
		return nil
	}

	return errors.New(
		"--require-kafka was given, but this deployment's configuration resolves to no Kafka " +
			"broker. The event outbox relay would not start and every captured event would stay " +
			"pending in blnk.event_outbox while the server looked healthy. Set brokers in one of " +
			"these, highest precedence first:\n" +
			"  BLNK_KAFKA_BROKERS        the conventional prefixed name; wins over every other " +
			"source\n" +
			"  KAFKA_BROKERS             the deployment contract name\n" +
			"  BLNK_KAFKA_KAFKA_BROKERS  the key envconfig derives for this field\n" +
			"  \"kafka\": { \"brokers\": [\"host:9092\"] } in the configuration file, used when " +
			"no environment name is set\n" +
			"A name that is set and EMPTY is not absent: it clears what a lower-precedence source " +
			"supplied, which is how Kafka is turned off for one run. So " +
			"`KAFKA_BROKERS= blnk start --require-kafka` means no brokers for this run and is " +
			"refused here rather than falling back to the configuration file. Start without " +
			"--require-kafka to run the documented pre-migration mode, in which ledger events are " +
			"delivered over the legacy webhook transport",
	)
}

// runServer starts the API listener and every background processor the server role
// owns, blocks until ctx is cancelled or the listener fails, and then shuts everything
// down in order.
func runServer(ctx context.Context, b *blnkInstance) error {
	// Load configuration
	cfg, err := config.Fetch()
	if err != nil {
		logrus.Error(err)
	}

	// Initialize telemetry and observability before the router,
	// so MetricsHandler() is available when routes are registered.
	phClient, shutdown, err := initializeTelemetryAndObservability(ctx, cfg)
	if err != nil {
		return err
	}
	if shutdown != nil {
		defer func() {
			// A FRESH bounded context, not ctx. By the time this defer runs, ctx IS the
			// cancelled signal context, and an exporter flush on a cancelled context returns
			// immediately without exporting — so the telemetry describing the shutdown would be
			// the telemetry most reliably lost, exactly when it is wanted. Same ten-second
			// budget the worker role already uses for this.
			flush, cancel := context.WithTimeout(context.Background(), telemetryFlushTimeout)
			defer cancel()

			if err := shutdown(flush); err != nil {
				logrus.Errorf("Error during shutdown: %v", err)
			}
		}()
	}
	if phClient != nil {
		defer func() {
			// Logged rather than discarded. PostHog's Close flushes its queued batch, so an
			// error here means product telemetry from this run was dropped — which is not worth
			// failing a shutdown over, but is worth being able to see rather than inferring from
			// a gap in the data.
			if err := phClient.Close(); err != nil {
				withLoggableCause(nil, err).
					Warn("Error closing the PostHog client; queued telemetry from this run may be lost")
			}
		}()
	}

	// Initialize router (after OTel so /metrics handler is available)
	router := initializeRouter(b)

	// Initialize TypeSense
	tsClient, err := initializeTypeSense(ctx, cfg)
	if err != nil {
		logrus.Errorf("TypeSense initialization error: %v", err)
	} else if tsClient != nil {
		search.TryReindexIfNeeded(ctx, tsClient, b.blnk.GetDataSource())
	}

	// Close database connection pool on shutdown
	defer func() {
		ds, err := database.GetDBConnection(cfg)
		if err == nil && ds != nil {
			logrus.Info("Closing database connection pool...")
			if err := ds.Close(); err != nil {
				logrus.Errorf("Error closing database connection: %v", err)
			}
		}
	}()

	// Close the service container.
	defer closeServiceContainer(b)

	// Start lineage outbox processor
	// This worker processes pending lineage entries that were captured atomically with transactions
	lineageProcessor := blnk.NewLineageOutboxProcessor(b.blnk)
	lineageProcessor.Start(ctx)
	defer lineageProcessor.Stop()

	// Start the event metrics collector. It is the ONLY production maintainer of the event
	// pipeline's gauges, and without it every one of them is declared, initialised and
	// never recorded — so no rule in alerts/blnk-kafka-alerts.yml can fire whatever the
	// system is doing.
	stopEventMaintenance := startEventMaintenance(ctx, b, cfg)
	defer stopEventMaintenance()

	// Start the Kafka event outbox relay. It is the ONLY thing that publishes captured
	// event rows, so without it a Kafka-configured deployment fills blnk.event_outbox and
	// delivers nothing.
	stopEventRelay, relayErr := startEventRelay(ctx, b.blnk, cfg)
	defer stopEventRelay()

	if relayErr != nil {
		return relayErr
	}

	// The retention sweeper is NOT started here. Without it blnk.event_outbox grows
	// without bound — every delivered and every dead-lettered row stays for ever, so the
	// table the relay's claim query scans keeps getting larger and the oldest rows are
	// kept long past any replay window — but it is maintenance of a shared table, so it is
	// started by startEventMaintenance above, behind the singleton lease, together with
	// the metrics collector. It remains a no-op unless RELAY_EVENT_RETENTION_DAYS is set.

	// The settlement processor runs beside the relay and the sweeper, in the same role and
	// with the same lifecycle. It is what finishes the broker-side work a subscriber
	// operation could not: an authorization change that pruned the broker and then failed
	// to persist, a credential written whose compensating revocation also failed, a
	// credential revoked whose registry record could not be cleared. Each of those is
	// recorded durably on the subscriber row, and without this nothing ever discharges
	// them — the obligations accumulate and the registry stays diverged from the broker.
	stopSubscriberSettlement := startSubscriberSettlement(ctx, b.blnk)
	defer stopSubscriberSettlement()

	// THE BALANCE-MONITOR HANDOFF DRAINER, and it is NOT optional maintenance. Every
	// ledger transaction that moves a monitored balance records a handoff row inside its
	// own transaction — an intent saying "these monitors have not been judged yet",
	// carrying both inputs the judgement depends on: the balance as written and the
	// monitor definitions in force when it was written, so draining late cannot change the
	// verdict — and the post-commit evaluation stands down whenever the handoff is in
	// play. This processor is therefore the SOLE owner of `balance.monitor`: without it
	// the rows accumulate unevaluated, no monitor alert is ever published, and the only
	// symptom is silence.
	stopBalanceMonitorHandoff := startBalanceMonitorHandoff(ctx, b.blnk, cfg)
	defer stopBalanceMonitorHandoff()

	// LAST OF THE BACKGROUND WORKERS, because it is the only FEATURE-FLAGGED one. It seals
	// transactions into a tamper-evident chain off the hot path, and it belongs to hash
	// chaining rather than to the event pipeline — placed among the workers above it would
	// read as though the unconditional ones were also governed by this flag.
	if cfg.Transaction.HashChain.Enabled {
		chainProcessor := blnk.NewChainProcessor(b.blnk)
		chainProcessor.Start(ctx)
		defer chainProcessor.Stop()
	}

	// Start server. This blocks until the signal context is cancelled or the listener fails,
	// and returns the outcome to the command layer so the defers above unwind in order first.
	return startServer(ctx, router, cfg.Server.Port)
}

// startSubscriberSettlement starts the subscriber settlement processor and returns the
// function that stops it.
func startSubscriberSettlement(ctx context.Context, instance *blnk.Blnk) func() {
	processor := blnk.NewSubscriberSettlementProcessor(instance)

	if obstacle := processor.StartupObstacle(); obstacle != nil {
		if errors.Is(obstacle, blnk.ErrSubscriberSettlementDisabled) {
			logrus.Info(
				"subscriber settlement is inactive because no Kafka broker is configured; there is no " +
					"broker-side subscriber state to reconcile",
			)

			return func() {}
		}

		withLoggableCause(nil, obstacle).Warn(
			"a Kafka broker is configured but the subscriber settlement processor could not start, so " +
				"nothing will finish the broker-side work that failed subscriber operations left " +
				"outstanding",
		)

		return func() {}
	}

	processor.Start(ctx)

	return processor.Stop
}

// serviceContainerCloser is the one thing closeContainer needs from the service container.
type serviceContainerCloser interface {
	Close() error
}

// closeServiceContainer releases the service container held by the CLI instance.
func closeServiceContainer(instance *blnkInstance) {
	if instance == nil || instance.blnk == nil {
		return
	}

	closeContainer(instance.blnk)
}

// closeContainer closes one service container and reports the outcome.
func closeContainer(container serviceContainerCloser) {
	if container == nil {
		return
	}

	logrus.Info("Closing event publisher and queue client...")

	if err := container.Close(); err != nil {
		// withLoggableCause, not an interpolated %v: a close failure from a Kafka writer
		// carries the broker's own message, which can name topics, principals and addresses.
		// The bounded class goes in the line and the raw text goes to the trace-level sink.
		withLoggableCause(nil, err).Error(
			"closing the service container reported an error; some of the publisher, the shared " +
				"Kafka administrative client or the asynq client may not have shut down cleanly, " +
				"and a writer that could not flush its batch loses events the outbox already " +
				"records as dispatched",
		)
	}
}

// startEventRetention starts the event outbox retention sweeper and returns the
// function that stops it.
func startEventRetention(ctx context.Context, instance *blnk.Blnk) func() {
	sweeper := blnk.NewEventRetentionSweeper(instance)

	if obstacle := sweeper.StartupObstacle(); obstacle != nil {
		if errors.Is(obstacle, blnk.ErrEventRetentionDisabled) {
			logrus.Info(
				"event outbox retention is disabled; delivered event rows are kept indefinitely. Set " +
					"RELAY_EVENT_RETENTION_DAYS to a positive number of days to have them deleted after " +
					"that period. Dead-lettered rows are never deleted by age whatever this is set to: " +
					"a replay the broker acknowledges is what turns one into a deletable receipt",
			)

			return func() {}
		}

		withLoggableCause(nil, obstacle).Warn(
			"event outbox retention is configured but the sweeper could not start, so nothing will " +
				"delete delivered event rows",
		)

		return func() {}
	}

	sweeper.Start(ctx)

	return sweeper.Stop
}

// startBalanceMonitorHandoff starts the balance-monitor handoff processor and returns
// the function that stops it.
func startBalanceMonitorHandoff(ctx context.Context, instance *blnk.Blnk, cfg *config.Configuration) func() {
	if !cfg.EventPublishingConfigured() {
		logrus.Info(
			"balance monitor handoff processing is not started because no Kafka broker is configured; " +
				"monitor conditions are evaluated on the post-commit path and their alerts delivered " +
				"over the legacy webhook transport, exactly as before the event pipeline existed",
		)

		return func() {}
	}

	if instance == nil {
		logrus.Error(
			"balance monitor handoff processing could not start because the service container is " +
				"absent; handoffs recorded inside ledger transactions will accumulate unevaluated and " +
				"no balance.monitor alert will be published",
		)

		return func() {}
	}

	processor := blnk.NewBalanceMonitorHandoffProcessor(instance)
	processor.Start(ctx)

	return processor.Stop
}

// hardenHTTPServer applies the shared connection-lifetime bounds to srv and returns it.
func hardenHTTPServer(srv *http.Server) *http.Server {
	if srv == nil {
		return nil
	}

	srv.ReadHeaderTimeout = httpReadHeaderTimeout
	srv.ReadTimeout = httpReadTimeout
	srv.WriteTimeout = httpWriteTimeout
	srv.IdleTimeout = httpIdleTimeout
	srv.MaxHeaderBytes = httpMaxHeaderBytes

	return srv
}

// startEventMaintenance runs the leader-elected maintenance work and returns the
// function that stops it.
func startEventMaintenance(ctx context.Context, b *blnkInstance, cfg *config.Configuration) func() {
	ds, err := database.GetDBConnection(cfg)
	if err != nil || ds == nil {
		withLoggableCause(nil, err).Warn(
			"the event maintenance lease cannot be evaluated without a database connection, so the " +
				"metrics collector and the retention sweeper run UNGATED on this replica; on a " +
				"multi-replica deployment their gauge series and their deletes will overlap",
		)

		stopMetrics := startEventMetricsCollector(ctx, b.blnk, cfg)
		stopRetention := startEventRetention(ctx, b.blnk)

		return func() {
			stopRetention()
			stopMetrics()
		}
	}

	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		runEventMaintenanceLoop(ctx, stop, ds, b, cfg)
	}()

	var once sync.Once

	return func() {
		once.Do(func() { close(stop) })
		<-done
	}
}

// runEventMaintenanceLoop competes for the maintenance lease and runs the work while it
// holds it, until ctx is cancelled or stop is closed.
func runEventMaintenanceLoop(
	ctx context.Context,
	stop <-chan struct{},
	ds *database.Datasource,
	b *blnkInstance,
	cfg *config.Configuration,
) {
	for {
		lease := acquireEventMaintenanceLease(ctx, ds)

		if lease == nil {
			if !waitForEventMaintenance(ctx, stop, eventMaintenanceRetryInterval) {
				return
			}

			continue
		}

		logrus.Info(
			"this replica holds the event maintenance lease and is therefore the one that maintains " +
				"the pipeline's gauges and deletes expired outbox rows; other replicas publish events " +
				"but skip both",
		)

		stopMetrics := startEventMetricsCollector(ctx, b.blnk, cfg)
		stopRetention := startEventRetention(ctx, b.blnk)

		lost := holdEventMaintenanceLease(ctx, stop, lease)

		// STOPPED BEFORE THE LEASE IS RELEASED, and in reverse order of starting. Releasing
		// first would let another replica win the lease and start its own collector while this
		// one is still ticking — briefly producing exactly the duplicate the lease prevents.
		stopRetention()
		stopMetrics()

		releaseCtx, cancel := context.WithTimeout(context.Background(), eventMaintenanceLeaseOpTimeout)
		if err := lease.Release(releaseCtx); err != nil {
			withLoggableCause(nil, err).Warn(
				"releasing the event maintenance lease failed; it is released anyway when this " +
					"process's database session ends, so another replica will take over",
			)
		}

		cancel()

		if !lost {
			// Ordinary shutdown.
			return
		}

		logrus.Warn(
			"this replica lost the event maintenance lease while holding it, so it has stopped " +
				"maintaining the pipeline and will compete for the lease again; another replica has " +
				"most likely taken over",
		)
	}
}

// acquireEventMaintenanceLease makes one non-blocking attempt at the lease.
func acquireEventMaintenanceLease(ctx context.Context, ds *database.Datasource) *database.EventMaintenanceLease {
	attempt, cancel := context.WithTimeout(ctx, eventMaintenanceLeaseOpTimeout)
	defer cancel()

	lease, err := ds.TryAcquireEventMaintenanceLease(attempt, database.EventMaintenanceLockKey)
	if err == nil {
		return lease
	}

	if errors.Is(err, database.ErrEventMaintenanceLeaseHeld) {
		logrus.Debug(
			"another replica holds the event maintenance lease, so this one publishes events and " +
				"skips the metrics collector and the retention sweeper",
		)

		return nil
	}

	withLoggableCause(nil, err).Warn(
		"could not determine whether this replica may maintain the event pipeline; it will not " +
			"start the metrics collector or the retention sweeper on this attempt and will retry",
	)

	return nil
}

// holdEventMaintenanceLease keeps leadership until it is given up or lost.
func holdEventMaintenanceLease(
	ctx context.Context,
	stop <-chan struct{},
	lease *database.EventMaintenanceLease,
) bool {
	ticker := time.NewTicker(eventMaintenanceVerifyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-stop:
			return false
		case <-ticker.C:
			check, cancel := context.WithTimeout(ctx, eventMaintenanceLeaseOpTimeout)
			held := lease.StillHeld(check)

			cancel()

			if !held {
				return true
			}
		}
	}
}

// waitForEventMaintenance sleeps for d unless the loop is being shut down.
func waitForEventMaintenance(ctx context.Context, stop <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-stop:
		return false
	case <-timer.C:
		return true
	}
}
