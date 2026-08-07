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
	"syscall"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api"
	"github.com/blnkfinance/blnk/api/middleware"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
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
//
// It is a bound rather than a preference: assurance is a synchronous round trip to a broker
// that may not be listening yet, and an unbounded one would hold the process short of serving
// traffic for as long as the broker stayed unreachable. Fifteen seconds is comfortably longer
// than creating the topic catalogue takes on a healthy cluster and short enough that a broker which
// is not there yet costs one log line instead of a stalled deployment.
const eventTopicAssuranceTimeout = 15 * time.Second

/*
serveTLS starts an HTTPS server with TLS enabled using CertMagic for automatic certificate management.
It accepts a gin.Engine instance as the router and a ServerConfig struct for server configurations.
If no domain is specified, the server will default to running on localhost.
*/
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

	// Create and configure the HTTPS server
	server := &http.Server{
		Addr:      ":" + conf.Port, // Server address and port
		Handler:   r,               // Handler for HTTP requests (gin router)
		TLSConfig: cfg.TLSConfig(), // TLS configuration from CertMagic
	}

	logrus.Errorf("Starting HTTPS server on %s\n", conf.Port)
	// Start the HTTPS server with automatic certificate management
	if err := server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
		logrus.Fatalf("Failed to start HTTPS server: %v", err)
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

func startServer(router *gin.Engine, port string) error {
	server := newHTTPServer(router, port)

	// Start server in goroutine
	go func() {
		logrus.Infof("Server started on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	return gracefulShutdown(server, quit, 30*time.Second)
}

// newHTTPServer builds the API HTTP server for the given router and port.
func newHTTPServer(router *gin.Engine, port string) *http.Server {
	return &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}
}

// gracefulShutdown blocks until a signal arrives on quit, then shuts the
// server down, giving outstanding requests up to timeout to complete.
func gracefulShutdown(server *http.Server, quit <-chan os.Signal, timeout time.Duration) error {
	<-quit

	logrus.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logrus.Errorf("Server forced to shutdown: %v", err)
		return err
	}

	logrus.Info("Server exited gracefully")
	return nil
}

// startEventMetricsCollector starts the periodic collector that maintains the event
// pipeline's three gauges, and returns the function that stops it and releases what it
// opened.
//
// A cleanup FUNCTION rather than a deferred stop inside this helper, because a defer here
// would fire the moment this function returned and the collector would stop before it ever
// ticked. The caller defers the returned function for the process lifetime.
//
// # Nothing here may prevent the server from serving
//
// A metrics collector is an observer. Every failure below is logged and degraded past
// rather than propagated: an admin client that cannot be built costs the consumer-lag
// measurement and nothing else, and the backlog and dead-letter gauges — the two that do
// not need a broker at all — still publish. Failing start-up because telemetry could not
// be wired would trade a monitoring gap for an outage.
//
// Parameters:
//   - ctx context.Context: cancelling it stops the collector.
//   - instance *blnk.Blnk: the service container, for its datasource.
//   - cfg *config.Configuration: read for the Kafka broker list.
//
// Returns:
//   - func(): stops the collector and closes the admin client and dead-letter service.
//     Never nil, so the caller can defer it unconditionally.
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
		logrus.WithError(err).Warn(
			"event metrics: the Kafka admin client could not be built, so subscriber consumer lag will " +
				"not be measured; the outbox backlog and dead-letter age gauges are unaffected",
		)
	} else {
		admin = kafkaAdmin
	}

	collector := blnk.NewBlnkEventMetricsCollector(instance, deadLetters, admin)
	collector.Start(ctx)

	return func() {
		collector.Stop()

		if admin != nil {
			if closeErr := admin.Close(); closeErr != nil {
				logrus.WithError(closeErr).Warn("event metrics: closing the Kafka admin client failed")
			}
		}

		if closeErr := deadLetters.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("event metrics: closing the dead-letter service failed")
		}
	}
}

// startEventRelay assures the Kafka topics exist and starts the transactional event outbox
// relay, returning the function that stops it.
//
// # Without this the pipeline has no production driver at all
//
// Every producer captures its event into blnk.event_outbox inside the ledger transaction, and
// the relay is the ONLY thing that publishes those rows to Kafka. Until this call existed the
// relay was constructible and fully tested and was never constructed by a running process, so
// a deployment with KAFKA_BROKERS set accumulated outbox rows indefinitely and delivered
// nothing — with no error anywhere, because capturing the row had succeeded every time.
//
// It runs in the SERVER role, beside the lineage outbox processor and the event metrics
// collector, because that is where this codebase already puts outbox background work. Putting
// it in the worker role would mean a fourth asynq server and two roles that both had to be
// deployed for events to flow.
//
// # Topic assurance comes FIRST, and a failure does not stop the relay
//
// Assurance is what gives the topics their required partition count and replication factor;
// a topic auto-created by the broker would have neither, and per-aggregate ordering depends on
// the partition count. So it runs before the first publish can happen.
//
// A failure is logged and stepped past rather than fatal, and that is deliberate. The usual
// cause is a broker that is not listening yet — a compose stack coming up, a rolling restart —
// and refusing to start the relay would mean events stayed unpublished until somebody
// restarted the process, converting a transient condition into an outage. The relay retries
// every publish and dead-letters what it cannot deliver, so a genuinely missing topic surfaces
// as dead letters and the 15-minute dead-letter alert, not as silence.
//
// # No brokers is a legitimate steady state
//
// With KAFKA_BROKERS unset there is nothing to relay to and the producers deliver over the
// legacy webhook transport directly. That is reported at info level once, not treated as a
// misconfiguration: it is how every deployment runs before it opts into Kafka, and it is what
// the graceful-degradation contract promises.
//
// Parameters:
//   - ctx context.Context: cancelling it stops the relay.
//   - instance *blnk.Blnk: the service container, which supplies the datasource, the shared
//     publisher and the legacy transport.
//   - cfg *config.Configuration: read for the broker list.
//
// Returns:
//   - func(): stops the relay and closes the admin client. Never nil, so the caller can defer
//     it unconditionally.
func startEventRelay(
	ctx context.Context,
	instance *blnk.Blnk,
	cfg *config.Configuration,
) func() {
	if cfg == nil || !blnk.KafkaBrokersConfigured(cfg.Kafka.Brokers) {
		logrus.Info(
			"no Kafka brokers are configured, so the event outbox relay is not started; ledger events " +
				"are delivered over the legacy webhook transport, which is the documented steady state " +
				"for a deployment that has not migrated",
		)

		return func() {}
	}

	assureEventTopics(ctx, cfg)

	relay := blnk.NewEventRelayProcessor(instance)
	relay.Start(ctx)

	return relay.Stop
}

// assureEventTopics creates or grows the event topics to the configured geometry, logging what
// it did and what it could not do.
//
// It is separated from startEventRelay so the admin client's lifetime is exactly this call:
// assurance is a one-shot startup operation, and holding its connections open for the process
// lifetime would keep a SASL session per broker for something that never runs again.
//
// Parameters:
//   - ctx context.Context: bounded here, because assurance must not delay startup indefinitely
//     against an unreachable broker.
//   - cfg *config.Configuration: read for the broker list and the topic geometry.
func assureEventTopics(ctx context.Context, cfg *config.Configuration) {
	admin, err := blnk.NewKafkaAdmin(cfg)
	if err != nil {
		logrus.WithError(err).Error(
			"the Kafka admin client could not be built, so the event topics were not assured; the relay " +
				"still starts and publishes, but a topic that does not exist — or one whose partition " +
				"count or replication factor is wrong — will not be corrected",
		)

		return
	}
	defer func() {
		if closeErr := admin.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("closing the Kafka admin client after topic assurance failed")
		}
	}()

	assurance, cancel := context.WithTimeout(ctx, eventTopicAssuranceTimeout)
	defer cancel()

	report, err := admin.EnsureTopics(assurance)
	if err != nil {
		// The report is populated even on failure, so how far assurance got is reported
		// alongside the reason it stopped.
		logrus.WithError(err).WithFields(logrus.Fields{
			"created":            report.CreatedCount,
			"grown":              report.GrownCount,
			"unchanged":          report.UnchangedCount,
			"growth_refused":     report.GrowthRefusedCount,
			"partitions":         report.Partitions,
			"replication_factor": report.ReplicationFactor,
		}).Error(
			"assuring the Kafka event topics failed; the relay still starts, so events publish to " +
				"whatever topics exist and dead-letter what they cannot reach",
		)

		return
	}

	logrus.WithFields(logrus.Fields{
		"topics":             len(report.Topics),
		"created":            report.CreatedCount,
		"grown":              report.GrownCount,
		"unchanged":          report.UnchangedCount,
		"partitions":         report.Partitions,
		"replication_factor": report.ReplicationFactor,
	}).Info("Kafka event topics assured")
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
func serverCommands(b *blnkInstance) *cobra.Command {
	// Define the `start` command for starting the server
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start blnk server", // Short description of the command
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()

			// Load configuration
			cfg, err := config.Fetch()
			if err != nil {
				logrus.Error(err)
			}

			// Initialize telemetry and observability before the router,
			// so MetricsHandler() is available when routes are registered.
			phClient, shutdown, err := initializeTelemetryAndObservability(ctx, cfg)
			if err != nil {
				logrus.Fatal(err)
			}
			if shutdown != nil {
				defer func() {
					if err := shutdown(ctx); err != nil {
						logrus.Errorf("Error during shutdown: %v", err)
					}
				}()
			}
			if phClient != nil {
				defer phClient.Close()
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

			// Start lineage outbox processor
			// This worker processes pending lineage entries that were captured atomically with transactions
			lineageProcessor := blnk.NewLineageOutboxProcessor(b.blnk)
			lineageProcessor.Start(ctx)
			defer lineageProcessor.Stop()

			// Start the hash-chain processor when enabled. It seals transactions
			// into a tamper-evident chain off the hot path.
			if cfg.Transaction.HashChain.Enabled {
				chainProcessor := blnk.NewChainProcessor(b.blnk)
				chainProcessor.Start(ctx)
				defer chainProcessor.Stop()
			}

			// Assure the Kafka topic catalogue, THEN start the event outbox relay. The
			// order is the point: the relay's first publish must not be the thing that
			// discovers a missing topic, because auto-creation is disabled and a publish
			// to a topic that does not exist fails, retries and then fails to
			// dead-letter for the same reason.
			//
			// Both calls are unconditional and both self-guard, which is what keeps this
			// call site four lines and keeps a Kafka-less deployment working unchanged.
			// assureEventTopics logs and returns when no broker is configured;
			// Start refuses, with a named reason, when the publisher is absent or is the
			// no-op — and refusing there is essential rather than tidy, because running
			// the relay against the no-op publisher would mark the entire outbox
			// dispatched while sending nothing, destroying every pending event.
			//
			// The relay belongs to the SERVER role, beside the lineage processor and the
			// event metrics collector, following this repository's convention that outbox
			// relays live where the outbox background work already is. Starting it in the
			// worker role as well would have two processes claiming the same rows — which
			// the FOR UPDATE SKIP LOCKED claim tolerates, but it would double the broker
			// connections and the dual-delivery enqueues for no gain.
			assureEventTopics(ctx, cfg)

			eventRelay := blnk.NewEventRelayProcessor(b.blnk)
			eventRelay.Start(ctx)
			defer eventRelay.Stop()

			// Start the event metrics collector. It is the ONLY production maintainer of
			// the event pipeline's three gauges — the outbox backlog, the dead-letter age
			// and subscriber consumer lag — and without it all three are declared,
			// initialised and never recorded, so the two rules in
			// alerts/blnk-kafka-alerts.yml cannot fire whatever the system is doing.
			//
			// It runs in the SERVER role only, beside the lineage processor and for the
			// same reason: this is where the outbox background work already lives.
			// Starting it in the worker role as well would have two processes writing the
			// same gauges, each zeroing the other's series as stale.
			//
			// It is unconditional. A deployment with no Kafka still accumulates outbox
			// rows, and the backlog gauge is exactly what shows that; the collector skips
			// the measurements it has no dependency for rather than declining to run.
			stopEventMetrics := startEventMetricsCollector(ctx, b.blnk, cfg)
			defer stopEventMetrics()

			// Start the Kafka event outbox relay. It is the ONLY thing that publishes the
			// rows every producer captures inside its ledger transaction, so without it a
			// Kafka-configured deployment fills the outbox and delivers nothing. Topic
			// assurance runs first, inside the helper; see its documentation for why a
			// failure there does not stop the relay.
			stopEventRelay := startEventRelay(ctx, b.blnk, cfg)
			defer stopEventRelay()

			// The retention sweeper runs beside the relay, in the same role and with the same
			// lifecycle. Without it blnk.event_outbox grows without bound: every delivered and
			// every dead-lettered row stays for ever, so the table the relay's claim query scans
			// keeps getting larger and the oldest rows are kept long past any replay window.
			// It is a no-op unless RELAY_EVENT_RETENTION_DAYS is set.
			stopEventRetention := startEventRetention(ctx, b.blnk)
			defer stopEventRetention()

			// Start server
			if err := startServer(router, cfg.Server.Port); err != nil {
				logrus.Fatal(err)
			}
		},
	}

	return cmd
}

// startEventRetention starts the event outbox retention sweeper and returns the function
// that stops it.
//
// # The two obstacles are logged differently, and the difference matters
//
// RETENTION DISABLED is the default and is logged at info. It is not a fault: an operator
// who has not set a retention period has not misconfigured anything, and reporting it as a
// warning on every start-up would train them to ignore the one message that is a warning.
// The line still names the variable, because an operator who BELIEVES retention is on needs
// to be able to discover that it is not.
//
// ANYTHING ELSE is logged at warning, because it means retention IS configured and is not
// running — the control the operator asked for is absent, and the symptom of that is a table
// that quietly keeps growing.
//
// A cleanup FUNCTION rather than a deferred stop, for the reason the other two starters
// return one: a defer inside this helper would fire on return and stop the sweeper before it
// ever ticked.
//
// Parameters:
//   - ctx context.Context: cancelling it stops the sweeper.
//   - instance *blnk.Blnk: the service container, for its datasource and configuration.
//
// Returns:
//   - func(): stops the sweeper and waits for the sweep in flight. Never nil.
func startEventRetention(ctx context.Context, instance *blnk.Blnk) func() {
	sweeper := blnk.NewEventRetentionSweeper(instance)

	if obstacle := sweeper.StartupObstacle(); obstacle != nil {
		if errors.Is(obstacle, blnk.ErrEventRetentionDisabled) {
			logrus.Info(
				"event outbox retention is disabled; delivered and dead-lettered event rows are kept " +
					"indefinitely. Set RELAY_EVENT_RETENTION_DAYS to a positive number of days to have " +
					"them deleted after that period",
			)

			return func() {}
		}

		logrus.WithError(obstacle).Warn(
			"event outbox retention is configured but the sweeper could not start, so nothing will " +
				"delete delivered or dead-lettered event rows",
		)

		return func() {}
	}

	sweeper.Start(ctx)

	return sweeper.Stop
}
