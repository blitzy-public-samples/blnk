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
//
// It is a bound rather than a preference: assurance is a synchronous round trip to a broker
// that may not be listening yet, and an unbounded one would hold the process short of serving
// traffic for as long as the broker stayed unreachable. Fifteen seconds is comfortably longer
// than creating the topic catalogue takes on a healthy cluster and short enough that a broker which
// is not there yet costs one log line instead of a stalled deployment.
const eventTopicAssuranceTimeout = 15 * time.Second

// A BOUNDED BOOT-TIME RETRY USED TO LIVE HERE — three attempts a few seconds apart, inside the
// synchronous start-up path — and it is superseded by the BACKGROUND retry below, which is
// governed by eventTopicAssuranceRetryBase and eventTopicAssuranceRetryMax.
//
// Both answered the same failure: on a first deploy the broker's StatefulSet and the server's
// Deployment come up together and the server frequently wins, so a single attempt left the
// topics unassured for the life of the process — every event then failed to publish AND failed
// to dead-letter, because the .dlt sibling was missing for the same reason, and the only remedy
// was a restart. The background form is strictly the better answer: it keeps retrying for as
// long as it takes, with a growing backoff, without holding start-up for a single second, so a
// broker that is minutes away costs a delay rather than a stalled deployment.
//
// The ONE property the bounded form had that the background form did not is kept, in
// retryEventTopicAssurance: a geometry refusal is PERMANENT and must not be retried at all.

/*
serveTLS starts an HTTPS server with TLS enabled using CertMagic for automatic certificate management.
It accepts a gin.Engine instance as the router and a ServerConfig struct for server configurations.
If no domain is specified, the server will default to running on localhost.
*/
// resolveCertStoragePath returns the configured certificate storage path,
// outcome rather than the exceptional one.
const serverShutdownTimeout = 20 * time.Second

// would notice.
const telemetryFlushTimeout = 10 * time.Second

// flight and nothing is owed, so it is short.
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

// not be noticed for hours.
const (
	eventTopicAssuranceRetryBase = 5 * time.Second
	eventTopicAssuranceRetryMax  = 60 * time.Second
)

// Leadership timings for the event maintenance work. PERF-P13.
const (
	// eventMaintenanceRetryInterval is how often a replica that is NOT the leader tries again.
	//
	// Retrying at all is what makes this failover rather than a one-off election. Without it,
	// the replicas that lost the first attempt would never try again, so the death of the
	// leader would stop all measurement and all deletion until something restarted a pod —
	// and the symptom of that is silence, which is indistinguishable from health.
	//
	// Thirty seconds bounds how long the pipeline is unmeasured after a leader dies, against a
	// cost of one non-blocking statement per replica per interval.
	eventMaintenanceRetryInterval = 30 * time.Second

	// eventMaintenanceVerifyInterval is how often the leader re-confirms with the database
	// that it is still the leader. See EventMaintenanceLease.StillHeld for why believing it is
	// not sufficient.
	eventMaintenanceVerifyInterval = 30 * time.Second

	// eventMaintenanceLeaseOpTimeout bounds one acquire, verify or release.
	//
	// Short: these are single statements on an already-open connection, so anything slower
	// than this means the database is in trouble — and the right response to that is to stop
	// believing this replica is the leader, not to wait.
	eventMaintenanceLeaseOpTimeout = 5 * time.Second
)

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

	// Create and configure the HTTPS server.
	//
	// HARDENED THROUGH THE SAME HELPER AS THE PLAINTEXT SERVER (PERF-P25), because this is the
	// listener that faces the internet. Without the bounds an idle or deliberately slow peer
	// holds a connection, its goroutine and its read buffer indefinitely, and TLS makes that
	// worse rather than better: a handshake allocates before any request line is read, so a
	// slow-loris here costs more per held connection than on the plaintext port. Constructing
	// this one by hand was how it came to be the only server in the process without timeouts.
	server := hardenHTTPServer(&http.Server{
		Addr:      ":" + conf.Port, // Server address and port
		Handler:   r,               // Handler for HTTP requests (gin router)
		TLSConfig: cfg.TLSConfig(), // TLS configuration from CertMagic
	})

	logrus.Errorf("Starting HTTPS server on %s\n", conf.Port)
	// Start the HTTPS server with automatic certificate management.
	//
	// RETURNED, never fatal. logrus.Fatalf calls os.Exit, which runs no deferred function
	// anywhere on the stack — so a TLS listener that failed to bind used to terminate the
	// process with the event relay, the retention sweeper, the metrics collector, the lineage
	// processor, the database pool and the tracing exporter all still open. The relay's
	// claimed rows kept their leases until they expired, the exporter dropped whatever it had
	// buffered, and the pool's connections were closed by the server rather than by us. This
	// function already returns an error and its caller already handles one; the exit was
	// simply skipping both.
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

// startServer serves the API and blocks until the signal context is cancelled or the listener
// fails.
//
// It installs NO signal handler of its own, which is the whole of the lifecycle fix here. A
// private signal.Notify meant a SIGTERM was seen by exactly one participant: every background
// processor only discovered it indirectly, when the deferred stops ran after this drain had
// already completed. Watching the shared context instead means they all begin winding down the
// instant the signal lands, CONCURRENTLY with the drain, so the deferred stops are joins on work
// already finishing rather than the start of it.
//
// Parameters:
//   - ctx context.Context: the signal context. Cancelling it drains and returns.
//   - router *gin.Engine: the handler.
//   - port string: the port to bind. "0" lets the kernel choose, which is what a test uses.
//
// Returns:
//   - error: the listener's failure, named, or the drain's error. Nil on a clean shutdown.
func startServer(ctx context.Context, router *gin.Engine, port string) error {
	server := newHTTPServer(router, port)

	// A listener that fails on its OWN — the port is taken, the socket cannot be bound — used to
	// call logrus.Fatalf from inside this goroutine. That is os.Exit, from a goroutine, so every
	// deferred stop above it was skipped: the relay was never stopped, the scheduled cleanups were
	// never drained, and the database pool was never closed.
	//
	// Reported on a channel instead, so the failure travels back to the caller and the caller's
	// ordered shutdown runs. Written to only for a REAL error — the ErrServerClosed that Shutdown
	// causes is the expected end of a healthy life — and CLOSED on the way out, which is what
	// tells the drain that the listener stopped cleanly.
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

// gracefulShutdown blocks until either a signal arrives on quit or the listener fails, then
// shuts the server down, giving outstanding requests up to timeout to complete.
//
// # Why it waits on two things
//
// A signal is the expected way to stop, and it is followed by a drain. A LISTENER FAILURE is
// the other way, and there is nothing to drain — the listener never accepted anything. Both
// have to end this function, or the process sits for ever waiting for a signal while serving
// nothing: that was the state the removed logrus.Fatalf concealed, since exiting the process
// is one way to avoid noticing that you are blocked.
//
// The listener error is RETURNED rather than logged, so it reaches the Cobra command and
// becomes the process's exit status, and it is returned in PREFERENCE to the shutdown error:
// the failure to bind is the cause, and the shutdown of a server that never served is not
// interesting next to it.
//
// A closed listenErr channel with no value means the listener stopped cleanly, which is what
// http.ErrServerClosed is — the normal consequence of the Shutdown below. Reading a nil error
// from a closed channel is therefore correct rather than a case to guard against.
//
// Parameters:
//   - server *http.Server: the server to shut down.
//   - quit <-chan os.Signal: signalled on SIGINT or SIGTERM.
//   - listenErr <-chan error: carries the listener's failure, if any. May be nil, in which
//     case only a signal ends the wait.
//   - timeout time.Duration: how long outstanding requests get to finish.
//
// Returns:
//   - error: the listener's error when it failed, the shutdown error when the drain did not
//     finish in time, otherwise nil.
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
//
// The "cause" field is redacted — network topology and secret values removed, control
// characters stripped, length bounded — and is what a deployment writes at info, warn and
// error. The "cause_verbatim" field carries the unredacted text and is attached ONLY when the
// standard logger is at debug, which is the restricted sink.
//
// It matters most here of anywhere. These are START-UP lines: a broker that cannot be reached,
// an admin client that cannot be built, topic assurance that failed. Their errors are the ones
// richest in topology — broker addresses, resolver addresses, TLS server names — and start-up
// output is the part of a log most likely to be pasted into an issue, a chat message or a
// support ticket.
//
// It is the counterpart of the identically named helpers in packages blnk, api and database,
// and all four delegate to internal/logsafe, so one failure is redacted identically wherever
// it surfaces.
//
// Parameters:
//   - entry *logrus.Entry: the entry to extend. A nil entry is treated as a fresh one.
//   - err error: the error to attach. A nil error leaves the entry untouched.
//
// Returns:
//   - *logrus.Entry: the entry with the cause fields attached.
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
//
// A cleanup FUNCTION rather than a deferred stop inside this helper, because a defer here
// would fire the moment this function returned and the collector would stop before it ever
// ticked. The caller defers the returned function for the process lifetime, and it is
// never nil so that defer needs no guard.
//
// # Nothing here may prevent the server from serving
//
// A metrics collector is an observer. Every failure below is logged and degraded past
// rather than propagated: an admin client that cannot be built costs the consumer-lag gauges
// and nothing else, while every gauge read from PostgreSQL — the outbox backlog, the
// dead-letter age, and the subscriber revocation, settlement and orphan gauges, none of which
// need a broker — still publishes on every tick, zeros included. Failing start-up because
// telemetry could not be wired would trade a monitoring gap for an outage.
//
// # The collector is bounded in time as well as in work
//
// Every collection runs under DefaultMetricsCollectionTickBudget and every individual
// dependency call under DefaultMetricsCollectionCallBudget, so a broker that accepts a
// connection and then never answers, or a database that stops responding mid-sweep, costs
// one degraded tick rather than freezing the refresh of every gauge indefinitely. That
// matters because a collector stuck inside one call leaves EVERY gauge at its last value
// while continuing to look healthy — the series are present, they are just frozen — which
// is the failure mode alerts on stale telemetry exist to catch. The collector publishes its
// own last-collection and last-success ages so that condition is observable rather than
// inferred.
//
// # Lag coverage is budgeted and rotates
//
// Consumer lag costs two broker round trips per authorised topic per subscriber, so one
// sweep examines at most Kafka.MetricsSubscriberBudget registry rows
// (EVENT_METRICS_SUBSCRIBER_BUDGET, defaulted and clamped by config). A registry larger
// than the budget is NOT permanently truncated: the sweep resumes from where the previous
// one stopped, so coverage rotates and every subscriber is measured within a bounded number
// of ticks, and the collector publishes whether the latest sweep covered the whole registry
// so a partial inventory is never mistaken for a complete one.
//
// Parameters:
//   - ctx context.Context: cancelling it stops the collector.
//   - instance *blnk.Blnk: the service container, for its datasource and its configured
//     lag-sweep budget.
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

	// THE MEASUREMENT BUDGET IS CONFIGURED, not left at the collector's default. SEC-10.
	//
	// The collector caps how many subscribers one tick measures lag for, and a subscriber
	// past that cap gets NO lag series at all — so the consumer-lag alert cannot fire for it
	// however far behind it is. The cap was reachable only through WithSubscriberBudget and
	// nothing called it, so every deployment ran on the built-in 200 with no way to raise it
	// short of a code change. A registry larger than that measured its first two hundred
	// subscribers and left the rest silently unalerted.
	//
	// setRelayDefaults guarantees a positive value, so this never trips the configurator's
	// own non-positive fallback. The shortfall, whatever the budget, is published on
	// blnk.subscribers.lag_unmeasured and alerted on by ConsumerLagCoverageIncomplete.
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

// startEventRelay assures the Kafka topics exist and starts the transactional event outbox
// relay, returning the function that stops it.
//
// # Without this the pipeline has no production driver at all
//
// The relay is the ONLY thing that publishes captured event rows to Kafka, whether the row
// was captured inside its mutation's transaction — the ordinary transaction, ledger,
// identity and balance creation paths — or standalone after the mutation committed, as
// balance monitor alerts, bulk batch summaries and system errors are. Without this call a
// deployment with KAFKA_BROKERS set accumulates outbox rows indefinitely and delivers
// nothing, with no error anywhere, because capturing the row succeeded every time.
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

	stopAssurance := assureEventTopics(ctx, instance, cfg)
	reportStrandedTopicPrefixes(ctx, instance)

	// THE CLAIM GATE. Assurance above is allowed to fail and be stepped past, because the
	// usual cause is a broker that is not listening yet and refusing to start the relay would
	// turn that into an outage needing a human. This is the other half of that decision: the
	// relay starts, so recovery is automatic, but it CLAIMS NOTHING until every destination
	// topic is known to exist.
	//
	// Without the gate, starting anyway had a real cost. Claiming leases a row; publishing to
	// a topic that does not exist fails, because auto-creation is disabled; the row retries on
	// the backoff schedule until its budget is spent; and the dead-letter write then fails FOR
	// THE SAME REASON, because the dead-letter sibling is missing too. The row ends failed with
	// no dead-letter topic recorded, and a boot against an unprovisioned broker could do that
	// to every pending row before anyone noticed the topics were absent.
	relay := blnk.NewEventRelayProcessor(instance).
		WithCatalogueGate(blnk.NewTopicCatalogueGate(cfg))
	relay.Start(ctx)

	return func() {
		relay.Stop()
		stopAssurance()
	}
}

// reportStrandedTopicPrefixes names, at start-up, any topic namespace that outbox rows still
// name and this deployment no longer owns.
//
// # Why it is reported here and reported loudly
//
// An outbox row records its destination topic at insert time, so changing KAFKA_TOPIC_PREFIX
// leaves committed rows naming the previous generation's topics. Those rows stay publishable
// only while the old prefix is declared in KAFKA_HISTORICAL_TOPIC_PREFIXES. Declare nothing
// and the publisher refuses each of those topics: the rows stay claimable for ever, their
// dead-letter writes and replays are refused too, and every status count still reads as
// ordinary outstanding work. The events are safe; delivery has stopped; and the only evidence
// is a topic name on rows nobody is reading.
//
// Start-up is the right moment because that is when the prefix is read. A rename reaches a
// process on its next restart, which is exactly this call, so the finding lands in the same
// boot log as the configuration that caused it — with the variable to set and the number of
// events waiting behind it.
//
// # Why it does not stop the server
//
// Blocking the prefix change until the old rows drain is the other sanctioned answer and the
// wrong one for a running ledger: the refusal would land on a rolling restart, taking down a
// server whose ledger and API are healthy over a condition that affects event delivery alone
// and is fixed by adding one variable. The relay starts, every other topic keeps draining,
// and the log names what is not.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - instance *blnk.Blnk: the service the audit reads its outbox through.
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

// assureEventTopics creates or grows the event topics to the configured geometry, logging what
// it did and what it could not do.
//
// It is separated from startEventRelay so the admin client's lifetime is exactly this call:
// assurance is a one-shot startup operation, and holding its connections open for the process
// lifetime would keep a SASL session per broker for something that never runs again.
//
// # It self-guards on an unconfigured broker list
//
// There is nothing to assure without brokers, and asking anyway is not harmless: the admin
// client answers "no brokers are configured", which this function would report at ERROR as an
// assurance failure — on the shipped default, in a supported steady state, and alongside a
// claim that "the relay still starts" which is not true on that path either. Its caller
// already declines to run in that state; the guard is here as well so a future call site
// cannot reintroduce the same misreport. Debug rather than info, because startEventRelay says
// it once at info and two lines for one condition is how a log stops being read.
//
// Parameters:
//   - ctx context.Context: bounded here, because assurance must not delay startup indefinitely
//     against an unreachable broker.
//   - cfg *config.Configuration: read for the broker list and the topic geometry.
func assureEventTopics(ctx context.Context, instance *blnk.Blnk, cfg *config.Configuration) func() {
	if cfg == nil || !blnk.KafkaBrokersConfigured(cfg.Kafka.Brokers) {
		logrus.Debug(
			"no Kafka brokers are configured, so there are no event topics to assure; the relay is not " +
				"started either, which startEventRelay reports once at info",
		)

		return func() {}
	}

	// ONE SYNCHRONOUS ATTEMPT FIRST, so that on a healthy cluster the topics exist before the
	// relay's first publish — the ordering the relay depends on, since auto-creation is disabled
	// and a publish to a missing topic fails, retries, and then fails to dead-letter because the
	// .dlt topic is missing for the same reason.
	if attemptEventTopicAssurance(ctx, instance) == nil {
		return func() {}
	}

	// IT FAILED, SO KEEP TRYING (PERF-P18). This used to be the end of the road: one attempt,
	// logged at error, and never revisited. On a first deploy that is the ordinary case rather
	// than an edge case — the broker's StatefulSet and the server's Deployment come up together,
	// so the server frequently wins — and the consequence was permanent: the topics were never
	// created, so every event failed to publish and then failed to dead-letter, and the only
	// remedy was to restart the pod. Retrying converts a permanent outage into a delay.
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
//
// It borrows the PROCESS-OWNED administrative client (PERF-P10) rather than building and
// closing one of its own. Two reasons: assurance now runs repeatedly, so a per-attempt client
// would mean a fresh transport, TCP connection and SASL/SCRAM handshake on every retry against
// a broker that is by definition already struggling; and the process-owned client is closed
// exactly once, by Blnk.Close(), so this code must not close what it did not open.
//
// Parameters:
//   - ctx context.Context: parent for the bounded attempt.
//   - instance *blnk.Blnk: the service container, for its shared administrative client.
//
// Returns:
//   - error: nil when the catalogue is assured. Non-nil is retryable by the caller.
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
	// function borrows it rather than owning it — and closing a borrowed client is worse than
	// leaking one: the retry loop calls this again after a failure, the credential-issuance
	// endpoint and the metrics collector share the same handle, and every one of them would
	// then be working against a closed connection. The container closes it on shutdown.

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

// retryEventTopicAssurance retries assurance with capped exponential backoff until it succeeds.
//
// # Why it does not give up
//
// There is no attempt count, deliberately. The condition being waited on is "the broker is
// reachable and will accept administrative requests", and there is no number of attempts after
// which the right answer changes: a cluster that is thirty minutes into a rolling restart still
// needs its topics when it comes back. Exhausting a retry budget would recreate exactly the
// permanent failure this exists to remove, only later and harder to diagnose.
//
// What bounds it instead is the process lifetime: ctx is the signal context and stop is closed
// by the relay's shutdown, so this ends when the server does.
//
// Parameters:
//   - ctx context.Context: cancelling it ends the retry loop.
//   - stop <-chan struct{}: closing it ends the loop even when ctx is live, which is what a
//     listener failure needs — it unwinds the defers without cancelling the context.
//   - instance *blnk.Blnk: the service container.
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

		// A GEOMETRY REFUSAL IS NOT TRANSIENT, so this stops instead of restating it for ever.
		// A topic that already holds records and needs more partitions refuses identically on
		// every attempt, and an inadequate replication factor is a property of the cluster:
		// neither is something another attempt can achieve, and growing a live topic is a
		// planned migration rather than a retry. Retrying would print the same refusal on a
		// widening schedule for the life of the process, which buries the one line an operator
		// has to act on under copies of itself.
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
// The command layer owns the SIGNAL and the EXIT STATUS; runServer owns the lifecycle. That is
// the same split cmd/workers.go uses, and it is what makes the lifecycle testable.
//
// Parameters:
//   - b *blnkInstance: the service container the role serves from.
//
// Returns:
//   - *cobra.Command: the `start` command.
func serverCommands(b *blnkInstance) *cobra.Command {
	// Define the `start` command for starting the server
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start blnk server", // Short description of the command
		// RunE, not Run, and that is the whole of the lifecycle fix at this level.
		//
		// Every failure in this role used to end in logrus.Fatal, which calls os.Exit and
		// therefore runs NO deferred function: the event relay, the retention sweeper, the
		// event metrics collector, the lineage processor, the hash-chain processor, the service
		// container, the database pool, the PostHog client and the tracing exporter were all
		// left open, and the process died holding relay leases that had to expire on their own.
		// Returning the error instead unwinds the stack through every defer in runServer, in
		// reverse registration order, and Cobra reports it and sets a non-zero exit status.
		//
		// SilenceUsage is set on the root command's side of this contract: a runtime failure is
		// not a usage error, and printing the help text after one buries the reason.
		RunE: func(cmd *cobra.Command, args []string) error {
			// ONE signal context for the whole process. Everything that has to stop when this
			// process is asked to stop reads this context and nothing else: every background
			// processor, and the HTTP listener inside runServer.
			//
			// Previously the processors were handed context.Background() — a context that is
			// never cancelled — and the listener installed a private signal.Notify. So a
			// SIGTERM was seen by exactly one participant, and the processors only discovered
			// it indirectly, when the deferred stops ran after the drain had completed.
			ctx, stopSignals := signal.NotifyContext(
				context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stopSignals()

			return runServer(ctx, b)
		},
	}

	return cmd
}

// runServer starts the API listener and every background processor the server role owns, blocks
// until ctx is cancelled or the listener fails, and then shuts everything down in order.
//
// # The shutdown order, and why it is this order
//
// Go runs defers last-in-first-out, so the registration order below IS the shutdown order,
// reversed. Read from the bottom up it is: the background processors stop, then the service
// container closes, then the database pool closes, then telemetry flushes.
//
// That order is not cosmetic. The processors write to the database and the broker, so stopping
// them first means nothing is in flight when the things they use go away. Blnk.Close() then
// drains the scheduled subscriber cleanups and closes the shared Kafka administrative client
// and the publisher — work that still needs the DATABASE, which is why it must come before the
// pool is closed and not after. Telemetry is flushed last so the shutdown itself is observable.
//
// # Why the cancellation overlaps the drain
//
// Every processor is started with ctx, the signal context. So the instant the signal lands they
// all begin winding down, CONCURRENTLY with the HTTP drain that startServer is performing. The
// deferred Stop() calls that follow are then joins on work already finishing rather than the
// start of it. Serialised — drain fully, then ask each processor to stop and wait — the total
// regularly exceeded the termination grace and the remainder was SIGKILLed, which is precisely
// what leaves a claimed outbox row locked until its lease expires.
//
// Parameters:
//   - ctx context.Context: the signal context. Cancelling it shuts the whole role down.
//   - b *blnkInstance: the service container.
//
// Returns:
//   - error: a start-up or listener failure. Nil on a clean, signalled shutdown.
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
			// immediately without exporting — so the telemetry describing the shutdown would
			// be the telemetry most reliably lost, exactly when it is wanted. Same
			// ten-second budget the worker role already uses for this.
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
			// failing a shutdown over, but is worth being able to see rather than inferring
			// from a gap in the data.
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

	// Close the service container (PERF-P17).
	//
	// Blnk.Close() existed and no production command ever called it, so on every shutdown the
	// process leaked what it owns: the Kafka writer per topic and their broker connections, the
	// shared administrative client, and the asynq client. It also, since PERF-P09, DRAINS the
	// scheduled subscriber cleanups — a credential revocation owed after a failed issuance, a
	// provisioning fence still held — so skipping it does not merely leak sockets, it abandons
	// compensating work and leaves a subscriber fenced until its lease expires.
	//
	// REGISTERED HERE, between the pool's defer and the processors' defers, and the position is
	// the whole point of where it sits. Defers run last-in-first-out, so this runs AFTER every
	// processor has stopped — nothing is publishing when the publisher closes — and BEFORE the
	// connection pool closes, which the drained cleanups still need to write through. Moved
	// either side of those two and it would either close the publisher under a live relay or
	// drain database work against a closed pool.
	//
	// The work is in a NAMED HELPER rather than an inline closure, for the reason the three
	// start* helpers around it are: runServer cannot be executed in a unit test, so anything
	// written inline here is only ever assertable as source text. See closeServiceContainer,
	// which is reachable from a test and carries the nil handling.
	defer closeServiceContainer(b)

	// Start lineage outbox processor
	// This worker processes pending lineage entries that were captured atomically with transactions
	lineageProcessor := blnk.NewLineageOutboxProcessor(b.blnk)
	lineageProcessor.Start(ctx)
	defer lineageProcessor.Stop()

	// Start the event metrics collector. It is the ONLY production maintainer of the
	// event pipeline's gauges, and without it every one of them is declared, initialised
	// and never recorded — so no rule in alerts/blnk-kafka-alerts.yml can fire whatever
	// the system is doing.
	//
	// The gauges are deliberately NOT counted here. Naming a number is how a comment
	// comes to disagree with the instrument list it describes, and this one already has
	// twice; startEventMetricsCollector's own documentation enumerates them, and
	// internal/metrics/metrics.go declares them.
	//
	// It runs in the SERVER role only, beside the lineage processor and for the
	// same reason: this is where the outbox background work already lives.
	// Starting it in the worker role as well would have two processes writing the
	// same gauges, each zeroing the other's series as stale.
	//
	// It is unconditional as to KAFKA, and it stays useful without it. A deployment with
	// no Kafka captures no event rows, so the gauges read from PostgreSQL report ZERO
	// rather than a backlog — and reporting zero is what lets a resolved alert resolve
	// instead of firing for ever on a series that simply stopped being written. The
	// gauges that need an admin client are not exported without one; the collector skips
	// the measurements it has no dependency for rather than declining to run.
	//
	// It is NOT unconditional as to REPLICAS (PERF-P13). The collector and the
	// retention sweeper are now started behind a singleton lease, because they are
	// maintenance of a shared table rather than serving: N replicas would publish N
	// series for one truth, each retiring the others' as stale, and issue N
	// competing sets of deletes. The relay below is deliberately left outside the
	// lease — its FOR UPDATE SKIP LOCKED claim divides work between replicas rather
	// than duplicating it. The sweeper's own start-up conditions are unchanged and
	// still evaluated inside the starter this now wraps.
	stopEventMaintenance := startEventMaintenance(ctx, b, cfg)
	defer stopEventMaintenance()

	// Start the Kafka event outbox relay. It is the ONLY thing that publishes captured
	// event rows, so without it a Kafka-configured deployment fills blnk.event_outbox
	// and delivers nothing.
	//
	// "Captured" covers both routes, and the relay does not distinguish them: rows
	// enrolled in their mutation's own transaction — the transaction, ledger, identity
	// and balance paths — and rows written standalone once the mutation had already
	// committed, as a balance monitor alert, a bulk batch summary and a system error are.
	//
	// EXACTLY ONE CALL, and this is it. The relay used also to be constructed and
	// started inline a few lines above, which meant one process ran TWO relays: two
	// topic-assurance passes, two sets of broker connections, two dual-delivery
	// enqueue paths, and a duplicated start/stop pair in the log that made the
	// lifecycle unreadable during incident triage. Worse, the inline copy did not
	// guard on the broker list, so the shipped Kafka-less default — a legitimate
	// steady state — reported itself twice at ERROR level while the guarded helper
	// reported the same condition at info. A second call here is not redundancy; it
	// is a second relay.
	//
	// Topic assurance runs first, INSIDE the helper, and the order is the point: the
	// relay's first publish must not be the thing that discovers a missing topic,
	// because auto-creation is disabled and a publish to a topic that does not exist
	// fails, retries, and then fails to dead-letter for the same reason. See the
	// helper's documentation for why a failure there is logged and stepped past
	// rather than fatal, and for why no brokers at all is answered with a single
	// info line and no relay.
	//
	// The relay belongs to the SERVER role, beside the lineage processor and the
	// event metrics collector, following this repository's convention that outbox
	// relays live where the outbox background work already is. Starting it in the
	// worker role as well would have two processes claiming the same rows — which
	// the FOR UPDATE SKIP LOCKED claim tolerates, but it would double the broker
	// connections and the dual-delivery enqueues for no gain.
	stopEventRelay := startEventRelay(ctx, b.blnk, cfg)
	defer stopEventRelay()

	// The retention sweeper is NOT started here. Without it blnk.event_outbox grows without
	// bound — every delivered and every dead-lettered row stays for ever, so the table the
	// relay's claim query scans keeps getting larger and the oldest rows are kept long past any
	// replay window — but it is maintenance of a shared table, so it is started by
	// startEventMaintenance above, behind the singleton lease, together with the metrics
	// collector (PERF-P13). It remains a no-op unless RELAY_EVENT_RETENTION_DAYS is set.

	// The settlement processor runs beside the relay and the sweeper, in the same role and with
	// the same lifecycle. It is what finishes the broker-side work a subscriber operation could
	// not: an authorization change that pruned the broker and then failed to persist, a
	// credential written whose compensating revocation also failed, a credential revoked whose
	// registry record could not be cleared. Each of those is recorded durably on the subscriber
	// row, and without this nothing ever discharges them — the obligations accumulate and the
	// registry stays diverged from the broker.
	//
	// It is a no-op when no broker is configured, which is a legitimate steady state: with no
	// broker there is no broker-side state to reconcile.
	stopSubscriberSettlement := startSubscriberSettlement(ctx, b.blnk)
	defer stopSubscriberSettlement()

	// THE BALANCE-MONITOR HANDOFF DRAINER, and it is NOT optional maintenance. Every ledger
	// transaction that moves a monitored balance records a handoff row inside its own
	// transaction — an intent saying "these monitors have not been judged yet" — and the
	// post-commit evaluation stands down whenever the handoff is in play. This processor is
	// therefore the SOLE owner of `balance.monitor`: without it the rows accumulate
	// unevaluated, no monitor alert is ever published, and the only symptom is silence.
	//
	// It belongs beside the relay and the settlement processor rather than behind the
	// maintenance lease, because it is not maintenance of a shared table — it is the
	// evaluation half of a ledger write, and every server instance may take its share of it.
	// The claim is FOR UPDATE SKIP LOCKED, so N instances divide the work rather than
	// duplicating it.
	//
	// It declines to start when no broker is configured, which is the same predicate the
	// writer uses to decide whether to record a handoff at all — so a Kafka-less deployment
	// keeps the post-commit evaluation and has nothing to drain.
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

// startSubscriberSettlement starts the subscriber settlement processor and returns the function
// that stops it.
//
// # The two obstacles are logged differently, and the difference matters
//
// NO BROKER CONFIGURED is logged at info. It is a legitimate steady state — the AAP requires the
// whole event-streaming feature to degrade to a no-op without Kafka — and with no broker there is
// no broker-side state that could diverge from the registry. Reporting it as a warning on every
// start-up of every Kafka-less deployment would train an operator to ignore the one message that
// IS a warning.
//
// ANYTHING ELSE is logged at warning, because it means a broker IS configured and the settlement
// pass is not running. The symptom of that is silent: obligations accumulate on subscriber rows,
// the registry and the broker stay diverged, and the only visible trace is the settlement gauges
// climbing — which is exactly why the absence has to be said out loud at start-up.
//
// A cleanup FUNCTION rather than a deferred stop, for the reason every other starter here returns
// one: a defer inside this helper would fire on return and stop the processor before it ever
// ticked.
//
// Parameters:
//   - ctx context.Context: cancelling it stops the processor.
//   - instance *blnk.Blnk: the service container, for its datasource and configuration.
//
// Returns:
//   - func(): stops the processor and waits for the pass in flight. Never nil.
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
//
// The seam is this narrow deliberately. Widening it to *blnk.Blnk would make the failure
// branch below unreachable from a test, because a container built without Kafka brokers gets
// the no-op publisher and closes cleanly by design — the graceful-degradation contract — so
// there would be no way to construct a container whose close fails.
type serviceContainerCloser interface {
	Close() error
}

// closeServiceContainer releases the service container held by the CLI instance.
//
// It is what the server command defers. The nil checks are HERE rather than at the call site
// so that the deferred call reads as one line and cannot be made unsafe by an edit to that
// line: the CLI reaches the server command only after preRun has built the container, but
// tests construct a blnkInstance with no container at all, and Close dereferences the
// container's fields.
//
// The typed-nil check is not redundant with the one inside closeContainer. A nil *blnk.Blnk
// placed in an interface produces a NON-nil interface holding a nil pointer, so passing it
// through would defeat that check and panic on the call.
//
// Parameters:
//   - instance *blnkInstance: the CLI instance whose container to close. A nil instance and a
//     nil container are both no-ops.
func closeServiceContainer(instance *blnkInstance) {
	if instance == nil || instance.blnk == nil {
		return
	}

	closeContainer(instance.blnk)
}

// closeContainer closes one service container and reports the outcome.
//
// The error is LOGGED rather than returned or discarded. It cannot be returned: this runs from
// a defer during shutdown, after the command's result is already decided. It must not be
// discarded, because the thing a failing close reports is a LOST FLUSH — a kafka.Writer that
// could not produce what it had batched — and those events are already marked dispatched in
// the outbox. A silent `_ =` would make that indistinguishable from a clean shutdown, which is
// exactly the failure the close exists to prevent.
//
// Parameters:
//   - container serviceContainerCloser: the container to close. A nil container is a no-op.
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

// startEventRetention starts the event outbox retention sweeper and returns the function
// that stops it.
//
// # The two obstacles are logged differently, and the difference matters
//
// RETENTION DISABLED is the default and is logged at INFO, naming the variable so an operator
// who believes retention is on can discover that it is not. Warning on the shipped default
// would train them to ignore the one message that is a warning.
//
// ANYTHING ELSE is logged at WARNING: retention IS configured and is not running, so the
// control the operator asked for is absent and the symptom is a table that quietly grows.
//
// It returns a cleanup FUNCTION for the reason the other two starters do — a defer inside this
// helper would fire on return and stop the sweeper before it ever ticked.
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

// startBalanceMonitorHandoff starts the balance-monitor handoff processor and returns the
// function that stops it.
//
// # What it owns, and why nothing else can
//
// The atomic writers record a handoff row inside every ledger transaction that moves a
// monitored balance. That row is an INTENT — "these monitors have not been judged yet" —
// and this processor is the only thing that acts on it. It evaluates the snapshot with the
// unchanged condition logic and writes the resulting alerts and the handoff's completion in
// one database transaction, which is what brings `balance.monitor` under requirement R-2:
// the alert can no longer be lost to a crash between a balance commit and an insert.
//
// The post-commit evaluation in transaction_execution.go stands down whenever the handoff
// is in play, so this is not a second opinion — it is the sole owner. That is why a missing
// start here would produce no alerts at all rather than merely slower ones, and why the
// no-broker case is answered by not writing handoffs in the first place rather than by
// starting this and hoping.
//
// # The obstacle is logged at INFO, not at WARN
//
// No Kafka broker is a legitimate, shipped steady state, not a misconfiguration. Such a
// deployment writes no handoffs — database.recordBalanceMonitorHandoffs reads the same
// predicate — and keeps the post-commit evaluation, so declining to start here is the
// correct and complete behaviour and there is nothing for an operator to fix. Logging it
// at WARN would make every Kafka-less deployment emit a warning on every start for
// behaving exactly as documented.
//
// Parameters:
//   - ctx context.Context: cancelled to stop the processor.
//   - instance *blnk.Blnk: the service handle.
//   - cfg *config.Configuration: read for the broker list only.
//
// Returns:
//   - func(): the stop function, safe to call even when nothing was started.
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
//
// One helper rather than the same five fields written out at each listener, so the API server
// and the worker monitoring server cannot drift apart — a listener hardened in one place and
// not the other is the same exposure as no hardening at all, and harder to notice.
//
// Parameters:
//   - srv *http.Server: the server to bound. Nil is returned unchanged, so a caller that may
//     not have built a server need not branch.
//
// Returns:
//   - *http.Server: srv, with the bounds applied.
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

// startEventMaintenance runs the leader-elected maintenance work and returns the function that
// stops it. PERF-P13.
//
// # What is gated and what is not
//
// GATED: the event metrics collector and the retention sweeper. Both are maintenance of a
// shared table, and running N of each is not N times the benefit — for the collector it is N
// competing answers to one question, and for the sweeper it is N processes contending for the
// same row locks. See database/event_maintenance.go for the full reasoning.
//
// NOT GATED, and started unconditionally by runServer: the relay. Its claim is FOR UPDATE SKIP
// LOCKED, which divides work between replicas instead of duplicating it, so gating it would
// discard the horizontal scaling the outbox pattern is for.
//
// # Degradation is towards RUNNING, deliberately
//
// If the lease cannot be evaluated at all — no configuration, no pool — this starts the
// maintenance work anyway, ungated, and says so at warning. The alternative fails towards
// silence: no gauges, so no rule in alerts/blnk-kafka-alerts.yml can fire, and no
// sweeper, so the outbox grows without bound. A duplicated gauge series on a multi-replica
// deployment is a monitoring nuisance that an operator can see; unmeasured and unbounded
// growth is neither visible nor bounded. Single-replica deployments — the common case, and the
// only one the shipped manifests default to — are unaffected either way.
//
// Parameters:
//   - ctx context.Context: cancelling it stops the loop and whatever it started.
//   - b *blnkInstance: the service container, passed to the two starters.
//   - cfg *config.Configuration: read for the datasource and the Kafka broker list.
//
// Returns:
//   - func(): stops the loop, stops the maintenance work and releases the lease. Never nil,
//     and it does NOT depend on ctx having been cancelled — a listener failure unwinds the
//     defers with a live context, so a stop that waited only on ctx would deadlock there.
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

// runEventMaintenanceLoop competes for the maintenance lease and runs the work while it holds
// it, until ctx is cancelled or stop is closed.
//
// The shape is: try to become the leader; if you are, run the work and hold the lease; if you
// lose the lease, stop the work and go back to competing. A replica that is not the leader
// spends one non-blocking statement per interval and nothing else.
//
// Parameters:
//   - ctx context.Context: cancelling it ends the loop.
//   - stop <-chan struct{}: closing it ends the loop even when ctx is live.
//   - ds *database.Datasource: the pool the lease is taken on.
//   - b *blnkInstance: the service container.
//   - cfg *config.Configuration: configuration for the two starters.
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
//
// The two ways of not getting it are reported differently on purpose. Losing to another replica
// is the expected answer on N-1 replicas of N and is logged at debug, because logging it at
// info would produce a line per replica per interval for ever. Failing to ASK is a fault and is
// logged at warning, because it means this replica cannot participate at all and, if every
// replica is in the same state, nothing is maintaining the pipeline.
//
// Parameters:
//   - ctx context.Context: parent for the bounded attempt.
//   - ds *database.Datasource: the pool to take the lease on.
//
// Returns:
//   - *database.EventMaintenanceLease: the lease, or nil if it was not acquired.
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
//
// Returns:
//   - bool: true when the lease was LOST and the caller should compete again; false when ctx
//     was cancelled or stop was closed, which is an ordinary shutdown.
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
//
// Returns:
//   - bool: true when the wait completed and the caller should carry on; false when ctx was
//     cancelled or stop was closed.
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
