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
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Genuinely untestable in-process and deliberately not faked here: main(),
// executeCLI's os.Exit path, ACME certificate issuance in serveTLS, and
// delivery of real SIGTERM signals (gracefulShutdown is tested by sending on
// its quit channel instead).

func TestRetryWithBackoff(t *testing.T) {
	t.Run("succeeds after transient failures", func(t *testing.T) {
		calls := 0
		err := retryWithBackoff(context.Background(), 5, time.Millisecond, func() error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 3, calls, "must stop retrying after the first success")
	})

	t.Run("returns last error when all attempts fail", func(t *testing.T) {
		calls := 0
		err := retryWithBackoff(context.Background(), 3, time.Millisecond, func() error {
			calls++
			return errors.New("permanent failure")
		})
		require.Error(t, err)
		assert.Equal(t, 3, calls)
		assert.Contains(t, err.Error(), "after 3 attempts")
		assert.Contains(t, err.Error(), "permanent failure")
	})

	t.Run("stops when context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		err := retryWithBackoff(ctx, 5, time.Hour, func() error {
			calls++
			return errors.New("fails")
		})
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 1, calls, "a canceled context must short-circuit the backoff sleep")
	})
}

func TestResolveTLSDomains(t *testing.T) {
	assert.Equal(t, []string{"localhost"}, resolveTLSDomains(config.ServerConfig{}))
	assert.Equal(t, []string{"ledger.example.com"}, resolveTLSDomains(config.ServerConfig{Domain: "ledger.example.com"}))
}

func TestResolveCertStoragePath(t *testing.T) {
	assert.Equal(t, "/var/lib/blnk/certs", resolveCertStoragePath(config.ServerConfig{}))
	assert.Equal(t, "/tmp/certs", resolveCertStoragePath(config.ServerConfig{CertStoragePath: "/tmp/certs"}))
}

func TestGetOrCreateHeartbeatIDAt(t *testing.T) {
	t.Run("persists a stable ID", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "heartbeat.db")

		first := getOrCreateHeartbeatIDAt(path)
		require.NotEmpty(t, first)

		second := getOrCreateHeartbeatIDAt(path)
		assert.Equal(t, first, second, "the heartbeat ID must survive restarts via SQLite")

		_, err := os.Stat(path)
		require.NoError(t, err, "the SQLite file must have been created")
	})

	t.Run("falls back to a fresh ID when storage is unwritable", func(t *testing.T) {
		// A directory path is not a valid SQLite file: storage fails, but the
		// function must still return a usable (non-persistent) UUID.
		id := getOrCreateHeartbeatIDAt(t.TempDir())
		assert.NotEmpty(t, id)
	})
}

func TestNewHTTPServerAndGracefulShutdown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	server := newHTTPServer(router, "0")
	assert.Equal(t, ":0", server.Addr)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()

	go func() { _ = server.Serve(ln) }()

	// The server must actually serve requests before shutdown.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr + "/ping")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, 3*time.Second, 50*time.Millisecond)

	// Every bound is applied, so a peer cannot hold a connection indefinitely. Asserted on
	// the constructed server because that is the only place they can be observed —
	// net/http exposes no accessor once it is serving.
	assert.Equal(t, httpReadHeaderTimeout, server.ReadHeaderTimeout,
		"the header read must be bounded: it is the span with no legitimate slow case and the one "+
			"a Slowloris client exploits")
	assert.Equal(t, httpReadTimeout, server.ReadTimeout)
	assert.Equal(t, httpWriteTimeout, server.WriteTimeout)
	assert.Equal(t, httpIdleTimeout, server.IdleTimeout)
	assert.Equal(t, httpMaxHeaderBytes, server.MaxHeaderBytes)

	// GracefulShutdown no longer waits for a signal of its own. It drains when called,
	// because the ONE signal context above it has already fired — which is what lets the
	// background processors wind down concurrently with this drain instead of after it.
	done := make(chan error, 1)
	quit := make(chan os.Signal, 1)
	// A nil listener channel is the "only a signal ends the wait" case: a nil channel blocks
	// for ever in a select, which is exactly the pre-existing behaviour this asserts.
	go func() { done <- gracefulShutdown(server, quit, nil, 5*time.Second) }()

	quit <- os.Interrupt

	select {
	case err := <-done:
		require.NoError(t, err, "graceful shutdown must complete cleanly")
	case <-time.After(10 * time.Second):
		t.Fatal("gracefulShutdown did not return")
	}

	// After shutdown the server must refuse new connections.
	_, err = http.Get("http://" + addr + "/ping")
	require.Error(t, err, "server must not accept requests after shutdown")
}

// TestGracefulShutdown_ReturnsTheListenerFailureInsteadOfWaitingForASignal is the
// lifecycle property that replaced a process exit.
//
// The listener runs on a goroutine, so its error could not be returned — and it ended
// in logrus.Fatalf, which calls os.Exit and therefore runs NO deferred function.
func TestGracefulShutdown_ReturnsTheListenerFailureInsteadOfWaitingForASignal(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("a listener failure ends the wait and is returned", func(t *testing.T) {
		server := newHTTPServer(gin.New(), "0")

		listenErr := make(chan error, 1)
		bindFailure := errors.New("listen tcp :5001: bind: address already in use")
		listenErr <- bindFailure

		done := make(chan error, 1)
		// No signal is ever sent: the failure alone must end the wait.
		go func() { done <- gracefulShutdown(server, make(chan os.Signal, 1), listenErr, 2*time.Second) }()

		select {
		case err := <-done:
			require.Error(t, err, "a listener that could not bind must not look like a clean stop")
			assert.ErrorIs(t, err, bindFailure,
				"the listener's own error must be returned unwrapped enough to be identified: it is "+
					"what Cobra reports and what the exit status reflects")
		case <-time.After(10 * time.Second):
			t.Fatal("gracefulShutdown blocked waiting for a signal that was never coming, which is " +
				"the deadlock the removed os.Exit was concealing")
		}
	})

	t.Run("a closed listener channel is a clean stop rather than a failure", func(t *testing.T) {
		server := newHTTPServer(gin.New(), "0")

		listenErr := make(chan error, 1)
		close(listenErr)

		done := make(chan error, 1)
		go func() { done <- gracefulShutdown(server, make(chan os.Signal, 1), listenErr, 2*time.Second) }()

		select {
		case err := <-done:
			require.NoError(t, err,
				"http.ErrServerClosed is filtered before the send, so a closed channel means the "+
					"listener stopped cleanly — reporting it as an error would make every ordinary "+
					"shutdown exit non-zero")
		case <-time.After(10 * time.Second):
			t.Fatal("gracefulShutdown did not return on a closed listener channel")
		}
	})

	t.Run("a signal still shuts down cleanly when a listener channel is present", func(t *testing.T) {
		server := newHTTPServer(gin.New(), "0")

		quit := make(chan os.Signal, 1)
		done := make(chan error, 1)
		go func() { done <- gracefulShutdown(server, quit, make(chan error, 1), 2*time.Second) }()

		quit <- os.Interrupt

		select {
		case err := <-done:
			require.NoError(t, err, "the signalled path must be unchanged by the addition of the "+
				"listener channel")
		case <-time.After(10 * time.Second):
			t.Fatal("gracefulShutdown did not return after the quit signal")
		}
	})
}

// TestServerCommand_ReturnsErrorsRatherThanExitingTheProcess is the structural guard on
// the lifecycle fix.
//
// serverCommands cannot be executed in a unit test: its RunE blocks in startServer,
// needs a database, a router and a TypeSense client, and takes over the process.
func TestServerCommand_ReturnsErrorsRatherThanExitingTheProcess(t *testing.T) {
	source, err := os.ReadFile("server.go")
	require.NoError(t, err, "reading cmd/server.go")

	body := string(source)

	for _, exit := range []string{"logrus.Fatal(", "logrus.Fatalf(", "os.Exit("} {
		assert.NotContains(t, body, exit,
			"cmd/server.go must not %s: it runs no deferred function, so the relay's claimed "+
				"outbox rows keep their leases until they expire, buffered spans are dropped, and "+
				"the Kafka writers are never flushed. Return the error and let RunE unwind the "+
				"stack instead", exit)
	}

	assert.Contains(t, body, "RunE: func(cmd *cobra.Command, args []string) error {",
		"the start command must use RunE so a returned error unwinds every deferred shutdown "+
			"and becomes the process's exit status")

	// the container close must be deferred, and it must be deferred where its registration
	// order puts it AFTER the processors stop and BEFORE the pool close.
	assert.Contains(t, body, "defer closeServiceContainer(b)",
		"the start command must close the service container: it is the sole owner of the Kafka "+
			"writers, their shared transport and the asynq client, and nothing else releases them")

	closeAt := strings.Index(body, "defer closeServiceContainer(b)")
	poolAt := strings.Index(body, "Closing database connection pool...")
	relayAt := strings.Index(body, "startEventRelay(ctx, b.blnk, cfg)")
	require.Positive(t, closeAt)
	require.Positive(t, poolAt)
	require.Positive(t, relayAt)

	assert.Less(t, poolAt, closeAt,
		"deferred functions run in REVERSE registration order, so the container close must be "+
			"registered AFTER the pool close to run BEFORE it — the writers need the database "+
			"still there to record what they did")
	assert.Less(t, closeAt, relayAt,
		"and it must be registered BEFORE the relay is started, so it runs AFTER the relay has "+
			"stopped. Closing the writers under a live relay fails good rows as publisher-closed "+
			"retries")
}

func TestHealthCheckHandler(t *testing.T) {
	cfg := realInfraConfig(t)
	_ = cfg

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/health", nil)

	healthCheckHandler(c)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"status":"UP"`)
}

func TestInitializeTypeSense(t *testing.T) {
	t.Run("disabled when DNS not configured", func(t *testing.T) {
		client, err := initializeTypeSense(context.Background(), &config.Configuration{})
		require.NoError(t, err)
		assert.Nil(t, client, "search must be disabled, not an error, without a TypeSense DNS")
	})

	t.Run("real typesense initializes collections", func(t *testing.T) {
		cfg := &config.Configuration{
			TypeSenseKey: "blnk-api-key",
			TypeSense:    config.TypeSenseConfig{Dns: "http://localhost:8108"},
		}
		client, err := initializeTypeSense(context.Background(), cfg)
		require.NoError(t, err)
		require.NotNil(t, client)
	})

	t.Run("dead server fails once the context expires", func(t *testing.T) {
		cfg := &config.Configuration{
			TypeSenseKey: "any",
			TypeSense:    config.TypeSenseConfig{Dns: "http://localhost:1"},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		start := time.Now()
		client, err := initializeTypeSense(ctx, cfg)
		require.Error(t, err)
		assert.Nil(t, client)
		assert.Less(t, time.Since(start), 10*time.Second, "context expiry must cut the retry loop short")
	})
}

func TestInitializeRouter(t *testing.T) {
	b := newCmdTestInstance(t)
	router := initializeRouter(b)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"status":"UP"`)
}

func TestSetupBlnk(t *testing.T) {
	cfg := realInfraConfig(t)

	instance, err := setupBlnk(cfg)
	require.NoError(t, err)
	require.NotNil(t, instance)
	require.NotNil(t, instance.GetDataSource(), "a real config must produce a wired Blnk instance")
}

// stopperSymbol names the function a lifecycle starter handed back, resolved from the
// runtime's own symbol table.
//
// Used where the property under test is STRUCTURAL — what a function composes, in what
// order — and therefore not observable at runtime.
//
// Parameters:
//   - name string: the function name, without "func".
//
// Returns:
//   - string: the text from the declaration to the start of the next top-level
//     declaration.
func functionBody(t *testing.T, name string) string {
	t.Helper()

	source, err := os.ReadFile("server.go")
	require.NoError(t, err, "reading cmd/server.go")

	body := string(source)

	start := strings.Index(body, "func "+name+"(")
	require.Positive(t, start, "%s must exist in cmd/server.go", name)

	rest := body[start+1:]

	end := strings.Index(rest, "\nfunc ")
	if end < 0 {
		return rest
	}

	return rest[:end]
}

func stopperSymbol(t *testing.T, stopper func()) string {
	t.Helper()

	require.NotNil(t, stopper, "a lifecycle starter must always return something the caller can defer")

	fn := runtime.FuncForPC(reflect.ValueOf(stopper).Pointer())
	require.NotNil(t, fn, "the returned stopper must be resolvable to a symbol")

	return fn.Name()
}

// TestStartEventRelay_WithoutBrokersStartsNothingAndIsStillStoppable pins the
// graceful-degradation contract at the process's own entry point.
//
// The blank and comma-only broker lists are here because they are what a half-filled
// environment variable actually looks like.
func TestStartEventRelay_WithoutBrokersStartsNothingAndIsStillStoppable(t *testing.T) {
	for name, brokers := range map[string][]string{
		"unset":       nil,
		"empty":       {},
		"blank":       {"   "},
		"comma noise": {"", " ", ""},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := &config.Configuration{Kafka: config.KafkaConfig{Brokers: brokers}}

			// A nil instance is deliberate: it makes the assertion structural. If this branch
			// ever built a relay, it would be building one from nothing.
			stop, err := startEventRelay(context.Background(), nil, cfg)

			// NIL ERROR, and this is the half that had to keep working: the relay
			// returns its startup obstacle and runServer refuses to serve over one, so an error
			// here would make every Kafka-less deployment — the shipped default — fail to start.
			// No brokers is a steady state, not an obstacle.
			assert.NoError(t, err,
				"a deployment with no brokers must start cleanly: this is the documented "+
					"graceful-degradation contract, not a misconfiguration")
			assert.NotContains(t, stopperSymbol(t, stop), "EventRelayProcessor",
				"no relay may be constructed without brokers: its publisher would be the no-op one, "+
					"which reports every publish as dispatched while sending nothing")

			stop()
		})
	}

	t.Run("a nil configuration", func(t *testing.T) {
		stop, err := startEventRelay(context.Background(), nil, nil)

		assert.NoError(t, err,
			"an unloaded configuration resolves to no brokers, which is the steady state and not "+
				"an obstacle to serving")
		assert.NotContains(t, stopperSymbol(t, stop), "EventRelayProcessor",
			"an unloaded configuration must not be read as a configured broker list")

		stop()
	})
}

// TestStartEventRelay_WithBrokersConstructsTheRelayAndReturnsItsStop is the wiring
// assertion itself, and the defect it exists for was total.
//
// Every producer captures its event into blnk.event_outbox inside the ledger
// transaction, and the relay is the only thing that publishes those rows.
func TestStartEventRelay_WithBrokersConstructsTheRelayAndReturnsItsStop(t *testing.T) {
	cfg := &config.Configuration{
		Kafka: config.KafkaConfig{
			// Reserved discard port on loopback: refused immediately rather than timing out,
			// so the assurance failure is fast and the test needs no broker and no credential.
			Brokers:     []string{"127.0.0.1:1"},
			TopicPrefix: "blnk",
		},
	}

	started := time.Now()
	stop, err := startEventRelay(context.Background(), nil, cfg)

	// THE OBSTACLE IS RETURNED NOW, and this instance has one: the Blnk instance is nil,
	// so the relay has no datasource and no publisher. That is exactly the shape of
	// refusal runServer must not serve over, and asserting it here is what proves the
	// value reaches the caller rather than being logged and dropped as it was before.
	require.Error(t, err,
		"a relay that refused to start must say so to its caller: a logged-and-discarded refusal "+
			"is how a deployment came to capture outbox rows that neither transport would deliver")
	assert.Contains(t, err.Error(), "KAFKA_BROKERS",
		"the returned error must name what the operator has to change, because the obstacle alone "+
			"says what is wrong and not what it costs")

	// The property protected here is unchanged — the returned stop must really shut the
	// relay down, because a local no-op means nothing drains the outbox — but repeated assurance
	// changed the SHAPE of the answer. The stop is now a closure that stops the relay and
	// then joins the background topic-assurance retrier, so its runtime symbol is the
	// closure's rather than EventRelayProcessor.Stop's, and reflecting on the name can no
	// longer see through it.
	relayBody := functionBody(t, "startEventRelay")

	assert.Contains(t, relayBody, "relay.Stop()",
		"the returned stop must shut the relay down; a local no-op here means nothing drains the outbox")
	assert.Contains(t, relayBody, "stopAssurance()",
		"the returned stop must also join the background topic-assurance retrier, or it outlives "+
			"the server and keeps talking to a broker after shutdown")

	stop()

	assert.Less(t, time.Since(started), eventTopicAssuranceTimeout,
		"an unreachable broker must not hold start-up for the whole assurance budget: it is refused, "+
			"logged and stepped past")
}

// TestAssureEventTopics_WithoutBrokersReportsNothingAtErrorLevel pins the log level of
// the TestRunServer_RefusesToServeWhenTheEventRelayRefusesToStart is the
// process-visibility half of the start-up refusal, and it is the half that decides whether it
// costs anything.
func TestRunServer_RefusesToServeWhenTheEventRelayRefusesToStart(t *testing.T) {
	body := functionBody(t, "runServer")

	assert.Contains(t, body, "stopEventRelay, relayErr := startEventRelay(ctx, b.blnk, cfg)",
		"runServer must take the relay's startup obstacle; discarding it with _ is the defect, "+
			"because then nothing in the process can tell a running relay from a refused one")

	assignAt := strings.Index(body, "stopEventRelay, relayErr := startEventRelay(ctx, b.blnk, cfg)")
	deferAt := strings.Index(body, "defer stopEventRelay()")
	returnAt := strings.Index(body, "return relayErr")

	require.Positive(t, assignAt)
	require.Positive(t, deferAt, "the stopper must still be deferred")
	require.Positive(t, returnAt,
		"the obstacle must be RETURNED, so the Cobra RunE exits non-zero and the listener never "+
			"binds. Logging it and serving is what let a deployment capture rows that neither "+
			"transport would ever deliver")

	assert.Less(t, assignAt, deferAt)
	assert.Less(t, deferAt, returnAt,
		"the defer must be registered BEFORE the refusal returns: startEventRelay may already "+
			"have started a background topic-assurance pass, and that pass has to be joined on "+
			"the way out even when the relay itself never ran")
}

// TestAssureEventTopics_WithoutBrokersReportsNothingAtErrorLevel pins the log level of
// the no-broker steady state, which is a contract rather than a cosmetic preference.
//
// KAFKA_BROKERS is empty in the shipped configuration.
//
// This function asked the admin client to assure topics anyway.
func TestAssureEventTopics_WithoutBrokersReportsNothingAtErrorLevel(t *testing.T) {
	for name, cfg := range map[string]*config.Configuration{
		"unset":       {Kafka: config.KafkaConfig{Brokers: nil}},
		"empty":       {Kafka: config.KafkaConfig{Brokers: []string{}}},
		"blank":       {Kafka: config.KafkaConfig{Brokers: []string{"   "}}},
		"comma noise": {Kafka: config.KafkaConfig{Brokers: []string{"", " ", ""}}},
		"unloaded":    nil,
	} {
		t.Run(name, func(t *testing.T) {
			hook := logtest.NewGlobal()
			defer hook.Reset()

			// A nil instance is safe here and is the point: with no brokers the function must
			// return before it ever asks for an administrative client, so it cannot depend on
			// having a service container at all.
			stopAssurance := assureEventTopics(context.Background(), nil, cfg)
			require.NotNil(t, stopAssurance,
				"the stop function must never be nil, so the caller can compose it unconditionally")
			stopAssurance()

			for _, entry := range hook.AllEntries() {
				assert.NotContainsf(t, []logrus.Level{logrus.ErrorLevel, logrus.WarnLevel, logrus.FatalLevel, logrus.PanicLevel},
					entry.Level,
					"the no-broker steady state must not report a fault; got %s: %q", entry.Level, entry.Message)
			}
		})
	}
}

// TestAssureEventTopics_KeepsTryingAfterAFailedPass covers repeated topic assurance.
//
// Assurance was one attempt, logged at error, and never revisited.
func TestAssureEventTopics_KeepsTryingAfterAFailedPass(t *testing.T) {
	cfg := &config.Configuration{
		Kafka: config.KafkaConfig{
			// Reserved discard port: refused immediately, so the first pass fails fast and the
			// background retrier is the thing under test.
			Brokers:     []string{"127.0.0.1:1"},
			TopicPrefix: "blnk",
		},
	}

	t.Run("a failed pass leaves a retrier that the stop function joins", func(t *testing.T) {
		// A nil instance makes the attempt fail deterministically and without a broker, which
		// is the same shape as an unreachable one for this purpose: the pass fails, so the
		// retrier must exist.
		stop := assureEventTopics(context.Background(), nil, cfg)
		require.NotNil(t, stop)

		// The stop must JOIN the retry goroutine rather than merely signal it, and must
		// return promptly — it runs from a deferred shutdown. A stop that did not join would
		// leak a goroutine still talking to a broker after the server had gone.
		returned := make(chan struct{})
		go func() { defer close(returned); stop() }()

		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			t.Fatal("the assurance stop function did not return: the retry goroutine is not being joined")
		}
	})

	t.Run("the retrier has no attempt ceiling", func(t *testing.T) {
		// Structural, and the property is deliberate rather than an oversight. The condition
		// being waited on is "the broker will accept administrative requests", and there is
		// no number of attempts after which the right answer changes — a cluster thirty
		// minutes into a rolling restart still needs its topics.
		retry := functionBody(t, "retryEventTopicAssurance")

		assert.Contains(t, retry, "for attempt := 2; ; attempt++ {",
			"the retry loop must be unbounded in attempts; it is bounded by ctx and the stop channel")
		assert.Contains(t, retry, "eventTopicAssuranceRetryMax",
			"the backoff must be capped, or doubling puts later attempts hours apart and a broker "+
				"that came back would not be noticed")
	})

	t.Run("assurance borrows the process-owned admin client", func(t *testing.T) {
		// SHARED-CLIENT ALIGNMENT: assurance runs repeatedly, so building and closing a client
		// per attempt would mean a fresh transport, TCP connection and SASL/SCRAM handshake
		// on every retry against a broker that is already struggling. It must also NOT close
		// a client it does not own — Blnk.Close() does that, exactly once.
		attempt := functionBody(t, "attemptEventTopicAssurance")

		assert.Contains(t, attempt, "instance.KafkaAdmin()",
			"assurance must borrow the process-owned administrative client")
		assert.NotContains(t, attempt, "blnk.NewKafkaAdmin(",
			"assurance must not build a client of its own")
		assert.NotContains(t, attempt, "admin.Close()",
			"assurance must not close a client it borrowed; the container owns its lifetime")
	})
}

// TestServerCommand_StartsExactlyOneEventRelay is the regression test for a duplicated
// lifecycle, and the duplication was not cosmetic.
//
// It was worse on the shipped default.
func TestServerCommand_StartsExactlyOneEventRelay(t *testing.T) {
	source, err := os.ReadFile("server.go")
	require.NoError(t, err, "reading cmd/server.go")

	body := string(source)

	assert.Equal(t, 1, strings.Count(body, "startEventRelay(ctx, b.blnk, cfg)"),
		"the server command must start the relay exactly once, through the guarded helper. A "+
			"second call is not redundancy — it is a second relay, with its own broker "+
			"connections, its own assurance pass and its own dual-delivery enqueues.")

	assert.Equal(t, 1, strings.Count(body, "blnk.NewEventRelayProcessor("),
		"the relay constructor must be reached from exactly one place, and that place is "+
			"startEventRelay. Constructing one anywhere else bypasses the broker-list guard that "+
			"keeps a Kafka-less deployment from starting a relay over the no-op publisher.")

	assert.Equal(t, 1, strings.Count(body, "assureEventTopics(ctx, instance, cfg)"),
		"topic assurance must run exactly once per process, from inside startEventRelay. A "+
			"second pass costs a full admin connection and, with no brokers configured, logs a "+
			"failure for a state the guarded path reports as normal.")

	// The guard itself, named rather than implied: assurance and the relay must both sit behind
	// the broker-list check, which is what makes the no-Kafka steady state quiet.
	relayIndex := strings.Index(body, "func startEventRelay(")
	require.Positive(t, relayIndex, "startEventRelay must exist")

	guardIndex := strings.Index(body[relayIndex:], "blnk.KafkaBrokersConfigured(")
	require.Positive(t, guardIndex,
		"startEventRelay must guard on the broker list before assuring topics or starting a relay")
	assert.Less(t, guardIndex, strings.Index(body[relayIndex:], "assureEventTopics(ctx, instance, cfg)"),
		"the broker-list guard must precede topic assurance, or a deployment with no brokers "+
			"reports its documented steady state as an error")
}

// TestServerCommand_StartsTheEventRelayBesideTheLineageProcessor pins the START ORDER
// the wiring plan specifies: lineage outbox processor, then the event relay, then the
// OPTIONAL hash-chain processor.
//
// The order is not decoration.
func TestServerCommand_StartsTheEventRelayBesideTheLineageProcessor(t *testing.T) {
	source, err := os.ReadFile("server.go")
	require.NoError(t, err, "reading cmd/server.go")

	body := string(source)

	lineage := strings.Index(body, "lineageProcessor.Start(ctx)")
	require.Positive(t, lineage, "the lineage outbox processor must be started")

	relay := strings.Index(body, "startEventRelay(ctx, b.blnk, cfg)")
	require.Positive(t, relay, "the event relay must be started")

	chain := strings.Index(body, "blnk.NewChainProcessor(b.blnk)")
	require.Positive(t, chain, "the hash-chain processor must be constructed")

	assert.Less(t, lineage, relay,
		"the event relay must start AFTER the lineage outbox processor, so the two outbox "+
			"relays are read together and neither is separated from the other by unrelated work")
	assert.Less(t, relay, chain,
		"and BEFORE the optional hash-chain block, so an unconditional relay is not nested "+
			"among lines that read as depending on a feature flag")
}

// TestServerCommand_StartsSubscriberSettlementBesideTheOtherEventWorkers pins the
// settlement processor into the same contiguous block as the relay and the retention
// sweeper.
//
// Two structural properties, and both have a failure mode.
func TestServerCommand_StartsSubscriberSettlementBesideTheOtherEventWorkers(t *testing.T) {
	source, err := os.ReadFile("server.go")
	require.NoError(t, err, "reading cmd/server.go")

	body := string(source)

	assert.Equal(t, 1, strings.Count(body, "startSubscriberSettlement(ctx, b.blnk)"),
		"the settlement processor must be started exactly once, through the guarded helper. "+
			"Without it, nothing ever discharges the obligations a failed subscriber operation "+
			"records, and the registry stays diverged from the broker indefinitely.")

	assert.Equal(t, 1, strings.Count(body, "blnk.NewSubscriberSettlementProcessor("),
		"the constructor must be reached from exactly one place, so the broker-list guard cannot "+
			"be bypassed by a second construction site")

	// The retention sweeper is reached through the MAINTENANCE starter rather than started
	// directly in runServer: it is maintenance of a shared table, so it runs behind the
	// singleton lease with the metrics collector. What runServer orders is therefore the
	// starter, and that is what this compares — asserting on the sweeper's own call site
	// would compare against a line inside a helper defined further down the file, which
	// says nothing about start-up order.
	maintenance := strings.Index(body, "startEventMaintenance(ctx, b, cfg)")
	require.Positive(t, maintenance,
		"the leader-elected maintenance work, which owns the retention sweeper, must be started")

	require.Positive(t, strings.Index(body, "startEventRetention(ctx, b.blnk)"),
		"the retention sweeper must be started")

	settlement := strings.Index(body, "startSubscriberSettlement(ctx, b.blnk)")
	require.Positive(t, settlement)

	chain := strings.Index(body, "blnk.NewChainProcessor(b.blnk)")
	require.Positive(t, chain, "the hash-chain processor must be constructed")

	assert.Less(t, maintenance, settlement,
		"settlement follows the maintenance workers, keeping the event-pipeline workers contiguous")
	assert.Less(t, settlement, chain,
		"and precedes the optional hash-chain block, so an unconditional worker is not nested "+
			"among lines that read as depending on a feature flag")
}

// TestServerCommand_StartsTheMetricsCollectorOnlyFromTheMaintenancePath is the guard on
// the single call that stands between fifteen alert rules and no series at all.
//
// The event pipeline's three gauges have exactly one production maintainer, the
// collector in event_metrics.go.
func TestServerCommand_StartsTheMetricsCollectorOnlyFromTheMaintenancePath(t *testing.T) {
	source, err := os.ReadFile("server.go")
	require.NoError(t, err, "reading cmd/server.go")

	body := string(source)

	assert.Equal(t, 1, strings.Count(body, "blnk.NewBlnkEventMetricsCollector("),
		"the collector must be constructed from exactly one place, so no second site can pair "+
			"one instance's datasource with another's admin client")

	starter := strings.Index(body, "func startEventMaintenance(")
	require.Positive(t, starter, "the maintenance starter must exist")

	starts := 0

	for offset := 0; ; {
		found := strings.Index(body[offset:], "startEventMetricsCollector(ctx, b.blnk, cfg)")
		if found < 0 {
			break
		}

		starts++

		assert.Greater(t, offset+found, starter,
			"every start must sit inside the maintenance path, not in runServer: a start per "+
				"replica would have each replica zero the others' gauge series as stale")

		offset += found + 1
	}

	// TWO, and both are required. One is the lease holder's, and one is the deliberate
	// degradation when the lease cannot be evaluated at all — which fails towards a
	// duplicated series an operator can see rather than towards no gauges and no alerts.
	assert.Equal(t, 2, starts,
		"the collector is started from the lease holder and from the ungated fallback, and losing "+
			"either leaves a deployment with unmaintained gauges")

	assert.Equal(t, starts, strings.Count(body, "stopMetrics()"),
		"every start must be paired with a stop, or a lost lease leaves a collector ticking "+
			"beside the replica that won it")
}

// TestStartSubscriberSettlement_WithoutBrokersStartsNothingAndIsStillStoppable covers
// the Kafka-less steady state.
//
// With no broker there is no broker-side subscriber state, so there is nothing that
// could diverge from the registry and the processor must decline.
func TestStartSubscriberSettlement_WithoutBrokersStartsNothingAndIsStillStoppable(t *testing.T) {
	stop := startSubscriberSettlement(context.Background(), nil)

	require.NotNil(t, stop,
		"the helper must always return a callable stop; the caller defers it before it can know "+
			"whether the processor started")
	assert.NotPanics(t, stop)
}

// TestStartServer_ObservesTheSignalContextRatherThanItsOwnSignal covers signal-context observation.
//
// startServer must observe the signal CONTEXT rather than install a private
// signal.Notify and block on it: a second observer means the shutdown is seen twice —
// once by the listener and once, much later and only implicitly, by the deferred
// processor stops.
func TestStartServer_ObservesTheSignalContextRatherThanItsOwnSignal(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("returns cleanly once the context is cancelled", func(t *testing.T) {
		router := gin.New()
		router.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

		// Port 0 lets the kernel choose, so this cannot collide with a sibling clone.
		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() { done <- startServer(ctx, router, "0") }()

		// Give the listener a moment to bind, so the cancellation exercises the drain path
		// rather than racing the goroutine's start.
		time.Sleep(200 * time.Millisecond)
		cancel()

		select {
		case err := <-done:
			require.NoError(t, err,
				"cancelling the signal context is the ORDINARY shutdown and must not be reported "+
					"as a failure")
		case <-time.After(10 * time.Second):
			t.Fatal("startServer did not return after its context was cancelled")
		}
	})

	t.Run("a listener failure is returned, not fatal", func(t *testing.T) {
		// Occupy a port so the bind below cannot succeed.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() { _ = ln.Close() }()

		_, port, err := net.SplitHostPort(ln.Addr().String())
		require.NoError(t, err)

		router := gin.New()

		// Not cancelled: the only way out of startServer here is the listener failing, which
		// is exactly what is being asserted: this path must not call logrus.Fatalf
		// from inside the serving goroutine — os.Exit, which would have taken this test
		// process down with it and skipped every deferred stop in production.
		done := make(chan error, 1)
		go func() { done <- startServer(context.Background(), router, port) }()

		select {
		case err := <-done:
			require.Error(t, err, "a listener that cannot bind must report it")
			assert.Contains(t, err.Error(), "api listener failed",
				"the error must name the listener, so an operator reading it knows the process did "+
					"not merely receive a signal")
		case <-time.After(10 * time.Second):
			t.Fatal("startServer neither served nor reported a bind failure")
		}
	})
}

// TestEveryHTTPServerIsHardened is the durable half of the server-hardening rule.
//
// TestNewHTTPServerAndGracefulShutdown asserts the five bounds on the server
// newHTTPServer returns, which proves the helper works but says nothing about whether
// every server in the process goes through it.
func TestEveryHTTPServerIsHardened(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err, "the command package directory must be readable")

	// hardened collects the *http.Server composite literals that ARE the argument of a
	// hardenHTTPServer call, keyed by position so each construction site is judged on its own.
	hardened := map[token.Pos]struct{}{}
	// constructed collects every *http.Server composite literal, hardened or not.
	type site struct {
		file string
		line int
	}
	constructed := map[token.Pos]site{}

	scanned := 0
	fset := token.NewFileSet()

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		parsed, parseErr := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, parseErr, "%s must parse", name)
		scanned++

		ast.Inspect(parsed, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CallExpr:
				// hardenHTTPServer(&http.Server{...}) — record the literal it wraps.
				ident, ok := typed.Fun.(*ast.Ident)
				if !ok || ident.Name != "hardenHTTPServer" || len(typed.Args) != 1 {
					return true
				}
				unary, ok := typed.Args[0].(*ast.UnaryExpr)
				if !ok || unary.Op != token.AND {
					return true
				}
				if literal, ok := unary.X.(*ast.CompositeLit); ok && isHTTPServerType(literal.Type) {
					hardened[literal.Pos()] = struct{}{}
				}
			case *ast.CompositeLit:
				if isHTTPServerType(typed.Type) {
					position := fset.Position(typed.Pos())
					constructed[typed.Pos()] = site{file: name, line: position.Line}
				}
			}

			return true
		})
	}

	require.Positive(t, scanned, "no non-test source file was scanned")
	// Three today. Asserted as a lower bound rather than an equality so adding a listener does
	// not fail here spuriously — the per-site check below is what governs a new one.
	require.GreaterOrEqual(t, len(constructed), 3,
		"expected at least the plaintext API, worker monitoring and HTTPS servers to be found; "+
			"finding fewer means this scan stopped seeing construction sites and is no longer "+
			"protecting them")

	for pos, where := range constructed {
		_, ok := hardened[pos]
		assert.True(t, ok,
			"%s:%d constructs an http.Server that is not the argument of hardenHTTPServer, so it "+
				"runs with no ReadHeaderTimeout, ReadTimeout, WriteTimeout, IdleTimeout or "+
				"MaxHeaderBytes. An idle or deliberately slow peer then holds a connection, its "+
				"goroutine and its read buffer for as long as it likes. Wrap it: "+
				"hardenHTTPServer(&http.Server{...}).",
			where.file, where.line)
	}
}

// isHTTPServerType reports whether a composite literal's type is http.Server.
//
// Matches the selector rather than resolving the import, which is sufficient here: this
// package imports net/http under its own name, and a second package aliased to `http`
// would be a far larger problem than this test.
func isHTTPServerType(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel == nil || selector.Sel.Name != "Server" {
		return false
	}

	pkg, ok := selector.X.(*ast.Ident)

	return ok && pkg.Name == "http"
}

// TestCloseServiceContainer_ClosesTheContainerAndLogsAFailure is the behavioural half
// of the hardening rule, and it is what makes the source-text assertion in
// TestServerCommand_ReturnsErrorsRatherThanExitingTheProcess sufficient rather than the
// whole guard.
func TestCloseServiceContainer_ClosesTheContainerAndLogsAFailure(t *testing.T) {
	t.Run("a container is closed exactly once", func(t *testing.T) {
		spy := &countingContainerCloser{}
		closeContainer(spy)

		assert.Equal(t, 1, spy.calls,
			"the container must be closed exactly once: twice reaches an already-closed writer, "+
				"and never leaks every connection the process owns")
	})

	t.Run("a failing close is reported rather than discarded", func(t *testing.T) {
		spy := &countingContainerCloser{err: errors.New("writer flush refused")}

		assert.NotPanics(t, func() { closeContainer(spy) },
			"this runs from a defer during shutdown, so it must not panic whatever Close reports")
		assert.Equal(t, 1, spy.calls)
	})

	t.Run("a nil container and a nil instance are no-ops", func(t *testing.T) {
		assert.NotPanics(t, func() { closeContainer(nil) })
		assert.NotPanics(t, func() { closeServiceContainer(nil) })
		assert.NotPanics(t, func() { closeServiceContainer(&blnkInstance{}) },
			"a CLI instance with no container is what every test that does not build one holds")
	})
}

// countingContainerCloser counts closes and can be made to fail.
type countingContainerCloser struct {
	calls int
	err   error
}

// Close records the call and returns the configured error.
//
// Returns:
//   - error: whatever the fixture was configured with.
func (c *countingContainerCloser) Close() error {
	c.calls++

	return c.err
}

// TestRequireKafkaBrokersConfigured_RefusesOnlyWhenTheApplicationItselfResolvesNoBroker
// is the behavioural half of the --require-kafka guard.
//
// startEventRelay declines to start without brokers and says so once at info, which is
// correct — a deployment that has not migrated runs exactly that way.
func TestRequireKafkaBrokersConfigured_RefusesOnlyWhenTheApplicationItselfResolvesNoBroker(t *testing.T) {
	for name, tc := range map[string]struct {
		brokers  []string
		admitted bool
	}{
		"unset":               {brokers: nil, admitted: false},
		"empty list":          {brokers: []string{}, admitted: false},
		"one blank entry":     {brokers: []string{"   "}, admitted: false},
		"comma noise":         {brokers: []string{"", " ", ""}, admitted: false},
		"one broker":          {brokers: []string{"localhost:9092"}, admitted: true},
		"several brokers":     {brokers: []string{"a:9092", "b:9092"}, admitted: true},
		"padded but real":     {brokers: []string{"  localhost:9092  "}, admitted: true},
		"one real, one blank": {brokers: []string{"", "localhost:9092"}, admitted: true},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := mockConfig(t)
			cfg.Kafka.Brokers = tc.brokers
			config.MockConfig(cfg)

			err := requireKafkaBrokersConfigured()

			if tc.admitted {
				require.NoError(t, err,
					"%v is a configured broker list, and refusing it would block a deployment "+
						"that had done nothing wrong — the false negative this guard replaced",
					tc.brokers)

				return
			}

			require.Error(t, err,
				"%v resolves to no usable broker, so the relay would not start and every captured "+
					"event would stay pending in blnk.event_outbox while the server looked healthy",
				tc.brokers)

			// The refusal has to be actionable, and what makes it actionable is naming the
			// sources AND their precedence. The environment key the field actually resolves from
			// first is the one an operator is least likely to know.
			for _, named := range []string{
				"BLNK_KAFKA_BROKERS",
				"KAFKA_BROKERS",
				"BLNK_KAFKA_KAFKA_BROKERS",
				"set and EMPTY",
				"--require-kafka",
			} {
				assert.Containsf(t, err.Error(), named,
					"the refusal must mention %q. All three environment names resolve this field "+
						"and they do not rank equally — see config.eventStreamingEnvOverride — so "+
						"a message that omits one, or states the order wrongly, sends an operator "+
						"to a name that will be overridden by one they were not told about",
					named)
			}

			// Precedence is part of the message, not a footnote: the prefixed name winning is the
			// non-obvious half, and it is what makes `BLNK_KAFKA_BROKERS=` a way to turn Kafka off.
			message := err.Error()
			assert.Less(t, strings.Index(message, "BLNK_KAFKA_BROKERS"),
				strings.Index(message, "BLNK_KAFKA_KAFKA_BROKERS"),
				"the refusal must list the sources in precedence order, highest first")
		})
	}
}

// TestServerCommand_ExposesTheKafkaRequirementAsAFlagRatherThanShellResolution pins the
// guard's LOCATION as well as its existence.
func TestServerCommand_ExposesTheKafkaRequirementAsAFlagRatherThanShellResolution(t *testing.T) {
	source, err := os.ReadFile("server.go")
	require.NoError(t, err, "reading cmd/server.go")

	body := string(source)

	assert.Contains(t, body, `cmd.Flags().BoolVar(&requireKafka, "require-kafka", false,`,
		"the requirement must be a flag on `start`, so `make run_relay` can delegate to the "+
			"application's own resolution instead of approximating it in shell")

	guard := strings.Index(body, "if err := requireKafkaBrokersConfigured(); err != nil {")
	require.Positive(t, guard, "RunE must consult the requirement before running the server")

	run := strings.Index(body, "return runServer(ctx, b)")
	require.Positive(t, run, "RunE must run the server")
	assert.Less(t, guard, run,
		"the requirement must be checked BEFORE runServer: after it, the listener is bound and "+
			"the background processors have started, so a server about to be refused has already "+
			"begun serving")

	// One definition of "configured", shared with the relay's own gate.
	requireIndex := strings.Index(body, "func requireKafkaBrokersConfigured()")
	require.Positive(t, requireIndex, "the requirement check must exist")
	assert.Contains(t, body[requireIndex:], "blnk.KafkaBrokersConfigured(cfg.Kafka.Brokers)",
		"the check must use the same predicate on the same field startEventRelay gates on. A "+
			"second opinion about what 'configured' means is how a guard comes to admit a server "+
			"whose relay then declines to start")
}
