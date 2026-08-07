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
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/gin-gonic/gin"
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

	quit := make(chan os.Signal, 1)
	done := make(chan error, 1)
	go func() { done <- gracefulShutdown(server, quit, 5*time.Second) }()

	quit <- os.Interrupt

	select {
	case err := <-done:
		require.NoError(t, err, "graceful shutdown must complete cleanly")
	case <-time.After(10 * time.Second):
		t.Fatal("gracefulShutdown did not return after the quit signal")
	}

	// After shutdown the server must refuse new connections.
	_, err = http.Get("http://" + addr + "/ping")
	require.Error(t, err, "server must not accept requests after shutdown")
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

// stopperSymbol names the function a lifecycle starter handed back, resolved from the runtime's
// own symbol table.
//
// It is what lets these tests tell "a relay was constructed and its Stop was returned" from "a
// local no-op was returned", which is the ONLY externally visible difference between the two
// branches of startEventRelay: both return a callable func(), and neither exposes the processor.
// Asserting on the symbol is a statement about the value the caller will defer, not about the
// source text — a rename of Stop, or a branch that stopped handing the relay's own stopper back,
// each fail here with the name that was actually returned.
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
// A deployment with no KAFKA_BROKERS is a legitimate steady state, not a misconfiguration: its
// producers deliver over the legacy webhook transport directly, so there is nothing for a relay
// to drain. Starting one anyway would be actively harmful — the publisher would be the no-op
// implementation, which reports every publish as dispatched without sending anything.
//
// The blank and comma-only broker lists are here because they are what a half-filled
// environment variable actually looks like. `KAFKA_BROKERS=","` must mean "no brokers", and a
// starter that tested only for a nil slice would treat it as configured.
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
			stop := startEventRelay(context.Background(), nil, cfg)

			assert.NotContains(t, stopperSymbol(t, stop), "EventRelayProcessor",
				"no relay may be constructed without brokers: its publisher would be the no-op one, "+
					"which reports every publish as dispatched while sending nothing")

			stop()
		})
	}

	t.Run("a nil configuration", func(t *testing.T) {
		stop := startEventRelay(context.Background(), nil, nil)

		assert.NotContains(t, stopperSymbol(t, stop), "EventRelayProcessor",
			"an unloaded configuration must not be read as a configured broker list")

		stop()
	})
}

// TestStartEventRelay_WithBrokersConstructsTheRelayAndReturnsItsStop is the wiring assertion
// itself, and the defect it exists for was total.
//
// Every producer captures its event into blnk.event_outbox inside the ledger transaction, and
// the relay is the only thing that publishes those rows. It was constructible, fully unit-tested
// and NEVER CONSTRUCTED BY A RUNNING PROCESS: a deployment with KAFKA_BROKERS set accumulated
// outbox rows indefinitely and delivered nothing, with no error anywhere, because capturing each
// row had succeeded. Nothing in the suite failed, because nothing asserted that production code
// starts it.
//
// The broker is deliberately unreachable, which also exercises the documented decision that a
// topic-assurance failure is logged and stepped past rather than fatal: assurance against a
// broker that is not listening yet is a transient condition, and refusing to start would convert
// it into an outage that only a restart could clear.
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
	stop := startEventRelay(context.Background(), nil, cfg)

	assert.Contains(t, stopperSymbol(t, stop), "EventRelayProcessor",
		"with brokers configured the relay must be constructed and ITS stop returned, so the server's "+
			"deferred call really shuts the relay down; a local no-op here means nothing drains the outbox")

	stop()

	assert.Less(t, time.Since(started), eventTopicAssuranceTimeout,
		"an unreachable broker must not hold start-up for the whole assurance budget: it is refused, "+
			"logged and stepped past")
}
