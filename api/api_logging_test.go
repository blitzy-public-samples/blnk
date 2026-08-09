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

package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/logsafe"
)

// This file covers what the HTTP layer WRITES ABOUT A REQUEST rather than what it answers:
// the access log line, the recovery line, and the proxy-trust decision both of them depend
// on.
//
// The three failure modes it guards are all invisible from the response, which is why they
// need their own file:
//
//   - A log line that carries the request PATH carries the ledger, transaction and identity
//     identifiers in it, to a sink that is retained, shipped onward and read by more people
//     than the API itself.
//   - A client address taken from a header any caller can set makes the log a record of what
//     the caller CLAIMED, while looking exactly like a record of what happened.
//   - A recovery line that prints the panic verbatim republishes whatever the panicking code
//     was holding — a broker address, a connection string, an internal file path.
//
// Every assertion checks the RAW rendering as well as the decoded field, because a value that
// moved out of a field and into the message text would still satisfy a field assertion while
// disclosing exactly as much.

// captureAPILogs redirects the standard logger for the duration of fn and returns the decoded
// entries alongside the raw bytes.
//
// The standard logger is process-global, so the previous output, formatter and level are
// restored through t.Cleanup rather than a defer: a require failure inside fn aborts the
// goroutine, and a deferred restore that never ran would leave every later test in the
// package logging into a dead buffer.
//
// Parameters:
//   - t *testing.T: the test, for cleanup registration and decode failures.
//   - level logrus.Level: the level to capture at.
//   - fn func(): the code whose logging is being observed.
//
// Returns:
//   - []map[string]interface{}: the decoded entries.
//   - string: the raw rendering, for absence assertions.
func captureAPILogs(
	t *testing.T,
	level logrus.Level,
	fn func(),
) ([]map[string]interface{}, string) {
	t.Helper()

	logger := logrus.StandardLogger()
	previousOut := logger.Out
	previousFormatter := logger.Formatter
	previousLevel := logger.GetLevel()

	t.Cleanup(func() {
		logger.SetOutput(previousOut)
		logger.SetFormatter(previousFormatter)
		logger.SetLevel(previousLevel)
	})

	var buf bytes.Buffer

	logger.SetOutput(&buf)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(level)

	fn()

	written := buf.Bytes()

	var entries []map[string]interface{}

	decoder := json.NewDecoder(bytes.NewReader(written))
	for decoder.More() {
		entry := map[string]interface{}{}
		require.NoError(t, decoder.Decode(&entry), "the captured log must be decodable JSON")
		entries = append(entries, entry)
	}

	return entries, string(written)
}

// firstEntryWithMessage returns the first captured entry whose msg equals message.
//
// Parameters:
//   - t *testing.T: the test, for the not-found failure.
//   - entries []map[string]interface{}: the captured entries.
//   - message string: the exact message to look for.
//
// Returns:
//   - map[string]interface{}: the matching entry.
func firstEntryWithMessage(
	t *testing.T,
	entries []map[string]interface{},
	message string,
) map[string]interface{} {
	t.Helper()

	for _, entry := range entries {
		if msg, _ := entry["msg"].(string); msg == message {
			return entry
		}
	}

	require.FailNowf(t, "log entry not found", "no captured entry with msg %q in %v", message, entries)

	return nil
}

// TestLogrusAccessLogger_LogsRouteTemplateNotIdentifiers is the core of the finding: the line
// says which endpoint was called without saying which record it was called about.
func TestLogrusAccessLogger_LogsRouteTemplateNotIdentifiers(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(logrusAccessLogger())
	router.GET("/transactions/:transaction_id", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	entries, raw := captureAPILogs(t, logrus.InfoLevel, func() {
		req := httptest.NewRequest(http.MethodGet, "/transactions/txn_9f2b1c4e0a", nil)
		router.ServeHTTP(httptest.NewRecorder(), req)
	})

	entry := firstEntryWithMessage(t, entries, "http request")

	assert.Equal(t, "/transactions/:transaction_id", entry["route"])
	assert.NotContains(t, raw, "txn_9f2b1c4e0a",
		"the access log must not carry the identifier from the request path")
	assert.Nil(t, entry["path"],
		"the raw path field is what carried identifiers; it must be gone, not merely shortened")
}

// TestLogrusAccessLoggerDropsQueryString keeps the property the earlier revision of this file
// established — a query string never reaches the log — now that the field is the route
// template. A token in a query parameter is a credential, and a credential in a log is a
// credential in every system the log is shipped to.
func TestLogrusAccessLoggerDropsQueryString(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(logrusAccessLogger())
	router.GET("/transactions", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	entries, raw := captureAPILogs(t, logrus.InfoLevel, func() {
		req := httptest.NewRequest(http.MethodGet, "/transactions?token=secret&limit=20", nil)
		router.ServeHTTP(httptest.NewRecorder(), req)
	})

	entry := firstEntryWithMessage(t, entries, "http request")

	assert.Equal(t, "/transactions", entry["route"])
	assert.NotContains(t, raw, "token=secret", "the access log must not carry the query string")
}

// TestLogrusAccessLogger_UnmatchedRouteIsNotEchoed proves a 404's path — which is entirely
// attacker-chosen, and is where scanners put their probes — is reported as a fixed word
// instead of being written back into the log.
func TestLogrusAccessLogger_UnmatchedRouteIsNotEchoed(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(logrusAccessLogger())

	entries, raw := captureAPILogs(t, logrus.InfoLevel, func() {
		req := httptest.NewRequest(http.MethodGet, "/.env-or-some-probe", nil)
		router.ServeHTTP(httptest.NewRecorder(), req)
	})

	entry := firstEntryWithMessage(t, entries, "http request")

	assert.Equal(t, unmatchedRoute, entry["route"])
	assert.NotContains(t, raw, ".env-or-some-probe",
		"an unmatched path is caller-controlled and must not be echoed into the log")
	assert.Equal(t, float64(http.StatusNotFound), entry["status"],
		"the status is what tells an operator the request found nothing")
}

// TestLogrusAccessLogger_ClientIPIsNotForgeableWithoutTrustedProxies is the CWE-345 half of
// the finding. With no proxy trusted, a forwarded header is ignored and the address logged is
// the peer that actually opened the connection.
func TestLogrusAccessLogger_ClientIPIsNotForgeableWithoutTrustedProxies(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	require.NoError(t, router.SetTrustedProxies(nil))
	router.Use(logrusAccessLogger())
	router.GET("/transactions", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	entries, raw := captureAPILogs(t, logrus.InfoLevel, func() {
		req := httptest.NewRequest(http.MethodGet, "/transactions", nil)
		req.RemoteAddr = "192.0.2.9:51234"
		req.Header.Set("X-Forwarded-For", "203.0.113.77")
		req.Header.Set("X-Real-Ip", "203.0.113.78")
		router.ServeHTTP(httptest.NewRecorder(), req)
	})

	entry := firstEntryWithMessage(t, entries, "http request")

	assert.Equal(t, "192.0.2.9", entry["client_ip"],
		"the logged address must be the connection's own peer, not a header the caller set")
	assert.NotContains(t, raw, "203.0.113.77", "a forged forwarded address must not reach the log")
	assert.NotContains(t, raw, "203.0.113.78", "a forged real-ip address must not reach the log")
}

// TestLogrusAccessLogger_ClientIPHonoursAConfiguredTrustedProxy proves the fix did not simply
// disable forwarded headers for everybody. An operator who names their own proxy still gets
// the real client address through it, which is why the setting exists rather than being
// hard-coded off.
func TestLogrusAccessLogger_ClientIPHonoursAConfiguredTrustedProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	require.NoError(t, router.SetTrustedProxies([]string{"192.0.2.9/32"}))
	router.Use(logrusAccessLogger())
	router.GET("/transactions", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	entries, _ := captureAPILogs(t, logrus.InfoLevel, func() {
		req := httptest.NewRequest(http.MethodGet, "/transactions", nil)
		req.RemoteAddr = "192.0.2.9:51234"
		req.Header.Set("X-Forwarded-For", "203.0.113.77")
		router.ServeHTTP(httptest.NewRecorder(), req)
	})

	entry := firstEntryWithMessage(t, entries, "http request")

	assert.Equal(t, "203.0.113.77", entry["client_ip"],
		"a forwarded address from a proxy the operator named must be believed")
}

// TestLogrusRecovery_RedactsPanicDetailAtNormalLevel proves a panic carrying a broker address
// does not republish it, while still leaving enough on the line to recognise the panic.
func TestLogrusRecovery_RedactsPanicDetailAtNormalLevel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	require.NoError(t, router.SetTrustedProxies(nil))
	router.Use(logrusRecovery())
	router.GET("/events/stats", func(_ *gin.Context) {
		panic(errors.New("dial tcp 10.0.3.14:9092: connect: connection refused"))
	})

	recorder := httptest.NewRecorder()

	entries, raw := captureAPILogs(t, logrus.InfoLevel, func() {
		req := httptest.NewRequest(http.MethodGet, "/events/stats", nil)
		req.RemoteAddr = "192.0.2.9:51234"
		router.ServeHTTP(recorder, req)
	})

	entry := firstEntryWithMessage(t, entries, "panic recovered")

	assert.Equal(t, "/events/stats", entry["route"])
	assert.Equal(t, "*errors.errorString", entry["panic_type"],
		"the panic's type is what distinguishes one recurring panic from another")
	assert.Contains(t, entry["panic"], "connection refused",
		"the diagnosis must survive, or the line answers nothing")
	assert.Contains(t, entry["panic"], logsafe.Placeholder)
	assert.NotContains(t, raw, "10.0.3.14", "the broker address must not reach the log")
	assert.Nil(t, entry["panic_verbatim"],
		"the unredacted value belongs only in the debug sink")
	assert.Nil(t, entry["stack"],
		"the stack names internal packages and file paths and belongs only in the debug sink")

	assert.Equal(t, http.StatusInternalServerError, recorder.Code)
	assert.Empty(t, recorder.Body.String(),
		"the panic has never reached the caller and must not start doing so")
}

// TestLogrusRecovery_EmitsVerbatimAndStackOnlyAtDebug proves the escape hatch is real: an
// operator who turns debug on gets the whole panic and the stack, which is what makes
// redacting the default acceptable rather than merely quieter.
func TestLogrusRecovery_EmitsVerbatimAndStackOnlyAtDebug(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(logrusRecovery())
	router.GET("/events/stats", func(_ *gin.Context) {
		panic(errors.New("dial tcp 10.0.3.14:9092: connect: connection refused"))
	})

	entries, _ := captureAPILogs(t, logrus.DebugLevel, func() {
		req := httptest.NewRequest(http.MethodGet, "/events/stats", nil)
		router.ServeHTTP(httptest.NewRecorder(), req)
	})

	entry := firstEntryWithMessage(t, entries, "panic recovered")

	require.NotNil(t, entry["panic_verbatim"])
	assert.Contains(t, entry["panic_verbatim"], "10.0.3.14:9092",
		"debug is the sink for the detail an operator has explicitly asked for")

	stack, hasStack := entry["stack"].(string)
	require.True(t, hasStack, "a stack must be available at debug")
	assert.Contains(t, stack, "goroutine")
}

// TestLogrusRecovery_HandlesANonErrorPanic guards the other panic shape. A bare string panic
// is common — a nil-map write, an index out of range — and rendering it must not depend on the
// value implementing error.
func TestLogrusRecovery_HandlesANonErrorPanic(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(logrusRecovery())
	router.GET("/events/stats", func(_ *gin.Context) {
		panic("failed reaching kafka-0.internal:9092 while building the report")
	})

	entries, raw := captureAPILogs(t, logrus.InfoLevel, func() {
		req := httptest.NewRequest(http.MethodGet, "/events/stats", nil)
		router.ServeHTTP(httptest.NewRecorder(), req)
	})

	entry := firstEntryWithMessage(t, entries, "panic recovered")

	assert.Equal(t, "string", entry["panic_type"])
	assert.Contains(t, entry["panic"], "while building the report")
	assert.NotContains(t, raw, "kafka-0.internal:9092",
		"an endpoint in a string panic must be redacted exactly as one in an error is")
}

// TestLogrusRecovery_DoesNotDumpRequestHeaders is the assertion that gin's own recovery
// writer stays off.
//
// That writer prints the panic, the stack and a dump of the request headers to os.Stderr,
// masking Authorization and nothing else — so Blnk's X-Blnk-Key would be published in full by
// any panic on an authenticated request. This points gin's error writer at a buffer and
// requires that nothing reaches it.
func TestLogrusRecovery_DoesNotDumpRequestHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	previousErrorWriter := gin.DefaultErrorWriter

	t.Cleanup(func() { gin.DefaultErrorWriter = previousErrorWriter })

	var ginWriter bytes.Buffer

	gin.DefaultErrorWriter = &ginWriter

	router := gin.New()
	router.Use(logrusRecovery())
	router.GET("/events/stats", func(_ *gin.Context) {
		panic(errors.New("something went wrong"))
	})

	_, raw := captureAPILogs(t, logrus.InfoLevel, func() {
		req := httptest.NewRequest(http.MethodGet, "/events/stats", nil)
		req.Header.Set("X-Blnk-Key", "sk_live_master_key_value")
		req.Header.Set("Authorization", "Bearer some-token")
		router.ServeHTTP(httptest.NewRecorder(), req)
	})

	assert.Empty(t, ginWriter.String(),
		"gin's own recovery writer must be disabled; it dumps the stack and the request headers")
	assert.NotContains(t, ginWriter.String(), "sk_live_master_key_value")
	assert.NotContains(t, raw, "sk_live_master_key_value",
		"the caller's API key must never appear in a recovery line")
}

// TestNewAPI_RefusesToStartOnMalformedTrustedProxies proves the setting fails closed. Ignoring
// a malformed value would leave the engine on its trust-everything default while the
// operator's configuration said otherwise — configured-looking and forgeable.
//
// It builds the instance directly rather than through setupRouterWithConfig, because the
// expected result is a nil *Api and that helper calls Router() on whatever NewAPI returns.
func TestNewAPI_RefusesToStartOnMalformedTrustedProxies(t *testing.T) {
	gin.SetMode(gin.TestMode)

	instance := newAPIWithTrustedProxies(t, "not-a-cidr")

	assert.Nil(t, instance,
		"a malformed proxy list must stop the API rather than fall back to trusting everything")
}

// TestNewAPI_AcceptsAConfiguredProxyList is the companion that keeps the previous test honest:
// the nil result must be caused by the malformed value, not by anything else in the fixture.
func TestNewAPI_AcceptsAConfiguredProxyList(t *testing.T) {
	gin.SetMode(gin.TestMode)

	instance := newAPIWithTrustedProxies(t, "10.0.0.0/8, 192.0.2.9")

	assert.NotNil(t, instance, "a well-formed proxy list must be accepted")
}

// newAPIWithTrustedProxies builds an Api instance with a given BLNK_SERVER_TRUSTED_PROXIES
// value, returning whatever NewAPI returned — nil included.
//
// Parameters:
//   - t *testing.T: the test, for fixture failures.
//   - proxies string: the raw configuration value.
//
// Returns:
//   - *Api: the constructed instance, or nil when NewAPI refused.
func newAPIWithTrustedProxies(t *testing.T, proxies string) *Api {
	t.Helper()

	cfg := &config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_api_trusted_proxies",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	}
	cfg.Server.TrustedProxies = proxies

	config.MockConfig(cfg)

	cnf, err := config.Fetch()
	require.NoError(t, err, "failed to fetch config")

	db, err := database.NewDataSource(cnf)
	require.NoError(t, err, "failed to create datasource")

	newBlnk, err := blnk.NewBlnk(db)
	require.NoError(t, err, "failed to create blnk instance")

	return NewAPI(newBlnk)
}

// TestNewAPI_TrustsNoProxyByDefault is the end-to-end counterpart of the middleware test: a
// router built the way a deployment builds it, with the shipped configuration, ignores a
// forged forwarded address.
func TestNewAPI_TrustsNoProxyByDefault(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router, _, _ := setupRouterWithConfig(t, nil)
	require.NotNil(t, router)

	entries, raw := captureAPILogs(t, logrus.InfoLevel, func() {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "192.0.2.9:51234"
		req.Header.Set("X-Forwarded-For", "203.0.113.77")
		router.ServeHTTP(httptest.NewRecorder(), req)
	})

	entry := firstEntryWithMessage(t, entries, "http request")

	assert.Equal(t, "192.0.2.9", entry["client_ip"])
	assert.NotContains(t, raw, "203.0.113.77")
}

// TestEventAPILogging_NoSiteUsesLogrusWithError is a source-level guard over the files this
// feature owns.
//
// The behavioural tests above prove the sites that exist behave; this proves none was MISSED
// and that a new one cannot quietly reintroduce the defect. logrus.WithError renders
// err.Error() verbatim into the record at whatever level the line is emitted at, and the
// errors these files log are Kafka and database errors whose text names broker addresses and
// connection strings.
//
// The list is deliberately limited to the files this change owns. api/transactions.go,
// api/errors.go and api/reconciliation_api.go log the same way and are outside this feature's
// scope; converting them is a separate, self-contained change.
func TestEventAPILogging_NoSiteUsesLogrusWithError(t *testing.T) {
	files := []string{
		"api.go",
		"events.go",
		"subscribers.go",
		"middleware/sunset.go",
		"middleware/auth.go",
		"middleware/scope.go",
	}

	for _, name := range files {
		t.Run(name, func(t *testing.T) {
			contents, err := os.ReadFile(name)
			require.NoErrorf(t, err, "%s must be readable", name)

			var offending []string

			for number, line := range strings.Split(string(contents), "\n") {
				if strings.Contains(line, "WithError(") {
					offending = append(offending,
						fmt.Sprintf("%s:%d: %s", name, number+1, strings.TrimSpace(line)))
				}
			}

			assert.Emptyf(t, offending,
				"these sites must log through withLoggableCause or logsafe.Cause, which redact "+
					"network topology and bound the text, rather than through logrus.WithError:\n%s",
				strings.Join(offending, "\n"))
		})
	}
}

// TestWithLoggableCause_RedactsAtNormalLevelAndKeepsVerbatimAtDebug pins the helper every
// handler in this package logs an error through, so a future handler that uses it inherits
// both halves of the contract.
func TestWithLoggableCause_RedactsAtNormalLevelAndKeepsVerbatimAtDebug(t *testing.T) {
	transport := errors.New("dial tcp 10.0.3.14:9092: connect: connection refused")

	t.Run("normal level redacts and omits the verbatim field", func(t *testing.T) {
		entries, raw := captureAPILogs(t, logrus.InfoLevel, func() {
			withLoggableCause(nil, transport).Warn("kafka read failed")
		})

		entry := firstEntryWithMessage(t, entries, "kafka read failed")

		assert.Contains(t, entry["cause"], "connection refused")
		assert.Nil(t, entry["cause_verbatim"])
		assert.Nil(t, entry["error"],
			"WithError's field is what carried the unredacted text and must not reappear")
		assert.NotContains(t, raw, "10.0.3.14")
	})

	t.Run("debug level adds the verbatim field", func(t *testing.T) {
		entries, _ := captureAPILogs(t, logrus.DebugLevel, func() {
			withLoggableCause(nil, transport).Warn("kafka read failed")
		})

		entry := firstEntryWithMessage(t, entries, "kafka read failed")

		assert.Contains(t, entry["cause_verbatim"], "10.0.3.14:9092")
	})

	t.Run("a nil error leaves the entry untouched", func(t *testing.T) {
		entries, _ := captureAPILogs(t, logrus.InfoLevel, func() {
			withLoggableCause(logrus.WithField("subject", "kept"), nil).Info("nothing failed")
		})

		entry := firstEntryWithMessage(t, entries, "nothing failed")

		assert.Equal(t, "kept", entry["subject"])
		assert.Nil(t, entry["cause"])
	})
}
