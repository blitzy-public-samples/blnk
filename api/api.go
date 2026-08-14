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
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/typesense/typesense-go/typesense/api"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/middleware"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/logsafe"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// Api represents the API structure for handling requests.
type Api struct {
	blnk   *blnk.Blnk
	router *gin.Engine
	auth   *middleware.AuthMiddleware
}

// Router sets up the routes for the API and returns the router instance.
func (a Api) Router() *gin.Engine {
	router := a.router

	// The webhook retirement guard is installed in NewAPI, ahead of authentication, rather
	// than on this router. A guard added here would sit behind Authenticate below and so
	// could never see the two request shapes the retirement has to answer: an unsupported
	// verb on a retired path, and a caller whose credential has lapsed. Both must be 410
	// rather than 405 or 401, because the surface itself is gone.

	// Apply auth middleware to all routes
	router.Use(a.auth.Authenticate())

	// Ledger routes
	router.POST("/ledgers", a.CreateLedger)
	router.GET("/ledgers/:id", a.GetLedger)
	router.GET("/ledgers", a.GetAllLedgers)
	router.POST("/ledgers/filter", a.FilterLedgers)
	router.PUT("/ledgers/:id", a.UpdateLedger)

	// Balance routes
	router.POST("/balances", a.CreateBalance)
	router.GET("/balances", a.GetBalances)
	router.POST("/balances/filter", a.FilterBalances)
	router.GET("/balances/:id", a.GetBalance)
	router.GET("/balances/indicator/:indicator/currency/:currency", a.GetBalanceByIndicator)
	router.GET("/balances/:id/at", a.GetBalanceAtTime)
	router.POST("/balances-snapshots", a.TakeBalanceSnapshots)
	router.PUT("/balances/:id/identity", a.UpdateBalanceIdentity)
	router.GET("/balances/:id/lineage", a.GetBalanceLineage)

	// Balance Monitor routes
	router.POST("/balance-monitors", a.CreateBalanceMonitor)
	router.GET("/balance-monitors/:id", a.GetBalanceMonitor)
	router.GET("/balance-monitors", a.GetAllBalanceMonitors)
	router.GET("/balance-monitors/balances/:balance_id", a.GetBalanceMonitorsByBalanceID)
	router.PUT("/balance-monitors/:id", a.UpdateBalanceMonitor)
	router.DELETE("/balance-monitors/:id", a.DeleteBalanceMonitor)

	// Transaction routes
	router.POST("/transactions", a.QueueTransaction)
	router.POST("/transactions/bulk", a.CreateBulkTransactions)
	router.POST("/transactions/filter", a.FilterTransactions)
	router.POST("/refund-transaction/:id", a.RefundTransaction)
	router.GET("/transactions", a.GetAllTransactions)
	router.GET("/transactions/:id", a.GetTransaction)
	router.GET("/transactions/reference/:reference", a.GetTransactionByRef)
	router.PUT("/transactions/inflight/:txID", a.UpdateInflightStatus)
	router.POST("/transactions/inflight/bulk/void", a.BulkVoidInflight)
	router.POST("/transactions/inflight/bulk/commit", a.BulkCommitInflight)
	router.GET("/transactions/:id/lineage", a.GetTransactionLineage)

	// Recovery routes
	router.POST("/transactions/recover", a.RecoverQueuedTransactions)

	// Identity routes
	router.POST("/identities", a.CreateIdentity)
	router.GET("/identities/:id", a.GetIdentity)
	router.PUT("/identities/:id", a.UpdateIdentity)
	router.GET("/identities", a.GetAllIdentities)
	router.DELETE("/identities/:id", a.DeleteIdentity)
	router.POST("/identities/filter", a.FilterIdentities)
	router.GET("/identities/:id/tokenized-fields", a.GetTokenizedFields)
	router.POST("/identities/:id/tokenize/:field", a.TokenizeIdentityField)
	router.GET("/identities/:id/detokenize/:field", a.DetokenizeIdentityField)
	router.POST("/identities/:id/tokenize", a.TokenizeIdentity)
	router.POST("/identities/:id/detokenize", a.DetokenizeIdentity)

	// Account routes
	router.POST("/accounts", a.CreateAccount)
	router.GET("/accounts/:id", a.GetAccount)
	router.GET("/accounts", a.GetAllAccounts)
	router.POST("/accounts/filter", a.FilterAccounts)

	// Mocked Account route
	router.GET("/mocked-account", a.generateMockAccount)

	// Backup routes
	router.GET("/backup", a.BackupDB)
	router.GET("/backup-s3", a.BackupDBS3)

	// Search routes
	router.POST("/search/:collection", a.Search)
	router.POST("/multi-search", a.MultiSearch)
	// Reindex routes
	router.POST("/search/reindex", a.StartReindex)
	router.GET("/search/reindex", a.GetReindexProgress)

	// Reconciliation routes
	router.POST("/reconciliation/upload", a.UploadExternalData)
	router.POST("/reconciliation/matching-rules", a.CreateMatchingRule)
	router.PUT("/reconciliation/matching-rules/:id", a.UpdateMatchingRule)
	router.DELETE("/reconciliation/matching-rules/:id", a.DeleteMatchingRule)
	router.POST("/reconciliation/start", a.StartReconciliation)
	router.POST("/reconciliation/start-instant", a.InstantReconciliation)
	router.GET("/reconciliation/:id", a.GetReconciliation)

	// Metadata routes
	router.POST("/:entity-id/metadata", a.UpdateMetadata)

	// Hook management routes
	router.POST("/hooks", a.RegisterHook)
	router.PUT("/hooks/:id", a.UpdateHook)
	router.GET("/hooks/:id", a.GetHook)
	router.GET("/hooks", a.ListHooks)
	router.DELETE("/hooks/:id", a.DeleteHook)

	// API Key routes
	router.POST("/api-keys", a.CreateAPIKey)
	router.GET("/api-keys", a.ListAPIKeys)
	router.DELETE("/api-keys/:id", a.RevokeAPIKey)

	// Event streaming routes. Both segments under "/events" are static on
	// purpose: no route parameter is registered directly there, so the
	// dead-letter and stats subtrees can never be shadowed.
	router.GET("/events/dead-letter", a.ListDeadLetterEvents)
	// Replay is the only write in the dead-letter workflow. There is no discard, edit or
	// purge route: a dead-lettered row is evidence of a publish that failed, and the daily
	// outbox-versus-offset reconciliation in docs/kafka-operations.md counts it.
	router.POST("/events/dead-letter/:event_id/replay", a.ReplayDeadLetterEvent)
	router.GET("/events/stats", a.GetEventOutboxStats)

	// Subscriber routes: the registry of Kafka principals and their credentials.
	router.POST("/subscribers", a.CreateSubscriber)
	router.GET("/subscribers", a.ListSubscribers)
	router.GET("/subscribers/:subscriber_id", a.GetSubscriber)
	router.PUT("/subscribers/:subscriber_id", a.UpdateSubscriber)
	router.DELETE("/subscribers/:subscriber_id", a.DeleteSubscriber)
	router.POST("/subscribers/:subscriber_id/kafka-credentials", a.IssueKafkaCredentials)

	// Every /subscribers route above is an OPERATOR action gated on the master key. None of
	// them carries event data: subscribers consume Kafka directly with the per-subscriber
	// SASL/SCRAM credential the last route issues, so no read-the-events endpoint exists
	// here to proxy it.

	// Deprecated webhook-subscription management routes.
	router.POST(middleware.DeprecatedWebhookSubscriptionRoute, middleware.WebhookSunsetGuard(), a.RegisterWebhookSubscription)
	router.GET(middleware.DeprecatedWebhookSubscriptionRoute, middleware.WebhookSunsetGuard(), a.GetWebhookSubscription)
	router.PUT(middleware.DeprecatedWebhookSubscriptionRoute, middleware.WebhookSunsetGuard(), a.UpdateWebhookSubscription)
	router.DELETE(middleware.DeprecatedWebhookSubscriptionRoute, middleware.WebhookSunsetGuard(), a.DeleteWebhookSubscription)

	return a.router
}

// NewAPI creates a new Api instance with the provided Blnk service and sets up the router.
//
// Parameters:
// - b: The Blnk service used to interact with business logic.
//
// Returns:
// - *Api: A new instance of the Api with the configured router.
func NewAPI(b *blnk.Blnk) *Api {
	gin.SetMode(gin.ReleaseMode)
	conf, err := config.Fetch()
	if err != nil {
		return nil
	}
	r := gin.New()

	r.MaxMultipartMemory = 8 << 20 // 8 MiB

	// PROXY TRUST, decided before any middleware runs, because everything that reports a
	// client address depends on it.
	if err := r.SetTrustedProxies(conf.TrustedProxyCIDRs()); err != nil {
		logrus.WithField("cause", logsafe.Cause(err)).Error(
			"BLNK_SERVER_TRUSTED_PROXIES is not a valid list of CIDR blocks or IP literals, so the " +
				"API was not started. Correct it, or leave it unset to trust no proxy and take the " +
				"client address from the connection itself",
		)

		return nil
	}

	r.Use(logrusAccessLogger())
	r.Use(logrusRecovery())

	// SecurityHeaders only SETS response headers and never aborts, so it precedes every
	// middleware that can: an aborted response carries the same headers as a served one.
	r.Use(middleware.SecurityHeaders())

	// The retired-path barrier. It must precede every middleware that can abort, or a
	// request to a retired path would be answered by whichever of them aborted first — 401
	// from authentication, 413 from the size limit, 429 from the rate limiter — instead of
	// the 410 the surface's retirement requires.
	r.Use(middleware.WebhookSunsetPreAuthGuard())

	r.Use(middleware.RequestSizeLimit(conf.Server.MaxRequestBodySizeMB * 1024 * 1024))
	auth := middleware.NewAuthMiddleware(b)
	r.Use(middleware.RateLimitMiddleware(conf))
	// The server span every request trace is rooted in, and the point from which the trace
	// reaches the event pipeline: handlers pass c.Request.Context() into the service
	// layer, the event capture writes the active trace context onto the outbox row, and
	// the relay's publish links back to it. That chain starts here.
	r.Use(otelgin.Middleware("BLNK",
		otelgin.WithFilter(func(r *http.Request) bool {
			// Exclude high-frequency operational endpoints from tracing to avoid polluting the
			// trace feed with noise.
			return r.URL.Path != "/metrics"
		}),
	))

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, "server running...")
	})

	return &Api{blnk: b, router: r, auth: auth}
}

// logrusAccessLogger logs one line per request, identifying it by ROUTE TEMPLATE rather
// than by the path that was requested.
func logrusAccessLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		method := c.Request.Method

		c.Next()

		logrus.WithFields(logrus.Fields{
			"method":       method,
			"route":        requestRoute(c),
			"status":       c.Writer.Status(),
			"latency_ms":   time.Since(start).Milliseconds(),
			"client_ip":    c.ClientIP(),
			"error_count":  len(c.Errors),
			"response_len": c.Writer.Size(),
		}).Info("http request")
	}
}

// logrusRecovery turns a panic into a 500 and records it WITHOUT putting the panic's
// own text or the stack into the log at a normal level.
func logrusRecovery() gin.HandlerFunc {
	return gin.CustomRecoveryWithWriter(nil, func(c *gin.Context, recovered interface{}) {
		fields := logrus.Fields{
			"method":     c.Request.Method,
			"route":      requestRoute(c),
			"client_ip":  c.ClientIP(),
			"panic_type": fmt.Sprintf("%T", recovered),
			"panic":      redactedPanicMessage(recovered),
		}

		if logrus.IsLevelEnabled(logrus.DebugLevel) {
			fields["panic_verbatim"] = logsafe.Value(
				fmt.Sprintf("%v", recovered), logsafe.MaxErrorLength,
			)
			fields["stack"] = logsafe.Value(string(debug.Stack()), maxLoggedStackLength)
		}

		logrus.WithFields(fields).Error("panic recovered")
		c.AbortWithStatus(http.StatusInternalServerError)
	})
}

// Search performs a search query on a specified collection.
//
// Parameters:
// - c: The Gin context containing the request and response.
func (a Api) Search(c *gin.Context) {
	collection, passed := c.Params.Get("collection")
	if !passed {
		respondCode(c, apierror.ErrGenMissingParameter, "collection is required. pass id in the route /:collection", nil)
		return
	}

	var query api.SearchCollectionParams
	err := c.BindJSON(&query)
	if err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}

	resp, err := a.blnk.Search(collection, &query)
	if err != nil {
		respondError(c, err, withDefault(apierror.ErrSrchQueryInvalid))
		return
	}

	c.JSON(http.StatusCreated, resp)
}

// MultiSearch performs a multi-search query.
//
// Parameters:
// - c: The Gin context containing the request and response.
func (a Api) MultiSearch(c *gin.Context) {
	var searchRequests api.MultiSearchSearchesParameter
	if err := c.BindJSON(&searchRequests); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}

	resp, err := a.blnk.MultiSearch(&searchRequests)
	if err != nil {
		respondError(c, err, withDefault(apierror.ErrSrchQueryInvalid))
		return
	}

	c.JSON(http.StatusOK, resp)
}

// maxLoggedStackLength caps the goroutine stack written to a debug-level field.
const maxLoggedStackLength = 8192

// unmatchedRoute is the route field for a request that matched no registered route.
const unmatchedRoute = "unmatched"

// requestRoute reports the matched route template for a request, or unmatchedRoute.
func requestRoute(c *gin.Context) string {
	route := c.FullPath()
	if route == "" {
		return unmatchedRoute
	}

	return logsafe.Value(route, logsafe.MaxValueLength)
}

// redactedPanicMessage renders a recovered value for a normal-level log line.
func redactedPanicMessage(recovered interface{}) string {
	if recovered == nil {
		return ""
	}

	if err, isError := recovered.(error); isError {
		return logsafe.Cause(err)
	}

	return logsafe.Cause(fmt.Errorf("%v", recovered))
}

// withLoggableCause attaches a dependency error to a log entry in the two renderings an
// operator needs, and it is the ONLY way this package should put an error into a line.
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
