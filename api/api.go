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
//
// Responses:
// - 200 OK: When the router is successfully set up.
func (a Api) Router() *gin.Engine {
	router := a.router

	// THE WEBHOOK RETIREMENT GUARD IS NOT INSTALLED HERE, and that is a correction rather
	// than an omission.

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
	// REPLAY IS THE WHOLE OF THE DEAD-LETTER WORKFLOW, and there is deliberately no second
	// write beside it.
	router.POST("/events/dead-letter/:event_id/replay", a.ReplayDeadLetterEvent)
	router.GET("/events/stats", a.GetEventOutboxStats)

	// Subscriber routes: the registry of Kafka principals and their credentials.
	router.POST("/subscribers", a.CreateSubscriber)
	router.GET("/subscribers", a.ListSubscribers)
	router.GET("/subscribers/:subscriber_id", a.GetSubscriber)
	router.PUT("/subscribers/:subscriber_id", a.UpdateSubscriber)
	router.DELETE("/subscribers/:subscriber_id", a.DeleteSubscriber)
	router.POST("/subscribers/:subscriber_id/kafka-credentials", a.IssueKafkaCredentials)

	// THERE IS NO DATA-PLANE ROUTE UNDER /subscribers, and its absence is the design
	// rather than an omission. Subscribers consume Kafka DIRECTLY with the per-subscriber
	// SASL/SCRAM credential the endpoint above issues; every route
	// registered here is an OPERATOR action gated on the master key.

	// Deprecated webhook-subscription management routes.
	//
	// Retained only for the dual-delivery window. Once WEBHOOK_DEPRECATION_SUNSET_DATE has
	// passed these four answer 410 Gone, as does every other method addressed to the same
	// path and every unauthenticated attempt at it — all of which is the work of the
	// single path-scoped guard installed at the top of this function, ahead of
	// authentication. That placement is what covers the cases a per-route guard cannot
	// see: an unsupported verb and a path with no handler both reach no route at all, and
	// a request rejected by authentication would otherwise be answered 401 on a surface
	// that no longer exists.
	//
	// The per-route guard is attached here as the SECOND barrier, and the two cannot
	// disagree: both resolve the retirement through blnk.WebhookSunsetSnapshotAt, the
	// single decision point, and both render the Sunset and Deprecation headers from the
	// same window. The pre-auth guard exists to reach the requests that never route — an
	// unsupported verb, and a caller whose credential has also lapsed; this one is the
	// retirement's local, visible statement at each registration and would still refuse if
	// the global middleware were ever removed from the chain.
	//
	// THESE FOUR ROUTES ARE NOT DELETED BY THE TERMINAL RELEASE, and that is a decision
	// rather than an omission. The requirement is that this surface answer 410 Gone on
	// EVERY request after the sunset, and a route that has been removed answers 404
	// instead — so the registrations are what there is to answer with, and the guard is
	// what answers. They also have to be here for a deployment whose own window has not
	// closed yet, since the retirement is a per-deployment configuration instant rather
	// than a property of this binary.
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

	// MOVED AHEAD OF THE ABORTING MIDDLEWARE BELOW, and the move is the point.
	//
	// SecurityHeaders only SETS response headers; it never aborts.
	r.Use(middleware.SecurityHeaders())

	// THE RETIRED-PATH BARRIER, AND IT MUST PRECEDE EVERY MIDDLEWARE THAT CAN ABORT.
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
//
// The SECOND cost is cardinality, and it is the one that bites a log platform rather
// than a reader. `path` is an indexed field in every structured log store; a value
// containing an identifier means one distinct field value per event, per subscriber,
// per transaction.
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
//
// gin.CustomRecovery installs gin's error writer alongside the handler, and that writer
// prints — unconditionally, to os.Stderr, which in a container IS the log — the panic
// value verbatim, the full stack, AND A DUMP OF THE REQUEST HEADERS. It masks exactly
// one header, Authorization, so Blnk's own authentication header, X-Blnk-Key, is
// printed in full: a single panic on an authenticated request publishes the caller's
// API key to the log.
//
// So the normal-level line carries the panic's TYPE and a redacted rendering of its
// message: enough to tell one recurring panic from another and to find it in the code,
// without publishing internals to everyone who can read the log. The verbatim value and
// the stack are emitted at debug, which is the level an operator turns on deliberately
// when they are debugging exactly this.
//
// Returns:
//   - gin.HandlerFunc: the recovery middleware.
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
// It binds the incoming JSON request to a SearchCollectionParams object,
// executes the search query, and responds with the search results.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or performing the search.
// - 201 Created: If the search query is successfully executed and results are returned.
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
// It binds the incoming JSON request to a MultiSearchParameter object,
// executes the multi-search query, and responds with the search results.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or performing the search.
// - 200 OK: If the multi-search query is successfully executed and results are returned.
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
//
// Larger than an error cap because a stack is legitimately long and a truncated one can be
// useless — the frame that matters may be anywhere in it — and still bounded, because a
// stack from deep recursion is unbounded and would otherwise be able to fill the log by
// itself.
const maxLoggedStackLength = 8192

// unmatchedRoute is the route field for a request that matched no registered route.
//
// A fixed word rather than the requested path. A 404's path is entirely attacker-chosen —
// it is where scanners put their probes — and echoing it is how a log becomes a mirror for
// whatever a caller wants written into it. The status code already says the request found
// nothing, and the count of these lines is the signal an operator acts on.
const unmatchedRoute = "unmatched"

// requestRoute reports the matched route template for a request, or unmatchedRoute.
//
// Bounded even though a route template is this codebase's own string: the bound costs
// nothing and it means no future route, however long, can produce an unbounded log
// field.
//
// Parameters:
//   - c *gin.Context: the request context, after routing.
//
// Returns:
//   - string: the route template, or unmatchedRoute when none matched.
func requestRoute(c *gin.Context) string {
	route := c.FullPath()
	if route == "" {
		return unmatchedRoute
	}

	return logsafe.Value(route, logsafe.MaxValueLength)
}

// redactedPanicMessage renders a recovered value for a normal-level log line.
//
// An error is rendered through logsafe.Cause, so a panic that wrapped a broker or
// database failure loses its addresses the same way a returned error would. Anything
// else is rendered with %v and then sanitized and redacted identically, because a panic
// value is frequently a string that was built from one of those errors.
//
// Parameters:
//   - recovered interface{}: the value passed to the recovery handler.
//
// Returns:
//   - string: the redacted, sanitized, bounded rendering.
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
//
// The "cause" field is the redacted rendering, which is what a deployment writes at
// info, warn and error. The "cause_verbatim" field carries the unredacted text and is
// attached ONLY when the standard logger is at debug — the restricted sink.
//
// Parameters:
//   - entry *logrus.Entry: the entry to extend. A nil entry is treated as a fresh one,
//     so a caller with no fields to add does not have to construct one.
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
