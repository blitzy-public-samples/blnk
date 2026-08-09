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

	// THE WEBHOOK RETIREMENT GUARD IS NOT INSTALLED HERE, and that is a correction rather than
	// an omission.
	//
	// It used to be, with router.Use, immediately before Authenticate. Both of those positions
	// are necessary — before authentication, so a caller is not told 401 or 403 about a surface
	// that is never coming back and left fixing credentials for it; and before route matching,
	// so a PATCH, OPTIONS or HEAD that matches no registered handler is answered 410 rather than
	// the router's own 404 or 405, since requirement R-12 says every request and four methods is
	// not every request.
	//
	// Neither is sufficient. Every router.Use in this method runs AFTER every one in NewAPI, and
	// NewAPI installs RequestSizeLimit and RateLimitMiddleware — both of which ABORT. So a
	// throttled or oversized request to a retired route was answered 429 or 413 and never reached
	// this guard at all. It is therefore installed in NewAPI, ahead of every aborting middleware;
	// see the comment there. Installing it in both places would only run its path test twice per
	// request.
	//
	// The per-route WebhookSunsetGuard below is unaffected and still attached to the four
	// registered verbs: two independent barriers for a behaviour whose failure mode is "the
	// routes quietly keep working" is deliberate.

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
	// REPLAY IS THE WHOLE OF THE DEAD-LETTER WORKFLOW, and there is deliberately no
	// second write beside it.
	//
	// A `resolve` route was registered here and has been removed. It recorded an
	// operator's decision that a dead-lettered event needed no further action, so that
	// retention could delete the row — and it made the surface fourteen routes where
	// the agreed plan approves thirteen. Worse, the two writes could not compose: a
	// resolved row could not then be replayed, because marking the successful
	// re-publish `dispatched` violated the state constraint that kept a resolution
	// confined to the dead-lettered states, and the refusal released the row as
	// replayable again — so a broker-acknowledged replay reported failure and invited a
	// duplicate publish.
	//
	// Retention is now modelled through this route alone: a dead-lettered row is
	// retained indefinitely, keeps feeding the dead-letter age gauge and keeps the
	// DeadLetterMessageStuck alert firing until a replay the broker acknowledges turns
	// it into a dispatched receipt — which is the only state the retention sweep may
	// delete by age.
	router.POST("/events/dead-letter/:event_id/replay", a.ReplayDeadLetterEvent)
	router.GET("/events/stats", a.GetEventOutboxStats)

	// Subscriber routes: the registry of Kafka principals and their credentials.
	router.POST("/subscribers", a.CreateSubscriber)
	router.GET("/subscribers", a.ListSubscribers)
	router.GET("/subscribers/:subscriber_id", a.GetSubscriber)
	router.PUT("/subscribers/:subscriber_id", a.UpdateSubscriber)
	router.DELETE("/subscribers/:subscriber_id", a.DeleteSubscriber)
	router.POST("/subscribers/:subscriber_id/kafka-credentials", a.IssueKafkaCredentials)

	// Deprecated webhook-subscription management routes.
	//
	// Retained only for the dual-delivery window. Once
	// WEBHOOK_DEPRECATION_SUNSET_DATE has passed these four answer 410 Gone, as does
	// every other method addressed to the same path and every unauthenticated attempt
	// at it — all of which is the work of the single path-scoped guard installed at the
	// top of this function, ahead of authentication. That placement is what covers the
	// cases a per-route guard cannot see: an unsupported verb and a path with no handler
	// both reach no route at all, and a request rejected by authentication would
	// otherwise be answered 401 on a surface that no longer exists.
	//
	// The per-route guard is attached here as the SECOND barrier, and the two cannot
	// disagree: both resolve the retirement through blnk.WebhookSunsetSnapshotAt, the
	// single decision point, and both render the Sunset and Deprecation headers from the
	// same window. The pre-auth guard exists to reach the requests that never route — an
	// unsupported verb, and a caller whose credential has also lapsed; this one is the
	// retirement's local, visible statement at each registration and would still refuse if
	// the global middleware were ever removed from the chain. For a behaviour whose failure
	// mode is "the routes quietly keep working", two independent barriers is deliberate.
	//
	// NEITHER BARRIER REACHES THE /hooks ROUTES ABOVE. Those are the PRE_TRANSACTION and
	// POST_TRANSACTION request-time callouts of a different, fully supported feature, and the
	// rest of /subscribers is the REPLACEMENT for the surface being retired — including the
	// credential endpoint a subscriber needs in order to leave it. Both guards match this exact
	// path shape rather than a prefix, for precisely that reason.
	//
	// It is attached PER ROUTE rather than with router.Use or to a group, and the four routes
	// below are the whole of its reach. The reason is DUPLICATION, not safety: it does test the
	// path, so a global installation would be correct — it would simply do the pre-auth guard's
	// job a second time, after authentication, for every request in the API.
	//
	// Removing these routes and their handlers is a LATER, SEPARATE RELEASE. Passing the
	// retirement instant changes behaviour, not code: the handlers, the legacy sender and its
	// configuration block all remain, which is what makes the retirement reversible by moving
	// the date and what means somebody still has to perform the removal.
	//
	// The path is middleware.DeprecatedWebhookSubscriptionRoute rather than four literals,
	// so the routes registered here and the paths the pre-auth guard recognises are one
	// fact stated in the file that owns the retirement decision.
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
	//
	// Gin's default is to trust every proxy, which means it believes the leftmost
	// X-Forwarded-For header on any request: a caller could choose the address that
	// appears in the access log, and in a rate-limit decision or an audit built on it.
	// Handing it the configured list — nil when nothing is configured — replaces that with
	// "believe a forwarded address only from a proxy the operator named". With nothing
	// configured, ClientIP resolves to the peer that actually opened the connection, which
	// cannot be forged.
	//
	// A malformed entry is fatal rather than ignored. Continuing would leave the engine on
	// its trust-everything default while the operator's configuration says otherwise,
	// which is the one outcome worse than refusing: it looks configured and is not.
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
	// SecurityHeaders only SETS response headers; it never aborts. Running it before the two
	// middlewares that do means every refusal this chain produces carries them — the retirement's
	// 410, a 413 for an oversized body, a 429 for a throttled caller — where previously the first
	// two of those were written by middleware that ran before it and went out bare.
	r.Use(middleware.SecurityHeaders())

	// THE RETIRED-PATH BARRIER, AND IT MUST PRECEDE EVERY MIDDLEWARE THAT CAN ABORT.
	//
	// This used to be installed in Router(), which runs its router.Use calls AFTER every one in
	// this function. RequestSizeLimit and RateLimitMiddleware both abort, so a request to a
	// retired webhook-subscription route was answered 413 or 429 — never reaching either sunset
	// guard. A throttled caller was told to slow down and retry a surface that is gone, and would
	// have retried it forever. Requirement R-12 says the webhook REST API answers 410 Gone on
	// EVERY request, and "every request except the throttled ones" does not satisfy it.
	//
	// Its blast radius is unchanged by the move. WebhookSunsetPreAuthGuard tests the request
	// against one exact three-segment path shape and the four retired methods before it does
	// anything else, so /hooks, the live subscriber registry, the credential endpoint and every
	// ledger and transaction route pass through it untouched — exactly as they did when it sat in
	// Router(). It is installed HERE ONLY; Router() no longer installs it, because two
	// registrations would run the same path test twice per request for no benefit.
	//
	// It sits before otelgin, which costs the 410 its server span. That is deliberate and it is
	// the cheaper side of the trade: putting otelgin first would mean moving RateLimitMiddleware
	// behind it too, and every throttled request under a flood would then open a span — turning
	// an attack into trace volume. The retirement stays observable through the access log line
	// logrusAccessLogger has already opened above.
	r.Use(middleware.WebhookSunsetPreAuthGuard())

	r.Use(middleware.RequestSizeLimit(conf.Server.MaxRequestBodySizeMB * 1024 * 1024))
	auth := middleware.NewAuthMiddleware(b)
	r.Use(middleware.RateLimitMiddleware(conf))
	// The server span every request trace is rooted in, and the point from which the trace
	// reaches the event pipeline: handlers pass c.Request.Context() into the service layer, the
	// event capture writes the active trace context onto the outbox row, and the relay's publish
	// links back to it. That chain starts here.
	//
	// NO SpanNameFormatter IS SUPPLIED, DELIBERATELY. otelgin names the span from c.FullPath() —
	// the registered ROUTE TEMPLATE, "/events/dead-letter/:event_id/replay" — and sets the
	// http.route attribute from the same value. A formatter reading r.URL.Path instead would name
	// the span after the concrete path, which puts a financial identifier in a span name and gives
	// the tracing backend one operation per event id: unbounded name cardinality on the busiest
	// surface in the service. If a formatter is ever needed here it must derive from the route
	// template, never from the request path.
	r.Use(otelgin.Middleware("BLNK",
		otelgin.WithFilter(func(r *http.Request) bool {
			// Exclude high-frequency operational endpoints from tracing
			// to avoid polluting the trace feed with noise.
			//
			// /metrics is a scrape target hit by a monitor on a fixed interval, so a span per
			// scrape describes nothing anybody would read and would outnumber the request spans
			// that do. This is the only exclusion: every surface that MUTATES anything is traced,
			// because those are the traces the event pipeline's spans link back to.
			return r.URL.Path != "/metrics"
		}),
	))

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, "server running...")
	})

	return &Api{blnk: b, router: r, auth: auth}
}

// logrusAccessLogger logs one line per request, identifying it by ROUTE TEMPLATE rather than by
// the path that was requested.
//
// # Why the template and not the path (SEC-09)
//
// The `path` field used to be c.Request.URL.Path — the concrete path, with its path parameters
// substituted. That was harmless while every route took an opaque business id, and it stopped
// being harmless the moment routes like /events/dead-letter/:event_id/replay and
// /subscribers/:subscriber_id/kafka-credentials existed: every request to them wrote a financial
// or tenant identifier into an access log that is shipped to whatever log pipeline the deployment
// runs, retained on its schedule, and readable by anyone with log access rather than by anyone
// with an API key.
//
// The SECOND cost is cardinality, and it is the one that bites a log platform rather than a
// reader. `path` is an indexed field in every structured log store; a value containing an
// identifier means one distinct field value per event, per subscriber, per transaction. The route
// template is a closed set — one value per registered route — so the field becomes groupable, and
// "which endpoint is slow" becomes answerable at all.
//
// Nothing correlating is lost. The identifier is still in the handler's own log lines and in the
// response, and the request's trace carries the same route template as its span name, so a log
// line and a trace can still be joined. What is gone is the identifier's appearance in a field
// nobody needed it in.
//
// The field is named `route` rather than `path`, because after this change it holds a route
// template and not a path: keeping the old name would have left every existing query selecting a
// field whose meaning had silently changed under it. `path` is not emitted at all, so a query
// that still selects it fails visibly instead of quietly reading a different thing.
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

// logrusRecovery turns a panic into a 500 and records it WITHOUT putting the panic's own
// text or the stack into the log at a normal level.
//
// # gin's OWN recovery writer is disabled here, and that is the larger half of this
//
// gin.CustomRecovery installs gin's error writer alongside the handler, and that writer
// prints — unconditionally, to os.Stderr, which in a container IS the log — the panic value
// verbatim, the full stack, AND A DUMP OF THE REQUEST HEADERS. It masks exactly one header,
// Authorization, so Blnk's own authentication header, X-Blnk-Key, is printed in full: a
// single panic on an authenticated request publishes the caller's API key to the log.
//
// Passing a nil writer to CustomRecoveryWithWriter turns that emitter off, which leaves the
// line below as the only thing a panic writes. Nothing is lost: the panic value and the
// stack are both still available here, and both are emitted at debug.
//
// # What a recovered value contains
//
// A panic value carries whatever the panicking code was holding: a nil-map write reveals
// little, but a panic from a database or broker call renders with the connection string or
// the broker address, and a panic carrying a wrapped error reproduces that error's full
// text. The stack goes further — it names internal packages, file paths and line numbers,
// which is a map of the deployment.
//
// So the normal-level line carries the panic's TYPE and a redacted rendering of its
// message: enough to tell one recurring panic from another and to find it in the code,
// without publishing internals to everyone who can read the log. The verbatim value and
// the stack are emitted at debug, which is the level an operator turns on deliberately
// when they are debugging exactly this.
//
// The response itself is unchanged: a bare 500 with no body, which is what it already was.
// Nothing about the panic has ever reached the caller, and nothing does now.
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
// nothing and it means no future route, however long, can produce an unbounded log field.
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
// An error is rendered through logsafe.Cause, so a panic that wrapped a broker or database
// failure loses its addresses the same way a returned error would. Anything else is
// rendered with %v and then sanitized and redacted identically, because a panic value is
// frequently a string that was built from one of those errors.
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
// The "cause" field is the redacted rendering, which is what a deployment writes at info,
// warn and error. The "cause_verbatim" field carries the unredacted text and is attached
// ONLY when the standard logger is at debug — the restricted sink.
//
// It is the counterpart of the identically named helper in package blnk, and both delegate
// to internal/logsafe, so an error logged by a handler and the same error logged by the
// service beneath it are redacted and bounded identically. Using logrus.WithError instead
// is the defect this replaces: it renders err.Error() verbatim into the "error" field at
// whatever level the line is emitted at, and the errors this package logs are Kafka and
// database errors that name broker addresses and connection strings.
//
// Parameters:
//   - entry *logrus.Entry: the entry to extend. A nil entry is treated as a fresh one, so a
//     caller with no fields to add does not have to construct one.
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
