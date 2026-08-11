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
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/apierror"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
)

// This file holds the three master-key-gated operational endpoints of the Kafka
// event pipeline:
//
//	GET  /events/dead-letter                        ListDeadLetterEvents
//	POST /events/dead-letter/:event_id/replay       ReplayDeadLetterEvent
//	GET  /events/stats                              GetEventOutboxStats
//
// # WHERE THE BOUNDARY SITS
//
// Domain rules stay out of this file: no SQL, no retry policy, no metrics
// instrument and no definition of what "stuck" means. What the handlers do own is
// the HTTP contract — parameter validation, paging bounds, response shape and
// error codes — and, for the statistics endpoint, composing two sources into one
// answer. Three divisions are load-bearing rather than stylistic:
//
//   - The dead-letter inventory and its filtering predicates live in the root
//     package's EventDeadLetterService, so the relay, the metrics collector and
//     this endpoint share one answer. Only the page bounds and the count rules
//     are decided here, at the HTTP boundary they belong to.
//   - Replay re-publishes the STORED BYTES of the event. Nothing here
//     unmarshals, re-marshals, re-keys or otherwise reconstructs the message,
//     because byte-for-byte replay fidelity is an acceptance criterion and any
//     round trip through a Go struct would reorder JSON keys and break it.
//   - Error responses go through respondCode/respondError from api/errors.go
//     with a typed code from internal/apierror. An ad-hoc c.JSON status here
//     would bypass statusByCode, which is the single source of truth for the
//     status of every error code, and unknown codes silently become 500.
//
// GetEventOutboxStats is the one handler that orchestrates rather than delegates:
// it reads the per-status counts from the repository, optionally builds a Kafka
// admin client to read topic offsets, and computes the reconciliation from both.
// That composition is here because it is a reporting view assembled for this
// response and consumed by nothing else; its arithmetic and its caveats are
// documented on the handler.
//
// # PRIVILEGE
//
// All three are OPERATOR endpoints: the inventory names every stuck event in the
// deployment, the replay re-publishes ledger events, and the statistics expose
// the whole outbox and the broker's offsets. They are therefore gated on the
// master key as their very first act, before any parameter is read and before
// any service is touched, mirroring ensureHookManagementAuthorized. A non-master
// caller learns only that the master key is required — never whether an event id
// exists, which would otherwise turn the replay route into an oracle.
//
// # WHAT THE RESPONSES DO NOT SAY
//
// Write-side exactly-once is a property of the events captured INSIDE their
// mutation's database transaction, not of the catalogue as a whole. That is every
// event type but two, and the two differ from each other:
//
//   - `bulk_transaction.<status>` is captured in the same transaction as the batch
//     coordinator's terminal transition, so it is lost only if that transaction
//     never commits — which leaves the batch countable as unfinalized.
//   - `system.error` reports a process fault rather than a ledger mutation, so it
//     has no transaction to join and is captured standalone. It is the one type
//     that is at-most-once as a matter of course.
//
// Everything else, `balance.monitor` and a coalesced batch's `transaction.*`
// events included, has its row inserted before the mutation commits.
// docs/event-streaming.md carries the per-event-type table. Kafka delivery is
// at-least-once regardless of how a row was captured, and event_id is the
// subscriber's idempotency key, so no message or field below claims a stronger
// guarantee than that.

const (
	// deadLetterPageDefaultLimit and deadLetterPageMaxLimit are the page bounds
	// applied at the HTTP boundary.
	//
	// They deliberately match ParseFiltersFromBody in api/filter_helper.go
	// exactly — including its habit of resetting an oversized limit to the
	// default rather than clamping it to the ceiling — so that every list
	// endpoint in this package answers a page request the same way.
	//
	// The service layer's own ceiling is higher, and it is reached only by
	// in-process walks: the filtered listing pages the inventory internally,
	// and the dead-letter age gauge scans it, both at the repository's maximum.
	// Nothing reaching this handler can request that page size. Operator
	// procedures that use the HTTP endpoint are bound by the two constants
	// below like every other caller.
	deadLetterPageDefaultLimit = 20
	deadLetterPageMaxLimit     = 100
)

// Query-parameter names, declared once so the accepted-parameter lists below and
// the readers agree by construction.
const (
	eventQueryParamLimit     = "limit"
	eventQueryParamCursor    = "cursor"
	eventQueryParamEventType = "event_type"
	eventQueryParamTopic     = "topic"
	eventQueryParamDLTTopic  = "dlt_topic"
	eventQueryParamStatus    = "status"

	eventQueryParamOccurredFrom   = "occurred_from"
	eventQueryParamOccurredTo     = "occurred_to"
	eventQueryParamIncludeCount   = "include_count"
	eventQueryParamIncludeOffsets = "include_offsets"
	eventQueryParamWindow         = "window"
	eventQueryParamSortBy         = "sort_by"
	eventQueryParamSortOrder      = "sort_order"

	// eventOffsetsBestEffortValue is the third value include_offsets accepts, alongside
	// "true" and "false". It names the posture that used to be reachable only by omitting
	// the parameter, and it is declared here so the reader and the refusal message cannot
	// disagree about how it is spelled (PERF-M02).
	eventOffsetsBestEffortValue = "best_effort"
)

// Window bounds for GET /events/stats.
//
// # PERF-P04/P05: why the statistics endpoint takes a window at all
//
// Two of its readings were whole-history: the per-status counts and the terminal-record audit
// the zero-loss verdict is drawn from. At 500 events per second the outbox gains 43.2 million
// rows a day, so both grew without bound — and the verdict itself was unsound over the whole
// history, because broker end offsets are cumulative while the outbox's retention sweep
// deletes rows, so the tolerated surplus grew by however much the outbox had forgotten.
//
// A window fixes both. The dispatched count and the audit are bounded by it, the broker side
// is measured over the SAME interval, and every other status is still counted exactly and in
// full — so nothing an operator acts on is hidden by choosing a short window.
//
// # PERF-M05: and why the ceiling equals the default, so the window may only be NARROWED
//
// The ceiling was a week. The window bounds exactly one figure — the exact COUNT of the
// dispatched population — and at 500 events per second a day of that is 43.2 million index
// entries while a week is 302.4 million. A count over 302 million entries is not servable inside
// any request timeout, so permitting it handed an authenticated caller a scan the database pays
// for and the caller never receives. Everything an operator acts on is counted for ALL TIME
// regardless of the window, and the zero-loss audit is bounded by retention rather than by this
// parameter, so a wider window bought a historical figure and nothing else.
//
// The parameter's remaining job is to narrow: `?window=15m` for a twenty-minute incident is the
// case it exists for and is unaffected.
const (
	// eventStatsDefaultWindow is the window applied when a request names none. One day,
	// matching the daily zero-loss reconciliation the runbook describes.
	eventStatsDefaultWindow = 24 * time.Hour

	// eventStatsMaxWindow is the longest window accepted, and it is the default: a request
	// may ask for less than the daily reconciliation period but never for more. It must stay
	// equal to blnk.maxEventStatisticsWindow, which is the service layer's own floor under
	// every non-HTTP caller — a ceiling here that exceeded the service's would be silently
	// clamped, which is the one behaviour this endpoint refuses rather than performs.
	eventStatsMaxWindow = eventStatsDefaultWindow
)

// deadLetterQueryParameters is every query parameter GET /events/dead-letter
// accepts, in the order the error message lists them.
//
// sort_by and sort_order are accepted and INERT, exactly as they are for every
// other list endpoint in this package: ParseQueryOptions coerces an unknown sort
// field to the empty string, and the inventory's ordering is fixed by the
// repository. They are listed rather than rejected so that a caller reusing a
// generic list-endpoint client is not refused for sending them, and the fixed
// ordering is documented on ListDeadLetterEvents.
var deadLetterQueryParameters = []string{
	eventQueryParamLimit,
	eventQueryParamCursor,
	eventQueryParamEventType,
	eventQueryParamTopic,
	eventQueryParamDLTTopic,
	eventQueryParamStatus,
	eventQueryParamOccurredFrom,
	eventQueryParamOccurredTo,
	eventQueryParamIncludeCount,
	eventQueryParamSortBy,
	eventQueryParamSortOrder,
}

// eventStatsQueryParameters is every query parameter GET /events/stats accepts.
var eventStatsQueryParameters = []string{
	eventQueryParamIncludeOffsets,
	eventQueryParamWindow,
}

// errEventsRequireMasterKey is the message a non-master caller receives from any
// of the three endpoints in this file.
var errEventsRequireMasterKey = errors.New("event management requires master key")

// ensureEventManagementAuthorized enforces the master key on the event
// management surface. It returns false once it has written the refusal, so a
// caller must return immediately without touching the response.
//
// The refusal carries apierror.ErrAuthMasterKeyRequired. That code and
// ErrAuthUnknownResource both resolve to HTTP 403, so a test proving this gate
// fired — as opposed to the route prefix being unmapped in the authorization
// middleware — must assert on error_detail.code and never on the status.
func ensureEventManagementAuthorized(c *gin.Context) bool {
	if isMasterKeyRequest(c) {
		return true
	}

	respondCode(c, apierror.ErrAuthMasterKeyRequired, errEventsRequireMasterKey.Error(), nil)

	return false
}

// rejectUnsupportedQueryParameters refuses a request that carries a query
// parameter the endpoint cannot honour.
//
// It is shared by the event surface and the subscriber surface — both close their
// parameter sets for the reason below, and one implementation means the two cannot
// disagree about what an unsupported name does.
//
// # Why a filter this endpoint cannot apply is an ERROR and not an omission
//
// These are triage and reconciliation endpoints. Quietly ignoring a narrowing a
// caller asked for hands them a result that answers a different question than
// the one they posed, and they have no way to tell: an operator who filtered by
// an occurrence window and received the whole inventory would read stale entries
// as current. The root package takes the same position on the one filter it does
// validate — an unsupported status filter is rejected rather than allowed to
// return an empty page, because "nothing is stuck" is exactly the wrong answer
// to give an operator who asked something else.
//
// So the accepted set is closed, and it is the accepted set of THIS endpoint
// rather than of the filter package: the dead-letter inventory supports the
// filters DeadLetterListOptions declares and no others. Every one of them —
// including the occurrence window, which used to be refused because no layer
// carried it — is applied in SQL by the repository, so an accepted filter is a
// filter that actually narrows the query rather than one simulated over an
// already-paged result.
//
// Every offending name is reported at once, sorted, so a caller fixes one
// request rather than discovering its parameters one round trip at a time.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written
//     when this returns.
//   - supported []string: the accepted parameter names, in the order they should
//     be listed back to the caller.
//
// Returns:
//   - bool: true when every parameter present is supported.
func rejectUnsupportedQueryParameters(c *gin.Context, supported []string) bool {
	if c.Request == nil || c.Request.URL == nil {
		return true
	}

	var unsupported []string
	for name := range c.Request.URL.Query() {
		if !slices.Contains(supported, name) {
			unsupported = append(unsupported, name)
		}
	}

	if len(unsupported) == 0 {
		return true
	}

	// Map iteration order is unspecified, so the names are sorted to keep the
	// message — and any test asserting on it — deterministic.
	slices.Sort(unsupported)

	respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
		"unsupported query parameter(s) %s; this endpoint accepts only %s",
		strings.Join(quotedQueryParameters(unsupported), ", "),
		strings.Join(quotedQueryParameters(supported), ", "),
	), nil)

	return false
}

// quotedQueryParameters renders parameter names for an error message.
//
// Parameters:
//   - names []string: the names to render.
//
// Returns:
//   - []string: a fresh slice of quoted names, in the order given.
func quotedQueryParameters(names []string) []string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, fmt.Sprintf("%q", name))
	}

	return quoted
}

// includeCountFromQuery parses the include_count option EXPLICITLY.
//
// # Why this is not ParseQueryOptions
//
// ParseQueryOptions reads include_count as `value == "true"`, so "TRUE", "True",
// "1" and "yes" all mean false. A caller that asked for a total and received a bare
// array has no way to tell its spelling was ignored from a deployment that does not
// support counting, and the two call for opposite responses. A misspelling that
// silently changes the response SHAPE is worse than one that is refused.
//
// strconv.ParseBool is the accepted vocabulary — "1", "t", "T", "true", "TRUE",
// "True" and their false counterparts — because it is Go's own and because a caller
// guessing at a spelling is most likely to guess one of those. Anything else is a
// validation error naming the value.
//
// An ABSENT or empty parameter is false rather than an error: not asking for a count
// is the ordinary request, and `?include_count=` is what an HTTP client that
// serialises an unset option produces.
//
// api/filter_helper.go, which owns ParseQueryOptions, is shared by every listing in
// the API and is not modified here — tightening it would change the behaviour of
// endpoints no finding covers. This helper is used by the endpoints that must not
// silently drop the option, and it takes precedence over ParseQueryOptions wherever
// both are read.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when
//     this returns.
//
// Returns:
//   - bool: whether a total was requested.
//   - bool: false when the refusal has been written.
func includeCountFromQuery(c *gin.Context) (bool, bool) {
	raw := strings.TrimSpace(c.Query(eventQueryParamIncludeCount))
	if raw == "" {
		return false, true
	}

	includeCount, err := strconv.ParseBool(raw)
	if err != nil {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%q is not a valid value for %q; use true or false",
			raw, eventQueryParamIncludeCount,
		), nil)

		return false, false
	}

	return includeCount, true
}

// ListDeadLetterEvents serves GET /events/dead-letter: the paged inventory an
// operator triages dead-lettered ledger events from.
//
// It reads the outbox table through the root package's dead-letter service and
// never a Kafka topic, so it answers with the broker down — which is precisely
// when it is wanted. Both terminal failure states are listed: a row is `failed`
// the moment its retry budget is spent and `dead_lettered` only once the event
// has additionally reached its `<topic>.dlt` sibling, so listing only the latter
// would hide the events whose dead-letter write itself failed. Only a
// dead_lettered event can be replayed.
//
// # Paging and ordering
//
// limit and offset follow this package's uniform convention: a non-positive or
// oversized limit becomes 20, a negative offset becomes 0, and a value that is
// not an integer at all is a client mistake and is refused rather than silently
// defaulted. Ordering is FIXED by the repository at newest occurrence first,
// ties broken by descending id, which is what makes paging stable; sort_by and
// sort_order are accepted for client compatibility and do not change it.
//
// # Filters
//
//	event_type     exact match on the event name, e.g. "transaction.applied"
//	topic          exact match on the ORIGINAL category topic, e.g. "blnk.transactions"
//	dlt_topic      the same filter expressed as the ".dlt" sibling, e.g. "blnk.transactions.dlt"
//	status         "failed" or "dead_lettered"; anything else is refused by the service
//	occurred_from  RFC3339 lower bound on the event's occurrence, inclusive
//	occurred_to    RFC3339 upper bound on the event's occurrence, inclusive
//
// Every filter is applied IN SQL by the repository, and the offset is therefore an
// offset into the filtered set. The occurrence window is what makes incident-scoped
// triage — "what is stuck from the twenty minutes the broker was down" — a single
// request rather than a page-and-eyeball exercise; it bounds occurred_at, the same
// column the inventory is ordered by, so the window and the paging agree about what
// "newest first" selects. A reversed window is refused by the service rather than
// returning an empty page, because an empty page reads as "nothing is stuck".
//
// # Counting
//
// include_count=true is honoured alongside EVERY filter, and the total it returns
// is the size of the FILTERED set — the count is taken with the same narrowing the
// page was read with. It used to be refused whenever event_type or topic was set,
// because the only count available was a per-status aggregate over the whole
// table, which would have described a different set from the page beside it.
//
// # Responses
//
// 200 with the DeadLetterPageResponse envelope — `{data, next_cursor, has_more,
// total_count?}` — ALWAYS, whether or not a total was asked for. A client reads
// `.data[]`; the body is never a bare array, because cursor paging has to return the
// cursor somewhere. total_count is present only with include_count=true, is available
// for EVERY narrowing including event_type and topic, and is read from the same
// snapshot as the page, so the two cannot describe different sets.
//
// An empty inventory is 200 with `"data": []` — never 404, and `data` is never `null`:
// "no events are stuck" is a successful answer, and a reconciliation script must be
// able to range over `.data[]` unconditionally.
// 403 GEN_* / AUTH_MASTER_KEY_REQUIRED for a non-master caller, 400
// GEN_VALIDATION_ERROR for an unusable parameter, 500 GEN_INTERNAL for a
// repository failure.
//
// Each item is projected by model.NewDeadLetterEvent, which is the single place
// the stored payload, the raw driver error text and the internal failure struct
// are dropped. This handler never assembles an item field by field, so it cannot
// leak them.
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) ListDeadLetterEvents(c *gin.Context) {
	if !ensureEventManagementAuthorized(c) {
		return
	}

	if !rejectUnsupportedQueryParameters(c, deadLetterQueryParameters) {
		return
	}

	options, ok := deadLetterListOptionsFromQuery(c)
	if !ok {
		return
	}

	// Parsed explicitly rather than through ParseQueryOptions, which reads only the
	// lowercase "true" and would silently drop "TRUE" or "1" — changing the response
	// SHAPE without saying so. See includeCountFromQuery.
	includeCount, ok := includeCountFromQuery(c)
	if !ok {
		return
	}

	// ONE READ WHEN A TOTAL WAS ASKED FOR, TWO OTHERWISE.
	//
	// The page and the total used to be two independent calls, and the response then asserted a
	// relationship between them that nothing established: an entry dead-lettered between the two
	// reads is counted by one and absent from the other, so the total described a set the page
	// was not a slice of. On a triage endpoint that reads as a different amount of stuck work
	// than there is, and a client comparing the page against the total does not terminate.
	//
	// The combined read draws both from one read-only REPEATABLE READ snapshot. A caller that did
	// not ask for a total still takes the cheaper single-statement path, because there is then no
	// second answer to be coherent with.
	var (
		page  coremodel.DeadLetterInventoryPage
		total int64
		err   error
	)

	if includeCount {
		page, total, err = a.blnk.ListAndCountDeadLetterEvents(c.Request.Context(), options)
	} else {
		page, err = a.blnk.ListDeadLetterEvents(c.Request.Context(), options)
	}

	if err != nil {
		// The service returns a typed APIError for an unusable status filter and
		// for a missing datasource, so respondError resolves the code from the
		// error itself; the default only covers a repository error that carries
		// none.
		respondError(c, err, withDefault(apierror.ErrGenInternal))

		return
	}

	// Allocated with make so an empty inventory marshals as [] rather than null,
	// which is what lets a reconciliation script range over the response
	// unconditionally.
	items := make([]model.DeadLetterEvent, 0, len(page.Entries))
	for _, entry := range page.Entries {
		items = append(items, model.NewDeadLetterEvent(entry))
	}

	// THE CURSOR FOR THE NEXT PAGE, and the only way to ask for one. It is opaque: a caller
	// passes it back verbatim as ?cursor=, and its absence means this page is the last. A paging
	// client therefore terminates on a missing cursor rather than on an empty page, which is one
	// fewer request and — unlike an offset — cannot repeat or skip a row when the inventory
	// changes underneath it.
	response := DeadLetterPageResponse{Data: items, HasMore: page.HasMore}
	if page.NextCursor != nil {
		response.NextCursor = page.NextCursor.Encode()
	}

	if includeCount {
		// Counted from the SAME narrowing the page was drawn with and in the SAME snapshot, so
		// the total is a total of this very page's population. The narrowing matters because a
		// total was once derived from a whole-table per-status aggregate that knew nothing about
		// event_type, topic or the occurrence window; the snapshot matters because two reads
		// sharing a predicate still observe two populations.
		//
		// What it is not is a promise about the paging SESSION. Paging spans many requests over a
		// live inventory that the relay adds to and a replay removes from, so a later page can
		// reveal entries this total did not count. See DeadLetterPageResponse.TotalCount.
		response.TotalCount = &total
	}

	c.JSON(http.StatusOK, response)
}

// DeadLetterPageResponse is the body GET /events/dead-letter returns.
//
// It carries the page and the position to resume from, which is what replaced an offset
// (PERF-P08): a caller pages by handing next_cursor back rather than by naming a depth, so a
// page's cost is independent of how deep it is and no page can repeat or skip a row because
// something was written while the caller was paging.
//
// total_count is present only when include_count was asked for, and it is then counted from the
// SAME narrowing the page was drawn with, in the SAME database snapshot. It was once refused for
// every filter but status, because the total came from a whole-table per-status aggregate that
// knew nothing about event_type, topic or the occurrence window. It is counted, never estimated.
//
// What total_count is exact ABOUT is stated on the field, because the honest scope of the number
// is narrower than "the size of the backlog" and an operator acting on it during an incident
// needs to know which.
type DeadLetterPageResponse struct {
	// Data is the page, newest occurrence first. Never null.
	Data []model.DeadLetterEvent `json:"data"`

	// NextCursor is the opaque token that requests the following page. Absent on the last
	// page, which is the termination condition.
	NextCursor string `json:"next_cursor,omitempty"`

	// HasMore mirrors the presence of NextCursor, so a client can branch on a boolean
	// without inspecting the token.
	HasMore bool `json:"has_more"`

	// TotalCount is how many entries the request's narrowing matched, present only when
	// include_count was asked for.
	//
	// # What it is exact about
	//
	// It is counted — never estimated — with the page's own filters, and it is read from the SAME
	// snapshot as data, so the total and THIS page describe one population. That pairing is the
	// property this field is for, and it used to be absent: the two were separate reads on
	// separate connections, and an entry dead-lettered between them was counted by one and
	// missing from the other.
	//
	// # What it is NOT
	//
	// It is not a promise about a paging SESSION. The inventory is live — the relay dead-letters
	// entries into it and a replay takes them out — and paging is many requests, so a later page
	// may hold entries this total did not count, and entries counted here may be gone by the time
	// they would have been paged to. Treat it as "how much matched, as at this page", which is
	// what a triage decision needs, rather than as a fixed size to page towards.
	TotalCount *int64 `json:"total_count,omitempty"`
}

// deadLetterListOptionsFromQuery reads the page and the filters from the query
// string, writing the refusal itself when a value is unusable.
//
// The status filter is passed through verbatim after trimming rather than
// validated here: the service owns the vocabulary and rejects an unsupported
// value with a typed validation error naming the two states it accepts, and a
// second copy of that list in this file would be a second thing to keep correct.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written
//     when this returns.
//
// Returns:
//   - blnk.DeadLetterListOptions: the page and its narrowing.
//   - bool: false when the refusal has been written.
func deadLetterListOptionsFromQuery(c *gin.Context) (blnk.DeadLetterListOptions, bool) {
	limit, ok := deadLetterPageLimitFromQuery(c)
	if !ok {
		return blnk.DeadLetterListOptions{}, false
	}

	cursor, ok := deadLetterCursorFromQuery(c)
	if !ok {
		return blnk.DeadLetterListOptions{}, false
	}

	occurredFrom, ok := deadLetterOccurrenceBoundFromQuery(c, eventQueryParamOccurredFrom)
	if !ok {
		return blnk.DeadLetterListOptions{}, false
	}

	occurredTo, ok := deadLetterOccurrenceBoundFromQuery(c, eventQueryParamOccurredTo)
	if !ok {
		return blnk.DeadLetterListOptions{}, false
	}

	topic, ok := deadLetterTopicFilterFromQuery(c)
	if !ok {
		return blnk.DeadLetterListOptions{}, false
	}

	return blnk.DeadLetterListOptions{
		Limit:        limit,
		Cursor:       cursor,
		EventType:    strings.TrimSpace(c.Query(eventQueryParamEventType)),
		Topic:        topic,
		Status:       strings.TrimSpace(c.Query(eventQueryParamStatus)),
		OccurredFrom: occurredFrom,
		OccurredTo:   occurredTo,
	}, true
}

// deadLetterOccurrenceBoundFromQuery reads one end of the occurrence window.
//
// # RFC3339 only, and rejected rather than ignored
//
// The format is RFC3339 because that is the format the event envelope's occurred_at is
// serialised in and the format every dead-letter item echoes back — a caller pasting a
// timestamp out of a response into a filter must have it accepted. An offset is required
// by the format, so "2026-01-02T03:04:05Z" and "2026-01-02T04:04:05+01:00" are both
// accepted and both name the same instant; a bare date or a local timestamp with no zone
// is refused, because guessing a zone would silently shift the window by hours.
//
// An unparseable value is a 400 rather than an ignored parameter. This is a triage
// endpoint: an operator who asked for a twenty-minute window and silently received the
// whole inventory would read stale entries as current, and nothing in the response would
// tell them the filter was dropped.
//
// An absent or blank value leaves that end unbounded, which is what makes a one-sided
// window — "everything since the incident started" — expressible without inventing a
// sentinel for the other end.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this
//     returns.
//   - parameter string: the query-parameter name to read, for the error message.
//
// Returns:
//   - time.Time: the parsed instant, or the zero time when the parameter is absent.
//   - bool: false when the refusal has been written.
func deadLetterOccurrenceBoundFromQuery(c *gin.Context, parameter string) (time.Time, bool) {
	raw := strings.TrimSpace(c.Query(parameter))
	if raw == "" {
		return time.Time{}, true
	}

	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"Invalid %s value; it must be an RFC3339 timestamp such as %q",
			parameter, "2006-01-02T15:04:05Z",
		), nil)

		return time.Time{}, false
	}

	return parsed, true
}

// deadLetterPageLimitFromQuery reads limit and applies this package's normalisation.
//
// A non-positive or oversized value becomes the default, matching
// ParseFiltersFromBody. A value that does not parse as an integer is refused
// instead, matching GetAllLedgers: defaulting it would silently serve a page the
// caller did not ask for, and the mistake is the caller's to see.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written
//     when this returns.
//
// Returns:
//   - int: the normalised limit.
//   - bool: false when the refusal has been written.
func deadLetterPageLimitFromQuery(c *gin.Context) (int, bool) {
	raw := strings.TrimSpace(c.Query(eventQueryParamLimit))
	if raw == "" {
		return deadLetterPageDefaultLimit, true
	}

	limit, err := strconv.Atoi(raw)
	if err != nil {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf("Invalid %s value", eventQueryParamLimit), nil)

		return 0, false
	}

	if limit <= 0 || limit > deadLetterPageMaxLimit {
		limit = deadLetterPageDefaultLimit
	}

	return limit, true
}

// deadLetterCursorFromQuery reads the opaque page cursor.
//
// # PERF-P08: this replaced an OFFSET, and the replacement is not cosmetic
//
// The endpoint used to accept any non-negative offset and pass it straight into an OFFSET
// clause. PostgreSQL reads and discards every row before an offset, so the cost of a page grew
// with its depth and the depth was the caller's to choose without any bound — on a table that
// gains 43.2 million rows a day. The page-size cap bounded the RESPONSE and bounded no work at
// all.
//
// A cursor names the ordering key of the last row the previous page returned, so every page is
// a range scan of the same size. It is also stable: rows written while a caller pages do not
// move the position behind the cursor, whereas an offset silently repeats and skips rows
// whenever the set changes.
//
// A MALFORMED CURSOR IS REFUSED, never coerced. Defaulting it to the beginning would restart a
// paging client at page one, which is an infinite loop for any client that pages until the
// cursor is absent.
//
// # The check is a DECODE, and the refusal says so
//
// It used to read "is not a cursor this endpoint issued", which claimed more than it does. A
// keyset cursor is a coordinate, not a capability: it carries no signature and no issuer, so any
// value that decodes to a well-formed (occurred_at, id) pair is accepted as a position —
// including one another endpoint's listing produced. That is harmless, because the cursor
// carries no authorization and the request is master-key gated either way, but a message
// promising provenance invites the opposite mental model, and a client debugging a rejected
// cursor would go looking for an issuance record that does not exist.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written
//     when this returns.
//
// Returns:
//   - *model.DeadLetterCursor: the decoded position, nil for the first page.
//   - bool: false when the refusal has been written.
func deadLetterCursorFromQuery(c *gin.Context) (*coremodel.DeadLetterCursor, bool) {
	cursor, err := coremodel.ParseDeadLetterCursor(c.Query(eventQueryParamCursor))
	if err != nil {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%q could not be decoded as a page position; omit it for the first page, or pass back "+
				"the %q value from a previous response verbatim",
			eventQueryParamCursor, "next_cursor",
		), nil)

		return nil, false
	}

	return cursor, true
}

// deadLetterTopicFilterFromQuery resolves the topic filter to an ORIGINAL category
// topic, which is what the service filters on.
//
// Both spellings are accepted because both are natural. An operator reading the
// inventory sees dlt_topic on every item and will filter by it; an operator
// asking "which transaction events are stuck" thinks in category topics. The
// ".dlt" suffix is therefore trimmed using the package's own exported constant —
// never a literal — so the two spellings resolve to one filter.
//
// # Two spellings that DISAGREE are refused
//
// Supplying both used to be resolved by preferring `topic` and discarding
// `dlt_topic` silently. That is the one behaviour worth an error here: the caller
// named two different narrowings and received a page drawn from one of them, so
// the answer describes a question they did not ask and nothing in it says so — and
// on this endpoint a page narrower or wider than requested reads as a different
// amount of loss. Supplying both with the SAME resolved topic is accepted, because
// `?topic=blnk.transactions&dlt_topic=blnk.transactions.dlt` is one instruction
// written twice rather than a contradiction.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when
//     this returns.
//
// Returns:
//   - string: the original category topic to filter on, or "" for no narrowing.
//   - bool: false when the refusal has been written.
func deadLetterTopicFilterFromQuery(c *gin.Context) (string, bool) {
	topic := strings.TrimSuffix(
		strings.TrimSpace(c.Query(eventQueryParamTopic)), blnk.DeadLetterTopicSuffix)
	deadLetterTopic := strings.TrimSuffix(
		strings.TrimSpace(c.Query(eventQueryParamDLTTopic)), blnk.DeadLetterTopicSuffix)

	if topic != "" && deadLetterTopic != "" && topic != deadLetterTopic {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%q and %q name different topics (%q and %q); supply one of them, or the same topic "+
				"in both spellings",
			eventQueryParamTopic, eventQueryParamDLTTopic,
			topic, deadLetterTopic+blnk.DeadLetterTopicSuffix,
		), nil)

		return "", false
	}

	if topic != "" {
		return topic, true
	}

	return deadLetterTopic, true
}

// ReplayDeadLetterEvent serves POST /events/dead-letter/:event_id/replay: it
// re-publishes one dead-lettered event to its ORIGINAL category topic.
//
// # Fidelity is structural, not something this handler achieves
//
// The service re-publishes the bytes stored on the outbox row, stripping only the
// failure metadata the dead-letter copy added. This handler passes an id in and
// serialises an acknowledgement out; it does not read, decode, re-encode or
// re-key the message, because a round trip through a Go struct would reorder JSON
// object keys and break the byte-for-byte guarantee the replay is scored on. The
// acknowledgement deliberately does not echo the payload either, so a caller
// cannot be tempted to diff the wrong pair of byte strings.
//
// The event id is unchanged by a replay, which is what lets a subscriber
// deduplicating on event_id absorb the copy. Delivery remains at-least-once.
//
// # Responses
//
//	200 OK                        the broker acknowledged the re-publish
//	400 GEN_MISSING_PARAMETER     no event id in the route
//	403 AUTH_MASTER_KEY_REQUIRED  the caller does not hold the master key
//	404 EVENT_NOT_FOUND           no event with that id exists
//	409 EVENT_NOT_DEAD_LETTERED   the event exists but is not replayable — already
//	                              replayed, or a concurrent replay holds it
//	500 EVENT_REPLAY_FAILED       the re-publish failed, or it succeeded and the
//	                              outbox entry could not be cleared
//	503 EVENT_KAFKA_UNAVAILABLE   no broker is configured, or the broker is down
//
// The service already returns typed errors carrying these codes, so the options
// on respondError are the safety net rather than the mechanism. They matter all
// the same: api/errors.go classifies an unclassified error by message, and its
// table ends in a broad "not found" catch-all, so an error that reached this
// handler untyped would otherwise answer GEN_NOT_FOUND instead of
// EVENT_NOT_FOUND. The two upgrades and the default close that gap.
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) ReplayDeadLetterEvent(c *gin.Context) {
	if !ensureEventManagementAuthorized(c) {
		return
	}

	eventID, passed := c.Params.Get("event_id")
	if !passed || strings.TrimSpace(eventID) == "" {
		respondCode(c, apierror.ErrGenMissingParameter,
			"event_id is required. pass it in the route /events/dead-letter/:event_id/replay", nil)

		return
	}

	eventID = strings.TrimSpace(eventID)

	// The identifier is checked against the CANONICAL form the pipeline mints before it
	// is used to look anything up, because a value that cannot be an event id is a
	// malformed request and not a missing event.
	//
	// It matters for two reasons. A 404 EVENT_NOT_FOUND tells an operator the event was
	// purged or never existed and sends them to look for it; a 400 tells them the id
	// they pasted is wrong, which is the actual problem, and every id this pipeline
	// mints is a lowercase canonical UUID — model.NewEventID generates it and the
	// event_id column carries a uniqueness constraint over exactly that form. And the
	// lookup itself is an exact string comparison, so an uppercased or braced spelling
	// of a real event's id would miss the row and be reported as a nonexistent event.
	if !coremodel.IsCanonicalUUID(eventID) {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"event_id must be a canonical lowercase UUID such as %q",
			"3f2504e0-4f89-11d3-9a0c-0305e82c3301",
		), nil)

		return
	}

	outcome, err := a.blnk.ReplayDeadLetteredEvent(c.Request.Context(), eventID)
	if err != nil {
		respondError(c, err,
			withDefault(apierror.ErrEventReplayFailed),
			withUpgrade(apierror.ErrGenNotFound, apierror.ErrEventNotFound),
			withUpgrade(apierror.ErrGenConflict, apierror.ErrEventNotDeadLettered),
		)

		return
	}

	c.JSON(http.StatusOK, model.ReplayEventResponse{
		EventID: outcome.EventID,
		// The ORIGINAL category topic the event went back to, never the
		// dead-letter topic it was listed from.
		Topic:      outcome.Topic,
		Status:     string(outcome.Status),
		ReplayedAt: outcome.ReplayedAt,
	})
}

// GetEventOutboxStats serves GET /events/stats: the outbox side of the daily
// zero-loss reconciliation, and — when the broker can be read — the broker side
// and the verdict as well.
//
// # This handler orchestrates nothing
//
// It authorises, reads three things out of the query string, makes ONE call to
// blnk.EventOutboxStatistics and projects the result onto the response DTO. The
// five-step sequence behind that call — the per-status counts, the terminal-record
// audit, the Kafka admin client, the topic end offsets and the reconciliation
// verdict — and the per-failure choice between degrading and refusing live in the
// root package. They used to live here, which put the sequencing rules and the
// failure policy of a multi-step operation inside a function whose job is to
// translate HTTP: the sequence was unreachable from anything that is not a Gin
// request, and each degradation decision was expressed as a `c.JSON` early return,
// so "which failures are tolerable" could only be read by tracing response writes.
//
// # It must answer with no Kafka at all
//
// A deployment with no brokers configured is a legitimate steady state, not a
// failure: the publisher resolves to its no-op implementation and the service
// runs exactly as it did before the pipeline existed. The service therefore reads
// the per-status counts from PostgreSQL FIRST and treats the broker round trip as
// an enrichment. When the offsets cannot be produced the counts are still returned
// with 200, offsets_complete is false and the offset keys are OMITTED rather than
// emitted as nulls a reconciliation script would have to special-case.
//
// include_offsets selects between the three postures — and, because the two are one decision,
// whether the DISPATCHED HISTORY is counted at all (PERF-M05/M02):
//
//	absent       skipped. No broker round trip, and the counts cover the exact UNRESOLVED
//	             inventory only. This is the cheap default every routine caller should take.
//	false        skipped, said explicitly. Identical to absent.
//	best_effort  the dispatched history is counted over the window and the broker is read
//	             when one is configured; a failure is logged, the broker-side keys are
//	             omitted, and the answer is still 200. This is what a broker-less
//	             deployment wants, and what the daily check uses when a 503 is unhelpful.
//	true         required. As best_effort, except that a failure to read the broker answers
//	             503 EVENT_KAFKA_UNAVAILABLE, because the caller asked for the half of the
//	             reconciliation that is missing. This is what the reconciliation runbook uses.
//
// The dispatched figure and the broker offsets are wanted by one caller and no other: the only
// reason to know how many rows were dispatched in an interval is to compare it against what the
// broker recorded over that interval. `dispatched_history_counted` on the response says which
// reading was taken, because an absent `dispatched` key means "none in the window" and "never
// counted" indistinguishably.
//
// # The offsets are read for the WHOLE inventory, deliberately
//
// No topic-narrowing parameter is offered. The verdict compares the broker's
// records against EVERY outbox row that claims a publication, so measuring a
// subset of the topics would manufacture a shortfall and report loss that has not
// happened. Restricting the topics is only meaningful alongside a matching
// restriction on the outbox side, which no repository method offers.
//
// # Responses
//
// 200 with the statistics DTO. 403 AUTH_MASTER_KEY_REQUIRED for a non-master
// caller, 400 GEN_VALIDATION_ERROR for an unusable parameter, 500 GEN_INTERNAL
// when the outbox itself cannot be read, and 503 EVENT_KAFKA_UNAVAILABLE only
// when offsets were explicitly required and could not be produced.
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) GetEventOutboxStats(c *gin.Context) {
	if !ensureEventManagementAuthorized(c) {
		return
	}

	if !rejectUnsupportedQueryParameters(c, eventStatsQueryParameters) {
		return
	}

	inclusion, ok := eventOffsetInclusionFromQuery(c)
	if !ok {
		return
	}

	// THE WINDOW THIS ENDPOINT ADVERTISES. It is listed in eventStatsQueryParameters, so
	// `?window=15m` is accepted rather than refused as unknown — and until this read it was then
	// silently discarded, which is the worst of the three possible behaviours. An operator
	// investigating a twenty-minute incident received a day and nothing in the response said so.
	window, ok := eventStatsWindowFromQuery(c)
	if !ok {
		return
	}

	// ONE call. The sequencing — counts, then audit only when the broker side will be
	// measured, then the offsets, then the verdict — and the per-failure decision
	// between degrading and refusing both belong to the root package, which is what
	// makes them reachable from something that is not a Gin request and readable
	// without tracing response writes. This handler translates.
	statistics, err := a.blnk.EventOutboxStatistics(c.Request.Context(), inclusion, window)
	if err != nil {
		// The service returns typed errors: ErrInternalServer when the outbox itself
		// cannot be read, and ErrKafkaUnavailable when the broker was REQUIRED and
		// could not be read, already carrying a sanitized message. The default is the
		// safety net for an untyped repository error.
		respondError(c, err, withDefault(apierror.ErrGenInternal))

		return
	}

	c.JSON(http.StatusOK, eventOutboxStatsResponseFrom(statistics))
}

// eventOffsetInclusionFromQuery reads include_offsets, writing the refusal itself for
// a value that is neither "true" nor "false".
//
// An unrecognised value is refused rather than treated as false: silently
// skipping the broker would return a response whose offsets_complete is false for
// a reason the caller cannot see, and they would read a missing verdict as an
// unreachable broker.
//
// # PERF-M02: an ABSENT parameter now SKIPS the broker side, and counts no history
//
// Absent used to mean best-effort, which made the DEFAULT reading a broker round trip plus an
// exact count of a day of dispatched history — 43.2 million index entries at the target rate.
// The callers that omit the parameter are precisely the ones that want neither: the load
// harness's drain loop polled this endpoint on that default every second while waiting for the
// outbox to quiesce, so the measurement perturbed the system it was measuring, and any health
// check or dashboard doing the same paid the same price for fields it discarded.
//
// The expensive, optional, failure-prone enrichment is now opt-in, and blnk.EventOffsetsSkipped
// is the zero value of the posture type so the HTTP default and the Go default cannot document
// different behaviour.
//
// # best_effort is spelled out because it is no longer the default
//
// The posture itself did not change and neither did `true`: it still means REQUIRED, so a
// reconciliation that cannot read the broker receives a 503 rather than a quietly halved
// answer. What changed is that best-effort — read the broker when one is configured, degrade to
// the counts when it cannot be read, never fail — used to be reachable only by omitting the
// parameter, and omitting it now means skipped. It therefore has a name.
//
// That name matters for one caller in particular: an operator who wants the dispatched history
// on a deployment with no brokers at all. Skipped does not count history, `true` would answer
// 503, and `best_effort` is exactly right — it counts the history, attempts the broker, and
// returns 200 with the counts when there is no broker to read. That is also the posture every
// broker-less deployment ran in permanently before this parameter had to be named.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written
//     when this returns.
//
// Returns:
//   - blnk.EventOffsetInclusion: the requested posture, in the root package's own
//     vocabulary. Reading straight into it rather than into a local enum keeps this
//     handler from owning a second copy of a policy the service decides.
//   - bool: false when the refusal has been written.
func eventOffsetInclusionFromQuery(c *gin.Context) (blnk.EventOffsetInclusion, bool) {
	switch strings.ToLower(strings.TrimSpace(c.Query(eventQueryParamIncludeOffsets))) {
	case "", "false":
		return blnk.EventOffsetsSkipped, true
	case "true":
		return blnk.EventOffsetsRequired, true
	case eventOffsetsBestEffortValue, "best-effort":
		return blnk.EventOffsetsBestEffort, true
	default:
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%s must be %q, %q or %q when it is supplied",
			eventQueryParamIncludeOffsets, "true", "false", eventOffsetsBestEffortValue,
		), nil)

		return blnk.EventOffsetsSkipped, false
	}
}

// eventStatsWindowFromQuery reads the `window` option, refusing a value it cannot honour.
//
// # Refused rather than clamped, here of all places
//
// The service clamps — it is the floor under every non-HTTP caller — but a REQUEST that names a
// window this endpoint will not serve must be told so. This is the endpoint acceptance criterion
// V-2 is scored on: an operator who asks for a fortnight and silently receives a week compares
// two intervals believing they are one, and the arithmetic that follows is wrong in a direction
// nothing on the response reveals. A 400 costs them one corrected request.
//
// An absent or blank value is not an error and yields the default, which is the daily period the
// reconciliation runbook describes.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this returns.
//
// Returns:
//   - time.Duration: the requested window, or the default when none was named.
//   - bool: false when the refusal has been written.
func eventStatsWindowFromQuery(c *gin.Context) (time.Duration, bool) {
	raw := strings.TrimSpace(c.Query(eventQueryParamWindow))
	if raw == "" {
		return eventStatsDefaultWindow, true
	}

	window, err := time.ParseDuration(raw)
	if err != nil {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%q must be a duration such as %q or %q", eventQueryParamWindow, "24h", "15m",
		), nil)

		return 0, false
	}

	if window <= 0 || window > eventStatsMaxWindow {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%q must be greater than zero and no more than %s", eventQueryParamWindow, eventStatsMaxWindow,
		), nil)

		return 0, false
	}

	// A WHOLE NUMBER OF SECONDS, refused rather than truncated. The response reports the
	// interval it measured as `window_seconds`, an integer, so `?window=1500ms` was accepted,
	// measured as 1.5 seconds and REPORTED as 1 — an operator comparing a figure against the
	// window they asked for would be comparing it against a different one, with nothing in the
	// response to reveal the difference. Refusing costs them one corrected request; rounding
	// costs them the arithmetic that follows.
	if window%time.Second != 0 {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%q must be a whole number of seconds, such as %q or %q; this endpoint reports the "+
				"interval it measured in seconds and cannot describe %s without losing precision",
			eventQueryParamWindow, "15m", "36h", window,
		), nil)

		return 0, false
	}

	return window, true
}

// eventOutboxStatsResponseFrom projects the service's statistics onto the response
// DTO.
//
// # Every state is assigned, and that completeness is the point
//
// The reconciliation reads these counts, and a status present in the table but absent
// from the response would make the reported counts sum to less than the table's row
// count — at which point a short total is indistinguishable from a lost event. A
// status with no rows is absent from the aggregate, because GROUP BY only produces
// rows that exist, and indexing a map for a missing key yields zero, which is the
// correct reading of "none in that state".
//
// A state the aggregate holds that this DTO has no field for is reported to the log
// by the service, which is where the enumeration lives; nothing is silently dropped
// here without being named there.
//
// # The two measured flags decide what is emitted
//
// The broker-side keys are OMITTED rather than emitted as zeros or nulls when the
// broker was not read, because "measured and zero" and "not measured" are different
// answers and a reconciliation script must not have to guess which it received.
// offsets_complete is true only when something was actually measured AND every
// measured topic existed AND every partition reported — a reading that covered no
// topics must not present itself as a complete one.
//
// Parameters:
//   - statistics blnk.EventOutboxStatistics: the assembled statistics.
//
// Returns:
//   - model.EventOutboxStatsResponse: the response, ready to marshal.
func eventOutboxStatsResponseFrom(statistics blnk.EventOutboxStatistics) model.EventOutboxStatsResponse {
	counts := statistics.CountsByStatus

	response := model.EventOutboxStatsResponse{
		Pending:        counts[coremodel.EventOutboxStatusPending],
		Processing:     counts[coremodel.EventOutboxStatusProcessing],
		WebhookPending: counts[coremodel.EventOutboxStatusWebhookPending],
		Failed:         counts[coremodel.EventOutboxStatusFailed],
		DeadLettered:   counts[coremodel.EventOutboxStatusDeadLettered],
		Replaying:      counts[coremodel.EventOutboxStatusReplaying],
		GeneratedAt:    statistics.GeneratedAt,

		DispatchedHistoryCounted: statistics.DispatchedHistoryCounted,
	}

	// THE DISPATCHED COUNT, emitted only when it was actually taken (PERF-M05). Every other
	// count above is exact and complete on every request; this one is a count of the single
	// unbounded population and is taken only for a request that asked for the broker side.
	// Indexing the map would yield zero for both "none dispatched in the window" and "never
	// counted", and a reconciliation reading the second as the first concludes that nothing was
	// published at all — so the key is omitted instead, and the flag above says which it is.
	if statistics.DispatchedHistoryCounted {
		dispatched := counts[coremodel.EventOutboxStatusDispatched]
		response.Dispatched = &dispatched
	}

	// THE INTERVAL THE WINDOWED FIGURES COVER, reported so the numbers are self-describing. The
	// two fields are documented on the response type as being chosen with ?window= and they were
	// never assigned, so every reading looked unwindowed while the dispatched count was bounded.
	if !statistics.WindowStart.IsZero() {
		windowStart := statistics.WindowStart
		response.WindowStart = &windowStart
		response.WindowSeconds = int64(statistics.Window.Seconds())
	}

	// THE OWED EVENTS. Present whenever both censuses were read — including a zero-valued
	// reading, which means nothing is outstanding and is the reassuring answer an operator is
	// looking for — and ABSENT when either read failed, because zeros for "we could not tell"
	// is the one misreading a zero-loss check cannot afford. The service already makes that
	// distinction by returning nil; this projection must not flatten it.
	if census := statistics.ProducerAtomicity; census != nil {
		response.ProducerAtomicity = &model.ProducerAtomicityStats{
			MonitorHandoffPending:    census.MonitorHandoffPending,
			MonitorHandoffProcessing: census.MonitorHandoffProcessing,
			MonitorHandoffCompleted:  census.MonitorHandoffCompleted,
			MonitorHandoffFailed:     census.MonitorHandoffFailed,
			UnfinalizedBatches:       census.UnfinalizedBatches,
			OldestUnfinalizedBatchAt: census.OldestUnfinalizedBatchAt,
		}
	}

	if !statistics.OffsetsRead {
		return response
	}

	report := statistics.Offsets
	response.TopicEndOffsets = report.EndOffsetsByTopic()
	response.MissingTopics = report.MissingTopics
	response.PartitionsUnavailable = report.PartitionsUnavailable
	response.OffsetsComplete = len(response.TopicEndOffsets) > 0 &&
		len(report.MissingTopics) == 0 &&
		report.PartitionsUnavailable == 0
	response.MeasuredWindows = model.NewMeasuredOffsetWindows(report.PartitionIntervals())

	if !report.MeasuredAt.IsZero() {
		measuredAt := report.MeasuredAt
		response.OffsetsMeasuredAt = &measuredAt
	}

	// Nil when nothing was measured, which is a documented absence rather than an
	// error: with no topics covered there is nothing to compare against, so no verdict
	// is reported.
	if statistics.Reconciliation == nil {
		return response
	}

	verdict := statistics.Reconciliation
	response.Reconciliation = &model.OutboxReconciliationResult{
		TerminalEvents:     verdict.TerminalEvents,
		CorroboratedEvents: verdict.CorroboratedEvents,
		UnconfirmedEvents:  verdict.UnconfirmedEvents,
		UnmeasuredEvents:   verdict.UnmeasuredEvents,
		AgedOutEvents:      verdict.AgedOutEvents,
		BeyondEndEvents:    verdict.BeyondEndEvents,
		DuplicatedRecords:  verdict.DuplicatedRecords,
		MessagesWritten:    verdict.MessagesWritten,
		RecordsRetained:    verdict.RecordsRetained,
		BlnkRecordShare:    verdict.BlnkRecordShare,
		// THE SURPLUS AND THE SCOPE IT WAS MEASURED IN, which have to travel together. overhead
		// is records minus claims, and it is only interpretable once a reader knows whether the
		// two sides describe the same interval: a cumulative surplus grows for the life of the
		// topic and means nothing, while a windowed one is the number the verdict is drawn from.
		// Reporting overhead without windowed invites the first to be read as the second, and
		// leaving windowed unset reported `false` — "this comparison declined to conclude" — for
		// every verdict, including the ones that did.
		Overhead:     verdict.Overhead,
		Windowed:     verdict.Windowed,
		LossDetected: verdict.LossDetected,
		Conclusive:   verdict.Conclusive,
		Caveats:      verdict.Caveats,
		Summary:      verdict.Summary(),
		MeasuredAt:   verdict.MeasuredAt,
	}

	if !verdict.WindowStart.IsZero() {
		windowStart := verdict.WindowStart
		response.Reconciliation.WindowStart = &windowStart
	}

	if !verdict.CoveredFrom.IsZero() {
		coveredFrom := verdict.CoveredFrom
		response.Reconciliation.CoveredFrom = &coveredFrom
	}
	if !verdict.CoveredTo.IsZero() {
		coveredTo := verdict.CoveredTo
		response.Reconciliation.CoveredTo = &coveredTo
	}
	if !verdict.OldestTerminalAt.IsZero() {
		oldest := verdict.OldestTerminalAt
		response.Reconciliation.OldestTerminalAt = &oldest
	}

	return response
}

// rejectUnsupportedEventQueryParameters and quotedEventQueryParameters WERE RETIRED HERE.
//
// They were a byte-for-byte SECOND COPY of rejectUnsupportedQueryParameters and
// quotedQueryParameters above — same body, same message, same sorted-names rationale — and
// nothing on the request path ever called them. Only two tests did, which is the part that
// mattered: the coverage for "an unknown query parameter is refused" was attached to the copy
// that never ran, so the guard actually protecting GET /events/dead-letter had none. Those
// tests now exercise the live function.
//
// A second implementation of a refusal is worse than no second implementation. Tightening one
// leaves the other permissive, and a reader cannot tell from either which one the router
// reaches.

// deadLetterInventoryTotal WAS RETIRED HERE. It had become a one-line pass-through to
// EventDeadLetterService.CountDeadLetterEvents, and the listing handler calls that directly.
//
// It is worth saying what it used to do, because the change is the fix rather than a tidy-up. It
// read the whole-table per-status aggregate, which can only answer for a status — so with an
// event_type, topic or occurrence filter set there was no filter-aware count in existence and
// include_count was REFUSED for exactly the narrowed listings an operator pages through. The
// service's count shares its predicate with the listing, so the total now describes the page's
// own result set for every filter, and the argument for that lives at the call site.
