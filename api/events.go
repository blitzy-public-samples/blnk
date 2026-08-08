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
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// This file holds the three master-key-gated operational endpoints of the Kafka
// event pipeline:
//
//	GET  /events/dead-letter                        ListDeadLetterEvents
//	POST /events/dead-letter/:event_id/replay       ReplayDeadLetterEvent
//	GET  /events/stats                              GetEventOutboxStats
//
// # IT TRANSLATES HTTP AND NOTHING ELSE
//
// Every handler below binds a request, calls exactly one service or repository
// operation, and serialises the result into a DTO from api/model. There is no
// SQL here, no Kafka client, no retry policy, no metrics instrument and no
// business rule. That division is load-bearing rather than stylistic:
//
//   - The dead-letter inventory, its filtering and its page arithmetic live in
//     the root package's EventDeadLetterService, so the same rules apply to the
//     relay, the metrics collector and this endpoint. A predicate re-implemented
//     here would be a second, quietly different answer to "what is stuck".
//   - Replay re-publishes the STORED BYTES of the event. Nothing here
//     unmarshals, re-marshals, re-keys or otherwise reconstructs the message,
//     because byte-for-byte replay fidelity is an acceptance criterion and any
//     round trip through a Go struct would reorder JSON keys and break it.
//   - Error responses go through respondCode/respondError from api/errors.go
//     with a typed code from internal/apierror. An ad-hoc c.JSON status here
//     would bypass statusByCode, which is the single source of truth for the
//     status of every error code, and unknown codes silently become 500.
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
// The outbox gives exactly-once semantics on the WRITE side only. Kafka delivery
// remains at-least-once, and event_id is the subscriber's idempotency key, so no
// message or field below claims a delivery guarantee stronger than that.

const (
	// deadLetterPageDefaultLimit and deadLetterPageMaxLimit are the page bounds
	// applied at the HTTP boundary.
	//
	// They deliberately match ParseFiltersFromBody in api/filter_helper.go
	// exactly — including its habit of resetting an oversized limit to the
	// default rather than clamping it to the ceiling — so that every list
	// endpoint in this package answers a page request the same way. The
	// service layer permits a larger page for in-process callers such as the
	// metrics collector and the reconciliation runbook; the public surface
	// keeps the smaller, uniform bound.
	deadLetterPageDefaultLimit = 20
	deadLetterPageMaxLimit     = 100

	// eventOutboxFilterTable is the table name handed to ParseQueryOptions.
	//
	// filter.GetValidFieldsForTable has no entry for it, so any sort_by
	// normalises to the empty string and the repository's own ordering
	// applies — newest occurrence first, ties broken by descending id. That is
	// intentional: ordering the dead-letter inventory is the repository's
	// decision, because paging stability depends on it, and a client-chosen
	// sort column would let a row be shown twice or skipped between pages.
	eventOutboxFilterTable = "event_outbox"

	// eventOffsetReadTimeout bounds the broker round trip the statistics
	// endpoint makes.
	//
	// It exists so that an unreachable broker degrades the response rather than
	// holding the request open: the outbox counts have already been read from
	// PostgreSQL by then, and reporting them without the offsets is a valid,
	// documented answer.
	eventOffsetReadTimeout = 10 * time.Second
)

// Query-parameter names, declared once so the accepted-parameter lists below and
// the readers agree by construction.
const (
	eventQueryParamLimit          = "limit"
	eventQueryParamOffset         = "offset"
	eventQueryParamEventType      = "event_type"
	eventQueryParamTopic          = "topic"
	eventQueryParamDLTTopic       = "dlt_topic"
	eventQueryParamStatus         = "status"
	eventQueryParamIncludeCount   = "include_count"
	eventQueryParamIncludeOffsets = "include_offsets"
	eventQueryParamSortBy         = "sort_by"
	eventQueryParamSortOrder      = "sort_order"
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
	eventQueryParamOffset,
	eventQueryParamEventType,
	eventQueryParamTopic,
	eventQueryParamDLTTopic,
	eventQueryParamStatus,
	eventQueryParamIncludeCount,
	eventQueryParamSortBy,
	eventQueryParamSortOrder,
}

// eventStatsQueryParameters is every query parameter GET /events/stats accepts.
var eventStatsQueryParameters = []string{
	eventQueryParamIncludeOffsets,
}

// errEventsRequireMasterKey is the message a non-master caller receives from any
// of the three endpoints in this file.
var errEventsRequireMasterKey = errors.New("event management requires master key")

// ensureEventManagementAuthorized enforces the master key on the event
// management surface, writing the refusal itself when the caller does not hold
// it.
//
// It mirrors ensureHookManagementAuthorized in shape and reuses
// isMasterKeyRequest rather than restating how the middleware records the
// principal, so there is one reading of "is this the master key" in this
// package.
//
// The refusal carries apierror.ErrAuthMasterKeyRequired. That code and
// ErrAuthUnknownResource both resolve to HTTP 403, so a test proving this gate
// fired — as opposed to the route prefix being unmapped in the authorization
// middleware — must assert on error_detail.code and never on the status.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written
//     when this returns.
//
// Returns:
//   - bool: true when the caller holds the master key and the handler may
//     continue; false when the refusal has been written.
func ensureEventManagementAuthorized(c *gin.Context) bool {
	if isMasterKeyRequest(c) {
		return true
	}

	respondCode(c, apierror.ErrAuthMasterKeyRequired, errEventsRequireMasterKey.Error(), nil)

	return false
}

// rejectUnsupportedEventQueryParameters refuses a request that carries a query
// parameter the endpoint cannot honour.
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
// filters DeadLetterListOptions declares and no others. An occurrence-window
// filter in particular is not available at any layer — neither the repository's
// ListDeadLetteredEvents nor the service's options carry one — so it is refused
// here rather than simulated in the handler, where filtering after the service
// has already paged would return short, unstable pages.
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
func rejectUnsupportedEventQueryParameters(c *gin.Context, supported []string) bool {
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
		strings.Join(quotedEventQueryParameters(unsupported), ", "),
		strings.Join(quotedEventQueryParameters(supported), ", "),
	), nil)

	return false
}

// quotedEventQueryParameters renders parameter names for an error message.
//
// Parameters:
//   - names []string: the names to render.
//
// Returns:
//   - []string: a fresh slice of quoted names, in the order given.
func quotedEventQueryParameters(names []string) []string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, fmt.Sprintf("%q", name))
	}

	return quoted
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
//	event_type  exact match on the event name, e.g. "transaction.applied"
//	topic       exact match on the ORIGINAL category topic, e.g. "blnk.transactions"
//	dlt_topic   the same filter expressed as the ".dlt" sibling, e.g. "blnk.transactions.dlt"
//	status      "failed" or "dead_lettered"; anything else is refused by the service
//
// There is no occurrence-window filter, at any layer, so one is refused rather
// than approximated here — see rejectUnsupportedEventQueryParameters. An operator who
// needs an arbitrary window pages the inventory, which is ordered by occurrence,
// or queries blnk.event_outbox directly as the operations runbook describes.
//
// # Responses
//
// 200 with a JSON array of dead-letter items, or with the {data, total_count}
// envelope when include_count=true. An empty inventory is 200 and `[]` — never
// 404 and never `null`: "no events are stuck" is a successful answer, and a
// reconciliation script must be able to range over the result unconditionally.
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

	if !rejectUnsupportedEventQueryParameters(c, deadLetterQueryParameters) {
		return
	}

	options, ok := deadLetterListOptionsFromQuery(c)
	if !ok {
		return
	}

	// ParseQueryOptions is the package's reader for the GET-side sort and count
	// options. Only IncludeCount is actionable here; see the ordering note above.
	queryOptions := ParseQueryOptions(c, eventOutboxFilterTable)

	if queryOptions.IncludeCount && !countableDeadLetterPage(options) {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%q is not supported together with the %q or %q filter, because no filter-aware count "+
				"of the dead-letter inventory exists; drop the filter to receive a total, or page "+
				"the filtered result without one",
			eventQueryParamIncludeCount, eventQueryParamEventType, eventQueryParamTopic,
		), nil)

		return
	}

	entries, err := a.blnk.ListDeadLetterEvents(c.Request.Context(), options)
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
	items := make([]model.DeadLetterEvent, 0, len(entries))
	for _, entry := range entries {
		items = append(items, model.NewDeadLetterEvent(entry))
	}

	if !queryOptions.IncludeCount {
		c.JSON(http.StatusOK, items)

		return
	}

	total, err := a.deadLetterInventoryTotal(c.Request.Context(), options)
	if err != nil {
		respondError(c, err, withDefault(apierror.ErrGenInternal))

		return
	}

	c.JSON(http.StatusOK, FilterResponse{Data: items, TotalCount: &total})
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

	offset, ok := deadLetterPageOffsetFromQuery(c)
	if !ok {
		return blnk.DeadLetterListOptions{}, false
	}

	return blnk.DeadLetterListOptions{
		Limit:     limit,
		Offset:    offset,
		EventType: strings.TrimSpace(c.Query(eventQueryParamEventType)),
		Topic:     deadLetterTopicFilterFromQuery(c),
		Status:    strings.TrimSpace(c.Query(eventQueryParamStatus)),
	}, true
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

// deadLetterPageOffsetFromQuery reads offset and clamps a negative value to zero, matching
// ParseFiltersFromBody. A non-integer value is refused for the same reason as in
// deadLetterPageLimitFromQuery.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written
//     when this returns.
//
// Returns:
//   - int: the normalised offset.
//   - bool: false when the refusal has been written.
func deadLetterPageOffsetFromQuery(c *gin.Context) (int, bool) {
	raw := strings.TrimSpace(c.Query(eventQueryParamOffset))
	if raw == "" {
		return 0, true
	}

	offset, err := strconv.Atoi(raw)
	if err != nil {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf("Invalid %s value", eventQueryParamOffset), nil)

		return 0, false
	}

	if offset < 0 {
		offset = 0
	}

	return offset, true
}

// deadLetterTopicFilterFromQuery resolves the topic filter to an ORIGINAL category
// topic, which is what the service filters on.
//
// Both spellings are accepted because both are natural. An operator reading the
// inventory sees dlt_topic on every item and will filter by it; an operator
// asking "which transaction events are stuck" thinks in category topics. The
// ".dlt" suffix is therefore trimmed using the package's own exported constant —
// never a literal — so the two spellings resolve to one filter. topic wins when
// both are present, because it is the value the service compares against
// unmodified.
//
// Parameters:
//   - c *gin.Context: the request.
//
// Returns:
//   - string: the original category topic to filter on, or "" for no narrowing.
func deadLetterTopicFilterFromQuery(c *gin.Context) string {
	if topic := strings.TrimSpace(c.Query(eventQueryParamTopic)); topic != "" {
		return strings.TrimSuffix(topic, blnk.DeadLetterTopicSuffix)
	}

	deadLetterTopic := strings.TrimSpace(c.Query(eventQueryParamDLTTopic))

	return strings.TrimSuffix(deadLetterTopic, blnk.DeadLetterTopicSuffix)
}

// countableDeadLetterPage reports whether an EXACT total can be produced for the
// requested narrowing.
//
// It can when the page is unfiltered or narrowed only by status, because
// CountEventOutboxByStatus is a per-status aggregate over the whole table. It
// cannot when event_type or topic is set: no filter-aware count exists at any
// layer, and reporting the size of the returned page as total_count would be a
// falsehood a paging client would loop on forever.
//
// Parameters:
//   - options blnk.DeadLetterListOptions: the requested narrowing.
//
// Returns:
//   - bool: true when deadLetterInventoryTotal can answer exactly.
func countableDeadLetterPage(options blnk.DeadLetterListOptions) bool {
	return options.EventType == "" && options.Topic == ""
}

// deadLetterInventoryTotal counts the whole inventory the page was drawn from.
//
// With no status filter the inventory is both terminal failure states together,
// which is exactly what the listing returns; with one it is that state alone.
// The counts come from the same per-status aggregate the statistics endpoint and
// the backlog gauge read, so a caller comparing the two sees one number.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate query.
//   - options blnk.DeadLetterListOptions: the requested narrowing. Only Status is
//     consulted; callers must have established countableDeadLetterPage first.
//
// Returns:
//   - int64: the number of entries the filtered inventory holds.
//   - error: the repository's own typed error.
func (a *Api) deadLetterInventoryTotal(
	ctx context.Context,
	options blnk.DeadLetterListOptions,
) (int64, error) {
	datasource, err := a.eventDataSource()
	if err != nil {
		return 0, err
	}

	counts, err := datasource.CountEventOutboxByStatus(ctx)
	if err != nil {
		return 0, err
	}

	if options.Status != "" {
		return counts[options.Status], nil
	}

	return counts[coremodel.EventOutboxStatusDeadLettered] +
		counts[coremodel.EventOutboxStatusFailed], nil
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
// # It must answer with no Kafka at all
//
// A deployment with no brokers configured is a legitimate steady state, not a
// failure: the publisher resolves to its no-op implementation and the service
// runs exactly as it did before the pipeline existed. This endpoint therefore
// reads the per-status counts from PostgreSQL FIRST and treats the broker round
// trip as an enrichment. When the offsets cannot be produced the counts are still
// returned with 200, offsets_complete is false and the offset keys are omitted
// rather than emitted as nulls a reconciliation script would have to special-case.
//
// include_offsets selects between the three postures:
//
//	absent   best effort. The broker is read when one is configured, and a
//	         failure is logged and omitted. This is what the runbook uses.
//	true     required. A failure answers 503 EVENT_KAFKA_UNAVAILABLE, because the
//	         caller asked for the half of the reconciliation that is missing.
//	false    skipped. No broker round trip is made at all.
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

	if !rejectUnsupportedEventQueryParameters(c, eventStatsQueryParameters) {
		return
	}

	inclusion, ok := eventOffsetInclusionFromQuery(c)
	if !ok {
		return
	}

	datasource, err := a.eventDataSource()
	if err != nil {
		respondError(c, err, withDefault(apierror.ErrGenInternal))

		return
	}

	ctx := c.Request.Context()

	counts, err := datasource.CountEventOutboxByStatus(ctx)
	if err != nil {
		respondError(c, err, withDefault(apierror.ErrGenInternal))

		return
	}

	response := eventOutboxStatsFromCounts(counts)
	response.GeneratedAt = time.Now().UTC()
	warnOnUnreportedOutboxStatuses(counts)

	if inclusion == eventOffsetsSkipped {
		c.JSON(http.StatusOK, response)

		return
	}

	// The audit is the outbox side of the comparison and is only needed when the
	// broker side is going to be measured, so it is read here rather than above.
	audit, err := datasource.AuditTerminalEventRecords(ctx)
	if err != nil {
		if inclusion == eventOffsetsRequired {
			respondError(c, err, withDefault(apierror.ErrGenInternal))

			return
		}

		logrus.WithError(err).Warn(
			"the event outbox audit could not be read, so the statistics response reports the " +
				"per-status counts without the zero-loss verdict",
		)
		c.JSON(http.StatusOK, response)

		return
	}

	report, err := a.readEventTopicEndOffsets(ctx)
	if err != nil {
		if inclusion == eventOffsetsRequired {
			// A fixed, sanitized message rather than the transport's own words:
			// a Kafka client error renders with broker addresses and topology,
			// so the raw text is kept in the log line beside it instead.
			logrus.WithError(err).Error(
				"the Kafka topic end offsets could not be read for an event statistics request that " +
					"required them",
			)
			respondCode(c, apierror.ErrKafkaUnavailable,
				"The Kafka broker could not be read, so the topic end offsets this request required "+
					"are unavailable; retry once the broker recovers or omit include_offsets to "+
					"receive the outbox counts alone", nil)

			return
		}

		logrus.WithError(err).Warn(
			"the Kafka topic end offsets could not be read, so the statistics response reports the " +
				"per-status counts alone; this is the expected result when no brokers are configured",
		)
		c.JSON(http.StatusOK, response)

		return
	}

	applyEventTopicOffsetReport(&response, report, audit)

	c.JSON(http.StatusOK, response)
}

// eventOffsetInclusion is how a statistics request wants the broker side treated.
type eventOffsetInclusion int

const (
	// eventOffsetsBestEffort reads the broker when one is configured and omits the
	// offsets on any failure. It is the default, and the posture the daily
	// reconciliation runbook relies on.
	eventOffsetsBestEffort eventOffsetInclusion = iota

	// eventOffsetsRequired makes a failure to read the broker an error, because the
	// caller asked specifically for the half of the reconciliation that failed.
	eventOffsetsRequired

	// eventOffsetsSkipped makes no broker round trip at all, for a caller that wants
	// the outbox counts cheaply.
	eventOffsetsSkipped
)

// eventOffsetInclusionFromQuery reads include_offsets, writing the refusal itself for
// a value that is neither "true" nor "false".
//
// An unrecognised value is refused rather than treated as false: silently
// skipping the broker would return a response whose offsets_complete is false for
// a reason the caller cannot see, and they would read a missing verdict as an
// unreachable broker.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written
//     when this returns.
//
// Returns:
//   - eventOffsetInclusion: the requested posture.
//   - bool: false when the refusal has been written.
func eventOffsetInclusionFromQuery(c *gin.Context) (eventOffsetInclusion, bool) {
	switch strings.ToLower(strings.TrimSpace(c.Query(eventQueryParamIncludeOffsets))) {
	case "":
		return eventOffsetsBestEffort, true
	case "true":
		return eventOffsetsRequired, true
	case "false":
		return eventOffsetsSkipped, true
	default:
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%s must be %q or %q when it is supplied",
			eventQueryParamIncludeOffsets, "true", "false",
		), nil)

		return eventOffsetsBestEffort, false
	}
}

// errEventDataSourceMissing is the cause recorded when the API is asked for the
// outbox before a datasource exists. It is a programming or start-up fault rather
// than a client error, so it is reported as an internal failure with the cause
// kept out of the response body.
var errEventDataSourceMissing = errors.New("blnk: the event management API has no datasource")

// eventDataSource resolves the repository the statistics and count reads go
// through.
//
// GetDataSource is the only exported way into the datasource — the Blnk struct's
// fields are unexported — and it is guarded here because NewBlnk(nil) is a
// supported construction in this codebase: a handler must fail legibly with a
// typed error rather than panic on a nil dereference deep inside a query.
//
// Returns:
//   - database.IDataSource: the repository, never nil when err is nil.
//   - error: a typed internal APIError when no datasource is available.
func (a *Api) eventDataSource() (database.IDataSource, error) {
	if a == nil || a.blnk == nil {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The event outbox is unavailable because the service is not initialised",
			errEventDataSourceMissing,
		)
	}

	datasource := a.blnk.GetDataSource()
	if datasource == nil {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The event outbox is unavailable because the service is not initialised",
			errEventDataSourceMissing,
		)
	}

	return datasource, nil
}

// eventOutboxStatsFromCounts projects the per-status aggregate onto the response.
//
// EVERY state in the outbox state machine is assigned. That completeness is the
// point rather than tidiness: the reconciliation reads these counts, and a status
// present in the table but absent from the response would make the reported
// counts sum to less than the table's row count, at which point a short total is
// indistinguishable from a lost event.
//
// A status with no rows is absent from the aggregate — GROUP BY only produces
// rows that exist — and indexing a map for a missing key yields zero, which is
// the correct reading of "none in that state" and is why no two-value form is
// needed here.
//
// Parameters:
//   - counts map[string]int64: the aggregate from CountEventOutboxByStatus. May
//     be nil.
//
// Returns:
//   - model.EventOutboxStatsResponse: the counts, with the broker-side fields
//     left for applyEventTopicOffsetReport and GeneratedAt for the caller to stamp.
func eventOutboxStatsFromCounts(counts map[string]int64) model.EventOutboxStatsResponse {
	return model.EventOutboxStatsResponse{
		Pending:        counts[coremodel.EventOutboxStatusPending],
		Processing:     counts[coremodel.EventOutboxStatusProcessing],
		WebhookPending: counts[coremodel.EventOutboxStatusWebhookPending],
		Dispatched:     counts[coremodel.EventOutboxStatusDispatched],
		Failed:         counts[coremodel.EventOutboxStatusFailed],
		DeadLettered:   counts[coremodel.EventOutboxStatusDeadLettered],
		Replaying:      counts[coremodel.EventOutboxStatusReplaying],
	}
}

// warnOnUnreportedOutboxStatuses logs any status the table holds that this
// response has no field for.
//
// The status column deliberately permits values the code does not know about so
// the state machine can be extended without a migration, which means a new state
// can appear in the aggregate before the response shape learns about it. Its rows
// would then be missing from the reported totals, and a short total is exactly
// what a zero-loss reconciliation cannot tolerate. Logging it is the cheapest
// thing that makes the gap visible without inventing a response field for a
// status nothing can yet interpret.
//
// The comparison is driven from model.EventOutboxStatuses, the authoritative
// enumeration, rather than from a list restated here.
//
// Parameters:
//   - counts map[string]int64: the aggregate from CountEventOutboxByStatus.
func warnOnUnreportedOutboxStatuses(counts map[string]int64) {
	if len(counts) == 0 {
		return
	}

	known := coremodel.EventOutboxStatuses()

	var unreported []string
	for status := range counts {
		if !slices.Contains(known, status) {
			unreported = append(unreported, status)
		}
	}

	if len(unreported) == 0 {
		return
	}

	slices.Sort(unreported)

	logrus.WithField("statuses", strings.Join(unreported, ", ")).Warn(
		"the event outbox holds rows in states the statistics response has no field for, so the " +
			"reported per-status counts sum to less than the table's row count; add the state to " +
			"api/model.EventOutboxStatsResponse before trusting the zero-loss reconciliation",
	)
}

// readEventTopicEndOffsets measures the broker side of the reconciliation.
//
// The admin client is built per request and closed before returning. That is the
// right trade here and the wrong one in a loop: the statistics endpoint is a
// rare, operator-triggered read, and a per-request client costs one connection
// and one SASL handshake in exchange for this handler owning no client lifecycle.
// The relay and the metrics collector each hold their own long-lived client.
//
// A close failure is logged and never returned, matching how the server does it:
// the measurement is what the caller asked about, and reporting a
// connection-teardown problem as a failed read would send an operator looking for
// a broker fault that does not exist.
//
// The read is bounded by its own timeout derived from the request context, so an
// unreachable broker degrades the response instead of holding the request open.
//
// Parameters:
//   - ctx context.Context: the request context; cancellation is inherited.
//
// Returns:
//   - blnk.TopicOffsetReport: the per-topic detail and the sums.
//   - error: ErrKafkaAdminNotConfigured when no broker is configured, or the
//     broker's own wrapped error.
func (a *Api) readEventTopicEndOffsets(ctx context.Context) (blnk.TopicOffsetReport, error) {
	if a == nil || a.blnk == nil {
		return blnk.TopicOffsetReport{}, blnk.ErrKafkaAdminNotConfigured
	}

	configuration := a.blnk.Config()
	if configuration == nil || !blnk.KafkaBrokersConfigured(configuration.Kafka.Brokers) {
		// Reported as the unconfigured error rather than attempted: building a
		// client and letting the operation refuse would dial nothing but would
		// obscure the reason in the caller's log.
		return blnk.TopicOffsetReport{}, blnk.ErrKafkaAdminNotConfigured
	}

	admin, err := blnk.NewKafkaAdmin(configuration)
	if err != nil {
		return blnk.TopicOffsetReport{}, err
	}
	defer func() {
		if closeErr := admin.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn(
				"closing the Kafka admin client after reading the event topic end offsets failed",
			)
		}
	}()

	measurement, cancel := context.WithTimeout(ctx, eventOffsetReadTimeout)
	defer cancel()

	// No topic list is passed, so the full inventory is measured — see the note on
	// GetEventOutboxStats for why narrowing it would invalidate the verdict.
	return admin.TopicEndOffsets(measurement)
}

// applyEventTopicOffsetReport folds the broker measurement and the server's own
// zero-loss verdict into the response.
//
// The verdict comes from blnk.ReconcileAgainstOutbox, which is the only sanctioned
// way to compare the two sides. It is not recomputed here, and that is deliberate:
// the comparison is DIRECTIONAL — records are a lower bound on events, because a
// redelivery, a replay and a dead-letter copy each write their own record — so a
// surplus is expected and only a shortfall is evidence of loss. A caller
// subtracting the two sums and alerting on any difference alerts constantly.
//
// offsets_complete is true only when every measured topic existed and every
// partition reported, AND something was actually measured. That third condition
// matters: a reading that covered no topics at all must not present itself as a
// complete one.
//
// Parameters:
//   - response *model.EventOutboxStatsResponse: the response being assembled.
//   - report blnk.TopicOffsetReport: the broker-side measurement.
//   - audit coremodel.EventOutboxAudit: the outbox-side measurement.
func applyEventTopicOffsetReport(
	response *model.EventOutboxStatsResponse,
	report blnk.TopicOffsetReport,
	audit coremodel.EventOutboxAudit,
) {
	if response == nil {
		return
	}

	response.TopicEndOffsets = report.EndOffsetsByTopic()
	response.MissingTopics = report.MissingTopics
	response.PartitionsUnavailable = report.PartitionsUnavailable
	response.OffsetsComplete = len(response.TopicEndOffsets) > 0 &&
		len(report.MissingTopics) == 0 &&
		report.PartitionsUnavailable == 0

	if !report.MeasuredAt.IsZero() {
		measuredAt := report.MeasuredAt
		response.OffsetsMeasuredAt = &measuredAt
	}

	// With nothing measured there is nothing to compare against, so no verdict is
	// reported. That is the documented absence rather than an error: the
	// reconciliation simply cannot be performed today.
	if len(report.Topics) == 0 {
		return
	}

	verdict := blnk.ReconcileAgainstOutbox(report, audit)
	response.Reconciliation = &model.OutboxReconciliationResult{
		TerminalEvents:    verdict.TerminalEvents,
		ConfirmedEvents:   verdict.ConfirmedEvents,
		UnconfirmedEvents: verdict.UnconfirmedEvents,
		DuplicatedRecords: verdict.DuplicatedRecords,
		MessagesWritten:   verdict.MessagesWritten,
		Overhead:          verdict.Overhead,
		LossDetected:      verdict.LossDetected,
		Conclusive:        verdict.Conclusive,
		Caveats:           verdict.Caveats,
		Summary:           verdict.Summary(),
		MeasuredAt:        verdict.MeasuredAt,
	}
}
