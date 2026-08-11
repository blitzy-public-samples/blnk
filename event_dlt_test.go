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

package blnk

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/printer"
	"go/token"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// event_dlt_test.go covers the three operations event_dlt.go owns — dead-letter
// publication, listing and replay — plus the two metrics they maintain.
//
// Every test here runs with NO KAFKA BROKER AND NO DATABASE. The implementation
// depends on two small seams for exactly this reason: eventDeadLetterStore for the
// repository and deadLetterWriterResolver for the transport, both substituted below by
// in-memory fakes. The end-to-end proofs of the same guarantees live in
// event_replay_fidelity_test.go and the integration tests; asserting them at unit level
// here is what catches a regression without infrastructure, on every `go test -short`.
//
// # SCOPE BOUNDARY: Blnk publishes the `<topic>.dlt` naming convention and NOTHING MORE
//
// "Dead letter" throughout this file means BLNK'S OWN dead-letter topics. Blnk owns the
// `.dlt` sibling of every topic it owns, and it publishes that naming convention
// externally for one reason: so that a subscriber building its own consumer-side
// dead-lettering does not collide with a Blnk-owned topic name.
//
// Blnk therefore does NOT implement, and no test in this file may imply that it
// implements, any of the following:
//
//   - a consumer or consumer-group library of any kind,
//   - subscriber-side dead-letter management — creating, reading, draining or replaying
//     a dead-letter topic that a SUBSCRIBER owns,
//   - a consumer error-handling or poison-message framework.
//
// A subscriber's consumption failures are the subscriber's own to handle. Consistently
// with that, nothing under test here reads from Kafka at all: the listing and the replay
// both work from the outbox ROW, which carries dlt_topic and failure_metadata precisely
// so that no consumer is needed. TestEventDeadLetterSource_BuildsNoConsumerSurface
// enforces that boundary over the source itself rather than trusting this comment.

// dltFixedNow is the instant every dead-letter test measures against.
//
// The service's clock is a field, so it is replaced wholesale rather than tolerated with
// a delta: an age gauge asserted "within a second or so" would pass with the newest entry
// reported instead of the oldest, which is the exact defect the 15-minute alert depends
// on catching.
var dltFixedNow = time.Date(2026, time.April, 11, 9, 30, 0, 0, time.UTC)

// dltOccurredAt is the occurrence instant of the fixture event. Its sub-second component
// is deliberately not a round number so that a timestamp which was re-rendered rather
// than passed through shows up as a byte difference.
var dltOccurredAt = time.Date(2026, time.April, 11, 8, 15, 26, 535897000, time.UTC)

// dltTrapPayload is the fixture payload, and it is a TRAP rather than a sample.
//
// Every one of its five features changes visibly if the payload is ever decoded and
// re-encoded instead of being passed through as bytes:
//
//   - The keys are NOT in sorted order ("z_last" before "a_first"), and encoding/json
//     sorts map keys.
//   - "1.500" carries insignificant trailing zeros, which a number round trip drops.
//   - "<b>&</b>" contains all three characters Go's encoder HTML-escapes by default,
//     rewriting them as \u003c, \u0026 and \u003e.
//   - The large float literal is written in a form the encoder would renormalise to
//     exponent notation.
//   - The interior whitespace after two colons survives a splice and cannot survive a
//     re-encode.
//
// A byte-for-byte assertion against this payload therefore PROVES the pass-through
// property rather than merely being consistent with it. It is the same object shape the
// legacy webhook body has — the two-key {"event", "data"} envelope — because that is what
// LedgerEvent.payload carries verbatim.
const dltTrapPayload = `{"event":"transaction.applied","data":{"z_last":1,"a_first": 2,` +
	`"amount":1.500,"html":"<b>&</b>","big":10000000000000000000000.5,` +
	`"nested":{"y":1,"x": 2}}}`

// dltPublishFailureReason is the failure text carried into the metadata.
//
// It is long, and that is the point: the requirement is that the reason is the ACTUAL
// publish error and is not truncated to uselessness, so the assertion compares the whole
// string. A truncating implementation passes a short-string test and fails this one.
const dltPublishFailureReason = "blnk: writing event to topic blnk.transactions: " +
	"[7] Request Timed Out: the request exceeded the user-specified time limit in the request; " +
	"the leader for partition 3 did not receive acknowledgements from the required number of " +
	"in-sync replicas within request.timeout.ms"

// dltExhaustedAttempts is the retry budget an exhausted row has spent. It is the
// RELAY_MAX_RETRY_ATTEMPTS default, and the attempt count the failure metadata must
// report — not the budget plus one, and not the index of the final retry.
const dltExhaustedAttempts = 5

// dltCategoryRoute is one row of the dead-letter routing table: an event type, the
// category topic it belongs to, and the dead-letter topic it must land on.
type dltCategoryRoute struct {
	eventType       string
	originalTopic   string
	deadLetterTopic string
}

// dltCategoryRoutes is the dead-letter routing expectation for all four categories, with
// every topic name SPELLED OUT AS A LITERAL.
//
// Deriving these from DLTFor would make the test agree with the implementation by
// construction and prove nothing. Three of the four dead-letter names —
// blnk.transactions.dlt, blnk.balances.dlt and blnk.identities.dlt — are verbatim
// user-supplied examples from the requirement and must match byte for byte. The fourth,
// blnk.system.dlt, carries ledger.created, system.error and every event whose type the
// catalogue does not recognise, and follows the identical convention.
var dltCategoryRoutes = []dltCategoryRoute{
	{
		eventType:       "transaction.applied",
		originalTopic:   "blnk.transactions",
		deadLetterTopic: "blnk.transactions.dlt",
	},
	{
		eventType:       "bulk_transaction.applied",
		originalTopic:   "blnk.transactions",
		deadLetterTopic: "blnk.transactions.dlt",
	},
	{
		eventType:       "balance.created",
		originalTopic:   "blnk.balances",
		deadLetterTopic: "blnk.balances.dlt",
	},
	{
		eventType:       "balance.monitor",
		originalTopic:   "blnk.balances",
		deadLetterTopic: "blnk.balances.dlt",
	},
	{
		eventType:       "identity.created",
		originalTopic:   "blnk.identities",
		deadLetterTopic: "blnk.identities.dlt",
	},
	{
		eventType:       "ledger.created",
		originalTopic:   "blnk.system",
		deadLetterTopic: "blnk.system.dlt",
	},
	{
		eventType:       "system.error",
		originalTopic:   "blnk.system",
		deadLetterTopic: "blnk.system.dlt",
	},
}

// dltAllDeadLetterTopics is the complete set of dead-letter topics under the default
// prefix, spelled out for the same reason the routes are: the age gauge must publish a
// value for every one of them so a drained inventory reports zero rather than a stale age.
var dltAllDeadLetterTopics = []string{
	"blnk.transactions.dlt",
	"blnk.balances.dlt",
	"blnk.identities.dlt",
	// The system category's sibling. That category holds ledger.created, system.error and every event
	// whose type the catalogue does not recognise, and those last are exactly the events
	// most likely to fail to publish, so its dead-letter topic must be covered by the age
	// gauge like any other — a stalled entry there being invisible would hide the failure
	// of an event that was already a routing defect. A dead-letter topic is never
	// grantable, whatever its category is.
	"blnk.system.dlt",
}

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// dltMarkRecord is one recorded MarkEventDeadLettered call: the row it named, the
// dead-letter topic it recorded, and the metadata bytes it stored.
//
// The metadata is kept as the exact bytes the service passed, not as a decoded struct,
// because one of the properties asserted below is that the bytes stored on the row and the
// bytes spliced into the message are the SAME bytes rather than merely equivalent
// documents.
type dltMarkRecord struct {
	id         int64
	claimToken string
	dltTopic   string
	metadata   json.RawMessage

	// record is the broker coordinate of the DEAD-LETTER write — where the event's only
	// surviving copy now is. Recorded so a test can assert it is persisted rather than
	// discarded, which is what lets an operator read the exact record back.
	record model.BrokerRecord
}

// dltPageRequest is one recorded ListDeadLetteredEvents call.
//
// It is the repository's own narrowing contract rather than a limit/offset pair of this
// file's own invention, because that is now the WHOLE of what the service asks the
// repository for. Recording anything narrower would make the test unable to see the
// defect it exists to prevent: a filter that the service accepts and then fails to pass
// down would be invisible if only the page were captured.
type dltPageRequest = model.DeadLetterQuery

// dltFakeStore is an in-memory eventDeadLetterStore.
//
// It models the three repository behaviours the dead-letter operations actually depend
// on, and models them faithfully rather than conveniently:
//
//   - GetEventByID reports a missing row with the TYPED not-found error the real
//     repository constructs (the legacy apierror.ErrNotFound code), because the
//     classification of that error is itself under test.
//   - ListDeadLetteredEvents pages an inventory held in the repository's own order —
//     newest occurrence first — so offset arithmetic is exercised the way production
//     exercises it, including the oldest-end walk the age gauge performs.
//   - ListDeadLetteredEventsFiltered and CountDeadLetteredEvents apply the filter
//     PREDICATES THEMSELVES, mirroring deadLetterFilterClause: exact comparison, a status
//     filter replacing the two-state default, limit and offset counting MATCHES rather
//     than rows, and both narrowing off the same inventory so the listing and the count
//     cannot silently describe different sets. Modelling the predicates here rather than
//     in the service is the point — it is what makes a service that filtered in Go fail
//     these tests.
//   - MarkEventDispatched MUTATES the row to dispatched and DROPS IT from the inventory,
//     which is what the real query does implicitly by filtering on the two terminal
//     failure states. That is what makes "a replayed event is no longer listed" and "a
//     second replay is refused" observable here at all.
//
// It is mutex-guarded so the whole file is safe under `go test -race`.
type dltFakeStore struct {
	mu sync.Mutex

	// rows holds every row by business event id, addressable for lookup.
	rows map[string]*model.EventOutbox
	// inventory holds the event ids of the rows in the dead-letter listing, in
	// repository order: newest occurrence first.
	inventory []string
	// counts is what CountEventOutboxByStatus reports. A status with no rows is absent,
	// exactly as GROUP BY leaves it.
	counts map[string]int64

	// Injectable failures, each covering one documented error path.
	//
	// listErr covers BOTH listing entry points, because both are the same statement to a
	// caller: a repository that cannot page the inventory cannot page a filtered slice of
	// it either, and a fake that failed only one would let a propagation test pass while
	// the path the API actually uses swallowed the error.
	getErr              error
	listErr             error
	countErr            error
	countDeadLetterErr  error
	markDeadLetteredErr error
	markDispatchedErr   error
	claimReplayErr      error
	releaseReplayErr    error

	// Recorded calls.
	deadLettered []dltMarkRecord
	dispatched   []dltDispatchRecord
	listCalls    []dltPageRequest
	getCalls     []string
	countCalls   int

	// countDeadLetterCalls records the narrowing every CountDeadLetteredEvents call was
	// given, so a test can assert the total was counted over the SAME predicate as the
	// page rather than over the whole table.
	countDeadLetterCalls []dltPageRequest

	// inventoryQueries and countQueries record the query every SQL-narrowed inventory read
	// and count was given, so a test can prove the predicate reached the repository rather
	// than being applied above it.
	inventoryQueries []model.DeadLetterQuery

	// inventoryPages records the KEYSET request each inventory read was made with, so a test can
	// assert the limit the repository was asked for and the cursor it was handed — neither of
	// which survives the narrowing recorded above.
	inventoryPages []model.DeadLetterInventoryQuery
	countQueries   []model.DeadLetterQuery

	// ageCalls counts the grouped age reads and ageErr drives their failure, so the gauge's
	// degraded path is reachable and the aggregate is provably read once per refresh rather
	// than once per topic.
	ageCalls int
	ageErr   error

	replayClaims  []string
	replayRelease []dltReleaseRecord

	// nextClaimToken is the token ClaimEventForReplay hands out. It is a field rather
	// than a generated value so a test can assert that the token the service presents to
	// its follow-up transition is EXACTLY the one the claim issued — which is the whole
	// mechanism, and a generated token would make it unassertable.
	nextClaimToken string
}

// dltDispatchRecord is one MarkEventDispatched call, with the token it presented.
//
// The token is recorded because a transition that ignored it would be indistinguishable
// from one that honoured it if only the row id were captured — and ignoring it is exactly
// the defect the token exists to prevent.
type dltDispatchRecord struct {
	id         int64
	claimToken string

	// record is the broker coordinate of the REPLAY write, so a replayed row names the new
	// record rather than the dead-letter one it was read from.
	record model.BrokerRecord
}

// dltReleaseRecord is one ReleaseEventReplay call: the rollback that keeps a failed
// replay replayable.
type dltReleaseRecord struct {
	id         int64
	claimToken string
	replayErr  string

	// contextErr is what the context reported at the moment the release was entered. It is
	// what proves the release did not inherit the caller's cancellation: the service runs it
	// on context.WithoutCancel, so this must be nil even when the caller's context is dead.
	contextErr error

	// hasDeadline and deadline record the bound the release was given. Detachment alone is
	// not enough — a rollback on a context that can never expire would hold a request open
	// against a database that has gone away — so the service bounds it, and that bound is
	// asserted here rather than assumed.
	hasDeadline bool
	deadline    time.Time
}

// dltFakeStore must satisfy the seam it stands in for, and must fail the build here if
// the seam changes rather than at whichever call site happens to be compiled first.
var _ eventDeadLetterStore = (*dltFakeStore)(nil)

// newDltFakeStore builds an empty store.
func newDltFakeStore() *dltFakeStore {
	return &dltFakeStore{
		rows:   map[string]*model.EventOutbox{},
		counts: map[string]int64{},
	}
}

// withRow registers a row for lookup and, when its status is one of the two failure
// states, appends it to the dead-letter inventory.
//
// Callers add rows NEWEST FIRST, which is the order the repository returns them in.
func (s *dltFakeStore) withRow(row model.EventOutbox) *dltFakeStore {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored := row
	s.rows[row.EventID] = &stored

	// BOTH failure states join the inventory, matching ListDeadLetterInventory, which covers
	// dead_lettered and failed. failed especially: a failed row whose dlt_topic is still NULL
	// is the "dead-letter write is owed" state, so while it sits there its event exists in no
	// topic at all, which makes it the one an operator most needs to see.
	switch row.Status {
	case model.EventOutboxStatusDeadLettered,
		model.EventOutboxStatusFailed:
		s.inventory = append(s.inventory, row.EventID)
		s.counts[row.Status]++
	}

	return s
}

// withCount overrides a status count, for the cases where the counts and the inventory
// must deliberately disagree — a count that raced an insert, or a drained inventory.
func (s *dltFakeStore) withCount(status string, count int64) *dltFakeStore {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counts[status] = count

	return s
}

// GetEventByID returns a copy of the stored row, or the repository's typed not-found
// error.
func (s *dltFakeStore) GetEventByID(_ context.Context, eventID string) (*model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.getCalls = append(s.getCalls, eventID)

	if s.getErr != nil {
		return nil, s.getErr
	}

	row, ok := s.rows[eventID]
	if !ok {
		return nil, apierror.NewAPIError(apierror.ErrNotFound, "Event not found", sql.ErrNoRows)
	}

	copied := *row

	return &copied, nil
}

// ListDeadLetteredEvents applies the query's narrowing and then pages what matches,
// newest first.
//
// The FILTERING HAPPENS HERE, in the fake repository, because that is where it happens in
// production: the predicates are rendered into the SQL WHERE clause. It used to happen in
// the service, above this boundary, and a fake that still filtered above would let a
// service which silently dropped a filter pass.
//
// The consequence for the page arithmetic is the one that matters: limit and offset are
// applied to the MATCHING rows, so a filtered page is a page of the filtered set rather
// than a filtered page of the unfiltered one.
func (s *dltFakeStore) ListDeadLetteredEvents(_ context.Context, query model.DeadLetterQuery) ([]model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.listCalls = append(s.listCalls, query)

	if s.listErr != nil {
		return nil, s.listErr
	}

	matching := s.matchingLocked(query)

	limit := query.Limit
	if limit <= 0 {
		limit = defaultDeadLetterListLimit
	}
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}

	if offset >= len(matching) {
		// The repository returns a nil slice for a page past the end. Reproducing that
		// exactly is what lets the nil-to-empty normalisation be asserted.
		return nil, nil
	}

	end := offset + limit
	if end > len(matching) {
		end = len(matching)
	}

	page := make([]model.EventOutbox, 0, end-offset)
	page = append(page, matching[offset:end]...)

	return page, nil
}

// CountDeadLetteredEvents counts what the SAME narrowing matches, ignoring the page.
//
// It is deliberately driven from matchingLocked, the same predicate the listing uses, so
// this fake cannot exhibit the very defect the production count exists to rule out — a
// total describing a different set than the page it accompanies.
func (s *dltFakeStore) CountDeadLetteredEvents(_ context.Context, query model.DeadLetterQuery) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.countDeadLetterCalls = append(s.countDeadLetterCalls, query)

	if s.countDeadLetterErr != nil {
		return 0, s.countDeadLetterErr
	}

	return int64(len(s.matchingLocked(query))), nil
}

// matchingLocked returns the inventory rows the query's predicates admit, in repository
// order. The caller must hold the mutex.
//
// Comparison is exact and case-sensitive throughout, and the topic predicate is applied to
// the row's ORIGINAL topic column, both matching the SQL. The occurrence window is
// inclusive at both ends, so a window whose bounds are equal selects the events at exactly
// that instant.
func (s *dltFakeStore) matchingLocked(query model.DeadLetterQuery) []model.EventOutbox {
	matching := make([]model.EventOutbox, 0, len(s.inventory))
	for _, eventID := range s.inventory {
		row := *s.rows[eventID]
		if dltInventoryMatches(row, query) {
			matching = append(matching, row)
		}
	}

	return matching
}

// CountUnresolvedEventOutbox returns a copy of the status counts.
//
// It takes no window, matching the aggregate the dead-letter seam now declares (PERF-M05): the
// `failed` count this service reads is exact and complete for all time, and the dispatched
// history the previous windowed aggregate also counted was read by nobody.
func (s *dltFakeStore) CountUnresolvedEventOutbox(_ context.Context) (map[string]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.countCalls++

	if s.countErr != nil {
		return nil, s.countErr
	}

	counts := make(map[string]int64, len(s.counts))
	for status, count := range s.counts {
		if count > 0 {
			counts[status] = count
		}
	}

	return counts, nil
}

// MarkEventDeadLettered records the dead-letter topic and metadata on the row and moves it
// to the dead-lettered state, adding it to the inventory when it was not already there.
func (s *dltFakeStore) MarkEventDeadLettered(
	_ context.Context,
	id int64,
	claimToken, dltTopic string,
	failureMetadata json.RawMessage,
	record model.BrokerRecord,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deadLettered = append(s.deadLettered, dltMarkRecord{
		id: id, claimToken: claimToken, dltTopic: dltTopic, metadata: failureMetadata,
		record: record,
	})

	if s.markDeadLetteredErr != nil {
		return s.markDeadLetteredErr
	}

	for eventID, row := range s.rows {
		if row.ID != id {
			continue
		}

		row.Status = model.EventOutboxStatusDeadLettered
		row.DLTTopic = dltTopic
		row.FailureMetadata = failureMetadata
		if record.Confirmed() {
			partition := record.Partition
			offset := record.Offset
			row.KafkaTopic = record.Topic
			row.KafkaPartition = &partition
			row.KafkaOffset = &offset
		}

		if !dltContainsString(s.inventory, eventID) {
			// Newest first, matching the repository's ordering.
			s.inventory = append([]string{eventID}, s.inventory...)
			s.counts[model.EventOutboxStatusDeadLettered]++
		}

		break
	}

	return nil
}

// MarkEventDispatched moves the row to dispatched and removes it from the dead-letter
// inventory, mirroring the repository's status filter.
func (s *dltFakeStore) MarkEventDispatched(
	_ context.Context,
	id int64,
	claimToken string,
	record model.BrokerRecord,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dispatched = append(s.dispatched, dltDispatchRecord{
		id: id, claimToken: claimToken, record: record,
	})

	if s.markDispatchedErr != nil {
		return s.markDispatchedErr
	}

	for eventID, row := range s.rows {
		if row.ID != id {
			continue
		}

		previous := row.Status
		dispatchedAt := dltFixedNow
		row.Status = model.EventOutboxStatusDispatched
		row.DispatchedAt = &dispatchedAt

		// dlt_topic and failure_metadata are RETAINED on purpose: the history of what
		// went wrong survives the replay. Only the listing membership changes.
		remaining := make([]string, 0, len(s.inventory))
		for _, candidate := range s.inventory {
			if candidate != eventID {
				remaining = append(remaining, candidate)
			}
		}
		s.inventory = remaining

		if s.counts[previous] > 0 {
			s.counts[previous]--
		}

		break
	}

	return nil
}

// ClaimEventForReplay is the atomic dead_lettered -> replaying transition, faithful to
// the repository's discrimination between the two failure causes.
//
// It is a CLAIM and not a lookup, and modelling that faithfully in the fake is what lets
// the concurrency test mean anything: the second claim of one row must fail because the
// FIRST one moved it, not because the fake was told to fail.
func (s *dltFakeStore) ClaimEventForReplay(
	_ context.Context,
	eventID string,
	_ time.Duration,
) (*model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.replayClaims = append(s.replayClaims, eventID)

	if s.claimReplayErr != nil {
		return nil, s.claimReplayErr
	}
	// getErr models a database that is unreachable, and the CLAIM is now the first read
	// a replay performs — so it has to fail the same way GetEventByID would, or a
	// database outage would be reported as a missing event.
	if s.getErr != nil {
		return nil, s.getErr
	}

	row, ok := s.rows[eventID]
	if !ok {
		return nil, apierror.NewAPIError(apierror.ErrNotFound, "Event not found", nil)
	}

	if row.Status != model.EventOutboxStatusDeadLettered {
		return nil, apierror.NewAPIError(apierror.ErrConflict,
			fmt.Sprintf("Event is not available for replay: its status is %q", row.Status), nil)
	}

	token := s.nextClaimToken
	if token == "" {
		token = "replay-claim-token"
	}

	row.Status = model.EventOutboxStatusReplaying
	row.ClaimToken = token

	// The status count moves with the row, exactly as a real GROUP BY would. Leaving it on
	// dead_lettered would make the age gauge keep reporting an entry that is no longer in
	// that state, so the fake's bookkeeping has to be as faithful as the transition itself.
	if s.counts[model.EventOutboxStatusDeadLettered] > 0 {
		s.counts[model.EventOutboxStatusDeadLettered]--
	}
	s.counts[model.EventOutboxStatusReplaying]++

	claimed := *row
	return &claimed, nil
}

// ReleaseEventReplay is the rollback: replaying -> dead_lettered, recording the reason.
//
// # It OBSERVES the context, and that is not incidental
//
// The release runs on a context the service DETACHES from the caller's cancellation and bounds
// with its own timeout, because the commonest way a replay fails is the caller going away —
// a closed browser tab, a proxy timeout, a deploy taking the process down mid-replay. Run on
// the caller's context instead, the rollback fails for exactly the reason the replay did, every
// time, and the row is left in `replaying` holding a token nobody has.
//
// A fake that ignored the context could not tell those two implementations apart: both call
// this method with the same arguments and both record the same release. So this one behaves
// like the database — it REFUSES a cancelled context — and records the deadline it was given,
// which is what lets a test assert the detachment rather than trust it.
func (s *dltFakeStore) ReleaseEventReplay(ctx context.Context, id int64, claimToken, replayErr string) error {
	deadline, hasDeadline := ctx.Deadline()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.replayRelease = append(s.replayRelease, dltReleaseRecord{
		id: id, claimToken: claimToken, replayErr: replayErr,
		contextErr:  ctx.Err(),
		hasDeadline: hasDeadline,
		deadline:    deadline,
	})

	// A REAL DATABASE CALL FAILS ON A DEAD CONTEXT, so this one does too. lib/pq checks the
	// context before it writes and cancels the statement if it is already done, so a release
	// handed a cancelled context does not perform the update — it returns the cancellation.
	if err := ctx.Err(); err != nil {
		return err
	}

	if s.releaseReplayErr != nil {
		return s.releaseReplayErr
	}

	for eventID, row := range s.rows {
		if row.ID != id {
			continue
		}
		previous := row.Status
		row.Status = model.EventOutboxStatusDeadLettered
		row.ClaimToken = ""
		if replayErr != "" {
			row.LastError = replayErr
		}
		if !dltContainsString(s.inventory, eventID) {
			s.inventory = append([]string{eventID}, s.inventory...)
		}
		if s.counts[previous] > 0 {
			s.counts[previous]--
		}
		s.counts[model.EventOutboxStatusDeadLettered]++
		break
	}

	return nil
}

// snapshotReplayClaims returns the event ids ClaimEventForReplay was called with.
func (s *dltFakeStore) snapshotReplayClaims() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	claims := make([]string, len(s.replayClaims))
	copy(claims, s.replayClaims)
	return claims
}

// snapshotReplayReleases returns the rollback calls, with the tokens they presented.
func (s *dltFakeStore) snapshotReplayReleases() []dltReleaseRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	releases := make([]dltReleaseRecord, len(s.replayRelease))
	copy(releases, s.replayRelease)
	return releases
}

// row returns a copy of a stored row for assertion.
func (s *dltFakeStore) row(t *testing.T, eventID string) model.EventOutbox {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	row, ok := s.rows[eventID]
	require.True(t, ok, "the fake store must hold event %q", eventID)

	return *row
}

// snapshotDeadLettered returns a copy of the recorded MarkEventDeadLettered calls.
func (s *dltFakeStore) snapshotDeadLettered() []dltMarkRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]dltMarkRecord, len(s.deadLettered))
	copy(records, s.deadLettered)

	return records
}

// snapshotDispatched returns a copy of the recorded MarkEventDispatched calls, each with
// the claim token it presented.
func (s *dltFakeStore) snapshotDispatched() []dltDispatchRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]dltDispatchRecord, len(s.dispatched))
	copy(records, s.dispatched)

	return records
}

// snapshotDispatchedIDs projects just the row ids, for assertions that are about WHICH
// rows were dispatched rather than about the tokens.
func (s *dltFakeStore) snapshotDispatchedIDs() []int64 {
	ids := make([]int64, 0, len(s.dispatched))
	for _, record := range s.snapshotDispatched() {
		ids = append(ids, record.id)
	}

	return ids
}

// snapshotDeadLetterCounts returns the narrowing every CountDeadLetteredEvents call was
// given, oldest first, so a test can assert the total was counted over the SAME predicate
// the page was drawn with.
func (s *dltFakeStore) snapshotDeadLetterCounts() []dltPageRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]dltPageRequest(nil), s.countDeadLetterCalls...)
}

// snapshotPages returns a copy of the recorded listing requests.
func (s *dltFakeStore) snapshotPages() []dltPageRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	pages := make([]dltPageRequest, len(s.listCalls))
	copy(pages, s.listCalls)

	return pages
}

// snapshotCountQueries returns a copy of the narrowings the count was asked for.
func (s *dltFakeStore) snapshotCountQueries() []dltPageRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	queries := make([]dltPageRequest, len(s.countDeadLetterCalls))
	copy(queries, s.countDeadLetterCalls)

	return queries
}

// dltContainsString reports whether values contains target.
func dltContainsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}

	return false
}

// dltWrittenMessage is one message the fake transport was asked to write, together with
// the topic whose writer wrote it.
//
// The topic is recorded alongside the message rather than read from it because the
// production writers are per-topic and kafka-go rejects a message that names a topic when
// its writer already has one — so kafka.Message.Topic is legitimately empty, and asserting
// on it would assert the wrong thing.
type dltWrittenMessage struct {
	topic   string
	message kafka.Message
}

// dltFakeTransport stands in for the dead-letter writer pool.
//
// It answers a deadLetterWriterResolver and hands back a per-topic writer, which is the
// same shape the production resolver has: the Kafka publisher returns the writer it
// already holds for that topic. Every resolution and every write is recorded, so the
// destination, the key and the message BYTES are all assertable with no broker.
type dltFakeTransport struct {
	mu sync.Mutex

	// requested is every topic the resolver was asked for, in order.
	requested []string
	// written is every message that reached a writer, in order.
	written []dltWrittenMessage

	// resolveErr makes resolution fail, modelling a transport that cannot produce a
	// writer for the topic.
	resolveErr error
	// writeErr makes the write fail, modelling a broker that rejects or times out.
	writeErr error
	// noWriter makes resolution return (nil, nil): a deployment with no Kafka transport
	// at all, which is a legitimate steady state rather than an error.
	noWriter bool
}

// resolver returns the resolver this transport answers.
func (tr *dltFakeTransport) resolver() deadLetterWriterResolver {
	return func(topic string) (deadLetterMessageWriter, error) {
		tr.mu.Lock()
		tr.requested = append(tr.requested, topic)
		resolveErr := tr.resolveErr
		noWriter := tr.noWriter
		tr.mu.Unlock()

		if resolveErr != nil {
			return nil, resolveErr
		}
		if noWriter {
			return nil, nil
		}

		return &dltFakeWriter{transport: tr, topic: topic}, nil
	}
}

// snapshotWritten returns a copy of the recorded writes.
func (tr *dltFakeTransport) snapshotWritten() []dltWrittenMessage {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	written := make([]dltWrittenMessage, len(tr.written))
	copy(written, tr.written)

	return written
}

// snapshotRequested returns a copy of the topics the resolver was asked for.
func (tr *dltFakeTransport) snapshotRequested() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	requested := make([]string, len(tr.requested))
	copy(requested, tr.requested)

	return requested
}

// dltFakeWriter is the per-topic writer the fake transport resolves to.
type dltFakeWriter struct {
	transport *dltFakeTransport
	topic     string
}

var _ deadLetterMessageWriter = (*dltFakeWriter)(nil)

// WriteMessages records the messages, or fails when the transport is configured to.
func (w *dltFakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.transport.mu.Lock()
	defer w.transport.mu.Unlock()

	if w.transport.writeErr != nil {
		return w.transport.writeErr
	}

	for _, message := range msgs {
		w.transport.written = append(w.transport.written, dltWrittenMessage{
			topic: w.topic, message: message,
		})
	}

	return nil
}

// dltFakePublisher is a TopicEventPublisher that records what it was asked to publish.
//
// It reproduces the real publisher's RESULT SHAPE by resolving the topic, key and attempt
// through the very functions the Kafka implementation uses, so a caller reading the result
// sees the same fields either way and an assertion on the result is an assertion about
// production behaviour rather than about this fake.
//
// Deliberately, it is neither the Kafka publisher nor the no-op, which is what makes it
// usable to exercise the third arm of publisherWriterResolver: a publisher that cannot be
// written to with a composed message must be refused loudly rather than silently skipped.
type dltFakePublisher struct {
	mu sync.Mutex

	// requests is every publish request received, in order.
	requests []PublishRequest
	// err makes the publish fail.
	err error
	// closes counts Close calls, so publisher ownership can be asserted.
	closes int

	// onPublish runs before the request is recorded, for a test that needs something to happen
	// at the one instant the row is claimed and the replay is not yet over. Cancelling the
	// caller's context from here is the only way to reach the rollback path with a dead caller,
	// which is the state that path exists for.
	onPublish func()
}

var _ TopicEventPublisher = (*dltFakePublisher)(nil)

// Publish satisfies the mandated minimal contract by delegating to PublishToTopic, exactly
// as the real implementations do.
func (p *dltFakePublisher) Publish(ctx context.Context, event model.LedgerEvent) error {
	_, err := p.PublishToTopic(ctx, PublishRequest{Event: event})

	return err
}

// PublishToTopic records the request and reports a result shaped like the real one.
func (p *dltFakePublisher) PublishToTopic(_ context.Context, req PublishRequest) (PublishResult, error) {
	p.mu.Lock()
	hook := p.onPublish
	p.mu.Unlock()

	// Run OUTSIDE the lock: the hook exists to disturb the world mid-replay, and a hook that
	// needed this publisher would otherwise deadlock against it.
	if hook != nil {
		hook()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.requests = append(p.requests, req)

	result := PublishResult{
		Status:       model.PublishStatusDispatched,
		EventID:      req.Event.EventID,
		EventType:    req.Event.EventType,
		Topic:        resolveTopic(req),
		PartitionKey: resolvePartitionKey(req),
		Attempt:      resolveAttempt(req),
	}

	if p.err != nil {
		result.Status = model.PublishStatusRetrying
		result.Transient = true
		result.Err = p.err

		return result, p.err
	}

	return result, nil
}

// Close records the call.
func (p *dltFakePublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closes++

	return nil
}

// snapshotRequests returns a copy of the recorded publish requests.
func (p *dltFakePublisher) snapshotRequests() []PublishRequest {
	p.mu.Lock()
	defer p.mu.Unlock()

	requests := make([]PublishRequest, len(p.requests))
	copy(requests, p.requests)

	return requests
}

// dltCounterRecord is one recorded counter increment: the value and its attributes.
type dltCounterRecord struct {
	value      int64
	attributes map[string]string
}

// dltRecordedCounter stands in for a shared Int64Counter so that both the number of
// increments AND their attributes can be asserted.
//
// embedded.Int64Counter is EMBEDDED rather than implemented: that is how the OpenTelemetry
// API intends a third-party implementation of an instrument interface to be written, and it
// is what keeps this compiling if the interface gains a method.
type dltRecordedCounter struct {
	embedded.Int64Counter

	mu      sync.Mutex
	records []dltCounterRecord
}

var _ otelmetric.Int64Counter = (*dltRecordedCounter)(nil)

// Add records one increment and its attributes.
func (c *dltRecordedCounter) Add(_ context.Context, value int64, options ...otelmetric.AddOption) {
	recorded := otelmetric.NewAddConfig(options).Attributes()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = append(c.records, dltCounterRecord{
		value:      value,
		attributes: dltAttributeMap(recorded.ToSlice()),
	})
}

// Enabled reports that this recorder always processes measurements, which is what makes the
// assertions deterministic.
func (c *dltRecordedCounter) Enabled(context.Context) bool {
	return true
}

// snapshot returns a copy of the recorded increments.
func (c *dltRecordedCounter) snapshot() []dltCounterRecord {
	c.mu.Lock()
	defer c.mu.Unlock()

	records := make([]dltCounterRecord, len(c.records))
	copy(records, c.records)

	return records
}

// total sums the recorded increments, which is the value the dead-letter RATE is computed
// from.
func (c *dltRecordedCounter) total() int64 {
	var total int64
	for _, record := range c.snapshot() {
		total += record.value
	}

	return total
}

// dltFloatGaugeRecord is one recorded gauge measurement.
type dltFloatGaugeRecord struct {
	value      float64
	attributes map[string]string
}

// dltRecordedFloatGauge stands in for a shared Float64Gauge, keeping every measurement so
// that the value per topic can be asserted — including the zero a drained topic must
// publish.
type dltRecordedFloatGauge struct {
	embedded.Float64Gauge

	mu      sync.Mutex
	records []dltFloatGaugeRecord
}

var _ otelmetric.Float64Gauge = (*dltRecordedFloatGauge)(nil)

// Record keeps the measurement and its attributes.
func (g *dltRecordedFloatGauge) Record(_ context.Context, value float64, options ...otelmetric.RecordOption) {
	recorded := otelmetric.NewRecordConfig(options).Attributes()

	g.mu.Lock()
	defer g.mu.Unlock()

	g.records = append(g.records, dltFloatGaugeRecord{
		value:      value,
		attributes: dltAttributeMap(recorded.ToSlice()),
	})
}

// Enabled reports that this recorder always processes measurements.
func (g *dltRecordedFloatGauge) Enabled(context.Context) bool {
	return true
}

// byTopic returns the recorded value per topic attribute, which is the shape every
// assertion about the age gauge is expressed in.
//
// A topic recorded more than once keeps the LAST value, matching how a gauge is read.
func (g *dltRecordedFloatGauge) byTopic() map[string]float64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	values := map[string]float64{}
	for _, record := range g.records {
		values[record.attributes[publishAttrTopic]] = record.value
	}

	return values
}

// count returns how many measurements were recorded.
func (g *dltRecordedFloatGauge) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return len(g.records)
}

// dltAttributeMap flattens recorded attributes to a comparable map.
//
// Value.Emit is the rendering accessor of the attribute package version this module
// pins, and it renders the string, int64 and bool attributes this pipeline records
// exactly as the value was recorded.
func dltAttributeMap(pairs []attribute.KeyValue) map[string]string {
	attributes := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		attributes[string(pair.Key)] = pair.Value.Emit()
	}

	return attributes
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// dltPinTopicPrefix publishes a configuration carrying only the default topic prefix, so
// every derived topic name in a test is deterministic.
//
// It delegates to the shared storeKafkaTopicPrefix helper, which SAVES AND RESTORES
// config.ConfigStore through t.Cleanup. That restoration is what keeps this file from
// leaking state into the rest of the package's tests: the configuration store is an
// atomic.Value shared by the whole test binary, and a prefix left behind would silently
// rename the topics another test asserts on.
func dltPinTopicPrefix(t *testing.T) {
	t.Helper()

	storeKafkaTopicPrefix(t, DefaultTopicPrefix)
}

// dltExhaustedRow builds the outbox row of an event that has just spent its entire retry
// budget — the row the relay hands to the dead-letter path.
//
// Its status is FAILED with dlt_topic still EMPTY, and that pair is the whole point of the
// fixture. It is exactly what the relay's exhaustion arm writes, and it is the state that
// means "the retry budget is spent and the dead-letter write is still owed". The pair is NOT
// terminal: the row keeps its lease so no second worker writes the same event to a .dlt
// topic, and once that lease lapses ClaimFailedEventOutboxForDeadLetter re-claims it, which is
// what keeps a failed dead-letter write recoverable instead of stranding the event. Seeding
// dead_lettered here would model a hand-off that has already completed and would certify the
// opposite of what these tests exist to prove. The dead-letter listing covers failed and
// dead_lettered alike, so an operator sees the row in either state.
//
// The attempt window is genuinely SPREAD: the first attempt is 31 seconds before the last,
// which is what the documented backoff schedule (1s, 2s, 4s, 8s, 16s across five attempts)
// produces. Two distinct instants are essential rather than cosmetic — a fixture whose
// first and last attempts coincided would pass even if the implementation assigned them
// the wrong way round.
//
// Parameters:
//   - eventID: the business event id, which is also the subscriber idempotency key.
//   - eventType: the event name, which decides the category and therefore the topic.
//   - topic: the destination recorded on the row. Pass "" to exercise the fallback that
//     re-derives it from the event type.
func dltExhaustedRow(t *testing.T, eventID, eventType, topic string) model.EventOutbox {
	t.Helper()

	firstAttempt := dltFixedNow.Add(-1 * time.Minute)
	lastAttempt := firstAttempt.Add(31 * time.Second)

	row := model.EventOutbox{
		ID:          dltRowID(eventID),
		EventID:     eventID,
		EventType:   eventType,
		AggregateID: "txn_9c2f4a17",
		// THREE DIFFERENT VALUES in the three columns, deliberately.
		//
		// A real transaction row looks exactly like this: the partition key is the
		// source balance the transaction moved value from, the aggregate is the
		// transaction itself, and the ledger column is populated only when the payload
		// carries a ledger. Making all three distinct is what lets an assertion on the
		// message key mean something — a fixture that left the partition key blank, or
		// set it equal to the ledger, would pass whether the implementation keyed on
		// partition_key, ledger_id or aggregate_id, which is precisely how the key
		// diverging from the column the claim query serialises on went unnoticed.
		PartitionKey:     "bln_7d3ac6f1",
		LedgerID:         "ldg_5f1b8e04",
		Topic:            topic,
		SchemaVersion:    model.SchemaVersionV1,
		Payload:          json.RawMessage(dltTrapPayload),
		OccurredAt:       dltOccurredAt,
		Status:           model.EventOutboxStatusFailed,
		Attempts:         dltExhaustedAttempts,
		MaxAttempts:      dltExhaustedAttempts,
		LastError:        dltPublishFailureReason,
		FirstAttemptedAt: &firstAttempt,
		LastAttemptedAt:  &lastAttempt,
	}

	dltStampCanonicalEnvelope(t, &row)

	return row
}

// dltStampCanonicalEnvelope fills event_raw the way PrepareEventOutbox does at capture.
//
// The column is NOT NULL and it is what every transport reads, so a fixture without it is a
// row the database cannot hold and a scenario that exercises the compatibility fallback rather
// than the live path. It is stamped LAST, after every envelope member is final, because the
// stored bytes and the columns they were composed from must agree — a fixture that changed
// occurred_at after stamping would describe a row no writer could produce.
func dltStampCanonicalEnvelope(t *testing.T, row *model.EventOutbox) {
	t.Helper()

	raw, err := row.CanonicalEvent().CanonicalBytes()
	require.NoError(t, err, "the fixture envelope must serialise before it can be stored")
	row.EventRaw = raw
}

// dltRowID derives a stable, strictly positive surrogate key from an event id.
//
// Distinct ids per fixture matter rather than being tidiness: the store's state transitions are
// addressed BY ROW ID, so two fixtures sharing one id would let a transition mutate the wrong
// row — a defect that would surface as an intermittent failure depending on map iteration order.
// Deriving the id from the event id keeps every fixture distinct and every run identical.
func dltRowID(eventID string) int64 {
	// FNV-1a, spelled out so the value is obviously deterministic and obviously positive.
	hash := uint64(14695981039346656037)
	for i := 0; i < len(eventID); i++ {
		hash ^= uint64(eventID[i])
		hash *= 1099511628211
	}

	return int64(hash%1_000_000_000) + 1
}

// dltNewService builds a dead-letter service on a fixed clock with the fake transport
// installed.
//
// withTransport is the seam the implementation declares for exactly this: it installs the
// publisher and the writer resolver as ONE assignment, so transport() never falls through
// to building a real publisher from configuration and no test can accidentally dial a
// broker.
func dltNewService(store eventDeadLetterStore, publisher TopicEventPublisher, transport *dltFakeTransport) *EventDeadLetterService {
	service := NewEventDeadLetterService(store, nil)
	service.now = func() time.Time { return dltFixedNow }
	service.withTransport(publisher, transport.resolver())

	return service
}

// dltCaptureDeadLetterCounter swaps the shared dead-letter counter for a recorder.
//
// Swapping the package-level instrument keeps the test local: no global meter provider is
// installed, so no other test in the binary is affected, and the real instrument is
// restored on cleanup.
func dltCaptureDeadLetterCounter(t *testing.T) *dltRecordedCounter {
	t.Helper()

	recorder := &dltRecordedCounter{}
	original := metrics.EventsDeadLetteredTotal
	t.Cleanup(func() { metrics.EventsDeadLetteredTotal = original })
	metrics.EventsDeadLetteredTotal = recorder

	return recorder
}

// dltCaptureBrokerAcknowledgements swaps the shared broker-acknowledgement counter for a
// recorder, so the PURPOSE a dead-letter write is counted under can be asserted.
//
// The instrument is shared with the ordinary publish path, which is the whole point: the
// counter's value is that the three purposes are summable on one query, and a dead-letter
// write that recorded nothing left the documented triage breakdown permanently unanswerable.
func dltCaptureBrokerAcknowledgements(t *testing.T) *dltRecordedCounter {
	t.Helper()

	recorder := &dltRecordedCounter{}
	original := metrics.EventBrokerAcknowledgementsTotal
	t.Cleanup(func() { metrics.EventBrokerAcknowledgementsTotal = original })
	metrics.EventBrokerAcknowledgementsTotal = recorder

	return recorder
}

// dltCapturePublishAttempts swaps the shared publish-attempt counter for a recorder, so the
// outcome label a dead-letter write is recorded under can be asserted.
func dltCapturePublishAttempts(t *testing.T) *dltRecordedCounter {
	t.Helper()

	recorder := &dltRecordedCounter{}
	original := metrics.EventPublishAttemptsTotal
	t.Cleanup(func() { metrics.EventPublishAttemptsTotal = original })
	metrics.EventPublishAttemptsTotal = recorder

	return recorder
}

// dltCapturePublishDuration swaps the shared publish-duration histogram for a recorder, so
// the attempt token and the measured duration a dead-letter write records can be asserted.
//
// It reuses the recorder declared beside the publisher's own tests rather than redeclaring
// one: both files exercise the same instrument, and two recorders would let the two files
// disagree about what a recorded observation looks like.
func dltCapturePublishDuration(t *testing.T) *publisherRecordedHistogram {
	t.Helper()

	recorder := &publisherRecordedHistogram{}
	original := metrics.EventPublishDuration
	t.Cleanup(func() { metrics.EventPublishDuration = original })
	metrics.EventPublishDuration = recorder

	return recorder
}

// dltCaptureAgeGauge swaps the shared dead-letter age gauge for a recorder.
func dltCaptureAgeGauge(t *testing.T) *dltRecordedFloatGauge {
	t.Helper()

	recorder := &dltRecordedFloatGauge{}
	original := metrics.DLTOldestMessageAgeSeconds
	t.Cleanup(func() { metrics.DLTOldestMessageAgeSeconds = original })
	metrics.DLTOldestMessageAgeSeconds = recorder

	return recorder
}

// dltOriginalEnvelope returns the bytes the FIRST publish of this row produced.
//
// It goes through the production path — the row-to-request conversion and the value resolution
// the Kafka publisher itself performs — so it is the real message value and not a test's idea of
// one. For a row carrying event_raw, which every fixture here does, that resolution returns the
// STORED envelope. Every byte-fidelity assertion in this file is stated against it.
func dltOriginalEnvelope(t *testing.T, row model.EventOutbox) []byte {
	t.Helper()

	envelope, err := resolveEventValue(PublishRequestFromOutbox(row, 1))
	require.NoError(t, err, "the fixture row must serialise as a publishable envelope")

	return envelope
}

// dltDeadLetterRow returns the row as it stands AFTER a dead-lettering, as the repository
// would have left it: dead-lettered, with the dead-letter topic and the metadata bytes
// recorded.
//
// The metadata bytes are the ones the outcome carries, not a re-serialisation of the
// decoded record, because the point of the replay assertions is that the STORED bytes are
// what a replay reads.
func dltDeadLetterRow(row model.EventOutbox, outcome DeadLetterOutcome) model.EventOutbox {
	row.Status = model.EventOutboxStatusDeadLettered
	row.DLTTopic = outcome.DeadLetterTopic
	row.FailureMetadata = outcome.MetadataJSON

	return row
}

// dltAPIError extracts the typed APIError from an error, failing the test when there is
// none.
//
// APIError is a VALUE type in this codebase and does not unwrap to the error it wrapped, so
// the CODE is what has to be inspected — which is precisely why every assertion below reads
// the code rather than matching on a message.
func dltAPIError(t *testing.T, err error) apierror.APIError {
	t.Helper()

	require.Error(t, err, "an error was expected")

	var apiErr apierror.APIError
	require.True(t, errors.As(err, &apiErr), "the error must be a typed apierror.APIError, got %T: %v", err, err)

	return apiErr
}

// dltAssertCodeAndStatus asserts both halves of the error convention at once: the domain
// returned the intended CODE, and that code has an explicit statusByCode entry resolving to
// the intended HTTP STATUS.
//
// Both halves are needed. An unmapped code silently resolves to 500, so a test that checked
// only the code would pass while the API contract was wrong.
//
// Codes are compared through Normalize because internal layers in this codebase still
// construct the pre-catalog generic codes — the row-validation guard returns INVALID_INPUT —
// while a response always surfaces the canonical GEN_* replacement. Comparing the canonical
// forms asserts the contract a caller actually observes, and the status is asserted for BOTH
// the raw code and its canonical form so neither can be the one missing its mapping.
func dltAssertCodeAndStatus(t *testing.T, err error, code apierror.ErrorCode, status int) {
	t.Helper()

	apiErr := dltAPIError(t, err)
	assert.Equal(t, apierror.Normalize(code), apierror.Normalize(apiErr.Code),
		"the returned error code must be %s (got %s)", code, apiErr.Code)
	assert.Equal(t, status, apierror.StatusForCode(apiErr.Code),
		"apierror.StatusForCode(%s) must resolve to %d through an explicit statusByCode entry; "+
			"an unmapped code silently defaults to 500 and the API contract would be wrong",
		apiErr.Code, status)
	assert.Equal(t, status, apierror.StatusForCode(apierror.Normalize(apiErr.Code)),
		"the canonical form of %s must resolve to %d as well, since that is the code a response carries",
		apiErr.Code, status)
}

// dltEventIDs returns the event ids of a listing page, which is what makes an ordering or
// paging assertion readable.
func dltEventIDs(page model.DeadLetterInventoryPage) []string {
	ids := make([]string, 0, len(page.Entries))
	for _, entry := range page.Entries {
		ids = append(ids, entry.EventID)
	}

	return ids
}

// dltReadSource returns the text of a repository file, so a structural guarantee can be
// asserted rather than trusted.
func dltReadSource(t *testing.T, elements ...string) string {
	t.Helper()

	path := filepath.Join(append([]string{moduleRootDir(t)}, elements...)...)
	contents, err := os.ReadFile(path) //nolint:gosec // a fixed, repository-relative path
	require.NoError(t, err, "%s must be readable to assert its structure", path)

	return string(contents)
}

// dltReadCode returns a Go file's source with EVERY COMMENT REMOVED, so a structural assertion
// judges the code rather than the prose about it.
//
// The distinction is not pedantic: event_dlt.go deliberately WARNS in a comment that reaching for
// a kafka.Reader would mean the scope boundary has been misread, and it names the ".dlt" literal
// in a comment explaining why a blank topic is guarded. A naive substring search over the raw file
// would flag both and would therefore have to be weakened to the point of proving nothing.
// Parsing without comment collection and printing the AST back leaves exactly the executable
// source.
func dltReadCode(t *testing.T, elements ...string) string {
	t.Helper()

	path := filepath.Join(append([]string{moduleRootDir(t)}, elements...)...)

	// Mode 0 does not collect comments, so the printed AST carries none of them.
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	require.NoError(t, err, "%s must be parseable to assert its structure", path)

	var code bytes.Buffer
	require.NoError(t, printer.Fprint(&code, token.NewFileSet(), parsed),
		"%s must be printable back to source", path)

	return code.String()
}

// ---------------------------------------------------------------------------
// Dead-letter routing
// ---------------------------------------------------------------------------

// TestDeadLetterRouting_SendsEachCategoryToItsOwnDeadLetterTopic is the routing guarantee.
//
// An event that has spent its retry budget must land on the `.dlt` sibling of the category
// topic it failed to reach — never on another category's, and never on a name assembled
// some other way. The four expected names are spelled out as LITERALS in dltCategoryRoutes:
// three of them are verbatim user-supplied examples from the requirement, and deriving them
// from DLTFor would make this test agree with the implementation by construction.
//
// The message key is asserted at the same time, because a dead-letter topic that lost the
// key would spread one aggregate's failures across partitions and give up the ordering the
// category topic has.
func TestDeadLetterRouting_SendsEachCategoryToItsOwnDeadLetterTopic(t *testing.T) {
	dltPinTopicPrefix(t)

	for _, route := range dltCategoryRoutes {
		t.Run(route.eventType, func(t *testing.T) {
			row := dltExhaustedRow(t, "evt_"+route.eventType, route.eventType, route.originalTopic)
			store := newDltFakeStore().withRow(row)
			transport := &dltFakeTransport{}
			service := dltNewService(store, &dltFakePublisher{}, transport)

			outcome, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
			require.NoError(t, err)

			assert.Equal(t, route.deadLetterTopic, outcome.DeadLetterTopic,
				"%s must be dead-lettered to %s", route.eventType, route.deadLetterTopic)
			assert.Equal(t, route.originalTopic, outcome.OriginalTopic,
				"the outcome must report the ORIGINAL topic, which is what a replay targets")
			assert.Equal(t, route.originalTopic, outcome.Metadata.OriginalTopic,
				"the stored metadata must record the original topic, never the .dlt sibling")

			written := transport.snapshotWritten()
			require.Len(t, written, 1, "exactly one dead-letter message must be written")
			assert.Equal(t, route.deadLetterTopic, written[0].topic,
				"the writer resolved for %s must be the one that wrote the message", route.deadLetterTopic)
			assert.Equal(t, []byte(row.LedgerID), written[0].message.Key,
				"the dead-letter message must keep the ORIGINAL key so the .dlt topic preserves the "+
					"same per-aggregate ordering as the topic it failed to reach, and requirement R-6's "+
					"key is the ledger the row records")
			assert.Equal(t, row.LedgerID, outcome.PartitionKey)
			assert.NotEqual(t, row.AggregateID, outcome.PartitionKey,
				"the recorded ledger must be used rather than fallen through to the aggregate")
			assert.True(t, outcome.Published, "a resolved writer that accepted the message means published")
			assert.Equal(t, model.PublishStatusDeadLettered, outcome.Status)
		})
	}
}

// TestDeadLetterRouting_DerivesTheTopicFromTheEventTypeWhenTheRowRecordsNone covers the
// documented fallback.
//
// A row inserted before the topic column was populated, or built by a caller that left it
// empty, must still reach the right dead-letter topic. The fallback runs through the same
// event-type-to-category mapping the first publish would have used, so an event can never be
// stranded for want of a recorded destination.
func TestDeadLetterRouting_DerivesTheTopicFromTheEventTypeWhenTheRowRecordsNone(t *testing.T) {
	dltPinTopicPrefix(t)

	for _, route := range dltCategoryRoutes {
		t.Run(route.eventType, func(t *testing.T) {
			row := dltExhaustedRow(t, "evt_fallback_"+route.eventType, route.eventType, "")
			store := newDltFakeStore().withRow(row)
			transport := &dltFakeTransport{}
			service := dltNewService(store, &dltFakePublisher{}, transport)

			outcome, err := service.DeadLetter(context.Background(), row, errors.New("broker unavailable"))
			require.NoError(t, err)

			assert.Equal(t, route.originalTopic, outcome.OriginalTopic)
			assert.Equal(t, route.deadLetterTopic, outcome.DeadLetterTopic)
			assert.Equal(t, []string{route.deadLetterTopic}, transport.snapshotRequested(),
				"the resolver must be asked for the dead-letter topic and nothing else")
		})
	}
}

// TestDeadLetterRouting_RecordsTheDeadLetterTopicOnTheOutboxRow is what makes the API
// possible at all.
//
// The listing and the replay both read the ROW, not the topic — Blnk implements no consumer.
// If the dead-letter topic were only on the message and not in the dlt_topic column, an
// operator could see nothing and replay nothing, and the event would be preserved in a place
// no Blnk code can reach.
func TestDeadLetterRouting_RecordsTheDeadLetterTopicOnTheOutboxRow(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_recorded", "transaction.applied", "blnk.transactions")
	store := newDltFakeStore().withRow(row)
	transport := &dltFakeTransport{}
	service := dltNewService(store, &dltFakePublisher{}, transport)

	outcome, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
	require.NoError(t, err)

	records := store.snapshotDeadLettered()
	require.Len(t, records, 1, "the row must be recorded exactly once")
	assert.Equal(t, row.ID, records[0].id)
	assert.Equal(t, "blnk.transactions.dlt", records[0].dltTopic,
		"the dlt_topic column must record the same name the message was written to")
	assert.Equal(t, outcome.DeadLetterTopic, records[0].dltTopic)

	stored := store.row(t, row.EventID)
	assert.Equal(t, "blnk.transactions.dlt", stored.DLTTopic)
	assert.Equal(t, model.EventOutboxStatusDeadLettered, stored.Status,
		"the row must reach its terminal dead-lettered state")
	assert.JSONEq(t, string(outcome.MetadataJSON), string(stored.FailureMetadata))
	assert.Equal(t, []byte(outcome.MetadataJSON), []byte(stored.FailureMetadata),
		"the SAME metadata bytes must be stored on the row as were spliced into the message")
}

// TestDeadLetterRouting_LeavesADeadLetteredRowOutsideTheRelayClaimSet is the "cannot loop
// forever" guarantee.
//
// A dead-lettered row that the relay could claim again would be republished, fail again,
// and be dead-lettered again, indefinitely — a loop that produces duplicate dead-letter
// messages and an ever-growing inventory. The row is protected by TWO independent conditions
// in the claim query, either of which alone would suffice, and both are asserted here
// because losing one silently halves the protection:
//
//  1. the claim restricts to the pending and processing statuses, and dead_lettered is
//     neither, and
//  2. the claim requires attempts < max_attempts, and an exhausted row has spent its budget.
//
// The claim query is a package-private constant in database/, so the guarantee is asserted
// over the source text. That is deliberate: it holds for the real SQL rather than for a
// fake's approximation of it, and it needs no database.
func TestDeadLetterRouting_LeavesADeadLetteredRowOutsideTheRelayClaimSet(t *testing.T) {
	source := dltReadSource(t, "database", "event_outbox.go")

	claimStart := strings.Index(source, "claimPendingEventOutboxQuery")
	require.NotEqual(t, -1, claimStart, "the claim query must exist in database/event_outbox.go")

	claimEnd := strings.Index(source[claimStart:], "ClaimPendingEventOutbox claims")
	require.NotEqual(t, -1, claimEnd, "the claim query must be followed by its method documentation")
	claimQuery := source[claimStart : claimStart+claimEnd]

	// The SQL body only, not the surrounding documentation. The doc comment now
	// discusses the dead-lettered state at length — explaining why a dead-lettered row
	// does not block its partition key forever — and asserting over the comment text
	// would fail on prose rather than on behaviour.
	sqlStart := strings.Index(claimQuery, "`")
	require.NotEqual(t, -1, sqlStart, "the claim query constant must have a raw string body")
	claimSQL := claimQuery[sqlStart:]

	assert.Contains(t, claimSQL, "status IN ('pending', 'processing')",
		"the claim must restrict to the two claimable statuses, which excludes dead_lettered")
	assert.Contains(t, claimSQL, "candidate.attempts < candidate.max_attempts",
		"the claim must exclude a row that has spent its retry budget")
	assert.NotContains(t, claimSQL, model.EventOutboxStatusDeadLettered,
		"the claim query must never name the dead-lettered status")
	assert.NotContains(t, claimSQL, model.EventOutboxStatusReplaying,
		"nor the replaying status: a row a replay holds is not the relay's to publish")

	// The state vocabulary itself has to keep the two apart, or the SQL above would be
	// filtering on the wrong literal.
	assert.NotEqual(t, model.EventOutboxStatusPending, model.EventOutboxStatusDeadLettered)
	assert.NotEqual(t, model.EventOutboxStatusProcessing, model.EventOutboxStatusDeadLettered)
	assert.Equal(t, "dead_lettered", model.EventOutboxStatusDeadLettered)
}

// TestDeadLetterRouting_NeverSpendsAnotherAttemptOnTheRow pins the store seam.
//
// MarkEventFailed increments the attempts counter. Calling it from the dead-letter path would
// spend a sixth attempt against a five-attempt budget and make the attempt count in the
// failure metadata disagree with the configured maximum — which is exactly the number an
// operator reads to decide whether the retry policy is working. The absence is enforced over
// the interface's method set, so it cannot be reintroduced quietly.
//
// The same check covers the claim methods: a surface that could claim a row could take part
// in the relay's job, and dead-lettering is emphatically not that.
//
// The seam is asserted as a CLOSED SET rather than as a list of forbidden names, so every
// addition to it has to be justified here in writing. That is deliberate: the value of the
// assertion is the argument each entry has to survive, not the count — so the count in the
// comment below is a consequence of the arguments, never a target to be edited to match.
func TestDeadLetterRouting_NeverSpendsAnotherAttemptOnTheRow(t *testing.T) {
	seam := reflect.TypeOf((*eventDeadLetterStore)(nil)).Elem()

	declared := make([]string, 0, seam.NumMethod())
	for i := 0; i < seam.NumMethod(); i++ {
		declared = append(declared, seam.Method(i).Name)
	}

	// TWELVE METHODS NOW, and the argument for each is below. Nine of them mutate nothing
	// and four of those nine cannot even name a claimable row; the three that do write are
	// each conditional on a claim token this surface was handed rather than one it took.
	//
	// Two are the replay CLAIM and its rollback, which are not "claim methods" in the
	// relay's sense — neither can take a pending row or take part in publishing new
	// events. ClaimEventForReplay moves a row that is ALREADY dead-lettered into
	// replaying, which is what makes a replay atomic instead of a read followed by a check
	// that two concurrent requests could both pass.
	//
	// CountDeadLetteredEvents is a READ counted over the same predicate as the listing. It
	// is what makes an exact total available for a filtered page; it cannot mutate a row or
	// reach a claimable one.
	//
	// ListDeadLetterInventory is the NARROW TRIAGE PROJECTION the operator listing reads,
	// paged by keyset. It earns its place by being the cheap read: it returns each entry's
	// coordinate and the SIZE of its payload instead of the payload, so an inventory of
	// large events costs a page of metadata, and the cursor keeps that cost flat at any
	// depth. The full-row listing beside it stays because the age accounting and the replay
	// path need the stored bytes. Neither can mutate a row.
	//
	// CountDeadLetterInventory is the count driven from the INVENTORY predicate, the twin of
	// CountDeadLetteredEvents at the repository layer: one finding — a filtered listing that
	// could not be given a total — was answered twice, once over each of the two listing
	// predicates, and both answers are exercised by the repository's own tests. Keeping the twin
	// on the seam costs nothing a reviewer should worry about: it is a COUNT(*), so it cannot
	// mutate a row, cannot claim one, and cannot spend an attempt. What it must never become is a
	// second, drifting definition of "the inventory" — which is why both counts are required to
	// share their listing's predicate rather than to re-state it.
	//
	// ListAndCountDeadLetterInventory is the PAIRED read a listing that asked for a total goes
	// through, and it is on the seam because sharing a predicate was never enough on its own: two
	// statements on two connections observe two populations, so an entry dead-lettered between
	// them is counted by one and absent from the other and the total then describes a set the page
	// is not a slice of. It is two COUNT-and-SELECT reads inside one read-only REPEATABLE READ
	// transaction — read-only at the database level, not merely by convention, so it cannot mutate
	// a row, claim one or spend an attempt any more than its two halves can.
	//
	// OldestDeadLetterAgeByTopic is the grouped aggregate the age gauge is computed from
	// (PERF-P07). It replaced a bounded backwards walk over the whole inventory, which read
	// thousands of rows per refresh to report one instant per topic and understated the age
	// outright once the inventory outgrew its budget. It is a GROUP BY that returns one row
	// per dead-letter topic; it reads no payload and writes nothing.
	assert.ElementsMatch(t, []string{
		"GetEventByID",
		"ListDeadLetteredEvents",
		"ListDeadLetterInventory",
		"ListAndCountDeadLetterInventory",
		"CountDeadLetteredEvents",
		"CountDeadLetterInventory",
		// The UNRESOLVED aggregate, not the windowed one (PERF-M05). This service reads only
		// the `failed` count, which the unresolved reading gives exactly and for all time,
		// and the windowed reading additionally counted a day of dispatched history — 43.2
		// million index entries at the target rate — that nothing here looked at.
		"CountUnresolvedEventOutbox",
		"MarkEventDeadLettered",
		"MarkEventDispatched",
		"ClaimEventForReplay",
		"ReleaseEventReplay",
		"OldestDeadLetterAgeByTopic",
	}, declared, "the dead-letter store seam must expose exactly these twelve methods")

	assert.NotContains(t, declared, "ClaimPendingEventOutbox",
		"the dead-letter surface must not be able to claim a PENDING row: that is the relay's job, and a surface that could do it could take part in publishing new events")

	for _, forbidden := range []string{
		"MarkEventFailed",
		"ClaimPendingEventOutbox",
		"InsertEventOutbox",
		"InsertEventOutboxInTx",
		"MarkWebhookDispatched",
	} {
		_, found := seam.MethodByName(forbidden)
		assert.False(t, found,
			"%s must NOT be reachable from the dead-letter path", forbidden)
	}

	// And the runtime behaviour agrees: a completed dead-lettering touches exactly one
	// transition.
	dltPinTopicPrefix(t)
	row := dltExhaustedRow(t, "evt_no_extra_attempt", "transaction.applied", "blnk.transactions")
	store := newDltFakeStore().withRow(row)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	outcome, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
	require.NoError(t, err)

	assert.Len(t, store.snapshotDeadLettered(), 1)
	assert.Empty(t, store.snapshotDispatched(), "dead-lettering must not dispatch anything")
	assert.Equal(t, dltExhaustedAttempts, outcome.Metadata.AttemptCount,
		"the reported attempt count must equal the configured maximum, not maximum plus one")
	assert.Equal(t, dltExhaustedAttempts, store.row(t, row.EventID).Attempts,
		"the row's own attempts counter must be untouched by dead-lettering")
}

// TestDeadLetterRouting_RefusesToRecordADeadLetterWithoutABroker is the DLT-01 guard.
//
// This test used to assert the opposite, and the behaviour it asserted lost events. With no
// transport the service returned success, the caller marked the row dead_lettered, and the
// dead-letter counter was incremented — while nothing had been written anywhere. The row's
// only copy of the event was the row itself, dead_lettered is terminal so the relay would
// never claim it again, and both the row and the metric reported the event as safely
// preserved. Every signal an operator could read said the event was on a dead-letter topic;
// none of them was true.
//
// The contract now: no broker means NO DEAD-LETTERING. An error is returned, the row is left
// exactly as the caller had it — non-terminal, still in the inventory, still completable once
// a transport exists — and nothing is counted. The composed message is still returned on the
// partial outcome, so a caller can show what would have been written.
//
// The transport is the REAL production resolver wrapped around the REAL no-op publisher, so
// this asserts the wiring rather than a fake's imitation of it.
func TestDeadLetterRouting_RefusesToRecordADeadLetterWithoutABroker(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_no_broker", "balance.created", "blnk.balances")
	store := newDltFakeStore().withRow(row)

	noop := NewNoopEventPublisher()
	service := NewEventDeadLetterService(store, nil)
	service.now = func() time.Time { return dltFixedNow }
	service.withTransport(noop, publisherWriterResolver(noop))

	counter := dltCaptureDeadLetterCounter(t)

	outcome, err := service.DeadLetter(context.Background(), row, errors.New("no sink"))
	dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)

	assert.False(t, outcome.Published, "nothing can have been published without a broker")
	assert.Equal(t, "blnk.balances.dlt", outcome.DeadLetterTopic,
		"the resolved destination is still reported, so the failure names where the message was bound for")
	assert.NotEmpty(t, outcome.Message,
		"the message must still be composed so an operator can be shown what would have been written")

	assert.Empty(t, store.snapshotDeadLettered(),
		"a row must not be recorded as dead-lettered when no dead-letter message exists")
	stored := store.row(t, row.EventID)
	assert.Equal(t, model.EventOutboxStatusFailed, stored.Status,
		"the row keeps the non-terminal status the caller gave it, so it stays visible and completable")
	assert.Empty(t, stored.DLTTopic,
		"and dlt_topic stays unset, which is the half of the pair that makes the row re-claimable: "+
			"ClaimFailedEventOutboxForDeadLetter selects failed rows whose dlt_topic IS NULL, so the owed "+
			"write is attempted again once this lease lapses. Recording the coordinate here would retire "+
			"the row with its event on no topic at all")
	assert.Zero(t, counter.total(),
		"the dead-letter counter must not claim a dead-lettering that did not happen")
}

// TestDeadLetterRouting_RefusesToRecordADeadLetterWhenConfigurationCannotBeRead is the second
// half of the DLT-01 guard, and it covers the more dangerous of the two paths.
//
// "No brokers configured" is at least an observed fact. "Configuration could not be read" is
// an UNKNOWN, and it used to be silently downgraded to the first: a transient configuration
// failure in a deployment that runs Kafka every day produced a row marked dead_lettered, a
// counter increment, and no message. The event was gone, in a deployment where a broker was
// sitting there ready to take it.
//
// It now fails, for the same reason and with the same consequences as the no-broker case.
func TestDeadLetterRouting_RefusesToRecordADeadLetterWhenConfigurationCannotBeRead(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_no_config", "identity.created", "blnk.identities")
	store := newDltFakeStore().withRow(row)

	// No injected transport, so the service resolves one from configuration — and the seam
	// reports that configuration is unavailable.
	service := NewEventDeadLetterService(store, nil)
	service.now = func() time.Time { return dltFixedNow }

	original := fetchConfiguration
	t.Cleanup(func() { fetchConfiguration = original })
	fetchConfiguration = func() (*config.Configuration, error) {
		return nil, errors.New("configuration store is unavailable")
	}

	counter := dltCaptureDeadLetterCounter(t)

	outcome, err := service.DeadLetter(context.Background(), row, errors.New("exhausted"))
	dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)

	assert.False(t, outcome.Published)
	assert.Empty(t, store.snapshotDeadLettered(),
		"an unreadable configuration must not be treated as a deployment without Kafka")
	stored := store.row(t, row.EventID)
	assert.Equal(t, model.EventOutboxStatusFailed, stored.Status,
		"an unreadable configuration must leave the write OWED and recoverable, not retired")
	assert.Empty(t, stored.DLTTopic,
		"failed with dlt_topic still unset is what the repair claim selects on, so the write is "+
			"re-attempted once configuration is readable again")
	assert.Zero(t, counter.total())
}

// TestDeadLetterRouting_RefusesAPublisherItCannotComposeAMessageFor covers the third arm of
// the writer resolution.
//
// Only the Kafka publisher can be handed a composed dead-letter message; the no-op is the
// documented "no transport" case. Anything else must fail LOUDLY, because silently skipping
// the write would lose the dead-letter message while reporting success — the one outcome
// worth failing over.
func TestDeadLetterRouting_RefusesAPublisherItCannotComposeAMessageFor(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_wrong_publisher", "identity.created", "blnk.identities")
	store := newDltFakeStore().withRow(row)

	unsupported := &dltFakePublisher{}
	service := NewEventDeadLetterService(store, nil)
	service.now = func() time.Time { return dltFixedNow }
	service.withTransport(unsupported, publisherWriterResolver(unsupported))

	_, err := service.DeadLetter(context.Background(), row, errors.New("boom"))
	dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)
	assert.Empty(t, store.snapshotDeadLettered(),
		"a row must not be recorded as dead-lettered when its message never left the process")
}

// TestDeadLetterRouting_RejectsARowWithoutADatabaseIdentity keeps an unrecordable
// dead-letter from being written.
//
// A row with no id cannot be recorded, and an unrecorded dead-letter is invisible to the
// listing and unreachable by replay. Failing BEFORE the write is what keeps "it is on the
// dead-letter topic" and "an operator can find it" from diverging.
func TestDeadLetterRouting_RejectsARowWithoutADatabaseIdentity(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_no_id", "transaction.applied", "blnk.transactions")
	row.ID = 0

	transport := &dltFakeTransport{}
	service := dltNewService(newDltFakeStore().withRow(row), &dltFakePublisher{}, transport)

	_, err := service.DeadLetter(context.Background(), row, errors.New("boom"))
	dltAssertCodeAndStatus(t, err, apierror.ErrGenValidation, http.StatusBadRequest)
	assert.Empty(t, transport.snapshotWritten(), "nothing may be written for an unrecordable row")
}

// TestDeadLetterRouting_ReportsAFailedWriteAndLeavesTheRowInTheInventory pins the order of
// operations.
//
// The write happens first and the row is recorded second, deliberately. When the write fails
// the row is left in the failed state the relay already put it in — which the dead-letter
// listing covers — so the event stays visible to an operator instead of being reported as
// safely dead-lettered when its message never left the process.
func TestDeadLetterRouting_ReportsAFailedWriteAndLeavesTheRowInTheInventory(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_write_failed", "transaction.void", "blnk.transactions")
	store := newDltFakeStore().withRow(row)
	transport := &dltFakeTransport{writeErr: errors.New("broken pipe")}
	service := dltNewService(store, &dltFakePublisher{}, transport)

	counter := dltCaptureDeadLetterCounter(t)

	outcome, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
	dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)

	assert.False(t, outcome.Published)
	assert.Empty(t, store.snapshotDeadLettered(), "the row must not be recorded after a failed write")
	stored := store.row(t, row.EventID)
	assert.Equal(t, model.EventOutboxStatusFailed, stored.Status,
		"a failed dead-letter write must leave the row RETRYABLE: failed with dlt_topic still NULL is "+
			"what ClaimFailedEventOutboxForDeadLetter claims, so the write is re-attempted once this "+
			"holder's lease lapses")
	assert.Empty(t, stored.DLTTopic,
		"and the coordinate must stay unset, because recording it is what retires the row: a failed row "+
			"carrying a dlt_topic is outside the repair claim and outside replay, so the event would "+
			"exist only in the outbox with no copy on any topic")
	assert.NotEqual(t, model.EventOutboxStatusDeadLettered, stored.Status,
		"dead_lettered is the outcome this replaced: it reads as a completed hand-off")
	assert.Zero(t, counter.total(), "nothing was dead-lettered, so nothing may be counted")
}

// ---------------------------------------------------------------------------
// Failure-metadata assembly
// ---------------------------------------------------------------------------

// TestFailureMetadata_CarriesExactlyTheFiveContractFields pins the published shape.
//
// The requirement names five fields, and the metadata must carry EXACTLY those five: the
// original topic, the error reason, the attempt count, and the first- and last-attempted
// instants. A sixth would not be a harmless addition — the metadata is a published shape
// that the triage runbook and the API projection both read — and a missing one leaves an
// operator without an answer they were promised.
//
// The JSON tags are asserted alongside the field names because the tags are the wire
// contract; renaming a tag while keeping the Go field would silently break every reader.
func TestFailureMetadata_CarriesExactlyTheFiveContractFields(t *testing.T) {
	metadataType := reflect.TypeOf(model.FailureMetadata{})

	require.Equal(t, 5, metadataType.NumField(),
		"model.FailureMetadata must carry exactly the five contract fields and no others")

	expected := []struct {
		field string
		tag   string
		kind  reflect.Kind
	}{
		{field: "OriginalTopic", tag: "original_topic", kind: reflect.String},
		{field: "ErrorReason", tag: "error_reason", kind: reflect.String},
		{field: "AttemptCount", tag: "attempt_count", kind: reflect.Int},
		{field: "FirstAttemptedAt", tag: "first_attempted_at", kind: reflect.Struct},
		{field: "LastAttemptedAt", tag: "last_attempted_at", kind: reflect.Struct},
	}

	for index, want := range expected {
		field := metadataType.Field(index)
		assert.Equal(t, want.field, field.Name, "field %d must be %s", index, want.field)
		assert.Equal(t, want.tag, field.Tag.Get("json"), "%s must serialise as %q", want.field, want.tag)
		assert.Equal(t, want.kind, field.Type.Kind(), "%s must be a %s", want.field, want.kind)
	}

	// And the serialised document carries precisely those five members — no omitempty
	// dropping a zero value, and nothing extra.
	dltPinTopicPrefix(t)
	row := dltExhaustedRow(t, "evt_metadata_shape", "transaction.applied", "blnk.transactions")
	service := dltNewService(newDltFakeStore().withRow(row), &dltFakePublisher{}, &dltFakeTransport{})

	outcome, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
	require.NoError(t, err)

	var members map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(outcome.MetadataJSON, &members))

	memberNames := make([]string, 0, len(members))
	for name := range members {
		memberNames = append(memberNames, name)
	}
	assert.ElementsMatch(t, []string{
		"original_topic", "error_reason", "attempt_count", "first_attempted_at", "last_attempted_at",
	}, memberNames, "the serialised failure metadata must carry exactly the five contract members")
}

// TestFailureMetadata_AssertsEveryFieldIndividually is the field-by-field assertion the
// requirement asks for.
//
// Each of the five is checked against a distinct expected value, so a metadata builder that
// populated the right SHAPE with the wrong CONTENT — the classic copy-paste defect, where
// one field is assigned from another's source — fails here. Asserting only that the struct is
// non-empty would catch none of that.
func TestFailureMetadata_AssertsEveryFieldIndividually(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_metadata_fields", "transaction.applied", "blnk.transactions")
	service := dltNewService(newDltFakeStore().withRow(row), &dltFakePublisher{}, &dltFakeTransport{})

	cause := errors.New(dltPublishFailureReason)
	outcome, err := service.DeadLetter(context.Background(), row, cause)
	require.NoError(t, err)

	metadata := outcome.Metadata

	assert.Equal(t, "blnk.transactions", metadata.OriginalTopic,
		"original_topic must be the topic the event failed to reach, never the .dlt sibling")
	assert.Equal(t, dltPublishFailureReason, metadata.ErrorReason,
		"error_reason must be the actual publish error text, in full")
	assert.Equal(t, dltExhaustedAttempts, metadata.AttemptCount,
		"attempt_count must be the number of attempts actually made")
	assert.Equal(t, row.FirstAttemptedAt.UTC(), metadata.FirstAttemptedAt,
		"first_attempted_at must be the FIRST attempt's instant")
	assert.Equal(t, row.LastAttemptedAt.UTC(), metadata.LastAttemptedAt,
		"last_attempted_at must be the LAST attempt's instant")

	// The stored document agrees with the struct, member for member.
	var stored model.FailureMetadata
	require.NoError(t, json.Unmarshal(outcome.MetadataJSON, &stored))
	assert.Equal(t, metadata, stored, "the serialised metadata must round-trip to the same record")
}

// TestFailureMetadata_ReportsTheConfiguredMaximumAfterExhaustion pins the attempt count to
// the number an operator reads.
//
// After exhaustion the count must equal the CONFIGURED MAXIMUM — five by default. Two
// specific wrong answers are excluded explicitly, because both are natural off-by-one
// mistakes and both would misrepresent the retry policy: maximum plus one (counting the
// dead-letter write itself as an attempt) and the retry INDEX (four, counting only the
// retries after the first attempt).
//
// The three fallback arms are covered too, since each is separately reachable on a real row:
// the caller states nothing and the row's counter answers; neither states anything and the
// budget answers; and a stale caller count is overridden by the larger row counter, because
// both are lower bounds on the truth.
func TestFailureMetadata_ReportsTheConfiguredMaximumAfterExhaustion(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_attempts", "transaction.applied", "blnk.transactions")

	t.Run("caller states nothing", func(t *testing.T) {
		metadata := BuildFailureMetadata(row, errors.New("boom"), 0, dltFixedNow)

		assert.Equal(t, dltExhaustedAttempts, metadata.AttemptCount)
		assert.NotEqual(t, dltExhaustedAttempts+1, metadata.AttemptCount,
			"the dead-letter write must not be counted as a sixth attempt")
		assert.NotEqual(t, dltExhaustedAttempts-1, metadata.AttemptCount,
			"the count must be the attempts made, not the retry index")
	})

	t.Run("row counter never read back", func(t *testing.T) {
		unread := row
		unread.Attempts = 0

		metadata := BuildFailureMetadata(unread, errors.New("boom"), 0, dltFixedNow)
		assert.Equal(t, dltExhaustedAttempts, metadata.AttemptCount,
			"an exhausted row falls back to its budget, which is the configured maximum")
	})

	t.Run("stale caller count loses to the row", func(t *testing.T) {
		metadata := BuildFailureMetadata(row, errors.New("boom"), 2, dltFixedNow)
		assert.Equal(t, dltExhaustedAttempts, metadata.AttemptCount,
			"both counts are lower bounds, so the larger must be reported")
	})

	t.Run("caller ahead of the row wins", func(t *testing.T) {
		behind := row
		behind.Attempts = 3

		metadata := BuildFailureMetadata(behind, errors.New("boom"), dltExhaustedAttempts, dltFixedNow)
		assert.Equal(t, dltExhaustedAttempts, metadata.AttemptCount,
			"a row counter that is stale relative to an in-progress claim must not lower the count")
	})

	t.Run("never zero and never negative", func(t *testing.T) {
		bare := model.EventOutbox{ID: 1, EventID: "evt_bare", EventType: "transaction.applied"}

		metadata := BuildFailureMetadata(bare, errors.New("boom"), -4, dltFixedNow)
		assert.Equal(t, 1, metadata.AttemptCount,
			"reaching the dead-letter path at all means at least one attempt was made")
	})
}

// TestFailureMetadata_DistinguishesTheFirstAttemptFromTheLast is the swapped-assignment
// catcher.
//
// The two instants are asserted against a window that is genuinely 31 seconds wide — the span
// the documented backoff schedule produces over five attempts — so an implementation that
// assigned them the wrong way round fails. A fixture whose attempts coincided would pass
// either way, which is why the fixture is built with a real spread.
//
// The window is also asserted to run FORWARDS, because the pair is what an operator subtracts
// to tell a momentary broker blip from a sustained outage, and a negative duration answers
// neither question.
func TestFailureMetadata_DistinguishesTheFirstAttemptFromTheLast(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_window", "transaction.applied", "blnk.transactions")
	service := dltNewService(newDltFakeStore().withRow(row), &dltFakePublisher{}, &dltFakeTransport{})

	outcome, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
	require.NoError(t, err)

	first := outcome.Metadata.FirstAttemptedAt
	last := outcome.Metadata.LastAttemptedAt

	assert.Equal(t, row.FirstAttemptedAt.UTC(), first)
	assert.Equal(t, row.LastAttemptedAt.UTC(), last)
	assert.NotEqual(t, first, last, "the fixture must present two genuinely different instants")
	assert.True(t, first.Before(last), "the window must run forwards: first %s, last %s", first, last)
	assert.Equal(t, 31*time.Second, last.Sub(first),
		"the reported window must be the span the attempts actually covered")

	// Both are normalised to UTC so the RFC3339 rendering is unambiguous wherever the
	// process runs.
	assert.Equal(t, time.UTC, first.Location())
	assert.Equal(t, time.UTC, last.Location())

	t.Run("the relay's own window overrides the row", func(t *testing.T) {
		// The relay knows the window it actually retried over; the row only knows what its
		// last database write recorded.
		relayFirst := dltFixedNow.Add(-10 * time.Minute)
		relayLast := dltFixedNow.Add(-9 * time.Minute)

		overridden, overrideErr := service.PublishToDeadLetter(context.Background(), DeadLetterRequest{
			Row:              row,
			Cause:            errors.New(dltPublishFailureReason),
			FirstAttemptedAt: relayFirst,
			LastAttemptedAt:  relayLast,
		})
		require.NoError(t, overrideErr)

		assert.Equal(t, relayFirst, overridden.Metadata.FirstAttemptedAt)
		assert.Equal(t, relayLast, overridden.Metadata.LastAttemptedAt)
		assert.Equal(t, dltOriginalEnvelope(t, row), dltMustStrip(t, overridden.Message),
			"an attempt-window override must not disturb one byte of the envelope")
	})

	t.Run("an inverted window is squared up rather than published", func(t *testing.T) {
		inverted := row
		later := dltFixedNow
		earlier := dltFixedNow.Add(-time.Hour)
		inverted.FirstAttemptedAt = &later
		inverted.LastAttemptedAt = &earlier

		metadata := BuildFailureMetadata(inverted, errors.New("boom"), dltExhaustedAttempts, dltFixedNow)
		assert.Equal(t, metadata.FirstAttemptedAt, metadata.LastAttemptedAt,
			"a backwards window collapses to zero length rather than reporting a negative duration")
		assert.False(t, metadata.LastAttemptedAt.Before(metadata.FirstAttemptedAt))
	})

	t.Run("a row with no attempt timestamps falls back to the dead-letter instant", func(t *testing.T) {
		untimed := row
		untimed.FirstAttemptedAt = nil
		untimed.LastAttemptedAt = nil

		metadata := BuildFailureMetadata(untimed, errors.New("boom"), dltExhaustedAttempts, dltFixedNow)
		assert.Equal(t, dltFixedNow, metadata.FirstAttemptedAt,
			"a zero time.Time would serialise as year one and make the window nonsense")
		assert.Equal(t, dltFixedNow, metadata.LastAttemptedAt)
	})
}

// TestFailureMetadata_CarriesTheWholePublishErrorText keeps the reason useful.
//
// The reason is compared in FULL against a long, realistic broker error, so an implementation
// that truncated it to a fixed width — or replaced it with a generic sentence — fails. The
// three-step fallback chain is covered as well, and the last step matters most: an EMPTY
// reason answers nothing while looking like a successful read of a missing value, which is
// the exact failure mode the explicit sentence exists to prevent.
func TestFailureMetadata_CarriesTheWholePublishErrorText(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_reason", "transaction.applied", "blnk.transactions")

	t.Run("the caller's cause wins", func(t *testing.T) {
		metadata := BuildFailureMetadata(row, errors.New(dltPublishFailureReason), dltExhaustedAttempts, dltFixedNow)

		assert.Equal(t, dltPublishFailureReason, metadata.ErrorReason)
		assert.Len(t, metadata.ErrorReason, len(dltPublishFailureReason),
			"the reason must not be truncated: %d characters were recorded", len(metadata.ErrorReason))
		assert.Contains(t, metadata.ErrorReason, "in-sync replicas",
			"the tail of the broker's message is where the diagnosis usually is")
	})

	t.Run("the row's last error is the fallback", func(t *testing.T) {
		metadata := BuildFailureMetadata(row, nil, dltExhaustedAttempts, dltFixedNow)
		assert.Equal(t, row.LastError, metadata.ErrorReason)
	})

	t.Run("a blank cause falls through to the row", func(t *testing.T) {
		metadata := BuildFailureMetadata(row, errors.New("   "), dltExhaustedAttempts, dltFixedNow)
		assert.Equal(t, row.LastError, metadata.ErrorReason,
			"a whitespace-only cause is no reason at all")
	})

	t.Run("never empty", func(t *testing.T) {
		silent := row
		silent.LastError = ""

		metadata := BuildFailureMetadata(silent, nil, dltExhaustedAttempts, dltFixedNow)
		assert.Equal(t, unrecordedDeadLetterReason, metadata.ErrorReason)
		assert.NotEmpty(t, metadata.ErrorReason,
			"an empty reason answers nothing while looking like a value")
	})
}

// TestFailureMetadata_IsAttachedAdditivelySoTheOriginalBytesSurvive is the precondition for
// byte-faithful replay, asserted directly.
//
// The dead-letter message must be the original envelope followed by ONE extra member. Three
// things are asserted, and together they leave no room for a re-encode:
//
//  1. every byte of the envelope UP TO ITS CLOSING BRACE is a byte-exact prefix of the
//     dead-letter message — the splice replaces only that final brace,
//  2. the remainder is EXACTLY `,"failure_metadata":<metadata>}` and nothing else, and
//  3. stripping the member returns the original bytes, byte for byte.
//
// If the implementation folded the metadata into the envelope — decode, add a key, re-encode
// — the prefix property would fail immediately, and replay could never be byte-exact.
func TestFailureMetadata_IsAttachedAdditivelySoTheOriginalBytesSurvive(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_additive", "transaction.applied", "blnk.transactions")
	store := newDltFakeStore().withRow(row)
	transport := &dltFakeTransport{}
	service := dltNewService(store, &dltFakePublisher{}, transport)

	original := dltOriginalEnvelope(t, row)

	outcome, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
	require.NoError(t, err)

	written := transport.snapshotWritten()
	require.Len(t, written, 1)
	message := written[0].message.Value

	assert.Equal(t, outcome.Message, message,
		"the outcome must report exactly the bytes that were written")

	// The splice replaces the envelope's closing brace and appends, so everything before
	// that brace must survive untouched.
	require.Greater(t, len(original), 1)
	assert.True(t, bytes.HasPrefix(message, original[:len(original)-1]),
		"every envelope byte up to the closing brace must survive verbatim\n original: %s\n  message: %s",
		original, message)

	expectedSuffix := failureMetadataMember + string(outcome.MetadataJSON) + "}"
	require.GreaterOrEqual(t, len(message), len(original)-1)
	assert.Equal(t, expectedSuffix, string(message[len(original)-1:]),
		"the only difference must be the additive failure_metadata member")
	assert.Len(t, message, len(original)-1+len(expectedSuffix),
		"the dead-letter message must be exactly the envelope plus the attachment, with nothing else changed")

	recovered, stripErr := StripFailureMetadata(message)
	require.NoError(t, stripErr)
	assert.Equal(t, original, recovered,
		"the original event bytes must be recoverable unchanged from the dead-lettered message")

	// The metadata is a SIBLING of the six envelope members, never nested inside payload
	// and never replacing it.
	var members map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(message, &members))
	require.Contains(t, members, "failure_metadata")
	require.Contains(t, members, "payload")
	assert.NotContains(t, string(members["payload"]), "failure_metadata",
		"the metadata must never be nested inside the payload")
	assert.Equal(t, dltTrapPayload, string(members["payload"]),
		"the payload must be carried through untransformed, whitespace and key order included")
	for _, envelopeMember := range []string{
		"event_id", "event_type", "aggregate_id", "occurred_at", "payload", "schema_version",
	} {
		assert.Contains(t, members, envelopeMember,
			"not one of the six envelope members may be removed by the attachment")
	}
	assert.Len(t, members, 7, "the dead-letter message is the six envelope members plus exactly one")
}

// TestStripFailureMetadata_IsTheExactInverseAndIsIdempotent covers the recovery primitive on
// its own.
//
// It is idempotent by design so a caller need not know whether it is holding an original or a
// dead-lettered message, which is what makes it safe to apply on the way into a comparison.
// The last-occurrence search is covered too: a payload that happens to contain the same member
// name deeper inside must not be mistaken for the attachment, and that is a realistic case
// because the payload is opaque, subscriber-supplied JSON.
func TestStripFailureMetadata_IsTheExactInverseAndIsIdempotent(t *testing.T) {
	dltPinTopicPrefix(t)

	t.Run("an ordinary envelope is returned unchanged", func(t *testing.T) {
		row := dltExhaustedRow(t, "evt_strip_plain", "transaction.applied", "blnk.transactions")
		original := dltOriginalEnvelope(t, row)

		recovered, err := StripFailureMetadata(original)
		require.NoError(t, err)
		assert.Equal(t, original, recovered)
	})

	t.Run("stripping twice changes nothing the second time", func(t *testing.T) {
		row := dltExhaustedRow(t, "evt_strip_twice", "transaction.applied", "blnk.transactions")
		service := dltNewService(newDltFakeStore().withRow(row), &dltFakePublisher{}, &dltFakeTransport{})

		outcome, err := service.DeadLetter(context.Background(), row, errors.New("boom"))
		require.NoError(t, err)

		once := dltMustStrip(t, outcome.Message)
		twice := dltMustStrip(t, once)
		assert.Equal(t, once, twice, "StripFailureMetadata must be idempotent")
		assert.Equal(t, dltOriginalEnvelope(t, row), twice)
	})

	t.Run("a payload naming the member deeper inside is not mistaken for the attachment", func(t *testing.T) {
		row := dltExhaustedRow(t, "evt_strip_decoy", "transaction.applied", "blnk.transactions")
		row.Payload = json.RawMessage(
			`{"event":"transaction.applied","data":{"failure_metadata":"a subscriber's own field"}}`,
		)
		service := dltNewService(newDltFakeStore().withRow(row), &dltFakePublisher{}, &dltFakeTransport{})

		outcome, err := service.DeadLetter(context.Background(), row, errors.New("boom"))
		require.NoError(t, err)

		assert.Equal(t, dltOriginalEnvelope(t, row), dltMustStrip(t, outcome.Message),
			"only the LAST occurrence — the appended attachment — may be removed")
	})

	t.Run("a non-object is refused", func(t *testing.T) {
		_, err := StripFailureMetadata([]byte(`["not","an","object"]`))
		dltAssertCodeAndStatus(t, err, apierror.ErrInternalServer, http.StatusInternalServerError)
	})
}

// TestComposeDeadLetterMessage_RefusesMetadataItCannotSpliceSafely covers the composition
// guard.
//
// Splicing invalid bytes would emit a message that breaks every subscriber's parser, and no
// retry turns malformed bytes into valid ones — so the composition fails instead, exactly as
// the publisher refuses an invalid payload.
func TestComposeDeadLetterMessage_RefusesMetadataItCannotSpliceSafely(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_compose_guard", "transaction.applied", "blnk.transactions")

	for name, metadata := range map[string]json.RawMessage{
		"absent":  nil,
		"blank":   json.RawMessage("   "),
		"invalid": json.RawMessage(`{"original_topic":`),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ComposeDeadLetterMessage(row, metadata)
			dltAssertCodeAndStatus(t, err, apierror.ErrInternalServer, http.StatusInternalServerError)
		})
	}

	t.Run("valid metadata composes", func(t *testing.T) {
		message, err := ComposeDeadLetterMessage(row, json.RawMessage(`{"original_topic":"blnk.transactions"}`))
		require.NoError(t, err)
		assert.True(t, json.Valid(message), "the composed message must be valid JSON")
	})
}

// dltMustStrip recovers the original envelope from a dead-letter message, failing the test if
// it cannot.
func dltMustStrip(t *testing.T, message []byte) []byte {
	t.Helper()

	envelope, err := StripFailureMetadata(message)
	require.NoError(t, err, "the dead-letter message must yield its original envelope")

	return envelope
}

// ---------------------------------------------------------------------------
// Replay fidelity — acceptance criterion V-9
// ---------------------------------------------------------------------------

// dltReplayFixture is a dead-lettered event ready to be replayed, together with everything
// needed to judge the replay: the original message bytes, the store, the publisher and the
// service.
type dltReplayFixture struct {
	row       model.EventOutbox
	original  []byte
	outcome   DeadLetterOutcome
	store     *dltFakeStore
	publisher *dltFakePublisher
	service   *EventDeadLetterService
}

// dltNewReplayFixture dead-letters an event for real and then presents it as a stored,
// replayable row.
//
// The dead-lettering is performed rather than hand-written, which matters: the metadata the
// replay reads is the metadata the implementation produced, and the "original" bytes are the
// bytes the first publish would actually have written. A fixture assembled by hand could
// agree with neither.
//
// Parameters:
//   - eventType: the event name.
//   - topic: the destination recorded on the row.
//   - metadataTopic: when non-empty, the original_topic REWRITTEN into the stored metadata, so
//     that a replay honouring the record can be told apart from one re-deriving the topic.
func dltNewReplayFixture(t *testing.T, eventType, topic, metadataTopic string) *dltReplayFixture {
	t.Helper()

	row := dltExhaustedRow(t, "evt_"+eventType+"_replay", eventType, topic)
	original := dltOriginalEnvelope(t, row)

	store := newDltFakeStore().withRow(row)
	publisher := &dltFakePublisher{}
	service := dltNewService(store, publisher, &dltFakeTransport{})

	outcome, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
	require.NoError(t, err, "the fixture event must dead-letter cleanly")

	stored := dltDeadLetterRow(row, outcome)
	if metadataTopic != "" {
		metadata := outcome.Metadata
		metadata.OriginalTopic = metadataTopic
		rewritten, marshalErr := json.Marshal(metadata)
		require.NoError(t, marshalErr)
		stored.FailureMetadata = rewritten
	}

	// Re-register the row in its post-dead-letter state, which is what the repository now
	// holds and therefore what a replay will read.
	replayStore := newDltFakeStore().withRow(stored)
	replayPublisher := &dltFakePublisher{}
	replayService := dltNewService(replayStore, replayPublisher, &dltFakeTransport{})

	return &dltReplayFixture{
		row:       stored,
		original:  original,
		outcome:   outcome,
		store:     replayStore,
		publisher: replayPublisher,
		service:   replayService,
	}
}

// replayedMessage returns the bytes the recorded replay would have put on the wire.
//
// It resolves the captured request through the SAME function the Kafka publisher resolves its
// message value with, so the comparison is against the real message value rather than against a
// reconstruction of it. That means the stored envelope where the request carries one, which is
// the case for every row read out of the outbox. This is the assertion criterion V-9 is decided
// by, and resolving rather than re-serialising is what keeps it from being decided between two
// rebuilt values.
func (f *dltReplayFixture) replayedMessage(t *testing.T) []byte {
	t.Helper()

	requests := f.publisher.snapshotRequests()
	require.Len(t, requests, 1, "exactly one replay publish must have been issued")

	message, err := resolveEventValue(requests[0])
	require.NoError(t, err, "the replayed event must serialise")

	return message
}

// request returns the single recorded replay request.
func (f *dltReplayFixture) request(t *testing.T) PublishRequest {
	t.Helper()

	requests := f.publisher.snapshotRequests()
	require.Len(t, requests, 1, "exactly one replay publish must have been issued")

	return requests[0]
}

// TestReplayDeadLetteredEvent_ReproducesTheOriginalMessageByteForByte is acceptance criterion
// V-9, asserted at unit level.
//
// A replayed dead-lettered event must match the original BYTE FOR BYTE aside from the failure
// metadata, and the comparison here is on RAW BYTES rather than on unmarshalled maps. That
// distinction is the whole point: a map comparison discards key order, collapses number
// formatting and normalises escaping, so it would pass while the real guarantee was broken.
//
// event_replay_fidelity_test.go proves the same criterion end to end against a broker. This
// asserts it with no infrastructure at all, so the regression is caught on every `go test`.
func TestReplayDeadLetteredEvent_ReproducesTheOriginalMessageByteForByte(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

	outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err)

	replayed := fixture.replayedMessage(t)

	assert.Equal(t, fixture.original, replayed,
		"the replayed message must be byte-identical to the original\n original: %s\n replayed: %s",
		fixture.original, replayed)
	assert.Equal(t, string(fixture.original), string(replayed),
		"a byte difference must be reported as a readable diff, not just a length mismatch")

	// "Aside from the failure metadata" stated precisely: the dead-letter message carried it,
	// the replayed message does not, and stripping it from the former yields the latter.
	assert.Contains(t, string(fixture.outcome.Message), failureMetadataKey)
	assert.NotContains(t, string(replayed), failureMetadataKey,
		"the failure metadata must not travel back to the original topic")
	assert.Equal(t, replayed, dltMustStrip(t, fixture.outcome.Message),
		"stripping the attachment from the dead-letter message must yield exactly the replayed bytes")

	assert.Equal(t, model.PublishStatusDispatched, outcome.Status)
	assert.True(t, outcome.Recorded)

	// Under -v the three messages are printed side by side, so the criterion can be confirmed by
	// eye as well as by assertion. It is gated on verbosity because the payload can be large and
	// a passing run should stay quiet.
	if testing.Verbose() {
		t.Logf("original    : %s", fixture.original)
		t.Logf("dead-letter : %s", fixture.outcome.Message)
		t.Logf("replayed    : %s", replayed)
		t.Logf("original == replayed: %t; dead-letter adds %d bytes of failure metadata",
			bytes.Equal(fixture.original, replayed), len(fixture.outcome.Message)-len(fixture.original))
	}
}

// TestReplayDeadLetteredEvent_DoesNotReMarshalThePayload turns "we intended to pass the bytes
// through" into a PROVEN property.
//
// The fixture payload is built so that a round trip through a Go map or a typed struct would
// change it visibly in five independent ways at once — key order, trailing zeros, HTML
// escaping, large-number formatting and interior whitespace. Each is asserted separately, so a
// failure names which transformation crept in rather than only reporting that the bytes
// differ.
//
// This is what makes the byte-for-byte guarantee achievable rather than aspirational: replay
// re-publishes the STORED payload bytes, spliced into a freshly-composed envelope, and never
// decodes them.
func TestReplayDeadLetteredEvent_DoesNotReMarshalThePayload(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

	_, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err)

	replayed := string(fixture.replayedMessage(t))

	assert.Contains(t, replayed, dltTrapPayload,
		"the stored payload bytes must appear in the replayed message verbatim")

	assert.Contains(t, replayed, `"z_last":1,"a_first": 2`,
		"key ORDER must survive: encoding/json sorts map keys, so a re-marshal would put a_first first")
	assert.NotContains(t, replayed, `"a_first":2,"amount"`,
		"a sorted, re-encoded payload is exactly what must not appear")

	assert.Contains(t, replayed, `"amount":1.500`,
		"insignificant trailing zeros must survive: a number round trip renders 1.500 as 1.5")
	assert.NotContains(t, replayed, `"amount":1.5,`)

	assert.Contains(t, replayed, `"html":"<b>&</b>"`,
		"HTML escaping must not be applied: Go's encoder rewrites < > & by default")
	assert.NotContains(t, replayed, `\u003c`)
	assert.NotContains(t, replayed, `\u0026`)

	assert.Contains(t, replayed, `"big":10000000000000000000000.5`,
		"a large float literal must survive in the form it was stored in")
	assert.NotContains(t, replayed, `1e+22`)

	assert.Contains(t, replayed, `"nested":{"y":1,"x": 2}`,
		"interior whitespace cannot survive a re-encode, so its presence proves a splice")

	// The envelope's own timestamp is likewise rendered once, at the original publish, and
	// reproduced identically.
	assert.Contains(t, replayed, `"occurred_at":"2026-04-11T08:15:26.535897Z"`,
		"the occurrence instant must be rendered exactly as the first publish rendered it")
	assert.Contains(t, replayed, `"schema_version":1`)

	// The definitive statement: identical bytes.
	assert.Equal(t, string(fixture.original), replayed)
}

// TestReplayDeadLetteredEvent_TargetsTheTopicRecordedInTheStoredMetadata proves the
// destination is RECOVERED rather than re-derived.
//
// The stored metadata's original_topic is the authoritative record of where the event was
// headed. Re-deriving the destination from the event type would be wrong the moment
// KAFKA_TOPIC_PREFIX changed after the event was stored — the row would be replayed to a topic
// it was never destined for, and the subscriber waiting on the original topic would never see
// it.
//
// The fixture makes the two answers DIFFER on purpose: the metadata records
// "legacy.transactions" while the event type maps to "blnk.transactions" today. An
// implementation that re-derived would land on the latter and fail here.
func TestReplayDeadLetteredEvent_TargetsTheTopicRecordedInTheStoredMetadata(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "legacy.transactions")

	require.Equal(t, "blnk.transactions", TopicForEvent("transaction.applied"),
		"the fixture is only meaningful while the event type maps somewhere else today")

	outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err)

	assert.Equal(t, "legacy.transactions", outcome.Topic,
		"the replay must honour the topic recorded in the stored failure metadata")
	assert.Equal(t, "legacy.transactions", fixture.request(t).Topic)
	assert.Equal(t, "legacy.transactions", ReplayTopicFor(fixture.row),
		"the exported resolver must arrive at the same answer the publish did")

	assert.False(t, IsDeadLetterTopic(outcome.Topic),
		"a replay must never target a dead-letter topic")
	assert.NotEqual(t, fixture.row.DLTTopic, outcome.Topic,
		"the event goes back to its ORIGINAL topic, not to the .dlt sibling it was listed from")

	t.Run("the row's topic column is the next best record", func(t *testing.T) {
		row := fixture.row
		row.FailureMetadata = nil

		assert.Equal(t, "blnk.transactions", ReplayTopicFor(row))
	})

	t.Run("a dead-letter name in the metadata is refused and the row wins", func(t *testing.T) {
		row := fixture.row
		poisoned, err := json.Marshal(model.FailureMetadata{OriginalTopic: "blnk.transactions.dlt"})
		require.NoError(t, err)
		row.FailureMetadata = poisoned

		assert.Equal(t, "blnk.transactions", ReplayTopicFor(row),
			"replaying onto a dead-letter topic would loop the event back into the inventory")
	})

	t.Run("the event type's current mapping is the last resort", func(t *testing.T) {
		row := fixture.row
		row.FailureMetadata = nil
		row.Topic = ""

		assert.Equal(t, "blnk.transactions", ReplayTopicFor(row))
	})
}

// TestReplayDeadLetteredEvent_KeepsTheMessageKeyUnchanged is the ordering guarantee applied to
// the replay itself.
//
// The key is whatever the ORIGINAL publish used — the ledger the row records under requirement
// R-6, falling back to its stored partition key — and keying by it with a stable hash balancer
// is what pins every event sharing that key to one partition. A replay published under a
// different key, or under none, would land on a different partition, and the replay would
// itself violate the per-aggregate ordering the pipeline exists to preserve.
func TestReplayDeadLetteredEvent_KeepsTheMessageKeyUnchanged(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

	outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err)

	assert.Equal(t, fixture.row.LedgerID, outcome.PartitionKey,
		"the replay must be keyed by the row's ledger, exactly as the original publish was")
	assert.Equal(t, fixture.row.LedgerID, fixture.request(t).Key)
	assert.Equal(t, fixture.outcome.PartitionKey, outcome.PartitionKey,
		"the dead-letter write and the replay must use one and the same key")
	assert.NotEmpty(t, outcome.PartitionKey,
		"an empty key would spread the event across partitions and give up its ordering")
	assert.NotEqual(t, fixture.row.AggregateID, outcome.PartitionKey,
		"the recorded key must be used rather than fallen through to the aggregate")

	t.Run("a row without a ledger falls back to the stored partition key", func(t *testing.T) {
		row := fixture.row
		row.LedgerID = ""

		store := newDltFakeStore().withRow(row)
		publisher := &dltFakePublisher{}
		service := dltNewService(store, publisher, &dltFakeTransport{})

		replayed, replayErr := service.ReplayDeadLetteredEvent(context.Background(), row.EventID)
		require.NoError(t, replayErr)

		assert.Equal(t, row.PartitionKey, replayed.PartitionKey,
			"an event with no ledger still keys by the column the claim serialises on rather than "+
				"going unkeyed")
	})

	t.Run("a row without a partition key or a ledger id falls back to the aggregate", func(t *testing.T) {
		row := fixture.row
		row.PartitionKey = ""
		row.LedgerID = ""

		store := newDltFakeStore().withRow(row)
		publisher := &dltFakePublisher{}
		service := dltNewService(store, publisher, &dltFakeTransport{})

		replayed, replayErr := service.ReplayDeadLetteredEvent(context.Background(), row.EventID)
		require.NoError(t, replayErr)

		assert.Equal(t, row.AggregateID, replayed.PartitionKey,
			"the last rung of the chain still pins one aggregate's events to one partition")
	})
}

// TestReplayDeadLetteredEvent_KeepsTheEventIdUnchanged protects the subscriber's duplicate
// suppression.
//
// event_id is the idempotency key. A replay that minted a new one would be indistinguishable
// from a brand-new event, and every subscriber that deduplicates on it would process the
// replayed event a second time — turning an operator's recovery action into a double-apply.
func TestReplayDeadLetteredEvent_KeepsTheEventIdUnchanged(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

	outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err)

	assert.Equal(t, fixture.row.EventID, outcome.EventID,
		"the replayed event must carry the SAME event id")
	assert.Equal(t, fixture.row.EventID, fixture.request(t).Event.EventID)
	assert.Equal(t, fixture.row.EventType, outcome.EventType)

	// And the id on the wire agrees, which is the copy a subscriber actually deduplicates on.
	var envelope model.LedgerEvent
	require.NoError(t, json.Unmarshal(fixture.replayedMessage(t), &envelope))
	assert.Equal(t, fixture.row.EventID, envelope.EventID)
	assert.Equal(t, fixture.row.AggregateID, envelope.AggregateID)
	assert.Equal(t, model.SchemaVersionV1, envelope.SchemaVersion)
	assert.Equal(t, fixture.row.OccurredAt.UTC(), envelope.OccurredAt.UTC())
}

// TestReplayDeadLetteredEvent_LabelsTheAttemptPastTheExhaustedBudget keeps a replay out of the
// first-attempt latency reading.
//
// The publish-latency target is read as the p99 of the duration histogram filtered to
// attempt="1". An operator-triggered replay of an event that has been sitting in a dead-letter
// topic for hours is emphatically not a first attempt, and labelling it as one would poison
// the very number the target is measured against.
func TestReplayDeadLetteredEvent_LabelsTheAttemptPastTheExhaustedBudget(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

	_, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err)

	attempt := fixture.request(t).Attempt
	assert.Equal(t, dltExhaustedAttempts+replayAttemptOffset, attempt,
		"a replay must be labelled one past the exhausted budget")
	assert.Greater(t, attempt, 1, "a replay must never be recorded as attempt 1")
	assert.Greater(t, attempt, dltExhaustedAttempts,
		"the label must be past the budget so it cannot collide with a real attempt")
}

// TestReplayDeadLetteredEvent_IsNotCountedAsAFirstTimeDelivery is the other half of PERF-P21.
//
// The per-event delivery counter, blnk.events.dispatched.total, is incremented at the durable
// dispatched transition — and a successful replay takes that same transition. It must
// nevertheless NOT be counted, because this event has already been accounted for as
// dead-lettered and a counter never decrements. Counting it here would put one event in both
// terminal counters, so their sum would exceed the population; the dead-letter rate derived from
// them would then FALL as an operator worked through the inventory, reporting the pipeline as
// healthier the more triage was being done.
//
// The replay is not invisible: it is recorded on the per-attempt instruments under the fixed
// `replay` attempt label, which is where re-delivery belongs, and that is asserted alongside so
// the absence above cannot be satisfied by a replay that records nothing at all.
//
// The structural half matters as much as the behavioural one. This absence is not observable
// from a passing replay — a stray increment added on any other branch of this file would be
// invisible to a test that only drove the happy path — so the file is also asserted to make no
// reference to the instrument by any spelling.
func TestReplayDeadLetteredEvent_IsNotCountedAsAFirstTimeDelivery(t *testing.T) {
	dltPinTopicPrefix(t)

	dispatched := captureDispatchedCounter(t)
	attempts := dltCapturePublishAttempts(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

	_, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err)

	require.Len(t, fixture.store.snapshotDispatched(), 1,
		"the premise: a successful replay does take the durable dispatched transition")

	assert.Empty(t, dispatched.snapshot(),
		"and it must still not count as a delivered event: this one is already counted as "+
			"dead-lettered, so counting it again would make the two terminal counters overlap and "+
			"the dead-letter rate fall as the inventory was worked through")

	assert.NotEmpty(t, attempts.snapshot(),
		"the replay must be visible SOMEWHERE, or the absence above is satisfied by a replay "+
			"that records nothing; the per-attempt instruments are where re-delivery belongs")

	source := parseRepositoryGoFile(t, "event_dlt.go")
	assert.Zero(t, identifierUses(source, "EventsDispatchedTotal"),
		"no code path in this file may increment the per-event delivery count: the relay's "+
			"durable transition is its only owner, and a second writer here would count one "+
			"event twice")
}

// ---------------------------------------------------------------------------
// State transitions and repeat replay
// ---------------------------------------------------------------------------

// TestReplayDeadLetteredEvent_MovesTheRowToDispatchedAndClearsTheInventory pins the state
// transition event_dlt.go documents.
//
// A successful replay moves the row to DISPATCHED — the same terminal success state an ordinary
// publish reaches — and three consequences follow, all intended: the row leaves the dead-letter
// inventory so it is no longer presented as needing attention; its dlt_topic and
// failure_metadata are RETAINED so the history of what went wrong is not erased; and the status
// precondition for a second replay no longer holds.
func TestReplayDeadLetteredEvent_MovesTheRowToDispatchedAndClearsTheInventory(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

	before, err := fixture.service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{fixture.row.EventID}, dltEventIDs(before),
		"the event must be listed as dead-lettered before the replay")

	outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err)

	assert.True(t, outcome.Recorded, "the bookkeeping must be reported as done")
	dispatched := fixture.store.snapshotDispatched()
	require.Len(t, dispatched, 1, "the row must be moved on with MarkEventDispatched, exactly once")
	assert.Equal(t, fixture.row.ID, dispatched[0].id)
	assert.Equal(t, "replay-claim-token", dispatched[0].claimToken,
		"the transition must present the token the replay CLAIM issued; without it a concurrent replay could complete over the top of this one")
	assert.Equal(t, dltFixedNow, outcome.ReplayedAt)

	stored := fixture.store.row(t, fixture.row.EventID)
	assert.Equal(t, model.EventOutboxStatusDispatched, stored.Status,
		"a replayed row reaches the same terminal success state an ordinary publish does")
	assert.Equal(t, "blnk.transactions.dlt", stored.DLTTopic,
		"dlt_topic must be RETAINED: the history of what went wrong is not erased by a replay")
	assert.NotEmpty(t, stored.FailureMetadata,
		"failure_metadata must be retained for the same reason")

	after, err := fixture.service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{})
	require.NoError(t, err)
	assert.Empty(t, after.Entries,
		"a replayed event must no longer be listed as dead-lettered")
	assert.NotNil(t, after.Entries, "an empty inventory must still marshal as [] rather than null")
	assert.False(t, after.HasMore)
	assert.Nil(t, after.NextCursor)
}

// TestReplayDeadLetteredEvent_RefusesASecondReplay pins the repeat-replay decision.
//
// The two acceptable designs were "explicitly idempotent" and "explicitly rejected", and
// event_dlt.go documents the latter: the status precondition no longer holds once the row is
// dispatched, so a second replay is REFUSED with ErrEventNotDeadLettered. Silently duplicating
// the event is the outcome neither design permits, and it is what this test exists to exclude.
//
// The message is asserted too, because "not dead-lettered" alone would send an operator looking
// for the wrong problem; the implementation names the already-replayed case specifically.
func TestReplayDeadLetteredEvent_RefusesASecondReplay(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

	_, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err, "the first replay must succeed")
	require.Len(t, fixture.publisher.snapshotRequests(), 1)

	_, err = fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	dltAssertCodeAndStatus(t, err, apierror.ErrEventNotDeadLettered, http.StatusConflict)

	apiErr := dltAPIError(t, err)
	assert.Equal(t, "This event has already been replayed and cannot be replayed again", apiErr.Message,
		"the message must name the already-replayed case rather than only the state: an operator told merely \"not dead-lettered\" goes looking for the wrong problem")

	assert.Len(t, fixture.publisher.snapshotRequests(), 1,
		"a refused second replay must NOT publish the event again")
	assert.Equal(t, []int64{fixture.row.ID}, fixture.store.snapshotDispatchedIDs(),
		"and must not repeat the bookkeeping either")
}

// TestReplayDeadLetteredEvent_ReportsARepublishWhoseBookkeepingFailed covers the one case where
// a successful publish still returns an error.
//
// The event HAS been republished; only the row update failed. An error is returned rather than
// swallowed precisely because the operator must know the entry has not cleared — reporting
// success would leave a phantom in the inventory with nobody looking for it. The outcome is
// still populated, with Recorded false, so a caller can log what actually happened.
func TestReplayDeadLetteredEvent_ReportsARepublishWhoseBookkeepingFailed(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")
	fixture.store.markDispatchedErr = errors.New("connection reset by peer")

	outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	dltAssertCodeAndStatus(t, err, apierror.ErrEventReplayFailed, http.StatusInternalServerError)

	assert.False(t, outcome.Recorded,
		"the caller must be able to see that the row did not clear")
	assert.Equal(t, fixture.row.EventID, outcome.EventID,
		"the outcome must still be populated so the event can be logged")
	assert.Equal(t, model.PublishStatusDispatched, outcome.Status,
		"the publish itself succeeded, and the outcome must say so")
	assert.Len(t, fixture.publisher.snapshotRequests(), 1,
		"the event really was republished")
	assert.Equal(t, model.EventOutboxStatusDeadLettered, fixture.store.row(t, fixture.row.EventID).Status,
		"the row stays dead-lettered, so it remains visible and replayable")
}

// ---------------------------------------------------------------------------
// Typed errors, and the HTTP statuses they must resolve to
// ---------------------------------------------------------------------------

// TestDeadLetterErrorCodes_ResolveToTheirIntendedHTTPStatuses is the mapping proof.
//
// A typed code with NO statusByCode entry silently resolves to 500. Every one of the codes the
// dead-letter surface returns is therefore checked against the status its endpoint contract
// promises — the domain logic can be perfect and the API still wrong without these entries.
//
// The codes are listed as literals with their intended statuses so removing an entry from
// statusByCode fails here rather than surfacing as a 500 in production.
func TestDeadLetterErrorCodes_ResolveToTheirIntendedHTTPStatuses(t *testing.T) {
	expected := map[apierror.ErrorCode]int{
		apierror.ErrEventNotFound:        http.StatusNotFound,
		apierror.ErrEventNotDeadLettered: http.StatusConflict,
		apierror.ErrEventReplayFailed:    http.StatusInternalServerError,
		apierror.ErrKafkaUnavailable:     http.StatusServiceUnavailable,
		apierror.ErrGenValidation:        http.StatusBadRequest,
	}

	for code, status := range expected {
		assert.Equal(t, status, apierror.StatusForCode(code),
			"apierror.StatusForCode(%s) must be %d through an explicit statusByCode entry", code, status)
		assert.Equal(t, code, apierror.Normalize(code),
			"%s is already canonical and must pass through Normalize unchanged", code)
	}

	// The code strings themselves are part of the API contract that clients branch on.
	assert.Equal(t, apierror.ErrorCode("EVENT_NOT_FOUND"), apierror.ErrEventNotFound)
	assert.Equal(t, apierror.ErrorCode("EVENT_NOT_DEAD_LETTERED"), apierror.ErrEventNotDeadLettered)
	assert.Equal(t, apierror.ErrorCode("EVENT_REPLAY_FAILED"), apierror.ErrEventReplayFailed)
	assert.Equal(t, apierror.ErrorCode("EVENT_KAFKA_UNAVAILABLE"), apierror.ErrKafkaUnavailable)
}

// TestReplayDeadLetteredEvent_RejectsAnEventThatIsNotDeadLettered covers the precondition for
// every non-terminal state.
//
// Only a dead-lettered row has a dead-letter message to replay from. A failed row has exhausted
// its budget but never reached a dead-letter topic — PublishToDeadLetter is what moves it on —
// and a pending, processing or dispatched row was never a failure at all.
func TestReplayDeadLetteredEvent_RejectsAnEventThatIsNotDeadLettered(t *testing.T) {
	dltPinTopicPrefix(t)

	for _, status := range []string{
		model.EventOutboxStatusPending,
		model.EventOutboxStatusProcessing,
		model.EventOutboxStatusDispatched,
		model.EventOutboxStatusFailed,
	} {
		t.Run(status, func(t *testing.T) {
			row := dltExhaustedRow(t, "evt_state_"+status, "transaction.applied", "blnk.transactions")
			row.Status = status

			store := newDltFakeStore().withRow(row)
			publisher := &dltFakePublisher{}
			service := dltNewService(store, publisher, &dltFakeTransport{})

			_, err := service.ReplayDeadLetteredEvent(context.Background(), row.EventID)
			dltAssertCodeAndStatus(t, err, apierror.ErrEventNotDeadLettered, http.StatusConflict)

			assert.Empty(t, publisher.snapshotRequests(),
				"a refused replay must publish nothing")
			assert.Empty(t, store.snapshotDispatched(),
				"and must record nothing")
		})
	}
}

// TestReplayDeadLetteredEvent_RejectsAMissingEvent covers the not-found classification.
//
// Both shapes a store can report a missing row in are exercised, because the classifier has to
// cope with both: the repository wraps sql.ErrNoRows in a typed APIError whose CODE is what must
// be inspected — APIError does not unwrap to the error it wrapped — and a direct store
// implementation may report the bare sql.ErrNoRows.
//
// A genuine failure is deliberately NOT reclassified: a database that is down must not be
// reported as "no such event", which would send an operator looking for a typo instead of an
// outage.
func TestReplayDeadLetteredEvent_RejectsAMissingEvent(t *testing.T) {
	dltPinTopicPrefix(t)

	t.Run("no such row", func(t *testing.T) {
		service := dltNewService(newDltFakeStore(), &dltFakePublisher{}, &dltFakeTransport{})

		_, err := service.ReplayDeadLetteredEvent(context.Background(), "evt_does_not_exist")
		dltAssertCodeAndStatus(t, err, apierror.ErrEventNotFound, http.StatusNotFound)
	})

	t.Run("a bare sql.ErrNoRows is recognised", func(t *testing.T) {
		store := newDltFakeStore()
		store.getErr = sql.ErrNoRows
		service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

		_, err := service.ReplayDeadLetteredEvent(context.Background(), "evt_missing")
		dltAssertCodeAndStatus(t, err, apierror.ErrEventNotFound, http.StatusNotFound)
	})

	t.Run("an already-typed event-not-found stays itself", func(t *testing.T) {
		store := newDltFakeStore()
		store.getErr = apierror.NewAPIError(apierror.ErrEventNotFound, "No event with that id exists", nil)
		service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

		_, err := service.ReplayDeadLetteredEvent(context.Background(), "evt_missing")
		dltAssertCodeAndStatus(t, err, apierror.ErrEventNotFound, http.StatusNotFound)
	})

	t.Run("a real failure is not reclassified as not-found", func(t *testing.T) {
		store := newDltFakeStore()
		store.getErr = apierror.NewAPIError(
			apierror.ErrInternalServer, "Failed to retrieve event outbox entry", errors.New("dial tcp: refused"),
		)
		service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

		_, err := service.ReplayDeadLetteredEvent(context.Background(), "evt_unreachable")
		apiErr := dltAPIError(t, err)
		assert.NotEqual(t, apierror.ErrEventNotFound, apiErr.Code,
			"a database outage must not be reported as a missing event")
		assert.Equal(t, http.StatusInternalServerError, apierror.StatusForCode(apiErr.Code))
	})
}

// TestReplayDeadLetteredEvent_ReportsAFailedRepublish covers the publish-failure arm.
//
// The publisher's own error is wrapped in ErrEventReplayFailed rather than surfaced raw, so the
// endpoint has one code to document.
//
// # DATA-01: what the caller receives, and what it must not
//
// This test used to require the opposite of what it now requires: that the broker's own error
// text stay reachable in the API error's Details. That text is produced by the Kafka client,
// so it routinely carries the broker's address and port ("write tcp 10.0.0.4:9092: broken
// pipe") and its protocol state, and Details is serialised into the response body. An endpoint
// reporting that one event failed to republish was therefore also publishing the deployment's
// internal broker topology.
//
// The detail is now a bounded EventTransportErrorDetail: a fixed reason, the caller's own
// event identifiers, the topic, and the transient classification — which is the one part a
// caller can act on. The cause is not lost, it is redirected: it is logged with the error
// attached at the failure site, which is where an operator reads it.
func TestReplayDeadLetteredEvent_ReportsAFailedRepublish(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")
	fixture.publisher.err = errors.New("write tcp 10.0.0.4:9092: [6] Not Leader For Partition")

	outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	dltAssertCodeAndStatus(t, err, apierror.ErrEventReplayFailed, http.StatusInternalServerError)

	detail, ok := dltAPIError(t, err).Details.(EventTransportErrorDetail)
	require.True(t, ok, "a transport failure must carry the bounded detail type, got %T",
		dltAPIError(t, err).Details)
	assert.Equal(t, "the Kafka broker did not acknowledge the replayed message", detail.Reason)
	assert.Equal(t, fixture.row.EventID, detail.EventID, "the caller's own correlation handle is returned")
	assert.Equal(t, "transaction.applied", detail.EventType)
	assert.Equal(t, "blnk.transactions", detail.Topic)

	// Rendered every way a handler might render it, none of which may contain the broker.
	rendered := fmt.Sprint(detail)
	marshalled, marshalErr := json.Marshal(dltAPIError(t, err))
	require.NoError(t, marshalErr)
	for _, leak := range []string{"10.0.0.4", "9092", "Not Leader For Partition", "broken pipe"} {
		assert.NotContains(t, rendered, leak,
			"the broker's own error text must not reach a caller through %%v")
		assert.NotContains(t, string(marshalled), leak,
			"the broker's own error text must not reach a caller through the response body")
	}

	assert.False(t, outcome.Recorded)
	assert.Empty(t, fixture.store.snapshotDispatched(),
		"a failed republish must not clear the row from the inventory")
	assert.Equal(t, model.EventOutboxStatusDeadLettered, fixture.store.row(t, fixture.row.EventID).Status,
		"the event stays dead-lettered so it can be replayed again once the broker recovers")
}

// TestWriteDeadLetterMessage_DoesNotLeakTheBrokerToTheCaller is the dead-letter half of the
// same DATA-01 boundary.
//
// The dead-letter write is the other place a Kafka client error becomes an API error, and it
// is reached by the operator-facing path as well as by the relay, so it gets the same
// treatment and the same guard.
func TestWriteDeadLetterMessage_DoesNotLeakTheBrokerToTheCaller(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_broker_detail", "balance.created", "blnk.balances")
	store := newDltFakeStore().withRow(row)

	transport := &dltFakeTransport{
		// The exact shape a refused dial produces: a *net.OpError whose exported Addr
		// field carries the broker's address, wrapping the syscall error the transient
		// classifier recognises.
		writeErr: &net.OpError{
			Op:   "dial",
			Net:  "tcp",
			Addr: &net.TCPAddr{IP: net.ParseIP("10.9.8.7"), Port: 9092},
			Err:  syscall.ECONNREFUSED,
		},
	}
	service := dltNewService(store, &dltFakePublisher{}, transport)

	outcome, err := service.DeadLetter(context.Background(), row, errors.New("exhausted"))
	dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)
	assert.False(t, outcome.Published)

	detail, ok := dltAPIError(t, err).Details.(EventTransportErrorDetail)
	require.True(t, ok, "a transport failure must carry the bounded detail type, got %T",
		dltAPIError(t, err).Details)
	assert.Equal(t, "the Kafka broker did not acknowledge the dead-letter message", detail.Reason)
	assert.True(t, detail.Transient,
		"a refused connection is recoverable, and that classification is the actionable part")

	marshalled, marshalErr := json.Marshal(dltAPIError(t, err))
	require.NoError(t, marshalErr)
	for _, leak := range []string{"10.9.8.7", "9092", "connection refused", "dial"} {
		assert.NotContains(t, string(marshalled), leak,
			"a *net.OpError must not be serialised into the response body")
	}

	assert.Empty(t, store.snapshotDeadLettered(),
		"a refused write leaves the row non-terminal, exactly as the no-broker case does")
}

// TestReplayDeadLetteredEvent_RefusesToReplayWithoutABroker covers the deliberate asymmetry
// with dead-lettering.
//
// Dead-lettering degrades quietly when no broker is configured, because it is a background write
// and the event must stay visible. A replay is an explicit, operator-triggered request, so
// reporting success while publishing nothing would be a lie — it fails closed with
// ErrKafkaUnavailable instead.
func TestReplayDeadLetteredEvent_RefusesToReplayWithoutABroker(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

	noop := NewNoopEventPublisher()
	fixture.service.withTransport(noop, publisherWriterResolver(noop))

	_, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)

	assert.Empty(t, fixture.store.snapshotDispatched(),
		"nothing was published, so nothing may be recorded as replayed")
	assert.Equal(t, model.EventOutboxStatusDeadLettered, fixture.store.row(t, fixture.row.EventID).Status)
}

// TestReplayDeadLetteredEvent_RejectsABlankEventId keeps a missing path parameter from becoming
// a table scan or a nil lookup.
func TestReplayDeadLetteredEvent_RejectsABlankEventId(t *testing.T) {
	dltPinTopicPrefix(t)

	for name, eventID := range map[string]string{"empty": "", "whitespace": "   \t "} {
		t.Run(name, func(t *testing.T) {
			store := newDltFakeStore()
			service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

			_, err := service.ReplayDeadLetteredEvent(context.Background(), eventID)
			dltAssertCodeAndStatus(t, err, apierror.ErrGenValidation, http.StatusBadRequest)
			assert.Empty(t, store.getCalls, "a blank id must not reach the repository")
		})
	}

	t.Run("a surrounding-whitespace id is trimmed rather than rejected", func(t *testing.T) {
		fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

		outcome, err := fixture.service.ReplayDeadLetteredEvent(
			context.Background(), "  "+fixture.row.EventID+"  ",
		)
		require.NoError(t, err)
		assert.Equal(t, fixture.row.EventID, outcome.EventID)
	})
}

// TestDeadLetterOperations_ReportAMissingDatasourceLegibly covers the nil-store construction
// that NewBlnk(nil) makes reachable.
//
// Every operation must report it with a clear error rather than dereferencing nil, because a
// service built from such an instance is a real, if unusual, state and a panic in a handler is
// never an acceptable answer to it.
func TestDeadLetterOperations_ReportAMissingDatasourceLegibly(t *testing.T) {
	dltPinTopicPrefix(t)

	service := NewEventDeadLetterService(nil, NewNoopEventPublisher())

	_, listErr := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{})
	dltAssertCodeAndStatus(t, listErr, apierror.ErrInternalServer, http.StatusInternalServerError)

	_, replayErr := service.ReplayDeadLetteredEvent(context.Background(), "evt_any")
	dltAssertCodeAndStatus(t, replayErr, apierror.ErrInternalServer, http.StatusInternalServerError)

	_, deadLetterErr := service.DeadLetter(context.Background(), model.EventOutbox{ID: 1}, errors.New("boom"))
	dltAssertCodeAndStatus(t, deadLetterErr, apierror.ErrInternalServer, http.StatusInternalServerError)

	_, gaugeErr := service.RefreshDeadLetterAgeGauge(context.Background())
	dltAssertCodeAndStatus(t, gaugeErr, apierror.ErrInternalServer, http.StatusInternalServerError)

	assert.NoError(t, service.Close(), "closing a publisher this service does not own must be a no-op")
}

// ---------------------------------------------------------------------------
// Metrics — acceptance criteria V-3 (dead-letter rate) and V-4 (alerting)
// ---------------------------------------------------------------------------

// TestDeadLetterMetrics_CountsEachDeadLetteredEventExactlyOnce is what criterion V-3 rests on.
//
// The dead-letter RATE this counter is the numerator of must stay under 0.1%, and its
// denominator is the SUM of this counter and the published-events counter, not the published
// counter alone: the two PARTITION a captured event's terminal outcomes — an event is either
// delivered or dead-lettered, never both — so their sum is the population. Dividing by the
// successes would report dead-letters as a fraction of successes, overstating the rate and
// diverging without bound as failures rise.
//
// A double count would therefore report twice the real rate and fail a perfectly healthy system;
// a missing count would hide a genuine incident. "Exactly once" is asserted as exactly one
// increment of exactly one, not merely as "non-zero".
//
// The attribution is asserted at the same time: the counter carries the ORIGINAL category topic,
// never the `.dlt` sibling, which is what puts it in the same label space as the published-events
// counter. Attributing it to the `.dlt` name would leave the two terms of that sum in different
// label spaces and make the ratio uncomputable.
func TestDeadLetterMetrics_CountsEachDeadLetteredEventExactlyOnce(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_counted_once", "transaction.applied", "blnk.transactions")
	store := newDltFakeStore().withRow(row)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	counter := dltCaptureDeadLetterCounter(t)

	_, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
	require.NoError(t, err)

	records := counter.snapshot()
	require.Len(t, records, 1, "one dead-lettered event must produce exactly ONE increment")
	assert.Equal(t, int64(1), records[0].value, "the increment must be one, not a batch size")
	assert.Equal(t, int64(1), counter.total())

	assert.Equal(t, "blnk.transactions", records[0].attributes[publishAttrTopic],
		"the counter must be attributed to the ORIGINAL topic so it is comparable with the "+
			"published-events counter the dead-letter rate divides it by")
	assert.NotEqual(t, "blnk.transactions.dlt", records[0].attributes[publishAttrTopic])
	assert.Equal(t, "transaction.applied", records[0].attributes[publishAttrEventType])

	t.Run("a second, different event adds exactly one more", func(t *testing.T) {
		second := dltExhaustedRow(t, "evt_counted_once_again", "balance.created", "blnk.balances")
		store.withRow(second)

		_, secondErr := service.DeadLetter(context.Background(), second, errors.New("boom"))
		require.NoError(t, secondErr)

		assert.Equal(t, int64(2), counter.total(),
			"each dead-lettered event contributes exactly one")
	})
}

// TestDeadLetterMetrics_CountNothingWhenTheDeadLetteringDidNotComplete keeps the numerator
// honest.
//
// The counter answers "how many events ENDED UP dead-lettered", so it is incremented only after
// the row is recorded. Counting a failed write would inflate the rate with events that are still
// in the failed state and still being retried, and counting a write whose bookkeeping had to be
// retried would double-count the same event.
func TestDeadLetterMetrics_CountNothingWhenTheDeadLetteringDidNotComplete(t *testing.T) {
	dltPinTopicPrefix(t)

	t.Run("the write failed", func(t *testing.T) {
		row := dltExhaustedRow(t, "evt_uncounted_write", "transaction.applied", "blnk.transactions")
		service := dltNewService(
			newDltFakeStore().withRow(row),
			&dltFakePublisher{},
			&dltFakeTransport{writeErr: errors.New("broken pipe")},
		)
		counter := dltCaptureDeadLetterCounter(t)

		_, err := service.DeadLetter(context.Background(), row, errors.New("boom"))
		require.Error(t, err)
		assert.Empty(t, counter.snapshot(),
			"an event whose message never left the process has not been dead-lettered")
	})

	t.Run("the recording failed", func(t *testing.T) {
		row := dltExhaustedRow(t, "evt_uncounted_record", "transaction.applied", "blnk.transactions")
		store := newDltFakeStore().withRow(row)
		store.markDeadLetteredErr = errors.New("deadlock detected")
		service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})
		counter := dltCaptureDeadLetterCounter(t)

		_, err := service.DeadLetter(context.Background(), row, errors.New("boom"))
		require.Error(t, err)
		assert.Empty(t, counter.snapshot(),
			"the relay will retry the recording, and the retry must not double-count the event")
	})
}

// TestDeadLetterMetrics_RecordsEveryDeadLetterWriteWithItsPurposeAndDuration pins the
// contract of the shared attempt instruments for the dead-letter path.
//
// A dead-letter write is a real publish, so it belongs on those instruments — and it belongs
// there whichever way it ends. Three properties are asserted, and each was a defect:
//
//   - EVERY write is recorded, success and failure alike. Recording only successes made the
//     two signals an operator needs during a dead-letter outage invisible: the attempts
//     counter showed no failing writes and the histogram had no observations for the path
//     that was timing out, so "the dead-letter topic is unreachable" looked exactly like
//     "nothing is being dead-lettered".
//   - The OUTCOME is dead_lettered for an acknowledged write, because the pipeline-level
//     statement about the event is that it ended its life on a dead-letter topic, and failed
//     for a write that did not land.
//   - The ATTEMPT attribute is the fixed `dead_letter` token, which requires the purpose to
//     be carried. Without it the token is the attempt count from the ORIGINAL retry sequence,
//     which both misdescribes the observation and drops dead-letter latency into the
//     attempt="5" population the first-attempt latency target is read against.
func TestDeadLetterMetrics_RecordsEveryDeadLetterWriteWithItsPurposeAndDuration(t *testing.T) {
	dltPinTopicPrefix(t)

	t.Run("an acknowledged write is one dead_lettered attempt with the dead_letter token", func(t *testing.T) {
		row := dltExhaustedRow(t, "evt_attempt_outcome", "transaction.applied", "blnk.transactions")
		service := dltNewService(newDltFakeStore().withRow(row), &dltFakePublisher{}, &dltFakeTransport{})

		attempts := dltCapturePublishAttempts(t)
		duration := dltCapturePublishDuration(t)

		_, err := service.DeadLetter(context.Background(), row, errors.New("boom"))
		require.NoError(t, err)

		records := attempts.snapshot()
		require.Len(t, records, 1, "one write is one attempt")
		assert.Equal(t, string(model.PublishStatusDeadLettered), records[0].attributes[publishAttrOutcome],
			"the attempt must be attributed to the dead-lettered outcome, not to dispatched")

		observations := duration.snapshot()
		require.Len(t, observations, 1, "the write's latency must be measured, or a slow dead-letter path is invisible")
		assert.Equal(t, publishAttemptLabelDeadLetter, observations[0].attributes[publishAttrAttempt],
			"the attempt attribute must be the fixed dead_letter token: a number would come from the "+
				"original retry sequence and would contaminate the first-attempt latency population")
		assert.Equal(t, "blnk.transactions.dlt", observations[0].attributes[publishAttrTopic],
			"the duration is attributed to the topic actually written to")
		assert.GreaterOrEqual(t, observations[0].value, 0.0, "the duration is recorded in seconds")
	})

	t.Run("a write that did not land is one failed attempt, also measured", func(t *testing.T) {
		failing := dltExhaustedRow(t, "evt_attempt_outcome_failed", "transaction.applied", "blnk.transactions")
		service := dltNewService(
			newDltFakeStore().withRow(failing),
			&dltFakePublisher{},
			&dltFakeTransport{writeErr: errors.New("broken pipe")},
		)
		failedAttempts := dltCapturePublishAttempts(t)
		failedDuration := dltCapturePublishDuration(t)

		_, writeErr := service.DeadLetter(context.Background(), failing, errors.New("boom"))
		require.Error(t, writeErr)

		records := failedAttempts.snapshot()
		require.Len(t, records, 1,
			"a dead-letter write that failed is still a write this process attempted, and it is the one an "+
				"operator most needs to see: unrecorded, a broker refusing every dead-letter message is "+
				"indistinguishable from an empty dead-letter path")
		assert.Equal(t, string(model.PublishStatusRetrying), records[0].attributes[publishAttrOutcome],
			"a dead-letter write that did not land is recorded as retrying, which is literally what "+
				"happens next: the row is left non-terminal, stays in the dead-letter inventory, and "+
				"recoverUnpreservedDeadLetters re-claims it once its lease expires. dead_lettered is "+
				"reserved for a write a broker acknowledged")

		observations := failedDuration.snapshot()
		require.Len(t, observations, 1, "a timing-out write is exactly when latency data matters")
		assert.Equal(t, publishAttemptLabelDeadLetter, observations[0].attributes[publishAttrAttempt])
	})

	t.Run("a deployment with no transport records the failed attempt too", func(t *testing.T) {
		orphan := dltExhaustedRow(t, "evt_attempt_outcome_no_transport", "transaction.applied", "blnk.transactions")
		service := dltNewService(
			newDltFakeStore().withRow(orphan),
			&dltFakePublisher{},
			&dltFakeTransport{noWriter: true},
		)
		noTransportAttempts := dltCapturePublishAttempts(t)

		_, writeErr := service.DeadLetter(context.Background(), orphan, errors.New("boom"))
		require.Error(t, writeErr,
			"no transport means no dead-letter message, which is a failure and not a silent success")

		records := noTransportAttempts.snapshot()
		require.Len(t, records, 1,
			"from the event's point of view this is the same event as a refused write: its dead-letter "+
				"message did not land")
		assert.Equal(t, string(model.PublishStatusRetrying), records[0].attributes[publishAttrOutcome],
			"and it must not be dead_lettered: nothing was written, so counting it would report a "+
				"preservation that did not happen")
	})
}

// TestDeadLetterAgeGauge_ReportsTheOldestOutstandingEntry is what criterion V-4's 15-minute
// alert depends on.
//
// The gauge must report the age of the OLDEST unresolved entry. Reporting the newest would keep
// it near zero and the alert would never fire, while the queue quietly grew — a failure mode that
// looks healthy on every dashboard. The inventory therefore holds two entries of genuinely
// different ages on the same topic, and the assertion is on the older one.
//
// Rows in the FAILED state are included and attributed to the dead-letter topic they are bound
// for, because an event whose dead-letter write keeps failing is the most urgent entry in the
// inventory and excluding it would let it age indefinitely unnoticed.
func TestDeadLetterAgeGauge_ReportsTheOldestOutstandingEntry(t *testing.T) {
	dltPinTopicPrefix(t)

	oldest := dltAgedRow(t, "evt_age_oldest", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 45*time.Minute)
	newest := dltAgedRow(t, "evt_age_newest", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 2*time.Minute)
	pendingWrite := dltAgedRow(t, "evt_age_failed", "balance.created", "blnk.balances",
		model.EventOutboxStatusFailed, "", 20*time.Minute)

	// Newest first, exactly as the repository orders the inventory.
	store := newDltFakeStore().withRow(newest).withRow(pendingWrite).withRow(oldest)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	gauge := dltCaptureAgeGauge(t)

	report, err := service.RefreshDeadLetterAgeGauge(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(3), report.Outstanding,
		"both terminal failure states count as outstanding")
	assert.Equal(t, dltFixedNow, report.GeneratedAt)
	assert.Equal(t, 1, store.snapshotAgeCalls(),
		"PERF-P07: the whole gauge is ONE grouped aggregate; a per-topic or per-page query would "+
			"reintroduce the cost the count-then-deep-offset walk had")
	assert.Empty(t, store.snapshotPages(),
		"and it must read no PAGE of the inventory at all: the age is a MIN, not a scan for a minimum")

	assert.Equal(t, 45*time.Minute, report.OldestByTopic["blnk.transactions.dlt"],
		"the OLDEST entry's age must be reported, not the most recent one's")
	assert.NotEqual(t, 2*time.Minute, report.OldestByTopic["blnk.transactions.dlt"])
	assert.Equal(t, 20*time.Minute, report.OldestByTopic["blnk.balances.dlt"],
		"a failed row must be attributed to the dead-letter topic it is bound for")
	assert.Equal(t, time.Duration(0), report.OldestByTopic["blnk.identities.dlt"])
	assert.Equal(t, time.Duration(0), report.OldestByTopic["blnk.system.dlt"])
	assert.Equal(t, 45*time.Minute, report.OldestAge(),
		"OldestAge is the single number the 15-minute alert is expressed against")

	// The gauge itself carries the same values, one measurement per dead-letter topic.
	values := gauge.byTopic()
	assert.Len(t, values, len(dltAllDeadLetterTopics),
		"every dead-letter topic Blnk owns must be published")
	assert.InDelta(t, (45 * time.Minute).Seconds(), values["blnk.transactions.dlt"], 0.0001)
	assert.InDelta(t, (20 * time.Minute).Seconds(), values["blnk.balances.dlt"], 0.0001)
	assert.InDelta(t, 0.0, values["blnk.identities.dlt"], 0.0001)
	assert.InDelta(t, 0.0, values["blnk.system.dlt"], 0.0001)
	assert.Greater(t, values["blnk.transactions.dlt"], 900.0,
		"45 minutes must exceed the 900-second alert threshold, which is what makes the rule fire")

	t.Run("the age is exact however large the inventory is", func(t *testing.T) {
		// WHAT THIS REPLACED (PERF-P07). The gauge used to walk rows under a 5,000-row bound,
		// so past that bound it reported a LOWER BOUND on the age and set a Truncated flag. An
		// alert cannot fire on a lower bound: the true age crossing fifteen minutes is exactly
		// the case where the reported one might not.
		//
		// A grouped MIN has no such bound, so the assertion is now the stronger one — the age
		// of the oldest entry is reported exactly, and the cost of saying so does not grow with
		// the inventory, because the aggregate is answered from an index rather than from rows.
		crowded := newDltFakeStore()
		for i := range 10_000 {
			// Every bulk row is YOUNGER than the 45-minute fixture, so the expected answer
			// stays the one the outer test named and the assertion is about the inventory's
			// SIZE rather than about which row happens to be oldest.
			crowded = crowded.withRow(dltAgedRow(t,
				fmt.Sprintf("evt_age_bulk_%05d", i), "transaction.applied", "blnk.transactions",
				model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt",
				time.Duration((i%2000)+1)*time.Second))
		}
		crowded = crowded.withRow(oldest)

		crowdedService := dltNewService(crowded, &dltFakePublisher{}, &dltFakeTransport{})

		crowdedReport, crowdedErr := crowdedService.RefreshDeadLetterAgeGauge(context.Background())
		require.NoError(t, crowdedErr)

		assert.Equal(t, 45*time.Minute, crowdedReport.OldestAge(),
			"the oldest entry must be found whatever the inventory's size, with no lower-bound caveat")
		assert.Equal(t, int64(10_001), crowdedReport.Outstanding,
			"and the count is the aggregate's own, not a tally of rows somebody walked")
		assert.Equal(t, 1, crowded.snapshotAgeCalls(), "still one query")
		assert.Empty(t, crowded.snapshotPages(), "and still no page of rows")
	})

	t.Run("a future-dated row cannot lower the maximum", func(t *testing.T) {
		skewed := dltAgedRow(t, "evt_age_skewed", "identity.created", "blnk.identities",
			model.EventOutboxStatusDeadLettered, "blnk.identities.dlt", -10*time.Minute)
		skewedStore := newDltFakeStore().withRow(skewed)
		skewedService := dltNewService(skewedStore, &dltFakePublisher{}, &dltFakeTransport{})

		skewedReport, skewErr := skewedService.RefreshDeadLetterAgeGauge(context.Background())
		require.NoError(t, skewErr)

		assert.Equal(t, time.Duration(0), skewedReport.OldestByTopic["blnk.identities.dlt"],
			"clock skew must clamp to zero rather than report a negative age")
		assert.Equal(t, time.Duration(0), skewedReport.OldestAge())
	})
}

// TestDeadLetterAgeGauge_FallsBackToZeroWhenTheInventoryDrains is the other half of criterion
// V-4, and it is the half that is easy to get wrong.
//
// An unset gauge KEEPS ITS LAST VALUE in the exporter. An inventory that was just cleared would
// therefore keep alerting on an age that no longer exists — a permanent, unclearable alert for a
// problem that is over. Every dead-letter topic must be actively published as zero.
func TestDeadLetterAgeGauge_FallsBackToZeroWhenTheInventoryDrains(t *testing.T) {
	dltPinTopicPrefix(t)

	store := newDltFakeStore().withCount(model.EventOutboxStatusDispatched, 12)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	gauge := dltCaptureAgeGauge(t)

	report, err := service.RefreshDeadLetterAgeGauge(context.Background())
	require.NoError(t, err)

	assert.Zero(t, report.Outstanding)
	assert.Empty(t, store.snapshotPages(), "an empty inventory must not be walked at all")
	assert.Equal(t, time.Duration(0), report.OldestAge())

	values := gauge.byTopic()
	assert.Equal(t, len(dltAllDeadLetterTopics), gauge.count(),
		"one measurement per dead-letter topic must be published even when nothing is outstanding")
	for _, topic := range dltAllDeadLetterTopics {
		require.Contains(t, values, topic, "%s must be published as clear", topic)
		assert.InDelta(t, 0.0, values[topic], 0.0001,
			"%s must fall back to zero rather than hold a stale age", topic)
		assert.Equal(t, time.Duration(0), report.OldestByTopic[topic])
	}

	t.Run("draining a populated inventory returns the gauge to zero", func(t *testing.T) {
		fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")
		fixture.service.now = func() time.Time { return dltFixedNow }

		populated := dltCaptureAgeGauge(t)
		before, beforeErr := fixture.service.RefreshDeadLetterAgeGauge(context.Background())
		require.NoError(t, beforeErr)
		require.Greater(t, before.Outstanding, int64(0))
		require.Greater(t, populated.byTopic()["blnk.transactions.dlt"], 0.0)

		_, replayErr := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
		require.NoError(t, replayErr)

		drained := dltCaptureAgeGauge(t)
		after, afterErr := fixture.service.RefreshDeadLetterAgeGauge(context.Background())
		require.NoError(t, afterErr)

		assert.Zero(t, after.Outstanding)
		assert.InDelta(t, 0.0, drained.byTopic()["blnk.transactions.dlt"], 0.0001,
			"once the last entry is replayed the gauge must read zero, or the alert never clears")
	})
}

// TestReplayDeadLetteredEvent_TwoSimultaneousReplaysProduceExactlyOnePublish is the STATE-01
// proof under genuine concurrency.
//
// # Why the sequential test below is not this test
//
// TestReplayDeadLetteredEvent_ClaimsTheRowSoConcurrentReplaysCannotBothPublish runs its two
// replays one after the other, and what it establishes is that a row the first replay has
// ALREADY moved cannot be claimed again. That is a real property and it is not this one. It
// holds under an implementation with no atomicity at all — read the row, check the status in
// Go, publish — because by the time the second request runs, the first has finished and the
// stored status has changed. Two requests that arrive TOGETHER are the case that
// implementation breaks on: both read `dead_lettered`, both pass the check, and both publish.
//
// The duplicate that produces is byte-identical, because a replay republishes the stored
// bytes, so a subscriber deduplicating on event_id discards one. That is not a defence. It
// occupies a partition slot, it inflates every offset-based reconciliation, and leaning on
// consumer behaviour to cover a publishing-side defect is the opposite of a guarantee.
//
// # The arrangement
//
// Both requests are released from one barrier, and the WINNER IS PARKED inside the publisher —
// it blocks there, holding the claim, until the loser has been refused. So the loser makes its
// attempt at the moment the row is `replaying` and the first publish has not completed, which
// is precisely the window a read-check-publish implementation is wrong in. Exactly one publish
// must reach the writer, the loser must get a typed conflict, and only the winner may record a
// terminal state.
func TestReplayDeadLetteredEvent_TwoSimultaneousReplaysProduceExactlyOnePublish(t *testing.T) {
	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")
	fixture.store.nextClaimToken = "the-only-valid-token"

	// parked is closed once the loser has been refused, which is what releases the winner from
	// the publisher. loserSettled is how the publisher learns that happened.
	loserSettled := make(chan struct{})
	// Closed on cleanup as well, so a failure before the loser finishes cannot hang the test in
	// the publisher for the package's whole timeout.
	t.Cleanup(func() {
		select {
		case <-loserSettled:
		default:
			close(loserSettled)
		}
	})

	fixture.publisher.onPublish = func() {
		// The winner arrives here holding the claim. Waiting means the loser's claim attempt is
		// made while the row is `replaying` and this publish is still outstanding — the exact
		// interleaving a non-atomic check cannot survive.
		select {
		case <-loserSettled:
		case <-time.After(10 * time.Second):
			t.Error("the losing replay never settled; the winner waited in the publisher and the " +
				"claim contest did not resolve")
		}
	}

	type attempt struct {
		result ReplayOutcome
		err    error
	}
	attempts := make([]attempt, 2)

	release := make(chan struct{})
	ready := make(chan struct{}, len(attempts))

	var finished sync.WaitGroup
	for i := range attempts {
		finished.Add(1)
		go func(i int) {
			defer finished.Done()

			ready <- struct{}{}
			<-release

			attempts[i].result, attempts[i].err =
				fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)

			// The FIRST goroutine to return is the loser: the winner is parked in the publisher
			// until this fires. Closing here rather than after Wait is what makes the park
			// resolvable at all.
			select {
			case <-loserSettled:
			default:
				close(loserSettled)
			}
		}(i)
	}

	for range attempts {
		<-ready
	}
	close(release)
	finished.Wait()

	var winners, conflicts int
	for i, result := range attempts {
		if result.err == nil {
			winners++
			assert.Truef(t, result.result.Recorded,
				"attempt %d succeeded and must report the replay as recorded", i)

			continue
		}

		conflicts++
		dltAssertCodeAndStatus(t, result.err, apierror.ErrEventNotDeadLettered, http.StatusConflict)
	}

	require.Equalf(t, 1, winners,
		"EXACTLY ONE of two simultaneous replays may succeed; %d did. Two winners means the "+
			"dead-lettered precondition is checked outside the transition, and one stuck event "+
			"becomes two records on the topic", winners)
	require.Equal(t, 1, conflicts,
		"the loser must be refused with a typed conflict rather than a generic failure: an "+
			"operator retrying a replay needs to know the row was taken, not that something broke")

	assert.Lenf(t, fixture.store.snapshotReplayClaims(), 2,
		"BOTH requests must have ATTEMPTED the claim. If only one did, the contest never happened "+
			"and this test proves nothing")
	assert.Lenf(t, fixture.publisher.snapshotRequests(), 1,
		"only the request that WON the claim may publish. This is the assertion that fails when "+
			"the precondition is a separate read: both requests pass the check and both publish")

	dispatched := fixture.store.snapshotDispatched()
	require.Len(t, dispatched, 1, "only the winning request may record a terminal state")
	assert.Equal(t, "the-only-valid-token", dispatched[0].claimToken,
		"the winner must present the token ITS claim issued, or the conditional update is not "+
			"conditional on anything")

	assert.Empty(t, fixture.store.snapshotReplayReleases(),
		"a successful replay reaches dispatched; the loser never held a claim, so nothing may be "+
			"released back to dead_lettered")
}

// TestReplayDeadLetteredEvent_ClaimsTheRowSoConcurrentReplaysCannotBothPublish is the
// SEQUENTIAL half of the STATE-01 proof on the replay path.
//
// # The defect this guards against
//
// A replay used to be a READ followed by a CHECK followed by a PUBLISH: fetch the row,
// confirm it is dead-lettered, publish. Two requests for one event could both read a
// dead_lettered row, both pass the check, and both publish — so an operator
// double-clicking, or two operators working the same dead-letter backlog, put two copies
// of the event on the topic. Because a replay re-publishes the STORED bytes, those copies
// are byte-identical, so a subscriber deduplicating on event_id discards one; but the
// duplicate is real, it occupies a partition slot, and leaning on consumer behaviour to
// paper over a publishing-side defect is not a guarantee.
//
// # What is asserted, and what is NOT
//
// The precondition lives INSIDE the transition, so a row a previous replay has already moved
// cannot be claimed again: exactly one of two SEQUENTIAL replays claims the row, and the second
// is refused before reaching the publisher. The claim is asserted to have been attempted twice —
// proving the second request really did try — while the publisher saw one message.
//
// This says nothing about two requests arriving together, which is the harder case and the one
// a non-atomic check actually fails.
// TestReplayDeadLetteredEvent_TwoSimultaneousReplaysProduceExactlyOnePublish covers it.
func TestReplayDeadLetteredEvent_ClaimsTheRowSoConcurrentReplaysCannotBothPublish(t *testing.T) {
	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")
	fixture.store.nextClaimToken = "the-only-valid-token"

	first, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err, "the first replay must succeed")
	assert.True(t, first.Recorded)

	// The row is dispatched now, so a second request cannot claim it. That refusal is the
	// mechanism: the check is the transition, not a separate read.
	_, err = fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	dltAssertCodeAndStatus(t, err, apierror.ErrEventNotDeadLettered, http.StatusConflict)

	assert.Len(t, fixture.store.snapshotReplayClaims(), 2,
		"BOTH requests must have attempted the claim; if only one did, this test proves nothing about the second being refused")
	assert.Len(t, fixture.publisher.snapshotRequests(), 1,
		"only the request that WON the claim may publish; two publishes here is the duplicate the claim exists to prevent")

	dispatched := fixture.store.snapshotDispatched()
	require.Len(t, dispatched, 1, "only the winning request may record a terminal state")
	assert.Equal(t, "the-only-valid-token", dispatched[0].claimToken,
		"the winner must present the token ITS claim issued, or the conditional update is not conditional on anything")

	assert.Empty(t, fixture.store.snapshotReplayReleases(),
		"a successful replay reaches dispatched and must not release the claim back to dead_lettered")
}

// TestReplayDeadLetteredEvent_ReleasesTheClaimWhenTheReplayFails is the other half of the
// replay claim, and without it the claim would be a trap rather than a fix.
//
// A claimed row sits in the replaying state, which is outside BOTH the relay's claimable
// set and the dead-letter inventory. So a replay that claims a row and then fails to
// publish would strand the event where nothing at all would pick it up again — the fix for
// duplication would have introduced a way to lose an event's replayability entirely.
//
// Every exit path from a claimed row must therefore end in either a terminal transition or
// a release. Both failure shapes are covered: the publish failing, and the bookkeeping
// failing after a successful publish.
func TestReplayDeadLetteredEvent_ReleasesTheClaimWhenTheReplayFails(t *testing.T) {
	t.Run("a failed publish returns the row to dead_lettered", func(t *testing.T) {
		fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")
		fixture.store.nextClaimToken = "claim-then-fail"
		fixture.publisher.err = errors.New("broker refused the replay")

		_, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
		dltAssertCodeAndStatus(t, err, apierror.ErrEventReplayFailed, http.StatusInternalServerError)

		releases := fixture.store.snapshotReplayReleases()
		require.Len(t, releases, 1, "a failed replay must release its claim, or the row is stranded in replaying")
		assert.Equal(t, fixture.row.ID, releases[0].id)
		assert.Equal(t, "claim-then-fail", releases[0].claimToken,
			"the release must present the claim's own token")
		assert.Contains(t, releases[0].replayErr, "broker refused the replay",
			"the reason must be recorded so the next operator sees why the replay failed rather than only the original publish failure")

		assert.Equal(t, model.EventOutboxStatusDeadLettered,
			fixture.store.row(t, fixture.row.EventID).Status,
			"the row must be dead-lettered again, which is what keeps it replayable")

		// And it really is replayable again — the property the rollback exists for.
		fixture.publisher.err = nil
		_, retryErr := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
		require.NoError(t, retryErr, "a released row must be replayable again")
	})

	t.Run("a published event whose bookkeeping fails is still returned to the inventory", func(t *testing.T) {
		fixture := dltNewReplayFixture(t, "balance.created", "blnk.balances", "")
		fixture.store.nextClaimToken = "claim-then-mark-fails"
		fixture.store.markDispatchedErr = errors.New("connection reset")

		_, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
		dltAssertCodeAndStatus(t, err, apierror.ErrEventReplayFailed, http.StatusInternalServerError)

		assert.Len(t, fixture.publisher.snapshotRequests(), 1,
			"the event WAS republished; only the bookkeeping failed")

		releases := fixture.store.snapshotReplayReleases()
		require.Len(t, releases, 1,
			"the row must go back to dead_lettered rather than stay in replaying: the operator has been told it did not clear, so it has to be somewhere they can see it")
		assert.Equal(t, "claim-then-mark-fails", releases[0].claimToken)
		assert.Equal(t, model.EventOutboxStatusDeadLettered,
			fixture.store.row(t, fixture.row.EventID).Status)
	})
}

// TestReplayDeadLetteredEvent_ReleasesTheClaimEvenWhenTheCallerHasAlreadyGoneAway is the
// property releaseReplayClaim's detached context exists for, and the one the release tests
// above cannot reach.
//
// # Why the other release tests do not establish it
//
// TestReplayDeadLetteredEvent_ReleasesTheClaimWhenTheReplayFails proves the release HAPPENS on
// both failure shapes. It runs on context.Background(), which is never cancelled, so it holds
// identically whether the release inherits the caller's cancellation or not — and inheritance is
// exactly the defect. The commonest way a replay fails is the caller going away: an operator
// closing the tab, a proxy timing the request out, a rolling deploy taking the process down
// mid-replay. On the caller's context the rollback then fails for precisely the reason the replay
// did, every single time, and the row is left in `replaying` holding a token nobody has —
// invisible to the relay's claimable set AND to the dead-letter inventory.
//
// # The arrangement
//
// The caller's context is cancelled from inside the publisher, which is the one instant at which
// the row is claimed and the replay is not yet over. Then the publish fails. Everything after
// that point runs with a dead caller, so a release that inherited the cancellation could not
// perform its update at all — and the fake store refuses a dead context exactly as lib/pq does,
// so an inherited cancellation is a failed release rather than a silently passing one.
//
// What is required: the release ran, it saw a LIVE context, that context was BOUNDED, the row is
// dead-lettered again with the reason recorded, and it is replayable.
func TestReplayDeadLetteredEvent_ReleasesTheClaimEvenWhenTheCallerHasAlreadyGoneAway(t *testing.T) {
	failures := []struct {
		name string
		// arrange programs the failure shape, and returns the substring the recorded reason
		// must carry.
		arrange func(fixture *dltReplayFixture) string
		// publishes is how many publish attempts the shape produces, which distinguishes
		// "the publish failed" from "the publish succeeded and the bookkeeping failed".
		publishes int
	}{
		{
			name: "the publish fails after the caller is gone",
			arrange: func(fixture *dltReplayFixture) string {
				fixture.publisher.err = errors.New("broker refused the replay")

				return "broker refused the replay"
			},
			publishes: 1,
		},
		{
			name: "the bookkeeping fails after the caller is gone",
			arrange: func(fixture *dltReplayFixture) string {
				fixture.store.markDispatchedErr = errors.New("connection reset")

				return "connection reset"
			},
			publishes: 1,
		},
	}

	for _, failure := range failures {
		t.Run(failure.name, func(t *testing.T) {
			fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")
			fixture.store.nextClaimToken = "claim-held-by-a-gone-caller"
			wantReason := failure.arrange(fixture)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// CANCELLED FROM INSIDE THE PUBLISH: the row is claimed, the replay is under way,
			// and from here on every context derived from the caller's is dead.
			fixture.publisher.onPublish = cancel

			_, err := fixture.service.ReplayDeadLetteredEvent(ctx, fixture.row.EventID)
			require.Error(t, err, "a replay that did not complete must report a failure")

			require.Equal(t, failure.publishes, len(fixture.publisher.snapshotRequests()),
				"the publish must have been attempted; cancelling before it would test a different path")
			require.Errorf(t, ctx.Err(),
				"the caller's context must be dead by the time the rollback runs, or this test is "+
					"the same as the one that runs on context.Background()")

			releases := fixture.store.snapshotReplayReleases()
			require.Lenf(t, releases, 1,
				"the claim MUST be released even though the caller is gone. Zero releases means the "+
					"rollback ran on the caller's context and died with it, leaving the row in "+
					"`replaying` with a token nobody holds — outside the relay's claimable set and "+
					"outside the dead-letter inventory")

			release := releases[0]
			assert.NoErrorf(t, release.contextErr,
				"the release must run on a context DETACHED from the caller's cancellation. It saw "+
					"%v, which is the caller's own cancellation inherited — the failure mode that "+
					"makes the most likely replay failure the least recoverable", release.contextErr)
			assert.Truef(t, release.hasDeadline,
				"the detached context must still be BOUNDED. Detaching without a bound would hold "+
					"the rollback open indefinitely against a database that has gone away, on work "+
					"the claim's own lease recovery already covers")
			if release.hasDeadline {
				assert.WithinDurationf(t, time.Now().Add(replayReleaseTimeout), release.deadline,
					2*time.Second,
					"the bound must be replayReleaseTimeout (%s); a much longer one is an unbounded "+
						"rollback in disguise", replayReleaseTimeout)
			}

			assert.Equal(t, fixture.row.ID, release.id)
			assert.Equal(t, "claim-held-by-a-gone-caller", release.claimToken,
				"the release must present the token the claim issued, or the conditional update "+
					"matches nothing")
			assert.Containsf(t, release.replayErr, wantReason,
				"the reason must be recorded, so the next operator sees why THIS attempt failed "+
					"rather than only the original publish failure")

			assert.Equal(t, model.EventOutboxStatusDeadLettered,
				fixture.store.row(t, fixture.row.EventID).Status,
				"the row must be dead-lettered again, which is what keeps it replayable and visible")

			// AND IT REALLY IS REPLAYABLE, on a fresh caller. Anything less means the cancellation
			// cost the event its replayability, which is the outcome the release exists to prevent.
			fixture.publisher.err = nil
			fixture.publisher.onPublish = nil
			fixture.store.markDispatchedErr = nil
			_, retryErr := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
			require.NoError(t, retryErr,
				"a released row must be replayable by the next request; a cancelled caller must not "+
					"cost the event its replayability")
		})
	}
}

// TestDeadLetterAgeGauge_ReducesToTheMaximumRatherThanTheLastRowSeen closes the one gap the
// straightforward fixture above cannot close.
//
// The inventory is ordered by OCCURRENCE, while an entry's age is measured from its LAST ATTEMPT —
// and those two orderings genuinely disagree. An event can occur late and be given up on almost
// immediately, while an older event sits in a relay backlog and is only given up on minutes ago.
// When that happens the entry appearing LATER in the page is NEWER by age.
//
// So the per-topic reduction has to be a MAXIMUM. An implementation that simply assigned each age
// in turn would report whichever row it happened to see last — two minutes here — and the
// 15-minute alert would never fire for the entry that has genuinely been stuck for 45. A fixture
// whose occurrence and age orderings agreed would pass either way, which is exactly why this case
// is written out separately.
func TestDeadLetterAgeGauge_ReducesToTheMaximumRatherThanTheLastRowSeen(t *testing.T) {
	dltPinTopicPrefix(t)

	// Occurred 50 minutes ago, given up on 45 minutes ago: it has been sitting on the
	// dead-letter topic ever since, and it is the entry the alert exists for.
	stale := dltAgedRow(t, "evt_age_stale", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 45*time.Minute)
	stale.OccurredAt = dltFixedNow.Add(-50 * time.Minute)

	// Occurred 90 minutes ago but sat in a relay backlog, so its final attempt was only two
	// minutes ago. It occurred EARLIER, so it sorts later in the occurrence-ordered inventory,
	// yet it is far newer by age.
	lateRetry := dltAgedRow(t, "evt_age_late_retry", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 2*time.Minute)
	lateRetry.OccurredAt = dltFixedNow.Add(-90 * time.Minute)

	require.True(t, stale.OccurredAt.After(lateRetry.OccurredAt),
		"the fixture is only meaningful while the occurrence and age orderings disagree")
	require.True(t, stale.LastAttemptedAt.Before(*lateRetry.LastAttemptedAt))

	// Newest occurrence first, exactly as the repository orders it — which puts the NEWER-by-age
	// row last.
	store := newDltFakeStore().withRow(stale).withRow(lateRetry)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	gauge := dltCaptureAgeGauge(t)

	report, err := service.RefreshDeadLetterAgeGauge(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 45*time.Minute, report.OldestByTopic["blnk.transactions.dlt"],
		"the reduction must keep the MAXIMUM age, not the age of the last row examined")
	assert.NotEqual(t, 2*time.Minute, report.OldestByTopic["blnk.transactions.dlt"],
		"reporting the last row seen would understate the age and silence the 15-minute alert")
	assert.Equal(t, 45*time.Minute, report.OldestAge())
	assert.InDelta(t, (45 * time.Minute).Seconds(), gauge.byTopic()["blnk.transactions.dlt"], 0.0001)
	assert.Greater(t, gauge.byTopic()["blnk.transactions.dlt"], 900.0,
		"the published value must cross the 900-second threshold the alert rule is written against")
}

// TestDeadLetterAgeGauge_IsTheOnlyMaintainerOfTheAgeGauge keeps the newest entry from being
// published as the oldest.
//
// The dead-letter path only ever sees the event being given up on RIGHT NOW, which is the newest
// entry. Setting the age gauge there would report an age near zero every time, silently defeating
// the 15-minute alert while looking healthy.
func TestDeadLetterAgeGauge_IsTheOnlyMaintainerOfTheAgeGauge(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow(t, "evt_gauge_untouched", "transaction.applied", "blnk.transactions")
	service := dltNewService(newDltFakeStore().withRow(row), &dltFakePublisher{}, &dltFakeTransport{})

	gauge := dltCaptureAgeGauge(t)

	_, err := service.DeadLetter(context.Background(), row, errors.New("boom"))
	require.NoError(t, err)

	assert.Zero(t, gauge.count(),
		"dead-lettering an event must not touch the oldest-message-age gauge")
}

// dltAgedRow builds an inventory entry of a known age, in a known terminal failure state.
//
// The age is expressed as a duration before the fixed clock and applied to the LAST ATTEMPT,
// which is the instant an entry's age is measured from: the moment the event was given up on, and
// therefore the moment it started sitting in a dead-letter topic.
//
// A dead-lettered entry also carries its failure metadata, because that is what the repository
// holds for such a row and what the listing hands to the API projection. A failed entry carries
// none — it has not reached a dead-letter topic yet — which is itself a state the inventory has to
// present.
func dltAgedRow(
	t *testing.T,
	eventID, eventType, topic, status, dltTopic string,
	age time.Duration,
) model.EventOutbox {
	t.Helper()

	row := dltExhaustedRow(t, eventID, eventType, topic)

	lastAttempt := dltFixedNow.Add(-age)
	firstAttempt := lastAttempt.Add(-31 * time.Second)
	row.FirstAttemptedAt = &firstAttempt
	row.LastAttemptedAt = &lastAttempt
	row.OccurredAt = firstAttempt.Add(-time.Minute)
	row.Status = status
	row.DLTTopic = dltTopic

	// Re-stamped because occurred_at moved: the stored envelope carries that instant, so
	// leaving the inherited bytes behind would describe a row whose event_raw disagrees with
	// its own columns — a state no writer produces.
	dltStampCanonicalEnvelope(t, &row)

	if status == model.EventOutboxStatusDeadLettered {
		// Produced by the implementation's own builder, so the fixture cannot describe a shape
		// the code would never write.
		metadata, err := json.Marshal(
			BuildFailureMetadata(row, errors.New(dltPublishFailureReason), dltExhaustedAttempts, dltFixedNow),
		)
		if err != nil {
			panic("blnk: the dead-letter test fixture must serialise: " + err.Error())
		}
		row.FailureMetadata = metadata
	}

	return row
}

// ---------------------------------------------------------------------------
// Listing the dead-letter inventory
// ---------------------------------------------------------------------------

// TestListDeadLetterEvents_ReadsTheOutboxTableAndNeverAKafkaTopic is a design commitment, not an
// implementation detail.
//
// Blnk implements NO CONSUMER — building one is explicitly out of scope — and it does not need
// one, because the outbox row already carries dlt_topic and failure_metadata. The listing is
// therefore available with the broker down, which is precisely when an operator wants it.
//
// The proof is behavioural: the transport is rigged so that ANY attempt to resolve a writer fails
// the test, and the publisher counts every call. A listing that consumed a topic — or that even
// resolved a transport speculatively — would trip one of them.
func TestListDeadLetterEvents_ReadsTheOutboxTableAndNeverAKafkaTopic(t *testing.T) {
	dltPinTopicPrefix(t)

	entry := dltAgedRow(t, "evt_list_source", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 5*time.Minute)

	store := newDltFakeStore().withRow(entry)
	transport := &dltFakeTransport{resolveErr: errors.New("a listing must not touch the transport")}
	publisher := &dltFakePublisher{}
	service := dltNewService(store, publisher, transport)

	page, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{})
	require.NoError(t, err, "listing must work with no broker reachable at all")

	require.Len(t, page.Entries, 1)
	assert.Equal(t, entry.EventID, page.Entries[0].EventID)
	assert.Equal(t, "blnk.transactions.dlt", page.Entries[0].DLTTopic,
		"the dead-letter topic comes from the ROW, which is why no consumer is needed")
	assert.NotEmpty(t, page.Entries[0].FailureMetadata,
		"the failure record comes from the row as well")
	assert.Equal(t, len(entry.Payload), page.Entries[0].PayloadBytes,
		"the projection must report the body's SIZE; PERF-P06 removed the body itself from the "+
			"listing, so this is the only thing a triage view can say about it")
	assert.False(t, page.HasMore, "a single-entry inventory has no further page")
	assert.Nil(t, page.NextCursor, "and therefore no cursor")

	assert.Empty(t, transport.snapshotRequested(),
		"no writer may be resolved to serve a listing")
	assert.Empty(t, publisher.snapshotRequests(),
		"no publish may be issued to serve a listing")
	assert.Equal(t,
		[]model.DeadLetterInventoryQuery{{Limit: defaultDeadLetterListLimit}},
		store.snapshotInventoryPages(),
		"an unfiltered page must be one repository query, passed straight through")

	// The stored failure record decodes through the one shared decoder, which is how the API
	// projection reads it.
	metadata, decodeErr := DecodeFailureMetadata(page.Entries[0].FailureMetadata)
	require.NoError(t, decodeErr)
	require.NotNil(t, metadata)
	assert.Equal(t, "blnk.transactions", metadata.OriginalTopic)

	t.Run("a row that was never dead-lettered decodes to no metadata rather than an error", func(t *testing.T) {
		for name, raw := range map[string]json.RawMessage{
			"absent": nil,
			"empty":  json.RawMessage(""),
			"null":   json.RawMessage("null"),
		} {
			t.Run(name, func(t *testing.T) {
				decoded, err := DecodeFailureMetadata(raw)
				require.NoError(t, err, "an absent failure record is an ordinary state of the inventory")
				assert.Nil(t, decoded)
			})
		}
	})
}

// TestListDeadLetterEvents_CoversBothTerminalFailureStates keeps the events most in need of
// attention from being hidden.
//
// A row becomes FAILED the moment its retry budget is spent, and DEAD_LETTERED only once the
// event has additionally reached its `.dlt` sibling. Listing only the latter would hide exactly
// the events whose dead-letter write itself failed.
func TestListDeadLetterEvents_CoversBothTerminalFailureStates(t *testing.T) {
	dltPinTopicPrefix(t)

	deadLettered := dltAgedRow(t, "evt_list_dead", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 3*time.Minute)
	failed := dltAgedRow(t, "evt_list_failed", "balance.created", "blnk.balances",
		model.EventOutboxStatusFailed, "", 9*time.Minute)

	service := dltNewService(
		newDltFakeStore().withRow(deadLettered).withRow(failed),
		&dltFakePublisher{}, &dltFakeTransport{},
	)

	entries, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{})
	require.NoError(t, err)

	assert.Equal(t, []string{deadLettered.EventID, failed.EventID}, dltEventIDs(entries),
		"both terminal failure states must be listed, in the repository's newest-first order")
}

// TestListDeadLetterEvents_PagesWithTheDocumentedBounds pins the page arithmetic.
//
// A caller asking for a page that is too large has made a recoverable mistake, and degrading to
// a sane page is more useful than an error. The ceiling matters operationally: without it a
// triage endpoint becomes a full-table scan.
//
// The cursor is passed through VERBATIM rather than normalised (PERF-P08). It names a position
// and there is nothing to clamp; the offset it replaced was clamped at zero and unbounded above,
// which bounded nothing that mattered.
func TestListDeadLetterEvents_PagesWithTheDocumentedBounds(t *testing.T) {
	dltPinTopicPrefix(t)

	store := newDltFakeStore()
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	resume := &model.DeadLetterCursor{OccurredAt: time.Now().UTC().Add(-time.Hour), ID: 4242}

	cases := []struct {
		name          string
		given         DeadLetterListOptions
		expectedLimit int
		expectedCurs  *model.DeadLetterCursor
	}{
		{
			name:          "the zero value asks for the default page",
			given:         DeadLetterListOptions{},
			expectedLimit: defaultDeadLetterListLimit,
		},
		{
			name:          "a non-positive limit falls back to the default",
			given:         DeadLetterListOptions{Limit: 0},
			expectedLimit: defaultDeadLetterListLimit,
		},
		{
			name:          "a negative limit falls back to the default",
			given:         DeadLetterListOptions{Limit: -25},
			expectedLimit: defaultDeadLetterListLimit,
		},
		{
			name:          "an oversized limit is clamped to the ceiling",
			given:         DeadLetterListOptions{Limit: maxDeadLetterListLimit + 5000},
			expectedLimit: maxDeadLetterListLimit,
		},
		{
			name:          "an explicit limit is passed through unchanged",
			given:         DeadLetterListOptions{Limit: 25},
			expectedLimit: 25,
		},
		{
			name:          "a cursor is passed through verbatim",
			given:         DeadLetterListOptions{Limit: 25, Cursor: resume},
			expectedLimit: 25,
			expectedCurs:  resume,
		},
	}

	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := service.ListDeadLetterEvents(context.Background(), testCase.given)
			require.NoError(t, err)

			pages := store.snapshotInventoryPages()
			require.Len(t, pages, index+1)
			assert.Equal(t, testCase.expectedLimit, pages[index].Limit)
			assert.Equal(t, testCase.expectedCurs, pages[index].Cursor,
				"a cursor names a position and there is nothing to clamp, so it must reach the "+
					"repository exactly as the caller gave it")
		})
	}

	assert.Equal(t, 50, defaultDeadLetterListLimit, "the default page size is a published bound")
	assert.Equal(t, 500, maxDeadLetterListLimit, "the page ceiling is a published bound")
}

// TestListDeadLetterEvents_AppliesTheExposedFilters covers each filter and the combination.
//
// The topic filter is applied to the ORIGINAL topic rather than to the `.dlt` sibling, which is
// what makes "show me the transaction events that are stuck" expressible without the caller
// having to know the suffix convention.
//
// Paging over a FILTERED set resumes from the last MATCH rather than from a row position, so a
// filtered page behaves like a page of the filtered set rather than a filtered page of the
// unfiltered set — the latter would return short, unstable pages as the filter's hit rate varied.
// PERF-P06 moved the filters into the SQL statement, so this is now the statement's WHERE clause
// being exercised through the fake rather than an application-side match loop.
func TestListDeadLetterEvents_AppliesTheExposedFilters(t *testing.T) {
	dltPinTopicPrefix(t)

	applied := dltAgedRow(t, "evt_filter_applied", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", time.Minute)
	void := dltAgedRow(t, "evt_filter_void", "transaction.void", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 2*time.Minute)
	appliedAgain := dltAgedRow(t, "evt_filter_applied_again", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusFailed, "", 3*time.Minute)
	balance := dltAgedRow(t, "evt_filter_balance", "balance.monitor", "blnk.balances",
		model.EventOutboxStatusDeadLettered, "blnk.balances.dlt", 4*time.Minute)

	store := newDltFakeStore().withRow(applied).withRow(void).withRow(appliedAgain).withRow(balance)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	list := func(t *testing.T, opts DeadLetterListOptions) []string {
		t.Helper()

		page, err := service.ListDeadLetterEvents(context.Background(), opts)
		require.NoError(t, err)

		return dltEventIDs(page)
	}

	t.Run("by event type", func(t *testing.T) {
		assert.Equal(t,
			[]string{applied.EventID, appliedAgain.EventID},
			list(t, DeadLetterListOptions{EventType: "transaction.applied"}))
	})

	t.Run("by original topic", func(t *testing.T) {
		assert.Equal(t,
			[]string{applied.EventID, void.EventID, appliedAgain.EventID},
			list(t, DeadLetterListOptions{Topic: "blnk.transactions"}),
			"a topic filter must match the original topic, including a failed row with no dlt_topic yet")
	})

	t.Run("by status", func(t *testing.T) {
		assert.Equal(t,
			[]string{appliedAgain.EventID},
			list(t, DeadLetterListOptions{Status: model.EventOutboxStatusFailed}))
		assert.Equal(t,
			[]string{applied.EventID, void.EventID, balance.EventID},
			list(t, DeadLetterListOptions{Status: model.EventOutboxStatusDeadLettered}))
	})

	t.Run("filters combine", func(t *testing.T) {
		assert.Equal(t,
			[]string{applied.EventID},
			list(t, DeadLetterListOptions{
				EventType: "transaction.applied",
				Topic:     "blnk.transactions",
				Status:    model.EventOutboxStatusDeadLettered,
			}))
	})

	t.Run("surrounding whitespace is ignored", func(t *testing.T) {
		assert.Equal(t,
			[]string{applied.EventID, appliedAgain.EventID},
			list(t, DeadLetterListOptions{EventType: "  transaction.applied  "}))
	})

	t.Run("comparison is exact and case sensitive", func(t *testing.T) {
		assert.Empty(t, list(t, DeadLetterListOptions{EventType: "TRANSACTION.APPLIED"}),
			"producers emit fixed literals, so case folding would differ from the unfiltered path")
	})

	t.Run("a filtered page resumes from the last MATCH, not from a row position", func(t *testing.T) {
		first, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{
			EventType: "transaction.applied",
			Limit:     1,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{applied.EventID}, dltEventIDs(first))
		require.True(t, first.HasMore,
			"two rows match the filter, so a page of one must advertise more")
		require.NotNil(t, first.NextCursor,
			"and must hand back the position of the last MATCH it returned")

		second, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{
			EventType: "transaction.applied",
			Limit:     1,
			Cursor:    first.NextCursor,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{appliedAgain.EventID}, dltEventIDs(second),
			"resuming from a match must continue the FILTERED sequence, never re-walk the "+
				"unfiltered one")
		assert.False(t, second.HasMore, "the filtered set is exhausted")
		assert.Nil(t, second.NextCursor)
	})

	t.Run("a filter matching nothing returns an empty page, not nil", func(t *testing.T) {
		page, err := service.ListDeadLetterEvents(
			context.Background(), DeadLetterListOptions{EventType: "identity.created"},
		)
		require.NoError(t, err)
		assert.Empty(t, page.Entries)
		assert.NotNil(t, page.Entries, "an empty result must marshal as [] rather than null")
		assert.False(t, page.HasMore)
		assert.Nil(t, page.NextCursor)
	})

	t.Run("every filter reaches the repository rather than being applied above it", func(t *testing.T) {
		// The defect this closes: the service used to filter IN MEMORY by paging the
		// inventory up to a fixed scan ceiling, so a page that hit the ceiling was returned
		// looking exactly like a complete one and an exact total was impossible. Both were
		// properties of WHERE the filtering happened, which is why the assertion is about
		// what the repository was asked for and not only about what came back.
		resume := &model.DeadLetterCursor{OccurredAt: time.Now().UTC().Add(-time.Hour), ID: 512}
		narrowing := DeadLetterListOptions{
			EventType: "transaction.applied",
			Topic:     "blnk.transactions",
			Status:    model.EventOutboxStatusDeadLettered,
			Limit:     25,
			Cursor:    resume,
		}

		fresh := newDltFakeStore()
		freshService := dltNewService(fresh, &dltFakePublisher{}, &dltFakeTransport{})

		_, err := freshService.ListDeadLetterEvents(context.Background(), narrowing)
		require.NoError(t, err)

		require.Equal(t, []model.DeadLetterInventoryQuery{{
			EventType: "transaction.applied",
			Topic:     "blnk.transactions",
			Status:    model.EventOutboxStatusDeadLettered,
			Limit:     25,
			Cursor:    resume,
		}}, fresh.snapshotInventoryPages(),
			"a filtered page must be ONE repository query carrying the whole narrowing")
	})
}

// TestListDeadLetterEvents_AppliesTheOccurrenceWindow covers the filter an operator reaches
// for during an incident.
//
// "What is stuck from the twenty minutes the broker was down" is the question actually
// asked, and before this filter existed the only way to answer it was to page the whole
// inventory and read timestamps by eye — the endpoint refused the parameter outright. The
// window bounds occurred_at, the instant the ledger mutation happened, which is what
// correlates against an incident timeline and is also the column the inventory is ordered by.
//
// Both ends are inclusive, so a window stated from an incident's first to last second
// contains the events at both edges. A REVERSED window is refused rather than returning an
// empty page, for the same reason an unrecognised status is: an empty page reads to an
// operator as "nothing is stuck".
func TestListDeadLetterEvents_AppliesTheOccurrenceWindow(t *testing.T) {
	dltPinTopicPrefix(t)

	// dltAgedRow's age argument is measured back from the fixed clock, so a larger age is
	// an OLDER occurrence. Recorded here as the instants themselves so the window bounds
	// below read as bounds rather than as arithmetic.
	newest := dltAgedRow(t, "evt_window_newest", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", time.Minute)
	middle := dltAgedRow(t, "evt_window_middle", "transaction.void", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 10*time.Minute)
	oldest := dltAgedRow(t, "evt_window_oldest", "balance.created", "blnk.balances",
		model.EventOutboxStatusFailed, "", 30*time.Minute)

	service := dltNewService(
		newDltFakeStore().withRow(newest).withRow(middle).withRow(oldest),
		&dltFakePublisher{}, &dltFakeTransport{},
	)

	list := func(t *testing.T, opts DeadLetterListOptions) []string {
		t.Helper()

		entries, err := service.ListDeadLetterEvents(context.Background(), opts)
		require.NoError(t, err)

		return dltEventIDs(entries)
	}

	t.Run("a two-sided window selects the middle slice", func(t *testing.T) {
		assert.Equal(t,
			[]string{newest.EventID, middle.EventID},
			list(t, DeadLetterListOptions{
				OccurredFrom: middle.OccurredAt,
				OccurredTo:   newest.OccurredAt,
			}))
	})

	t.Run("both bounds are inclusive", func(t *testing.T) {
		assert.Equal(t,
			[]string{middle.EventID},
			list(t, DeadLetterListOptions{
				OccurredFrom: middle.OccurredAt,
				OccurredTo:   middle.OccurredAt,
			}),
			"an equal-bound window must select the events at exactly that instant, or a caller "+
				"correlating against a precise timestamp gets nothing")
	})

	t.Run("a one-sided window leaves the other end unbounded", func(t *testing.T) {
		assert.Equal(t,
			[]string{newest.EventID, middle.EventID},
			list(t, DeadLetterListOptions{OccurredFrom: middle.OccurredAt}))
		assert.Equal(t,
			[]string{middle.EventID, oldest.EventID},
			list(t, DeadLetterListOptions{OccurredTo: middle.OccurredAt}))
	})

	t.Run("the window combines with the other filters", func(t *testing.T) {
		assert.Equal(t,
			[]string{middle.EventID},
			list(t, DeadLetterListOptions{
				Topic:        "blnk.transactions",
				OccurredFrom: oldest.OccurredAt,
				OccurredTo:   middle.OccurredAt,
			}))
	})

	t.Run("a reversed window is a validation error, not an empty page", func(t *testing.T) {
		store := newDltFakeStore()
		strict := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

		_, err := strict.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{
			OccurredFrom: newest.OccurredAt,
			OccurredTo:   oldest.OccurredAt,
		})
		dltAssertCodeAndStatus(t, err, apierror.ErrGenValidation, http.StatusBadRequest)
		assert.Contains(t, err.Error(), "occurred_from",
			"the refusal must name the parameters the caller sent, so the fix is one edit rather "+
				"than a hunt through the runbook")
		assert.Contains(t, err.Error(), "occurred_to")

		assert.Empty(t, store.snapshotInventoryPages(),
			"an unsatisfiable window must not reach the repository")

		_, countErr := strict.CountDeadLetterEvents(context.Background(), DeadLetterListOptions{
			OccurredFrom: newest.OccurredAt,
			OccurredTo:   oldest.OccurredAt,
		})
		dltAssertCodeAndStatus(t, countErr, apierror.ErrGenValidation, http.StatusBadRequest)
		assert.Empty(t, store.snapshotCountQueries(),
			"the count must refuse the same window the listing refuses")
	})
}

// TestListAndCountDeadLetterEvents_IsOneReadNotTwo pins the shape of the paired read.
//
// The page and the total used to be two service calls, and no amount of shared predicate makes
// two reads observe one population: an entry dead-lettered between them is counted by one and
// absent from the other, and the total then describes a set the page is not a slice of.
//
// The assertion is on the CALLS rather than on the numbers, because the numbers agree in a quiet
// fixture either way. What distinguishes the fixed shape from the broken one is that the pair
// leaves the service through a single seam method — the one the repository answers from a single
// REPEATABLE READ snapshot.
func TestListAndCountDeadLetterEvents_IsOneReadNotTwo(t *testing.T) {
	dltPinTopicPrefix(t)

	applied := dltAgedRow(t, "evt_pair_applied", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", time.Minute)
	appliedAgain := dltAgedRow(t, "evt_pair_applied_again", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusFailed, "", 2*time.Minute)
	balance := dltAgedRow(t, "evt_pair_balance", "balance.created", "blnk.balances",
		model.EventOutboxStatusDeadLettered, "blnk.balances.dlt", 3*time.Minute)

	store := newDltFakeStore().withRow(applied).withRow(appliedAgain).withRow(balance)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	page, total, err := service.ListAndCountDeadLetterEvents(context.Background(),
		DeadLetterListOptions{Limit: 1, EventType: "transaction.applied"})
	require.NoError(t, err)

	require.Len(t, page.Entries, 1, "the page bound is honoured by the paired read too")
	assert.True(t, page.HasMore)
	require.NotNil(t, page.NextCursor, "a page with more behind it must carry its cursor")
	assert.Equal(t, int64(2), total,
		"the total counts the narrowing and ignores the page, so a client can see the size of the "+
			"backlog it is walking")

	// ONE NARROWING, REACHING BOTH HALVES. The fake records what the count was asked for
	// separately from what the page was, so a paired read that filtered only the page would be
	// visible here.
	counted := store.snapshotInventoryCountQueries()
	require.Len(t, counted, 1, "the paired read must count exactly once")
	assert.Empty(t, store.snapshotCountQueries(),
		"the paired read counts over the INVENTORY predicate, the one its page is drawn with, "+
			"rather than over the full-row listing's")
	assert.Equal(t, "transaction.applied", counted[0].EventType,
		"the count must carry the page's filter")
	assert.Zero(t, counted[0].Limit,
		"the page bound must not reach the count: which matches to return says nothing about how "+
			"many there are")

	paged := store.snapshotInventoryPages()
	require.Len(t, paged, 1, "the paired read must page exactly once")
	assert.Equal(t, 1, paged[0].Limit)
	assert.Equal(t, "transaction.applied", paged[0].EventType)
}

// TestListAndCountDeadLetterEvents_RefusesTheSameNarrowingTheSingleReadsDo keeps validation in
// front of the paired read.
//
// A third entry point into the inventory is a third place a bad filter could slip past. The
// normalisation is shared rather than repeated, and this is the assertion that it actually runs.
func TestListAndCountDeadLetterEvents_RefusesTheSameNarrowingTheSingleReadsDo(t *testing.T) {
	dltPinTopicPrefix(t)

	store := newDltFakeStore()
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	for name, opts := range map[string]DeadLetterListOptions{
		"a status the inventory cannot hold": {Status: model.EventOutboxStatusDispatched},
		"a reversed occurrence window": {
			OccurredFrom: time.Now().UTC(),
			OccurredTo:   time.Now().UTC().Add(-time.Hour),
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := service.ListAndCountDeadLetterEvents(context.Background(), opts)
			dltAssertCodeAndStatus(t, err, apierror.ErrGenValidation, http.StatusBadRequest)

			assert.Empty(t, store.snapshotInventoryPages(),
				"a refused narrowing must not reach the database at all")
		})
	}
}

// TestListAndCountDeadLetterEvents_ReportsAMissingDatasource covers the nil-store guard the
// other two reads already carry, because a third entry point is a third way to panic.
func TestListAndCountDeadLetterEvents_ReportsAMissingDatasource(t *testing.T) {
	var service *EventDeadLetterService

	_, _, err := service.ListAndCountDeadLetterEvents(context.Background(), DeadLetterListOptions{})
	dltAssertCodeAndStatus(t, err, apierror.ErrInternalServer, http.StatusInternalServerError)
}

// ---------------------------------------------------------------------------------------
// The dead-letter count, stated ONCE
// ---------------------------------------------------------------------------------------

// TestCountDeadLetterEvents_CountsTheSameSetThePageIsDrawnFrom is the whole count contract, in
// one place.
//
// # Why this suite is one suite
//
// It replaces FOUR test functions that asserted the same contract over four different fixture
// sets — CountsTheFilteredSet, CountsTheSameNarrowingAsThePage, CountsTheSameSetTheListingReturns
// and IsAboutTheSamePopulationAsThePage — plus two more that overlapped on the refusals. Between
// them they ran the same six narrowings four times, re-derived "the page does not affect the
// total" three times, and asserted the shared-predicate property in three slightly different
// ways. That is not additional coverage. It is one contract with four owners, and the cost is
// paid on every change: a seventh narrowing has to be added in four places, and the reader who
// finds only three of them cannot tell whether the fourth is a deliberate exception.
//
// Everything the four asserted is asserted here, and each property exactly once:
//
//   - The count answers each filter dimension, their combination, and zero for a narrowing that
//     matches nothing.
//   - The count agrees with the page it accompanies when the page can hold every match.
//   - LIMIT and OFFSET have no bearing on it, because a count is of the SET.
//   - The narrowing reaches the repository, TRIMMED, as one query per request.
//   - The count and the listing narrow by the same predicate — compared field by field, with the
//     page bounds zeroed, because the page a listing asks for is legitimately not the page a
//     count asks for.
func TestCountDeadLetterEvents_CountsTheSameSetThePageIsDrawnFrom(t *testing.T) {
	dltPinTopicPrefix(t)

	// ONE fixture set, chosen so every dimension distinguishes something and no two narrowings
	// select the same rows: two event types on one topic, a third on another, and both terminal
	// failure states represented — `failed` included because it is the entry an operator most
	// needs to see, its retry budget spent while its dead-letter write has not landed.
	applied := dltAgedRow(t, "evt_count_applied", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", time.Minute)
	void := dltAgedRow(t, "evt_count_void", "transaction.void", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 2*time.Minute)
	appliedFailed := dltAgedRow(t, "evt_count_applied_failed", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusFailed, "", 3*time.Minute)
	balance := dltAgedRow(t, "evt_count_balance", "balance.monitor", "blnk.balances",
		model.EventOutboxStatusDeadLettered, "blnk.balances.dlt", 4*time.Minute)

	store := newDltFakeStore().withRow(applied).withRow(void).withRow(appliedFailed).withRow(balance)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	narrowings := []struct {
		name    string
		options DeadLetterListOptions
		total   int64
	}{
		{"unfiltered counts both terminal states", DeadLetterListOptions{}, 4},
		{"by event type", DeadLetterListOptions{EventType: "transaction.applied"}, 2},
		{"by topic", DeadLetterListOptions{Topic: "blnk.transactions"}, 3},
		{"by status", DeadLetterListOptions{Status: model.EventOutboxStatusFailed}, 1},
		{
			"filters combine",
			DeadLetterListOptions{
				EventType: "transaction.applied",
				Status:    model.EventOutboxStatusDeadLettered,
			},
			1,
		},
		{
			"a combination nothing matches counts zero rather than failing",
			DeadLetterListOptions{EventType: "transaction.applied", Topic: "blnk.balances"},
			0,
		},
		{"an event type outside the inventory counts zero", DeadLetterListOptions{EventType: "ledger.created"}, 0},
	}

	for _, narrowing := range narrowings {
		t.Run(narrowing.name, func(t *testing.T) {
			total, err := service.CountDeadLetterEvents(context.Background(), narrowing.options)
			require.NoError(t, err)
			assert.Equal(t, narrowing.total, total)

			// THE COUNT AND THE PAGE DESCRIBE ONE SET. Asserting the listing's length against
			// the total, for a page large enough to hold every match, is what makes that
			// agreement observable rather than assumed: a total that disagrees with the page it
			// accompanies makes a paging client stop early or loop for ever.
			page, listErr := service.ListDeadLetterEvents(context.Background(), narrowing.options)
			require.NoError(t, listErr)
			assert.Len(t, page.Entries, int(narrowing.total),
				"the page and its total must describe the same population")

			// AND THE PAGE HAS NO BEARING ON THE TOTAL. A caller reading page two of a backlog
			// still needs its full size.
			paged := narrowing.options
			paged.Limit, paged.Offset = 2, 3
			pagedTotal, pagedErr := service.CountDeadLetterEvents(context.Background(), paged)
			require.NoError(t, pagedErr)
			assert.Equal(t, narrowing.total, pagedTotal,
				"LIMIT and OFFSET select WHICH matches to return and have no bearing on how many "+
					"there are")
		})
	}

	t.Run("the narrowing reaches the repository, trimmed, as one query", func(t *testing.T) {
		counting := newDltFakeStore()
		countingService := dltNewService(counting, &dltFakePublisher{}, &dltFakeTransport{})

		_, err := countingService.CountDeadLetterEvents(context.Background(), DeadLetterListOptions{
			EventType: " transaction.applied ",
			Topic:     " blnk.transactions ",
			Status:    model.EventOutboxStatusDeadLettered,
		})
		require.NoError(t, err)

		counted := counting.snapshotDeadLetterCounts()
		require.Len(t, counted, 1, "one count request must be one repository query")
		assert.Equal(t, "transaction.applied", counted[0].EventType,
			"the predicate must arrive trimmed: a padded value narrows to nothing in SQL")
		assert.Equal(t, "blnk.transactions", counted[0].Topic)
		assert.Equal(t, model.EventOutboxStatusDeadLettered, counted[0].Status)
	})

	t.Run("the count and the listing narrow by exactly the same predicate", func(t *testing.T) {
		fresh := newDltFakeStore().withRow(applied).withRow(void).withRow(appliedFailed)
		sharing := dltNewService(fresh, &dltFakePublisher{}, &dltFakeTransport{})

		// A page deliberately SMALLER than the match set, so a count that echoed the page rather
		// than the predicate would answer 1 instead of 2.
		options := DeadLetterListOptions{
			EventType: "transaction.applied",
			Topic:     "blnk.transactions",
			Limit:     1,
		}

		page, err := sharing.ListDeadLetterEvents(context.Background(), options)
		require.NoError(t, err)
		require.Len(t, page.Entries, 1, "the page is deliberately smaller than the match set")

		total, err := sharing.CountDeadLetterEvents(context.Background(), options)
		require.NoError(t, err)
		assert.Equal(t, int64(2), total,
			"the total must describe the FILTERED set and not the page, or a paging client cannot "+
				"know when to stop")

		listQueries := fresh.snapshotInventoryQueries()
		countQueries := fresh.snapshotDeadLetterCounts()
		require.Len(t, listQueries, 1)
		require.Len(t, countQueries, 1)

		// Compared on the NARROWING alone, with the bounds zeroed: the page a listing asks for is
		// legitimately not the page a count asks for, and it is the predicate that has to be
		// identical. Two independently assembled predicates would let a total describe a
		// population the page was not drawn from.
		listed, counted := listQueries[0], countQueries[0]
		listed.Limit, listed.Offset = 0, 0
		counted.Limit, counted.Offset = 0, 0
		assert.Equal(t, listed, counted,
			"an operator must not page through one set while being told the size of another")
	})
}

// TestCountDeadLetterEvents_RefusesAndPropagatesExactlyAsTheListingDoes is the count's failure
// contract, also in one place.
//
// It replaces the refusal halves of the four consolidated suites plus two dedicated ones. The
// property being defended throughout is that the count NEVER answers zero for a question it
// could not answer: zero is a real and reassuring number — "the backlog is empty" — so returning
// it for a rejected filter, a database outage or an uninitialised service would report a healthy
// dead-letter inventory in exactly the situations where nobody knows what the inventory holds.
func TestCountDeadLetterEvents_RefusesAndPropagatesExactlyAsTheListingDoes(t *testing.T) {
	dltPinTopicPrefix(t)

	t.Run("an unsupported status filter is refused before the repository is touched", func(t *testing.T) {
		store := newDltFakeStore().withRow(dltAgedRow(t, "evt_status_guard", "transaction.applied",
			"blnk.transactions", model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", time.Minute))
		service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

		for _, status := range []string{
			model.EventOutboxStatusPending,
			model.EventOutboxStatusProcessing,
			model.EventOutboxStatusDispatched,
			"nonsense",
		} {
			options := DeadLetterListOptions{Status: status}

			// THE LISTING AND THE COUNT MUST REFUSE THE SAME THING. A count that accepted a
			// filter the listing rejects would report a number for a set the caller can never
			// page through.
			_, listErr := service.ListDeadLetterEvents(context.Background(), options)
			require.Errorf(t, listErr, "%q is not a terminal failure state and is not a filter", status)
			assert.Equal(t, apierror.ErrGenValidation, dltAPIError(t, listErr).Code)

			total, countErr := service.CountDeadLetterEvents(context.Background(), options)
			dltAssertCodeAndStatus(t, countErr, apierror.ErrGenValidation, http.StatusBadRequest)
			assert.Zerof(t, total, "a refused count must not also report a number")
		}

		assert.Empty(t, store.snapshotDeadLetterCounts(),
			"a rejected filter must never reach the database")
		assert.Empty(t, store.snapshotCountQueries())
	})

	t.Run("the repository's own failure is propagated, not reported as zero", func(t *testing.T) {
		failing := newDltFakeStore()
		failing.countDeadLetterErr = apierror.NewAPIError(
			apierror.ErrInternalServer, "Failed to count dead-lettered events",
			errors.New("dial tcp: refused"),
		)
		service := dltNewService(failing, &dltFakePublisher{}, &dltFakeTransport{})

		total, err := service.CountDeadLetterEvents(context.Background(), DeadLetterListOptions{})
		assert.Zero(t, total)
		dltAssertCodeAndStatus(t, err, apierror.ErrInternalServer, http.StatusInternalServerError)
	})

	t.Run("a service with no datasource refuses rather than answering zero", func(t *testing.T) {
		_, err := NewEventDeadLetterService(nil, nil).
			CountDeadLetterEvents(context.Background(), DeadLetterListOptions{})
		dltAssertCodeAndStatus(t, err, apierror.ErrInternalServer, http.StatusInternalServerError)

		// And a nil receiver, which is what a caller holding an unconstructed service has.
		var nilService *EventDeadLetterService
		_, nilErr := nilService.CountDeadLetterEvents(context.Background(), DeadLetterListOptions{})
		dltAssertCodeAndStatus(t, nilErr, apierror.ErrInternalServer, http.StatusInternalServerError)
	})
}

// ---------------------------------------------------------------------------------------
// SEC-09 — the filtered inventory is complete, and its total says so
// ---------------------------------------------------------------------------------------

// TestListDeadLetterEvents_PushesEveryPredicateToTheRepository is the SEC-09 guard at the
// service layer.
//
// # The defect
//
// The service used to serve a filtered page by WALKING the repository's unfiltered pages and
// applying the predicates in Go, abandoning the walk after five thousand rows and logging a
// warning. The page it returned was then indistinguishable from a complete one, and the only
// record that the scan had given up was a log line the operator was not reading. During a loss
// investigation "nothing more is stuck" is the most dangerous wrong answer this endpoint gives.
//
// # What is asserted, and why it is the repository CALL rather than the result
//
// A result-only assertion would pass against the old walk too: with a small fixture the walk
// never reached its bound and returned the right rows. What distinguishes the two designs is
// WHAT THE SERVICE ASKS THE DATABASE FOR — one filtered query, or many unfiltered ones. So the
// recorded calls are the assertion.
func TestListDeadLetterEvents_PushesEveryPredicateToTheRepository(t *testing.T) {
	dltPinTopicPrefix(t)

	applied := dltAgedRow(t, "evt_push_applied", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", time.Minute)
	balance := dltAgedRow(t, "evt_push_balance", "balance.monitor", "blnk.balances",
		model.EventOutboxStatusDeadLettered, "blnk.balances.dlt", 2*time.Minute)

	store := newDltFakeStore().withRow(applied).withRow(balance)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	_, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{
		EventType: "transaction.applied",
		Topic:     "blnk.transactions",
		Status:    model.EventOutboxStatusDeadLettered,
		Limit:     10,
		Offset:    3,
	})
	require.NoError(t, err)

	// THE INVENTORY READ is what the listing issues: a narrow, keyset-paged projection that
	// never reads a payload (PERF-P06). It is recorded twice — once as the narrowing and once
	// as the page — because the two are asserted separately below.
	require.Len(t, store.inventoryQueries, 1,
		"ONE query, filtered. A walk would issue a page request per lap, and it is the number "+
			"of requests — not the rows they returned — that tells the two designs apart")
	assert.Empty(t, store.listCalls,
		"the listing must not read whole rows: the projection answers it, and reading bodies to "+
			"build a listing of kilobytes was the finding")

	assert.Equal(t, model.DeadLetterInventoryFilter{
		EventType: "transaction.applied",
		Topic:     "blnk.transactions",
		Status:    model.EventOutboxStatusDeadLettered,
		Limit:     10,
	}, store.inventoryQueries[0],
		"every predicate must reach the repository, and none may be invented")

	require.Len(t, store.inventoryPages, 1)
	assert.Equal(t, 10, store.inventoryPages[0].Limit,
		"the page BOUND goes to SQL, so the LIMIT applies to matching rows rather than to a "+
			"page that was filtered afterwards")
}

// TestListDeadLetterEvents_NoLongerTruncatesALargeFilteredInventory is the regression test for
// the five-thousand-row bound itself.
//
// It builds an inventory larger than deadLetterScanMaxRows in which every MATCHING entry sits
// past the old bound, which is the exact shape that produced a silently empty page: the walk
// scanned five thousand non-matching rows, gave up, and reported success with nothing in it.
// The entry now comes back, and the total confirms it — because the database applied the
// predicate before the page was taken.
func TestListDeadLetterEvents_NoLongerTruncatesALargeFilteredInventory(t *testing.T) {
	dltPinTopicPrefix(t)

	store := newDltFakeStore()

	// Newest first, matching repository order, so the needle is genuinely at the far end of
	// the inventory — past where the old walk stopped looking.
	for i := 0; i < deadLetterScanMaxRows+10; i++ {
		filler := dltAgedRow(t, fmt.Sprintf("evt_filler_%05d", i), "transaction.applied",
			"blnk.transactions", model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt",
			time.Duration(i)*time.Second)
		store.withRow(filler)
	}

	needle := dltAgedRow(t, "evt_needle", "identity.created", "blnk.identities",
		model.EventOutboxStatusDeadLettered, "blnk.identities.dlt", 24*time.Hour)
	store.withRow(needle)

	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})
	opts := DeadLetterListOptions{EventType: "identity.created"}

	entries, err := service.ListDeadLetterEvents(context.Background(), opts)
	require.NoError(t, err)
	assert.Equal(t, []string{needle.EventID}, dltEventIDs(entries),
		"an entry past the old scan bound must still be found: a filtered page that stopped "+
			"looking told an operator nothing was stuck when something was")

	total, err := service.CountDeadLetterEvents(context.Background(), opts)
	require.NoError(t, err)
	assert.Equal(t, int64(1), total,
		"and the total must agree with the page, which is the completeness check the "+
			"truncated walk defeated")
}

// ---------------------------------------------------------------------------------------
// SEC-08 — retention may only ever delete a receipt
// ---------------------------------------------------------------------------------------

// TestRefreshDeadLetterAgeGauge_CountsEveryOutstandingEntry is what keeps the
// DeadLetterMessageStuck alert honest, and it is the counterpart of the retention rule: an
// entry leaves this gauge by being REPLAYED and by nothing else.
//
// A `resolve` endpoint used to exempt an entry from the gauge — and from the retention purge —
// on an operator's word alone, without anything having been delivered. It has been removed: it
// was a fourteenth management route on a thirteen-route surface, and it could not compose with
// replay, because a resolved row whose re-publish the broker acknowledged could not then be
// marked dispatched. So the gauge counts every outstanding entry, and a successful replay is
// what takes one out of it.
func TestRefreshDeadLetterAgeGauge_CountsEveryOutstandingEntry(t *testing.T) {
	dltPinTopicPrefix(t)

	recent := dltAgedRow(t, "evt_gauge_recent", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 20*time.Minute)
	oldest := dltAgedRow(t, "evt_gauge_oldest", "transaction.void", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 40*time.Hour)

	store := newDltFakeStore().withRow(recent).withRow(oldest)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	report, err := service.RefreshDeadLetterAgeGauge(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(2), report.Outstanding,
		"every entry that has not been replayed is outstanding: an exemption granted without a "+
			"delivery is how a stuck event stops being reported")

	// The 40-hour entry sets the gauge, because the alert is about the OLDEST thing nobody has
	// got. Reporting the newest would keep a permanently stuck event permanently invisible.
	assert.GreaterOrEqual(t, report.OldestByTopic["blnk.transactions.dlt"], 40*time.Hour)

	// ONE GROUPED AGGREGATE, and no walk at all: the answer is computed in the statement rather
	// than by paging the inventory and filtering in Go, which is what makes the age exact at any
	// backlog size instead of a lower bound drawn from a bounded scan. Asserting the call COUNT
	// is how the two designs are told apart: a walk would issue a page request per lap.
	assert.Equal(t, 1, store.ageCalls,
		"the age gauge must be one grouped aggregate per refresh, not a walk whose cost grows "+
			"with the backlog it is measuring")
	assert.Empty(t, store.listCalls,
		"no row may be fetched to compute an age: the aggregate answers it in the database")
}

// TestListDeadLetterEvents_ReturnsAnEmptyPageRatherThanNil covers the normalisation on the
// unfiltered path.
//
// The repository returns a nil slice for an empty page. Normalising it means a caller can range
// over the result without a nil check and a handler renders [] rather than null — a difference
// every JSON client notices.
func TestListDeadLetterEvents_ReturnsAnEmptyPageRatherThanNil(t *testing.T) {
	dltPinTopicPrefix(t)

	service := dltNewService(newDltFakeStore(), &dltFakePublisher{}, &dltFakeTransport{})

	page, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{})
	require.NoError(t, err)
	assert.NotNil(t, page.Entries, "an empty inventory must still yield a non-nil slice")
	assert.Empty(t, page.Entries)
	assert.False(t, page.HasMore)
	assert.Nil(t, page.NextCursor)

	encoded, marshalErr := json.Marshal(page.Entries)
	require.NoError(t, marshalErr)
	assert.Equal(t, "[]", string(encoded))
}

// TestListDeadLetterEvents_RejectsAnUnsupportedStatusFilter is the one filter that is validated
// rather than clamped.
//
// A filter that quietly matched nothing would answer "nothing is stuck" to an operator who asked
// a different question — which is exactly the wrong answer to give during an incident.
func TestListDeadLetterEvents_RejectsAnUnsupportedStatusFilter(t *testing.T) {
	dltPinTopicPrefix(t)

	store := newDltFakeStore()
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	for _, status := range []string{
		model.EventOutboxStatusPending,
		model.EventOutboxStatusProcessing,
		model.EventOutboxStatusDispatched,
		"nonsense",
	} {
		t.Run(status, func(t *testing.T) {
			_, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{Status: status})
			dltAssertCodeAndStatus(t, err, apierror.ErrGenValidation, http.StatusBadRequest)
		})
	}

	assert.Empty(t, store.snapshotPages(), "a rejected filter must not reach the repository")

	t.Run("the repository's own failure is propagated unchanged", func(t *testing.T) {
		failing := newDltFakeStore()
		failing.listErr = apierror.NewAPIError(
			apierror.ErrInternalServer, "Failed to list dead-lettered events", errors.New("dial tcp: refused"),
		)
		failingService := dltNewService(failing, &dltFakePublisher{}, &dltFakeTransport{})

		_, err := failingService.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{})
		dltAssertCodeAndStatus(t, err, apierror.ErrInternalServer, http.StatusInternalServerError)
	})
}

// TestListDeadLetterEvents_DelegatesFilteringToTheRepository asserts the service asks for a
// FILTERED page rather than filtering a page it was given.
//
// This is the shape of the fix, and the shape is what makes the guarantee. Filtering used to
// happen here: the service asked for unfiltered pages of 500, applied the predicates in Go, and
// stopped after a fixed row budget. So one filtered request meant many repository requests, and
// the answer was a prefix of the matches with nothing to say so.
//
// Exactly ONE request, carrying the caller's page and the caller's filter, is therefore the
// assertion. A service that walked again would fail on the count of requests; one that dropped
// the predicates would fail on the recorded filter.
func TestListDeadLetterEvents_DelegatesFilteringToTheRepository(t *testing.T) {
	dltPinTopicPrefix(t)

	store := newDltFakeStore()
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	_, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{
		Limit:     25,
		Offset:    50,
		EventType: "  transaction.applied  ",
		Topic:     "blnk.transactions",
		Status:    model.EventOutboxStatusDeadLettered,
	})
	require.NoError(t, err)

	pages := store.snapshotInventoryPages()
	require.Len(t, pages, 1,
		"one filtered request must be one repository query; several means the predicates are being applied above the database")
	assert.Equal(t, 25, pages[0].Limit, "the caller's page size must reach the repository")
	assert.Equal(t, "transaction.applied", pages[0].EventType,
		"the trimmed event-type narrowing must reach the repository rather than being applied above it")
	assert.Equal(t, "blnk.transactions", pages[0].Topic,
		"the trimmed topic narrowing must reach the repository rather than being applied above it")
	assert.Equal(t, model.EventOutboxStatusDeadLettered, pages[0].Status,
		"the status narrowing must reach the repository rather than being applied above it")
}

// TestListDeadLetterEvents_ReturnsMatchesTheFormerScanBudgetWouldHaveHidden is the regression
// test for the silent truncation.
//
// The inventory here is deliberately LARGER than deadLetterScanMaxRows and every match sits
// past that bound. Under the walk this returned an empty page with a 200 — an operator asking
// "what identity events are stuck" was told "none" while three were — and no field in the
// response distinguished that from an exhausted scan. WithScanLimit is set to something small
// as well, to state that the gauge's bound is not the listing's bound: the listing has none.
func TestListDeadLetterEvents_ReturnsMatchesTheFormerScanBudgetWouldHaveHidden(t *testing.T) {
	dltPinTopicPrefix(t)

	store := newDltFakeStore()

	// Bulk filler built directly rather than through dltAgedRow: these rows are only ever
	// counted past, never inspected, and stamping a canonical envelope on each of 5,200
	// fixtures would cost seconds for nothing.
	for i := 0; i < deadLetterScanMaxRows+200; i++ {
		store.withRow(model.EventOutbox{
			ID:        int64(1_000_000 + i),
			EventID:   fmt.Sprintf("evt_filler_%05d", i),
			EventType: "transaction.applied",
			Topic:     "blnk.transactions",
			Status:    model.EventOutboxStatusDeadLettered,
			DLTTopic:  "blnk.transactions.dlt",
		})
	}

	// The matches, appended LAST, so they are the deepest rows in the inventory.
	buried := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		eventID := fmt.Sprintf("evt_buried_%d", i)
		store.withRow(model.EventOutbox{
			ID:        int64(2_000_000 + i),
			EventID:   eventID,
			EventType: "identity.created",
			Topic:     "blnk.identities",
			Status:    model.EventOutboxStatusDeadLettered,
			DLTTopic:  "blnk.identities.dlt",
		})
		buried = append(buried, eventID)
	}

	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{}).WithScanLimit(10)

	entries, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{
		EventType: "identity.created",
	})
	require.NoError(t, err)
	assert.Equal(t, buried, dltEventIDs(entries),
		"a match deeper in the inventory than any scan budget must still be listed: answering 200 with an empty page tells an operator nothing is stuck when something is")

	total, countErr := service.CountDeadLetterEvents(context.Background(), DeadLetterListOptions{
		EventType: "identity.created",
	})
	require.NoError(t, countErr)
	assert.Equal(t, int64(len(buried)), total,
		"the total must be of the filtered set, not of the table and not of the page")

	assert.Len(t, store.snapshotInventoryPages(), 1, "the listing must not page the inventory at all")
}

// ---------------------------------------------------------------------------
// Scope boundary
// ---------------------------------------------------------------------------

// TestEventDeadLetterSource_BuildsNoConsumerSurface enforces the MUST NOT boundary structurally.
//
// Blnk publishes the `<topic>.dlt` naming convention AND NOTHING MORE. It does not build a
// consumer or consumer-group library, it does not manage a subscriber's own dead-letter topics,
// and it implements no consumer error-handling or poison-message framework — a subscriber's
// consumption failures are the subscriber's to handle.
//
// Asserted over the source text rather than left to review, because the boundary is easy to cross
// with a single well-intentioned import: a kafka.Reader here would be the moment Blnk started
// owning consumption. The listing and the replay read the outbox ROW precisely so that no
// consumer is ever needed.
func TestEventDeadLetterSource_BuildsNoConsumerSurface(t *testing.T) {
	code := dltReadCode(t, "event_dlt.go")

	for _, consumerSurface := range []string{
		"kafka.Reader",
		"kafka.NewReader",
		"kafka.ReaderConfig",
		"ReadMessage",
		"FetchMessage",
		"ConsumerGroup",
		"CommitMessages",
	} {
		assert.NotContains(t, code, consumerSurface,
			"event_dlt.go must not reference %s: Blnk implements no consumer, and the listing and "+
				"replay both read the outbox row so that none is needed", consumerSurface)
	}

	// The boundary is documented in the file itself, so the next contributor reads it before
	// reaching for a reader. This one is asserted against the raw source, because a comment is
	// exactly what is being required.
	assert.Contains(t, dltReadSource(t, "event_dlt.go"), "subscriber-side dead-lettering is NOT Blnk's",
		"the scope boundary must stay documented at the top of event_dlt.go")

	// And the `.dlt` suffix has exactly one source of truth, in event_topics.go. Spelling it out
	// here would fork the published convention.
	assert.NotContains(t, code, `".dlt"`,
		"the .dlt suffix must be resolved through DLTFor, never spelled out in event_dlt.go")
	assert.Contains(t, code, "DLTFor(",
		"every dead-letter destination must be resolved through the single naming function")

	// The convention itself, asserted where subscribers read it.
	assert.Equal(t, ".dlt", DeadLetterTopicSuffix,
		"the published suffix is a breaking change to every subscriber, script, alert and runbook")
	assert.Equal(t, dltAllDeadLetterTopics, AllDeadLetterTopics(),
		"the Blnk-owned dead-letter topics are the whole of what Blnk owns")
	for _, topic := range dltAllDeadLetterTopics {
		assert.True(t, IsDeadLetterTopic(topic))
		assert.Equal(t, topic, DLTFor(topic),
			"DLTFor must be idempotent so a name can never become %s.dlt", topic)
	}
}

// ---------------------------------------------------------------------------------------
// Kafka transport policy — CRYPTO-01 and PRIV-01
//
// These tests cover NewKafkaTransport, which is the security boundary BOTH Kafka clients
// dial through: the producer that publishes ledger events, and the administrative client
// that mints credentials. Two decisions live there and nowhere else — whether the connection
// is encrypted, and which principal it authenticates as.
//
// They live in this file rather than beside the publisher because event_publisher_test.go
// belongs to a later checkpoint and is not in this scope, while the dead-letter path is an
// in-scope consumer of exactly this transport: every dead-letter write and every replay goes
// over it. Placing the guards here keeps them running now instead of waiting for a file
// another author owns.
//
// Nothing here performs I/O. Building a transport reads TLS material from disk and prepares a
// SCRAM mechanism, both local, so every case below is decided before a socket would open —
// which is the point: a misconfiguration must be refused at construction, not discovered on
// the first publish hours later.
// ---------------------------------------------------------------------------------------

// kafkaTransportConfig builds a Kafka configuration with a valid producer credential pair and
// TLS explicitly disabled-with-acknowledgement, which is the local development posture.
//
// Each test then changes the ONE field it is about, so a failure names the field rather than
// leaving the reader to diff two literals.
func kafkaTransportConfig() config.KafkaConfig {
	return config.KafkaConfig{
		Brokers:          []string{"localhost:9092"},
		TopicPrefix:      DefaultTopicPrefix,
		SASLUser:         "blnk-producer",
		SASLSecret:       "producer-secret",
		InsecureLocalDev: true,
	}
}

// TestNewKafkaTransport_RefusesPlaintextUnlessLocalDevIsAcknowledged is the CRYPTO-01 guard.
//
// SASL/SCRAM authenticates the client to the broker. It does not encrypt the connection and it
// does not authenticate the broker to the client, so over SASL_PLAINTEXT the SCRAM exchange and
// every produce request travel in the clear — including identity events carrying names, email
// addresses, phone numbers, postal addresses and dates of birth.
//
// The transport therefore refuses to dial without TLS unless an operator has explicitly said
// this is a local development broker. The refusal is what makes plaintext an opt-in rather than
// the accident of an unset variable: a deployment that simply never set KAFKA_TLS_ENABLED fails
// at construction instead of silently shipping ledger data unencrypted.
func TestNewKafkaTransport_RefusesPlaintextUnlessLocalDevIsAcknowledged(t *testing.T) {
	cfg := kafkaTransportConfig()
	cfg.InsecureLocalDev = false

	for _, role := range []KafkaTransportRole{KafkaTransportRoleProducer, KafkaTransportRoleAdmin} {
		roleCfg := cfg
		if role == KafkaTransportRoleAdmin {
			roleCfg.SASLAdminUser, roleCfg.SASLAdminSecret = "blnk-admin", "admin-secret"
		}

		transport, err := NewKafkaTransport(roleCfg, role)
		require.Error(t, err, "the %s transport must refuse to dial in the clear", role)
		assert.Nil(t, transport)
		assert.Contains(t, err.Error(), "KAFKA_TLS_ENABLED",
			"the refusal must name the variable that turns encryption on")
		assert.Contains(t, err.Error(), "KAFKA_INSECURE_LOCAL_DEV",
			"and the variable that acknowledges a local broker, so the operator has both choices")
	}
}

// TestNewKafkaTransport_AcceptsAcknowledgedPlaintextForTheLocalStack covers the one supported
// route to an unencrypted connection.
//
// The local single-broker KRaft stack listens on SASL_PLAINTEXT and only on that, so the escape
// hatch has to exist. It is deliberately named for what it is, and a nil TLS configuration is
// what makes kafka-go dial in the clear — asserted here so the acknowledgement is proven to
// have an effect rather than merely being accepted.
func TestNewKafkaTransport_AcceptsAcknowledgedPlaintextForTheLocalStack(t *testing.T) {
	transport, err := NewKafkaTransport(kafkaTransportConfig(), KafkaTransportRoleProducer)
	require.NoError(t, err)
	require.NotNil(t, transport)

	assert.Nil(t, transport.TLS, "an acknowledged local broker dials without TLS")
	assert.NotNil(t, transport.SASL, "it is still authenticated, which is a separate concern from encryption")
}

// TestNewKafkaTransport_BuildsAVerifiedTLSConfiguration covers the production posture.
//
// Three properties are asserted because each one is separately capable of being wrong while the
// connection still appears to work: the protocol floor, the server name used for verification,
// and that verification is on.
func TestNewKafkaTransport_BuildsAVerifiedTLSConfiguration(t *testing.T) {
	cfg := kafkaTransportConfig()
	cfg.InsecureLocalDev = false
	cfg.TLS = config.KafkaTLSConfig{Enabled: true, ServerName: "kafka.internal"}

	transport, err := NewKafkaTransport(cfg, KafkaTransportRoleProducer)
	require.NoError(t, err)
	require.NotNil(t, transport.TLS)

	assert.Equal(t, uint16(tls.VersionTLS12), transport.TLS.MinVersion,
		"TLS 1.2 is the floor; anything earlier has known weaknesses")
	assert.Equal(t, "kafka.internal", transport.TLS.ServerName,
		"the verified name must be the configured one, which is what makes an address that does not match the certificate usable")
	assert.False(t, transport.TLS.InsecureSkipVerify, "verification must be on by default")
}

// TestNewKafkaTransport_RefusesToSkipVerificationOutsideLocalDev covers the subtler half of
// CRYPTO-01.
//
// TLS with verification disabled still encrypts, so it looks like a working secure deployment.
// It stops a passive reader and does nothing whatsoever about an active one: an interposed
// broker is indistinguishable from the real one, and it collects the SCRAM handshake. A
// deployment that set this would believe it had a protection it does not have, which is why it
// is refused rather than warned about.
func TestNewKafkaTransport_RefusesToSkipVerificationOutsideLocalDev(t *testing.T) {
	cfg := kafkaTransportConfig()
	cfg.InsecureLocalDev = false
	cfg.TLS = config.KafkaTLSConfig{Enabled: true, InsecureSkipVerify: true}

	transport, err := NewKafkaTransport(cfg, KafkaTransportRoleProducer)
	require.Error(t, err)
	assert.Nil(t, transport)
	assert.Contains(t, err.Error(), "KAFKA_TLS_INSECURE_SKIP_VERIFY")
}

// TestNewKafkaTransport_RefusesAnUnusableCertificateAuthorityFile covers the trust-pool trap.
//
// An empty x509.CertPool is not an empty trust decision: with no certificates appended, the TLS
// stack falls back to the SYSTEM roots, so a CA file that parsed to nothing would silently widen
// trust from "the one authority this deployment issued its broker certificate from" to "every
// authority the host trusts". The failure has to be at load time, because afterwards it is
// indistinguishable from a correct configuration.
func TestNewKafkaTransport_RefusesAnUnusableCertificateAuthorityFile(t *testing.T) {
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, []byte("this is not a certificate\n"), 0o600))

	cfg := kafkaTransportConfig()
	cfg.InsecureLocalDev = false
	cfg.TLS = config.KafkaTLSConfig{Enabled: true, CAFile: caPath}

	transport, err := NewKafkaTransport(cfg, KafkaTransportRoleProducer)
	require.Error(t, err)
	assert.Nil(t, transport)
	assert.Contains(t, err.Error(), "no usable PEM certificate")
	assert.Contains(t, err.Error(), "system roots",
		"the message must say what the silent fallback would have been")
}

// TestNewKafkaTransport_RefusesAMissingCertificateAuthorityFile keeps an unreadable path from
// being treated as "no CA configured".
func TestNewKafkaTransport_RefusesAMissingCertificateAuthorityFile(t *testing.T) {
	cfg := kafkaTransportConfig()
	cfg.InsecureLocalDev = false
	cfg.TLS = config.KafkaTLSConfig{
		Enabled: true,
		CAFile:  filepath.Join(t.TempDir(), "absent.pem"),
	}

	transport, err := NewKafkaTransport(cfg, KafkaTransportRoleProducer)
	require.Error(t, err)
	assert.Nil(t, transport)
	assert.Contains(t, err.Error(), "KAFKA_TLS_CA_FILE")
}

// TestNewKafkaTransport_RefusesHalfAClientCertificate covers mutual TLS.
//
// A certificate without its key, or a key without its certificate, does not produce weaker
// mutual TLS — it produces NO mutual TLS, silently, while the operator believes the broker is
// authenticating them. Both orderings are tested because a check written for one field is easy
// to write in a way that misses the other.
func TestNewKafkaTransport_RefusesHalfAClientCertificate(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client.key")
	require.NoError(t, os.WriteFile(certPath, []byte("cert"), 0o600))
	require.NoError(t, os.WriteFile(keyPath, []byte("key"), 0o600))

	for name, tlsCfg := range map[string]config.KafkaTLSConfig{
		"certificate without key": {Enabled: true, CertFile: certPath},
		"key without certificate": {Enabled: true, KeyFile: keyPath},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := kafkaTransportConfig()
			cfg.InsecureLocalDev = false
			cfg.TLS = tlsCfg

			transport, err := NewKafkaTransport(cfg, KafkaTransportRoleProducer)
			require.Error(t, err)
			assert.Nil(t, transport)
			assert.Contains(t, err.Error(), "must be set together")
		})
	}
}

// TestKafkaTransportCredentials_ProducerPrefersItsOwnPrincipal is the PRIV-01 guard.
//
// The producer used to authenticate as KAFKA_SASL_ADMIN_USER — the principal that creates
// topics, alters SCRAM credentials and manages ACLs. Every ledger event was published by the
// most privileged identity in the deployment, so a leaked producer credential handed an
// attacker the cluster's authorization state rather than the ability to publish, and the
// broker's audit trail could not tell routine publishing from administration.
//
// The dedicated pair must win whenever it is set, even with admin credentials also present —
// which is the realistic case, since the same process configuration often carries both.
func TestKafkaTransportCredentials_ProducerPrefersItsOwnPrincipal(t *testing.T) {
	cfg := kafkaTransportConfig()
	cfg.SASLAdminUser, cfg.SASLAdminSecret = "blnk-admin", "admin-secret"

	user, secret, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
	require.NoError(t, err)
	assert.Equal(t, "blnk-producer", user, "the producer must not authenticate as the administrator")
	assert.Equal(t, "producer-secret", secret)

	adminUser, adminSecret, err := kafkaTransportCredentials(cfg, KafkaTransportRoleAdmin)
	require.NoError(t, err)
	assert.Equal(t, "blnk-admin", adminUser, "the admin role still resolves the admin principal")
	assert.Equal(t, "admin-secret", adminSecret)
}

// TestKafkaTransportCredentials_NeverFallsBackToTheAdminPrincipal replaces a test that asserted
// the opposite, because the behaviour it pinned was itself the defect (PRIV-01).
//
// The previous version required the producer role to FALL BACK to the admin pair, in the name of
// letting a single-credential deployment keep publishing across an upgrade. It warned while doing
// it, and that was the flaw in the reasoning: a warning is advice, the code went on publishing as
// the principal that can create topics, alter SCRAM credentials and manage ACLs, and so a leaked
// producer credential compromised the cluster's authorization state rather than merely allowing
// events to be published.
//
// The two failure modes are not symmetric. A deployment that has not provisioned a producer
// principal loses nothing by failing to start — it is a configuration step away from working —
// whereas one that quietly publishes as the administrator has already accepted a serious risk
// without being asked. So the fallback is gone, and its absence is what this test pins.
//
// TestKafkaTransportCredentials_RefusesToPublishAsTheAdministrator in event_admin_test.go covers
// the refusal's message and the three configurations that remain legitimate.
func TestKafkaTransportCredentials_NeverFallsBackToTheAdminPrincipal(t *testing.T) {
	cfg := kafkaTransportConfig()
	cfg.SASLUser, cfg.SASLSecret = "", ""
	cfg.SASLAdminUser, cfg.SASLAdminSecret = "blnk-admin", "admin-secret"

	require.False(t, cfg.AllowAdminProducer,
		"the least-privilege posture must be the zero value; an unset variable must not be the "+
			"only thing preventing the data plane from running as an administrator")

	user, secret, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrKafkaProducerCredentialsRequired)
	assert.Contains(t, err.Error(), "KAFKA_SASL_USER")
	assert.Contains(t, err.Error(), "KAFKA_SASL_SECRET")
	assert.Empty(t, user, "a refused resolution must not leak the administrative principal")
	assert.Empty(t, secret)
}

// TestKafkaTransportCredentials_BorrowsTheAdminPrincipalOnlyWhenExplicitlyAllowed covers the
// escape hatch.
//
// It exists for one case: an existing single-credential deployment must be able to keep
// publishing while it provisions a producer principal, rather than stop dead on an upgrade. That
// case is real, so the path is retained — but it must be ASKED FOR, so that the allowance lives
// in the configuration where a reviewer can see it instead of being inferred from two absent
// variables. The publisher warns on every construction that takes it.
func TestKafkaTransportCredentials_BorrowsTheAdminPrincipalOnlyWhenExplicitlyAllowed(t *testing.T) {
	cfg := kafkaTransportConfig()
	cfg.SASLUser, cfg.SASLSecret = "", ""
	cfg.SASLAdminUser, cfg.SASLAdminSecret = "blnk-admin", "admin-secret"
	cfg.AllowAdminProducer = true

	user, secret, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
	require.NoError(t, err,
		"the escape hatch is retained, so an explicitly-allowed admin-only configuration must "+
			"resolve rather than stop an upgrade dead")
	assert.Equal(t, "blnk-admin", user,
		"the administrative principal is what it borrows; there is nothing else configured")
	assert.Equal(t, "admin-secret", secret)

	// And the whole transport builds, so the deployment keeps publishing while the producer
	// principal is provisioned. The allowance is not silent: kafkaTransportCredentials warns at
	// every construction that takes this path, naming KAFKA_ALLOW_ADMIN_PRODUCER, so the
	// arrangement stays visible for as long as it lasts.
	transport, transportErr := NewKafkaTransport(cfg, KafkaTransportRoleProducer)
	require.NoError(t, transportErr)
	require.NotNil(t, transport)

	// THE FLAG IS THE ONLY THING THAT ADMITS IT, which is the property that makes the escape
	// hatch an escape hatch rather than the default. Cleared, the identical configuration is
	// refused — see TestKafkaTransportCredentials_NeverFallsBackToTheAdminPrincipal for the
	// refusal's own assertions.
	cfg.AllowAdminProducer = false
	_, _, refused := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
	require.Error(t, refused)
	assert.ErrorIs(t, refused, ErrKafkaProducerCredentialsRequired)
}

// TestKafkaTransportCredentials_RefusesAHalfConfiguredPair covers both roles.
//
// A username with no secret cannot authenticate, and neither can a secret with no username. The
// refusal happens at construction so the operator learns about it at startup rather than through
// an authentication failure on the first publish, in a log nobody is watching, hours later.
func TestKafkaTransportCredentials_RefusesAHalfConfiguredPair(t *testing.T) {
	t.Run("producer user without secret", func(t *testing.T) {
		cfg := kafkaTransportConfig()
		cfg.SASLSecret = ""

		_, _, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "producer")
	})

	t.Run("producer secret without user", func(t *testing.T) {
		cfg := kafkaTransportConfig()
		cfg.SASLUser = ""

		_, _, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "producer")
	})

	t.Run("admin secret without user", func(t *testing.T) {
		cfg := kafkaTransportConfig()
		cfg.SASLAdminSecret = "admin-secret"

		_, _, err := kafkaTransportCredentials(cfg, KafkaTransportRoleAdmin)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "admin")
	})

	t.Run("a producer with no pair still validates the admin pair", func(t *testing.T) {
		// The admin pair is the only available evidence that the cluster authenticates at all,
		// which is what decides whether a missing producer pair is a misconfiguration or a
		// legitimately unauthenticated broker. So it is still validated on the producer path —
		// and a half-configured admin pair is reported as that, ahead of the PRIV-01 refusal,
		// because it is the more specific fault.
		cfg := kafkaTransportConfig()
		cfg.SASLUser, cfg.SASLSecret = "", ""
		cfg.SASLAdminUser = "blnk-admin"

		_, _, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "admin")
	})
}

// TestKafkaTransportCredentials_NoCredentialsIsNotAnError covers the unauthenticated local
// broker.
//
// Both values empty is a legitimate configuration — an unauthenticated development broker — and
// it is distinguishable from a half-configured pair, which is not. The transport reports the
// combination of unauthenticated AND unencrypted with a warning rather than an error, because
// the local-dev acknowledgement has already been given for the encryption half.
func TestKafkaTransportCredentials_NoCredentialsIsNotAnError(t *testing.T) {
	cfg := kafkaTransportConfig()
	cfg.SASLUser, cfg.SASLSecret = "", ""

	user, secret, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
	require.NoError(t, err)
	assert.Empty(t, user)
	assert.Empty(t, secret)

	transport, err := NewKafkaTransport(cfg, KafkaTransportRoleProducer)
	require.NoError(t, err)
	assert.Nil(t, transport.SASL, "no credentials means no SASL mechanism, not an empty one")
}

// TestSaslCredentialError_NamesTheRolesOwnVariables covers the diagnostic, which is the whole
// value of the function.
//
// A credential that SASLprep rejects produces an error from the SCRAM client whose message
// EMBEDS THE PLAINTEXT PASSWORD, which is why that error is deliberately not wrapped. What
// replaces it has to be at least as useful, and pointing an operator at KAFKA_SASL_ADMIN_USER
// when the producer pair is at fault sends them to the wrong line of their configuration.
func TestSaslCredentialError_NamesTheRolesOwnVariables(t *testing.T) {
	// U+0007 is a prohibited control character under SASLprep, so preparing it fails while
	// the value itself is not a secret.
	prohibited := "producer\u0007"

	producerErr := saslCredentialError(KafkaTransportRoleProducer, prohibited)
	require.Error(t, producerErr)
	assert.Contains(t, producerErr.Error(), "KAFKA_SASL_USER")
	assert.NotContains(t, producerErr.Error(), "KAFKA_SASL_ADMIN_USER")

	adminErr := saslCredentialError(KafkaTransportRoleAdmin, prohibited)
	require.Error(t, adminErr)
	assert.Contains(t, adminErr.Error(), "KAFKA_SASL_ADMIN_USER")

	// A username that prepares cleanly means the SECRET is the offender, and the message must
	// say so — naming the secret's variable while never rendering its value.
	secretErr := saslCredentialError(KafkaTransportRoleProducer, "blnk-producer")
	require.Error(t, secretErr)
	assert.Contains(t, secretErr.Error(), "KAFKA_SASL_SECRET")
	assert.Contains(t, secretErr.Error(), "deliberately omitted",
		"the message must state that the secret was withheld, so its absence is not read as a bug")
}

// TestReplayFailureOutcome_PairsEveryCodeWithAMessageThatNamesTheRightCulprit pins the mapper
// itself, independently of the service that calls it.
//
// Two things are asserted that the service-level tests below cannot see. First, that each
// code resolves to its intended status through an explicit statusByCode entry — an unmapped
// code silently becomes 500, which would collapse the split this test exists to prove.
// Second, that the message accompanying each code names the right culprit: a 503 that said
// "failed to replay the event" would tell an operator to investigate Blnk while the broker
// was down.
func TestReplayFailureOutcome_PairsEveryCodeWithAMessageThatNamesTheRightCulprit(t *testing.T) {
	unavailableCode, unavailableMessage := replayFailureOutcome(kafka.LeaderNotAvailable)
	assert.Equal(t, apierror.ErrKafkaUnavailable, unavailableCode)
	assert.Equal(t, http.StatusServiceUnavailable, apierror.StatusForCode(unavailableCode),
		"the availability code must resolve to 503 through an explicit statusByCode entry")
	assert.Contains(t, strings.ToLower(unavailableMessage), "broker",
		"the message for an outage must name the broker, not the event")
	assert.Contains(t, strings.ToLower(unavailableMessage), "retry",
		"a 503 must tell the caller that repeating the request is the right response")

	failedCode, failedMessage := replayFailureOutcome(kafka.MessageSizeTooLarge)
	assert.Equal(t, apierror.ErrEventReplayFailed, failedCode)
	assert.Equal(t, http.StatusInternalServerError, apierror.StatusForCode(failedCode))
	assert.NotContains(t, strings.ToLower(failedMessage), "broker",
		"a defect this service owns must not be described as a broker problem")
	assert.NotEqual(t, unavailableMessage, failedMessage,
		"the two outcomes must be distinguishable by message as well as by code")

	// The third arm. An abandoned replay is neither of the two above, and it used to be reported
	// as the first — a 503 whose message named a healthy broker as the culprit. It answers the
	// approved EVENT_REPLAY_FAILED code and is told apart from a genuine Blnk-owned failure by
	// its message, because the published taxonomy has no separate timeout code to answer with.
	abandonedCode, abandonedMessage := replayFailureOutcome(
		fmt.Errorf("waiting for acknowledgement: %w", context.Canceled))
	assert.Equal(t, apierror.ErrEventReplayFailed, abandonedCode)
	assert.Equal(t, http.StatusInternalServerError, apierror.StatusForCode(abandonedCode),
		"the abandonment answer must resolve through an explicit statusByCode entry rather than "+
			"the unknown-code default")
	assert.NotEqual(t, unavailableCode, abandonedCode,
		"and it must NOT be the availability code, whose message would send an operator to a "+
			"broker that never stopped answering")
	assert.NotContains(t, strings.ToLower(abandonedMessage), "broker is unavailable",
		"an abandoned replay must not accuse the broker of being unavailable")
	assert.Contains(t, strings.ToLower(abandonedMessage), "dead-lettered",
		"the message must say the event is still dead-lettered, which is what makes repeating the "+
			"request obviously safe")
	assert.NotEqual(t, unavailableMessage, abandonedMessage)
	assert.NotEqual(t, failedMessage, abandonedMessage)

	// And the counter-case at the mapper: a dial that timed out is STILL an outage, even though
	// its error chain matches context expiry.
	dialTimeoutCode, _ := replayFailureOutcome(&net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.ParseIP("10.9.8.7"), Port: 9092},
		Err:  os.ErrDeadlineExceeded,
	})
	assert.Equal(t, apierror.ErrKafkaUnavailable, dialTimeoutCode,
		"an unreachable broker must not be reclassified as an abandoned request")
}

// TestIsBrokerUnavailableError_SeparatesAnOutageFromADefect pins the classifier the mapper
// reads, case by case.
//
// It is table-driven over the whole taxonomy because each entry closes a specific way the
// classification could go wrong, and several of them are counter-intuitive:
//
//   - BrokerNotAvailable and ReplicaNotAvailable are NOT in kafka-go's retriable set, so
//     they are only classified correctly because the implementation names them.
//   - MessageSizeTooLarge, InvalidTopic and RecordListTooLarge ARE Kafka protocol errors,
//     and they must still be defects. This is the case an interface test against net.Error
//     would break: kafka.Error implements Error, Timeout and Temporary, so it satisfies
//     net.Error, and classifying by that interface would turn every protocol error into an
//     outage.
//   - A PublishError's own verdict wins over any re-derivation, in BOTH directions — except
//     that a TRANSIENT verdict whose only cause is context termination is not an outage. See
//     TestIsBrokerUnavailableError_DoesNotBlameTheBrokerForALocalCancellation.
func TestIsBrokerUnavailableError_SeparatesAnOutageFromADefect(t *testing.T) {
	unavailable := map[string]error{
		"no leader available":                      kafka.LeaderNotAvailable,
		"no leader for the partition":              kafka.NotLeaderForPartition,
		"broker not available":                     kafka.BrokerNotAvailable,
		"replica not available":                    kafka.ReplicaNotAvailable,
		"request timed out":                        kafka.RequestTimedOut,
		"network exception":                        kafka.NetworkException,
		"not enough replicas":                      kafka.NotEnoughReplicas,
		"storage error":                            kafka.KafkaStorageError,
		"wrapped batch member":                     kafka.WriteErrors{nil, kafka.LeaderNotAvailable},
		"connection refused":                       &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		"host unreachable":                         &net.OpError{Op: "dial", Net: "tcp", Err: syscall.EHOSTUNREACH},
		"broker name does not resolve":             &net.DNSError{Err: "no such host", IsNotFound: true},
		"connection reset":                         syscall.ECONNRESET,
		"broken pipe":                              syscall.EPIPE,
		"publisher closed":                         ErrEventPublisherClosed,
		"publisher closed, wrapped":                fmt.Errorf("writer: %w", ErrEventPublisherClosed),
		"the publisher said transient":             &PublishError{Transient: true, Err: errors.New("some broker trouble")},
		"the publisher said transient, and it was": &PublishError{Transient: true, Err: kafka.NotLeaderForPartition},
	}

	for name, err := range unavailable {
		t.Run("unavailable/"+name, func(t *testing.T) {
			assert.True(t, IsBrokerUnavailableError(err),
				"%v must be classified as the broker being unavailable", err)
		})
	}

	defects := map[string]error{
		"message too large":               kafka.MessageSizeTooLarge,
		"record list too large":           kafka.RecordListTooLarge,
		"invalid topic":                   kafka.InvalidTopic,
		"unsupported version":             kafka.UnsupportedVersion,
		"empty batch":                     kafka.WriteErrors{},
		"batch of defects":                kafka.WriteErrors{kafka.MessageSizeTooLarge},
		"an unrecognised failure":         errors.New("blnk: something else entirely"),
		"the publisher said permanent":    &PublishError{Transient: false, Err: errors.New("json: unsupported value")},
		"the publisher overrides a retry": &PublishError{Transient: false, Err: kafka.LeaderNotAvailable},
	}

	for name, err := range defects {
		t.Run("defect/"+name, func(t *testing.T) {
			assert.False(t, IsBrokerUnavailableError(err),
				"%v must NOT be excused as a broker outage", err)
		})
	}

	assert.False(t, IsBrokerUnavailableError(nil), "a nil error is not a failure at all")
}

// TestIsBrokerUnavailableError_DoesNotBlameTheBrokerForALocalCancellation is TAXONOMY-01, and it
// is the half of the classification the delegation used to get wrong.
//
// # The defect this pins closed
//
// brokerUnavailable was `return classifyTransientPublishError(err)`, and that classifier calls
// context termination RETRYABLE — correctly, because a write abandoned by a shutdown, a lost
// outbox lease or a spent request budget is worth attempting again. Reading the same verdict as
// "the broker is the reason" made every such failure an outage: the replay endpoint answered
// EVENT_KAFKA_UNAVAILABLE with 503 and a message telling the operator to retry once the broker
// recovered, for a broker that had never stopped answering.
//
// # What is asserted, and why both halves are needed
//
// Each case must be RETRYABLE and NOT AN OUTAGE at the same time. Asserting only the second half
// would pass for a fix that had also stopped retrying these failures — which would dead-letter
// every event a graceful shutdown interrupted, exactly the regression RETRY-01 exists to
// prevent. The pair states the invariant precisely: the retryable set is strictly wider than the
// outage set, and context termination is the difference between them.
//
// The last case is the one that keeps the fix honest in the other direction. Go's net package
// maps a dial cancelled or timed out by its context onto errors that satisfy
// errors.Is(err, context.DeadlineExceeded), wrapped in a *net.OpError — so a broker that
// blackholes connections produces a genuine outage whose chain also looks like cancellation. It
// must still be classified as an outage, which is why the concrete signatures are tested first.
func TestIsBrokerUnavailableError_DoesNotBlameTheBrokerForALocalCancellation(t *testing.T) {
	local := map[string]error{
		"the caller cancelled":              context.Canceled,
		"the deadline expired":              context.DeadlineExceeded,
		"a cancellation reached us wrapped": fmt.Errorf("waiting for acknowledgement: %w", context.Canceled),
		"an expiry reached us wrapped":      fmt.Errorf("waiting for acknowledgement: %w", context.DeadlineExceeded),
		"the publisher classified it transient": &PublishError{
			Topic:     "blnk.transactions",
			EventID:   "evt_cancelled",
			EventType: "transaction.applied",
			Attempt:   2,
			Transient: true,
			Err:       context.Canceled,
		},
	}

	for name, err := range local {
		t.Run(name, func(t *testing.T) {
			assert.True(t, classifyTransientPublishError(err),
				"a cancelled or expired write says nothing about the event, so the budget must be "+
					"kept and another attempt must remain available")

			assert.False(t, IsBrokerUnavailableError(err),
				"%v is THIS PROCESS giving up, not the broker refusing: reported as an outage it "+
					"answers 503 and tells an operator to wait for a broker that is healthy", err)
		})
	}

	// The counter-case: an unreachable broker whose error chain ALSO matches context expiry,
	// which is what net.DialContext produces when its deadline fires. It is an outage.
	dialTimeout := &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.ParseIP("10.9.8.7"), Port: 9092},
		Err:  os.ErrDeadlineExceeded,
	}
	assert.True(t, IsBrokerUnavailableError(dialTimeout),
		"a dial that timed out is the broker being unreachable, and it must not be reclassified as "+
			"a local cancellation just because its chain matches context.DeadlineExceeded")
}

// TestReplayDeadLetteredEvent_ReportsAnUnavailableBrokerAsRetryable covers the publish-failure
// arm for every way the BROKER can be the reason.
//
// All of them resolve to ErrKafkaUnavailable and 503, which is the same answer the
// no-transport branch gives, and for the same reason: the event is intact, nothing about
// Blnk is broken, and the correct response is to repeat the request once the broker
// recovers. Answering 500 here — as a single catch-all replay code would — would classify a
// rolling restart as an internal defect, send an operator hunting for a bug that does not
// exist, and tell a client that retrying is pointless at the one moment it is the only
// thing that helps.
//
// The cases are the real SIGNALS rather than error text, because text is not a contract: a
// leaderless partition, a broker declaring itself unavailable (a code kafka-go does NOT
// mark retriable, so it has to be named explicitly), the publisher's own transient verdict,
// a batch whose members failed, a refused TCP connection, an expired deadline, and a
// transport that has been closed.
//
// # The detail travels with the status, and still says nothing about the broker
//
// Two guarantees are asserted together here because they are easy to satisfy separately and
// wrong separately. The bounded EventTransportErrorDetail reports transient TRUE, from the
// same verdict that chose the status, so nothing tells a client to retry and not to retry
// in one response. And it still carries no broker address, port or protocol text, so
// classifying an outage correctly does not become a way to describe the deployment's
// topology to whoever asked for the replay.
func TestReplayDeadLetteredEvent_ReportsAnUnavailableBrokerAsRetryable(t *testing.T) {
	dltPinTopicPrefix(t)

	cases := map[string]error{
		"the partition has no leader":            kafka.NotLeaderForPartition,
		"the leader is not available":            kafka.LeaderNotAvailable,
		"the broker declares itself unavailable": kafka.BrokerNotAvailable,
		"a replica is not available":             kafka.ReplicaNotAvailable,
		"the publisher classified the attempt transient": &PublishError{
			Topic:     "blnk.transactions",
			EventID:   "evt_transaction.applied_replay",
			EventType: "transaction.applied",
			Attempt:   6,
			Transient: true,
			Err:       kafka.RequestTimedOut,
		},
		"a member of the write batch failed": kafka.WriteErrors{nil, kafka.KafkaStorageError},
		"the connection was refused": &net.OpError{
			Op: "dial", Net: "tcp",
			Addr: &net.TCPAddr{IP: net.ParseIP("10.9.8.7"), Port: 9092},
			Err:  syscall.ECONNREFUSED,
		},
		"the broker host does not resolve": &net.DNSError{
			Err: "no such host", Name: "kafka.internal", IsNotFound: true,
		},
		// A dial whose own deadline fired. Its chain matches context.DeadlineExceeded as well as
		// carrying a *net.OpError, and it belongs HERE rather than on the abandonment arm: the
		// broker did not answer a connection attempt, which is an outage.
		"the dial timed out": &net.OpError{
			Op: "dial", Net: "tcp",
			Addr: &net.TCPAddr{IP: net.ParseIP("10.9.8.7"), Port: 9092},
			Err:  os.ErrDeadlineExceeded,
		},
		// The broker's OWN acknowledgement timeout, which arrives as a protocol code rather than
		// as a context error. A bare context expiry no longer resolves here — see
		// TestReplayDeadLetteredEvent_ReportsAnAbandonedReplayWithoutAccusingTheBroker — because it means this
		// process stopped waiting, not that the broker failed to answer.
		"the broker did not acknowledge in time": kafka.RequestTimedOut,
		"the transport is closed":                fmt.Errorf("resolving a writer: %w", ErrEventPublisherClosed),
	}

	for name, cause := range cases {
		t.Run(name, func(t *testing.T) {
			fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")
			fixture.publisher.err = cause

			outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
			dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)

			detail, ok := dltAPIError(t, err).Details.(EventTransportErrorDetail)
			require.True(t, ok, "a transport failure must carry the bounded detail type, got %T",
				dltAPIError(t, err).Details)
			assert.True(t, detail.Transient,
				"the body must agree with the 503: a caller reading transient=false would give up")
			assert.Equal(t, fixture.row.EventID, detail.EventID)
			assert.Equal(t, "blnk.transactions", detail.Topic)

			// DATA-01 still holds on this arm. Rendered every way a handler might render it.
			marshalled, marshalErr := json.Marshal(dltAPIError(t, err))
			require.NoError(t, marshalErr)
			for _, leak := range []string{"10.9.8.7", "9092", "kafka.internal", "Leader Not Available"} {
				assert.NotContains(t, fmt.Sprint(detail), leak,
					"the broker's own error text must not reach a caller through %%v")
				assert.NotContains(t, string(marshalled), leak,
					"the broker's own error text must not reach a caller through the response body")
			}

			assert.False(t, outcome.Recorded)
			assert.Empty(t, fixture.store.snapshotDispatched(),
				"a failed republish must not clear the row from the inventory")
			assert.Equal(t, model.EventOutboxStatusDeadLettered,
				fixture.store.row(t, fixture.row.EventID).Status,
				"the event stays dead-lettered so it can be replayed again once the broker recovers")
		})
	}
}

// TestReplayDeadLetteredEvent_ReportsAnAbandonedReplayWithoutAccusingTheBroker is the third arm
// of the split, at the endpoint.
//
// # What was wrong
//
// A replay whose context was cancelled — the caller disconnected, or the request's deadline
// expired — resolved to EVENT_KAFKA_UNAVAILABLE and 503, with a message stating that the Kafka
// broker was unavailable and asking the operator to retry once it recovered. The broker was
// healthy in every one of those cases; the request had been abandoned on this side. An operator
// following that message goes looking at Kafka, and a client's retry policy is told a dependency
// is down when nothing is.
//
// # What is asserted
//
// The code — the approved EVENT_REPLAY_FAILED rather than the availability code — the status it
// must resolve to through an explicit statusByCode entry, and the message, which must not accuse
// the broker and must say the event is still dead-lettered, because that is what makes repeating
// the request obviously safe. The row's terminal state is asserted for the same reason it is on
// the other two arms: an abandoned replay must leave the event replayable.
func TestReplayDeadLetteredEvent_ReportsAnAbandonedReplayWithoutAccusingTheBroker(t *testing.T) {
	dltPinTopicPrefix(t)

	cases := map[string]error{
		"the caller went away":              context.Canceled,
		"the request deadline expired":      context.DeadlineExceeded,
		"a cancellation reached us wrapped": fmt.Errorf("waiting for acknowledgement: %w", context.Canceled),
		"the publisher classified the abandoned attempt transient": &PublishError{
			Topic:     "blnk.transactions",
			EventID:   "evt_transaction.applied_replay",
			EventType: "transaction.applied",
			Attempt:   6,
			Transient: true,
			Err:       context.DeadlineExceeded,
		},
	}

	for name, cause := range cases {
		t.Run(name, func(t *testing.T) {
			fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")
			fixture.publisher.err = cause

			outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
			dltAssertCodeAndStatus(t, err, apierror.ErrEventReplayFailed, http.StatusInternalServerError)

			assert.NotEqual(t, apierror.ErrKafkaUnavailable, dltAPIError(t, err).Code,
				"an abandoned request must not be reported as the broker being unavailable")

			assert.NotContains(t, strings.ToLower(dltAPIError(t, err).Message), "broker is unavailable",
				"an abandoned request must not report the broker as unavailable")

			assert.False(t, outcome.Recorded)
			assert.Equal(t, model.EventOutboxStatusDeadLettered,
				fixture.store.row(t, fixture.row.EventID).Status,
				"the event stays dead-lettered, so the abandoned replay can simply be repeated")
		})
	}
}

// TestReplayDeadLetteredEvent_ReportsANonAvailabilityFailureAsAReplayDefect covers the other
// half of the split.
//
// ErrEventReplayFailed and its 500 are RESERVED for a failure this service owns, where a
// retry changes nothing: a message the broker will never accept at its current size, a
// topic name that is not valid, bytes that could not be marshalled, or a failure that
// cannot be attributed to the broker at all. The last case is the deliberately conservative
// direction of the classifier — an unrecognised error stays visible as a fault here rather
// than being written off as somebody else's outage.
//
// The detail reports transient FALSE on this arm, again from the same verdict as the status,
// so the two halves of the response cannot advise a caller differently.
func TestReplayDeadLetteredEvent_ReportsANonAvailabilityFailureAsAReplayDefect(t *testing.T) {
	dltPinTopicPrefix(t)

	cases := map[string]error{
		"the message is larger than the broker accepts": kafka.MessageSizeTooLarge,
		"the topic name is not valid":                   kafka.InvalidTopic,
		"the publisher classified the attempt permanent": &PublishError{
			Topic:     "blnk.transactions",
			EventID:   "evt_transaction.applied_replay",
			EventType: "transaction.applied",
			Attempt:   6,
			Transient: false,
			Err:       errors.New("json: unsupported value: +Inf"),
		},
		"the failure cannot be attributed to the broker": errors.New(
			"blnk: the composed message could not be written"),
	}

	for name, cause := range cases {
		t.Run(name, func(t *testing.T) {
			fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")
			fixture.publisher.err = cause

			outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
			dltAssertCodeAndStatus(t, err, apierror.ErrEventReplayFailed, http.StatusInternalServerError)

			detail, ok := dltAPIError(t, err).Details.(EventTransportErrorDetail)
			require.True(t, ok, "a transport failure must carry the bounded detail type, got %T",
				dltAPIError(t, err).Details)
			assert.False(t, detail.Transient,
				"the body must agree with the 500: a caller reading transient=true would retry for ever")

			assert.False(t, outcome.Recorded)
			assert.Empty(t, fixture.store.snapshotDispatched(),
				"a failed republish must not clear the row from the inventory")
			assert.Equal(t, model.EventOutboxStatusDeadLettered,
				fixture.store.row(t, fixture.row.EventID).Status,
				"the event stays dead-lettered, because a fixed defect makes it replayable again")
		})
	}
}

// TestDeadLetterOutcomeLogFields_RedactTheFinancialKeyAndBoundTheBrokerError is the
// disclosure boundary of the dead-letter path, expressed as a test.
//
// Every log line this file emits — three at Error, one at Warn, one at Info — is built from
// this projection, and the severities most likely to be shipped to an aggregator with a
// weaker access boundary than the ledger itself are exactly the ones it appears on. Two of
// its fields cannot be published verbatim:
//
//   - The PARTITION KEY names the ledger or balance the event belongs to. It is reported as
//     the same stable hash PublishResult.LogFields reports, so two lines about one aggregate
//     remain correlatable across the publish and dead-letter paths, and the plaintext value
//     stays on the outbox row where the database's access control governs it.
//   - The ERROR REASON is a broker or client-library string: unbounded, and free to contain
//     newlines that forge a second log line, control characters that corrupt a structured-log
//     parser, and a broker address the library chose to interpolate.
//
// The row and the stored failure metadata are deliberately unaffected: the API that serves
// triage reads them, and this is only about what reaches a log.
func TestDeadLetterOutcomeLogFields_RedactTheFinancialKeyAndBoundTheBrokerError(t *testing.T) {
	const partitionKey = "bln_7d3ac6f1"

	forged := "write tcp 10.4.2.9:9092: broken pipe\nERROR everything is fine\x07" +
		strings.Repeat("y", maxLoggedErrorLength*2)

	fields := DeadLetterOutcome{
		EventID:         "evt_redaction",
		EventType:       "transaction.applied",
		OriginalTopic:   "blnk.transactions",
		DeadLetterTopic: "blnk.transactions.dlt",
		PartitionKey:    partitionKey,
		Status:          model.PublishStatusDeadLettered,
		Metadata: model.FailureMetadata{
			OriginalTopic: "blnk.transactions",
			ErrorReason:   forged,
			AttemptCount:  5,
		},
	}.LogFields()

	assert.NotContains(t, fields, "partition_key",
		"the plaintext financial key must be gone, not merely accompanied by a hash")
	hashed, ok := fields["partition_key_hash"].(string)
	require.True(t, ok, "the hashed key must be present, or two lines about one aggregate cannot be correlated")
	assert.NotEqual(t, partitionKey, hashed, "the field must not be the identifier under a new name")
	assert.Equal(t, hashLogIdentifier(partitionKey), hashed,
		"the same function must hash it as on the publish path, or the two paths' lines stop correlating")

	reason, ok := fields["error_reason"].(string)
	require.True(t, ok, "the reason must still be reported: an operator needs to know what failed")
	assert.NotContains(t, reason, "\n", "a newline would forge a second log line")
	assert.NotContains(t, reason, "\x07", "control characters corrupt terminals and structured-log parsers")
	assert.LessOrEqual(t, len([]rune(reason)), maxLoggedErrorLength+len([]rune(logTruncationSuffix)),
		"an unbounded broker error must be capped before it reaches a log")
	assert.True(t, strings.HasSuffix(reason, logTruncationSuffix), "truncation must be marked, never silent")
	assert.Contains(t, reason, "broken pipe", "and the diagnostic part must survive the bounding")

	// The fields an operator navigates by are untouched: bounding the two above is worthless
	// if it costs the line its identity.
	assert.Equal(t, "evt_redaction", fields["event_id"])
	assert.Equal(t, "transaction.applied", fields["event_type"])
	assert.Equal(t, "blnk.transactions", fields["topic"])
	assert.Equal(t, "blnk.transactions.dlt", fields["dlt_topic"])
	assert.Equal(t, 5, fields["attempt_count"])
}

// dltInventoryMatches applies the SAME predicate the repository's SQL applies, and it is the
// ONLY narrowing on this double.
//
// It is deliberately an exact mirror rather than a convenient approximation, because the
// service now delegates ALL narrowing to the database and a fake that filtered more
// loosely — or that derived a missing topic from the event type, as the deleted Go-side
// predicate used to — would let the service pass here while the real query returned a
// different set. Comparisons are exact and case-sensitive on the stored columns, and the
// topic is the row's stored topic with no derivation: the real column is NOT NULL under
// CHECK (btrim(topic) <> ”), so a stored row cannot need one.
//
// # Why there is one of these and not three
//
// This store answered the same narrowing question from three places: the full-row listing and
// its count shared one matcher, the narrow projection had a second, and a free function stood
// beside both agreeing with neither. Three matchers over one predicate is worse than one loose
// matcher, because whichever a test happens to reach decides what it asserts over, and the
// answer stops being a statement about the statement at all. The survivor is the one that both
// TRIMS its predicates and constrains the population, because deadLetterFilterClause does both.
//
// The three string predicates are TRIMMED for that reason. A padded filter narrows to nothing in
// SQL, so a double comparing raw values would agree with the statement on a clean filter and
// disagree on a padded one.
func dltInventoryMatches(row model.EventOutbox, query model.DeadLetterQuery) bool {
	status := strings.TrimSpace(query.Status)
	eventType := strings.TrimSpace(query.EventType)
	topic := strings.TrimSpace(query.Topic)

	if status != "" && row.Status != status {
		return false
	}
	if eventType != "" && row.EventType != eventType {
		return false
	}
	if topic != "" && row.Topic != topic {
		return false
	}

	// The occurrence window, INCLUSIVE at both ends, exactly as the statement's two nullable
	// arms are: a caller correlating against a precise instant must get the events at it.
	if !query.OccurredFrom.IsZero() && row.OccurredAt.Before(query.OccurredFrom) {
		return false
	}
	if !query.OccurredTo.IsZero() && row.OccurredAt.After(query.OccurredTo) {
		return false
	}

	return true
}

// ListDeadLetterInventory pages the MATCHING rows newest first, applying the filters, the
// occurrence window and the keyset cursor exactly as the SQL does.
//
// The predicates and the cursor are honoured HERE rather than assumed away, because they live in
// the query rather than in the service: a fake that ignored them would let the service pass its
// filtering tests while the statement did the filtering, which is precisely the thing under
// test.
func (s *dltFakeStore) ListDeadLetterInventory(
	_ context.Context,
	query model.DeadLetterInventoryQuery,
) (model.DeadLetterInventoryPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Recorded as the narrowing, so an assertion reads the filters the service passed without
	// having to know which of the two query shapes carried them.
	filter := model.DeadLetterQuery{
		EventType:    query.EventType,
		Topic:        query.Topic,
		Status:       query.Status,
		OccurredFrom: query.OccurredFrom,
		OccurredTo:   query.OccurredTo,
		Limit:        query.Limit,
	}
	s.inventoryQueries = append(s.inventoryQueries, filter)
	s.inventoryPages = append(s.inventoryPages, query)

	if s.listErr != nil {
		return model.DeadLetterInventoryPage{}, s.listErr
	}

	return s.inventoryPageLocked(query, filter), nil
}

// inventoryPageLocked builds the page. The caller holds the mutex.
//
// It is separate from ListDeadLetterInventory so that the paired read can build the page and the
// total under ONE lock acquisition, which is the fake's stand-in for the repository's single
// snapshot.
func (s *dltFakeStore) inventoryPageLocked(
	query model.DeadLetterInventoryQuery,
	filter model.DeadLetterQuery,
) model.DeadLetterInventoryPage {
	limit := query.Limit
	if limit <= 0 {
		limit = len(s.inventory)
	}

	page := model.DeadLetterInventoryPage{Entries: make([]model.DeadLetterInventoryEntry, 0, limit)}
	passedCursor := query.Cursor == nil

	for _, eventID := range s.inventory {
		row := s.rows[eventID]

		if !passedCursor {
			// The inventory is newest first, so the cursor's row and everything before it
			// belong to the previous page.
			if row.OccurredAt.Equal(query.Cursor.OccurredAt) && row.ID == query.Cursor.ID {
				passedCursor = true
			}

			continue
		}

		if !dltInventoryMatches(*row, filter) {
			continue
		}

		if len(page.Entries) == limit {
			// The probe row the real query reads to answer "is there more", and the cursor
			// comes from the last RETURNED entry rather than from this one.
			page.HasMore = true

			break
		}

		page.Entries = append(page.Entries, dltInventoryEntryOf(*row))
	}

	if page.HasMore && len(page.Entries) > 0 {
		last := page.Entries[len(page.Entries)-1]
		page.NextCursor = &model.DeadLetterCursor{OccurredAt: last.OccurredAt, ID: last.ID}
	}

	return page
}

// CountDeadLetterInventory counts matches, ignoring the page, exactly as the repository's
// COUNT(*) over the shared predicate does.
func (s *dltFakeStore) CountDeadLetterInventory(
	_ context.Context,
	query model.DeadLetterQuery,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.countQueries = append(s.countQueries, query)

	if s.countErr != nil {
		return 0, s.countErr
	}

	return int64(len(s.matchingLocked(query))), nil
}

// ListAndCountDeadLetterInventory answers the page and the total from ONE observation of the
// fake's state, which is what the repository's read-only REPEATABLE READ transaction gives the
// real store.
//
// It delegates to the two single-purpose methods under a SINGLE lock acquisition rather than
// calling them — they each take the mutex — so the two answers are drawn from one state. A fake
// that called them in turn would release the lock between them and reproduce exactly the two-
// snapshot defect this method exists to close, and the test would then pass against a store that
// does not hold the property.
func (s *dltFakeStore) ListAndCountDeadLetterInventory(
	_ context.Context,
	query model.DeadLetterInventoryQuery,
) (model.DeadLetterInventoryPage, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	filter := query.FilterQuery()
	filter.Limit = query.Limit

	s.inventoryQueries = append(s.inventoryQueries, filter)
	s.inventoryPages = append(s.inventoryPages, query)
	s.countQueries = append(s.countQueries, query.FilterQuery())

	if s.listErr != nil {
		return model.DeadLetterInventoryPage{}, 0, s.listErr
	}

	if s.countErr != nil {
		return model.DeadLetterInventoryPage{}, 0, s.countErr
	}

	page := s.inventoryPageLocked(query, filter)

	return page, int64(len(s.matchingLocked(query.FilterQuery()))), nil
}

// snapshotInventoryQueries returns the queries the service handed the FILTERED listing.
//
// The whole query is captured rather than a page request, because the property under test is
// that the narrowing REACHED THE DATABASE. A service that accepted a filter and then applied
// it in Go would issue an unfiltered query here and still return the right rows for a small
// fixture — passing a test that only checked the returned entries, while behaving exactly as
// the defect did on a real inventory.
// snapshotInventoryCountQueries returns a copy of the narrowings the INVENTORY-predicate count
// was asked for, which is the one the paired read uses.
//
// It is distinct from snapshotCountQueries, which records the full-row count: the two are
// separate recorders because the two counts are separate repository methods, and a test asserting
// that a paired read counted once must not be satisfied by a call to the other one.
func (s *dltFakeStore) snapshotInventoryCountQueries() []model.DeadLetterQuery {
	s.mu.Lock()
	defer s.mu.Unlock()

	queries := make([]model.DeadLetterQuery, len(s.countQueries))
	copy(queries, s.countQueries)

	return queries
}

// snapshotInventoryPages returns a copy of the keyset requests the inventory was read with.
func (s *dltFakeStore) snapshotInventoryPages() []model.DeadLetterInventoryQuery {
	s.mu.Lock()
	defer s.mu.Unlock()

	pages := make([]model.DeadLetterInventoryQuery, len(s.inventoryPages))
	copy(pages, s.inventoryPages)

	return pages
}

func (s *dltFakeStore) snapshotInventoryQueries() []model.DeadLetterQuery {
	s.mu.Lock()
	defer s.mu.Unlock()

	queries := make([]model.DeadLetterQuery, len(s.inventoryQueries))
	copy(queries, s.inventoryQueries)

	return queries
}

// TestListDeadLetterEvents_NarrowingIsAppliedInSQLRatherThanAboveIt is the finding itself.
//
// # What was wrong
//
// A filtered listing used to be served by WALKING the unfiltered inventory in 500-row pages
// and testing each row in Go, abandoning the walk after 5,000 scanned rows. Two things
// followed, and neither was visible to the caller:
//
//   - A filter whose matches all lay beyond the ceiling returned an ordinary EMPTY page with
//     a 200. An operator asking "which transaction events are stuck" was told "none" while
//     transaction events were stuck. The likelihood of that answer rose with the size of the
//     inventory, which is exactly backwards for a tool reached during an incident.
//   - Those matches were unreachable by paging at all: no offset could get past the ceiling.
//
// # What is asserted here
//
// That the SERVICE DOES NOT NARROW. The fixture is small, so a service filtering in Go would
// return the same rows and pass any test that only inspected the result — which is precisely
// how the defect survived. So the assertion is on the query handed to the repository: the
// filters must be in it, and the unfiltered walk must not be used at all.
func TestListDeadLetterEvents_NarrowingIsAppliedInSQLRatherThanAboveIt(t *testing.T) {
	dltPinTopicPrefix(t)

	stuck := dltAgedRow(t, "evt_sql_txn", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 20*time.Minute)
	other := dltAgedRow(t, "evt_sql_bal", "balance.created", "blnk.balances",
		model.EventOutboxStatusFailed, "", 10*time.Minute)

	store := newDltFakeStore().withRow(stuck).withRow(other)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	entries, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{
		EventType: "transaction.applied",
		Topic:     "blnk.transactions",
		Status:    model.EventOutboxStatusDeadLettered,
	})
	require.NoError(t, err)

	require.Len(t, entries.Entries, 1, "only the matching entry may be returned")
	assert.Equal(t, "evt_sql_txn", entries.Entries[0].EventID)

	queries := store.snapshotInventoryQueries()
	require.Len(t, queries, 1, "a filtered page must be ONE query, not a walk of many")
	assert.Equal(t, "transaction.applied", queries[0].EventType,
		"the event-type filter must reach SQL; applied above it, matches beyond the old scan "+
			"ceiling were reported as absent")
	assert.Equal(t, "blnk.transactions", queries[0].Topic, "the topic filter must reach SQL")
	assert.Equal(t, model.EventOutboxStatusDeadLettered, queries[0].Status,
		"the status filter must reach SQL")

	assert.Empty(t, store.snapshotPages(),
		"the unfiltered paging method must not be used to serve a filtered listing; that method "+
			"IS the walk the ceiling truncated")
}

// TestListDeadLetterEvents_OffsetPagesTheMatchesRatherThanTheInventory asserts that a
// filtered page is a page of the FILTERED set.
//
// Skipping rows before filtering would make a page short and unstable as the filter's hit
// rate varied — a caller stepping offset by its limit would see gaps. The database applies
// predicate and page together, so offset counts matches; this pins that end to end and, with
// the deep offset, pins that a match past the front of the inventory is reachable.
func TestListDeadLetterEvents_OffsetPagesTheMatchesRatherThanTheInventory(t *testing.T) {
	dltPinTopicPrefix(t)

	store := newDltFakeStore()
	// Interleaved so that no page of the UNFILTERED inventory is a page of the filtered one:
	// every second row is a non-match.
	wanted := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		match := dltAgedRow(t, fmt.Sprintf("evt_match_%d", i), "transaction.applied",
			"blnk.transactions", model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt",
			time.Duration(60-i)*time.Minute)
		noise := dltAgedRow(t, fmt.Sprintf("evt_noise_%d", i), "balance.created",
			"blnk.balances", model.EventOutboxStatusDeadLettered, "blnk.balances.dlt",
			time.Duration(60-i)*time.Minute)
		store = store.withRow(match).withRow(noise)
		wanted = append(wanted, match.EventID)
	}

	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})
	filter := DeadLetterListOptions{EventType: "transaction.applied", Limit: 2}

	seen := make([]string, 0, len(wanted))
	for request := 0; request < 3; request++ {
		page, err := service.ListDeadLetterEvents(context.Background(), filter)
		require.NoError(t, err)
		require.Lenf(t, page.Entries, 2,
			"request %d must return a FULL page of matches; a short page here means rows were "+
				"skipped before the filter was applied", request)

		for _, entry := range page.Entries {
			assert.Equal(t, "transaction.applied", entry.EventType)
			seen = append(seen, entry.EventID)
		}

		// RESUMED FROM THE LAST MATCH, not from a row position. That is what makes a filtered
		// walk exact: an offset counts rows the filter rejected, so it skipped matches and
		// repeated others as soon as the filter removed anything.
		//
		// Taken from the last ENTRY rather than from NextCursor, because the final page of an
		// exactly-divided walk reports no further page and therefore carries no cursor — and the
		// position past the last match is exactly what the tail assertion below needs.
		last := page.Entries[len(page.Entries)-1]
		filter.Cursor = &model.DeadLetterCursor{OccurredAt: last.OccurredAt, ID: last.ID}
	}

	assert.ElementsMatch(t, wanted, seen,
		"paging by the limit must visit every match exactly once, with no gap and no repeat")

	// Past the end of the MATCHES, not past the end of the inventory: six non-matching rows
	// remain, and they must not appear.
	tail, err := service.ListDeadLetterEvents(context.Background(), filter)
	require.NoError(t, err)
	assert.Empty(t, tail.Entries,
		"a cursor past the last match returns an empty page, never a non-match: six non-matching "+
			"rows remain in the inventory and the filter belongs to the statement, so none of them "+
			"can surface")
}

// dltInventoryEntryOf projects a stored row into the NARROW inventory entry the repository
// returns, dropping the body and carrying only its size — the same projection the SQL performs
// with octet_length (PERF-P06).
func dltInventoryEntryOf(row model.EventOutbox) model.DeadLetterInventoryEntry {
	return model.DeadLetterInventoryEntry{
		ID:               row.ID,
		EventID:          row.EventID,
		EventType:        row.EventType,
		AggregateID:      row.AggregateID,
		PartitionKey:     row.PartitionKey,
		LedgerID:         row.LedgerID,
		Topic:            row.Topic,
		SchemaVersion:    row.SchemaVersion,
		OccurredAt:       row.OccurredAt,
		Status:           row.Status,
		Attempts:         row.Attempts,
		LastError:        row.LastError,
		FirstAttemptedAt: row.FirstAttemptedAt,
		LastAttemptedAt:  row.LastAttemptedAt,
		DLTTopic:         row.DLTTopic,
		FailureMetadata:  row.FailureMetadata,
		PayloadBytes:     len(row.Payload),
	}
}

// OldestDeadLetterAgeByTopic reports the oldest outstanding entry per dead-letter topic, as
// the repository's grouped aggregate does (PERF-P07).
func (s *dltFakeStore) OldestDeadLetterAgeByTopic(
	_ context.Context,
	deadLetterSuffix string,
) ([]model.DeadLetterTopicAge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ageCalls++

	if s.ageErr != nil {
		return nil, s.ageErr
	}

	grouped := make(map[string]model.DeadLetterTopicAge)
	for _, eventID := range s.inventory {
		row := s.rows[eventID]

		// EVERY OUTSTANDING ROW COUNTS, with no exemption, mirroring the statement. An entry
		// leaves this aggregate by being REPLAYED — a re-publish the broker acknowledges makes
		// it dispatched, and dispatched is not one of the two states the inventory holds — so
		// the gauge keeps reporting until the event has actually reached a subscriber.

		topic := row.DLTTopic
		if topic == "" {
			topic = row.Topic + deadLetterSuffix
		}

		aged := row.OccurredAt
		if row.LastAttemptedAt != nil {
			aged = *row.LastAttemptedAt
		}

		entry, seen := grouped[topic]
		entry.Topic = topic
		entry.Outstanding++
		if !seen || aged.Before(entry.Oldest) {
			entry.Oldest = aged
		}
		grouped[topic] = entry
	}

	ages := make([]model.DeadLetterTopicAge, 0, len(grouped))
	for _, entry := range grouped {
		ages = append(ages, entry)
	}

	return ages, nil
}

// snapshotAgeCalls returns how many times the grouped age aggregate was read.
//
// The COUNT is the assertion, not the result (PERF-P07): the answer looks identical whether it
// came from one aggregate or from a per-topic query in a loop, and the difference between those
// two is the finding.
func (s *dltFakeStore) snapshotAgeCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ageCalls
}

// ListDeadLetteredEventsFiltered pages the inventory, newest first, applying the filter
// the way SQL does: to the WHOLE population before the page is taken.
//
// Filtering before paging is what makes the double faithful. The old Go-side walk filtered
// AFTER paging and skipped Offset matches by hand; if this double did that, a test would
// pass against an implementation that had pushed a filter into SQL incorrectly.
func (s *dltFakeStore) ListDeadLetteredEventsFiltered(
	_ context.Context,
	filter model.DeadLetterInventoryFilter,
	limit, offset int,
) ([]model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	recorded := filter
	recorded.Limit, recorded.Offset = limit, offset
	s.listCalls = append(s.listCalls, recorded)

	if s.listErr != nil {
		return nil, s.listErr
	}

	matching := s.matchingLocked(filter)

	if offset >= len(matching) {
		// The repository returns a nil slice for a page past the end. Reproducing that
		// exactly is what lets the nil-to-empty normalisation be asserted.
		return nil, nil
	}

	end := offset + limit
	if end > len(matching) {
		end = len(matching)
	}

	page := make([]model.EventOutbox, 0, end-offset)
	page = append(page, matching[offset:end]...)

	return page, nil
}

// Four occurrence-window tests that arrived with this feature are GONE, and three of them are
// moot rather than merely duplicated.
//
// They were written for a service with TWO read paths — a single LIMIT/OFFSET query for a
// request the repository could answer exactly, and a bounded in-memory walk for the filters it
// could not — and they pinned which path a window took, on the reasoning that routing a
// windowed request through the walk would cap an index-served answer at the scan budget.
//
// That reasoning was right and this service acted on it further: there is ONE path now, and
// every narrowing including the window is applied in SQL (see DeadLetterListOptions.filtered,
// which survives as observability only). With no walk left there is no routing to assert, and
// asserting it against the offset-paged reader records pages the listing no longer reads.
//
// What they pinned that still matters is kept:
//
//   - the bounds reaching the repository, both bounds inclusive, one-sided windows, the window
//     combining with the other filters, and a reversed window refused BEFORE any read — all in
//     TestListDeadLetterEvents_AppliesTheOccurrenceWindow above, against the keyset query the
//     listing actually issues;
//   - the refusal NAMING the two parameters, which was this feature's own improvement, now
//     asserted there and stated in the message itself;
//   - UTC normalisation of a supplied bound, which deadLetterFilterClause performs when it
//     binds, covered by database.TestCountEventOutboxByStatus_NormalisesTheWindow;
//   - the age gauge scanning UNWINDOWED, which is a property of a different read and is
//     retained below in its own test, retargeted onto the inventory query.

// TestDeadLetterAgeGauge_ReadsNoWindowAtAll guards the one read that must never carry a window,
// and it pins a deliberate ABSENCE.
//
// The gauge exists to find the OLDEST outstanding entry, which is the value the 15-minute alert is
// written against. Any lower bound on that read would cap the age it could ever report, so an
// entry stuck for a day would be published as younger than the window — the gauge would look
// healthy at exactly the moment it is supposed to fire.
//
// # Why this asserts what is NOT read
//
// The property arrived as "the age scan's pages must be unbounded", written for a gauge that
// walked the inventory. This gauge does not walk it: OldestDeadLetterAgeByTopic is a grouped
// aggregate that takes no window and no page, so an unwindowed read is guaranteed by the shape of
// the call rather than by every page of it being checked. The assertion follows the property to
// where it now lives — one aggregate call, and no listing read that a window could be attached
// to. A future refactor that answered the gauge by paging the inventory would fail here, which is
// exactly when the original concern becomes live again.
func TestDeadLetterAgeGauge_ReadsNoWindowAtAll(t *testing.T) {
	dltPinTopicPrefix(t)

	stuck := dltAgedRow(t, "evt_unwindowed_stuck", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 45*time.Minute)
	store := newDltFakeStore().withRow(stuck)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	dltCaptureAgeGauge(t)

	report, err := service.RefreshDeadLetterAgeGauge(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 45*time.Minute, report.OldestAge(),
		"the full age must be reported: a capped read is how a stuck entry looks fresh")

	assert.Equal(t, 1, store.snapshotAgeCalls(),
		"the gauge must answer from the grouped aggregate, which takes no window")
	assert.Empty(t, store.snapshotInventoryPages(),
		"and it must read no inventory page at all: a paged read is what a window could bound, "+
			"and a bounded read caps the age the gauge can ever publish")
	assert.Empty(t, store.snapshotPages(),
		"nor the offset-paged listing, for the same reason")
}

// TestDeadLetterRouting_CountsTheDeadLetterWriteAsABrokerAcknowledgement is OBS-06.
//
// # The gap this closes
//
// blnk.events.broker_acknowledgements.total is declared "by topic, event type and purpose", the
// publisher's increment site states that EVERY purpose is counted because "a replay and a
// dead-letter write are both real acknowledgements", and docs/metrics.md tells an operator to
// break the counter out by purpose to see how much of the broker traffic is triage. The
// increment existed only on the ordinary publish path, so purpose="dead_letter" was never
// written: four acknowledged dead-letter writes produced four dead_lettered attempt records,
// four rows carrying a dlt_topic and four records on the `.dlt` topics, and a triage query that
// returned nothing at all. An absent series reads as no dead-letter traffic, which is
// indistinguishable from a pipeline with nothing wrong.
//
// # Why the two counters are asserted together
//
// EventsDeadLetteredTotal counts EVENTS whose dead-lettering is complete and is attributed to
// the ORIGINAL category topic; this counter counts WRITES the broker accepted and is attributed
// to the `.dlt` sibling that received them. Both are asserted here so the pair cannot be
// collapsed into one increment on one topic, which would break the dead-letter RATE query — it
// divides EventsDeadLetteredTotal against EventsPublishedTotal on matching topic names.
func TestDeadLetterRouting_CountsTheDeadLetterWriteAsABrokerAcknowledgement(t *testing.T) {
	dltPinTopicPrefix(t)

	t.Run("an acknowledged dead-letter write is counted under purpose=dead_letter", func(t *testing.T) {
		row := dltExhaustedRow(t, "evt_ack_counted", "transaction.applied", "blnk.transactions")
		store := newDltFakeStore().withRow(row)
		service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

		acknowledgements := dltCaptureBrokerAcknowledgements(t)
		deadLettered := dltCaptureDeadLetterCounter(t)

		outcome, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
		require.NoError(t, err)
		require.True(t, outcome.Published, "the fixture must actually reach the broker")

		records := acknowledgements.snapshot()
		require.Len(t, records, 1,
			"one acknowledged write is one acknowledgement: not zero, which is the defect, and "+
				"not two, which would inflate the broker-traffic reading")
		assert.EqualValues(t, 1, records[0].value)

		assert.Equal(t, string(PublishPurposeDeadLetter), records[0].attributes["purpose"],
			"the purpose is what makes the triage share readable, and dead_letter is the value the "+
				"instrument, the publisher's own comment and docs/metrics.md all promise")
		assert.Equal(t, "blnk.transactions.dlt", records[0].attributes["topic"],
			"the acknowledgement names the topic the record was WRITTEN to, which is the `.dlt` sibling")
		assert.Equal(t, "transaction.applied", records[0].attributes["event_type"],
			"and the event type, so triage traffic is attributable to the event family producing it")

		// The event-level counter keeps its own attribution, on the ORIGINAL topic, so the
		// dead-letter rate still divides against published events without a name mismatch.
		deadLetterRecords := deadLettered.snapshot()
		require.Len(t, deadLetterRecords, 1)
		assert.Equal(t, "blnk.transactions", deadLetterRecords[0].attributes["topic"],
			"EventsDeadLetteredTotal is attributed to the original category topic; the two counters "+
				"answer different questions and must not be collapsed")
	})

	t.Run("a write the broker refused is not counted as an acknowledgement", func(t *testing.T) {
		// The counter measures traffic the broker ACCEPTED. Counting a refused write here would
		// make acknowledgements run ahead of deliveries, which is the pipeline's documented
		// leading indicator of duplicate delivery — a false reading of the one signal that is
		// supposed to catch rows not being marked.
		row := dltExhaustedRow(t, "evt_ack_refused", "balance.created", "blnk.balances")
		store := newDltFakeStore().withRow(row)
		transport := &dltFakeTransport{writeErr: errors.New("broken pipe")}
		service := dltNewService(store, &dltFakePublisher{}, transport)

		acknowledgements := dltCaptureBrokerAcknowledgements(t)
		attempts := dltCapturePublishAttempts(t)

		_, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
		dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)

		assert.Zero(t, acknowledgements.total(),
			"nothing was acknowledged, so nothing may be counted as acknowledged")
		assert.EqualValues(t, 1, attempts.total(),
			"while the ATTEMPT is still counted, because a refused dead-letter write is exactly "+
				"what an operator needs to see")
	})

	t.Run("no transport means no acknowledgement", func(t *testing.T) {
		row := dltExhaustedRow(t, "evt_ack_no_broker", "identity.created", "blnk.identities")
		store := newDltFakeStore().withRow(row)

		noop := NewNoopEventPublisher()
		service := NewEventDeadLetterService(store, nil)
		service.now = func() time.Time { return dltFixedNow }
		service.withTransport(noop, publisherWriterResolver(noop))

		acknowledgements := dltCaptureBrokerAcknowledgements(t)

		_, err := service.DeadLetter(context.Background(), row, errors.New("no sink"))
		dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)

		assert.Zero(t, acknowledgements.total(),
			"a deployment with no broker must not report broker traffic")
	})
}

// TestDeadLetterOutcomeLogFields_RedactsBrokerTopologyFromTheFailureReason is DATA-02.
//
// # The disclosure this closes
//
// The failure reason was SANITIZED — control characters stripped, length capped — and not
// REDACTED, so its content reached the log intact. Every line built from this projection
// therefore named the broker's host and port, and a resolution failure named the internal DNS
// resolver's address as well, at the default info level and in the two lines a failing pipeline
// emits most: "the Kafka broker did not acknowledge a dead-letter message" and "ledger event
// dead-lettered and preserved on its dead-letter topic".
//
// docs/kafka-operations.md §"Reading the logs" publishes the policy as a three-row table, and
// the two rows that are NOT the log line are asserted elsewhere: the row's last_error and the
// dead-letter message's failure_metadata.error_reason keep the verbatim text, because both are
// reachable only behind the master key. This test covers the row that was wrong — the log line —
// and it asserts the diagnosis survives, because redaction that took the diagnosis with it would
// lengthen every outage it protected.
func TestDeadLetterOutcomeLogFields_RedactsBrokerTopologyFromTheFailureReason(t *testing.T) {
	for name, reason := range map[string]struct {
		text      string
		diagnosis string
		leaks     []string
	}{
		"a refused dial names the broker's address and port": {
			text: "kafka.(*Client).Produce: dial tcp 172.21.0.2:9092: connect: connection refused",
			// The words that tell an operator what to do.
			diagnosis: "connection refused",
			leaks:     []string{"172.21.0.2", "9092", "172.21.0.2:9092"},
		},
		"a resolution failure names the internal resolver too": {
			text:      "kafka.(*Client).Produce: dial tcp: lookup kafka on 127.0.0.11:53: no such host",
			diagnosis: "no such host",
			leaks:     []string{"127.0.0.11", "127.0.0.11:53", "lookup kafka on"},
		},
		"a broken pipe names both ends of the connection": {
			text:      "write tcp 10.0.0.4:34918->10.0.0.7:9092: write: broken pipe",
			diagnosis: "broken pipe",
			leaks:     []string{"10.0.0.4", "10.0.0.7", "34918"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			fields := DeadLetterOutcome{
				EventID:         "evt_topology",
				EventType:       "transaction.applied",
				OriginalTopic:   "blnk.transactions",
				DeadLetterTopic: "blnk.transactions.dlt",
				PartitionKey:    "bln_7d3ac6f1",
				Status:          model.PublishStatusDeadLettered,
				Metadata: model.FailureMetadata{
					OriginalTopic: "blnk.transactions",
					ErrorReason:   reason.text,
					AttemptCount:  5,
				},
			}.LogFields()

			rendered, ok := fields["error_reason"].(string)
			require.True(t, ok, "the reason must still be reported: an operator needs to know what failed")

			for _, leak := range reason.leaks {
				assert.NotContains(t, rendered, leak,
					"%q must not reach a log that is retained, shipped onwards and readable by more "+
						"people than hold the master key", leak)
			}

			assert.Contains(t, rendered, reason.diagnosis,
				"the diagnosis is the whole reason this field exists beside the closed failure_class; "+
					"redaction removes addresses, not words")
			assert.Contains(t, rendered, "[redacted]",
				"and the removal is visible, so nobody reads the line as a truncated or corrupted error")

			// The closed class is untouched, so a log query can still group on it — and it still
			// agrees with the reason, which is what makes the pair readable together.
			assert.Equal(t, "broker_unavailable", fields["failure_class"],
				"the class is derived from the reason and must survive the redaction of its addresses")
		})
	}
}
