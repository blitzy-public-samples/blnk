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
// user-supplied examples from the requirement and must match byte for byte; the fourth,
// blnk.system.dlt, carries the two event types that belong to none of the three named
// categories and follows the identical convention.
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
	"blnk.system.dlt",
	// The quarantine category's sibling. Quarantine holds events whose type the
	// catalogue does not recognise, and those are exactly the events most likely to
	// fail to publish, so its dead-letter topic must be covered by the age gauge like
	// any other — a stalled entry there being invisible would hide the failure of an
	// event that was already a routing defect.
	"blnk.quarantine.dlt",
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
}

// dltPageRequest is one recorded ListDeadLetteredEvents call.
type dltPageRequest struct {
	limit  int
	offset int
}

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
	getErr              error
	listErr             error
	countErr            error
	markDeadLetteredErr error
	markDispatchedErr   error
	claimReplayErr      error
	releaseReplayErr    error

	// Recorded calls.
	deadLettered  []dltMarkRecord
	dispatched    []dltDispatchRecord
	listCalls     []dltPageRequest
	getCalls      []string
	countCalls    int
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
}

// dltReleaseRecord is one ReleaseEventReplay call: the rollback that keeps a failed
// replay replayable.
type dltReleaseRecord struct {
	id         int64
	claimToken string
	replayErr  string
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

// withRow registers a row for lookup and, when its status is one of the two terminal
// failure states, appends it to the dead-letter inventory.
//
// Callers add rows NEWEST FIRST, which is the order the repository returns them in.
func (s *dltFakeStore) withRow(row model.EventOutbox) *dltFakeStore {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored := row
	s.rows[row.EventID] = &stored

	if row.Status == model.EventOutboxStatusDeadLettered || row.Status == model.EventOutboxStatusFailed {
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

// ListDeadLetteredEvents pages the inventory, newest first.
func (s *dltFakeStore) ListDeadLetteredEvents(_ context.Context, limit, offset int) ([]model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.listCalls = append(s.listCalls, dltPageRequest{limit: limit, offset: offset})

	if s.listErr != nil {
		return nil, s.listErr
	}

	if offset >= len(s.inventory) {
		// The repository returns a nil slice for a page past the end. Reproducing that
		// exactly is what lets the nil-to-empty normalisation be asserted.
		return nil, nil
	}

	end := offset + limit
	if end > len(s.inventory) {
		end = len(s.inventory)
	}

	page := make([]model.EventOutbox, 0, end-offset)
	for _, eventID := range s.inventory[offset:end] {
		page = append(page, *s.rows[eventID])
	}

	return page, nil
}

// CountEventOutboxByStatus returns a copy of the status counts.
func (s *dltFakeStore) CountEventOutboxByStatus(_ context.Context) (map[string]int64, error) {
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
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deadLettered = append(s.deadLettered, dltMarkRecord{
		id: id, claimToken: claimToken, dltTopic: dltTopic, metadata: failureMetadata,
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
func (s *dltFakeStore) MarkEventDispatched(_ context.Context, id int64, claimToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dispatched = append(s.dispatched, dltDispatchRecord{id: id, claimToken: claimToken})

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
func (s *dltFakeStore) ReleaseEventReplay(_ context.Context, id int64, claimToken, replayErr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.replayRelease = append(s.replayRelease, dltReleaseRecord{
		id: id, claimToken: claimToken, replayErr: replayErr,
	})

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

// snapshotPages returns a copy of the recorded listing requests.
func (s *dltFakeStore) snapshotPages() []dltPageRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	pages := make([]dltPageRequest, len(s.listCalls))
	copy(pages, s.listCalls)

	return pages
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
func dltExhaustedRow(eventID, eventType, topic string) model.EventOutbox {
	firstAttempt := dltFixedNow.Add(-1 * time.Minute)
	lastAttempt := firstAttempt.Add(31 * time.Second)

	return model.EventOutbox{
		ID:               dltRowID(eventID),
		EventID:          eventID,
		EventType:        eventType,
		AggregateID:      "txn_9c2f4a17",
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
// It goes through the production path — the row-to-request conversion and the envelope
// serialiser the Kafka publisher itself uses — so it is the real message value and not a
// test's idea of one. Every byte-fidelity assertion in this file is stated against it.
func dltOriginalEnvelope(t *testing.T, row model.EventOutbox) []byte {
	t.Helper()

	envelope, err := marshalLedgerEvent(PublishRequestFromOutbox(row, 1).Event)
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
func dltEventIDs(entries []model.EventOutbox) []string {
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
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
			row := dltExhaustedRow("evt_"+route.eventType, route.eventType, route.originalTopic)
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
				"the dead-letter message must keep the original ledger-id key so the .dlt topic "+
					"preserves the same per-aggregate ordering as the topic it failed to reach")
			assert.Equal(t, row.LedgerID, outcome.PartitionKey)
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
			row := dltExhaustedRow("evt_fallback_"+route.eventType, route.eventType, "")
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

	row := dltExhaustedRow("evt_recorded", "transaction.applied", "blnk.transactions")
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
func TestDeadLetterRouting_NeverSpendsAnotherAttemptOnTheRow(t *testing.T) {
	seam := reflect.TypeOf((*eventDeadLetterStore)(nil)).Elem()

	declared := make([]string, 0, seam.NumMethod())
	for i := 0; i < seam.NumMethod(); i++ {
		declared = append(declared, seam.Method(i).Name)
	}

	// Seven methods now, and the two additions are the replay CLAIM and its rollback.
	// They are not "claim methods" in the relay's sense — neither can take a pending row
	// or take part in publishing new events. ClaimEventForReplay moves a row that is
	// ALREADY dead-lettered into replaying, which is what makes a replay atomic instead
	// of a read followed by a check that two concurrent requests could both pass.
	assert.ElementsMatch(t, []string{
		"GetEventByID",
		"ListDeadLetteredEvents",
		"CountEventOutboxByStatus",
		"MarkEventDeadLettered",
		"MarkEventDispatched",
		"ClaimEventForReplay",
		"ReleaseEventReplay",
	}, declared, "the dead-letter store seam must expose exactly these seven methods")

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
	row := dltExhaustedRow("evt_no_extra_attempt", "transaction.applied", "blnk.transactions")
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

	row := dltExhaustedRow("evt_no_broker", "balance.created", "blnk.balances")
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
	assert.Equal(t, model.EventOutboxStatusFailed, store.row(t, row.EventID).Status,
		"the row keeps the non-terminal status the caller gave it, so it stays visible and completable")
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

	row := dltExhaustedRow("evt_no_config", "identity.created", "blnk.identities")
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
	assert.Equal(t, model.EventOutboxStatusFailed, store.row(t, row.EventID).Status)
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

	row := dltExhaustedRow("evt_wrong_publisher", "identity.created", "blnk.identities")
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

	row := dltExhaustedRow("evt_no_id", "transaction.applied", "blnk.transactions")
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

	row := dltExhaustedRow("evt_write_failed", "transaction.void", "blnk.transactions")
	store := newDltFakeStore().withRow(row)
	transport := &dltFakeTransport{writeErr: errors.New("broken pipe")}
	service := dltNewService(store, &dltFakePublisher{}, transport)

	counter := dltCaptureDeadLetterCounter(t)

	outcome, err := service.DeadLetter(context.Background(), row, errors.New(dltPublishFailureReason))
	dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)

	assert.False(t, outcome.Published)
	assert.Empty(t, store.snapshotDeadLettered(), "the row must not be recorded after a failed write")
	assert.Equal(t, model.EventOutboxStatusFailed, store.row(t, row.EventID).Status,
		"the row stays failed, which the dead-letter listing still covers")
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
	row := dltExhaustedRow("evt_metadata_shape", "transaction.applied", "blnk.transactions")
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

	row := dltExhaustedRow("evt_metadata_fields", "transaction.applied", "blnk.transactions")
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

	row := dltExhaustedRow("evt_attempts", "transaction.applied", "blnk.transactions")

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

	row := dltExhaustedRow("evt_window", "transaction.applied", "blnk.transactions")
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

	row := dltExhaustedRow("evt_reason", "transaction.applied", "blnk.transactions")

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

	row := dltExhaustedRow("evt_additive", "transaction.applied", "blnk.transactions")
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
		row := dltExhaustedRow("evt_strip_plain", "transaction.applied", "blnk.transactions")
		original := dltOriginalEnvelope(t, row)

		recovered, err := StripFailureMetadata(original)
		require.NoError(t, err)
		assert.Equal(t, original, recovered)
	})

	t.Run("stripping twice changes nothing the second time", func(t *testing.T) {
		row := dltExhaustedRow("evt_strip_twice", "transaction.applied", "blnk.transactions")
		service := dltNewService(newDltFakeStore().withRow(row), &dltFakePublisher{}, &dltFakeTransport{})

		outcome, err := service.DeadLetter(context.Background(), row, errors.New("boom"))
		require.NoError(t, err)

		once := dltMustStrip(t, outcome.Message)
		twice := dltMustStrip(t, once)
		assert.Equal(t, once, twice, "StripFailureMetadata must be idempotent")
		assert.Equal(t, dltOriginalEnvelope(t, row), twice)
	})

	t.Run("a payload naming the member deeper inside is not mistaken for the attachment", func(t *testing.T) {
		row := dltExhaustedRow("evt_strip_decoy", "transaction.applied", "blnk.transactions")
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

	row := dltExhaustedRow("evt_compose_guard", "transaction.applied", "blnk.transactions")

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

	row := dltExhaustedRow("evt_"+eventType+"_replay", eventType, topic)
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
// It serialises the captured request through the SAME envelope serialiser the Kafka publisher
// uses, so the comparison is against the real message value rather than against a
// reconstruction of it. This is the assertion criterion V-9 is decided by.
func (f *dltReplayFixture) replayedMessage(t *testing.T) []byte {
	t.Helper()

	requests := f.publisher.snapshotRequests()
	require.Len(t, requests, 1, "exactly one replay publish must have been issued")

	message, err := marshalLedgerEvent(requests[0].Event)
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
// The key is the ledger id, and keying by ledger id with a stable hash balancer is what pins
// every event for one ledger to one partition. A replay published under a different key — or
// under none — would land on a different partition, and the replay would itself violate the
// per-aggregate ordering the pipeline exists to preserve.
func TestReplayDeadLetteredEvent_KeepsTheMessageKeyUnchanged(t *testing.T) {
	dltPinTopicPrefix(t)

	fixture := dltNewReplayFixture(t, "transaction.applied", "blnk.transactions", "")

	outcome, err := fixture.service.ReplayDeadLetteredEvent(context.Background(), fixture.row.EventID)
	require.NoError(t, err)

	assert.Equal(t, fixture.row.LedgerID, outcome.PartitionKey,
		"the replay must be keyed by the row's ledger id, exactly as the original publish was")
	assert.Equal(t, fixture.row.LedgerID, fixture.request(t).Key)
	assert.Equal(t, fixture.outcome.PartitionKey, outcome.PartitionKey,
		"the dead-letter write and the replay must use one and the same key")
	assert.NotEmpty(t, outcome.PartitionKey,
		"an empty key would spread the event across partitions and give up its ordering")

	t.Run("a row without a ledger id falls back to the aggregate", func(t *testing.T) {
		row := fixture.row
		row.LedgerID = ""

		store := newDltFakeStore().withRow(row)
		publisher := &dltFakePublisher{}
		service := dltNewService(store, publisher, &dltFakeTransport{})

		replayed, replayErr := service.ReplayDeadLetteredEvent(context.Background(), row.EventID)
		require.NoError(t, replayErr)

		assert.Equal(t, row.AggregateID, replayed.PartitionKey,
			"the fallback still pins one aggregate's events to one partition")
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
	assert.Empty(t, after,
		"a replayed event must no longer be listed as dead-lettered")
	assert.NotNil(t, after, "an empty inventory must still marshal as [] rather than null")
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
			row := dltExhaustedRow("evt_state_"+status, "transaction.applied", "blnk.transactions")
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

	row := dltExhaustedRow("evt_broker_detail", "balance.created", "blnk.balances")
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
// The dead-letter RATE is evaluated as the ratio of this counter to the published-events
// counter, and it must stay under 0.1%. A double count would therefore report twice the real
// rate and fail a perfectly healthy system; a missing count would hide a genuine incident.
// "Exactly once" is asserted as exactly one increment of exactly one, not merely as "non-zero".
//
// The attribution is asserted at the same time: the counter carries the ORIGINAL category topic,
// never the `.dlt` sibling, which is what makes it directly comparable with the published-events
// counter it is divided by. Attributing it to the `.dlt` name would put the numerator and the
// denominator in different label spaces and make the ratio unreadable.
func TestDeadLetterMetrics_CountsEachDeadLetteredEventExactlyOnce(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow("evt_counted_once", "transaction.applied", "blnk.transactions")
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
		second := dltExhaustedRow("evt_counted_once_again", "balance.created", "blnk.balances")
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
		row := dltExhaustedRow("evt_uncounted_write", "transaction.applied", "blnk.transactions")
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
		row := dltExhaustedRow("evt_uncounted_record", "transaction.applied", "blnk.transactions")
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

// TestDeadLetterMetrics_RecordsTheDeadLetteredOutcomeOnThePublishAttemptCounter pins the third
// value of the attempts counter's documented label set.
//
// The dead-letter write itself is a real publish the broker accepted, so it is recorded as an
// attempt — but stamped with the DEAD-LETTERED outcome rather than dispatched, because the
// pipeline-level statement about the event is that it ended its life on a dead-letter topic. That
// is the value an operator filters on to see retry pressure separately from delivery volume.
func TestDeadLetterMetrics_RecordsTheDeadLetteredOutcomeOnThePublishAttemptCounter(t *testing.T) {
	dltPinTopicPrefix(t)

	row := dltExhaustedRow("evt_attempt_outcome", "transaction.applied", "blnk.transactions")
	service := dltNewService(newDltFakeStore().withRow(row), &dltFakePublisher{}, &dltFakeTransport{})

	attempts := dltCapturePublishAttempts(t)

	_, err := service.DeadLetter(context.Background(), row, errors.New("boom"))
	require.NoError(t, err)

	records := attempts.snapshot()
	require.Len(t, records, 1, "a successful dead-letter write records exactly one attempt")
	assert.Equal(t, string(model.PublishStatusDeadLettered), records[0].attributes[publishAttrOutcome],
		"the attempt must be attributed to the dead-lettered outcome, not to dispatched")

	t.Run("a failed dead-letter write records no attempt", func(t *testing.T) {
		failing := dltExhaustedRow("evt_attempt_outcome_failed", "transaction.applied", "blnk.transactions")
		service := dltNewService(
			newDltFakeStore().withRow(failing),
			&dltFakePublisher{},
			&dltFakeTransport{writeErr: errors.New("broken pipe")},
		)
		failedAttempts := dltCapturePublishAttempts(t)

		_, writeErr := service.DeadLetter(context.Background(), failing, errors.New("boom"))
		require.Error(t, writeErr)
		assert.Empty(t, failedAttempts.snapshot(),
			"the relay already recorded one attempt per real publish; this must not add another")
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

	oldest := dltAgedRow("evt_age_oldest", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 45*time.Minute)
	newest := dltAgedRow("evt_age_newest", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 2*time.Minute)
	pendingWrite := dltAgedRow("evt_age_failed", "balance.created", "blnk.balances",
		model.EventOutboxStatusFailed, "", 20*time.Minute)

	// Newest first, exactly as the repository orders the inventory.
	store := newDltFakeStore().withRow(newest).withRow(pendingWrite).withRow(oldest)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	gauge := dltCaptureAgeGauge(t)

	report, err := service.RefreshDeadLetterAgeGauge(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(3), report.Outstanding,
		"both terminal failure states count as outstanding")
	assert.Equal(t, 3, report.Scanned)
	assert.False(t, report.Truncated)
	assert.Equal(t, dltFixedNow, report.GeneratedAt)

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

	t.Run("a truncated scan still reads from the oldest end", func(t *testing.T) {
		// A forward walk would examine the NEWEST rows and, on truncation, report an age drawn
		// from exactly the wrong end of the inventory.
		bounded := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{}).WithScanLimit(1)
		bounded.now = func() time.Time { return dltFixedNow }

		truncated, truncErr := bounded.RefreshDeadLetterAgeGauge(context.Background())
		require.NoError(t, truncErr)

		assert.True(t, truncated.Truncated, "the scan bound must be reported, never silently applied")
		assert.Equal(t, 1, truncated.Scanned)
		assert.Equal(t, 45*time.Minute, truncated.OldestAge(),
			"a bounded scan must still find the oldest entry")
	})

	t.Run("a future-dated row cannot lower the maximum", func(t *testing.T) {
		skewed := dltAgedRow("evt_age_skewed", "identity.created", "blnk.identities",
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
	assert.Zero(t, report.Scanned, "an empty inventory must not be walked at all")
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

// TestReplayDeadLetteredEvent_ClaimsTheRowSoConcurrentReplaysCannotBothPublish is the
// STATE-01 proof on the replay path.
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
// # What is asserted
//
// The precondition now lives INSIDE the transition. Exactly one of two sequential replays
// claims the row, so exactly one publishes; the second is refused before reaching the
// publisher. The claim is asserted to have been attempted twice — proving the second
// request really did try — while the publisher saw one message.
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
	stale := dltAgedRow("evt_age_stale", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 45*time.Minute)
	stale.OccurredAt = dltFixedNow.Add(-50 * time.Minute)

	// Occurred 90 minutes ago but sat in a relay backlog, so its final attempt was only two
	// minutes ago. It occurred EARLIER, so it sorts later in the occurrence-ordered inventory,
	// yet it is far newer by age.
	lateRetry := dltAgedRow("evt_age_late_retry", "transaction.applied", "blnk.transactions",
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

	row := dltExhaustedRow("evt_gauge_untouched", "transaction.applied", "blnk.transactions")
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
func dltAgedRow(eventID, eventType, topic, status, dltTopic string, age time.Duration) model.EventOutbox {
	row := dltExhaustedRow(eventID, eventType, topic)

	lastAttempt := dltFixedNow.Add(-age)
	firstAttempt := lastAttempt.Add(-31 * time.Second)
	row.FirstAttemptedAt = &firstAttempt
	row.LastAttemptedAt = &lastAttempt
	row.OccurredAt = firstAttempt.Add(-time.Minute)
	row.Status = status
	row.DLTTopic = dltTopic

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

	entry := dltAgedRow("evt_list_source", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 5*time.Minute)

	store := newDltFakeStore().withRow(entry)
	transport := &dltFakeTransport{resolveErr: errors.New("a listing must not touch the transport")}
	publisher := &dltFakePublisher{}
	service := dltNewService(store, publisher, transport)

	entries, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{})
	require.NoError(t, err, "listing must work with no broker reachable at all")

	require.Len(t, entries, 1)
	assert.Equal(t, entry.EventID, entries[0].EventID)
	assert.Equal(t, "blnk.transactions.dlt", entries[0].DLTTopic,
		"the dead-letter topic comes from the ROW, which is why no consumer is needed")
	assert.NotEmpty(t, entries[0].FailureMetadata,
		"the failure record comes from the row as well")

	assert.Empty(t, transport.snapshotRequested(),
		"no writer may be resolved to serve a listing")
	assert.Empty(t, publisher.snapshotRequests(),
		"no publish may be issued to serve a listing")
	assert.Equal(t, []dltPageRequest{{limit: defaultDeadLetterListLimit, offset: 0}}, store.snapshotPages(),
		"an unfiltered page must be one repository query, passed straight through")

	// The stored failure record decodes through the one shared decoder, which is how the API
	// projection reads it.
	metadata, decodeErr := DecodeFailureMetadata(entries[0].FailureMetadata)
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

	deadLettered := dltAgedRow("evt_list_dead", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 3*time.Minute)
	failed := dltAgedRow("evt_list_failed", "balance.created", "blnk.balances",
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
// A caller asking for a page that is too large, or an offset below zero, has made a recoverable
// mistake, and degrading to a sane page is more useful than an error. The ceiling matters
// operationally: without it a triage endpoint becomes a full-table scan.
func TestListDeadLetterEvents_PagesWithTheDocumentedBounds(t *testing.T) {
	dltPinTopicPrefix(t)

	store := newDltFakeStore()
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	cases := []struct {
		name     string
		given    DeadLetterListOptions
		expected dltPageRequest
	}{
		{
			name:     "the zero value asks for the default page",
			given:    DeadLetterListOptions{},
			expected: dltPageRequest{limit: defaultDeadLetterListLimit, offset: 0},
		},
		{
			name:     "a non-positive limit falls back to the default",
			given:    DeadLetterListOptions{Limit: 0},
			expected: dltPageRequest{limit: defaultDeadLetterListLimit, offset: 0},
		},
		{
			name:     "a negative limit falls back to the default",
			given:    DeadLetterListOptions{Limit: -25},
			expected: dltPageRequest{limit: defaultDeadLetterListLimit, offset: 0},
		},
		{
			name:     "an oversized limit is clamped to the ceiling",
			given:    DeadLetterListOptions{Limit: maxDeadLetterListLimit + 5000},
			expected: dltPageRequest{limit: maxDeadLetterListLimit, offset: 0},
		},
		{
			name:     "a negative offset is clamped to zero",
			given:    DeadLetterListOptions{Limit: 10, Offset: -3},
			expected: dltPageRequest{limit: 10, offset: 0},
		},
		{
			name:     "an explicit page is passed through unchanged",
			given:    DeadLetterListOptions{Limit: 25, Offset: 75},
			expected: dltPageRequest{limit: 25, offset: 75},
		},
	}

	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := service.ListDeadLetterEvents(context.Background(), testCase.given)
			require.NoError(t, err)

			pages := store.snapshotPages()
			require.Len(t, pages, index+1)
			assert.Equal(t, testCase.expected, pages[index])
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
// Paging over a FILTERED set skips matches rather than rows, so a filtered page behaves like a
// page of the filtered set rather than a filtered page of the unfiltered set — the latter would
// return short, unstable pages as the filter's hit rate varied.
func TestListDeadLetterEvents_AppliesTheExposedFilters(t *testing.T) {
	dltPinTopicPrefix(t)

	applied := dltAgedRow("evt_filter_applied", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", time.Minute)
	void := dltAgedRow("evt_filter_void", "transaction.void", "blnk.transactions",
		model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", 2*time.Minute)
	appliedAgain := dltAgedRow("evt_filter_applied_again", "transaction.applied", "blnk.transactions",
		model.EventOutboxStatusFailed, "", 3*time.Minute)
	balance := dltAgedRow("evt_filter_balance", "balance.monitor", "blnk.balances",
		model.EventOutboxStatusDeadLettered, "blnk.balances.dlt", 4*time.Minute)

	store := newDltFakeStore().withRow(applied).withRow(void).withRow(appliedAgain).withRow(balance)
	service := dltNewService(store, &dltFakePublisher{}, &dltFakeTransport{})

	list := func(t *testing.T, opts DeadLetterListOptions) []string {
		t.Helper()

		entries, err := service.ListDeadLetterEvents(context.Background(), opts)
		require.NoError(t, err)

		return dltEventIDs(entries)
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

	t.Run("a filtered page skips MATCHES, not rows", func(t *testing.T) {
		assert.Equal(t,
			[]string{appliedAgain.EventID},
			list(t, DeadLetterListOptions{EventType: "transaction.applied", Offset: 1}),
			"offset must count matching entries, or a filtered page would be short and unstable")
		assert.Equal(t,
			[]string{applied.EventID},
			list(t, DeadLetterListOptions{EventType: "transaction.applied", Limit: 1}))
	})

	t.Run("a filter matching nothing returns an empty page, not nil", func(t *testing.T) {
		entries, err := service.ListDeadLetterEvents(
			context.Background(), DeadLetterListOptions{EventType: "identity.created"},
		)
		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.NotNil(t, entries, "an empty result must marshal as [] rather than null")
	})
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

	entries, err := service.ListDeadLetterEvents(context.Background(), DeadLetterListOptions{})
	require.NoError(t, err)
	assert.NotNil(t, entries, "an empty inventory must still yield a non-nil slice")
	assert.Empty(t, entries)

	encoded, marshalErr := json.Marshal(entries)
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
		"the four Blnk-owned dead-letter topics are the whole of what Blnk owns")
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

// TestKafkaTransportCredentials_FallsBackToTheAdminPrincipalOnlyWhenNoProducerExists covers the
// compatibility path.
//
// It exists for one reason: an existing single-credential deployment must keep publishing across
// an upgrade rather than stop dead. It is retained deliberately and it is not silent — the
// fallback warns, naming the variables to set — because a silent compatibility path is how a
// temporary allowance becomes the permanent configuration.
func TestKafkaTransportCredentials_FallsBackToTheAdminPrincipalOnlyWhenNoProducerExists(t *testing.T) {
	cfg := kafkaTransportConfig()
	cfg.SASLUser, cfg.SASLSecret = "", ""
	cfg.SASLAdminUser, cfg.SASLAdminSecret = "blnk-admin", "admin-secret"

	user, secret, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
	require.NoError(t, err)
	assert.Equal(t, "blnk-admin", user)
	assert.Equal(t, "admin-secret", secret)
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

	t.Run("a producer fallback inherits the admin pair's validation", func(t *testing.T) {
		cfg := kafkaTransportConfig()
		cfg.SASLUser, cfg.SASLSecret = "", ""
		cfg.SASLAdminUser = "blnk-admin"

		_, _, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
		require.Error(t, err,
			"falling back must not bypass the check the admin role would have applied")
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
//   - A PublishError's own verdict wins over any re-derivation, in BOTH directions.
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
		"deadline expired":                         context.DeadlineExceeded,
		"context cancelled":                        context.Canceled,
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
		"the acknowledgement deadline expired": fmt.Errorf(
			"waiting for acknowledgement: %w", context.DeadlineExceeded),
		"the transport is closed": fmt.Errorf("resolving a writer: %w", ErrEventPublisherClosed),
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
