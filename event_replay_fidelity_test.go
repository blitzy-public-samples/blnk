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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// event_replay_fidelity_test.go is the acceptance evidence for one criterion: a replayed
// dead-lettered event carries the same bytes as the original, aside from the failure
// metadata.
//
// What a replay reproduces is THE STORED ENVELOPE. blnk.event_outbox.event_raw holds the
// canonical bytes composed once at capture, and every later transport — a retry, the
// dead-letter copy, a replay — reads that column rather than rebuilding the envelope. That is
// what makes the byte guarantee a property of the data instead of a property of one build of
// the serialiser: an envelope member added or reordered in a later release cannot change the
// bytes of an event captured before it.
//
// Composition remains the documented fallback for a row written before the column existed,
// and it is deterministic: the payload is spliced in untransformed because
// model.LedgerEvent.Payload is json.RawMessage rather than a map or a typed struct, and the
// five envelope scalars are composed member by member — so one row always yields one byte
// sequence. TestReplayFidelity_PublishesTheStoredEnvelopeRatherThanARebuild is what separates
// the two: it stores an envelope the serialiser could not have produced and asserts the stored
// value is what reaches the wire, so this file cannot pass by having both ends of the
// comparison rebuild the same thing.
//
// ComposeDeadLetterMessage attaches the failure metadata by replacing the envelope's closing
// brace with `,"failure_metadata":<metadata>}`. Every envelope byte before that brace is
// preserved, so the envelope is not a complete prefix of the dead-letter message — the brace
// is the byte that moves — while StripFailureMetadata is the exact inverse and reconstructs
// the envelope byte for byte.
//
// The fixtures are chosen so a struct round trip would be visible: decoding into a map or a
// typed struct and re-encoding would reorder keys, renormalise number literals, re-render
// timestamps or drop members no Go struct declares.
//
// No broker is required. The transport is substituted through
// EventDeadLetterService.withTransport with a recording publisher and dead-letter writer, and
// the recording publisher resolves its message value through the real resolveEventValue, so
// the bytes it captures are the bytes a broker would have received — the stored envelope where
// the row has one, and a composed envelope only where it does not.
//
// Adjacent criteria live elsewhere: dual delivery in event_dual_delivery_test.go, ordering and
// crash recovery in their own integration tests, and the replay endpoint with its master-key
// gate in api/events_api_test.go.

// Pinned instants

var (
	// replayFidelityOccurredAt is the domain instant every fixture event occurred at.
	// The nanosecond component is deliberately non-zero and non-trailing-zero so its
	// RFC3339Nano rendering exercises the full-precision form.
	replayFidelityOccurredAt = time.Date(2026, time.April, 7, 9, 15, 22, 123456789, time.UTC)

	// replayFidelityFirstAttemptAt is when the relay first tried to publish.
	replayFidelityFirstAttemptAt = time.Date(2026, time.April, 7, 9, 15, 23, 0, time.UTC)

	// replayFidelityLastAttemptAt is when the relay's final attempt failed, 31 seconds
	// after the first — the 1s + 2s + 4s + 8s + 16s backoff schedule the five-attempt
	// budget produces.
	replayFidelityLastAttemptAt = time.Date(2026, time.April, 7, 9, 15, 54, 0, time.UTC)

	// replayFidelityDeadLetterAt is when the dead-letter write happened.
	replayFidelityDeadLetterAt = time.Date(2026, time.April, 7, 9, 15, 55, 0, time.UTC)

	// replayFidelityReplayAt is when an operator triggered the replay: a full day
	// later, so an implementation that stamped occurred_at with the replay time would
	// be off by 24 hours rather than by a rounding error.
	replayFidelityReplayAt = time.Date(2026, time.April, 8, 9, 15, 55, 0, time.UTC)
)

// replayFidelityMaxAttempts is the retry budget every fixture row carries. It matches
// the RELAY_MAX_RETRY_ATTEMPTS default of five, and the failure metadata's attempt
// count is asserted against it: a metadata attempt count that disagrees with the
// configured maximum means the exhaustion accounting is wrong.
const replayFidelityMaxAttempts = 5

// errReplayFidelityBroker is the transient broker failure that exhausts the retry
// budget. Its text is asserted to survive into FailureMetadata.ErrorReason, so an
// implementation that discarded the cause and stored an empty or generic reason fails.
var errReplayFidelityBroker = errors.New("write tcp 10.0.0.4:9092: broken pipe")

// THE FIXTURES

// replayFidelityTransactionPayload is a transaction.applied body as read back from the
// payload column. It routes to the transactions category.
const replayFidelityTransactionPayload = `{"data": {"rate": 0.10000000000000000555, "source": "bln_source_001", "destination": "bln_destination_001", "scaled_amount": 123456789012345678901234, "effective_date": "2024-05-01T12:34:56.789000+00:00", "precise_amount": 9007199254740993, "transaction_id": "txn_replay_fidelity_001", "unmodelled_extension": {"vendor_flag": true, "vendor_note": "retained verbatim"}}, "event": "transaction.applied"}`

// replayFidelityBalancePayload is a balance.monitor body. It routes to the balances
// category and carries a nested object, so member reordering is detectable at depth
// rather than only at the top level.
const replayFidelityBalancePayload = `{"data": {"condition": {"field": "debit_balance", "operator": "gte", "precise_value": 9007199254740993}, "balance_id": "bln_replay_fidelity_002", "monitor_id": "mon_replay_fidelity_002", "triggered_at": "2024-06-02T08:09:10.500000+00:00", "threshold_ratio": 0.30000000000000004441, "scaled_threshold": 123456789012345678901234, "unmodelled_extension": {"vendor_flag": true}}, "event": "balance.monitor"}`

// replayFidelityIdentityPayload is an identity.created body. It routes to the
// identities category.
const replayFidelityIdentityPayload = `{"data": {"dob": "1815-12-10T00:00:00.000000+00:00", "last_name": "Lovelace", "first_name": "Ada", "identity_id": "idt_replay_fidelity_003", "risk_weight": 0.70000000000000006661, "credit_score": 9007199254740993, "email_address": "ada@example.test", "scaled_income": 123456789012345678901234, "unmodelled_extension": {"vendor_flag": true}}, "event": "identity.created"}`

// replayFidelityLedgerPayload is a ledger.created body. It routes to the system
// category — one of the two that exist because ledger.created and system.error belong to
// none of the three categories the requirements name, and coverage is absolute. It is a
// category of its own rather than sharing the internal one because ledger.created must
// stay reachable by a subscriber and system.error must not.
const replayFidelityLedgerPayload = `{"data": {"name": "Replay Fidelity Ledger", "ledger_id": "ldg_replay_fidelity_004", "meta_data": {"region": "eu-west-1", "sequence": 9007199254740993, "utilisation": 0.90000000000000002220, "scaled_capacity": 123456789012345678901234}, "created_at": "2024-07-03T22:11:00.250000+00:00", "unmodelled_extension": {"vendor_flag": true}}, "event": "ledger.created"}`

// replayFidelityFixture is one event, its stored bytes and the destinations it must
// resolve to. One fixture exists per category so that a category-specific bug in
// dead-letter routing or in original-topic recovery is caught rather than masked by a
// single happy-path event.
type replayFidelityFixture struct {
	// name labels the subtest.
	name string
	// eventType is the event name, which is what the category mapping routes on.
	eventType string
	// aggregateID is the subject of the event and appears in the envelope.
	aggregateID string
	// partitionKey is the Kafka message key — model.EventOutbox.PartitionKey, the
	// column ClaimPendingEventOutbox serialises dispatch on. It is deliberately
	// DIFFERENT from aggregateID everywhere it can be, so an implementation that keyed
	// by the aggregate instead is caught rather than passing by coincidence.
	//
	// It used to be called ledgerID, and the values below show why that name was
	// wrong: a transaction keys by its SOURCE BALANCE and a monitor by the BALANCE it
	// watches, neither of which is a ledger. The two are separate columns precisely
	// because conflating them made the key lie about most of its values.
	partitionKey string
	// ledgerID is the AUTHORITATIVE ledger the event belongs to, and it takes no part
	// in partitioning. It is empty for every event whose payload carries no ledger — a
	// transaction, a balance monitor, an identity — exactly as PrepareEventOutbox
	// stores it, so the fixture cannot accidentally make ledger_id look like a usable
	// key.
	ledgerID string
	// payload is the stored payload column, byte for byte.
	payload string
	// topic is the category topic the event was destined for.
	topic string
	// deadLetterTopic is the exact `<topic>.dlt` sibling the event must land on.
	deadLetterTopic string
}

// replayFidelityFixtures returns one fixture per SUBSCRIBER-FACING event category, with the
// dead-letter topic names written out as LITERALS rather than derived with DLTFor.
//
// The internal system category has no fixture because no producer emits a system.error with a
// ledger-shaped payload to replay; TestReplayFidelityFixtures_AgreeWithProductionRouting is
// what keeps every fixture that IS here honest about where its event really routes.
func replayFidelityFixtures() []replayFidelityFixture {
	return []replayFidelityFixture{
		{
			name:        "transactions",
			eventType:   "transaction.applied",
			aggregateID: "txn_replay_fidelity_001",
			// A transaction keys by its source balance and carries NO ledger:
			// model.Transaction has no ledger field, so ledger_id stays NULL.
			partitionKey:    "bln_source_001",
			ledgerID:        "",
			payload:         replayFidelityTransactionPayload,
			topic:           "blnk.transactions",
			deadLetterTopic: "blnk.transactions.dlt",
		},
		{
			name:        "balances",
			eventType:   "balance.monitor",
			aggregateID: "mon_replay_fidelity_002",
			// A monitor keys by the balance it watches; BalanceMonitor carries no
			// ledger field either.
			partitionKey:    "bln_replay_fidelity_002",
			ledgerID:        "",
			payload:         replayFidelityBalancePayload,
			topic:           "blnk.balances",
			deadLetterTopic: "blnk.balances.dlt",
		},
		{
			name:        "identities",
			eventType:   "identity.created",
			aggregateID: "idt_replay_fidelity_003",
			// An identity is not scoped to a ledger in this model, so it keys by
			// itself and records no ledger.
			partitionKey:    "idt_replay_fidelity_003",
			ledgerID:        "",
			payload:         replayFidelityIdentityPayload,
			topic:           "blnk.identities",
			deadLetterTopic: "blnk.identities.dlt",
		},
		{
			name:        "ledger_created",
			eventType:   "ledger.created",
			aggregateID: "ldg_replay_fidelity_004",
			// ledger.created is one of the two shapes that genuinely DO carry a
			// ledger, so both columns hold it — which is how requirement R-6's
			// "partitioned by ledger ID" is honoured wherever a ledger exists. It
			// routes to the system category, which is where both event types outside
			// the three named categories are published.
			partitionKey:    "ldg_replay_fidelity_004",
			ledgerID:        "ldg_replay_fidelity_004",
			payload:         replayFidelityLedgerPayload,
			topic:           "blnk.system",
			deadLetterTopic: "blnk.system.dlt",
		},
	}
}

// row builds the outbox row the relay would have claimed for this fixture: the
// envelope columns populated, the retry budget set, and the row still pending.
//
// event_raw IS POPULATED, because the column is not optional in a row the relay claims.
// PrepareEventOutbox writes the canonical envelope once at capture and every later
// transport reads it back, so a fixture that left it empty would drive the composition
// fallback through the whole scenario and prove nothing about the value the database
// actually holds. It is composed here with the same serialiser production uses — this
// fixture is standing in for the persistence layer, not for the producer — and
// TestReplayFidelity_PublishesTheStoredEnvelopeRatherThanARebuild is what shows the
// stored value is genuinely the one on the wire.
func (f replayFidelityFixture) row(t *testing.T, id int64) model.EventOutbox {
	t.Helper()

	row := model.EventOutbox{
		ID:            id,
		EventID:       fmt.Sprintf("event-replay-fidelity-%s-%d", f.name, id),
		EventType:     f.eventType,
		AggregateID:   f.aggregateID,
		PartitionKey:  f.partitionKey,
		LedgerID:      f.ledgerID,
		Topic:         f.topic,
		SchemaVersion: model.SchemaVersionV1,
		Payload:       json.RawMessage(f.payload),
		OccurredAt:    replayFidelityOccurredAt,
		Status:        model.EventOutboxStatusPending,
		MaxAttempts:   replayFidelityMaxAttempts,
	}

	raw, err := row.CanonicalEvent().CanonicalBytes()
	require.NoError(t, err, "the fixture envelope must serialise before it can be stored")
	row.EventRaw = raw

	return row
}

// Test doubles
// ---------------------------------------------------------------------------

// movableTestClock is a settable, RACE-SAFE clock for a test that installs its own `now`.
//
// The mutex is not ceremony. A `func() time.Time { return now }` closure over a plain local
// variable is perfectly correct while only the test goroutine reads it — and three gate tests in
// event_admin_test.go do exactly that, legitimately, because they call Ready() themselves. It
// becomes a data race the moment the code under test reads the clock on a goroutine of its own,
// which the event relay does: `-race` caught precisely that in
// TestEventRelayProcessor_DoesNotClaimUntilTheCatalogueGateOpens, where the test advanced the
// instant while the relay's run loop was reading it through the gate.
//
// So: use this whenever anything the test starts reads the clock. It is named for the capability
// rather than for its first caller, because the replay-fidelity suite is no longer the only one
// that needs it.
type movableTestClock struct {
	mu  sync.Mutex
	now time.Time
}

// Now reports the current pinned instant.
func (c *movableTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

// Set moves the clock to at.
func (c *movableTestClock) Set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = at
}

// replayFidelityStore is an in-memory eventDeadLetterStore.
type replayFidelityStore struct {
	mu sync.Mutex

	// rows holds every row by its business event id.
	rows map[string]*model.EventOutbox

	// order preserves insertion order so listing is deterministic for rows that share
	// an occurrence instant, as every fixture row does.
	order []string

	// transitions records each applied transition as "<eventID>:<status>", in order,
	// so a test can assert not just the end state but the path taken to it.
	transitions []string

	// markDispatchedErr, when set, fails MarkEventDispatched. It models the one case
	// in which a successful replay still returns an error: the event was republished
	// but the bookkeeping did not land.
	markDispatchedErr error

	// getErr, when set, fails GetEventByID with a non-not-found error.
	getErr error

	// claimTokens records the token presented to every transition, keyed by the
	// transition name, so a test can assert the service carried the token the claim
	// issued rather than an empty string or one of its own invention.
	claimTokens map[string]string

	// issuedClaimToken is the token ClaimEventForReplay hands out. Fixed rather than
	// generated so the assertion above can compare against a known value.
	issuedClaimToken string
}

// recordClaimToken notes the token one transition presented. The caller holds the lock.
func (s *replayFidelityStore) recordClaimToken(transition, token string) {
	if s.claimTokens == nil {
		s.claimTokens = make(map[string]string, 4)
	}
	s.claimTokens[transition] = token
}

// presentedClaimToken returns the token a named transition was called with.
func (s *replayFidelityStore) presentedClaimToken(transition string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.claimTokens[transition]
}

// ClaimEventForReplay is the atomic dead_lettered -> replaying claim.
//
// The status precondition lives HERE, inside the transition, exactly as it does in SQL —
// which is what makes a second concurrent replay of one row fail because the first one
// moved it rather than because this double was told to fail.
func (s *replayFidelityStore) ClaimEventForReplay(
	_ context.Context,
	eventID string,
	_ time.Duration,
) (*model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.getErr != nil {
		return nil, s.getErr
	}

	row, ok := s.rows[eventID]
	if !ok {
		return nil, apierror.NewAPIError(
			apierror.ErrNotFound, "Event not found", fmt.Errorf("no event outbox row with event id %q", eventID),
		)
	}

	if row.Status != model.EventOutboxStatusDeadLettered {
		return nil, apierror.NewAPIError(apierror.ErrConflict,
			fmt.Sprintf("Event is not available for replay: its status is %q", row.Status), nil)
	}

	token := s.issuedClaimToken
	if token == "" {
		token = "replay-claim-token"
	}

	row.Status = model.EventOutboxStatusReplaying
	row.ClaimToken = token
	s.transitions = append(s.transitions, row.EventID+":"+model.EventOutboxStatusReplaying)

	claimed := *row
	return &claimed, nil
}

// ReleaseEventReplay is the rollback that keeps a failed replay replayable.
func (s *replayFidelityStore) ReleaseEventReplay(_ context.Context, id int64, claimToken, replayErr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recordClaimToken("release", claimToken)

	row := s.byID(id)
	if row == nil {
		return apierror.NewAPIError(
			apierror.ErrNotFound, "Event not found", fmt.Errorf("no event outbox row with id %d", id),
		)
	}

	row.Status = model.EventOutboxStatusDeadLettered
	row.ClaimToken = ""
	if replayErr != "" {
		row.LastError = replayErr
	}
	row.LockedUntil = nil
	// dlt_topic and failure_metadata are RETAINED, exactly as the SQL retains them: a
	// failed replay must leave the event exactly as replayable as it was before.
	s.transitions = append(s.transitions, row.EventID+":"+model.EventOutboxStatusDeadLettered)

	return nil
}

// newReplayFidelityStore returns an empty store.
func newReplayFidelityStore() *replayFidelityStore {
	return &replayFidelityStore{rows: make(map[string]*model.EventOutbox)}
}

// put inserts or replaces a row. The stored value is a copy, so a caller mutating its
// own row afterwards cannot silently change what the store holds.
func (s *replayFidelityStore) put(row model.EventOutbox) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.rows[row.EventID]; !exists {
		s.order = append(s.order, row.EventID)
	}
	stored := row
	s.rows[row.EventID] = &stored
}

// snapshot returns a copy of the row with that event id, and whether it exists.
func (s *replayFidelityStore) snapshot(eventID string) (model.EventOutbox, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	row, ok := s.rows[eventID]
	if !ok {
		return model.EventOutbox{}, false
	}

	return *row, true
}

// appliedTransitions returns the recorded transition log.
func (s *replayFidelityStore) appliedTransitions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, len(s.transitions))
	copy(out, s.transitions)

	return out
}

// byID finds a row by its surrogate primary key. The service's two write transitions
// address rows by id, whereas the read addresses them by event id, so the store has to
// support both.
func (s *replayFidelityStore) byID(id int64) *model.EventOutbox {
	for _, eventID := range s.order {
		if row := s.rows[eventID]; row != nil && row.ID == id {
			return row
		}
	}

	return nil
}

// GetEventByID returns the row with that event id, or a typed not-found error.
func (s *replayFidelityStore) GetEventByID(_ context.Context, eventID string) (*model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.getErr != nil {
		return nil, s.getErr
	}

	row, ok := s.rows[eventID]
	if !ok {
		// The legacy ErrNotFound code, which is what database/event_outbox.go
		// constructs. isNotFoundError normalises it, and using the same code here is
		// what makes this double exercise that normalisation rather than bypass it.
		return nil, apierror.NewAPIError(apierror.ErrNotFound, "Event not found", errors.New("no rows in result set"))
	}

	found := *row

	return &found, nil
}

// inventoryLocked was a second narrowing on this same store. It is RETIRED in favour of
// matchingLocked below, which every listing and count on this double already calls.
//
// The two took THE SAME PARAMETER: model.DeadLetterFilter is a type alias for
// model.DeadLetterQuery (model/event.go), so this was not a narrower or a wider shape, it was
// another name for one. matchingLocked is also the faithful one of the pair. It trims the three
// string predicates and it constrains the population to the two terminal failure states when no
// status is named — both of which deadLetterFilterClause does in SQL, and neither of which this
// one and its dltFilterMatches matcher did. The claim in the comment it carried, that a
// zero-valued filter selects the terminal states, was true of matchingLocked and not of itself.
//
// ListDeadLetterInventory pages the same matching rows as the NARROW projection the operator
// listing reads, keyed by cursor. The replay path itself reads full rows through
// ListDeadLetteredEvents above — this exists so the store satisfies the whole seam, and so a
// listing taken before a replay is drawn from the same inventory the replay mutates.
func (s *replayFidelityStore) ListDeadLetterInventory(
	_ context.Context,
	query model.DeadLetterInventoryQuery,
) (model.DeadLetterInventoryPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	matches := s.matchingLocked(model.DeadLetterQuery{
		EventType:    query.EventType,
		Topic:        query.Topic,
		Status:       query.Status,
		OccurredFrom: query.OccurredFrom,
		OccurredTo:   query.OccurredTo,
	})

	limit := query.Limit
	if limit <= 0 {
		limit = len(matches)
	}

	page := model.DeadLetterInventoryPage{Entries: make([]model.DeadLetterInventoryEntry, 0, limit)}
	passedCursor := query.Cursor == nil

	for i := range matches {
		if !passedCursor {
			if matches[i].OccurredAt.Equal(query.Cursor.OccurredAt) && matches[i].ID == query.Cursor.ID {
				passedCursor = true
			}

			continue
		}

		if len(page.Entries) == limit {
			page.HasMore = true

			break
		}

		row := matches[i]
		page.Entries = append(page.Entries, model.DeadLetterInventoryEntry{
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
		})
	}

	if page.HasMore && len(page.Entries) > 0 {
		last := page.Entries[len(page.Entries)-1]
		page.NextCursor = &model.DeadLetterCursor{OccurredAt: last.OccurredAt, ID: last.ID}
	}

	return page, nil
}

// ListDeadLetteredEvents pages the two terminal failure states, newest first.
func (s *replayFidelityStore) ListDeadLetteredEvents(
	_ context.Context,
	query model.DeadLetterQuery,
) ([]model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inventory := s.matchingLocked(query)

	offset := query.Offset
	if offset < 0 {
		offset = 0
	}
	if offset >= len(inventory) {
		return []model.EventOutbox{}, nil
	}
	inventory = inventory[offset:]
	if query.Limit > 0 && query.Limit < len(inventory) {
		inventory = inventory[:query.Limit]
	}

	return inventory, nil
}

// CountDeadLetteredEvents counts what the same narrowing matches, ignoring the page. It
// shares matchingLocked with the listing so the two cannot describe different sets.
func (s *replayFidelityStore) CountDeadLetteredEvents(
	_ context.Context,
	query model.DeadLetterQuery,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return int64(len(s.matchingLocked(query))), nil
}

// matchingLocked returns the inventory rows the query admits, newest first. The caller
// must hold the mutex.
//
// Reverse insertion order approximates the repository's "occurred_at DESC, id DESC": every
// fixture row shares one occurrence instant, so id descending is the operative clause, and
// rows are inserted in ascending id order.
func (s *replayFidelityStore) matchingLocked(query model.DeadLetterQuery) []model.EventOutbox {
	eventType := strings.TrimSpace(query.EventType)
	topic := strings.TrimSpace(query.Topic)
	status := strings.TrimSpace(query.Status)

	var inventory []model.EventOutbox
	for i := len(s.order) - 1; i >= 0; i-- {
		row := s.rows[s.order[i]]
		if row == nil {
			continue
		}
		if row.Status != model.EventOutboxStatusDeadLettered &&
			row.Status != model.EventOutboxStatusFailed {
			continue
		}
		if eventType != "" && row.EventType != eventType {
			continue
		}
		if topic != "" && row.Topic != topic {
			continue
		}
		if status != "" && row.Status != status {
			continue
		}
		if !query.OccurredFrom.IsZero() && row.OccurredAt.Before(query.OccurredFrom) {
			continue
		}
		if !query.OccurredTo.IsZero() && row.OccurredAt.After(query.OccurredTo) {
			continue
		}
		inventory = append(inventory, *row)
	}

	return inventory
}

// deadLetterInventoryLocked builds the terminal-failure inventory a query matches, newest
// first. The caller holds the lock.
//
// The predicate mirrors the repository's SQL exactly — case-sensitive equality on the
// stored columns, with no derivation of a missing topic — because the service now delegates
// all narrowing to the database, and a looser fake would let it pass here while the real
// query returned a different set.
func (s *replayFidelityStore) deadLetterInventoryLocked(
	query model.DeadLetterQuery,
) []model.EventOutbox {
	var inventory []model.EventOutbox
	// Reverse insertion order approximates the repository's "occurred_at DESC, id DESC",
	// as above.
	for i := len(s.order) - 1; i >= 0; i-- {
		row := s.rows[s.order[i]]
		if row == nil {
			continue
		}
		if row.Status != model.EventOutboxStatusDeadLettered &&
			row.Status != model.EventOutboxStatusFailed {
			continue
		}
		if query.Status != "" && row.Status != query.Status {
			continue
		}
		if query.EventType != "" && row.EventType != query.EventType {
			continue
		}
		if query.Topic != "" && row.Topic != query.Topic {
			continue
		}
		if !query.OccurredFrom.IsZero() && row.OccurredAt.Before(query.OccurredFrom) {
			continue
		}
		if !query.OccurredTo.IsZero() && row.OccurredAt.After(query.OccurredTo) {
			continue
		}

		inventory = append(inventory, *row)
	}

	return inventory
}

// OldestDeadLetterAgeByTopic groups the inventory by dead-letter topic, as the repository's
// aggregate does (PERF-P07).
func (s *replayFidelityStore) OldestDeadLetterAgeByTopic(
	_ context.Context,
	deadLetterSuffix string,
) ([]model.DeadLetterTopicAge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	grouped := make(map[string]model.DeadLetterTopicAge)
	for _, row := range s.rows {
		if row == nil {
			continue
		}
		if row.Status != model.EventOutboxStatusDeadLettered &&
			row.Status != model.EventOutboxStatusFailed {
			continue
		}

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

// CountDeadLetterInventory counts matches, ignoring the page.
func (s *replayFidelityStore) CountDeadLetterInventory(
	_ context.Context,
	query model.DeadLetterQuery,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return int64(len(s.deadLetterInventoryLocked(query))), nil
}

// ListAndCountDeadLetterInventory answers the page and the total from one observation of the
// double's state, under a single lock acquisition, matching the repository's single snapshot.
func (s *replayFidelityStore) ListAndCountDeadLetterInventory(
	ctx context.Context,
	query model.DeadLetterInventoryQuery,
) (model.DeadLetterInventoryPage, int64, error) {
	page, err := s.ListDeadLetterInventory(ctx, query)
	if err != nil {
		return model.DeadLetterInventoryPage{}, 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return page, int64(len(s.deadLetterInventoryLocked(query.FilterQuery()))), nil
}

// The map-shaped CountDeadLetteredEvents this double also carried is GONE.
//
// It answered "how many rows per terminal status inside this occurrence window" from a
// positional pair of bounds. The repository interface expresses exactly that through
// model.DeadLetterQuery — whose OccurredFrom/OccurredTo are bound as SQL index conditions
// and whose Status narrows to one state — so the double declared two methods of the same
// name for one capability and the package stopped compiling. The DeadLetterQuery form
// above is the one the interface declares and the one every caller uses.

// CountEventOutboxByStatus returns a status-keyed count of every row. A status with no
// rows is absent from the map, matching the repository's GROUP BY semantics.
func (s *replayFidelityStore) CountEventOutboxByStatus(_ context.Context, _ time.Time) (map[string]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	counts := make(map[string]int64)
	for _, row := range s.rows {
		counts[row.Status]++
	}

	return counts, nil
}

// MarkEventDeadLettered applies the dead-letter transition.
func (s *replayFidelityStore) MarkEventDeadLettered(
	_ context.Context,
	id int64,
	claimToken, dltTopic string,
	failureMetadata json.RawMessage,
	record model.BrokerRecord,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recordClaimToken("dead_lettered", claimToken)

	row := s.byID(id)
	if row == nil {
		return apierror.NewAPIError(
			apierror.ErrNotFound, "Event not found", fmt.Errorf("no event outbox row with id %d", id),
		)
	}

	row.Status = model.EventOutboxStatusDeadLettered
	row.DLTTopic = dltTopic
	// Copied rather than aliased, so a caller that reuses its metadata buffer cannot
	// retroactively change what the row holds.
	if len(failureMetadata) > 0 {
		stored := make(json.RawMessage, len(failureMetadata))
		copy(stored, failureMetadata)
		row.FailureMetadata = stored
	}
	row.LockedUntil = nil
	s.transitions = append(s.transitions, row.EventID+":"+model.EventOutboxStatusDeadLettered)

	return nil
}

// MarkEventDispatched applies the post-replay transition.
func (s *replayFidelityStore) MarkEventDispatched(
	_ context.Context,
	id int64,
	claimToken string,
	_ model.BrokerRecord,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recordClaimToken("dispatched", claimToken)

	if s.markDispatchedErr != nil {
		return s.markDispatchedErr
	}

	row := s.byID(id)
	if row == nil {
		return apierror.NewAPIError(
			apierror.ErrNotFound, "Event not found", fmt.Errorf("no event outbox row with id %d", id),
		)
	}

	row.Status = model.EventOutboxStatusDispatched
	dispatchedAt := replayFidelityReplayAt
	row.DispatchedAt = &dispatchedAt
	row.LockedUntil = nil
	// dlt_topic and failure_metadata are NOT cleared. The SQL retains them, and this
	// double must too: retention is what makes a second replay fail closed on the
	// "already replayed" branch and what keeps the failure history readable.
	s.transitions = append(s.transitions, row.EventID+":"+model.EventOutboxStatusDispatched)

	return nil
}

// Compile-time proof that the double is a faithful implementation of the seam. It
// fails the build here, on the line that states the contract, rather than at a call
// site.
var _ eventDeadLetterStore = (*replayFidelityStore)(nil)

// replayFidelityCapture is one message a double observed: where it was going, how it
// was keyed, and — the whole point of this file — the exact bytes.
type replayFidelityCapture struct {
	// topic is the destination the write targeted.
	topic string
	// key is the message key as a string. The empty string means the message was
	// written with no key at all.
	key string
	// value is the message value, captured by reference to the slice the code under
	// test produced. Nothing here mutates it.
	value []byte
	// attempt is the attempt label the publish carried, which distinguishes an
	// original publish (1) from a replay (past the exhausted budget).
	attempt int
	// failed reports whether the double was programmed to reject this write. The bytes
	// are captured either way, which is what lets the ORIGINAL bytes be taken from the
	// very first failing attempt rather than reconstructed afterwards.
	failed bool
}

// replayFidelityPublisher is a recording TopicEventPublisher.
type replayFidelityPublisher struct {
	mu sync.Mutex

	// captured holds every publish attempt in order, successes and failures alike.
	captured []replayFidelityCapture

	// failure, when set, makes every PublishToTopic fail with it, wrapped in a
	// PublishError classified transient — exactly the shape the Kafka publisher
	// produces for a recoverable broker failure.
	failure error

	// closed records that Close was called, so a test can assert lifecycle without
	// reaching into the service.
	closed bool
}

// newReplayFidelityPublisher returns a publisher that accepts every write.
func newReplayFidelityPublisher() *replayFidelityPublisher {
	return &replayFidelityPublisher{}
}

// failWith makes every subsequent publish fail with cause.
func (p *replayFidelityPublisher) failWith(cause error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.failure = cause
}

// succeed clears any programmed failure.
func (p *replayFidelityPublisher) succeed() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.failure = nil
}

// captures returns the recorded attempts, in order.
func (p *replayFidelityPublisher) captures() []replayFidelityCapture {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]replayFidelityCapture, len(p.captured))
	copy(out, p.captured)

	return out
}

// successes returns only the attempts the publisher accepted, which for this file's
// scenario is exactly the replays.
func (p *replayFidelityPublisher) successes() []replayFidelityCapture {
	var out []replayFidelityCapture
	for _, capture := range p.captures() {
		if !capture.failed {
			out = append(out, capture)
		}
	}

	return out
}

// Publish satisfies the mandated single-argument contract by delegating, so the double
// cannot drift from its own richer method.
func (p *replayFidelityPublisher) Publish(ctx context.Context, event model.LedgerEvent) error {
	_, err := p.PublishToTopic(ctx, PublishRequest{Event: event})

	return err
}

// PublishToTopic resolves the message value exactly as the Kafka publisher does, records
// what would have gone on the wire, and then reports success or the programmed failure.
//
// It calls the production resolveEventValue rather than serialising req.Event, and that
// distinction is the difference between this file proving something and proving nothing. The
// real publisher prefers the STORED envelope on the request and composes only when there is
// none; a double that always composed would report composed bytes for every attempt, so a
// regression that ignored the stored column would be invisible here — every comparison would
// still hold, between two rebuilt values.
func (p *replayFidelityPublisher) PublishToTopic(
	_ context.Context,
	req PublishRequest,
) (PublishResult, error) {
	result := PublishResult{
		EventID:      req.Event.EventID,
		EventType:    req.Event.EventType,
		Topic:        resolveTopic(req),
		PartitionKey: resolvePartitionKey(req),
		Attempt:      resolveAttempt(req),
	}

	value, err := resolveEventValue(req)
	if err != nil {
		result.Status = model.PublishStatusRetrying
		result.Err = &PublishError{
			Topic: result.Topic, EventID: result.EventID, EventType: result.EventType,
			Attempt: result.Attempt, Transient: false, Err: err,
		}

		return result, result.Err
	}

	p.mu.Lock()
	failure := p.failure
	p.captured = append(p.captured, replayFidelityCapture{
		topic:   result.Topic,
		key:     result.PartitionKey,
		value:   value,
		attempt: result.Attempt,
		failed:  failure != nil,
	})
	p.mu.Unlock()

	if failure != nil {
		result.Status = model.PublishStatusRetrying
		result.Transient = true
		result.Err = &PublishError{
			Topic: result.Topic, EventID: result.EventID, EventType: result.EventType,
			Attempt: result.Attempt, Transient: true, Err: failure,
		}

		return result, result.Err
	}

	result.Status = model.PublishStatusDispatched

	return result, nil
}

// Close records the call and always succeeds.
func (p *replayFidelityPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true

	return nil
}

// Compile-time proof that the double satisfies the full publisher contract.
var _ TopicEventPublisher = (*replayFidelityPublisher)(nil)

// replayFidelityWriter is a recording deadLetterMessageWriter: the one raw Kafka write
// the dead-letter path performs lands here.
type replayFidelityWriter struct {
	mu sync.Mutex

	// topic is the topic this writer was resolved for.
	topic string

	// messages holds every message written, in order.
	messages []kafka.Message

	// writeErr, when set, fails every write.
	writeErr error
}

// WriteMessages records the messages and reports the programmed outcome.
func (w *replayFidelityWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writeErr != nil {
		return w.writeErr
	}
	w.messages = append(w.messages, msgs...)

	return nil
}

// written returns the recorded messages, in order.
func (w *replayFidelityWriter) written() []kafka.Message {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := make([]kafka.Message, len(w.messages))
	copy(out, w.messages)

	return out
}

// Compile-time proof that the double satisfies the dead-letter write seam.
var _ deadLetterMessageWriter = (*replayFidelityWriter)(nil)

// Harness
// ---------------------------------------------------------------------------

// replayFidelityConfiguration returns a configuration in which event publishing is
// enabled by the Kafka transport alone.
func replayFidelityConfiguration() *config.Configuration {
	return &config.Configuration{
		Kafka: config.KafkaConfig{Brokers: []string{"localhost:9092"}},
		Relay: config.RelayConfig{MaxRetryAttempts: replayFidelityMaxAttempts},
	}
}

// replayFidelityStoreConfiguration publishes cnf to config.ConfigStore and restores
// whatever was there before once the test finishes.
func replayFidelityStoreConfiguration(t *testing.T, cnf *config.Configuration) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		// Nothing was published before this test. Leaving this test's configuration in
		// place could influence later ones, so publish an empty configuration, which is
		// strictly closer to the original state.
		config.ConfigStore.Store(&config.Configuration{})
	})

	config.ConfigStore.Store(cnf)
}

// replayFidelityHarness wires a dead-letter service to the recording doubles and owns
// their lifetimes.
type replayFidelityHarness struct {
	// store is the in-memory repository.
	store *replayFidelityStore
	// publisher records replays and the failing original attempts.
	publisher *replayFidelityPublisher
	// clock is the service's settable clock.
	clock *movableTestClock
	// service is the system under test.
	service *EventDeadLetterService

	// mu guards the writer pool and the resolution log.
	mu sync.Mutex
	// writers is one recording writer per dead-letter topic, mirroring the real
	// publisher's per-topic writer pool.
	writers map[string]*replayFidelityWriter
	// resolvedTopics records every topic a writer was asked for, in order, so a test
	// can assert that the dead-letter write asked for the `.dlt` sibling and nothing
	// else.
	resolvedTopics []string
	// resolveErr, when set, fails writer resolution — the "no usable transport" arm.
	resolveErr error
	// suppressWriter, when set, makes resolution return (nil, nil): the documented
	// "this deployment has no Kafka at all" state, which is not an error.
	suppressWriter bool
}

// newReplayFidelityHarness returns a harness with the clock at the dead-letter instant
// and a service ready to use.
func newReplayFidelityHarness(t *testing.T) *replayFidelityHarness {
	t.Helper()

	replayFidelityStoreConfiguration(t, replayFidelityConfiguration())

	harness := &replayFidelityHarness{
		store:     newReplayFidelityStore(),
		publisher: newReplayFidelityPublisher(),
		clock:     &movableTestClock{now: replayFidelityDeadLetterAt},
		writers:   make(map[string]*replayFidelityWriter),
	}

	service := NewEventDeadLetterService(harness.store, nil)
	service.now = harness.clock.Now
	service.withTransport(harness.publisher, harness.resolveWriter)
	harness.service = service

	t.Cleanup(func() {
		// Close must not close a publisher it does not own, so this asserts the
		// ownership rule as a side effect: the installed publisher stays open.
		require.NoError(t, service.Close())
		assert.False(t, harness.publisher.closed,
			"an installed publisher belongs to its owner and must not be closed by the service")
	})

	return harness
}

// resolveWriter is the dead-letter writer resolver installed on the service.
func (h *replayFidelityHarness) resolveWriter(topic string) (deadLetterMessageWriter, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.resolvedTopics = append(h.resolvedTopics, topic)

	if h.resolveErr != nil {
		return nil, h.resolveErr
	}
	if h.suppressWriter {
		return nil, nil
	}

	writer, ok := h.writers[topic]
	if !ok {
		writer = &replayFidelityWriter{topic: topic}
		h.writers[topic] = writer
	}

	return writer, nil
}

// writerFor returns the recording writer for a dead-letter topic, or nil when none was
// ever resolved.
func (h *replayFidelityHarness) writerFor(topic string) *replayFidelityWriter {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.writers[topic]
}

// resolutions returns the topics a writer was requested for, in order.
func (h *replayFidelityHarness) resolutions() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]string, len(h.resolvedTopics))
	copy(out, h.resolvedTopics)

	return out
}

// exhaustRetryBudget drives the row through its whole retry budget against a failing
// broker and returns the row as the relay would hold it at exhaustion.
func (h *replayFidelityHarness) exhaustRetryBudget(
	t *testing.T,
	row model.EventOutbox,
) (model.EventOutbox, error) {
	t.Helper()

	h.publisher.failWith(errReplayFidelityBroker)

	var lastErr error
	for attempt := 1; attempt <= row.MaxAttempts; attempt++ {
		result, err := h.publisher.PublishToTopic(
			context.Background(), PublishRequestFromOutbox(row, attempt),
		)
		require.Error(t, err, "attempt %d must fail while the broker is unavailable", attempt)
		require.Equal(t, model.PublishStatusRetrying, result.Status,
			"a failed attempt inside the budget reports retrying")
		require.True(t, IsTransientPublishError(err),
			"a broken connection is a transient failure and must be classified as one")

		row.Attempts = attempt
		row.LastError = err.Error()
		if attempt == 1 {
			first := replayFidelityFirstAttemptAt
			row.FirstAttemptedAt = &first
		}
		last := replayFidelityLastAttemptAt
		row.LastAttemptedAt = &last
		lastErr = err
	}

	require.Len(t, h.publisher.captures(), row.MaxAttempts,
		"every attempt in the budget must have been offered to the transport")

	return row, lastErr
}

// Byte-level assertion helpers

// replayFidelityTopLevelKeys returns the top-level member names of a JSON object IN
// DOCUMENT ORDER.
func replayFidelityTopLevelKeys(t *testing.T, raw []byte) []string {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(raw))

	opening, err := decoder.Token()
	require.NoError(t, err, "the value must be a readable JSON document")
	require.Equal(t, json.Delim('{'), opening, "the value must be a JSON object")

	var keys []string
	for decoder.More() {
		token, tokenErr := decoder.Token()
		require.NoError(t, tokenErr)
		name, isName := token.(string)
		require.True(t, isName, "expected a member name, got %v", token)
		keys = append(keys, name)

		var discard json.RawMessage
		require.NoError(t, decoder.Decode(&discard), "member %q must hold a valid value", name)
	}

	return keys
}

// replayFidelityDecodeEnvelope decodes an event envelope so a single field's VALUE can
// be asserted. It is never used to establish byte equality.
func replayFidelityDecodeEnvelope(t *testing.T, raw []byte) model.LedgerEvent {
	t.Helper()

	var event model.LedgerEvent
	require.NoError(t, json.Unmarshal(raw, &event), "the envelope must decode")

	return event
}

// replayFidelityCompact returns the JSON with insignificant whitespace removed, which
// is what any re-marshal of a json.RawMessage does.
func replayFidelityCompact(t *testing.T, raw []byte) []byte {
	t.Helper()

	var compacted bytes.Buffer
	require.NoError(t, json.Compact(&compacted, raw))

	return compacted.Bytes()
}

// replayFidelityMapRoundTrip returns the JSON after a map[string]interface{} round
// trip, which reorders members and renormalises number literals.
func replayFidelityMapRoundTrip(t *testing.T, raw []byte) []byte {
	t.Helper()

	var document map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &document))
	encoded, err := json.Marshal(document)
	require.NoError(t, err)

	return encoded
}

// replayFidelityEnvelopeRoundTrip returns the envelope after a model.LedgerEvent struct
// round trip — the most innocent-looking regression available, and the reason the
// fixtures carry insignificant whitespace.
func replayFidelityEnvelopeRoundTrip(t *testing.T, raw []byte) []byte {
	t.Helper()

	var event model.LedgerEvent
	require.NoError(t, json.Unmarshal(raw, &event))
	encoded, err := json.Marshal(event)
	require.NoError(t, err)

	return encoded
}

// replayFidelityWebhookRoundTrip returns the payload after a NewWebhook round trip.
func replayFidelityWebhookRoundTrip(t *testing.T, raw []byte) []byte {
	t.Helper()

	var body NewWebhook
	require.NoError(t, json.Unmarshal(raw, &body))
	encoded, err := json.Marshal(body)
	require.NoError(t, err)

	return encoded
}

// replayFidelityModelledBody stands in for ANY struct-typed view of the webhook body.
type replayFidelityModelledBody struct {
	Event string   `json:"event"`
	Data  struct{} `json:"data"`
}

// replayFidelityTypedRoundTrip returns the payload after a struct-typed round trip, in
// which every member the struct does not declare is silently dropped.
func replayFidelityTypedRoundTrip(t *testing.T, raw []byte) []byte {
	t.Helper()

	var body replayFidelityModelledBody
	require.NoError(t, json.Unmarshal(raw, &body))
	encoded, err := json.Marshal(body)
	require.NoError(t, err)

	return encoded
}

// replayFidelityAPICode extracts the CANONICAL typed error code from an error, failing
// the test when the error is not a typed apierror.
func replayFidelityAPICode(t *testing.T, err error) apierror.ErrorCode {
	t.Helper()

	require.Error(t, err)

	var value apierror.APIError
	if errors.As(err, &value) {
		return apierror.Normalize(value.Code)
	}

	var pointer *apierror.APIError
	if errors.As(err, &pointer) && pointer != nil {
		return apierror.Normalize(pointer.Code)
	}

	t.Fatalf("expected a typed apierror, got %T: %v", err, err)

	return ""
}

// The scenario
// ---------------------------------------------------------------------------

// replayFidelityScenario is everything one completed dead-letter-and-replay run
// produced, so the assertions read as statements about captured evidence rather than as
// a re-run of the scenario.
type replayFidelityScenario struct {
	// fixture is the event this run used.
	fixture replayFidelityFixture

	// row is the row as the relay held it at exhaustion: budget spent, failure reason
	// recorded, attempt window stamped.
	row model.EventOutbox

	// originalBytes is the message value as it WOULD HAVE BEEN PUBLISHED, captured
	// from the very first failing attempt. It is captured rather than recomputed:
	// recomputing it from the row with the same serialiser the implementation uses
	// would assume the equality under test.
	originalBytes []byte

	// outcome is the dead-letter publication record.
	outcome DeadLetterOutcome

	// deadLetterBytes is the message value the dead-letter writer actually received.
	deadLetterBytes []byte

	// replay is the replay record.
	replay ReplayOutcome

	// replayedBytes is the message value the replay published.
	replayedBytes []byte

	// storedRow is the row as it stood after dead-lettering and before the replay.
	storedRow model.EventOutbox
}

// runScenario takes one fixture from pending row to replayed event and returns the
// evidence.
func (h *replayFidelityHarness) runScenario(
	t *testing.T,
	fixture replayFidelityFixture,
	id int64,
) replayFidelityScenario {
	t.Helper()

	ctx := context.Background()

	// 1. A pending row, exactly as it was committed alongside the ledger mutation.
	row := fixture.row(t, id)
	require.Equal(t, model.EventOutboxStatusPending, row.Status,
		"the scenario must start from a pending row")
	h.store.put(row)

	// 2. Spend the whole retry budget against a broker that will not accept the write.
	exhausted, lastErr := h.exhaustRetryBudget(t, row)
	require.Error(t, lastErr)
	h.store.put(exhausted)

	// 3. The original bytes, taken from the attempts themselves. Every attempt must
	//    have offered identical bytes: the retry bookkeeping touches the attempt
	//    counter, the error and the attempt window, none of which is part of the
	//    envelope, so an attempt whose bytes differed would mean relay state had leaked
	//    into the message.
	attempts := h.publisher.captures()
	require.Len(t, attempts, replayFidelityMaxAttempts)
	originalBytes := attempts[0].value
	for i, attempt := range attempts {
		require.Equal(t, string(originalBytes), string(attempt.value),
			"attempt %d offered different bytes from attempt 1", i+1)
		require.Equal(t, fixture.topic, attempt.topic)
		require.Equal(t, fixture.partitionKey, attempt.key)
		require.True(t, attempt.failed)
	}

	// 4. Dead-letter the exhausted row.
	outcome, err := h.service.DeadLetter(ctx, exhausted, lastErr)
	require.NoError(t, err, "dead-lettering an exhausted row must succeed")
	require.True(t, outcome.Published, "the recording writer accepts the dead-letter write")

	writer := h.writerFor(fixture.deadLetterTopic)
	require.NotNil(t, writer, "a writer must have been resolved for %s", fixture.deadLetterTopic)
	written := writer.written()
	require.Len(t, written, 1, "exactly one dead-letter message must have been written")

	storedRow, ok := h.store.snapshot(exhausted.EventID)
	require.True(t, ok, "the dead-lettered row must still exist")
	require.Equal(t, model.EventOutboxStatusDeadLettered, storedRow.Status)

	// 5. The broker recovers and an operator replays the event a day later.
	h.publisher.succeed()
	h.clock.Set(replayFidelityReplayAt)

	replay, err := h.service.ReplayDeadLetteredEvent(ctx, storedRow.EventID)
	require.NoError(t, err, "replaying a dead-lettered event must succeed")

	replayed := h.publisher.successes()
	require.Len(t, replayed, 1, "the replay must be the one and only accepted publish")

	return replayFidelityScenario{
		fixture:         fixture,
		row:             exhausted,
		originalBytes:   originalBytes,
		outcome:         outcome,
		deadLetterBytes: written[0].Value,
		replay:          replay,
		replayedBytes:   replayed[0].value,
		storedRow:       storedRow,
	}
}

// The anti-vacuity guard
// ---------------------------------------------------------------------------

// TestReplayFidelityFixtures_AgreeWithProductionRouting ties the literal destinations above
// back to the routing they are meant to describe.
//
// Every other test in this file takes the destination FROM the fixture: the row is built with
// `Topic: f.topic`, and the dead-letter assertions compare against `f.deadLetterTopic`. That is
// deliberate — a literal is what stops those assertions being an echo of the implementation —
// but it also means a fixture can name a topic no producer would ever record and every test
// here still passes. It happened: the ledger fixture carried "blnk.system" for as long as
// ledger.created routed there, and kept carrying it after the event moved to its own grantable
// category, describing a row the relay cannot produce.
//
// This is the ONE test that closes the loop, and it is the only place in the file that is
// allowed to consult TopicForEvent. A stale fixture now fails here instead of quietly
// certifying replay fidelity for a route that does not exist.
func TestReplayFidelityFixtures_AgreeWithProductionRouting(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	for _, fixture := range replayFidelityFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			assert.Equal(t, TopicForEvent(fixture.eventType), fixture.topic,
				"%s routes to %s, so a fixture naming %s describes a row no producer creates",
				fixture.eventType, TopicForEvent(fixture.eventType), fixture.topic)
			assert.Equal(t, DeadLetterTopicForEvent(fixture.eventType), fixture.deadLetterTopic,
				"the dead-letter sibling must be the one the event's own category resolves to")
			assert.Equal(t, DLTFor(fixture.topic), fixture.deadLetterTopic,
				"and it must be the declared topic's sibling, so the pair cannot be half-updated")
		})
	}
}

// TestReplayFidelity_FixturesWouldExposeAReMarshal proves the fixtures can tell a
// correct implementation from a broken one.
func TestReplayFidelity_FixturesWouldExposeAReMarshal(t *testing.T) {
	for _, fixture := range replayFidelityFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			payload := []byte(fixture.payload)

			require.True(t, json.Valid(payload), "the fixture payload must be valid JSON")

			// 1. Insignificant whitespace. This is the property that catches the most
			//    innocent-looking regression of all — re-marshalling the LedgerEvent —
			//    because a json.RawMessage is compacted on its way back out.
			assert.NotEqual(t, string(payload), string(replayFidelityCompact(t, payload)),
				"the fixture must carry insignificant whitespace, or compaction becomes undetectable; rebuild it")

			// 2. Member order and number literals, via a map round trip.
			mapped := replayFidelityMapRoundTrip(t, payload)
			assert.NotEqual(t, string(payload), string(mapped),
				"a map round trip must visibly change the fixture; rebuild it")

			// 3. The codebase's own webhook body type is not a safe staging post
			//    either: its payload is an interface{}, so the inner object becomes a
			//    map and is reordered and renormalised on the way back out.
			assert.NotEqual(t, string(payload), string(replayFidelityWebhookRoundTrip(t, payload)),
				"a NewWebhook round trip must visibly change the fixture; rebuild it")

			// 4. Members no Go struct declares are dropped SILENTLY by any struct-typed
			//    view — no error, no warning. This is the failure mode that makes a
			//    decoded-document comparison useless and a byte comparison necessary.
			require.Contains(t, fixture.payload, `"unmodelled_extension"`,
				"the fixture must carry a member no Go struct in this repository declares")
			typed := replayFidelityTypedRoundTrip(t, payload)
			assert.NotEqual(t, string(payload), string(typed),
				"a struct-typed round trip must visibly change the fixture; rebuild it")
			assert.NotContains(t, string(typed), `"unmodelled_extension"`,
				"a struct-typed view is expected to DROP the unmodelled member, which is exactly the silent failure this file guards against")

			// The specific renormalisations, pinned individually so a fixture edit that
			// removed one of them is reported precisely rather than as a vague
			// "something changed".
			assert.Contains(t, fixture.payload, "9007199254740993",
				"the fixture must carry 2^53+1, which float64 cannot represent")
			assert.NotContains(t, string(mapped), "9007199254740993",
				"2^53+1 must be visibly lost through a float64 round trip")
			assert.Contains(t, string(mapped), "e+23",
				"the 1e23-scale integer must be visibly re-rendered in exponent notation")
			assert.Contains(t, fixture.payload, "+00:00",
				"the fixture must carry a timestamp string Go would re-render as ...Z")
		})
	}
}

// TestReplayFidelity_EnvelopeWouldExposeAReMarshal proves the ENVELOPE is discriminating
// too, not just the payload inside it.
func TestReplayFidelity_EnvelopeWouldExposeAReMarshal(t *testing.T) {
	harness := newReplayFidelityHarness(t)
	fixture := replayFidelityFixtures()[0]

	row := fixture.row(t, 1)
	envelope, err := marshalLedgerEvent(PublishRequestFromOutbox(row, 1).Event)
	require.NoError(t, err)

	assert.NotEqual(t, string(envelope), string(replayFidelityMapRoundTrip(t, envelope)),
		"a map round trip of the envelope must reorder its members visibly")
	assert.NotEqual(t, string(envelope), string(replayFidelityEnvelopeRoundTrip(t, envelope)),
		"a model.LedgerEvent round trip of the envelope must visibly change it; without this the most likely regression is undetectable")

	assert.Equal(t,
		[]string{"event_id", "event_type", "aggregate_id", "occurred_at", "payload", "schema_version"},
		replayFidelityTopLevelKeys(t, envelope),
		"the envelope member order is the subscriber-facing contract")

	// The harness is used only so the configured topic prefix is in place while the
	// envelope is built; nothing is published here.
	assert.Empty(t, harness.publisher.captures(), "this test publishes nothing")
}

// TestReplayFidelity_PublishesTheStoredEnvelopeRatherThanARebuild is the guard that keeps
// every other byte-equality assertion in this file from being able to pass vacuously.
//
// # Why the rest of the file is not enough on its own
//
// Every other comparison here holds between two values the implementation produced. If the
// original publish, the dead-letter copy and the replay each REBUILT the envelope from the
// row's columns, all three would agree — the serialiser is deterministic — and every
// assertion would pass while the stored bytes were being ignored entirely. The criterion
// would then be satisfied only for as long as the serialiser never changes: add an envelope
// member, reorder two, or alter how an instant is rendered, and every event captured before
// that release would replay as DIFFERENT bytes from the ones its subscribers originally saw,
// with no test noticing.
//
// # How this test separates the two
//
// It stores an envelope the serialiser DEMONSTRABLY COULD NOT HAVE PRODUCED — members in a
// different order, insignificant whitespace, and one member no version of model.LedgerEvent
// declares — and then asserts that exact byte sequence is what every transport carries. A
// rebuild cannot reproduce it, so an implementation that rebuilt would fail here and only
// here.
//
// The unmodelled member is the sharpest part of the fixture. It stands in for an envelope
// written by a FUTURE build of Blnk and read back by this one: the value must survive
// untouched, because the stored bytes are the record of what a subscriber was actually sent,
// not a rendering this build is entitled to reinterpret.
func TestReplayFidelity_PublishesTheStoredEnvelopeRatherThanARebuild(t *testing.T) {
	harness := newReplayFidelityHarness(t)
	ctx := context.Background()
	fixture := replayFidelityFixtures()[0]

	row := fixture.row(t, 1)

	// The stored envelope: same six members with the same values, plus a seventh this build
	// knows nothing about, in an order and with a whitespace layout the canonical serialiser
	// never emits.
	storedEnvelope := []byte("{\n" +
		`  "schema_version": ` + strconv.Itoa(model.SchemaVersionV1) + ",\n" +
		`  "event_id": "` + row.EventID + "\",\n" +
		`  "event_type": "` + fixture.eventType + "\",\n" +
		`  "aggregate_id": "` + fixture.aggregateID + "\",\n" +
		`  "occurred_at": "` + replayFidelityOccurredAt.Format(time.RFC3339Nano) + "\",\n" +
		`  "envelope_extension": {"written_by": "a later build of blnk"},` + "\n" +
		`  "payload": ` + fixture.payload + "\n" +
		"}")

	require.True(t, json.Valid(storedEnvelope), "the stored envelope must be valid JSON")

	// THE CONTROL. Without this the test could be satisfied by a fixture the serialiser
	// happens to reproduce, which would make the equalities below meaningless again.
	rebuilt, err := row.CanonicalEvent().CanonicalBytes()
	require.NoError(t, err)
	require.NotEqual(t, string(storedEnvelope), string(rebuilt),
		"the fixture must be UNREACHABLE by composition, or this test proves nothing; rebuild it")
	require.NotContains(t, string(rebuilt), `"envelope_extension"`,
		"a rebuild necessarily drops the unmodelled member, which is what makes it detectable")

	row.EventRaw = storedEnvelope
	harness.store.put(row)

	// The whole life of the event, from the first failing attempt to the replay a day later.
	exhausted, lastErr := harness.exhaustRetryBudget(t, row)
	require.Error(t, lastErr)
	harness.store.put(exhausted)

	outcome, err := harness.service.DeadLetter(ctx, exhausted, lastErr)
	require.NoError(t, err)
	require.True(t, outcome.Published)

	stored, ok := harness.store.snapshot(exhausted.EventID)
	require.True(t, ok)
	require.Equal(t, string(storedEnvelope), string(stored.EventRaw),
		"dead-lettering must not rewrite the stored envelope")

	harness.publisher.succeed()
	harness.clock.Set(replayFidelityReplayAt)

	replay, err := harness.service.ReplayDeadLetteredEvent(ctx, stored.EventID)
	require.NoError(t, err)
	require.Equal(t, model.PublishStatusDispatched, replay.Status)

	// --- Every transport carried the stored bytes. ---
	attempts := harness.publisher.captures()
	require.Len(t, attempts, replayFidelityMaxAttempts+1,
		"five failing attempts and one successful replay")

	for i, attempt := range attempts {
		assert.Equal(t, string(storedEnvelope), string(attempt.value),
			"publish %d carried a rebuilt envelope instead of the stored one", i+1)
		assert.NotEqual(t, string(rebuilt), string(attempt.value),
			"publish %d rebuilt the envelope from the row's columns", i+1)
	}

	written := harness.writerFor(fixture.deadLetterTopic).written()
	require.Len(t, written, 1)

	expectedDeadLetter := string(storedEnvelope[:len(storedEnvelope)-1]) +
		`,"failure_metadata":` + string(outcome.MetadataJSON) + `}`
	assert.Equal(t, expectedDeadLetter, string(written[0].Value),
		"the dead-letter copy must be the STORED envelope plus one additive member")

	recovered, err := StripFailureMetadata(written[0].Value)
	require.NoError(t, err)
	assert.Equal(t, string(storedEnvelope), string(recovered),
		"stripping the metadata must recover the stored envelope, unmodelled member and all")

	// And the unmodelled member survived every hop, which is the property that makes the
	// stored value an authority rather than a cache.
	assert.Contains(t, string(attempts[len(attempts)-1].value), `"envelope_extension"`,
		"a member this build does not model must still reach the topic on replay")
}

// Criterion V-9: byte-for-byte replay, across every category
// ---------------------------------------------------------------------------

// TestReplayFidelity_ReplayedMessageIsByteIdenticalToTheOriginal is the acceptance test
// for criterion V-9.
//
//  1. The replayed bytes are byte-identical to the original bytes.
//  2. The failure metadata is the ONLY difference between the original message and the
//     dead-letter message, isolated by reconstructing the dead-letter message from the
//     original plus the metadata and by stripping the metadata back off again.
//  3. The replay went to the ORIGINAL topic, never the dead-letter topic.
//  4. The message key is unchanged, so the replay lands on the same partition.
//  5. event_id is unchanged — it is the subscriber's idempotency key.
//  6. schema_version, event_type, aggregate_id and occurred_at are unchanged, with
//     occurred_at being the original occurrence instant and not the replay instant.
func TestReplayFidelity_ReplayedMessageIsByteIdenticalToTheOriginal(t *testing.T) {
	for index, fixture := range replayFidelityFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			harness := newReplayFidelityHarness(t)
			scenario := harness.runScenario(t, fixture, int64(index+1))

			// Printed so the difference can be confirmed by eye as well as by
			// assertion. The three values are the whole evidence base for this
			// criterion.
			t.Logf("ORIGINAL     : %s", scenario.originalBytes)
			t.Logf("DEAD-LETTERED: %s", scenario.deadLetterBytes)
			t.Logf("REPLAYED     : %s", scenario.replayedBytes)

			// --- 1. Byte identity. ---
			require.Equal(t, string(scenario.originalBytes), string(scenario.replayedBytes),
				"a replayed event must match the original byte for byte")
			assert.True(t, bytes.Equal(scenario.originalBytes, scenario.replayedBytes),
				"the replayed message value must be byte-equal to the original")

			// --- 2. The failure metadata is the only difference. ---
			// Reconstructing the dead-letter message from the original plus the
			// metadata states the relationship exactly: the original envelope is a
			// byte-exact prefix and the metadata is an additive final member.
			require.NotEmpty(t, scenario.outcome.MetadataJSON)
			expectedDeadLetter := string(scenario.originalBytes[:len(scenario.originalBytes)-1]) +
				`,"failure_metadata":` + string(scenario.outcome.MetadataJSON) + `}`
			assert.Equal(t, expectedDeadLetter, string(scenario.deadLetterBytes),
				"the dead-letter message must be the original envelope plus one additive member")
			assert.Equal(t, string(scenario.deadLetterBytes), string(scenario.outcome.Message),
				"the outcome must report the bytes that were actually written")

			stripped, err := StripFailureMetadata(scenario.deadLetterBytes)
			require.NoError(t, err)
			assert.Equal(t, string(scenario.originalBytes), string(stripped),
				"stripping the failure metadata must recover the original bytes exactly")
			assert.Equal(t, string(scenario.replayedBytes), string(stripped),
				"the replayed bytes and the stripped dead-letter bytes must be the same value")

			// --- 3. Destination. ---
			assert.Equal(t, fixture.topic, scenario.replay.Topic,
				"a replay goes back to the original category topic")
			assert.Equal(t, fixture.topic, scenario.replay.Result.Topic)
			assert.False(t, IsDeadLetterTopic(scenario.replay.Topic),
				"a replay must never target a dead-letter topic")
			assert.Equal(t, fixture.deadLetterTopic, scenario.outcome.DeadLetterTopic,
				"the dead-letter topic name is a published contract")
			assert.Equal(t, fixture.topic, scenario.outcome.OriginalTopic,
				"the dead-letter counter is attributed by the original topic, not the .dlt sibling")
			assert.Equal(t, []string{fixture.deadLetterTopic}, harness.resolutions(),
				"exactly one writer must have been resolved, for the .dlt sibling")

			// --- 4. The message key. ---
			//
			// It is the row's PARTITION KEY, which is the column
			// ClaimPendingEventOutbox serialises dispatch on. Keying on anything else
			// would put the database's ordering domain and the broker's partitioning
			// domain in disagreement, and the disagreement would be silent.
			assert.Equal(t, fixture.partitionKey, scenario.replay.PartitionKey,
				"the replay must reuse the original key so it lands on the same partition")
			assert.Equal(t, fixture.partitionKey, scenario.outcome.PartitionKey,
				"the dead-letter message must reuse the original key too")
			written := harness.writerFor(fixture.deadLetterTopic).written()
			require.Len(t, written, 1)
			assert.Equal(t, fixture.partitionKey, string(written[0].Key))
			assert.Empty(t, written[0].Topic,
				"a per-topic writer rejects a message that names its own topic")

			// --- 5 and 6. Every envelope field. ---
			original := replayFidelityDecodeEnvelope(t, scenario.originalBytes)
			replayed := replayFidelityDecodeEnvelope(t, scenario.replayedBytes)

			assert.Equal(t, scenario.row.EventID, replayed.EventID,
				"event_id is the subscriber idempotency key and must survive a replay unchanged")
			assert.Equal(t, original.EventID, replayed.EventID)
			assert.Equal(t, scenario.row.EventID, scenario.replay.EventID)

			assert.Equal(t, fixture.eventType, replayed.EventType)
			assert.Equal(t, fixture.eventType, scenario.replay.EventType)
			assert.Equal(t, fixture.aggregateID, replayed.AggregateID)
			assert.Equal(t, model.SchemaVersionV1, replayed.SchemaVersion)

			assert.True(t, replayed.OccurredAt.Equal(replayFidelityOccurredAt),
				"occurred_at must be the original occurrence instant, got %s", replayed.OccurredAt)
			assert.False(t, replayed.OccurredAt.Equal(replayFidelityReplayAt),
				"occurred_at must NOT be the replay instant")
			assert.True(t, scenario.replay.ReplayedAt.Equal(replayFidelityReplayAt),
				"the replay instant belongs on the outcome, not in the envelope")

			assert.Equal(t, fixture.payload, string(replayed.Payload),
				"the payload bytes must arrive exactly as they were stored")
			assert.Equal(t,
				[]string{"event_id", "event_type", "aggregate_id", "occurred_at", "payload", "schema_version"},
				replayFidelityTopLevelKeys(t, scenario.replayedBytes),
				"the replayed envelope must keep its contractual member order")

			// The replay is labelled past the exhausted budget so it stays out of the
			// attempt="1" latency series the publish-latency target is read from.
			assert.Equal(t, replayAttemptNumber(scenario.storedRow), scenario.replay.Result.Attempt)
			assert.Greater(t, scenario.replay.Result.Attempt, 1,
				"a replay must not be recorded as a first attempt")
			assert.Equal(t, model.PublishStatusDispatched, scenario.replay.Status)
			assert.True(t, scenario.replay.Recorded,
				"a successful replay must have cleared the row's dead-lettered state")
		})
	}
}

// The failure metadata: the one thing that IS different
// ---------------------------------------------------------------------------

// TestReplayFidelity_FailureMetadataCarriesEveryDocumentedField asserts the metadata
// that constitutes the "aside from" in "byte-for-byte aside from the failure metadata".
func TestReplayFidelity_FailureMetadataCarriesEveryDocumentedField(t *testing.T) {
	for index, fixture := range replayFidelityFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			harness := newReplayFidelityHarness(t)
			scenario := harness.runScenario(t, fixture, int64(index+1))

			// The metadata stored on the row and the metadata attached to the message
			// must be the same bytes, not merely equivalent values.
			assert.Equal(t, string(scenario.outcome.MetadataJSON), string(scenario.storedRow.FailureMetadata),
				"the row's failure_metadata column and the message's failure_metadata member must be identical bytes")

			metadata, err := DecodeFailureMetadata(scenario.storedRow.FailureMetadata)
			require.NoError(t, err)
			require.NotNil(t, metadata, "a dead-lettered row must carry failure metadata")

			// 1. Original topic — the record of where the event was headed, and the
			//    value a replay recovers its destination from.
			assert.Equal(t, fixture.topic, metadata.OriginalTopic)
			assert.False(t, IsDeadLetterTopic(metadata.OriginalTopic),
				"the original topic is the category topic, never the .dlt sibling")

			// 2. Error reason — the cause from the final attempt, carried through rather
			//    than replaced with a generic message.
			assert.NotEmpty(t, metadata.ErrorReason)
			assert.Contains(t, metadata.ErrorReason, errReplayFidelityBroker.Error(),
				"the underlying broker failure must survive into the reported reason")

			// 3. Attempt count — exactly the configured maximum.
			assert.Equal(t, replayFidelityMaxAttempts, metadata.AttemptCount,
				"an exhausted row reports the configured retry budget as its attempt count")
			assert.Equal(t, scenario.row.MaxAttempts, metadata.AttemptCount)

			// 4 and 5. The attempt window, bounding how long the failure persisted.
			assert.True(t, metadata.FirstAttemptedAt.Equal(replayFidelityFirstAttemptAt))
			assert.True(t, metadata.LastAttemptedAt.Equal(replayFidelityLastAttemptAt))
			assert.False(t, metadata.LastAttemptedAt.Before(metadata.FirstAttemptedAt),
				"the window must never run backwards")
			assert.Equal(t, 31*time.Second, metadata.LastAttemptedAt.Sub(metadata.FirstAttemptedAt),
				"the window must span the whole 1s+2s+4s+8s+16s backoff schedule")

			// The metadata member order is deterministic because it is marshaled from a
			// struct, which is what makes the reconstruction assertion in the byte
			// identity test valid rather than incidental.
			assert.Equal(t,
				[]string{"original_topic", "error_reason", "attempt_count", "first_attempted_at", "last_attempted_at"},
				replayFidelityTopLevelKeys(t, scenario.outcome.MetadataJSON))

			// And the whole point: the metadata is a SIBLING of the envelope members,
			// never nested inside the payload and never replacing it.
			assert.Equal(t,
				[]string{"event_id", "event_type", "aggregate_id", "occurred_at", "payload", "schema_version", "failure_metadata"},
				replayFidelityTopLevelKeys(t, scenario.deadLetterBytes),
				"the failure metadata must be an additive final member of the envelope")
		})
	}
}

// Post-replay state, and repeat replay
// ---------------------------------------------------------------------------

// TestReplayFidelity_ReplayedRowLeavesTheDeadLetterInventory asserts the row ends in the
// state the implementation documents, and that the inventory reflects it.
func TestReplayFidelity_ReplayedRowLeavesTheDeadLetterInventory(t *testing.T) {
	harness := newReplayFidelityHarness(t)
	fixture := replayFidelityFixtures()[0]
	scenario := harness.runScenario(t, fixture, 1)

	ctx := context.Background()

	// Before the replay the entry was in the inventory.
	require.Equal(t, model.EventOutboxStatusDeadLettered, scenario.storedRow.Status)

	after, ok := harness.store.snapshot(scenario.row.EventID)
	require.True(t, ok)

	assert.Equal(t, model.EventOutboxStatusDispatched, after.Status,
		"a replayed row reaches the same terminal success state an ordinary publish does")
	require.NotNil(t, after.DispatchedAt, "dispatched_at must be stamped")
	assert.True(t, after.DispatchedAt.Equal(replayFidelityReplayAt))
	assert.Nil(t, after.LockedUntil, "the lease must be released")

	assert.Equal(t, fixture.deadLetterTopic, after.DLTTopic,
		"dlt_topic must be retained so the failure history survives the fix")
	assert.NotEmpty(t, after.FailureMetadata,
		"failure_metadata must be retained so the failure history survives the fix")
	assert.Equal(t, string(scenario.storedRow.FailureMetadata), string(after.FailureMetadata),
		"a replay must not rewrite the stored failure metadata")

	// The payload is untouched by the whole round trip, which is the durable half of the
	// byte-fidelity guarantee: the row a second replay would read is the row the first
	// one read.
	assert.Equal(t, fixture.payload, string(after.Payload),
		"the stored payload bytes must be untouched by dead-lettering and replay")

	// THREE transitions, not two, and the middle one is the point. A replay CLAIMS the row
	// — dead_lettered → replaying — before it publishes anything, which is what makes two
	// concurrent replays of one event impossible: only the request whose transition changed
	// a row proceeds. The path is therefore dead_lettered → replaying → dispatched, and any
	// path that skipped replaying would be a replay that published on a read-then-check.
	assert.Equal(t,
		[]string{
			scenario.row.EventID + ":" + model.EventOutboxStatusDeadLettered,
			scenario.row.EventID + ":" + model.EventOutboxStatusReplaying,
			scenario.row.EventID + ":" + model.EventOutboxStatusDispatched,
		},
		harness.store.appliedTransitions(),
		"the row must reach dispatched by way of dead_lettered and a replay CLAIM, and by no other path")

	// The token the claim issued is the token the success transition presented. Anything
	// else — an empty string, or one the service invented — would mean the conditional
	// update was not actually conditional on holding the row.
	assert.Equal(t, "replay-claim-token", harness.store.presentedClaimToken("dispatched"),
		"the success transition must present the token the replay claim issued")

	inventory, err := harness.service.ListDeadLetterEvents(ctx, DeadLetterListOptions{})
	require.NoError(t, err)
	for _, entry := range inventory.Entries {
		assert.NotEqual(t, scenario.row.EventID, entry.EventID,
			"a replayed event must no longer be listed as dead-lettered")
	}
	assert.Empty(t, inventory.Entries, "the only entry in this store has been replayed")

	counts, err := harness.store.CountEventOutboxByStatus(ctx, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), counts[model.EventOutboxStatusDispatched])
	assert.Zero(t, counts[model.EventOutboxStatusDeadLettered])
}

// TestReplayFidelity_RepeatReplayIsRejectedRatherThanDuplicating asserts that replaying
// the same event twice is EXPLICITLY REFUSED and never silently duplicates it.
func TestReplayFidelity_RepeatReplayIsRejectedRatherThanDuplicating(t *testing.T) {
	harness := newReplayFidelityHarness(t)
	fixture := replayFidelityFixtures()[0]
	scenario := harness.runScenario(t, fixture, 1)

	publishedAfterFirstReplay := len(harness.publisher.successes())
	require.Equal(t, 1, publishedAfterFirstReplay)

	outcome, err := harness.service.ReplayDeadLetteredEvent(context.Background(), scenario.row.EventID)

	require.Error(t, err, "a second replay of the same event must be refused")
	assert.Equal(t, apierror.ErrEventNotDeadLettered, replayFidelityAPICode(t, err))
	assert.Contains(t, err.Error(), "already been replayed",
		"the refusal must name the real situation, not merely 'not dead-lettered'")
	assert.False(t, outcome.Recorded)
	assert.Empty(t, outcome.EventID,
		"a refused replay returns no outcome to act on")

	assert.Len(t, harness.publisher.successes(), publishedAfterFirstReplay,
		"a refused replay must publish nothing at all")

	after, ok := harness.store.snapshot(scenario.row.EventID)
	require.True(t, ok)
	assert.Equal(t, model.EventOutboxStatusDispatched, after.Status,
		"a refused replay must not disturb the row")
	assert.Len(t, harness.store.appliedTransitions(), 3,
		"a refused replay must apply no FURTHER transition: the three recorded are dead_lettered, the first replay's claim, and its dispatch")
}

// TestReplayFidelity_ReplayRejectionsCarryTheirDocumentedCodeAndStatus asserts each
// refusal returns its own typed code, and that each code maps to the status it is meant
// to.
func TestReplayFidelity_ReplayRejectionsCarryTheirDocumentedCodeAndStatus(t *testing.T) {
	harness := newReplayFidelityHarness(t)
	ctx := context.Background()
	fixture := replayFidelityFixtures()[0]

	// A pending row: present, but with no dead-letter message to replay from.
	pending := fixture.row(t, 1)
	harness.store.put(pending)

	cases := []struct {
		name    string
		eventID string
		code    apierror.ErrorCode
		status  int
		because string
	}{
		{
			name:    "a blank id is a malformed request",
			eventID: "   ",
			code:    apierror.ErrGenValidation,
			status:  http.StatusBadRequest,
			because: "an id is required to identify what to replay",
		},
		{
			name:    "an unknown id is not found",
			eventID: "event-replay-fidelity-does-not-exist",
			code:    apierror.ErrEventNotFound,
			status:  http.StatusNotFound,
			because: "a wrong id must be reported as a wrong id",
		},
		{
			name:    "a pending event is not dead-lettered",
			eventID: pending.EventID,
			code:    apierror.ErrEventNotDeadLettered,
			status:  http.StatusConflict,
			because: "only a dead-lettered event has a dead-letter message to replay from",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			outcome, err := harness.service.ReplayDeadLetteredEvent(ctx, testCase.eventID)

			require.Error(t, err, testCase.because)
			assert.Equal(t, testCase.code, replayFidelityAPICode(t, err))
			assert.Equal(t, testCase.status, apierror.StatusForCode(testCase.code),
				"%s must map to %d", testCase.code, testCase.status)
			assert.NotEqual(t, http.StatusInternalServerError, apierror.StatusForCode(testCase.code),
				"%s has no statusByCode entry and is silently defaulting to 500", testCase.code)

			assert.False(t, outcome.Recorded)
			assert.Empty(t, harness.publisher.captures(),
				"a refused replay must never reach the transport")
		})
	}

	// The pending row is untouched by every refusal, which is what makes the refusals
	// safe to retry once the event has genuinely been dead-lettered.
	after, ok := harness.store.snapshot(pending.EventID)
	require.True(t, ok)
	assert.Equal(t, model.EventOutboxStatusPending, after.Status)
	assert.Empty(t, harness.store.appliedTransitions())
}

// Where the replay destination comes from
// ---------------------------------------------------------------------------

// TestReplayFidelity_ReplayTopicComesFromTheStoredFailureMetadata asserts the replay
// destination is RECOVERED FROM THE STORED METADATA rather than re-derived from the event
// type.
func TestReplayFidelity_ReplayTopicComesFromTheStoredFailureMetadata(t *testing.T) {
	harness := newReplayFidelityHarness(t)
	ctx := context.Background()

	const legacyTopic = "legacy.transactions"
	const legacyDeadLetterTopic = "legacy.transactions.dlt"

	fixture := replayFidelityFixtures()[0]
	require.NotEqual(t, legacyTopic, TopicForEvent(fixture.eventType),
		"the test is only meaningful when the recorded topic and the current mapping disagree")

	row := fixture.row(t, 1)
	row.Topic = legacyTopic
	harness.store.put(row)

	exhausted, lastErr := harness.exhaustRetryBudget(t, row)
	harness.store.put(exhausted)

	outcome, err := harness.service.DeadLetter(ctx, exhausted, lastErr)
	require.NoError(t, err)
	assert.Equal(t, legacyTopic, outcome.OriginalTopic,
		"the recorded topic must be preserved in the metadata, not replaced by the current mapping")
	assert.Equal(t, legacyDeadLetterTopic, outcome.DeadLetterTopic,
		"the dead-letter sibling is derived from the recorded topic")
	assert.Equal(t, []string{legacyDeadLetterTopic}, harness.resolutions())

	// Erase the row's own record of its destination. Only the failure metadata now knows
	// where this event was headed.
	stored, ok := harness.store.snapshot(exhausted.EventID)
	require.True(t, ok)
	stored.Topic = ""
	harness.store.put(stored)
	require.Equal(t, TopicForEvent(fixture.eventType), originalTopicOf(stored),
		"with the topic column cleared, the row alone resolves to the CURRENT mapping")
	require.Equal(t, legacyTopic, ReplayTopicFor(stored),
		"the stored failure metadata must still supply the original destination")

	harness.publisher.succeed()
	harness.clock.Set(replayFidelityReplayAt)

	replay, err := harness.service.ReplayDeadLetteredEvent(ctx, exhausted.EventID)
	require.NoError(t, err)

	assert.Equal(t, legacyTopic, replay.Topic,
		"the replay must go to the topic the metadata recorded, not the one the event type maps to now")
	assert.NotEqual(t, TopicForEvent(fixture.eventType), replay.Topic)

	// Byte fidelity is unaffected by any of this: the destination is transport routing
	// and takes no part in the message value.
	attempts := harness.publisher.captures()
	require.Len(t, attempts, replayFidelityMaxAttempts+1)
	replayed := attempts[len(attempts)-1]
	assert.Equal(t, legacyTopic, replayed.topic)
	assert.Equal(t, string(attempts[0].value), string(replayed.value),
		"a changed destination must not change one byte of the message")
	assert.Equal(t, fixture.partitionKey, replayed.key,
		"a changed destination must not change the partition key either")
}

// The real producer path
// ---------------------------------------------------------------------------

// TestReplayFidelity_RowFromTheRealProducerPathReplaysFaithfully runs the scenario on a
// row built by the PRODUCTION producer path rather than by this file's fixture builder.
func TestReplayFidelity_RowFromTheRealProducerPathReplaysFaithfully(t *testing.T) {
	harness := newReplayFidelityHarness(t)
	ctx := context.Background()
	fixture := replayFidelityFixtures()[0]

	// The producer's own two-key body, with the fixture's data object as the payload.
	// json.RawMessage is what carries the crafted member order and number literals
	// through json.Marshal untouched — a map would have re-sorted them here, before the
	// code under test ever saw them.
	dataObject := replayFidelityDataMember(t, fixture.payload)

	// Built directly rather than through NewBlnk so that no Redis client, asynq client,
	// queue or search client is constructed: none of them is on the path under test.
	// PrepareEventOutbox never touches the datasource, which is why nil is correct here;
	// PublishEvent's handing of the row to the repository is covered by
	// event_outbox_test.go.
	producer := &Blnk{config: replayFidelityConfiguration()}

	row := mustPrepareEventOutbox(t, producer, NewWebhook{
		Event:   fixture.eventType,
		Payload: dataObject,
	})
	require.NotNil(t, row, "event publishing is configured, so a row must be prepared")

	// The row as the producer leaves it, before the database assigns anything.
	assert.Equal(t, model.EventOutboxStatusPending, row.Status,
		"a freshly captured event is pending until a relay claims it")
	assert.Equal(t, fixture.eventType, row.EventType)
	assert.Equal(t, fixture.topic, row.Topic)
	assert.Equal(t, model.SchemaVersionV1, row.SchemaVersion)
	assert.Equal(t, replayFidelityMaxAttempts, row.MaxAttempts)
	assert.NotEmpty(t, row.EventID)
	// A raw-JSON payload carries no typed aggregate for the producer to read, so the
	// documented fallback chain ends at the event type. Asserted rather than glossed
	// over, because the invariant that matters is that the PARTITION KEY is NEVER
	// empty: an unkeyed message is spread across partitions and loses its ordering
	// guarantee with nothing in the data to show it.
	assert.Equal(t, fixture.eventType, row.PartitionKey)
	assert.Equal(t, fixture.eventType, row.AggregateID)
	assert.NotEmpty(t, row.PartitionKey)
	// The LEDGER column, by contrast, stays empty — and must. A raw-JSON payload carries
	// no ledger, so there is no ledger to record, and putting the event type in a column
	// called ledger_id is exactly the conflation the two-column split removed.
	assert.Empty(t, row.LedgerID,
		"an untyped payload carries no ledger: the column stays NULL rather than inheriting the routing key")
	assert.Zero(t, row.Attempts)
	assert.Nil(t, row.FirstAttemptedAt)
	assert.Nil(t, row.DispatchedAt)
	assert.Empty(t, row.DLTTopic)
	assert.Empty(t, row.FailureMetadata)

	// The producer marshaled the WHOLE two-key webhook body — both keys — which is the
	// payload-preservation contract, and it is the same document the column gives back.
	assert.Equal(t, []string{"event", "data"}, replayFidelityTopLevelKeys(t, row.Payload),
		"the stored payload is the two-key legacy webhook body, in that order")
	assert.JSONEq(t, fixture.payload, string(row.Payload),
		"the producer's rendering and the column's rendering must be the same document")
	assert.NotEqual(t, fixture.payload, string(row.Payload),
		"and they must be DIFFERENT byte renderings of it, or this test is not adding anything")

	// The database assigns the BIGSERIAL id. A row without one is refused by the
	// dead-letter path on purpose, because a dead-letter that cannot be recorded on its
	// row is invisible to the inventory and unreachable by replay.
	require.Zero(t, row.ID, "the producer does not invent a database id")
	_, err := harness.service.PublishToDeadLetter(ctx, DeadLetterRequest{Row: *row})
	require.Error(t, err, "a row with no database id cannot be dead-lettered")
	assert.Equal(t, apierror.ErrGenValidation, replayFidelityAPICode(t, err))

	persisted := *row
	persisted.ID = 1
	harness.store.put(persisted)

	exhausted, lastErr := harness.exhaustRetryBudget(t, persisted)
	harness.store.put(exhausted)

	outcome, err := harness.service.DeadLetter(ctx, exhausted, lastErr)
	require.NoError(t, err)
	assert.Equal(t, fixture.deadLetterTopic, outcome.DeadLetterTopic)

	harness.publisher.succeed()
	harness.clock.Set(replayFidelityReplayAt)

	replay, err := harness.service.ReplayDeadLetteredEvent(ctx, persisted.EventID)
	require.NoError(t, err)
	assert.Equal(t, fixture.topic, replay.Topic)
	assert.Equal(t, persisted.EventID, replay.EventID,
		"the producer's event id is the subscriber idempotency key and must survive")

	attempts := harness.publisher.captures()
	require.Len(t, attempts, replayFidelityMaxAttempts+1)
	original := attempts[0].value
	replayed := attempts[len(attempts)-1].value

	t.Logf("ORIGINAL (producer-built): %s", original)
	t.Logf("REPLAYED (producer-built): %s", replayed)

	require.Equal(t, string(original), string(replayed),
		"a producer-built event must replay byte for byte just as a column-read one does")

	stripped, err := StripFailureMetadata(outcome.Message)
	require.NoError(t, err)
	assert.Equal(t, string(original), string(stripped),
		"the failure metadata must remain the only difference for a producer-built event")

	// The payload the producer wrote is the payload that arrives, unaltered by the whole
	// dead-letter and replay round trip.
	assert.Equal(t, string(row.Payload),
		string(replayFidelityDecodeEnvelope(t, replayed).Payload),
		"the producer's payload bytes must arrive exactly as they were marshaled")
}

// replayFidelityDataMember extracts the "data" member of a webhook body as RAW BYTES.
func replayFidelityDataMember(t *testing.T, body string) json.RawMessage {
	t.Helper()

	var members map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(body), &members))

	data, ok := members["data"]
	require.True(t, ok, "a webhook body always carries a data member")

	return data
}

// Harness hygiene
// ---------------------------------------------------------------------------

// TestReplayFidelity_LeaksNoConfigurationBetweenTests asserts this file's harness is well
// behaved.
func TestReplayFidelity_LeaksNoConfigurationBetweenTests(t *testing.T) {
	const sentinelPrefix = "replay-fidelity-leak-sentinel"

	before := config.ConfigStore.Load()

	t.Run("a subtest publishes a recognisable configuration", func(t *testing.T) {
		configuration := replayFidelityConfiguration()
		configuration.Kafka.TopicPrefix = sentinelPrefix
		replayFidelityStoreConfiguration(t, configuration)

		require.Equal(t, sentinelPrefix+".transactions", TopicForEvent("transaction.applied"),
			"the sentinel configuration must genuinely be in force inside the subtest")
	})

	assert.Equal(t, before, config.ConfigStore.Load(),
		"the configuration store must be restored exactly as the subtest found it")
	assert.NotEqual(t, sentinelPrefix, TopicPrefix(),
		"no test may leave its own topic prefix in the process-global configuration store")
}

// replayFidelityClock was this file's settable clock. It is RETIRED: movableTestClock above
// is the same type under the name the capability earned once a second suite needed it, and
// its doc comment carries the race-safety argument this one had lost. The harness field is a
// *movableTestClock, so this was the copy nothing installed.
//
// ListDeadLetteredEventsFiltered pages the two terminal failure states, newest first,
// applying the filter to the whole population BEFORE the page is taken — which is what
// SQL does, and therefore the only faithful order for a double to apply it in.
func (s *replayFidelityStore) ListDeadLetteredEventsFiltered(
	_ context.Context,
	filter model.DeadLetterInventoryFilter,
	limit, offset int,
) ([]model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inventory := s.matchingLocked(filter)

	if offset >= len(inventory) {
		return []model.EventOutbox{}, nil
	}
	inventory = inventory[offset:]
	if limit > 0 && limit < len(inventory) {
		inventory = inventory[:limit]
	}

	return inventory, nil
}
