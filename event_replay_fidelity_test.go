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

// event_replay_fidelity_test.go proves ONE acceptance criterion and nothing else:
// a replayed dead-lettered event matches the original BYTE FOR BYTE, aside from the
// failure metadata.
//
// # WHY BYTE EQUALITY IS ACHIEVABLE AT ALL
//
// It is achievable because replay re-publishes the ORIGINAL STORED BYTES rather than
// re-marshalling from a struct. Two design decisions in the code under test make that
// true, and this file exists to hold both of them in place:
//
//  1. model.LedgerEvent.Payload is typed json.RawMessage — not map[string]interface{},
//     not a typed struct — precisely so the payload bytes pass through untransformed.
//  2. marshalLedgerEvent composes the envelope member by member and SPLICES those bytes
//     in verbatim, and ComposeDeadLetterMessage attaches the failure metadata by
//     replacing the envelope's closing brace with `,"failure_metadata":<metadata>}`.
//     The original envelope is therefore a byte-exact PREFIX of the dead-letter message,
//     and StripFailureMetadata is the exact byte-level inverse.
//
// Every alternative implementation of the same feature — decode into a map and
// re-encode, decode into a typed struct and re-encode, "just re-marshal the
// LedgerEvent" — would reorder keys, renormalise number literals, re-render timestamp
// strings, strip insignificant whitespace, or silently drop members no Go struct
// declares. Any one of those makes the guarantee unachievable rather than merely
// harder.
//
// THIS FILE'S JOB IS TO FAIL IF A FUTURE CHANGE REINTRODUCES A STRUCT ROUND TRIP.
// That is only possible if the fixtures would visibly change under one, which is why
// the fixtures below are what they are and why
// TestReplayFidelity_FixturesWouldExposeAReMarshal exists to keep them that way. A
// fixture that round-trips cleanly cannot tell a correct implementation from a broken
// one, and a test that cannot tell them apart is worse than no test.
//
// # NO BROKER IS REQUIRED
//
// Everything here runs under `go test -short` with no Kafka anywhere. The transport is
// substituted through EventDeadLetterService.withTransport, which installs a recording
// publisher and a recording dead-letter writer, so there is no live broker to guard
// with testing.Short() and nothing to skip. The recording publisher marshals through
// the REAL marshalLedgerEvent, so the bytes it captures are the bytes a broker would
// have received.
//
// # SCOPE — what deliberately is NOT here
//
//   - No dual-delivery comparison. event_dual_delivery_test.go owns that criterion.
//   - No ordering or crash-recovery assertions. Their own integration tests own those.
//   - No HTTP-level assertions on the replay endpoint or its master-key gate.
//     api/events_api_test.go owns those.
//   - No consumer implementation beyond the verification above. Blnk ships no consumer
//     library and no subscriber-side dead-letter management, and nothing here reads
//     from Kafka.

// ---------------------------------------------------------------------------
// Pinned instants
//
// Every timestamp in this file is fixed. The service clock is settable (see
// replayFidelityClock) so that "the event occurred at X", "we gave up on it at Y" and
// "an operator replayed it at Z" are three distinguishable instants rather than three
// readings of time.Now() that happen to be microseconds apart. The assertion that a
// replay carries the ORIGINAL occurrence time and not the replay time is only
// meaningful when the two cannot coincide.
// ---------------------------------------------------------------------------

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
//
// The name carries the err prefix staticcheck's ST1012 requires for a package-level
// error value, and keeps the ReplayFidelity discriminator so it cannot collide with a
// symbol declared by another test file in this package.
var errReplayFidelityBroker = errors.New("write tcp 10.0.0.4:9092: broken pipe")

// ---------------------------------------------------------------------------
// THE FIXTURES
//
// These payload byte strings are the single most important design decision in this
// file, so the reasoning is spelled out rather than implied.
//
// # They are real production bytes, not invented ones
//
// blnk.event_outbox.payload is a JSONB column. Postgres does not store JSONB
// verbatim: it parses the document and re-renders it on read, ordering members by
// (key length, then bytewise) and inserting a space after every colon and every
// comma. The byte strings below are the VERBATIM `::jsonb::text` renderings produced
// by the Postgres 16 instance this repository develops against, so they are exactly
// what scanEventOutbox hands the relay after ClaimPendingEventOutbox — not an
// approximation of it.
//
// # Every property below is load-bearing
//
// Each fixture is built so that a struct or map round trip is VISIBLE. Remove any one
// of these and the corresponding class of regression stops being detectable:
//
//   - INSIGNIFICANT WHITESPACE (`": "`, `", "`). Any re-marshal runs the bytes through
//     encoding/json's compactor, which strips it. This is the property that catches
//     even the most innocent-looking regression — "just re-marshal the LedgerEvent" —
//     because json.RawMessage is compacted on the way out.
//   - NON-ALPHABETICAL MEMBER ORDER. Postgres orders by length-then-bytewise; Go
//     marshals a map with its keys sorted lexicographically. The two never agree here,
//     so any map round trip reorders members.
//   - A NUMBER THAT LOSES PRECISION AS A float64: 9007199254740993 is 2^53 + 1 and
//     comes back as 9007199254740992.
//   - A NUMBER THAT CHANGES NOTATION: 123456789012345678901234 is re-rendered as
//     1.2345678901234569e+23. (Note that a merely large integer such as 1000000 does
//     NOT change under Go's encoder — Go only switches to exponent notation past about
//     1e21 — so the value has to be this big to make the notation change happen.)
//   - A HIGH-PRECISION DECIMAL: 0.10000000000000000555 collapses to 0.1.
//   - A TIMESTAMP STRING IN A FORM GO WOULD RE-RENDER: "…T12:34:56.789000+00:00"
//     becomes "…T12:34:56.789Z" the moment anything decodes it into a time.Time.
//   - AN "unmodelled_extension" MEMBER THAT NO GO STRUCT IN THIS REPOSITORY DECLARES,
//     which a typed round trip drops silently — the failure mode with no error message
//     and no stack trace, and the reason a byte comparison is the right assertion.
//
// No fixture contains `<`, `>` or `&`. Those characters are HTML-escaped by
// json.Marshal when the producer builds the row, so they are already in escaped form
// by the time they are stored and are worthless as discriminators; including them
// would only add noise.
// ---------------------------------------------------------------------------

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

// replayFidelitySystemPayload is a ledger.created body. It routes to the fourth,
// system category — the one that exists because ledger.created and system.error belong
// to none of the three categories the requirements name, and coverage is absolute.
const replayFidelitySystemPayload = `{"data": {"name": "Replay Fidelity Ledger", "ledger_id": "ldg_replay_fidelity_004", "meta_data": {"region": "eu-west-1", "sequence": 9007199254740993, "utilisation": 0.90000000000000002220, "scaled_capacity": 123456789012345678901234}, "created_at": "2024-07-03T22:11:00.250000+00:00", "unmodelled_extension": {"vendor_flag": true}}, "event": "ledger.created"}`

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

// replayFidelityFixtures returns one fixture per event category, with the four
// dead-letter topic names written out as LITERALS rather than derived with DLTFor.
//
// Spelling them out is the point. Deriving the expected value from the same function
// the implementation uses would make the assertion tautological: DLTFor could append
// ".deadletter" and the test would still pass. These four names are a published
// contract that provisioning scripts, alert rules, subscriber documentation and
// operator runbooks all depend on, so they are pinned here as text.
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
			name:        "system",
			eventType:   "ledger.created",
			aggregateID: "ldg_replay_fidelity_004",
			// ledger.created is one of the two shapes that genuinely DO carry a
			// ledger, so both columns hold it — which is how requirement R-6's
			// "partitioned by ledger ID" is honoured wherever a ledger exists.
			partitionKey:    "ldg_replay_fidelity_004",
			ledgerID:        "ldg_replay_fidelity_004",
			payload:         replayFidelitySystemPayload,
			topic:           "blnk.system",
			deadLetterTopic: "blnk.system.dlt",
		},
	}
}

// row builds the outbox row the relay would have claimed for this fixture: the
// envelope columns populated, the retry budget set, and the row still pending.
//
// Every subsequent state is reached by exercising the code under test rather than by
// hand-editing the row, so the transitions this file asserts are the implementation's
// own.
func (f replayFidelityFixture) row(id int64) model.EventOutbox {
	return model.EventOutbox{
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
}

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// replayFidelityClock is the settable clock installed on the service under test.
//
// It exists so that the dead-letter instant and the replay instant are a day apart and
// therefore distinguishable. Every reading goes through the mutex, so the doubles stay
// safe under `go test -race` even though the tests themselves are sequential.
type replayFidelityClock struct {
	mu  sync.Mutex
	now time.Time
}

// Now reports the current pinned instant.
func (c *replayFidelityClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

// Set moves the clock to at.
func (c *replayFidelityClock) Set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = at
}

// replayFidelityStore is an in-memory eventDeadLetterStore.
//
// It implements the five-method repository seam the dead-letter service depends on and
// applies the SAME state transitions database/event_outbox.go does, which is what lets
// this file assert the post-replay row state without a database:
//
//   - MarkEventDeadLettered sets status to dead_lettered, records dlt_topic and
//     failure_metadata, and releases the lease.
//   - MarkEventDispatched sets status to dispatched, stamps dispatched_at and releases
//     the lease — and deliberately RETAINS dlt_topic and failure_metadata, exactly as
//     the SQL does, because the history of what went wrong must survive a replay.
//   - GetEventByID reports a missing row as a typed not-found APIError rather than as
//     (nil, nil), matching the repository's documented deviation.
//   - ListDeadLetteredEvents returns only the two terminal failure states, newest
//     occurrence first with id descending as the tie-break.
//
// Reproducing the transitions rather than stubbing them is the difference between
// asserting that the service asked for the right transition and asserting that the row
// ends up in the right state. This file wants the latter.
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
//
// The caller must hold s.mu.
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

// ListDeadLetteredEvents pages the two terminal failure states, newest first.
func (s *replayFidelityStore) ListDeadLetteredEvents(
	_ context.Context,
	limit, offset int,
) ([]model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var inventory []model.EventOutbox
	// Reverse insertion order approximates the repository's "occurred_at DESC, id
	// DESC": every fixture row shares one occurrence instant, so id descending is the
	// operative clause, and rows are inserted in ascending id order.
	for i := len(s.order) - 1; i >= 0; i-- {
		row := s.rows[s.order[i]]
		if row == nil {
			continue
		}
		if row.Status != model.EventOutboxStatusDeadLettered &&
			row.Status != model.EventOutboxStatusFailed {
			continue
		}
		inventory = append(inventory, *row)
	}

	if offset >= len(inventory) {
		return []model.EventOutbox{}, nil
	}
	inventory = inventory[offset:]
	if limit > 0 && limit < len(inventory) {
		inventory = inventory[:limit]
	}

	return inventory, nil
}

// CountEventOutboxByStatus returns a status-keyed count of every row. A status with no
// rows is absent from the map, matching the repository's GROUP BY semantics.
func (s *replayFidelityStore) CountEventOutboxByStatus(_ context.Context) (map[string]int64, error) {
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
func (s *replayFidelityStore) MarkEventDispatched(_ context.Context, id int64, claimToken string) error {
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
//
// It marshals through the REAL marshalLedgerEvent — the same function the Kafka
// publisher uses — so the bytes it captures are the bytes a broker would have received.
// Reimplementing the serialisation here would make every byte assertion in this file a
// tautology about the double.
//
// It is NOT the no-op publisher, deliberately. ReplayDeadLetteredEvent refuses to
// replay through a no-op and returns ErrKafkaUnavailable, so a double based on
// NoopEventPublisher could never reach the code this file exists to test.
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

// PublishToTopic marshals the envelope with the production serialiser, records what
// would have gone on the wire, and then reports success or the programmed failure.
//
// The order matters: the bytes are captured BEFORE the failure is applied, so a failed
// attempt still yields the message that would have been sent. That is what makes the
// original bytes observable from attempt one rather than having to be recomputed —
// recomputing them would be assuming the very equality under test.
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

	value, err := marshalLedgerEvent(req.Event)
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
//
// It is per-topic, exactly as *kafka.Writer is, and it asserts that contract itself:
// kafka-go rejects a message that names a topic when the writer already has one, so a
// message arriving here with Topic set would be a real defect and is captured so a test
// can say so.
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

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// replayFidelityConfiguration returns a configuration in which event publishing is
// enabled by the Kafka transport alone.
//
// The legacy webhook URL is left empty on purpose, so that nothing in this file can
// accidentally depend on the legacy transport or POST anywhere. The broker address is
// never dialled: the transport is substituted before any operation runs, and it is
// present only because it is what makes event publishing "configured" for
// PrepareEventOutbox.
func replayFidelityConfiguration() *config.Configuration {
	return &config.Configuration{
		Kafka: config.KafkaConfig{Brokers: []string{"localhost:9092"}},
		Relay: config.RelayConfig{MaxRetryAttempts: replayFidelityMaxAttempts},
	}
}

// replayFidelityStoreConfiguration publishes cnf to config.ConfigStore and restores
// whatever was there before once the test finishes.
//
// config.ConfigStore is a process-global atomic.Value, so leaking a Kafka-configured
// state out of one test produces confusing failures in unrelated ones. The restore is
// registered with t.Cleanup so it runs even when a test fails or calls t.Fatal, and it
// is what lets the whole suite be run twice consecutively with the same result.
//
// It writes to the store DIRECTLY rather than through config.MockConfig, matching the
// approach the other event tests take: MockConfig runs validateAndAddDefaults, which
// refuses to store a configuration lacking a data-source and Redis DSN — the value
// would be silently dropped — and which would also apply defaults this file needs to
// control itself.
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
//
// The service is built with NO publisher and then has one INSTALLED through
// withTransport. That sequencing is deliberate: passing the recording publisher to the
// constructor would have it wrapped by publisherWriterResolver, which resolves a writer
// only for the real Kafka publisher and rejects anything else — so the dead-letter
// write would fail with ErrKafkaUnavailable before reaching the composition this file
// tests. Installing the transport explicitly substitutes both halves at once, which is
// exactly what withTransport documents itself as being for.
type replayFidelityHarness struct {
	// store is the in-memory repository.
	store *replayFidelityStore
	// publisher records replays and the failing original attempts.
	publisher *replayFidelityPublisher
	// clock is the service's settable clock.
	clock *replayFidelityClock
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
		clock:     &replayFidelityClock{now: replayFidelityDeadLetterAt},
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
//
// It reproduces the relay's per-attempt bookkeeping rather than importing the relay:
// every attempt goes through PublishRequestFromOutbox and the publisher's own
// PublishToTopic, the attempt counter advances, the failure reason is recorded, and the
// attempt window is stamped. Reproducing it keeps this file's subject strictly the
// dead-letter and replay path — the retry SCHEDULE is asserted by the relay's own test,
// and a dependency on the relay here would couple two acceptance criteria that must be
// able to fail independently.
//
// No wall-clock time passes: the backoff schedule is not exercised, only the budget.
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

// ---------------------------------------------------------------------------
// Byte-level assertion helpers
//
// Every comparison in this file is on RAW BYTES. Nothing here unmarshals both sides
// into maps and compares those: a map comparison discards member order, which is one of
// the exact properties a re-marshal destroys, so it would report equality for bytes
// that are demonstrably different. Decoding is used only to read a single field's
// VALUE, never to establish equality.
// ---------------------------------------------------------------------------

// replayFidelityTopLevelKeys returns the top-level member names of a JSON object IN
// DOCUMENT ORDER.
//
// It walks the token stream rather than decoding into a map, because a map has no
// order. Member values are consumed as raw JSON and discarded, so nesting depth and
// content are irrelevant.
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
//
// NewWebhook.Payload is interface{}, so this does NOT drop members — the inner object
// decodes to a map. What it does do is reorder that map's members and renormalise its
// numbers, which is precisely the point: even the codebase's own webhook body type
// cannot be used as a staging post for these bytes without changing them.
func replayFidelityWebhookRoundTrip(t *testing.T, raw []byte) []byte {
	t.Helper()

	var body NewWebhook
	require.NoError(t, json.Unmarshal(raw, &body))
	encoded, err := json.Marshal(body)
	require.NoError(t, err)

	return encoded
}

// replayFidelityModelledBody stands in for ANY struct-typed view of the webhook body.
//
// It declares the outer two-key contract and, inside data, nothing at all. That is not a
// shortcut: encoding/json silently discards every member of a JSON object that the
// target struct does not declare, so a struct with no data members demonstrates the
// failure mode for all of them at once, and does so identically for all four fixtures
// without needing a different struct per category.
//
// The member named "unmodelled_extension" in every fixture is deliberately one that NO
// Go struct anywhere in this repository declares — it is the vendor-extension case a
// real payload can genuinely carry. It therefore disappears through any typed view, with
// no error and no warning, which is exactly why the guarantee has to be asserted on
// BYTES and not on a decoded document.
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
//
// Both the value and the pointer form are handled because apierror.NewAPIError returns
// a VALUE, while some layers wrap a pointer, and a helper that handled only one would
// silently fall through to a confusing failure.
//
// The code is normalised because the codebase still constructs legacy generic codes in
// places — the replay path's own blank-row guard raises INVALID_INPUT rather than
// GEN_VALIDATION_ERROR — and it is the canonical code a caller is answered with.
// Asserting the canonical form is therefore both more accurate and immune to a legacy
// code being replaced by its canonical equivalent later.
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

// ---------------------------------------------------------------------------
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
//
// The sequence is the production one, step for step: the relay spends the retry budget
// against a failing broker, the exhausted row is dead-lettered to its `<topic>.dlt`
// sibling with failure metadata attached, the broker recovers, and an operator replays
// the event a day later.
func (h *replayFidelityHarness) runScenario(
	t *testing.T,
	fixture replayFidelityFixture,
	id int64,
) replayFidelityScenario {
	t.Helper()

	ctx := context.Background()

	// 1. A pending row, exactly as it was committed alongside the ledger mutation.
	row := fixture.row(id)
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

// ---------------------------------------------------------------------------
// The anti-vacuity guard
// ---------------------------------------------------------------------------

// TestReplayFidelity_FixturesWouldExposeAReMarshal proves the fixtures can tell a
// correct implementation from a broken one.
//
// THIS TEST IS THE REASON THE REST OF THE FILE IS WORTH ANYTHING. A byte-equality
// assertion over a fixture that survives a round trip unchanged passes just as happily
// against an implementation that decodes and re-encodes as against one that splices
// bytes. It would look like coverage and provide none.
//
// So each fixture is checked against the three re-marshals a future change could
// plausibly introduce, and each must CHANGE the bytes. If one of these assertions ever
// fails, the fixture has stopped being discriminating and must be rebuilt before the
// file can be trusted again — the failure message says so.
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
//
// The payload checks above cannot catch a regression that decodes the envelope into a
// map: a Go map marshals with its keys sorted, and the envelope's own member order is
// contractual, so that reordering has to be detectable on its own terms. This asserts
// both re-marshals of the whole envelope change it.
func TestReplayFidelity_EnvelopeWouldExposeAReMarshal(t *testing.T) {
	harness := newReplayFidelityHarness(t)
	fixture := replayFidelityFixtures()[0]

	row := fixture.row(1)
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

// ---------------------------------------------------------------------------
// Criterion V-9: byte-for-byte replay, across every category
// ---------------------------------------------------------------------------

// TestReplayFidelity_ReplayedMessageIsByteIdenticalToTheOriginal is the acceptance test
// for criterion V-9.
//
// It runs the whole scenario once per event CATEGORY, so a category-specific bug in
// dead-letter routing or in original-topic recovery is caught rather than hidden behind
// a single happy-path event, and it pins the four dead-letter topic names as literals.
//
// The assertions, in the order they appear:
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

// ---------------------------------------------------------------------------
// The failure metadata: the one thing that IS different
// ---------------------------------------------------------------------------

// TestReplayFidelity_FailureMetadataCarriesEveryDocumentedField asserts the metadata
// that constitutes the "aside from" in "byte-for-byte aside from the failure metadata".
//
// All five documented fields are checked, because the metadata exists to answer an
// operator's three questions — where was this event meant to go, why did it not get
// there, and over what window did we try — and a field left at its zero value answers
// nothing while looking like a successful read.
//
// The attempt count is checked against the CONFIGURED MAXIMUM specifically. An exhausted
// row has spent its whole budget, so a count that disagrees with the maximum means the
// exhaustion accounting is wrong: too high and something spent an extra attempt against
// a five-attempt budget, too low and the row was dead-lettered before its budget ran out.
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

// ---------------------------------------------------------------------------
// Post-replay state, and repeat replay
// ---------------------------------------------------------------------------

// TestReplayFidelity_ReplayedRowLeavesTheDeadLetterInventory asserts the row ends in the
// state the implementation documents, and that the inventory reflects it.
//
// Three properties are asserted together because they are one behaviour: a replayed row
// becomes dispatched, so it disappears from the triage inventory and stops being
// presented as needing attention — while retaining dlt_topic and failure_metadata, so
// the history of what went wrong is not erased by fixing it.
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
	for _, entry := range inventory {
		assert.NotEqual(t, scenario.row.EventID, entry.EventID,
			"a replayed event must no longer be listed as dead-lettered")
	}
	assert.Empty(t, inventory, "the only entry in this store has been replayed")

	counts, err := harness.store.CountEventOutboxByStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), counts[model.EventOutboxStatusDispatched])
	assert.Zero(t, counts[model.EventOutboxStatusDeadLettered])
}

// TestReplayFidelity_RepeatReplayIsRejectedRatherThanDuplicating asserts that replaying
// the same event twice is EXPLICITLY REFUSED and never silently duplicates it.
//
// This is the accidental-duplication case the status precondition exists to prevent — an
// operator working a triage list and replaying the same entry twice. Either documented
// behaviour would be acceptable, idempotent or rejected, but silence would not: a second
// publish with no acknowledgement of the repeat is a duplicate the operator did not know
// they had caused.
//
// The implementation refuses, because a successful replay moves the row out of the
// dead-lettered state and the precondition no longer holds. Both halves are asserted:
// the typed refusal, AND that no second message was published.
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
//
// The status half is not incidental. apierror.StatusForCode defaults an UNMAPPED code to
// 500, so a code added without a statusByCode entry would answer "internal server error"
// to a caller who asked for an event that does not exist — indistinguishable from a bug
// in Blnk, and it would send an operator looking in the wrong place. Asserting the
// mapping here is what stops that from being possible, and each is additionally asserted
// NOT to be 500 so that a code silently falling through to the default cannot pass.
//
// The three refusals are distinct operator situations and must not be conflated:
//   - a blank id is a malformed request,
//   - an unknown id is a wrong id,
//   - a present but not-dead-lettered event is a state error, most often a repeat replay.
func TestReplayFidelity_ReplayRejectionsCarryTheirDocumentedCodeAndStatus(t *testing.T) {
	harness := newReplayFidelityHarness(t)
	ctx := context.Background()
	fixture := replayFidelityFixtures()[0]

	// A pending row: present, but with no dead-letter message to replay from.
	pending := fixture.row(1)
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

// ---------------------------------------------------------------------------
// Where the replay destination comes from
// ---------------------------------------------------------------------------

// TestReplayFidelity_ReplayTopicComesFromTheStoredFailureMetadata asserts the replay
// destination is RECOVERED FROM THE STORED METADATA rather than re-derived from the event
// type.
//
// Under the default configuration both routes give the same answer, so an assertion made
// on a default-prefixed event proves nothing — it would pass against an implementation
// that re-derived the topic every time. This test therefore constructs the one situation
// in which the two disagree: an event stored while KAFKA_TOPIC_PREFIX was something else,
// so its recorded destination is "legacy.transactions" while the event type's CURRENT
// mapping is "blnk.transactions".
//
// It then goes further and removes the row's own topic column after dead-lettering, so
// that originalTopicOf would fall back to the current mapping. The stored failure
// metadata becomes the ONLY place the original destination survives, and the replay is
// asserted to honour it. What was recorded wins.
func TestReplayFidelity_ReplayTopicComesFromTheStoredFailureMetadata(t *testing.T) {
	harness := newReplayFidelityHarness(t)
	ctx := context.Background()

	const legacyTopic = "legacy.transactions"
	const legacyDeadLetterTopic = "legacy.transactions.dlt"

	fixture := replayFidelityFixtures()[0]
	require.NotEqual(t, legacyTopic, TopicForEvent(fixture.eventType),
		"the test is only meaningful when the recorded topic and the current mapping disagree")

	row := fixture.row(1)
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

// ---------------------------------------------------------------------------
// The real producer path
// ---------------------------------------------------------------------------

// TestReplayFidelity_RowFromTheRealProducerPathReplaysFaithfully runs the scenario on a
// row built by the PRODUCTION producer path rather than by this file's fixture builder.
//
// The fixtures above are the bytes as they come back OUT of the JSONB payload column,
// which is what the relay works from and therefore the right input for a fidelity test.
// This test closes the loop at the other end: it has PrepareEventOutbox — the function
// every one of the eight producer call sites reaches through PublishEvent — build the row,
// asserts the row is pending with the payload the producer marshaled, and then puts THAT
// row through the same dead-letter and replay sequence.
//
// The two renderings are the same JSON document in different byte form, which is asserted
// with JSONEq. Note carefully that JSONEq is used HERE AND ONLY HERE, to state a
// deliberate semantic equivalence between what the producer wrote and what the column
// gives back. Every fidelity assertion in this file is on raw bytes, because a semantic
// comparison is exactly what would let a re-marshal through.
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
//
// It is raw on purpose. Handing PrepareEventOutbox a json.RawMessage is what carries the
// crafted member order and number literals through the producer's own json.Marshal
// untouched; decoding the fixture into a map first would re-sort its members and
// renormalise its numbers before the code under test ever saw them, and the test would
// then be measuring the fixture builder rather than the implementation.
func replayFidelityDataMember(t *testing.T, body string) json.RawMessage {
	t.Helper()

	var members map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(body), &members))

	data, ok := members["data"]
	require.True(t, ok, "a webhook body always carries a data member")

	return data
}

// ---------------------------------------------------------------------------
// Harness hygiene
// ---------------------------------------------------------------------------

// TestReplayFidelity_LeaksNoConfigurationBetweenTests asserts this file's harness is well
// behaved.
//
// config.ConfigStore is a process-global atomic.Value, and every test above publishes to
// it. Without the t.Cleanup restore, a Kafka-configured state would leak into unrelated
// tests in this package and fail them far from their cause — and the suite would stop
// being repeatable, so running it twice in a row would not give the same answer. This
// publishes a recognisable configuration through a subtest and then asserts the store no
// longer carries it, which is only true if the cleanup ran.
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
