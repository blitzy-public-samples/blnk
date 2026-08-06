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

// Tests for the event outbox relay.
//
// Every test here runs WITHOUT A BROKER AND WITHOUT A DATABASE, which is deliberate: the
// relay's contract is expressed against four repository methods, one publisher method, one
// dead-letter hand-off and one legacy enqueue, so substituting those seams exercises the
// real decision logic — the retry schedule, the ordering guarantee, the exhaustion
// hand-off, the dual-delivery branch — with nothing skipped and nothing mocked away that
// matters. The end-to-end proofs against a live broker are the integration tests named in
// the acceptance criteria (ordering, recovery, isolation, dual delivery).
//
// The fake store below is a small STATE MACHINE rather than a recorder, because the parts
// most worth testing are sequences: five failed attempts must produce four scheduled
// retries and exactly one dead-letter, and a re-claim after a crash must republish only
// what was not marked. A recorder cannot express either.
package blnk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// relayFixedNow is the clock every test pins, so scheduled instants and durations are exact
// rather than approximately now.
var relayFixedNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// errRelayTransient stands in for a recoverable broker failure.
var errRelayTransient = errors.New("relay test: broker unavailable")

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// relayClaimRecord is one recorded claim: the geometry the relay asked for.
type relayClaimRecord struct {
	batchSize    int
	lockDuration time.Duration
}

// relayMarkRecord is one recorded transition: which row, under which claim token.
type relayMarkRecord struct {
	id         int64
	claimToken string
}

// relayFailRecord is one recorded failed attempt.
type relayFailRecord struct {
	id         int64
	claimToken string
	reason     string
	retryAfter time.Duration
}

// relayFakeStore models blnk.event_outbox closely enough to drive multi-attempt sequences:
// it claims FIFO with a fresh token, returns a failed row to the claimable set with the
// caller's backoff applied, and exhausts a row's budget the way MarkEventFailed's in-SQL
// CASE does.
//
// It deliberately does NOT reproduce the repository's one-row-per-partition-key claim rule.
// That rule is one of the two independent reasons ordering holds; leaving it out is what
// lets these tests exercise the OTHER one — the relay's own grouping — by handing a batch
// two rows that share a key.
type relayFakeStore struct {
	mu sync.Mutex

	// pending is the claimable set, oldest first.
	pending []model.EventOutbox
	// inflight is every row currently claimed, by row id, so a lease can be expired.
	inflight map[int64]model.EventOutbox
	// terminal records rows that reached a terminal state, by row id.
	terminal map[int64]string

	claims       []relayClaimRecord
	claimed      [][]model.EventOutbox
	dispatched   []relayMarkRecord
	failures     []relayFailRecord
	webhookMarks []relayMarkRecord

	claimErr    error
	dispatchErr error
	failErr     error
	webhookErr  error

	tokens int

	// released is closed the first time a claim is served, and claimGate blocks the claim
	// until the test lets it through. Both are nil unless a test opts in.
	claimGate chan struct{}
	gateOnce  sync.Once
	gateHit   chan struct{}

	now func() time.Time
}

func newRelayFakeStore(rows ...model.EventOutbox) *relayFakeStore {
	store := &relayFakeStore{
		pending:  append([]model.EventOutbox(nil), rows...),
		inflight: map[int64]model.EventOutbox{},
		terminal: map[int64]string{},
		now:      func() time.Time { return relayFixedNow },
	}

	return store
}

// ClaimPendingEventOutbox hands out up to batchSize due rows, stamping one fresh token for
// the batch exactly as the repository does.
func (s *relayFakeStore) ClaimPendingEventOutbox(
	ctx context.Context,
	batchSize int,
	lockDuration time.Duration,
) ([]model.EventOutbox, error) {
	if gate := s.gate(); gate != nil {
		s.gateOnce.Do(func() { close(s.gateHit) })

		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.claims = append(s.claims, relayClaimRecord{batchSize: batchSize, lockDuration: lockDuration})

	if s.claimErr != nil {
		return nil, s.claimErr
	}

	if batchSize <= 0 {
		return nil, errors.New("relay test: batch size must be greater than zero")
	}

	s.tokens++
	token := fmt.Sprintf("token-%d", s.tokens)
	now := s.now()

	var (
		claimed []model.EventOutbox
		kept    []model.EventOutbox
	)

	for _, row := range s.pending {
		if len(claimed) >= batchSize || row.NextAttemptAt.After(now) {
			kept = append(kept, row)

			continue
		}

		row.Status = model.EventOutboxStatusProcessing
		row.ClaimToken = token
		lease := now.Add(lockDuration)
		row.LockedUntil = &lease
		if row.FirstAttemptedAt == nil {
			first := now
			row.FirstAttemptedAt = &first
		}
		last := now
		row.LastAttemptedAt = &last

		s.inflight[row.ID] = row
		claimed = append(claimed, row)
	}

	s.pending = kept
	s.claimed = append(s.claimed, claimed)

	return claimed, nil
}

// MarkEventDispatched moves a claimed row to the success terminal state, refusing a token
// that is not the current one.
func (s *relayFakeStore) MarkEventDispatched(_ context.Context, id int64, claimToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dispatched = append(s.dispatched, relayMarkRecord{id: id, claimToken: claimToken})

	if s.dispatchErr != nil {
		return s.dispatchErr
	}

	row, held := s.inflight[id]
	if !held || row.ClaimToken != claimToken {
		return fmt.Errorf("relay test: claim lost on row %d", id)
	}

	delete(s.inflight, id)
	s.terminal[id] = model.EventOutboxStatusDispatched

	return nil
}

// MarkEventFailed records the attempt and reproduces the repository's two arms: back to the
// claimable set with the caller's backoff applied, or failed with the token retained.
func (s *relayFakeStore) MarkEventFailed(
	_ context.Context,
	id int64,
	claimToken, errMsg string,
	retryAfter time.Duration,
) (model.EventFailureOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failures = append(s.failures, relayFailRecord{
		id: id, claimToken: claimToken, reason: errMsg, retryAfter: retryAfter,
	})

	if s.failErr != nil {
		return model.EventFailureOutcome{}, s.failErr
	}

	row, held := s.inflight[id]
	if !held || row.ClaimToken != claimToken {
		return model.EventFailureOutcome{}, fmt.Errorf("relay test: claim lost on row %d", id)
	}

	row.Attempts++
	row.LastError = errMsg
	row.LockedUntil = nil

	if row.Attempts >= row.MaxAttempts {
		row.Status = model.EventOutboxStatusFailed
		s.inflight[id] = row
		s.terminal[id] = model.EventOutboxStatusFailed

		return model.EventFailureOutcome{
			Status:     row.Status,
			Attempts:   row.Attempts,
			Exhausted:  true,
			ClaimToken: claimToken,
		}, nil
	}

	row.Status = model.EventOutboxStatusPending
	row.ClaimToken = ""
	row.NextAttemptAt = s.now().Add(retryAfter)

	delete(s.inflight, id)
	s.pending = append(s.pending, row)

	return model.EventFailureOutcome{Status: row.Status, Attempts: row.Attempts}, nil
}

// MarkWebhookDispatched records the dual-delivery marker on the claimed row.
func (s *relayFakeStore) MarkWebhookDispatched(_ context.Context, id int64, claimToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.webhookMarks = append(s.webhookMarks, relayMarkRecord{id: id, claimToken: claimToken})

	if s.webhookErr != nil {
		return s.webhookErr
	}

	row, held := s.inflight[id]
	if !held || row.ClaimToken != claimToken {
		return fmt.Errorf("relay test: claim lost on row %d", id)
	}

	row.WebhookDispatched = true
	s.inflight[id] = row

	return nil
}

// expireLeases returns every still-claimed row to the claimable set, which is what a relay
// crash looks like from the database's point of view once the lease has run out.
func (s *relayFakeStore) expireLeases() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, row := range s.inflight {
		if _, done := s.terminal[id]; done {
			continue
		}

		row.Status = model.EventOutboxStatusPending
		row.ClaimToken = ""
		row.LockedUntil = nil
		row.NextAttemptAt = time.Time{}
		s.pending = append(s.pending, row)
		delete(s.inflight, id)
	}
}

func (s *relayFakeStore) gate() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.claimGate
}

// withClaimGate makes the first claim block until the returned release function is called,
// so a test can hold a batch open and observe the lifecycle around it.
func (s *relayFakeStore) withClaimGate() (hit <-chan struct{}, release func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.claimGate = make(chan struct{})
	s.gateHit = make(chan struct{})
	gate := s.claimGate

	return s.gateHit, func() { close(gate) }
}

func (s *relayFakeStore) snapshotFailures() []relayFailRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]relayFailRecord(nil), s.failures...)
}

func (s *relayFakeStore) snapshotDispatched() []relayMarkRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]relayMarkRecord(nil), s.dispatched...)
}

func (s *relayFakeStore) snapshotWebhookMarks() []relayMarkRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]relayMarkRecord(nil), s.webhookMarks...)
}

func (s *relayFakeStore) snapshotClaims() []relayClaimRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]relayClaimRecord(nil), s.claims...)
}

func (s *relayFakeStore) terminalState(id int64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.terminal[id]

	return state, ok
}

// relayFakePublisher is a TopicEventPublisher that records every request and can be made to
// fail. It resolves its result through the same helpers the real publisher uses, so an
// assertion on the result is an assertion about production behaviour.
type relayFakePublisher struct {
	mu sync.Mutex

	requests []PublishRequest
	err      error
	// failFor fails only the events whose id is listed, so a batch can have one bad row.
	failFor map[string]bool
	// beforePublish runs inside the publish, which is how a test observes concurrency.
	beforePublish func(req PublishRequest)
}

var _ TopicEventPublisher = (*relayFakePublisher)(nil)

func (p *relayFakePublisher) Publish(ctx context.Context, event model.LedgerEvent) error {
	_, err := p.PublishToTopic(ctx, PublishRequest{Event: event})

	return err
}

func (p *relayFakePublisher) PublishToTopic(_ context.Context, req PublishRequest) (PublishResult, error) {
	p.mu.Lock()
	hook := p.beforePublish
	p.mu.Unlock()

	if hook != nil {
		hook(req)
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
		MaxAttempts:  resolveMaxAttempts(req),
		Purpose:      resolvePurpose(req),
	}

	failure := p.err
	if failure == nil && p.failFor[req.Event.EventID] {
		failure = errRelayTransient
	}

	if failure != nil {
		result.Status = model.PublishStatusRetrying
		result.Transient = true
		result.Retryable = true
		result.Err = failure

		return result, failure
	}

	return result, nil
}

func (p *relayFakePublisher) Close() error { return nil }

func (p *relayFakePublisher) snapshotRequests() []PublishRequest {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]PublishRequest(nil), p.requests...)
}

func (p *relayFakePublisher) publishedIDs() []string {
	ids := make([]string, 0)
	for _, req := range p.snapshotRequests() {
		ids = append(ids, req.Event.EventID)
	}

	return ids
}

// relayFakeDeadLetterer records the rows the relay hands off on exhaustion.
type relayFakeDeadLetterer struct {
	mu sync.Mutex

	rows   []model.EventOutbox
	causes []error
	err    error
}

var _ eventRelayDeadLetterer = (*relayFakeDeadLetterer)(nil)

func (d *relayFakeDeadLetterer) DeadLetter(
	_ context.Context,
	row model.EventOutbox,
	cause error,
) (DeadLetterOutcome, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.rows = append(d.rows, row)
	d.causes = append(d.causes, cause)

	if d.err != nil {
		return DeadLetterOutcome{}, d.err
	}

	return DeadLetterOutcome{
		EventID:         row.EventID,
		EventType:       row.EventType,
		OriginalTopic:   row.Topic,
		DeadLetterTopic: DLTFor(row.Topic),
		Metadata:        BuildFailureMetadata(row, cause, row.Attempts, relayFixedNow),
		Published:       true,
		Status:          model.PublishStatusDeadLettered,
	}, nil
}

func (d *relayFakeDeadLetterer) snapshotRows() []model.EventOutbox {
	d.mu.Lock()
	defer d.mu.Unlock()

	return append([]model.EventOutbox(nil), d.rows...)
}

// relayLegacyEnqueue is one recorded legacy webhook enqueue.
type relayLegacyEnqueue struct {
	eventID string
	body    []byte
}

// relayFakeLegacy records the legacy dual-delivery leg. It disappears with the branch it
// stands in for at the webhook sunset.
type relayFakeLegacy struct {
	mu sync.Mutex

	enqueued []relayLegacyEnqueue
	err      error
}

var _ eventRelayLegacyTransport = (*relayFakeLegacy)(nil)

func (l *relayFakeLegacy) EnqueueLegacyWebhookDelivery(eventID string, body []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.err != nil {
		return l.err
	}

	l.enqueued = append(l.enqueued, relayLegacyEnqueue{
		eventID: eventID,
		body:    append([]byte(nil), body...),
	})

	return nil
}

func (l *relayFakeLegacy) snapshot() []relayLegacyEnqueue {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]relayLegacyEnqueue(nil), l.enqueued...)
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

// relayHarness is a processor wired to fakes, with the fakes kept to hand.
type relayHarness struct {
	processor   *EventRelayProcessor
	store       *relayFakeStore
	publisher   *relayFakePublisher
	deadLetters *relayFakeDeadLetterer
	legacy      *relayFakeLegacy
}

// newRelayHarness builds a processor over fakes, with the sunset in the future and the clock
// pinned. It sets the fields NewEventRelayProcessor would, which is what keeps these tests
// free of a database, a broker and a Redis connection.
func newRelayHarness(t *testing.T, rows ...model.EventOutbox) *relayHarness {
	t.Helper()

	store := newRelayFakeStore(rows...)
	publisher := &relayFakePublisher{}
	deadLetters := &relayFakeDeadLetterer{}
	legacy := &relayFakeLegacy{}

	processor := NewEventRelayProcessor(&Blnk{config: relayConfiguration()})
	processor.store = store
	processor.publisher = publisher
	processor.deadLetters = deadLetters
	processor.legacy = legacy
	processor.now = func() time.Time { return relayFixedNow }
	processor.sunsetPassed = func(time.Time) bool { return false }

	return &relayHarness{
		processor:   processor,
		store:       store,
		publisher:   publisher,
		deadLetters: deadLetters,
		legacy:      legacy,
	}
}

// relayConfiguration is a configuration carrying the documented relay defaults.
func relayConfiguration() *config.Configuration {
	return &config.Configuration{
		Relay: config.RelayConfig{
			MaxRetryAttempts:   5,
			RetryBaseBackoffMS: 1000,
			RetryMaxBackoffMS:  30000,
		},
	}
}

// relayRow builds a claimable outbox row whose payload is a real legacy webhook body, so the
// dual-delivery assertions compare the bytes production would carry.
func relayRow(id int64, eventID, eventType, partitionKey string, occurredAt time.Time) model.EventOutbox {
	payload, err := json.Marshal(NewWebhook{
		Event:   eventType,
		Payload: map[string]any{"transaction_id": eventID, "amount": 1250},
	})
	if err != nil {
		panic(err)
	}

	return model.EventOutbox{
		ID:            id,
		EventID:       eventID,
		EventType:     eventType,
		AggregateID:   partitionKey,
		PartitionKey:  partitionKey,
		Topic:         TopicForEvent(eventType),
		SchemaVersion: model.SchemaVersionV1,
		Payload:       payload,
		OccurredAt:    occurredAt,
		Status:        model.EventOutboxStatusPending,
		MaxAttempts:   5,
	}
}

// relayTransactionRow is the common case: a transaction event on one ledger.
func relayTransactionRow(id int64, eventID string) model.EventOutbox {
	return relayRow(id, eventID, "transaction.applied", "ldg_relay_1", relayFixedNow.Add(time.Duration(id)*time.Second))
}

// relayReadOwnSource reads event_relay.go so a test can assert the ABSENCE of something.
//
// Reading the source is the only way to express "this decision is not duplicated here": a
// second sunset comparison, or a date parse, would be invisible to a behavioural test as long
// as it happened to agree with event_sunset.go — and the whole point of single ownership is
// that it must keep agreeing after somebody changes one of them.
func relayReadOwnSource(t *testing.T) string {
	t.Helper()

	path := filepath.Join(moduleRootDir(t), "event_relay.go")
	contents, err := os.ReadFile(path) //nolint:gosec // a fixed, repository-relative path
	require.NoError(t, err, "%s must be readable to assert its structure", path)

	return string(contents)
}

// relayEntriesWithMessage collects every log entry whose message contains the needle.
func relayEntriesWithMessage(hook *logtest.Hook, needle string) []*logrus.Entry {
	matches := make([]*logrus.Entry, 0)
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, needle) {
			matches = append(matches, entry)
		}
	}

	return matches
}

// ---------------------------------------------------------------------------
// The retry schedule (requirement R-4)
// ---------------------------------------------------------------------------

// TestRelayRetryPolicy_DefaultScheduleIsExactlyOneTwoFourEightSixteenSeconds pins the
// mandated schedule value by value.
//
// "Roughly increasing" is not the requirement and would not survive the mutation gate: a
// multiplier of 3, a base of 500ms or an off-by-one on the attempt index all produce a
// monotonically growing schedule and all violate R-4.
func TestRelayRetryPolicy_DefaultScheduleIsExactlyOneTwoFourEightSixteenSeconds(t *testing.T) {
	policy := newRelayRetryPolicy(relayConfiguration().Relay)

	require.Equal(t, 5, policy.maxAttempts, "the default retry budget is five attempts")

	expected := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
	}

	for index, want := range expected {
		attempt := index + 1
		assert.Equal(t, want, policy.backoffFor(attempt),
			"the delay after attempt %d must be exactly %s", attempt, want)
	}

	assert.Less(t, policy.backoffFor(policy.maxAttempts), policy.maxBackoff,
		"the 30s cap is deliberately never reached within five attempts; do not 'fix' the "+
			"multiplier or the cap to make it engage")
}

// TestRelayRetryPolicy_AppliesTheCapWhenTheConfiguredScheduleWouldExceedIt asserts the cap
// engages exactly when configuration makes it relevant — which is the only reason it exists.
func TestRelayRetryPolicy_AppliesTheCapWhenTheConfiguredScheduleWouldExceedIt(t *testing.T) {
	policy := newRelayRetryPolicy(config.RelayConfig{
		MaxRetryAttempts:   5,
		RetryBaseBackoffMS: 10000,
		RetryMaxBackoffMS:  30000,
	})

	assert.Equal(t, 10*time.Second, policy.backoffFor(1), "the first delay is the configured base")
	assert.Equal(t, 20*time.Second, policy.backoffFor(2), "the second doubles, still under the cap")
	assert.Equal(t, 30*time.Second, policy.backoffFor(3), "the third would be 40s and is capped")
	assert.Equal(t, 30*time.Second, policy.backoffFor(4), "every later delay stays at the cap")
	assert.Equal(t, 30*time.Second, policy.backoffFor(500),
		"an absurd attempt number must stay at the cap rather than overflow")
}

// TestRelayRetryPolicy_BaseAboveTheCapYieldsTheCap covers the inverted window: an operator
// gets the bounded schedule rather than a refusal to start.
func TestRelayRetryPolicy_BaseAboveTheCapYieldsTheCap(t *testing.T) {
	policy := newRelayRetryPolicy(config.RelayConfig{
		MaxRetryAttempts:   3,
		RetryBaseBackoffMS: 60000,
		RetryMaxBackoffMS:  30000,
	})

	for attempt := 1; attempt <= 3; attempt++ {
		assert.Equal(t, 30*time.Second, policy.backoffFor(attempt),
			"a base above the cap must be bounded by it on attempt %d", attempt)
	}
}

// TestNewRelayRetryPolicy_FallsBackToTheDocumentedDefaults asserts the second line of
// defence for a Configuration built directly, which is how the whole existing test suite
// constructs one. Zero-length delays would spend the entire budget inside one poll interval.
func TestNewRelayRetryPolicy_FallsBackToTheDocumentedDefaults(t *testing.T) {
	for name, cfg := range map[string]config.RelayConfig{
		"zero valued": {},
		"negative":    {MaxRetryAttempts: -3, RetryBaseBackoffMS: -1, RetryMaxBackoffMS: -1},
	} {
		policy := newRelayRetryPolicy(cfg)

		assert.Equal(t, config.MaxRelayRetryAttempts, policy.maxAttempts, "%s: attempts", name)
		assert.Equal(t, 1*time.Second, policy.baseBackoff, "%s: base backoff", name)
		assert.Equal(t, 30*time.Second, policy.maxBackoff, "%s: max backoff", name)
		assert.Equal(t, 1*time.Second, policy.backoffFor(1), "%s: first delay", name)
		assert.Equal(t, 16*time.Second, policy.backoffFor(5), "%s: fifth delay", name)
	}
}

// TestRelayRetryPolicy_HonoursConfiguredValues asserts configuration is used, not ignored.
func TestRelayRetryPolicy_HonoursConfiguredValues(t *testing.T) {
	policy := newRelayRetryPolicy(config.RelayConfig{
		MaxRetryAttempts:   3,
		RetryBaseBackoffMS: 250,
		RetryMaxBackoffMS:  2000,
	})

	assert.Equal(t, 3, policy.maxAttempts)
	assert.Equal(t, 250*time.Millisecond, policy.backoffFor(1))
	assert.Equal(t, 500*time.Millisecond, policy.backoffFor(2))
	assert.Equal(t, 1*time.Second, policy.backoffFor(3))
	assert.Equal(t, 2*time.Second, policy.backoffFor(4), "capped at the configured maximum")
}

// TestRelayRetryPolicy_TreatsAnAttemptBelowOneAsTheFirst asserts the guard: returning zero
// would collapse the schedule into immediate retries.
func TestRelayRetryPolicy_TreatsAnAttemptBelowOneAsTheFirst(t *testing.T) {
	policy := newRelayRetryPolicy(relayConfiguration().Relay)

	assert.Equal(t, 1*time.Second, policy.backoffFor(0))
	assert.Equal(t, 1*time.Second, policy.backoffFor(-7))
}

// ---------------------------------------------------------------------------
// Construction and the refusal to run without a usable transport
// ---------------------------------------------------------------------------

// TestNewEventRelayProcessor_UsesTheHouseDefaults asserts the processor is born with
// LineageOutboxProcessor's geometry, which is what makes the two readable side by side.
func TestNewEventRelayProcessor_UsesTheHouseDefaults(t *testing.T) {
	processor := NewEventRelayProcessor(&Blnk{config: relayConfiguration()})

	require.NotNil(t, processor)
	assert.Equal(t, 100, processor.batchSize, "batch size matches the house relay")
	assert.Equal(t, 1*time.Second, processor.pollInterval, "poll interval matches the house relay")
	assert.Equal(t, 30*time.Second, processor.lockDuration, "lock duration matches the house relay")
	assert.Equal(t, defaultEventRelayConcurrency, processor.concurrency)
	assert.NotNil(t, processor.stopCh, "stopCh must exist before Start")
	assert.NotNil(t, processor.now, "the clock must always be set")
	assert.False(t, processor.IsRunning())

	assert.Equal(t, 5, processor.retry.maxAttempts, "the retry schedule comes from configuration")
	assert.Equal(t, 1*time.Second, processor.retry.baseBackoff)
	assert.Equal(t, 30*time.Second, processor.retry.maxBackoff)
}

// TestNewEventRelayProcessor_SunsetDecisionComesFromEventSunsetOnly asserts the relay reads
// the ONE decision point rather than comparing instants itself. Two comparisons could
// disagree, and the pair that would disagree is "stop dual-writing" and "answer 410 Gone".
func TestNewEventRelayProcessor_SunsetDecisionComesFromEventSunsetOnly(t *testing.T) {
	processor := NewEventRelayProcessor(&Blnk{config: relayConfiguration()})

	require.NotNil(t, processor.sunsetPassed, "the sunset predicate must always be wired")

	now := time.Now()
	assert.Equal(t, WebhookSunsetPassed(now), processor.sunsetPassed(now),
		"the relay's decision must be event_sunset.go's decision, not a second comparison")

	source := relayReadOwnSource(t)
	assert.NotContains(t, source, "WebhookDeprecationSunsetDate",
		"the relay must never read the configured sunset date; event_sunset.go owns it")
	assert.NotContains(t, source, "time.Parse",
		"the relay must never parse the sunset date; event_sunset.go owns it")
}

// TestNewEventRelayProcessor_IsNilSafe asserts a processor built from nothing is a value that
// refuses to work rather than a panic waiting in a ledger process.
func TestNewEventRelayProcessor_IsNilSafe(t *testing.T) {
	processor := NewEventRelayProcessor(nil)
	require.NotNil(t, processor)

	assert.Error(t, processor.startupObstacle(), "a processor without an instance cannot run")
	assert.Equal(t, config.MaxRelayRetryAttempts, processor.retry.maxAttempts,
		"the schedule is still resolved so the value is fully formed")

	assert.NotPanics(t, func() {
		processor.Start(context.Background())
		processor.Stop()
	})
	assert.False(t, processor.IsRunning(), "a refused Start must not report as running")

	var absent *EventRelayProcessor
	assert.NotPanics(t, func() {
		absent.Stop()
		_ = absent.IsRunning()
	}, "a nil processor must be safe to stop and inspect")
	assert.False(t, absent.IsRunning())
}

// TestEventRelayProcessor_RefusesToStartWithoutAUsableTransport is the data-loss guard.
//
// The no-op publisher case is the important one: it reports every publish as dispatched, so a
// relay holding it would mark an entire outbox dispatched having sent nothing — every event
// silently retired, with no error, no dead letter and nothing to replay from.
func TestEventRelayProcessor_RefusesToStartWithoutAUsableTransport(t *testing.T) {
	cases := map[string]struct {
		mutate func(p *EventRelayProcessor)
		needle string
	}{
		"no datasource": {
			mutate: func(p *EventRelayProcessor) { p.store = nil },
			needle: "no datasource",
		},
		"no publisher": {
			mutate: func(p *EventRelayProcessor) { p.publisher = nil },
			needle: "no Kafka publisher",
		},
		"the no-op publisher": {
			mutate: func(p *EventRelayProcessor) { p.publisher = NewNoopEventPublisher() },
			needle: "no-op event publisher",
		},
		"no dead-letter service": {
			mutate: func(p *EventRelayProcessor) { p.deadLetters = nil },
			needle: "no dead-letter service",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			hook := logtest.NewGlobal()
			defer hook.Reset()

			harness := newRelayHarness(t, relayTransactionRow(1, "evt-1"))
			testCase.mutate(harness.processor)

			obstacle := harness.processor.startupObstacle()
			require.Error(t, obstacle, "the relay must refuse to run")
			assert.Contains(t, obstacle.Error(), testCase.needle,
				"the refusal must name what is missing")

			harness.processor.Start(context.Background())
			t.Cleanup(harness.processor.Stop)

			assert.False(t, harness.processor.IsRunning(), "a refused Start must not run")
			assert.Empty(t, harness.store.snapshotClaims(),
				"a refused relay must not claim a single row")
			assert.Empty(t, harness.store.snapshotDispatched(),
				"nothing may be marked dispatched by a relay that cannot publish")

			require.NotEmpty(t, relayEntriesWithMessage(hook, "Event outbox relay not started"),
				"the refusal must be logged, not silent")
		})
	}
}

// TestEventRelayProcessor_AdoptsTheProcessPublisherAndBuildsOneDeadLetterService asserts the
// constructor wires the collaborators once, which is what the dead-letter service's own
// documentation requires of a caller in a loop.
func TestEventRelayProcessor_AdoptsTheProcessPublisherAndBuildsOneDeadLetterService(t *testing.T) {
	publisher := &relayFakePublisher{}
	instance := &Blnk{config: relayConfiguration(), events: publisher}

	processor := NewEventRelayProcessor(instance)

	assert.Same(t, publisher, processor.publisher, "the relay must reuse the process publisher")
	require.NotNil(t, processor.deadLetters, "a dead-letter service must be built once, here")
	assert.Equal(t, instance, processor.legacy, "the legacy leg is the Blnk instance itself")

	minimal := &Blnk{config: relayConfiguration(), events: relayMinimalPublisher{}}
	bare := NewEventRelayProcessor(minimal)
	assert.Nil(t, bare.publisher,
		"a publisher that cannot be given a destination topic must not be adopted")
	assert.Nil(t, bare.deadLetters,
		"no dead-letter service without a publisher to write with")
}

// relayMinimalPublisher satisfies only the mandated minimal contract, which cannot express a
// destination topic. It exists to prove the constructor's narrowing.
type relayMinimalPublisher struct{}

func (relayMinimalPublisher) Publish(context.Context, model.LedgerEvent) error { return nil }

// TestEventRelayProcessor_ConfiguratorsChainAndRejectNonPositiveValues asserts the fluent
// configurators behave like the house ones and cannot be used to disable the relay.
func TestEventRelayProcessor_ConfiguratorsChainAndRejectNonPositiveValues(t *testing.T) {
	processor := NewEventRelayProcessor(&Blnk{config: relayConfiguration()})

	returned := processor.
		WithBatchSize(25).
		WithPollInterval(250 * time.Millisecond).
		WithLockDuration(10 * time.Second).
		WithConcurrency(3)

	assert.Same(t, processor, returned, "every configurator must return the processor")
	assert.Equal(t, 25, processor.batchSize)
	assert.Equal(t, 250*time.Millisecond, processor.pollInterval)
	assert.Equal(t, 10*time.Second, processor.lockDuration)
	assert.Equal(t, 3, processor.concurrency)

	processor.WithBatchSize(0).WithPollInterval(0).WithLockDuration(-1).WithConcurrency(0)

	assert.Equal(t, 25, processor.batchSize, "a zero batch would publish nothing")
	assert.Equal(t, 250*time.Millisecond, processor.pollInterval, "a zero interval panics a ticker")
	assert.Equal(t, 10*time.Second, processor.lockDuration, "an expired lease is a correctness bug")
	assert.Equal(t, 3, processor.concurrency, "a zero permit count would stall the relay")
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// TestEventRelayProcessor_StartIsIdempotent asserts the double-start guard: a second Start
// must not spawn a second loop, which would double every claim.
func TestEventRelayProcessor_StartIsIdempotent(t *testing.T) {
	harness := newRelayHarness(t)
	harness.processor.WithPollInterval(5 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness.processor.Start(ctx)
	harness.processor.Start(ctx)
	harness.processor.Start(ctx)

	require.True(t, harness.processor.IsRunning())

	harness.processor.Stop()
	assert.False(t, harness.processor.IsRunning(), "Stop must clear the running flag")

	// A second loop would still be claiming after the first was stopped.
	before := len(harness.store.snapshotClaims())
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, before, len(harness.store.snapshotClaims()),
		"no goroutine may survive Stop; a second Start must not have spawned one")
}

// TestEventRelayProcessor_StopBeforeStartIsSafe asserts Stop on a processor that was never
// started neither panics nor blocks — the call site defers Stop unconditionally.
func TestEventRelayProcessor_StopBeforeStartIsSafe(t *testing.T) {
	harness := newRelayHarness(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		harness.processor.Stop()
		harness.processor.Stop()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked on a processor that was never started")
	}

	assert.False(t, harness.processor.IsRunning())
}

// TestEventRelayProcessor_StartAfterStopRunsAgain is the stopCh re-creation test. Without it
// the loop would exit immediately on a channel that is already closed, and the relay would
// look started while doing nothing.
func TestEventRelayProcessor_StartAfterStopRunsAgain(t *testing.T) {
	harness := newRelayHarness(t)
	harness.processor.WithPollInterval(5 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness.processor.Start(ctx)
	require.Eventually(t, func() bool { return len(harness.store.snapshotClaims()) > 0 },
		2*time.Second, 5*time.Millisecond, "the first run must poll")
	harness.processor.Stop()

	claimsAfterFirstRun := len(harness.store.snapshotClaims())

	harness.processor.Start(ctx)
	require.True(t, harness.processor.IsRunning(), "a restarted relay must report as running")
	require.Eventually(t, func() bool {
		return len(harness.store.snapshotClaims()) > claimsAfterFirstRun
	}, 2*time.Second, 5*time.Millisecond, "a restarted relay must poll again")

	harness.processor.Stop()
}

// TestEventRelayProcessor_ContextCancellationStopsTheLoop asserts the ctx.Done arm of the
// select is live, so a process shutdown ends the relay without anybody calling Stop.
func TestEventRelayProcessor_ContextCancellationStopsTheLoop(t *testing.T) {
	harness := newRelayHarness(t)
	harness.processor.WithPollInterval(5 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	harness.processor.Start(ctx)

	require.Eventually(t, func() bool { return len(harness.store.snapshotClaims()) > 0 },
		2*time.Second, 5*time.Millisecond, "the relay must be polling before cancellation")

	cancel()

	require.Eventually(t, func() bool {
		before := len(harness.store.snapshotClaims())
		time.Sleep(25 * time.Millisecond)

		return len(harness.store.snapshotClaims()) == before
	}, 2*time.Second, 25*time.Millisecond, "cancelling the context must stop the polling")

	// Stop remains safe and must not hang, even though the loop has already returned.
	harness.processor.Stop()
	assert.False(t, harness.processor.IsRunning())
}

// TestEventRelayProcessor_StopWaitsForInFlightWork asserts Stop's promise: it returns only
// after the batch in flight has finished, so a shutdown never abandons a row mid-publish.
func TestEventRelayProcessor_StopWaitsForInFlightWork(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-inflight"))
	harness.processor.WithPollInterval(5 * time.Millisecond)

	hit, release := harness.store.withClaimGate()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness.processor.Start(ctx)

	select {
	case <-hit:
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never reached the claim")
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		harness.processor.Stop()
	}()

	select {
	case <-stopped:
		t.Fatal("Stop returned while a batch was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	release()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after the in-flight batch completed")
	}

	assert.Len(t, harness.publisher.snapshotRequests(), 1,
		"the in-flight row must have been published before Stop returned")
}

// ---------------------------------------------------------------------------
// Claiming, publishing and marking
// ---------------------------------------------------------------------------

// TestProcessBatch_PublishesAndMarksEveryClaimedRow is the happy path, and it also pins the
// claim geometry: the batch size and lease the relay asks for are the configured ones.
func TestProcessBatch_PublishesAndMarksEveryClaimedRow(t *testing.T) {
	harness := newRelayHarness(t,
		relayTransactionRow(1, "evt-1"),
		relayTransactionRow(2, "evt-2"),
		relayTransactionRow(3, "evt-3"),
	)
	harness.processor.WithBatchSize(10).WithLockDuration(30 * time.Second)

	claimed := harness.processor.processBatch(context.Background())

	assert.Equal(t, 3, claimed, "every due row must be claimed")
	assert.Equal(t, []string{"evt-1", "evt-2", "evt-3"}, harness.publisher.publishedIDs(),
		"every claimed row must be published, in occurrence order")

	dispatched := harness.store.snapshotDispatched()
	require.Len(t, dispatched, 3, "every published row must be marked dispatched")
	for _, record := range dispatched {
		assert.Equal(t, "token-1", record.claimToken,
			"every transition must present the token the claim issued")
	}

	for id := int64(1); id <= 3; id++ {
		state, done := harness.store.terminalState(id)
		assert.True(t, done, "row %d must have reached a terminal state", id)
		assert.Equal(t, model.EventOutboxStatusDispatched, state)
	}

	claims := harness.store.snapshotClaims()
	require.Len(t, claims, 1)
	assert.Equal(t, 10, claims[0].batchSize, "the configured batch size must reach the claim")
	assert.Equal(t, 30*time.Second, claims[0].lockDuration, "the configured lease must reach the claim")

	assert.Empty(t, harness.store.snapshotFailures(), "a successful publish records no failure")
	assert.Empty(t, harness.deadLetters.snapshotRows(), "nothing may be dead-lettered")
}

// TestProcessBatch_BuildsThePublishRequestFromTheStoredRow asserts every field the publisher
// attributes its telemetry by, and that the envelope mirrors the row rather than being
// rebuilt from a domain object.
func TestProcessBatch_BuildsThePublishRequestFromTheStoredRow(t *testing.T) {
	row := relayTransactionRow(7, "evt-request")
	row.Attempts = 2
	row.MaxAttempts = 5
	row.Topic = "blnk.transactions"

	harness := newRelayHarness(t, row)

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	requests := harness.publisher.snapshotRequests()
	require.Len(t, requests, 1)
	request := requests[0]

	assert.Equal(t, "blnk.transactions", request.Topic,
		"the destination comes from the row, so a stored row keeps its original routing")
	assert.Equal(t, row.PartitionKey, request.Key,
		"the key is the row's partition key — the value the claim serialised dispatch on")
	assert.Equal(t, 3, request.Attempt,
		"attempts counts recorded failures, so the attempt now under way is one past it")
	assert.Equal(t, 5, request.MaxAttempts, "the row's budget must reach the publisher")
	assert.Equal(t, PublishPurposeOriginal, request.Purpose,
		"a relay publish is a first delivery and must not pollute the replay population")
	assert.Equal(t, relayFixedNow, request.ClaimedAt,
		"the claim instant must reach the publisher so the histogram measures claim to ack")

	assert.Equal(t, row.EventID, request.Event.EventID)
	assert.Equal(t, row.EventType, request.Event.EventType)
	assert.Equal(t, row.AggregateID, request.Event.AggregateID)
	assert.Equal(t, row.OccurredAt, request.Event.OccurredAt)
	assert.Equal(t, model.SchemaVersionV1, request.Event.SchemaVersion)
	assert.JSONEq(t, string(row.Payload), string(request.Event.Payload),
		"the payload must be the stored bytes, not a re-marshalled struct")
	assert.Equal(t, []byte(row.Payload), []byte(request.Event.Payload),
		"and byte-identical, because that is what the dual-delivery guarantee rests on")
}

// TestProcessBatch_ReturnsQuietlyWhenThereIsNothingToDo asserts an empty backlog is silent,
// which matters at a one-second poll interval.
func TestProcessBatch_ReturnsQuietlyWhenThereIsNothingToDo(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newRelayHarness(t)

	assert.Zero(t, harness.processor.processBatch(context.Background()))
	assert.Empty(t, harness.publisher.snapshotRequests())
	assert.Empty(t, relayEntriesWithMessage(hook, "Processing"),
		"an empty batch must not log; a poll every second would flood the log")
}

// TestProcessBatch_LogsAClaimFailureAndReportsNoWork asserts a failing database is loud and
// cannot make one tick spin through fifty empty batches.
func TestProcessBatch_LogsAClaimFailureAndReportsNoWork(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newRelayHarness(t, relayTransactionRow(1, "evt-1"))
	harness.store.claimErr = errors.New("relay test: connection refused")

	assert.Zero(t, harness.processor.processBatch(context.Background()),
		"a claim error must report no work so the tick does not chain")
	assert.Empty(t, harness.publisher.snapshotRequests())

	entries := relayEntriesWithMessage(hook, "failed to claim event outbox entries")
	require.NotEmpty(t, entries, "a claim failure must be logged")
	assert.Equal(t, logrus.ErrorLevel, entries[0].Level)
}

// TestProcessTick_ChainsFullBatchesAndStopsOnAShortOne asserts the throughput mechanism: one
// tick drains the backlog instead of publishing one batch per poll interval.
func TestProcessTick_ChainsFullBatchesAndStopsOnAShortOne(t *testing.T) {
	rows := make([]model.EventOutbox, 0, 5)
	for id := int64(1); id <= 5; id++ {
		rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-%d", id)))
	}

	harness := newRelayHarness(t, rows...)
	harness.processor.WithBatchSize(2)

	harness.processor.processTick(context.Background())

	assert.Len(t, harness.publisher.snapshotRequests(), 5,
		"one tick must drain the backlog rather than stopping after one batch")

	claims := harness.store.snapshotClaims()
	assert.Len(t, claims, 3,
		"two full batches must chain and the short third must end the tick")
}

// TestProcessTick_HonoursTheStopSignalBetweenBatches asserts a shutdown is not delayed by up
// to fifty more batches.
func TestProcessTick_HonoursTheStopSignalBetweenBatches(t *testing.T) {
	rows := make([]model.EventOutbox, 0, 6)
	for id := int64(1); id <= 6; id++ {
		rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-%d", id)))
	}

	harness := newRelayHarness(t, rows...)
	harness.processor.WithBatchSize(2)

	// Stop the processor's channel after the first batch by closing it from inside a publish.
	var published atomic.Int32
	harness.publisher.beforePublish = func(PublishRequest) {
		if published.Add(1) == 2 {
			close(harness.processor.stopCh)
		}
	}

	harness.processor.processTick(context.Background())

	assert.Len(t, harness.publisher.snapshotRequests(), 2,
		"the tick must stop chaining once the stop signal is visible")
	assert.Len(t, harness.store.snapshotDispatched(), 2,
		"and the batch it had already claimed must be finished, not abandoned mid-flight")
}

// TestProcessBatch_AGracefulStopFinishesTheClaimedBatch asserts the asymmetry between the two
// shutdown signals. A batch this relay already leased is the "work in flight" Stop promises to
// wait for; abandoning it would strand those events for the whole lease duration on every
// deploy.
func TestProcessBatch_AGracefulStopFinishesTheClaimedBatch(t *testing.T) {
	rows := make([]model.EventOutbox, 0, 4)
	for id := int64(1); id <= 4; id++ {
		rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-%d", id)))
	}

	harness := newRelayHarness(t, rows...)
	harness.processor.WithBatchSize(4).WithConcurrency(1)

	// Stop is signalled while the batch is being published.
	var published atomic.Int32
	harness.publisher.beforePublish = func(PublishRequest) {
		if published.Add(1) == 1 {
			close(harness.processor.stopCh)
		}
	}

	require.Equal(t, 4, harness.processor.processBatch(context.Background()))

	assert.Len(t, harness.publisher.snapshotRequests(), 4,
		"a graceful stop must let the claimed batch finish rather than stranding it for a lease")
	assert.Len(t, harness.store.snapshotDispatched(), 4)
}

// TestProcessBatch_AnAbortStopsDispatchingImmediately is the other half of that asymmetry: a
// cancelled context is an abort, and the rows not yet started keep their lease.
func TestProcessBatch_AnAbortStopsDispatchingImmediately(t *testing.T) {
	rows := make([]model.EventOutbox, 0, 6)
	for id := int64(1); id <= 6; id++ {
		rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-%d", id)))
	}

	harness := newRelayHarness(t, rows...)
	harness.processor.WithBatchSize(6).WithConcurrency(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var published atomic.Int32
	harness.publisher.beforePublish = func(PublishRequest) {
		if published.Add(1) == 2 {
			cancel()
		}
	}

	harness.processor.processBatch(ctx)

	requests := harness.publisher.snapshotRequests()
	assert.Len(t, requests, 2, "an abort must stop dispatching rows that have not started")
	assert.Len(t, harness.store.snapshotDispatched(), 2,
		"and the bookkeeping for what WAS published must still be recorded, on the detached context")
}

// TestProcessTick_BoundsTheNumberOfBatchesOneTickChains asserts the ceiling exists, so a
// large backfill cannot starve the ticker.
func TestProcessTick_BoundsTheNumberOfBatchesOneTickChains(t *testing.T) {
	total := int64(maxEventRelayBatchesPerTick + 10)
	rows := make([]model.EventOutbox, 0, total)
	for id := int64(1); id <= total; id++ {
		rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-%d", id)))
	}

	harness := newRelayHarness(t, rows...)
	harness.processor.WithBatchSize(1)

	harness.processor.processTick(context.Background())

	assert.Len(t, harness.store.snapshotClaims(), maxEventRelayBatchesPerTick,
		"one tick must chain at most maxEventRelayBatchesPerTick batches")
	assert.Len(t, harness.publisher.snapshotRequests(), maxEventRelayBatchesPerTick)
}

// ---------------------------------------------------------------------------
// Ordering (acceptance criterion V-6)
// ---------------------------------------------------------------------------

// TestProcessBatch_PreservesPerAggregateOrderUnderConcurrency is the ordering guarantee in
// the one arrangement that could break it: several aggregates interleaved in one batch,
// published concurrently.
//
// It asserts the property that actually matters — per-aggregate order — rather than global
// order, because global order is not what partitioned Kafka provides and pinning it would
// forbid the concurrency the throughput requirement needs.
func TestProcessBatch_PreservesPerAggregateOrderUnderConcurrency(t *testing.T) {
	const perAggregate = 6

	aggregates := []string{"ldg_a", "ldg_b", "ldg_c", "ldg_d"}

	var (
		rows     []model.EventOutbox
		expected = map[string][]string{}
	)

	// Interleaved on purpose: a[0], b[0], c[0], d[0], a[1], b[1], … so a naive
	// round-robin across workers would reorder every aggregate.
	id := int64(0)
	for index := range perAggregate {
		for _, aggregate := range aggregates {
			id++
			eventID := fmt.Sprintf("%s-%d", aggregate, index)
			rows = append(rows, relayRow(
				id, eventID, "transaction.applied", aggregate,
				relayFixedNow.Add(time.Duration(id)*time.Millisecond),
			))
			expected[aggregate] = append(expected[aggregate], eventID)
		}
	}

	harness := newRelayHarness(t, rows...)
	harness.processor.WithBatchSize(len(rows)).WithConcurrency(len(aggregates))

	// Publishing at staggered speeds is what turns a latent ordering bug into a failure: a
	// slow first event for one aggregate would let its successor overtake it.
	harness.publisher.beforePublish = func(req PublishRequest) {
		if strings.HasSuffix(req.Event.EventID, "-0") {
			time.Sleep(5 * time.Millisecond)
		}
	}

	require.Equal(t, len(rows), harness.processor.processBatch(context.Background()))

	observed := map[string][]string{}
	for _, req := range harness.publisher.snapshotRequests() {
		observed[req.Key] = append(observed[req.Key], req.Event.EventID)
	}

	for _, aggregate := range aggregates {
		assert.Equal(t, expected[aggregate], observed[aggregate],
			"aggregate %s must be published in occurrence order", aggregate)
	}

	assert.Len(t, harness.store.snapshotDispatched(), len(rows),
		"concurrency must not lose a row")
}

// TestGroupEventRowsByPartitionKey_KeepsClaimOrderAndSeparatesKeys asserts the local half of
// the ordering guarantee directly, so it holds even if the claim query's one-row-per-key
// predicate were ever weakened.
func TestGroupEventRowsByPartitionKey_KeepsClaimOrderAndSeparatesKeys(t *testing.T) {
	rows := []model.EventOutbox{
		relayRow(1, "a-1", "transaction.applied", "ldg_a", relayFixedNow),
		relayRow(2, "b-1", "transaction.applied", "ldg_b", relayFixedNow.Add(time.Second)),
		relayRow(3, "a-2", "transaction.applied", "ldg_a", relayFixedNow.Add(2*time.Second)),
		relayRow(4, "c-1", "transaction.applied", "ldg_c", relayFixedNow.Add(3*time.Second)),
		relayRow(5, "a-3", "transaction.applied", "ldg_a", relayFixedNow.Add(4*time.Second)),
	}

	groups := groupEventRowsByPartitionKey(rows)

	require.Len(t, groups, 3, "one group per distinct partition key")
	assert.Equal(t, []string{"a-1", "a-2", "a-3"}, relayGroupEventIDs(groups[0]),
		"rows sharing a key stay together, in claim order, so one goroutine publishes them")
	assert.Equal(t, []string{"b-1"}, relayGroupEventIDs(groups[1]))
	assert.Equal(t, []string{"c-1"}, relayGroupEventIDs(groups[2]))

	assert.Equal(t, "ldg_a", groups[0][0].PartitionKey,
		"groups are ordered by first appearance, keeping the batch's FIFO shape")
}

// TestGroupEventRowsByPartitionKey_KeepsUnkeyedRowsApart asserts a blank key does not collapse
// unrelated rows into one serialised group. A persisted row always has a key, so this covers
// the case where the data is already wrong.
func TestGroupEventRowsByPartitionKey_KeepsUnkeyedRowsApart(t *testing.T) {
	first := relayRow(11, "u-1", "transaction.applied", "", relayFixedNow)
	second := relayRow(12, "u-2", "transaction.applied", "", relayFixedNow.Add(time.Second))

	groups := groupEventRowsByPartitionKey([]model.EventOutbox{first, second})

	require.Len(t, groups, 2, "two unkeyed rows must not be forced into one group")
	assert.Equal(t, []string{"u-1"}, relayGroupEventIDs(groups[0]))
	assert.Equal(t, []string{"u-2"}, relayGroupEventIDs(groups[1]))

	assert.Empty(t, groupEventRowsByPartitionKey(nil), "no rows, no groups")
}

func relayGroupEventIDs(group []model.EventOutbox) []string {
	ids := make([]string, 0, len(group))
	for _, row := range group {
		ids = append(ids, row.EventID)
	}

	return ids
}

// ---------------------------------------------------------------------------
// Per-attempt logging and the retry schedule in force (requirement R-4)
// ---------------------------------------------------------------------------

// TestProcessRow_LogsEveryFailedAttemptWithTheFiveRequiredFields is requirement R-4's log
// contract: a line for EVERY attempt including the first, not only for the final failure, and
// each carrying the attempt, the maximum attempts, the error, the event id and the topic.
func TestProcessRow_LogsEveryFailedAttemptWithTheFiveRequiredFields(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newRelayHarness(t, relayTransactionRow(1, "evt-logged"))
	harness.publisher.err = errRelayTransient

	// Five attempts, each a separate claim, because the backoff is durable rather than a
	// sleep: the row returns to the claimable set with a due instant, and the fake's clock
	// makes it due again.
	for attempt := 1; attempt <= 5; attempt++ {
		harness.store.now = func() time.Time { return relayFixedNow.Add(time.Hour * time.Duration(attempt)) }
		harness.processor.processBatch(context.Background())
	}

	entries := relayEntriesWithMessage(hook, "publishing a ledger event failed")
	require.Len(t, entries, 5,
		"every attempt must be logged, including the first — not only the final failure")

	for index, entry := range entries {
		attempt := index + 1

		assert.Equal(t, attempt, entry.Data["attempt"],
			"line %d must name the attempt it describes", attempt)
		assert.Equal(t, 5, entry.Data["max_attempts"],
			"line %d must state the budget so 'attempt 3 of 5' is readable", attempt)
		assert.Equal(t, "evt-logged", entry.Data["event_id"],
			"line %d must name the event an operator has to look at", attempt)
		assert.Equal(t, "blnk.transactions", entry.Data["topic"],
			"line %d must name the destination", attempt)
		require.Contains(t, entry.Data, "error", "line %d must carry the error reason", attempt)
		assert.Contains(t, fmt.Sprint(entry.Data["error"]), "broker unavailable",
			"line %d must carry the reason itself, not a placeholder", attempt)
	}
}

// TestProcessRow_SchedulesTheConfiguredBackoffOnEveryFailure asserts the durable schedule: the
// delay handed to the repository is the configured one for that attempt, so the row is not
// claimable again until it is due.
func TestProcessRow_SchedulesTheConfiguredBackoffOnEveryFailure(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-backoff"))
	harness.publisher.err = errRelayTransient

	for attempt := 1; attempt <= 5; attempt++ {
		harness.store.now = func() time.Time { return relayFixedNow.Add(time.Hour * time.Duration(attempt)) }
		harness.processor.processBatch(context.Background())
	}

	failures := harness.store.snapshotFailures()
	require.Len(t, failures, 5, "five attempts must record five failures")

	expected := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
	}
	for index, want := range expected {
		assert.Equal(t, want, failures[index].retryAfter,
			"attempt %d must schedule its next attempt %s later", index+1, want)
	}

	for _, failure := range failures {
		assert.Contains(t, failure.reason, "broker unavailable",
			"the reason stored in last_error must name the failure")
	}
}

// TestProcessRow_DoesNotRetryInProcess asserts one publish attempt per claim. Sleeping the
// schedule here would outlive the 30-second lease and let a second instance republish the row
// mid-sleep.
func TestProcessRow_DoesNotRetryInProcess(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-single-attempt"))
	harness.publisher.err = errRelayTransient

	started := time.Now()
	harness.processor.processBatch(context.Background())
	elapsed := time.Since(started)

	assert.Len(t, harness.publisher.snapshotRequests(), 1,
		"one claim must produce exactly one publish attempt")
	assert.Len(t, harness.store.snapshotFailures(), 1)
	assert.Less(t, elapsed, 500*time.Millisecond,
		"the relay must not sleep the backoff; the schedule is persisted, not waited out")

	source := relayReadOwnSource(t)
	assert.NotContains(t, source, "time.Sleep",
		"the relay must never sleep a retry delay in process")
}

// TestProcessRow_MarkFailureLeavesTheRowClaimable asserts nothing is dropped when the
// bookkeeping itself fails: the row keeps its lease and returns to the claimable set.
func TestProcessRow_MarkFailureLeavesTheRowClaimable(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newRelayHarness(t, relayTransactionRow(1, "evt-mark-failed"))
	harness.publisher.err = errRelayTransient
	harness.store.failErr = errors.New("relay test: database unavailable")

	harness.processor.processBatch(context.Background())

	assert.Empty(t, harness.deadLetters.snapshotRows(),
		"a row whose failure could not be recorded must not be dead-lettered")
	_, terminal := harness.store.terminalState(1)
	assert.False(t, terminal, "the row must stay non-terminal so its lease can bring it back")

	require.NotEmpty(t, relayEntriesWithMessage(hook, "recording a failed publish attempt failed"))
}

// TestProcessRow_LogsWhenAPublishedRowCannotBeMarked asserts the at-least-once window is
// reported rather than hidden: the event is on the topic, the row does not say so, and the
// duplicate that follows is suppressed at the subscriber on event_id.
func TestProcessRow_LogsWhenAPublishedRowCannotBeMarked(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newRelayHarness(t, relayTransactionRow(1, "evt-unmarked"))
	harness.store.dispatchErr = errors.New("relay test: database unavailable")

	harness.processor.processBatch(context.Background())

	require.Len(t, harness.publisher.snapshotRequests(), 1, "the event must still be published")

	entries := relayEntriesWithMessage(hook, "could not be marked dispatched")
	require.NotEmpty(t, entries, "the duplicate window must be logged")
	assert.Contains(t, entries[0].Message, "suppressed on event_id",
		"the log line must name the subscriber's idempotency obligation")
}

// ---------------------------------------------------------------------------
// Exhaustion and the dead-letter hand-off (requirement R-5)
// ---------------------------------------------------------------------------

// TestProcessRow_DeadLettersOnExhaustionWithEverythingTheMetadataNeeds drives the whole
// five-attempt sequence and asserts the terminal hand-off: exactly one dead-letter, carrying
// the claim token MarkEventFailed retained, and a row from which the mandated failure
// metadata resolves correctly.
func TestProcessRow_DeadLettersOnExhaustionWithEverythingTheMetadataNeeds(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-exhausted"))
	harness.publisher.err = errRelayTransient

	for attempt := 1; attempt <= 5; attempt++ {
		harness.store.now = func() time.Time { return relayFixedNow.Add(time.Hour * time.Duration(attempt)) }
		harness.processor.processBatch(context.Background())
	}

	// A sixth poll must find nothing: an exhausted row is outside the claimable set.
	harness.store.now = func() time.Time { return relayFixedNow.Add(24 * time.Hour) }
	assert.Zero(t, harness.processor.processBatch(context.Background()),
		"an exhausted row must not be claimed again")

	assert.Len(t, harness.publisher.snapshotRequests(), 5,
		"the budget is five attempts, so there must be five publishes and no more")

	rows := harness.deadLetters.snapshotRows()
	require.Len(t, rows, 1, "exhaustion must hand off exactly once")
	handed := rows[0]

	assert.Equal(t, "evt-exhausted", handed.EventID)
	assert.Equal(t, int64(1), handed.ID, "the row must carry its database identity")
	assert.Equal(t, 5, handed.Attempts, "the attempt count must be the one the database recorded")
	assert.Equal(t, model.EventOutboxStatusFailed, handed.Status,
		"the row is already failed when the dead-letter write is owed")
	assert.NotEmpty(t, handed.ClaimToken,
		"the retained claim token must travel on the row, or the dead-letter transition is refused")

	failures := harness.store.snapshotFailures()
	require.Len(t, failures, 5)
	assert.Equal(t, handed.ClaimToken, failures[4].claimToken,
		"the token handed on must be the one the final attempt held")

	// The metadata the dead-letter writer will produce from this row is the relay's real
	// responsibility: the writer composes it, the relay must supply a row it can compose from.
	metadata := BuildFailureMetadata(handed, errRelayTransient, handed.Attempts, relayFixedNow)
	assert.Equal(t, "blnk.transactions", metadata.OriginalTopic)
	assert.Equal(t, 5, metadata.AttemptCount)
	assert.Contains(t, metadata.ErrorReason, "broker unavailable")
	assert.False(t, metadata.FirstAttemptedAt.IsZero(),
		"first_attempted_at must be stamped on the first attempt")
	assert.False(t, metadata.LastAttemptedAt.IsZero(),
		"last_attempted_at must move with every attempt")
	assert.False(t, metadata.LastAttemptedAt.Before(metadata.FirstAttemptedAt),
		"the retry window must not run backwards")
	assert.True(t, metadata.LastAttemptedAt.After(metadata.FirstAttemptedAt),
		"five attempts an hour apart must bound a real window")

	state, terminal := harness.store.terminalState(1)
	assert.True(t, terminal)
	assert.Equal(t, model.EventOutboxStatusFailed, state,
		"the relay marks the row failed; only the dead-letter writer may say dead_lettered")
}

// TestProcessRow_ReportsAFailedDeadLetterHandOffLoudly asserts a row whose dead-letter write
// failed stays visible rather than being reported as safely preserved.
func TestProcessRow_ReportsAFailedDeadLetterHandOffLoudly(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	row := relayTransactionRow(1, "evt-dlt-failed")
	row.Attempts = 4

	harness := newRelayHarness(t, row)
	harness.publisher.err = errRelayTransient
	harness.deadLetters.err = errors.New("relay test: broker unavailable for the dead-letter write")

	harness.processor.processBatch(context.Background())

	require.Len(t, harness.deadLetters.snapshotRows(), 1, "the hand-off must have been attempted")

	entries := relayEntriesWithMessage(hook, "could not be written to its dead-letter topic")
	require.NotEmpty(t, entries, "a failed hand-off must be logged at error level")
	assert.Equal(t, logrus.ErrorLevel, entries[0].Level)
	assert.Contains(t, entries[0].Message, "remains in the dead-letter inventory",
		"the log line must state that the event is still visible to an operator")
}

// TestProcessRow_WithoutADeadLetterServiceDoesNotPanic covers the unreachable branch: Start
// refuses without a dead-letter service, and a nil dereference in a ledger process is never an
// acceptable answer anyway.
func TestProcessRow_WithoutADeadLetterServiceDoesNotPanic(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	row := relayTransactionRow(1, "evt-no-dlt")
	row.Attempts = 4

	harness := newRelayHarness(t, row)
	harness.publisher.err = errRelayTransient
	harness.processor.deadLetters = nil

	assert.NotPanics(t, func() { harness.processor.processBatch(context.Background()) })
	require.NotEmpty(t, relayEntriesWithMessage(hook, "no dead-letter service is configured"))
}

// TestProcessRow_UsesTheRowBudgetRatherThanTheConfiguredOne asserts the budget reported and
// passed to the publisher is the one the database enforces, so the logs and the metric label
// cannot disagree with the actual outcome.
func TestProcessRow_UsesTheRowBudgetRatherThanTheConfiguredOne(t *testing.T) {
	row := relayTransactionRow(1, "evt-row-budget")
	row.MaxAttempts = 2

	harness := newRelayHarness(t, row)
	harness.publisher.err = errRelayTransient

	for attempt := 1; attempt <= 2; attempt++ {
		harness.store.now = func() time.Time { return relayFixedNow.Add(time.Hour * time.Duration(attempt)) }
		harness.processor.processBatch(context.Background())
	}

	requests := harness.publisher.snapshotRequests()
	require.Len(t, requests, 2, "a two-attempt row must be attempted twice")
	assert.Equal(t, 2, requests[0].MaxAttempts, "the row's budget must reach the publisher")

	require.Len(t, harness.deadLetters.snapshotRows(), 1,
		"the row's own budget decides exhaustion, not the configured five")

	assert.Equal(t, 5, harness.processor.rowMaxAttempts(model.EventOutbox{}),
		"a row that states no budget falls back to the configured one")
}

// ---------------------------------------------------------------------------
// The dual-delivery window (requirement R-12, acceptance criterion V-8)
//
// DELETED AT THE WEBHOOK SUNSET, together with the branch these tests cover.
// ---------------------------------------------------------------------------

// TestDualDelivery_EnqueuesTheStoredBytesAndRecordsTheMarker asserts the mechanism that makes
// payload identity structural: the legacy transport is handed the row's stored bytes, which are
// the same bytes Kafka receives, so the two cannot drift apart.
func TestDualDelivery_EnqueuesTheStoredBytesAndRecordsTheMarker(t *testing.T) {
	row := relayTransactionRow(1, "evt-dual")
	harness := newRelayHarness(t, row)

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	enqueued := harness.legacy.snapshot()
	require.Len(t, enqueued, 1, "the legacy leg must run while the sunset is in the future")
	assert.Equal(t, "evt-dual", enqueued[0].eventID,
		"the event id must be the task identity, so a re-claim cannot double-enqueue")
	assert.Equal(t, []byte(row.Payload), enqueued[0].body,
		"the legacy body must be the row's stored bytes, byte for byte")

	requests := harness.publisher.snapshotRequests()
	require.Len(t, requests, 1)
	assert.Equal(t, enqueued[0].body, []byte(requests[0].Event.Payload),
		"both transports must carry identical bytes — they read the same row")

	marks := harness.store.snapshotWebhookMarks()
	require.Len(t, marks, 1, "the dual-delivery outcome must be recorded on the row")
	assert.Equal(t, int64(1), marks[0].id)
	assert.Equal(t, "token-1", marks[0].claimToken,
		"the marker must be written while the claim is still held")

	dispatched := harness.store.snapshotDispatched()
	require.Len(t, dispatched, 1, "the Kafka leg must still reach its terminal state")
}

// TestDualDelivery_StopsAfterTheSunset asserts the post-sunset state: Kafka is the only
// transport, and the decision comes from event_sunset.go.
func TestDualDelivery_StopsAfterTheSunset(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-post-sunset"))
	harness.processor.sunsetPassed = func(time.Time) bool { return true }

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	assert.Empty(t, harness.legacy.snapshot(),
		"no legacy webhook may be enqueued once the sunset has passed")
	assert.Empty(t, harness.store.snapshotWebhookMarks(),
		"and nothing may be recorded for a leg that did not run")
	assert.Len(t, harness.publisher.snapshotRequests(), 1,
		"the Kafka publish is unaffected by the sunset")
	assert.Len(t, harness.store.snapshotDispatched(), 1)
}

// TestDualDelivery_TakesTheSunsetDecisionOncePerBatch asserts the decision is not re-evaluated
// per row, which at five hundred events a second would be five hundred decisions for one
// answer.
func TestDualDelivery_TakesTheSunsetDecisionOncePerBatch(t *testing.T) {
	rows := make([]model.EventOutbox, 0, 4)
	for id := int64(1); id <= 4; id++ {
		rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-%d", id)))
	}

	harness := newRelayHarness(t, rows...)

	var decisions atomic.Int32
	harness.processor.sunsetPassed = func(time.Time) bool {
		decisions.Add(1)

		return false
	}

	require.Equal(t, 4, harness.processor.processBatch(context.Background()))

	assert.Equal(t, int32(1), decisions.Load(), "one decision per batch, not one per row")
	assert.Len(t, harness.legacy.snapshot(), 4, "every row must still get its legacy leg")
}

// TestDualDelivery_SkipsARowWhoseLegacyLegIsAlreadyDone asserts the flag's purpose: a row
// republished to Kafka after a crash must not enqueue a second webhook.
func TestDualDelivery_SkipsARowWhoseLegacyLegIsAlreadyDone(t *testing.T) {
	row := relayTransactionRow(1, "evt-already-sent")
	row.WebhookDispatched = true

	harness := newRelayHarness(t, row)

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	assert.Empty(t, harness.legacy.snapshot(),
		"a row already marked must not enqueue a duplicate webhook")
	assert.Empty(t, harness.store.snapshotWebhookMarks(), "and must not be re-marked")
	assert.Len(t, harness.publisher.snapshotRequests(), 1, "the Kafka leg still runs")
}

// TestDualDelivery_LegacyFailuresNeverAffectTheKafkaPath asserts the isolation between the two
// transports: a webhook receiver being unavailable must not consume a Kafka retry attempt, and
// must not stop a row reaching its terminal state.
func TestDualDelivery_LegacyFailuresNeverAffectTheKafkaPath(t *testing.T) {
	t.Run("the enqueue fails", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		harness := newRelayHarness(t, relayTransactionRow(1, "evt-legacy-enqueue-failed"))
		harness.legacy.err = errors.New("relay test: redis unavailable")

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		assert.Len(t, harness.publisher.snapshotRequests(), 1, "the Kafka publish must still happen")
		assert.Len(t, harness.store.snapshotDispatched(), 1, "and the row must still be dispatched")
		assert.Empty(t, harness.store.snapshotFailures(),
			"a legacy failure must never spend a Kafka retry attempt")
		assert.Empty(t, harness.deadLetters.snapshotRows())

		entries := relayEntriesWithMessage(hook, "enqueuing the legacy webhook delivery failed")
		require.NotEmpty(t, entries, "the legacy failure must be logged")
		assert.Equal(t, logrus.WarnLevel, entries[0].Level,
			"a deprecated transport failing is a warning, not an error on the new one")
	})

	t.Run("the marker fails", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		harness := newRelayHarness(t, relayTransactionRow(1, "evt-legacy-mark-failed"))
		harness.store.webhookErr = errors.New("relay test: database unavailable")

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		assert.Len(t, harness.legacy.snapshot(), 1, "the task is enqueued and will be delivered")
		assert.Len(t, harness.store.snapshotDispatched(), 1, "the Kafka leg still completes")

		entries := relayEntriesWithMessage(hook, "could not be marked")
		require.NotEmpty(t, entries)
		assert.Contains(t, entries[0].Message, "suppressed by the task identity",
			"the log must state why the missing marker is not a delivery defect")
	})
}

// TestDualDelivery_RunsEvenWhenTheKafkaPublishFails is the failure mode that matters most
// during the window: the legacy transport is what unmigrated subscribers are still consuming,
// so a broker outage must not silently stop delivering to them.
func TestDualDelivery_RunsEvenWhenTheKafkaPublishFails(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-kafka-down"))
	harness.publisher.err = errRelayTransient

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	assert.Len(t, harness.legacy.snapshot(), 1,
		"a Kafka outage must not suppress delivery to subscribers who have not migrated")
	assert.Len(t, harness.store.snapshotWebhookMarks(), 1,
		"and the legacy leg must be recorded so the retry does not re-enqueue it")
	assert.Len(t, harness.store.snapshotFailures(), 1, "the Kafka leg still records its failure")
}

// TestDualDelivery_WithoutALegacyTransportIsANoOp covers the post-sunset shape of the code,
// where the legacy field is gone: the relay must publish exactly as it does now.
func TestDualDelivery_WithoutALegacyTransportIsANoOp(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-no-legacy"))
	harness.processor.legacy = nil

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	assert.Len(t, harness.publisher.snapshotRequests(), 1)
	assert.Len(t, harness.store.snapshotDispatched(), 1)
	assert.Empty(t, harness.store.snapshotWebhookMarks())
}

// ---------------------------------------------------------------------------
// Crash recovery (acceptance criterion V-7)
// ---------------------------------------------------------------------------

// TestCrashRecovery_RepublishesOnlyWhatTheCrashLeftUnmarked kills the relay mid-batch, expires
// the lease the way the database does, restarts, and asserts the honest guarantee: nothing is
// lost, and the only duplicate is the row that was published but not marked — suppressed at the
// subscriber on event_id.
func TestCrashRecovery_RepublishesOnlyWhatTheCrashLeftUnmarked(t *testing.T) {
	rows := make([]model.EventOutbox, 0, 4)
	for id := int64(1); id <= 4; id++ {
		rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-%d", id)))
	}

	harness := newRelayHarness(t, rows...)
	// Sequential, so "the crash happened after the second row" is a fact rather than a race.
	harness.processor.WithBatchSize(4).WithConcurrency(1)

	// The crash: the process dies after the second row is published, before its row is marked.
	var published atomic.Int32
	harness.store.dispatchErr = nil
	harness.publisher.beforePublish = func(PublishRequest) {
		if published.Add(1) == 2 {
			harness.store.mu.Lock()
			harness.store.dispatchErr = errors.New("relay test: the process died")
			harness.store.mu.Unlock()
		}
	}

	harness.processor.processBatch(context.Background())

	firstRun := harness.publisher.publishedIDs()
	require.Len(t, firstRun, 4, "the batch was claimed, so every row was attempted")

	// The restart. Rows that never reached a terminal state come back when the lease expires,
	// which is the mechanism that makes a crashed relay's work recoverable rather than lost.
	harness.store.mu.Lock()
	harness.store.dispatchErr = nil
	harness.store.mu.Unlock()
	harness.publisher.beforePublish = nil
	harness.store.expireLeases()

	harness.processor.processBatch(context.Background())

	delivered := map[string]int{}
	for _, id := range harness.publisher.publishedIDs() {
		delivered[id]++
	}

	for id := int64(1); id <= 4; id++ {
		eventID := fmt.Sprintf("evt-%d", id)
		assert.GreaterOrEqual(t, delivered[eventID], 1,
			"%s must be delivered; the outbox exists so that nothing is lost", eventID)
		assert.LessOrEqual(t, delivered[eventID], 2,
			"%s must be redelivered at most once — the crash window is one attempt wide", eventID)

		state, terminal := harness.store.terminalState(id)
		assert.True(t, terminal, "%s must end in a terminal state after the restart", eventID)
		assert.Equal(t, model.EventOutboxStatusDispatched, state)
	}

	assert.Equal(t, 1, delivered["evt-1"],
		"a row marked before the crash must not be republished")
	assert.Equal(t, 2, delivered["evt-2"],
		"the row published but not marked is exactly the documented at-least-once duplicate")

	assert.Empty(t, harness.deadLetters.snapshotRows(),
		"a crash must not consume a retry attempt or dead-letter anything")
	assert.Empty(t, harness.store.snapshotFailures(),
		"the publishes succeeded; only the bookkeeping failed")
}

// TestCrashRecovery_LeavesUnprocessedRowsClaimableWhenTheContextIsCancelled asserts a shutdown
// mid-batch strands nothing: the rows it did not get to keep their lease and come back.
func TestCrashRecovery_LeavesUnprocessedRowsClaimableWhenTheContextIsCancelled(t *testing.T) {
	rows := make([]model.EventOutbox, 0, 6)
	for id := int64(1); id <= 6; id++ {
		rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-%d", id)))
	}

	harness := newRelayHarness(t, rows...)
	harness.processor.WithBatchSize(6).WithConcurrency(1)

	ctx, cancel := context.WithCancel(context.Background())

	var published atomic.Int32
	harness.publisher.beforePublish = func(PublishRequest) {
		if published.Add(1) == 2 {
			cancel()
		}
	}

	harness.processor.processBatch(ctx)
	harness.publisher.beforePublish = nil

	firstRun := len(harness.publisher.publishedIDs())
	assert.Less(t, firstRun, 6, "cancellation must stop dispatching the rest of the batch")
	assert.GreaterOrEqual(t, firstRun, 2, "the rows already in flight must complete")

	// The bookkeeping the relay already owed must have been completed on its detached context,
	// even though the caller's context was cancelled: otherwise the rows it published would be
	// republished for no reason.
	assert.Len(t, harness.store.snapshotDispatched(), firstRun,
		"work already done on the wire must be recorded despite the cancellation")

	harness.store.expireLeases()
	harness.processor.processBatch(context.Background())

	delivered := map[string]bool{}
	for _, id := range harness.publisher.publishedIDs() {
		delivered[id] = true
	}
	for id := int64(1); id <= 6; id++ {
		assert.True(t, delivered[fmt.Sprintf("evt-%d", id)],
			"evt-%d must be delivered after the restart; nothing may be stranded", id)
	}
}

// ---------------------------------------------------------------------------
// Failure-reason handling
// ---------------------------------------------------------------------------

// TestRelayFailureReason_SanitisesBoundsAndNeverReturnsEmpty asserts the treatment of a string
// that comes from a broker and is written to both the log and the row's last_error column.
func TestRelayFailureReason_SanitisesBoundsAndNeverReturnsEmpty(t *testing.T) {
	assert.Equal(t, "the publish failed without reporting a reason", relayFailureReason(nil),
		"an empty last_error is indistinguishable from a row nobody has tried")

	assert.Equal(t, "the publish failed without reporting a reason",
		relayFailureReason(errors.New("")),
		"an error with no text must still produce a readable reason")

	forged := relayFailureReason(errors.New("write failed\nERROR everything is fine"))
	assert.NotContains(t, forged, "\n", "a newline would forge a second log line")
	assert.Contains(t, forged, "write failed")

	long := relayFailureReason(errors.New(strings.Repeat("x", maxLoggedErrorLength*3)))
	assert.LessOrEqual(t, len([]rune(long)), maxLoggedErrorLength+len([]rune(logTruncationSuffix)),
		"an unbounded broker error must be capped before it reaches a log or a text column")
	assert.Contains(t, long, logTruncationSuffix, "truncation must be marked, never silent")
}

// TestDetachedBookkeepingContext_SurvivesCancellationButIsBounded asserts the property the
// bookkeeping context exists for, and the bound that stops it becoming a shutdown hang.
func TestDetachedBookkeepingContext_SurvivesCancellationButIsBounded(t *testing.T) {
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), relayCtxKey{}, "kept"))
	detached, release := detachedBookkeepingContext(parent)
	defer release()

	cancel()

	require.Error(t, parent.Err(), "the parent must be cancelled for this to mean anything")
	assert.NoError(t, detached.Err(),
		"a transition the relay already owes must survive the caller's cancellation")
	assert.Equal(t, "kept", detached.Value(relayCtxKey{}),
		"the values must be carried through, only the cancellation is dropped")

	deadline, ok := detached.Deadline()
	require.True(t, ok, "the detached context must be bounded, or shutdown could hang forever")
	assert.WithinDuration(t, time.Now().Add(eventRelayBookkeepingTimeout), deadline, time.Second)
}

// relayCtxKey is a private context key for the test above.
type relayCtxKey struct{}
