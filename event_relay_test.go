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
// Every test here runs WITHOUT A BROKER AND WITHOUT A DATABASE, which is deliberate:
// the relay's contract is expressed against four repository methods, one publisher
// method, one dead-letter hand-off and one legacy enqueue, so substituting those seams
// exercises the real decision logic — the retry schedule, the ordering guarantee, the
// exhaustion hand-off, the dual-delivery branch — with nothing skipped and nothing
// mocked away that matters.
package blnk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// relayFixedNow is the clock every test pins, so scheduled instants and durations are exact
// rather than approximately now.
var relayFixedNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// The fake now renews the hand-off lease from the lease the RELAY supplied, normalised
// through deadLetterHandoffDeadline exactly as the repository normalises it — so a test
// that sets WithLockDuration reasons about the duration it set rather than about a
// constant this file chose.

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

	// record is the broker coordinate the transition was given. Recorded so a test can
	// assert that the relay PERSISTS where the broker put the message — the mapping the
	// zero-loss reconciliation is built on — rather than only that it marked the row.
	record model.BrokerRecord
}

// relayFailRecord is one recorded failed attempt.
type relayFailRecord struct {
	id         int64
	claimToken string
	reason     string
	retryAfter time.Duration

	// terminal is the caller's permanent-failure verdict, recorded so a test can assert
	// the relay FORWARDED the publisher's classification rather than leaving the
	// retry-versus- exhaustion decision to the attempt count alone.
	terminal bool

	// deadLetterLease is the hand-off lease the caller supplied for the exhaustion arm.
	deadLetterLease time.Duration

	// record is the broker coordinate the transition was given, empty on a transition that
	// carries none.
	record model.BrokerRecord
}

// relayFakeStore models blnk.event_outbox closely enough to drive multi-attempt
// sequences: it claims FIFO with a fresh token, returns a failed row to the claimable
// set with the caller's backoff applied, and exhausts a row's budget the way
// MarkEventFailed's in-SQL CASE does.
type relayFakeStore struct {
	mu sync.Mutex

	// pending is the claimable set, oldest first.
	pending []model.EventOutbox
	// inflight is every row currently claimed, by row id, so a lease can be expired.
	inflight map[int64]model.EventOutbox
	// terminal records rows that reached a terminal state, by row id.
	terminal map[int64]string

	claims     []relayClaimRecord
	claimed    [][]model.EventOutbox
	dispatched []relayMarkRecord
	failures   []relayFailRecord
	// terminalFailures records MarkEventPermanentlyFailed calls — the transition the relay
	// takes when the publisher reports a failure no further attempt can change. It is kept
	// SEPARATE from failures so a test can tell "recorded a permanent failure once" from
	// "recorded five ordinary attempts", which is the whole distinction the terminal gate
	// introduces.
	terminalFailures []relayFailRecord
	webhookMarks     []relayMarkRecord
	// webhookPendings records MarkEventWebhookPending calls — the transition that keeps a
	// failed legacy enqueue recoverable instead of losing it behind a terminal state.
	webhookPendings []relayFailRecord

	// renewals records every lease renewal, and failedClaims every dead-letter repair
	// claim, so the heartbeat and the repair pass can be asserted rather than inferred.
	renewals    []relayRenewalRecord
	failedRows  []model.EventOutbox
	failedTaken bool

	// webhookOwedRows is the LEGACY leg's recovery set: rows whose Kafka leg has reached
	// an end state — dispatched, failed or dead_lettered — with webhook_dispatched still
	// FALSE. It is seeded and handed out exactly once, mirroring failedRows, because the
	// pass under test polls every tick and an unbounded set would loop.
	webhookOwedRows  []model.EventOutbox
	webhookOwedTaken bool

	// The REPAIR CAPACITY instrumentation. Both repair claims now take a CONFIGURED batch
	// size and are CHAINED within one tick, and neither property is visible from the rows
	// a pass produced — a chain of three batches and one batch of three rows leave the
	// same result behind.
	failedClaimSizes  []int
	webhookClaimSizes []int
	failedWaves       [][]model.EventOutbox
	webhookWaves      [][]model.EventOutbox
	// legacyAttempts records every MarkEventLegacyWebhookAttempted call, so the recovery
	// path's own bounded budget can be asserted rather than inferred.
	legacyAttempts []relayFailRecord

	claimErr          error
	dispatchErr       error
	failErr           error
	terminalFailErr   error
	webhookErr        error
	webhookPendingErr error
	renewErr          error
	failedClaimErr    error
	webhookOwedErr    error
	legacyAttemptErr  error

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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

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

// MarkEventDispatched moves a claimed row to the success terminal state, refusing a
// token that is not the current one.
//
// Like every method here it HONOURS THE CONTEXT, and that is not incidental.
func (s *relayFakeStore) MarkEventDispatched(
	ctx context.Context,
	id int64,
	claimToken string,
	record model.BrokerRecord,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.dispatched = append(s.dispatched, relayMarkRecord{
		id: id, claimToken: claimToken, record: record,
	})

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

// MarkEventFailed records the attempt and reproduces the repository's two arms: back to
// the claimable set with the caller's backoff applied, or failed with the token AND its
// lease retained.
//
// The lease is what makes the retained token mean anything on the exhaustion arm.
func (s *relayFakeStore) MarkEventFailed(
	ctx context.Context,
	id int64,
	claimToken, errMsg string,
	retryAfter time.Duration,
	terminal bool,
	deadLetterLease time.Duration,
) (model.EventFailureOutcome, error) {
	if err := ctx.Err(); err != nil {
		return model.EventFailureOutcome{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.failures = append(s.failures, relayFailRecord{
		id: id, claimToken: claimToken, reason: errMsg, retryAfter: retryAfter,
		terminal: terminal, deadLetterLease: deadLetterLease,
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

	// The caller's verdict is ORed into the arithmetic exactly as the real UPDATE's CASE
	// does, so a permanent failure exhausts the row on whichever attempt it happened. A
	// double that only counted attempts would report a retry where production reports
	// exhaustion, and the relay's dead-letter hand-off would go untested for the whole
	// permanent-failure class.
	if terminal || row.Attempts >= row.MaxAttempts {
		row.Status = model.EventOutboxStatusFailed
		// THE LEASE IS RENEWED, not cleared, exactly as the real statement does it. The token
		// is retained so only this worker may write the dead-letter message, and the lease is
		// what makes that retention mean something: the repair claim takes a failed row with
		// no dlt_topic and no LIVE lease and stamps a fresh token over it, so a cleared lease
		// here would let a second instance take the row mid-hand-off and publish the same
		// dead-letter message.
		row.LockedUntil = s.deadLetterHandoffDeadline(deadLetterLease)
		s.inflight[id] = row
		s.terminal[id] = model.EventOutboxStatusFailed

		return model.EventFailureOutcome{
			Status:     row.Status,
			Attempts:   row.Attempts,
			Exhausted:  true,
			Terminal:   terminal,
			ClaimToken: claimToken,
		}, nil
	}

	// The retry arm DOES clear the lease: the row goes back to the claimable set and is
	// protected by next_attempt_at instead.
	row.LockedUntil = nil
	row.Status = model.EventOutboxStatusPending
	row.ClaimToken = ""
	row.LockedUntil = nil
	row.NextAttemptAt = s.now().Add(retryAfter)

	delete(s.inflight, id)
	s.pending = append(s.pending, row)

	return model.EventFailureOutcome{Status: row.Status, Attempts: row.Attempts}, nil
}

// MarkEventPermanentlyFailed reproduces the permanent-failure transition: the attempt
// is counted, the row becomes failed on THIS attempt whatever budget remained, and the
// claim token is retained for the dead-letter hand-off.
func (s *relayFakeStore) MarkEventPermanentlyFailed(
	ctx context.Context,
	id int64,
	claimToken, errMsg string,
	deadLetterLease time.Duration,
) (model.EventFailureOutcome, error) {
	if err := ctx.Err(); err != nil {
		return model.EventFailureOutcome{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.terminalFailures = append(s.terminalFailures, relayFailRecord{
		id: id, claimToken: claimToken, reason: errMsg, deadLetterLease: deadLetterLease,
	})

	if s.terminalFailErr != nil {
		return model.EventFailureOutcome{}, s.terminalFailErr
	}

	row, held := s.inflight[id]
	if !held || row.ClaimToken != claimToken {
		return model.EventFailureOutcome{}, fmt.Errorf("relay test: claim lost on row %d", id)
	}

	row.Attempts++
	row.LastError = errMsg
	row.Status = model.EventOutboxStatusFailed
	// Renewed for the dead-letter hand-off, as the exhaustion arm above does and for the same
	// reason, and from the caller's own lease. See MarkEventFailed's terminal arm.
	row.LockedUntil = s.deadLetterHandoffDeadline(deadLetterLease)

	s.inflight[id] = row
	s.terminal[id] = model.EventOutboxStatusFailed

	return model.EventFailureOutcome{
		Status:     row.Status,
		Attempts:   row.Attempts,
		Exhausted:  true,
		ClaimToken: claimToken,
	}, nil
}

// MarkWebhookDispatched records the dual-delivery marker on the claimed row.
func (s *relayFakeStore) MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

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

// MarkEventWebhookPending reproduces the repository's two arms for a Kafka leg that is
// done while the legacy leg is still owed: back to the CLAIMABLE set as webhook_pending
// with the caller's backoff applied, or dispatched with the legacy leg abandoned once
// the webhook budget is spent.
func (s *relayFakeStore) MarkEventWebhookPending(
	ctx context.Context,
	id int64,
	claimToken, errMsg string,
	retryAfter time.Duration,
	record model.BrokerRecord,
) (model.EventWebhookOutcome, error) {
	if err := ctx.Err(); err != nil {
		return model.EventWebhookOutcome{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.webhookPendings = append(s.webhookPendings, relayFailRecord{
		id: id, claimToken: claimToken, reason: errMsg, retryAfter: retryAfter, record: record,
	})

	if s.webhookPendingErr != nil {
		return model.EventWebhookOutcome{}, s.webhookPendingErr
	}

	row, held := s.inflight[id]
	if !held || row.ClaimToken != claimToken {
		return model.EventWebhookOutcome{}, fmt.Errorf("relay test: claim lost on row %d", id)
	}

	row.WebhookAttempts++
	row.LastError = errMsg
	row.LockedUntil = nil
	if row.KafkaDispatchedAt == nil {
		acknowledged := s.now()
		row.KafkaDispatchedAt = &acknowledged
	}
	// COALESCE semantics, as the real statement has: a webhook-only retry carries no
	// coordinate and must not erase the one the successful publish recorded.
	if coordinate, confirmed := record, record.Confirmed(); confirmed && row.KafkaOffset == nil {
		partition := coordinate.Partition
		offset := coordinate.Offset
		row.KafkaTopic = coordinate.Topic
		row.KafkaPartition = &partition
		row.KafkaOffset = &offset
	}

	if row.WebhookAttempts >= row.MaxAttempts {
		row.Status = model.EventOutboxStatusDispatched
		row.ClaimToken = ""
		delete(s.inflight, id)
		s.terminal[id] = model.EventOutboxStatusDispatched

		return model.EventWebhookOutcome{
			Status:          row.Status,
			WebhookAttempts: row.WebhookAttempts,
			Abandoned:       true,
		}, nil
	}

	row.Status = model.EventOutboxStatusWebhookPending
	row.ClaimToken = ""
	row.NextAttemptAt = s.now().Add(retryAfter)

	delete(s.inflight, id)
	s.pending = append(s.pending, row)

	return model.EventWebhookOutcome{Status: row.Status, WebhookAttempts: row.WebhookAttempts}, nil
}

// RenewEventOutboxLease extends the lease of every row still held under claimToken, exactly as
// the repository's statement does: rows that have reached a terminal state no longer carry the
// token, so they are outside its reach and the returned count reports only what is still in
// flight.
func (s *relayFakeStore) RenewEventOutboxLease(
	ctx context.Context,
	claimToken string,
	lease time.Duration,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.renewals = append(s.renewals, relayRenewalRecord{claimToken: claimToken, lease: lease})

	if s.renewErr != nil {
		return 0, s.renewErr
	}

	var renewed int64
	expiry := s.now().Add(lease)

	for id, row := range s.inflight {
		if row.ClaimToken != claimToken || row.Status != model.EventOutboxStatusProcessing {
			continue
		}

		row.LockedUntil = &expiry
		s.inflight[id] = row
		renewed++
	}

	return renewed, nil
}

// ClaimFailedEventOutboxForDeadLetter hands out the seeded unpreserved rows once, stamping a
// FRESH claim token and leaving the status at failed — which is what the repository does, and
// what lets MarkEventDeadLettered accept the row afterwards.
func (s *relayFakeStore) ClaimFailedEventOutboxForDeadLetter(
	ctx context.Context,
	batchSize int,
	lockDuration time.Duration,
) ([]model.EventOutbox, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failedClaimErr != nil {
		return nil, s.failedClaimErr
	}

	if batchSize <= 0 {
		return nil, errors.New("relay test: dead-letter recovery batch size must be greater than zero")
	}

	s.failedClaimSizes = append(s.failedClaimSizes, batchSize)

	// WAVES FIRST when a test seeded them: a backlog larger than one batch is what the chaining
	// bound is about, and the one-shot field below cannot express it.
	if len(s.failedWaves) > 0 {
		wave := s.failedWaves[0]
		s.failedWaves = s.failedWaves[1:]

		return s.leaseRepairWave(wave, batchSize, lockDuration, "recovery-token"), nil
	}

	if s.failedTaken || len(s.failedRows) == 0 {
		return nil, nil
	}
	s.failedTaken = true

	s.tokens++
	token := fmt.Sprintf("recovery-token-%d", s.tokens)
	lease := s.now().Add(lockDuration)

	claimed := make([]model.EventOutbox, 0, len(s.failedRows))
	for _, row := range s.failedRows {
		if len(claimed) >= batchSize {
			break
		}

		// THE LEASE PREDICATE, mirrored from the real statement: a failed row that still
		// holds a LIVE lease belongs to the worker performing its dead-letter hand-off and
		// must not be taken. Omitting this is the defect expressed in a fake — the repair
		// pass would appear able to take a row mid-hand-off, and a test written against it
		// would prove the opposite of the production invariant.
		if row.LockedUntil != nil && row.LockedUntil.After(s.now()) {
			continue
		}

		row.ClaimToken = token
		row.LockedUntil = &lease
		s.inflight[row.ID] = row
		claimed = append(claimed, row)
	}

	return claimed, nil
}

// leaseRepairWave leases one seeded wave of repair rows under a fresh token, truncated
// to the batch size the caller asked for.
//
// Shared by both repair claims because both repository statements do the same three
// things — cap at the batch size, stamp one fresh token per claim, take a lease — and
// two copies of that in a fake are two chances for one of them to stop matching the
// statement it stands in for.
//
// The caller holds s.mu.
//
// Parameters:
//   - wave []model.EventOutbox: the rows this claim is to serve.
//   - batchSize int: the cap the caller asked for.
//   - lockDuration time.Duration: the lease width.
//   - tokenPrefix string: distinguishes the two legs' tokens in a failure message.
//
// Returns:
//   - []model.EventOutbox: the leased rows, or nil for an empty wave.
func (s *relayFakeStore) leaseRepairWave(
	wave []model.EventOutbox,
	batchSize int,
	lockDuration time.Duration,
	tokenPrefix string,
) []model.EventOutbox {
	if len(wave) == 0 {
		return nil
	}

	s.tokens++
	token := fmt.Sprintf("%s-%d", tokenPrefix, s.tokens)
	lease := s.now().Add(lockDuration)

	claimed := make([]model.EventOutbox, 0, len(wave))
	for _, row := range wave {
		if len(claimed) >= batchSize {
			break
		}

		row.ClaimToken = token
		row.LockedUntil = &lease
		s.inflight[row.ID] = row
		claimed = append(claimed, row)
	}

	return claimed
}

// snapshotRepairClaimSizes returns the batch sizes both repair claims were called with, in order.
func (s *relayFakeStore) snapshotRepairClaimSizes() (deadLetter, webhook []int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]int(nil), s.failedClaimSizes...), append([]int(nil), s.webhookClaimSizes...)
}

// inflightRow returns the store's current copy of a claimed row, so a test can assert
// on the lease and the token a transition left behind.
func (s *relayFakeStore) inflightRow(id int64) (model.EventOutbox, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	row, held := s.inflight[id]

	return row, held
}

// ClaimPendingWebhookDeliveries hands out the seeded rows whose LEGACY leg is still
// owed, once, stamping a FRESH token and leaving the status EXACTLY as it is.
func (s *relayFakeStore) ClaimPendingWebhookDeliveries(
	ctx context.Context,
	batchSize int,
	lockDuration time.Duration,
) ([]model.EventOutbox, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.webhookOwedErr != nil {
		return nil, s.webhookOwedErr
	}

	if batchSize <= 0 {
		return nil, errors.New("relay test: webhook recovery batch size must be greater than zero")
	}

	s.webhookClaimSizes = append(s.webhookClaimSizes, batchSize)

	if len(s.webhookWaves) > 0 {
		wave := s.webhookWaves[0]
		s.webhookWaves = s.webhookWaves[1:]

		return s.leaseRepairWave(wave, batchSize, lockDuration, "webhook-recovery-token"), nil
	}

	if s.webhookOwedTaken || len(s.webhookOwedRows) == 0 {
		return nil, nil
	}
	s.webhookOwedTaken = true

	s.tokens++
	token := fmt.Sprintf("webhook-recovery-token-%d", s.tokens)
	lease := s.now().Add(lockDuration)

	claimed := make([]model.EventOutbox, 0, len(s.webhookOwedRows))
	for _, row := range s.webhookOwedRows {
		if len(claimed) >= batchSize {
			break
		}

		row.ClaimToken = token
		row.LockedUntil = &lease
		s.inflight[row.ID] = row
		claimed = append(claimed, row)
	}

	return claimed, nil
}

// MarkEventLegacyWebhookAttempted records one failed legacy enqueue against a row whose
// Kafka leg has finished, and reproduces the two properties the real statement is built
// around: the STATUS IS NOT TOUCHED, and abandoned is decided from the incremented
// count.
//
// last_error is likewise left alone, because on a failed or dead-lettered row it holds
// the Kafka failure reason the dead-letter metadata reports.
func (s *relayFakeStore) MarkEventLegacyWebhookAttempted(
	ctx context.Context,
	id int64,
	claimToken string,
	retryAfter time.Duration,
) (model.EventWebhookOutcome, error) {
	if err := ctx.Err(); err != nil {
		return model.EventWebhookOutcome{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.legacyAttempts = append(s.legacyAttempts, relayFailRecord{
		id: id, claimToken: claimToken, retryAfter: retryAfter,
	})

	if s.legacyAttemptErr != nil {
		return model.EventWebhookOutcome{}, s.legacyAttemptErr
	}

	row, held := s.inflight[id]
	if !held || row.ClaimToken != claimToken {
		return model.EventWebhookOutcome{}, fmt.Errorf("relay test: claim lost on row %d", id)
	}

	row.WebhookAttempts++
	row.LockedUntil = nil
	row.ClaimToken = ""
	row.NextAttemptAt = s.now().Add(retryAfter)

	delete(s.inflight, id)

	return model.EventWebhookOutcome{
		// THE ROW'S OWN STATUS, unchanged, which is the whole contract.
		Status:          row.Status,
		WebhookAttempts: row.WebhookAttempts,
		Abandoned:       row.WebhookAttempts >= row.MaxAttempts,
	}, nil
}

// snapshotLegacyAttempts returns the recorded recovery-path failure transitions.
func (s *relayFakeStore) snapshotLegacyAttempts() []relayFailRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]relayFailRecord, len(s.legacyAttempts))
	copy(out, s.legacyAttempts)

	return out
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

// retireInflight drops every claimed row's token, which is what a FINISHED batch looks
// like from the database's point of view: every terminal transition clears claim_token,
// so nothing is left under it for a lease renewal to extend.
func (s *relayFakeStore) retireInflight() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, row := range s.inflight {
		row.ClaimToken = ""
		row.Status = model.EventOutboxStatusDispatched
		s.inflight[id] = row
	}
}

// setClaimErr injects — or clears — a claim failure under the store's own lock, so a fault can
// be introduced and withdrawn while a relay is running without racing its claim.
func (s *relayFakeStore) setClaimErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.claimErr = err
}

// claimableIDs returns the ids of every row in the claimable set that is DUE at the
// given instant — the same predicate the repository's claim applies, so a test can ask
// "could anything pick this row up again?" without driving a whole batch. rowByID
// returns the row as the store now holds it, wherever it currently lives — claimable,
// leased, or terminal.
func (s *relayFakeStore) rowByID(t *testing.T, id int64) model.EventOutbox {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, row := range s.pending {
		if row.ID == id {
			return row
		}
	}

	if row, held := s.inflight[id]; held {
		return row
	}

	require.FailNowf(t, "row not found", "the store holds no row with id %d", id)

	return model.EventOutbox{}
}

// deadLetterHandoffDeadline resolves the instant a terminal transition holds the row
// under while its dead-letter write is owed, mirroring the repository's
// normalizeDeadLetterHandoffLease.
//
// The caller must hold s.mu.
func (s *relayFakeStore) deadLetterHandoffDeadline(lease time.Duration) *time.Time {
	if lease <= 0 {
		lease = defaultRelayDeadLetterHandoffLease
	}

	deadline := s.now().Add(lease)

	return &deadline
}

func (s *relayFakeStore) claimableIDs(at time.Time) []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]int64, 0, len(s.pending))
	for _, row := range s.pending {
		if row.NextAttemptAt.After(at) {
			continue
		}

		ids = append(ids, row.ID)
	}

	return ids
}

// unpublishedBacklog counts the rows that are still un-published work: claimable rows
// plus rows held under a lease.
//
// It is deliberately the SAME arithmetic as EventMetricsCollector.collectOutboxBacklog,
// which reports pending + processing to metrics.OutboxPendingBacklog.
func (s *relayFakeStore) unpublishedBacklog() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	backlog := int64(len(s.pending))
	for id := range s.inflight {
		if _, done := s.terminal[id]; !done {
			backlog++
		}
	}

	return backlog
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

// snapshotTerminalFailures returns the MarkEventPermanentlyFailed calls. It is separate from
// snapshotFailures so a test can assert "one permanent record and no budgeted attempts", which
// is the exact shape the terminal gate produces and the exact shape the previous behaviour did
// not.
func (s *relayFakeStore) snapshotTerminalFailures() []relayFailRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]relayFailRecord(nil), s.terminalFailures...)
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

// relayRenewalRecord is one lease renewal.
type relayRenewalRecord struct {
	claimToken string
	lease      time.Duration
}

// snapshotRenewals returns the lease renewals, in call order.
func (s *relayFakeStore) snapshotRenewals() []relayRenewalRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]relayRenewalRecord(nil), s.renewals...)
}

// snapshotWebhookPendings returns the MarkEventWebhookPending calls, in call order.
func (s *relayFakeStore) snapshotWebhookPendings() []relayFailRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]relayFailRecord(nil), s.webhookPendings...)
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
	// failFor fails only the events whose id is listed, so a batch can have one bad row. The
	// failure is TRANSIENT, which is the ordinary case: a broker briefly unreachable.
	failFor map[string]bool
	// permanentFailFor fails the listed events PERMANENTLY, reproducing what the real
	// publisher reports for a message no retry can deliver — an envelope over the size
	// ceiling, bytes that are not valid JSON, a destination outside Blnk's namespace.
	permanentFailFor map[string]error
	// beforePublish runs inside the publish, which is how a test observes concurrency.
	beforePublish func(req PublishRequest)

	// coordinates hands back a broker coordinate per event, modelling what the real
	// publisher reads off Writer.Completion. Absent means the publish reports no
	// coordinate, which is a legitimate outcome the relay has to record honestly as
	// unconfirmed.
	coordinates map[string]model.BrokerRecord

	// permanent makes every failure this publisher reports a PERMANENT one, which is the
	// classification the real publisher applies to an unauthorised principal, a
	// destination outside the topic catalogue, bytes that will never parse and a message
	// over the size limit. It is how a test drives the relay's terminal gate, which reads
	// PublishResult.PermanentFailure and must NOT spend the retry budget on any of those.
	permanent bool

	// unclassified makes the publisher return a bare error with an EMPTY result, which is
	// what an implementation that does no classification produces. The relay must read
	// that as retryable: the publisher is an interface seam, so "no verdict" cannot be
	// allowed to mean "give up on this event".
	unclassified bool
}

// failingPermanently makes the publisher report its failures as permanent rather than
// transient. The error itself is set separately, because what the relay branches on is the
// CLASSIFICATION and not the error value.
func (p *relayFakePublisher) failingPermanently() *relayFakePublisher {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.permanent = true

	return p
}

// failPermanentlyFor makes one event fail with a failure no retry can fix.
func (p *relayFakePublisher) failPermanentlyFor(eventID string, cause error) *relayFakePublisher {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.permanentFailFor == nil {
		p.permanentFailFor = make(map[string]error)
	}
	p.permanentFailFor[eventID] = cause

	return p
}

// reporting makes the publisher hand back a coordinate for one event.
func (p *relayFakePublisher) reporting(eventID string, record model.BrokerRecord) *relayFakePublisher {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.coordinates == nil {
		p.coordinates = make(map[string]model.BrokerRecord)
	}
	p.coordinates[eventID] = record

	return p
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
		Record:       p.coordinates[req.Event.EventID],
	}

	// PERMANENT first, because it is the stricter answer: an event listed in both maps is
	// one the test means to be unpublishable.
	if cause, permanent := p.permanentFailFor[req.Event.EventID]; permanent {
		result.Status = model.PublishStatusRetrying
		result.Terminal = true
		result.Transient = false
		result.Retryable = false
		// CLASSIFIED IS THE FIELD THE RELAY'S GATE READS, through PermanentFailure, and it is
		// affirmative by design so that a publisher which classifies nothing can never end an
		// event's life on its first attempt. A double that sets Terminal and leaves this
		// false therefore looks permanent in the log and in the metric while the relay treats
		// it as unclassified and retries it — which is exactly the shape that let the
		// permanent path go unexercised.
		result.Classified = true
		result.Err = &PublishError{
			Topic:     result.Topic,
			EventID:   result.EventID,
			EventType: result.EventType,
			Attempt:   result.Attempt,
			Transient: false,
			Err:       cause,
		}

		return result, result.Err
	}

	failure := p.err
	if failure == nil && p.failFor[req.Event.EventID] {
		failure = errRelayTransient
	}

	if failure != nil {
		if p.unclassified {
			return PublishResult{}, failure
		}

		// The two classifications the real publisher produces, and the relay takes a
		// different path for each: a transient verdict goes to the budgeted retry whose
		// exhaustion the database decides, a permanent one goes straight to the dead-letter
		// hand-off.
		result.Status = model.PublishStatusRetrying
		result.Classified = true
		if p.permanent {
			result.Transient = false
			result.Retryable = false
		} else {
			result.Transient = true
			result.Retryable = true
		}
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

// snapshotCauses returns the failures the rows were dead-lettered with, in call order. The
// cause becomes the failure metadata's error_reason, so it is part of what an operator reads.
func (d *relayFakeDeadLetterer) snapshotCauses() []error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return append([]error(nil), d.causes...)
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

	// wiredWindow is the dual-delivery predicate NewEventRelayProcessor ACTUALLY
	// INSTALLED, captured before the harness replaced it with a stub.
	wiredWindow func(now time.Time) bool
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

	// Captured before it is replaced, so a test can put the real decision back. The
	// harness's stub says the window is OPEN, which is the state the dual-delivery
	// assertions are written against.
	wiredWindow := processor.dualDeliveryActive
	processor.dualDeliveryActive = func(time.Time) bool { return true }
	processor.windowState = func(time.Time) WebhookWindowState { return WebhookWindowActive }

	return &relayHarness{
		processor:   processor,
		store:       store,
		publisher:   publisher,
		deadLetters: deadLetters,
		legacy:      legacy,
		wiredWindow: wiredWindow,
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

// relayRow builds a claimable outbox row whose payload is a real legacy webhook body,
// so the dual-delivery assertions compare the bytes production would carry.
func relayRow(id int64, eventID, eventType, partitionKey string, occurredAt time.Time) model.EventOutbox {
	payload, err := json.Marshal(NewWebhook{
		Event:   eventType,
		Payload: map[string]any{"transaction_id": eventID, "amount": 1250},
	})
	if err != nil {
		panic(err)
	}

	row := model.EventOutbox{
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

	raw, err := row.CanonicalEvent().CanonicalBytes()
	if err != nil {
		panic(err)
	}
	row.EventRaw = raw

	return row
}

// relayTransactionRow is the common case: a transaction event on one ledger.
func relayTransactionRow(id int64, eventID string) model.EventOutbox {
	return relayRow(id, eventID, "transaction.applied", "ldg_relay_1", relayFixedNow.Add(time.Duration(id)*time.Second))
}

// relayParseOwnSource parses event_relay.go so a test can assert the ABSENCE of
// something.
//
// An absence is the one property a behavioural test cannot reach: a second sunset
// comparison, or a date parse, is invisible from the outside for exactly as long as it
// happens to agree with event_sunset.go — which is the whole interval before somebody
// changes one of them, and the entire reason single ownership is worth asserting.
//
// IT IS THE AST, NOT THE TEXT, and that is the whole difference.
func relayParseOwnSource(t *testing.T) *ast.File {
	t.Helper()

	return parseRepositoryGoFile(t, "event_relay.go")
}

// relayPinLogLevel sets the standard logger's level for one test and restores whatever
// was in place afterwards.
func relayPinLogLevel(t *testing.T, level logrus.Level) {
	t.Helper()

	previous := logrus.GetLevel()
	t.Cleanup(func() { logrus.SetLevel(previous) })

	logrus.SetLevel(level)
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
// The retry schedule
// ---------------------------------------------------------------------------

// TestRelayRetryPolicy_DefaultScheduleIsExactlyOneTwoFourEightSixteenSeconds pins the
// mandated schedule value by value.
//
// THIS TEST IS HALF THE PROOF AND MUST NOT BE READ AS THE WHOLE OF IT.
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

	require.Len(t, expected, policy.maxAttempts,
		"the schedule must have one delay per attempt in the configured budget")

	for index, want := range expected {
		attempt := index + 1
		assert.Equal(t, want, policy.backoffFor(attempt),
			"the delay after attempt %d must be exactly %s", attempt, want)

		// EVERY delay in the budget is strictly below the cap, not merely the last one.
		assert.Less(t, policy.backoffFor(attempt), policy.maxBackoff,
			"the 30s cap is deliberately never reached within five attempts; do not 'fix' "+
				"the multiplier or the cap to make it engage on attempt %d", attempt)

		if index == 0 {
			continue
		}

		// The multiplier itself, pinned as a relationship rather than inferred from the
		// literals above. The multiplier is fixed at 2, and a mutant that changed it to 3 while the
		// literals were "corrected" to match would still fail here.
		assert.Equal(t, expected[index-1]*eventRelayBackoffMultiplier, want,
			"attempt %d's delay must be exactly the previous one doubled", attempt)
	}
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

// TestNewEventRelayProcessor_DualDeliveryDecisionComesFromEventSunsetOnly asserts the
// relay reads the ONE decision point rather than comparing instants itself. Two
// comparisons could disagree, and the pair that would disagree is "stop dual-writing"
// and "answer 410 Gone".
func TestNewEventRelayProcessor_DualDeliveryDecisionComesFromEventSunsetOnly(t *testing.T) {
	// The clock is a fixed instant rather than the wall clock, so the fixture cannot start
	// meaning something else as time passes.
	now := relayFixedNow

	// A window that has CLOSED, so the expected answer is FALSE. Both sides of the
	// boundary are exercised below, which is what stops a constructor that hardwired
	// either constant from satisfying this test.
	storeDeprecationWindow(t, now.AddDate(0, 0, -60), now.AddDate(0, 0, -30))

	processor := NewEventRelayProcessor(&Blnk{config: relayConfiguration()})

	require.NotNil(t, processor.dualDeliveryActive, "the window predicate must always be wired")
	require.NotNil(t, processor.windowState, "and so must the four-valued resolution")

	require.False(t, WebhookDualDeliveryActive(now),
		"fixture check: with a window that closed 30 days ago the legacy leg must not run")
	assert.Equal(t, WebhookDualDeliveryActive(now), processor.dualDeliveryActive(now),
		"the relay's decision must be event_sunset.go's decision, not a second comparison")
	assert.Equal(t, WebhookWindowClosed, processor.windowState(now))

	// And the other side, so neither field can be a constant.
	storeDeprecationWindow(t, now.AddDate(0, 0, -15), now.AddDate(0, 0, 15))
	require.True(t, WebhookDualDeliveryActive(now),
		"fixture check: inside the window both transports must run")
	assert.Equal(t, WebhookDualDeliveryActive(now), processor.dualDeliveryActive(now),
		"and it must track configuration in both directions")
	assert.Equal(t, WebhookWindowActive, processor.windowState(now))

	// A window that has not OPENED yet is the third state, and it is distinct from the
	// closed one: dual delivery is off in both, and only one of them is a misconfiguration
	// the relay refuses to start in.
	storeDeprecationWindow(t, now.AddDate(0, 0, 30), now.AddDate(0, 0, 60))
	assert.False(t, processor.dualDeliveryActive(now))
	assert.Equal(t, WebhookWindowPending, processor.windowState(now))

	// The structural half: the relay holds no second copy of the decision. Behaviour above
	// proves the relay's answer AGREES with event_sunset.go's today; this proves there is
	// only one thing that could answer, so it cannot stop agreeing tomorrow.
	source := relayParseOwnSource(t)
	assert.Zero(t, identifierUses(source, "WebhookDeprecationSunsetDate"),
		"the relay must never read the configured sunset date; event_sunset.go owns it, and a "+
			"second reader is a second interpretation of the same string")
	assert.Zero(t, identifierUses(source, "WebhookDeprecationStartDate"),
		"nor the start date, for the same reason: the window is one decision with two ends")
	assert.Zero(t, qualifiedCallCount(source, "time", "Parse"),
		"and it must never parse a date; a second parse is a second chance to disagree about "+
			"what the configured instant means")

	// The comparison primitives too, so the rule is about the OPERATION rather than about
	// one spelling of one helper. A relay that compared instants itself would satisfy
	// every assertion above and still own a second copy of the boundary.
	calls := selectorCallNames(source)
	assert.Zero(t, calls["Before"],
		"the relay must not compare instants itself; that comparison is the sunset decision")
	assert.Zero(t, calls["After"], "the same, in the other direction")
}

// TestEventRelayProcessor_RefusesToStartBeforeTheWindowOpens is the other half of
// gating on both ends of the window, and the reason the hard gate is safe.
func TestEventRelayProcessor_RefusesToStartBeforeTheWindowOpens(t *testing.T) {
	now := relayFixedNow
	storeDeprecationWindow(t, now.AddDate(0, 0, 30), now.AddDate(0, 0, 60))

	harness := newRelayHarness(t)
	// The harness pins the window open for the dual-delivery assertions; this test is about
	// the real resolution, so both seams go back to what the constructor installed.
	harness.processor.dualDeliveryActive = harness.wiredWindow
	harness.processor.windowState = WebhookDualDeliveryWindowState

	err := harness.processor.startupObstacle()
	require.Error(t, err, "a relay whose window has not opened must refuse to run")
	assert.Contains(t, err.Error(), "has not opened yet")
	assert.Contains(t, err.Error(), "WEBHOOK_DEPRECATION_SUNSET_DATE",
		"the error must name the ONE variable an operator can actually correct; the window's "+
			"opening instant is derived from the sunset and has no key of its own, so naming a "+
			"start-date variable would look actionable and change nothing")
	assert.NotContains(t, err.Error(), "WEBHOOK_DEPRECATION_START_DATE",
		"and it must not name a retired key")

	// AND THE OBSTACLE REACHES THE CALLER.
	startErr := harness.processor.Start(context.Background())
	require.Error(t, startErr, "Start must return the obstacle, not only log it")
	assert.Equal(t, err.Error(), startErr.Error(),
		"and it must be the SAME obstacle startupObstacle reports, so the process fails with the "+
			"remedy in it rather than with a generic message")
	assert.False(t, harness.processor.IsRunning(), "and it must not be running")
	harness.processor.Stop()

	// An OPEN window is admissible, which is what proves the check is about the window's
	// state rather than about the window existing at all.
	storeDeprecationWindow(t, now.AddDate(0, 0, -15), now.AddDate(0, 0, 15))
	assert.NoError(t, harness.processor.startupObstacle(),
		"inside the window the relay must start normally")

	// So is a CLOSED one: the sunset having passed is the intended end state, not a
	// misconfiguration, and the relay is still the thing that publishes to Kafka.
	storeDeprecationWindow(t, now.AddDate(0, 0, -60), now.AddDate(0, 0, -30))
	assert.NoError(t, harness.processor.startupObstacle(),
		"after the sunset the relay must still run; Kafka is then the only transport")
}

// TestEventRelayProcessor_RefusesToStartWithAnUnusableWindow is the relay leg of the
// fail-closed rule.
//
// A relay only reaches the window check with a REAL publisher — the no-op is refused
// above it — so Kafka is configured. For such a process WebhookWindowUnavailable means
// configuration describes no usable dual-delivery window at all, and the sunset
// predicate fails closed on that: WebhookSunsetPassed answers true, so the per-row
// legacy leg is skipped and NO webhooks are enqueued, while nothing in configuration
// said a retirement had happened. Subscribers who have not migrated stop receiving
// events with nothing failing to say so — the same silent loss as the not-yet-open
// case, arrived at from the other end of the window.
//
// config.resolveWebhookDeprecationWindow now refuses to LOAD brokers without a usable
// window, so this arm is a second line of defence. It is reachable only by publishing
// configuration some other way, which is exactly what the fetchConfiguration seam does
// here — the global store cannot hold this shape, because validateAndAddDefaults
// rejects it.
func TestEventRelayProcessor_RefusesToStartWithAnUnusableWindow(t *testing.T) {
	restoreFetchConfiguration(t)
	sunsetParseWarnings.reset()

	harness := newRelayHarness(t)
	// The real resolution, not the harness's pinned-open stub: the state under test is one
	// only the production resolver produces.
	harness.processor.dualDeliveryActive = harness.wiredWindow
	harness.processor.windowState = WebhookDualDeliveryWindowState

	for name, sunset := range map[string]string{
		"no sunset at all":      "",
		"an unparseable sunset": "30 days from now",
	} {
		t.Run(name, func(t *testing.T) {
			sunsetParseWarnings.reset()
			fetchConfiguration = func() (*config.Configuration, error) {
				return &config.Configuration{
					Kafka:                        config.KafkaConfig{Brokers: []string{"broker-1:9092"}},
					WebhookDeprecationSunsetDate: sunset,
				}, nil
			}

			require.Equal(t, WebhookWindowUnavailable, harness.processor.windowStateAt(relayFixedNow),
				"the fixture must actually produce the unusable state, or this proves nothing")

			err := harness.processor.startupObstacle()
			require.Error(t, err, "a publishing relay with no usable window must refuse to run")
			assert.Contains(t, err.Error(), "WEBHOOK_DEPRECATION_SUNSET_DATE",
				"the error must name the variable an operator has to set, or it is not actionable")

			startErr := harness.processor.Start(context.Background())
			require.Error(t, startErr,
				"Start must RETURN the obstacle: a logged-and-discarded refusal is how a "+
					"deployment came to serve while draining nothing")
			assert.Equal(t, err.Error(), startErr.Error(),
				"and it must be the same obstacle, so the variable to set survives the return")
			assert.False(t, harness.processor.IsRunning(), "and it must not be running")
			harness.processor.Stop()
		})
	}

	// A USABLE window is admissible, which is what proves the check is about the window being
	// unusable rather than about the resolver being consulted at all.
	t.Run("a usable window still starts", func(t *testing.T) {
		fetchConfiguration = func() (*config.Configuration, error) {
			return &config.Configuration{
				Kafka:                        config.KafkaConfig{Brokers: []string{"broker-1:9092"}},
				WebhookDeprecationStartDate:  relayFixedNow.AddDate(0, 0, -15).Format(time.RFC3339),
				WebhookDeprecationSunsetDate: relayFixedNow.AddDate(0, 0, 15).Format(time.RFC3339),
			}, nil
		}

		assert.NoError(t, harness.processor.startupObstacle(),
			"inside a configured window the relay must start normally")
	})
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
		assert.Error(t, processor.Start(context.Background()),
			"a processor built from nothing must return its refusal rather than only logging it")
		processor.Stop()
	})
	assert.False(t, processor.IsRunning(), "a refused Start must not report as running")

	var nilStart *EventRelayProcessor
	assert.NotPanics(t, func() {
		assert.Error(t, nilStart.Start(context.Background()),
			"a nil receiver must answer with an error rather than panicking; the return type is "+
				"now what a caller branches on")
	})

	var absent *EventRelayProcessor
	assert.NotPanics(t, func() {
		absent.Stop()
		_ = absent.IsRunning()
	}, "a nil processor must be safe to stop and inspect")
	assert.False(t, absent.IsRunning())
}

// TestEventRelayProcessor_RefusesToStartWithoutAUsableTransport is the data-loss guard.
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

			startErr := harness.processor.Start(context.Background())
			t.Cleanup(harness.processor.Stop)

			require.Error(t, startErr,
				"Start must return the obstacle so the process that owns the lifecycle can refuse "+
					"to serve; logging alone is what let this state ship")
			assert.Equal(t, obstacle.Error(), startErr.Error(),
				"and it must be the same obstacle, naming what is missing")
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

	// EVERY Start MUST ANSWER nil, including the two that do nothing. Idempotency is the
	// contract — cmd/server.go refuses to serve when Start reports an obstacle — so a
	// repeated call that reported one would make a harmless double start look like a real
	// refusal.
	require.NoError(t, harness.processor.Start(ctx))
	require.NoError(t, harness.processor.Start(ctx), "a second Start is a no-op, not an obstacle")
	require.NoError(t, harness.processor.Start(ctx), "and so is a third")

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

	require.NoError(t, harness.processor.Start(ctx))
	require.Eventually(t, func() bool { return len(harness.store.snapshotClaims()) > 0 },
		2*time.Second, 5*time.Millisecond, "the first run must poll")
	harness.processor.Stop()

	claimsAfterFirstRun := len(harness.store.snapshotClaims())

	require.NoError(t, harness.processor.Start(ctx),
		"a relay that was stopped and started again must not report an obstacle: Stop clears the "+
			"running flag, it does not withdraw a precondition")
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
	require.NoError(t, harness.processor.Start(ctx))

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

	require.NoError(t, harness.processor.Start(ctx))

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

// TestEventRelayProcessor_IsRunningTracksTheLifecycle walks the flag through every
// state a caller can observe it in.
func TestEventRelayProcessor_IsRunningTracksTheLifecycle(t *testing.T) {
	harness := newRelayHarness(t)
	harness.processor.WithPollInterval(5 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert.False(t, harness.processor.IsRunning(), "before Start: not running")

	require.NoError(t, harness.processor.Start(ctx), "a usable relay must start")
	assert.True(t, harness.processor.IsRunning(), "after Start: running")

	// A SECOND Start RETURNS NIL, and that is a contract rather than an accident: the
	// caller asked for a draining relay and there is one. cmd/server.go refuses to serve
	// on a non-nil return, so an error here would turn a harmless double call into a
	// failed boot.
	assert.NoError(t, harness.processor.Start(ctx),
		"an idempotent second Start must not report the running relay as a refusal")
	assert.True(t, harness.processor.IsRunning(),
		"an ignored second Start must leave the flag alone rather than toggling it")

	harness.processor.Stop()
	assert.False(t, harness.processor.IsRunning(), "after Stop: not running")

	harness.processor.Stop()
	assert.False(t, harness.processor.IsRunning(), "a second Stop must leave it that way")

	require.NoError(t, harness.processor.Start(ctx),
		"a restart after two Stops must not report an obstacle")
	assert.True(t, harness.processor.IsRunning(), "after a restart: running again")
	harness.processor.Stop()
	assert.False(t, harness.processor.IsRunning(), "and stoppable again")

	// A REFUSED Start must never report as running. This is the case that matters
	// operationally: the relay declines to run because it has no usable transport, and an
	// operator reading a "running" flag would have no idea the outbox was not draining.
	refused := newRelayHarness(t)
	refused.processor.publisher = nil

	assert.Error(t, refused.processor.Start(ctx),
		"and the refusal must be RETURNED as well as reflected in the flag, so a caller that "+
			"never reads IsRunning still cannot serve over it")
	t.Cleanup(refused.processor.Stop)

	assert.False(t, refused.processor.IsRunning(),
		"a relay that refused to start must never report as running")
}

// TestEventRelayProcessor_AClaimFailureDoesNotStopTheLoop asserts the run loop SURVIVES
// a failing database and recovers on its own.
func TestEventRelayProcessor_AClaimFailureDoesNotStopTheLoop(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newRelayHarness(t, relayTransactionRow(1, "evt-after-recovery"))
	harness.processor.WithPollInterval(5 * time.Millisecond)
	harness.store.setClaimErr(errors.New("relay test: connection refused"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, harness.processor.Start(ctx))
	t.Cleanup(harness.processor.Stop)

	require.Eventually(t, func() bool { return len(harness.store.snapshotClaims()) >= 3 },
		2*time.Second, 5*time.Millisecond,
		"a claim error must not end the run loop; the relay must keep polling")

	assert.True(t, harness.processor.IsRunning(),
		"and must still report as running while the database is unavailable")
	assert.Empty(t, harness.publisher.snapshotRequests(),
		"nothing can be published while the claim fails")
	require.NotEmpty(t, relayEntriesWithMessage(hook, "failed to claim event outbox entries"),
		"every failed claim must be logged, not swallowed")

	// The database comes back. No restart, no intervention: the next tick claims the backlog.
	harness.store.setClaimErr(nil)

	require.Eventually(t, func() bool { return len(harness.store.snapshotDispatched()) == 1 },
		2*time.Second, 5*time.Millisecond,
		"once claiming succeeds again the relay must drain the backlog without a restart")

	assert.Equal(t, []string{"evt-after-recovery"}, harness.publisher.publishedIDs(),
		"and the row that was waiting throughout the outage must be the one delivered")
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

	assert.Equal(t, row.EventRaw, request.Raw,
		"the STORED CANONICAL ENVELOPE must reach the publisher: it is what resolveEventValue "+
			"puts on the wire, so a retry, the dead-letter copy and a replay of this row are "+
			"byte-identical by construction rather than by the serialiser happening not to have "+
			"changed between them")
	assert.NotEmpty(t, request.Raw,
		"a claimed row always carries event_raw — the column is NOT NULL — so an empty value here "+
			"means the relay assembled the envelope itself and the fallback is silently in use")
}

// TestProcessBatch_PublishesTheStoredEnvelopeAndNeverARebuild is the relay's half of
// the byte-fidelity guarantee, and it is asserted here because the relay is the PRIMARY
// publisher: a stored envelope honoured on replay but rebuilt on first delivery would
// mean subscribers received one byte sequence and the replay of that same event
// another.
func TestProcessBatch_PublishesTheStoredEnvelopeAndNeverARebuild(t *testing.T) {
	row := relayTransactionRow(11, "evt-stored-envelope")

	rebuilt, err := row.CanonicalEvent().CanonicalBytes()
	require.NoError(t, err)

	stored := []byte("{\n" +
		`  "schema_version": 1,` + "\n" +
		`  "event_id": "` + row.EventID + "\",\n" +
		`  "event_type": "` + row.EventType + "\",\n" +
		`  "aggregate_id": "` + row.AggregateID + "\",\n" +
		`  "occurred_at": "` + row.OccurredAt.Format(time.RFC3339Nano) + "\",\n" +
		`  "envelope_extension": {"written_by": "a later build of blnk"},` + "\n" +
		`  "payload": ` + string(row.Payload) + "\n" +
		"}")
	require.True(t, json.Valid(stored), "the stored envelope must be valid JSON")
	require.NotEqual(t, string(rebuilt), string(stored),
		"the fixture must be UNREACHABLE by composition, or this test proves nothing")

	row.EventRaw = stored

	harness := newRelayHarness(t, row)
	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	requests := harness.publisher.snapshotRequests()
	require.Len(t, requests, 1)

	value, err := resolveEventValue(requests[0])
	require.NoError(t, err)

	assert.Equal(t, string(stored), string(value),
		"the relay must publish the bytes the row stores")
	assert.NotEqual(t, string(rebuilt), string(value),
		"the relay must not rebuild the envelope from the row's columns")
	assert.Contains(t, string(value), `"envelope_extension"`,
		"a member this build does not model must still reach the topic, because the stored value "+
			"is the record of what a subscriber was sent and not a rendering this build may reinterpret")
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
// Ordering
// ---------------------------------------------------------------------------

// TestProcessBatch_PublishesInTheClaimOrderRatherThanReorderingIt is the relay's half
// of the FIFO guarantee.
func TestProcessBatch_PublishesInTheClaimOrderRatherThanReorderingIt(t *testing.T) {
	// Descending ids, descending event ids, ASCENDING occurrence — the three orders are
	// deliberately in conflict so only one of them can be the one observed.
	claimOrder := []model.EventOutbox{
		relayRow(90, "evt-zulu", "transaction.applied", "ldg_1", relayFixedNow.Add(1*time.Second)),
		relayRow(70, "evt-yankee", "transaction.applied", "ldg_2", relayFixedNow.Add(2*time.Second)),
		relayRow(50, "evt-xray", "transaction.applied", "ldg_3", relayFixedNow.Add(3*time.Second)),
		relayRow(30, "evt-whiskey", "transaction.applied", "ldg_4", relayFixedNow.Add(4*time.Second)),
		relayRow(10, "evt-victor", "transaction.applied", "ldg_5", relayFixedNow.Add(5*time.Second)),
	}
	expected := []string{"evt-zulu", "evt-yankee", "evt-xray", "evt-whiskey", "evt-victor"}

	t.Run("across aggregates published sequentially", func(t *testing.T) {
		harness := newRelayHarness(t, claimOrder...)
		harness.processor.WithBatchSize(len(claimOrder)).WithConcurrency(1)

		require.Equal(t, len(claimOrder), harness.processor.processBatch(context.Background()))

		assert.Equal(t, expected, harness.publisher.publishedIDs(),
			"the publish order must be the claim order — oldest occurrence first")

		// The same property stated as the requirement states it, so the assertion cannot be
		// satisfied by a coincidence of event-id ordering.
		occurrences := make([]time.Time, 0, len(claimOrder))
		for _, req := range harness.publisher.snapshotRequests() {
			occurrences = append(occurrences, req.Event.OccurredAt)
		}
		for index := 1; index < len(occurrences); index++ {
			assert.True(t, occurrences[index].After(occurrences[index-1]),
				"publish %d must have occurred later than publish %d", index, index-1)
		}
	})

	t.Run("within one aggregate under the default concurrency", func(t *testing.T) {
		// Every row below shares one partition key, so they all land in one group and must be
		// published in order even though the relay is free to run eight groups at once.
		rows := make([]model.EventOutbox, 0, len(claimOrder))
		for index, row := range claimOrder {
			rows = append(rows, relayRow(
				row.ID, row.EventID, row.EventType, "ldg_shared",
				relayFixedNow.Add(time.Duration(index+1)*time.Second),
			))
		}

		harness := newRelayHarness(t, rows...)
		harness.processor.WithBatchSize(len(rows))

		require.Equal(t, defaultEventRelayConcurrency, harness.processor.concurrency,
			"this must run at the shipped concurrency, not a serialised one")
		require.Equal(t, len(rows), harness.processor.processBatch(context.Background()))

		assert.Equal(t, expected, harness.publisher.publishedIDs(),
			"one aggregate must be published in occurrence order regardless of concurrency")
	})
}

// TestProcessBatch_BoundsConcurrencyAndKeepsOneAggregateOnOneWorker asserts the two
// properties that make concurrent publishing safe rather than merely fast.
//
// Concurrency and ordering sound incompatible, and the reason they are not is specific:
// Kafka orders within a PARTITION, the partition comes from the message key, and the
// key is the row's partition key — so ordering survives concurrency exactly as long as
// no two rows sharing a key are ever in flight at the same time.
func TestProcessBatch_BoundsConcurrencyAndKeepsOneAggregateOnOneWorker(t *testing.T) {
	t.Run("no more publishes are in flight than there are permits", func(t *testing.T) {
		const (
			permits    = 3
			aggregates = 9
		)

		rows := make([]model.EventOutbox, 0, aggregates)
		for index := range aggregates {
			rows = append(rows, relayRow(
				int64(index+1), fmt.Sprintf("evt-%d", index), "transaction.applied",
				fmt.Sprintf("ldg_%d", index), relayFixedNow.Add(time.Duration(index)*time.Millisecond),
			))
		}

		harness := newRelayHarness(t, rows...)
		harness.processor.WithBatchSize(aggregates).WithConcurrency(permits)

		var (
			inFlight atomic.Int32
			peak     atomic.Int32
			once     sync.Once
			reached  = make(chan struct{})
			release  = make(chan struct{})
		)

		// Every publish parks until the ceiling has been observed, so the assertion does not
		// depend on how fast the goroutines happen to be scheduled.
		harness.publisher.beforePublish = func(PublishRequest) {
			current := inFlight.Add(1)
			for {
				seen := peak.Load()
				if current <= seen || peak.CompareAndSwap(seen, current) {
					break
				}
			}

			if current >= permits {
				once.Do(func() { close(reached) })
			}

			<-release
			inFlight.Add(-1)
		}

		go func() {
			// The timeout is the failure path: if fewer than `permits` publishes are ever
			// concurrent, releasing anyway lets the peak assertion below report it rather than
			// deadlocking the suite.
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
			}

			close(release)
		}()

		require.Equal(t, aggregates, harness.processor.processBatch(context.Background()))

		assert.LessOrEqual(t, peak.Load(), int32(permits),
			"the semaphore must bound in-flight publishes; more than %d at once means the "+
				"permit count is not being honoured", permits)
		assert.Equal(t, int32(permits), peak.Load(),
			"and it must actually reach the ceiling, or the relay is publishing sequentially "+
				"and cannot meet the throughput requirement")
		assert.Len(t, harness.store.snapshotDispatched(), aggregates,
			"every row must still be published and marked")
	})

	t.Run("two rows sharing an aggregate are never published at the same time", func(t *testing.T) {
		const (
			perAggregate = 3
			permits      = 8
		)

		aggregates := []string{"ldg_a", "ldg_b", "ldg_c", "ldg_d"}

		var rows []model.EventOutbox
		id := int64(0)
		for index := range perAggregate {
			for _, aggregate := range aggregates {
				id++
				rows = append(rows, relayRow(
					id, fmt.Sprintf("%s-%d", aggregate, index), "transaction.applied",
					aggregate, relayFixedNow.Add(time.Duration(id)*time.Millisecond),
				))
			}
		}

		harness := newRelayHarness(t, rows...)
		harness.processor.WithBatchSize(len(rows)).WithConcurrency(permits)

		var (
			mu       sync.Mutex
			active   = map[string]int{}
			overlaps []string
		)

		harness.publisher.beforePublish = func(req PublishRequest) {
			mu.Lock()
			active[req.Key]++
			if active[req.Key] > 1 {
				overlaps = append(overlaps, req.Key)
			}
			mu.Unlock()

			// A real window rather than an instantaneous one: without it two publishes could
			// share a key and simply never coincide.
			time.Sleep(2 * time.Millisecond)

			mu.Lock()
			active[req.Key]--
			mu.Unlock()
		}

		require.Equal(t, len(rows), harness.processor.processBatch(context.Background()))

		mu.Lock()
		observedOverlaps := append([]string(nil), overlaps...)
		mu.Unlock()

		assert.Empty(t, observedOverlaps,
			"rows sharing a partition key must be published one at a time; overlapping keys "+
				"means work was round-robined across workers instead of partitioned by aggregate")

		// And the order within each aggregate is the occurrence order, which is the property
		// the non-overlap above exists to protect.
		observed := map[string][]string{}
		for _, req := range harness.publisher.snapshotRequests() {
			observed[req.Key] = append(observed[req.Key], req.Event.EventID)
		}
		for _, aggregate := range aggregates {
			expected := make([]string, 0, perAggregate)
			for index := range perAggregate {
				expected = append(expected, fmt.Sprintf("%s-%d", aggregate, index))
			}
			assert.Equal(t, expected, observed[aggregate],
				"aggregate %s must be published in occurrence order", aggregate)
		}
	})
}

// TestProcessBatch_PreservesPerAggregateOrderUnderConcurrency is the ordering guarantee
// in the one arrangement that could break it: several aggregates interleaved in one
// batch, published concurrently.
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

// TestGroupEventRowsByPartitionKey_GroupsByTheKeyThePublishUses closes the gap between
// the grouping and the routing.
//
// Concurrency makes the resulting misordering non-deterministic, so it would surface as
// an intermittent ordering complaint with nothing in the data to explain it.
func TestGroupEventRowsByPartitionKey_GroupsByTheKeyThePublishUses(t *testing.T) {
	// Two rows on ONE ledger whose stored keys differ — the divergent case. Under the old
	// grouping these were two groups; they are one partition.
	first := relayRow(21, "d-1", "transaction.applied", "bln_legacy_one", relayFixedNow)
	first.LedgerID = "ldg_shared"
	second := relayRow(22, "d-2", "transaction.applied", "bln_legacy_two", relayFixedNow.Add(time.Second))
	second.LedgerID = "ldg_shared"

	// A third row on a DIFFERENT ledger, so the test proves the grouping still separates
	// genuinely independent aggregates rather than collapsing everything into one group.
	other := relayRow(23, "e-1", "transaction.applied", "bln_legacy_one", relayFixedNow.Add(2*time.Second))
	other.LedgerID = "ldg_other"

	require.NotEqual(t, first.PartitionKey, second.PartitionKey,
		"the fixture's whole point is that the stored keys differ")
	require.Equal(t, first.EffectiveKey(), second.EffectiveKey(),
		"...while the key the publish uses is the same")

	groups := groupEventRowsByPartitionKey([]model.EventOutbox{first, second, other})

	require.Len(t, groups, 2, "one group per PUBLISHED key, not per stored column")
	assert.Equal(t, []string{"d-1", "d-2"}, relayGroupEventIDs(groups[0]),
		"two rows headed for one partition must be published by one goroutine, in claim order")
	assert.Equal(t, []string{"e-1"}, relayGroupEventIDs(groups[1]),
		"a different ledger is a different partition and may proceed concurrently")

	// AND THE STORED COLUMN ALONE WOULD SPLIT THEM. Asserted explicitly so the test states the
	// defect it guards rather than only the fixed behaviour.
	byStoredColumn := map[string]int{}
	for _, row := range []model.EventOutbox{first, second, other} {
		byStoredColumn[row.PartitionKey]++
	}
	assert.Len(t, byStoredColumn, 2,
		"the stored keys form a different partitioning of these rows, which is what made the old "+
			"grouping wrong rather than merely redundant")
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
// Per-attempt logging and the retry schedule in force
// ---------------------------------------------------------------------------

// TestProcessRow_LogsOneDurableRecordPerFailedAttemptAndNeverRestatesTheAttempt is the
// relay's half of the per-attempt logging contract, and it pins both halves of it.
//
// The five delays asserted here are the frozen schedule the requirement mandates — 1s,
// 2s, 4s, 8s, 16s — read from the log rather than from the policy helper.
func TestProcessRow_LogsOneDurableRecordPerFailedAttemptAndNeverRestatesTheAttempt(t *testing.T) {
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

	// HALF ONE: the relay does not restate the attempt. Asserted by message, because that is
	// how an operator counts attempts and how the duplication was originally noticed.
	assert.Empty(t, relayEntriesWithMessage(hook, "publishing a ledger event failed"),
		"the relay must not log the attempt a second time: the publisher's unconditional line "+
			"is the single canonical per-attempt record, and a relay copy beside it doubles the "+
			"log volume of an incident and makes line counting wrong")

	// HALF TWO: one durable record per attempt, and the delays are the mandated schedule.
	scheduled := relayEntriesWithMessage(hook, "scheduled for another attempt")
	require.Len(t, scheduled, 4,
		"five attempts have four gaps between them, so four of them schedule a further attempt")

	waited := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
	}
	for index, entry := range scheduled {
		attempt := index + 1

		assert.Equal(t, logrus.WarnLevel, entry.Level,
			"a scheduled retry is a warning: the event has not been delivered yet")
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
		assert.Equal(t, waited[index].String(), entry.Data["retry_after"],
			"attempt %d must report the mandated delay it scheduled", attempt)
	}

	// The fifth delay. It IS produced by the live path and IS recorded on the row; what it
	// never does is separate two publishes, because the budget is spent.
	exhausted := relayEntriesWithMessage(hook, "last attempt this row's budget allowed")
	require.Len(t, exhausted, 1, "exactly one attempt spends the budget")
	assert.Equal(t, 5, exhausted[0].Data["attempt"])
	assert.Equal(t, (16 * time.Second).String(), exhausted[0].Data["retry_after"],
		"the schedule's fifth value is 16s and the live path produces it like the other four")
	assert.Equal(t, false, exhausted[0].Data["retry_after_waited"],
		"and the line must say plainly that nothing waits it, so 'produced' is never read as 'waited'")
}

// TestProcessRow_LogsASuccessfulAttemptAtDebugCarryingTheSameFields covers logAttempt's
// other arm, and the level split between the two.
func TestProcessRow_LogsASuccessfulAttemptAtDebugCarryingTheSameFields(t *testing.T) {
	t.Run("at debug level the attempt is logged with the same field set", func(t *testing.T) {
		relayPinLogLevel(t, logrus.DebugLevel)

		hook := logtest.NewGlobal()
		defer hook.Reset()

		row := relayTransactionRow(1, "evt-success-logged")
		// One failure already recorded, so the attempt now under way is the second. A row
		// with no history would make an off-by-one on the attempt number invisible.
		row.Attempts = 1

		harness := newRelayHarness(t, row)

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		entries := relayEntriesWithMessage(hook, "published a ledger event")
		require.Len(t, entries, 1, "a successful attempt must be logged exactly once")

		entry := entries[0]
		assert.Equal(t, logrus.DebugLevel, entry.Level,
			"a success is debug; only a failure is loud enough to warn about")

		assert.Equal(t, 2, entry.Data["attempt"],
			"the line must name the attempt that succeeded, counted from the row's history")
		assert.Equal(t, 5, entry.Data["max_attempts"], "and the budget it succeeded within")
		assert.Equal(t, "evt-success-logged", entry.Data["event_id"])
		assert.Equal(t, "blnk.transactions", entry.Data["topic"])
		assert.Equal(t, "transaction.applied", entry.Data["event_type"],
			"the event type answers 'which producer' without a second lookup")
		assert.Equal(t, int64(1), entry.Data["outbox_id"])
		assert.Equal(t, string(model.PublishStatusDispatched), entry.Data["status"],
			"the publisher's own verdict must be carried, not re-derived")
		assert.Contains(t, entry.Data, "duration_ms")

		assert.NotContains(t, entry.Data, "error",
			"a successful attempt has no error, and inventing one would make the log lie")
		assert.NotContains(t, entry.Data, "partition_key",
			"the partition key is a financial identifier and must never be logged in clear")
	})

	t.Run("at info level it stays silent", func(t *testing.T) {
		relayPinLogLevel(t, logrus.InfoLevel)

		hook := logtest.NewGlobal()
		defer hook.Reset()

		harness := newRelayHarness(t, relayTransactionRow(1, "evt-success-quiet"))

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		assert.Len(t, harness.store.snapshotDispatched(), 1,
			"the row must still be published and marked; only the logging is suppressed")
		assert.Empty(t, relayEntriesWithMessage(hook, "published a ledger event"),
			"a line per published event at five hundred a second must not reach an info log")
	})

	t.Run("a failure reports its durable consequence whatever the level, and never the attempt twice", func(t *testing.T) {
		relayPinLogLevel(t, logrus.InfoLevel)

		hook := logtest.NewGlobal()
		defer hook.Reset()

		harness := newRelayHarness(t, relayTransactionRow(1, "evt-failure-always-logged"))
		harness.publisher.err = errRelayTransient

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		// The RETRY SCHEDULING is what the relay owns, and no level filter may hide it: it
		// carries the delay stamped on the row and the instant the row is next due, neither
		// of which the publisher knows.
		scheduled := relayEntriesWithMessage(hook, "scheduled for another attempt")
		require.Len(t, scheduled, 1,
			"the durable retry decision must survive a level filter — it is the only record of "+
				"when this event will be tried again")
		assert.Equal(t, logrus.WarnLevel, scheduled[0].Level)
		assert.Equal(t, (1 * time.Second).String(), scheduled[0].Data["retry_after"],
			"and it must carry the delay, which is the whole point of the line")

		// The ATTEMPT itself is the publisher's line, and the relay must not emit a second
		// one. Asserted at info because that is the level a production deployment runs at,
		// which is where the duplication actually cost something.
		assert.Empty(t, relayEntriesWithMessage(hook, "publishing a ledger event failed"),
			"one attempt, one canonical record: the relay must not restate what the publisher "+
				"has already logged unconditionally")
	})
}

// TestProcessRow_SchedulesTheConfiguredBackoffOnEveryFailure is THE LIVE PROOF of
// the configured retry schedule: it drives a real EventRelayProcessor through a full
// five-attempt lifecycle and reads the delay the relay actually handed the repository
// on each one.
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

	// Stated as a relationship as well as a literal, so a mutant that "corrected" both the
	// table above and the implementation to a four-value schedule still fails here.
	require.Len(t, expected, 5,
		"the mandated schedule has one delay per attempt in the budget, and the live path "+
			"produces every one of them")
	assert.Equal(t, 16*time.Second, failures[4].retryAfter,
		"the attempt that spends the budget still produces and records the schedule's final "+
			"delay; it is not waited, because no sixth publish follows it, and reading "+
			"'not waited' as 'not produced' is what makes the mandated sequence look truncated")

	for _, failure := range failures {
		assert.Contains(t, failure.reason, "broker unavailable",
			"the reason stored in last_error must name the failure")
	}
}

// Its own doc records why the EventsDispatchedTotal suite is deliberately not folded in
// with them.

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

	// The structural half. The elapsed-time assertion above proves THIS path does not sleep;
	// this proves no path does, including the ones a fixture does not reach.
	source := relayParseOwnSource(t)
	assert.Zero(t, qualifiedCallCount(source, "time", "Sleep"),
		"the relay must never sleep a retry delay in process: the backoff is persisted as "+
			"next_attempt_at and waited out by the claim predicate, and a sleep here would "+
			"outlive the lease and let a second instance republish the row mid-sleep")

	// The equivalent written another way. time.After in a blocking receive, or a Timer, sleeps
	// just as effectively, so the rule names the operation rather than one function.
	assert.Zero(t, qualifiedCallCount(source, "time", "After"),
		"nor block on a timer channel, which is the same wait spelled differently")
	assert.Zero(t, qualifiedCallCount(source, "time", "NewTimer"),
		"nor a Timer, for the same reason")
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
// Exhaustion and the dead-letter hand-off
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
// The dual-delivery window. DELETED AT THE WEBHOOK SUNSET, together with the branch these
// tests cover.
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
	harness.processor.dualDeliveryActive = func(time.Time) bool { return false }
	harness.processor.windowState = func(time.Time) WebhookWindowState { return WebhookWindowClosed }

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	assert.Empty(t, harness.legacy.snapshot(),
		"no legacy webhook may be enqueued once the sunset has passed")
	assert.Empty(t, harness.store.snapshotWebhookMarks(),
		"and nothing may be recorded for a leg that did not run")
	assert.Len(t, harness.publisher.snapshotRequests(), 1,
		"the Kafka publish is unaffected by the sunset")
	assert.Len(t, harness.store.snapshotDispatched(), 1)
}

// TestDualDelivery_FollowsTheConfiguredSunsetDateThroughEventSunset drives the branch
// from REAL CONFIGURATION rather than from a stub predicate.
//
// Every other test in this section pins the window predicate so that the boundary is
// exact.
func TestDualDelivery_FollowsTheConfiguredSunsetDateThroughEventSunset(t *testing.T) {
	cases := map[string]struct {
		startOffset  int
		sunsetOffset int
		wantLegacy   int
		wantExplain  string
		wantMarkings int
	}{
		"the window is still open": {
			startOffset:  -15,
			sunsetOffset: 15,
			wantLegacy:   1,
			wantExplain:  "inside the window both transports must run",
			wantMarkings: 1,
		},
		"the window has closed": {
			startOffset:  -60,
			sunsetOffset: -30,
			wantLegacy:   0,
			wantExplain:  "a sunset date in the past must leave Kafka as the only transport",
			wantMarkings: 0,
		},
		"the window has not opened": {
			startOffset:  30,
			sunsetOffset: 60,
			wantLegacy:   0,
			wantExplain: "before the window opens the legacy leg must not run either; the relay " +
				"refuses to START in this state, which is what stops it stranding subscribers",
			wantMarkings: 0,
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			// Saved and restored with t.Cleanup by the helper the sunset tests already use. The
			// offsets are relative to relayFixedNow, which is where the harness pins the clock,
			// so each window sits unambiguously on its side of the boundary.
			storeDeprecationWindow(t,
				relayFixedNow.AddDate(0, 0, testCase.startOffset),
				relayFixedNow.AddDate(0, 0, testCase.sunsetOffset),
			)

			harness := newRelayHarness(t, relayTransactionRow(1, "evt-configured-sunset"))
			// Put back the predicate THE CONSTRUCTOR INSTALLED — not a fresh reference to
			// WebhookDualDeliveryActive, which would re-derive the right answer even if the
			// constructor had wired something else.
			require.NotNil(t, harness.wiredWindow, "the constructor must install a predicate")
			harness.processor.dualDeliveryActive = harness.wiredWindow

			require.Equal(t, 1, harness.processor.processBatch(context.Background()))

			assert.Len(t, harness.legacy.snapshot(), testCase.wantLegacy, testCase.wantExplain)
			assert.Len(t, harness.store.snapshotWebhookMarks(), testCase.wantMarkings,
				"the marker must be written exactly when the legacy leg ran")

			assert.Len(t, harness.publisher.snapshotRequests(), 1,
				"the Kafka publish happens on both sides of the boundary")
			assert.Len(t, harness.store.snapshotDispatched(), 1,
				"and the row reaches its terminal state either way")
		})
	}
}

// TestDualDelivery_ARestartDoesNotDoubleEnqueueTheLegacyWebhook proves the
// no-double-enqueue guarantee through a REAL re-claim cycle rather than a pre-set flag.
//
// Note which duplicate is and is not acceptable here.
func TestDualDelivery_ARestartDoesNotDoubleEnqueueTheLegacyWebhook(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-restart-once"))

	// The crash: everything on the wire succeeds, and the process dies before the row can be
	// marked dispatched.
	harness.store.mu.Lock()
	harness.store.dispatchErr = errors.New("relay test: the process died")
	harness.store.mu.Unlock()

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))
	require.Len(t, harness.legacy.snapshot(), 1, "the legacy leg ran before the crash")
	require.Len(t, harness.store.snapshotWebhookMarks(), 1, "and was recorded on the row")

	// The restart: the database is healthy again and the lease has run out.
	harness.store.mu.Lock()
	harness.store.dispatchErr = nil
	harness.store.mu.Unlock()
	harness.store.expireLeases()

	require.Equal(t, 1, harness.processor.processBatch(context.Background()),
		"a row that never reached a terminal state must be claimable again after its lease")

	assert.Len(t, harness.legacy.snapshot(), 1,
		"the legacy webhook must NOT be enqueued a second time — webhook_dispatched is the "+
			"marker that survives the re-claim and prevents it")
	assert.Len(t, harness.store.snapshotWebhookMarks(), 1,
		"and a row whose legacy leg is already done must not be re-marked")

	assert.Len(t, harness.publisher.snapshotRequests(), 2,
		"the Kafka leg IS republished, and that is the documented at-least-once duplicate; it "+
			"is suppressed at the subscriber on event_id, not prevented here")

	state, terminal := harness.store.terminalState(1)
	assert.True(t, terminal, "the second attempt must complete the row")
	assert.Equal(t, model.EventOutboxStatusDispatched, state)
	assert.Empty(t, harness.store.snapshotFailures(),
		"a crash in the bookkeeping must not consume a publish retry attempt")
}

// TestDualDelivery_EvaluatesTheBoundaryImmediatelyBeforeEachEnqueue asserts the
// decision is taken PER ROW, immediately before the enqueue — not once at the top of
// the batch.
func TestDualDelivery_EvaluatesTheBoundaryImmediatelyBeforeEachEnqueue(t *testing.T) {
	t.Run("one decision per row", func(t *testing.T) {
		rows := make([]model.EventOutbox, 0, 4)
		for id := int64(1); id <= 4; id++ {
			rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-%d", id)))
		}

		harness := newRelayHarness(t, rows...)

		var decisions atomic.Int32
		harness.processor.dualDeliveryActive = func(time.Time) bool {
			decisions.Add(1)

			return true
		}

		require.Equal(t, 4, harness.processor.processBatch(context.Background()))

		assert.Equal(t, int32(4), decisions.Load(),
			"the boundary must be evaluated once per row, immediately before that row's enqueue")
		assert.Len(t, harness.legacy.snapshot(), 4, "every row must still get its legacy leg")
	})

	t.Run("the boundary passing mid-batch stops the remaining legacy legs", func(t *testing.T) {
		rows := make([]model.EventOutbox, 0, 4)
		for id := int64(1); id <= 4; id++ {
			rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-crossing-%d", id)))
		}

		// Concurrency is pinned to one so the batch is processed in claim order and "the
		// first two rows" is a well-defined set. With the default of eight the rows would
		// publish in parallel and which two crossed the boundary would be a race.
		harness := newRelayHarness(t, rows...)
		harness.processor.WithConcurrency(1)

		var decisions atomic.Int32
		harness.processor.dualDeliveryActive = func(time.Time) bool {
			// Open for the first two evaluations, closed from the third: the sunset arriving
			// while this batch is still being published.
			return decisions.Add(1) <= 2
		}

		require.Equal(t, 4, harness.processor.processBatch(context.Background()))

		assert.Equal(t, int32(4), decisions.Load(), "every row must consult the boundary itself")
		assert.Len(t, harness.legacy.snapshot(), 2,
			"only the rows dispatched BEFORE the boundary may have a legacy leg; a batch-level "+
				"decision would have enqueued all four")
		assert.Len(t, harness.publisher.snapshotRequests(), 4,
			"the Kafka leg is unaffected by the boundary — it is the transport that remains")
		assert.Len(t, harness.store.snapshotDispatched(), 4,
			"and every row still reaches its terminal state")
	})
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
		assert.Empty(t, harness.store.snapshotFailures(),
			"a legacy failure must never spend a Kafka retry attempt")
		assert.Empty(t, harness.deadLetters.snapshotRows())

		// THE ROW IS NOT DISPATCHED, and that is the whole point. dispatched is outside the
		// claim predicate, so a row marked dispatched with its webhook still owed is a
		// webhook nobody will ever enqueue again.
		assert.Empty(t, harness.store.snapshotDispatched(),
			"a row whose legacy leg is still owed must NOT be moved to its terminal state")

		pendings := harness.store.snapshotWebhookPendings()
		require.Len(t, pendings, 1,
			"the outstanding legacy leg must be recorded, which is what keeps the row claimable for it")
		assert.Equal(t, int64(1), pendings[0].id)
		assert.Equal(t, "token-1", pendings[0].claimToken,
			"the transition must be conditional on the claim this pass held")
		assert.Contains(t, pendings[0].reason, "redis unavailable",
			"last_error must carry why the enqueue failed, or an operator has nothing to triage")
		assert.Equal(t, time.Second, pendings[0].retryAfter,
			"the first webhook retry waits the configured base backoff, indexed by the WEBHOOK "+
				"attempt count rather than the Kafka one")

		state, terminal := harness.store.terminalState(1)
		assert.False(t, terminal, "and the row must not be terminal")
		assert.Empty(t, state)

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

// TestDualDelivery_AFailedEnqueueIsRetriedWithoutRepublishingToKafka is the positive
// proof that the two legs are tracked independently, and it is the test the previous
// behaviour could not have passed.
//
// The first pass publishes to Kafka and fails to enqueue. The second pass — a REAL
// re-claim of the same row, not a pre-set flag — must enqueue the webhook and nothing
// else.
//
//   - if the row were not claimable, the webhook would be lost, which is the defect;
//   - if the re-claim republished to Kafka, retrying a deprecated-transport failure
//     would put a duplicate on the topic — paid for by every subscriber's idempotency
//     filter, for the benefit of a transport being retired.
func TestDualDelivery_AFailedEnqueueIsRetriedWithoutRepublishingToKafka(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-legacy-recovered"))
	harness.legacy.err = errors.New("relay test: redis unavailable")

	// PASS ONE: Kafka succeeds, the enqueue does not.
	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	require.Len(t, harness.publisher.snapshotRequests(), 1, "the Kafka leg must have been published")
	require.Len(t, harness.store.snapshotWebhookPendings(), 1, "the outstanding leg must be recorded")
	require.Empty(t, harness.store.snapshotDispatched(), "and the row must not be terminal")

	// The row must be back in the claimable set once its webhook backoff has elapsed. This is
	// the property that matters: dispatched rows are never claimed again.
	afterBackoff := relayFixedNow.Add(72 * time.Hour)
	require.Contains(t, harness.store.claimableIDs(afterBackoff), int64(1),
		"a row whose legacy leg is owed must return to the claimable set")

	// PASS TWO: the queue is back. The clock moves past the backoff so the row is due.
	harness.legacy.err = nil
	harness.store.now = func() time.Time { return afterBackoff }
	harness.processor.now = func() time.Time { return afterBackoff }

	require.Equal(t, 1, harness.processor.processBatch(context.Background()),
		"the row must be claimed a second time, for its webhook alone")

	enqueued := harness.legacy.snapshot()
	require.Len(t, enqueued, 1, "the webhook must be enqueued on the retry")
	assert.Equal(t, "evt-legacy-recovered", enqueued[0].eventID,
		"under the same task identity, so a further re-claim cannot double-enqueue")

	assert.Len(t, harness.publisher.snapshotRequests(), 1,
		"THE KAFKA LEG MUST NOT BE REPUBLISHED: its acknowledgement is recorded on the row, so "+
			"retrying the webhook costs the topic nothing")

	marks := harness.store.snapshotWebhookMarks()
	require.Len(t, marks, 1, "the legacy leg must be recorded once it is enqueued")
	assert.Equal(t, int64(1), marks[0].id)

	dispatched := harness.store.snapshotDispatched()
	require.Len(t, dispatched, 1, "and NOW the row is terminal, with both legs done")

	state, terminal := harness.store.terminalState(1)
	assert.True(t, terminal)
	assert.Equal(t, model.EventOutboxStatusDispatched, state)

	assert.Empty(t, harness.store.snapshotFailures(),
		"no Kafka retry attempt may have been spent on any of this")
	assert.Empty(t, harness.deadLetters.snapshotRows(),
		"and a legacy failure must never dead-letter an event that reached its topic")
}

// TestDualDelivery_AnUnreachableQueueAbandonsTheLegacyLegLoudly covers the terminal arm
// of the webhook budget, which exists so a permanently unreachable queue cannot keep a
// row claimable forever.
func TestDualDelivery_AnUnreachableQueueAbandonsTheLegacyLegLoudly(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	row := relayTransactionRow(1, "evt-legacy-abandoned")
	// One attempt away from the budget, so this pass is the last one. Reproducing all five
	// passes would assert the same arithmetic MarkEventWebhookPending's own tests cover.
	row.WebhookAttempts = row.MaxAttempts - 1

	harness := newRelayHarness(t, row)
	harness.legacy.err = errors.New("relay test: redis permanently unavailable")

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	pendings := harness.store.snapshotWebhookPendings()
	require.Len(t, pendings, 1)

	state, terminal := harness.store.terminalState(1)
	require.True(t, terminal,
		"the legacy budget is spent, so the row must not stay claimable indefinitely")
	assert.Equal(t, model.EventOutboxStatusDispatched, state,
		"and it is terminal on the strength of the Kafka delivery, which did happen")

	assert.NotContains(t, harness.store.claimableIDs(relayFixedNow.Add(72*time.Hour)), int64(1),
		"an abandoned legacy leg must not leave the row cycling through claims forever")

	entries := relayEntriesWithMessage(hook, "ABANDONED")
	require.NotEmpty(t, entries, "abandoning a promised webhook must be reported")
	assert.Equal(t, logrus.ErrorLevel, entries[0].Level,
		"this is the only notice that a webhook will never be delivered, so it is an error")
	assert.Contains(t, entries[0].Data, "event_id",
		"and it must name the event, or it is not actionable")
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
// Crash recovery
//
// THE GUARANTEE, STATED HONESTLY — nothing below asserts exactly-once end-to-end
// delivery, because that is not what this pipeline provides and a test claiming it
// would be wrong.
//
// What IS guaranteed:
//
//   - NO LOSS. The event and the ledger mutation commit in one database transaction,
//     and a claim takes a LEASE rather than removing the row, so a relay that dies
//     mid-batch strands nothing: the rows re-enter the claimable set when the lease
//     expires.
//   - DUPLICATE SUPPRESSION IS POSSIBLE. blnk.event_outbox.event_id is uniquely indexed
//     and travels on the message, so a subscriber can recognise a redelivery.
// ---------------------------------------------------------------------------

// TestEventRelay_LockExpiryReturnsAClaimedRowToTheClaimableSet asserts the mechanism
// the whole recovery story rests on: a claim is a LEASE, not a delete.
//
// Both directions matter.
func TestEventRelay_LockExpiryReturnsAClaimedRowToTheClaimableSet(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-leased"))

	// The row is published but never marked, so it stays claimed under its lease — the state a
	// crashed relay leaves behind.
	harness.store.mu.Lock()
	harness.store.dispatchErr = errors.New("relay test: the process died")
	harness.store.mu.Unlock()

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	assert.NotContains(t, harness.store.claimableIDs(relayFixedNow.Add(24*time.Hour)), int64(1),
		"a leased row must not be claimable while its lease is live, however long anyone waits")
	assert.Zero(t, harness.processor.processBatch(context.Background()),
		"so a second poll finds nothing to do")

	// The lease runs out. From the database's point of view this is indistinguishable from the
	// relay having crashed, which is the point.
	harness.store.expireLeases()

	assert.Contains(t, harness.store.claimableIDs(relayFixedNow), int64(1),
		"an expired lease must return the row to the claimable set")

	harness.store.mu.Lock()
	harness.store.dispatchErr = nil
	harness.store.mu.Unlock()

	require.Equal(t, 1, harness.processor.processBatch(context.Background()),
		"and the next instance — or the same one after a restart — must pick it up")

	state, terminal := harness.store.terminalState(1)
	assert.True(t, terminal, "the recovered row must reach a terminal state")
	assert.Equal(t, model.EventOutboxStatusDispatched, state)
	assert.Len(t, harness.publisher.snapshotRequests(), 2,
		"published twice: the at-least-once duplicate, suppressed at the subscriber on event_id")
}

// TestEventRelay_NoTransitionLeavesARowPermanentlyUnclaimable enumerates every way the
// relay can leave a row and asserts that none of them is a dead end.
//
// This is the invariant that makes "no loss" true in practice rather than in principle.
func TestEventRelay_NoTransitionLeavesARowPermanentlyUnclaimable(t *testing.T) {
	// Far enough ahead that any scheduled retry delay has certainly come due, so a row that is
	// merely waiting is not mistaken for a row that is stuck.
	wellAfterAnyBackoff := relayFixedNow.Add(72 * time.Hour)

	// spendBudget leaves one attempt in the row's budget, so a single failed publish exhausts it.
	spendBudget := func(row *model.EventOutbox) { row.Attempts = row.MaxAttempts - 1 }

	cases := map[string]struct {
		// prepareRow adjusts the row before it becomes claimable; injectFault arranges the
		// failure. They are separate so the row can be handed to the harness at construction
		// rather than pushed into the store's state behind its own lock.
		prepareRow  func(row *model.EventOutbox)
		injectFault func(h *relayHarness)
		// wantTerminal is the terminal status expected, or "" when the row must instead return
		// to the claimable set.
		wantTerminal string
		because      string
	}{
		"the publish fails with budget left": {
			injectFault:  func(h *relayHarness) { h.publisher.err = errRelayTransient },
			wantTerminal: "",
			because:      "a scheduled retry must become due, not disappear",
		},
		"the publish fails with the budget spent": {
			prepareRow:   spendBudget,
			injectFault:  func(h *relayHarness) { h.publisher.err = errRelayTransient },
			wantTerminal: model.EventOutboxStatusFailed,
			because:      "an exhausted row is terminal only because its event is on the dead-letter topic",
		},
		"the dead-letter hand-off fails": {
			prepareRow: spendBudget,
			injectFault: func(h *relayHarness) {
				h.publisher.err = errRelayTransient
				h.deadLetters.err = errors.New("relay test: the dead-letter write failed")
			},
			wantTerminal: model.EventOutboxStatusFailed,
			because:      "the row stays failed and therefore stays visible in the dead-letter inventory",
		},
		"marking the row dispatched fails": {
			injectFault: func(h *relayHarness) {
				h.store.dispatchErr = errors.New("relay test: database unavailable")
			},
			wantTerminal: "",
			because:      "the event is on the topic but the row does not say so, so it must come back",
		},
		"recording the failed attempt fails": {
			injectFault: func(h *relayHarness) {
				h.publisher.err = errRelayTransient
				h.store.failErr = errors.New("relay test: database unavailable")
			},
			wantTerminal: "",
			because:      "an unrecorded failure must not consume the row",
		},
		// NOT terminal, and that is the corrected expectation rather than a relaxed one. A
		// legacy enqueue failure leaves a webhook OWED, and this test's subject is precisely
		// that no transition strands a row: the row must come back so the webhook is retried.
		"the legacy leg fails": {
			injectFault: func(h *relayHarness) {
				h.legacy.err = errors.New("relay test: redis unavailable")
			},
			wantTerminal: "",
			because: "a webhook is still owed, so the row must return to the claimable set for " +
				"the legacy leg alone rather than being marked dispatched with the webhook lost",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			row := relayTransactionRow(1, "evt-transition")
			if testCase.prepareRow != nil {
				testCase.prepareRow(&row)
			}

			harness := newRelayHarness(t, row)
			testCase.injectFault(harness)

			require.Equal(t, 1, harness.processor.processBatch(context.Background()))

			// The lease runs out, as it always eventually does.
			harness.store.expireLeases()

			state, terminal := harness.store.terminalState(row.ID)

			if testCase.wantTerminal == "" {
				assert.False(t, terminal,
					"%s: the row must not be terminal — %s", name, testCase.because)
				assert.Contains(t, harness.store.claimableIDs(wellAfterAnyBackoff), row.ID,
					"%s: the row must be claimable again — %s", name, testCase.because)

				return
			}

			assert.True(t, terminal, "%s: the row must be terminal — %s", name, testCase.because)
			assert.Equal(t, testCase.wantTerminal, state,
				"%s: %s", name, testCase.because)
		})
	}
}

// TestEventRelay_DrainsTheBacklogTheOutboxGaugeReports asserts the relay's contract
// with blnk.outbox.pending, which is NOT to record it.
//
// So what this asserts is the relay's actual responsibility: MOVING the number the
// collector reports.
func TestEventRelay_DrainsTheBacklogTheOutboxGaugeReports(t *testing.T) {
	const rowCount = 4

	rows := make([]model.EventOutbox, 0, rowCount)
	for id := int64(1); id <= rowCount; id++ {
		rows = append(rows, relayTransactionRow(id, fmt.Sprintf("evt-backlog-%d", id)))
	}

	t.Run("draining the outbox takes the backlog to zero", func(t *testing.T) {
		harness := newRelayHarness(t, rows...)
		harness.processor.WithBatchSize(rowCount)

		require.Equal(t, int64(rowCount), harness.store.unpublishedBacklog(),
			"every captured event starts as un-published work")

		require.Equal(t, rowCount, harness.processor.processBatch(context.Background()))

		assert.Zero(t, harness.store.unpublishedBacklog(),
			"once every row is dispatched the backlog the gauge reports must be zero")
	})

	t.Run("a relay that cannot publish leaves the backlog standing", func(t *testing.T) {
		harness := newRelayHarness(t, rows...)
		harness.processor.WithBatchSize(rowCount)
		harness.publisher.err = errRelayTransient

		require.Equal(t, rowCount, harness.processor.processBatch(context.Background()))

		assert.Equal(t, int64(rowCount), harness.store.unpublishedBacklog(),
			"a broker outage must leave the backlog visible; a gauge that fell to zero here "+
				"would make the alert unable to fire during the outage it exists for")
	})

	t.Run("the relay writes the per-event count and nothing else", func(t *testing.T) {
		// captureBacklogGauge swaps the shared gauge for a recorder and restores it with
		// t.Cleanup, so this cannot leak into the collector's own tests.
		recorder := captureBacklogGauge(t)

		harness := newRelayHarness(t, rows...)
		harness.processor.WithBatchSize(rowCount)

		require.Equal(t, rowCount, harness.processor.processBatch(context.Background()))

		assert.Empty(t, recorder.values(),
			"the relay must not write blnk.outbox.pending; EventMetricsCollector owns it, and a "+
				"second writer would make the gauge disagree with itself between ticks")

		// The structural half. The recorder above proves this batch wrote nothing; this proves
		// no code path in the file can, which is what "the collector owns the gauge" means.
		source := relayParseOwnSource(t)
		assert.Zero(t, identifierUses(source, "OutboxPendingBacklog"),
			"and it must not reference the gauge at all: a second writer would make the gauge "+
				"disagree with itself between the collector's ticks")

		// THE ALLOWLIST, and it replaces a flat ban on importing internal/metrics.
		//
		// What has to stay asserted is the boundary the ban was standing in for, so it is
		// stated directly instead: ONE WRITER PER INSTRUMENT. The allowlist is therefore not
		// a budget but a list of facts only the relay can observe, and each entry has to
		// survive that test in writing:
		//
		//   - EventsDispatchedTotal is taken at the durable transition, which happens here.
		//   - EventRepairsCompletedTotal counts rows a repair pass drove to their destination
		//     Nothing outside this file performs a repair, so nothing else can count one; the
		//     collector reading the table would see the row GONE from the backlog and could
		//     not tell a repair from a purge.
		//   - EventRepairSaturated is whether this tick's repair BUDGET ended the chain.
		selections := qualifiedSelections(source, "metrics")
		assert.Equal(t,
			[]string{"EventRepairSaturated", "EventRepairsCompletedTotal", "EventsDispatchedTotal"},
			sortedKeys(selections),
			"the relay may record only the facts nothing else can observe: the per-event delivery "+
				"count and its own repair progress. The publisher owns the per-attempt instruments "+
				"and EventMetricsCollector owns every count-of-rows gauge, and a second writer of "+
				"either would make them disagree with themselves between ticks")
		assert.Positive(t, selections["EventsDispatchedTotal"],
			"and it must record that one: an event that reaches dispatched with nothing counting "+
				"it is an event missing from the V-1 and V-3 verdicts")
		assert.Positive(t, selections["EventRepairsCompletedTotal"],
			"and the repair drain rate: a repair backlog with no counter behind it cannot be "+
				"divided into a time-to-clear, which is the only actionable reading of it")

		// The collector's gauge must NOT be reachable from here, stated as an absence for the
		// same reason OutboxPendingBacklog is above.
		assert.Zero(t, identifierUses(source, "EventRepairBacklog"),
			"the repair BACKLOG is a count of rows, so EventMetricsCollector owns it and publishes "+
				"it from the aggregate it already reads; a relay writing it would be a second "+
				"writer of one gauge")
	})
}

// sortedKeys returns a map's keys in order, so a set can be asserted with a stable Equal
// rather than with an unordered comparison per element.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

// ---------------------------------------------------------------------------
// The per-event delivery count
// ---------------------------------------------------------------------------

// captureDispatchedCounter swaps blnk.events.dispatched.total for a recorder and
// restores the original when the test ends.
//
// It lives here, beside the two increments, and is shared with event_dlt_test.go, which
// asserts the REPLAY path leaves this counter alone.
func captureDispatchedCounter(t *testing.T) *publisherRecordedCounter {
	t.Helper()

	recorder := &publisherRecordedCounter{}
	original := metrics.EventsDispatchedTotal
	t.Cleanup(func() { metrics.EventsDispatchedTotal = original })
	metrics.EventsDispatchedTotal = recorder

	return recorder
}

// TestEventRelay_CountsEachEventOnceWhenItReachesDispatched is the dispatch-counting guard.
//
// blnk.events.published.total is incremented on the BROKER ACKNOWLEDGEMENT, inside the
// publisher, before the outbox row has been moved.
//
// blnk.events.dispatched.total is incremented at the DURABLE TRANSITION instead.
func TestEventRelay_CountsEachEventOnceWhenItReachesDispatched(t *testing.T) {
	t.Run("one increment per event, attributed by topic and event type", func(t *testing.T) {
		counter := captureDispatchedCounter(t)

		harness := newRelayHarness(t,
			relayTransactionRow(1, "evt-count-1"),
			relayTransactionRow(2, "evt-count-2"),
			relayTransactionRow(3, "evt-count-3"),
		)
		harness.processor.WithBatchSize(10)

		require.Equal(t, 3, harness.processor.processBatch(context.Background()))
		require.Len(t, harness.store.snapshotDispatched(), 3,
			"the premise: every row reached the durable transition")

		records := counter.snapshot()
		require.Len(t, records, 3,
			"three events were delivered, so the per-event counter must move exactly three times")
		assert.EqualValues(t, 3, counter.total())

		for _, record := range records {
			assert.EqualValues(t, 1, record.value, "an event is one event, never a batch")
			// THE SAME ATTRIBUTE SET AS THE WRITE COUNTER, asserted as a whole set because the
			// ratio between the two is only computable if their label sets match: an extra
			// attribute here would split this series against an unsplit denominator and make the
			// redelivery factor unjoinable. The write counter's own set is pinned in
			// TestEventPublisher_RecordsThePublishInstruments.
			assert.Equal(t, map[string]string{
				"topic":      "blnk.transactions",
				"event_type": "transaction.applied",
			}, record.attributes,
				"dispatched events are attributed by topic and event type, and by nothing else")
		}
	})

	t.Run("a broker write the database did not record is not counted", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		counter := captureDispatchedCounter(t)

		harness := newRelayHarness(t, relayTransactionRow(1, "evt-count-unrecorded"))
		harness.store.dispatchErr = errors.New("relay test: the transition failed")

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		require.Len(t, harness.publisher.snapshotRequests(), 1,
			"the premise: the broker did acknowledge the write")
		assert.Empty(t, counter.snapshot(),
			"an event whose row was not moved is going to be PUBLISHED AGAIN when its lease "+
				"expires, so counting it here would count it twice — which is the defect this "+
				"counter exists to remove, reintroduced one line earlier")

		require.NotEmpty(t, relayEntriesWithMessage(hook, "could not be marked dispatched"),
			"and the failure is still reported, so the redelivery is not silent")
	})

	t.Run("a redelivery writes twice and counts one event", func(t *testing.T) {
		// The whole finding, end to end. This is the sequence the published counter reads as
		// two events and the dispatched counter reads as one.
		counter := captureDispatchedCounter(t)

		harness := newRelayHarness(t, relayTransactionRow(1, "evt-count-redelivered"))

		// PASS ONE: the broker takes the message, the process dies before the row is moved.
		harness.store.dispatchErr = errors.New("relay test: the process died")
		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		require.Len(t, harness.publisher.snapshotRequests(), 1)
		require.Empty(t, counter.snapshot(), "nothing durable happened, so nothing is counted")

		// PASS TWO: the lease expires, the row comes back, the same event is written again.
		harness.store.mu.Lock()
		harness.store.dispatchErr = nil
		harness.store.mu.Unlock()
		harness.store.expireLeases()

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		assert.Len(t, harness.publisher.snapshotRequests(), 2,
			"two BROKER WRITES really happened, and blnk.events.published.total is right to "+
				"count both: that is what write throughput and the redelivery factor are read from")
		assert.Len(t, counter.snapshot(), 1,
			"but ONE EVENT was delivered, and the per-event counter must say one — a verdict that "+
				"read two here would over-report throughput and halve the apparent dead-letter rate")
		assert.EqualValues(t, 1, counter.total())

		state, terminal := harness.store.terminalState(1)
		require.True(t, terminal)
		assert.Equal(t, model.EventOutboxStatusDispatched, state)
	})

	t.Run("the abandoned legacy leg is counted, because that row IS dispatched", func(t *testing.T) {
		// The second arm. MarkEventWebhookPending moves the row straight to dispatched when
		// the webhook budget is spent, so the event's delivery is durably recorded there
		// rather than through MarkEventDispatched.
		counter := captureDispatchedCounter(t)

		row := relayTransactionRow(1, "evt-count-abandoned")
		row.WebhookAttempts = row.MaxAttempts - 1

		harness := newRelayHarness(t, row)
		harness.legacy.err = errors.New("relay test: redis permanently unavailable")

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		state, terminal := harness.store.terminalState(1)
		require.True(t, terminal)
		require.Equal(t, model.EventOutboxStatusDispatched, state,
			"the premise: the abandon arm ends the row at dispatched")
		require.Empty(t, harness.store.snapshotDispatched(),
			"and it gets there WITHOUT MarkEventDispatched, which is why a counter wired only to "+
				"that transition would miss it")

		assert.Len(t, counter.snapshot(), 1,
			"the event is on its Kafka topic and its row says so, so it counts exactly once")
	})

	t.Run("a row still owed its webhook is counted on the pass that dispatches it", func(t *testing.T) {
		// The retry arm of the same transition, which does NOT reach dispatched. The row is
		// claimed a second time for its webhook alone, and it is that second pass — not the
		// one that published to Kafka — which moves it to dispatched and counts it.
		counter := captureDispatchedCounter(t)

		harness := newRelayHarness(t, relayTransactionRow(1, "evt-count-owed"))
		harness.legacy.err = errors.New("relay test: redis unavailable")

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))
		require.Len(t, harness.store.snapshotWebhookPendings(), 1,
			"the premise: the row is webhook_pending, not dispatched")
		assert.Empty(t, counter.snapshot(),
			"a row that is still claimable is not delivered work yet: counting it here and again "+
				"when it finally dispatches would count one event twice")

		afterBackoff := relayFixedNow.Add(72 * time.Hour)
		harness.legacy.err = nil
		harness.store.now = func() time.Time { return afterBackoff }
		harness.processor.now = func() time.Time { return afterBackoff }

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		assert.Len(t, harness.publisher.snapshotRequests(), 1,
			"the premise: the webhook-only pass republished nothing")
		assert.Len(t, counter.snapshot(), 1,
			"and the event is counted once, on the pass that made its delivery durable")
	})

	t.Run("a failed publish counts nothing at all", func(t *testing.T) {
		counter := captureDispatchedCounter(t)

		harness := newRelayHarness(t, relayTransactionRow(1, "evt-count-failed"))
		harness.publisher.err = errRelayTransient

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))
		require.Len(t, harness.store.snapshotFailures(), 1, "the premise: the attempt failed")

		assert.Empty(t, counter.snapshot(),
			"an event that has not been delivered must not appear in the delivered count, or the "+
				"dead-letter rate is divided by a number that includes its own numerator")
	})

	t.Run("an exhausted event is counted as dead-lettered and never as dispatched", func(t *testing.T) {
		// The two terminal counters must partition the events, not overlap: the dead-letter rate reads
		// dead-lettered over dispatched, so an event appearing in both would understate the
		// rate while a broker was rejecting everything.
		counter := captureDispatchedCounter(t)

		row := relayTransactionRow(1, "evt-count-exhausted")
		row.Attempts = row.MaxAttempts - 1

		harness := newRelayHarness(t, row)
		harness.publisher.err = errRelayTransient

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))
		require.Len(t, harness.deadLetters.snapshotRows(), 1,
			"the premise: the budget is spent and the event was handed to the dead-letter writer")

		assert.Empty(t, counter.snapshot(),
			"a dead-lettered event was never dispatched, and counting it as both would make the "+
				"two terminal counters overlap")
	})
}

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

	// The bookkeeping the relay already owed must have been completed on its detached
	// context, even though the caller's context was cancelled: otherwise the rows it
	// published would be republished for no reason.
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

// ---------------------------------------------------------------------------
// Lease safety while a batch is in flight
// ---------------------------------------------------------------------------

// TestEventRelay_RenewsTheLeaseWhileTheBatchIsInFlight is the fix for the arithmetic
// that made the shipped defaults unsafe.
//
// The heartbeat renews under THIS BATCH'S CLAIM TOKEN and with the configured lease,
// and it stops when the batch does.
func TestEventRelay_RenewsTheLeaseWhileTheBatchIsInFlight(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-renewed"))
	// A short lease so the renewal interval (lease/3, floored at 250ms) elapses inside the
	// test rather than in thirty seconds.
	harness.processor.WithLockDuration(900 * time.Millisecond)

	// The publish blocks until released, which is what keeps the batch in flight long enough
	// for the heartbeat to tick — the same shape as a slow broker.
	release := make(chan struct{})
	harness.publisher.beforePublish = func(PublishRequest) { <-release }

	done := make(chan int, 1)
	go func() { done <- harness.processor.processBatch(context.Background()) }()

	// Two renewals, so the heartbeat is a repeating ticker rather than a single extension.
	require.Eventually(t, func() bool {
		return len(harness.store.snapshotRenewals()) >= 2
	}, 5*time.Second, 20*time.Millisecond,
		"the lease must be renewed repeatedly while the batch is in flight; a single extension "+
			"would still expire under a batch that outlives two intervals")

	close(release)

	select {
	case claimed := <-done:
		require.Equal(t, 1, claimed)
	case <-time.After(10 * time.Second):
		t.Fatal("the batch did not finish")
	}

	renewals := harness.store.snapshotRenewals()
	require.NotEmpty(t, renewals)
	for i, renewal := range renewals {
		assert.Equal(t, "token-1", renewal.claimToken,
			"renewal %d must extend THIS batch's claim; renewing under another token would extend "+
				"somebody else's lease and let this batch's rows expire", i)
		assert.Equal(t, 900*time.Millisecond, renewal.lease,
			"renewal %d must extend by the configured lease, so the extension and the original "+
				"claim agree about how long a hold lasts", i)
	}

	// The heartbeat must not outlive the batch. Waiting a few intervals and comparing counts
	// is what proves the goroutine exited rather than merely that it stopped being observed.
	settled := len(harness.store.snapshotRenewals())
	time.Sleep(time.Second)
	assert.Equal(t, settled, len(harness.store.snapshotRenewals()),
		"renewals must stop when the batch finishes; a heartbeat that outlived its batch would hold "+
			"a lease on rows it no longer owns and delay their recovery after a crash")
}

// TestEventRelay_LeaseRenewalRetiresWhenNothingIsLeftInFlight asserts the heartbeat
// stops itself when the database reports nothing under the token.
//
// Every terminal transition clears claim_token, so a zero count means every row is
// finished.
func TestEventRelay_LeaseRenewalRetiresWhenNothingIsLeftInFlight(t *testing.T) {
	harness := newRelayHarness(t, relayTransactionRow(1, "evt-renewal-retires"))
	harness.processor.WithLockDuration(750 * time.Millisecond)

	release := make(chan struct{})
	harness.publisher.beforePublish = func(PublishRequest) { <-release }

	done := make(chan int, 1)
	go func() { done <- harness.processor.processBatch(context.Background()) }()

	require.Eventually(t, func() bool {
		return len(harness.store.snapshotRenewals()) >= 1
	}, 5*time.Second, 20*time.Millisecond, "the heartbeat must renew at least once")

	// The row is retired out from under the heartbeat, so the next renewal finds nothing.
	// This is exactly what a finished batch looks like from the database's point of view.
	harness.store.retireInflight()

	require.Eventually(t, func() bool {
		before := len(harness.store.snapshotRenewals())
		time.Sleep(600 * time.Millisecond)

		return len(harness.store.snapshotRenewals()) == before
	}, 5*time.Second, 100*time.Millisecond,
		"a renewal that extends nothing means the batch is done, so the heartbeat must retire itself")

	close(release)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the batch did not finish")
	}
}

// TestEventRelay_ALeaseRenewalFailureDoesNotAbortTheBatch asserts a failed renewal is
// reported and stepped past.
//
// The batch has publishes in flight.
func TestEventRelay_ALeaseRenewalFailureDoesNotAbortTheBatch(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newRelayHarness(t, relayTransactionRow(1, "evt-renewal-failed"))
	harness.processor.WithLockDuration(600 * time.Millisecond)
	harness.store.renewErr = errors.New("relay test: database unavailable")

	release := make(chan struct{})
	harness.publisher.beforePublish = func(PublishRequest) { <-release }

	done := make(chan int, 1)
	go func() { done <- harness.processor.processBatch(context.Background()) }()

	require.Eventually(t, func() bool {
		return len(relayEntriesWithMessage(hook, "renewing the lease on a batch in flight failed")) >= 1
	}, 5*time.Second, 20*time.Millisecond, "a failed renewal must be reported")

	entries := relayEntriesWithMessage(hook, "renewing the lease on a batch in flight failed")
	assert.Equal(t, logrus.WarnLevel, entries[0].Level,
		"a failed renewal is a warning: the duplicate it risks is recoverable at the subscriber, "+
			"whereas abandoning the batch would strand publishes that are in flight")

	close(release)

	select {
	case claimed := <-done:
		assert.Equal(t, 1, claimed, "the batch must complete despite the renewal failure")
	case <-time.After(10 * time.Second):
		t.Fatal("the batch did not finish")
	}

	assert.Len(t, harness.publisher.snapshotRequests(), 1, "the publish must still have happened")
	assert.Len(t, harness.store.snapshotDispatched(), 1, "and the row must still be dispatched")
}

// ---------------------------------------------------------------------------
// The dead-letter hand-off lease
// ---------------------------------------------------------------------------

// TestEventRelay_ATerminalFailureKeepsTheRowLeasedThroughItsDeadLetterHandOff is the
// cross-instance race, modelled at the level the relay's store contract can express it.
//
// Both terminal transitions RETAIN the claim token, and the reason is exclusivity: the
// dead-letter write and the MarkEventDeadLettered that records it are still owed, and
// only the worker that took the decision may perform them.
//
// The repair claim admits a failed row with no dlt_topic and NO LIVE LEASE, and it
// stamps a FRESH token over whatever was there.
//
// The row the STORE holds after the terminal transition — not the copy the relay
// carries in memory, which is not what another instance's predicate evaluates — must
// carry the retained token AND a lease that is still live.
//
// The behavioural proof against real PostgreSQL, including the fencing of the
// superseded token and the recovery once the lease lapses, is
// TestClaimFailedEventOutboxForDeadLetter_RecoversAnUnpreservedRowNothingElseCanReach_RealDB.
func TestEventRelay_ATerminalFailureKeepsTheRowLeasedThroughItsDeadLetterHandOff(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		publisher func(*relayFakePublisher)
		// terminal says which transition the relay is expected to have used, so the test
		// cannot pass by exercising the same arm twice under two names.
		permanent bool
	}{
		{
			name: "budget exhausted by transient failures",
			publisher: func(p *relayFakePublisher) {
				p.err = errRelayTransient
			},
		},
		{
			name: "a permanent failure on the first attempt",
			publisher: func(p *relayFakePublisher) {
				p.err = errRelayTransient
				p.failingPermanently()
			},
			permanent: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			row := relayTransactionRow(1, "evt-handoff")
			// A one-attempt budget so a single failure reaches the terminal arm without
			// walking the whole backoff schedule.
			row.MaxAttempts = 1

			harness := newRelayHarness(t, row)
			testCase.publisher(harness.publisher)

			harness.processor.processTick(context.Background())

			// The hand-off happened: the dead-letter writer was given the row.
			handedOff := harness.deadLetters.snapshotRows()
			require.Len(t, handedOff, 1, "a terminal failure must hand the row to the dead-letter writer")

			if testCase.permanent {
				require.Len(t, harness.store.snapshotTerminalFailures(), 1,
					"a permanent failure must go through the permanent transition")
			} else {
				require.Len(t, harness.store.snapshotFailures(), 1,
					"an exhausted budget must go through the ordinary failure transition")
			}

			stored, held := harness.store.inflightRow(row.ID)
			require.True(t, held,
				"the row must still be held under its claim: the dead-letter write it owes has not been recorded")
			assert.Equal(t, model.EventOutboxStatusFailed, stored.Status)
			assert.Equal(t, handedOff[0].ClaimToken, stored.ClaimToken,
				"the token the dead-letter writer was handed must be the token the row still holds, "+
					"or the transition that records the write cannot be authorised")

			require.NotNil(t, stored.LockedUntil,
				"the terminal transition must LEAVE A LEASE on the row: the repair claim takes a "+
					"failed row with no dead-letter record and no live lease, so clearing it here "+
					"lets a second instance publish the same dead-letter message")
			assert.True(t, stored.LockedUntil.After(relayFixedNow),
				"and that lease must still be live at the instant of the hand-off, or it excludes nothing")

			// The predicate that the lease exists to satisfy, exercised directly: with the row
			// seeded into the repair backlog, a concurrent repair pass must decline it.
			harness.store.mu.Lock()
			harness.store.failedRows = []model.EventOutbox{stored}
			harness.store.failedTaken = false
			harness.store.mu.Unlock()

			concurrent, err := harness.store.ClaimFailedEventOutboxForDeadLetter(
				context.Background(), 20, time.Minute)
			require.NoError(t, err)
			assert.Empty(t, concurrent,
				"a row under a live hand-off lease must not be claimable by another instance's repair pass")
		})
	}
}

// ---------------------------------------------------------------------------
// Dead-letter preservation recovery
// ---------------------------------------------------------------------------

// TestEventRelay_RetriesADeadLetterWriteThatFailed is the fix for the only state in
// this machine from which nothing moved forward.
//
// When a row exhausts its retry budget it becomes 'failed', and the worker that spent
// the last attempt writes the event to its `<topic>.dlt` sibling.
func TestEventRelay_RetriesADeadLetterWriteThatFailed(t *testing.T) {
	unpreserved := relayTransactionRow(1, "evt-unpreserved")
	unpreserved.Status = model.EventOutboxStatusFailed
	unpreserved.Attempts = unpreserved.MaxAttempts
	unpreserved.LastError = "broker unavailable"

	// Seeded as awaiting preservation, NOT as claimable: the publish claim must not be able
	// to reach it, which is the whole reason a second claim path exists.
	harness := newRelayHarness(t)
	harness.store.failedRows = []model.EventOutbox{unpreserved}

	harness.processor.processTick(context.Background())

	rows := harness.deadLetters.snapshotRows()
	require.Len(t, rows, 1, "the repair pass must hand the unpreserved row back to the dead-letter writer")
	assert.Equal(t, "evt-unpreserved", rows[0].EventID)
	assert.NotEmpty(t, rows[0].ClaimToken,
		"the row must carry a claim token, or MarkEventDeadLettered cannot be authorised")
	assert.NotEqual(t, "token-1", rows[0].ClaimToken,
		"and it must be a FRESH token: the original belonged to a worker that may no longer exist")

	causes := harness.deadLetters.snapshotCauses()
	require.Len(t, causes, 1)
	require.Error(t, causes[0])
	assert.Contains(t, causes[0].Error(), "broker unavailable",
		"the metadata must report the failure that ended the retry budget, not the fact of the repair")

	assert.Empty(t, harness.publisher.snapshotRequests(),
		"a repair must not republish to the ORIGINAL topic: the event's retry budget is spent, and "+
			"the only thing owed is its preservation")
}

// TestEventRelay_ADeadLetterRepairThatFailsAgainStaysRecoverable asserts the repair is
// idempotent under continued failure.
func TestEventRelay_ADeadLetterRepairThatFailsAgainStaysRecoverable(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	unpreserved := relayTransactionRow(1, "evt-still-unpreserved")
	unpreserved.Status = model.EventOutboxStatusFailed
	unpreserved.Attempts = unpreserved.MaxAttempts
	unpreserved.LastError = "broker unavailable"

	harness := newRelayHarness(t)
	harness.store.failedRows = []model.EventOutbox{unpreserved}
	harness.deadLetters.err = errors.New("relay test: broker still unavailable")

	harness.processor.processTick(context.Background())

	require.Len(t, harness.deadLetters.snapshotRows(), 1, "the repair must have been attempted")

	state, terminal := harness.store.terminalState(1)
	assert.False(t, terminal,
		"a repair that failed must not move the row to a terminal state; the event still exists only here")
	assert.Empty(t, state)

	entries := relayEntriesWithMessage(hook, "retrying the dead-letter preservation of this event failed again")
	require.NotEmpty(t, entries, "a repeated failure must stay visible")
	assert.Equal(t, logrus.WarnLevel, entries[0].Level)

	assert.Empty(t, harness.store.snapshotFailures(),
		"a preservation retry must not record a publish attempt: the publish budget is already spent")
}

// TestEventRelay_TheRepairPassIsQuietAndCheapWhenThereIsNothingToRepair asserts the
// ordinary case, which is the one that runs five hundred times a second's worth of
// ticks.
func TestEventRelay_TheRepairPassIsQuietAndCheapWhenThereIsNothingToRepair(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newRelayHarness(t, relayTransactionRow(1, "evt-ordinary"))

	harness.processor.processTick(context.Background())

	assert.Empty(t, harness.deadLetters.snapshotRows(),
		"with nothing awaiting preservation the dead-letter writer must not be called")
	assert.Empty(t, relayEntriesWithMessage(hook, "retrying the dead-letter preservation"),
		"and the pass must say nothing at all")

	assert.Len(t, harness.publisher.snapshotRequests(), 1,
		"the ordinary publish path must be unaffected by the repair pass running first")
	assert.Len(t, harness.store.snapshotDispatched(), 1)
}

// TestEventRelay_ARepairClaimFailureIsReportedAndTheTickContinues asserts a failure to
// even LOOK for repair work does not cost the tick its publishing.
//
// The two paths share a database and a tick.
func TestEventRelay_ARepairClaimFailureIsReportedAndTheTickContinues(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newRelayHarness(t, relayTransactionRow(1, "evt-repair-claim-failed"))
	harness.store.failedClaimErr = errors.New("relay test: statement timeout")

	harness.processor.processTick(context.Background())

	entries := relayEntriesWithMessage(hook, "could not claim events awaiting dead-letter preservation")
	require.NotEmpty(t, entries, "the failure must be reported")
	assert.Equal(t, logrus.ErrorLevel, entries[0].Level,
		"being unable to look for events that exist nowhere else is an error, not a warning")

	assert.Len(t, harness.publisher.snapshotRequests(), 1,
		"and the tick must still publish the rows that are due")
	assert.Len(t, harness.store.snapshotDispatched(), 1)
}

// ---------------------------------------------------------------------------
// The publisher's permanent-failure verdict reaches the durable state
// ---------------------------------------------------------------------------

// TestEventRelay_APermanentPublishFailureExhaustsTheRowImmediately is the relay half of
// the verdict hand-off.
//
// The publisher already knows which failures no retry can fix: an envelope over the
// size ceiling, bytes that are not valid JSON, a destination outside the topic
// namespace Blnk owns.
//
// The assertions are on both halves of the hand-off: the verdict took the PERMANENT
// transition rather than the budgeted one, and the row went straight to the dead-letter
// writer on attempt one.
func TestEventRelay_APermanentPublishFailureExhaustsTheRowImmediately(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	row := relayTransactionRow(1, "evt-permanent")
	require.Equal(t, 5, row.MaxAttempts, "the fixture must have budget left, or nothing is being proven")

	harness := newRelayHarness(t, row)
	harness.publisher.failPermanentlyFor(row.EventID, errors.New("the serialised event is over the size ceiling"))

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	terminalFailures := harness.store.snapshotTerminalFailures()
	require.Len(t, terminalFailures, 1,
		"the publisher's PERMANENT verdict must be forwarded to the durable transition, and the "+
			"permanent transition is the one that records it; discarding the verdict returns a row "+
			"that can never be published to pending and spends its whole schedule rediscovering that")

	assert.Empty(t, harness.store.snapshotFailures(),
		"and the BUDGETED transition must not be used. Routing an affirmative permanent verdict "+
			"through MarkEventFailed with terminal=true reaches the same row state, so only this "+
			"assertion distinguishes the two — and the budgeted statement is the one whose "+
			"short-circuit an unclassified result must never be able to trigger")

	assert.Equal(t, model.EventOutboxStatusFailed, harness.store.rowByID(t, row.ID).Status,
		"the row must be out of the claimable set on attempt one")

	rows := harness.deadLetters.snapshotRows()
	require.Len(t, rows, 1,
		"a permanent failure must reach the dead-letter writer on the attempt it happened, so the event "+
			"is preserved and visible now rather than after the full backoff schedule")
	assert.Equal(t, row.EventID, rows[0].EventID)
	assert.Equal(t, 1, rows[0].Attempts,
		"and the attempt count must report the truth — one attempt — rather than being inflated to the budget")

	assert.Len(t, harness.publisher.snapshotRequests(), 1,
		"the remaining budget must NOT be spent: no retry can make an unpublishable message publishable")

	// The message the PERMANENT transition emits, not the budgeted arm's. They are
	// different lines because they are different decisions, and matching the wrong one is
	// how this test went on passing while the verdict was being routed through the
	// budgeted statement.
	entries := relayEntriesWithMessage(hook, "publish failed permanently")
	require.NotEmpty(t, entries,
		"the reason for an early exhaustion must be stated, or 'attempt 1 of 5, exhausted' reads as a defect")
	assert.Equal(t, logrus.WarnLevel, entries[0].Level)
	assert.Contains(t, entries[0].Message, "the remaining retry budget is not spent",
		"and the line must say the budget was deliberately left unspent rather than exhausted")
}

// TestEventRelay_ATransientPublishFailureKeepsItsWholeBudget is the other side of the
// same decision, and it is what stops the fix above from becoming a regression.
//
// A broker being briefly unreachable is the ordinary case, and it MUST still be bounded
// by max_attempts rather than exhausted on sight.
func TestEventRelay_ATransientPublishFailureKeepsItsWholeBudget(t *testing.T) {
	row := relayTransactionRow(1, "evt-transient")

	harness := newRelayHarness(t, row)
	harness.publisher.failFor = map[string]bool{row.EventID: true}

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	failures := harness.store.snapshotFailures()
	require.Len(t, failures, 1)
	assert.False(t, failures[0].terminal,
		"a transient failure must NOT be forwarded as permanent, or a momentary broker blip dead-letters "+
			"every event in flight")

	assert.Empty(t, harness.deadLetters.snapshotRows(),
		"and the row must keep its budget rather than being preserved on attempt one")

	state, terminal := harness.store.terminalState(row.ID)
	assert.False(t, terminal, "the row must be back in the claimable set, not terminal")
	assert.Empty(t, state)
}

// ---------------------------------------------------------------------------
// The legacy webhook leg survives a Kafka leg that ended in failure
// ---------------------------------------------------------------------------

// TestEventRelay_BothLegsFailingOnTheExhaustingAttemptDoesNotDiscardTheWebhook is the
// acceptance test for the dual-delivery guarantee's worst case.
//
// The relay enqueues the legacy webhook FIRST and publishes to Kafka SECOND. A failed
// enqueue is deliberately swallowed, because a webhook receiver being down must not
// consume a Kafka retry attempt or dead-letter an event on the new transport.
//
//   - publish SUCCEEDED → MarkEventWebhookPending, status webhook_pending, inside the
//     claim predicate.
//   - publish FAILED with budget left → back to pending, whole row retried.
//   - publish FAILED on the attempt that spent the budget → failed, then dead_lettered.
func TestEventRelay_BothLegsFailingOnTheExhaustingAttemptDoesNotDiscardTheWebhook(t *testing.T) {
	// A row on its LAST attempt, so the coming publish failure exhausts it.
	row := relayTransactionRow(1, "evt-both-legs-failed")
	row.Attempts = row.MaxAttempts - 1

	harness := newRelayHarness(t, row)
	harness.publisher.failFor = map[string]bool{row.EventID: true}
	harness.legacy.err = errors.New("relay test: the webhook queue is unreachable")

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	// Both legs did fail, which is the precondition this test is about.
	assert.Empty(t, harness.legacy.snapshot(), "the enqueue must have failed")
	require.Len(t, harness.deadLetters.snapshotRows(), 1, "the Kafka leg must have been given up on")

	state, terminal := harness.store.terminalState(row.ID)
	require.True(t, terminal, "the Kafka leg must be terminal, which is what puts the row out of the ordinary claim")
	require.Equal(t, model.EventOutboxStatusFailed, state)

	// The recovery pass now finds the row THERE. Seeded as the widened claim would return it:
	// terminal on the failure path, with the webhook still owed.
	owed := row
	owed.Status = model.EventOutboxStatusDeadLettered
	owed.Attempts = row.MaxAttempts
	owed.WebhookDispatched = false
	owed.LastError = "broker unavailable"

	recovery := newRelayHarness(t)
	recovery.store.webhookOwedRows = []model.EventOutbox{owed}

	recovery.processor.processTick(context.Background())

	enqueued := recovery.legacy.snapshot()
	require.Len(t, enqueued, 1,
		"the outstanding webhook must be enqueued by the recovery pass; without it the delivery promised "+
			"for the migration window is lost with one warning line as the only trace")
	assert.Equal(t, owed.EventID, enqueued[0].eventID,
		"the event id is the asynq task identity, which is what makes a re-claim idempotent")
	assert.Equal(t, []byte(owed.Payload), enqueued[0].body,
		"and the STORED bytes must be delivered, so payload identity holds on the recovery path too")

	marks := recovery.store.snapshotWebhookMarks()
	require.Len(t, marks, 1, "the recovered leg must be recorded, or the next pass enqueues it again")
	assert.Equal(t, owed.ID, marks[0].id)
	assert.NotEmpty(t, marks[0].claimToken,
		"the recovery claim must stamp a fresh token: every terminal transition cleared the old one")

	assert.Empty(t, recovery.publisher.snapshotRequests(),
		"the recovery pass must NEVER publish: this event is dead-lettered, and republishing it would "+
			"revive a terminal row and duplicate its dead-letter accounting")
	assert.Empty(t, recovery.store.snapshotFailures(),
		"and it must not record a Kafka attempt either; the Kafka budget is spent and its state is settled")
	assert.Empty(t, recovery.store.snapshotLegacyAttempts(),
		"a successful enqueue records no failure transition")
}

// TestEventRelay_ARecoveredWebhookThatFailsAgainSpendsOnlyItsOwnBudget asserts the
// recovery path is bounded, and bounded by the LEGACY leg's own counter.
func TestEventRelay_ARecoveredWebhookThatFailsAgainSpendsOnlyItsOwnBudget(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	owed := relayTransactionRow(1, "evt-recovery-failed-again")
	owed.Status = model.EventOutboxStatusDeadLettered
	owed.Attempts = owed.MaxAttempts
	owed.WebhookAttempts = 1

	harness := newRelayHarness(t)
	harness.store.webhookOwedRows = []model.EventOutbox{owed}
	harness.legacy.err = errors.New("relay test: the webhook queue is still unreachable")

	harness.processor.processTick(context.Background())

	attempts := harness.store.snapshotLegacyAttempts()
	require.Len(t, attempts, 1, "the failed recovery must be recorded, or it is unbounded")
	assert.Equal(t, owed.ID, attempts[0].id)
	assert.Equal(t, 2*time.Second, attempts[0].retryAfter,
		"the backoff must be indexed by the WEBHOOK attempt count — one recorded already, so this is "+
			"attempt two and its delay is 2s on the documented 1s/2s/4s/8s/16s curve")

	assert.Empty(t, harness.store.snapshotFailures(),
		"a webhook failure must NEVER record a Kafka attempt: that would let the deprecated transport "+
			"spend the new one's retry budget")
	assert.Empty(t, harness.store.snapshotWebhookMarks(),
		"and nothing may be marked delivered")

	state, terminal := harness.store.terminalState(owed.ID)
	assert.False(t, terminal,
		"the fake records no NEW terminal transition: the row's status was already dead_lettered and the "+
			"recovery path must leave it exactly so")
	assert.Empty(t, state)

	entries := relayEntriesWithMessage(hook, "recovering the legacy webhook leg of this event failed again")
	require.NotEmpty(t, entries, "a repeated failure must stay visible")
	assert.Equal(t, logrus.WarnLevel, entries[0].Level)
}

// TestEventRelay_TheWebhookRecoveryPassIsSilentAfterTheSunset asserts the pass observes
// the same boundary the inline enqueue does.
//
// From the sunset onwards there is no leg to finish: the promise has ended and Kafka is
// the only transport.
func TestEventRelay_TheWebhookRecoveryPassIsSilentAfterTheSunset(t *testing.T) {
	owed := relayTransactionRow(1, "evt-after-sunset")
	owed.Status = model.EventOutboxStatusDeadLettered

	harness := newRelayHarness(t)
	harness.store.webhookOwedRows = []model.EventOutbox{owed}
	harness.processor.dualDeliveryActive = func(time.Time) bool { return false }

	harness.processor.processTick(context.Background())

	assert.Empty(t, harness.legacy.snapshot(),
		"no webhook may be enqueued once the window has closed")
	assert.False(t, harness.store.webhookOwedTaken,
		"and the claim must not even be issued: skipping the query is cheaper than claiming rows and "+
			"discarding them, and it keeps the boundary in ONE predicate")
}

// TestEventRelay_AWebhookRecoveryClaimFailureIsReportedAndTheTickContinues asserts the
// pass cannot cost the tick its publishing.
//
// The passes share a database and a tick.
func TestEventRelay_AWebhookRecoveryClaimFailureIsReportedAndTheTickContinues(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newRelayHarness(t, relayTransactionRow(1, "evt-webhook-claim-failed"))
	harness.store.webhookOwedErr = errors.New("relay test: statement timeout")

	harness.processor.processTick(context.Background())

	entries := relayEntriesWithMessage(hook, "could not claim events whose legacy webhook leg is still owed")
	require.NotEmpty(t, entries, "the failure must be reported")
	assert.Equal(t, logrus.ErrorLevel, entries[0].Level)

	assert.Len(t, harness.publisher.snapshotRequests(), 1,
		"and the tick must still publish the rows that are due")
	assert.Len(t, harness.store.snapshotDispatched(), 1)
}

// ---------------------------------------------------------------------------
// Broker coordinate persistence
// ---------------------------------------------------------------------------

// TestEventRelay_PersistsTheBrokerCoordinateOfTheRecordItProduced is the relay half of
// the broker-coordinate contract.
//
// The publisher captures the coordinate (event_publisher_test.go) and the repository
// stores it (database/event_outbox_test.go).
func TestEventRelay_PersistsTheBrokerCoordinateOfTheRecordItProduced(t *testing.T) {
	row := relayTransactionRow(1, "evt-coordinate")
	harness := newRelayHarness(t, row)
	harness.processor.dualDeliveryActive = func(time.Time) bool { return false }

	record := model.BrokerRecord{Topic: "blnk.transactions", Partition: 2, Offset: 44_101}
	harness.publisher.reporting(row.EventID, record)

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	dispatched := harness.store.snapshotDispatched()
	require.Len(t, dispatched, 1)
	assert.Equal(t, record, dispatched[0].record,
		"the coordinate the broker reported must reach the row, or the event cannot be matched to "+
			"its record and the zero-loss reconciliation cannot be conclusive")
}

// TestEventRelay_RecordsAnUnconfirmedPublishHonestly covers the absence.
//
// A broker that acknowledges a write while the client reports no coordinate has still
// published the event — so the publish must NOT fail.
func TestEventRelay_RecordsAnUnconfirmedPublishHonestly(t *testing.T) {
	row := relayTransactionRow(1, "evt-coordinate")
	harness := newRelayHarness(t, row)
	harness.processor.dualDeliveryActive = func(time.Time) bool { return false }
	// No coordinate configured, so the publish reports none.

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	dispatched := harness.store.snapshotDispatched()
	require.Len(t, dispatched, 1)
	_, confirmed := dispatched[0].record, dispatched[0].record.Confirmed()
	assert.False(t, confirmed,
		"an unreported coordinate must be recorded as absent, never as a fabricated location")

	assert.Empty(t, harness.store.snapshotFailures(),
		"an unreported coordinate is NOT a publish failure: the broker acknowledged the write")
}

// TestEventRelay_CarriesTheCoordinateThroughAnOutstandingWebhookLeg covers the
// dual-delivery path, where the row does not reach a terminal state.
//
// A webhook_pending row has been published to Kafka — its record is on the topic and
// the zero-loss audit counts it — so it must name that record too.
func TestEventRelay_CarriesTheCoordinateThroughAnOutstandingWebhookLeg(t *testing.T) {
	row := relayTransactionRow(1, "evt-coordinate")
	harness := newRelayHarness(t, row)

	record := model.BrokerRecord{Topic: "blnk.transactions", Partition: 5, Offset: 8}
	harness.publisher.reporting(row.EventID, record)
	harness.legacy.err = errors.New("redis unavailable")

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	pendings := harness.store.snapshotWebhookPendings()
	require.Len(t, pendings, 1,
		"a failed legacy enqueue must record the Kafka leg separately rather than marking the row done")
	assert.Equal(t, record, pendings[0].record,
		"the row waiting on its webhook must still name the record its Kafka leg produced")

	assert.Empty(t, harness.store.snapshotDispatched(),
		"the row is not terminal yet; the webhook is still owed")
}

// TestEventRelay_AWebhookOnlyPassCarriesNoCoordinate pins the case COALESCE exists for.
//
// A row whose Kafka leg is already recorded is re-claimed ONLY for its outstanding
// webhook: nothing is published, so there is no new record to name — and the row
// already carries the coordinate its earlier successful publish produced.
func TestEventRelay_AWebhookOnlyPassCarriesNoCoordinate(t *testing.T) {
	row := relayTransactionRow(1, "evt-coordinate")
	acknowledged := relayFixedNow.Add(-time.Minute)
	row.KafkaDispatchedAt = &acknowledged

	// The row already names its record, as a real re-claimed row would.
	partition := 5
	offset := int64(8)
	row.KafkaTopic = "blnk.transactions"
	row.KafkaPartition = &partition
	row.KafkaOffset = &offset

	harness := newRelayHarness(t, row)
	harness.processor.dualDeliveryActive = func(time.Time) bool { return true }
	// A coordinate IS available from the publisher, so a relay that published anyway would
	// record it — which is what makes the absence below meaningful rather than vacuous.
	harness.publisher.reporting(row.EventID,
		model.BrokerRecord{Topic: "blnk.transactions", Partition: 1, Offset: 999})

	require.Equal(t, 1, harness.processor.processBatch(context.Background()))

	assert.Empty(t, harness.publisher.snapshotRequests(),
		"a row whose Kafka leg is recorded must not be republished; that would duplicate a message "+
			"already on the topic")

	dispatched := harness.store.snapshotDispatched()
	require.Len(t, dispatched, 1)
	assert.False(t, dispatched[0].record.Confirmed(),
		"NO COORDINATE may be supplied on a pass that published nothing; the statement's COALESCE "+
			"then preserves the one the row already carries")
}

// TestProcessRow_HonoursAPermanentFailureInsteadOfSpendingTheBudget is the relay half
// of the rule that a publish verdict is acted on rather than computed, logged and then
// ignored.
//
// The assertions are therefore about WHICH transition ran, how many times, and what the
// log said.
func TestProcessRow_HonoursAPermanentFailureInsteadOfSpendingTheBudget(t *testing.T) {
	t.Run("a permanent failure is terminal on the first attempt", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		row := relayTransactionRow(1, "evt-permanent")
		harness := newRelayHarness(t, row)
		harness.publisher.failingPermanently().err = errors.New("relay test: topic authorization failed")

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		terminal := harness.store.snapshotTerminalFailures()
		require.Len(t, terminal, 1,
			"a permanent failure must be recorded through the permanent transition exactly once")
		assert.Equal(t, row.ID, terminal[0].id)
		assert.Contains(t, terminal[0].reason, "topic authorization failed",
			"the reason must reach last_error so an operator sees why the event stopped")

		assert.Empty(t, harness.store.snapshotFailures(),
			"and NOT through the budgeted transition: recording an ordinary attempt is what spent "+
				"five round trips and ~17 seconds proving the broker meant it")

		require.Len(t, harness.deadLetters.rows, 1,
			"the row must reach the dead-letter writer on this attempt, not after the schedule")

		// The claim token has to survive onto the row handed off, or the dead-letter write
		// and the transition that records it are not exclusive to this worker and two workers
		// can put two copies of one event on the topic.
		assert.NotEmpty(t, harness.deadLetters.rows[0].ClaimToken,
			"the terminal transition must retain the claim token for the hand-off")
		assert.Equal(t, 1, harness.deadLetters.rows[0].Attempts,
			"the failure metadata must report the ONE attempt that was really made, which is what "+
				"tells an operator the event never had a chance rather than that it fought for 17s")

		// Only one publish, which is the cost this fix removes.
		assert.Len(t, harness.publisher.snapshotRequests(), 1,
			"exactly one broker round trip for a condition no further round trip can change")
	})

	t.Run("the log no longer contradicts the decision it just took", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		harness := newRelayHarness(t, relayTransactionRow(2, "evt-permanent-log"))
		harness.publisher.failingPermanently().err = errors.New("relay test: not authorised")

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		assert.Empty(t, relayEntriesWithMessage(hook, "scheduled for another attempt"),
			"nothing may announce a retry for an attempt it has just reported as non-retryable")

		permanent := relayEntriesWithMessage(hook, "publish failed permanently")
		require.Len(t, permanent, 1, "the terminal decision must be stated once, in its own line")
		assert.Equal(t, logrus.WarnLevel, permanent[0].Level)
		assert.Equal(t, 1, permanent[0].Data["attempt"],
			"and it must name the attempt it happened on")

		dead := relayEntriesWithMessage(hook, "event dead-lettered")
		require.Len(t, dead, 1)
		assert.Equal(t, "permanent_failure", dead[0].Data["terminal_reason"],
			"the dead-letter line must say WHY the event is terminal: 'after exhausting its retry "+
				"budget' sends an operator looking for a broker outage that never happened")
		assert.Contains(t, dead[0].Message, "permanent publish failure")
	})

	t.Run("a transient failure still goes through the budgeted path", func(t *testing.T) {
		harness := newRelayHarness(t, relayTransactionRow(3, "evt-transient"))
		harness.publisher.err = errRelayTransient

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		assert.Len(t, harness.store.snapshotFailures(), 1,
			"a recoverable failure keeps its budget, and the exhaustion decision stays in SQL "+
				"where two racing instances cannot both conclude they were last")
		assert.Empty(t, harness.store.snapshotTerminalFailures(),
			"the terminal transition must not be reachable from a recoverable failure")
		assert.Empty(t, harness.deadLetters.rows, "and nothing is dead-lettered while budget remains")
	})

	t.Run("a publisher that classifies nothing is retried, not abandoned", func(t *testing.T) {
		// The publisher is an interface seam. A result with no status and no classification
		// must fall through to the budgeted path: reading "no verdict" as "give up" would
		// dead-letter every event a bare-error publisher failed on, on its first attempt.
		hook := logtest.NewGlobal()

		row := relayTransactionRow(4, "evt-unclassified")
		row.MaxAttempts = 5
		harness := newRelayHarness(t, row)
		harness.publisher.unclassified = true
		harness.publisher.err = errors.New("relay test: bare failure with no verdict")

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		failures := harness.store.snapshotFailures()
		require.Len(t, failures, 1,
			"an unclassified failure must be treated as retryable, which is the conservative "+
				"reading and the one that cannot lose a deliverable event")
		assert.False(t, failures[0].terminal,
			"THE VERDICT ITSELF: an unclassified failure must be forwarded as NOT terminal. A "+
				"true here exhausts the row inside MarkEventFailed's CASE on attempt one, "+
				"whatever the attempt count says, and no assertion about which transition was "+
				"called can detect that")
		assert.Equal(t, harness.processor.retry.backoffFor(1), failures[0].retryAfter,
			"and the first backoff must be scheduled, which is what a retry means")

		assert.Empty(t, harness.store.snapshotTerminalFailures(),
			"the permanent-failure transition belongs to an affirmative verdict only")
		assert.Empty(t, harness.deadLetters.rows,
			"and nothing may reach a dead-letter topic while four attempts are unspent")

		_, reachedTerminal := harness.store.terminalState(4)
		assert.False(t, reachedTerminal,
			"the row must not be left in a terminal state; an exhausted budget on attempt one "+
				"is the exact symptom the double negation produced")
		assert.Contains(t, harness.store.claimableIDs(harness.processor.now().Add(time.Hour)), int64(4),
			"and it must be back in the claimable set so a later tick retries it")

		exhaustion := relayEntriesWithMessage(hook, "publish failed permanently")
		assert.Empty(t, exhaustion,
			"and nothing may claim the failure was permanent, because nothing classified it")
	})

	t.Run("a lost claim abandons the row instead of dead-lettering it twice", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		harness := newRelayHarness(t, relayTransactionRow(5, "evt-permanent-claim-lost"))
		harness.publisher.failingPermanently().err = errors.New("relay test: not authorised")
		harness.store.terminalFailErr = errors.New("relay test: claim lost on row 5")

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))

		assert.Empty(t, harness.deadLetters.rows,
			"a worker that could not record the terminal state must not dead-letter: another "+
				"instance owns the row now and would write the same event a second time")

		entries := relayEntriesWithMessage(hook, "recording a permanently failed publish attempt failed")
		require.Len(t, entries, 1, "and the abandonment must be reported")
		assert.Equal(t, logrus.ErrorLevel, entries[0].Level)
	})
}

// ---------------------------------------------------------------------------
// The unique published-events count belongs to the DURABLE transition
// ---------------------------------------------------------------------------

// Its own doc records why the EventsDispatchedTotal suite is deliberately not folded in
// with them.

// defaultRelayDeadLetterHandoffLease mirrors the database package's
// defaultEventClaimLease, which is the lease a terminal transition falls back to when
// its caller supplies a non-positive one.
const defaultRelayDeadLetterHandoffLease = 30 * time.Second

// TestProcessRow_LogsEveryFailedAttemptWithTheFiveRequiredFields is the requirement log
// contract: a line for EVERY attempt including the first, not only for the final
// failure, and each carrying the attempt, the maximum attempts, the error, the event id
// and the topic.
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

	entries := relayEntriesWithMessage(hook, "publish failed")
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

// TestProcessRow_SuppliesItsLockDurationAsTheDeadLetterHandOffLease asserts the
// plumbing that makes the retained claim token on a terminal transition mean anything.
//
// Both terminal transitions retain the row's claim token so that only the worker that
// spent the last attempt may write to the dead-letter topic, and both hold the row's
// LEASE for the same span so that the dead-letter repair claim — whose predicate admits
// a failed row with no dead-letter record and a lease that is NULL or past — cannot
// adopt the row while that write is in flight.
//
// A caller that forgot to plumb its lock duration through would pass the zero value,
// and `NOW() + '0s'` is an instant that has already passed.
//
// Both transitions are covered because they are separate statements with separate call
// sites, and only one of them is on the retry path a generic failure test would reach.
func TestProcessRow_SuppliesItsLockDurationAsTheDeadLetterHandOffLease(t *testing.T) {
	const lease = 17 * time.Second

	t.Run("the exhaustion arm", func(t *testing.T) {
		harness := newRelayHarness(t, relayTransactionRow(1, "evt-handoff-lease"))
		harness.processor.WithLockDuration(lease)
		harness.publisher.err = errRelayTransient

		harness.processor.processBatch(context.Background())

		failures := harness.store.snapshotFailures()
		require.Len(t, failures, 1)
		assert.Equal(t, lease, failures[0].deadLetterLease,
			"the relay must hand its own lock duration to MarkEventFailed as the dead-letter hand-off "+
				"lease: it is the same ownership window the claim took, and a zero here would leave the "+
				"window to a repository fallback the operator never configured")
	})

	t.Run("the permanent-failure transition", func(t *testing.T) {
		harness := newRelayHarness(t, relayTransactionRow(1, "evt-handoff-lease-permanent"))
		harness.processor.WithLockDuration(lease)
		harness.publisher.failingPermanently().err = errors.New("relay test: not authorised")

		harness.processor.processBatch(context.Background())

		terminal := harness.store.snapshotTerminalFailures()
		require.Len(t, terminal, 1,
			"a classified permanent failure must take the permanent-failure transition, not the retry arm")
		assert.Equal(t, lease, terminal[0].deadLetterLease,
			"and it must carry the same lock duration: this transition retains the claim token on the "+
				"FIRST attempt, so the window it holds the row for is the only thing stopping the repair "+
				"pass from writing a second copy to the dead-letter topic")
	})
}

// relayPublishedCountCase is one single-pass arrangement and the number of times
// blnk.events.published.total must move for it.
type relayPublishedCountCase struct {
	// row overrides the default transaction row when an arm needs a pre-set field.
	row func() model.EventOutbox
	// arrange injects the failures and flags that select the arm under test.
	arrange func(*relayHarness)
	// premise asserts the arm really was reached, so a zero count is evidence of
	// the contract rather than of an arrangement that never fired.
	premise func(*testing.T, *relayHarness)
	// expected is the required number of increments.
	expected int
	// because states what the number means and what reading it wrongly would cost.
	because string
}

// TestEventRelay_CountsOneDurableKafkaDeliveryPerEvent is the guard on what
// blnk.events.published.total means, and it is the ONLY guard on it.
//
// Those two facts are separated by a database round trip that can fail, and delivery is
// at-least-once — the row keeps its lease, the lease expires, another pass publishes
// the same event. Incremented on the acknowledgement alone, the counter therefore moved
// twice for one event, over-reporting deliveries exactly when the pipeline was in
// trouble.
func TestEventRelay_CountsOneDurableKafkaDeliveryPerEvent(t *testing.T) {
	dualDeliveryOff := func(harness *relayHarness) {
		harness.processor.dualDeliveryActive = func(time.Time) bool { return false }
	}

	for name, testCase := range map[string]relayPublishedCountCase{
		"a row marked dispatched counts exactly once": {
			premise: func(t *testing.T, harness *relayHarness) {
				require.Len(t, harness.store.snapshotDispatched(), 1,
					"the durable transition must have run")
			},
			expected: 1,
			because:  "one delivered event, one increment",
		},

		"a row marked dispatched with dual delivery off still counts exactly once": {
			arrange: dualDeliveryOff,
			premise: func(t *testing.T, harness *relayHarness) {
				require.Len(t, harness.store.snapshotDispatched(), 1,
					"the row must be marked dispatched, which is the transition the count hangs off")
			},
			expected: 1,
			because: "the count follows the KAFKA leg becoming durable and must not vary with " +
				"whether a legacy transport is configured, or the figure would step when the " +
				"sunset passes and nothing about delivery had changed",
		},

		"an acknowledged publish whose durable transition fails is NOT counted": {
			arrange: func(harness *relayHarness) {
				harness.store.dispatchErr = errors.New("relay test: marking dispatched failed")
			},
			premise: func(t *testing.T, harness *relayHarness) {
				require.Len(t, harness.publisher.snapshotRequests(), 1,
					"the broker DID acknowledge the write")
				require.Len(t, harness.store.snapshotDispatched(), 1,
					"and the transition was ATTEMPTED — the fake records the call before failing")
				_, terminal := harness.store.terminalState(1)
				require.False(t, terminal,
					"but it did not take effect, so the row is not terminal and stays claimable")
			},
			expected: 0,
			because: "nothing durable records this delivery, so the row returns to the claimable " +
				"set and is published again — counting here and again then is exactly the double " +
				"count that moving off the acknowledgement removes. The gap between " +
				"EventBrokerAcknowledgementsTotal and this counter is the diagnostic for the state",
		},

		"a failed publish is not counted": {
			arrange: func(harness *relayHarness) { harness.publisher.err = errRelayTransient },
			premise: func(t *testing.T, harness *relayHarness) {
				require.Len(t, harness.store.snapshotFailures(), 1, "the attempt must have failed")
			},
			expected: 0,
			because: "an event that was never delivered must not appear in the delivery count, or " +
				"the dead-letter rate's denominator would include its own numerator's population",
		},

		"a webhook-pending settlement counts the Kafka leg once": {
			arrange: func(harness *relayHarness) {
				harness.processor.dualDeliveryActive = func(time.Time) bool { return true }
				harness.legacy.err = errors.New("relay test: the webhook queue is unreachable")
			},
			premise: func(t *testing.T, harness *relayHarness) {
				require.Len(t, harness.store.snapshotWebhookPendings(), 1,
					"the Kafka leg must be recorded by MarkEventWebhookPending on this arm")
				require.Empty(t, harness.store.snapshotDispatched(),
					"and the row must not be terminal yet")
			},
			expected: 1,
			because: "MarkEventWebhookPending stamps kafka_dispatched_at and consumes the claim " +
				"token exactly as MarkEventDispatched does, so skipping this arm would undercount " +
				"every event delivered during a legacy-queue outage",
		},

		"a webhook-only re-claim publishes nothing and counts nothing": {
			row: func() model.EventOutbox {
				row := relayTransactionRow(1, "evt-already-published")
				acknowledged := relayFixedNow.Add(-time.Minute)
				row.KafkaDispatchedAt = &acknowledged

				return row
			},
			arrange: func(harness *relayHarness) {
				harness.processor.dualDeliveryActive = func(time.Time) bool { return true }
			},
			premise: func(t *testing.T, harness *relayHarness) {
				require.Empty(t, harness.publisher.publishedIDs(),
					"a row whose Kafka leg is already recorded must not be republished")
			},
			expected: 0,
			because: "this event was counted when its Kafka leg settled on an earlier claim; " +
				"counting the webhook-only pass too would count one event twice",
		},

		"a dead-lettered row is not counted as a delivery": {
			row: func() model.EventOutbox {
				row := relayTransactionRow(1, "evt-dead-lettered")
				row.Attempts = row.MaxAttempts - 1

				return row
			},
			arrange: func(harness *relayHarness) { harness.publisher.err = errRelayTransient },
			premise: func(t *testing.T, harness *relayHarness) {
				state, terminal := harness.store.terminalState(1)
				require.True(t, terminal, "the last attempt must exhaust the row")
				require.Equal(t, model.EventOutboxStatusFailed, state,
					"failed is where the exhausting transition leaves it: the `.dlt` write is what "+
						"moves it on to dead_lettered, settled under the retained lease")
				require.Len(t, harness.deadLetters.snapshotRows(), 1,
					"and the row must have been handed to the dead-letter writer")
			},
			expected: 0,
			because: "a dead-lettered event was never delivered to its category topic, and it is " +
				"the NUMERATOR of the rate this counter is the denominator of; counting it on both " +
				"sides would understate the rate at exactly the moment it matters",
		},
	} {
		t.Run(name, func(t *testing.T) {
			instruments := publisherCaptureInstruments(t)

			row := relayTransactionRow(1, "evt-published-count")
			if testCase.row != nil {
				row = testCase.row()
			}

			harness := newRelayHarness(t, row)
			if testCase.arrange != nil {
				testCase.arrange(harness)
			}

			require.Equal(t, 1, harness.processor.processBatch(context.Background()),
				"the row must be claimed and processed, or this arm asserts nothing")

			testCase.premise(t, harness)

			assert.EqualValues(t, testCase.expected, instruments.published.total(), testCase.because)
		})
	}

	// THE ATTRIBUTE SET, asserted once rather than in every counting arm: it is a property
	// of the increment and not of the end state that produced it. The pair must match
	// EventsDeadLetteredTotal's and EventsDispatchedTotal's exactly, or the
	// dead-letter-rate query divides series that do not correspond.
	t.Run("the increment is attributed by topic and event type and by nothing else", func(t *testing.T) {
		instruments := publisherCaptureInstruments(t)
		harness := newRelayHarness(t, relayTransactionRow(1, "evt-published-attributes"))

		require.Equal(t, 1, harness.processor.processBatch(context.Background()))
		require.Len(t, harness.store.snapshotDispatched(), 1)

		published := instruments.published.snapshot()
		require.Len(t, published, 1, "one delivered event, one increment")
		assert.EqualValues(t, 1, published[0].value, "an event is one event, never a batch")
		assert.Equal(t, map[string]string{
			"topic":      "blnk.transactions",
			"event_type": "transaction.applied",
		}, published[0].attributes,
			"an extra attribute here would split this series against an unsplit denominator and "+
				"make the dead-letter rate unjoinable")
	})

	// THE MULTI-PASS ARM, which no single-pass table row can express: the same event
	// crossing two claims and two durable transitions must still be counted once. Two of
	// the retired suites each carried a version of this; the assertions of both are kept.
	t.Run("a row that finishes its two legs on separate claims counts once in total", func(t *testing.T) {
		instruments := publisherCaptureInstruments(t)
		harness := newRelayHarness(t, relayTransactionRow(1, "evt-two-leg-counted-once"))
		harness.legacy.err = errors.New("relay test: redis unavailable")

		// PASS ONE. Kafka succeeds, the legacy enqueue does not, so the Kafka leg is recorded
		// by MarkEventWebhookPending — the broker coordinate is persisted with it — and the
		// row stays claimable for the webhook alone.
		require.Equal(t, 1, harness.processor.processBatch(context.Background()))
		require.Len(t, harness.store.snapshotWebhookPendings(), 1,
			"the outstanding legacy leg must be recorded")
		require.Empty(t, harness.store.snapshotDispatched(), "and the row must not be terminal yet")
		require.EqualValues(t, 1, instruments.published.total(),
			"an event whose webhook is still owed HAS been delivered to Kafka: waiting for the "+
				"webhook would under-report every event during a queue outage and never count one "+
				"whose webhook is ultimately abandoned")

		// PASS TWO. A real re-claim, for the webhook alone. This pass publishes NOTHING —
		// kafka_dispatched_at is already set — and drives the row to dispatched.
		afterBackoff := relayFixedNow.Add(72 * time.Hour)
		harness.legacy.err = nil
		harness.store.now = func() time.Time { return afterBackoff }
		harness.processor.now = func() time.Time { return afterBackoff }

		require.Equal(t, 1, harness.processor.processBatch(context.Background()),
			"the row must be claimed a second time, for its webhook alone")
		require.Len(t, harness.publisher.snapshotRequests(), 1,
			"the second pass must not republish; the message is already on the topic")
		require.Len(t, harness.store.snapshotDispatched(), 1, "and NOW the row is terminal")

		assert.EqualValues(t, 1, instruments.published.total(),
			"ONE event, ONE increment, across two claims and two durable transitions. The "+
				"dispatched transition on the second pass records no new delivery, so counting "+
				"there as well would count one event twice — and only for events whose webhook leg "+
				"was slow, which is a bias invisible in the number itself")
	})
}

// ---------------------------------------------------------------------------
// Repair capacity
// ---------------------------------------------------------------------------

// relayRepairWave builds count rows in one repair wave, each with its OWN message key.
//
// Parameters:
//   - base int64: the first surrogate key, so two waves cannot collide.
//   - count int: how many rows the wave holds.
//   - status string: the status the rows carry.
//
// Returns:
//   - []model.EventOutbox: the wave, with attempts spent so it belongs to the repair
//     path.
func relayRepairWave(base int64, count int, status string) []model.EventOutbox {
	wave := make([]model.EventOutbox, 0, count)

	for index := 0; index < count; index++ {
		id := base + int64(index)
		row := relayRow(
			id,
			fmt.Sprintf("evt-repair-%d", id),
			"transaction.applied",
			fmt.Sprintf("ldg_repair_%d", id),
			relayFixedNow.Add(time.Duration(id)*time.Second),
		)
		row.Status = status
		row.Attempts = row.MaxAttempts
		row.LastError = "broker unavailable"

		wave = append(wave, row)
	}

	return wave
}

// TestEventRelay_RepairClaimsTakeTheConfiguredBatchSize pins the first half of
// The repair batch is a SETTING rather than a literal.
//
// Both repair passes claimed a fixed 20 rows, once per tick.
func TestEventRelay_RepairClaimsTakeTheConfiguredBatchSize(t *testing.T) {
	t.Run("the shipped default reaches both claims", func(t *testing.T) {
		harness := newRelayHarness(t)

		harness.processor.processTick(context.Background())

		deadLetter, webhook := harness.store.snapshotRepairClaimSizes()
		assert.Equal(t, []int{config.DefaultRelayRepairBatchSize}, deadLetter,
			"the dead-letter repair claim must take the configured batch, not a literal")
		assert.Equal(t, []int{config.DefaultRelayRepairBatchSize}, webhook,
			"and so must the legacy webhook repair claim: one configured capacity, both legs")
	})

	t.Run("an operator's value reaches both claims", func(t *testing.T) {
		harness := newRelayHarness(t)
		harness.processor.WithRepairCapacity(7, 3, 2)

		harness.processor.processTick(context.Background())

		deadLetter, webhook := harness.store.snapshotRepairClaimSizes()
		assert.Equal(t, []int{7}, deadLetter)
		assert.Equal(t, []int{7}, webhook)
	})

	t.Run("a non-positive value is refused rather than applied", func(t *testing.T) {
		harness := newRelayHarness(t)
		harness.processor.WithRepairCapacity(0, -1, 0)

		harness.processor.processTick(context.Background())

		deadLetter, _ := harness.store.snapshotRepairClaimSizes()
		assert.Equal(t, []int{config.DefaultRelayRepairBatchSize}, deadLetter,
			"zero rows a tick is a silently disabled recovery path, and the rows it would abandon "+
				"are the only copies of events that reached no topic at all, so a non-positive value "+
				"must leave the configured capacity standing")
	})
}

// TestEventRelay_RepairChainsBatchesUntilTheBacklogDrains is the second half of
// One tick repairs as much as its budget allows rather than one batch.
//
// A full batch means there may be more, so the pass runs again; a SHORT batch means the
// set is drained and the chain ends.
func TestEventRelay_RepairChainsBatchesUntilTheBacklogDrains(t *testing.T) {
	const batch = 3

	t.Run("the dead-letter leg", func(t *testing.T) {
		harness := newRelayHarness(t)
		harness.processor.WithRepairCapacity(batch, 10, 4)
		harness.store.failedWaves = [][]model.EventOutbox{
			relayRepairWave(100, batch, model.EventOutboxStatusFailed),
			relayRepairWave(200, batch, model.EventOutboxStatusFailed),
			// SHORT, so the chain ends here rather than at the budget.
			relayRepairWave(300, 1, model.EventOutboxStatusFailed),
		}

		harness.processor.processTick(context.Background())

		sizes, _ := harness.store.snapshotRepairClaimSizes()
		assert.Len(t, sizes, 3,
			"a full batch must be followed by another claim in the SAME tick, and the short third "+
				"batch must end the chain")

		assert.Len(t, harness.deadLetters.snapshotRows(), 2*batch+1,
			"every row of every chained batch must be preserved, not just the first batch's")
	})

	t.Run("the legacy webhook leg", func(t *testing.T) {
		harness := newRelayHarness(t)
		harness.processor.WithRepairCapacity(batch, 10, 4)
		harness.store.webhookWaves = [][]model.EventOutbox{
			relayRepairWave(400, batch, model.EventOutboxStatusDispatched),
			relayRepairWave(500, 1, model.EventOutboxStatusDispatched),
		}

		harness.processor.processTick(context.Background())

		_, sizes := harness.store.snapshotRepairClaimSizes()
		assert.Len(t, sizes, 2, "the legacy leg must chain on the same rule as the dead-letter leg")

		assert.Len(t, harness.legacy.snapshot(), batch+1,
			"every claimed row's webhook must be enqueued, across every chained batch")
	})
}

// TestEventRelay_RepairStopsAtItsPerTickBudget is the other side of chaining, and the
// reason the bound exists at all.
//
// An unbounded chain would let one tick attempt an entire historical backlog, holding
// the publish loop — which the same tick owes — behind it for as long as that took.
func TestEventRelay_RepairStopsAtItsPerTickBudget(t *testing.T) {
	const batch = 2

	harness := newRelayHarness(t)
	harness.processor.WithRepairCapacity(batch, 2, 2)
	harness.store.failedWaves = [][]model.EventOutbox{
		relayRepairWave(600, batch, model.EventOutboxStatusFailed),
		relayRepairWave(700, batch, model.EventOutboxStatusFailed),
		// Beyond the budget. It must be untouched, and still be there for the next tick.
		relayRepairWave(800, batch, model.EventOutboxStatusFailed),
	}

	harness.processor.processTick(context.Background())

	sizes, _ := harness.store.snapshotRepairClaimSizes()
	assert.Len(t, sizes, 2,
		"the chain must stop at the per-tick budget, so a large repair backlog cannot starve the "+
			"publish loop it shares the tick with")

	assert.Len(t, harness.deadLetters.snapshotRows(), 2*batch,
		"exactly the budgeted rows are repaired this tick")

	harness.processor.processTick(context.Background())

	assert.Len(t, harness.deadLetters.snapshotRows(), 3*batch,
		"and the deferred wave is repaired on the NEXT tick: the budget defers work rather than "+
			"dropping it")
}

// TestEventRelay_RepairPreservesPerKeyOrderWhileWritingConcurrently is the safety
// property the concurrency setting rests on.
//
// A repair write is still a write to a Kafka topic, so two rows of one aggregate
// written at the same time can be appended in either order.
func TestEventRelay_RepairPreservesPerKeyOrderWhileWritingConcurrently(t *testing.T) {
	harness := newRelayHarness(t)
	harness.processor.WithRepairCapacity(10, 1, 8)

	// ONE key across all three rows, occurrence-ordered, so a concurrent write would be
	// observable as a permutation.
	wave := relayRepairWave(900, 3, model.EventOutboxStatusFailed)
	for index := range wave {
		wave[index].PartitionKey = "ldg_repair_shared"
		wave[index].LedgerID = "ldg_repair_shared"
	}
	harness.store.failedWaves = [][]model.EventOutbox{wave}

	harness.processor.processTick(context.Background())

	rows := harness.deadLetters.snapshotRows()
	require.Len(t, rows, 3, "every row of the wave must be preserved")

	got := make([]string, 0, len(rows))
	for _, row := range rows {
		got = append(got, row.EventID)
	}

	assert.Equal(t, []string{"evt-repair-900", "evt-repair-901", "evt-repair-902"}, got,
		"rows sharing a message key must be written in claim order: they land on one Kafka "+
			"partition, so writing them concurrently would let the broker append them in either order")
}
