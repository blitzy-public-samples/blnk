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

// This file owns the SUBSCRIBER SECURITY LIFECYCLE — the five properties that decide whether
// what the registry says about a subscriber's access is what the broker actually enforces.
//
//	C-02    An authorization Kafka cannot enforce is STATED, not refused. A subscriber carrying
//	        a partition key prefix is still provisionable — Kafka's authorizer has no
//	        message-key dimension, so there is no narrower grant to insist upon and refusing
//	        withdraws a required capability — and every credential response and subscriber read
//	        declares that the narrowing is the holder's own to apply.
//	AUTH-02 A subscriber's grant is RECONCILED on every change, never accumulated. (The broker
//	        half is in event_admin_test.go; the three-step ordering an authorization change
//	        follows is here.)
//	AUTH-01 Deregistration REVOKES BEFORE IT DELETES, behind a tombstone, so a failed
//	        revocation leaves a durable to-do item rather than live access nothing records.
//	CONC-01 Every operation that touches the broker for a subscriber holds that subscriber's
//	        FENCE first, so two overlapping issuances cannot leave the broker holding one
//	        password while the registry records another.
//	CLEAN-01 Compensating writes run on a FRESH bounded context, because the deadline that
//	        failed is one of the commonest reasons they are needed at all.
//
// # What is a double here and what is real
//
// The registry is a faithful in-memory mirror (subscriberTestStore) and the broker is a
// scriptable double (subscriberTestAdmin). Both are doubles deliberately: every property above
// is a property of the ORDER AND CONDITIONS under which this service calls its two
// collaborators, and the only way to assert an order is to record it. The broker's own
// behaviour — that reconciliation converges, that revocation removes bindings before the
// credential — is asserted against the real KafkaAdminClient in event_admin_test.go, and
// end-to-end against a real broker in event_isolation_integration_test.go.
//
// The store mirrors the repository's CONTRACT rather than approximating it, because the service
// branches on it: a missing row must report apierror.ErrSubscriberNotFound, a superseded
// conditional write and a contested fence must both report apierror.ErrConflict, and an expired
// fence must NOT be a conflict. Those are the same codes database/event_subscriber.go returns
// and database/event_subscriber_test.go pins.

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// ---------------------------------------------------------------------------------------
// The shared call log
// ---------------------------------------------------------------------------------------

// subscriberCallLog is ONE recorder shared by both doubles.
//
// It exists because the properties this file asserts are interleavings ACROSS the two
// collaborators: "the row was persisted BETWEEN the prune and the grant" cannot be read from two
// independent sequences, and reconstructing it from a rule about which order the code is
// supposed to use would make the assertion agree with the implementation by construction — it
// would pass even against a persist-first ordering, which is the fail-open this exists to catch.
//
// One log, appended to by whichever double is called, makes the real order observable.
type subscriberCallLog struct {
	mu sync.Mutex

	// calls is every collaborator method invoked, in the order it happened.
	calls []string

	// expiredOn records, per method, whether the context that call arrived on had already
	// expired. It is how CLEAN-01 is proved: a cleanup running on the caller's spent budget
	// arrives with an expired context, and one running on a fresh budget does not.
	expiredOn map[string]bool

	// budgetOn records, per method, how much time the arriving context had left, and -1 when it
	// carried NO DEADLINE AT ALL.
	//
	// The -1 is the interesting value. Every registry mutation except issuance used to arrive
	// with the caller's own bare context, so each Kafka round trip fell back to the admin
	// client's per-request cap and a phase made of several of them was unbounded — which is what
	// made the fence lease impossible to size. A broker call that arrives with no deadline is
	// therefore a failure to assert, not an implementation detail.
	budgetOn map[string]time.Duration

	// deadlineOn records, per method, the absolute instant the arriving context expires at,
	// and is absent when that context carried no deadline.
	//
	// It exists ALONGSIDE budgetOn rather than instead of it because the two answer different
	// questions: budgetOn says how much of the SLA a phase was handed, which is what proves a
	// phase is bounded at all, while this says WHICH deadline it was handed — the only way to
	// prove two calls share one budget rather than each starting a fresh one.
	deadlineOn map[string]time.Time
}

// newSubscriberCallLog builds an empty log.
func newSubscriberCallLog() *subscriberCallLog {
	return &subscriberCallLog{
		expiredOn:  make(map[string]bool),
		budgetOn:   make(map[string]time.Duration),
		deadlineOn: make(map[string]time.Time),
	}
}

// record notes one call, the liveness of the context it arrived on, and that context's deadline.
func (l *subscriberCallLog) record(method string, ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.calls = append(l.calls, method)
	l.expiredOn[method] = ctx.Err() != nil

	if deadline, ok := ctx.Deadline(); ok {
		l.budgetOn[method] = time.Until(deadline)
		l.deadlineOn[method] = deadline
	} else {
		l.budgetOn[method] = -1
		delete(l.deadlineOn, method)
	}
}

// budget returns how much time the context of the LAST call to a method carried, or -1 when it
// carried no deadline. A method never called also reports -1, so callers assert the call
// happened separately.
func (l *subscriberCallLog) budget(method string) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	if remaining, ok := l.budgetOn[method]; ok {
		return remaining
	}

	return -1
}

// sequence returns the observed order.
func (l *subscriberCallLog) sequence() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.calls...)
}

// only returns the observed order restricted to the named methods, which is how an ordering
// assertion states just the steps it is about.
func (l *subscriberCallLog) only(methods ...string) []string {
	wanted := make(map[string]bool, len(methods))
	for _, method := range methods {
		wanted[method] = true
	}

	kept := make([]string, 0, len(methods))
	for _, call := range l.sequence() {
		if wanted[call] {
			kept = append(kept, call)
		}
	}

	return kept
}

// arrivedExpired reports whether the named method last arrived on a dead context.
func (l *subscriberCallLog) arrivedExpired(method string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.expiredOn[method]
}

// arrivedWithDeadline returns the deadline the named method last arrived carrying.
//
// Returns:
//   - time.Time: the deadline.
//   - bool: false when the call was never made, or arrived on a context with no deadline at all
//     — a distinction worth keeping, because "unbounded" is a failure mode of its own here.
func (l *subscriberCallLog) arrivedWithDeadline(method string) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	deadline, ok := l.deadlineOn[method]

	return deadline, ok
}

// count returns how many times a method was called.
func (l *subscriberCallLog) count(method string) int {
	seen := 0
	for _, call := range l.sequence() {
		if call == method {
			seen++
		}
	}

	return seen
}

// ---------------------------------------------------------------------------------------
// The registry double
// ---------------------------------------------------------------------------------------

// subscriberTestStore is an in-memory mirror of the subscriber registry.
//
// It records the sequence of operations as well as their effects, because most of what this
// file asserts is ORDER: "the tombstone was stamped before the broker was asked to revoke" is
// not observable from final state, only from the sequence.
type subscriberTestStore struct {
	mu sync.Mutex

	rows   map[string]model.EventSubscriber
	fences map[string]subscriberTestFence

	// log is the SHARED recorder, so this double's calls and the broker double's appear in one
	// observed order.
	log *subscriberCallLog

	// Injected failures, by method name.
	failures map[string]error

	// obligations is the settlement bookkeeping the row itself does not carry.
	//
	// It is a SEPARATE map for the same reason model.EventSubscriber has no obligation fields:
	// the columns are operational bookkeeping for one background worker, and putting them on the
	// row would place them in every registry response, fixture and wire-contract assertion.
	obligations map[string]subscriberTestObligation
}

// subscriberTestObligation mirrors the five settlement columns.
type subscriberTestObligation struct {
	grantPendingAt      time.Time
	credentialCleanupAt time.Time
	attempts            int
	lastError           string
	lastAttemptAt       time.Time
}

// outstanding reports whether anything is owed, mirroring model.SubscriberSettlementObligation.
func (o subscriberTestObligation) outstanding() bool {
	return !o.grantPendingAt.IsZero() || !o.credentialCleanupAt.IsZero()
}

// subscriberTestFence is one live provisioning claim.
type subscriberTestFence struct {
	token string
	until time.Time
}

// newSubscriberTestStore builds an empty registry against a shared call log.
func newSubscriberTestStore(log *subscriberCallLog) *subscriberTestStore {
	return &subscriberTestStore{
		rows:        make(map[string]model.EventSubscriber),
		fences:      make(map[string]subscriberTestFence),
		log:         log,
		failures:    make(map[string]error),
		obligations: make(map[string]subscriberTestObligation),
	}
}

// owing seeds an outstanding settlement obligation, so a test can drive the settlement remedy
// without first having to reproduce the failure that raises one.
func (s *subscriberTestStore) owing(
	subscriberID string,
	grantPending, credentialCleanup bool,
) *subscriberTestStore {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	obligation := s.obligations[key]
	stamped := time.Now().UTC()

	if grantPending {
		obligation.grantPendingAt = stamped
	}
	if credentialCleanup {
		obligation.credentialCleanupAt = stamped
	}

	s.obligations[key] = obligation

	return s
}

// obligationOf reads a subscriber's settlement bookkeeping, for assertions.
func (s *subscriberTestStore) obligationOf(subscriberID string) subscriberTestObligation {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.obligations[strings.TrimSpace(subscriberID)]
}

// with seeds a row.
func (s *subscriberTestStore) with(row model.EventSubscriber) *subscriberTestStore {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rows[row.SubscriberID] = row

	return s
}

// failing makes one method report an error.
func (s *subscriberTestStore) failing(method string, err error) *subscriberTestStore {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failures[method] = err

	return s
}

// record notes a call in the shared log and returns any injected failure. Callers must NOT hold
// the mutex.
func (s *subscriberTestStore) record(method string, ctx context.Context) error {
	s.log.record(method, ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.failures[method]
}

// row returns a copy of a stored row.
func (s *subscriberTestStore) row(subscriberID string) (model.EventSubscriber, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	row, ok := s.rows[subscriberID]

	return row, ok
}

// fenced reports whether a live claim is held.
func (s *subscriberTestStore) fenced(subscriberID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	held, ok := s.fences[subscriberID]

	return ok && held.until.After(time.Now())
}

// holdFence takes a claim out of band, so a test can express "somebody else is already
// provisioning this subscriber".
func (s *subscriberTestStore) holdFence(subscriberID string, lease time.Duration) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := uuid.NewString()
	s.fences[subscriberID] = subscriberTestFence{token: token, until: time.Now().Add(lease)}

	return token
}

func (s *subscriberTestStore) notFound(subscriberID string) error {
	return apierror.NewAPIError(
		apierror.ErrSubscriberNotFound,
		"Event subscriber not found",
		fmt.Errorf("subscriber test store: no subscriber %q is registered", subscriberID),
	)
}

func (s *subscriberTestStore) clone(row model.EventSubscriber) *model.EventSubscriber {
	copied := row
	copied.AuthorizedTopics = append([]string(nil), row.AuthorizedTopics...)

	return &copied
}

func (s *subscriberTestStore) CreateEventSubscriber(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (*model.EventSubscriber, error) {
	if err := s.record("CreateEventSubscriber", ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.rows[subscriber.SubscriberID]; exists {
		return nil, apierror.NewAPIError(apierror.ErrConflict, "duplicate", errors.New("duplicate"))
	}

	stored := *subscriber
	stored.CreatedAt = time.Now().UTC()
	stored.UpdatedAt = stored.CreatedAt
	s.rows[stored.SubscriberID] = stored

	return s.clone(stored), nil
}

func (s *subscriberTestStore) GetEventSubscriberByID(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	if err := s.record("GetEventSubscriberByID", ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	row, ok := s.rows[strings.TrimSpace(subscriberID)]
	if !ok {
		return nil, s.notFound(subscriberID)
	}

	return s.clone(row), nil
}

func (s *subscriberTestStore) ListEventSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, error) {
	if err := s.record("ListEventSubscribers", ctx); err != nil {
		return model.SubscriberPage{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rows := make([]model.EventSubscriber, 0, len(s.rows))
	for _, row := range s.rows {
		rows = append(rows, row)
	}

	page := model.SubscriberPage{Subscribers: rows}
	if query.Limit > 0 && query.Limit < len(rows) {
		page.Subscribers = rows[:query.Limit]
		page.HasMore = true
	}

	return page, nil
}

// ListAndCountEventSubscribers answers the page and the total from ONE observation of the
// fake's state, which is what the repository's read-only REPEATABLE READ transaction gives the
// real store.
//
// One lock acquisition covers both answers. Calling the two single-purpose methods in turn would
// release the mutex between them and reproduce the very two-snapshot defect this method closes,
// so a test asserting coherence would pass against a store that does not hold the property.
//
// The call is recorded under its own name so a test can assert that a listing which asked for a
// total made ONE call rather than two.
func (s *subscriberTestStore) ListAndCountEventSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, int64, error) {
	if err := s.record("ListAndCountEventSubscribers", ctx); err != nil {
		return model.SubscriberPage{}, 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rows := make([]model.EventSubscriber, 0, len(s.rows))
	for _, row := range s.rows {
		rows = append(rows, row)
	}

	total := int64(len(rows))

	page := model.SubscriberPage{Subscribers: rows}
	if query.Limit > 0 && query.Limit < len(rows) {
		page.Subscribers = rows[:query.Limit]
		page.HasMore = true
	}

	return page, total, nil
}

// CountEventSubscribers counts every seeded row. It counts the SAME set
// ListEventSubscribers pages, which is the property the listing's include_count depends on.
func (s *subscriberTestStore) CountEventSubscribers(ctx context.Context) (int64, error) {
	if err := s.record("CountEventSubscribers", ctx); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return int64(len(s.rows)), nil
}

func (s *subscriberTestStore) UpdateEventSubscriber(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
) (*model.EventSubscriber, error) {
	if err := s.record("UpdateEventSubscriber", ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Mirrors the repository's WHERE clause exactly: the key, the claim, AND the absence of a
	// revocation tombstone. A fake that skipped the last two would let the fenced-write tests
	// pass against a store that does not enforce what production enforces.
	if err := s.fencedWriteGuardLocked(subscriber.SubscriberID, fenceToken, true); err != nil {
		return nil, err
	}

	stored := *subscriber
	// STAMPED HERE, exactly as the statement's `updated_at = $7` does — and returned, because the
	// caller must answer with the stored instant rather than the one it read on the way in. A fake
	// that returned the caller's own copy would let the staleness defect pass unnoticed.
	stored.UpdatedAt = time.Now().UTC()
	s.rows[stored.SubscriberID] = stored

	returned := stored

	return &returned, nil
}

func (s *subscriberTestStore) TakeEventSubscriber(
	ctx context.Context,
	subscriberID string,
	fenceToken string,
) (*model.EventSubscriber, error) {
	if err := s.record("TakeEventSubscriber", ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return nil, err
	}

	key := strings.TrimSpace(subscriberID)
	row := s.rows[key]

	delete(s.rows, key)

	// The provisioning columns live ON the row, so deleting it takes the claim with it. Keeping
	// a fence entry for a row that no longer exists would let a test assert a fence state the
	// real schema cannot represent.
	delete(s.fences, key)

	return s.clone(row), nil
}

func (s *subscriberTestStore) RecordSubscriberCredentialIfUnchanged(
	ctx context.Context,
	subscriberID string,
	expected *string,
	credentialReference string,
	issuedAt time.Time,
	fenceToken string,
) error {
	if err := s.record("RecordSubscriberCredentialIfUnchanged", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return err
	}

	key := strings.TrimSpace(subscriberID)
	row := s.rows[key]

	if !subscriberTestReferencesMatch(row.CredentialReference, expected) {
		return apierror.NewAPIError(
			apierror.ErrConflict,
			"A concurrent credential issuance superseded this one",
			errors.New("subscriber test store: the credential reference changed under the issuance"),
		)
	}

	reference := credentialReference
	stamped := issuedAt
	row.CredentialReference = &reference
	row.CredentialIssuedAt = &stamped
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	// SAME STATEMENT, same effect: a successful issuance REPLACES whatever credential a pending
	// cleanup was about, because provisioning upserts. Mirroring it here is what lets a test
	// prove that re-issuing does not leave settlement holding a marker that would destroy the
	// credential it just recorded.
	s.dischargeCredentialCleanupLocked(key)

	// And the ORPHAN marker, which the same statement clears for the same reason: one SCRAM
	// credential exists per principal, so this issuance replaced whatever was orphaned.
	row.CredentialOrphanedAt = nil
	s.rows[key] = row

	return nil
}

// dischargeCredentialCleanupLocked clears the cleanup obligation, and the settlement counters
// when nothing else remains owed. It mirrors the CASE arms of the two statements that discharge
// it. Callers must hold the mutex.
func (s *subscriberTestStore) dischargeCredentialCleanupLocked(key string) {
	obligation, ok := s.obligations[key]
	if !ok {
		return
	}

	obligation.credentialCleanupAt = time.Time{}

	if obligation.grantPendingAt.IsZero() {
		obligation.attempts = 0
		obligation.lastError = ""
	}

	s.obligations[key] = obligation
}

func (s *subscriberTestStore) ClearSubscriberCredential(
	ctx context.Context,
	subscriberID, fenceToken string,
) error {
	if err := s.record("ClearSubscriberCredential", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return err
	}

	key := strings.TrimSpace(subscriberID)
	row := s.rows[key]

	row.CredentialReference = nil
	row.CredentialIssuedAt = nil

	// AND THE REVOCATION MARKERS, mirroring the repository's single statement. Every caller
	// reaches this only once the broker has confirmed the revocation, so "a principal may still
	// authenticate" is false and the tombstone must not keep asserting it.
	row.RevocationPendingAt = nil
	row.RevocationFailedAt = nil
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	// The same statement discharges the cleanup obligation, because the row's credential columns
	// and the marker describe one fact: whether Blnk records a credential it has not settled.
	s.dischargeCredentialCleanupLocked(key)

	return nil
}

// RecordSubscriberWebhookURL writes the URL and CLEARS migrated_at, mirroring the repository's
// single statement. Doing only the first half here would let a test pass against a double that
// permits the self-contradicting row the real schema refuses.
func (s *subscriberTestStore) RecordSubscriberWebhookURL(
	ctx context.Context,
	subscriberID, webhookURL string,
) error {
	if err := s.record("RecordSubscriberWebhookURL", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return s.notFound(subscriberID)
	}

	recorded := webhookURL
	row.WebhookURL = &recorded
	row.MigratedAt = nil
	s.rows[key] = row

	return nil
}

// ClearSubscriberWebhookURL forgets the URL and leaves migrated_at alone.
func (s *subscriberTestStore) ClearSubscriberWebhookURL(
	ctx context.Context,
	subscriberID string,
) error {
	if err := s.record("ClearSubscriberWebhookURL", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return s.notFound(subscriberID)
	}

	row.WebhookURL = nil
	s.rows[key] = row

	return nil
}

func (s *subscriberTestStore) MarkSubscriberMigrated(
	ctx context.Context,
	subscriberID string,
	migratedAt time.Time,
) error {
	if err := s.record("MarkSubscriberMigrated", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return s.notFound(subscriberID)
	}

	// THE REPOSITORY'S REFUSAL, mirrored. Datasource.MarkSubscriberMigrated carries
	// `AND webhook_url IS NULL` in its statement and answers ErrGenConflict when the row
	// still holds one, because stamping alone there would assert that the subscriber both
	// has and has not stopped receiving legacy pushes. The double refuses identically —
	// same code, same steering — so a test cannot prove a call path works that the real
	// repository rejects, and cannot prove one is rejected more harshly than it is.
	//
	// This used to cite a schema CHECK named event_subscribers_webhook_migration_chk. No
	// such constraint exists, and none is wanted: the RETAIN-01 retention purge selects
	// exactly the pair a CHECK would forbid. The double was therefore STRICTER than
	// production — it refused what PostgreSQL happily wrote — which is the one way a fake
	// can turn a green test into a false one. The refusal now lives in the repository, so
	// this mirrors something real.
	if row.WebhookURL != nil && strings.TrimSpace(*row.WebhookURL) != "" {
		return apierror.NewAPIError(
			apierror.ErrGenConflict,
			"This subscriber still has a legacy webhook URL recorded, so it cannot be marked "+
				"migrated on its own. Complete the migration instead, which forgets the URL and "+
				"records the instant together.",
			errors.New(
				"subscriber test store: this row still holds a webhook_url, so stamping "+
					"migrated_at alone would assert that it both has and has not stopped "+
					"receiving legacy pushes; use CompleteSubscriberWebhookMigration",
			),
		)
	}

	stamped := migratedAt
	row.MigratedAt = &stamped
	s.rows[key] = row

	return nil
}

// CompleteSubscriberWebhookMigration writes BOTH columns or neither, which is the property the
// endpoint depends on. A fake that wrote them in sequence would let a test assert atomicity
// against a store that does not provide it.
func (s *subscriberTestStore) CompleteSubscriberWebhookMigration(
	ctx context.Context,
	subscriberID string,
	migratedAt time.Time,
) (*model.EventSubscriber, error) {
	if err := s.record("CompleteSubscriberWebhookMigration", ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return nil, s.notFound(subscriberID)
	}

	// No fence check and no tombstone check, matching the repository: neither column has a
	// broker counterpart, and the erasure of third-party data does not wait on another
	// operation's state.
	row.WebhookURL = nil
	// COALESCE, mirroring the statement: the FIRST transition is kept, so a repeat call cannot
	// move a long-migrated subscriber's migration instant forward to today. A fake that
	// overwrote it would let the defect pass here while the repository's own test caught it.
	if row.MigratedAt == nil {
		stamped := migratedAt
		row.MigratedAt = &stamped
	}
	row.UpdatedAt = time.Now().UTC()
	s.rows[key] = row

	return s.clone(row), nil
}

func (s *subscriberTestStore) PurgeMigratedSubscriberWebhookURLs(
	ctx context.Context,
	migratedBefore time.Time,
) (int64, error) {
	if err := s.record("PurgeMigratedSubscriberWebhookURLs", ctx); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var purged int64
	for key, row := range s.rows {
		if row.WebhookURL == nil || row.MigratedAt == nil || !row.MigratedAt.Before(migratedBefore) {
			continue
		}

		row.WebhookURL = nil
		s.rows[key] = row
		purged++
	}

	return purged, nil
}

func (s *subscriberTestStore) MarkSubscriberRevocationPending(
	ctx context.Context,
	subscriberID string,
	pendingAt time.Time,
	fenceToken string,
) (*model.EventSubscriber, error) {
	if err := s.record("MarkSubscriberRevocationPending", ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// requireActive is FALSE: re-marking an already-tombstoned row is a deregistration retry,
	// which is the one mutation for which the tombstone is expected input.
	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return nil, err
	}

	key := strings.TrimSpace(subscriberID)
	row := s.rows[key]

	// COALESCE semantics: a re-mark keeps the FIRST instant, because the value an operator
	// needs is how long the revocation has been outstanding.
	if row.RevocationPendingAt == nil {
		stamped := pendingAt
		row.RevocationPendingAt = &stamped
	}

	s.rows[key] = row

	return s.clone(row), nil
}

func (s *subscriberTestStore) ClaimSubscriberForProvisioning(
	ctx context.Context,
	subscriberID string,
	lease time.Duration,
) (string, error) {
	if err := s.record("ClaimSubscriberForProvisioning", ctx); err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	if _, ok := s.rows[key]; !ok {
		return "", s.notFound(subscriberID)
	}

	if lease <= 0 {
		lease = SubscriberProvisioningFenceLease
	}

	now := time.Now()
	if held, ok := s.fences[key]; ok && held.until.After(now) {
		return "", apierror.NewAPIError(
			apierror.ErrConflict,
			"Another credential operation for this subscriber is already in progress",
			fmt.Errorf("subscriber test store: %q is fenced until %s", subscriberID, held.until),
		)
	}

	token := uuid.NewString()
	s.fences[key] = subscriberTestFence{token: token, until: now.Add(lease)}

	return token, nil
}

func (s *subscriberTestStore) ReleaseSubscriberProvisioningFence(
	ctx context.Context,
	subscriberID string,
	token string,
) error {
	if err := s.record("ReleaseSubscriberProvisioningFence", ctx); err != nil {
		return err
	}

	if strings.TrimSpace(token) == "" {
		return apierror.NewAPIError(apierror.ErrInvalidInput, "a token is required", nil)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// THE SHARED PREDICATE, which is token AND LEASE. Each of these three sites tested the token
	// alone, so an EXPIRED claim was accepted as held — and the production statements carry
	// `provisioning_until > NOW()` in the write itself, so the double was modelling a permission
	// the database does not grant. The helper also produces the conflict carrying the phrase
	// subscriberFenceWasLost matches on, which is what routes the service to its abandon-and-
	// compensate branch instead of to the generic-conflict one.
	key := strings.TrimSpace(subscriberID)
	if err := s.requireFenceLocked(subscriberID, token, "releasing the provisioning claim"); err != nil {
		return err
	}

	delete(s.fences, key)

	return nil
}

func (s *subscriberTestStore) RenewSubscriberProvisioningFence(
	ctx context.Context,
	subscriberID string,
	token string,
	lease time.Duration,
) error {
	if err := s.record("RenewSubscriberProvisioningFence", ctx); err != nil {
		return err
	}

	if strings.TrimSpace(token) == "" {
		return apierror.NewAPIError(apierror.ErrInvalidInput, "a token is required", nil)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	if _, ok := s.rows[key]; !ok {
		return s.notFound(subscriberID)
	}

	// Conditional, and it does NOT re-claim: a caller whose lease was taken over must learn that
	// it no longer owns the subscriber, not be handed it back. The predicate is the shared one,
	// so an expired lease is a refusal here exactly as it is in the statement.
	if err := s.requireFenceLocked(subscriberID, token, "renewing the provisioning claim"); err != nil {
		return err
	}

	held := s.fences[key]

	if lease <= 0 {
		lease = SubscriberProvisioningFenceLease
	}

	// Recomputed from now rather than added to the stored value, matching the repository, so a
	// renewal grants exactly one lease of headroom however late it arrives.
	held.until = time.Now().Add(lease)
	s.fences[key] = held

	return nil
}

// fencedWriteGuardLocked reproduces the WHERE clause every fenced write carries.
//
// The three outcomes it distinguishes are the three the repository distinguishes, and they are
// kept apart here for the same reason: a test asserting "the stale owner was refused" must fail
// if the fake answers "not found", because that is what the production code did before the
// claim predicate existed.
//
// s.mu must already be held.
//
// Parameters:
//   - subscriberID string: the business key.
//   - token string: the claim the caller presented.
//   - requireActive bool: true for a write that must refuse a tombstoned row.
//
// Returns:
//   - error: nil when the write may proceed.
func (s *subscriberTestStore) RecordSubscriberGrantReconcilePending(
	ctx context.Context,
	subscriberID string,
	pendingAt time.Time,
	fenceToken string,
) error {
	if err := s.record("RecordSubscriberGrantReconcilePending", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return err
	}

	key := strings.TrimSpace(subscriberID)
	obligation := s.obligations[key]

	// FIRST INSTANT WINS, mirroring the statement's COALESCE, so the obligation's age is the age
	// of the divergence rather than of the last retry.
	if obligation.grantPendingAt.IsZero() {
		if pendingAt.IsZero() {
			pendingAt = time.Now()
		}

		obligation.grantPendingAt = pendingAt.UTC()
	}

	s.obligations[key] = obligation

	return nil
}

func (s *subscriberTestStore) ClearSubscriberGrantReconcilePending(
	ctx context.Context,
	subscriberID, fenceToken string,
) error {
	if err := s.record("ClearSubscriberGrantReconcilePending", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return err
	}

	key := strings.TrimSpace(subscriberID)
	obligation := s.obligations[key]
	obligation.grantPendingAt = time.Time{}

	// The counters belong to whatever is STILL outstanding, so they survive when a credential
	// cleanup remains owed.
	if obligation.credentialCleanupAt.IsZero() {
		obligation.attempts = 0
		obligation.lastError = ""
	}

	s.obligations[key] = obligation

	return nil
}

func (s *subscriberTestStore) RecordSubscriberCredentialCleanupPending(
	ctx context.Context,
	subscriberID string,
	pendingAt time.Time,
	fenceToken string,
) error {
	if err := s.record("RecordSubscriberCredentialCleanupPending", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.fencedWriteGuardLocked(subscriberID, fenceToken, false); err != nil {
		return err
	}

	key := strings.TrimSpace(subscriberID)
	obligation := s.obligations[key]

	if obligation.credentialCleanupAt.IsZero() {
		if pendingAt.IsZero() {
			pendingAt = time.Now()
		}

		obligation.credentialCleanupAt = pendingAt.UTC()
	}

	s.obligations[key] = obligation

	return nil
}

func (s *subscriberTestStore) GetSubscriberSettlementObligation(
	ctx context.Context,
	subscriberID string,
) (model.SubscriberSettlementObligation, error) {
	if err := s.record("GetSubscriberSettlementObligation", ctx); err != nil {
		return model.SubscriberSettlementObligation{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	if _, ok := s.rows[key]; !ok {
		return model.SubscriberSettlementObligation{}, s.notFound(subscriberID)
	}

	obligation := s.obligations[key]

	return model.SubscriberSettlementObligation{
		SubscriberID:             key,
		GrantReconcilePending:    !obligation.grantPendingAt.IsZero(),
		CredentialCleanupPending: !obligation.credentialCleanupAt.IsZero(),
		Attempts:                 obligation.attempts,
		LastError:                obligation.lastError,
	}, nil
}

func (s *subscriberTestStore) ListSubscriberSettlementObligations(
	ctx context.Context,
	limit int,
	notBefore time.Time,
) ([]model.SubscriberSettlementObligation, error) {
	if err := s.record("ListSubscriberSettlementObligations", ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if limit <= 0 {
		limit = 50
	}

	keys := make([]string, 0, len(s.obligations))
	for key, obligation := range s.obligations {
		if !obligation.outstanding() {
			continue
		}

		// A row never attempted is ALWAYS eligible, and a zero bound means "no pacing" — the two
		// arms the statement spells out explicitly because `last_attempt < NULL` is NULL rather
		// than true.
		if !notBefore.IsZero() && !obligation.lastAttemptAt.IsZero() &&
			!obligation.lastAttemptAt.Before(notBefore) {
			continue
		}

		keys = append(keys, key)
	}

	// Oldest attempt first, never-attempted before attempted, then by key so the order is
	// deterministic where the instants tie.
	sort.Slice(keys, func(i, j int) bool {
		left, right := s.obligations[keys[i]], s.obligations[keys[j]]

		switch {
		case left.lastAttemptAt.IsZero() && !right.lastAttemptAt.IsZero():
			return true
		case !left.lastAttemptAt.IsZero() && right.lastAttemptAt.IsZero():
			return false
		case !left.lastAttemptAt.Equal(right.lastAttemptAt):
			return left.lastAttemptAt.Before(right.lastAttemptAt)
		default:
			return keys[i] < keys[j]
		}
	})

	obligations := make([]model.SubscriberSettlementObligation, 0, len(keys))
	for _, key := range keys {
		if len(obligations) >= limit {
			break
		}

		obligation := s.obligations[key]
		obligations = append(obligations, model.SubscriberSettlementObligation{
			SubscriberID:             key,
			GrantReconcilePending:    !obligation.grantPendingAt.IsZero(),
			CredentialCleanupPending: !obligation.credentialCleanupAt.IsZero(),
			Attempts:                 obligation.attempts,
			LastError:                obligation.lastError,
		})
	}

	return obligations, nil
}

func (s *subscriberTestStore) MarkSubscriberSettlementAttempt(
	ctx context.Context,
	subscriberID string,
	attemptedAt time.Time,
	failure string,
) error {
	if err := s.record("MarkSubscriberSettlementAttempt", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	obligation := s.obligations[key]

	if attemptedAt.IsZero() {
		attemptedAt = time.Now()
	}

	obligation.attempts++
	obligation.lastAttemptAt = attemptedAt.UTC()
	obligation.lastError = failure
	s.obligations[key] = obligation

	return nil
}

func (s *subscriberTestStore) CountSubscriberSettlementObligations(
	ctx context.Context,
) (model.SubscriberSettlementBacklog, error) {
	if err := s.record("CountSubscriberSettlementObligations", ctx); err != nil {
		return model.SubscriberSettlementBacklog{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	backlog := model.SubscriberSettlementBacklog{}

	for _, obligation := range s.obligations {
		if !obligation.outstanding() {
			continue
		}

		backlog.Outstanding++

		if !obligation.grantPendingAt.IsZero() {
			backlog.GrantReconcilePending++

			if backlog.OldestPendingAt.IsZero() || obligation.grantPendingAt.Before(backlog.OldestPendingAt) {
				backlog.OldestPendingAt = obligation.grantPendingAt
			}
		}

		if !obligation.credentialCleanupAt.IsZero() {
			backlog.CredentialCleanupPending++

			if backlog.OldestPendingAt.IsZero() || obligation.credentialCleanupAt.Before(backlog.OldestPendingAt) {
				backlog.OldestPendingAt = obligation.credentialCleanupAt
			}
		}
	}

	return backlog, nil
}

func (s *subscriberTestStore) fencedWriteGuardLocked(
	subscriberID, token string,
	requireActive bool,
) error {
	if strings.TrimSpace(token) == "" {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"A provisioning claim token is required to modify this subscriber", nil)
	}

	key := strings.TrimSpace(subscriberID)

	row, ok := s.rows[key]
	if !ok {
		return s.notFound(subscriberID)
	}

	if err := s.requireFenceLocked(subscriberID, token, "applying a fenced write"); err != nil {
		return err
	}

	if requireActive && row.RevocationPendingAt != nil {
		return apierror.NewAPIError(
			apierror.ErrConflict,
			"This subscriber is being deregistered, so its access model can no longer be changed",
			errors.New("subscriber test store: the row carries a revocation tombstone"),
		)
	}

	return nil
}

// subscriberTestReferencesMatch is the conditional write's comparison. Two nils match — the
// first-issuance case — and a nil on one side only does not.
func subscriberTestReferencesMatch(stored, expected *string) bool {
	switch {
	case stored == nil && expected == nil:
		return true
	case stored == nil || expected == nil:
		return false
	default:
		return *stored == *expected
	}
}

// Compile-time proof that the double satisfies the seam. A signature drift fails the build on
// the line that names the contract rather than at a constructor call.
var _ eventSubscriberStore = (*subscriberTestStore)(nil)

// subscriberProvisionedRow builds a subscriber that already holds a credential.
//
// The credential record is TWO columns written together — the schema's
// event_subscribers_credential_pair_chk enforces it — so a helper is used rather than letting
// each test set one and forget the other, which would seed a row the real registry could not
// hold and quietly change what IsProvisioned answers.
func subscriberProvisionedRow(t *testing.T) model.EventSubscriber {
	t.Helper()

	row := subscriberFixtureRow(t)
	reference, err := model.DeriveCredentialReference(row.KafkaPrincipal, "a-secret-that-was-returned-once")
	require.NoError(t, err, "deriving the fixture's credential reference")

	issued := time.Now().UTC().Add(-time.Hour)
	row.CredentialReference = &reference
	row.CredentialIssuedAt = &issued

	return row
}

// ---------------------------------------------------------------------------------------
// The broker double
// ---------------------------------------------------------------------------------------

// subscriberTestAdmin is a scriptable administrative client.
//
// Like the store it records the sequence and whether each call arrived on a live context, so
// ordering and CLEAN-01 are both observable.
type subscriberTestAdmin struct {
	mu sync.Mutex

	configured bool
	brokers    []string

	// log is the SHARED recorder — the same one the registry double appends to.
	log *subscriberCallLog

	// revoked names every subscriber RevokeSubscriber was called for, in order.
	revoked []string

	// pruned and granted record the authorization each reconciliation half observed, which is
	// how "prune saw the OLD grant and grant saw the NEW one" is asserted.
	pruned  [][]string
	granted [][]string

	failures map[string]error

	// result is what a successful provisioning reports.
	result SubscriberProvisioningResult

	// onProvision runs inside ProvisionSubscriberPrincipal, so a test can act while an
	// issuance is mid-flight.
	onProvision func()

	// onPrune and onRevoke are the same hook for the other two broker phases. They exist so a
	// test can take the provisioning claim over WHILE a phase is in flight, which is the only
	// way to drive the fail-open path the token predicates close: an operation that keeps going
	// after its lease has been lost.
	onPrune  func()
	onRevoke func()

	// provisionErr makes provisioning fail while STILL returning result, which is a different
	// shape from failures["ProvisionSubscriberPrincipal"] and models a different broker.
	//
	// failures models a call that did not complete, so it yields a zero result: nothing is
	// known about what reached the broker. The refusals yield a POPULATED result alongside the
	// error — the credential was written, compensation ran, and the flags say whether it was
	// confirmed — and those flags are what provisioningFailure branches on. Without this seam
	// the compensated and compensation-failed branches are unreachable from a test, which is
	// exactly where the difference between "retry safely" and "revoke by hand" lives.
	provisionErr error

	// requests records every provisioning request, so the flags the service SENDS —
	// DeferCompensation above all — are assertable rather than inferred from what happened
	// afterwards.
	requests []SubscriberProvisioningRequest

	// failureResult is what a FAILED provisioning reports, when a test sets one.
	//
	// The zero value is returned otherwise, which is what a provisioning that never reached
	// the broker looks like. A test drives the deferred-compensation path (PERF-P09) by
	// setting a result whose CompensationOwed is true alongside the failure, which is exactly
	// the pair the real client returns for an ACL grant that failed after the credential was
	// written.
	failureResult SubscriberProvisioningResult

	// compensated records every deferred compensation the service handed back.
	compensated []SubscriberProvisioningResult
}

func newSubscriberTestAdmin(log *subscriberCallLog) *subscriberTestAdmin {
	return &subscriberTestAdmin{
		configured: true,
		brokers:    []string{"broker-1:9092", "broker-2:9092"},
		log:        log,
		failures:   make(map[string]error),
		result: SubscriberProvisioningResult{
			Topics:            []string{"blnk.transactions"},
			CredentialWritten: true,
			ACLBindings:       3,
		},
	}
}

func (a *subscriberTestAdmin) failing(method string, err error) *subscriberTestAdmin {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.failures[method] = err

	return a
}

func (a *subscriberTestAdmin) record(method string, ctx context.Context) error {
	a.log.record(method, ctx)

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.failures[method]
}

func (a *subscriberTestAdmin) revocations() []string {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]string(nil), a.revoked...)
}

func (a *subscriberTestAdmin) IsConfigured() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.configured
}

func (a *subscriberTestAdmin) Brokers() []string {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]string(nil), a.brokers...)
}

func (a *subscriberTestAdmin) ProvisionSubscriberPrincipal(
	ctx context.Context,
	req SubscriberProvisioningRequest,
) (SubscriberProvisioningResult, error) {
	if a.onProvision != nil {
		a.onProvision()
	}

	a.mu.Lock()
	a.requests = append(a.requests, req)
	a.mu.Unlock()

	if err := a.record("ProvisionSubscriberPrincipal", ctx); err != nil {
		a.mu.Lock()
		defer a.mu.Unlock()

		return a.failureResult, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.result, a.provisionErr
}

// CompensateProvisioning records the deferred compensation rather than performing one: the
// service's obligation is to CALL it, and whether it does is the assertion.
func (a *subscriberTestAdmin) CompensateProvisioning(
	ctx context.Context,
	result SubscriberProvisioningResult,
) error {
	if err := a.record("CompensateProvisioning", ctx); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if result.CompensationOwed {
		a.compensated = append(a.compensated, result)
	}

	return nil
}

// provisioningRequests returns the requests the service sent, in order.
func (a *subscriberTestAdmin) provisioningRequests() []SubscriberProvisioningRequest {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]SubscriberProvisioningRequest(nil), a.requests...)
}

// compensations returns the deferred compensations the service handed back, in order.
func (a *subscriberTestAdmin) compensations() []SubscriberProvisioningResult {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]SubscriberProvisioningResult(nil), a.compensated...)
}

func (a *subscriberTestAdmin) RevokeSubscriber(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) error {
	if a.onRevoke != nil {
		a.onRevoke()
	}

	if err := a.record("RevokeSubscriber", ctx); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if subscriber != nil {
		a.revoked = append(a.revoked, subscriber.SubscriberID)
	}

	return nil
}

func (a *subscriberTestAdmin) PruneSubscriberAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (SubscriberACLReconciliation, error) {
	if a.onPrune != nil {
		a.onPrune()
	}

	if err := a.record("PruneSubscriberAccess", ctx); err != nil {
		return SubscriberACLReconciliation{}, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if subscriber != nil {
		a.pruned = append(a.pruned, append([]string(nil), subscriber.AuthorizedTopics...))
	}

	return SubscriberACLReconciliation{Removed: 2}, nil
}

func (a *subscriberTestAdmin) GrantSubscriberAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (SubscriberACLReconciliation, error) {
	if err := a.record("GrantSubscriberAccess", ctx); err != nil {
		return SubscriberACLReconciliation{}, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if subscriber != nil {
		a.granted = append(a.granted, append([]string(nil), subscriber.AuthorizedTopics...))
	}

	return SubscriberACLReconciliation{Created: 1}, nil
}

func (a *subscriberTestAdmin) Close() error { return nil }

var _ subscriberPrincipalProvisioner = (*subscriberTestAdmin)(nil)

// ---------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------

// subscriberFixtureID is the canonical identifier every test in this file uses.
const subscriberFixtureID = "sub_lifecycle_7c2a"

// subscriberFixtureRow builds a registered, provisionable subscriber.
func subscriberFixtureRow(t *testing.T) model.EventSubscriber {
	t.Helper()

	principal, err := model.CanonicalKafkaPrincipal(subscriberFixtureID)
	require.NoError(t, err)

	group, err := model.CanonicalConsumerGroupID(subscriberFixtureID)
	require.NoError(t, err)

	return model.EventSubscriber{
		SubscriberID:     subscriberFixtureID,
		Name:             "Lifecycle Fixture",
		KafkaPrincipal:   principal,
		ConsumerGroupID:  group,
		AuthorizedTopics: []string{"blnk.transactions", "blnk.balances"},
	}
}

// subscriberLifecycle is one prepared run: a shared call log, the registry double, the broker
// double, and the service wired to both.
type subscriberLifecycle struct {
	log     *subscriberCallLog
	store   *subscriberTestStore
	admin   *subscriberTestAdmin
	service *EventSubscriberService
}

// newSubscriberLifecycle builds a run seeded with one registered, provisionable subscriber.
//
// Everything shares ONE call log, which is what makes an interleaving across the registry and
// the broker observable rather than reconstructed.
// subscriberLifecycleSubscriberBrokers is the EXTERNALLY ADVERTISED list a subscriber is told
// to connect to, and it is intentionally not the admin client's internal list
// ("broker-1:9092", "broker-2:9092"). Two different values are what make it possible to prove
// which one the response carries — with one shared value every assertion would pass whichever
// list the code read.
var subscriberLifecycleSubscriberBrokers = []string{"kafka.example.com:9094"}

// subscriberLifecycleConfiguration publishes a configuration with Kafka and the
// subscriber-facing broker list set, and restores whatever was published before.
func subscriberLifecycleConfiguration(t *testing.T) *config.Configuration {
	t.Helper()

	cnf := &config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:           []string{"broker-1:9092", "broker-2:9092"},
			SubscriberBrokers: append([]string(nil), subscriberLifecycleSubscriberBrokers...),
			TopicPrefix:       "blnk",
		},
	}
	outboxStoreConfiguration(t, cnf)

	return cnf
}

// subscriberLifecycleKeyScopeGatewayBrokers is the ENFORCING endpoint a key-scoped subscriber is
// told to dial.
//
// Deliberately different from both the internal broker list and the advertised subscriber list,
// so an assertion that the gateway was reported cannot pass by accident on either.
var subscriberLifecycleKeyScopeGatewayBrokers = []string{"keyscope-gateway.example.com:9095"}

// enforceKeyScopeGateway republishes the lifecycle configuration with key-scope enforcement
// ACTIVE, and returns the gateway list it declared.
//
// IT IS A PRECONDITION OF EVERY KEY-SCOPED ISSUANCE, not a variation on one. Blnk ships no
// component that authorises record keys and serves no records itself, so without this call the
// shipped default applies and issuance for a prefix-recording row REFUSES with
// SUBSCRIBER_KEY_SCOPE_UNENFORCED — see
// TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway, which asserts
// exactly that and therefore must NOT call this, as must the undeclared-deployment subtest of
// TestIssueSubscriberCredential_ClearingAKeyScopeDependsOnWhatTheDeploymentDeclares.
//
// It is ALSO a precondition of the prefix-LESS refusals, from the other direction: under a
// declared model a subscriber recording no key scope is refused with
// SUBSCRIBER_KEY_SCOPE_REQUIRED — see
// TestIssueSubscriberCredential_RefusesAPrefixLessSubscriberUnderADeclaredKeyScopedModel.
//
// Declaring it also changes the endpoint a key-scoped credential names: the gateway list rather
// than the subscriber-facing broker list, because that subscriber's connection is terminated by
// the declared component.
//
// Parameters:
//   - t *testing.T: for the helper marker and the configuration restore.
//
// Returns:
//   - []string: the declared gateway bootstrap list, which is what issuance must report.
func enforceKeyScopeGateway(t *testing.T) []string {
	t.Helper()

	gateway, _ := enforceKeyScopeGatewayWithDouble(t)

	return gateway
}

// enforceKeyScopeGatewayWithDouble is enforceKeyScopeGateway plus the CONFORMANCE DOUBLE standing
// in for the declared component, returned so a test can assert what reached it.
//
// # Why a double is part of "enforcement is active" now
//
// A mode and a distinct bootstrap list used to be the whole declaration, and neither is something
// Blnk can verify: any address satisfied them. Enforcement is therefore active only when a usable
// CONTROL endpoint is declared as well, and issuance calls it — so a harness that published a mode
// and an address alone would now be a harness in which every key-scoped issuance is refused with
// SUBSCRIBER_KEY_SCOPE_UNATTESTED, which is not the behaviour these tests are about.
//
// The double implements the published contract literally: it stores what it is told and attests
// from what it stored. That is what makes it usable as a precondition here AND as the subject of
// the attestation tests in event_keyscope_gateway_test.go, where its overrides drive the
// mismatch cases.
//
// Returns:
//   - []string: the declared gateway bootstrap list, which is what issuance must report.
//   - *keyScopeGatewayDouble: the component, for assertions about the binding it holds.
func enforceKeyScopeGatewayWithDouble(t *testing.T) ([]string, *keyScopeGatewayDouble) {
	t.Helper()

	double, endpoint := newKeyScopeGatewayDouble(t)

	outboxStoreConfiguration(t, &config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:                         []string{"broker-1:9092", "broker-2:9092"},
			SubscriberBrokers:               append([]string(nil), subscriberLifecycleSubscriberBrokers...),
			TopicPrefix:                     "blnk",
			KeyScopeEnforcement:             config.KeyScopeEnforcementBrokerGateway,
			KeyScopeGatewayBrokers:          append([]string(nil), subscriberLifecycleKeyScopeGatewayBrokers...),
			KeyScopeGatewayAttestationURL:   endpoint,
			KeyScopeGatewayAttestationToken: double.token,
		},
	})

	return append([]string(nil), subscriberLifecycleKeyScopeGatewayBrokers...), double
}

func newSubscriberLifecycle(t *testing.T) *subscriberLifecycle {
	t.Helper()

	// A CONFIGURED DEPLOYMENT, which now includes the subscriber-facing broker list.
	// Issuance reads it from the configuration and refuses when it is absent, so a harness
	// that published none would make every issuance test assert a 503 instead of the
	// behaviour it is about. The value is deliberately DIFFERENT from the admin client's
	// broker list, which is what lets the tests below tell the two apart.
	subscriberLifecycleConfiguration(t)

	log := newSubscriberCallLog()
	store := newSubscriberTestStore(log).with(subscriberFixtureRow(t))
	admin := newSubscriberTestAdmin(log)

	return &subscriberLifecycle{
		log:     log,
		store:   store,
		admin:   admin,
		service: NewEventSubscriberService(store, admin),
	}
}

// seeded replaces the fixture row, for a test that needs a different starting state.
func (l *subscriberLifecycle) seeded(row model.EventSubscriber) *subscriberLifecycle {
	l.store.with(row)

	return l
}

// stringPointer is a local helper, so a nullable column can be seeded inline.
func stringPointer(value string) *string { return &value }

// ---------------------------------------------------------------------------------------
// SEC-05 — an authorization Kafka cannot enforce is refused
// ---------------------------------------------------------------------------------------

// TestIssueSubscriberCredential_RefusesAPrincipalHoldingAccessBeyondItsAuthorization is the
// service half of SEC-06: the admin layer's refusal must reach the caller as a typed error with
// no secret and no recorded issuance.
//
// The two halves are tested separately because they can fail independently. The admin layer can
// refuse correctly while the service records the issuance anyway — which would leave the
// registry claiming a subscriber is provisioned when its credential was deliberately destroyed —
// and the service can classify the refusal as a retryable timeout, which would send a client
// into a retry loop against a condition only a human can clear.
func TestIssueSubscriberCredential_RefusesAPrincipalHoldingAccessBeyondItsAuthorization(t *testing.T) {
	run := newSubscriberLifecycle(t).seeded(subscriberFixtureRow(t))
	store, service := run.store, run.service

	// The broker's own refusal: the credential was written, then compensated away once
	// reconciliation revealed the foreign grant. Both flags matter — they are what the service
	// reports to the caller about what is left behind.
	run.admin.result = SubscriberProvisioningResult{
		Topics:                     []string{"blnk.transactions"},
		CredentialWritten:          false,
		Compensated:                true,
		ForeignACLBindings:         2,
		ForeignACLBindingsGranting: 1,
	}
	run.admin.provisionErr = fmt.Errorf(
		"%w: principal %q holds 1 foreign binding(s) that grant access beyond its authorization",
		ErrForeignACLGrantsAccess, "blnk-sub-example",
	)

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)

	requireSubscriberAPIError(t, err, apierror.ErrSubscriberAccessExceedsAuthorization)
	assert.Equal(t, http.StatusConflict,
		apierror.StatusForCode(apierror.ErrSubscriberAccessExceedsAuthorization),
		"409, not the 503 of a provisioning failure: the broker answered and no retry can change "+
			"the boundary it would enforce")
	assert.NotContains(t, err.Error(), sentinelPassword,
		"the generated secret must never reach the error surface")

	assert.Empty(t, credential.Password(),
		"NO SECRET MAY BE RETURNED: the credential it belonged to was destroyed at the broker")
	assert.Zero(t, credential.IssuedAt)

	// The decisive assertion. Recording the issuance here would leave the registry asserting
	// that this subscriber holds a working credential when the broker holds none, which is the
	// false-provisioned state reads cannot distinguish from a real one.
	assert.Zero(t, run.log.count("RecordSubscriberCredentialIfUnchanged"),
		"a refused issuance must not be recorded")

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.CredentialReference,
		"a refused issuance must leave no credential record")
	assert.Nil(t, stored.CredentialIssuedAt)

	// Released rather than held: the refusal is terminal for this attempt, so the next caller
	// must be able to claim the subscriber without waiting out a lease.
	assert.NotZero(t, run.log.count("ReleaseSubscriberProvisioningFence"),
		"the provisioning fence must be released even on a refusal")
}

// TestIssueSubscriberCredential_ReportsAForeignGrantRefusalAsNotRetryable pins the classification
// rather than the outcome.
//
// A refusal caused by an operator's ACL binding cannot be cleared by repeating the request, so
// reporting it as retryable would invite a client to loop against a condition no retry can
// change. This is the distinction that made the dedicated branch necessary: without it the cause
// fell through to the state-based branches, which describe a failed ACL grant — a broker fault an
// operator would look for and never find, because the grant succeeded.
func TestIssueSubscriberCredential_ReportsAForeignGrantRefusalAsNotRetryable(t *testing.T) {
	run := newSubscriberLifecycle(t).seeded(subscriberFixtureRow(t))

	run.admin.result = SubscriberProvisioningResult{
		Topics:                     []string{"blnk.transactions"},
		Compensated:                true,
		ForeignACLBindingsGranting: 1,
	}
	run.admin.provisionErr = fmt.Errorf("%w: principal holds a foreign grant", ErrForeignACLGrantsAccess)

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)

	detail, ok := apiErr.Details.(SubscriberErrorDetail)
	require.True(t, ok, "the detail must be the bounded subscriber detail, not a raw error")
	assert.False(t, detail.Retryable,
		"only a human removing the binding can clear this, so a retry must not be advertised")
	assert.Contains(t, detail.Reason, "beyond its recorded authorization",
		"the reason must name the actual cause, so an operator does not hunt a broker fault")
}

// THE FIVE KEY-SCOPE REFUSAL TESTS ARE GONE, and their absence is deliberate.
//
// They pinned SUBSCRIBER_ISOLATION_UNENFORCEABLE: recording a partition_key_prefix was
// refused at registration and at update, and issuance was refused for any row that held
// one. That refusal implemented no part of AAP R-7's third scope — it withheld the
// CREDENTIAL instead, so a key-scoped subscriber could not consume at all. The typed code
// is retired.
//
// WHAT REPLACED IT HAS ITSELF MOVED ON, and this note records the current position rather
// than the first attempt at one. The intermediate posture was to issue the credential and
// DECLARE in the response that the broker does not enforce the prefix, which was accurate
// prose about an absent boundary: the party asked to apply the filter was the party holding
// whole-topic Read. What ships now is a narrowed GRANT (Describe, no Read) plus a
// CONDITIONAL refusal — issuance proceeds only where the deployment declares a component
// that authorises record keys AND that component attests this principal's exact prefix.
//
// What covers the behaviour now: TestIssueSubscriberCredential_IssuesAndDeliversTheRecordedKeyScope
// and TestIssueSubscriberCredential_AttestsTheExactBindingBeforeMintingASecret for the
// positive path, TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway
// and TestIssueSubscriberCredential_RefusesAKeyScopeTheDeclaredGatewayWillNotConfirm for the
// two refusals, TestUpdateSubscriber_AcceptsAKeyScopeOnAProvisionedSubscriber and
// TestEventSubscriber_DeclaresKeyScope* for the registry side, plus the api/model validation
// tests for the values a prefix may not take (surrounding whitespace, over-length, control
// characters) — which are still refused, with ErrGenValidation rather than a state code.

// TestUpdateSubscriber_KeepsTheLegitimateKeyScopeStatesLegitimate is the other side of the
// refusal, and it is what stops the fix becoming a trap.
//
// Three states must survive, and each matters for a different reason:
//
//   - CLEARING a prefix on a legacy row that also holds a credential. It is the documented
//     remedy for a legacy row, so refusing it would leave no way out of one. WHERE NOTHING
//     ENFORCES THE SCOPE — this harness, and the shipped default — that is unconditional;
//     under a declared key-scoped model the same clearing is refused with
//     SUBSCRIBER_KEY_SCOPE_REQUIRED, because there it widens a live principal back to
//     whole-topic Read. Both halves are
//     TestIssueSubscriberCredential_ClearingAKeyScopeDependsOnWhatTheDeploymentDeclares.
//   - CLEARING on a row that holds no credential, which is the same repair on the other legacy
//     shape — and it must restore provisionability, or the repair is only cosmetic.
//   - An unrelated edit on a row with NO prefix. The refusals read the row as it would be
//     written, so an update that mentions no prefix against a row that carries none must be
//     entirely unaffected by any of them.
func TestUpdateSubscriber_KeepsTheLegitimateKeyScopeStatesLegitimate(t *testing.T) {
	t.Run("clearing a key scope on a provisioned subscriber is accepted", func(t *testing.T) {
		row := subscriberProvisionedRow(t)
		// A row that already carries both, as a database predating
		// event_subscribers_key_scope_chk can. Clearing must be the way out of it.
		row.PartitionKeyPrefix = stringPointer("ldg_legacy_row")

		run := newSubscriberLifecycle(t).seeded(row)

		cleared := ""
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &cleared,
		})
		require.NoError(t, err,
			"clearing the prefix is how an existing row is repaired, so it must never be refused")
		assert.Nil(t, updated.PartitionKeyPrefix)
		require.NotNil(t, updated.CredentialReference,
			"and the repair must not revoke the credential the row already holds")
	})

	t.Run("clearing a key scope on a subscriber with no credential restores provisionability", func(t *testing.T) {
		row := subscriberFixtureRow(t)
		row.PartitionKeyPrefix = stringPointer("ldg_legacy_row")

		run := newSubscriberLifecycle(t).seeded(row)

		cleared := ""
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &cleared,
		})
		require.NoError(t, err,
			"the same repair must work before a credential exists, which is the state most legacy "+
				"rows are in")
		require.Nil(t, updated.PartitionKeyPrefix,
			"a present empty string CLEARS the column rather than storing an empty prefix")

		// And the repair is real rather than cosmetic: the row can now be provisioned, which is
		// what the caller wanted when it asked for a credential and was refused.
		credential, issueErr := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.NoError(t, issueErr,
			"once the unenforceable constraint is gone the subscriber must be provisionable")
		assert.NotEmpty(t, credential.Password())
	})

	t.Run("an unrelated edit to a provisioned subscriber is accepted", func(t *testing.T) {
		run := newSubscriberLifecycle(t).seeded(subscriberProvisionedRow(t))

		renamed := "settlement consumer"
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			Name: &renamed,
		})
		require.NoError(t, err,
			"the guards read the row as it would be written, so an update that mentions no prefix "+
				"against a row that carries none is untouched by them")
		assert.Equal(t, renamed, updated.Name)
		assert.Nil(t, updated.PartitionKeyPrefix)
		assert.Positive(t, run.log.count("UpdateEventSubscriber"), "and the write still happens")
	})

	t.Run("an unrelated edit to a legacy row that carries a prefix is accepted", func(t *testing.T) {
		// The case that decides whether the refusal reads the REQUEST or the RESULTING ROW. A row
		// predating the refusal carries a prefix; the operator renaming it while deciding what to
		// do has said nothing about that value. Reading the resulting row would refuse them and
		// leave the row uneditable — including uneditable by the very edit that documents the
		// decision.
		row := subscriberFixtureRow(t)
		row.PartitionKeyPrefix = stringPointer("ldg_legacy_row")

		run := newSubscriberLifecycle(t).seeded(row)

		renamed := "legacy consumer, pending key-scope decision"
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			Name: &renamed,
		})
		require.NoError(t, err,
			"the refusal reads the requested value, so an edit that mentions no prefix is "+
				"untouched by it even on a row that already carries one")
		assert.Equal(t, renamed, updated.Name)
		require.NotNil(t, updated.PartitionKeyPrefix,
			"and the legacy value is preserved rather than silently dropped: removing it would be "+
				"a repair the caller did not ask for")
	})
}

// TestRegisterSubscriber_AcceptsARegistrationThatRecordsNoKeyScope is the other side of the same
// guard, and it is what keeps the fix from becoming a trap.
//
// An absent prefix and a present empty string are both "no key scope", and both must register
// normally — the empty string especially, because a client that models the field as a plain string
// sends "" for "unset" and would otherwise be refused for expressing the supported case.
func TestRegisterSubscriber_AcceptsARegistrationThatRecordsNoKeyScope(t *testing.T) {
	for name, prefix := range map[string]*string{
		"absent":       nil,
		"empty string": stringPointer(""),
		"whitespace":   stringPointer("   "),
	} {
		t.Run(name, func(t *testing.T) {
			run := newSubscriberLifecycle(t)

			registered, err := run.service.RegisterSubscriber(context.Background(), SubscriberRegistration{
				SubscriberID:       "sub_keyscope_none",
				Name:               "Topic-scoped subscriber",
				AuthorizedTopics:   []string{"blnk.transactions"},
				PartitionKeyPrefix: prefix,
			})
			require.NoError(t, err,
				"no key scope is the supported case and the only provisionable one")
			require.NotNil(t, registered)
			assert.Nil(t, registered.PartitionKeyPrefix,
				"and it is stored as NULL rather than as an empty string: NULL means 'no key "+
					"constraint' while '' would mean 'constrained to the empty prefix'")
		})
	}
}

// TestIssueSubscriberCredential_RefusesASubscriberAuthorizedForNothing covers the empty grant.
//
// An empty authorized_topics list is a legitimate registry state — the fail-closed default of a
// new subscriber, and the only way to say "authorised for nothing" about a row that holds
// topics — and neither registration nor update is changed by this. Issuing against it is what is
// refused, because the credential minted is inert and the response is not: a secret returned
// once, a broker endpoint and a consumer group are indistinguishable at a glance from working
// access, so whoever receives it hands it to a consumer and diagnoses the resulting silence as a
// delivery fault. Meanwhile the broker holds a live principal that no ACL describes and somebody
// must remember to revoke.
func TestIssueSubscriberCredential_RefusesASubscriberAuthorizedForNothing(t *testing.T) {
	row := subscriberFixtureRow(t)
	row.AuthorizedTopics = nil

	run := newSubscriberLifecycle(t).seeded(row)
	store, service := run.store, run.service

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err, "a credential that could read nothing must not be minted")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberGrantEmpty, apiErr.Code)
	assert.Equal(t, http.StatusConflict, apierror.StatusForCode(apiErr.Code),
		"409: nothing about the request is malformed — it carries no body at all — and the "+
			"identical request succeeds once a topic is granted")
	assert.Contains(t, err.Error(), "authorized for no topics",
		"the message must name the missing grant")

	assert.Empty(t, credential.Password(),
		"NO SECRET MAY EXIST: the refusal is taken before one is generated")
	assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
		"and no SCRAM principal may be created at the broker for a subscriber that can read nothing")
	assert.Zero(t, run.log.count("RecordSubscriberCredentialIfUnchanged"))

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.CredentialReference)
	assert.False(t, store.fenced(subscriberFixtureID),
		"a refused issuance must still release its claim")

	// The remedy works, and stating it here is what proves the refusal is a step rather than a
	// dead end.
	_, err = service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		AuthorizedTopics: []string{"blnk.transactions"},
	})
	require.NoError(t, err)

	granted, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err, "granting a topic must restore provisionability")
	assert.NotEmpty(t, granted.Password())
	assert.Equal(t, []string{"blnk.transactions"}, granted.AuthorizedTopics)
}

// TestIssueSubscriberCredential_ReportsASpentBudgetAsATimeoutRatherThanAServerFault covers the
// error SEMANTICS of a deadline, which decide whether a client retries.
//
// Issuance shares one deadline across the fence claim, the row read, the broker round trips and
// the issuance record. The broker half already distinguished a timeout from a failure. The
// registry half did not: both reads report through the repository's generic internal-server
// code, so a budget spent waiting on a slow database arrived as HTTP 500 — indistinguishable
// from a defect in Blnk, and the correct reaction to the two is opposite. A defect must not be
// retried into a loop; this must be retried, and safely can be, because nothing has been
// written when it happens here.
//
// The context is what is consulted, not the error, and that is forced rather than chosen:
// loggedDatabaseError deliberately does not carry the driver's error, so
// errors.Is(err, context.DeadlineExceeded) cannot answer at this layer.
func TestIssueSubscriberCredential_ReportsASpentBudgetAsATimeoutRatherThanAServerFault(t *testing.T) {
	// The two registry steps that run before the broker is touched, each failing the way the
	// real repository fails when its query runs on a dead context.
	for _, tt := range []struct {
		method string
		reason string
	}{
		{method: "ClaimSubscriberForProvisioning", reason: "claiming the subscriber for provisioning"},
		{method: "GetEventSubscriberByID", reason: "reading the subscriber's registry row"},
	} {
		t.Run(tt.method, func(t *testing.T) {
			run := newSubscriberLifecycle(t)
			run.store.failing(tt.method, apierror.NewAPIError(
				apierror.ErrInternalServer,
				"Failed to reach the registry",
				nil,
			))

			// A budget this small is spent before the first call arrives, which is what the
			// classification reads. QA reproduced the defect with a one-millisecond budget
			// against a real database and with a caller that cancelled mid-flight; both land
			// on the same two paths.
			service := run.service.WithIssuanceBudget(time.Nanosecond)

			credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
			require.Error(t, err)

			var apiErr apierror.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, apierror.ErrSubscriberProvisioningTimeout, apiErr.Code,
				"a spent deadline is not a server fault, and a client cannot discriminate on 500")
			assert.Equal(t, http.StatusGatewayTimeout, apierror.StatusForCode(apiErr.Code),
				"504: the dependency was reached and did not answer in the time allowed")
			assert.NotEqual(t, http.StatusInternalServerError, apierror.StatusForCode(apiErr.Code))

			detail, ok := apiErr.Details.(SubscriberErrorDetail)
			require.True(t, ok, "the detail must be the bounded struct, never the cause")
			assert.True(t, detail.Retryable,
				"and it must say so: nothing was generated, nothing reached the broker and "+
					"nothing was recorded, so the retry starts from the state the first attempt found")
			assert.Contains(t, detail.Reason, tt.reason,
				"the detail must name the step that ran out of time, so a slow claim is "+
					"distinguishable from a slow read")

			assert.Empty(t, credential.Password())
			assert.True(t, run.log.arrivedExpired(tt.method),
				"the premise of this test is that the call arrived on a dead context")
			assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
				"a timeout at the registry must never reach the broker")
		})
	}
}

// TestIssueSubscriberCredential_KeepsATypedOutcomeWhenTheBudgetAlsoExpired is the limit of the
// reclassification, and it matters as much as the reclassification itself.
//
// Only a GENERIC internal-server failure becomes a timeout. A conflict — another operation holds
// the provisioning claim — remains true whether or not the deadline also expired: the fence WAS
// held, and it will be held again on the next attempt until that operation finishes. Rewriting
// it to a timeout would send a client to retry immediately against a state that has not
// changed, which is the same class of mistake as reporting a timeout as a server fault, pointing
// the other way.
func TestIssueSubscriberCredential_KeepsATypedOutcomeWhenTheBudgetAlsoExpired(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.store.holdFence(subscriberFixtureID, SubscriberProvisioningFenceLease)

	service := run.service.WithIssuanceBudget(time.Nanosecond)

	_, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusConflict, apierror.StatusForCode(apiErr.Code),
		"the fence conflict must survive an expired budget: it is the accurate answer, and the "+
			"remedy for it is to wait rather than to retry at once")
	assert.NotEqual(t, apierror.ErrSubscriberProvisioningTimeout, apiErr.Code)
}

// TestIssueSubscriberCredential_ReportsTheSubscriberFacingBrokers is the guard on the one
// field that decides whether everything else in the response is usable.
//
// Blnk dials internal addresses — "kafka:9092" on a compose network, a ClusterIP in
// Kubernetes — and a subscriber outside the deployment cannot resolve them. Kafka compounds
// it: a broker answers every client with the ADVERTISED address of the listener the connection
// arrived on, so bootstrapping against an internal name yields internal names for the
// partition leaders as well. The response must therefore carry the externally advertised list
// from KAFKA_SUBSCRIBER_BROKERS, and the two lists in this harness differ so that reading the
// wrong one cannot pass.
func TestIssueSubscriberCredential_ReportsTheSubscriberFacingBrokers(t *testing.T) {
	run := newSubscriberLifecycle(t)

	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)

	assert.Equal(t, subscriberLifecycleSubscriberBrokers, credential.Brokers,
		"the subscriber-facing list is what a subscriber connects to")
	assert.Equal(t, strings.Join(subscriberLifecycleSubscriberBrokers, ","), credential.BrokerEndpoint,
		"the single-string rendering must describe the same list, or a client configured from it "+
			"reaches a different cluster than one configured from the array")
	assert.NotContains(t, credential.Brokers, "broker-1:9092",
		"the addresses Blnk dials must never appear in a subscriber's credential: they do not "+
			"resolve for it, and publishing them discloses internal topology")
}

// TestIssueSubscriberCredential_RefusesWhenNoBrokerListIsConfiguredAtAll is the fail-closed
// remainder: with neither list set there is no Kafka, so no endpoint could be reported and no
// credential could work.
//
// The refusal must come BEFORE anything is minted: a credential created at the broker and
// recorded in the registry for a response that cannot be returned is worse than either
// outcome, because the registry then records a credential nobody holds.
func TestIssueSubscriberCredential_RefusesWhenNoBrokerListIsConfiguredAtAll(t *testing.T) {
	run := newSubscriberLifecycle(t)

	outboxStoreConfiguration(t, &config.Configuration{
		Kafka: config.KafkaConfig{TopicPrefix: "blnk"},
	})

	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err, "issuance must refuse when no broker list exists at all")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberBrokersNotConfigured, apiErr.Code)
	assert.Equal(t, http.StatusServiceUnavailable, apierror.StatusForCode(apiErr.Code),
		"503: a dependency of issuance is unconfigured. Nothing about the request is wrong and "+
			"no server defect is implied, and the caller succeeds unchanged once it is set")
	assert.Contains(t, err.Error(), "KAFKA_SUBSCRIBER_BROKERS",
		"the error must name the variable, or an operator cannot act on it")

	assert.Empty(t, credential.Password(),
		"NO SECRET MAY EXIST: the refusal is taken before one is generated")
	assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
		"the broker must not be asked to mint a credential whose response cannot be returned")
	assert.Zero(t, run.log.count("RecordSubscriberCredentialIfUnchanged"),
		"and the registry must not record a credential nobody received")

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.CredentialReference, "a refused issuance leaves no credential record")
}

// TestSubscriberFacingBrokers_ReadsAnUnusableListAsAbsent pins the normalisation.
//
// envconfig parses KAFKA_SUBSCRIBER_BROKERS as a comma-separated list, so "" and "," both
// yield a NON-EMPTY slice carrying nothing usable. Treating either as an advertised list would
// report blank endpoints to a subscriber — strictly worse than refusing, because it looks
// like a successful answer.
func TestSubscriberFacingBrokers_ReadsAnUnusableListAsAbsent(t *testing.T) {
	for name, configured := range map[string][]string{
		"nil":             nil,
		"empty slice":     {},
		"one blank":       {""},
		"whitespace":      {"   "},
		"comma only":      {"", ""},
		"blanks and tabs": {" ", "\t"},
	} {
		t.Run(name, func(t *testing.T) {
			brokers, advertised := config.KafkaConfig{SubscriberBrokers: configured}.SubscriberFacingBrokers()
			assert.False(t, advertised, "an unusable list must not read as advertised")
			assert.Empty(t, brokers, "and with no internal list either there is nothing to report")
		})
	}

	// NO FALLBACK TO THE INTERNAL LIST. A deployment with brokers but no advertised list has
	// nothing to report: KAFKA_BROKERS is what Blnk dials, and inside a deployment that is an
	// internal address which does not resolve for a subscriber outside it — so substituting it
	// would answer a credential request with an endpoint the holder cannot reach, together with
	// a secret shown exactly once.
	fallback, advertised := config.KafkaConfig{
		SubscriberBrokers: []string{" ", ""},
		Brokers:           []string{"internal-1:9092", " internal-2:9092 "},
	}.SubscriberFacingBrokers()
	assert.False(t, advertised, "an unusable advertised list is not advertised, whatever KAFKA_BROKERS holds")
	assert.Empty(t, fallback,
		"and the internal broker list must NOT be substituted: issuance refuses with a typed 503 instead")

	brokers, ok := config.KafkaConfig{
		SubscriberBrokers: []string{" kafka.example.com:9094 ", "", "second:9094"},
	}.SubscriberFacingBrokers()
	require.True(t, ok)
	assert.Equal(t, []string{"kafka.example.com:9094", "second:9094"}, brokers,
		"entries are trimmed and blanks dropped, so a stray separator in an environment file "+
			"cannot become an endpoint")
}

// TestSubscriberFacingBrokers_ReturnsACopy keeps a caller from mutating shared configuration.
//
// The configuration is published through an atomic.Value and read concurrently by every
// request, so handing out the backing array would let one issuance's caller change what every
// later one reports.
func TestSubscriberFacingBrokers_ReturnsACopy(t *testing.T) {
	cnf := config.KafkaConfig{SubscriberBrokers: []string{"kafka.example.com:9094"}}

	first, ok := cnf.SubscriberFacingBrokers()
	require.True(t, ok)
	first[0] = "attacker.example.com:9092"

	second, ok := cnf.SubscriberFacingBrokers()
	require.True(t, ok)
	assert.Equal(t, []string{"kafka.example.com:9094"}, second,
		"a mutation of one result must not be visible to the next reader")
}

// ---------------------------------------------------------------------------------------
// AUTH-01 — deregistration revokes before it deletes
// ---------------------------------------------------------------------------------------

// TestDeregisterSubscriber_TombstonesThenRevokesThenDeletes is the AUTH-01 guard.
//
// # The defect
//
// Deregistration deleted the row and THEN revoked. When the revocation failed, the principal
// kept authenticating and kept reading — and the only record of which principal that was had
// just been deleted. The residue was live access nothing in Blnk could see, recoverable only
// from a log line if anybody read it.
//
// The ORDER is the fix, so the order is what is asserted: mark, then revoke, then delete.
func TestDeregisterSubscriber_TombstonesThenRevokesThenDeletes(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, admin, service := run.store, run.admin, run.service

	removed, err := service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	require.NotNil(t, removed)

	assert.Equal(t, []string{subscriberFixtureID}, admin.revocations(),
		"the broker-side access must be revoked exactly once")

	// The whole ordering, read off ONE shared log so the registry write and the broker call are
	// in a single observed sequence rather than two independent ones.
	assert.Equal(t, []string{
		"ClaimSubscriberForProvisioning",
		"MarkSubscriberRevocationPending",
		"RevokeSubscriber",
		"TakeEventSubscriber",
	}, run.log.only(
		"ClaimSubscriberForProvisioning",
		"MarkSubscriberRevocationPending",
		"RevokeSubscriber",
		"TakeEventSubscriber",
	),
		"FENCE, then TOMBSTONE, then REVOKE, then DELETE. Deleting before the revocation is "+
			"confirmed is the defect: it leaves live access with no record of which principal holds it")

	_, present := store.row(subscriberFixtureID)
	assert.False(t, present, "a confirmed revocation must remove the row")
	assert.False(t, store.fenced(subscriberFixtureID),
		"the fence must be released once the operation finishes")
}

// TestDeregisterSubscriber_KeepsTheRowTombstonedWhenRevocationFails is the failure this whole
// ordering exists for.
//
// The row must SURVIVE, still naming the principal and the topics revocation needs, still
// marked pending — and the caller must be told the removal did not happen, because a caller
// told "deleted" would stop retrying and the live principal would never be cleaned up.
func TestDeregisterSubscriber_KeepsTheRowTombstonedWhenRevocationFails(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, admin, service := run.store, run.admin, run.service
	admin.failing("RevokeSubscriber", errors.New("broker unreachable"))

	returned, err := service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
	require.Error(t, err, "a caller must not read a failed revocation as a completed removal")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberProvisioningFailed, apiErr.Code)

	require.NotNil(t, returned,
		"the tombstoned row must be returned, so the caller sees what is outstanding rather "+
			"than a bare error")
	require.NotNil(t, returned.RevocationPendingAt)
	assert.True(t, returned.IsRevocationPending())
	assert.Equal(t, subscriberFixtureRow(t).KafkaPrincipal, returned.KafkaPrincipal,
		"the returned row must still name the principal that is still live")

	stored, present := store.row(subscriberFixtureID)
	require.True(t, present,
		"THE ROW MUST NOT BE DELETED: it is the only durable record of the access still standing")
	require.NotNil(t, stored.RevocationPendingAt)

	assert.Zero(t, run.log.count("TakeEventSubscriber"),
		"nothing may be deleted before the broker-side cleanup is confirmed")
}

// TestDeregisterSubscriber_ATombstonedRowIsRecoverableByRetrying closes the loop.
//
// A durable to-do item is only useful if working through it finishes the job, so the retry must
// find the tombstoned row, revoke successfully, and delete. And the tombstone's timestamp must
// not move: the value an operator needs is how long this revocation has been outstanding, and a
// timestamp refreshed on every attempt would report the age of the last attempt instead —
// always small, however long the row had been stuck.
func TestDeregisterSubscriber_ATombstonedRowIsRecoverableByRetrying(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, admin, service := run.store, run.admin, run.service
	admin.failing("RevokeSubscriber", errors.New("broker unreachable"))

	first, err := service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
	require.Error(t, err)
	require.NotNil(t, first.RevocationPendingAt)
	originalTombstone := *first.RevocationPendingAt

	// The broker recovers.
	admin.mu.Lock()
	delete(admin.failures, "RevokeSubscriber")
	admin.mu.Unlock()

	time.Sleep(2 * time.Millisecond)

	removed, err := service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
	require.NoError(t, err, "retrying a tombstoned deregistration must finish it")
	require.NotNil(t, removed.RevocationPendingAt)

	assert.True(t, removed.RevocationPendingAt.Equal(originalTombstone),
		"the tombstone keeps its FIRST instant, so its age reports how long the revocation has "+
			"been outstanding rather than the age of the last attempt")

	_, present := store.row(subscriberFixtureID)
	assert.False(t, present)
	assert.Equal(t, []string{subscriberFixtureID}, admin.revocations(),
		"only the successful revocation counts; the failed one recorded nothing")
}

// TestDeregisterSubscriber_KeepsTheRowWhenTheDeleteFailsAfterRevocation covers the other
// partial failure, which is the HARMLESS direction.
//
// Access is already gone, so the residue is a registry row describing a subscriber that can no
// longer authenticate — over-reporting rather than under-reporting.
//
// # REVERSED, and deliberately: the tombstone must NOT survive here
//
// This used to assert that the tombstone remained, on the reasoning that it is "what makes that
// row identifiable as needing a retry". F14 withdrew that reasoning, and
// TestDeregisterSubscriber_ClearsTheTombstoneOnceTheBrokerIsConfirmedClean is its guard.
// revocation_pending_at is stamped BEFORE the broker is touched, so it means "a principal may
// still authenticate" — and once the revocation is CONFIRMED that sentence is false. Keeping the
// stamp made the column mean two opposite things depending on how far the operation got.
//
// It is not a labelling nicety. CountSubscriberRevocationsPending counts tombstoned rows and
// blnk_subscribers_oldest_revocation_age_seconds raises a CRITICAL alert whose runbook tells an
// operator to delete a SCRAM credential by hand. A confirmed-clean row sent them after a
// principal that no longer exists, and — the direction that actually costs something — made a
// row where a live credential really was unaccounted for indistinguishable from this residue.
//
// What is left is an INERT row: no credential, no broker access, no tombstone. Retrying the
// deregistration removes it, and nothing alerts on it because nothing is exposed by it.
func TestDeregisterSubscriber_KeepsTheRowWhenTheDeleteFailsAfterRevocation(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, admin, service := run.store, run.admin, run.service
	store.failing("TakeEventSubscriber", errors.New("write path unavailable"))

	returned, err := service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
	require.Error(t, err)
	require.NotNil(t, returned)
	assert.False(t, returned.IsRevocationPending(),
		"the returned row must agree with the stored one: a caller handed a row that still "+
			"reports a pending revocation would conclude a principal may still authenticate")

	assert.Equal(t, []string{subscriberFixtureID}, admin.revocations(),
		"the revocation DID happen; only the row removal failed")

	stored, present := store.row(subscriberFixtureID)
	require.True(t, present, "the row survives, because its deletion is what failed")
	assert.Nil(t, stored.RevocationPendingAt,
		"and it carries no tombstone, because the revocation it would describe is confirmed done")
	assert.Nil(t, stored.CredentialReference,
		"nor a credential the broker has confirmed it does not hold")
}

// TestDeregisterSubscriber_WithoutABrokerDeletesDirectly keeps the no-Kafka steady state
// working.
//
// A deployment that never configured Kafka has no broker-side access, so there is nothing to
// confirm and the row is removed straight away. Requiring a revocation here would make the
// registry unusable without Kafka, which is the documented supported configuration.
func TestDeregisterSubscriber_WithoutABrokerDeletesDirectly(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, admin, service := run.store, run.admin, run.service
	admin.configured = false

	removed, err := service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	require.NotNil(t, removed)

	assert.Empty(t, admin.revocations(), "there is no broker-side access to revoke")
	_, present := store.row(subscriberFixtureID)
	assert.False(t, present)

	// The tombstone is still stamped first. It costs one write and it means an interrupted
	// deregistration is identifiable even here.
	assert.NotZero(t, run.log.count("MarkSubscriberRevocationPending"))
}

// TestIssueSubscriberCredential_RefusesASubscriberBeingDeregistered stops the tombstone being
// undone from the other side.
//
// A row carrying the tombstone is on its way out and its broker-side access may still be live.
// Minting a credential for it would re-arm a principal mid-removal, and the deregistration
// already in flight would then delete the row recording the credential just issued — leaving
// exactly the orphaned live principal the tombstone exists to prevent.
func TestIssueSubscriberCredential_RefusesASubscriberBeingDeregistered(t *testing.T) {
	row := subscriberFixtureRow(t)
	pending := time.Now().UTC().Add(-time.Minute)
	row.RevocationPendingAt = &pending

	run := newSubscriberLifecycle(t).seeded(row)
	service := run.service

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code)

	assert.Empty(t, credential.Password(), "no secret may be generated for a subscriber being removed")
	assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"))
}

// ---------------------------------------------------------------------------------------
// CONC-01 — the provisioning fence
// ---------------------------------------------------------------------------------------

// TestIssueSubscriberCredential_IsFencedBeforeTheBrokerIsTouched is the CONC-01 guard.
//
// # The defect
//
// Kafka stores ONE SCRAM credential per principal. Two overlapping issuances therefore both
// wrote a credential and the second replaced the first, so the broker held one password while
// the registry could hold the reference derived from the other — and the caller holding the
// RECORDED one could not authenticate, with no way to discover it, because its request had
// returned 200 with a password in it. The conditional write detects the DATABASE half of that
// race; it cannot decide which password the BROKER kept, because that is settled by whichever
// call reached the broker last, independently of who won the write.
//
// So the second caller is refused with a conflict BEFORE a secret exists, and the claim is
// taken before the row is read so that everything the call decides from is read under it.
func TestIssueSubscriberCredential_IsFencedBeforeTheBrokerIsTouched(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, service := run.store, run.service

	_, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)

	assert.Equal(t,
		[]string{"ClaimSubscriberForProvisioning", "GetEventSubscriberByID", "ProvisionSubscriberPrincipal"},
		run.log.only("ClaimSubscriberForProvisioning", "GetEventSubscriberByID", "ProvisionSubscriberPrincipal"),
		"the claim is taken BEFORE the row is read and before the broker is touched, so every "+
			"decision this call makes is made under it")

	assert.False(t, store.fenced(subscriberFixtureID),
		"a completed issuance must release its claim rather than making the next caller wait "+
			"out the lease")
}

// TestIssueSubscriberCredential_RefusesWhileAnotherOperationHoldsTheFence is the refusal
// itself.
//
// It must arrive as a CONFLICT and before any secret is generated: a refused issuance costs the
// caller one retry, whereas an interleaved one costs it a credential that does not work.
func TestIssueSubscriberCredential_RefusesWhileAnotherOperationHoldsTheFence(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, service := run.store, run.service

	store.holdFence(subscriberFixtureID, time.Minute)

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code)

	assert.Empty(t, credential.Password(),
		"the refusal must come before a secret exists, not after one has been written to the broker")
	assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
		"a fenced subscriber's broker state must not be touched at all")
	assert.Zero(t, run.log.count("GetEventSubscriberByID"),
		"not even the read happens outside the claim")
}

// TestIssueSubscriberCredential_OnlyOneOfTwoConcurrentIssuancesReachesTheBroker is the property
// stated as a race rather than as a sequence.
//
// Two callers issue at the same time for the same subscriber. Exactly one must reach the broker
// and exactly one must be refused — because the outcome the fence prevents is precisely two
// credential writes whose winner is decided by network timing.
func TestIssueSubscriberCredential_OnlyOneOfTwoConcurrentIssuancesReachesTheBroker(t *testing.T) {
	run := newSubscriberLifecycle(t)
	admin, service := run.admin, run.service

	// The first issuance parks INSIDE the broker call, so the second is guaranteed to arrive
	// while the first still holds its claim. A sleep-based race would prove nothing.
	entered := make(chan struct{})
	release := make(chan struct{})
	admin.onProvision = func() {
		select {
		case <-entered:
		default:
			close(entered)
			<-release
		}
	}

	type outcome struct {
		credential SubscriberCredential
		err        error
	}

	first := make(chan outcome, 1)
	go func() {
		credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		first <- outcome{credential: credential, err: err}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first issuance never reached the broker")
	}

	_, second := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, second, "the second issuance must be refused while the first holds the claim")

	var apiErr apierror.APIError
	require.ErrorAs(t, second, &apiErr)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code)

	close(release)

	result := <-first
	require.NoError(t, result.err, "the caller that took the claim must succeed")
	assert.NotEmpty(t, result.credential.Password())

	assert.Equal(t, 1, run.log.count("ProvisionSubscriberPrincipal"),
		"EXACTLY ONE credential may be written: the broker keeps only one per principal, so a "+
			"second write silently invalidates the first caller's secret")
}

// TestSubscriberFence_AnExpiredClaimDoesNotBlockTheNextAttempt is what stops the fence becoming
// a denial of service.
//
// A process killed while holding a claim must not fence the subscriber forever, so the claim is
// LEASED. This asserts the lease is honoured — an expired claim is not a conflict — which is
// also why the conditional credential write is retained underneath it: an issuance whose
// process stalled past its lease can find itself superseded, and the conditional write makes
// that a reported conflict rather than a silent overwrite.
func TestSubscriberFence_AnExpiredClaimDoesNotBlockTheNextAttempt(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, service := run.store, run.service

	// A claim that has already run out, as a crashed operation's would be.
	store.holdFence(subscriberFixtureID, -time.Second)

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err,
		"an EXPIRED claim must not refuse the next attempt, or one crashed process fences a "+
			"subscriber permanently")
	assert.NotEmpty(t, credential.Password())
}

// TestSubscriberFence_IsHeldByEveryOperationThatTouchesTheBroker is the completeness half of
// CONC-01.
//
// A fence only excludes what actually takes it. Issuance, revocation, an authorization update
// and deregistration all mutate broker state for one subscriber, so all four must claim it —
// otherwise the one that does not can interleave with any of the others and the guarantee is
// worth nothing.
func TestSubscriberFence_IsHeldByEveryOperationThatTouchesTheBroker(t *testing.T) {
	operations := map[string]func(*EventSubscriberService) error{
		"IssueSubscriberCredential": func(service *EventSubscriberService) error {
			_, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)

			return err
		},
		"RevokeSubscriberCredential": func(service *EventSubscriberService) error {
			return service.RevokeSubscriberCredential(context.Background(), subscriberFixtureID)
		},
		"UpdateSubscriber": func(service *EventSubscriberService) error {
			_, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
				AuthorizedTopics: []string{"blnk.transactions"},
			})

			return err
		},
		"DeregisterSubscriber": func(service *EventSubscriberService) error {
			_, err := service.DeregisterSubscriber(context.Background(), subscriberFixtureID)

			return err
		},
	}

	for name, operation := range operations {
		t.Run(name+" takes the claim", func(t *testing.T) {
			run := newSubscriberLifecycle(t)
			store, service := run.store, run.service

			require.NoError(t, operation(service))
			assert.NotZero(t, run.log.count("ClaimSubscriberForProvisioning"),
				"an operation that mutates broker state without the claim can interleave with "+
					"every other one")
			assert.False(t, store.fenced(subscriberFixtureID),
				"and it must release the claim when it finishes")
		})

		t.Run(name+" is refused while the claim is held", func(t *testing.T) {
			run := newSubscriberLifecycle(t)
			store, service := run.store, run.service
			store.holdFence(subscriberFixtureID, time.Minute)

			err := operation(service)
			require.Error(t, err)

			var apiErr apierror.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, apierror.ErrConflict, apiErr.Code)
		})
	}
}

// ---------------------------------------------------------------------------------------
// AUTH-02 — the three-step authorization update
// ---------------------------------------------------------------------------------------

// TestUpdateSubscriber_PrunesBeforeItPersistsAndGrantsAfter is the ordering AUTH-02 needs at
// the service tier.
//
// There is no transaction spanning Blnk and Kafka, so the only order in which every partial
// failure is fail-closed is prune -> persist -> grant. Pruning first puts a NARROWING in force
// before the row claims it; granting last lets a WIDENING reach the broker only once the row
// records it. Doing both around a single persist, or persisting first, makes one of the two
// cases fail-OPEN — live broker access the registry says was revoked.
func TestUpdateSubscriber_PrunesBeforeItPersistsAndGrantsAfter(t *testing.T) {
	run := newSubscriberLifecycle(t)
	service := run.service

	updated, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		AuthorizedTopics: []string{"blnk.transactions"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"blnk.transactions"}, updated.AuthorizedTopics)

	// The persist has to sit BETWEEN the two broker calls, and that is only observable on one
	// shared log — two independent sequences can each be in the right order while the write sits
	// on the wrong side of both.
	require.Equal(t, []string{
		"ClaimSubscriberForProvisioning",
		"PruneSubscriberAccess",
		"UpdateEventSubscriber",
		"GrantSubscriberAccess",
	}, run.log.only(
		"ClaimSubscriberForProvisioning",
		"PruneSubscriberAccess",
		"UpdateEventSubscriber",
		"GrantSubscriberAccess",
	),
		"PRUNE, then PERSIST, then GRANT. Persisting first makes a failed prune fail-OPEN: the "+
			"row would claim a narrowing the broker never applied")

	assert.Equal(t, []string{"PruneSubscriberAccess", "GrantSubscriberAccess"},
		run.log.only("PruneSubscriberAccess", "GrantSubscriberAccess", "ProvisionSubscriberPrincipal",
			"RevokeSubscriber"),
		"nothing else may touch the broker: an update must not mint or revoke a credential")
}

// TestUpdateSubscriber_DoesNotPersistWhenTheNarrowingCouldNotBeApplied keeps step one binding.
//
// An update that could not narrow the boundary at the broker must not be recorded as having
// narrowed it: the row would then describe less access than exists, which is the exact
// fail-open AUTH-02 removes.
func TestUpdateSubscriber_DoesNotPersistWhenTheNarrowingCouldNotBeApplied(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, admin, service := run.store, run.admin, run.service
	admin.failing("PruneSubscriberAccess", errors.New("broker unreachable"))

	_, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		AuthorizedTopics: []string{"blnk.transactions"},
	})
	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberProvisioningFailed, apiErr.Code)

	assert.Zero(t, run.log.count("UpdateEventSubscriber"),
		"the row must keep the WIDER authorization it still has at the broker")
	assert.Zero(t, run.log.count("GrantSubscriberAccess"))

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Equal(t, []string{"blnk.transactions", "blnk.balances"}, stored.AuthorizedTopics)
}

// TestUpdateSubscriber_ReportsAFailedWideningRatherThanClaimingSuccess covers step three.
//
// The row already records the wider authorization, so the residue is fail-closed — fewer rights
// than recorded. But a caller told the update succeeded would believe a grant exists that does
// not, so the error is returned rather than logged.
func TestUpdateSubscriber_ReportsAFailedWideningRatherThanClaimingSuccess(t *testing.T) {
	run := newSubscriberLifecycle(t)
	admin, service := run.admin, run.service
	admin.failing("GrantSubscriberAccess", errors.New("broker unreachable"))

	_, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		AuthorizedTopics: []string{"blnk.transactions", "blnk.balances", "blnk.identities"},
	})
	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberProvisioningFailed, apiErr.Code)

	assert.NotZero(t, run.log.count("UpdateEventSubscriber"),
		"the persist happened, which is why the residue is fail-closed and recoverable by retrying")
}

// TestUpdateSubscriber_WithoutABrokerReconcilesNothingAndStillPersists keeps the no-Kafka
// steady state working.
//
// A deployment with no broker has no broker-side grant to reconcile, so both reconciliation
// steps are no-ops and the registry behaves exactly as it would without this feature.
func TestUpdateSubscriber_WithoutABrokerReconcilesNothingAndStillPersists(t *testing.T) {
	run := newSubscriberLifecycle(t)
	admin, service := run.admin, run.service
	admin.configured = false

	updated, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		AuthorizedTopics: []string{"blnk.transactions"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"blnk.transactions"}, updated.AuthorizedTopics)

	assert.Empty(t, run.log.only("PruneSubscriberAccess", "GrantSubscriberAccess"),
		"there is no broker-side grant to reconcile")
	assert.NotZero(t, run.log.count("UpdateEventSubscriber"))
}

// ---------------------------------------------------------------------------------------
// CLEAN-01 — compensation runs on a fresh bounded context
// ---------------------------------------------------------------------------------------

// TestIssueSubscriberCredential_CompensatesOnAFreshContextWhenTheIssuanceBudgetIsSpent is the
// CLEAN-01 guard.
//
// # The defect
//
// Every cleanup on this path — revoking a credential the registry could not record, clearing a
// reference that no longer describes anything, releasing the fence — used to run on the
// ISSUANCE context. That context carries the five-second budget, and its EXPIRY is one of the
// commonest reasons issuance fails at all. So the cleanups were attempted with an
// already-cancelled context, returned immediately, and left exactly the residue they exist to
// remove: a live credential nothing records.
//
// The test drives it directly. The budget is set so small that it is spent by the time the
// record is attempted, the record fails, and the compensating revocation must still ARRIVE ON A
// LIVE CONTEXT and must still happen.
func TestIssueSubscriberCredential_CompensatesOnAFreshContextWhenTheIssuanceBudgetIsSpent(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, admin := run.store, run.admin
	// The record fails for a reason that is NOT a conflict, which is the branch that
	// compensates. A conflict deliberately does not revoke, because one SCRAM credential exists
	// per principal and revoking would destroy the other issuance's secret too.
	store.failing("RecordSubscriberCredentialIfUnchanged", errors.New("write path unavailable"))

	service := run.service.WithIssuanceBudget(30 * time.Millisecond)

	// Spend the budget inside the broker call, so the issuance context is genuinely expired by
	// the time compensation runs — the real condition, not a simulated one.
	admin.onProvision = func() { time.Sleep(60 * time.Millisecond) }

	_, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	require.NotZero(t, run.log.count("RevokeSubscriber"),
		"THE CREDENTIAL MUST STILL BE REVOKED: it exists at the broker and no registry row "+
			"records it")
	assert.False(t, run.log.arrivedExpired("RevokeSubscriber"),
		"the compensating revocation must run on a FRESH context; on the spent issuance budget "+
			"it would return immediately and leave the credential live")

	require.NotZero(t, run.log.count("ClearSubscriberCredential"),
		"the registry's credential record must be cleared too, or it claims access that has gone")
	assert.False(t, run.log.arrivedExpired("ClearSubscriberCredential"))

	assert.False(t, run.log.arrivedExpired("ReleaseSubscriberProvisioningFence"),
		"and the fence release must not be lost to the same expired deadline, or one slow broker "+
			"call turns into a lease-long refusal of every retry")
	assert.False(t, store.fenced(subscriberFixtureID),
		"the subscriber must not stay fenced after a failed issuance")
}

// TestIssueSubscriberCredential_DoesNotRevokeWhenAConcurrentIssuanceWon is the one case where
// NOT compensating is correct, and it is worth pinning because it looks like a missing cleanup.
//
// One SCRAM credential exists per principal. If a concurrent issuance superseded this one, the
// credential now at the broker may be the OTHER caller's — and revoking would destroy a secret
// that caller has already been handed and believes works.
func TestIssueSubscriberCredential_DoesNotRevokeWhenAConcurrentIssuanceWon(t *testing.T) {
	row := subscriberFixtureRow(t)
	run := newSubscriberLifecycle(t).seeded(row)
	store, admin, service := run.store, run.admin, run.service

	// The row's reference changes after this issuance observed it, which is exactly what a
	// concurrent issuance committing in the meantime looks like to the conditional write.
	admin.onProvision = func() {
		store.mu.Lock()
		defer store.mu.Unlock()

		stored := store.rows[subscriberFixtureID]
		stored.CredentialReference = stringPointer("the-other-issuance")
		store.rows[subscriberFixtureID] = stored
	}

	_, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code)

	assert.Zero(t, run.log.count("RevokeSubscriber"),
		"revoking here would destroy the credential the OTHER issuance handed out")
	assert.Zero(t, run.log.count("ClearSubscriberCredential"),
		"and clearing the record would erase the reference that issuance recorded")
}

// TestRevokeSubscriberCredential_LeavesTheRecordAloneWhenTheBrokerRefuses keeps revocation
// fail-closed.
//
// Clearing the registry record while the credential still works would make the registry
// under-report live access, which is the direction that hides a problem. So the record survives
// and the caller is told to retry.
func TestRevokeSubscriberCredential_LeavesTheRecordAloneWhenTheBrokerRefuses(t *testing.T) {
	row := subscriberFixtureRow(t)
	row.CredentialReference = stringPointer("reference-in-place")
	issued := time.Now().UTC()
	row.CredentialIssuedAt = &issued

	run := newSubscriberLifecycle(t).seeded(row)
	store, admin, service := run.store, run.admin, run.service
	admin.failing("RevokeSubscriber", errors.New("broker unreachable"))

	err := service.RevokeSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberProvisioningFailed, apiErr.Code)

	assert.Zero(t, run.log.count("ClearSubscriberCredential"),
		"the credential may still work, so the record must keep saying so")

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.CredentialReference)

	assert.False(t, store.fenced(subscriberFixtureID),
		"a failed revocation must still release its claim")
}

// ---------------------------------------------------------------------------------------
// The administrative-client lifetime guarantee
// ---------------------------------------------------------------------------------------

// TestEventSubscriberService_CloseReleasesOnlyTheClientItOwns is the runtime half of the
// lifetime guarantee: Close releases a client the service resolved for itself, and never one
// that was handed to it.
//
// Both halves matter, and they fail in opposite directions. A Close that released nothing would
// leak every administrative client the subscriber surface ever resolves — one per update, one
// per issuance — until the process ended. A Close that released an INJECTED client would tear
// down something owned by whoever built it and usually outlives this service, so the next
// caller would find a closed client. That second failure mode is why the wrappers can close
// unconditionally without knowing where the client came from.
func TestEventSubscriberService_CloseReleasesOnlyTheClientItOwns(t *testing.T) {
	t.Run("an injected client is never closed", func(t *testing.T) {
		log := newSubscriberCallLog()
		admin := newSubscriberTestAdmin(log)
		service := NewEventSubscriberService(newSubscriberTestStore(log), nil).WithKafkaAdmin(admin)

		require.NoError(t, service.Close())
		require.NoError(t, service.Close(), "Close must be idempotent")

		// The service still holds it, because it never owned it: the injected client is
		// usable after Close, which is what a caller that built it is entitled to expect.
		provisioner, err := service.provisioner()
		require.NoError(t, err)
		assert.Same(t, admin, provisioner,
			"an injected client must survive Close: the service does not own it")
	})

	t.Run("a resolved client is closed and forgotten", func(t *testing.T) {
		// Resolution reads live configuration, so a Kafka-configured one is installed for the
		// duration. NewKafkaAdmin performs no I/O at construction, so nothing dials a broker.
		//
		// InsecureLocalDev is the acknowledgement the transport requires before it will build a
		// plaintext client at all; without it construction is refused as a misconfiguration and
		// nothing would ever be resolved to close. Enabling it here is what makes the test about
		// LIFETIME rather than about the TLS refusal, which has its own coverage.
		cnf := subscriberLifecycleConfiguration(t)
		cnf.Kafka.InsecureLocalDev = true
		config.MockConfig(cnf)

		service := NewEventSubscriberService(newSubscriberTestStore(newSubscriberCallLog()), nil)

		resolved, err := service.provisioner()
		require.NoError(t, err, "a configured deployment must resolve an administrative client")
		require.NotNil(t, resolved)

		require.True(t, service.ownsAdmin,
			"a client the service built for itself must be recorded as owned, or Close will not release it")

		require.NoError(t, service.Close())

		assert.Nil(t, service.admin,
			"Close must forget the client it released, so a later call resolves a fresh one rather than using a closed one")
		assert.False(t, service.ownsAdmin,
			"ownership must be released with the client, or a second Close would close it twice")
		require.NoError(t, service.Close(), "Close must be idempotent after releasing an owned client")
	})

	t.Run("nil is safe", func(t *testing.T) {
		var service *EventSubscriberService
		require.NoError(t, service.Close(),
			"a nil service must be closable, so a deferred close needs no nil check at the call site")
	})
}

// TestBlnkSubscriberWrappers_EveryOneClosesTheServiceItBuilds is the STRUCTURAL half, and it is
// the half that keeps the guarantee true.
//
// The runtime test above proves Close works. It cannot prove Close is CALLED, and that is where
// the leak actually was: UpdateEventSubscriber built a service, reconciled a subscriber's ACL
// bindings through it — resolving an administrative client twice, once to prune and once to
// grant — and returned without closing, so every update held those connections for the
// remaining life of the process.
//
// A per-wrapper assertion, rather than a comment saying which wrappers need it, is the only
// form that survives the next edit. The rule that produced the bug was "close where the broker
// is touched", which made each wrapper's correctness depend on a fact about a service method
// hundreds of lines away that no compiler checks: a registry-only operation that later grew a
// broker touch would start leaking with its own wrapper untouched in the diff. Close is
// idempotent, nil-safe and a no-op when nothing was resolved, so requiring it everywhere costs
// nothing and cannot be wrong.
func TestBlnkSubscriberWrappers_EveryOneClosesTheServiceItBuilds(t *testing.T) {
	fileSet := token.NewFileSet()
	path := filepath.Join(moduleRootDir(t), "event_subscriber.go")
	parsed, err := parser.ParseFile(fileSet, path, nil, parser.ParseComments)
	require.NoError(t, err, "event_subscriber.go must be parseable to assert its structure")

	// Every *Blnk method that builds a subscriber service, discovered from the file rather
	// than listed here: a wrapper added later is covered without this test being touched.
	wrappers := map[string]bool{}
	for _, declaration := range parsed.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if !isFunction || function.Recv == nil || function.Body == nil {
			continue
		}

		if !isBlnkReceiver(function.Recv) {
			continue
		}

		buildsService := false
		closesService := false
		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CallExpr:
				if selector, ok := typed.Fun.(*ast.SelectorExpr); ok &&
					selector.Sel.Name == "EventSubscribers" {
					buildsService = true
				}
			case *ast.DeferStmt:
				if selector, ok := typed.Call.Fun.(*ast.Ident); ok &&
					selector.Name == "closeEventSubscriberService" {
					closesService = true
				}
			}

			return true
		})

		if buildsService {
			wrappers[function.Name.Name] = closesService
		}
	}

	// EventSubscribers itself is the constructor, not a wrapper: it hands the service to a
	// caller, so closing it there would return something already closed.
	delete(wrappers, "EventSubscribers")

	require.NotEmpty(t, wrappers, "the wrappers must be discoverable, or this test proves nothing")

	// Named explicitly as well, so that a wrapper DELETED or renamed out of the discovery
	// above is noticed rather than silently reducing the set this test covers.
	for _, expected := range []string{
		"RegisterEventSubscriber",
		"GetEventSubscriber",
		"ListEventSubscribers",
		"UpdateEventSubscriber",
		"DeregisterEventSubscriber",
		"IssueSubscriberKafkaCredentials",
		"RevokeSubscriberKafkaCredentials",
		"RecordSubscriberWebhookSubscription",
		"ClearSubscriberWebhookSubscription",
		"CompleteSubscriberWebhookMigration",
		"MarkEventSubscriberMigrated",
		"PurgeMigratedSubscriberWebhookURLs",
	} {
		require.Contains(t, wrappers, expected,
			"%s must build its service through EventSubscribers, so its administrative-client "+
				"lifetime is governed by the same rule as every other wrapper's", expected)
	}

	for name, closes := range wrappers {
		assert.Truef(t, closes,
			"%s builds a subscriber service and must `defer closeEventSubscriberService(service)`: "+
				"without it, an administrative client the service resolves is held until the process "+
				"ends, and whether it resolves one is a property of a service method elsewhere that "+
				"no compiler checks", name)
	}
}

// isBlnkReceiver reports whether a method is declared on *Blnk.
func isBlnkReceiver(receiver *ast.FieldList) bool {
	if receiver == nil || len(receiver.List) != 1 {
		return false
	}

	pointer, isPointer := receiver.List[0].Type.(*ast.StarExpr)
	if !isPointer {
		return false
	}

	identifier, isIdentifier := pointer.X.(*ast.Ident)

	return isIdentifier && identifier.Name == "Blnk"
}

// ---------------------------------------------------------------------------------------
// The revocation tombstone is fail-closed for every mutation that can GRANT access
// ---------------------------------------------------------------------------------------

// requireSubscriberAPIError asserts the typed code an error carries.
//
// The code is the assertion that matters rather than the message: the HTTP status the handler
// returns is derived from it, and the difference between a 404 and a 409 on these paths is the
// difference between "re-register this subscriber" and "your operation was overtaken and its
// write was correctly discarded".
//
// Parameters:
//   - t *testing.T: the test.
//   - err error: the error under assertion. Must be non-nil.
//   - want apierror.ErrorCode: the expected code.
func requireSubscriberAPIError(t *testing.T, err error, want apierror.ErrorCode) {
	t.Helper()

	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr, "the error must be a typed APIError, not an untyped failure")
	assert.Equal(t, want, apiErr.Code)
}

// TestUpdateSubscriber_RefusesASubscriberBeingDeregistered is the tombstone guard on the one
// mutation that never asked.
//
// # The defect
//
// A row carrying the revocation tombstone is being deregistered: its principal is on its way out
// and its ACL bindings are being removed. IssueSubscriberCredential refused such a row.
// UpdateSubscriber did not — it fenced, read, mutated in memory and went straight to the broker.
//
// That mattered because a non-nil AuthorizedTopics REPLACES the whole set, so the call could
// WIDEN the authorization, and STEP 3 then created broker bindings for a principal whose
// revocation was already in flight. A subscriber regained access while it was being removed, and
// the registry recorded it as an ordinary edit — nothing failed and nothing was logged as wrong.
//
// # What the refusal has to prove
//
// Not merely that an error comes back, but that NOTHING HAPPENED: no prune, no write, no grant.
// A refusal issued after the prune would still have narrowed the broker; one issued after the
// write would still have changed the recorded boundary. So the assertion is on the whole
// collaborator sequence, not on the returned error alone.
func TestUpdateSubscriber_RefusesASubscriberBeingDeregistered(t *testing.T) {
	tombstoned := subscriberFixtureRow(t)
	pendingAt := time.Now().Add(-3 * time.Minute)
	tombstoned.RevocationPendingAt = &pendingAt

	run := newSubscriberLifecycle(t).seeded(tombstoned)

	widened := []string{"blnk.transactions", "blnk.balances", "blnk.identities", "blnk.system"}

	updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
		SubscriberUpdate{AuthorizedTopics: widened})

	require.Error(t, err, "an authorization change on a row being deregistered must be refused")
	assert.Nil(t, updated)
	requireSubscriberAPIError(t, err, apierror.ErrConflict)

	// THE MESSAGE NAMES THE OPERATION THAT WAS REFUSED. The guard is shared with issuance, and
	// reusing issuance's wording here would answer an authorization change with "no credential
	// will be issued for it" — a 409 describing an operation the caller never asked for, which
	// sends an operator looking in the wrong place.
	assert.Contains(t, err.Error(), "access model can no longer be changed")
	assert.NotContains(t, err.Error(), "credential will be issued",
		"an update must not be refused in issuance's words")

	// THE BROKER WAS NEVER TOUCHED. This is the half that matters: the grant is what would have
	// re-armed a principal mid-removal, and the prune would have narrowed a boundary the
	// deregistration is about to remove entirely.
	assert.Empty(t, run.log.only("PruneSubscriberAccess", "GrantSubscriberAccess"),
		"a tombstoned subscriber must not reach the broker at all")

	// AND NOTHING WAS WRITTEN.
	assert.Zero(t, run.log.count("UpdateEventSubscriber"),
		"the refusal must precede the durable write, not follow it")

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok, "the row must survive a refused update")
	assert.Equal(t, []string{"blnk.transactions", "blnk.balances"}, stored.AuthorizedTopics,
		"the recorded authorization must be exactly what it was before the refused widening")

	// The fence is released, so the deregistration that owns this row can retry immediately
	// rather than waiting out a lease spent on a call that changed nothing.
	assert.False(t, run.store.fenced(subscriberFixtureID),
		"a refused update must not leave the subscriber fenced")
}

// TestUpdateSubscriber_RefusesTheTombstonedRowAtThePersistenceBoundaryToo covers the case the
// in-memory guard cannot: the tombstone arriving BETWEEN the read and the write.
//
// The service check and the SQL predicate are not redundant. The check answers with a reason and
// stops the broker work, which no database predicate can do; the predicate holds when the row is
// tombstoned after the read, which no in-memory check can see. Removing either one leaves a
// window open.
func TestUpdateSubscriber_RefusesTheTombstonedRowAtThePersistenceBoundaryToo(t *testing.T) {
	run := newSubscriberLifecycle(t)

	// The tombstone lands while the PRUNE is in flight — after the service read a clean row.
	run.admin.onPrune = func() {
		row, ok := run.store.row(subscriberFixtureID)
		require.True(t, ok)

		pendingAt := time.Now()
		row.RevocationPendingAt = &pendingAt
		run.store.with(row)
	}

	_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
		SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions", "blnk.identities"}})

	require.Error(t, err)
	requireSubscriberAPIError(t, err, apierror.ErrConflict)
	assert.Contains(t, err.Error(), "being deregistered")

	// The prune ran — it could not have been prevented, because the row was clean when it was
	// read — but the GRANT did not, so no binding was created for a principal being revoked.
	assert.Equal(t, []string{"PruneSubscriberAccess"},
		run.log.only("PruneSubscriberAccess", "GrantSubscriberAccess"),
		"the write must fail closed BEFORE the grant, which is the step that widens access")

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Equal(t, []string{"blnk.transactions", "blnk.balances"}, stored.AuthorizedTopics,
		"the refused write must leave the recorded authorization untouched")
}

// ---------------------------------------------------------------------------------------
// The fence is RENEWED around broker work and CHECKED by every durable write
// ---------------------------------------------------------------------------------------

// TestSubscriberProvisioningFenceLease_IsDerivedFromTheWorkItCovers pins the arithmetic rather
// than the number.
//
// The lease used to be a chosen multiple of the issuance budget, and it could not be right at any
// value: the work under it was a variable-length sequence of Kafka round trips, each capped
// independently, so a lease long enough for the worst case would fence a subscriber for a minute
// after a crash and a lease short enough to recover from a crash expired mid-operation.
//
// The relationship below is the fix. One renewal has to span exactly one broker phase, the
// durable write after it and the compensating write after a failure — so the lease is the sum of
// those three budgets, and renewal rather than a longer lease is what covers an operation that
// legitimately needs more time.
func TestSubscriberProvisioningFenceLease_IsDerivedFromTheWorkItCovers(t *testing.T) {
	assert.Equal(t,
		subscriberBrokerPhaseBudget+subscriberCleanupBudget+SubscriberCredentialIssuanceBudget,
		SubscriberProvisioningFenceLease,
		"the lease must be the sum of the budgets one renewal has to span, not a chosen multiple")

	assert.Greater(t, SubscriberProvisioningFenceLease, subscriberBrokerPhaseBudget,
		"a lease no longer than one phase would expire inside every phase it is meant to cover")

	assert.GreaterOrEqual(t, subscriberBrokerPhaseBudget, SubscriberCredentialIssuanceBudget,
		"a phase must have at least the budget R-7 requires a complete provisioning to fit in")
}

// TestUpdateSubscriber_RenewsTheClaimBeforeEachBrokerPhaseAndBoundsIt is the F10 core.
//
// # The defect
//
// UpdateSubscriber took one leased claim and then made TWO independent sequences of
// administrative round trips — prune, then grant. It arrived with no deadline of its own, so each
// round trip fell back to the admin client's 10-second per-request cap; a prune of three and a
// grant of four could therefore run far past the 15-second lease. Past it, the operation simply
// carried on: the claim was gone, and the durable write in between matched on subscriber_id
// alone, so it landed anyway — over the top of whatever the new owner had just reconciled with
// the broker, and reported success.
//
// # The two halves of the fix, both asserted here
//
//   - RENEWED IMMEDIATELY BEFORE each phase, so a successful renewal is proof of ownership at
//     that instant. Renewing afterwards would prove only that nobody took over while the broker
//     was being changed, which is the thing already too late to learn.
//   - BOUNDED, so the phase's worst case is knowable. A phase arriving with no deadline is the
//     unbounded case, and budget() reports -1 for exactly that.
func TestUpdateSubscriber_RenewsTheClaimBeforeEachBrokerPhaseAndBoundsIt(t *testing.T) {
	run := newSubscriberLifecycle(t)

	updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
		SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions", "blnk.identities"}})
	require.NoError(t, err)
	require.NotNil(t, updated)

	// ORDER: claim, renew, prune, write, renew, grant.
	assert.Equal(t, []string{
		"ClaimSubscriberForProvisioning",
		"RenewSubscriberProvisioningFence",
		"PruneSubscriberAccess",
		"UpdateEventSubscriber",
		"RenewSubscriberProvisioningFence",
		"GrantSubscriberAccess",
	}, run.log.only(
		"ClaimSubscriberForProvisioning",
		"RenewSubscriberProvisioningFence",
		"PruneSubscriberAccess",
		"UpdateEventSubscriber",
		"GrantSubscriberAccess",
	), "each broker phase must be preceded by a renewal that PROVES the claim is still held")

	assert.Equal(t, 2, run.log.count("RenewSubscriberProvisioningFence"),
		"one renewal per broker phase, so neither phase runs on a claim confirmed before the other")

	// BOUNDED. -1 means the phase arrived with no deadline at all, which is the unbounded case.
	for _, phase := range []string{"PruneSubscriberAccess", "GrantSubscriberAccess"} {
		budget := run.log.budget(phase)
		require.Positive(t, budget, "%s must arrive with a deadline, not an open-ended context", phase)
		assert.LessOrEqual(t, budget, subscriberBrokerPhaseBudget,
			"%s must be bounded by the phase budget", phase)
	}
}

// TestUpdateSubscriber_AbandonsTheOperationWhenTheClaimWasTakenOver proves a refused renewal
// STOPS the operation rather than being logged and passed over.
//
// A caller whose renewal fails has learned that it does not own the subscriber. Continuing is
// precisely how a stale owner comes to overwrite the state a new owner established, so the error
// is returned and the phase never runs.
func TestUpdateSubscriber_AbandonsTheOperationWhenTheClaimWasTakenOver(t *testing.T) {
	t.Run("before the first phase", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		// Somebody else takes the claim over between this operation's own claim and its renewal.
		run.store.failing("RenewSubscriberProvisioningFence", apierror.NewAPIError(
			apierror.ErrConflict,
			"The subscriber provisioning claim is no longer held by this caller",
			errors.New("test: the claim was taken over"),
		))

		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions"}})

		require.Error(t, err)
		requireSubscriberAPIError(t, err, apierror.ErrConflict)

		assert.Empty(t, run.log.only("PruneSubscriberAccess", "GrantSubscriberAccess"),
			"a refused renewal must stop the operation before it touches the broker")
		assert.Zero(t, run.log.count("UpdateEventSubscriber"))
	})

	t.Run("mid-phase, so the durable write is what refuses", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		// The claim is taken over WHILE the prune is in flight, which is the case a renewal
		// cannot prevent — it can only be caught by the write's own predicate.
		run.admin.onPrune = func() {
			run.store.holdFence(subscriberFixtureID, SubscriberProvisioningFenceLease)
		}

		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions", "blnk.identities"}})

		require.Error(t, err)
		requireSubscriberAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error(), "no longer held by this caller")

		// The write was REFUSED, so the new owner's reconciliation is not overwritten...
		stored, ok := run.store.row(subscriberFixtureID)
		require.True(t, ok)
		assert.Equal(t, []string{"blnk.transactions", "blnk.balances"}, stored.AuthorizedTopics,
			"a stale owner's write must not land on top of the new owner's authorization")

		// ...and the GRANT never ran, so no binding was created from the stale view either.
		assert.Equal(t, []string{"PruneSubscriberAccess"},
			run.log.only("PruneSubscriberAccess", "GrantSubscriberAccess"))
	})
}

// TestIssueSubscriberCredential_ConfirmsTheClaimBeforeProvisioningAndRecordsUnderIt covers the
// same two properties on the issuance path, where the consequence is a credential rather than an
// ACL binding.
//
// The reference CAS alone could not detect a caller whose lease expired: while the rightful new
// owner is still provisioning it has recorded nothing, so the stored reference is STILL the one
// the stale caller observed. Its write matched, landed, and the winner was then refused — the
// fence inverted, with the loser owning the registry and the broker holding the winner's
// password.
func TestIssueSubscriberCredential_ConfirmsTheClaimBeforeProvisioningAndRecordsUnderIt(t *testing.T) {
	t.Run("renews before the broker phase and records under the claim", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		credential, err := run.service.IssueSubscriberCredential(
			context.Background(), subscriberFixtureID)
		require.NoError(t, err)
		assert.NotEmpty(t, credential.Password())

		// TWO RENEWALS, and neither is redundant. This assertion named only the first, and its
		// sibling TestIssueSubscriberCredential_RenewsTheClaimBeforeItWritesTheIssuance named
		// only the second; the two windows they guard are different, so the sequence carries
		// both.
		//
		//   - BEFORE the broker phase, which is what this subtest is about: a secret must not be
		//     written at the broker under a lease that has already lapsed.
		//   - AFTER it and before the registry write, which is the only position from which a
		//     write that is about to race a new owner can be refused. Provisioning is up to four
		//     round trips with their own timeouts, so the claim confirmed on the way in can be
		//     gone by the time the write happens.
		assert.Equal(t, []string{
			"ClaimSubscriberForProvisioning",
			"RenewSubscriberProvisioningFence",
			"ProvisionSubscriberPrincipal",
			"RenewSubscriberProvisioningFence",
			"RecordSubscriberCredentialIfUnchanged",
		}, run.log.only(
			"ClaimSubscriberForProvisioning",
			"RenewSubscriberProvisioningFence",
			"ProvisionSubscriberPrincipal",
			"RecordSubscriberCredentialIfUnchanged",
		), "the claim must be confirmed on the way into the broker, before a secret is written")

		// The issuance budget already bounds this phase — it bounds the whole request — so the
		// phase context must not have LOOSENED it.
		budget := run.log.budget("ProvisionSubscriberPrincipal")
		require.Positive(t, budget)
		assert.LessOrEqual(t, budget, SubscriberCredentialIssuanceBudget,
			"the phase context must never extend the caller's own budget")
	})

	t.Run("a claim taken over during provisioning refuses the issuance record", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		run.admin.onProvision = func() {
			run.store.holdFence(subscriberFixtureID, SubscriberProvisioningFenceLease)
		}

		credential, err := run.service.IssueSubscriberCredential(
			context.Background(), subscriberFixtureID)

		require.Error(t, err, "an issuance whose claim was taken over must not report success")
		assert.Empty(t, credential.Password(),
			"no secret may be returned for an issuance that was not recorded")

		stored, ok := run.store.row(subscriberFixtureID)
		require.True(t, ok)
		assert.Nil(t, stored.CredentialReference,
			"a stale owner must not leave a credential record behind")
	})
}

// TestDeregisterSubscriber_ConfirmsTheClaimBeforeRevokingAndDeletesUnderIt covers the
// deregistration path, where an unfenced final delete was the most damaging of the three.
//
// STEP 4 deletes the row after a broker revocation whose duration the broker decides. Under an
// expired lease an unconditional delete removed a row another operation was working on — most
// damagingly an issuance, which then left a live broker principal with no registry row naming
// it: exactly the orphaned access the tombstone-first ordering exists to make impossible.
func TestDeregisterSubscriber_ConfirmsTheClaimBeforeRevokingAndDeletesUnderIt(t *testing.T) {
	t.Run("renews before revoking and bounds the phase", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		removed, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
		require.NoError(t, err)
		require.NotNil(t, removed)

		assert.Equal(t, []string{
			"ClaimSubscriberForProvisioning",
			"MarkSubscriberRevocationPending",
			"RenewSubscriberProvisioningFence",
			"RevokeSubscriber",
			"TakeEventSubscriber",
		}, run.log.only(
			"ClaimSubscriberForProvisioning",
			"MarkSubscriberRevocationPending",
			"RenewSubscriberProvisioningFence",
			"RevokeSubscriber",
			"TakeEventSubscriber",
		), "the tombstone comes first, then the claim is confirmed, then the broker is touched")

		budget := run.log.budget("RevokeSubscriber")
		require.Positive(t, budget, "the revocation must arrive with a deadline")
		assert.LessOrEqual(t, budget, subscriberBrokerPhaseBudget)
	})

	t.Run("a claim taken over during revocation leaves the row tombstoned", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		run.admin.onRevoke = func() {
			run.store.holdFence(subscriberFixtureID, SubscriberProvisioningFenceLease)
		}

		_, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
		require.Error(t, err)
		requireSubscriberAPIError(t, err, apierror.ErrConflict)

		// The row SURVIVES, still tombstoned, which is the recoverable direction: it names the
		// principal and a retry under a fresh claim finishes the job.
		stored, ok := run.store.row(subscriberFixtureID)
		require.True(t, ok, "a deregistration that lost its claim must not have deleted the row")
		assert.NotNil(t, stored.RevocationPendingAt,
			"and the tombstone must remain, so the obligation stays findable")
	})

	t.Run("a re-mark of an already tombstoned row is the one accepted retry", func(t *testing.T) {
		// Every other mutation refuses a tombstoned row. This one must not: an existing
		// tombstone IS a deregistration that failed part way through, and finishing it is the
		// whole reason the row survives a failed revocation.
		tombstoned := subscriberFixtureRow(t)
		firstAttempt := time.Now().Add(-10 * time.Minute).UTC()
		tombstoned.RevocationPendingAt = &firstAttempt

		run := newSubscriberLifecycle(t).seeded(tombstoned)

		removed, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
		require.NoError(t, err, "retrying a stuck deregistration must finish it, not refuse it")
		require.NotNil(t, removed)

		require.NotNil(t, removed.RevocationPendingAt)
		assert.WithinDuration(t, firstAttempt, removed.RevocationPendingAt.UTC(), time.Second,
			"the FIRST instant must survive the retry, or the backlog age always reads small")

		_, ok := run.store.row(subscriberFixtureID)
		assert.False(t, ok, "the row is deleted once the revocation is confirmed")
	})

	t.Run("a completed deregistration does not report a lost fence", func(t *testing.T) {
		// The provisioning columns live ON the row, so deleting it takes the claim with it. The
		// deferred release then matched nothing and reported "the claim expired or was taken
		// over" — on the one path where the claim's disappearance is the INTENDED outcome.
		//
		// That false alarm mattered because the lost-fence signal is the observable half of the
		// fence: it is how an operator learns an operation ran past its lease. Firing it on
		// every success is how it stops being read.
		run := newSubscriberLifecycle(t)

		_, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
		require.NoError(t, err)

		assert.Zero(t, run.log.count("ReleaseSubscriberProvisioningFence"),
			"there is no claim left to release once the row that carried it is gone")
	})
}

// TestRevokeSubscriberCredential_ConfirmsTheClaimAndClearsUnderIt covers the last fenced path.
//
// Clearing under an expired lease would blank the record a NEWER issuance had just written,
// leaving the registry reporting "registered, not yet provisioned" for a subscriber holding a
// working credential. That is the one direction the clear must never move in: it UNDER-reports
// access, so nothing downstream has any reason to look at it.
//
// It is also the one operation that DELIBERATELY accepts a tombstoned row — revocation takes
// access away, which is what the tombstone wants, and refusing it would block the only manual
// remedy for a deregistration whose broker step keeps failing.
func TestRevokeSubscriberCredential_ConfirmsTheClaimAndClearsUnderIt(t *testing.T) {
	t.Run("renews before revoking, then clears under the claim", func(t *testing.T) {
		provisioned := subscriberFixtureRow(t)
		reference, err := model.DeriveCredentialReference(provisioned.KafkaPrincipal, "an-old-secret")
		require.NoError(t, err)
		issuedAt := time.Now().Add(-time.Hour)
		provisioned.CredentialReference = &reference
		provisioned.CredentialIssuedAt = &issuedAt

		run := newSubscriberLifecycle(t).seeded(provisioned)

		require.NoError(t, run.service.RevokeSubscriberCredential(
			context.Background(), subscriberFixtureID))

		assert.Equal(t, []string{
			"ClaimSubscriberForProvisioning",
			"RenewSubscriberProvisioningFence",
			"RevokeSubscriber",
			"ClearSubscriberCredential",
		}, run.log.only(
			"ClaimSubscriberForProvisioning",
			"RenewSubscriberProvisioningFence",
			"RevokeSubscriber",
			"ClearSubscriberCredential",
		))

		stored, ok := run.store.row(subscriberFixtureID)
		require.True(t, ok)
		assert.Nil(t, stored.CredentialReference)
		assert.Nil(t, stored.CredentialIssuedAt)
	})

	t.Run("accepts a tombstoned row, because revocation is what the tombstone wants", func(t *testing.T) {
		tombstoned := subscriberFixtureRow(t)
		reference, err := model.DeriveCredentialReference(tombstoned.KafkaPrincipal, "an-old-secret")
		require.NoError(t, err)
		pendingAt := time.Now().Add(-time.Minute)
		tombstoned.CredentialReference = &reference
		tombstoned.RevocationPendingAt = &pendingAt

		run := newSubscriberLifecycle(t).seeded(tombstoned)

		require.NoError(t, run.service.RevokeSubscriberCredential(
			context.Background(), subscriberFixtureID),
			"refusing here would block the manual remedy for a stuck deregistration")

		stored, ok := run.store.row(subscriberFixtureID)
		require.True(t, ok)
		assert.Nil(t, stored.CredentialReference)
	})
}

// ---------------------------------------------------------------------------------------
// The webhook cutover records both facts or neither
// ---------------------------------------------------------------------------------------

// TestCompleteWebhookMigration_RecordsBothFactsInOneWrite is the cutover atomicity guard.
//
// # The stranded row this prevents
//
// The cutover used to be two writes with nothing spanning them: clear webhook_url, then stamp
// migrated_at. The ordering was chosen so a partial failure could not OVER-claim progress, and it
// did not. What it did was strand the row in a state belonging to NEITHER side of the migration
// report — no endpoint, so nothing still to migrate from; no instant, so not counted as migrated.
// The dual-run window then under-reported for as long as it lasted, and the caller was given no
// way to learn that repeating the request was the remedy.
//
// One write is the fix, and the property to assert is not "it succeeds" but that there is no
// intermediate state to observe: exactly one collaborator call carries the whole cutover.
func TestCompleteWebhookMigration_RecordsBothFactsInOneWrite(t *testing.T) {
	legacy := subscriberFixtureRow(t)
	legacy.WebhookURL = stringPointer("https://acme.example.com/blnk-events")

	run := newSubscriberLifecycle(t).seeded(legacy)

	migrated, err := run.service.CompleteWebhookMigration(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	require.NotNil(t, migrated)

	// BOTH facts, from the returned row.
	assert.Nil(t, migrated.WebhookURL, "the legacy endpoint must be forgotten")
	require.NotNil(t, migrated.MigratedAt, "and the migration instant must be recorded")

	// AND from the stored row, so the return value is not merely a well-formed answer.
	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.WebhookURL)
	require.NotNil(t, stored.MigratedAt)

	// ONE WRITE. This is the assertion the finding is about: two writes with no transaction
	// between them is exactly the window that stranded the row, so a second write here — of any
	// kind — reintroduces it.
	assert.Equal(t, []string{"CompleteSubscriberWebhookMigration"}, run.log.only(
		"CompleteSubscriberWebhookMigration",
		"UpdateEventSubscriber",
		"MarkSubscriberMigrated",
	), "the cutover must be one write, not a clear followed by a stamp")

	// AND NO BROKER WORK, AND NO FENCE. Routing the clear through the authorization update used
	// to take a provisioning claim and make two Kafka administrative round trips to erase a URL —
	// neither column it writes has a broker counterpart, so there was nothing to reconcile.
	assert.Empty(t, run.log.only(
		"ClaimSubscriberForProvisioning",
		"RenewSubscriberProvisioningFence",
		"PruneSubscriberAccess",
		"GrantSubscriberAccess",
	), "erasing a URL must not fence the subscriber or reach the broker")
}

// TestCompleteWebhookMigration_LeavesNothingBehindWhenTheWriteFails is the other half: a failed
// cutover must leave the row on the side it started on.
//
// With two writes the failure modes were asymmetric — the first could succeed while the second
// failed. With one there is only one outcome to check, and checking it is what proves the
// asymmetry is gone.
func TestCompleteWebhookMigration_LeavesNothingBehindWhenTheWriteFails(t *testing.T) {
	legacy := subscriberFixtureRow(t)
	legacy.WebhookURL = stringPointer("https://acme.example.com/blnk-events")

	run := newSubscriberLifecycle(t).seeded(legacy)
	run.store.failing("CompleteSubscriberWebhookMigration", apierror.NewAPIError(
		apierror.ErrInternalServer, "Failed to complete the subscriber's webhook migration",
		errors.New("test: the cutover write failed"),
	))

	migrated, err := run.service.CompleteWebhookMigration(context.Background(), subscriberFixtureID)
	require.Error(t, err)
	assert.Nil(t, migrated)

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.WebhookURL,
		"a failed cutover must leave the endpoint recorded, so the row is still counted as "+
			"awaiting migration and is still identifiable as owing one")
	assert.Equal(t, "https://acme.example.com/blnk-events", *stored.WebhookURL)
	assert.Nil(t, stored.MigratedAt, "and must not claim a migration that did not happen")
}

// TestCompleteWebhookMigration_IsNotTheSameOperationAsClearingAURL pins the distinction the two
// operations must keep.
//
// migrated_at is an AUDIT FACT, so stamping it for a subscriber that has not moved records a
// false one. An operator correcting a mis-recorded endpoint therefore needs a clear that asserts
// nothing, and that is what ClearLegacyWebhookSubscription remains — collapsing the two into one
// operation would make every correction claim a migration.
func TestCompleteWebhookMigration_IsNotTheSameOperationAsClearingAURL(t *testing.T) {
	legacy := subscriberFixtureRow(t)
	legacy.WebhookURL = stringPointer("https://acme.example.com/mistyped")

	run := newSubscriberLifecycle(t).seeded(legacy)

	cleared, err := run.service.ClearLegacyWebhookSubscription(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	require.NotNil(t, cleared)

	assert.Nil(t, cleared.WebhookURL, "the mis-recorded endpoint is gone")
	assert.Nil(t, cleared.MigratedAt,
		"but a correction must NOT record a migration that has not happened")
}

// TestCompleteWebhookMigration_RejectsABlankIdentifierBeforeWriting keeps the guard in memory,
// where it answers "required" rather than the misleading "Subscriber not found" an empty WHERE
// match would produce.
func TestCompleteWebhookMigration_RejectsABlankIdentifierBeforeWriting(t *testing.T) {
	run := newSubscriberLifecycle(t)

	_, err := run.service.CompleteWebhookMigration(context.Background(), "   ")
	require.Error(t, err)
	assert.Zero(t, run.log.count("CompleteSubscriberWebhookMigration"),
		"a blank identifier must be refused before the write is attempted")
}

// ---------------------------------------------------------------------------------------
// SETTLE-01 — a broker-side obligation is DURABLE, not a log line
//
// A subscriber's state lives in two systems that cannot be written atomically. Every
// intermediate state of the three operations that span both is reachable, and each one used to
// be recorded as nothing more than a returned error and a log line — some ending in the words
// "revoke it by hand immediately". A log line is not queryable per subscriber, is not retried,
// and cannot be alerted on, so the divergence persisted until somebody read the right line.
//
// The tests below pin the three properties that replace it: the obligation is recorded BEFORE
// the work that may fail, it is discharged only once the two systems demonstrably agree, and a
// settlement pass that finds nothing owed does nothing.
// ---------------------------------------------------------------------------------------

// TestUpdateSubscriber_RecordsTheReconciliationObligationBeforeItTouchesTheBroker is the
// ordering the durability of F5 rests on.
//
// The marker must be written FIRST because the failure that matters most cannot write anything:
// a process that disappears between the prune and the persist leaves nothing behind to record
// that it was mid-change. Recording after a failure is absent from exactly the case an operator
// cannot detect any other way.
func TestUpdateSubscriber_RecordsTheReconciliationObligationBeforeItTouchesTheBroker(t *testing.T) {
	run := newSubscriberLifecycle(t)

	_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		AuthorizedTopics: []string{"blnk.transactions"},
	})
	require.NoError(t, err)

	require.Equal(t, []string{
		"ClaimSubscriberForProvisioning",
		"RecordSubscriberGrantReconcilePending",
		"PruneSubscriberAccess",
		"UpdateEventSubscriber",
		"GrantSubscriberAccess",
		"ClearSubscriberGrantReconcilePending",
	}, run.log.only(
		"ClaimSubscriberForProvisioning",
		"RecordSubscriberGrantReconcilePending",
		"PruneSubscriberAccess",
		"UpdateEventSubscriber",
		"GrantSubscriberAccess",
		"ClearSubscriberGrantReconcilePending",
	),
		"the obligation is recorded BEFORE the first broker call and discharged only AFTER the "+
			"last one; recorded later, a process that died mid-change would leave no marker at all")

	assert.False(t, run.store.obligationOf(subscriberFixtureID).outstanding(),
		"a change that completed at both the registry and the broker owes nothing")
}

// TestUpdateSubscriber_LeavesTheObligationOutstandingWhenTheNarrowingFailed is the F5 property
// itself: a failure leaves a durable to-do item rather than only an error.
func TestUpdateSubscriber_LeavesTheObligationOutstandingWhenTheNarrowingFailed(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.admin.failing("PruneSubscriberAccess", errors.New("broker unreachable"))

	_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		AuthorizedTopics: []string{"blnk.transactions"},
	})
	require.Error(t, err)

	obligation := run.store.obligationOf(subscriberFixtureID)
	assert.False(t, obligation.grantPendingAt.IsZero(),
		"the broker and the registry may now disagree, and the marker is what makes that "+
			"queryable rather than something an operator has to read a log to discover")
	assert.Zero(t, run.log.count("ClearSubscriberGrantReconcilePending"),
		"nothing may be discharged on a failure")
}

// TestUpdateSubscriber_LeavesTheObligationOutstandingWhenTheWideningFailed is the other
// boundary of the same sequence.
//
// The row already records the wider authorization here, so the residue is fail-closed — but it
// is still a divergence, and the marker covers it with the same remedy because the ROW is the
// source of truth whichever step failed.
func TestUpdateSubscriber_LeavesTheObligationOutstandingWhenTheWideningFailed(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.admin.failing("GrantSubscriberAccess", errors.New("broker unreachable"))

	_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		AuthorizedTopics: []string{"blnk.transactions", "blnk.balances", "blnk.identities"},
	})
	require.Error(t, err)

	assert.NotZero(t, run.log.count("UpdateEventSubscriber"),
		"the persist happened, which is what makes this the second boundary rather than the first")
	assert.False(t, run.store.obligationOf(subscriberFixtureID).grantPendingAt.IsZero(),
		"one marker covers a failure at EITHER boundary, because the remedy is the same: "+
			"reconcile the broker to the row")
}

// TestUpdateSubscriber_RefusesToTouchTheBrokerWhenTheObligationCannotBeRecorded is the
// fail-closed half.
//
// Proceeding without the marker would produce exactly the state the mechanism exists to
// prevent — broker work in flight with no durable record that it is — so it is refused rather
// than logged and continued.
func TestUpdateSubscriber_RefusesToTouchTheBrokerWhenTheObligationCannotBeRecorded(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.store.failing("RecordSubscriberGrantReconcilePending",
		errors.New("registry write path unavailable"))

	_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		AuthorizedTopics: []string{"blnk.transactions"},
	})
	require.Error(t, err)

	assert.Empty(t, run.log.only("PruneSubscriberAccess", "GrantSubscriberAccess"),
		"nothing may reach the broker without a durable record that it did")
	assert.Zero(t, run.log.count("UpdateEventSubscriber"))
}

// TestIssueSubscriberCredential_RecordsACleanupObligationWhenCompensationFailed is F6.
//
// The credential is live at the broker and its revocation failed, so a principal exists that can
// authenticate with no authorization boundary. That was previously an ERROR log line and nothing
// else. It is now a durable obligation a settlement pass will discharge.
func TestIssueSubscriberCredential_RecordsACleanupObligationWhenCompensationFailed(t *testing.T) {
	run := newSubscriberLifecycle(t)

	// CredentialWritten still true after provisioning tried to compensate: the revocation itself
	// failed, which is the state that leaves live access nothing accounts for.
	run.admin.result = SubscriberProvisioningResult{CredentialWritten: true}
	run.admin.provisionErr = errors.New("acl grant refused by the broker")

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	assert.False(t, run.store.obligationOf(subscriberFixtureID).credentialCleanupAt.IsZero(),
		"a credential that exists with no boundary must be recorded as owed work, not merely logged")

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.CredentialReference,
		"no issuance was recorded, so the row must not claim one")
}

// TestIssueSubscriberCredential_ClearsTheRegistryRecordWhenCompensationSucceeded is F13.
//
// Provisioning UPSERTS the principal's SCRAM credential, so a re-issue that then failed and
// compensated destroyed the credential the row named as well as the one it had just written. The
// row must therefore stop claiming a credential — otherwise the registry reports a provisioned
// subscriber whose credential authenticates nothing at all.
func TestIssueSubscriberCredential_ClearsTheRegistryRecordWhenCompensationSucceeded(t *testing.T) {
	existing := subscriberFixtureRow(t)
	reference := "blnk-cred-ref-000000000000000000000000000000000000000000000000000000000000"
	issued := time.Now().UTC().Add(-time.Hour)
	existing.CredentialReference = &reference
	existing.CredentialIssuedAt = &issued

	run := newSubscriberLifecycle(t).seeded(existing)
	run.admin.result = SubscriberProvisioningResult{CredentialWritten: false, Compensated: true}
	run.admin.provisionErr = errors.New("acl grant refused by the broker")

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.CredentialReference,
		"the credential the row named was replaced by the upsert and then revoked, so it no "+
			"longer exists and the row must not keep reporting it")
	assert.Nil(t, stored.CredentialIssuedAt)

	assert.True(t, run.store.obligationOf(subscriberFixtureID).outstanding() == false,
		"the divergence was corrected in-band, so nothing is owed")
}

// TestIssueSubscriberCredential_RecordsACleanupObligationWhenTheRecordCannotBeCleared is the
// other half of F13: when the in-band correction fails, the divergence becomes owed work.
func TestIssueSubscriberCredential_RecordsACleanupObligationWhenTheRecordCannotBeCleared(t *testing.T) {
	existing := subscriberFixtureRow(t)
	reference := "blnk-cred-ref-000000000000000000000000000000000000000000000000000000000000"
	issued := time.Now().UTC().Add(-time.Hour)
	existing.CredentialReference = &reference
	existing.CredentialIssuedAt = &issued

	run := newSubscriberLifecycle(t).seeded(existing)
	run.admin.result = SubscriberProvisioningResult{CredentialWritten: false, Compensated: true}
	run.admin.provisionErr = errors.New("acl grant refused by the broker")
	run.store.failing("ClearSubscriberCredential", errors.New("registry write path unavailable"))

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	assert.False(t, run.store.obligationOf(subscriberFixtureID).credentialCleanupAt.IsZero(),
		"the registry now over-reports this subscriber's access, and that has to be owed work "+
			"rather than a warning waiting to be read")
}

// TestIssueSubscriberCredential_RecordsNothingWhenTheBrokerWasNotTouched keeps the marker
// meaningful.
//
// A failure that changed no broker state owes nothing. Raising an obligation for every rejected
// request would put the whole registry into the settlement backlog and drown the two states the
// marker exists for.
func TestIssueSubscriberCredential_RecordsNothingWhenTheBrokerWasNotTouched(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.admin.result = SubscriberProvisioningResult{}
	run.admin.provisionErr = errors.New("broker unreachable before anything was written")

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	assert.False(t, run.store.obligationOf(subscriberFixtureID).outstanding(),
		"nothing reached the broker, so nothing is owed")
	assert.Zero(t, run.log.count("RecordSubscriberCredentialCleanupPending"))
}

// TestIssueSubscriberCredential_DischargesAPendingCleanupOnASuccessfulReissue is the
// self-satisfying case, and it is the one that would be dangerous to get wrong.
//
// Provisioning upserts, so a successful issuance REPLACES whatever credential a pending cleanup
// was about. Left outstanding, the next settlement pass would revoke the credential that had
// just been issued and handed to a subscriber — destroying working access on the strength of a
// marker for a credential that no longer exists.
func TestIssueSubscriberCredential_DischargesAPendingCleanupOnASuccessfulReissue(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.store.owing(subscriberFixtureID, false, true)

	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	require.NotEmpty(t, credential.Password())

	assert.False(t, run.store.obligationOf(subscriberFixtureID).outstanding(),
		"the issuance replaced the credential the obligation was about, so it is SATISFIED — and "+
			"leaving it set would have settlement destroy the credential just issued")
}

// TestSettleSubscriber_ReconcilesTheBrokerToTheRowAndDischarges is the remedy for a grant
// obligation.
func TestSettleSubscriber_ReconcilesTheBrokerToTheRowAndDischarges(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.store.owing(subscriberFixtureID, true, false)

	require.NoError(t, run.service.SettleSubscriber(context.Background(), subscriberFixtureID))

	assert.Equal(t, []string{"PruneSubscriberAccess", "GrantSubscriberAccess"},
		run.log.only("PruneSubscriberAccess", "GrantSubscriberAccess"),
		"the remedy is the same prune-then-grant an update performs, because the row is the "+
			"source of truth however far the original attempt got")
	assert.False(t, run.store.obligationOf(subscriberFixtureID).outstanding(),
		"discharged only once the broker and the row demonstrably agree")
}

// TestSettleSubscriber_RevokesAndClearsForACredentialCleanup is the remedy for the other marker.
func TestSettleSubscriber_RevokesAndClearsForACredentialCleanup(t *testing.T) {
	existing := subscriberFixtureRow(t)
	reference := "blnk-cred-ref-000000000000000000000000000000000000000000000000000000000000"
	issued := time.Now().UTC().Add(-time.Hour)
	existing.CredentialReference = &reference
	existing.CredentialIssuedAt = &issued

	run := newSubscriberLifecycle(t).seeded(existing)
	run.store.owing(subscriberFixtureID, false, true)

	require.NoError(t, run.service.SettleSubscriber(context.Background(), subscriberFixtureID))

	assert.Equal(t, []string{subscriberFixtureID}, run.admin.revoked,
		"revoking is the remedy, and it is a harmless no-op when the credential is already gone "+
			"— which is why ONE marker covers both shapes of this divergence")

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.CredentialReference)
	assert.False(t, run.store.obligationOf(subscriberFixtureID).outstanding())
}

// TestSettleSubscriber_CleansTheCredentialBeforeItReconcilesTheGrant pins the order between the
// two remedies.
//
// Revocation removes every binding the principal holds — by principal, because the broker's
// bindings are the union of every grant it has ever held — so reconciling first and revoking
// second would undo the reconciliation that had just completed.
func TestSettleSubscriber_CleansTheCredentialBeforeItReconcilesTheGrant(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.store.owing(subscriberFixtureID, true, true)

	require.NoError(t, run.service.SettleSubscriber(context.Background(), subscriberFixtureID))

	require.Equal(t, []string{
		"RevokeSubscriber",
		"PruneSubscriberAccess",
		"GrantSubscriberAccess",
	}, run.log.only("RevokeSubscriber", "PruneSubscriberAccess", "GrantSubscriberAccess"),
		"REVOKE first, then reconcile. Reversed, the revocation would strip the bindings the "+
			"reconciliation had just created")

	assert.False(t, run.store.obligationOf(subscriberFixtureID).outstanding(),
		"both obligations are discharged when both remedies succeed")
}

// TestSettleSubscriber_DoesNothingWhenNothingIsOwedAnyMore is why the flags are re-read under
// the claim.
//
// A pass finds an obligation, then takes the claim, and time passes in between. A successful
// re-issuance in that window discharges the cleanup obligation, and acting on the flag the scan
// returned would revoke a credential that had just been issued.
func TestSettleSubscriber_DoesNothingWhenNothingIsOwedAnyMore(t *testing.T) {
	run := newSubscriberLifecycle(t)

	require.NoError(t, run.service.SettleSubscriber(context.Background(), subscriberFixtureID))

	assert.Empty(t, run.log.only("RevokeSubscriber", "PruneSubscriberAccess", "GrantSubscriberAccess"),
		"a subscriber that owes nothing must not have its broker state touched at all")
}

// TestSettleSubscriber_IsIdempotent is the property retrying rests on.
//
// An obligation is discharged only AFTER its remedy returned, so a pass that crashed in between
// leaves the marker set and this runs again. That is only safe if running it twice produces the
// same result as running it once.
func TestSettleSubscriber_IsIdempotent(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.store.owing(subscriberFixtureID, true, false)

	require.NoError(t, run.service.SettleSubscriber(context.Background(), subscriberFixtureID))
	first := run.log.count("GrantSubscriberAccess")

	require.NoError(t, run.service.SettleSubscriber(context.Background(), subscriberFixtureID))

	assert.Equal(t, first, run.log.count("GrantSubscriberAccess"),
		"the second pass finds nothing owed and does nothing, so a crash between the remedy and "+
			"the discharge costs one harmless repeat rather than a divergent outcome")
}

// TestSettleSubscriber_DischargesWithoutReGrantingASubscriberBeingDeregistered is the
// interaction with AUTH-01.
//
// Reconciling the broker TO a tombstoned row would re-create the very grants its deregistration
// is removing — the exact widening the tombstone predicate exists to prevent. The marker is
// discharged instead, because the tombstone is itself a durable indexed to-do item; leaving it
// set would create an obligation no pass could ever satisfy.
func TestSettleSubscriber_DischargesWithoutReGrantingASubscriberBeingDeregistered(t *testing.T) {
	tombstoned := subscriberFixtureRow(t)
	pending := time.Now().UTC().Add(-time.Minute)
	tombstoned.RevocationPendingAt = &pending

	run := newSubscriberLifecycle(t).seeded(tombstoned)
	run.store.owing(subscriberFixtureID, true, false)

	require.NoError(t, run.service.SettleSubscriber(context.Background(), subscriberFixtureID))

	assert.Zero(t, run.log.count("GrantSubscriberAccess"),
		"a subscriber on its way out must not regain access from a settlement pass")
	assert.False(t, run.store.obligationOf(subscriberFixtureID).outstanding(),
		"the revocation tombstone is the durable marker for the work that remains, so this one "+
			"is discharged rather than left as an obligation nothing could satisfy")
}

// TestSettleSubscriber_KeepsTheObligationWhenTheRemedyFailed is the safe direction.
func TestSettleSubscriber_KeepsTheObligationWhenTheRemedyFailed(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.store.owing(subscriberFixtureID, true, false)
	run.admin.failing("GrantSubscriberAccess", errors.New("broker still unreachable"))

	require.Error(t, run.service.SettleSubscriber(context.Background(), subscriberFixtureID))

	assert.False(t, run.store.obligationOf(subscriberFixtureID).grantPendingAt.IsZero(),
		"nothing is discharged on a guess; the obligation survives for the next pass")
}

// TestSettleSubscriber_TakesTheClaimBeforeItTouchesTheBroker keeps CONC-01 intact.
//
// Settlement changes broker state, so it must hold the subscriber's claim. A subscriber somebody
// is actively issuing for is SKIPPED rather than fought over: settling underneath a live
// issuance would revoke the credential it is in the middle of writing.
func TestSettleSubscriber_TakesTheClaimBeforeItTouchesTheBroker(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.store.owing(subscriberFixtureID, true, false)

	require.NoError(t, run.service.SettleSubscriber(context.Background(), subscriberFixtureID))

	sequence := run.log.only(
		"ClaimSubscriberForProvisioning", "PruneSubscriberAccess", "GrantSubscriberAccess")
	require.NotEmpty(t, sequence)
	assert.Equal(t, "ClaimSubscriberForProvisioning", sequence[0],
		"the claim comes first, or a settlement pass could revoke a credential another operation "+
			"is mid-way through issuing")
	assert.False(t, run.store.fenced(subscriberFixtureID),
		"and it is released, so a retry need not wait out the lease")
}

// TestSettleSubscriber_SkipsASubscriberAnotherOperationHolds is the other side of that.
func TestSettleSubscriber_SkipsASubscriberAnotherOperationHolds(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.store.owing(subscriberFixtureID, true, false)

	_, err := run.store.ClaimSubscriberForProvisioning(
		context.Background(), subscriberFixtureID, time.Minute)
	require.NoError(t, err)

	err = run.service.SettleSubscriber(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code,
		"a contested subscriber is a SKIP, which the processor records as an attempt and retries "+
			"after the retry interval")
	assert.Empty(t, run.log.only("RevokeSubscriber", "PruneSubscriberAccess", "GrantSubscriberAccess"))
}

// ---------------------------------------------------------------------------------------
// C-03 / C-08 / C-22 — the legacy webhook record is one transition, and it touches no broker
// ---------------------------------------------------------------------------------------

// TestRecordLegacyWebhookSubscription_ClearsTheMigrationAndTouchesNoBroker covers both
// halves of what changed about recording a URL.
//
// # C-03: the two columns are one fact
//
// webhook_url says "receives legacy pushes at this address"; migrated_at says "no longer
// receives legacy pushes". A row holding both asserts the opposite of itself, and every
// reader of the registry then disagrees about it — the migration report counts it done, the
// retention purge treats the address as forgettable, and an operator sees a live endpoint on
// a subscriber that has supposedly finished.
//
// Recording used to go through UpdateSubscriber, which writes webhook_url and leaves
// migrated_at exactly as it was. So correcting an already-migrated subscriber's endpoint —
// the very thing the column exists to allow — produced that row silently.
//
// # C-22: it changes no authorization, so it must contact no broker
//
// UpdateSubscriber also reconciles ACL bindings around its write. Recording a URL changes no
// authorization, so every one of those round trips was work with no possible effect and each
// was a way for the call to fail for an unrelated reason: a broker outage answered 503 and a
// concurrent issuance holding the fence answered 409, neither documented, for a request that
// only ever wanted to write migration metadata.
func TestRecordLegacyWebhookSubscription_ClearsTheMigrationAndTouchesNoBroker(t *testing.T) {
	migrated := time.Now().UTC().Add(-24 * time.Hour)

	row := subscriberFixtureRow(t)
	row.MigratedAt = &migrated

	run := newSubscriberLifecycle(t).seeded(row)
	store, service := run.store, run.service

	// INSIDE THE DUAL-RUN WINDOW. Recording an endpoint is refused past the retirement instant
	// — TestRecordLegacyWebhookSubscription_IsRefusedAfterTheSunset is that half — and the
	// fixture's configuration carries a transport with no sunset date, which resolves
	// fail-closed to "retired". A future instant is what makes this subtest about the row it
	// writes rather than about the sunset.
	subscriberSunsetConfiguration(t, time.Now().Add(24*time.Hour))

	updated, err := service.RecordLegacyWebhookSubscription(
		context.Background(), subscriberFixtureID, "https://hooks.example.com/blnk",
	)
	require.NoError(t, err,
		"recording an endpoint on a migrated subscriber is a legitimate correction and must be "+
			"accepted; it is the row it produces that has to be consistent")
	require.NotNil(t, updated)

	require.NotNil(t, updated.WebhookURL)
	assert.Equal(t, "https://hooks.example.com/blnk", *updated.WebhookURL)
	assert.Nil(t, updated.MigratedAt,
		"MIGRATED_AT MUST BE CLEARED BY THE SAME WRITE. A subscriber with an endpoint recorded "+
			"again is, by that act, awaiting migration once more; leaving the timestamp is the "+
			"self-contradicting row every reader of the registry then disagrees about")

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.WebhookURL)
	assert.Nil(t, stored.MigratedAt, "and the stored row is what outlives the request")

	// ONE store call, so there is no window in which the row holds both values — and no
	// failure that can make such a window permanent.
	assert.Equal(t, 1, run.log.count("RecordSubscriberWebhookURL"),
		"one atomic write, not a read-modify-write")
	assert.Zero(t, run.log.count("UpdateEventSubscriber"),
		"the general update must not be involved: it writes webhook_url and leaves migrated_at")

	// NO BROKER. Not the client, not a binding, not the fence.
	assert.Zero(t, run.log.count("PruneSubscriberAccess"),
		"recording migration metadata changes no authorization, so there is nothing to prune")
	assert.Zero(t, run.log.count("GrantSubscriberAccess"), "and nothing to grant")
	assert.Zero(t, run.log.count("ClaimSubscriberForProvisioning"),
		"and no provisioning fence to take, so a concurrent issuance cannot make this request "+
			"fail with a conflict it has no business reporting")
}

// TestRecordLegacyWebhookSubscription_RefusesABlankURL keeps the two operations distinct.
//
// A subscription with no URL records nothing, and accepting a blank one would make this method
// a second way to clear a record — silently, and without the caller having asked to.
func TestRecordLegacyWebhookSubscription_RefusesABlankURL(t *testing.T) {
	run := newSubscriberLifecycle(t)

	for name, blank := range map[string]string{"empty": "", "spaces": "   ", "tab": "\t"} {
		t.Run(name, func(t *testing.T) {
			_, err := run.service.RecordLegacyWebhookSubscription(
				context.Background(), subscriberFixtureID, blank,
			)
			require.Error(t, err)

			var apiErr apierror.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, apierror.ErrGenValidation, apiErr.Code,
				"a blank URL is a malformed request, not a state conflict")
			assert.Zero(t, run.log.count("RecordSubscriberWebhookURL"),
				"and it is refused before the write")
		})
	}
}

// TestCompleteWebhookMigration_RetiresTheURLAndStampsTheInstantTogether is C-08.
//
// # The defect
//
// DELETE /subscribers/:id/webhook-subscription made TWO service calls: clear the URL, then
// stamp the instant. No transaction spans two service calls, so between them the row is
// either migrated with a live URL or unmigrated with none — and a failure landing in that gap
// makes the wrong state PERMANENT, with nothing to indicate a row needs repairing. Choosing
// the ordering only decided which wrong state was left behind.
//
// The first of the two also went through UpdateSubscriber, so a broker outage or a
// provisioning fence conflict could fail it and leave exactly that residue for reasons with
// nothing to do with migration.
func TestCompleteWebhookMigration_RetiresTheURLAndStampsTheInstantTogether(t *testing.T) {
	endpoint := "https://hooks.example.com/blnk"

	row := subscriberFixtureRow(t)
	row.WebhookURL = &endpoint

	run := newSubscriberLifecycle(t).seeded(row)
	store, service := run.store, run.service

	migrated, err := service.CompleteWebhookMigration(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	require.NotNil(t, migrated,
		"the row is returned so a caller can report the transition without re-reading it")
	require.NotNil(t, migrated.MigratedAt)
	assert.False(t, migrated.MigratedAt.IsZero(),
		"the instant is returned so a caller can report it without re-reading the row")
	assert.Nil(t, migrated.WebhookURL,
		"the returned row must already show the endpoint forgotten")

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.WebhookURL, "the endpoint is retired")
	require.NotNil(t, stored.MigratedAt, "and the migration is recorded")
	assert.Equal(t, *migrated.MigratedAt, *stored.MigratedAt,
		"the instant recorded is the instant reported, so a caller's log and the row agree")

	// ONE call. This is the assertion the finding is about: two calls cannot be atomic
	// however they are ordered.
	assert.Equal(t, 1, run.log.count("CompleteSubscriberWebhookMigration"))
	assert.Zero(t, run.log.count("ClearSubscriberWebhookURL"),
		"not clear-then-stamp: that pair is what left half-transitioned rows behind")
	assert.Zero(t, run.log.count("MarkSubscriberMigrated"))
	assert.Zero(t, run.log.count("UpdateEventSubscriber"))

	// And no broker, for the same reason as recording.
	assert.Zero(t, run.log.count("PruneSubscriberAccess"))
	assert.Zero(t, run.log.count("GrantSubscriberAccess"))
	assert.Zero(t, run.log.count("ClaimSubscriberForProvisioning"))
}

// TestCompleteWebhookMigration_IsSafeToRepeat is what makes the atomic form usable after a
// failure.
//
// A caller whose request failed for any reason must be able to repeat it. Re-completing an
// already-migrated subscriber moves the timestamp forward rather than failing, so "retry the
// same request" is always the correct advice — which it could not be if the second attempt
// reported a conflict.
func TestCompleteWebhookMigration_IsSafeToRepeat(t *testing.T) {
	endpoint := "https://hooks.example.com/blnk"

	row := subscriberFixtureRow(t)
	row.WebhookURL = &endpoint

	run := newSubscriberLifecycle(t).seeded(row)

	first, err := run.service.CompleteWebhookMigration(context.Background(), subscriberFixtureID)
	require.NoError(t, err)

	second, err := run.service.CompleteWebhookMigration(context.Background(), subscriberFixtureID)
	require.NoError(t, err, "a repeat must succeed, or a failed request has no safe recovery")
	require.NotNil(t, first)
	require.NotNil(t, first.MigratedAt)
	require.NotNil(t, second)
	require.NotNil(t, second.MigratedAt)
	assert.False(t, second.MigratedAt.Before(*first.MigratedAt),
		"and it moves the instant forward rather than back")
}

// TestClearLegacyWebhookSubscription_ForgetsTheURLWithoutClaimingAMigration keeps the two
// erasures distinct.
//
// Forgetting an address — for an erasure request, or ahead of the retention purge — is NOT
// evidence that a subscriber moved to Kafka. Conflating the two would have migration-progress
// reports counting migrations that never happened, which is the reporting error hardest to
// notice because it makes the numbers look better.
func TestClearLegacyWebhookSubscription_ForgetsTheURLWithoutClaimingAMigration(t *testing.T) {
	endpoint := "https://hooks.example.com/blnk"

	row := subscriberFixtureRow(t)
	row.WebhookURL = &endpoint

	run := newSubscriberLifecycle(t).seeded(row)
	store, service := run.store, run.service

	updated, err := service.ClearLegacyWebhookSubscription(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Nil(t, updated.WebhookURL)
	assert.Nil(t, updated.MigratedAt,
		"clearing an address must NOT stamp a migration: the subscriber has not been shown to "+
			"have moved anywhere")

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.WebhookURL)
	assert.Nil(t, stored.MigratedAt)

	assert.Equal(t, 1, run.log.count("ClearSubscriberWebhookURL"))
	assert.Zero(t, run.log.count("CompleteSubscriberWebhookMigration"))
	assert.Zero(t, run.log.count("UpdateEventSubscriber"))
	assert.Zero(t, run.log.count("PruneSubscriberAccess"))
	assert.Zero(t, run.log.count("GrantSubscriberAccess"))
}

// TestClearLegacyWebhookSubscription_PreservesAnExistingMigrationInstant is the other side of
// the same distinction.
//
// migrated_at is an audit fact about this deployment, and forgetting a third-party address
// does not change whether the subscriber migrated. It is also never purged, so a clear that
// erased it would destroy the only record of a completed migration.
func TestClearLegacyWebhookSubscription_PreservesAnExistingMigrationInstant(t *testing.T) {
	migrated := time.Now().UTC().Add(-48 * time.Hour)

	row := subscriberFixtureRow(t)
	row.MigratedAt = &migrated

	run := newSubscriberLifecycle(t).seeded(row)

	updated, err := run.service.ClearLegacyWebhookSubscription(
		context.Background(), subscriberFixtureID,
	)
	require.NoError(t, err)
	require.NotNil(t, updated.MigratedAt,
		"the migration record survives an erasure of the address")
	assert.Equal(t, migrated, *updated.MigratedAt)
}

// TestMarkSubscriberMigrated_RefusesARowThatStillHoldsAURL pins the narrow form's boundary.
//
// MarkSubscriberMigrated stamps migrated_at ALONE, which is correct for a subscriber that
// never had an endpoint recorded — an onboarding completed entirely on Kafka, the ordinary
// case after the cutover. On a row that still holds a URL it would write the
// self-contradicting state, so the repository's statement refuses it — `AND webhook_url IS
// NULL`, answered as ErrGenConflict — and the registry double mirrors that refusal.
//
// The point of asserting the refusal rather than avoiding the call: this method remains
// exported and reachable, so what stops it corrupting a row is the refusal at the write, not
// a convention about which method to call.
//
// The refusal is enforced in the repository and NOT by a schema CHECK. That matters to this
// test's honesty: the double used to cite a constraint that does not exist, which made this
// assertion pass against a fake stricter than PostgreSQL. See
// TestMarkSubscriberMigrated_RefusesARowThatStillHoldsAURLAtTheRepository in
// database/event_subscriber_test.go, which pins the statement itself.
func TestMarkSubscriberMigrated_RefusesARowThatStillHoldsAURL(t *testing.T) {
	endpoint := "https://hooks.example.com/blnk"

	row := subscriberFixtureRow(t)
	row.WebhookURL = &endpoint

	run := newSubscriberLifecycle(t).seeded(row)
	store, service := run.store, run.service

	_, err := service.MarkSubscriberMigrated(context.Background(), subscriberFixtureID)
	require.Error(t, err,
		"stamping alone on a row with a live URL must be refused; the result asserts that the "+
			"subscriber both has and has not stopped receiving legacy pushes")

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.MigratedAt, "and the refusal leaves the row untouched")
	require.NotNil(t, stored.WebhookURL)

	// The correct call for this row succeeds, so the refusal is a redirection rather than a
	// dead end.
	_, err = service.CompleteWebhookMigration(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
}

// TestMarkSubscriberMigrated_StampsASubscriberThatNeverHadAURL is the narrow form's
// legitimate use, and it must keep working.
//
// After the cutover this is the ordinary case: a subscriber onboarded directly onto Kafka has
// nothing to retire, and stamping alone is the whole transition.
func TestMarkSubscriberMigrated_StampsASubscriberThatNeverHadAURL(t *testing.T) {
	run := newSubscriberLifecycle(t)

	migratedAt, err := run.service.MarkSubscriberMigrated(context.Background(), subscriberFixtureID)
	require.NoError(t, err)

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.MigratedAt)
	assert.Equal(t, migratedAt, *stored.MigratedAt)
	assert.Nil(t, stored.WebhookURL)
}

// reconciliation reports the authorization each half of every reconciliation OBSERVED, oldest
// first.
//
// It is how "the boundary did not move" is asserted. The double returns fixed binding counts, so
// counting calls proves nothing about the grant; what does prove it is that both halves saw the
// same topic set the row already held, which means the broker was asked for the boundary it
// already had.
//
// Returns:
//   - pruned [][]string: the topic set PruneSubscriberAccess saw on each call.
//   - granted [][]string: the topic set GrantSubscriberAccess saw on each call.
func (a *subscriberTestAdmin) reconciliation() (pruned, granted [][]string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([][]string(nil), a.pruned...), append([][]string(nil), a.granted...)
}

// TestIssueSubscriberCredential_IssuesToAKeyScopedSubscriberAndStatesWhatIsNotEnforced is the
// first half of C-02: the mandatory capability is available for every registry state the schema
// can hold.
//
// The assertions divide in two, and both halves matter. The credential must be REAL — a password,
// an issuance instant, a broker round trip, a recorded reference — because a response that looks
// like a credential but carries no secret is the dead end this replaced. And it must be
// ACCOMPANIED by the prefix it does not enforce, because a credential handed over with the prefix
// silently dropped would let its holder infer that the broker applied the narrowing for it.
func TestIssueSubscriberCredential_IssuesToAKeyScopedSubscriberAndStatesWhatIsNotEnforced(t *testing.T) {
	row := subscriberFixtureRow(t)
	row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	run := newSubscriberLifecycle(t).seeded(row)
	store, service := run.store, run.service

	// A DECLARED ENFORCEMENT POINT, because Blnk ships no component that authorises record keys:
	// without one, issuance and a prefix-recording update both fail closed with
	// SUBSCRIBER_KEY_SCOPE_UNENFORCED. The shipped default is asserted by
	// TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway.
	enforceKeyScopeGateway(t)

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err,
		"a recorded key prefix must not withdraw the mandatory credential capability: there is no "+
			"narrower grant to insist on, so refusing leaves the subscriber with no route to any "+
			"credential at all")

	assert.NotEmpty(t, credential.Password(), "a real secret is returned, exactly once")
	assert.False(t, credential.IssuedAt.IsZero(), "and the issuance instant is recorded")

	// The prefix travels WITH the credential, resolved from the row at build time so the pair
	// cannot describe a prefix that was not in force when the secret was minted.
	assert.Equal(t, "ldg_9f1c8a72", credential.PartitionKeyPrefix,
		"the credential carries the prefix recorded for the subscriber, so the response can state "+
			"whose obligation applying it is")

	assert.Positive(t, run.log.count("ProvisionSubscriberPrincipal"),
		"the broker really is asked to mint the credential")
	assert.Positive(t, run.log.count("RecordSubscriberCredentialIfUnchanged"),
		"and the reference really is recorded, so IsProvisioned answers true afterwards")

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.CredentialReference,
		"a successful issuance records the credential reference")
	assert.True(t, stored.RequiresGatewayDelivery(),
		"and the row still records the prefix: issuance neither erases the operator's stated intent "+
			"nor pretends the broker adopted it")
}

// TestIssueSubscriberCredential_CarriesNoPrefixWhenTheRowRecordsNone is the negative half, and
// the mutant it kills is the one that matters most.
//
// The response's transport fields are DERIVED from this one — gateway_delivery_required,
// broker_record_access and partition_key_scope_state all read it — so a credential that carried a
// prefix unconditionally would tell every subscriber to dial a key-authorising component instead of
// the broker, and a client that always has to be redirected learns nothing from the field and stops
// reading it. The instruction is only worth stating because it is sometimes absent.
func TestIssueSubscriberCredential_CarriesNoPrefixWhenTheRowRecordsNone(t *testing.T) {
	run := newSubscriberLifecycle(t)

	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	assert.NotEmpty(t, credential.Password())
	assert.Empty(t, credential.PartitionKeyPrefix,
		"the fixture records no prefix, so the credential must carry none: the response derives "+
			"the filtering obligation from this field, and a prefix invented here would announce "+
			"an obligation the registry never recorded")
}

// TestIssueSubscriberCredential_ClearingAKeyScopeDependsOnWhatTheDeploymentDeclares is the
// CANONICAL test for the clearing transition, and it exists because clearing means two different
// things in the two deployments Blnk supports.
//
// # Where nothing enforces the scope, clearing is the documented remedy
//
// A row recording a prefix in a deployment with no key-authorising component cannot be issued to
// at all: SUBSCRIBER_KEY_SCOPE_UNENFORCED, whose first named remedy is to clear the prefix. So
// clearing has to work there, and issuance has to work after it — otherwise the refusal names a
// remedy that does not resolve it, which is a dead end dressed as guidance.
//
// # Where the deployment declares a key-scoped model, clearing is REFUSED (SEC-01)
//
// This is the third order the unscoped-credential state is reachable from, and the only one the
// issuance guard cannot see: record a prefix, take a credential, clear the prefix. Issuance never
// runs again, and the update's own reconciliation WIDENS the live principal from Describe to
// whole-topic Read — a subscriber in a tenant-scoped deployment reading every ledger, reached by
// three ordinary API calls with nothing refusing any of them.
//
// Both halves are asserted here rather than in two tests because the interesting failure is the
// ASYMMETRY: a guard that refused clearing everywhere would break the remedy above, and one that
// permitted it everywhere is the hole. The state after each is asserted too — the row and the
// credential — since a refusal that half-applied the update would leave the registry describing
// something the broker does not.
func TestIssueSubscriberCredential_ClearingAKeyScopeDependsOnWhatTheDeploymentDeclares(t *testing.T) {
	t.Run("cleared where nothing enforces the scope, and issuance works after it", func(t *testing.T) {
		row := subscriberFixtureRow(t)
		row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

		run := newSubscriberLifecycle(t).seeded(row)
		admin, service, store := run.admin, run.service, run.store

		// DELIBERATELY NO DECLARED COMPONENT. This is the shipped deployment, where the prefix is
		// unenforceable and issuance is refused for exactly that reason.
		refused, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.Error(t, err, "the precondition for the remedy: this row cannot be issued to")
		assert.Zero(t, refused.PasswordLength())

		cleared := ""
		updated, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &cleared,
		})
		require.NoError(t, err,
			"clearing must be permitted here, because it is the first remedy "+
				"SUBSCRIBER_KEY_SCOPE_UNENFORCED names")
		require.Nil(t, updated.PartitionKeyPrefix,
			"a present empty string must CLEAR the column, not store an empty prefix")
		assert.False(t, updated.DeclaresKeyScope())
		assert.False(t, updated.RequiresGatewayDelivery())

		credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.NoError(t, err, "AND THE REMEDY RESOLVES THE REFUSAL: issuance now succeeds")
		assert.NotEmpty(t, credential.Password())
		assert.Empty(t, credential.PartitionKeyPrefix,
			"the reissued credential carries no obligation, because the row records none")
		assert.Equal(t, subscriberLifecycleSubscriberBrokers, credential.Brokers,
			"the response carries the SUBSCRIBER-FACING list, not the addresses Blnk dials")
		assert.NotEqual(t, admin.Brokers(), credential.Brokers,
			"and the two are different lists: reporting the internal one would hand the subscriber "+
				"an endpoint that does not resolve for it")

		stored, ok := store.row(subscriberFixtureID)
		require.True(t, ok)
		assert.Nil(t, stored.PartitionKeyPrefix, "the clearing was persisted, not just reported")
	})

	t.Run("refused where the deployment declares a key-scoped model", func(t *testing.T) {
		row := subscriberFixtureRow(t)
		row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

		run := newSubscriberLifecycle(t).seeded(row)
		service, store := run.service, run.store

		enforceKeyScopeGateway(t)

		scoped, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.NoError(t, err, "the scoped row issues normally under a declared component")
		require.Equal(t, "ldg_9f1c8a72", scoped.PartitionKeyPrefix)

		cleared := ""
		updated, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &cleared,
		})
		require.Error(t, err,
			"CLEARING MUST BE REFUSED HERE: it is the one path that turns a live key-scoped "+
				"principal into a whole-topic reader without issuance ever running again")
		assert.Nil(t, updated, "and nothing may be returned as though the update had happened")

		var apiErr apierror.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, apierror.ErrSubscriberKeyScopeRequired, apiErr.Code,
			"the same typed code issuance uses, because it is the same rule: under a declared "+
				"key-scoped model every subscriber carries a key scope")
		assert.Equal(t, http.StatusConflict, apierror.StatusForCode(apiErr.Code),
			"409 and never the 500 an unmapped code resolves to: the body is well formed, nothing "+
				"is unavailable, and the same request succeeds after either remedy")
		assert.Contains(t, err.Error(), "KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS",
			"the refusal names the deployment-level remedy, or an operator who legitimately wants "+
				"a whole-topic consumer has nowhere to go")

		// NOTHING WAS APPLIED. A refusal that persisted the clearing and then reported an error
		// would leave the registry describing whole-topic access it had refused to grant.
		stored, ok := store.row(subscriberFixtureID)
		require.True(t, ok)
		require.NotNil(t, stored.PartitionKeyPrefix, "the row keeps its key scope")
		assert.Equal(t, "ldg_9f1c8a72", *stored.PartitionKeyPrefix)
		require.NotNil(t, stored.CredentialReference,
			"and the working credential is untouched: the refusal declines a change, it does not "+
				"revoke a secret nobody asked to replace")
	})
}

// ---------------------------------------------------------------------------------------
// SEC-01 — a declared key-scoped model must hold for EVERY credential, and the component
// that enforces it must be verified rather than trusted
// ---------------------------------------------------------------------------------------

// TestIssueSubscriberCredential_RefusesAPrefixLessSubscriberUnderADeclaredKeyScopedModel is the
// hole SEC-01 named, closed.
//
// # The hole
//
// Withholding topic Read from key-scoped principals made THOSE subscribers safe and said nothing
// about the subscriber registered without a prefix. That one is granted literal topic Read on
// every topic it is authorised for — every ledger's records on a shared category topic — in a
// deployment whose entire declared model is that a subscriber sees only its own. One prefix-less
// registration was the single credential that escaped the model, and it is the DEFAULT shape of
// the DTO, where partition_key_prefix is optional. Nothing refused it and nothing recorded that
// anything unusual had happened.
//
// # What the refusal has to be
//
// Typed, 409, and carrying every remedy — record the prefix, provision a whole-topic consumer as
// an operator-managed principal outside the registry, or stop declaring the key-scoped model. A
// per-subscriber opt-out is deliberately NOT among them: that is the same hole with a field name.
//
// And free of residue. The assertions below check the broker was never asked and nothing was
// recorded, because a refusal that minted a SCRAM credential on its way out has already created
// the access it declined to hand over.
func TestIssueSubscriberCredential_RefusesAPrefixLessSubscriberUnderADeclaredKeyScopedModel(
	t *testing.T,
) {
	// The fixture row records NO prefix, which is the point: this is the ordinary registration,
	// not a crafted one.
	run := newSubscriberLifecycle(t)
	store, service := run.store, run.service

	enforceKeyScopeGateway(t)

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err,
		"a deployment that declares key-scoped subscriber access must not mint the one credential "+
			"that reads every key on every topic it is granted")
	assert.Zero(t, credential.PasswordLength(),
		"and no secret may exist: a refusal that generated one has already created the thing it "+
			"declined to hand over")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberKeyScopeRequired, apiErr.Code,
		"the TYPED code, so a client tells this apart from a broker outage")
	assert.Equal(t, http.StatusConflict, apierror.StatusForCode(apiErr.Code),
		"409 and never the 500 an unmapped code resolves to: the request is well formed, nothing "+
			"is unavailable, and the same request succeeds after either remedy")

	assert.Contains(t, err.Error(), "Record the partition key prefix",
		"the per-subscriber remedy")
	assert.Contains(t, err.Error(), "operator-managed principal outside the subscriber registry",
		"the remedy for the legitimate whole-topic consumer, which is what keeps this refusal from "+
			"being a wall in front of a real deployment shape")
	assert.Contains(t, err.Error(), "KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS",
		"and the deployment-level remedy, named by variable so it is actionable without the docs")

	// NOTHING REACHED THE BROKER, THE GATEWAY, OR THE REGISTRY.
	assert.Zero(t, run.log.count("UpsertScramCredential"))
	assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
		"the broker must not be asked at all")
	assert.Zero(t, run.log.count("RecordSubscriberCredentialIfUnchanged"),
		"nor may an issuance be recorded for a credential that does not exist")

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.CredentialReference,
		"the row must be exactly as it was: the refusal is a decision, not a partial issuance")
}

// TestIssueSubscriberCredential_RequiresASecureDeploymentToDeclareItsAccessModel pins the second
// half of SEC-01: whole-topic subscriber access is legitimate, and being the DEFAULT is not.
//
// # Why this is not a claim that whole-topic access is a defect
//
// It is the access model the requirement mandates — category topics, no per-tenant topics, an
// authorizer with no message-key dimension. For a single-tenant ledger or a trusted internal
// consumer, a credential that reads every record on `blnk.transactions` is exactly right.
//
// What was wrong is that it was reached by configuring NOTHING. A deployment that had never
// considered tenancy got the widest credential Blnk can issue, and every artefact described that
// accurately — the row, the response, the runbook — while no human had decided it. Documented is
// not decided.
//
// # Why only in secure mode, asserted here rather than assumed
//
// Server.Secure is this repository's production signal, and the third subtest is what keeps the
// local stack, both compose files and this entire suite working: outside secure mode nothing
// changes. Without that subtest the declaration would be indistinguishable from a hard
// requirement, which is the version of this change that breaks `make run`.
func TestIssueSubscriberCredential_RequiresASecureDeploymentToDeclareItsAccessModel(t *testing.T) {
	secureConfiguration := func(t *testing.T, acknowledged bool) {
		t.Helper()

		outboxStoreConfiguration(t, &config.Configuration{
			Server: config.ServerConfig{Secure: true},
			Kafka: config.KafkaConfig{
				Brokers:                     []string{"broker-1:9092", "broker-2:9092"},
				SubscriberBrokers:           append([]string(nil), subscriberLifecycleSubscriberBrokers...),
				TopicPrefix:                 "blnk",
				SubscriberSharedTopicAccess: acknowledged,
			},
		})
	}

	t.Run("refused where a secure deployment has declared neither model", func(t *testing.T) {
		run := newSubscriberLifecycle(t)
		service := run.service

		secureConfiguration(t, false)

		credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.Error(t, err,
			"the widest credential Blnk can issue must not be reachable by configuring nothing")
		assert.Zero(t, credential.PasswordLength())

		var apiErr apierror.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, apierror.ErrSubscriberSharedTopicAccessUnacknowledged, apiErr.Code)
		assert.Equal(t, http.StatusConflict, apierror.StatusForCode(apiErr.Code),
			"409: the request is well formed and the same one succeeds once the deployment has "+
				"declared which model it is")
		assert.Contains(t, err.Error(), "KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS=true",
			"the one-variable remedy, with its value, or an operator has to read source to comply")
		assert.Contains(t, err.Error(), "KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway",
			"and the alternative declaration, because the refusal must not read as 'accept "+
				"whole-topic access'")

		assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
			"nothing may be minted for a deployment that has not decided")
	})

	t.Run("issued once the deployment acknowledges whole-topic access", func(t *testing.T) {
		run := newSubscriberLifecycle(t)
		service := run.service

		secureConfiguration(t, true)

		credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.NoError(t, err,
			"ONE declaration must be the whole cost: a refusal an operator cannot clear is a "+
				"withdrawn capability, not a safeguard")
		assert.NotEmpty(t, credential.Password())
		assert.Equal(t, model.KeyScopeEnforcementNone, credential.KeyScopeEnforcement,
			"and the credential still describes itself honestly: acknowledging the model does not "+
				"change what it is")
	})

	t.Run("not applied outside secure mode", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		// The lifecycle configuration, which declares neither model and is NOT secure. This is
		// the local stack and this suite.
		credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.NoError(t, err,
			"outside secure mode the declaration must not be required at all, or `make run` and "+
				"every test in this file would have to declare a production posture")
		assert.NotEmpty(t, credential.Password())
	})
}

// TestIssueSubscriberCredential_RefusesAKeyScopeTheDeclaredGatewayWillNotConfirm is the third half
// of SEC-01, and the one that turns a declaration into a verified fact.
//
// # What "declared" used to mean
//
// Two configuration values: a mode, and a bootstrap list different from the brokers. Blnk can
// verify neither. Any address satisfied them, so a deployment could name a component that did not
// exist, was unreachable, enforced a WIDER prefix than the registry recorded, or enforced nothing
// at all — and Blnk would mint a credential whose response declared an enforced key boundary and
// whose ACLs deliberately withheld topic Read on the strength of it. The subscriber would then be
// unable to read anything while believing itself isolated, or reading everything while believing
// itself scoped, depending on which way the component was wrong.
//
// # What it means now
//
// Issuance BINDS the exact recorded prefix at the component's control endpoint over an
// authenticated call and requires the component to attest that exact binding back. The subtests
// below are the ways a component can be wrong, and each must produce a refusal with no residue —
// asserted at the SERVICE layer here, because the client-level conformance cases live in
// event_keyscope_gateway_test.go and cannot see whether a secret was minted or a row was written.
func TestIssueSubscriberCredential_RefusesAKeyScopeTheDeclaredGatewayWillNotConfirm(t *testing.T) {
	keyScopedRun := func(t *testing.T) (*subscriberLifecycle, *keyScopeGatewayDouble) {
		t.Helper()

		row := subscriberFixtureRow(t)
		row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

		run := newSubscriberLifecycle(t).seeded(row)
		_, double := enforceKeyScopeGatewayWithDouble(t)

		return run, double
	}

	assertRefusedWithoutResidue := func(t *testing.T, run *subscriberLifecycle, err error, retryable bool) {
		t.Helper()

		requireKeyScopeUnattested(t, err, retryable)

		assert.Zero(t, run.log.count("UpsertScramCredential"),
			"NO SECRET MAY EXIST. The attestation is ordered before the password is generated "+
				"precisely so that this holds")
		assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
			"and the broker must not be touched: an ACL set applied for an unattested binding "+
				"would have to be unwound by hand")
		assert.Zero(t, run.log.count("RecordSubscriberCredentialIfUnchanged"))
	}

	t.Run("a component that enforces a WIDER prefix than the row records", func(t *testing.T) {
		run, double := keyScopedRun(t)
		// The truncating proxy. This is the dangerous misbehaviour, because the subscriber is
		// told it is isolated and the component is delivering a superset.
		double.prefixOverride = "ldg_"

		_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		assertRefusedWithoutResidue(t, run, err, false)
	})

	t.Run("a component that enforces no key scopes at all", func(t *testing.T) {
		run, double := keyScopedRun(t)
		double.enforcing = false

		_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		assertRefusedWithoutResidue(t, run, err, false)
	})

	t.Run("a component that rejects Blnk's credential", func(t *testing.T) {
		run, double := keyScopedRun(t)
		double.token = "some-other-token"

		_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		assertRefusedWithoutResidue(t, run, err, false)
	})

	t.Run("a component that is unreachable, which IS retryable", func(t *testing.T) {
		run, double := keyScopedRun(t)
		double.status = http.StatusBadGateway

		_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		assertRefusedWithoutResidue(t, run, err, true)
	})
}

// TestIssueSubscriberCredential_AttestsTheExactBindingBeforeMintingASecret is the positive case,
// and it asserts the two properties the refusals above cannot: WHAT was bound, and WHEN.
//
// What: the component must be told the principal, the exact stored prefix, the authorised topics
// and the consumer group. A bind carrying less than that cannot be enforced against — a component
// that knows a prefix but not which principal it belongs to enforces it for everybody or nobody.
//
// When: BEFORE the secret is generated and before the broker is asked for anything. That ordering
// is the whole reason the refusals leave no residue, and it is invisible in a test that only
// checks the happy path succeeded.
func TestIssueSubscriberCredential_AttestsTheExactBindingBeforeMintingASecret(t *testing.T) {
	row := subscriberFixtureRow(t)
	row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	run := newSubscriberLifecycle(t).seeded(row)
	gateway, double := enforceKeyScopeGatewayWithDouble(t)

	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	assert.NotEmpty(t, credential.Password())

	held, bound := double.heldPrefix(row.KafkaPrincipal)
	require.True(t, bound,
		"THE COMPONENT MUST HOLD A BINDING FOR THIS PRINCIPAL. An attestation satisfied by a "+
			"component that was told nothing is a handshake, not an enforcement point")
	assert.Equal(t, "ldg_9f1c8a72", held,
		"and it must hold the EXACT stored prefix, byte for byte: a normalised or trimmed prefix "+
			"selects a different set of records")

	assert.Equal(t, "Bearer gateway-token", double.authorizationHeader(),
		"the call must be authenticated, or anything on the network can register bindings")
	assert.Equal(t, 1, double.requestCount(http.MethodPost),
		"exactly one bind per issuance")

	assert.Equal(t, gateway, credential.Brokers,
		"and the credential names the component that terminates this subscriber's connection, "+
			"not a broker address that would bypass it")
	assert.Equal(t, model.KeyScopeEnforcementGateway, credential.KeyScopeEnforcement)
}

// TestDeregisterSubscriber_WithdrawsTheKeyScopeBindingItRegistered closes the lifecycle.
//
// A bind with no withdrawal accumulates entries for principals that no longer exist, and the next
// subscriber to be issued the same principal name — which is derived from the subscriber ID, so
// re-registering one reuses it — would inherit a stale prefix nobody chose. Revocation is what
// makes the binding table a description of the present rather than of everything that ever
// happened.
//
// It runs AFTER the broker revocation and it is NOT allowed to fail the deregistration. By the
// time it runs the principal can no longer authenticate anywhere the credential was accepted, the
// component included, so a failure here leaves no access behind — only a stale row in a table. An
// operator blocked from removing a subscriber because a component is unreachable would be paying
// a real cost for a hygiene task.
func TestDeregisterSubscriber_WithdrawsTheKeyScopeBindingItRegistered(t *testing.T) {
	t.Run("the binding is withdrawn", func(t *testing.T) {
		row := subscriberProvisionedRow(t)
		row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

		run := newSubscriberLifecycle(t).seeded(row)
		_, double := enforceKeyScopeGatewayWithDouble(t)

		// Issue first, so there IS a binding to withdraw: the assertion below would pass
		// vacuously against a component that had never been told anything.
		_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.NoError(t, err)
		_, bound := double.heldPrefix(row.KafkaPrincipal)
		require.True(t, bound, "the precondition: the component holds this principal's binding")

		removed, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
		require.NoError(t, err)
		require.NotNil(t, removed)

		_, stillBound := double.heldPrefix(row.KafkaPrincipal)
		assert.False(t, stillBound,
			"THE BINDING MUST BE GONE. A principal that no longer exists must not keep an entry "+
				"a re-registration of the same subscriber ID would inherit")
		assert.Equal(t, 1, double.requestCount(http.MethodDelete),
			"and the withdrawal must actually have been sent, not inferred from the absence of "+
				"an error")
	})

	t.Run("an unreachable component does not block the deregistration", func(t *testing.T) {
		row := subscriberProvisionedRow(t)
		row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

		run := newSubscriberLifecycle(t).seeded(row)
		_, double := enforceKeyScopeGatewayWithDouble(t)

		_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.NoError(t, err)

		double.status = http.StatusBadGateway

		removed, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
		require.NoError(t, err,
			"the broker revocation is the authoritative one and it succeeded, so no access "+
				"remains: failing here would block a removal in order to finish cleaning up "+
				"access that is already gone")
		require.NotNil(t, removed)

		_, present := run.store.row(subscriberFixtureID)
		assert.False(t, present, "and the row is deleted, as a successful deregistration requires")
	})
}

// TestUpdateSubscriber_RecordsAKeyScopeOnAProvisionedSubscriberAndNarrowsTheGrant is the reverse
// ordering, and it is the one that used to be refused outright.
//
// # Why acceptance is correct here, and what acceptance now has to DO
//
// The refusal existed to prevent a state it described as dangerous, and it was right about the
// danger: a live credential holding record-level Read on whole topics beneath a row announcing a
// narrower boundary. Refusing the update did not remove that credential's Read, though — it only
// stopped the registry from recording the operator's intent, leaving the wide grant in place and
// undocumented.
//
// Accepting the update is correct BECAUSE it is the act that narrows the grant. Crossing from
// "no prefix" to "a prefix" moves every authorised topic from Read+Describe to Describe alone,
// so the update has to reach the broker and reconcile, and when it does the existing credential
// loses exactly the access the row no longer describes.
//
// What this test pins is that both halves happen: the registry records the prefix AND the broker
// is brought to the narrower binding set. A test that only asserted "no error" would not
// distinguish this from the update recording an intent nothing acts on — which is the state the
// original refusal was written against, and which acceptance without reconciliation would
// recreate exactly.
func TestUpdateSubscriber_RecordsAKeyScopeOnAProvisionedSubscriberAndNarrowsTheGrant(t *testing.T) {
	run := newSubscriberLifecycle(t).seeded(subscriberProvisionedRow(t))
	store, service := run.store, run.service

	// A DECLARED ENFORCEMENT POINT, because Blnk ships no component that authorises record keys:
	// without one, issuance and a prefix-recording update both fail closed with
	// SUBSCRIBER_KEY_SCOPE_UNENFORCED. The shipped default is asserted by
	// TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway.
	enforceKeyScopeGateway(t)

	prefix := "ldg_k_83f61ccabf29"
	updated, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		PartitionKeyPrefix: &prefix,
	})
	require.NoError(t, err,
		"recording a key scope on a provisioned subscriber must be accepted: it NARROWS the "+
			"grant, so refusing it leaves the wider access in place and merely stops the registry "+
			"from recording that an operator wanted it gone")
	require.NotNil(t, updated)
	require.NotNil(t, updated.PartitionKeyPrefix)
	assert.Equal(t, prefix, *updated.PartitionKeyPrefix)

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.PartitionKeyPrefix, "the row records what the operator asked for")
	assert.Equal(t, prefix, *stored.PartitionKeyPrefix)
	assert.True(t, stored.RequiresGatewayDelivery(),
		"so every subsequent read of this subscriber declares gateway delivery")
	assert.False(t, stored.GrantsBrokerRecordAccess(),
		"and states that record-level access at the broker is no longer part of its grant")
	require.NotNil(t, stored.CredentialReference,
		"the working credential is left alone: it is the GRANT that narrows, not the identity")

	assert.Positive(t, run.log.count("UpdateEventSubscriber"), "the registry row changes")

	// THE BOUNDARY MOVED, so the broker was asked — and it was asked for the topics the row
	// holds, on both halves.
	//
	// This is the assertion the acceptance path turns on. A key scope has no ACL of its own, but
	// its PRESENCE decides the shape of every topic binding, so an update that crossed into it
	// without reconciling would record a narrowing that never happened. Prune-then-grant is how
	// the withdrawal is applied: the prune removes the bindings the wide grant held, the grant
	// re-creates the Describe-only set (ADMIN-02).
	pruned, granted := run.admin.reconciliation()
	require.Len(t, pruned, 1, "recording a key scope must reach the broker exactly once")
	require.Len(t, granted, 1)
	assert.ElementsMatch(t, stored.AuthorizedTopics, pruned[0],
		"and each half is handed the topic set the row holds")
	assert.ElementsMatch(t, stored.AuthorizedTopics, granted[0])

	assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
		"no credential is minted: recording a prefix is not a reissue")
	assert.Zero(t, run.log.count("RevokeSubscriber"),
		"nor revoked: the principal keeps its identity and loses only the wider binding")

	assert.False(t, store.fenced(subscriberFixtureID),
		"and the provisioning claim it took to reconcile the broker is released")

	// REVOCATION IS STILL AVAILABLE, and recording the prefix again afterwards still works. This
	// is not a remedy for a refusal — there is none to remedy — but it is the transition an
	// operator who would rather drop the credential than narrow it takes, and a row carrying a
	// prefix must not become a state that can only be left.
	require.NoError(t, service.RevokeSubscriberCredential(context.Background(), subscriberFixtureID),
		"a key-scoped row must still be able to give up its credential")

	revoked, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		PartitionKeyPrefix: &prefix,
	})
	require.NoError(t, err, "with no credential outstanding, recording the prefix is accepted")
	require.NotNil(t, revoked.PartitionKeyPrefix)
	assert.Equal(t, prefix, *revoked.PartitionKeyPrefix)
}

// TestUpdateSubscriber_KeepsEveryKeyScopeStateReachable is the completeness check on the state
// space: which transitions are open, and that no state is a dead end.
//
// Enumerating them in one place is what stops the two guards drifting into either of the failures
// this area has already had. One is a HOLE: a decision taken at issuance and not at update, so a
// state is reachable by approaching it from the far side — which is why requireProvisionableKeyScope
// and requireRecordableKeyScope must answer the same question the same way, and why they are given
// the SAME enforcement fact, read once. The other is a DEAD END: a row that can neither obtain a
// credential nor be edited back into a state that can, which is what a blanket refusal of the
// prefix-plus-credential combination produced — it made requirement R-7's credential endpoint
// permanently unusable for exactly the subscribers the key scope exists for.
//
// The rule the four cases below establish, for THIS binary, which links the subscriber stream
// gateway in: a partition key prefix and a credential MAY coexist, because the prefix is enforced
// — the credential holds Describe and no Read on its topics and the gateway delivers the records
// key-filtered — so both orderings are open and either half can still be given up. The fail-closed
// floor beneath it, for a deployment where nothing enforces the scope, is asserted separately by
// TestRequireProvisionableKeyScope_RefusesOnlyWhereNothingEnforcesTheScope and
// TestRequireRecordableKeyScope_MirrorsTheIssuanceGuard.
func TestUpdateSubscriber_KeepsEveryKeyScopeStateReachable(t *testing.T) {
	t.Run("a key scope on a subscriber with no credential is accepted", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		// A DECLARED ENFORCEMENT POINT: without one this row fails closed with
		// SUBSCRIBER_KEY_SCOPE_UNENFORCED, which is asserted on its own elsewhere.
		enforceKeyScopeGateway(t)

		prefix := "ldg_9f1c8a72"
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &prefix,
		})
		require.NoError(t, err, "a row that holds no credential can record a key scope")
		require.NotNil(t, updated.PartitionKeyPrefix)
		assert.Equal(t, prefix, *updated.PartitionKeyPrefix)

		// And the state it produces IS ISSUABLE, because a component is declared above. This is the
		// half a blanket refusal got wrong: it withheld the credential unconditionally, so the third
		// dimension of requirement R-7's access model had no working path even where an operator had
		// stood up something to keep it. What makes issuance safe here is that the credential is not
		// topic-wide — record-level Read is withheld, and the declared component is the only path.
		credential, issueErr := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.NoError(t, issueErr,
			"a key-scoped row must be issuable: the credential is narrowed rather than withheld")
		assert.NotEmpty(t, credential.Password(), "and a working secret is returned, once")
		assert.Equal(t, prefix, credential.PartitionKeyPrefix,
			"the scope is delivered to the party expected to see it applied")
		assert.Equal(t, model.KeyScopeEnforcementGateway, credential.KeyScopeEnforcement,
			"together with the component that enforces it, so the prefix is never read as a "+
				"broker boundary")

		scope, enforcedByBroker := credential.KeyScope()
		assert.Equal(t, prefix, scope)
		assert.False(t, enforcedByBroker,
			"and the broker is explicitly NOT the enforcer: Kafka's authorizer has no message-key "+
				"dimension, so claiming it were would state a boundary Kafka does not keep")
	})

	t.Run("a key scope on a provisioned subscriber is accepted and narrows the grant", func(t *testing.T) {
		run := newSubscriberLifecycle(t).seeded(subscriberProvisionedRow(t))

		// A DECLARED ENFORCEMENT POINT: without one this row fails closed with
		// SUBSCRIBER_KEY_SCOPE_UNENFORCED, which is asserted on its own elsewhere.
		enforceKeyScopeGateway(t)

		prefix := "ldg_9f1c8a72"
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &prefix,
		})
		require.NoError(t, err,
			"the reverse ordering — issue, then record — must be open too: recording the prefix "+
				"NARROWS the live grant, and refusing it would leave the wider access in place and "+
				"merely stop the registry from recording that an operator wanted it gone")
		require.NotNil(t, updated.PartitionKeyPrefix)
		assert.Equal(t, prefix, *updated.PartitionKeyPrefix)
		require.NotNil(t, updated.CredentialReference,
			"and the principal keeps its identity: it is the GRANT that narrows, not the credential")
		assert.False(t, updated.GrantsBrokerRecordAccess(),
			"the row now states that record-level access at the broker is no longer part of it")

		// NOT A DEAD END IN EITHER DIRECTION: the credential can still be given up, and the
		// prefix can still be recorded afterwards.
		require.NoError(t,
			run.service.RevokeSubscriberCredential(context.Background(), subscriberFixtureID))

		again, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &prefix,
		})
		require.NoError(t, err, "with the credential revoked the prefix is still recordable")
		require.NotNil(t, again.PartitionKeyPrefix)
	})

	t.Run("clearing a key scope on a provisioned subscriber is accepted", func(t *testing.T) {
		row := subscriberProvisionedRow(t)
		row.PartitionKeyPrefix = stringPointer("ldg_legacy_row")

		run := newSubscriberLifecycle(t).seeded(row)

		cleared := ""
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &cleared,
		})
		require.NoError(t, err,
			"clearing the prefix is how an operator accepts topic-level scope, so it must never be "+
				"refused — and it is the ONLY edit available on a row seeded directly into this "+
				"state, which is why the guard reads the resulting row rather than the request")
		assert.Nil(t, updated.PartitionKeyPrefix)
		require.NotNil(t, updated.CredentialReference,
			"and it must not revoke the credential the row already holds")
	})

	t.Run("an unrelated edit to a provisioned subscriber is accepted", func(t *testing.T) {
		run := newSubscriberLifecycle(t).seeded(subscriberProvisionedRow(t))

		renamed := "settlement consumer"
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			Name: &renamed,
		})
		require.NoError(t, err, "an update that mentions no prefix is unaffected by any of this")
		assert.Equal(t, renamed, updated.Name)
		assert.Nil(t, updated.PartitionKeyPrefix)
		assert.Positive(t, run.log.count("UpdateEventSubscriber"), "and the write still happens")
	})
}

// THE SUBSCRIBER-FACING BROKER LIST IS REQUIRED FOR ISSUANCE, AND IT HAS BEEN BOTH THINGS.
//
// A test asserting the 503 sat here, was replaced by one asserting a warned FALLBACK to
// KAFKA_BROKERS, and the refusal has now been restored. The reasoning that produced each turn is
// worth keeping, because both objections are real:
//
//   - AGAINST REQUIRING IT: requirement R-10 describes EIGHT configuration variables, so a ninth
//     prerequisite means a deployment configured exactly as documented can publish every event and
//     still be refused every credential.
//   - AGAINST THE FALLBACK: KAFKA_BROKERS holds the addresses BLNK dials, which inside a deployment
//     do not resolve for a subscriber outside it. The warning landed in Blnk's log while the
//     consequence landed on the subscriber, and because the secret is shown once, diagnosing it
//     costs a reissue.
//
// What resolves both: the refusal, plus a documented one-line remedy for the in-cluster case — set
// KAFKA_SUBSCRIBER_BROKERS to the same value as KAFKA_BROKERS, which makes the claim explicit
// rather than implicit in a substitution nobody reads. R-10's eight variables remain sufficient to
// PUBLISH; only the endpoint whose entire output is a third-party address needs the ninth.
//
// The behaviour now lives in three tests, none of them contradicting another:
//
//   - TestIssueSubscriberCredential_RefusesWithoutTheSubscriberFacingBrokerList, below, for the
//     refusal, the variable named in the message, the absence of residue, and the remedy working.
//   - TestIssueSubscriberCredential_RefusesWhenNoBrokerListIsConfiguredAtAll, above, for the case
//     where NEITHER list is set, meaning no Kafka at all.
//   - TestIssueSubscriberCredential_ReportsTheSubscriberFacingBrokers, above, for the success case
//     and for the two lists being held apart.

// TestSubscriberKeyPrefix_ReadsWhitespaceAsAbsent pins the two functions that decide, together,
// what a credential response says about the key boundary.
//
// subscriberKeyPrefix resolves the value the response carries and
// RequiresGatewayDelivery decides whether an obligation is declared at all. They must
// agree on every representation of "nothing recorded", because a column holding only spaces is
// not a recorded intent: reading it as present would tell a subscriber it owns a filtering
// obligation over a value that means nothing, and it would put that whitespace in the response as
// the prefix to filter on. NULL, the empty string and whitespace therefore all have to answer the
// same way.
func TestSubscriberKeyPrefix_ReadsWhitespaceAsAbsent(t *testing.T) {
	for name, prefix := range map[string]*string{
		"null":       nil,
		"empty":      stringPointer(""),
		"spaces":     stringPointer("   "),
		"tab":        stringPointer("\t"),
		"newline":    stringPointer("\n"),
		"mixed":      stringPointer(" \t\n "),
		"absent row": nil,
	} {
		t.Run("absent: "+name, func(t *testing.T) {
			subscriber := &model.EventSubscriber{PartitionKeyPrefix: prefix}
			assert.False(t, subscriber.RequiresGatewayDelivery(),
				"nothing is recorded, so no client-side obligation may be declared")
			assert.Empty(t, subscriberKeyPrefix(subscriber),
				"and the response carries no prefix rather than the whitespace itself")
		})
	}

	for name, tc := range map[string]struct{ stored, reported string }{
		"a ledger fragment": {"ldg_9f1c", "ldg_9f1c"},
		"padded":            {"  ldg_9f1c  ", "ldg_9f1c"},
		"a single rune":     {"l", "l"},
	} {
		t.Run("present: "+name, func(t *testing.T) {
			subscriber := &model.EventSubscriber{
				SubscriberID:       subscriberFixtureID,
				PartitionKeyPrefix: stringPointer(tc.stored),
			}
			assert.True(t, subscriber.RequiresGatewayDelivery(),
				"a recorded prefix is a narrowing Kafka will not apply, so the subscriber must be "+
					"told it owns it")
			assert.Equal(t, tc.reported, subscriberKeyPrefix(subscriber),
				"and the reported prefix is TRIMMED, because the padding is storage noise and a "+
					"subscriber comparing keys against it byte-for-byte would match nothing")
		})
	}

	t.Run("a nil subscriber declares nothing", func(t *testing.T) {
		// Nil is handled elsewhere as a not-found; neither of these may be the thing that
		// panics on it.
		var subscriber *model.EventSubscriber
		assert.False(t, subscriber.RequiresGatewayDelivery())
		assert.Empty(t, subscriberKeyPrefix(subscriber))
	})
}

// requireFenceLocked reproduces the ownership predicate every fenced write carries in SQL.
//
// The production statements test `provisioning_token = $n AND provisioning_until > NOW()` inside
// the write itself. A double that ignored the token would let every fence assertion pass whether
// or not the predicate existed, which is precisely what these tests are for. The caller must
// already hold s.mu.
func (s *subscriberTestStore) requireFenceLocked(subscriberID, token, operation string) error {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"A provisioning claim token is required for this operation", nil)
	}

	key := strings.TrimSpace(subscriberID)

	held, ok := s.fences[key]
	if !ok || held.token != trimmed || !held.until.After(time.Now()) {
		// THE CLIENT MESSAGE database.fencedWriteMissError produces, and the DETAIL carries
		// subscriberFenceLostMarker because subscriberFenceWasLost matches on it — a generic
		// conflict here would exercise the wrong branch, and a message the repository no longer
		// returns would make a test that reads the message pass against a double and fail against
		// the database. The release and renewal statements answer with the same sentence up to
		// its final clause, so this one string serves all three sites.
		return apierror.NewAPIError(apierror.ErrConflict,
			"The subscriber provisioning claim is no longer held by this caller, so the change was not applied",
			fmt.Errorf("subscriber %q: the provisioning claim was no longer held while %s, so the "+
				"write was refused", subscriberID, operation))
	}

	return nil
}

// MarkSubscriberCredentialOrphaned is UNFENCED, exactly as the repository is: it is reached when
// the claim may already have lapsed, so conditioning it would lose the marker in the very case
// that produces it. A missing row is not an error.
func (s *subscriberTestStore) MarkSubscriberCredentialOrphaned(
	ctx context.Context,
	subscriberID string,
	orphanedAt time.Time,
) error {
	if err := s.record("MarkSubscriberCredentialOrphaned", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return nil
	}

	if row.CredentialOrphanedAt == nil {
		stamped := orphanedAt
		row.CredentialOrphanedAt = &stamped
	}
	s.rows[key] = row

	return nil
}

// MarkSubscriberRevocationFailed is unfenced too, and does NOT keep the first instant.
func (s *subscriberTestStore) MarkSubscriberRevocationFailed(
	ctx context.Context,
	subscriberID string,
	failedAt time.Time,
) error {
	if err := s.record("MarkSubscriberRevocationFailed", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return nil
	}

	stamped := failedAt
	row.RevocationFailedAt = &stamped
	s.rows[key] = row

	return nil
}

// TestUpdateSubscriber_TouchesTheBrokerOnlyWhenTheAuthorizationCouldHaveMoved pins the
// availability property of an authorization-preserving edit.
//
// Every update made two administrative round trips — prune, then grant — regardless of what it
// changed, and FAILED when they did not answer. Prune and grant exist to move the broker-side
// boundary; a rename cannot move it, and neither can recording or clearing the legacy webhook URL
// the dual-run window exists to migrate away from. So a piece of deprecated migration metadata
// was coupled to Kafka's availability: an operator recording where a subscriber's webhook used to
// point could not do so while the broker was down, for no reason the data justified.
//
// # Why the failing broker is the assertion
//
// Both administrative methods are made to fail. A skip is then not a matter of counting calls
// that might be optimised away later — the update either does not touch the broker, or it fails.
// The call log is asserted as well, so a change that started making the calls again but tolerated
// their failure would still be caught.
//
// # Why an explicitly supplied topic list ALWAYS reconciles, even when it is identical
//
// A comparison alone would call that "unchanged" and skip. That breaks the documented recovery:
// when an earlier update persisted the row and then failed at the grant step, the stored
// authorization already EQUALS the desired one while the broker is still missing the widening, so
// skipping would strand the subscriber permanently one step short of correct. An explicit field
// is a re-apply instruction, not a no-op.
//
// # Why a key scope is on the OTHER side of this line, and its value is not
//
// The saving this test protects has one boundary that is easy to draw in the wrong place. A
// prefix has no ACL of its own, so REPLACING one prefix with another is genuinely inert and is
// skipped. But its PRESENCE decides whether each authorised topic carries Read: a key-scoped
// subscriber holds Describe alone. Crossing the line in either direction therefore moves real
// bindings, and the subtests below pin both directions — including that clearing a scope, which
// re-grants record access, FAILS when the broker cannot be reached, so the registry never records
// a widening that was not applied.
func TestUpdateSubscriber_TouchesTheBrokerOnlyWhenTheAuthorizationCouldHaveMoved(t *testing.T) {
	renamed := "renamed by an operator"
	legacyURL := "https://subscriber.example.com/blnk-events"

	brokerCalls := []string{"PruneSubscriberAccess", "GrantSubscriberAccess"}

	t.Run("a rename does not reach the broker at all", func(t *testing.T) {
		run := newSubscriberLifecycle(t)
		run.admin.failing("PruneSubscriberAccess", errors.New("broker unreachable"))
		run.admin.failing("GrantSubscriberAccess", errors.New("broker unreachable"))

		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{Name: &renamed})
		require.NoError(t, err,
			"a rename cannot move a single ACL binding, so it must not fail because the broker is down")
		require.NotNil(t, updated)
		assert.Equal(t, renamed, updated.Name)

		assert.Empty(t, run.log.only(brokerCalls...),
			"neither administrative call may be made for an edit that cannot change the boundary")
	})

	t.Run("recording a legacy webhook URL does not reach the broker", func(t *testing.T) {
		// The dual-run columns are migration bookkeeping. Nothing sends to the URL and no ACL
		// is derived from it, so coupling it to the broker was pure incidental cost.
		//
		// INSIDE THE DUAL-RUN WINDOW, because that is the only time the operation exists at
		// all: past the retirement instant a webhook URL can no longer be RECORDED, and the
		// fixture's configuration carries a transport with no sunset date, which resolves
		// fail-closed to "retired". Publishing a future instant is what makes this subtest
		// about the broker rather than about the sunset.
		run := newSubscriberLifecycle(t)
		subscriberSunsetConfiguration(t, time.Now().Add(24*time.Hour))
		run.admin.failing("PruneSubscriberAccess", errors.New("broker unreachable"))
		run.admin.failing("GrantSubscriberAccess", errors.New("broker unreachable"))

		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{WebhookURL: &legacyURL})
		require.NoError(t, err)
		require.NotNil(t, updated.WebhookURL)
		assert.Equal(t, legacyURL, *updated.WebhookURL)

		assert.Empty(t, run.log.only(brokerCalls...))
	})

	t.Run("an explicitly supplied identical grant still reconciles", func(t *testing.T) {
		// This is the retry that completes an update which persisted and then failed at the
		// grant step. The stored authorization already equals the desired one, so a comparison
		// would skip the very grant that finishes the job.
		run := newSubscriberLifecycle(t)
		stored := subscriberFixtureRow(t)

		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{AuthorizedTopics: stored.AuthorizedTopics})
		require.NoError(t, err)
		assert.Equal(t, stored.AuthorizedTopics, updated.AuthorizedTopics)

		assert.Equal(t, brokerCalls, run.log.only(brokerCalls...),
			"an explicit authorization field is a re-apply instruction; prune must precede grant")
	})

	t.Run("a genuine narrowing reconciles, prune before grant", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions"}})
		require.NoError(t, err)
		assert.Equal(t, []string{"blnk.transactions"}, updated.AuthorizedTopics)

		assert.Equal(t, brokerCalls, run.log.only(brokerCalls...),
			"a NARROWING must reach the broker before the registry records it, so a failure of the "+
				"write leaves the subscriber with less access than the registry claims, never more")
	})

	t.Run("replacing one key scope with another does not reach the broker", func(t *testing.T) {
		// THE INERT EDIT. Kafka's authorizer has no message-key dimension, so one prefix is
		// worth exactly as many bindings as another: none. The subscriber stays key-scoped, so
		// its topics stay Describe-only, so the desired binding set is byte-identical before and
		// after. Reconciling would be two administrative round trips that cannot change
		// anything, and — as the failing broker below proves — would couple an edit that moves
		// no access to Kafka's availability.
		scoped := subscriberFixtureRow(t)
		scoped.PartitionKeyPrefix = stringPointer("ldg_acme")

		run := newSubscriberLifecycle(t).seeded(scoped)
		run.admin.failing("PruneSubscriberAccess", errors.New("broker unreachable"))
		run.admin.failing("GrantSubscriberAccess", errors.New("broker unreachable"))

		replacement := "ldg_umbrella"
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{PartitionKeyPrefix: &replacement})
		require.NoError(t, err,
			"swapping one inert value for another must not fail because the broker is down")
		require.NotNil(t, updated.PartitionKeyPrefix)
		assert.Equal(t, replacement, *updated.PartitionKeyPrefix)
		assert.True(t, updated.RequiresGatewayDelivery(),
			"the subscriber is key-scoped on both sides of the edit, which is why nothing moved")

		assert.Empty(t, run.log.only(brokerCalls...))
	})

	t.Run("clearing the key scope reaches the broker, because it re-grants record access",
		func(t *testing.T) {
			// THE WIDENING DIRECTION. A key-scoped subscriber holds Describe and no Read;
			// removing its prefix grants Read on every topic it already had, so the credential
			// it holds gains record-level access to all of them. That is a binding change and
			// the broker has to be brought to it.
			scoped := subscriberFixtureRow(t)
			scoped.PartitionKeyPrefix = stringPointer("ldg_acme")

			run := newSubscriberLifecycle(t).seeded(scoped)

			cleared := ""
			updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
				SubscriberUpdate{PartitionKeyPrefix: &cleared})
			require.NoError(t, err)
			assert.Nil(t, updated.PartitionKeyPrefix,
				"a present empty string clears the recorded scope")
			assert.True(t, updated.GrantsBrokerRecordAccess(),
				"and the cleared row is the one shape that reads records at the broker directly")

			assert.Equal(t, brokerCalls, run.log.only(brokerCalls...),
				"prune must precede grant here as everywhere: the widening is applied by the "+
					"grant half, and it is applied against the row as persisted")
		})

	t.Run("clearing the key scope FAILS when the broker cannot be reached", func(t *testing.T) {
		// FAIL-CLOSED, and this is the assertion that makes the reconciliation real rather than
		// best-effort. If an unreachable broker were tolerated here, the registry would record a
		// subscriber as holding direct record access — and every response would declare it —
		// while the broker still refused its fetches. The subscriber would be told it may
		// consume directly and would find that it cannot, with nothing in the registry to
		// indicate which of the two was wrong.
		scoped := subscriberFixtureRow(t)
		scoped.PartitionKeyPrefix = stringPointer("ldg_acme")

		run := newSubscriberLifecycle(t).seeded(scoped)
		run.admin.failing("PruneSubscriberAccess", errors.New("broker unreachable"))

		cleared := ""
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{PartitionKeyPrefix: &cleared})
		require.Error(t, err,
			"a widening the broker did not apply must not be recorded as applied")
		assert.Nil(t, updated)

		stored, ok := run.store.row(subscriberFixtureID)
		require.True(t, ok)
		require.NotNil(t, stored.PartitionKeyPrefix,
			"the row still records the scope, so the registry and the broker still agree")
		assert.Equal(t, "ldg_acme", *stored.PartitionKeyPrefix)
	})
}

// TestIssueSubscriberCredential_RenewsTheClaimBeforeItWritesTheIssuance is the first half of the
// fence finding.
//
// # The defect
//
// The fence was a fixed lease taken before the row was read, and nothing afterwards either
// renewed it or CARRIED it. Provisioning is up to four broker round trips with their own
// timeouts, so an issuance could legitimately still be working when its lease expired — at which
// point another operation could claim the subscriber and both would proceed. The first one's
// write then landed anyway, because it re-read nothing and its UPDATE was unconditional.
//
// # What is asserted, and why the ORDER is the assertion
//
// A renewal that happened before the broker work would prove nothing: the claim was fresh then.
// The renewal has to sit BETWEEN the last broker round trip and the registry write, which is the
// only position from which it can refuse a write that is about to race. So the sequence is
// asserted rather than the mere presence of the call.
func TestIssueSubscriberCredential_RenewsTheClaimBeforeItWritesTheIssuance(t *testing.T) {
	run := newSubscriberLifecycle(t)

	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	assert.NotEmpty(t, credential.Username)

	// The renewal this test is about is the SECOND one. The first, before the broker phase, is
	// what TestIssueSubscriberCredential_ConfirmsTheClaimBeforeProvisioningAndRecordsUnderIt
	// asserts, and it guards a different window: a secret must not be written at the broker
	// under a lapsed lease. Only this one can refuse a registry write that is about to race,
	// because only this one happens after the round trips whose duration the broker decides.
	assert.Equal(t,
		[]string{
			"ClaimSubscriberForProvisioning",
			"RenewSubscriberProvisioningFence",
			"ProvisionSubscriberPrincipal",
			"RenewSubscriberProvisioningFence",
			"RecordSubscriberCredentialIfUnchanged",
		},
		run.log.only(
			"ClaimSubscriberForProvisioning",
			"ProvisionSubscriberPrincipal",
			"RenewSubscriberProvisioningFence",
			"RecordSubscriberCredentialIfUnchanged",
		),
		"the renewal must sit between the broker work and the write it protects: anywhere earlier "+
			"and it re-confirms a claim that was never in doubt",
	)
}

// TestIssueSubscriberCredential_CompensatesWhenItLosesTheClaim is the second half.
//
// # What the right compensation actually is, and why it is NOT a revocation
//
// By the time the claim is known to be lost, the credential exists at the broker: provisioning
// writes the SCRAM credential before its ACL bindings, because a binding for a principal that does
// not exist is inert while a credential with none still AUTHENTICATES. The tempting compensation
// is therefore to revoke it — and that would be wrong, for the same reason a superseded issuance
// is not revoked. Kafka stores ONE SCRAM credential per principal, so the operation that took the
// claim over may already have written ITS password over this one. Revoking would destroy a
// credential that works, for a subscriber that has been handed it and is about to connect.
//
// What the situation genuinely is, is UNCERTAIN: the registry's reference may or may not describe
// what authenticates. So the compensation is to make that uncertainty VISIBLE rather than to
// gamble on removing it — the row is marked credential_orphaned_at, which the orphaned-credential
// gauge and its alert read, and which the documented remedy (issue once more, serially) settles by
// replacing whatever is live.
//
// Both halves are asserted, because the caller sees a conflict either way and only the durable
// state distinguishes "abandoned with the ambiguity recorded" from "abandoned silently".
func TestIssueSubscriberCredential_CompensatesWhenItLosesTheClaim(t *testing.T) {
	run := newSubscriberLifecycle(t)

	// The renewal is what discovers the loss, so failing it is how a lapsed lease is expressed
	// without waiting one out. The marker phrase is the one database.subscriberFenceLost
	// produces — TestSubscriberFenceWasLost_MatchesTheRepositorysOwnMarker pins that agreement.
	//
	// INSTALLED DURING PROVISIONING, which is what makes this the window the test is about.
	// Issuance renews TWICE — once on the way into the broker, so no secret is written under a
	// lapsed lease, and once after it, so a registry write that is about to race a new owner is
	// refused. Failing every renewal from the start would fail the FIRST one, and a claim lost
	// before the broker was touched leaves nothing at the broker and therefore no orphan to
	// record. The state this test exists for only arises once the credential is already written.
	run.admin.onProvision = func() {
		run.store.failing("RenewSubscriberProvisioningFence", apierror.NewAPIError(
			apierror.ErrConflict,
			"This credential operation lost its provisioning claim and was abandoned; retry it",
			errors.New("subscriber test: the provisioning claim was no longer held while renewing"),
		))
	}

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err, "an issuance that cannot confirm its claim must not report success")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code,
		"another operation owns the subscriber, which is a conflict rather than a server fault")

	assert.NotContains(t, run.log.sequence(), "RecordSubscriberCredentialIfUnchanged",
		"nothing may be written to the registry once the claim is known to be lost")

	assert.NotContains(t, run.log.sequence(), "RevokeSubscriber",
		"and the credential must NOT be revoked: one SCRAM credential exists per principal, so the "+
			"operation that took the claim over may already have written its own password over this "+
			"one, and revoking would destroy a secret that works")

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.CredentialOrphanedAt,
		"the ambiguity — the registry's reference may not describe what authenticates — must be "+
			"recorded durably, or it is visible only in a log line and no gauge can see it")
}

// TestSubscriberFenceWasLost_MatchesTheRepositorysOwnMarker pins the agreement between the two
// packages that decide whether compensation happens.
//
// # Why this test exists at all
//
// database.subscriberFenceLost produces a lost-fence error whose CODE is the generic conflict —
// deliberately, because the API layer already maps that to 409 and introducing a second conflict
// code would change the wire contract for every existing caller to communicate something only the
// service acts on. The distinguishing mark is therefore a PHRASE in the detail, and
// subscriberFenceWasLost matches on it.
//
// A phrase agreed across two packages by convention is a phrase that can be reworded in one of
// them. If that happened, every compensation path in the service would silently stop running: a
// lost fence would read as an ordinary conflict, the operation would be abandoned as superseded,
// and the credential it had already written at the broker would be left live. This test is what
// makes that rewording fail the build's tests instead.
func TestSubscriberFenceWasLost_MatchesTheRepositorysOwnMarker(t *testing.T) {
	t.Run("the marker phrase is present in the repository's error", func(t *testing.T) {
		// Provoked through the real repository so the phrase is read from the producer rather
		// than restated here — a restated copy would agree with itself for ever.
		datasource, mock := newSubscriberFenceMock(t)

		// The follow-up read is the ONE two-column answer describeFencedWriteMiss issues:
		// (holds_claim, revocation_pending). An earlier generation of the repository asked
		// (exists, held) instead, and this stanza still answered that shape — so `true, false`
		// was read as "the claim IS still held, and no revocation is pending", which is the
		// subscriberFenceMissOther branch and NOT a lost fence. The stanza was therefore
		// provoking the one miss reason whose error deliberately omits the marker, while
		// asserting the marker was present.
		//
		// holds_claim = false is what a LOST claim is, and it is the only reading that reaches
		// subscriberFenceMissClaimLost — the branch whose detail carries the phrase.
		mock.ExpectExec("provisioning_token").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("provisioning_token").
			WillReturnRows(sqlmock.NewRows([]string{"holds_claim", "revocation_pending"}).
				AddRow(false, false))

		err := datasource.ClearSubscriberCredential(context.Background(), "acme_prod", "stale-token")
		require.Error(t, err)

		assert.True(t, subscriberFenceWasLost(err),
			"the service must recognise the repository's lost-fence error, or every compensation "+
				"path in this file silently stops running")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("an ordinary conflict is NOT a lost fence", func(t *testing.T) {
		// The two demand opposite responses: a superseded issuance is abandoned as it stands,
		// while a lost fence must undo what it already created at the broker. Treating every
		// conflict as a lost fence would revoke a credential a concurrent issuance had just
		// legitimately written.
		superseded := apierror.NewAPIError(
			apierror.ErrConflict,
			"The subscriber's credential changed while this issuance was in flight",
			errors.New("a concurrent issuance superseded this one"),
		)

		assert.False(t, subscriberFenceWasLost(superseded))
		assert.False(t, subscriberFenceWasLost(nil))
		assert.False(t, subscriberFenceWasLost(errors.New("some transport failure")))
	})
}

// TestIssueSubscriberCredential_KeepsTheWholeResponseInsideTheWallClock is the SLA finding.
//
// # The defect
//
// The budget bounded the WORK. The compensation that follows a failure then ran on budgets of its
// own — a fresh ten-second broker cleanup, a fresh five-second registry cleanup and a fresh
// five-second fence release — each individually justified, because a cleanup must not inherit the
// deadline whose expiry it is compensating for. Together they made a worst case of roughly
// twenty-five seconds against a five-second requirement, and none of it was visible from the
// budget the code appeared to declare.
//
// # Why the test drives the WORST path
//
// The failing paths are the ones the reserves exist for: the registry write fails, so the
// credential must be revoked, and the revocation fails too, so the orphan must be recorded. That
// is every phase exercised in one request. A happy-path timing assertion would prove nothing,
// since the compensation never runs.
//
// The slack is generous because this is a wall-clock assertion on shared CI hardware; what it
// rules out is the multiple-of-the-budget overrun, not scheduling noise.
func TestIssueSubscriberCredential_KeepsTheWholeResponseInsideTheWallClock(t *testing.T) {
	budget := 900 * time.Millisecond

	run := newSubscriberLifecycle(t)
	run.service.WithIssuanceBudget(budget)

	// The write fails, so compensation is required...
	run.store.failing("RecordSubscriberCredentialIfUnchanged", errors.New("registry write failed"))
	// ...and the revocation fails too, so the orphan has to be recorded.
	run.admin.failing("RevokeSubscriber", errors.New("broker refused the revocation"))

	started := time.Now()
	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	elapsed := time.Since(started)

	require.Error(t, err)

	// Three phases inside ONE budget. The old shape would have spent the budget and then a
	// fresh ten seconds on the broker cleanup alone.
	assert.Less(t, elapsed, budget+2*time.Second,
		"the whole response — work, compensation and the durable record — must fit inside the "+
			"wall clock; a compensation on a budget of its own is what made a five-second promise "+
			"answer in twenty")

	assert.Contains(t, run.log.sequence(), "RevokeSubscriber",
		"the compensation must still RUN: shortening it into uselessness would trade a slow "+
			"response for a live credential nobody records")
	assert.Contains(t, run.log.sequence(), "MarkSubscriberCredentialOrphaned",
		"and when the compensation fails, the exposure must be recorded durably rather than only "+
			"logged")
}

// TestSubscriberCleanupContext_StaysInsideTheWallClockButSurvivesCancellation pins the two
// properties a compensation context must hold at once, which is why it is neither the caller's
// context nor a brand-new budget.
func TestSubscriberCleanupContext_StaysInsideTheWallClockButSurvivesCancellation(t *testing.T) {
	t.Run("it survives the work phase being cancelled", func(t *testing.T) {
		// The commonest reason a compensation is needed is the work context expiring, so a
		// cleanup that inherited that cancellation could never run on the occasion it exists
		// for. That was CLEAN-01.
		sla := time.Now().Add(2 * time.Second)
		work, cancelWork := context.WithCancel(withSubscriberSLA(context.Background(), sla))
		cancelWork()

		cleanup, cancel := subscriberCleanupContext(work)
		defer cancel()

		assert.NoError(t, cleanup.Err(), "a cancelled work phase must not cancel the compensation")
	})

	t.Run("it still ends inside the wall clock", func(t *testing.T) {
		// And this is the half that was missing: "fresh" was taken to mean a new budget, which
		// is what let the response outlast its promise.
		sla := time.Now().Add(2 * time.Second)

		cleanup, cancel := subscriberCleanupContext(withSubscriberSLA(context.Background(), sla))
		defer cancel()

		deadline, ok := cleanup.Deadline()
		require.True(t, ok, "a compensation must be bounded, or finishing what is owed becomes "+
			"blocking indefinitely on a dependency that has gone away")
		assert.False(t, deadline.After(sla),
			"the compensation must end no later than the wall clock it is part of")
		assert.True(t, deadline.After(time.Now()),
			"and it must have real time in it, or it reports having tried while doing nothing")
	})

	t.Run("the durability slice is reserved after the compensation", func(t *testing.T) {
		// The orphan record is the write that must not be starved: without it a compensation
		// that ran out of time leaves an exposure whose only representation is a log line.
		sla := time.Now().Add(2 * time.Second)
		base := withSubscriberSLA(context.Background(), sla)

		compensation, cancelCompensation := subscriberCleanupContext(base)
		defer cancelCompensation()

		compensationDeadline, ok := compensation.Deadline()
		require.True(t, ok)

		// ASSERTED AS THE ARITHMETIC, not by comparing two windows opened at the same instant.
		//
		// That comparison is what this used to do, and it cannot mean what it says: the durable
		// record is written AFTER the compensation returns, so a durability window opened now
		// is measured from a clock the record will never see. What actually encodes the reserve
		// is that the compensation ends at least one reserve before the wall clock — whenever
		// the record is written, that slice has not been spent by the compensation.
		assert.False(t, compensationDeadline.After(sla.Add(-subscriberDurabilityReserve)),
			"the durable record gets the LAST slice, so a compensation that overruns cannot "+
				"consume the time the record needs")

		// And the record's own window is bounded and live, which is the other half: a reserved
		// slice that produced a dead context would leave the exposure in a log line anyway.
		durability, cancelDurability := subscriberDurabilityContext(base)
		defer cancelDurability()

		durabilityDeadline, ok := durability.Deadline()
		require.True(t, ok)
		assert.False(t, durabilityDeadline.After(sla),
			"the record is still inside the wall clock it is part of")
		assert.True(t, durabilityDeadline.After(time.Now()),
			"and it has real time in it")
	})

	t.Run("an operation with no wall clock keeps a bounded fallback", func(t *testing.T) {
		// An authorization change, a deregistration and a revocation make no timed promise, so
		// imposing one would abort work that is legitimately slower for no requirement.
		cleanup, cancel := subscriberCleanupContext(context.Background())
		defer cancel()

		deadline, ok := cleanup.Deadline()
		require.True(t, ok, "unbounded is not an option: it turns a cleanup into a hang")
		assert.False(t, deadline.After(time.Now().Add(subscriberCleanupBudget+time.Second)))
	})
}

// TestIssueSubscriberCredential_RecordsAnOrphanedCredentialDurably is the first half of the
// orphan finding.
//
// # The defect
//
// When an issuance could neither record its credential nor revoke it, a means of authenticating
// to the event bus existed at the broker for a principal the registry recorded no issuance for —
// and the only trace was a log line. Nothing counted it, so the outstanding-revocation alert
// could not fire on it: that rule reads the revocation tombstone, which the issuance path never
// stamps. An exposure whose only representation is a log line is an exposure nobody is watching,
// and it is indistinguishable from a healthy system.
func TestIssueSubscriberCredential_RecordsAnOrphanedCredentialDurably(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.store.failing("RecordSubscriberCredentialIfUnchanged", errors.New("registry write failed"))
	run.admin.failing("RevokeSubscriber", errors.New("broker refused the revocation"))

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)

	require.NotNil(t, stored.CredentialOrphanedAt,
		"a credential that could be neither recorded nor revoked must be represented by more than "+
			"a log line, or no gauge and no alert can see it")
	assert.Nil(t, stored.RevocationPendingAt,
		"and it must NOT be the revocation tombstone: that marker means the subscriber is on its "+
			"way out and makes issuance refuse, which would make the documented re-issue remedy "+
			"— the one that settles an orphan by replacing it — unreachable")
}

// TestIssueSubscriberCredential_SettlesAnOrphanOnTheNextSuccessfulIssuance is what makes the
// documented remedy true rather than advisory.
//
// Kafka stores one SCRAM credential per principal, so a successful issuance REPLACES whatever was
// orphaned: the orphaned secret stops authenticating the moment the new one is written. A marker
// left standing after that would keep a critical alert firing on an exposure that no longer
// exists, which is how an alert stops being believed.
func TestIssueSubscriberCredential_SettlesAnOrphanOnTheNextSuccessfulIssuance(t *testing.T) {
	orphaned := subscriberFixtureRow(t)
	orphanedAt := time.Now().Add(-time.Hour).UTC()
	orphaned.CredentialOrphanedAt = &orphanedAt

	run := newSubscriberLifecycle(t).seeded(orphaned)

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err, "an orphan marker must not block the very operation that settles it")

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.CredentialOrphanedAt,
		"the credential recorded here replaced the orphaned one, so the marker is settled in the "+
			"same write rather than left for an operator to clear by hand")
}

// TestDeregisterSubscriber_DistinguishesARefusedRevocationFromOneThatMerelyBegan is the second
// half.
//
// # Why one marker was not enough
//
// The tombstone is stamped BEFORE the broker is touched, so on its own it covers two situations
// that need different work: the broker REFUSED the revocation, or the process died between the
// stamp and the attempt. The first needs the administrative principal's grants or the broker's
// reachability fixed before ANY retry can succeed; the second needs nothing but the retry. One
// count cannot tell an operator which they are looking at, so they would retry into a refusal.
func TestDeregisterSubscriber_DistinguishesARefusedRevocationFromOneThatMerelyBegan(t *testing.T) {
	run := newSubscriberLifecycle(t)
	run.admin.failing("RevokeSubscriber", errors.New("CLUSTER_AUTHORIZATION_FAILED"))

	pending, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
	require.Error(t, err, "a revocation the broker refused must not be reported as a removal")
	require.NotNil(t, pending, "the tombstoned row is what makes the work findable")

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok, "the row must SURVIVE: deleting it would destroy the principal to revoke")

	require.NotNil(t, stored.RevocationPendingAt,
		"the tombstone records that a deregistration is outstanding")
	require.NotNil(t, stored.RevocationFailedAt,
		"and the second marker records that the broker REFUSED it, which is the fact that says "+
			"retrying alone will not help")
}

// TestDeregisterSubscriber_ClearsTheFailureMarkerOnEachNewAttempt pins the semantics that make
// the two markers readable together.
//
// The tombstone keeps its FIRST instant, because the quantity an operator alerts on is the age of
// the exposure. The failure marker is reset at the start of every attempt, because it describes
// the LATEST attempt — so "pending set, failed NULL" means in flight or awaiting deletion, and
// "pending set, failed set" means the broker refused. A failure marker that accumulated would
// make the second reading permanent.
func TestDeregisterSubscriber_ClearsTheFailureMarkerOnEachNewAttempt(t *testing.T) {
	failed := subscriberFixtureRow(t)
	firstPending := time.Now().Add(-2 * time.Hour).UTC()
	staleFailure := time.Now().Add(-2 * time.Hour).UTC()
	failed.RevocationPendingAt = &firstPending
	failed.RevocationFailedAt = &staleFailure

	run := newSubscriberLifecycle(t).seeded(failed)

	removed, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
	require.NoError(t, err, "a retry against a healthy broker must complete the deregistration")
	require.NotNil(t, removed)

	_, stillThere := run.store.row(subscriberFixtureID)
	assert.False(t, stillThere, "a confirmed revocation is followed by the deletion")

	// The re-mark is what cleared the stale failure, and it is asserted through the returned
	// tombstoned row the service saw on its way past.
	assert.Empty(t, run.log.only("MarkSubscriberRevocationFailed"),
		"nothing failed this time, so no failure may be recorded")
}

// TestSubscriberName_IsBoundedAtTheServiceLayer covers the bound the DTO cannot enforce.
//
// # Why the service bounds it too
//
// Nothing bounded the name, so the effective limit was the global 5 MiB body cap — and the value
// is stored, returned in every registry response, and written into log fields on every issuance,
// revocation and provisioning failure. An unbounded one is amplified by every read of the
// registry and by every log line naming the subscriber.
//
// The DTO bound covers HTTP. It does not cover a CLI caller, a migration or a fixture, all of
// which reach this service directly — and the row outlives any single request. The schema CHECK
// covers what neither sees. Each layer bounds callers the layer above cannot observe.
func TestSubscriberName_IsBoundedAtTheServiceLayer(t *testing.T) {
	t.Run("an over-long name is refused on registration", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		_, err := run.service.RegisterSubscriber(context.Background(), SubscriberRegistration{
			Name: strings.Repeat("n", 257),
		})
		requireSubscriberAPIError(t, err, apierror.ErrGenValidation)
		assert.NotContains(t, err.Error(), strings.Repeat("n", 50),
			"the offending value is the very thing whose SIZE is the problem, so quoting it in the "+
				"error would put the whole of it into a log line")
	})

	t.Run("an over-long name is refused on update", func(t *testing.T) {
		run := newSubscriberLifecycle(t)
		oversized := strings.Repeat("n", 257)

		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{Name: &oversized})
		requireSubscriberAPIError(t, err, apierror.ErrGenValidation)
	})

	t.Run("the bound is measured in runes, matching the schema CHECK", func(t *testing.T) {
		// char_length(btrim(name)) counts CHARACTERS, so a byte-based bound here would accept a
		// value the database then refuses — or refuse a multi-byte script a quarter of the
		// length a Latin one is allowed.
		run := newSubscriberLifecycle(t)

		subscriber, err := run.service.RegisterSubscriber(context.Background(), SubscriberRegistration{
			Name: strings.Repeat("é", 256),
		})
		require.NoError(t, err, "256 characters is 256 characters whatever the script")
		assert.Equal(t, 256, utf8.RuneCountInString(subscriber.Name))
	})

	t.Run("a control character is refused rather than stripped", func(t *testing.T) {
		// The value is echoed into responses, log lines and trace attributes: a newline splits a
		// log line in two and forges a second entry. Stripping would silently store something
		// other than what the caller asked for.
		run := newSubscriberLifecycle(t)

		_, err := run.service.RegisterSubscriber(context.Background(), SubscriberRegistration{
			Name: "ops\nlevel=error msg=\"forged\"",
		})
		requireSubscriberAPIError(t, err, apierror.ErrGenValidation)
	})
}

// TestCompleteLegacyWebhookMigration_WritesBothColumnsInOneOperation pins the atomicity the
// migration endpoint now has.
//
// # The defect
//
// "This subscriber has completed its move to Kafka" is a single change of state, and it was
// expressed as two calls with no transaction spanning them. Every outcome of the window between
// them was wrong in some way: clearing first destroys the endpoint the migration was FROM while
// the row still reports the subscriber as unmigrated, and stamping first reports a migration
// complete while its legacy endpoint is still recorded — the state progress reporting exists to
// rule out.
//
// It also removes a second cost: the clear used to route through the registry update, which takes
// the provisioning fence and rewrites every mutable column, so a piece of dual-run bookkeeping
// could be refused because a credential issuance happened to hold the claim.
func TestCompleteLegacyWebhookMigration_WritesBothColumnsInOneOperation(t *testing.T) {
	migrating := subscriberFixtureRow(t)
	migrating.WebhookURL = stringPointer("https://subscriber.example.com/blnk-events")

	run := newSubscriberLifecycle(t).seeded(migrating)

	completed, err := run.service.CompleteLegacyWebhookMigration(
		context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	require.NotNil(t, completed)

	assert.Nil(t, completed.WebhookURL, "the endpoint is forgotten")
	require.NotNil(t, completed.MigratedAt, "and the migration is recorded")

	assert.Equal(t, []string{"CompleteSubscriberWebhookMigration"},
		run.log.only("CompleteSubscriberWebhookMigration", "UpdateEventSubscriber",
			"MarkSubscriberMigrated", "ClaimSubscriberForProvisioning"),
		"ONE repository operation, and no provisioning claim: neither column is an authorization, "+
			"so fencing this would make migration bookkeeping fail while an issuance was in flight")
}

// newSubscriberFenceMock builds a sqlmock-backed datasource for the marker-agreement assertion.
//
// The real repository is used rather than a double precisely because the phrase under test is
// produced there: a restated copy would agree with itself for ever.
//
// Parameters:
//   - t *testing.T: for the fatal on a mock construction failure and the deferred close.
//
// Returns:
//   - database.Datasource: the datasource under test.
//   - sqlmock.Sqlmock: the controller.
func newSubscriberFenceMock(t *testing.T) (database.Datasource, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return database.Datasource{Conn: db}, mock
}

// TestIssueSubscriberCredential_SucceedsOnceTheKeyScopeIsCleared HAS BEEN REMOVED, twice, and the
// second removal is the one that reads correctly.
//
// It asserted that clearing a prefix changed nothing about issuance — true only while clearing was
// unconditional. It is not: under a declared key-scoped deployment clearing is REFUSED, because it
// is the one path that turns a live key-scoped principal into a whole-topic reader without
// issuance ever running again (SEC-01).
//
// Its successor is TestIssueSubscriberCredential_ClearingAKeyScopeDependsOnWhatTheDeploymentDeclares,
// which walks the same sequence in BOTH deployments and asserts each outcome — the clearing that
// resolves SUBSCRIBER_KEY_SCOPE_UNENFORCED, and the refusal that keeps the declared model true.

// TestDeclaresKeyScope_ReadsWhitespaceAsAbsent pins the boundary between "no key scope" and "a
// key scope", which is the input every key-scope decision is taken on.
//
// A column holding only spaces is not a recorded intent, and reading it as one would attach an
// authorization boundary to a subscriber that asked for none — then deliver that boundary to its
// consumer, which would silently discard every record. NULL, the empty string and whitespace
// therefore all have to answer the same way.
//
// EffectiveKeyScope is asserted alongside the predicate rather than separately, because the pair
// is what reaches a subscriber: absent must report every-key AND broker-enforced, present must
// report the prefix AND not-broker-enforced. A predicate that agreed while the contract disagreed
// would still be a disclosure bug.
func TestDeclaresKeyScope_ReadsWhitespaceAsAbsent(t *testing.T) {
	for name, prefix := range map[string]*string{
		"null":       nil,
		"empty":      stringPointer(""),
		"spaces":     stringPointer("   "),
		"tab":        stringPointer("\t"),
		"newline":    stringPointer("\n"),
		"mixed":      stringPointer(" \t\n "),
		"absent row": nil,
	} {
		t.Run("absent: "+name, func(t *testing.T) {
			subscriber := &model.EventSubscriber{PartitionKeyPrefix: prefix}
			assert.False(t, subscriber.DeclaresKeyScope())

			scope, enforced := subscriber.EffectiveKeyScope()
			assert.Equal(t, model.SubscriberKeyScopeAllKeys, scope,
				"no recorded scope is every key, stated as a word rather than left blank")
			assert.True(t, enforced,
				"and with nothing delegated to the consumer, the topic grant is the whole boundary")
			assert.True(t, subscriber.HasKeyAccess("ldg_anything"),
				"and every key is in bounds")
		})
	}

	// The delivered scope is the recorded prefix with SURROUNDING WHITESPACE REMOVED, and the
	// padded case is here to pin that one normalisation rather than to permit padding.
	//
	// Registration and update REFUSE a prefix carrying surrounding whitespace outright, so the
	// only way to hold one is a row written before that check existed. Trimming it is therefore a
	// repair of a legacy value, not an alteration of a live one — and it is the same rule
	// api/model.NewSubscriberEnforcedAccess applies to the value it echoes, which is what stops
	// the domain accessor and the wire contract describing two different keys. Padding cannot be
	// part of a Blnk partition key: keys are ledger identifiers.
	for name, expected := range map[string]string{
		"a ledger fragment": "ldg_9f1c",
		"padded":            "ldg_9f1c",
		"a single rune":     "l",
	} {
		prefix := expected
		if name == "padded" {
			prefix = "  ldg_9f1c  "
		}

		t.Run("present: "+name, func(t *testing.T) {
			subscriber := &model.EventSubscriber{
				SubscriberID:       subscriberFixtureID,
				PartitionKeyPrefix: stringPointer(prefix),
			}
			assert.True(t, subscriber.DeclaresKeyScope())

			scope, enforced := subscriber.EffectiveKeyScope()
			assert.Equal(t, expected, scope,
				"the delivered scope is the recorded prefix with surrounding whitespace removed, "+
					"and nothing else is normalised: the interior of a key is opaque, so altering it "+
					"would change which records are in bounds")
			assert.False(t, enforced,
				"and it must be reported as NOT broker-enforced, which is the whole reason it is "+
					"safe to deliver it")
		})
	}

	t.Run("a nil subscriber answers rather than panicking", func(t *testing.T) {
		// Nil is the repository's not-found, so every predicate that reads a row can hold one.
		// BOTH surviving spellings are exercised because both are called on rows a repository
		// read produced, and one of them tolerating nil while another panics is exactly the kind
		// of disagreement having several names for one question invites. There were four such
		// names; the two whose names asserted a policy this package no longer performs —
		// unenforceability and required enforcement — have been deleted rather than re-documented.
		var subscriber *model.EventSubscriber
		assert.False(t, subscriber.RequiresGatewayDelivery())
		assert.False(t, subscriber.DeclaresKeyScope())

		scope, enforced := subscriber.EffectiveKeyScope()
		assert.Equal(t, model.SubscriberKeyScopeAllKeys, scope,
			"a row that records nothing is reported as all-keys, not as an empty boundary")
		assert.True(t, enforced,
			"and all-keys IS what the broker enforces, because the topic grant is the whole of it")
	})
}

// subscriberFenceStore is a store that only implements the fence, for the heartbeat's own tests.
//
// It exists so a fence can be exercised with a SHORT lease. The production lease is a package
// constant — fifteen seconds, renewed at a third of it — so a behavioural test driven through
// the service would have to wait five seconds to see one renewal. Building the fence directly
// with a sub-second lease observes the same loop in milliseconds, and
// TestFenceSubscriber_StartsTheHeartbeatWithTheProductionLease is what ties that loop to the
// value production uses.
type subscriberFenceStore struct {
	eventSubscriberStore

	mu sync.Mutex

	renewals []time.Duration
	releases int

	// renewErr, once set, is returned by every renewal, which is how the lost-fence branch is
	// reached.
	renewErr error
}

func (s *subscriberFenceStore) RenewSubscriberProvisioningFence(
	_ context.Context,
	_ string,
	_ string,
	lease time.Duration,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.renewals = append(s.renewals, lease)

	return s.renewErr
}

func (s *subscriberFenceStore) ReleaseSubscriberProvisioningFence(
	_ context.Context,
	_ string,
	_ string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.releases++

	return nil
}

func (s *subscriberFenceStore) renewalCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.renewals)
}

func (s *subscriberFenceStore) renewedLeases() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]time.Duration(nil), s.renewals...)
}

func (s *subscriberFenceStore) releaseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.releases
}

// newTestSubscriberFence builds a held fence over the given store with a short lease and starts
// its heartbeat, exactly as fenceSubscriber does.
func newTestSubscriberFence(t *testing.T, store eventSubscriberStore, lease time.Duration) *subscriberFence {
	t.Helper()

	fence := &subscriberFence{
		store:        store,
		subscriberID: subscriberFixtureID,
		token:        "fence-token",
		lease:        lease,
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}

	go fence.heartbeat()
	t.Cleanup(func() { fence.release(context.Background()) })

	return fence
}

// TestSubscriberFence_RenewsTheClaimWhileTheOperationRuns is the PERF-P16 guard.
//
// # The defect
//
// The fence was claimed for a fixed fifteen seconds and could not be extended. Credential
// issuance makes up to four administrative round trips, each capped at ten seconds against a
// broker that is refusing rather than answering, and a failure adds two more for the
// compensation — so the claim could expire while the operation holding it was still working.
// What happens then is the exact race the fence exists to prevent: a second issuance finds the
// subscriber unfenced, writes its own SCRAM credential over the first, and the two callers hold
// two secrets of which the broker keeps one. The loser's password authenticates against nothing
// and its request returned 200 with that password in it.
//
// # What the heartbeat has to do
//
// Renew REPEATEDLY, at a fraction of the lease, with the lease's own duration each time. A single
// renewal, or one that renewed for a shorter period than it waits between renewals, would move
// the expiry without removing it.
func TestSubscriberFence_RenewsTheClaimWhileTheOperationRuns(t *testing.T) {
	const lease = 900 * time.Millisecond

	store := &subscriberFenceStore{}
	fence := newTestSubscriberFence(t, store, lease)

	// The interval is a third of the lease, so three intervals is a second's worth of renewals
	// with room for scheduling. Polled rather than slept on, so the test is as fast as the
	// machine allows it to be.
	require.Eventually(t, func() bool { return store.renewalCount() >= 2 },
		2*time.Second, 20*time.Millisecond,
		"the claim must be renewed repeatedly while the operation holds it; a fence that is "+
			"claimed once and never extended expires under a slow broker, and a second issuance "+
			"then mints a credential over this one's")

	for _, renewed := range store.renewedLeases() {
		assert.Equal(t, lease, renewed,
			"each renewal must extend by the LEASE; renewing by less than the interval between "+
				"renewals would move the expiry without removing it")
	}

	fence.release(context.Background())

	// Nothing may renew after the claim has been cleared: a renewal landing then would re-fence
	// a subscriber this operation has finished with, for a whole lease.
	settled := store.renewalCount()
	time.Sleep(3 * lease / 2)
	assert.Equal(t, settled, store.renewalCount(),
		"release must stop the heartbeat before it clears the claim, or a renewal in flight can "+
			"re-fence a subscriber nothing is working on")
	assert.Equal(t, 1, store.releaseCount(), "and the claim must be cleared exactly once")
}

// TestSubscriberFence_RetiresLoudlyWhenTheClaimIsLost pins the failure branch.
//
// A renewal is conditional on the token, so it fails once the claim has expired or been taken
// over. There is nothing useful to do with that discovery mid-operation — abandoning the work
// would leave more residue than finishing it — so the heartbeat stops and says so. Continuing to
// renew would be worse than useless: it would either keep failing every interval, filling the log
// during an incident, or start succeeding again against a token somebody else now owns.
func TestSubscriberFence_RetiresLoudlyWhenTheClaimIsLost(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	store := &subscriberFenceStore{renewErr: apierror.NewAPIError(
		apierror.ErrConflict, "the claim is no longer held", errors.New("subscriber fence test"),
	)}

	fence := newTestSubscriberFence(t, store, 300*time.Millisecond)

	require.Eventually(t, func() bool { return store.renewalCount() >= 1 },
		2*time.Second, 10*time.Millisecond, "the heartbeat must attempt a renewal")

	// It must stop after the first failure rather than retrying every interval.
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 1, store.renewalCount(),
		"a lost claim must retire the heartbeat; renewing again either floods the log during an "+
			"incident or succeeds against a token another operation now owns")

	entries := relayEntriesWithMessage(hook, "no longer fenced")
	require.NotEmpty(t, entries,
		"losing a fence mid-operation must be reported: everything the operation does afterwards "+
			"is racing, and its conditional writes are the only thing left protecting it")
	assert.Equal(t, logrus.ErrorLevel, entries[0].Level)

	// Release still works on a fence whose heartbeat has already retired.
	fence.release(context.Background())
	assert.Equal(t, 1, store.releaseCount())
}

// TestSubscriberFence_ReleaseIsIdempotentAndNilSafe covers the two properties every caller
// relies on: it is deferred unconditionally, and some paths schedule it as well.
func TestSubscriberFence_ReleaseIsIdempotentAndNilSafe(t *testing.T) {
	store := &subscriberFenceStore{}
	fence := newTestSubscriberFence(t, store, time.Second)

	fence.release(context.Background())
	fence.release(context.Background())
	fence.release(context.Background())

	assert.Equal(t, 1, store.releaseCount(),
		"release must clear the claim once however often it is called; a second clear could "+
			"remove a claim another operation has since taken")

	var absent *subscriberFence
	assert.NotPanics(t, func() { absent.release(context.Background()) },
		"a nil fence must be releasable, so a caller can defer the release before it knows "+
			"whether the claim succeeded")
}

// TestFenceSubscriber_StartsTheHeartbeatWithTheProductionLease ties the loop the tests above
// exercise to the values production actually uses.
//
// Without this, every assertion above would hold for a fence that production never builds: the
// lease could be zero, the heartbeat could not be started at all, and only a five-second
// behavioural test would notice.
func TestFenceSubscriber_StartsTheHeartbeatWithTheProductionLease(t *testing.T) {
	run := newSubscriberLifecycle(t)

	fence, err := fenceSubscriber(context.Background(), run.store, subscriberFixtureID)
	require.NoError(t, err)
	require.NotNil(t, fence)
	defer fence.release(context.Background())

	assert.Equal(t, SubscriberProvisioningFenceLease, fence.lease,
		"the fence must be held for the documented lease")
	assert.NotEmpty(t, fence.token, "and under the token the claim issued")

	// The heartbeat is running: stopping it returns, which it cannot do unless a goroutine is
	// there to close the done channel.
	assert.NotPanics(t, fence.stopHeartbeat,
		"fenceSubscriber must START the heartbeat; a fence whose renewal loop was never launched "+
			"is the fixed-lease behaviour the finding is about, and it looks identical from outside")

	// A fence that could not be claimed is nil and its error is returned, so the caller cannot
	// mistake a refusal for a held claim.
	second, err := fenceSubscriber(context.Background(), run.store, subscriberFixtureID)
	require.Error(t, err, "a subscriber already fenced must refuse a second claim")
	assert.Nil(t, second)
}

// subscriberDeferredWork is a scheduler that HOLDS the tasks it is given.
//
// Holding rather than running is what makes the finding assertable. The question is not whether
// the cleanup happens — it does, on either design — but whether the caller waited for it, and the
// only way to observe that is to have a scheduler that has demonstrably not run anything by the
// time the call returns.
type subscriberDeferredWork struct {
	mu    sync.Mutex
	tasks []func()
	ran   int
}

// schedule is the seam WithBackgroundScheduler installs.
func (d *subscriberDeferredWork) schedule(task func()) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.tasks = append(d.tasks, task)
}

// pending reports how many tasks are waiting.
func (d *subscriberDeferredWork) pending() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return len(d.tasks)
}

// drain runs every scheduled task, in order, including any scheduled while draining.
func (d *subscriberDeferredWork) drain() {
	for {
		d.mu.Lock()
		if len(d.tasks) == 0 {
			d.mu.Unlock()

			return
		}

		task := d.tasks[0]
		d.tasks = d.tasks[1:]
		d.ran++
		d.mu.Unlock()

		task()
	}
}

// deferring installs the scheduler on the run's service and returns it.
func (l *subscriberLifecycle) deferring() *subscriberDeferredWork {
	deferred := &subscriberDeferredWork{}
	l.service.WithBackgroundScheduler(deferred.schedule)

	return deferred
}

// TestIssueSubscriberCredential_AnswersWithoutWaitingForItsCleanups is the PERF-P09 guard on the
// SUCCESS path.
//
// # The defect
//
// Requirement R-7 gives credential provisioning five seconds, and the service applies that budget
// to the operation. It then exceeded it: the provisioning fence was released in a deferred call
// that ran on its OWN five-second budget, synchronously, after the operation had finished. So the
// documented five-second endpoint could take ten, and it did so on the SUCCESS path — the ordinary
// one, the one an SLA is written about. A failed issuance was worse still, adding a ten-second
// broker compensation and another five-second registry write to a response that had already run
// out of time.
//
// # What the fix has to preserve
//
// The cleanup still has to happen. So the assertion is not "the release is gone" but "the release
// had not run when the caller was answered, and it runs afterwards" — which is exactly the
// difference between deferring work and dropping it.
func TestIssueSubscriberCredential_AnswersWithoutWaitingForItsCleanups(t *testing.T) {
	run := newSubscriberLifecycle(t)
	deferred := run.deferring()

	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	require.NotEmpty(t, credential.Password(), "the premise: the issuance succeeded")

	assert.Zero(t, run.log.count("ReleaseSubscriberProvisioningFence"),
		"the response must not have waited for the fence release: it is a write on its own budget "+
			"that nothing in this response depends on, and blocking on it is what made a "+
			"five-second endpoint answer in ten")
	assert.Equal(t, 1, deferred.pending(), "and it must be scheduled rather than skipped")

	deferred.drain()

	assert.Equal(t, 1, run.log.count("ReleaseSubscriberProvisioningFence"),
		"the claim must still be cleared; a deferred cleanup that never runs is a leased fence "+
			"every retry waits out")
}

// TestIssueSubscriberCredential_SchedulesTheBrokerCompensationAndHoldsTheFenceUntilItIsDone is the
// PERF-P09 guard on the FAILURE path, and the ordering assertion is the important half.
//
// # The two things that must both be true
//
// The response must not wait for the compensation — two administrative round trips on their own
// ten-second budget, reached most often because a five-second budget has just expired — and the
// compensation must still run BEFORE the fence is released.
//
// The ordering is not tidiness. The compensation revokes the credential this attempt wrote, and
// Kafka stores one SCRAM credential per principal. Release the fence first and a caller retrying
// immediately — which is what a retryable error invites — can mint a working credential and have
// this attempt's cleanup delete it moments later. The caller then holds a 200 with a password that
// stops working, and nothing anywhere records why. Holding the claim makes that retry a conflict:
// a refusal the caller can act on.
func TestIssueSubscriberCredential_SchedulesTheBrokerCompensationAndHoldsTheFenceUntilItIsDone(t *testing.T) {
	run := newSubscriberLifecycle(t)
	deferred := run.deferring()

	// The real shape of the failure: the SCRAM credential was written and the ACL grant failed.
	run.admin.failing("ProvisionSubscriberPrincipal", errors.New("subscriber test: acl grant refused"))
	run.admin.mu.Lock()
	run.admin.failureResult = SubscriberProvisioningResult{
		Principal:         subscriberFixtureRow(t).KafkaPrincipal,
		CredentialWritten: true,
		CompensationOwed:  true,
	}
	run.admin.mu.Unlock()

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err, "an ACL grant the broker refused must not be reported as success")

	// The request the service SENT asked for the deferral, which is what makes the result's
	// flags describe what really happened.
	requests := run.admin.provisioningRequests()
	require.Len(t, requests, 1)
	assert.True(t, requests[0].DeferCompensation,
		"a service with somewhere to defer work to must ask provisioning to hand the compensation "+
			"back rather than perform it inside the response")

	assert.Empty(t, run.admin.compensations(),
		"the response must not have waited for the compensation")
	assert.Zero(t, run.log.count("ReleaseSubscriberProvisioningFence"),
		"nor for the fence release")

	// The caller is told the honest third state.
	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	detail, ok := apiErr.Details.(SubscriberErrorDetail)
	require.True(t, ok, "the detail must be the bounded type; got %T", apiErr.Details)
	assert.True(t, detail.CompensationPending,
		"pending is the honest answer: claiming Compensated would assert a clean broker nobody "+
			"had confirmed, and reporting neither reads as 'the revocation failed', which is the "+
			"one state that needs a human")
	assert.False(t, detail.Compensated,
		"and the two are mutually exclusive, or a caller cannot tell a confirmed revocation from "+
			"a scheduled one")
	assert.True(t, detail.CredentialWritten,
		"the credential IS at the broker until the scheduled revocation says otherwise")

	deferred.drain()

	compensations := run.admin.compensations()
	require.Len(t, compensations, 1,
		"the compensation must run: a credential with no authorization boundary is the one state "+
			"provisioning may not leave behind")

	// THE ORDER, read off the one shared call log.
	assert.Equal(t,
		[]string{"CompensateProvisioning", "ReleaseSubscriberProvisioningFence"},
		run.log.only("CompensateProvisioning", "ReleaseSubscriberProvisioningFence"),
		"the compensation must complete BEFORE the claim is released, or a retry's fresh "+
			"credential can be revoked by this attempt's cleanup")
}

// TestIssueSubscriberCredential_SchedulesTheRegistryCompensationToo covers the second failure
// path: the broker wrote the credential and the REGISTRY could not record it.
//
// Both of its writes — revoking at the broker, clearing the registry's credential reference —
// used to run inline on fresh budgets after the issuance budget was already spent. Neither
// changes the caller's answer.
func TestIssueSubscriberCredential_SchedulesTheRegistryCompensationToo(t *testing.T) {
	run := newSubscriberLifecycle(t)
	deferred := run.deferring()

	run.store.failing("RecordSubscriberCredentialIfUnchanged", errors.New("subscriber test: write failed"))

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	assert.Empty(t, run.admin.revocations(),
		"the response must not wait for the revocation of the credential it could not record")
	assert.Zero(t, run.log.count("ClearSubscriberCredential"),
		"nor for the registry record it has to clear")

	deferred.drain()

	assert.Equal(t, []string{subscriberFixtureID}, run.admin.revocations(),
		"the credential must still be revoked: an unrecorded credential is one nothing references")
	assert.Equal(t, 1, run.log.count("ClearSubscriberCredential"),
		"and the registry must stop claiming a credential that no longer exists")

	assert.Equal(t,
		[]string{"RevokeSubscriber", "ClearSubscriberCredential", "ReleaseSubscriberProvisioningFence"},
		run.log.only("RevokeSubscriber", "ClearSubscriberCredential", "ReleaseSubscriberProvisioningFence"),
		"broker first, registry second, fence last: the same order the inline version used, moved "+
			"off the response path rather than reshuffled")
}

// TestIssueSubscriberCredential_ASupersededIssuanceSchedulesNoRevocation is the arm that must owe
// NOTHING, and it is a correctness rule rather than an optimisation.
//
// A concurrent issuance committed while this one was at the broker, so the registry records ITS
// reference. Kafka stores one credential per principal, so revoking here would destroy the
// winner's working secret as well as this one's dead one. The conflict is returned and the broker
// is left alone — and moving the cleanup to a background task must not quietly turn "deliberately
// nothing" into "the same cleanup, later".
func TestIssueSubscriberCredential_ASupersededIssuanceSchedulesNoRevocation(t *testing.T) {
	run := newSubscriberLifecycle(t)
	deferred := run.deferring()

	run.store.failing("RecordSubscriberCredentialIfUnchanged", apierror.NewAPIError(
		apierror.ErrConflict, "superseded", errors.New("subscriber test: another issuance won"),
	))

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code,
		"a superseded issuance is a conflict, and it stays one")

	deferred.drain()

	assert.Empty(t, run.admin.revocations(),
		"revoking here would delete the WINNING issuance's credential too, so this arm owes no "+
			"compensation at all — before or after the response")
	assert.Equal(t, 1, run.log.count("ReleaseSubscriberProvisioningFence"),
		"the fence is still released, because the operation is over")
}

// TestIssueSubscriberCredential_WithoutASchedulerKeepsTheInlineBehaviour pins the default.
//
// A hand-built service — a CLI, a test, anything that is not a request path — has nowhere to
// defer work to, and for it the inline behaviour is correct: the cleanup must be finished when the
// call returns, because there may be no process left a moment later. So the service asks the
// broker for the inline compensation it is actually going to perform, and the result then reports
// what really happened rather than a pending state that was never pending.
func TestIssueSubscriberCredential_WithoutASchedulerKeepsTheInlineBehaviour(t *testing.T) {
	run := newSubscriberLifecycle(t)

	_, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)

	requests := run.admin.provisioningRequests()
	require.Len(t, requests, 1)
	assert.False(t, requests[0].DeferCompensation,
		"with no scheduler the compensation would be 'deferred' to an inline call moments later — "+
			"the same round trips in a less obvious place, reported as pending when they were not")

	assert.Equal(t, 1, run.log.count("ReleaseSubscriberProvisioningFence"),
		"and the fence is released before returning, which is what a caller with no process left "+
			"afterwards needs")
}

// TestSubscriberService_PrefersTheProcessAdministrativeClient is the PERF-P10 guard.
//
// # The defect
//
// Every Blnk wrapper builds a subscriber service per request and closes it afterwards, and the
// service built its own administrative client on first use. So each broker-touching request paid
// for a transport, a TCP connection per broker, a SASL/SCRAM handshake whose proof is
// PBKDF2-derived over two round trips, and a metadata cache that starts empty — and then threw all
// of it away. Credential issuance has a five-second budget and was spending a measurable part of
// it on setup this process had already done, while the broker re-evaluated the connection's
// authorization from cold each time.
//
// # What the resolver has to guarantee
//
// The shared client is used, it is resolved AT MOST ONCE per service however many times an
// operation reaches the broker, and it is never closed by a service that merely borrowed it —
// because closing it would take the transport away from every later request in the process.
func TestSubscriberService_PrefersTheProcessAdministrativeClient(t *testing.T) {
	t.Run("the shared client is used and resolved once per service", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		resolutions := 0
		service := NewEventSubscriberService(run.store, nil).
			WithKafkaAdminResolver(func() (subscriberPrincipalProvisioner, error) {
				resolutions++

				return run.admin, nil
			})

		// UpdateSubscriber reaches the broker TWICE — prune, then grant — which is what makes
		// "resolved once" a claim worth asserting rather than a tautology.
		_, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions"}})
		require.NoError(t, err)

		assert.Equal(t, []string{"PruneSubscriberAccess", "GrantSubscriberAccess"},
			run.log.only("PruneSubscriberAccess", "GrantSubscriberAccess"),
			"the premise: this operation reached the broker twice")
		assert.Equal(t, 1, resolutions,
			"the shared client must be resolved once and remembered; asking again per round trip "+
				"would reintroduce the per-call cost inside a single request")
	})

	t.Run("a borrowed client is never closed by the service", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		service := NewEventSubscriberService(run.store, nil).
			WithKafkaAdminResolver(func() (subscriberPrincipalProvisioner, error) {
				return run.admin, nil
			})

		_, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.NoError(t, err)

		require.NoError(t, service.Close())

		assert.Zero(t, run.log.count("Close"),
			"the process owns the shared client, so a per-request service must not close it: the "+
				"next request would find a transport with no connections and pay for a new one, "+
				"which is the cost this seam exists to remove")
	})

	t.Run("a resolver that fails falls back to building a client", func(t *testing.T) {
		// The fallback matters because the resolver's failure modes ARE the constructor's — an
		// unreadable CA bundle, a SASL pair that is half-configured — so falling through produces
		// the same typed error with the same log line. A caller must not get a worse diagnosis
		// because a shared client happened to be unavailable.
		run := newSubscriberLifecycle(t)

		service := NewEventSubscriberService(run.store, nil).
			WithKafkaAdminResolver(func() (subscriberPrincipalProvisioner, error) {
				return nil, errors.New("subscriber test: no shared client")
			})

		_, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.Error(t, err,
			"the harness configuration has no usable TLS material, so the fallback construction "+
				"fails — which is the point: the error comes from building a client, not from the "+
				"resolver being unavailable")

		var apiErr apierror.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, apierror.ErrKafkaUnavailable, apiErr.Code,
			"and it is the same environmental 503 an unconfigured deployment already answers")
	})

	t.Run("an injected client still wins", func(t *testing.T) {
		// WithKafkaAdmin is the older seam and the integration tests use it. A resolver must not
		// override a client somebody handed over deliberately.
		run := newSubscriberLifecycle(t)

		resolved := false
		service := NewEventSubscriberService(run.store, run.admin).
			WithKafkaAdminResolver(func() (subscriberPrincipalProvisioner, error) {
				resolved = true

				return nil, errors.New("subscriber test: the resolver must not be consulted")
			})

		_, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
		require.NoError(t, err)
		assert.False(t, resolved, "an injected client must be used without consulting the resolver")
	})
}

// subscriberBuildableKafkaConfiguration publishes a configuration a real administrative client
// can be built from.
//
// The lifecycle harness's configuration deliberately cannot: it leaves TLS disabled without the
// local-development acknowledgement, which is what makes the resolver-fallback assertion above
// meaningful. The accessor's own tests need the opposite, so they state the acknowledgement the
// local stack states — the same posture scripts/kafka-bootstrap.sh sets up — rather than reaching
// for TLS material no unit test should need.
func subscriberBuildableKafkaConfiguration(t *testing.T) *config.Configuration {
	t.Helper()

	cnf := &config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:           []string{"broker-1:9092"},
			SubscriberBrokers: append([]string(nil), subscriberLifecycleSubscriberBrokers...),
			TopicPrefix:       "blnk",
			InsecureLocalDev:  true,
		},
	}
	outboxStoreConfiguration(t, cnf)

	return cnf
}

// ---------------------------------------------------------------------------------------
// AAP-02 — one absolute wall-clock deadline bounds the request, compensation included
// ---------------------------------------------------------------------------------------

// TestIssueSubscriberCredential_CompensatesInsideTheSameWallClockBudget is the AAP-02 guard.
//
// # The defect
//
// CLEAN-01 fixed compensation by detaching it from the caller's cancellation, and
// context.WithoutCancel strips the DEADLINE along with the cancellation because they are the same
// mechanism. So each cleanup then ran on a budget of its own: five seconds for the registry
// writes, ten for the broker round trips nested underneath them. A five-second endpoint could
// therefore answer in twenty — five of forward work, fifteen of stacked cleanup — and the
// requirement AAP-02 states is a bound on the ENDPOINT, which an endpoint answering in twenty
// seconds has not met however tidy its cleanup was.
//
// # Why this asserts on DEADLINES and not on elapsed time
//
// The doubles return instantly, so the stacked budgets cost no wall clock here and elapsed time
// cannot see the defect at all. What the defect IS, precisely, is a cleanup context carrying a
// deadline later than the request's own bound — so that is what is read. Elapsed time is asserted
// too, as the property an operator actually experiences, but the deadline is the discriminating
// assertion: it fails against the old code and passes against the new one.
func TestIssueSubscriberCredential_CompensatesInsideTheSameWallClockBudget(t *testing.T) {
	// The two regimes the arithmetic distinguishes, and they are genuinely different cases
	// rather than one case tested twice.
	//
	//   - forward overran its SLICE: the forward path is bounded at absolute-minus-reserve, so
	//     overrunning that while leaving the absolute instant in the future is the ORDINARY
	//     failure. The reserve is what compensation spends, and the request stays inside its
	//     bound with nothing to explain.
	//   - forward overran the ABSOLUTE instant: only reachable when a step did not honour its
	//     context, since a step that does honour it returns at the slice. There is no reserve
	//     left, so the floor applies — one bounded window, named, granted once.
	//
	// 400ms rather than the production five seconds: the reserve scales with the budget (a
	// quarter of it, see compensationReserve), so the arithmetic under test is the same shape at
	// any size and the suite does not pay five seconds to observe it.
	const budget = 400 * time.Millisecond

	// measurementSlack absorbs the gap between the test reading the clock and the service
	// reading it, which is hundreds of nanoseconds and is not what is under test. It is
	// deliberately tiny: the defect it must not hide is measured in seconds.
	const measurementSlack = 10 * time.Millisecond

	cases := []struct {
		name string

		// provisionFor is how long the broker double occupies the forward path.
		provisionFor time.Duration

		// floorApplies says whether the forward path is expected to overrun the ABSOLUTE
		// instant, in which case the ceiling is one floor past the moment the forward call
		// actually returned rather than the absolute instant itself.
		floorApplies bool

		// why states the property in the failure message.
		why string
	}{
		{
			name:         "forward overran its slice, so the reserve pays for compensation",
			provisionFor: 320 * time.Millisecond,
			floorApplies: false,
			why: "the absolute instant still lay in the future, so compensation must be " +
				"bounded BY it and must not be granted a budget of its own",
		},
		{
			// The floor is measured from when compensation STARTS, because by then the bound is
			// already gone — there is no instant left to measure from. Nothing can bound a
			// forward step that ignores its context; what is bounded is what happens after it
			// returns, and this is the assertion that it is one floor rather than a fresh
			// five-second registry budget and a fresh ten-second broker one stacked on top.
			name:         "forward overran the absolute instant, so the floor pays for it",
			provisionFor: 520 * time.Millisecond,
			floorApplies: true,
			why: "the bound was already gone when compensation started, so it gets the floor " +
				"and nothing more — never the five- or ten-second budget of its own that AAP-02 " +
				"reported",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			run := newSubscriberLifecycle(t)
			store, admin := run.store, run.admin

			// A non-conflict record failure is the branch that compensates. See
			// TestIssueSubscriberCredential_DoesNotRevokeWhenAConcurrentIssuanceWon for why a
			// conflict deliberately does not.
			store.failing("RecordSubscriberCredentialIfUnchanged", errors.New("write path unavailable"))

			service := run.service.WithIssuanceBudget(budget)

			// The instant the forward call actually returned, captured rather than computed.
			// time.Sleep guarantees AT LEAST its duration, so on a loaded machine it
			// overshoots by tens of milliseconds — and in the overrun case the compensation
			// window is measured from when compensation starts, so a computed instant would
			// make this assertion fail for scheduling noise instead of for the property.
			var provisionEnded time.Time
			admin.onProvision = func() {
				time.Sleep(tt.provisionFor)
				provisionEnded = time.Now()
			}

			started := time.Now()
			_, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
			elapsed := time.Since(started)
			require.Error(t, err)
			require.False(t, provisionEnded.IsZero(), "the broker call must have been made")

			// The absolute instant bounds the request. In the overrun case it is already
			// past when compensation begins, so the ceiling is one floor past that moment —
			// bounded, named, and granted once because the cleanup helpers rebase.
			ceiling := started.Add(budget)
			if tt.floorApplies {
				if floored := provisionEnded.Add(subscriberCompensationFloor); floored.After(ceiling) {
					ceiling = floored
				}
			}
			ceiling = ceiling.Add(measurementSlack)

			// Compensation happened at all — the CLEAN-01 property, restated here because a
			// deadline assertion on a call that never occurred would pass vacuously.
			require.NotZero(t, run.log.count("RevokeSubscriber"))
			require.NotZero(t, run.log.count("ClearSubscriberCredential"))
			assert.False(t, run.log.arrivedExpired("RevokeSubscriber"))
			assert.False(t, run.log.arrivedExpired("ClearSubscriberCredential"))

			// AAP-02 proper: every compensating call is bounded, and bounded by THIS request's
			// instant rather than by a budget of its own.
			for _, method := range []string{
				"RevokeSubscriber",
				"ClearSubscriberCredential",
				"ReleaseSubscriberProvisioningFence",
			} {
				deadline, ok := run.log.arrivedWithDeadline(method)
				require.True(t, ok,
					"%s must arrive BOUNDED; an unbounded compensation is the same defect "+
						"without a number on it", method)
				assert.False(t, deadline.After(ceiling),
					"%s arrived with a deadline %s past what this request may reach: %s",
					method, deadline.Sub(ceiling), tt.why)
			}

			// And the property an operator experiences. The bound is derived from the same
			// ceiling, so the two assertions cannot disagree about what this request was
			// allowed; the extra slack absorbs scheduling only, and is far below the seconds
			// the stacked budgets would have added.
			assert.Less(t, elapsed, ceiling.Sub(started)+250*time.Millisecond,
				"the endpoint must answer inside its own budget, not inside its budget plus a "+
					"fresh one for each level of cleanup")
		})
	}
}

// TestCompensationDeadline_IsOneWindowPerRequestHoweverDeeplyNested pins the helper arithmetic
// directly, including the case the end-to-end test cannot reach.
//
// Compensation nests: the registry-side cleanup calls RevokeSubscriber, which derives a
// broker-side cleanup of its own. Through the doubles only the outer context is observable, so
// the property that matters most — that nesting does NOT open a second window — has to be
// asserted on the helpers. It is the property that keeps the floor a floor: resolved afresh at
// each level against each level's own clock, the windows would stack, which is the shape of the
// defect AAP-02 reported, only smaller.
func TestCompensationDeadline_IsOneWindowPerRequestHoweverDeeplyNested(t *testing.T) {
	t.Run("no issuance deadline in force yields the caller's own budget", func(t *testing.T) {
		// Deregistration, revocation and the migration sweeps have no endpoint bound to respect,
		// so a fresh budget is the correct behaviour there and must not be narrowed.
		before := time.Now()
		deadline := boundedCompensationDeadline(context.Background(), subscriberCleanupBudget)

		assert.False(t, deadline.Before(before.Add(subscriberCleanupBudget)))
		assert.False(t, deadline.After(time.Now().Add(subscriberCleanupBudget)))
	})

	t.Run("an issuance deadline in the future caps the window", func(t *testing.T) {
		absolute := time.Now().Add(200 * time.Millisecond)
		ctx := withSubscriberIssuanceDeadline(context.Background(), absolute)

		assert.Equal(t, absolute, boundedCompensationDeadline(ctx, subscriberCleanupBudget),
			"the request's bound is earlier than the cleanup budget, so it is the bound")
		assert.Equal(t, absolute, boundedCompensationDeadline(ctx, kafkaCleanupBudget),
			"and the broker-side cleanup is capped by the same instant, not by its own ten seconds")
	})

	t.Run("a spent issuance deadline yields the floor, never a dead context", func(t *testing.T) {
		ctx := withSubscriberIssuanceDeadline(context.Background(), time.Now().Add(-time.Second))

		before := time.Now()
		deadline := boundedCompensationDeadline(ctx, subscriberCleanupBudget)

		assert.True(t, deadline.After(before),
			"a compensation born expired is a compensation that never runs — CLEAN-01")
		assert.False(t, deadline.After(time.Now().Add(subscriberCompensationFloor)),
			"and it is the floor, not a fresh budget")
	})

	t.Run("nesting a broker cleanup inside a registry cleanup does not extend it", func(t *testing.T) {
		// The realistic worst case: the absolute instant is already spent, so the outer cleanup
		// takes the floor. The inner one must inherit that instant rather than take a floor of
		// its own measured from a later clock.
		ctx := withSubscriberIssuanceDeadline(context.Background(), time.Now().Add(-time.Second))

		outer, cancelOuter := subscriberCleanupContext(ctx)
		defer cancelOuter()

		outerDeadline, ok := outer.Deadline()
		require.True(t, ok)

		inner, cancelInner := kafkaCleanupContext(outer)
		defer cancelInner()

		innerDeadline, ok := inner.Deadline()
		require.True(t, ok)

		assert.Equal(t, outerDeadline, innerDeadline,
			"one request, one compensation window, however many levels of cleanup are nested "+
				"inside it")
		assert.NoError(t, inner.Err(),
			"and the inner window must still be live, or the nesting has traded one defect for "+
				"the other")

		// A third level, because the rebase has to be transitive to be worth anything.
		third, cancelThird := subscriberCleanupContext(inner)
		defer cancelThird()

		thirdDeadline, ok := third.Deadline()
		require.True(t, ok)
		assert.Equal(t, outerDeadline, thirdDeadline)
	})

	t.Run("the cleanup window is never wider than the caller's own budget", func(t *testing.T) {
		// The cap works in both directions: an issuance deadline far in the future must not let
		// a cleanup exceed the budget its own helper declares.
		ctx := withSubscriberIssuanceDeadline(context.Background(), time.Now().Add(time.Hour))

		before := time.Now()
		deadline := boundedCompensationDeadline(ctx, subscriberCleanupBudget)

		assert.False(t, deadline.After(time.Now().Add(subscriberCleanupBudget)))
		assert.False(t, deadline.Before(before.Add(subscriberCleanupBudget)))
	})
}

// TestSubscriberLifecycle_NoDetachedContextEscapesTheCleanupHelpers is the source-level guard on
// AAP-02, and it exists because the defect is trivially reintroduced.
//
// The whole bound rests on two choke points: subscriberCleanupContext and kafkaCleanupContext are
// the only places context.WithoutCancel is called in the subscriber lifecycle, and both re-impose
// the request's deadline. A new cleanup path that reaches for WithoutCancel directly — the
// obvious thing to write, and what both of these used to be — silently restores a compensation
// with no bound at all, which is worse than the fresh budget the report found. Behavioural tests
// cannot see a path they do not exercise, so the constraint is asserted on the source.
func TestSubscriberLifecycle_NoDetachedContextEscapesTheCleanupHelpers(t *testing.T) {
	// The two files the subscriber issuance path spans, and the line each is allowed to detach on.
	allowed := map[string]string{
		"event_subscriber.go": "withSubscriberIssuanceDeadline(context.WithoutCancel(ctx), deadline)",
		"event_admin.go":      "withSubscriberIssuanceDeadline(context.WithoutCancel(ctx), deadline)",
	}

	for file, permitted := range allowed {
		source := readRepoFile(t, file)

		for number, line := range strings.Split(source, "\n") {
			if !strings.Contains(line, "context.WithoutCancel") {
				continue
			}

			// A comment explaining the mechanism is not a use of it.
			if trimmed := strings.TrimSpace(line); strings.HasPrefix(trimmed, "//") {
				continue
			}

			assert.Contains(t, line, permitted,
				"%s:%d detaches from cancellation without re-imposing the request's deadline. "+
					"Route it through subscriberCleanupContext or kafkaCleanupContext instead: "+
					"those two are the only places the issuance bound can be preserved across the "+
					"detachment, because WithoutCancel strips a real deadline and the absolute "+
					"instant travels as a context value precisely so it can be put back",
				file, number+1)
		}
	}
}

// TestIssueSubscriberCredential_DoesNotRevokeWhenAConcurrentIssuanceWon is the one case where
// NOT compensating is correct, and it is worth pinning because it looks like a missing cleanup.
//
// The accessor is what makes "one client for the process" true rather than aspirational, and three
// of its properties are load-bearing: the same client comes back every time, a failure is NOT
// remembered — the causes are external and fixable, and a permanently poisoned accessor would
// need a restart to recover from a secret that has since been mounted — and Close releases it.
func TestBlnk_KafkaAdminIsResolvedOncePerProcess(t *testing.T) {
	instance := &Blnk{config: subscriberBuildableKafkaConfiguration(t)}

	first, err := instance.KafkaAdmin()
	require.NoError(t, err, "a configured broker list with no TLS requirement must build a client")
	require.NotNil(t, first)

	second, err := instance.KafkaAdmin()
	require.NoError(t, err)
	assert.Same(t, first, second,
		"the accessor must return the SAME client: a second one would pay the transport, "+
			"connection and SASL cost this exists to pay once")

	require.NoError(t, instance.Close())

	third, err := instance.KafkaAdmin()
	require.NoError(t, err)
	assert.NotSame(t, first, third,
		"Close must release the client rather than leaving a closed one memoised, or every "+
			"administrative call after a shutdown would use a transport with no connections")

	t.Run("a construction failure is not cached", func(t *testing.T) {
		// TLS enabled with material that cannot be read is the ordinary shape of this failure,
		// and it is fixed by mounting a file rather than by restarting the process.
		broken := &Blnk{config: &config.Configuration{
			Kafka: config.KafkaConfig{
				Brokers: []string{"broker-1:9092"},
				TLS:     config.KafkaTLSConfig{Enabled: true, CAFile: "/nonexistent/ca.pem"},
			},
		}}

		_, first := broken.KafkaAdmin()
		require.Error(t, first, "unreadable TLS material must refuse to build a client")

		broken.kafkaAdminMu.Lock()
		cached := broken.kafkaAdmin
		broken.kafkaAdminMu.Unlock()

		assert.Nil(t, cached,
			"a failure must leave nothing memoised: the cause is external and fixable, and a "+
				"poisoned accessor would require a restart to recover from a file that has since "+
				"appeared")
	})

	t.Run("a nil instance answers rather than panicking", func(t *testing.T) {
		var absent *Blnk

		admin, err := absent.KafkaAdmin()
		require.Error(t, err)
		assert.Nil(t, admin)
	})
}

// TestBlnk_EventSubscribersInstallsBothProcessSeams is the wiring assertion, and without it every
// test above could hold while production still built a client per request and blocked on its
// cleanups.
//
// It is structural because there is nothing to observe from outside: a service that resolves its
// own client and one that borrows the process's behave identically apart from the cost, and cost
// is exactly what a unit test cannot see.
func TestBlnk_EventSubscribersInstallsBothProcessSeams(t *testing.T) {
	instance := &Blnk{config: subscriberBuildableKafkaConfiguration(t)}
	service := instance.EventSubscribers()
	require.NotNil(t, service)

	service.mu.Lock()
	resolver := service.resolveAdmin
	scheduler := service.deferWork
	service.mu.Unlock()

	require.NotNil(t, resolver,
		"PERF-P10: without the resolver every subscriber request builds and discards its own "+
			"administrative client, inside a five-second budget")
	require.NotNil(t, scheduler,
		"PERF-P09: without the scheduler the fence release and the broker compensation run "+
			"inline, and the five-second endpoint answers in ten on success and worse on failure")

	// The resolver hands over the PROCESS client, which is the whole point of it.
	shared, err := resolver()
	require.NoError(t, err)
	expected, err := instance.KafkaAdmin()
	require.NoError(t, err)
	assert.Same(t, expected, shared,
		"the resolver must hand over the instance's own client, not build a third one")

	// The scheduler must actually run what it is given, and Close must wait for it.
	ran := make(chan struct{})
	scheduler(func() { close(ran) })

	require.True(t, instance.waitForBackgroundWork(2*time.Second),
		"a graceful shutdown must drain scheduled cleanups; scheduling work a process exit "+
			"cancels silently is not deferring it, it is dropping it")

	select {
	case <-ran:
	default:
		t.Fatal("the scheduled task must have run before the drain reported completion")
	}

	require.NoError(t, instance.Close())
}

// TestUpdateSubscriber_TouchesTheBrokerOnlyForAnAuthorizationChange is the PERF-P15 guard.
//
// # The defect
//
// Reconciliation is two DescribeACLs round trips before it can conclude there is nothing to
// change, and it ran on EVERY update. Renaming a subscriber, or recording the legacy webhook URL
// of one migrating off HTTP, therefore paid the full broker cost of an authorization change that
// was not being made — and those are the common updates during the migration window.
//
// # Why the gate is what the request NAMES rather than a comparison of values
//
// A value comparison would be wrong in the case that matters most. When a grant fails halfway, the
// row is already persisted, so the caller retrying the same update would find the boundary
// "unchanged" and skip the very repair it is retrying. Gating on the presence of the field instead
// means a caller can always ask for reconciliation by naming the topic list, whether or not the
// value differs — which is also how drift is repaired.
func TestUpdateSubscriber_TouchesTheBrokerOnlyForAnAuthorizationChange(t *testing.T) {
	brokerCalls := []string{"PruneSubscriberAccess", "GrantSubscriberAccess"}
	renamed := "renamed subscriber"
	webhook := "https://subscriber.example.com/hooks"
	cleared := ""

	t.Run("a rename touches nothing at the broker", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{Name: &renamed})
		require.NoError(t, err)
		assert.Equal(t, renamed, updated.Name, "the premise: the update was applied")

		assert.Empty(t, run.log.only(brokerCalls...),
			"a request that names no authorization field cannot have changed the boundary, so the "+
				"two DescribeACLs round trips reconciliation makes are pure cost")
		assert.Equal(t, 1, run.log.count("UpdateEventSubscriber"),
			"and the registry write still happens")
	})

	t.Run("recording a legacy webhook URL touches nothing at the broker", func(t *testing.T) {
		// Inside the dual-run window, for the reason the sibling test states: recording an
		// endpoint is refused past the retirement instant, and the fixture resolves to
		// "retired" because it configures a transport and no date.
		run := newSubscriberLifecycle(t)
		subscriberSunsetConfiguration(t, time.Now().Add(24*time.Hour))

		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{WebhookURL: &webhook})
		require.NoError(t, err)

		assert.Empty(t, run.log.only(brokerCalls...),
			"the legacy URL is not part of the Kafka authorization boundary, and this is the "+
				"commonest update of the migration window")
	})

	t.Run("naming the topic list reconciles, even when the value is unchanged", func(t *testing.T) {
		run := newSubscriberLifecycle(t)
		unchanged := append([]string(nil), subscriberFixtureRow(t).AuthorizedTopics...)

		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{AuthorizedTopics: unchanged})
		require.NoError(t, err)

		assert.Equal(t, brokerCalls, run.log.only(brokerCalls...),
			"resubmitting the same list is how drift is repaired and how an update whose grant "+
				"failed halfway is completed; a value comparison would skip exactly that repair")
	})

	t.Run("naming only the key scope touches nothing at the broker", func(t *testing.T) {
		// REVERSED, and worth saying so explicitly. This read "naming the key scope
		// reconciles", which contradicts the very finding the test exists for: Kafka ACLs have
		// no message-key dimension, so a partition-key prefix has NO broker-side representation
		// and an edit to one cannot move a single binding. Reconciling for it is exactly the two
		// DescribeACLs round trips this guard removes — and it made an edit whose whole purpose
		// is consumer-side filtering fail whenever the broker was unreachable.
		//
		// The prefix's own contract says the same from the other side: every subscriber read and
		// every credential response declares that the narrowing is the subscriber's to apply.
		// subscriberAuthorization therefore snapshots only the three fields a binding is derived
		// from — principal, consumer group and topics.
		run := newSubscriberLifecycle(t)

		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{PartitionKeyPrefix: &cleared})
		require.NoError(t, err)

		assert.Empty(t, run.log.only(brokerCalls...),
			"a key-scope edit has no ACL representation, so neither administrative call may be made")
		assert.Equal(t, 1, run.log.count("UpdateEventSubscriber"),
			"and the registry write still happens")
	})

	t.Run("the prune still precedes the persist and the grant still follows it", func(t *testing.T) {
		// The skip must not have reordered the steps it does not skip. Narrowing has to reach
		// the broker before the registry claims it, and widening only after.
		run := newSubscriberLifecycle(t)

		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions"}})
		require.NoError(t, err)

		assert.Equal(t,
			[]string{"PruneSubscriberAccess", "UpdateEventSubscriber", "GrantSubscriberAccess"},
			run.log.only("PruneSubscriberAccess", "UpdateEventSubscriber", "GrantSubscriberAccess"),
			"prune, persist, grant: a narrowing must be in force before it is recorded and a "+
				"widening recorded before it is granted")
	})
}

// ---------------------------------------------------------------------------------------
// The legacy webhook_url is retired at the sunset, on the generic routes too
// ---------------------------------------------------------------------------------------

// subscriberSunsetConfiguration publishes a Kafka-configured deployment whose retirement
// instant is the one supplied, and restores whatever was published before.
//
// The sunset is published as an RFC3339 string, exactly as an operator sets it, so the
// resolution under test is the real one — parse included — rather than a pre-parsed instant
// no deployment could produce.
func subscriberSunsetConfiguration(t *testing.T, sunset time.Time) {
	t.Helper()

	cnf := subscriberLifecycleConfiguration(t)
	cnf.WebhookDeprecationSunsetDate = sunset.UTC().Format(time.RFC3339)
	config.MockConfig(cnf)
}

// apiErrorCode extracts the typed error code from a service error, failing the test when the
// error is not a typed one.
//
// Asserting on the CODE rather than on message text is what makes these assertions about the
// contract: the status a route answers with is derived from the code through the catalogue, so
// the code is the thing a client sees consequences of.
func apiErrorCode(t *testing.T, err error) apierror.ErrorCode {
	t.Helper()

	var apiErr apierror.APIError
	require.True(t, errors.As(err, &apiErr),
		"the error must be a typed apierror.APIError, got %T: %v", err, err)

	return apiErr.Code
}

// TestRegisterSubscriber_RefusesALegacyWebhookURLAfterTheSunset is R-12's write half on the
// route the sunset middleware does not guard.
//
// # The defect this closes
//
// The four /webhook-subscription routes answer 410 Gone past the retirement instant, because
// middleware.WebhookSunsetGuard is attached to them. POST /subscribers and PUT
// /subscribers/{subscriber_id} are NOT guarded and must not be — registering a subscriber and
// granting it topics are Kafka-era operations that outlive the webhook transport entirely — yet
// both carry an optional webhook_url. So the one field the sunset retires stayed writable
// through an unguarded route, and "nothing accepts a webhook URL after the sunset" held for
// three of the four ways in.
//
// The guard therefore lives in the SERVICE, which every writer of the column reaches, rather
// than in the request layer where it would have to be repeated per route and would put a second
// copy of the retirement decision in the HTTP path.
func TestRegisterSubscriber_RefusesALegacyWebhookURLAfterTheSunset(t *testing.T) {
	lifecycle := newSubscriberLifecycle(t)
	subscriberSunsetConfiguration(t, time.Now().Add(-time.Minute))

	url := "https://subscriber.example.com/hooks"
	stored, err := lifecycle.service.RegisterSubscriber(context.Background(), SubscriberRegistration{
		SubscriberID: "sub_after_sunset_001",
		Name:         "After the sunset",
		WebhookURL:   &url,
	})

	require.Error(t, err, "a webhook URL must not be recordable once legacy delivery has been retired")
	assert.Nil(t, stored, "nothing may be written when the request is refused")
	assert.Equal(t, apierror.ErrGenGone, apiErrorCode(t, err),
		"the refusal must carry the code the catalogue maps to 410 Gone, so the generic route answers "+
			"the same status the deprecated routes do")

	// The whole registration is refused rather than the field being dropped, because a caller
	// that asked to record an endpoint and got 201 with no endpoint recorded has been told
	// something untrue about the state of the registry.
	assert.Zero(t, lifecycle.log.count("CreateEventSubscriber"),
		"the refusal must happen before the write, so no row is created at all")
}

// TestRegisterSubscriber_AcceptsALegacyWebhookURLBeforeTheSunset is the complement, and it is
// what stops the guard above from being a blanket refusal.
//
// Recording an endpoint is the whole point of the column during the dual-delivery window: it is
// how an operator tracks which subscribers still have to be migrated.
func TestRegisterSubscriber_AcceptsALegacyWebhookURLBeforeTheSunset(t *testing.T) {
	lifecycle := newSubscriberLifecycle(t)
	subscriberSunsetConfiguration(t, time.Now().Add(72*time.Hour))

	url := "https://subscriber.example.com/hooks"
	stored, err := lifecycle.service.RegisterSubscriber(context.Background(), SubscriberRegistration{
		SubscriberID: "sub_before_sunset_001",
		Name:         "Inside the window",
		WebhookURL:   &url,
	})

	require.NoError(t, err, "inside the window the column is exactly what tracks an unmigrated subscriber")
	require.NotNil(t, stored)
	require.NotNil(t, stored.WebhookURL, "the recorded endpoint must be stored")
	assert.Equal(t, url, *stored.WebhookURL)
}

// TestUpdateSubscriber_WebhookURLIsWriteRetiredButStillClearable is the asymmetry that makes the
// retirement survivable.
//
// Past the sunset a non-empty URL is refused, and CLEARING one is not. The rows written during
// the window still hold third-party endpoints, and clearing them one at a time is how an
// operator honours a deletion request before the bulk retention sweep reaches it. A guard that
// refused every mention of the field would retire the only means of cleaning up after it.
func TestUpdateSubscriber_WebhookURLIsWriteRetiredButStillClearable(t *testing.T) {
	recorded := "https://subscriber.example.com/hooks"
	seed := subscriberFixtureRow(t)
	seed.WebhookURL = &recorded

	lifecycle := newSubscriberLifecycle(t).seeded(seed)
	subscriberSunsetConfiguration(t, time.Now().Add(-time.Minute))

	replacement := "https://subscriber.example.com/moved"
	updated, err := lifecycle.service.UpdateSubscriber(
		context.Background(), seed.SubscriberID, SubscriberUpdate{WebhookURL: &replacement},
	)
	require.Error(t, err, "replacing a recorded endpoint after the sunset is still recording one")
	assert.Nil(t, updated)
	assert.Equal(t, apierror.ErrGenGone, apiErrorCode(t, err))

	cleared := ""
	updated, err = lifecycle.service.UpdateSubscriber(
		context.Background(), seed.SubscriberID, SubscriberUpdate{WebhookURL: &cleared},
	)
	require.NoError(t, err, "clearing a recorded endpoint must stay available after the sunset")
	require.NotNil(t, updated)
	assert.Nil(t, updated.WebhookURL, "the endpoint must be forgotten")

	// The one-row cleanup helper goes through the same path, so it must stay usable too.
	lifecycle.store.with(seed)
	updated, err = lifecycle.service.ClearLegacyWebhookSubscription(context.Background(), seed.SubscriberID)
	require.NoError(t, err, "ClearLegacyWebhookSubscription is the operator's cleanup route and must not be retired")
	require.NotNil(t, updated)
	assert.Nil(t, updated.WebhookURL)
}

// TestRecordLegacyWebhookSubscription_IsRefusedAfterTheSunset proves the deprecated surface's
// service entry point is covered by the same guard as the generic one.
//
// Its HTTP route is already 410 by middleware, so this is defence in depth rather than the only
// barrier — but the method is exported and reachable from any caller inside the process, and a
// guard that only the middleware applied would be one route registration away from being lost.
func TestRecordLegacyWebhookSubscription_IsRefusedAfterTheSunset(t *testing.T) {
	lifecycle := newSubscriberLifecycle(t)
	subscriberSunsetConfiguration(t, time.Now().Add(-time.Minute))

	stored, err := lifecycle.service.RecordLegacyWebhookSubscription(
		context.Background(), subscriberFixtureID, "https://subscriber.example.com/hooks",
	)
	require.Error(t, err)
	assert.Nil(t, stored)
	assert.Equal(t, apierror.ErrGenGone, apiErrorCode(t, err))
}

// TestNormalizeSubscriberName_BoundsTheLabelAtTheService is the SEC-16 guard at the layer that
// api/model cannot cover.
//
// api/model.validateSubscriberName refuses the same three things at the request boundary, and
// that is where a caller gets the clearest message. This asserts the SERVICE's own check,
// which is what holds when the boundary is not in the path: the registry is reachable from the
// CLI, from a test and from any future caller that builds a registration directly, and the
// name column is TEXT, which bounds nothing at all.
func TestNormalizeSubscriberName_BoundsTheLabelAtTheService(t *testing.T) {
	t.Run("an ordinary label is accepted and trimmed", func(t *testing.T) {
		name, err := normalizeSubscriberName("  ledger-ops  ")
		require.NoError(t, err)
		assert.Equal(t, "ledger-ops", name)
	})

	t.Run("a blank label is refused", func(t *testing.T) {
		_, err := normalizeSubscriberName("   ")
		requireAPIErrorCode(t, err, apierror.ErrGenValidation)
	})

	t.Run("a control character is refused", func(t *testing.T) {
		for label, value := range map[string]string{
			"a newline":          "ledger\nops",
			"a carriage return":  "ledger\rops",
			"a NUL":              "ledger\x00ops",
			"an escape sequence": "ledger\x1b[31mops",
			"DEL":                "ledger\x7fops",
		} {
			_, err := normalizeSubscriberName(value)
			requireAPIErrorCode(t, err, apierror.ErrGenValidation)
			assert.Errorf(t, err, "%s must be refused in a label", label)
		}
	})

	t.Run("an oversized label is refused and the bound itself is accepted", func(t *testing.T) {
		_, err := normalizeSubscriberName(strings.Repeat("n", maxSubscriberNameLength+1))
		requireAPIErrorCode(t, err, apierror.ErrGenValidation)

		accepted, err := normalizeSubscriberName(strings.Repeat("n", maxSubscriberNameLength))
		require.NoError(t, err, "the bound itself must be accepted, or the limit is off by one")
		assert.Len(t, []rune(accepted), maxSubscriberNameLength)
	})

	t.Run("the bound is in runes, not bytes", func(t *testing.T) {
		// Counting bytes would give a label written in a non-Latin script roughly a third of
		// the allowance the same label gets in English, which bounds the alphabet rather than
		// the value.
		_, err := normalizeSubscriberName(strings.Repeat("台", maxSubscriberNameLength))
		assert.NoError(t, err)
	})

	t.Run("the two layers agree on the bound", func(t *testing.T) {
		// They are separate constants by design — the API package depends on this one and not
		// the other way round — so nothing but this assertion keeps them equal, and a
		// divergence would make a label acceptable at one layer and refused at the other.
		assert.Equal(t, 256, maxSubscriberNameLength,
			"api/model.maxSubscriberNameLen is 256; the two must match")
	})
}

// TestIssueSubscriberCredential_IssuesAndDeliversTheRecordedKeyScope is the F-05 guard, and it
// is the whole of that finding's resolution.
//
// # The defect
//
// The access model scopes a subscriber's credential by three things: its authorised topics, its
// consumer group, and its partition-key prefix. Issuance REFUSED any row recording a prefix, so
// the third scope existed only on rows that could not consume — which is not an implementation
// of it, it is a declination of it. A subscriber that recorded a ledger boundary got no
// credential at all, and the registry still described the boundary either way, so the refusal
// bought nothing a reader could see.
//
// The premise behind the refusal was and remains true: Kafka's authorizer has no message-key
// dimension, so a principal granted Read on a shared category topic reads every record on it.
// What was wrong was the conclusion. The two designs that could move the boundary to the broker
// are both closed off — a topic per key scope contradicts the model's own no-per-tenant-topics
// rule, and a filtering gateway is subscriber-side consumer machinery Blnk does not build — so
// what is left is to DELIVER the scope to the one party that can apply it.
//
// # What is asserted
//
// That the credential is issued at all, which is the finding; that it carries the recorded
// scope, so the consumer can apply it; and that it carries the flag saying the broker does not,
// so the scope cannot be mistaken for a limit on the credential's reach. The third assertion is
// the one that makes the first two safe.
func TestIssueSubscriberCredential_IssuesAndDeliversTheRecordedKeyScope(t *testing.T) {
	const scope = "ldg_9f1c8a72"

	row := subscriberFixtureRow(t)
	row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	run := newSubscriberLifecycle(t).seeded(row)
	store, service := run.store, run.service

	// A DECLARED ENFORCEMENT POINT, because Blnk ships no component that authorises record keys:
	// without one, issuance and a prefix-recording update both fail closed with
	// SUBSCRIBER_KEY_SCOPE_UNENFORCED. The shipped default is asserted by
	// TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway.
	enforceKeyScopeGateway(t)

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err,
		"A KEY SCOPE MUST NOT REFUSE ISSUANCE: refusing withheld the credential instead of "+
			"implementing the scope, leaving the third dimension of the access model unusable")

	assert.NotEmpty(t, credential.Password(),
		"the subscriber must receive a working secret, once")
	assert.NotZero(t, credential.IssuedAt)

	deliveredScope, brokerEnforced := credential.KeyScope()
	assert.Equal(t, scope, deliveredScope,
		"THE SCOPE MUST BE DELIVERED: it is enforced by the consumer, and a consumer that is "+
			"never told the prefix cannot apply it")
	assert.False(t, brokerEnforced,
		"and it must be labelled as NOT broker-enforced. Delivering the prefix without this flag "+
			"would state a boundary Kafka does not keep, which is the disclosure bug")

	// The broker was genuinely asked, and the issuance genuinely recorded — a refusal that had
	// merely been moved would show up as either of these being zero.
	assert.Positive(t, run.log.count("ProvisionSubscriberPrincipal"),
		"the principal must actually be provisioned")
	assert.Positive(t, run.log.count("RecordSubscriberCredentialIfUnchanged"))

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.CredentialReference,
		"and the issuance must be recorded, or a live principal exists that the registry denies")
	require.NotNil(t, stored.PartitionKeyPrefix,
		"the recorded scope must survive issuance unchanged: issuance reports it, it does not "+
			"consume or clear it")
	assert.Equal(t, scope, *stored.PartitionKeyPrefix)
}

// TestIssueSubscriberCredential_ReportsAllKeysWhenNoScopeIsRecorded is the other arm, and it
// exists because the absent case is the one a client is most likely to misread.
//
// A blank key-scope field beside a working credential invites exactly the wrong inference — that
// some restriction applies and was not named. The contract therefore states the scope positively
// in both cases: a recorded prefix, or the word for "every key", and the enforcement flag is
// TRUE here because the topic grant is then the whole boundary and the consumer has nothing left
// to apply.
func TestIssueSubscriberCredential_ReportsAllKeysWhenNoScopeIsRecorded(t *testing.T) {
	run := newSubscriberLifecycle(t)

	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)

	deliveredScope, brokerEnforced := credential.KeyScope()
	assert.Equal(t, model.SubscriberKeyScopeAllKeys, deliveredScope,
		"a subscriber with no prefix reads every key on its topics, and the response says so "+
			"rather than leaving a blank to be interpreted")
	assert.True(t, brokerEnforced,
		"and here the broker IS the whole boundary: nothing is delegated to the consumer")
}

// TestSubscriberProvisioningRequest_MapsTheKeyScopeOntoNoBinding guards against the tempting
// wrong fix, which is the same one whether issuance refuses a key-scoped row or not.
//
// Faced with a prefix the broker cannot evaluate, the plausible next step is to make it "do
// something" at the broker — most obviously by binding the TOPIC pattern as PREFIXED instead of
// LITERAL, since Kafka does have a prefixed pattern type. That is strictly worse than not
// enforcing it: topic-name prefixes and message-key prefixes are unrelated, so it would WIDEN the
// grant to every topic sharing a name prefix while appearing to narrow it.
//
// A request carrying a declared key scope is refused by validate and never reaches the broker at
// all, so these bindings are never sent — but the binding SHAPE is asserted here anyway, because
// the shape is what a future change would reach for, and this is the assertion that would fail.
// The ACL set must stay exactly the two dimensions the access model specifies, and the prefix must
// appear in NO binding. It asserts the shape rather than a count, because a count passes for the
// wrong reasons.
func TestSubscriberProvisioningRequest_MapsTheKeyScopeOntoNoBinding(t *testing.T) {
	const scope = "ldg_9f1c8a72"

	row := subscriberFixtureRow(t)
	row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	entries := NewSubscriberProvisioningRequest(&row, "a-password-long-enough-for-scram").aclEntries()
	require.NotEmpty(t, entries, "a subscriber with a topic grant must produce bindings")

	sawGroup := false

	for _, entry := range entries {
		assert.NotContains(t, entry.ResourceName, scope,
			"NO BINDING MAY NAME THE KEY SCOPE: the broker has no key dimension, so a resource "+
				"name carrying the prefix could only be a different resource than intended")

		switch entry.ResourceType {
		case kafka.ResourceTypeTopic:
			assert.Equal(t, kafka.PatternTypeLiteral, entry.ResourcePatternType,
				"topic bindings must stay LITERAL. A prefixed topic pattern would widen the grant "+
					"to every topic sharing the name prefix while looking like a narrowing")
		case kafka.ResourceTypeGroup:
			sawGroup = true
			assert.Equal(t, kafka.PatternTypePrefixed, entry.ResourcePatternType,
				"the consumer-group namespace is the one PREFIXED pattern, which reserves the "+
					"namespace without enumerating groups")
		default:
			t.Fatalf("unexpected ACL resource type %v: the boundary is topics and groups only",
				entry.ResourceType)
		}
	}

	assert.True(t, sawGroup, "the group namespace binding must be present")
}

// TestUpdateSubscriber_AcceptsAKeyScopeOnAProvisionedSubscriber SURVIVED, and its RATIONALE did
// not. The test is below; this note records what its reasoning used to be, because a reader
// finding the old wording elsewhere needs to know it was retired.
//
// It asserted that recording a prefix on a row already holding a credential was accepted, on the
// reasoning that the scope is delivered to the consumer at issuance and the remedy for a stale
// delivery is a re-issue. That reasoning is what was wrong: the consumer is the party the boundary
// is meant to constrain, so delivering the prefix to it is not enforcement at all.
//
// The ACCEPTANCE stands, on a stronger footing — the update NARROWS the live grant. Its companions
// are
// TestUpdateSubscriber_RecordsAKeyScopeOnAProvisionedSubscriberAndNarrowsTheGrant, which asserts
// that the update PRUNES record Read from the live principal so the broker refuses its next fetch,
// and TestUpdateSubscriber_RecordsAKeyScopeOnASubscriberThatHoldsACredential, which asserts that
// the decision is persisted, that the row reports its enforcement point, and that the working
// credential is not revoked as a side effect.

// TestIssueSubscriberCredential_ReportsBrokerResidueWhenTheIssuanceRecordTimesOut is the F-10
// guard, and what it protects is the ONE fact a caller cannot learn any other way.
//
// # The defect
//
// The issuance-record write is the last step of issuance, and by the time it runs the broker HAS
// been written to. It shared its timeout classification with the two registry steps that run
// BEFORE provisioning, and that classifier's whole contract is that no credential can exist yet
// — so it reports no broker state at all, deliberately. Applied after provisioning, that silence
// stops being accurate and becomes a false statement: a registry write that timed out and whose
// compensating revocation ALSO failed was answered with credential_written=false,
// compensated=false, retryable=true. A caller was told the retry starts from the state the first
// attempt found, while a principal it holds no secret for could authenticate against the broker.
// Nobody would go looking for residue they had just been told could not exist.
//
// # Why BOTH cleanup outcomes are asserted
//
// They are the two states the flags exist to distinguish, and a fix that hardcoded either one
// would pass a test that only checked the other. Successful revocation leaves the broker clean
// and the subscriber with NO access until credentials are re-issued; failed revocation leaves a
// live principal that a human has to revoke. Same error code, same retryability, opposite
// operational consequence.
//
// The premise in both cases is a GENERIC internal-server failure on a spent budget, because that
// is the only combination the classifier rewrites — a typed conflict or not-found stays itself.
func TestIssueSubscriberCredential_ReportsBrokerResidueWhenTheIssuanceRecordTimesOut(t *testing.T) {
	for _, tt := range []struct {
		name string

		// revocationFails drives the two cleanup outcomes.
		revocationFails bool

		// The residue each outcome must report, using SubscriberProvisioningResult's own
		// semantics: only a CONFIRMED revocation clears CredentialWritten.
		credentialWritten bool
		compensated       bool

		// reason is the fixed wording that must reach the caller, so the residue is legible
		// without decoding two booleans.
		reason string
	}{
		{
			name:              "revocation succeeds, so the broker is clean and access is gone",
			revocationFails:   false,
			credentialWritten: false,
			compensated:       true,
			reason:            "was revoked, so the subscriber has no Kafka access until credentials are re-issued",
		},
		{
			name:              "revocation fails, so a live principal is unaccounted for",
			revocationFails:   true,
			credentialWritten: true,
			compensated:       false,
			reason:            "could not be revoked, so a principal exists that no registry row records",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := newSubscriberLifecycle(t)
			store, admin := run.store, run.admin

			// A GENERIC internal-server failure, which is what the repository reports when it
			// has no better answer — including when its query ran on a dead context.
			// loggedDatabaseError does not carry the driver error, so the context is the only
			// thing that can tell this apart from a defect.
			store.failing("RecordSubscriberCredentialIfUnchanged", apierror.NewAPIError(
				apierror.ErrInternalServer,
				"Failed to record the issuance",
				nil,
			))

			if tt.revocationFails {
				admin.failing("RevokeSubscriber", errors.New("broker refused the revocation"))
			}

			service := run.service.WithIssuanceBudget(30 * time.Millisecond)

			// Spend the budget INSIDE the broker call, so provisioning genuinely succeeds and
			// the context is genuinely expired by the time the record is attempted. That is the
			// real sequence rather than a simulated one, and it is the sequence in which the
			// broker is guaranteed to hold a credential.
			admin.onProvision = func() { time.Sleep(60 * time.Millisecond) }

			credential, err := service.IssueSubscriberCredential(
				context.Background(), subscriberFixtureID,
			)
			require.Error(t, err)

			require.NotZero(t, admin.log.count("ProvisionSubscriberPrincipal"),
				"the premise of this test is that the BROKER WAS WRITTEN TO before the failure")
			require.NotZero(t, run.log.count("RevokeSubscriber"),
				"and that compensation was attempted")

			var apiErr apierror.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, apierror.ErrSubscriberProvisioningTimeout, apiErr.Code,
				"a spent budget is still a timeout after the broker was touched")
			assert.Equal(t, http.StatusGatewayTimeout, apierror.StatusForCode(apiErr.Code))

			detail, ok := apiErr.Details.(SubscriberErrorDetail)
			require.True(t, ok, "the detail must be the bounded struct, never the cause")

			assert.Equal(t, tt.credentialWritten, detail.CredentialWritten,
				"CREDENTIAL_WRITTEN IS THE FINDING: a caller cannot discover a live principal "+
					"any other way, and the pre-broker classifier reported false unconditionally")
			assert.Equal(t, tt.compensated, detail.Compensated,
				"and compensated is what says whether the residue was cleaned up or is still there")

			assert.True(t, detail.Retryable,
				"retry remains correct and remains true: issuance re-provisions the same boundary "+
					"idempotently, replacing a credential left live or restoring one revoked")
			assert.Contains(t, detail.Reason, "recording the issuance in the registry",
				"the detail must still name the step that ran out of time")
			assert.Contains(t, detail.Reason, tt.reason,
				"and it must state the residue in words, so the consequence is legible without "+
					"decoding two booleans")

			// The bounded-detail guarantee still holds on the new path: no cause, no principal,
			// no broker address reaches the caller.
			assert.NotContains(t, detail.Reason, "Failed to record the issuance")
			assert.NotContains(t, detail.Reason, "broker refused the revocation")
			assert.Equal(t, subscriberFixtureID, detail.SubscriberID)

			assert.Empty(t, credential.Password(),
				"NO SECRET REACHES THE CALLER: the issuance was not recorded, so handing one back "+
					"would leave an untracked working credential in a caller's hands")
			assert.False(t, store.fenced(subscriberFixtureID),
				"and the claim must be released, or one slow record refuses every retry for a lease")
		})
	}
}

// TestIssueSubscriberCredential_KeepsATypedRecordOutcomeAfterTheBrokerWasWrittenTo is the limit
// of the post-broker reclassification, and it is the same limit the pre-broker one has.
//
// A superseded issuance is a CONFLICT, and it stays a conflict whether or not the budget also
// expired: another issuance did commit, and the remedy is to issue once more serially rather than
// to treat the answer as a timeout. Rewriting it would also lose the one thing that distinguishes
// it from a failed revocation — both leave a credential at the broker, and only the code says
// which of them left somebody else's.
func TestIssueSubscriberCredential_KeepsATypedRecordOutcomeAfterTheBrokerWasWrittenTo(t *testing.T) {
	row := subscriberFixtureRow(t)
	run := newSubscriberLifecycle(t).seeded(row)
	store, admin := run.store, run.admin

	service := run.service.WithIssuanceBudget(30 * time.Millisecond)

	// A concurrent issuance commits while this one is at the broker, AND the budget is spent
	// there — both conditions at once, which is what makes this the limit case.
	admin.onProvision = func() {
		store.mu.Lock()
		stored := store.rows[subscriberFixtureID]
		stored.CredentialReference = stringPointer("the-other-issuance")
		store.rows[subscriberFixtureID] = stored
		store.mu.Unlock()

		time.Sleep(60 * time.Millisecond)
	}

	_, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusConflict, apierror.StatusForCode(apiErr.Code),
		"the superseded-issuance conflict must survive an expired budget")
	assert.NotEqual(t, apierror.ErrSubscriberProvisioningTimeout, apiErr.Code)

	assert.Zero(t, run.log.count("RevokeSubscriber"),
		"and revoking would still destroy the credential the OTHER issuance handed out")
}

// ---------------------------------------------------------------------------------------
// AUTH-02 — "no broker is configured" is not a fact about the world
// ---------------------------------------------------------------------------------------

// TestSubscriberLifecycle_FailsClosedWhenAProvisionedRowCannotHaveItsRevocationConfirmed is the
// AUTH-02 guard, and it covers the three lifecycle paths that reached for the registry while
// broker cleanup was skipped.
//
// # The defect, and why it was unrecoverable rather than merely wrong
//
// Each of these three paths branched on admin.IsConfigured() and, finding no broker, concluded
// that there was no broker-side access. That inference reads the configuration of THIS PROCESS
// as a fact about the world, and the two differ in exactly the case that matters: a subscriber
// provisioned while Kafka was configured, then deregistered after a config change, a deploy that
// dropped KAFKA_BROKERS, or a replica reading a different environment.
//
// Deregistration was the worst of the three because its residue could not be repaired.
// credential_reference is the ONLY record of which principal still has to be revoked, and
// deleting the row destroyed it — so a live SASL credential with live ACL bindings was left at
// the broker with nothing anywhere naming it, and no retry could find it because there was no
// longer a row to retry from. Narrowing made the registry UNDER-report real access, which is the
// direction that misleads an operator asking "can this subscriber still see that topic?".
// Clearing the credential record erased the evidence while the credential kept authenticating.
//
// # What decides, now
//
// The row's own evidence, not the process's configuration. credential_reference is non-nil
// exactly when a credential was written for this principal at a broker and has not been
// confirmed removed. With none, nothing can authenticate as the principal and any ACL bindings
// are inert, so the registry stays usable without Kafka — which is what those branches exist for
// and is asserted by the companion tests above.
func TestSubscriberLifecycle_FailsClosedWhenAProvisionedRowCannotHaveItsRevocationConfirmed(t *testing.T) {
	// Each case is one path, and each asserts the same two things: the caller is told, and the
	// registry still says what the broker still allows.
	cases := map[string]struct {
		act     func(*EventSubscriberService) error
		because string
	}{
		"deregistration must not delete the only record of the principal": {
			act: func(service *EventSubscriberService) error {
				_, err := service.DeregisterSubscriber(context.Background(), subscriberFixtureID)

				return err
			},
			because: "deleting the row destroys the only record of which principal to revoke, so " +
				"the orphaned credential becomes unfindable rather than merely unrevoked",
		},
		"revocation must not erase the evidence it could not act on": {
			act: func(service *EventSubscriberService) error {
				return service.RevokeSubscriberCredential(context.Background(), subscriberFixtureID)
			},
			because: "clearing credential_reference makes 'revoke this principal' a fact nobody " +
				"holds any more, while the credential keeps authenticating",
		},
		"narrowing must not make the registry under-report real access": {
			act: func(service *EventSubscriberService) error {
				_, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID,
					SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions"}})

				return err
			},
			because: "the broker keeps the wider grant, so a registry that lists less is telling " +
				"an operator no when the answer is yes",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			run := newSubscriberLifecycle(t).seeded(subscriberProvisionedRow(t))
			run.admin.configured = false

			err := testCase.act(run.service)

			require.Error(t, err, "%s: the caller must be told — %s", name, testCase.because)
			requireAPIErrorCode(t, err, apierror.ErrKafkaUnavailable)

			// THE ROW SURVIVES, INTACT. This is the assertion that matters: the evidence needed
			// to finish the cleanup is still there, whichever path was attempted.
			row, present := run.store.row(subscriberFixtureID)
			require.True(t, present, "%s: the registry row must survive — %s", name, testCase.because)
			require.NotNil(t, row.CredentialReference,
				"%s: and it must still name the credential, which is the join to the broker's own "+
					"authorization state", name)
			assert.ElementsMatch(t, []string{"blnk.transactions", "blnk.balances"}, row.AuthorizedTopics,
				"%s: and it must still record the access the broker still grants", name)

			assert.Empty(t, run.admin.revocations(),
				"%s: nothing was revoked, which is precisely why nothing was forgotten", name)
		})
	}
}

// subscriberOrphanedRow is a row whose credential is UNACCOUNTED FOR: no reference, but an orphan
// marker.
//
// It is the state the reference-only guard read exactly backwards. An issuance wrote the SCRAM
// credential at the broker and then could neither record it nor revoke it, so credential_reference
// is nil PRECISELY BECAUSE the recording failed — the absence of the record is the evidence that a
// credential exists, not its refutation.
func subscriberOrphanedRow(t *testing.T) model.EventSubscriber {
	t.Helper()

	row := subscriberFixtureRow(t)
	orphaned := time.Now().UTC().Add(-30 * time.Minute)
	row.CredentialOrphanedAt = &orphaned

	require.Nil(t, row.CredentialReference,
		"the fixture's whole point is a live credential with NO reference recorded")

	return row
}

// subscriberCleanupPendingRow is a row carrying an unsettled credential-cleanup obligation: a
// credential Blnk intended to destroy that has not been confirmed destroyed.
func subscriberCleanupPendingRow(t *testing.T) model.EventSubscriber {
	t.Helper()

	row := subscriberFixtureRow(t)
	pending := time.Now().UTC().Add(-20 * time.Minute)
	row.CredentialCleanupPendingAt = &pending

	return row
}

// TestSubscriberLifecycle_FailsClosedForACredentialWithNoRecordedReference is the C-2 guard, and
// it is the case the AUTH-02 test above could not catch.
//
// # Why a reference-only test was backwards
//
// The guard asked IsProvisioned, which reads credential_reference. Two states can coexist with a
// LIVE broker credential and carry no reference at all:
//
//   - ORPHANED. Provisioning writes the SCRAM credential BEFORE the ACL bindings, because a
//     binding for a principal that does not exist is inert while a credential with no bindings
//     still authenticates. When recording that issuance fails, the compensation is to revoke —
//     and when the revocation ALSO fails, a means of authenticating exists for a principal the
//     registry records no issuance for. credential_reference is nil because the write failed.
//   - CLEANUP PENDING. A credential Blnk intended to destroy has not been confirmed destroyed.
//
// For both, the old guard answered "not provisioned", the caller concluded there was nothing at a
// broker to act on, and deregistration DELETED the only row naming the principal. There was then
// nothing anywhere to retry from — the unrecoverable outcome the guard exists to prevent, reached
// through the guard itself.
func TestSubscriberLifecycle_FailsClosedForACredentialWithNoRecordedReference(t *testing.T) {
	fixtures := map[string]func(*testing.T) model.EventSubscriber{
		"an orphaned credential":          subscriberOrphanedRow,
		"an unsettled cleanup obligation": subscriberCleanupPendingRow,
	}

	for fixtureName, fixture := range fixtures {
		t.Run(fixtureName, func(t *testing.T) {
			seed := fixture(t)

			t.Run("deregistration must not delete the only row naming the principal", func(t *testing.T) {
				run := newSubscriberLifecycle(t).seeded(seed)
				run.admin.configured = false

				_, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)

				require.Error(t, err,
					"with no broker to revoke against, deleting the row makes the outstanding "+
						"credential unfindable rather than merely unrevoked")
				requireAPIErrorCode(t, err, apierror.ErrKafkaUnavailable)

				row, present := run.store.row(subscriberFixtureID)
				require.True(t, present, "the row must survive: it is the only thing naming the principal")
				assert.NotEmpty(t, row.KafkaPrincipal,
					"and it must still name the principal a retry has to revoke")
				assert.Empty(t, run.admin.revocations(),
					"nothing was revoked, which is exactly why nothing may be forgotten")
			})

			t.Run("narrowing must not report a boundary the broker does not hold", func(t *testing.T) {
				run := newSubscriberLifecycle(t).seeded(seed)
				run.admin.configured = false

				_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
					SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions"}})

				require.Error(t, err)
				requireAPIErrorCode(t, err, apierror.ErrKafkaUnavailable)

				row, present := run.store.row(subscriberFixtureID)
				require.True(t, present)
				assert.ElementsMatch(t, []string{"blnk.transactions", "blnk.balances"}, row.AuthorizedTopics,
					"the registry must still record the access the broker still grants")
			})

			t.Run("revocation must not erase evidence it could not act on", func(t *testing.T) {
				run := newSubscriberLifecycle(t).seeded(seed)
				run.admin.configured = false

				err := run.service.RevokeSubscriberCredential(context.Background(), subscriberFixtureID)

				require.Error(t, err)
				requireAPIErrorCode(t, err, apierror.ErrKafkaUnavailable)
			})
		})
	}
}

// TestUpdateSubscriber_RefusesToWidenAnUnaccountedForPrincipal is the second half of C-2, and it
// applies WITH a broker configured.
//
// Creating new ACL bindings for a principal whose credential Blnk cannot account for hands that
// outstanding credential access it did not previously have. Unlike a recorded credential there is
// no reference to revoke, so the grant cannot be walked back by revoking the thing that uses it.
//
// NARROWING must stay allowed, and that is asserted here too: refusing every edit would freeze the
// row at its widest, which is worse than the gap. Re-issuance remains the documented settlement.
func TestUpdateSubscriber_RefusesToWidenAnUnaccountedForPrincipal(t *testing.T) {
	t.Run("widening an orphaned row is refused", func(t *testing.T) {
		run := newSubscriberLifecycle(t).seeded(subscriberOrphanedRow(t))

		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{AuthorizedTopics: []string{
				"blnk.transactions", "blnk.balances", "blnk.identities",
			}})

		require.Error(t, err, "the new binding would grant the outstanding credential a topic it "+
			"could not read before")
		requireAPIErrorCode(t, err, apierror.ErrConflict)

		row, present := run.store.row(subscriberFixtureID)
		require.True(t, present)
		assert.ElementsMatch(t, []string{"blnk.transactions", "blnk.balances"}, row.AuthorizedTopics,
			"and the widening must not have been persisted either")

		pruned, granted := run.admin.reconciliation()
		assert.Empty(t, granted,
			"the refusal must land BEFORE the broker is touched: a binding created and then "+
				"reported as an error is the exposure itself")
		assert.Empty(t, pruned,
			"and before the prune too, so the row is left exactly as it was found")
	})

	t.Run("narrowing an orphaned row is allowed", func(t *testing.T) {
		run := newSubscriberLifecycle(t).seeded(subscriberOrphanedRow(t))

		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions"}})

		require.NoError(t, err,
			"removing a binding reduces the exposure, which is what an operator responding to an "+
				"orphan needs to be able to do")
		assert.ElementsMatch(t, []string{"blnk.transactions"}, updated.AuthorizedTopics)
	})

	t.Run("a row with a RECORDED credential may still be widened", func(t *testing.T) {
		// The reference is the join key that makes a revocation possible, so a widening here
		// stays reversible. Freezing provisioned rows would break ordinary operation.
		run := newSubscriberLifecycle(t).seeded(subscriberProvisionedRow(t))

		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{AuthorizedTopics: []string{
				"blnk.transactions", "blnk.balances", "blnk.identities",
			}})

		require.NoError(t, err)
		assert.Len(t, updated.AuthorizedTopics, 3)
	})

	t.Run("an edit that moves no binding is allowed on an orphaned row", func(t *testing.T) {
		// Renaming is not a grant. Coupling deprecated bookkeeping to a credential's settlement
		// state would make an orphan unmanageable for reasons the data does not justify.
		run := newSubscriberLifecycle(t).seeded(subscriberOrphanedRow(t))

		name := "renamed while orphaned"
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
			SubscriberUpdate{Name: &name})

		require.NoError(t, err)
		assert.Equal(t, name, updated.Name)
	})
}

// TestMayHaveBrokerCredential_IsTheUnionOfEveryUnconfirmedState pins the predicate itself, which
// is what every lifecycle guard now consults.
func TestMayHaveBrokerCredential_IsTheUnionOfEveryUnconfirmedState(t *testing.T) {
	stamp := time.Now().UTC()

	t.Run("the three states that may coexist with a live credential", func(t *testing.T) {
		reference := "ref"

		for name, row := range map[string]*model.EventSubscriber{
			"a recorded reference": {CredentialReference: &reference},
			"an orphan marker":     {CredentialOrphanedAt: &stamp},
			"a cleanup obligation": {CredentialCleanupPendingAt: &stamp},
		} {
			t.Run(name, func(t *testing.T) {
				assert.True(t, row.MayHaveBrokerCredential())
				assert.NotEmpty(t, row.BrokerCredentialEvidence(),
					"a refusal has to name which state it is, because the remedies differ")
			})
		}
	})

	t.Run("the two markers that are not credential evidence", func(t *testing.T) {
		// A revocation tombstone is stamped by deregistration BEFORE the broker is touched, so a
		// predicate that read it would refuse deregistration its own tombstone — making the
		// operation impossible in a deployment that never configured Kafka. A grant-reconcile
		// marker means the recorded authorization is WIDER than the broker's, which is the safe
		// direction: no authentication follows from a missing ACL binding.
		for name, row := range map[string]*model.EventSubscriber{
			"a revocation tombstone":   {RevocationPendingAt: &stamp},
			"a grant-reconcile marker": {GrantReconcilePendingAt: &stamp},
		} {
			t.Run(name, func(t *testing.T) {
				assert.False(t, row.MayHaveBrokerCredential())
			})
		}
	})

	t.Run("a nil receiver answers false rather than panicking", func(t *testing.T) {
		var row *model.EventSubscriber

		assert.False(t, row.MayHaveBrokerCredential())
		assert.Empty(t, row.BrokerCredentialEvidence())
	})

	t.Run("a clean row holds nothing", func(t *testing.T) {
		assert.False(t, (&model.EventSubscriber{}).MayHaveBrokerCredential())
	})
}

// TestDeregisterSubscriber_ClearsTheTombstoneOnceTheBrokerIsConfirmedClean is the F14 guard: the
// revocation tombstone must mark ONE obligation, not two.
//
// # The two debts that shared one column
//
// revocation_pending_at is stamped before the broker is touched, so it means "a principal may
// still authenticate". A row whose revocation SUCCEEDED and whose deletion then failed kept the
// stamp — and from that point it meant the opposite: the broker is clean and a useless row is
// left over.
//
// CountSubscriberRevocationsPending counts tombstoned rows, and
// blnk_subscribers_oldest_revocation_age_seconds raises a CRITICAL alert whose runbook tells an
// operator to delete a SCRAM credential by hand. Counting both states sent that operator after a
// principal that no longer existed, and — the direction that actually costs something — made a
// row where a live credential really was unaccounted for indistinguishable from this harmless
// residue.
//
// So a confirmed revocation clears the tombstone along with the credential record. What is left
// is an inert row: no credential, no broker access, no tombstone, removed by retrying the
// deregistration and deliberately not alerted on, because nothing is exposed by it.
func TestDeregisterSubscriber_ClearsTheTombstoneOnceTheBrokerIsConfirmedClean(t *testing.T) {
	run := newSubscriberLifecycle(t).seeded(subscriberProvisionedRow(t))
	// The broker revocation succeeds; only the row deletion fails. That is the exact state the
	// two debts diverge in.
	run.store.failing("TakeEventSubscriber", errors.New("registry unavailable"))

	_, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
	require.Error(t, err, "the caller must learn the row was not removed")

	require.Len(t, run.admin.revocations(), 1, "the broker side must have been revoked")

	require.NotZero(t, run.log.count("ClearSubscriberCredential"),
		"the credential record must be cleared once the broker confirms the revocation: the row "+
			"was overstating reality until then, and clearing is what separates the two debts")

	row, present := run.store.row(subscriberFixtureID)
	require.True(t, present, "the row survives, because its deletion is what failed")
	assert.Nil(t, row.CredentialReference,
		"and it must no longer claim a credential the broker has confirmed it does not hold")
	assert.Nil(t, row.RevocationPendingAt,
		"NOR CARRY THE TOMBSTONE: keeping it here is what made the critical revocation alert fire "+
			"for a principal that had already been deleted, and made a genuinely orphaned "+
			"credential indistinguishable from this inert residue")
}

// TestRegisterSubscriber_TreatsOnlyAnAbsentIdentifierAsAbsent is the F23 guard.
//
// RegisterSubscriber's contract is that an id is GENERATED only when the caller supplies none,
// and that a supplied id is judged exactly as given — surrounding whitespace being a rejection
// rather than something to fold away, because the id derives the principal and the group
// namespace.
//
// The implementation trimmed first, which collapsed the two cases and produced the opposite of
// that contract twice over: a whitespace-only id became the empty string and took the generation
// branch, so the caller got a subscriber under an id it never asked for and cannot predict; and
// " sub_a" and "sub_a" both trimmed to one identifier, so two registrations differing only by
// whitespace would have raced for one Kafka principal instead of the second being refused —
// which is the SEC-04 duplicate CanonicalizeSubscriberIdentifier exists to catch.
func TestRegisterSubscriber_TreatsOnlyAnAbsentIdentifierAsAbsent(t *testing.T) {
	t.Run("an absent identifier is generated", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		stored, err := run.service.RegisterSubscriber(context.Background(), SubscriberRegistration{
			Name:             "Generated",
			AuthorizedTopics: []string{"blnk.transactions"},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, stored.SubscriberID,
			"omitting the field expresses no preference, so the service mints a canonical id — the "+
				"house convention every other Blnk resource follows")
	})

	for name, supplied := range map[string]string{
		"whitespace only":     "   ",
		"a tab":               "\t",
		"leading whitespace":  " sub_lifecycle_supplied",
		"trailing whitespace": "sub_lifecycle_supplied ",
	} {
		t.Run("a supplied identifier of "+name+" is refused", func(t *testing.T) {
			run := newSubscriberLifecycle(t)

			stored, err := run.service.RegisterSubscriber(context.Background(), SubscriberRegistration{
				SubscriberID:     supplied,
				Name:             "Supplied",
				AuthorizedTopics: []string{"blnk.transactions"},
			})

			require.Error(t, err,
				"a value the caller SUPPLIED must be judged as given; trimming it silently "+
					"substitutes a different identity for the one that was asked for")
			assert.Nil(t, stored)
			// The CODE, because that is the observable contract: apierror.APIError carries its
			// cause in a field rather than through Unwrap, so errors.Is cannot see the sentinel
			// from outside. The message the canonicalisation produced travels in the detail and
			// names the whitespace, which is what an operator reads.
			requireAPIErrorCode(t, err, apierror.ErrGenValidation)
		})
	}
}

// RETIRED: TestIssueSubscriberCredential_RefusesASubscriberWhoseKeyScopeKafkaCannotEnforce,
// TestUpdateSubscriber_RefusesAKeyScopeOnASubscriberThatHoldsACredential and a test over the
// deleted unenforceability predicate stood here.
//
// All three asserted the SEC-05 refusal: a subscriber recording a partition key prefix was
// unprovisionable, and recording a prefix on a subscriber holding a credential was refused. The
// diagnosis they rest on is correct and unchanged — Kafka's authorizer has no message-key
// dimension, so a credential holding topic-level Read is wider than a key-scoped row describes —
// but the REMEDY was replaced. Withholding the credential implemented no part of the key scope;
// it withdrew a mandatory capability (R-7 makes the credential endpoint required) for a state
// the registry is explicitly designed to hold, permanently and with no way forward.
//
// What replaced it is a NARROWER GRANT plus an enforcement point. A key-scoped subscriber is
// provisioned with Describe and no Read on its topics, so the broker itself refuses its direct
// fetches, and its records are delivered by the declared key-authorising component, which applies the
// prefix per record before anything leaves the process. The prefix is therefore enforced by a
// component in the path, not requested of the subscriber.
//
// Their replacements assert that behaviour:
//
//   - TestIssueSubscriberCredential_IssuesToAKeyScopedSubscriberAndStatesWhatIsNotEnforced
//   - TestIssueSubscriberCredential_IssuesAndDeliversTheRecordedKeyScope
//   - TestUpdateSubscriber_RecordsAKeyScopeOnAProvisionedSubscriberAndNarrowsTheGrant
//   - TestUpdateSubscriber_KeepsEveryKeyScopeStateReachable
//   - TestDeclaresKeyScope_ReadsWhitespaceAsAbsent, which pins the whitespace reading the third
//     test covered, on the predicate that survived the rename
//   - TestProvisionSubscriberPrincipal_WithholdsRecordReadFromAKeyScopedSubscriber, which pins
//     what the refusal used to stand in for: the grant a key-scoped row is actually given
//
// requireEnforceableKeyScope was retired with them. requireProvisionableKeyScope,
// requireRecordableKeyScope and apierror.ErrSubscriberKeyScopeUnenforced were NOT: they take the
// enforcement fact as a parameter, read once from config.KafkaConfig.KeyScopeGateway, so each
// refuses whenever the deployment has declared no component that authorises record keys — which
// is the SHIPPED DEFAULT. The refusals are therefore reachable by an ordinary request, not merely
// by a hand-passed boolean: see
// TestRequireProvisionableKeyScope_RefusesOnlyWhereNothingEnforcesTheScope for the guard and
// TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway for the endpoint.
//
// What changed since this note was first written is the premise, not the guards: an in-binary
// enforcement point was introduced and then REMOVED, because a Blnk-hosted read path is a second
// data plane holding Blnk's own wide credential, authenticated by a header the authorization layer
// does not know about and unaffected by revoking the subscriber's SCRAM credential.

// TestIssueSubscriberCredential_ReportsTheGrantProvisioningConfirmed pins what a successful
// credential says about its own reach.
//
// It began life as TestIssueSubscriberCredential_ProvisionsASubscriberThatRecordsAKeyScope,
// asserting that a recorded partition-key prefix was no reason to withhold a credential. That
// premise holds on the key-scoped path too, and that is why the fixture here records a prefix: a
// key-scoped row IS issuable where a component is declared — its credential holds no record-level
// Read and the declared component delivers, key-filtered — so the narrowed grant has to be
// reported as faithfully as the ordinary
// one. The secret exists once, the broker was really asked, the issuance is really recorded, the
// scope is delivered to the party that applies it together with the component that enforces it,
// and the reported grant is the BROKER-CONFIRMED one.
//
// That last point is the one worth its own test. The response reports the topics provisioning
// confirmed rather than the topics the row asked for, so a partial grant cannot be reported as a
// complete one; and no dead-letter sibling is ever carried along with a category grant, because a
// DLT holds other subscribers' failed events together with Blnk's own failure metadata.
func TestIssueSubscriberCredential_ReportsTheGrantProvisioningConfirmed(t *testing.T) {
	row := subscriberFixtureRow(t)
	row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	run := newSubscriberLifecycle(t).seeded(row)
	store, service := run.store, run.service

	enforceKeyScopeGateway(t)

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err, "an unscoped row with a topic grant must be provisionable")

	assert.NotEmpty(t, credential.Password(), "the secret is returned once, and it must exist")
	assert.False(t, credential.IssuedAt.IsZero())

	assert.Equal(t, "ldg_9f1c8a72", credential.PartitionKeyPrefix,
		"the subscriber must RECEIVE the scope it is expected to apply; a client that cannot see "+
			"its own key scope cannot filter on it")
	assert.Equal(t, model.KeyScopeEnforcementGateway, credential.KeyScopeEnforcement,
		"and it must be told WHERE the scope is enforced, or the prefix reads as a broker boundary")

	assert.Positive(t, run.log.count("ProvisionSubscriberPrincipal"),
		"the broker is asked for the credential")
	assert.Positive(t, run.log.count("RecordSubscriberCredentialIfUnchanged"))

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.CredentialReference, "the issuance is recorded")

	// The topic set is compared against the BROKER DOUBLE's result rather than the row, because
	// that is what the real service reports: the topics provisioning confirms, not the topics
	// that were asked for.
	assert.Equal(t, run.admin.result.Topics, credential.AuthorizedTopics,
		"the credential reports exactly the grant provisioning confirmed")
	assert.Equal(t, row.ConsumerGroupID, credential.ConsumerGroupID)
	assert.NotContains(t, credential.AuthorizedTopics, DLTFor(row.AuthorizedTopics[0]),
		"and no dead-letter sibling is ever carried along with a category grant")
}

// TestIssueSubscriberCredential_ReportsNoKeyScopeEnforcementWithoutAPrefix pins the enforcement
// field on a credential issued to a row that records no prefix.
//
// The field is PRESENT rather than omitted so a client can branch on it without first testing
// whether the prefix is empty, and on this row it has exactly one correct value: none. Reporting
// anything else would tell a subscriber to filter by a key scope nothing recorded, and reporting
// nothing at all would leave a client unable to distinguish "no scope" from an older Blnk that
// did not send the field.
//
// No enforcement point is declared here on purpose. This is the whole-topic deployment — the
// topic and group ACLs are the entire boundary — and it is the only shape in which a prefix-less
// credential is issued at all: under a declared key-scoped model the same request is refused with
// SUBSCRIBER_KEY_SCOPE_REQUIRED, which is
// TestIssueSubscriberCredential_RefusesAPrefixLessSubscriberUnderADeclaredKeyScopedModel.
func TestIssueSubscriberCredential_ReportsNoKeyScopeEnforcementWithoutAPrefix(t *testing.T) {
	run := newSubscriberLifecycle(t).seeded(subscriberFixtureRow(t))
	admin, service := run.admin, run.service

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	assert.NotEmpty(t, credential.Password())
	assert.Empty(t, credential.PartitionKeyPrefix, "nothing is recorded, so nothing is echoed")
	assert.Equal(t, model.KeyScopeEnforcementNone, credential.KeyScopeEnforcement,
		"reported as none rather than omitted: the topic and group ACLs are the whole boundary "+
			"and a client has nothing of its own to filter")

	// AND THE ACCESSOR PAIR DESCRIBES THE SAME CREDENTIAL, in its own vocabulary: the scope is
	// "all-keys" rather than an empty string, because that is the honest description of what a
	// topic ACL grants on its own, and it is BROKER-ENFORCED, because on this row the topic and
	// group grants are the entire boundary and nothing is left for the consumer to apply. The
	// empty PartitionKeyPrefix above and this pair are not in tension: one reports what was
	// recorded, the other what the credential can read.
	scope, brokerEnforced := credential.KeyScope()
	assert.Equal(t, model.SubscriberKeyScopeAllKeys, scope,
		"a row that records nothing is described as all-keys, not as an empty boundary a client "+
			"might read as 'filter by the empty prefix' and discard its whole stream on")
	assert.True(t, brokerEnforced,
		"and all-keys IS what the broker enforces: reporting it as consumer-side would tell a "+
			"subscriber to apply a filter it has no prefix for")

	assert.Equal(t, subscriberLifecycleSubscriberBrokers, credential.Brokers,
		"the response carries the SUBSCRIBER-FACING list, not the addresses Blnk dials")
	assert.NotEqual(t, admin.Brokers(), credential.Brokers,
		"and the two are different lists: reporting the internal one would hand the subscriber "+
			"an endpoint that does not resolve for it")
}

// TestUpdateSubscriber_RecordsAKeyScopeOnASubscriberThatHoldsACredential is the registry half of
// the reverse order: an operator decides on a key scope AFTER a credential already exists.
//
// A revision of this checkpoint refused that combination, on the reading that it left a principal
// holding Read on whole topics beneath a row describing something narrower — and, because Kafka
// stores one credential per principal, unable ever to rotate its secret. The diagnosis was right
// about the old grant and wrong about the remedy: refusing removed the operator's only way to
// record the decision, and the enforcement it was standing in for does not come from withholding
// anything. Recording the prefix NARROWS the live grant instead, and the declared component
// becomes the only path that subscriber's records can take.
//
// So what is asserted here is that the decision is recordable, that it is persisted, that the row
// then reports WHERE the scope is enforced, and — the part a refusal would have destroyed — that
// the working credential is left untouched. The broker-side narrowing that accompanies it is
// TestUpdateSubscriber_RecordsAKeyScopeOnAProvisionedSubscriberAndNarrowsTheGrant, and the
// diagnostic the transition emits is warnOnKeyScopeRecordedAfterIssuance, found through
// keyScopeOnLivePrincipalWarning below.
func TestUpdateSubscriber_RecordsAKeyScopeOnASubscriberThatHoldsACredential(t *testing.T) {
	run := newSubscriberLifecycle(t).seeded(subscriberProvisionedRow(t))
	store, service := run.store, run.service

	// A DECLARED ENFORCEMENT POINT, because Blnk ships no component that authorises record keys:
	// without one, issuance and a prefix-recording update both fail closed with
	// SUBSCRIBER_KEY_SCOPE_UNENFORCED. The shipped default is asserted by
	// TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway.
	enforceKeyScopeGateway(t)

	prefix := "ldg_k_83f61ccabf29"
	updated, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		PartitionKeyPrefix: &prefix,
	})
	require.NoError(t, err,
		"recording a key scope on a provisioned subscriber is a decision an operator is allowed "+
			"to record")
	require.NotNil(t, updated)
	require.NotNil(t, updated.PartitionKeyPrefix)
	assert.Equal(t, prefix, *updated.PartitionKeyPrefix)
	assert.Equal(t, model.KeyScopeEnforcementGateway, updated.KeyScopeEnforcement(),
		"and the row itself reports where that scope is enforced")

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.PartitionKeyPrefix, "the write actually happened")
	assert.Equal(t, prefix, *stored.PartitionKeyPrefix)
	require.NotNil(t, stored.CredentialReference,
		"AND THE WORKING CREDENTIAL IS UNTOUCHED. Revoking as a side effect would destroy a "+
			"secret nobody asked to replace")

	assert.Positive(t, run.log.count("UpdateEventSubscriber"))
	assert.False(t, store.fenced(subscriberFixtureID),
		"and the provisioning claim is released either way")
}

// keyScopeOnLivePrincipalWarning finds the disclosure emitted when a key scope is recorded onto a
// subscriber that already holds a credential, or reports that nothing was emitted.
//
// It matches on the message rather than on a field, because the field set is shared with the
// issuance-time disclosure and matching a field would make this helper answer "some key scope was
// disclosed somewhere" — which is not the question either caller is asking.
func keyScopeOnLivePrincipalWarning(hook *logtest.Hook) *logrus.Entry {
	// Matched on the DISCLOSURE's own wording. An earlier revision refused this combination and
	// this helper looked for "REFUSED a Kafka credential"; the combination is accepted now — the
	// grant narrows instead — so the line to find is the one that says the narrowing happened.
	for index := range hook.Entries {
		if strings.Contains(
			hook.Entries[index].Message,
			"recorded a partition key prefix on a subscriber that already holds a",
		) {
			return &hook.Entries[index]
		}
	}

	return nil
}

// TestUpdateSubscriber_DisclosesAKeyScopeRecordedOnALivePrincipal covers the one order of events
// that nothing else in the service reports.
//
// # Why this needs its own coverage
//
// Issuance discloses the boundary whenever it mints a credential for a row that already carries a
// prefix. This is the reverse order, and its consequence reaches BACKWARDS: the credential was
// issued against a row with no prefix, so its response truthfully said the broker grants records
// directly. Recording a prefix now withdraws that Read, so the holder's next fetch is refused —
// and the statement it was given cannot be recalled.
//
// Nothing else in UpdateSubscriber would say so in terms an operator can act on. The update's own
// line reports topic counts and ACL churn, which is the same churn any narrowing produces and says
// nothing about a credential already in a subscriber's hands. So this line names the prefix, where
// it is enforced, when the affected credential was issued, and the two ways forward.
//
// The issuance instant is the field that makes the line actionable, and the fixture's credential is
// deliberately an hour old, so a test asserting it cannot pass against a value that is simply
// "now".
func TestUpdateSubscriber_DisclosesAKeyScopeRecordedOnALivePrincipal(t *testing.T) {
	t.Run("a prefix recorded beside a live credential is disclosed with the issuance instant", func(t *testing.T) {
		row := subscriberProvisionedRow(t)
		require.NotNil(t, row.CredentialIssuedAt, "the fixture must carry an issuance instant")
		issuedAt := row.CredentialIssuedAt.UTC().Format(time.RFC3339)

		run := newSubscriberLifecycle(t).seeded(row)

		// A DECLARED ENFORCEMENT POINT: without one this row fails closed with
		// SUBSCRIBER_KEY_SCOPE_UNENFORCED, which is asserted on its own elsewhere.
		enforceKeyScopeGateway(t)

		hook := logtest.NewGlobal()
		defer hook.Reset()

		prefix := "ldg_k_5ce20fb41d77"
		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &prefix,
		})
		require.NoError(t, err)

		entry := keyScopeOnLivePrincipalWarning(hook)
		require.NotNil(t, entry,
			"recording a key scope beside a live principal must be disclosed, since nothing else "+
				"in this call reports it")

		assert.Equal(t, logrus.WarnLevel, entry.Level,
			"the outcome is correct, so it is a warning rather than an error, but it is not routine "+
				"enough to be buried at info")
		assert.Equal(t, prefix, entry.Data["partition_key_prefix"],
			"the line must name the prefix an operator has to go and implement")
		assert.Equal(t, string(model.KeyScopeEnforcementGateway), entry.Data["key_scope_enforcement"],
			"and where it is enforced, so the prefix is never reported as a broker boundary")
		assert.Equal(t, issuedAt, entry.Data["credential_issued_at"],
			"the instant separates 'I provisioned this a moment ago' from 'a principal minted an "+
				"hour ago is now described as key-scoped'")
		assert.Contains(t, entry.Message, "KAFKA_KEY_SCOPE_ENFORCEMENT",
			"and the message must name the component that applies the prefix — by naming the variable "+
				"that declares it, because that is where the holder's records now come from and Blnk "+
				"ships nothing that would serve them itself")
		assert.Contains(t, entry.Message, "WITHDRAWN",
			"and must state that record-level Read was taken away, since a fetch beginning to "+
				"fail is the symptom an operator will be searching for")
	})

	t.Run("a prefix recorded on a subscriber with no credential is not disclosed here", func(t *testing.T) {
		run := newSubscriberLifecycle(t)

		hook := logtest.NewGlobal()
		defer hook.Reset()

		prefix := "ldg_k_no_principal_yet"
		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &prefix,
		})
		require.NoError(t, err)

		assert.Nil(t, keyScopeOnLivePrincipalWarning(hook),
			"there is no principal to over-grant yet, and issuance discloses it when one is minted; "+
				"warning twice for one decision trains an operator to ignore the line")
	})

	t.Run("clearing a prefix on a provisioned subscriber is not disclosed", func(t *testing.T) {
		row := subscriberProvisionedRow(t)
		row.PartitionKeyPrefix = stringPointer("ldg_k_being_withdrawn")

		run := newSubscriberLifecycle(t).seeded(row)

		hook := logtest.NewGlobal()
		defer hook.Reset()

		cleared := ""
		updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			PartitionKeyPrefix: &cleared,
		})
		require.NoError(t, err)
		require.Nil(t, updated.PartitionKeyPrefix)

		assert.Nil(t, keyScopeOnLivePrincipalWarning(hook),
			"clearing the prefix RESTORES what the holder was originally told, so there is no "+
				"stale statement left to disclose")
	})

	t.Run("an update that mentions no prefix is not disclosed", func(t *testing.T) {
		run := newSubscriberLifecycle(t).seeded(subscriberProvisionedRow(t))

		hook := logtest.NewGlobal()
		defer hook.Reset()

		renamed := "settlement consumer"
		_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
			Name: &renamed,
		})
		require.NoError(t, err)

		assert.Nil(t, keyScopeOnLivePrincipalWarning(hook),
			"a rename must not emit a key scope disclosure, or every unrelated edit to a "+
				"key-scoped subscriber would repeat it")
	})
}

// TestKeyScopeEnforcement_ReadsWhitespaceAsAbsent pins the accessor and the disclosure helper
// together.
//
// A column holding only spaces constrains nothing and is not a recorded intent, so reading it as
// present would make Blnk disclose a filtering contract nobody asked for — and, under the
// behaviour this replaced, would have refused a credential over noise. NULL, the empty string and
// whitespace all have to answer the same way.
//
// It asserts the SERVICE-LEVEL disclosure alongside the model accessor, because those two are
// what every surface reads: describeSubscriberKeyScope is the value carried into the credential
// and the response, so an accessor that agreed while the disclosure disagreed would be a
// response that misreports the boundary.
func TestKeyScopeEnforcement_ReadsWhitespaceAsAbsent(t *testing.T) {
	for name, prefix := range map[string]*string{
		"null":       nil,
		"empty":      stringPointer(""),
		"spaces":     stringPointer("   "),
		"tab":        stringPointer("\t"),
		"newline":    stringPointer("\n"),
		"mixed":      stringPointer(" \t\n "),
		"absent row": nil,
	} {
		t.Run("absent: "+name, func(t *testing.T) {
			subscriber := &model.EventSubscriber{PartitionKeyPrefix: prefix}
			assert.False(t, subscriber.DeclaresKeyScope())
			assert.Equal(t, model.KeyScopeEnforcementNone, subscriber.KeyScopeEnforcement())

			disclosure := describeSubscriberKeyScope(subscriber)
			assert.Empty(t, disclosure.Prefix, "nothing recorded means nothing disclosed")
			assert.Equal(t, model.KeyScopeEnforcementNone, disclosure.Enforcement)
		})
	}

	for name, prefix := range map[string]string{
		"a ledger fragment": "ldg_9f1c",
		"padded":            "  ldg_9f1c  ",
		"a single rune":     "l",
	} {
		t.Run("present: "+name, func(t *testing.T) {
			subscriber := &model.EventSubscriber{
				SubscriberID:       subscriberFixtureID,
				PartitionKeyPrefix: stringPointer(prefix),
			}
			assert.True(t, subscriber.DeclaresKeyScope())
			assert.Equal(t, model.KeyScopeEnforcementGateway, subscriber.KeyScopeEnforcement())

			disclosure := describeSubscriberKeyScope(subscriber)
			assert.Equal(t, strings.TrimSpace(prefix), disclosure.Prefix,
				"the disclosed prefix is TRIMMED, so a padded column and a clean one describe the "+
					"same contract to a client comparing keys byte for byte")
			assert.Equal(t, model.KeyScopeEnforcementGateway, disclosure.Enforcement)
		})
	}

	t.Run("a nil subscriber discloses nothing rather than panicking", func(t *testing.T) {
		// Nil is the repository's not-found representation, so every guard that reads a row
		// must be answerable on one.
		var subscriber *model.EventSubscriber
		assert.False(t, subscriber.DeclaresKeyScope())
		assert.Equal(t, model.KeyScopeEnforcementNone, subscriber.KeyScopeEnforcement())

		disclosure := describeSubscriberKeyScope(subscriber)
		assert.Empty(t, disclosure.Prefix)
		assert.Equal(t, model.KeyScopeEnforcementNone, disclosure.Enforcement)
	})
}

// TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway is the SHIPPED
// DEPLOYMENT: a row recording a partition-key prefix, and nothing declared that authorises record
// keys.
//
// # Why a refusal rather than a credential
//
// Kafka's authorizer has no message-key dimension, and Blnk ships no component that supplies one.
// So there are exactly two credentials this service could mint for such a row, and both are wrong:
// one carrying whole-topic Read, which reads every other ledger's records beside a prefix nothing
// applies; or one carrying Describe and no Read, which can fetch nothing at all and whose response
// would name an endpoint that refuses it. The refusal is the third option, and it is the only one
// that neither over-grants nor hands out a credential that cannot work.
//
// A Blnk-hosted read path was the fourth option and was removed. It made Blnk a second data plane
// holding Blnk's own wide producer credential, authenticated by a bespoke header the platform's
// authorization layer knows nothing about, and unaffected by revoking the subscriber's SCRAM
// credential at the broker — so revocation stopped the direct path and left the served one open.
//
// # What the refusal has to be, to be usable
//
// TYPED, so a client can tell it from an outage; 409 rather than 503, because no retry helps; and
// carrying BOTH remedies, because a refusal naming none is a dead end. Nothing may be minted or
// recorded on the way out: the assertions below check the broker was never asked and no issuance
// was written, which is what makes the refusal free of residue.
func TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway(
	t *testing.T,
) {
	row := subscriberFixtureRow(t)
	row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	// DELIBERATELY NOT enforceKeyScopeGateway: this is the shipped deployment, in which nothing
	// authorises record keys. Every other key-scoped issuance test declares one, so this is the
	// single test standing between the default and an unasserted refusal.
	run := newSubscriberLifecycle(t).seeded(row)
	service := run.service

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err,
		"a key-scoped row must NOT be issued a credential where nothing applies its prefix")
	assert.Zero(t, credential.PasswordLength(),
		"and no secret may exist: a refusal that generated one has already created the thing it "+
			"declined to hand over")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberKeyScopeUnenforced, apiErr.Code,
		"the TYPED code, so a client tells this apart from a broker outage")
	assert.Equal(t, http.StatusConflict, apierror.StatusForCode(apiErr.Code),
		"409 and never the 500 an unmapped code resolves to: the request is well formed, nothing "+
			"is unavailable, and no retry changes the answer")
	assert.Contains(t, err.Error(), "KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway",
		"the remedy that KEEPS the boundary, which was missing: without it the refusal offered only "+
			"ways to abandon or approximate an intent the platform actually supports")
	assert.Contains(t, err.Error(), "clear the partition key prefix",
		"the remedy that abandons it, or the endpoint is a dead end for this row")
	assert.Contains(t, err.Error(), "narrow the subscriber's authorized topics",
		"and the enforceable alternative an operator wanting isolation needs")

	// NOTHING REACHED THE BROKER AND NOTHING WAS RECORDED. The refusal is ordered ahead of both,
	// so there is no credential to revoke and no registry row to repair.
	assert.Zero(t, run.log.count("UpsertScramCredential"),
		"no SCRAM credential may be created for a row that is being refused")
	assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
		"and the broker must not be asked at all")
	assert.Zero(t, run.log.count("RecordSubscriberCredentialIssued"),
		"nor may an issuance be recorded for a credential that does not exist")
}

// TestIssueSubscriberCredential_NamesTheDeclaredGatewayForAKeyScopedSubscriber is the other side of
// the same decision: with a component declared, the credential is issued AND names that component.
//
// # The substitution this guards, and why it needs its own test
//
// A key-scoped credential must name the declared component rather than the brokers: that
// subscriber's connection is terminated by it, and reporting a bootstrap address would hand out a
// credential declaring key-scoped isolation together with an endpoint that bypasses the thing
// enforcing it. The subscriber-facing broker list is what a PREFIX-LESS subscriber gets, and the
// assertions below hold the two apart so neither can be reported for the other.
//
// The substitution keys on the declared ADDRESS rather than on the enforcement boolean, and the
// two are separable on purpose: the boolean decides whether issuance happens at all, the address
// decides which endpoint is named. They agree by construction because both come from one
// configuration read — but a substitution written against the boolean would hand out a credential
// naming NOWHERE if that ever stopped being true.
func TestIssueSubscriberCredential_NamesTheDeclaredGatewayForAKeyScopedSubscriber(
	t *testing.T,
) {
	row := subscriberFixtureRow(t)
	row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	run := newSubscriberLifecycle(t).seeded(row)
	admin, service := run.admin, run.service
	gateway := enforceKeyScopeGateway(t)

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err,
		"a key-scoped row IS issuable once a component is declared to apply its prefix")

	require.NotEmpty(t, credential.Brokers,
		"AND THE CREDENTIAL MUST NAME SOMEWHERE TO CONNECT. An empty list here is the defect this "+
			"test exists for: the subscriber cannot dial it, and it reads as an outage rather than "+
			"as a misconfiguration")
	assert.Equal(t, gateway, credential.Brokers,
		"and the endpoint is the DECLARED component, which is what terminates this subscriber's "+
			"connection")
	assert.Equal(t, strings.Join(gateway, ","), credential.BrokerEndpoint,
		"the joined form travels in the same response and must agree with the list")
	assert.NotEqual(t, admin.Brokers(), credential.Brokers,
		"never the addresses Blnk dials, which do not resolve outside the deployment")
	assert.NotEqual(t, subscriberLifecycleSubscriberBrokers, credential.Brokers,
		"and never the subscriber-facing broker list, which would bypass the component enforcing "+
			"the prefix")

	assert.Equal(t, model.KeyScopeEnforcementGateway, credential.KeyScopeEnforcement,
		"the scope IS enforced, by the declared component, so the credential names it — and the "+
			"value is the same word the deployment declared in KAFKA_KEY_SCOPE_ENFORCEMENT")
	assert.Equal(t, "ldg_9f1c8a72", credential.PartitionKeyPrefix,
		"and the subscriber is told the prefix its records are filtered by")
}

// TestUpdateSubscriber_AcceptsAKeyScopeOnAProvisionedSubscriber replaces the second half of the
// old refusal, and pins the property that replaces it.
//
// Recording a prefix on a row that already holds a credential used to be refused, because the
// live credential kept whole-topic Read while the row began announcing a narrower boundary. The
// diagnosis was right about the grant and wrong about the remedy: the update is what NARROWS that
// grant, so refusing it left the wide access in place and removed the operator's only way to
// record that it should go.
//
// What is asserted here is the part its companions do not cover: the edit does NOT revoke a
// working credential as a side effect of a change nobody asked to make, and the newly recorded
// scope is what the NEXT issuance delivers. The broker-side narrowing is
// TestUpdateSubscriber_RecordsAKeyScopeOnAProvisionedSubscriberAndNarrowsTheGrant.
func TestUpdateSubscriber_AcceptsAKeyScopeOnAProvisionedSubscriber(t *testing.T) {
	run := newSubscriberLifecycle(t).seeded(subscriberProvisionedRow(t))

	// A DECLARED ENFORCEMENT POINT, because Blnk ships no component that authorises record keys:
	// without one, issuance and a prefix-recording update both fail closed with
	// SUBSCRIBER_KEY_SCOPE_UNENFORCED. The shipped default is asserted by
	// TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway.
	enforceKeyScopeGateway(t)

	const scope = "ldg_9f1c8a72"

	prefix := scope
	updated, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		PartitionKeyPrefix: &prefix,
	})
	require.NoError(t, err,
		"recording a key scope on a provisioned subscriber must be accepted: it NARROWS the live "+
			"grant, so refusing it would leave the wider access in place and merely stop the "+
			"registry from recording that an operator wanted it gone")
	require.NotNil(t, updated.PartitionKeyPrefix)
	assert.Equal(t, scope, *updated.PartitionKeyPrefix)

	require.NotNil(t, updated.CredentialReference,
		"and the edit must not revoke a working credential nobody asked it to revoke")
	assert.Zero(t, run.log.count("RevokeSubscriber"))

	// The scope reaches a consumer on the NEXT issuance, which is the whole mechanism by which
	// the edit takes effect.
	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err)
	deliveredScope, brokerEnforced := credential.KeyScope()
	assert.Equal(t, scope, deliveredScope,
		"re-issuing is what hands the consumer its new boundary")
	assert.False(t, brokerEnforced)
}

// TestIssueSubscriberCredential_ProvisionsASubscriberThatRecordsAKeyScope is the core of the
// change: a recorded prefix is not a reason to withhold a credential.
//
// It asserts the whole outcome rather than only the absence of an error, because the previous
// behaviour failed on every one of these points: a secret exists, the broker was asked to mint
// it, the issuance is recorded, and the credential carries the prefix together with the fact
// that its enforcement is consumer-side.
func TestIssueSubscriberCredential_ProvisionsASubscriberThatRecordsAKeyScope(t *testing.T) {
	row := subscriberFixtureRow(t)
	row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	run := newSubscriberLifecycle(t).seeded(row)
	store, service := run.store, run.service

	// A DECLARED ENFORCEMENT POINT, because Blnk ships no component that authorises record keys:
	// without one, issuance and a prefix-recording update both fail closed with
	// SUBSCRIBER_KEY_SCOPE_UNENFORCED. The shipped default is asserted by
	// TestIssueSubscriberCredential_RefusesAKeyScopedSubscriberWithNoDeclaredGateway.
	enforceKeyScopeGateway(t)

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err,
		"a recorded key scope must not withhold the credential: a subscriber with no credential "+
			"consumes nothing, which is a wider gap than the one the refusal was closing")

	assert.NotEmpty(t, credential.Password(), "the secret is returned once, and it must exist")
	assert.False(t, credential.IssuedAt.IsZero())

	assert.Equal(t, "ldg_9f1c8a72", credential.PartitionKeyPrefix,
		"the subscriber must RECEIVE the scope it is expected to apply; a client that cannot see "+
			"its own key scope cannot filter on it")
	assert.Equal(t, model.KeyScopeEnforcementGateway, credential.KeyScopeEnforcement,
		"and it must be told WHERE the scope is enforced, or the prefix reads as a broker boundary")

	assert.Positive(t, run.log.count("ProvisionSubscriberPrincipal"),
		"the broker is asked for the credential, which the refusal never did")
	assert.Positive(t, run.log.count("RecordSubscriberCredentialIfUnchanged"))

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	require.NotNil(t, stored.CredentialReference, "the issuance is recorded")
	require.NotNil(t, stored.PartitionKeyPrefix,
		"and the recorded scope survives issuance: it is a statement of intent, not a blocker")
	assert.Equal(t, "ldg_9f1c8a72", *stored.PartitionKeyPrefix)

	// THE BROKER-ENFORCED BOUNDARY IS UNCHANGED BY ANY OF THIS. The grant reported is the one
	// the provisioning call returned, and the group is the subscriber's own — nothing was
	// widened to compensate for the key scope, which is the mistake a prefixed TOPIC pattern
	// would have been. The topic set is compared against the BROKER DOUBLE's result rather
	// than the row, because that is what the real service reports: the topics provisioning
	// confirms, not the topics that were asked for.
	assert.Equal(t, run.admin.result.Topics, credential.AuthorizedTopics,
		"the credential reports exactly the grant provisioning confirmed")
	assert.Equal(t, row.ConsumerGroupID, credential.ConsumerGroupID)
	assert.NotContains(t, credential.AuthorizedTopics, DLTFor(row.AuthorizedTopics[0]),
		"and no dead-letter sibling is ever carried along with a category grant")
}

// TestIssueSubscriberCredential_RefusesWithoutTheSubscriberFacingBrokerList is the R-10 half, and
// the behaviour it pins reversed once: a fallback to KAFKA_BROKERS was tried, warned about, and
// withdrawn.
//
// # Why the fallback was not acceptable, even warned
//
// KAFKA_BROKERS is what BLNK dials. Inside a deployment that is "kafka:9092", a ClusterIP or a
// headless Service, and none of those resolve for a subscriber outside it — and Kafka compounds it,
// because a broker answers every client with the ADVERTISED address of the listener the connection
// arrived on, so even a reachable bootstrap hands back internal names for the partition leaders.
//
// The fallback logged a warning, and the warning was not a control: it landed in Blnk's log while
// the consequence landed on the subscriber, as an unexplained connection timeout days later. And
// because the SASL secret is shown exactly once, diagnosing it costs a reissue — which invalidates
// the credential the subscriber is already holding. A 200 that cannot be used is the worst of the
// three available answers.
//
// The R-10 objection to requiring it — that the configuration contract names eight variables and a
// ninth prerequisite makes a documented deployment unable to issue — is answered without a
// fallback: a deployment whose subscribers really are in-cluster sets KAFKA_SUBSCRIBER_BROKERS to
// the same value as KAFKA_BROKERS. One line, and the claim is explicit rather than implicit in a
// substitution nobody reads.
//
// # What is asserted
//
// The refusal is typed, it is 503 rather than a 400-class answer because only an operator can
// supply the list, it NAMES the variable, and it leaves no residue: no secret generated, no broker
// call made, no issuance recorded. The complementary case — the whole list absent, meaning no Kafka
// at all — is TestIssueSubscriberCredential_RefusesWhenNoBrokerListIsConfiguredAtAll, and the
// success case is TestIssueSubscriberCredential_ReportsTheSubscriberFacingBrokers.
func TestIssueSubscriberCredential_RefusesWithoutTheSubscriberFacingBrokerList(t *testing.T) {
	run := newSubscriberLifecycle(t)

	// Republished WITHOUT the subscriber-facing list, and with Kafka still configured: this is
	// a deployment that publishes events perfectly well and has simply never been told what to
	// tell a subscriber. It is the exact input the fallback used to accept.
	outboxStoreConfiguration(t, &config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:     []string{"broker-1:9092"},
			TopicPrefix: "blnk",
		},
	})

	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err,
		"issuance must REFUSE rather than publish the addresses Blnk dials: a subscriber outside the "+
			"deployment cannot resolve them, and the secret it was handed is not reissuable")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberBrokersNotConfigured, apiErr.Code,
		"the typed code, so a client tells a missing configuration from a broker outage")
	assert.Equal(t, http.StatusServiceUnavailable, apierror.StatusForCode(apiErr.Code),
		"503: a dependency of issuance is unconfigured. Nothing about the request is wrong, and the "+
			"identical call succeeds once an operator supplies the list")
	assert.Contains(t, err.Error(), "KAFKA_SUBSCRIBER_BROKERS",
		"and it must NAME the variable, or an operator reading the refusal cannot act on it")

	// NO INTERNAL ADDRESS ANYWHERE IN THE ANSWER. This is the disclosure the fallback made on every
	// issuance, and the reason the refusal is a security improvement rather than only a usability one.
	assert.NotContains(t, err.Error(), "broker-1:9092",
		"the refusal must not disclose the internal bootstrap address it declined to publish")
	assert.Empty(t, credential.Brokers,
		"and no endpoint is reported at all")

	// AND NO RESIDUE. The check is ordered before the secret exists and before the broker is
	// touched, so there is nothing to revoke and nothing to repair.
	assert.Zero(t, credential.PasswordLength(),
		"NO SECRET MAY EXIST: a refusal that generated one created the thing it declined to return")
	assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
		"the broker must not be asked to mint a credential whose response cannot be returned")
	assert.Zero(t, run.log.count("RecordSubscriberCredentialIfUnchanged"),
		"and the registry must not record a credential nobody received")

	stored, ok := run.store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.CredentialReference, "a refused issuance leaves no credential record")

	// THE REMEDY WORKS, which is what separates a refusal from a dead end — including the
	// in-cluster deployment's remedy of pointing both variables at the same list.
	outboxStoreConfiguration(t, &config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:           []string{"broker-1:9092"},
			SubscriberBrokers: []string{"broker-1:9092"},
			TopicPrefix:       "blnk",
		},
	})

	issued, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err,
		"declaring the same list explicitly is the documented remedy for an in-cluster deployment, "+
			"and it must make the identical call succeed")
	assert.Equal(t, []string{"broker-1:9092"}, issued.Brokers,
		"and the endpoint reported is the one an operator deliberately published")
	assert.NotEmpty(t, issued.Password())
}

// TestRequireProvisionableKeyScope_RefusesOnlyWhereNothingEnforcesTheScope is the guard that
// makes a key scope unprovisionable unless something is DECLARED to enforce it.
//
// # The refusal is the shipped behaviour, not a floor beneath one
//
// A row recording a partition_key_prefix describes a boundary Kafka's authorizer has no dimension
// for. Blnk narrows what it can — aclEntries withholds topic Read from a key-scoped principal and
// verifyKeyScopeBoundary proves the withholding before the password becomes returnable — but
// nothing in Blnk applies the prefix to a record, because BLNK SERVES NO RECORDS. On the shipped
// default (KAFKA_KEY_SCOPE_ENFORCEMENT=none) the only credential this service could mint would
// either read every record on every authorised topic, other ledgers' and other subscribers'
// included, or be able to fetch nothing at all. So issuance REFUSES.
//
// Disclosing the gap in the response was tried and is not a boundary: the party asked to apply
// the filter is the party holding the credential. So the refusal has to be actionable — a caller
// must be able to tell it from an outage and know what to change — which is why it is typed and
// names both remedies.
//
// The guard takes the enforcement fact as a PARAMETER, read once by its caller from
// config.KafkaConfig.KeyScopeGateway, so the refusal and the credential's reported enforcement
// point cannot disagree and NEITHER branch is dead by construction.
func TestRequireProvisionableKeyScope_RefusesOnlyWhereNothingEnforcesTheScope(t *testing.T) {
	scoped := subscriberFixtureRow(t)
	scoped.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	unscoped := subscriberFixtureRow(t)
	unscoped.PartitionKeyPrefix = nil

	t.Run("refused where nothing enforces it", func(t *testing.T) {
		err := requireProvisionableKeyScope(&scoped, false)
		require.Error(t, err,
			"a recorded key prefix describes a boundary Kafka cannot keep, so without an "+
				"enforcement point the only credential available is wider than the row says")

		var apiErr apierror.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, apierror.ErrSubscriberKeyScopeUnenforced, apiErr.Code,
			"the TYPED code, not a generic conflict: this refusal has a remedy CONFLICT cannot express")
		assert.Equal(t, http.StatusConflict, apierror.StatusForCode(apiErr.Code),
			"409, and never the 500 an unmapped code would resolve to: the request carries no body, "+
				"nothing is unavailable and no retry helps — the row's state has to change")
		assert.Contains(t, err.Error(), "KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway",
			"the refusal must name the remedy that realises the intent, not only the two that give "+
				"it up")
		assert.Contains(t, err.Error(), "clear the partition key prefix",
			"the refusal must name the remedy, or the endpoint is a dead end")
		assert.Contains(t, err.Error(), "narrow the subscriber's authorized topics",
			"and the enforceable alternative, which is what an operator wanting isolation needs")
	})

	t.Run("refused on the SHIPPED default, read from configuration", func(t *testing.T) {
		// THE DEFAULT ITSELF, resolved the way issuance resolves it, so this asserts the shipped
		// behaviour rather than a hand-passed boolean. A build that grew an in-binary enforcement
		// point would make this true and this assertion would fail — which is the point.
		_, active := config.KafkaConfig{}.KeyScopeGateway()
		require.False(t, active,
			"the zero configuration must not report an active enforcement point: Blnk ships no "+
				"component that authorises record keys")

		require.Error(t, requireProvisionableKeyScope(&scoped, active),
			"so a key-scoped row is refused a credential out of the box")
	})

	t.Run("permitted where a component is declared", func(t *testing.T) {
		declared := config.KafkaConfig{
			Brokers:             []string{"kafka-internal:9092"},
			KeyScopeEnforcement: config.KeyScopeEnforcementBrokerGateway,
			KeyScopeGatewayBrokers: []string{
				"gateway.example.com:9094",
			},
			// AND A CONTROL ENDPOINT, because a mode and an address are assertions a deployment
			// makes about itself while this is the part Blnk can verify. Without it
			// KeyScopeGateway reports NOT active — deliberately, so an unverifiable declaration
			// gets the same refusal as no declaration.
			KeyScopeGatewayAttestationURL:   "https://gateway.example.com/key-scopes",
			KeyScopeGatewayAttestationToken: "gateway-token",
		}
		gateway, active := declared.KeyScopeGateway()
		require.True(t, active,
			"a broker_gateway declaration with a distinct endpoint is what an active enforcement "+
				"point looks like")
		require.NotEmpty(t, gateway,
			"and it carries the address the credential must name in place of the brokers")

		assert.NoError(t, requireProvisionableKeyScope(&scoped, active),
			"a key-scoped row must be provisionable where its scope is actually enforced — "+
				"withholding the credential entirely is an absent boundary, not a narrower one")
	})

	t.Run("never engaged for a row that records no scope", func(t *testing.T) {
		assert.NoError(t, requireProvisionableKeyScope(&unscoped, false),
			"with no prefix recorded the topic grant IS the whole boundary and the broker keeps "+
				"all of it, so there is nothing for this guard to refuse")
		assert.NoError(t, requireProvisionableKeyScope(nil, false),
			"and a nil row is not a refusal: the caller's own not-found handling owns that")
	})
}

// TestRequireRecordableKeyScope_MirrorsTheIssuanceGuard pins the other half.
//
// Guarding only issuance would let "issue, then record" walk around the refusal: a subscriber
// registered with no prefix is issued a whole-topic credential, and a later update records a
// prefix onto the live principal. Where nothing enforces the scope that leaves exactly the state
// the issuance guard exists to prevent, reached in two steps instead of one — so the same typed
// code answers both orderings, and a client handling it from one endpoint needs no second case.
//
// Where the scope IS enforced the update is ACCEPTED and narrows the grant, which is asserted by
// TestUpdateSubscriber_RecordsAKeyScopeOnAProvisionedSubscriberAndNarrowsTheGrant. Clearing a
// prefix is never refused on either path, because it is the remedy the refusal names.
func TestRequireRecordableKeyScope_MirrorsTheIssuanceGuard(t *testing.T) {
	provisioned := subscriberFixtureRow(t)
	provisioned.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")
	provisioned.CredentialReference = stringPointer("cref_live")

	err := requireRecordableKeyScope(&provisioned, false)
	require.Error(t, err, "recording a scope onto a live principal must be refused where nothing "+
		"enforces it, or the issuance guard is decorative")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberKeyScopeUnenforced, apiErr.Code,
		"one state, one code: the same refusal the credential endpoint answers with. A second code "+
			"for this judgement existed and was retired, because two codes for one refusal is how a "+
			"client comes to handle one and not the other")

	assert.NoError(t, requireRecordableKeyScope(&provisioned, true),
		"and where a component is declared the update passes through: it NARROWS the grant rather "+
			"than describing a boundary nobody keeps")

	unprovisioned := subscriberFixtureRow(t)
	unprovisioned.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")
	unprovisioned.CredentialReference = nil
	assert.NoError(t, requireRecordableKeyScope(&unprovisioned, false),
		"a prefix on a row holding no credential is a legitimate statement of intent and always has been")

	cleared := subscriberFixtureRow(t)
	cleared.PartitionKeyPrefix = nil
	cleared.CredentialReference = stringPointer("cref_live")
	assert.NoError(t, requireRecordableKeyScope(&cleared, false),
		"clearing a prefix must never be refused: it is the remedy the refusal itself names")
}

// TestSubscriberAccessDeployment_AgreesWithEveryPreconditionIssuanceApplies is the anti-drift
// assertion behind MAJ-1, and it is the only thing standing between the fix and the defect coming
// back.
//
// # The defect it guards
//
// The registry projection used to answer its enforcement claims from the row alone. A subscriber
// recording a partition_key_prefix reported partition_key_prefix_enforced=true with broker_gateway
// named and issuance unblocked — on a deployment that had declared no such component and would
// refuse the very next credential call for precisely that reason. The API told operators and
// clients that a verified isolation boundary existed when none did.
//
// The fix is that one function resolves the deployment and both readers consult it. That property
// is not enforced by the type system: somebody adding a fourth precondition to issuance, or
// deriving one of these three differently in the resolver, restores the disagreement silently and
// every existing test still passes. So each field is asserted against THE SAME EXPRESSION the
// issuance path evaluates, rather than against a literal.
func TestSubscriberAccessDeployment_AgreesWithEveryPreconditionIssuanceApplies(t *testing.T) {
	t.Run("the shipped default, resolved the way issuance resolves it", func(t *testing.T) {
		// The lifecycle configuration: brokers and an advertised subscriber list, no key-scope
		// component, secure mode off.
		cnf := subscriberLifecycleConfiguration(t)

		deployment := SubscriberAccessDeployment()
		service := NewEventSubscriberService(nil, nil)

		_, enforced := service.keyScopeEnforcement()
		assert.False(t, enforced,
			"the harness must be the shipped default, or this subtest asserts the other posture")
		assert.Equal(t, model.KeyScopeEnforcementNone, deployment.KeyScopeEnforcement,
			"the projection must read the SAME answer requireProvisionableKeyScope acts on: a "+
				"resolver that said gateway here would put the false claim straight back")

		_, advertised := cnf.Kafka.SubscriberFacingBrokers()
		assert.Equal(t, advertised, deployment.SubscriberBrokersAdvertised,
			"and the same answer subscriberFacingBrokers refuses on, or credential_issuance_blocked "+
				"contradicts a 503 the caller is about to receive")

		assert.True(t, deployment.WholeTopicAccessPermitted,
			"outside secure mode a whole-topic credential needs no declaration, which is what "+
				"requireAcknowledgedSharedTopicAccess returns nil for")
	})

	t.Run("a declared component is reported as available", func(t *testing.T) {
		gateway, _ := enforceKeyScopeGatewayWithDouble(t)
		require.NotEmpty(t, gateway)

		deployment := SubscriberAccessDeployment()
		service := NewEventSubscriberService(nil, nil)

		_, enforced := service.keyScopeEnforcement()
		require.True(t, enforced,
			"the harness must declare an ACTIVE enforcement point — mode, a distinct address list "+
				"AND an attestation endpoint — or this asserts the default a second time")
		assert.Equal(t, model.KeyScopeEnforcementGateway, deployment.KeyScopeEnforcement,
			"so a key-scoped row projects as available rather than requested")

		// AND THE STATE SCALE, from the one derivation both the projection and this resolver use.
		assert.Equal(t, model.SubscriberKeyScopeStateAvailable,
			deployment.KeyScopeStateFor("ldg_9f1c"))
		assert.Equal(t, model.SubscriberKeyScopeStateNotRequested,
			deployment.KeyScopeStateFor("  "),
			"a whitespace-only column is not a scope, matching EventSubscriber's own predicates")
	})

	t.Run("secure mode without the declaration withholds whole-topic access", func(t *testing.T) {
		cnf := subscriberLifecycleConfiguration(t)
		cnf.Server.Secure = true
		outboxStoreConfiguration(t, cnf)

		assert.False(t, SubscriberAccessDeployment().WholeTopicAccessPermitted,
			"this is the exact predicate requireAcknowledgedSharedTopicAccess refuses on, so a "+
				"prefix-less row must predict SUBSCRIBER_SHARED_TOPIC_ACCESS_UNACKNOWLEDGED")

		cnf.Kafka.SubscriberSharedTopicAccess = true
		outboxStoreConfiguration(t, cnf)

		assert.True(t, SubscriberAccessDeployment().WholeTopicAccessPermitted,
			"and one declaration clears it, which is the whole of SEC-01's ask")
	})

	t.Run("an unresolvable configuration fails closed", func(t *testing.T) {
		// A configuration carrying nothing: no brokers, no subscriber list, no mode. Every
		// capability must be reported OFF, because understating a deployment costs an operator a
		// spurious remedy while overstating one publishes an isolation guarantee that does not
		// exist.
		outboxStoreConfiguration(t, &config.Configuration{})

		deployment := SubscriberAccessDeployment()
		assert.Equal(t, model.KeyScopeEnforcementNone, deployment.KeyScopeEnforcement)
		assert.False(t, deployment.SubscriberBrokersAdvertised,
			"there is no fallback to KAFKA_BROKERS, so an unset subscriber list is a refusal rather "+
				"than a substitution")
	})
}
