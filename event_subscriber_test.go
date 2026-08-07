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
//	SEC-05  An authorization Kafka cannot enforce is refused, not recorded. A subscriber
//	        carrying a partition key prefix is UNPROVISIONABLE, because Kafka's authorizer has
//	        no message-key dimension and a credential issued against one would grant every
//	        record on every authorised topic while the registry described something narrower.
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
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
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
}

// newSubscriberCallLog builds an empty log.
func newSubscriberCallLog() *subscriberCallLog {
	return &subscriberCallLog{expiredOn: make(map[string]bool)}
}

// record notes one call and the liveness of the context it arrived on.
func (l *subscriberCallLog) record(method string, ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.calls = append(l.calls, method)
	l.expiredOn[method] = ctx.Err() != nil
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
}

// subscriberTestFence is one live provisioning claim.
type subscriberTestFence struct {
	token string
	until time.Time
}

// newSubscriberTestStore builds an empty registry against a shared call log.
func newSubscriberTestStore(log *subscriberCallLog) *subscriberTestStore {
	return &subscriberTestStore{
		rows:     make(map[string]model.EventSubscriber),
		fences:   make(map[string]subscriberTestFence),
		log:      log,
		failures: make(map[string]error),
	}
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
	_, _ int,
) ([]model.EventSubscriber, error) {
	if err := s.record("ListEventSubscribers", ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rows := make([]model.EventSubscriber, 0, len(s.rows))
	for _, row := range s.rows {
		rows = append(rows, row)
	}

	return rows, nil
}

func (s *subscriberTestStore) UpdateEventSubscriber(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) error {
	if err := s.record("UpdateEventSubscriber", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.rows[subscriber.SubscriberID]; !ok {
		return s.notFound(subscriber.SubscriberID)
	}

	stored := *subscriber
	stored.UpdatedAt = time.Now().UTC()
	s.rows[stored.SubscriberID] = stored

	return nil
}

func (s *subscriberTestStore) TakeEventSubscriber(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	if err := s.record("TakeEventSubscriber", ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return nil, s.notFound(subscriberID)
	}

	delete(s.rows, key)

	return s.clone(row), nil
}

func (s *subscriberTestStore) RecordSubscriberCredentialIfUnchanged(
	ctx context.Context,
	subscriberID string,
	expected *string,
	credentialReference string,
	issuedAt time.Time,
) error {
	if err := s.record("RecordSubscriberCredentialIfUnchanged", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return s.notFound(subscriberID)
	}

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

	return nil
}

func (s *subscriberTestStore) ClearSubscriberCredential(
	ctx context.Context,
	subscriberID string,
) error {
	if err := s.record("ClearSubscriberCredential", ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return s.notFound(subscriberID)
	}

	row.CredentialReference = nil
	row.CredentialIssuedAt = nil
	row.UpdatedAt = time.Now().UTC()
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

	stamped := migratedAt
	row.MigratedAt = &stamped
	s.rows[key] = row

	return nil
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
) (*model.EventSubscriber, error) {
	if err := s.record("MarkSubscriberRevocationPending", ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(subscriberID)
	row, ok := s.rows[key]
	if !ok {
		return nil, s.notFound(subscriberID)
	}

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

	key := strings.TrimSpace(subscriberID)
	held, ok := s.fences[key]
	if !ok || held.token != token {
		return apierror.NewAPIError(
			apierror.ErrConflict,
			"The provisioning claim is no longer held by this caller",
			errors.New("subscriber test store: the claim token does not match"),
		)
	}

	delete(s.fences, key)

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
	_ SubscriberProvisioningRequest,
) (SubscriberProvisioningResult, error) {
	if a.onProvision != nil {
		a.onProvision()
	}

	if err := a.record("ProvisionSubscriberPrincipal", ctx); err != nil {
		return SubscriberProvisioningResult{}, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.result, nil
}

func (a *subscriberTestAdmin) RevokeSubscriber(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) error {
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

// TestIssueSubscriberCredential_RefusesASubscriberWhoseKeyScopeKafkaCannotEnforce is the
// SEC-05 guard, and it is the whole of the finding's resolution.
//
// # The defect
//
// partition_key_prefix was recorded, echoed back in API responses and named in the migration as
// part of the subscriber's authorization — and it constrained NOTHING. Kafka's authorizer has no
// message-key dimension: an ACL grants Read on a TOPIC, so a credential issued for a subscriber
// with a key prefix could read EVERY record on EVERY authorised topic, including every other
// tenant's. The registry therefore described a narrower boundary than the one that existed, and
// anyone reading the registry to decide who could see what would read it wrong. A qualification
// in a comment does not survive that reading.
//
// # Why refusal rather than a narrower grant
//
// There is no narrower grant to make. Per-tenant topics are excluded by the AAP, and the
// alternative — issue the credential and hope nobody trusts the column — is the defect. So the
// only fail-closed answer is that a subscriber recording a key scope is UNPROVISIONABLE, and
// clearing the prefix or narrowing the authorised topics is how a caller makes it provisionable.
//
// The refusal is a CONFLICT, not a validation error: the request is well formed and the
// registry state is what refuses it, and the fix is a state change (clear the prefix) rather
// than a differently-worded request.
func TestIssueSubscriberCredential_RefusesASubscriberWhoseKeyScopeKafkaCannotEnforce(t *testing.T) {
	row := subscriberFixtureRow(t)
	row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	run := newSubscriberLifecycle(t).seeded(row)
	store, service := run.store, run.service

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err, "a key scope Kafka cannot enforce must refuse issuance outright")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrSubscriberIsolationUnenforceable, apiErr.Code,
		"the TYPED code, not the generic conflict: both are 409, but a client discriminates on "+
			"the code and this refusal has a specific remedy")
	assert.Equal(t, http.StatusConflict, apierror.StatusForCode(apiErr.Code),
		"the request is well formed; it is the recorded state that refuses it")
	assert.Contains(t, err.Error(), "partition key prefix",
		"the message must name what to change")

	assert.Empty(t, credential.Password(),
		"NO SECRET MAY EXIST: the refusal is taken before one is generated")
	assert.Zero(t, credential.IssuedAt)

	assert.Zero(t, run.log.count("ProvisionSubscriberPrincipal"),
		"the broker must never be asked to mint a credential whose real scope would exceed the "+
			"authorization the registry records")
	assert.Zero(t, run.log.count("RecordSubscriberCredentialIfUnchanged"))

	stored, ok := store.row(subscriberFixtureID)
	require.True(t, ok)
	assert.Nil(t, stored.CredentialReference,
		"a refused issuance must leave no credential record")
}

// TestIssueSubscriberCredential_SucceedsOnceTheKeyScopeIsCleared is the other half of SEC-05:
// the refusal must be RECOVERABLE, not a dead end.
//
// Clearing the prefix is a normal authorization update, and it is what a caller does when it
// accepts that access is granted per topic. If it did not restore provisionability the design
// would be a trap rather than a fail-closed default.
func TestIssueSubscriberCredential_SucceedsOnceTheKeyScopeIsCleared(t *testing.T) {
	row := subscriberFixtureRow(t)
	row.PartitionKeyPrefix = stringPointer("ldg_9f1c8a72")

	run := newSubscriberLifecycle(t).seeded(row)
	admin, service := run.admin, run.service

	_, refused := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, refused)

	cleared := ""
	updated, err := service.UpdateSubscriber(context.Background(), subscriberFixtureID, SubscriberUpdate{
		PartitionKeyPrefix: &cleared,
	})
	require.NoError(t, err)
	require.Nil(t, updated.PartitionKeyPrefix,
		"a present empty string must CLEAR the column, not store an empty prefix")
	assert.False(t, updated.KeyScopeUnenforceable())

	credential, err := service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.NoError(t, err, "clearing the unenforceable constraint must restore provisionability")
	assert.NotEmpty(t, credential.Password())
	assert.Equal(t, subscriberLifecycleSubscriberBrokers, credential.Brokers,
		"the response carries the SUBSCRIBER-FACING list, not the addresses Blnk dials")
	assert.NotEqual(t, admin.Brokers(), credential.Brokers,
		"and the two are different lists: reporting the internal one would hand the subscriber "+
			"an endpoint that does not resolve for it")
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

// TestIssueSubscriberCredential_RefusesWhenNoSubscriberFacingBrokersAreConfigured is the
// fail-closed half.
//
// The tempting implementation is to fall back to the internal list. It returns 200 with an
// endpoint the subscriber cannot dial, so the failure surfaces as a connection timeout in the
// subscriber's own logs days later — with a secret that is returned once and must be reissued
// to retry. Refusing costs an operator one variable.
//
// The refusal must also come BEFORE anything is minted: a credential created at the broker and
// recorded in the registry for a response that cannot be returned is worse than either
// outcome, because the registry then records a credential nobody holds.
func TestIssueSubscriberCredential_RefusesWhenNoSubscriberFacingBrokersAreConfigured(t *testing.T) {
	run := newSubscriberLifecycle(t)

	// Republished WITHOUT the subscriber-facing list, and with Kafka still configured: this is
	// a deployment that publishes events perfectly well and has simply never been told what to
	// tell a subscriber.
	outboxStoreConfiguration(t, &config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:     []string{"broker-1:9092"},
			TopicPrefix: "blnk",
		},
	})

	credential, err := run.service.IssueSubscriberCredential(context.Background(), subscriberFixtureID)
	require.Error(t, err, "issuance must refuse rather than report an unusable endpoint")

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
// yield a NON-EMPTY slice carrying nothing usable. Treating either as configured would report
// a list of blank endpoints to a subscriber — strictly worse than the refusal, because it
// looks like a successful answer.
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
			brokers, ok := config.KafkaConfig{SubscriberBrokers: configured}.SubscriberFacingBrokers()
			assert.False(t, ok, "an unusable list must read as not configured")
			assert.Empty(t, brokers)
		})
	}

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

// TestKeyScopeUnenforceable_ReadsWhitespaceAsAbsent pins the predicate itself.
//
// A column holding only spaces constrains nothing and is not a recorded intent, so reading it
// as present would make a subscriber unprovisionable for a value that means nothing. NULL, the
// empty string and whitespace all have to answer the same way, because a fail-closed check that
// fails closed on noise is a support ticket rather than a security property.
func TestKeyScopeUnenforceable_ReadsWhitespaceAsAbsent(t *testing.T) {
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
			assert.False(t, subscriber.KeyScopeUnenforceable())
			assert.NoError(t, requireProvisionableKeyScope(subscriber))
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
			assert.True(t, subscriber.KeyScopeUnenforceable())
			require.Error(t, requireProvisionableKeyScope(subscriber))
		})
	}

	t.Run("a nil subscriber is not unprovisionable", func(t *testing.T) {
		// Nil is handled elsewhere as a not-found; this check must not be the thing that
		// panics on it.
		var subscriber *model.EventSubscriber
		assert.False(t, subscriber.KeyScopeUnenforceable())
		assert.NoError(t, requireProvisionableKeyScope(subscriber))
	})
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
// longer authenticate — over-reporting rather than under-reporting. The tombstone is what makes
// that row identifiable as needing a retry.
func TestDeregisterSubscriber_KeepsTheRowWhenTheDeleteFailsAfterRevocation(t *testing.T) {
	run := newSubscriberLifecycle(t)
	store, admin, service := run.store, run.admin, run.service
	store.failing("TakeEventSubscriber", errors.New("write path unavailable"))

	returned, err := service.DeregisterSubscriber(context.Background(), subscriberFixtureID)
	require.Error(t, err)
	require.NotNil(t, returned)
	assert.True(t, returned.IsRevocationPending())

	assert.Equal(t, []string{subscriberFixtureID}, admin.revocations(),
		"the revocation DID happen; only the row removal failed")

	stored, present := store.row(subscriberFixtureID)
	require.True(t, present)
	require.NotNil(t, stored.RevocationPendingAt,
		"the tombstone is what marks this row as needing a retry")
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
