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
package mocks

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/big"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/mock"
)

// MockDataSource is a mock implementation of the IDataSource interface
type MockDataSource struct {
	mock.Mock

	// capturedEventOutboxes records every event outbox row handed to the three
	// atomic transaction writers, in call order.
	//
	// It exists because those rows arrive as a VARIADIC tail and cannot be
	// forwarded into m.Called without breaking argument matching for every
	// expectation already written against the writers — see the note above
	// RecordTransactionWithBalancesAndOutbox. Without somewhere to record them, a
	// caller could pass an event row, or fail to pass one, and no test could tell
	// the difference: "the mutation was recorded" was assertable and "its event was
	// captured with it" was not, which is precisely the half of the transactional
	// outbox guarantee that matters.
	//
	// Read it through CapturedEventOutboxes and clear it with
	// ResetCapturedEventOutboxes.
	capturedEventOutboxes []*model.EventOutbox

	// capturedMu guards capturedEventOutboxes. The writers are called from
	// goroutines in several tests, and testify's own lock does not extend to fields
	// this file adds, so an unguarded slice append would be a data race the race
	// detector fails the suite on.
	capturedMu sync.Mutex
}

// captureEventOutboxes records the variadic event rows from one writer call,
// skipping nil entries exactly as the real datasource does so that what is recorded
// is what would have been persisted rather than what was passed.
func (m *MockDataSource) captureEventOutboxes(rows []*model.EventOutbox) {
	if len(rows) == 0 {
		return
	}

	m.capturedMu.Lock()
	defer m.capturedMu.Unlock()
	for _, row := range rows {
		if row == nil {
			continue
		}
		m.capturedEventOutboxes = append(m.capturedEventOutboxes, row)
	}
}

// CapturedEventOutboxes returns a copy of every non-nil event outbox row passed to
// the atomic transaction writers so far, in call order.
//
// A copy is returned rather than the backing slice so a test can hold the result
// across further calls without it changing underneath, and so no test can mutate the
// mock's record of what it observed.
func (m *MockDataSource) CapturedEventOutboxes() []*model.EventOutbox {
	m.capturedMu.Lock()
	defer m.capturedMu.Unlock()

	captured := make([]*model.EventOutbox, len(m.capturedEventOutboxes))
	copy(captured, m.capturedEventOutboxes)
	return captured
}

// ResetCapturedEventOutboxes clears the record, for a test that reuses one mock
// across several phases and needs each phase's captures in isolation.
func (m *MockDataSource) ResetCapturedEventOutboxes() {
	m.capturedMu.Lock()
	defer m.capturedMu.Unlock()
	m.capturedEventOutboxes = nil
}

// Compile-time proof that MockDataSource still satisfies the full IDataSource
// contract.
//
// Without this, a method missing from the mock surfaces as a compile error in
// every unrelated package that builds a mock — the api package, the root blnk
// package, internal/search — with no indication that the mock is the cause. This
// single line turns that confusing suite-wide failure into one clear local one at
// the file that actually needs fixing.
//
// It is stated here, immediately after the type it constrains and ahead of the
// method bodies, so that the contract this file exists to satisfy is the first
// thing a reader meets rather than something they have to find at the bottom.
//
// It belongs in this package and NOT in the database package: database does not
// import mocks, and adding the assertion there would create an import cycle.
var _ database.IDataSource = (*MockDataSource)(nil)

// Transaction methods

func (m *MockDataSource) RecordTransaction(ctx context.Context, txn *model.Transaction) (*model.Transaction, error) {
	args := m.Called(ctx, txn)
	return args.Get(0).(*model.Transaction), args.Error(1)
}

func (m *MockDataSource) RecordTransactionWithBalances(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.Transaction, error) {
	args := m.Called(ctx, txn, sourceBalance, destinationBalance)
	return args.Get(0).(*model.Transaction), args.Error(1)
}

// The three atomic writers below accept the same variadic event outbox tail as the
// real datasource. They RECORD it, through captureEventOutboxes, and they
// deliberately DO NOT forward it into m.Called().
//
// # Why it is recorded
//
// Discarding it made half of the transactional outbox guarantee unassertable. A
// test could confirm that a mutation was recorded and could not confirm that its
// event was captured alongside it — so a caller that stopped passing an event row
// entirely would break the guarantee with every test still green. Recording the
// rows on the mock closes that: see CapturedEventOutboxes.
//
// # Why it is still not forwarded into m.Called
//
// testify matches an expectation by argument count and position, so forwarding the
// variadic would change the argument list every existing expectation was written
// against — a caller that set up five mock.Anything matchers would stop matching
// the moment a sixth argument appeared, and the failure would surface as an
// unexpected-call panic at run time rather than as a compile error. Forwarding it
// only when non-empty is worse still: the mock's arity would then depend on caller
// data, so an expectation would match today and stop matching the day a caller
// began passing rows, with nothing in either file to explain why.
//
// Recording gives assertability without touching argument matching, which is the
// same source-compatibility property the variadic exists to provide on the real
// interface. Assert on CapturedEventOutboxes, not on the Called() argument list.
func (m *MockDataSource) RecordTransactionWithBalancesAndOutbox(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance, outbox *model.LineageOutbox, eventOutbox ...*model.EventOutbox) (*model.Transaction, error) {
	m.captureEventOutboxes(eventOutbox)
	// eventOutbox is deliberately NOT forwarded into m.Called — see the note above.
	// Do not "complete" this call: it breaks matching at run time, not compile time.
	args := m.Called(ctx, txn, sourceBalance, destinationBalance, outbox)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Transaction), args.Error(1)
}

func (m *MockDataSource) RecordTransactionsWithBalancesAndOutboxes(ctx context.Context, txns []*model.Transaction, sourceBalance, destinationBalance *model.Balance, outboxes []*model.LineageOutbox, eventOutboxes ...*model.EventOutbox) ([]*model.Transaction, error) {
	m.captureEventOutboxes(eventOutboxes)
	// eventOutboxes is deliberately NOT forwarded into m.Called — see the note on
	// RecordTransactionWithBalancesAndOutbox above. Adding it here would change the
	// argument count testify matches on, which fails at run time, not compile time.
	args := m.Called(ctx, txns, sourceBalance, destinationBalance, outboxes)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) RecordTransactionsWithBalanceSetAndOutboxes(ctx context.Context, txns []*model.Transaction, balances []*model.Balance, outboxes []*model.LineageOutbox, eventOutboxes ...*model.EventOutbox) ([]*model.Transaction, error) {
	m.captureEventOutboxes(eventOutboxes)
	// eventOutboxes is deliberately NOT forwarded into m.Called — see the note on
	// RecordTransactionWithBalancesAndOutbox above. Adding it here would change the
	// argument count testify matches on, which fails at run time, not compile time.
	args := m.Called(ctx, txns, balances, outboxes)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetTransaction(ctx context.Context, id string) (*model.Transaction, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(*model.Transaction), args.Error(1)
}

func (m *MockDataSource) IsParentTransactionVoid(ctx context.Context, parentID string) (bool, error) {
	args := m.Called(ctx, parentID)
	return args.Bool(0), args.Error(1)
}

func (m *MockDataSource) GetTransactionByRef(ctx context.Context, reference string) (model.Transaction, error) {
	args := m.Called(ctx, reference)
	return args.Get(0).(model.Transaction), args.Error(1)
}

func (m *MockDataSource) TransactionExistsByRef(ctx context.Context, reference string) (bool, error) {
	args := m.Called(ctx, reference)
	return args.Bool(0), args.Error(1)
}

func (m *MockDataSource) GetExistingTransactionReferences(ctx context.Context, references []string) (map[string]struct{}, error) {
	args := m.Called(ctx, references)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]struct{}), args.Error(1)
}

func (m *MockDataSource) GetAllTransactions(ctx context.Context, limit, offset int) ([]model.Transaction, error) {
	args := m.Called(limit, offset)
	return args.Get(0).([]model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetAllTransactionsWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Transaction, error) {
	args := m.Called(ctx, filters, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetAllTransactionsWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Transaction, *int64, error) {
	args := m.Called(ctx, filters, opts, limit, offset)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}
	var count *int64
	if args.Get(1) != nil {
		count = args.Get(1).(*int64)
	}
	return args.Get(0).([]model.Transaction), count, args.Error(2)
}

func (m *MockDataSource) GetTotalCommittedTransactions(ctx context.Context, parentID string) (*big.Int, error) {
	args := m.Called(ctx, parentID)
	return args.Get(0).(*big.Int), args.Error(1)
}

func (m *MockDataSource) GetTransactionsPaginated(ctx context.Context, id string, batchSize int, offset int64) ([]*model.Transaction, error) {
	args := m.Called(ctx, id, batchSize, offset)
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	args := m.Called(ctx, parentTransactionID, batchSize, offset)
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	args := m.Called(ctx, parentTransactionID, batchSize, offset)
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GroupTransactions(ctx context.Context, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	args := m.Called(ctx, groupCriteria, batchSize, offset)
	return args.Get(0).(map[string][]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetTransactionsByParent(ctx context.Context, parentID string, limit int, offset int64) ([]*model.Transaction, error) {
	args := m.Called(ctx, parentID, limit, offset)
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetTransactionsByShadowFor(ctx context.Context, parentTransactionID string) ([]model.Transaction, error) {
	args := m.Called(ctx, parentTransactionID)
	return args.Get(0).([]model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetStuckQueuedTransactions(ctx context.Context, threshold time.Duration, batchSize int) ([]*model.Transaction, error) {
	args := m.Called(ctx, threshold, batchSize)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetQueuedTransactionsForCoalescing(ctx context.Context, source, destination, currency, excludeTransactionID string, createdAtOrAfter time.Time, limit int) ([]*model.Transaction, error) {
	args := m.Called(ctx, source, destination, currency, excludeTransactionID, createdAtOrAfter, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetQueuedTransactionsForSourceCoalescing(ctx context.Context, source, currency, excludeTransactionID string, createdAtOrAfter time.Time, limit int) ([]*model.Transaction, error) {
	args := m.Called(ctx, source, currency, excludeTransactionID, createdAtOrAfter, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetQueuedTransactionsForDestinationCoalescing(ctx context.Context, destination, currency, excludeTransactionID string, createdAtOrAfter time.Time, limit int) ([]*model.Transaction, error) {
	args := m.Called(ctx, destination, currency, excludeTransactionID, createdAtOrAfter, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) CountQueuedTransactionsForPairLane(ctx context.Context, source, destination, currency, lane string) (int, error) {
	args := m.Called(ctx, source, destination, currency, lane)
	return args.Int(0), args.Error(1)
}

// Ledger methods

func (m *MockDataSource) CreateLedger(ledger model.Ledger) (model.Ledger, error) {
	args := m.Called(ledger)
	return args.Get(0).(model.Ledger), args.Error(1)
}

func (m *MockDataSource) GetAllLedgers(limit, offset int) ([]model.Ledger, error) {
	args := m.Called(limit, offset)
	return args.Get(0).([]model.Ledger), args.Error(1)
}

func (m *MockDataSource) GetAllLedgersWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Ledger, error) {
	args := m.Called(ctx, filters, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Ledger), args.Error(1)
}

func (m *MockDataSource) GetAllLedgersWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Ledger, *int64, error) {
	args := m.Called(ctx, filters, opts, limit, offset)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}
	var count *int64
	if args.Get(1) != nil {
		count = args.Get(1).(*int64)
	}
	return args.Get(0).([]model.Ledger), count, args.Error(2)
}

func (m *MockDataSource) GetLedgerByID(id string) (*model.Ledger, error) {
	args := m.Called(id)
	return args.Get(0).(*model.Ledger), args.Error(1)
}

func (m *MockDataSource) UpdateLedger(id, name string) (*model.Ledger, error) {
	args := m.Called(id, name)
	return args.Get(0).(*model.Ledger), args.Error(1)
}

// Metadata update methods
func (m *MockDataSource) UpdateLedgerMetadata(id string, metadata map[string]interface{}) error {
	args := m.Called(id, metadata)
	return args.Error(0)
}

func (m *MockDataSource) UpdateTransactionMetadata(ctx context.Context, id string, metadata map[string]interface{}) error {
	args := m.Called(ctx, id, metadata)
	return args.Error(0)
}

func (m *MockDataSource) UpdateBalanceMetadata(ctx context.Context, id string, metadata map[string]interface{}) error {
	args := m.Called(ctx, id, metadata)
	return args.Error(0)
}

func (m *MockDataSource) UpdateIdentityMetadata(id string, metadata map[string]interface{}) error {
	args := m.Called(id, metadata)
	return args.Error(0)
}

// Balance methods

func (m *MockDataSource) CreateBalance(balance model.Balance) (model.Balance, error) {
	args := m.Called(balance)
	return args.Get(0).(model.Balance), args.Error(1)
}

func (m *MockDataSource) GetBalanceByID(id string, include []string, withQueued bool) (*model.Balance, error) {
	args := m.Called(id, include, withQueued)
	return args.Get(0).(*model.Balance), args.Error(1)
}

func (m *MockDataSource) GetBalanceByIDLite(id string) (*model.Balance, error) {
	args := m.Called(id)
	return args.Get(0).(*model.Balance), args.Error(1)
}

func (m *MockDataSource) GetBalancesByIDsLite(ctx context.Context, ids []string) (map[string]*model.Balance, error) {
	args := m.Called(ctx, ids)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]*model.Balance), args.Error(1)
}

func (m *MockDataSource) GetAllBalances(limit, offset int) ([]model.Balance, error) {
	args := m.Called(limit, offset)
	return args.Get(0).([]model.Balance), args.Error(1)
}

func (m *MockDataSource) GetAllBalancesWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Balance, error) {
	args := m.Called(ctx, filters, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Balance), args.Error(1)
}

func (m *MockDataSource) GetAllBalancesWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Balance, *int64, error) {
	args := m.Called(ctx, filters, opts, limit, offset)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}
	var count *int64
	if args.Get(1) != nil {
		count = args.Get(1).(*int64)
	}
	return args.Get(0).([]model.Balance), count, args.Error(2)
}

func (m *MockDataSource) UpdateBalance(balance *model.Balance) error {
	args := m.Called(balance)
	return args.Error(0)
}

func (m *MockDataSource) GetBalanceByIndicator(indicator, currency string) (*model.Balance, error) {
	args := m.Called(indicator, currency)
	return args.Get(0).(*model.Balance), args.Error(1)
}

func (m *MockDataSource) UpdateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	args := m.Called(ctx, sourceBalance, destinationBalance)
	return args.Error(0)
}

func (m *MockDataSource) GetSourceDestination(sourceId, destinationId string) ([]*model.Balance, error) {
	args := m.Called(sourceId, destinationId)
	return args.Get(0).([]*model.Balance), args.Error(1)
}

func (m *MockDataSource) GetBalanceAtTime(ctx context.Context, balanceID string, targetTime time.Time, fromSource bool) (*model.Balance, error) {
	args := m.Called(ctx, balanceID, targetTime, fromSource)
	return args.Get(0).(*model.Balance), args.Error(1)
}

// Account methods

func (m *MockDataSource) CreateAccount(account model.Account) (model.Account, error) {
	args := m.Called(account)
	return args.Get(0).(model.Account), args.Error(1)
}

func (m *MockDataSource) GetAccountByID(id string, include []string) (*model.Account, error) {
	args := m.Called(id, include)
	return args.Get(0).(*model.Account), args.Error(1)
}

func (m *MockDataSource) GetAllAccounts() ([]model.Account, error) {
	args := m.Called()
	return args.Get(0).([]model.Account), args.Error(1)
}

func (m *MockDataSource) GetAllAccountsWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Account, error) {
	args := m.Called(ctx, filters, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Account), args.Error(1)
}

func (m *MockDataSource) GetAllAccountsWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Account, *int64, error) {
	args := m.Called(ctx, filters, opts, limit, offset)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}
	var count *int64
	if args.Get(1) != nil {
		count = args.Get(1).(*int64)
	}
	return args.Get(0).([]model.Account), count, args.Error(2)
}

func (m *MockDataSource) GetAccountByNumber(number string) (*model.Account, error) {
	args := m.Called(number)
	return args.Get(0).(*model.Account), args.Error(1)
}

func (m *MockDataSource) UpdateAccount(account *model.Account) error {
	args := m.Called(account)
	return args.Error(0)
}

func (m *MockDataSource) DeleteAccount(id string) error {
	args := m.Called(id)
	return args.Error(0)
}

// BalanceMonitor methods

func (m *MockDataSource) CreateMonitor(monitor model.BalanceMonitor) (model.BalanceMonitor, error) {
	args := m.Called(monitor)
	return args.Get(0).(model.BalanceMonitor), args.Error(1)
}

func (m *MockDataSource) GetMonitorByID(id string) (*model.BalanceMonitor, error) {
	args := m.Called(id)
	return args.Get(0).(*model.BalanceMonitor), args.Error(1)
}

func (m *MockDataSource) GetAllMonitors() ([]model.BalanceMonitor, error) {
	args := m.Called()
	return args.Get(0).([]model.BalanceMonitor), args.Error(1)
}

func (m *MockDataSource) GetBalanceMonitors(balanceID string) ([]model.BalanceMonitor, error) {
	args := m.Called(balanceID)
	return args.Get(0).([]model.BalanceMonitor), args.Error(1)
}

func (m *MockDataSource) UpdateMonitor(monitor *model.BalanceMonitor) error {
	args := m.Called(monitor)
	return args.Error(0)
}

func (m *MockDataSource) DeleteMonitor(id string) error {
	args := m.Called(id)
	return args.Error(0)
}

// Identity methods

func (m *MockDataSource) CreateIdentity(identity model.Identity) (model.Identity, error) {
	args := m.Called(identity)
	return args.Get(0).(model.Identity), args.Error(1)
}

func (m *MockDataSource) GetIdentityByID(id string) (*model.Identity, error) {
	args := m.Called(id)
	return args.Get(0).(*model.Identity), args.Error(1)
}

func (m *MockDataSource) GetAllIdentities() ([]model.Identity, error) {
	args := m.Called()
	return args.Get(0).([]model.Identity), args.Error(1)
}

func (m *MockDataSource) GetAllIdentitiesPaginated(limit, offset int) ([]model.Identity, error) {
	args := m.Called(limit, offset)
	return args.Get(0).([]model.Identity), args.Error(1)
}

func (m *MockDataSource) GetAllIdentitiesWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Identity, error) {
	args := m.Called(ctx, filters, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Identity), args.Error(1)
}

func (m *MockDataSource) GetAllIdentitiesWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Identity, *int64, error) {
	args := m.Called(ctx, filters, opts, limit, offset)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}
	var count *int64
	if args.Get(1) != nil {
		count = args.Get(1).(*int64)
	}
	return args.Get(0).([]model.Identity), count, args.Error(2)
}

func (m *MockDataSource) UpdateIdentity(identity *model.Identity) error {
	args := m.Called(identity)
	return args.Error(0)
}

func (m *MockDataSource) DeleteIdentity(id string) error {
	args := m.Called(id)
	return args.Error(0)
}

// Reconciliation methods

func (m *MockDataSource) RecordReconciliation(ctx context.Context, rec *model.Reconciliation) error {
	args := m.Called(ctx, rec)
	return args.Error(0)
}

func (m *MockDataSource) GetReconciliation(ctx context.Context, id string) (*model.Reconciliation, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(*model.Reconciliation), args.Error(1)
}

func (m *MockDataSource) UpdateReconciliationStatus(ctx context.Context, id string, status string, matchedCount, unmatchedCount int) error {
	args := m.Called(ctx, id, status, matchedCount, unmatchedCount)
	return args.Error(0)
}

func (m *MockDataSource) GetReconciliationsByUploadID(ctx context.Context, uploadID string) ([]*model.Reconciliation, error) {
	args := m.Called(ctx, uploadID)
	return args.Get(0).([]*model.Reconciliation), args.Error(1)
}

func (m *MockDataSource) RecordMatch(ctx context.Context, match *model.Match) error {
	args := m.Called(ctx, match)
	return args.Error(0)
}

func (m *MockDataSource) RecordMatches(ctx context.Context, reconciliationID string, match []model.Match) error {
	args := m.Called(ctx, reconciliationID, match)
	return args.Error(0)
}

func (m *MockDataSource) RecordUnmatched(ctx context.Context, reconciliationID string, unmatched []string) error {
	args := m.Called(ctx, reconciliationID, unmatched)
	return args.Error(0)
}

func (m *MockDataSource) GetMatchesByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.Match, error) {
	args := m.Called(ctx, reconciliationID)
	return args.Get(0).([]*model.Match), args.Error(1)
}

func (m *MockDataSource) GetExternalTransactionsPaginated(ctx context.Context, uploadID string, batchSize int, offset int64) ([]*model.ExternalTransaction, error) {
	args := m.Called(ctx, uploadID, batchSize, offset)
	return args.Get(0).([]*model.ExternalTransaction), args.Error(1)
}

func (m *MockDataSource) RecordExternalTransaction(ctx context.Context, tx *model.ExternalTransaction, reconciliationID string) error {
	args := m.Called(ctx, tx, reconciliationID)
	return args.Error(0)
}

func (m *MockDataSource) RecordMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	args := m.Called(ctx, rule)
	return args.Error(0)
}

func (m *MockDataSource) GetMatchingRules(ctx context.Context) ([]*model.MatchingRule, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*model.MatchingRule), args.Error(1)
}

func (m *MockDataSource) GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(*model.MatchingRule), args.Error(1)
}

func (m *MockDataSource) UpdateMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	args := m.Called(ctx, rule)
	return args.Error(0)
}

func (m *MockDataSource) DeleteMatchingRule(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockDataSource) SaveReconciliationProgress(ctx context.Context, reconciliationID string, progress model.ReconciliationProgress) error {
	args := m.Called(ctx, reconciliationID, progress)
	return args.Error(0)
}

func (m *MockDataSource) LoadReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error) {
	args := m.Called(ctx, reconciliationID)
	return args.Get(0).(model.ReconciliationProgress), args.Error(1)
}

func (m *MockDataSource) FetchAndGroupExternalTransactions(ctx context.Context, uploadID string, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	args := m.Called(ctx, uploadID, groupCriteria, batchSize, offset)
	return args.Get(0).(map[string][]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) TakeBalanceSnapshots(ctx context.Context, batchSize int) (int, error) {
	args := m.Called(ctx, batchSize)
	return args.Get(0).(int), args.Error(1)
}

func (m *MockDataSource) CreateAPIKey(ctx context.Context, name, ownerID string, scopes []string, expiresAt time.Time) (*model.APIKey, error) {
	args := m.Called(ctx, name, ownerID, scopes, expiresAt)
	return args.Get(0).(*model.APIKey), args.Error(1)
}

func (m *MockDataSource) ListAPIKeys(ctx context.Context, ownerID string) ([]*model.APIKey, error) {
	args := m.Called(ctx, ownerID)
	return args.Get(0).([]*model.APIKey), args.Error(1)
}

func (m *MockDataSource) RevokeAPIKey(ctx context.Context, id, ownerID string) error {
	args := m.Called(ctx, id, ownerID)
	return args.Error(0)
}

func (m *MockDataSource) GetAPIKey(ctx context.Context, id string) (*model.APIKey, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(*model.APIKey), args.Error(1)
}

func (m *MockDataSource) UpdateLastUsed(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockDataSource) TransactionExistsByIDOrParentID(ctx context.Context, id string) (bool, error) {
	args := m.Called(ctx, id)
	return args.Bool(0), args.Error(1)
}

func (m *MockDataSource) IsTransactionRefunded(ctx context.Context, transaction *model.Transaction) (bool, error) {
	args := m.Called(ctx, transaction)
	return args.Bool(0), args.Error(1)
}

func (m *MockDataSource) UpdateBalanceIdentity(balanceID string, identityID string) error {
	args := m.Called(balanceID, identityID)
	return args.Error(0)
}

func (m *MockDataSource) GetTransactionsByCriteria(ctx context.Context, minAmount, maxAmount *float64, currency *string, minDate, maxDate *time.Time, limit int, offset int64) ([]*model.Transaction, error) {
	args := m.Called(ctx, minAmount, maxAmount, currency, minDate, maxDate, limit, offset)
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

// Lineage methods

func (m *MockDataSource) UpsertLineageMapping(ctx context.Context, mapping model.LineageMapping) error {
	args := m.Called(ctx, mapping)
	return args.Error(0)
}

func (m *MockDataSource) GetLineageMappings(ctx context.Context, balanceID string) ([]model.LineageMapping, error) {
	args := m.Called(ctx, balanceID)
	return args.Get(0).([]model.LineageMapping), args.Error(1)
}

func (m *MockDataSource) GetLineageMappingByProvider(ctx context.Context, balanceID, provider string) (*model.LineageMapping, error) {
	args := m.Called(ctx, balanceID, provider)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.LineageMapping), args.Error(1)
}

func (m *MockDataSource) DeleteLineageMapping(ctx context.Context, id int64) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

// Lineage Outbox methods

func (m *MockDataSource) InsertLineageOutboxInTx(ctx context.Context, tx *sql.Tx, outbox *model.LineageOutbox) error {
	args := m.Called(ctx, tx, outbox)
	return args.Error(0)
}

func (m *MockDataSource) InsertLineageOutbox(ctx context.Context, outbox *model.LineageOutbox) error {
	args := m.Called(ctx, outbox)
	return args.Error(0)
}

func (m *MockDataSource) ClaimPendingOutboxEntries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.LineageOutbox, error) {
	args := m.Called(ctx, batchSize, lockDuration)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.LineageOutbox), args.Error(1)
}

func (m *MockDataSource) MarkOutboxCompleted(ctx context.Context, id int64) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockDataSource) MarkOutboxFailed(ctx context.Context, id int64, errMsg string) error {
	args := m.Called(ctx, id, errMsg)
	return args.Error(0)
}

func (m *MockDataSource) GetOutboxByTransactionID(ctx context.Context, transactionID string) (*model.LineageOutbox, error) {
	args := m.Called(ctx, transactionID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.LineageOutbox), args.Error(1)
}

func (m *MockDataSource) HasPendingCreditOutbox(ctx context.Context, balanceID string) (bool, error) {
	args := m.Called(ctx, balanceID)
	return args.Bool(0), args.Error(1)
}

func (m *MockDataSource) ChainPendingTransactions(ctx context.Context, cutoff time.Time, batchSize int) (int, error) {
	args := m.Called(ctx, cutoff, batchSize)
	return args.Int(0), args.Error(1)
}

func (m *MockDataSource) GetChainState(ctx context.Context) (*model.ChainState, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.ChainState), args.Error(1)
}

func (m *MockDataSource) GetChainedTransactionsAfter(ctx context.Context, afterSeq int64, limit int) ([]model.ChainedTransaction, error) {
	args := m.Called(ctx, afterSeq, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.ChainedTransaction), args.Error(1)
}

func (m *MockDataSource) CountUnchainedTransactions(ctx context.Context, cutoff time.Time) (int64, error) {
	args := m.Called(ctx, cutoff)
	return args.Get(0).(int64), args.Error(1)
}

// Event outbox methods

func (m *MockDataSource) InsertEventOutboxInTx(ctx context.Context, tx *sql.Tx, e *model.EventOutbox) error {
	args := m.Called(ctx, tx, e)
	return args.Error(0)
}

func (m *MockDataSource) InsertEventOutbox(ctx context.Context, e *model.EventOutbox) error {
	args := m.Called(ctx, e)
	return args.Error(0)
}

func (m *MockDataSource) ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	args := m.Called(ctx, batchSize, lockDuration)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.EventOutbox), args.Error(1)
}

func (m *MockDataSource) MarkEventDispatched(ctx context.Context, id int64, claimToken string) error {
	args := m.Called(ctx, id, claimToken)
	return args.Error(0)
}

// MarkEventFailed returns the outcome the real datasource decides in SQL. A test
// that stubs only the error must still supply an outcome, because the caller reads
// Exhausted to decide whether to dead-letter — returning a zero outcome on the
// error path is correct and is what the nil check below produces.
func (m *MockDataSource) MarkEventFailed(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration) (model.EventFailureOutcome, error) {
	args := m.Called(ctx, id, claimToken, errMsg, retryAfter)
	if args.Get(0) == nil {
		return model.EventFailureOutcome{}, args.Error(1)
	}
	return args.Get(0).(model.EventFailureOutcome), args.Error(1)
}

func (m *MockDataSource) MarkEventDeadLettered(ctx context.Context, id int64, claimToken, dltTopic string, failureMetadata json.RawMessage) error {
	args := m.Called(ctx, id, claimToken, dltTopic, failureMetadata)
	return args.Error(0)
}

func (m *MockDataSource) ClaimEventForReplay(ctx context.Context, eventID string, lockDuration time.Duration) (*model.EventOutbox, error) {
	args := m.Called(ctx, eventID, lockDuration)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.EventOutbox), args.Error(1)
}

func (m *MockDataSource) ReleaseEventReplay(ctx context.Context, id int64, claimToken, replayErr string) error {
	args := m.Called(ctx, id, claimToken, replayErr)
	return args.Error(0)
}

func (m *MockDataSource) MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error {
	args := m.Called(ctx, id, claimToken)
	return args.Error(0)
}

func (m *MockDataSource) PurgeTerminalEventsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	args := m.Called(ctx, cutoff, limit)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockDataSource) GetEventByID(ctx context.Context, eventID string) (*model.EventOutbox, error) {
	args := m.Called(ctx, eventID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.EventOutbox), args.Error(1)
}

func (m *MockDataSource) ListDeadLetteredEvents(ctx context.Context, limit, offset int) ([]model.EventOutbox, error) {
	args := m.Called(ctx, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.EventOutbox), args.Error(1)
}

func (m *MockDataSource) CountEventOutboxByStatus(ctx context.Context) (map[string]int64, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]int64), args.Error(1)
}

// Event subscriber methods
//
// None of these carries a plaintext secret, mirroring the real repository: the
// credential method takes an already-derived, non-reversible reference and an
// issuance instant, and no getter hands one back.

func (m *MockDataSource) CreateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (*model.EventSubscriber, error) {
	args := m.Called(ctx, subscriber)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.EventSubscriber), args.Error(1)
}

func (m *MockDataSource) GetEventSubscriberByID(ctx context.Context, subscriberID string) (*model.EventSubscriber, error) {
	args := m.Called(ctx, subscriberID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.EventSubscriber), args.Error(1)
}

func (m *MockDataSource) ListEventSubscribers(ctx context.Context, limit, offset int) ([]model.EventSubscriber, error) {
	args := m.Called(ctx, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.EventSubscriber), args.Error(1)
}

func (m *MockDataSource) UpdateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) error {
	args := m.Called(ctx, subscriber)
	return args.Error(0)
}

func (m *MockDataSource) DeleteEventSubscriber(ctx context.Context, subscriberID string) error {
	args := m.Called(ctx, subscriberID)
	return args.Error(0)
}

func (m *MockDataSource) RecordSubscriberCredential(ctx context.Context, subscriberID, credentialReference string, issuedAt time.Time) error {
	args := m.Called(ctx, subscriberID, credentialReference, issuedAt)
	return args.Error(0)
}

func (m *MockDataSource) RecordSubscriberCredentialIfUnchanged(ctx context.Context, subscriberID string, expected *string, credentialReference string, issuedAt time.Time) error {
	args := m.Called(ctx, subscriberID, expected, credentialReference, issuedAt)
	return args.Error(0)
}

func (m *MockDataSource) TakeEventSubscriber(ctx context.Context, subscriberID string) (*model.EventSubscriber, error) {
	args := m.Called(ctx, subscriberID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.EventSubscriber), args.Error(1)
}

func (m *MockDataSource) ClearSubscriberCredential(ctx context.Context, subscriberID string) error {
	args := m.Called(ctx, subscriberID)
	return args.Error(0)
}

func (m *MockDataSource) MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error {
	args := m.Called(ctx, subscriberID, migratedAt)
	return args.Error(0)
}

func (m *MockDataSource) PurgeMigratedSubscriberWebhookURLs(ctx context.Context, migratedBefore time.Time) (int64, error) {
	args := m.Called(ctx, migratedBefore)
	return args.Get(0).(int64), args.Error(1)
}
