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
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/mock"
	"math/big"
	"sync"
	"time"
)

// MockDataSource is a mock implementation of the IDataSource interface
type MockDataSource struct {
	mock.Mock

	// capturedEventOutboxes records every event outbox row handed to the three atomic
	// transaction writers, in call order.
	capturedEventOutboxes []*model.EventOutbox

	// capturedMu guards capturedEventOutboxes. The writers are called from goroutines in
	// several tests, and testify's own lock does not extend to fields this file adds, so
	// an unguarded slice append would be a data race the race detector fails the suite on.
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

// CapturedEventOutboxes returns a copy of every non-nil event outbox row passed to the
// atomic transaction writers so far, in call order.
//
// A copy is returned rather than the backing slice so a test can hold the result across
// further calls without it changing underneath, and so no test can mutate the mock's
// record of what it observed.
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

// runEventPreparer runs the first non-nil preparer from a create writer's variadic tail
// against the entity that create is about to return, and records the row it produced.
//
// Parameters:
//   - entity T: the created entity, exactly as it will be returned to the caller.
//   - preparers []database.EventPreparer[T]: the writer's variadic tail.
//
// Returns:
//   - error: the preparer's error, which the caller returns as the create's error just
//     as the real writer aborts its transaction.
func runEventPreparer[T any](m *MockDataSource, entity T, preparers []database.EventPreparer[T]) error {
	for _, prepare := range preparers {
		if prepare == nil {
			continue
		}

		row, err := prepare(entity)
		if err != nil {
			return err
		}

		m.captureEventOutboxes([]*model.EventOutbox{row})

		return nil
	}

	return nil
}

// Compile-time proof that MockDataSource still satisfies the full IDataSource contract.
//
// It is stated here, immediately after the type it constrains and ahead of the method
// bodies, so that the contract this file exists to satisfy is the first thing a reader
// meets rather than something they have to find at the bottom.
var _ database.IDataSource = (*MockDataSource)(nil)

// RecordTransaction accepts the same variadic event outbox tail as the real datasource,
// records it through captureEventOutboxes, and deliberately does not forward it into
// m.Called() — for the same reason the atomic writers below do not. Every expectation
// already written as m.On("RecordTransaction", ctx, txn) keeps matching, while a test
// can still assert that the rejection path committed its transaction.rejected event
// with the status mutation.
func (m *MockDataSource) RecordTransaction(ctx context.Context, txn *model.Transaction, eventOutbox ...*model.EventOutbox) (*model.Transaction, error) {
	m.captureEventOutboxes(eventOutbox)
	args := m.Called(ctx, txn)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Transaction), args.Error(1)
}

func (m *MockDataSource) RecordTransactionWithBalances(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.Transaction, error) {
	args := m.Called(ctx, txn, sourceBalance, destinationBalance)
	return args.Get(0).(*model.Transaction), args.Error(1)
}

// The three atomic writers below accept the same variadic event outbox tail as the real
// datasource. They RECORD it, through captureEventOutboxes, and they deliberately DO
// NOT forward it into m.Called().
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

// CreateLedger accepts the same variadic EventPreparer tail as the real datasource and
// RUNS the preparer against the ledger it is about to return, recording the row it
// produced.
func (m *MockDataSource) CreateLedger(ledger model.Ledger, prepareEvent ...database.EventPreparer[model.Ledger]) (model.Ledger, error) {
	args := m.Called(ledger)
	created, err := args.Get(0).(model.Ledger), args.Error(1)
	if err != nil {
		return created, err
	}

	if prepareErr := runEventPreparer(m, created, prepareEvent); prepareErr != nil {
		return model.Ledger{}, prepareErr
	}

	return created, nil
}

// CreateLedgerWithEvent records the variadic event rows through captureEventOutboxes
// and forwards only the ledger into m.Called, so an expectation written for
// CreateLedger matches this variant unchanged. The reasoning is the same one documented
// above RecordTransactionWithBalancesAndOutbox: forwarding a variadic changes the
// argument count testify matches on, and that breaks at run time rather than at compile
// time. Assert atomic capture on CapturedEventOutboxes.
func (m *MockDataSource) CreateLedgerWithEvent(ctx context.Context, ledger model.Ledger, eventOutbox ...*model.EventOutbox) (model.Ledger, error) {
	m.captureEventOutboxes(eventOutbox)
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

// CreateBalance accepts and RUNS the variadic EventPreparer tail against the balance it
// is about to return, recording the row it produced. See CreateLedger for why the
// preparer is run rather than dropped.
//
// The empty-balance case is honoured as the real writer honours it: a create reported
// as successful with no BalanceID created nothing, so nothing is captured.
func (m *MockDataSource) CreateBalance(balance model.Balance, prepareEvent ...database.EventPreparer[model.Balance]) (model.Balance, error) {
	args := m.Called(balance)
	created, err := args.Get(0).(model.Balance), args.Error(1)
	if err != nil || created.BalanceID == "" {
		return created, err
	}

	if prepareErr := runEventPreparer(m, created, prepareEvent); prepareErr != nil {
		return model.Balance{}, prepareErr
	}

	return created, nil
}

// CreateBalanceWithEvent behaves like CreateLedgerWithEvent: the event rows are
// recorded, not forwarded, so existing CreateBalance expectations keep matching.
func (m *MockDataSource) CreateBalanceWithEvent(ctx context.Context, balance model.Balance, eventOutbox ...*model.EventOutbox) (model.Balance, error) {
	m.captureEventOutboxes(eventOutbox)
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

// CreateIdentity accepts and RUNS the variadic EventPreparer tail against the
// identity it is about to return, recording the row it produced. See CreateLedger for
// why the preparer is run rather than dropped.
func (m *MockDataSource) CreateIdentity(identity model.Identity, prepareEvent ...database.EventPreparer[model.Identity]) (model.Identity, error) {
	args := m.Called(identity)
	created, err := args.Get(0).(model.Identity), args.Error(1)
	if err != nil {
		return created, err
	}

	if prepareErr := runEventPreparer(m, created, prepareEvent); prepareErr != nil {
		return model.Identity{}, prepareErr
	}

	return created, nil
}

// CreateIdentityWithEvent behaves like CreateLedgerWithEvent: the event rows are
// recorded, not forwarded, so existing CreateIdentity expectations keep matching.
func (m *MockDataSource) CreateIdentityWithEvent(ctx context.Context, identity model.Identity, eventOutbox ...*model.EventOutbox) (model.Identity, error) {
	m.captureEventOutboxes(eventOutbox)
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

// RenewEventOutboxLease reports how many in-flight rows had their lease extended. A test
// that stubs only the error must still supply a count, which the type assertion below
// requires — a renewal reporting an unexpected zero would look like the end of a batch.
func (m *MockDataSource) RenewEventOutboxLease(ctx context.Context, claimToken string, lease time.Duration) (int64, error) {
	args := m.Called(ctx, claimToken, lease)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockDataSource) ClaimFailedEventOutboxForDeadLetter(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	args := m.Called(ctx, batchSize, lockDuration)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.EventOutbox), args.Error(1)
}

func (m *MockDataSource) MarkEventDispatched(ctx context.Context, id int64, claimToken string, record model.BrokerRecord) error {
	args := m.Called(ctx, id, claimToken, record)
	return args.Error(0)
}

// MarkEventFailed returns the outcome the real datasource decides in SQL. A test that
// stubs only the error must still supply an outcome, because the caller reads Exhausted
// to decide whether to dead-letter — returning a zero outcome on the error path is
// correct and is what the nil check below produces.
func (m *MockDataSource) MarkEventFailed(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration, terminal bool, deadLetterLease time.Duration) (model.EventFailureOutcome, error) {
	args := m.Called(ctx, id, claimToken, errMsg, retryAfter, terminal, deadLetterLease)
	if args.Get(0) == nil {
		return model.EventFailureOutcome{}, args.Error(1)
	}
	return args.Get(0).(model.EventFailureOutcome), args.Error(1)
}

// MarkEventPermanentlyFailed returns the outcome the real datasource produces for a
// permanent failure. As with MarkEventFailed a test that stubs only the error must
// still supply an outcome, because the caller reads Exhausted and ClaimToken to perform
// the dead-letter hand-off — and a zero outcome on the error path is what the nil check
// below produces.
func (m *MockDataSource) MarkEventPermanentlyFailed(ctx context.Context, id int64, claimToken, errMsg string, deadLetterLease time.Duration) (model.EventFailureOutcome, error) {
	args := m.Called(ctx, id, claimToken, errMsg, deadLetterLease)
	if args.Get(0) == nil {
		return model.EventFailureOutcome{}, args.Error(1)
	}
	return args.Get(0).(model.EventFailureOutcome), args.Error(1)
}

func (m *MockDataSource) MarkEventDeadLettered(ctx context.Context, id int64, claimToken, dltTopic string, failureMetadata json.RawMessage, record model.BrokerRecord) error {
	args := m.Called(ctx, id, claimToken, dltTopic, failureMetadata, record)
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

// ClaimPendingWebhookDeliveries and MarkWebhookDispatched mock the legacy leg of the
// dual-delivery window and are DELETED with it at the webhook sunset.
func (m *MockDataSource) ClaimPendingWebhookDeliveries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	args := m.Called(ctx, batchSize, lockDuration)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.EventOutbox), args.Error(1)
}

func (m *MockDataSource) MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error {
	args := m.Called(ctx, id, claimToken)
	return args.Error(0)
}

// MarkEventLegacyWebhookAttempted mocks the recovery path's failure transition, which
// records a webhook attempt WITHOUT touching the Kafka leg's terminal state. As with
// MarkEventWebhookPending, a test that stubs only the error must still supply an outcome,
// because the caller reads Abandoned to decide what the operator is told.
func (m *MockDataSource) MarkEventLegacyWebhookAttempted(ctx context.Context, id int64, claimToken string, retryAfter time.Duration) (model.EventWebhookOutcome, error) {
	args := m.Called(ctx, id, claimToken, retryAfter)
	if args.Get(0) == nil {
		return model.EventWebhookOutcome{}, args.Error(1)
	}
	return args.Get(0).(model.EventWebhookOutcome), args.Error(1)
}

// MarkEventWebhookPending returns the outcome the real datasource decides in SQL. As
// with MarkEventFailed, a test that stubs only the error must still supply an outcome:
// the caller reads Abandoned to decide whether the legacy leg is finished or will be
// retried, and a zero outcome on the error path is what the nil check produces.
func (m *MockDataSource) MarkEventWebhookPending(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration, record model.BrokerRecord) (model.EventWebhookOutcome, error) {
	args := m.Called(ctx, id, claimToken, errMsg, retryAfter, record)
	if args.Get(0) == nil {
		return model.EventWebhookOutcome{}, args.Error(1)
	}
	return args.Get(0).(model.EventWebhookOutcome), args.Error(1)
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

func (m *MockDataSource) CountDeadLetteredEvents(ctx context.Context, query model.DeadLetterQuery) (int64, error) {
	args := m.Called(ctx, query)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockDataSource) ListDeadLetteredEvents(ctx context.Context, query model.DeadLetterQuery) ([]model.EventOutbox, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.EventOutbox), args.Error(1)
}

func (m *MockDataSource) ListDeadLetteredEventsFiltered(ctx context.Context, filter model.DeadLetterInventoryFilter, limit, offset int) ([]model.EventOutbox, error) {
	args := m.Called(ctx, filter, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.EventOutbox), args.Error(1)
}

func (m *MockDataSource) ListDeadLetterInventory(ctx context.Context, query model.DeadLetterInventoryQuery) (model.DeadLetterInventoryPage, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return model.DeadLetterInventoryPage{}, args.Error(1)
	}
	return args.Get(0).(model.DeadLetterInventoryPage), args.Error(1)
}

func (m *MockDataSource) ListAndCountDeadLetterInventory(ctx context.Context, query model.DeadLetterInventoryQuery) (model.DeadLetterInventoryPage, int64, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return model.DeadLetterInventoryPage{}, 0, args.Error(2)
	}
	return args.Get(0).(model.DeadLetterInventoryPage), args.Get(1).(int64), args.Error(2)
}

func (m *MockDataSource) OldestDeadLetterAgeByTopic(ctx context.Context, topicPrefix string) ([]model.DeadLetterTopicAge, error) {
	args := m.Called(ctx, topicPrefix)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.DeadLetterTopicAge), args.Error(1)
}

func (m *MockDataSource) CountDeadLetterInventory(ctx context.Context, query model.DeadLetterQuery) (int64, error) {
	args := m.Called(ctx, query)
	return args.Get(0).(int64), args.Error(1)
}

// CountUnresolvedEventOutbox is the LIGHTWEIGHT status aggregate: every non-dispatched status,
// exact and unwindowed. It takes no window argument; a test that expects a window here is
// expecting the on-demand history reading below instead.
func (m *MockDataSource) CountUnresolvedEventOutbox(ctx context.Context) (map[string]int64, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]int64), args.Error(1)
}

func (m *MockDataSource) CountEventOutboxByStatus(ctx context.Context, since time.Time) (map[string]int64, error) {
	args := m.Called(ctx, since)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]int64), args.Error(1)
}

// AuditEventRecordsInIntervals returns the outbox side of the zero-loss reconciliation,
// classified against the measured broker windows. The intervals are part of the
// expectation so a test can assert that the windows actually reached the audit — an
// audit run against no windows classifies every row as unmeasured, which is a different
// verdict entirely.
func (m *MockDataSource) AuditEventRecordsInIntervals(ctx context.Context, intervals []model.PartitionOffsetInterval) (model.EventRecordIntervalAudit, error) {
	args := m.Called(ctx, intervals)
	if args.Get(0) == nil {
		return model.EventRecordIntervalAudit{}, args.Error(1)
	}
	return args.Get(0).(model.EventRecordIntervalAudit), args.Error(1)
}

// AuditEventRecordCoordinates returns the per-partition broker coordinates the outbox
// claims, which the reconciliation checks against the broker's live bounds.
func (m *MockDataSource) AuditEventRecordCoordinates(ctx context.Context) (model.EventRecordCoordinateAudit, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return model.EventRecordCoordinateAudit{}, args.Error(1)
	}
	return args.Get(0).(model.EventRecordCoordinateAudit), args.Error(1)
}

// SumPurgedTerminalEvents returns what retention has removed, which is the reconciliation's
// matched baseline.
func (m *MockDataSource) SumPurgedTerminalEvents(ctx context.Context) (model.EventOutboxPurgeTotals, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return model.EventOutboxPurgeTotals{}, args.Error(1)
	}
	return args.Get(0).(model.EventOutboxPurgeTotals), args.Error(1)
}

// ListUndrainedEventTopics returns the per-topic backlog behind the stranded-prefix audit. A
// nil first argument yields an empty slice rather than a nil one, so a caller ranging over the
// result reads "no topic owes anything" — the correct reading of a failed measurement, and the
// one that keeps the audit from reporting a stranded generation it never saw.
func (m *MockDataSource) ListUndrainedEventTopics(ctx context.Context) ([]model.EventTopicBacklog, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return []model.EventTopicBacklog{}, args.Error(1)
	}
	return args.Get(0).([]model.EventTopicBacklog), args.Error(1)
}

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

func (m *MockDataSource) ListEventSubscribers(ctx context.Context, query model.SubscriberPageQuery) (model.SubscriberPage, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return model.SubscriberPage{}, args.Error(1)
	}
	return args.Get(0).(model.SubscriberPage), args.Error(1)
}

func (m *MockDataSource) ListAndCountEventSubscribers(ctx context.Context, query model.SubscriberPageQuery) (model.SubscriberPage, int64, error) {
	args := m.Called(ctx, query)
	if args.Get(0) == nil {
		return model.SubscriberPage{}, 0, args.Error(2)
	}
	return args.Get(0).(model.SubscriberPage), args.Get(1).(int64), args.Error(2)
}

func (m *MockDataSource) CountEventSubscribers(ctx context.Context) (int64, error) {
	args := m.Called(ctx)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockDataSource) UpdateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber, fenceToken string) (*model.EventSubscriber, error) {
	args := m.Called(ctx, subscriber, fenceToken)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.EventSubscriber), args.Error(1)
}

func (m *MockDataSource) DeleteEventSubscriber(ctx context.Context, subscriberID string) error {
	args := m.Called(ctx, subscriberID)
	return args.Error(0)
}

func (m *MockDataSource) RecordSubscriberCredential(ctx context.Context, subscriberID, credentialReference string, issuedAt time.Time) error {
	args := m.Called(ctx, subscriberID, credentialReference, issuedAt)
	return args.Error(0)
}

func (m *MockDataSource) RecordSubscriberCredentialIfUnchanged(ctx context.Context, subscriberID string, expected *string, credentialReference string, issuedAt time.Time, claimToken string) error {
	args := m.Called(ctx, subscriberID, expected, credentialReference, issuedAt, claimToken)
	return args.Error(0)
}

func (m *MockDataSource) TakeEventSubscriber(ctx context.Context, subscriberID string, claimToken string) (*model.EventSubscriber, error) {
	args := m.Called(ctx, subscriberID, claimToken)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.EventSubscriber), args.Error(1)
}

func (m *MockDataSource) ClearSubscriberCredential(ctx context.Context, subscriberID, fenceToken string) error {
	args := m.Called(ctx, subscriberID, fenceToken)
	return args.Error(0)
}

func (m *MockDataSource) RecordSubscriberWebhookURL(ctx context.Context, subscriberID, webhookURL string) error {
	args := m.Called(ctx, subscriberID, webhookURL)
	return args.Error(0)
}

func (m *MockDataSource) ClearSubscriberWebhookURL(ctx context.Context, subscriberID string) error {
	args := m.Called(ctx, subscriberID)
	return args.Error(0)
}

func (m *MockDataSource) MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error {
	args := m.Called(ctx, subscriberID, migratedAt)
	return args.Error(0)
}

func (m *MockDataSource) CompleteSubscriberWebhookMigration(ctx context.Context, subscriberID string, migratedAt time.Time) (*model.EventSubscriber, error) {
	args := m.Called(ctx, subscriberID, migratedAt)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.EventSubscriber), args.Error(1)
}

func (m *MockDataSource) RecordSubscriberGrantReconcilePending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) error {
	args := m.Called(ctx, subscriberID, pendingAt, fenceToken)
	return args.Error(0)
}

func (m *MockDataSource) ClearSubscriberGrantReconcilePending(ctx context.Context, subscriberID, fenceToken string) error {
	args := m.Called(ctx, subscriberID, fenceToken)
	return args.Error(0)
}

func (m *MockDataSource) RecordSubscriberCredentialCleanupPending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) error {
	args := m.Called(ctx, subscriberID, pendingAt, fenceToken)
	return args.Error(0)
}

// CountSubscriberSettlementObligations returns the settlement backlog. A test that stubs only
// the error must still supply a backlog value, and the nil check below turns that into the zero
// backlog — which callers must NOT publish as a gauge, because a zero would read as "everything
// is settled" when the truth is that nothing could be read.
func (m *MockDataSource) CountSubscriberSettlementObligations(ctx context.Context) (model.SubscriberSettlementBacklog, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return model.SubscriberSettlementBacklog{}, args.Error(1)
	}
	return args.Get(0).(model.SubscriberSettlementBacklog), args.Error(1)
}

func (m *MockDataSource) GetSubscriberSettlementObligation(ctx context.Context, subscriberID string) (model.SubscriberSettlementObligation, error) {
	args := m.Called(ctx, subscriberID)
	if args.Get(0) == nil {
		return model.SubscriberSettlementObligation{}, args.Error(1)
	}
	return args.Get(0).(model.SubscriberSettlementObligation), args.Error(1)
}

// ListSubscriberSettlementObligations returns the settlement backlog. A test that stubs only
// the error must still supply a slice, and the nil check below turns that into an EMPTY
// backlog — which a settlement pass must not read as "nothing is owed", because the truth is
// that nothing could be read.
func (m *MockDataSource) ListSubscriberSettlementObligations(ctx context.Context, limit int, notBefore time.Time) ([]model.SubscriberSettlementObligation, error) {
	args := m.Called(ctx, limit, notBefore)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.SubscriberSettlementObligation), args.Error(1)
}

func (m *MockDataSource) MarkSubscriberSettlementAttempt(ctx context.Context, subscriberID string, attemptedAt time.Time, failure string) error {
	args := m.Called(ctx, subscriberID, attemptedAt, failure)
	return args.Error(0)
}

func (m *MockDataSource) PurgeMigratedSubscriberWebhookURLs(ctx context.Context, migratedBefore time.Time) (int64, error) {
	args := m.Called(ctx, migratedBefore)
	return args.Get(0).(int64), args.Error(1)
}

// CountSubscriberRevocationsPending returns the outstanding-revocation backlog. A test that
// stubs only the error must still supply a backlog value, and the nil check below turns that
// into the zero backlog — which callers must NOT publish as a gauge, because a zero would
// read as "everything is settled" when the truth is that nothing could be read.
func (m *MockDataSource) CountSubscriberRevocationsPending(ctx context.Context) (model.SubscriberRevocationBacklog, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return model.SubscriberRevocationBacklog{}, args.Error(1)
	}
	return args.Get(0).(model.SubscriberRevocationBacklog), args.Error(1)
}

// CountSubscriberAccessResidue returns the unaccounted-access aggregate. A test that stubs only
// the error must still supply a residue value, and the nil check below turns that into the zero
// residue — which callers must NOT publish as a gauge, because a zero would read as "nothing
// outstanding" when the truth is that nothing could be read.
func (m *MockDataSource) CountSubscriberAccessResidue(ctx context.Context) (model.SubscriberAccessResidue, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return model.SubscriberAccessResidue{}, args.Error(1)
	}
	return args.Get(0).(model.SubscriberAccessResidue), args.Error(1)
}

func (m *MockDataSource) MarkSubscriberCredentialOrphaned(ctx context.Context, subscriberID string, orphanedAt time.Time) error {
	args := m.Called(ctx, subscriberID, orphanedAt)
	return args.Error(0)
}

func (m *MockDataSource) MarkSubscriberRevocationFailed(ctx context.Context, subscriberID string, failedAt time.Time) error {
	args := m.Called(ctx, subscriberID, failedAt)
	return args.Error(0)
}

func (m *MockDataSource) MarkSubscriberRevocationPending(ctx context.Context, subscriberID string, pendingAt time.Time, claimToken string) (*model.EventSubscriber, error) {
	args := m.Called(ctx, subscriberID, pendingAt, claimToken)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.EventSubscriber), args.Error(1)
}

func (m *MockDataSource) ClaimSubscriberForProvisioning(ctx context.Context, subscriberID string, lease time.Duration) (string, error) {
	args := m.Called(ctx, subscriberID, lease)
	return args.String(0), args.Error(1)
}

func (m *MockDataSource) RenewSubscriberProvisioningFence(ctx context.Context, subscriberID string, token string, lease time.Duration) error {
	args := m.Called(ctx, subscriberID, token, lease)
	return args.Error(0)
}

func (m *MockDataSource) ReleaseSubscriberProvisioningFence(ctx context.Context, subscriberID string, token string) error {
	args := m.Called(ctx, subscriberID, token)
	return args.Error(0)
}

// ClaimPendingBalanceMonitorHandoffs mocks the FIFO lease over
// blnk.balance_monitor_handoff. A nil first return yields an empty slice rather than a
// nil one, so a test that stubs only the error still drives the caller's "nothing to do"
// branch instead of tripping a nil dereference in it.
func (m *MockDataSource) ClaimPendingBalanceMonitorHandoffs(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.BalanceMonitorHandoff, error) {
	args := m.Called(ctx, batchSize, lockDuration)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.BalanceMonitorHandoff), args.Error(1)
}

// CompleteBalanceMonitorHandoffWithEvents mocks the ONE transaction that writes the
// alerts and the completion together.
func (m *MockDataSource) CompleteBalanceMonitorHandoffWithEvents(ctx context.Context, handoffID string, events []*model.EventOutbox) error {
	args := m.Called(ctx, handoffID, events)
	return args.Error(0)
}

// MarkBalanceMonitorHandoffFailed mocks recording an evaluation failure.
func (m *MockDataSource) MarkBalanceMonitorHandoffFailed(ctx context.Context, handoffID, reason string, permanent bool) error {
	args := m.Called(ctx, handoffID, reason, permanent)
	return args.Error(0)
}

// CountBalanceMonitorHandoffByStatus mocks the handoff status census.
func (m *MockDataSource) CountBalanceMonitorHandoffByStatus(ctx context.Context) (map[string]int64, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]int64), args.Error(1)
}

// InsertBulkTransactionBatch mocks recording that a bulk batch began.
func (m *MockDataSource) InsertBulkTransactionBatch(ctx context.Context, batch *model.BulkTransactionBatch) error {
	args := m.Called(ctx, batch)
	return args.Error(0)
}

// FinalizeBulkTransactionBatchWithEvent mocks the atomic outcome-and-event write. The
// boolean is the "this call performed the transition" answer, which is what tells a
// retrying caller to stop rather than record a second event for one outcome.
func (m *MockDataSource) FinalizeBulkTransactionBatchWithEvent(ctx context.Context, batchID string, outcome *model.BulkTransactionBatch, event *model.EventOutbox) (bool, error) {
	args := m.Called(ctx, batchID, outcome, event)
	return args.Bool(0), args.Error(1)
}

// CountUnfinalizedBulkTransactionBatches mocks the stuck-batch census. The timestamp is
// returned as nil when the stub supplies nil, which is the "there are none" answer the
// caller distinguishes from a zero time.
func (m *MockDataSource) CountUnfinalizedBulkTransactionBatches(ctx context.Context, olderThan time.Duration) (int64, *time.Time, error) {
	args := m.Called(ctx, olderThan)
	var oldest *time.Time
	if args.Get(1) != nil {
		oldest = args.Get(1).(*time.Time)
	}
	return int64(args.Int(0)), oldest, args.Error(2)
}

// ExistingEventIDs mocks the batched durability lookup the post-commit path uses to
// decide whether a transaction's event still has to be captured.
//
// Reached only when event publishing is CONFIGURED — durableTransactionEvents
// short-circuits on an unconfigured publisher — so the many tests that build a
// broker-less configuration never call it and need no expectation for it.
func (m *MockDataSource) ExistingEventIDs(ctx context.Context, eventIDs []string) (map[string]struct{}, error) {
	args := m.Called(ctx, eventIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(map[string]struct{}), args.Error(1)
}

// AuditTerminalEventRecords returns the outbox side of the zero-loss reconciliation. A test
// that stubs only the error must still supply an audit, because the caller reads
// UnconfirmedRows to decide whether the verdict can be conclusive at all — the zero audit the
// nil check produces reports nothing published, which is the correct reading of a failed
// measurement.
func (m *MockDataSource) AuditTerminalEventRecords(ctx context.Context) (model.EventOutboxAudit, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return model.EventOutboxAudit{}, args.Error(1)
	}
	return args.Get(0).(model.EventOutboxAudit), args.Error(1)
}
