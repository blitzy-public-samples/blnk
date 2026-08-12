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

package database

import (
	"context"
	"errors"
	"math/big"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withRegisteredCapture installs a capture for the duration of one test and restores
// the previous registration afterwards.
func withRegisteredCapture(t *testing.T, capture TransactionEventCapture) {
	t.Helper()

	previous := registeredTransactionEventCapture()
	RegisterTransactionEventCapture(capture)
	t.Cleanup(func() { RegisterTransactionEventCapture(previous) })
}

// captureRecordingEachCall returns a capture that builds a distinct row per transaction and
// records the ledger it was handed, so a test can assert BOTH that a row was produced and that
// the writer resolved the right ledger for it.
func captureRecordingEachCall(ledgers map[string]string) TransactionEventCapture {
	return func(_ context.Context, txn *model.Transaction, ledgerID string) (*model.EventOutbox, error) {
		ledgers[txn.TransactionID] = ledgerID

		row := newEventOutboxFixture("derived-" + txn.TransactionID + "-")
		row.LedgerID = ledgerID
		row.PartitionKey = ledgerID

		return row, nil
	}
}

// apiErrorDetail returns the wrapped cause carried on an APIError's Details field.
//
// APIError.Error() renders only the code and the operator-facing message, so a test
// asserting on the diagnostic — which transaction failed, and why — has to read
// Details, where NewAPIError puts the wrapped error.
func apiErrorDetail(t *testing.T, err error) string {
	t.Helper()

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr, "the writer must report a typed API error")

	cause, ok := apiErr.Details.(error)
	require.True(t, ok, "the detail must carry the wrapped cause")

	return cause.Error()
}

// newBatchTransactionFixture builds a transaction complete enough for the batch writer's COPY
// to bind every column, so a test exercising the event capture is not tripped by an unrelated
// nil field.
func newBatchTransactionFixture(id string) *model.Transaction {
	now := time.Now().UTC()

	return &model.Transaction{
		TransactionID: id,
		Source:        "bln_source",
		Reference:     "ref_" + id,
		AmountString:  "10.00",
		PreciseAmount: big.NewInt(1000),
		Precision:     100,
		Currency:      "USD",
		Destination:   "bln_dest",
		Description:   "batched",
		Status:        "APPLIED",
		CreatedAt:     now,
		MetaData:      map[string]interface{}{"batch": true},
		Hash:          "hash_" + id,
		EffectiveDate: &now,
	}
}

// expectTransactionCopy scripts the COPY the batch writer uses for transaction rows.
//
// The batch writer streams transactions through pq.CopyInSchema rather than issuing an
// INSERT, so a test that scripts an INSERT never reaches the event capture that follows
// it.
func expectTransactionCopy(mock sqlmock.Sqlmock) {
	copySQL := pq.CopyInSchema(
		"blnk",
		"transactions",
		"transaction_id",
		"parent_transaction",
		"source",
		"reference",
		"amount",
		"precise_amount",
		"precision",
		"currency",
		"destination",
		"description",
		"status",
		"created_at",
		"meta_data",
		"scheduled_for",
		"hash",
		"effective_date",
	)

	copyPrepare := mock.ExpectPrepare(regexp.QuoteMeta(copySQL))
	copyPrepare.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	copyPrepare.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	copyPrepare.WillBeClosed()
}

// TestResolveBatchEventOutboxes_PrefersSuppliedRowsOverDerivation pins the precedence.
//
// A caller that prepared its own events is authoritative.
func TestResolveBatchEventOutboxes_PrefersSuppliedRowsOverDerivation(t *testing.T) {
	derived := false
	withRegisteredCapture(t, func(_ context.Context, _ *model.Transaction, _ string) (*model.EventOutbox, error) {
		derived = true

		return newEventOutboxFixture("should-not-happen-"), nil
	})

	supplied := newEventOutboxFixture("supplied-")
	txns := []*model.Transaction{{TransactionID: "txn_1"}}

	rows, err := resolveBatchEventOutboxes(context.Background(), txns, nil, []*model.EventOutbox{supplied})

	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Same(t, supplied, rows[0], "the caller's own row must be the one inserted")
	assert.False(t, derived, "derivation must not run when the caller supplied rows")
}

// TestResolveBatchEventOutboxes_DerivesOneRowPerTransactionWhenNoneSupplied is the fix
// itself, at the level the fix lives.
//
// The coalescing path reaches the batch writer with no event rows, and its file cannot
// be changed to supply them.
func TestResolveBatchEventOutboxes_DerivesOneRowPerTransactionWhenNoneSupplied(t *testing.T) {
	seen := map[string]string{}
	withRegisteredCapture(t, captureRecordingEachCall(seen))

	txns := []*model.Transaction{
		{TransactionID: "txn_1", Source: "bln_a", Destination: "bln_b"},
		{TransactionID: "txn_2", Source: "bln_b", Destination: "bln_a"},
		{TransactionID: "txn_3", Source: "bln_c", Destination: "bln_a"},
	}
	balances := []*model.Balance{
		{BalanceID: "bln_a", LedgerID: "ldg_one", Balance: big.NewInt(0)},
		{BalanceID: "bln_b", LedgerID: "ldg_one", Balance: big.NewInt(0)},
		{BalanceID: "bln_c", LedgerID: "ldg_two", Balance: big.NewInt(0)},
	}

	rows, err := resolveBatchEventOutboxes(context.Background(), txns, balances, nil)

	require.NoError(t, err)
	require.Len(t, rows, 3, "one event per committed transaction, with no exceptions")

	assert.Equal(t, map[string]string{
		"txn_1": "ldg_one",
		"txn_2": "ldg_one",
		// The SOURCE ledger wins, which is the rule transactionLedgerID applies on the
		// execution path. If the two disagreed, one transaction's events would hash to
		// different partitions depending on which path recorded them.
		"txn_3": "ldg_two",
	}, seen, "each transaction's ledger must be resolved from the balance set, source first")
}

// TestResolveBatchEventOutboxes_CapturesNothingWhenPublishingIsUnconfigured protects
// every broker-less deployment and most of the existing test suite.
func TestResolveBatchEventOutboxes_CapturesNothingWhenPublishingIsUnconfigured(t *testing.T) {
	withRegisteredCapture(t, func(_ context.Context, _ *model.Transaction, _ string) (*model.EventOutbox, error) {
		return nil, nil
	})

	txns := []*model.Transaction{{TransactionID: "txn_1"}, {TransactionID: "txn_2"}}

	rows, err := resolveBatchEventOutboxes(context.Background(), txns, nil, nil)

	require.NoError(t, err)
	assert.Empty(t, rows, "an unconfigured publisher captures nothing and fails nothing")
}

// TestResolveBatchEventOutboxes_RefusesAMixedCaptureResult refuses to insert a partial
// batch.
//
// Publishing is configured process-wide, so either every row is nil or none is; a mixed
// result is a defect.
func TestResolveBatchEventOutboxes_RefusesAMixedCaptureResult(t *testing.T) {
	withRegisteredCapture(t, func(_ context.Context, txn *model.Transaction, _ string) (*model.EventOutbox, error) {
		if txn.TransactionID == "txn_2" {
			return nil, nil
		}

		return newEventOutboxFixture("mixed-"), nil
	})

	txns := []*model.Transaction{{TransactionID: "txn_1"}, {TransactionID: "txn_2"}}

	rows, err := resolveBatchEventOutboxes(context.Background(), txns, nil, nil)

	require.Error(t, err)
	assert.Nil(t, rows)
	assert.Contains(t, apiErrorDetail(t, err), "produced no event row",
		"the detail must name the real problem rather than a count mismatch")
}

// TestResolveBatchEventOutboxes_AbandonsTheBatchWhenCaptureFails asserts the failure
// direction.
func TestResolveBatchEventOutboxes_AbandonsTheBatchWhenCaptureFails(t *testing.T) {
	sentinel := errors.New("payload will not serialise")
	withRegisteredCapture(t, func(_ context.Context, _ *model.Transaction, _ string) (*model.EventOutbox, error) {
		return nil, sentinel
	})

	rows, err := resolveBatchEventOutboxes(context.Background(),
		[]*model.Transaction{{TransactionID: "txn_1"}}, nil, nil)

	require.Error(t, err)
	assert.Nil(t, rows)

	detail := apiErrorDetail(t, err)
	assert.Contains(t, detail, "txn_1", "the failing transaction must be identifiable")
	assert.Contains(t, detail, sentinel.Error(), "the underlying cause must be preserved")
}

// TestResolveBatchEventOutboxes_IsInertWithNoCaptureRegistered is what keeps every existing
// caller, test and deployment behaving exactly as before.
func TestResolveBatchEventOutboxes_IsInertWithNoCaptureRegistered(t *testing.T) {
	withRegisteredCapture(t, nil)

	rows, err := resolveBatchEventOutboxes(context.Background(),
		[]*model.Transaction{{TransactionID: "txn_1"}}, nil, nil)

	require.NoError(t, err)
	assert.Empty(t, rows)
}

// TestTransactionLedgerIDFromSet_ResolvesSourceThenDestination pins the resolution rule that
// the execution path and this path must share.
func TestTransactionLedgerIDFromSet_ResolvesSourceThenDestination(t *testing.T) {
	ledgers := ledgerIDsByBalanceID([]*model.Balance{
		{BalanceID: "bln_source", LedgerID: "ldg_source"},
		{BalanceID: "bln_dest", LedgerID: "ldg_dest"},
		// A balance with no ledger is omitted from the index rather than mapped to "", so a
		// miss and a blank ledger are one answer.
		{BalanceID: "bln_unledgered", LedgerID: ""},
		nil,
	})

	cases := map[string]struct {
		txn      *model.Transaction
		expected string
	}{
		"the source ledger wins": {
			txn:      &model.Transaction{Source: "bln_source", Destination: "bln_dest"},
			expected: "ldg_source",
		},
		"the destination is the fallback": {
			txn:      &model.Transaction{Source: "bln_missing", Destination: "bln_dest"},
			expected: "ldg_dest",
		},
		"a source with no ledger falls through to the destination": {
			txn:      &model.Transaction{Source: "bln_unledgered", Destination: "bln_dest"},
			expected: "ldg_dest",
		},
		"neither balance known yields no ledger": {
			txn:      &model.Transaction{Source: "bln_x", Destination: "bln_y"},
			expected: "",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, transactionLedgerIDFromSet(tc.txn, ledgers))
		})
	}
}

// TestRecordTransactionsWithBalanceSetAndOutboxes_InsertsDerivedEventsInsideTheTransaction
// is the end-to-end proof that the derived rows land INSIDE the mutation's transaction.
func TestRecordTransactionsWithBalanceSetAndOutboxes_InsertsDerivedEventsInsideTheTransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	derived := newEventOutboxFixture("atomic-")
	withRegisteredCapture(t, func(_ context.Context, _ *model.Transaction, ledgerID string) (*model.EventOutbox, error) {
		derived.LedgerID = ledgerID

		return derived, nil
	})

	mock.ExpectBegin()
	expectTransactionCopy(mock)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_id"}).AddRow(int64(91), derived.EventID))
	mock.ExpectCommit()

	result, err := ds.RecordTransactionsWithBalanceSetAndOutboxes(context.Background(),
		[]*model.Transaction{newBatchTransactionFixture("txn_1")}, nil, nil)

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, int64(91), derived.ID, "the derived row must come back with its generated id")
	assert.NoError(t, mock.ExpectationsWereMet(),
		"a coalesced batch must insert its events inside the transaction that commits the mutations")
}

// TestRecordTransactionsWithBalanceSetAndOutboxes_RollsBackWhenCaptureFails asserts the
// mutation does not survive a capture failure.
func TestRecordTransactionsWithBalanceSetAndOutboxes_RollsBackWhenCaptureFails(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	withRegisteredCapture(t, func(_ context.Context, _ *model.Transaction, _ string) (*model.EventOutbox, error) {
		return nil, errors.New("capture failed")
	})

	mock.ExpectBegin()
	expectTransactionCopy(mock)
	mock.ExpectRollback()

	result, err := ds.RecordTransactionsWithBalanceSetAndOutboxes(context.Background(),
		[]*model.Transaction{newBatchTransactionFixture("txn_1")}, nil, nil)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.NoError(t, mock.ExpectationsWereMet(),
		"a batch whose event could not be captured must not commit its mutations")
}

// TestRegisterTransactionEventCapture_LastRegistrationWinsAndNilClears documents the
// registration semantics a test relies on to restore the pre-test state.
func TestRegisterTransactionEventCapture_LastRegistrationWinsAndNilClears(t *testing.T) {
	withRegisteredCapture(t, nil)
	require.Nil(t, registeredTransactionEventCapture(), "nil must clear the registration")

	first := func(_ context.Context, _ *model.Transaction, _ string) (*model.EventOutbox, error) {
		return newEventOutboxFixture("first-"), nil
	}
	RegisterTransactionEventCapture(first)
	require.NotNil(t, registeredTransactionEventCapture())

	marker := newEventOutboxFixture("second-")
	RegisterTransactionEventCapture(func(_ context.Context, _ *model.Transaction, _ string) (*model.EventOutbox, error) {
		return marker, nil
	})

	row, err := registeredTransactionEventCapture()(context.Background(), &model.Transaction{}, "")
	require.NoError(t, err)
	assert.Same(t, marker, row, "the most recent registration must be the one in force")
}

// TestRegisterBalanceMonitorAlertCapture_LastRegistrationWinsAndNilClears documents the
// same semantics for the monitor alert capture, which a test relies on to restore the
// pre-test state.
//
// NIL IS NOT MERELY "UNSET" HERE.
func TestRegisterBalanceMonitorAlertCapture_LastRegistrationWinsAndNilClears(t *testing.T) {
	previous := registeredBalanceMonitorAlertCapture()
	t.Cleanup(func() { RegisterBalanceMonitorAlertCapture(previous) })

	RegisterBalanceMonitorAlertCapture(nil)
	require.Nil(t, registeredBalanceMonitorAlertCapture(), "nil must clear the registration")

	RegisterBalanceMonitorAlertCapture(func(context.Context, *model.Balance, model.BalanceMonitor) (*model.EventOutbox, error) {
		return newEventOutboxFixture("first-"), nil
	})
	require.NotNil(t, registeredBalanceMonitorAlertCapture())

	marker := newEventOutboxFixture("second-")
	RegisterBalanceMonitorAlertCapture(func(context.Context, *model.Balance, model.BalanceMonitor) (*model.EventOutbox, error) {
		return marker, nil
	})

	row, err := registeredBalanceMonitorAlertCapture()(context.Background(),
		&model.Balance{BalanceID: "bln_x"}, model.BalanceMonitor{MonitorID: "mon_x"})
	require.NoError(t, err)
	assert.Same(t, marker, row, "the most recent registration must be the one in force")
}

// TestExistingEventIDs_ReportsOnlyTheIDsThatExist covers the batched durability lookup the
// post-commit path uses to decide whether a capture is still owed.
func TestExistingEventIDs_ReportsOnlyTheIDsThatExist(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT event_id")).
		WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow("evt_present"))

	present, err := ds.ExistingEventIDs(context.Background(), []string{"evt_present", "evt_absent"})

	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{"evt_present": {}}, present)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestExistingEventIDs_IssuesNoQueryForAnEmptyRequest keeps the post-commit path from paying a
// round trip for a batch that derived no ids.
func TestExistingEventIDs_IssuesNoQueryForAnEmptyRequest(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	for name, ids := range map[string][]string{
		"nil":                                    nil,
		"empty":                                  {},
		"only blanks":                            {"", "   "},
		"blank tolerated alongside nothing else": {"\t"},
	} {
		t.Run(name, func(t *testing.T) {
			present, err := ds.ExistingEventIDs(context.Background(), ids)

			require.NoError(t, err)
			assert.Empty(t, present)
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet(), "no statement may be issued")
}

// TestExistingEventIDs_ReturnsTheErrorRatherThanAnEmptySet pins the safe failure
// direction.
//
// An empty set on error would read as "nothing is captured", which is harmless.
func TestExistingEventIDs_ReturnsTheErrorRatherThanAnEmptySet(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT event_id")).
		WillReturnError(errors.New("connection reset"))

	present, err := ds.ExistingEventIDs(context.Background(), []string{"evt_1"})

	require.Error(t, err)
	assert.Nil(t, present)
}
