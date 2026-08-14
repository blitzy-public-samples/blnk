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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// handoff_event_capture_test.go covers the capture RecordTransaction performs for a
// transaction the queue has ACCEPTED but not executed.
//
// It is the only writer that sees such a transaction. Execution persists a separate copy
// through an atomic writer, which captures that copy's own transaction.applied, so
// without the capture under test transaction.queued and transaction.scheduled were named
// in the catalogue and published by nothing.

// newHandoffTransactionFixture builds the shape processSingleTransaction persists: an
// accepted transaction with real balance ids and no balances beside it.
//
// Parameters:
//   - status string: the accepted status, QUEUED or SCHEDULED.
//
// Returns:
//   - *model.Transaction: the fixture, with the precise amount recordTransactionInTx
//     dereferences.
func newHandoffTransactionFixture(status string) *model.Transaction {
	return &model.Transaction{
		TransactionID: "txn_accepted",
		Reference:     "ref-accepted",
		Source:        "bln_source",
		Destination:   "bln_destination",
		Currency:      "USD",
		AmountString:  "100",
		PreciseAmount: big.NewInt(10000),
		Precision:     100,
		Status:        status,
		MetaData:      map[string]interface{}{},
	}
}

// withRegisteredTransactionEventCapture installs a capture for one test and restores
// whatever was there afterwards, so tests that rely on there being NO capture are not
// affected by the order they run in.
//
// Parameters:
//   - t *testing.T: the test, for the cleanup registration.
//   - capture TransactionEventCapture: the capture to install.
func withRegisteredTransactionEventCapture(t *testing.T, capture TransactionEventCapture) {
	t.Helper()

	previous := registeredTransactionEventCapture()
	RegisterTransactionEventCapture(capture)
	t.Cleanup(func() { RegisterTransactionEventCapture(previous) })
}

// TestHandoffLifecycleEventType_NamesOnlyTheTwoAcceptedStatuses is the guard that keeps
// the capture off every status another writer owns.
//
// A second row under the same derived event id would violate event_outbox's unique index
// on event_id, and because the insert shares the ledger write's transaction that
// violation rolls back the MUTATION. So this is not a tidiness check: the wrong answer
// here refuses financially valid transactions.
func TestHandoffLifecycleEventType_NamesOnlyTheTwoAcceptedStatuses(t *testing.T) {
	for status, want := range map[string]string{
		"QUEUED":    model.EventTypeTransactionQueued,
		"SCHEDULED": model.EventTypeTransactionScheduled,
		"queued":    model.EventTypeTransactionQueued,
		"APPLIED":   "",
		"INFLIGHT":  "",
		"VOID":      "",
		"REJECTED":  "",
		"COMMIT":    "",
		"":          "",
	} {
		got := handoffLifecycleEventType(&model.Transaction{Status: status})
		assert.Equalf(t, want, got,
			"status %q must resolve to %q: an accepted transaction is captured here and an "+
				"executed one is captured by the atomic writer, and capturing an executed one "+
				"twice rolls back its ledger write", status, want)
	}

	assert.Empty(t, handoffLifecycleEventType(nil),
		"a nil transaction has no lifecycle event")
}

// TestRecordTransaction_CapturesTheQueuedEventInTheSameTransaction is P4-F01's contract:
// the acceptance of a transaction is announced, atomically with the row recording it.
func TestRecordTransaction_CapturesTheQueuedEventInTheSameTransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	captured := 0
	withRegisteredTransactionEventCapture(t,
		func(_ context.Context, txn *model.Transaction, ledgerID string) (*model.EventOutbox, error) {
			captured++
			assert.Equal(t, "QUEUED", txn.Status)
			assert.Equal(t, "ldg_from_source_balance", ledgerID,
				"the capture must be handed the ledger read off the transaction's own balances, "+
					"or the event is keyed on a weaker dimension than transaction.queued declares")

			row := newEventOutboxFixture("queued-")
			row.EventType = model.EventTypeTransactionQueued

			return row, nil
		})

	// The ledger lookup precedes the write: the event row is built before the transaction
	// opens, exactly as the atomic writers build theirs.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT balance.ledger_id")).
		WithArgs("bln_source", "bln_destination").
		WillReturnRows(sqlmock.NewRows([]string{"ledger_id"}).AddRow("ldg_from_source_balance"))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.transactions")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(21)))
	mock.ExpectCommit()

	recorded, err := ds.RecordTransaction(context.Background(), newHandoffTransactionFixture("QUEUED"))

	require.NoError(t, err)
	require.NotNil(t, recorded)
	assert.Equal(t, 1, captured, "exactly one event describes one accepted transaction")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRecordTransaction_CapturesTheScheduledEventInTheSameTransaction is P4-F02's
// contract.
func TestRecordTransaction_CapturesTheScheduledEventInTheSameTransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	withRegisteredTransactionEventCapture(t,
		func(_ context.Context, txn *model.Transaction, _ string) (*model.EventOutbox, error) {
			assert.Equal(t, "SCHEDULED", txn.Status)

			row := newEventOutboxFixture("scheduled-")
			row.EventType = model.EventTypeTransactionScheduled

			return row, nil
		})

	mock.ExpectQuery(regexp.QuoteMeta("SELECT balance.ledger_id")).
		WillReturnRows(sqlmock.NewRows([]string{"ledger_id"}).AddRow("ldg_scheduled"))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.transactions")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(22)))
	mock.ExpectCommit()

	_, err := ds.RecordTransaction(context.Background(), newHandoffTransactionFixture("SCHEDULED"))

	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRecordTransaction_RollsBackWhenTheAcceptedEventCannotBeCaptured asserts an accepted
// transaction is not recorded without its event.
//
// The alternative accepts a movement no subscriber is ever told about, with no outbox row
// for the daily reconciliation to count against the broker's offsets.
func TestRecordTransaction_RollsBackWhenTheAcceptedEventCannotBeCaptured(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	withRegisteredTransactionEventCapture(t,
		func(context.Context, *model.Transaction, string) (*model.EventOutbox, error) {
			return nil, errors.New("json: unsupported type: chan int")
		})

	mock.ExpectQuery(regexp.QuoteMeta("SELECT balance.ledger_id")).
		WillReturnRows(sqlmock.NewRows([]string{"ledger_id"}).AddRow("ldg_refused"))
	// No BEGIN: the capture fails before any statement runs.

	_, err := ds.RecordTransaction(context.Background(), newHandoffTransactionFixture("QUEUED"))

	require.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the refusal must precede BEGIN, so no transaction row survives without its event")
}

// TestRecordTransaction_CapturesNothingForAnExecutedTransaction is the other half of the
// guard: the atomic writer owns an executed transaction's event, and a second capture here
// would collide with it under the same derived id.
func TestRecordTransaction_CapturesNothingForAnExecutedTransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	withRegisteredTransactionEventCapture(t,
		func(context.Context, *model.Transaction, string) (*model.EventOutbox, error) {
			t.Fatal("no capture may run for a transaction another writer announces")

			return nil, nil
		})

	// The plain single-statement path: no ledger lookup, no transaction, no event.
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.transactions")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := ds.RecordTransaction(context.Background(), newHandoffTransactionFixture("APPLIED"))

	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRecordTransaction_PrefersACallerSuppliedEventOverTheAcceptedCapture keeps
// transaction_rejection.go's own row authoritative.
func TestRecordTransaction_PrefersACallerSuppliedEventOverTheAcceptedCapture(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	withRegisteredTransactionEventCapture(t,
		func(context.Context, *model.Transaction, string) (*model.EventOutbox, error) {
			t.Fatal("a caller that supplied its own event must not have a second one derived")

			return nil, nil
		})

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.transactions")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(23)))
	mock.ExpectCommit()

	supplied := newEventOutboxFixture("supplied-")
	supplied.EventType = model.EventTypeTransactionQueued

	_, err := ds.RecordTransaction(context.Background(),
		newHandoffTransactionFixture("QUEUED"), supplied)

	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRecordTransaction_CapturesNothingWhenPublishingIsUnconfigured is the graceful
// degradation every deployment without Kafka depends on: no capture is registered, so the
// accepted transaction is recorded by the single-statement path exactly as before.
func TestRecordTransaction_CapturesNothingWhenPublishingIsUnconfigured(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	withRegisteredTransactionEventCapture(t, nil)

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.transactions")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err := ds.RecordTransaction(context.Background(), newHandoffTransactionFixture("QUEUED"))

	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet(),
		"with no capture registered nothing may be looked up and nothing may be inserted "+
			"beyond the transaction itself")
}

// TestTransactionLedgerID_FallsBackToTheAggregateRatherThanFailing asserts a balance the
// lookup cannot find is not an error.
//
// An internal balance reference names no stored row, and refusing the transaction over it
// would make the capture less reliable than the thing it announces.
func TestTransactionLedgerID_FallsBackToTheAggregateRatherThanFailing(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT balance.ledger_id")).
		WillReturnRows(sqlmock.NewRows([]string{"ledger_id"}))

	assert.Empty(t, ds.transactionLedgerID(context.Background(),
		newHandoffTransactionFixture("QUEUED")),
		"a miss must yield no ledger rather than an error")
	assert.NoError(t, mock.ExpectationsWereMet())

	assert.Empty(t, ds.transactionLedgerID(context.Background(), nil),
		"a nil transaction names no balance, so nothing is queried")
	assert.Empty(t, ds.transactionLedgerID(context.Background(), &model.Transaction{}),
		"a transaction with no balances names nothing to look up")
}
