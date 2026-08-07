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
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// entity_event_outbox_test.go covers the atomic writers that create a ledger, an identity,
// a balance or a balance-less transaction TOGETHER WITH the event describing it.
//
// # What these tests are for, and why the ordinary happy-path tests cannot substitute
//
// The whole value of these writers is the ORDER AND ENROLMENT of two statements, and an
// implementation that inserts the entity, commits, and then inserts the event passes every
// functional test there is: the row exists, the event exists, the returned value is
// correct, nothing errors. The guarantee is nonetheless gone, and the symptom only ever
// appears as a process dying between the two — a committed ledger nobody will hear about,
// or a balance whose creation event was lost.
//
// So every test here asserts the SCRIPT rather than the outcome. sqlmock enforces
// expectation order, so a script of Begin → entity insert → event insert → Commit cannot be
// satisfied by statements issued on the pooled connection, or issued after the transaction
// closed, or issued in the wrong order. And the rollback tests assert the converse
// direction: an event that cannot be written must take the mutation down with it, because a
// mutation whose event was lost is exactly what requirement R-2 forbids.

// TestCreateLedgerWithEventOutbox_InsertsLedgerAndEventInOneTransaction proves the ledger
// row and its event share one transaction.
func TestCreateLedgerWithEventOutbox_InsertsLedgerAndEventInOneTransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.ledgers")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		// ONE column, because the single-row insert the entity writers use is
		// `RETURNING id`. The batch insert returns id and event_id, since it has to
		// match many returned rows back to many entries; one entity has exactly one
		// event and needs no such matching.
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(11)))
	mock.ExpectCommit()

	event := newEventOutboxFixture("ldg-")
	event.EventType = "ledger.created"
	event.Topic = "blnk.system"

	created, err := ds.CreateLedger(
		model.Ledger{Name: "Atomic ledger"},
		ledgerEventBuilder(t, event),
	)

	require.NoError(t, err)
	assert.NotEmpty(t, created.LedgerID, "the created ledger must carry its generated id")
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the ledger insert and the event insert must both be issued between BEGIN and COMMIT, "+
			"in that order; an event inserted after the commit leaves a window in which a "+
			"committed ledger has no event")
}

// TestCreateLedgerWithEventOutbox_RollsBackTheLedgerWhenTheEventCannotBeCaptured is the
// assertion that makes R-2 a guarantee rather than a best effort.
//
// A mutation whose event cannot be recorded MUST NOT COMMIT. The alternative — commit the
// ledger, log the event failure — is the silent-loss behaviour the outbox exists to
// eliminate: the caller is told it succeeded and no mechanism anywhere can afterwards
// discover that an event was due.
func TestCreateLedgerWithEventOutbox_RollsBackTheLedgerWhenTheEventCannotBeCaptured(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.ledgers")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnError(errors.New("event outbox unavailable"))
	mock.ExpectRollback()

	_, err := ds.CreateLedger(
		model.Ledger{Name: "Doomed ledger"},
		ledgerEventBuilder(t, newEventOutboxFixture("ldg-fail-")),
	)

	require.Error(t, err, "a ledger whose event cannot be captured must not be reported as created")
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the transaction must be rolled back, so no ledger row survives without its event")
}

// TestCreateLedgerWithEventOutbox_NoEventRowStillCommits keeps the
// no-op-when-unconfigured contract.
//
// PrepareEventOutbox returns nil when event publishing is not configured, which is a
// legitimate steady state — Blnk has always run with no notification sink. A writer that
// refused an absent event row would break every such deployment.
func TestCreateLedgerWithEventOutbox_NoEventRowStillCommits(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.ledgers")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	// Deliberately no event insert expectation: a nil row must produce no statement at
	// all, not an empty INSERT.

	created, err := ds.CreateLedger(
		model.Ledger{Name: "Unconfigured publishing"},
		func(model.Ledger) (*model.EventOutbox, error) {
			// Exactly what PrepareEventOutbox returns when event publishing is not
			// configured: no row, no error.
			return nil, nil
		},
	)

	require.NoError(t, err)
	assert.NotEmpty(t, created.LedgerID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCreateIdentityWithEventOutbox_InsertsIdentityAndEventInOneTransaction is the identity
// half of the same guarantee.
func TestCreateIdentityWithEventOutbox_InsertsIdentityAndEventInOneTransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.identity")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(12)))
	mock.ExpectCommit()

	event := newEventOutboxFixture("idt-")
	event.EventType = "identity.created"
	event.Topic = "blnk.identities"

	created, err := ds.CreateIdentity(
		model.Identity{FirstName: "Ada", LastName: "Lovelace"},
		func(created model.Identity) (*model.EventOutbox, error) {
			require.NotEmpty(t, created.IdentityID,
				"the builder must see the SETTLED row, so the payload can carry the id the legacy webhook body carried")
			return event, nil
		},
	)

	require.NoError(t, err)
	assert.NotEmpty(t, created.IdentityID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCreateIdentityWithEventOutbox_RollsBackWhenTheEventCannotBeCaptured mirrors the
// ledger rollback assertion.
func TestCreateIdentityWithEventOutbox_RollsBackWhenTheEventCannotBeCaptured(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.identity")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnError(errors.New("event outbox unavailable"))
	mock.ExpectRollback()

	failing := newEventOutboxFixture("idt-fail-")
	_, err := ds.CreateIdentity(
		model.Identity{FirstName: "Grace"},
		func(model.Identity) (*model.EventOutbox, error) { return failing, nil },
	)

	require.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCreateBalanceWithEventOutbox_InsertsBalanceAndEventInOneTransaction is the balance
// half of the same guarantee.
func TestCreateBalanceWithEventOutbox_InsertsBalanceAndEventInOneTransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.balances")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(13)))
	mock.ExpectCommit()

	event := newEventOutboxFixture("bln-")
	event.EventType = "balance.created"
	event.Topic = "blnk.balances"

	created, err := ds.CreateBalance(
		model.Balance{Currency: "USD", LedgerID: "ldg_1"},
		func(created model.Balance) (*model.EventOutbox, error) {
			require.NotEmpty(t, created.BalanceID,
				"the builder must see the SETTLED row, so the payload can carry the generated balance id")
			return event, nil
		},
	)

	require.NoError(t, err)
	assert.NotEmpty(t, created.BalanceID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCreateBalanceWithEventOutbox_RollsBackWhenTheEventCannotBeCaptured mirrors the
// ledger rollback assertion.
func TestCreateBalanceWithEventOutbox_RollsBackWhenTheEventCannotBeCaptured(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.balances")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnError(errors.New("event outbox unavailable"))
	mock.ExpectRollback()

	failing := newEventOutboxFixture("bln-fail-")
	_, err := ds.CreateBalance(
		model.Balance{Currency: "USD", LedgerID: "ldg_1"},
		func(model.Balance) (*model.EventOutbox, error) { return failing, nil },
	)

	require.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCreateBalanceWithEventOutbox_ExistingIndicatorCapturesNoEvent covers the one
// outcome where nothing happened.
//
// createBalance answers a unique_indicator_currency violation with a zero balance and a nil
// error, because an existing indicator/currency pair is not a caller error. No balance was
// created, so no balance.created event may be captured: publishing one would announce the
// creation of a balance whose id the payload does not even carry.
func TestCreateBalanceWithEventOutbox_ExistingIndicatorCapturesNoEvent(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.balances")).
		WillReturnError(&pq.Error{Code: "23505", Message: `duplicate key value violates unique constraint "unique_indicator_currency"`})
	// ROLLBACK, not COMMIT, and not as a preference: PostgreSQL aborts the whole
	// transaction on the failed statement, so a COMMIT sent here would be executed as a
	// rollback by the server anyway. Nothing was created, so there is nothing to commit
	// and nothing to announce.
	mock.ExpectRollback()
	// Deliberately no event insert: nothing was created.

	builderCalls := 0
	created, err := ds.CreateBalance(
		model.Balance{Currency: "USD", LedgerID: "ldg_1", Indicator: "taken"},
		func(model.Balance) (*model.EventOutbox, error) {
			builderCalls++
			return newEventOutboxFixture("bln-dup-"), nil
		},
	)

	require.NoError(t, err, "an existing indicator/currency pair is not an error, matching CreateBalance")
	assert.Empty(t, created.BalanceID, "no balance was created")
	assert.Zero(t, builderCalls,
		"the event builder must not even run for a balance that was not created")
	assert.NoError(t, mock.ExpectationsWereMet(),
		"no event may be captured for a balance that was not created")
}

// TestRecordTransactionWithEventOutbox_RecordsTransactionAndEventInOneTransaction is the
// rejection path's guarantee.
//
// A rejected transaction moves no balances, so the balance-updating writers cannot serve
// it, and before this writer existed the rejection was persisted by one statement and its
// transaction.rejected event by another — the split R-2 forbids.
func TestRecordTransactionWithEventOutbox_RecordsTransactionAndEventInOneTransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.transactions")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(14)))
	mock.ExpectCommit()

	event := newEventOutboxFixture("rej-")
	event.EventType = "transaction.rejected"

	recorded, err := ds.RecordTransaction(
		context.Background(),
		newRejectedTransactionFixture(),
		event,
	)

	require.NoError(t, err)
	require.NotNil(t, recorded)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRecordTransactionWithEventOutbox_RollsBackWhenTheEventCannotBeCaptured asserts the
// rejection does not commit without its event.
func TestRecordTransactionWithEventOutbox_RollsBackWhenTheEventCannotBeCaptured(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.transactions")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnError(errors.New("event outbox unavailable"))
	mock.ExpectRollback()

	_, err := ds.RecordTransaction(
		context.Background(),
		newRejectedTransactionFixture(),
		newEventOutboxFixture("rej-fail-"),
	)

	require.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRecordTransactionWithEventOutbox_RefusesTwoEventsForOneTransaction asserts the
// cardinality rule is enforced before any statement runs.
//
// Two rows for one transaction is a duplicate publication, and once the mutation has
// committed nothing downstream can detect it: both events exist, both look legitimate, and
// the subscriber sees the rejection twice. resolveEventOutboxes refuses it up front, which
// is why no BEGIN is scripted here.
func TestRecordTransactionWithEventOutbox_RefusesTwoEventsForOneTransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}
	// No expectations at all: the refusal must happen before the transaction opens.

	_, err := ds.RecordTransaction(
		context.Background(),
		newRejectedTransactionFixture(),
		newEventOutboxFixture("rej-a-"),
		newEventOutboxFixture("rej-b-"),
	)

	require.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the cardinality refusal must precede BEGIN, so no partial work is attempted")
}

// ledgerEventBuilder returns a builder that hands back one prepared row and asserts, on the
// way past, that the writer invoked it with the SETTLED ledger.
//
// That assertion is the reason the builder exists at all rather than a plain row parameter:
// the ledger id is generated by the insert, and the legacy webhook body carried the CREATED
// ledger, id included. A builder invoked with the caller's un-inserted argument would produce
// a payload that differs from the body it is required to reproduce field-for-field.
func ledgerEventBuilder(t *testing.T, row *model.EventOutbox) EventPreparer[model.Ledger] {
	t.Helper()

	return func(created model.Ledger) (*model.EventOutbox, error) {
		require.NotEmpty(t, created.LedgerID,
			"the builder must see the settled ledger, so the payload can carry the generated id")
		require.False(t, created.CreatedAt.IsZero(),
			"the builder must see the settled creation timestamp")

		return row, nil
	}
}

// TestCreateLedgerWithEventOutbox_RollsBackWhenTheEventCannotBeBuilt covers the failure the
// builder itself can produce.
//
// A payload that will not marshal is a producer defect, and PrepareEventOutbox reports it as
// an error rather than logging it and returning nil. The mutation must not commit: a ledger
// created without the event it owed is a loss nothing downstream can discover, because the
// outbox counts rows that exist and cannot count a row that was never written.
func TestCreateLedgerWithEventOutbox_RollsBackWhenTheEventCannotBeBuilt(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO blnk.ledgers")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectRollback()
	// Deliberately no event insert: the builder fails before one could be issued.

	_, err := ds.CreateLedger(
		model.Ledger{Name: "Unmarshalable payload"},
		func(model.Ledger) (*model.EventOutbox, error) {
			return nil, errors.New("json: unsupported type: chan int")
		},
	)

	require.Error(t, err, "a builder failure must fail the creation")
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the transaction must be rolled back, so no ledger survives without its event")
}

// newRejectedTransactionFixture builds the shape RejectTransaction persists: a transaction
// carrying the rejected status and a rejection reason in its metadata, with the precise
// amount recordTransactionInTx dereferences.
func newRejectedTransactionFixture() *model.Transaction {
	return &model.Transaction{
		TransactionID: "txn_" + "rejected",
		Reference:     "ref-rejected",
		Source:        "bln_source",
		Destination:   "bln_destination",
		Currency:      "USD",
		AmountString:  "100",
		PreciseAmount: big.NewInt(10000),
		Precision:     100,
		Status:        "REJECTED",
		MetaData:      map[string]interface{}{"blnk_rejection_reason": "insufficient funds"},
	}
}
