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
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateLedger_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	ledger := model.Ledger{
		Name: "Test Ledger",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(ledger.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.ledgers").
		WithArgs(metaDataJSON, ledger.Name, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	createdLedger, err := ds.CreateLedger(ledger)
	assert.NoError(t, err)
	assert.NotEmpty(t, createdLedger.LedgerID)
	assert.WithinDuration(t, time.Now(), createdLedger.CreatedAt, time.Second)
}

func TestCreateLedger_UniqueViolation(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	ledger := model.Ledger{
		Name: "Test Ledger",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(ledger.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.ledgers").
		WithArgs(metaDataJSON, ledger.Name, sqlmock.AnyArg()).
		WillReturnError(&pq.Error{Code: "23505", Message: "unique_violation"})

	_, err = ds.CreateLedger(ledger)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code)
}

func TestGetAllLedgers_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{
		"key": "value",
	}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	rows := sqlmock.NewRows([]string{"ledger_id", "name", "created_at", "meta_data"}).
		AddRow("ldg1", "Ledger 1", time.Now(), metaDataJSON).
		AddRow("ldg2", "Ledger 2", time.Now(), metaDataJSON)

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers ORDER BY created_at DESC LIMIT \\$1 OFFSET \\$2").
		WithArgs(2, 0).
		WillReturnRows(rows)
	ledgers, err := ds.GetAllLedgers(2, 0)
	assert.NoError(t, err)
	assert.Len(t, ledgers, 2)
	assert.Equal(t, "Ledger 1", ledgers[0].Name)
}

func TestGetLedgerByID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{
		"key": "value",
	}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	row := sqlmock.NewRows([]string{"ledger_id", "name", "created_at", "meta_data"}).
		AddRow("ldg1", "Ledger 1", time.Now(), metaDataJSON)

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id = ?").
		WithArgs("ldg1").
		WillReturnRows(row)

	ledger, err := ds.GetLedgerByID("ldg1")
	assert.NoError(t, err)
	assert.Equal(t, "Ledger 1", ledger.Name)
}

func TestGetLedgerByID_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id = ?").
		WithArgs("ldg1").
		WillReturnError(sql.ErrNoRows)

	_, err = ds.GetLedgerByID("ldg1")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)
}

func TestCreateLedger_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	ledger := model.Ledger{
		Name: "Test Ledger",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(ledger.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.ledgers").
		WithArgs(metaDataJSON, ledger.Name, sqlmock.AnyArg()).
		WillReturnError(&pq.Error{Code: "42P01", Message: "relation does not exist"})

	_, err = ds.CreateLedger(ledger)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestGetAllLedgers_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers").
		WithArgs(20, 0).
		WillReturnError(sql.ErrConnDone)

	ledgers, err := ds.GetAllLedgers(20, 0)
	assert.Error(t, err)
	assert.Nil(t, ledgers)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestGetAllLedgers_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers ORDER BY created_at DESC LIMIT \\$1 OFFSET \\$2").
		WithArgs(20, 0).
		WillReturnRows(sqlmock.NewRows([]string{"ledger_id", "name", "created_at", "meta_data"}))

	ledgers, err := ds.GetAllLedgers(20, 0)
	assert.NoError(t, err)
	assert.Len(t, ledgers, 0)
}

func TestGetAllLedgers_InvalidLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers ORDER BY created_at DESC LIMIT \\$1 OFFSET \\$2").
		WithArgs(20, 0).
		WillReturnRows(sqlmock.NewRows([]string{"ledger_id", "name", "created_at", "meta_data"}).
			AddRow("ldg1", "Ledger 1", time.Now(), metaDataJSON))

	ledgers, err := ds.GetAllLedgers(-5, 0)
	assert.NoError(t, err)
	assert.Len(t, ledgers, 1)
}

func TestGetLedgerByID_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id = ?").
		WithArgs("ldg1").
		WillReturnError(sql.ErrConnDone)

	_, err = ds.GetLedgerByID("ldg1")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestUpdateLedger_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id = ?").
		WithArgs("ldg1").
		WillReturnRows(sqlmock.NewRows([]string{"ledger_id", "name", "created_at", "meta_data"}).
			AddRow("ldg1", "Old Name", time.Now(), metaDataJSON))

	mock.ExpectExec("UPDATE blnk.ledgers SET name").
		WithArgs("New Name", "ldg1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	ledger, err := ds.UpdateLedger("ldg1", "New Name")
	assert.NoError(t, err)
	assert.Equal(t, "New Name", ledger.Name)
	assert.Equal(t, "ldg1", ledger.LedgerID)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateLedger_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id = ?").
		WithArgs("ldg_notfound").
		WillReturnError(sql.ErrNoRows)

	_, err = ds.UpdateLedger("ldg_notfound", "New Name")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)
}

func TestUpdateLedger_UniqueViolation(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{"key": "value"}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	mock.ExpectQuery("SELECT ledger_id, name, created_at, meta_data FROM blnk.ledgers WHERE ledger_id = ?").
		WithArgs("ldg1").
		WillReturnRows(sqlmock.NewRows([]string{"ledger_id", "name", "created_at", "meta_data"}).
			AddRow("ldg1", "Old Name", time.Now(), metaDataJSON))

	mock.ExpectExec("UPDATE blnk.ledgers SET name").
		WithArgs("Duplicate Name", "ldg1").
		WillReturnError(&pq.Error{Code: "23505", Message: "unique_violation"})

	_, err = ds.UpdateLedger("ldg1", "Duplicate Name")
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code)
}

// TestCreateLedger_CommitsTheEventWithTheLedger is requirement R-2 for ledger creation, and
// the assertion is the SHAPE OF THE TRANSACTION rather than the value of anything.
//
// ledger.created used to be inserted from a goroutine after CreateLedger had already
// committed, so a crash or a failed insert in between left a ledger that no subscriber would
// ever hear about and nothing to replay from. Ordered sqlmock expectations are what pin the
// repair: BEGIN, the ledger INSERT, the event INSERT, COMMIT, in that order and inside one
// transaction. Moving the event insert after the commit — the very defect — leaves every
// value in this test unchanged and breaks only this ordering, which is exactly why the
// ordering is what is asserted.
func TestCreateLedger_CommitsTheEventWithTheLedger(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ledger := model.Ledger{Name: "Atomic Ledger", MetaData: map[string]interface{}{"key": "value"}}

	metaDataJSON, err := json.Marshal(ledger.MetaData)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO blnk.ledgers").
		WithArgs(metaDataJSON, ledger.Name, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("INSERT INTO blnk.event_outbox").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(41)))
	mock.ExpectCommit()

	var seen model.Ledger
	created, err := ds.CreateLedger(ledger, func(entity model.Ledger) (*model.EventOutbox, error) {
		seen = entity

		return &model.EventOutbox{
			EventID:      "9c1f4d2e-7b3a-4c58-8e6d-1f2a3b4c5d6e",
			EventType:    "ledger.created",
			AggregateID:  entity.LedgerID,
			PartitionKey: entity.LedgerID,
			LedgerID:     entity.LedgerID,
			// The topic must be one Blnk owns — the insert validates that — and
			// ledger.created routes to the subscriber-facing ledgers category, so that a
			// subscriber credential can be granted it without also being handed
			// blnk.system, which carries Blnk's own internal diagnostics.
			// Spelled as a literal because TopicForEvent lives in the root blnk package,
			// which this one cannot import — root imports database, not the reverse.
			Topic:   "blnk." + model.EventCategorySystem,
			Payload: json.RawMessage(`{"event":"ledger.created","data":{}}`),
		}, nil
	})

	require.NoError(t, err)
	assert.Contains(t, created.LedgerID, "ldg_")
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the ledger and its event must be inserted inside ONE transaction, in that order")

	// The preparer sees the CREATED ledger, not the requested one. That is not a nicety: the
	// event's aggregate id, partition key and payload are all derived from the generated id,
	// so a preparer handed the caller's value would build an event describing a ledger that
	// does not exist.
	assert.Equal(t, created.LedgerID, seen.LedgerID,
		"the preparer must be handed the created ledger, carrying the generated id")
	assert.False(t, seen.CreatedAt.IsZero(),
		"and carrying the creation timestamp the insert stamped")
}

// TestCreateLedger_APreparerFailureCreatesNoLedger pins the fail-closed half of the same
// guarantee.
//
// If the event cannot be built there are two options, and only one of them is safe: commit the
// ledger and lose the event, or refuse both. Committing would produce a ledger no subscriber
// knows exists, invisibly, while reporting success — so the transaction is rolled back and the
// error is returned. sqlmock's ExpectRollback is what proves the ledger INSERT was undone
// rather than merely unreported.
func TestCreateLedger_APreparerFailureCreatesNoLedger(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO blnk.ledgers").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectRollback()

	prepareErr := errors.New("the payload could not be serialised")
	created, err := ds.CreateLedger(model.Ledger{Name: "Doomed"},
		func(model.Ledger) (*model.EventOutbox, error) { return nil, prepareErr })

	require.ErrorIs(t, err, prepareErr, "the preparer's error must reach the caller unchanged")
	assert.Empty(t, created.LedgerID, "and no ledger may be reported as created")
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the ledger INSERT must be rolled back, not left committed with its event missing")
}

// TestCreateLedger_ANilRowCommitsTheLedgerAlone covers the unconfigured deployment, which is
// the case that must not become a failure.
//
// PrepareEventOutbox returns (nil, nil) when no transport is configured — the
// no-op-when-unconfigured contract inherited from SendWebhook — so a preparer that returns no
// row must leave the ledger creation succeeding on its own. Without this, configuring nothing
// would break ledger creation outright.
func TestCreateLedger_ANilRowCommitsTheLedgerAlone(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO blnk.ledgers").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	created, err := ds.CreateLedger(model.Ledger{Name: "No Transport"},
		func(model.Ledger) (*model.EventOutbox, error) { return nil, nil })

	require.NoError(t, err)
	assert.Contains(t, created.LedgerID, "ldg_")
	assert.NoError(t, mock.ExpectationsWereMet(),
		"no event insert may be issued for a nil row, and the ledger must still commit")
}

// TestCreateLedger_WithoutAPreparerIssuesOneStatement pins the cost of the unconfigured path
// and the source-compatibility the variadic tail exists for.
//
// A caller that supplies NO preparer — every pre-existing caller, and the service itself when
// nothing is configured — must take the original single-statement path. Not "a transaction that
// inserts nothing extra": no transaction at all. A BEGIN here would mean every ledger creation
// in a deployment with no event transport paid for a transaction to insert nothing, and it
// would break every existing expectation written against this method.
func TestCreateLedger_WithoutAPreparerIssuesOneStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	mock.ExpectExec("INSERT INTO blnk.ledgers").WillReturnResult(sqlmock.NewResult(1, 1))

	created, err := ds.CreateLedger(model.Ledger{Name: "Plain"})

	require.NoError(t, err)
	assert.Contains(t, created.LedgerID, "ldg_")
	assert.NoError(t, mock.ExpectationsWereMet(),
		"a create with no preparer must issue exactly one statement and open no transaction")
}

// TestCreateLedger_ANilPreparerInTheTailIsSkipped covers the argument a caller assembles
// conditionally.
//
// The service returns a nil preparer when publishing is unconfigured and passes it anyway,
// because branching at nine call sites is what the variadic tail exists to avoid. A nil entry
// must therefore select the single-statement path rather than being invoked.
func TestCreateLedger_ANilPreparerInTheTailIsSkipped(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	mock.ExpectExec("INSERT INTO blnk.ledgers").WillReturnResult(sqlmock.NewResult(1, 1))

	created, err := ds.CreateLedger(model.Ledger{Name: "Conditional"}, nil)

	require.NoError(t, err)
	assert.Contains(t, created.LedgerID, "ldg_")
	assert.NoError(t, mock.ExpectationsWereMet())
}
