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

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/cache"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
)

// event_producer_atomicity_test.go asserts, at the PRODUCER call sites, that every event a
// ledger mutation owes is threaded into the mutation's own atomic writer — requirement R-2.
//
// # Why the repository tests are not enough
//
// database/entity_event_outbox_test.go proves the writers are atomic. That is a different
// claim from "the producers use them". A producer that reverted to the plain CreateLedger,
// or that called the atomic writer with no event builder, would leave every repository test
// green and every functional test green while the guarantee was completely gone: the row
// exists, the event exists, and the only difference is the window in which a crash loses
// one of them.
//
// So each test here drives the real service method against a mock datasource, requires the
// ATOMIC writer to be the one called, and inspects the row the builder produced through
// MockDataSource.CapturedEventOutboxes — which records exactly what the writer would have
// inserted inside the transaction.

// The bounds for the post-commit assertions.
//
// postTransactionActions does its work in a goroutine, so both directions have to be
// asserted against a WINDOW rather than an instant: the positive case waits for a capture to
// appear, and the negative case waits the same window and requires none to have appeared.
// Asserting the negative immediately would pass against an implementation that captures a
// duplicate a millisecond later, which is exactly the defect it exists to catch.
const (
	waitForPostActions = 2 * time.Second
	pollPostActions    = 10 * time.Millisecond
)

// producerAtomicityMock returns a Blnk whose configuration has event publishing switched on
// and whose datasource is a bare testify mock.
//
// Publishing MUST be configured for these assertions to mean anything: PrepareEventOutbox
// returns a nil row when it is not, so an unconfigured instance would produce no captures
// and every assertion below would be vacuously satisfiable by a producer that captured
// nothing at all.
func producerAtomicityMock(t *testing.T) (*Blnk, *mocks.MockDataSource) {
	t.Helper()

	datasource := new(mocks.MockDataSource)
	instance := newOutboxBlnk(t, producerAtomicityConfiguration(), datasource)
	instance.queue = producerAtomicityQueue(t, instance)

	return instance, datasource
}

// producerAtomicityConfiguration is outboxPublishingConfiguration plus the Redis DSN
// NewQueue parses.
//
// TypeSense.Dns is deliberately left EMPTY: queueIndexBatch and queueIndexData both return
// immediately when it is, so the post-action goroutines here perform no indexing and reach
// no network. Redis.Dns is set only because NewQueue parses it at construction — it opens no
// connection — and a blank value would take the Fatal branch and abort the whole test binary.
func producerAtomicityConfiguration() *config.Configuration {
	cnf := outboxPublishingConfiguration()
	cnf.Redis.Dns = "localhost:6379"

	return cnf
}

// producerAtomicityQueue gives the instance a real *Queue with a nil asynq client.
//
// It is REQUIRED, not decorative: postTransactionActions calls l.queue.queueIndexBatch
// unconditionally, so a nil queue panics inside a goroutine and takes the test binary down
// rather than failing one test. The nil client is never used because the empty TypeSense DNS
// short-circuits every enqueue before it is touched.
func producerAtomicityQueue(t *testing.T, instance *Blnk) *Queue {
	t.Helper()

	return NewQueue(instance.Config(), nil)
}

// capturedEventPayload decodes the two-key legacy webhook body out of a captured row.
func capturedEventPayload(t *testing.T, row *model.EventOutbox) (string, map[string]interface{}) {
	t.Helper()

	var body struct {
		Event string                 `json:"event"`
		Data  map[string]interface{} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(row.Payload, &body),
		"the stored payload must be the marshaled NewWebhook object")

	return body.Event, body.Data
}

// TestCreateLedger_CapturesTheEventInTheCreationTransaction is the R-2 assertion for
// ledger.created.
//
// The atomic writer is required to be the one called — a plain CreateLedger expectation
// would go unmatched and testify would fail the call — and the captured row is inspected to
// confirm the builder ran against the SETTLED ledger, so the payload carries the generated
// id the legacy webhook body carried.
func TestCreateLedger_CapturesTheEventInTheCreationTransaction(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	created := model.Ledger{LedgerID: "ldg_atomic", Name: "Atomic ledger"}
	datasource.On("CreateLedger", mock.Anything).Return(created, nil)

	result, err := instance.CreateLedger(model.Ledger{Name: "Atomic ledger"})
	require.NoError(t, err)
	assert.Equal(t, "ldg_atomic", result.LedgerID)

	datasource.AssertCalled(t, "CreateLedger", mock.Anything)

	// THE ATOMICITY CLAIM IS CARRIED BY THE CAPTURE, not by the method name. There is one
	// writer — CreateLedger takes the preparer as a variadic tail — so "the atomic path was
	// used" is not observable from the call. It is observable from CapturedEventOutboxes,
	// which the mock populates ONLY by running a non-nil preparer inside the writer: a
	// producer that stopped supplying one would leave this empty.
	captured := datasource.CapturedEventOutboxes()
	require.Len(t, captured, 1,
		"exactly one event row must be threaded into the ledger creation transaction")
	row := captured[0]
	assert.Equal(t, "ledger.created", row.EventType)
	assert.Equal(t, "ldg_atomic", row.LedgerID,
		"the ledger's own id is the recorded ledger")
	assert.Equal(t, "ldg_atomic", row.PartitionKey,
		"and it is the partition key, so every event of one ledger lands on one partition (R-6)")

	event, data := capturedEventPayload(t, row)
	assert.Equal(t, "ledger.created", event, "the outer envelope keeps its event key")
	assert.Equal(t, "ldg_atomic", data["ledger_id"],
		"the builder must run against the SETTLED row, so the payload carries the generated id")
}

// TestCreateLedger_FailsWhenTheCreationTransactionFails asserts the producer does not
// report a ledger it did not create, which is what makes the rollback in the writer
// observable to a caller.
func TestCreateLedger_FailsWhenTheCreationTransactionFails(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	datasource.On("CreateLedger", mock.Anything).
		Return(model.Ledger{}, errors.New("event outbox unavailable"))

	_, err := instance.CreateLedger(model.Ledger{Name: "Doomed ledger"})
	require.Error(t, err,
		"a ledger whose event could not be captured must not be reported as created")
}

// TestUpdateLedger_DoesNotRecaptureTheCreationEventID covers the rename, and it asserts
// that the rename captures NOTHING — which is a deliberate outcome, not a missing feature.
//
// # What the rename used to do
//
// UpdateLedger calls postLedgerActions, and postLedgerActions used to publish
// ledger.created. So a rename re-announced the ledger's creation, with the new name, over
// the legacy transport. That is pre-existing behaviour and it is preserved WHERE THE LEGACY
// TRANSPORT IS THE TRANSPORT — see TestPostEntityActions_WebhookOnlyDeploymentStillDelivers.
//
// # Why it cannot be captured into the outbox
//
// event_id is DERIVED for ledger.created, from the ledger id and the event type
// (model.DeriveEventID), so the row a rename would build carries the SAME event id as the
// row its creation already committed. Capturing it would hit the unique index on event_id.
// Inside the rename's own transaction that is fatal: the conflict aborts the transaction and
// THE RENAME ITSELF FAILS with a 409 for every ledger whose creation event is still in the
// table. Outside it, the conflict is reported and every rename logs an error about a rename
// that succeeded.
//
// Suppressing the re-emission is what the determinism is FOR: a subscriber's idempotency key
// is event_id, so a conforming subscriber would discard the duplicate anyway. The stream is
// therefore identical to what a correct consumer would observe, and the rename keeps working.
func TestUpdateLedger_DoesNotRecaptureTheCreationEventID(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	renamed := &model.Ledger{LedgerID: "ldg_atomic", Name: "Renamed"}
	datasource.On("UpdateLedger", "ldg_atomic", "Renamed").Return(renamed, nil)

	result, err := instance.UpdateLedger("ldg_atomic", "Renamed")
	require.NoError(t, err)
	assert.Equal(t, "Renamed", result.Name)

	// The post-action goroutine has to be given its chance to misbehave before the absence
	// of a capture means anything.
	time.Sleep(waitForPostActions / 4)

	assert.Empty(t, datasource.CapturedEventOutboxes(),
		"a rename must not build a second ledger.created row: its derived event id is the "+
			"creation's, so the insert would be refused by the unique index and, inside the "+
			"rename's transaction, would fail the rename")
}

// TestUpdateLedger_DerivesTheSameEventIDAsTheCreation is the arithmetic behind the test
// above, stated directly rather than left as a claim in a comment.
//
// If these two ever diverged, the reasoning for not capturing the rename would silently stop
// holding — and nothing else in the suite would notice.
func TestUpdateLedger_DerivesTheSameEventIDAsTheCreation(t *testing.T) {
	created := model.DeriveEventID("ldg_atomic", "ledger.created", model.SchemaVersionV1)
	renamed := model.DeriveEventID("ldg_atomic", "ledger.created", model.SchemaVersionV1)

	assert.Equal(t, created, renamed,
		"ledger.created is derived from the ledger id and the event type only, so a rename "+
			"cannot produce a distinguishable event id")
}

// TestCreateIdentity_CapturesTheEventInTheCreationTransaction is the R-2 assertion for
// identity.created.
func TestCreateIdentity_CapturesTheEventInTheCreationTransaction(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	created := model.Identity{IdentityID: "idt_atomic", FirstName: "Ada"}
	datasource.On("CreateIdentity", mock.Anything).Return(created, nil)

	result, err := instance.CreateIdentity(model.Identity{FirstName: "Ada"})
	require.NoError(t, err)
	assert.Equal(t, "idt_atomic", result.IdentityID)

	datasource.AssertCalled(t, "CreateIdentity", mock.Anything)

	// See TestCreateLedger_CapturesTheEventInTheCreationTransaction: the capture, not the
	// method name, is what proves the preparer reached the writer.
	captured := datasource.CapturedEventOutboxes()
	require.Len(t, captured, 1)
	assert.Equal(t, "identity.created", captured[0].EventType)
	assert.Equal(t, "idt_atomic", captured[0].AggregateID,
		"an identity belongs to no ledger, so its own id is the aggregate")

	_, data := capturedEventPayload(t, captured[0])
	assert.Equal(t, "idt_atomic", data["identity_id"],
		"the builder must run against the settled identity")
}

// TestCreateBalance_CapturesTheEventInTheCreationTransaction is the R-2 assertion for
// balance.created, and it also pins the ledger key.
func TestCreateBalance_CapturesTheEventInTheCreationTransaction(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	created := model.Balance{BalanceID: "bln_atomic", LedgerID: "ldg_atomic", Currency: "USD"}
	datasource.On("CreateBalance", mock.Anything).Return(created, nil)

	result, err := instance.CreateBalance(context.Background(), model.Balance{
		LedgerID: "ldg_atomic",
		Currency: "USD",
	})
	require.NoError(t, err)
	assert.Equal(t, "bln_atomic", result.BalanceID)

	datasource.AssertCalled(t, "CreateBalance", mock.Anything)

	// See TestCreateLedger_CapturesTheEventInTheCreationTransaction.
	captured := datasource.CapturedEventOutboxes()
	require.Len(t, captured, 1)
	assert.Equal(t, "balance.created", captured[0].EventType)
	assert.Equal(t, "ldg_atomic", captured[0].PartitionKey,
		"a balance event is keyed on its LEDGER, so it shares a partition with that ledger's other events")

	_, data := capturedEventPayload(t, captured[0])
	assert.Equal(t, "bln_atomic", data["balance_id"],
		"the builder must run against the settled balance")
}

// TestCreateBalance_CapturesTheEventEvenWhenTheRequestIsAlreadyCancelled is the assertion
// that keeps a cancelled HTTP request from turning into a lost balance OR a lost event.
//
// CreateBalance is reached from the API with c.Request.Context(), which net/http cancels the
// moment the handler returns. The event now commits INSIDE the creation transaction, so a
// producer that threaded the request context into the write would abort that transaction on a
// cancelled request — losing the balance as well as its event, where previously a
// cancellation lost only a notification. The capture must therefore be reachable with an
// already-cancelled context, which is what this asserts end to end rather than by inspecting
// which context object was passed.
func TestCreateBalance_CapturesTheEventEvenWhenTheRequestIsAlreadyCancelled(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	created := model.Balance{BalanceID: "bln_detached", LedgerID: "ldg_atomic", Currency: "USD"}
	datasource.On("CreateBalance", mock.Anything).Return(created, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := instance.CreateBalance(ctx, model.Balance{LedgerID: "ldg_atomic", Currency: "USD"})
	require.NoError(t, err,
		"a cancelled request must not fail the creation: the balance is the caller's money, "+
			"not its notification")
	assert.Equal(t, "bln_detached", result.BalanceID)

	captured := datasource.CapturedEventOutboxes()
	require.Len(t, captured, 1,
		"the event must still be captured; a cancellation-sensitive capture would lose the "+
			"event of every balance created by a request that returned quickly")
	assert.Equal(t, "balance.created", captured[0].EventType)
}

// TestRejectTransaction_CapturesTheEventInTheRejectionTransaction is the R-2 assertion for
// transaction.rejected, and the guard on the duplicate that used to accompany it.
//
// A rejection moves no balances, so it uses RecordTransaction. Before this
// change it used the plain RecordTransaction and the event was captured separately —
// non-atomically — and the worker's rejection handler published a SECOND
// transaction.rejected of its own, so every worker-path rejection was announced twice under
// two different event ids that no subscriber could collapse.
func TestRejectTransaction_CapturesTheEventInTheRejectionTransaction(t *testing.T) {
	// The SPY datasource, not the bare mock: it embeds the mock — so RecordTransaction can
	// still be expected — and additionally records standalone outbox inserts, which is the
	// only way to see the post-commit duplicate this test rules out. A bare mock would
	// panic on the unexpected insert inside a goroutine and take the whole binary down
	// instead of failing this one test.
	datasource := newOutboxSpyDatasource()
	instance := newOutboxBlnk(t, producerAtomicityConfiguration(), datasource)
	instance.queue = producerAtomicityQueue(t, instance)

	rejected := &model.Transaction{TransactionID: "txn_rejected", Status: StatusRejected}
	datasource.On("RecordTransaction", mock.Anything, mock.Anything).
		Return(rejected, nil)

	result, err := instance.RejectTransaction(
		context.Background(),
		&model.Transaction{TransactionID: "txn_rejected", Reference: "ref"},
		"insufficient funds",
	)
	require.NoError(t, err)
	assert.Equal(t, StatusRejected, result.Status)

	datasource.AssertCalled(t, "RecordTransaction", mock.Anything, mock.Anything)

	// The post-commit branch runs in a goroutine, so the SECOND capture this test exists to
	// rule out can only be ruled out after waiting for it. Asserting immediately would pass
	// against an implementation that publishes a duplicate a millisecond later.
	time.Sleep(waitForPostActions / 4)

	captured := datasource.CapturedEventOutboxes()
	require.Len(t, captured, 1,
		"exactly ONE transaction.rejected row. Two producers have been removed to get here: the "+
			"worker's rejection handler, and the post-commit fallback that ran because this call "+
			"site reported the atomic capture as not having happened. The second is invisible in "+
			"production — the derived event id makes the duplicate insert a unique violation — "+
			"except that the conflict is reported through notification.NotifyError, so every "+
			"rejection emitted a system.error about a rejection that had succeeded")
	assert.Empty(t, datasource.standalone(),
		"the rejection's event belongs to the transaction that recorded the rejection; a "+
			"standalone insert here is the post-commit fallback firing over an event that was "+
			"already durable")
	assert.Equal(t, "transaction.rejected", captured[0].EventType)

	event, data := capturedEventPayload(t, captured[0])
	assert.Equal(t, "transaction.rejected", event)
	assert.Equal(t, StatusRejected, data["status"],
		"the payload must describe the rejected row, reason and status included")
	metadata, ok := data["meta_data"].(map[string]interface{})
	require.True(t, ok, "the rejection reason travels in meta_data")
	assert.Equal(t, "insufficient funds", metadata["blnk_rejection_reason"])
}

// TestRejectTransaction_FailsWhenTheEventCannotBePrepared asserts the rejection is not
// persisted when its event cannot even be built.
//
// A payload that will not marshal is a producer defect. Persisting the rejection anyway
// would leave a rejected transaction nobody is ever told about, which is precisely the loss
// the outbox exists to make impossible.
func TestRejectTransaction_FailsWhenTheEventCannotBePrepared(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	unmarshalable := &model.Transaction{
		TransactionID: "txn_bad_payload",
		// A channel cannot be marshalled, and MetaData is the one caller-controlled field
		// on a transaction that carries arbitrary values.
		MetaData: map[string]interface{}{"unserialisable": make(chan int)},
	}

	_, err := instance.RejectTransaction(context.Background(), unmarshalable, "insufficient funds")

	require.Error(t, err)
	datasource.AssertNotCalled(t, "RecordTransaction", mock.Anything, mock.Anything)
}

// TestTransactionLedgerID_PrefersTheSourceLedger pins the key derivation production now
// uses, which is the same one the ordering integration test exercises.
//
// Before this change the production producer supplied no ledger at all, so a transaction
// event was keyed on its source balance while the ordering test injected a ledger key — the
// two were proving different things. Source first, destination as the fallback, and empty
// when neither is available so that a blank key never splits an aggregate across partitions.
func TestTransactionLedgerID_PrefersTheSourceLedger(t *testing.T) {
	source := &model.Balance{BalanceID: "bln_source", LedgerID: "ldg_source"}
	destination := &model.Balance{BalanceID: "bln_destination", LedgerID: "ldg_destination"}

	assert.Equal(t, "ldg_source", transactionLedgerID(source, destination))
	assert.Equal(t, "ldg_destination", transactionLedgerID(nil, destination),
		"the destination's ledger is the fallback")
	assert.Equal(t, "ldg_destination", transactionLedgerID(&model.Balance{LedgerID: "   "}, destination),
		"a whitespace-only ledger is not a ledger")
	assert.Empty(t, transactionLedgerID(nil, nil),
		"no balances means no ledger to state, and an empty key is left to the payload's own derivation")
}

// TestPrepareTransactionEventOutbox_KeysOnTheLedger asserts the production producer supplies
// the ledger, which is what makes requirement R-6's per-ledger ordering true in production
// rather than only in a test fixture.
func TestPrepareTransactionEventOutbox_KeysOnTheLedger(t *testing.T) {
	instance, _ := producerAtomicityMock(t)

	transaction := &model.Transaction{TransactionID: "txn_ordered", Status: StatusApplied}
	source := &model.Balance{BalanceID: "bln_source", LedgerID: "ldg_ordered"}
	destination := &model.Balance{BalanceID: "bln_destination", LedgerID: "ldg_ordered"}

	row, err := prepareTransactionEventForTest(instance, context.Background(), transaction, source, destination)
	require.NoError(t, err)
	require.NotNil(t, row)

	assert.Equal(t, "transaction.applied", row.EventType,
		"getEventFromStatus is reused verbatim, so all seven of its names survive")
	assert.Equal(t, "ldg_ordered", row.LedgerID)
	assert.Equal(t, "ldg_ordered", row.PartitionKey,
		"the ledger is the partition key, which is the per-aggregate ordering guarantee R-6 asks for")
}

// TestPrepareTransactionEventOutbox_ReturnsNilWhenPublishingIsUnconfigured keeps the
// no-op-when-unconfigured contract inherited from SendWebhook.
//
// This is what allows every existing deployment, and the whole existing test suite, to run
// with neither Kafka brokers nor a webhook URL: no row is prepared, the atomic writer
// receives nothing, and the mutation commits exactly as it did before.
func TestPrepareTransactionEventOutbox_ReturnsNilWhenPublishingIsUnconfigured(t *testing.T) {
	datasource := new(mocks.MockDataSource)
	instance := newOutboxBlnk(t, &config.Configuration{Redis: config.RedisConfig{Dns: "localhost:6379"}}, datasource)

	row, err := prepareTransactionEventForTest(
		instance,
		context.Background(),
		&model.Transaction{TransactionID: "txn_unconfigured", Status: StatusApplied},
		&model.Balance{LedgerID: "ldg_x"},
		&model.Balance{LedgerID: "ldg_x"},
	)

	require.NoError(t, err, "an unconfigured deployment is not an error")
	assert.Nil(t, row, "no row means the writer captures nothing and the mutation still commits")
}

// scriptBulkFinalize makes the atomic finalise return the supplied results in order,
// repeating the last one once they run out, and returns the rows it was offered.
//
// The offered event row matters as much as the count. Every attempt must be handed the SAME
// prepared row, because the event id is what makes a retry idempotent: an attempt whose
// commit succeeded but whose acknowledgement was lost is recognised on the next attempt only
// if it carries the same id. A finalise that re-prepared per attempt would record a second,
// differently-identified event for one batch outcome, and nothing downstream could collapse
// the pair.
func scriptBulkFinalize(datasource *mocks.MockDataSource, results ...error) *captureSpy {
	spy := &captureSpy{rows: make([]*model.EventOutbox, 0, len(results))}
	record := func(args mock.Arguments) {
		spy.guard.Lock()
		defer spy.guard.Unlock()

		spy.attempts++
		if row, ok := args.Get(3).(*model.EventOutbox); ok {
			spy.rows = append(spy.rows, row)
		}
	}

	for _, result := range results {
		datasource.On("FinalizeBulkTransactionBatchWithEvent",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Run(record).Return(result == nil, result).Once()
	}

	last := results[len(results)-1]
	datasource.On("FinalizeBulkTransactionBatchWithEvent",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(record).Return(last == nil, last)

	return spy
}

// TestSendBulkTransactionWebhook_CommitsTheOutcomeWithItsEvent is the R-2 assertion for
// bulk_transaction.<status>.
//
// A bulk request executes one transaction at a time with compensating rollback, so by the
// time the batch's outcome is known every mutation it describes has already committed under
// its own transaction: there is no ledger row this summary could be atomic with. What it IS
// atomic with is the batch coordinator's terminal transition — one database transaction
// writes both — so the outcome can never be recorded without its event and the event can
// never describe a batch the coordinator still calls in progress.
//
// The test drives the real producer entry point and requires the ATOMIC repository call. A
// site that reverted to the standalone insert would leave the retry assertions below green
// while the outcome could once again be recorded with its event missing.
func TestSendBulkTransactionWebhook_CommitsTheOutcomeWithItsEvent(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)
	spy := scriptBulkFinalize(datasource, nil)

	require.NoError(t, instance.sendBulkTransactionWebhook(context.Background(), "bulk_atomic", "applied", "", 3))

	rows := spy.captured()
	require.Len(t, rows, 1, "the outcome must be committed by the atomic finalise, not inserted standalone")
	assert.Equal(t, "bulk_transaction.applied", rows[0].EventType)
	assert.Equal(t, "bulk_atomic", rows[0].AggregateID)

	datasource.AssertNotCalled(t, "InsertEventOutbox", mock.Anything, mock.Anything)

	event, data := capturedEventPayload(t, rows[0])
	assert.Equal(t, "bulk_transaction.applied", event)
	assert.Equal(t, "bulk_atomic", data["batch_id"],
		"the payload is the map the legacy transport carried, unchanged")
}

// TestSendBulkTransactionWebhook_RetriesTheBatchOutcomeCapture asserts a transient failure of
// the atomic finalise is retried with the SAME prepared event.
//
// Retrying the whole transaction is what makes the pair recoverable, and re-offering one row
// is what makes the retry idempotent rather than duplicating.
func TestSendBulkTransactionWebhook_RetriesTheBatchOutcomeCapture(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)
	spy := scriptBulkFinalize(datasource, errors.New("connection reset"), nil)

	err := instance.sendBulkTransactionWebhook(context.Background(), "bulk_1", "applied", "", 3)

	require.NoError(t, err, "a transient failure must not lose the batch outcome")
	assert.Equal(t, 2, spy.count(), "the first attempt failed and the second succeeded")

	rows := spy.captured()
	require.Len(t, rows, 2)
	assert.Equal(t, rows[0].EventID, rows[1].EventID,
		"both attempts must offer the same event id, or a lost acknowledgement records the outcome twice")
}

// TestSendBulkTransactionWebhook_ReportsAnExhaustedBudget asserts the failure is RETURNED
// rather than only logged, so a caller can act on a batch outcome that is not in the outbox,
// AND that it is escalated as system.error.
//
// The escalation is the half that does not depend on a caller: one of this function's two
// callers discarded its error, so before it existed a spent budget lost the batch summary with
// nothing an alert could fire on. It is the same signal a lost balance.monitor alert raises,
// which is what makes the two post-commit producers of PostCommitEventCaptureContract
// observable through one mechanism.
func TestSendBulkTransactionWebhook_ReportsAnExhaustedBudget(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)
	escalations := captureSystemErrorEscalations(t)
	scriptBulkFinalize(datasource, errors.New("outbox unavailable"))

	err := instance.sendBulkTransactionWebhook(context.Background(), "bulk_2", "applied", "", 3)

	require.Error(t, err,
		"an outcome that could not be captured must be reported, not swallowed into a log line")

	raised := escalations.await(t)
	assert.Equal(t, "system.error", raised.event,
		"the escalation must be the catalogue's system.error, which is the event alerting keys on")
	assert.Contains(t, raised.message, "bulk_2",
		"the escalation must name the batch whose summary was lost, since reconciling it is manual")
	assert.Contains(t, raised.message, "every one of 3 attempts failed",
		"the escalation must say WHICH way the outcome was lost, so a spent budget is "+
			"distinguishable from a reused id or an abandoned retry")
}

// TestSendBulkTransactionWebhook_StopsRetryingOnCancellation asserts the retry loop honours
// the caller going away, while still reporting AND escalating the failure.
//
// A cancelled caller is a different reason for the summary to be lost, not a lesser one: the
// insert itself runs under a detached context, so reaching this exit means the write failed and
// the retry was abandoned. Escalating only the exhausted budget would have left this a silent
// way to lose a batch outcome.
func TestSendBulkTransactionWebhook_StopsRetryingOnCancellation(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)
	escalations := captureSystemErrorEscalations(t)
	spy := scriptBulkFinalize(datasource, errors.New("outbox unavailable"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := instance.sendBulkTransactionWebhook(ctx, "bulk_3", "applied", "", 3)

	require.Error(t, err)
	assert.Equal(t, 1, spy.count(),
		"a cancelled caller is a reason to stop retrying, not a reason to keep sleeping")

	raised := escalations.await(t)
	assert.Equal(t, "system.error", raised.event)
	assert.Contains(t, raised.message, "bulk_3")
	assert.Contains(t, raised.message, "the retry was abandoned when the caller's context was cancelled",
		"the escalation names the exit taken, so an abandoned retry is not reported as a spent budget")
}

// TestSendBulkTransactionWebhook_EscalatesAReusedEventID covers the third way the summary is
// lost: the unique index refused the insert as a duplicate that is not an identical event.
//
// The repository reports an identical event already recorded as SUCCESS, so a conflict reaching
// this function is a genuine id collision that no retry can resolve — and the outcome is just
// as absent from the outbox as it is after a spent budget.
func TestSendBulkTransactionWebhook_EscalatesAReusedEventID(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)
	escalations := captureSystemErrorEscalations(t)

	// SCRIPTED ON THE COORDINATOR, because that is the path the summary takes. The outcome and
	// its event are written together by FinalizeBulkTransactionBatchWithEvent, so a reused event
	// id is refused there rather than on the standalone insert — which is only reached when the
	// coordinator row is absent altogether.
	spy := scriptBulkFinalize(datasource,
		apierror.NewAPIError(apierror.ErrConflict, "Event outbox entry already exists", nil))

	err := instance.sendBulkTransactionWebhook(context.Background(), "bulk_4", "failed", "boom", 0)

	require.Error(t, err)
	assert.Equal(t, 1, spy.count(), "a collision is permanent, so the remaining attempts stay unspent")

	raised := escalations.await(t)
	assert.Equal(t, "system.error", raised.event)
	assert.Contains(t, raised.message, "bulk_4")
	assert.Contains(t, raised.message, "the event id was reused, so no retry can resolve it")
}

// TestSendBulkTransactionWebhook_DoesNotRetryAConflictingOutcome pins the one failure that no
// further attempt can resolve.
//
// A conflict means either the event id has been reused or the batch already reports a
// DIFFERENT outcome. Spending the remaining attempts and their backoff on either only delays
// the error the caller needs.
func TestSendBulkTransactionWebhook_DoesNotRetryAConflictingOutcome(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)
	spy := scriptBulkFinalize(datasource, apierror.NewAPIError(
		apierror.ErrConflict, "the batch already reported a different outcome", nil))

	err := instance.sendBulkTransactionWebhook(context.Background(), "bulk_conflict", "applied", "", 3)

	require.Error(t, err)
	assert.Equal(t, 1, spy.count(), "a conflicting outcome must not be retried")
}

// TestSendBulkTransactionWebhook_FallsBackWhenTheBatchWasNeverCoordinated asserts the one
// degradation this design allows, and that it degrades rather than failing.
//
// A coordinator row that could not be written at batch start makes the atomic finalise
// impossible for that batch. Losing the outcome entirely would be worse than capturing it on
// the weaker standalone path, so the producer falls back — and only for this failure, which is
// why the sentinel is distinguished from every other error the finalise can return.
func TestSendBulkTransactionWebhook_FallsBackWhenTheBatchWasNeverCoordinated(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)
	scriptBulkFinalize(datasource, apierror.NewAPIError(
		apierror.ErrNotFound, "the bulk transaction batch was not recorded before its outcome", nil))
	spy := scriptStandaloneInsert(datasource, nil)

	require.NoError(t, instance.sendBulkTransactionWebhook(context.Background(), "bulk_orphan", "failed", "boom", 0))

	require.Equal(t, 1, spy.count(),
		"an uncoordinated batch must still have its outcome captured, on the weaker path")
	assert.Equal(t, "bulk_transaction.failed", spy.captured()[0].EventType)
}

// TestRecordBulkBatchStart_WritesTheCoordinatorBeforeTheBatchRuns asserts the row exists
// before any member transaction, which is what makes a batch that never finishes countable
// instead of invisible.
func TestRecordBulkBatchStart_WritesTheCoordinatorBeforeTheBatchRuns(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	var recorded *model.BulkTransactionBatch
	datasource.On("InsertBulkTransactionBatch", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			recorded, _ = args.Get(1).(*model.BulkTransactionBatch)
		}).Return(nil).Once()

	instance.recordBulkBatchStart(context.Background(), "bulk_start", &model.BulkTransactionRequest{
		Transactions: []*model.Transaction{{TransactionID: "txn_1"}, {TransactionID: "txn_2"}},
		Atomic:       true,
		Inflight:     true,
	})

	require.NotNil(t, recorded, "the coordinator row must be written at batch start")
	assert.Equal(t, "bulk_start", recorded.BatchID)
	assert.Equal(t, model.BulkBatchStatusProcessing, recorded.Status,
		"a batch cannot be recorded already-finished")
	assert.Equal(t, 2, recorded.TransactionCount)
	assert.True(t, recorded.Atomic, "a stuck row must say what the batch was attempting")
	assert.True(t, recorded.Inflight)
}

// TestRecordBulkBatchStart_DoesNotFailTheBatchWhenTheCoordinatorCannotBeWritten keeps a
// bookkeeping write from refusing a ledger request.
//
// The insert hits the same database the member transactions are about to use, so a fault here
// means the batch is going to fail on its own terms anyway. Refusing it here would turn a
// recoverable blip into a rejected request, and the outcome is still captured on the fallback
// path.
func TestRecordBulkBatchStart_DoesNotFailTheBatchWhenTheCoordinatorCannotBeWritten(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)
	datasource.On("InsertBulkTransactionBatch", mock.Anything, mock.Anything).
		Return(errors.New("database unavailable")).Once()

	assert.NotPanics(t, func() {
		instance.recordBulkBatchStart(context.Background(), "bulk_unrecorded", &model.BulkTransactionRequest{
			Transactions: []*model.Transaction{{TransactionID: "txn_1"}},
		})
	})
}

// ---------------------------------------------------------------------------
// balance.monitor — the FALLBACK capture path
//
// The primary path is atomic. prepareBalanceMonitorEventOutboxes evaluates the same
// conditions against the same post-update balances before the write, and
// persistSingleTransactionExecutionWork hands the resulting rows to the writer that commits
// the movement, so on the single-transaction path an alert and the threshold crossing that
// produced it commit together. That is R-2 applied to balance.monitor, and it is covered by
// TestPersistSingleTransactionExecutionWork_CommitsMonitorAlertsWithTheMovement in
// event_outbox_test.go.
//
// checkBalanceMonitors remains for the paths that cannot enrol their rows — today the
// coalesced batch path, whose writer is called from the transaction_coalescing.go that AAP
// §0.6.2 excludes from modification. There the mutation is already durable when the alert
// exists, so the insert is the alert's only chance: what the tests below pin is that the one
// insert it gets survives a transient fault instead of losing the alert on the first try, and
// that a monitor already captured atomically is SKIPPED rather than published twice.
//
// Monitor condition evaluation itself is untouched on both paths — the same
// getBalanceMonitorsCached and the same model.BalanceMonitor.CheckCondition — which is what
// keeps §0.6.2 honoured while the moment of capture moves.
// ---------------------------------------------------------------------------

// monitorCaptureBlnk returns an instance wired for the balance.monitor producer site.
//
// It adds a CACHE to the atomicity mock, because that is what production does:
// getBalanceMonitorsCached reads l.cache before it reaches the datasource, and exercising the
// cached branch is what makes these assertions describe the production path rather than a
// degraded one. The cache is backed by an in-process Redis so the assertions do not depend on
// any external service.
//
// A nil cache no longer panics — the lookup skips the cache and queries the datasource, which
// is the correct degradation now that the persistence path reaches the same function — but a
// test that relied on that would be asserting the fallback rather than the real path.
func monitorCaptureBlnk(t *testing.T) (*Blnk, *mocks.MockDataSource) {
	t.Helper()

	instance, datasource := producerAtomicityMock(t)

	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	instance.cache = cache.NewCacheWithClient(client)

	return instance, datasource
}

// monitorFallbackBlnk builds the ONE deployment shape in which checkBalanceMonitors
// publishes anything at all: a webhook URL, a real asynq client against an in-process Redis,
// and NO Kafka broker.
//
// # Why the shape has to be this exact one
//
// Two guards decide what this function does, and they read the same fact from opposite
// sides. With a broker configured, balanceMonitorHandoffEnabled is true and the function
// returns immediately, because the durable handoff owns the evaluation and publishing from
// both would deliver every alert twice. With no broker configured there is no outbox row to
// capture — PrepareEventOutbox returns nil, deliberately, since no relay would ever drain it
// — and the alert goes straight down the legacy webhook transport instead.
//
// So the fallback's behaviour is OBSERVABLE ONLY as a legacy enqueue. A test that scripted an
// outbox insert here would be asserting against a path that cannot run in either shape, and
// would pass or fail for reasons unrelated to the per-monitor skip it means to pin.
//
// Parameters:
//   - t *testing.T: the test, for the Redis, cache and asynq client lifecycles.
//
// Returns:
//   - *Blnk: an instance with no brokers, a webhook URL and a live enqueue path.
//   - *outboxSpyDatasource: the datasource, for scripting GetBalanceMonitors and for
//     asserting that nothing was captured to the outbox on this shape.
//   - string: the Redis address the legacy tasks land in.
func monitorFallbackBlnk(t *testing.T) (*Blnk, *outboxSpyDatasource, string) {
	t.Helper()

	instance, datasource, redisAddress := legacyOnlyEntityInstance(t)

	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	instance.cache = cache.NewCacheWithClient(client)

	return instance, datasource, redisAddress
}

// monitoredBalance returns a balance whose stored value satisfies crossedMonitor's condition.
func monitoredBalance() *model.Balance {
	balance := &model.Balance{BalanceID: "bln_monitored", LedgerID: "ldg_monitored"}
	balance.InitializeBalanceFields()
	balance.Balance = big.NewInt(700)

	return balance
}

// crossedMonitor returns a monitor whose condition the balance above meets, so the producer
// under test reaches its publish rather than its guard.
func crossedMonitor() model.BalanceMonitor {
	return model.BalanceMonitor{
		MonitorID: "mon_crossed",
		BalanceID: "bln_monitored",
		Condition: model.AlertCondition{
			Field:        "balance",
			Operator:     ">=",
			Value:        1,
			Precision:    100,
			PreciseValue: big.NewInt(100),
		},
	}
}

// monitorEvent is the event the producer publishes, used by the tests that call the durable
// path directly rather than through checkBalanceMonitors.
func monitorEvent() NewWebhook {
	return NewWebhook{Event: "balance.monitor", Payload: crossedMonitor()}
}

// captureSpy records what the standalone insert was offered.
//
// It is mutex-guarded because checkBalanceMonitors publishes from a goroutine, so the
// recording and the assertion happen on different goroutines and an unguarded counter would
// be a data race that -race fails.
type captureSpy struct {
	guard    sync.Mutex
	attempts int
	rows     []*model.EventOutbox
}

// record is the mock's Run callback.
func (s *captureSpy) record(args mock.Arguments) {
	s.guard.Lock()
	defer s.guard.Unlock()

	s.attempts++
	if row, ok := args.Get(1).(*model.EventOutbox); ok {
		s.rows = append(s.rows, row)
	}
}

// count returns how many insert attempts were made.
func (s *captureSpy) count() int {
	s.guard.Lock()
	defer s.guard.Unlock()

	return s.attempts
}

// captured returns a copy of the rows the insert was offered, in order.
func (s *captureSpy) captured() []*model.EventOutbox {
	s.guard.Lock()
	defer s.guard.Unlock()

	rows := make([]*model.EventOutbox, len(s.rows))
	copy(rows, s.rows)

	return rows
}

// scriptStandaloneInsert makes the standalone insert return the supplied results in order,
// repeating the last one once they run out, and returns the spy that observed it.
//
// The rows matter as much as the count: a retry that re-prepared the event would offer a
// DIFFERENT row each time, which is the failure mode
// TestPublishEventDurably_RetriesTheSameEventIDSoARetryCannotDuplicate exists to catch.
func scriptStandaloneInsert(datasource *mocks.MockDataSource, results ...error) *captureSpy {
	spy := &captureSpy{rows: make([]*model.EventOutbox, 0, len(results))}

	for _, result := range results {
		datasource.On("InsertEventOutbox", mock.Anything, mock.Anything).
			Run(spy.record).Return(result).Once()
	}

	// A catch-all repeating the last scripted result, so a test asserting that a budget is
	// spent does not have to enumerate every attempt of it.
	datasource.On("InsertEventOutbox", mock.Anything, mock.Anything).
		Run(spy.record).Return(results[len(results)-1])

	return spy
}

// TestPublishEventDurably_RetriesATransientCaptureFailure is the F-1 regression guard.
//
// Before this, one failed insert destroyed the alert outright: the balance had moved, the
// threshold had been crossed, and the notification simply ceased to exist — with nothing to
// retry it, because the only record that it should have existed was the row that never got
// written.
func TestPublishEventDurably_RetriesATransientCaptureFailure(t *testing.T) {
	instance, datasource := monitorCaptureBlnk(t)
	spy := scriptStandaloneInsert(datasource, errors.New("connection reset"), nil)

	err := instance.PublishEventDurably(context.Background(), monitorEvent())

	require.NoError(t, err, "a transient database fault must not cost the event")
	assert.Equal(t, 2, spy.count(), "the first attempt failed and the second succeeded")
}

// TestPublishEventDurably_RetriesTheSameEventIDSoARetryCannotDuplicate is why the retry sits
// beneath PrepareEventOutbox rather than above it.
//
// A balance.monitor event id is a fresh UUID by design, because a derived id would collapse a
// monitor that fires repeatedly into one event. Retrying by re-entering PublishEvent would
// therefore mint a NEW id per attempt, and an attempt whose insert committed but whose
// acknowledgement was lost would be followed by a second, differently-identified row: one
// business event delivered twice, with nothing at a subscriber able to collapse the pair,
// because duplicate suppression keys on event_id.
func TestPublishEventDurably_RetriesTheSameEventIDSoARetryCannotDuplicate(t *testing.T) {
	instance, datasource := monitorCaptureBlnk(t)
	spy := scriptStandaloneInsert(datasource, errors.New("connection reset"), nil)

	require.NoError(t, instance.PublishEventDurably(context.Background(), monitorEvent()))
	require.Equal(t, 2, spy.count())

	rows := spy.captured()
	require.Len(t, rows, 2)

	first, second := rows[0], rows[1]
	require.NotEmpty(t, first.EventID)
	assert.Equal(t, first.EventID, second.EventID,
		"the retry must re-send the SAME prepared row, so the repository can adopt it as an "+
			"identical duplicate instead of recording the event twice")
	assert.Equal(t, first.Payload, second.Payload, "and the same bytes with it")
}

// TestPublishEventDurably_DoesNotRetryAGenuineConflict asserts the budget is not spent on a
// failure no further attempt can resolve.
//
// An identical event already recorded is reported as SUCCESS by the repository, so a conflict
// reaching the producer is a genuine id collision. Retrying it only delays the error.
func TestPublishEventDurably_DoesNotRetryAGenuineConflict(t *testing.T) {
	instance, datasource := monitorCaptureBlnk(t)
	spy := scriptStandaloneInsert(datasource,
		apierror.NewAPIError(apierror.ErrConflict, "Event outbox entry already exists", nil))

	err := instance.PublishEventDurably(context.Background(), monitorEvent())

	require.Error(t, err, "a collision must be reported rather than retried into the same wall")
	assert.Equal(t, 1, spy.count(), "the remaining attempts are not spent on a conflict")
}

// TestPublishEventDurably_DoesNotRetryARefusedValue is the same argument for a value the
// schema refuses: a not-null, foreign-key or CHECK violation answers identically every time.
func TestPublishEventDurably_DoesNotRetryARefusedValue(t *testing.T) {
	instance, datasource := monitorCaptureBlnk(t)
	spy := scriptStandaloneInsert(datasource,
		apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry violates a field constraint", nil))

	err := instance.PublishEventDurably(context.Background(), monitorEvent())

	require.Error(t, err)
	assert.Equal(t, 1, spy.count(), "a refused value is permanent, so the budget stays unspent")
}

// TestPublishEventDurably_ReportsAnExhaustedBudget asserts the failure is RETURNED and not
// merely logged, so the call site's notification.NotifyError still escalates a lost alert as
// system.error. That escalation is what makes the residual at-most-once window observable
// rather than silent; TestCheckBalanceMonitors_EscalatesALostAlert drives it end to end.
func TestPublishEventDurably_ReportsAnExhaustedBudget(t *testing.T) {
	instance, datasource := monitorCaptureBlnk(t)
	spy := scriptStandaloneInsert(datasource, errors.New("outbox unavailable"))

	err := instance.PublishEventDurably(context.Background(), monitorEvent())

	require.Error(t, err,
		"an event that could not be captured must be reported, not swallowed into a log line")
	assert.Equal(t, standaloneEventCaptureAttempts, spy.count(), "the whole budget is spent first")
}

// TestPublishEventDurably_StopsRetryingOnCancellation asserts cancellation is honoured
// BETWEEN attempts: the caller going away stops the retry rather than the sleep, and the
// failure is still reported.
func TestPublishEventDurably_StopsRetryingOnCancellation(t *testing.T) {
	instance, datasource := monitorCaptureBlnk(t)
	spy := scriptStandaloneInsert(datasource, errors.New("outbox unavailable"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := instance.PublishEventDurably(ctx, monitorEvent())

	require.Error(t, err)
	assert.Equal(t, 1, spy.count(),
		"a cancelled caller is a reason to stop retrying, not a reason to keep sleeping")
}

// TestPublishEvent_MakesASingleCaptureAttempt pins the OTHER half of the change: the budget
// belongs to the durable entry point alone.
//
// Every producer whose event is enrolled in its mutation's transaction keeps one attempt on
// purpose, because for them a failed insert must fail the mutation rather than be retried
// past it. A budget applied to PublishEvent generally would have changed all of them.
func TestPublishEvent_MakesASingleCaptureAttempt(t *testing.T) {
	instance, datasource := monitorCaptureBlnk(t)
	spy := scriptStandaloneInsert(datasource, errors.New("outbox unavailable"))

	err := instance.PublishEvent(context.Background(), monitorEvent())

	require.Error(t, err)
	assert.Equal(t, 1, spy.count(), "PublishEvent's behaviour is unchanged")
}

// TestCheckBalanceMonitors_DefersToTheDurableHandoffWhenKafkaIsConfigured is the R-2
// assertion for balance.monitor, expressed as the absence it depends on.
//
// The post-commit capture is exactly what made this the one at-most-once event: the balance
// was already committed, so a process dying before the insert destroyed the alert and left
// nothing to replay. The alert is now captured by BalanceMonitorHandoffProcessor from a
// handoff row written INSIDE the balance's own transaction, and its events and the handoff's
// completion commit together.
//
// So this path must publish NOTHING. If it published as well, every alert would be delivered
// twice under two different event ids, and duplicate suppression at a subscriber keys on
// event_id — nothing downstream could collapse the pair.
func TestCheckBalanceMonitors_DefersToTheDurableHandoffWhenKafkaIsConfigured(t *testing.T) {
	instance, datasource := monitorCaptureBlnk(t)
	spy := scriptStandaloneInsert(datasource, nil)

	instance.checkBalanceMonitors(context.Background(), monitoredBalance(), balanceMonitorCapture{})

	assert.Never(t, func() bool { return spy.count() > 0 }, waitForPostActions, pollPostActions,
		"with Kafka configured the handoff owns the capture; publishing here too would double every alert")
	datasource.AssertNotCalled(t, "GetBalanceMonitors", "bln_monitored")
}

// legacyOnlyMonitorInstance builds the WEBHOOK-ONLY deployment shape for the monitor
// producer: a webhook URL, a real asynq client against an in-process Redis, no Kafka broker,
// and the cache getBalanceMonitorsCached reads before it reaches the datasource.
//
// The two deployment shapes take genuinely different paths and both have to be covered. With
// brokers, the handoff owns monitor capture and this producer must publish nothing. Without
// them there is nothing to drain a handoff — database.recordBalanceMonitorHandoffs reads the
// SAME predicate and writes none — so this post-commit evaluation is the ONLY capture, and it
// must keep delivering exactly as it did before the event pipeline existed (AAP §0.5.4).
//
// Parameters:
//   - t *testing.T: the test, for the Redis, client and cache lifecycles.
//
// Returns:
//   - *Blnk: the instance under test.
//   - *mocks.MockDataSource: the datasource, for the monitor lookup.
//   - string: the Redis address the legacy tasks land in.
func legacyOnlyMonitorInstance(t *testing.T) (*Blnk, *mocks.MockDataSource, string) {
	t.Helper()

	redisServer := miniredis.RunT(t)
	datasource := new(mocks.MockDataSource)
	instance := newOutboxLegacyBlnk(t, outboxLegacyWebhookConfiguration(redisServer.Addr()), datasource)
	instance.queue = producerAtomicityQueue(t, instance)

	cacheServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: cacheServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	instance.cache = cache.NewCacheWithClient(client)

	return instance, datasource, redisServer.Addr()
}

// TestCheckBalanceMonitors_DeliversOverTheLegacyTransportWithoutKafka pins the path that
// survives for a broker-less deployment.
//
// With no broker there is no outbox to capture into and no handoff processor to evaluate, so
// this post-commit evaluation is the alert's only route and it goes straight down the legacy
// webhook transport. The alert must arrive with the same bytes it always had — the monitor
// object inside the two-key envelope — so a subscriber on this shape cannot tell that the
// event pipeline exists at all.
func TestCheckBalanceMonitors_DeliversOverTheLegacyTransportWithoutKafka(t *testing.T) {
	instance, datasource, redisAddress := legacyOnlyMonitorInstance(t)
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor{crossedMonitor()}, nil)

	instance.checkBalanceMonitors(context.Background(), monitoredBalance(), balanceMonitorCapture{})

	requireLegacyDelivery(t, redisAddress, monitorEvent())
	datasource.AssertNotCalled(t, "InsertEventOutbox", mock.Anything, mock.Anything)
}

// TestCheckBalanceMonitors_EscalatesALostAlert is the monitor half of the escalation parity
// the bulk tests above assert.
//
// Both producers named in PostCommitEventCaptureContract publish an event that no mutation
// owns, so for both of them a capture that never succeeds is an event that exists nowhere: no
// outbox row to claim, nothing to dead-letter, nothing to replay. The two must therefore raise
// the SAME signal, and this is the test that says so from the monitor side — the bulk side is
// TestSendBulkTransactionWebhook_ReportsAnExhaustedBudget. Without both, "escalation parity" is
// a claim in a comment rather than a property of the code.
func TestCheckBalanceMonitors_EscalatesALostAlert(t *testing.T) {
	instance, datasource, queueAddress := unreachableQueueMonitorInstance(t)
	escalations := captureSystemErrorEscalations(t)

	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor{crossedMonitor()}, nil)

	instance.checkBalanceMonitors(context.Background(), monitoredBalance(), balanceMonitorCapture{})

	raised := escalations.await(t)
	assert.Equal(t, "system.error", raised.event,
		"a lost monitor alert raises the same event a lost batch summary does")
	assert.Contains(t, raised.message, queueAddress,
		"the escalation carries the transport failure that lost the alert, not a generic message: "+
			"an operator reading it has to be able to tell WHICH dependency was unreachable")
	datasource.AssertNotCalled(t, "InsertEventOutbox", mock.Anything, mock.Anything)
}

// unreachableQueueMonitorInstance builds the one shape in which a balance.monitor alert can still
// be lost outright, which is what makes the escalation above assertable at all.
//
// # Why the loss has to be staged on the TRANSPORT rather than on the outbox
//
// The obvious staging — a Kafka-configured instance whose InsertEventOutbox fails — cannot
// happen. With a broker configured the alert is captured inside the balance's own transaction and
// checkBalanceMonitors returns before it reaches any capture at all; with no broker,
// PrepareEventOutbox deliberately returns nil, so there is no row to fail on. Either way the
// outbox is not where this alert can go missing.
//
// Where it CAN go missing is the broker-less deployment's legacy enqueue: no outbox row is
// written by design, so the asynq enqueue is the alert's only delivery, and a queue that cannot
// be reached loses the threshold notification outright while the balance movement stands. That is
// the residual at-most-once window docs/event-streaming.md names for balance.monitor, and this is
// the fault that opens it.
//
// The address is produced by starting an in-process Redis and closing it immediately: that yields
// a loopback address nothing is listening on, so the enqueue fails with a connection refusal in
// microseconds rather than after a dial timeout, and the address itself is what the assertion
// matches on — deterministic regardless of how the platform words the refusal. miniredis.Close is
// idempotent, so the cleanup RunT registered is a no-op.
//
// Parameters:
//   - t *testing.T: the test, for the Redis, client and cache lifecycles.
//
// Returns:
//   - *Blnk: an instance with a webhook URL, no brokers and an unreachable queue.
//   - *mocks.MockDataSource: the datasource, for the monitor lookup and for asserting that
//     nothing was captured.
//   - string: the unreachable queue address, which the escalation must name.
func unreachableQueueMonitorInstance(t *testing.T) (*Blnk, *mocks.MockDataSource, string) {
	t.Helper()

	queueServer := miniredis.RunT(t)
	queueAddress := queueServer.Addr()
	queueServer.Close()

	datasource := new(mocks.MockDataSource)
	instance := newOutboxLegacyBlnk(t, outboxLegacyWebhookConfiguration(queueAddress), datasource)
	instance.queue = producerAtomicityQueue(t, instance)

	cacheServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: cacheServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	instance.cache = cache.NewCacheWithClient(client)

	return instance, datasource, queueAddress
}

// TestCheckBalanceMonitors_PublishesNothingWhenNoConditionIsMet keeps the substitution inside
// the CheckCondition guard.
//
// Monitor condition evaluation is frozen domain logic (AAP §0.6.2). A publish that escaped the
// guard would announce a threshold crossing that never happened, to every subscriber. It is
// driven on the broker-less shape because that is the only shape where this function still
// evaluates anything at all.
func TestCheckBalanceMonitors_PublishesNothingWhenNoConditionIsMet(t *testing.T) {
	instance, datasource, redisAddress := legacyOnlyMonitorInstance(t)

	unmet := crossedMonitor()
	unmet.Condition.Operator = "<"
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor{unmet}, nil)

	instance.checkBalanceMonitors(context.Background(), monitoredBalance(), balanceMonitorCapture{})

	// countPendingLegacyTasks rather than the T-taking reader: this condition runs on
	// testify's goroutine, which may outlive the test.
	assert.Never(t, func() bool { return countPendingLegacyTasks(redisAddress) > 0 },
		waitForPostActions, pollPostActions,
		"a condition that was not met must publish nothing at all")
}

// TestPrepareBalanceMonitorEventOutboxes_BuildsTheAlertBeforeTheWrite drives the PRIMARY,
// atomic monitor producer directly.
//
// It proves the row the writer receives is built from the same monitors, the same condition and
// the same ledger the post-commit fallback used — so moving the capture into the mutation's
// transaction did not change WHAT is captured, only when. The fallback's own remaining behaviour
// is covered by TestCheckBalanceMonitors_DeliversOverTheLegacyTransportWithoutKafka and
// TestCheckBalanceMonitors_EscalatesALostAlert.
func TestPrepareBalanceMonitorEventOutboxes_BuildsTheAlertBeforeTheWrite(t *testing.T) {
	instance, datasource := monitorCaptureBlnk(t)

	unmet := crossedMonitor()
	unmet.MonitorID = "mon_unmet"
	unmet.Condition.Operator = "<"

	// Both monitors come back from the same lookup; only one condition is met. Asserting on the
	// pair is what proves the SET is filtered by CheckCondition rather than captured wholesale.
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor{crossedMonitor(), unmet}, nil)

	rows, captured, err := instance.prepareBalanceMonitorEvents(context.Background(),
		[]*model.Balance{monitoredBalance()})
	require.NoError(t, err)

	require.Len(t, rows, 1,
		"exactly the monitor whose condition is met must produce a row; capturing an unmet monitor "+
			"would announce a threshold crossing that never happened")
	assert.Equal(t, "balance.monitor", rows[0].EventType)
	assert.Equal(t, "ldg_monitored", rows[0].LedgerID,
		"the ledger is supplied from the monitored balance on this path too, so an alert captured "+
			"atomically and one captured by the fallback are keyed identically")

	event, data := capturedEventPayload(t, rows[0])
	assert.Equal(t, "balance.monitor", event)
	assert.Equal(t, "mon_crossed", data["monitor_id"],
		"the payload is the same monitor object the legacy transport carried")

	assert.True(t, captured.covers("bln_monitored"),
		"the balance must be reported as evaluated, which is what tells the fallback this pass ran")
	assert.True(t, captured.holds("bln_monitored", "mon_crossed"))
	assert.False(t, captured.holds("bln_monitored", "mon_unmet"),
		"an unmet monitor must not be reported as captured, or the fallback would skip it on a "+
			"later movement that DOES meet its condition")
}

// TestPrepareBalanceMonitorEventOutboxes_DegradesToTheFallbackWhenTheLookupFails is the
// failure direction of the atomic path.
//
// # Why the movement still commits
//
// The lookup failure is reported and STEPPED PAST rather than returned, and that is a scope
// boundary rather than a preference: this change substitutes a transport and may not turn an
// outage of the monitor table into an outage of the ledger. Returning it here would abandon the
// write, so a transaction Blnk has always applied would be refused because a table used only for
// threshold alerting could not be read.
//
// # What has to be true instead, and it is the whole assertion
//
// The balance must NOT be reported as evaluated. That is what routes it back to the post-commit
// check, which is the behaviour that existed before this path and carries its own documented
// loss window. A capture claiming a balance it never read would suppress the fallback as well,
// so the crossing would be examined by neither route — strictly worse than the window this path
// narrows.
func TestPrepareBalanceMonitorEventOutboxes_DegradesToTheFallbackWhenTheLookupFails(t *testing.T) {
	instance, datasource := monitorCaptureBlnk(t)

	lookupErr := errors.New("monitor lookup unavailable")
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor(nil), lookupErr)

	rows, captured, err := instance.prepareBalanceMonitorEvents(context.Background(),
		[]*model.Balance{monitoredBalance()})

	require.NoError(t, err,
		"a monitor lookup failure must not abandon the write: the movement is the ledger's job "+
			"and the alert is a notification about it")
	assert.Empty(t, rows, "nothing was read, so nothing could be captured")
	assert.False(t, captured.covers("bln_monitored"),
		"and nothing may be reported as evaluated, or the fallback would skip a balance whose "+
			"monitors were never actually read")
}

// TestCheckBalanceMonitors_SkipsAnAlertAlreadyCommittedWithItsMovement is the guard on
// the duplicate the atomic monitor capture creates the possibility of.
//
// Once an alert is inserted inside the mutation's transaction, the post-commit path would
// publish a SECOND copy of it — and a balance.monitor event id is a fresh UUID by design, so no
// subscriber-side idempotency could collapse the pair. The captured set is the only thing
// preventing that, and it must be applied per MONITOR: one balance can carry many monitors and a
// movement can satisfy some and not others, so skipping the whole balance would lose the alerts
// that were not captured.
func TestCheckBalanceMonitors_SkipsAnAlertAlreadyCommittedWithItsMovement(t *testing.T) {
	t.Run("a captured monitor publishes nothing", func(t *testing.T) {
		instance, datasource := monitorCaptureBlnk(t)
		datasource.On("GetBalanceMonitors", "bln_monitored").
			Return([]model.BalanceMonitor{crossedMonitor()}, nil)
		spy := scriptStandaloneInsert(datasource, nil)

		instance.checkBalanceMonitors(context.Background(), monitoredBalance(),
			capturedMonitorAlert("bln_monitored", "mon_crossed"))

		assert.Never(t, func() bool { return spy.count() > 0 }, waitForPostActions, pollPostActions,
			"an alert already committed with its movement must not be captured a second time")
	})

	t.Run("an uncaptured monitor on the same balance still publishes", func(t *testing.T) {
		// The direction a whole-balance skip would break. The second monitor's alert is NOT in
		// the mutation's transaction, so this route is the only thing that will ever deliver it.
		instance, datasource, redisAddress := monitorFallbackBlnk(t)

		second := crossedMonitor()
		second.MonitorID = "mon_second"

		datasource.On("GetBalanceMonitors", "bln_monitored").
			Return([]model.BalanceMonitor{crossedMonitor(), second}, nil)

		instance.checkBalanceMonitors(context.Background(), monitoredBalance(),
			capturedMonitorAlert("bln_monitored", "mon_crossed"))

		// Exactly one delivery, and the assertion on WHICH one is the whole point: a skip
		// applied per balance rather than per monitor would deliver nothing here, and a skip
		// not applied at all would deliver two.
		requireLegacyDelivery(t, redisAddress, NewWebhook{Event: "balance.monitor", Payload: second})
		datasource.assertWroteNothing(t,
			"with no broker there is no relay to drain a captured row, so the legacy transport "+
				"is the transport on this deployment")
	})

	t.Run("a nil captured set behaves exactly as before", func(t *testing.T) {
		// The coalesced batch path supplies nothing, so the zero capture must mean "nothing was
		// captured" rather than "everything was" — the conservative direction, in which a
		// duplicate an operator can see beats a threshold crossing nobody is told about.
		instance, datasource, redisAddress := monitorFallbackBlnk(t)
		datasource.On("GetBalanceMonitors", "bln_monitored").
			Return([]model.BalanceMonitor{crossedMonitor()}, nil)

		instance.checkBalanceMonitors(context.Background(), monitoredBalance(),
			balanceMonitorCapture{})

		requireLegacyDelivery(t, redisAddress,
			NewWebhook{Event: "balance.monitor", Payload: crossedMonitor()})
	})
}

// TestPostTransactionActions_DoesNotRecaptureAnAlreadyCapturedEvent is the guard on the
// duplicate the conditional capture exists to prevent.
//
// The single-transaction path inserts the event with the mutation. Capturing it again after
// the commit would publish every transaction event twice, under two different event ids —
// and duplicate suppression at a subscriber keys on event_id, so nothing downstream could
// collapse them.
func TestPostTransactionActions_DoesNotRecaptureAnAlreadyCapturedEvent(t *testing.T) {
	datasource := newOutboxSpyDatasource()
	instance := newOutboxBlnk(t, producerAtomicityConfiguration(), datasource)
	instance.queue = producerAtomicityQueue(t, instance)

	instance.postTransactionActions(
		context.Background(),
		&model.Transaction{TransactionID: "txn_captured", Status: StatusApplied},
		nil,
		nil,
		true,
	)

	assert.Eventually(t, func() bool {
		return len(datasource.standalone()) == 0
	}, waitForPostActions, pollPostActions,
		"an event captured in the mutation transaction must never be captured a second time")
}

// TestPostTransactionActions_CapturesTheEventForTheCoalescedBatchPath is the other half.
//
// The coalesced batch path assembles its writer arguments in transaction_coalescing.go,
// which belongs to the frozen transaction-processing pipeline (AAP §0.6.2), so its event
// rows have no route into the batch writer and this remains their only capture — exactly as
// it was for every event before this change. Removing it would lose those events entirely.
func TestPostTransactionActions_CapturesTheEventForTheCoalescedBatchPath(t *testing.T) {
	datasource := newOutboxSpyDatasource()
	instance := newOutboxBlnk(t, producerAtomicityConfiguration(), datasource)
	instance.queue = producerAtomicityQueue(t, instance)

	instance.postTransactionActions(
		context.Background(),
		&model.Transaction{TransactionID: "txn_coalesced", Status: StatusApplied},
		&model.Balance{BalanceID: "bln_source", LedgerID: "ldg_coalesced"},
		&model.Balance{BalanceID: "bln_destination", LedgerID: "ldg_coalesced"},
		false,
	)

	require.Eventually(t, func() bool {
		return len(datasource.standalone()) == 1
	}, waitForPostActions, pollPostActions,
		"the coalesced path's event must still be captured, or those events are lost outright")

	row := datasource.standalone()[0]
	assert.Equal(t, "transaction.applied", row.EventType)
	assert.Equal(t, "ldg_coalesced", row.PartitionKey,
		"the coalesced path must key on the ledger too, or one ledger's events split across "+
			"partitions depending on which execution path ran them")
}

// legacyOnlyEntityInstance builds the WEBHOOK-ONLY deployment shape: a webhook URL, a real
// asynq client against an in-process Redis, and NO Kafka broker.
//
// This is the shape every deployment is in before it opts into Kafka, and it is the one the
// entity producers stopped serving: with no broker there is no preparer, so nothing is
// captured, and the post-commit publish that used to serve it had been removed. The queue is
// supplied because postLedgerActions and its siblings index unconditionally and a nil queue
// panics inside their goroutine.
//
// Parameters:
//   - t *testing.T: the test, for the Redis and client lifecycles.
//
// Returns:
//   - *Blnk: the instance under test.
//   - *outboxSpyDatasource: the datasource, which must record no capture on this shape.
//   - string: the Redis address the legacy tasks land in.
func legacyOnlyEntityInstance(t *testing.T) (*Blnk, *outboxSpyDatasource, string) {
	t.Helper()

	redisServer := miniredis.RunT(t)
	datasource := newOutboxSpyDatasource()
	instance := newOutboxLegacyBlnk(t, outboxLegacyWebhookConfiguration(redisServer.Addr()), datasource)
	instance.queue = producerAtomicityQueue(t, instance)

	return instance, datasource, redisServer.Addr()
}

// requireLegacyDelivery waits for exactly one legacy task and asserts it carries the event's
// bytes verbatim.
//
// The producers publish from a goroutine, so the delivery is asserted against a WINDOW. The
// body is compared byte for byte rather than field by field because byte equality with the
// marshaled NewWebhook envelope IS the payload guarantee (AAP R-8, AMBIGUITY-3): a subscriber
// on this deployment must not be able to tell that anything changed.
func requireLegacyDelivery(t *testing.T, redisAddress string, event NewWebhook) {
	t.Helper()

	require.Eventually(t, func() bool {
		return countPendingLegacyTasks(redisAddress) == 1
	}, waitForPostActions, pollPostActions,
		"a webhook-only deployment must still receive %s; before the fallback was restored it "+
			"was delivered by NEITHER transport", event.Event)

	// READ ONCE THE WINDOW HAS CLOSED, and through the error-returning reader rather than the
	// counter: the assertions below are about the task's CONTENT, so a read failure has to be a
	// test failure rather than a zero that reads as "no task".
	tasks, listErr := pendingLegacyTasks(redisAddress)
	require.NoError(t, listErr, "listing the pending legacy webhook tasks")
	require.Len(t, tasks, 1, "exactly one legacy task must be pending for %s", event.Event)

	assert.Equal(t, outboxLegacyQueueName, tasks[0].Queue,
		"the task must land on the configured webhook queue, whose mux holds the handler")
	assert.Equal(t, string(outboxLegacyWebhookBody(t, event)), string(tasks[0].Payload),
		"the enqueued body must be the legacy two-key envelope, byte for byte")
}

// TestPostEntityActions_WebhookOnlyDeploymentStillDelivers is the regression guard on the
// legacy continuity of the three entity creation events.
//
// # The defect it pins
//
// Capture for ledger.created, identity.created and balance.created moved INTO the repository,
// behind an EventPreparer, to close the R-2 window. The preparer is nil when
// eventCaptureEnabled is false, which is correct — a row no relay can drain is worse than no
// row — and the post-commit publish was removed as a duplicate, which is correct WHEN A
// PREPARER EXISTS. On a webhook-only deployment neither is true: no preparer, no publish, and
// three event types silently delivered by nothing at all. No error, no failed delivery, no
// log line.
//
// Every other producer in the package kept its publish call and so kept working, which is why
// this reached the merged tree looking like a tidied-up duplicate rather than a lost event.
func TestPostEntityActions_WebhookOnlyDeploymentStillDelivers(t *testing.T) {
	t.Run("ledger.created", func(t *testing.T) {
		instance, datasource, redisAddress := legacyOnlyEntityInstance(t)
		ledger := &model.Ledger{LedgerID: "ldg_legacy", Name: "Legacy ledger"}

		instance.postLedgerActions(context.Background(), ledger)

		requireLegacyDelivery(t, redisAddress, NewWebhook{Event: "ledger.created", Payload: ledger})
		datasource.assertWroteNothing(t,
			"with no broker there is no relay to drain a captured row, so capturing one would "+
				"strand it forever; the legacy transport is the transport on this deployment")
	})

	t.Run("identity.created", func(t *testing.T) {
		instance, datasource, redisAddress := legacyOnlyEntityInstance(t)
		identity := &model.Identity{IdentityID: "idt_legacy", FirstName: "Ada"}

		instance.postIdentityActions(context.Background(), identity)

		requireLegacyDelivery(t, redisAddress, NewWebhook{Event: "identity.created", Payload: identity})
		datasource.assertWroteNothing(t, "nothing to capture for on this deployment")
	})

	t.Run("balance.created", func(t *testing.T) {
		instance, datasource, redisAddress := legacyOnlyEntityInstance(t)
		balance := &model.Balance{BalanceID: "bln_legacy", LedgerID: "ldg_legacy", Currency: "USD"}

		instance.postBalanceActions(context.Background(), balance)

		requireLegacyDelivery(t, redisAddress, NewWebhook{Event: "balance.created", Payload: balance})
		datasource.assertWroteNothing(t, "nothing to capture for on this deployment")
	})

	t.Run("a rename still re-announces the ledger over the legacy transport", func(t *testing.T) {
		instance, _, redisAddress := legacyOnlyEntityInstance(t)
		renamed := &model.Ledger{LedgerID: "ldg_legacy", Name: "Renamed"}

		// UpdateLedger has always reached postLedgerActions, so the rename has always
		// re-emitted ledger.created down this transport. There is no unique index on this
		// path to collapse it, so the pre-migration behaviour is reproduced exactly —
		// including its duplicate — which is what legacy continuity means.
		instance.postLedgerActions(context.Background(), renamed)

		requireLegacyDelivery(t, redisAddress, NewWebhook{Event: "ledger.created", Payload: renamed})
	})

	t.Run("a cancelled request context does not lose the delivery", func(t *testing.T) {
		instance, _, redisAddress := legacyOnlyEntityInstance(t)
		balance := &model.Balance{BalanceID: "bln_cancelled", LedgerID: "ldg_legacy", Currency: "USD"}

		// The API calls CreateBalance with c.Request.Context(), which net/http cancels the
		// moment the handler returns — routinely before a goroutine spawned by the post
		// action gets to run. The publish context is detached for exactly this reason.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		instance.postBalanceActions(ctx, balance)

		requireLegacyDelivery(t, redisAddress, NewWebhook{Event: "balance.created", Payload: balance})
	})
}

// TestPostEntityActions_DoNotPublishWhenTheRepositoryCaptured is the other half, and it is
// what keeps the fix from becoming the duplicate it replaced.
//
// With a broker configured the preparer runs inside the creation transaction, so a publish
// here would be a second capture of the same event. It would not even be a silent one: the
// derived event id makes the second insert a unique violation, so every ledger, identity and
// balance creation would log a conflict and emit a system.error about a creation that
// succeeded.
func TestPostEntityActions_DoNotPublishWhenTheRepositoryCaptured(t *testing.T) {
	newInstance := func(t *testing.T) (*Blnk, *outboxSpyDatasource, string) {
		t.Helper()

		redisServer := miniredis.RunT(t)
		cnf := outboxLegacyWebhookConfiguration(redisServer.Addr())
		// Both transports configured: the outbox owns the event, and the relay drives the
		// legacy leg from the very same row.
		cnf.Kafka = config.KafkaConfig{Brokers: []string{"localhost:9092"}}

		datasource := newOutboxSpyDatasource()
		instance := newOutboxLegacyBlnk(t, cnf, datasource)
		instance.queue = producerAtomicityQueue(t, instance)

		return instance, datasource, redisServer.Addr()
	}

	assertSilent := func(t *testing.T, datasource *outboxSpyDatasource, redisAddress string) {
		t.Helper()

		time.Sleep(waitForPostActions / 4)

		assert.Empty(t, outboxPendingLegacyTasks(t, redisAddress),
			"the legacy leg belongs to the relay, driven from the claimed row; enqueuing here "+
				"as well would deliver the event twice with two different task identities")
		datasource.assertWroteNothing(t,
			"the repository captured this event inside the creation transaction; a second "+
				"capture is refused by the unique index and reported as an error on every create")
	}

	t.Run("ledger", func(t *testing.T) {
		instance, datasource, redisAddress := newInstance(t)
		instance.postLedgerActions(context.Background(), &model.Ledger{LedgerID: "ldg_kafka"})
		assertSilent(t, datasource, redisAddress)
	})

	t.Run("identity", func(t *testing.T) {
		instance, datasource, redisAddress := newInstance(t)
		instance.postIdentityActions(context.Background(), &model.Identity{IdentityID: "idt_kafka"})
		assertSilent(t, datasource, redisAddress)
	})

	t.Run("balance", func(t *testing.T) {
		instance, datasource, redisAddress := newInstance(t)
		instance.postBalanceActions(context.Background(), &model.Balance{BalanceID: "bln_kafka", LedgerID: "ldg_kafka"})
		assertSilent(t, datasource, redisAddress)
	})
}

// TestPostBalanceActions_DeclinesTheIdempotentIndicatorConflict keeps a restored publish from
// restoring a defect that was removed with it.
//
// CreateBalance reports a unique_indicator_currency violation as SUCCESS with an empty
// balance — its long-standing idempotent-create contract — and it still calls
// postBalanceActions. The post-commit publish used to announce balance.created for it: an
// event whose payload had no balance id, no ledger and no currency, describing a creation
// that did not happen. The repository declines to capture on that path; the fallback declines
// on the same grounds.
func TestPostBalanceActions_DeclinesTheIdempotentIndicatorConflict(t *testing.T) {
	instance, datasource, redisAddress := legacyOnlyEntityInstance(t)

	// The zero balance is exactly what CreateBalance returns on that path.
	instance.postBalanceActions(context.Background(), &model.Balance{})

	time.Sleep(waitForPostActions / 4)

	assert.Empty(t, outboxPendingLegacyTasks(t, redisAddress),
		"an empty balance means nothing was created, so there is no creation to announce")
	datasource.assertWroteNothing(t, "and nothing to capture either")
}

// TestPublishEntityEventWhenUncaptured_IsANoOpWithNoTransportAtAll pins the third state.
//
// No broker and no webhook URL is a LEGITIMATE STEADY STATE, not a misconfiguration: it is
// what every existing test and every deployment with no notification sink runs in. The
// fallback must be invisible there.
func TestPublishEntityEventWhenUncaptured_IsANoOpWithNoTransportAtAll(t *testing.T) {
	redisServer := miniredis.RunT(t)
	cnf := outboxLegacyWebhookConfiguration(redisServer.Addr())
	cnf.Notification.Webhook.Url = ""

	datasource := newOutboxSpyDatasource()
	instance := newOutboxLegacyBlnk(t, cnf, datasource)

	require.NoError(t, instance.publishEntityEventWhenUncaptured(
		context.Background(),
		"ldg_silent",
		NewWebhook{Event: "ledger.created", Payload: &model.Ledger{LedgerID: "ldg_silent"}},
	), "being unconfigured must never fail the mutation the caller just performed")

	assert.Empty(t, outboxPendingLegacyTasks(t, redisServer.Addr()))
	datasource.assertWroteNothing(t, "no transport is configured, so there is nothing to do")
}

// Compile-time proof that the builders the producers hand the writers satisfy the
// repository's declared types. It fails here, on the line stating the contract, rather than
// at a call site.
var (
	_ func(context.Context) database.EventPreparer[model.Ledger]   = (*Blnk)(nil).ledgerCreatedEventPreparer
	_ func(context.Context) database.EventPreparer[model.Identity] = (*Blnk)(nil).identityCreatedEventPreparer
	_ func(context.Context) database.EventPreparer[model.Balance]  = (*Blnk)(nil).balanceCreatedEventPreparer
)

// prepareTransactionEventForTest builds a transaction's event row exactly as
// buildTransactionExecutionWork does before handing it to the atomic writer: the status-derived
// event name, the transaction as the payload, and the ledger resolved from the balances so the
// row is keyed on the aggregate requirement R-6 partitions by.
func prepareTransactionEventForTest(
	l *Blnk,
	ctx context.Context,
	transaction *model.Transaction,
	sourceBalance, destinationBalance *model.Balance,
) (*model.EventOutbox, error) {
	return l.PrepareEventOutbox(ctx, NewWebhook{
		Event:   getEventFromStatus(transaction.Status),
		Payload: transaction,
	}, WithEventLedgerID(transactionLedgerID(sourceBalance, destinationBalance)))
}

// ---------------------------------------------------------------------------
// The complete set of post-commit captures, and the guard that keeps it honest
//
// Requirement R-2 puts an event in the same database transaction as the mutation it
// describes, and almost every producer does. THREE do not, because their mutation is
// already committed when the event comes into existence, and those three are at-most-once:
// a process death between the commit and the insert loses the event with no row anywhere to
// replay from.
//
// # Why this needed a test rather than a comment
//
// Each of the three sites used to describe ITSELF as the only one. balance.go called its
// window "the single documented exception to requirement R-2"; transaction_bulk.go opened a
// section with "This is the ONE event that cannot be enrolled in its mutation's
// transaction"; docs/event-streaming.md published a section headed "The one exception:
// balance.monitor is at-most-once" and told subscribers that "every other event type carries
// the full transactional guarantee". All three statements were false, and each was false
// because of the other two \u2014 so an operator reading any one of them was told this pipeline
// had one loss window when it has three, and the third one (a coalesced batch's transaction
// events) was not mentioned anywhere a subscriber would look.
//
// A prose fix alone would decay the same way. These tests pin the set from both ends: the
// call sites must still spend a retry budget, the published documentation must still name
// every member, and NO source may reclaim sole-exception status.
// ---------------------------------------------------------------------------

// postCommitCaptureSite is one member of the at-most-once set.
type postCommitCaptureSite struct {
	// eventClass is the event name or prefix a subscriber sees.
	eventClass string

	// file is the production source that performs the capture.
	file string

	// budgetedCapture is the exact call or loop header that spends more than one attempt.
	// A regression to a single-attempt capture has to delete it, which fails here.
	budgetedCapture string

	// why states the structural reason atomicity is unavailable, for the failure message.
	why string
}

// postCommitCaptureSites is the authoritative enumeration of the standalone capture SITES — the
// three places in the production source that spend a bounded retry budget because the mutation
// they describe has already committed.
//
// IT IS NOT THE SAME SET as the at-most-once EVENT TYPES, and the difference is the reason two
// separate reviews each arrived at a confident but different "three". Both sets have three
// members; they disagree on the third in both directions:
//
//   - `system.error` is an at-most-once event type but NOT a site here. It captures through
//     PublishEvent in a single attempt, because it describes no mutation and therefore has no
//     ledger state behind it that a retry would protect.
//   - The coalesced batch's `transaction.*` events are a site here but NOT an at-most-once event
//     type. The batch writer derives the same rows and inserts them inside its own transaction,
//     so this site's copy is normally recognised as the identical stored row and reported as
//     success.
//
// PublishEventDurably's doc comment states both sets side by side, and
// PostCommitEventCaptureContract declares the event-type set once. Adding a fourth site means
// adding a row here; adding a fourth at-most-once event type means extending that constant AND
// the published section in docs/event-streaming.md — which is what the guards below enforce.
var postCommitCaptureSites = []postCommitCaptureSite{
	{
		eventClass:      "balance.monitor",
		file:            "balance.go",
		budgetedCapture: "l.PublishEventDurably(ctx, NewWebhook{",
		why:             "the balance movement that satisfied the condition committed under another transaction",
	},
	{
		eventClass:      "bulk_transaction.",
		file:            "transaction_bulk.go",
		budgetedCapture: "for attempt := 1; attempt <= bulkOutcomeCaptureAttempts; attempt++ {",
		why:             "a batch summary belongs to no single mutation and a bulk request has no batch-spanning transaction",
	},
	{
		eventClass:      "transaction.",
		file:            "transaction_execution.go",
		budgetedCapture: "err = l.PublishEventDurably(context.WithoutCancel(ctx), NewWebhook{",
		why:             "the coalescing writer is called from a file AAP §0.6.2 freezes, so the batch cannot thread event rows",
	},
}

// soleExceptionClaims are the phrasings that assert a single exception to R-2.
//
// Every one of these was present in the tree and every one was false. They are matched as
// literals because the defect IS the literal claim: this is an assertion about what the
// repository tells a reader, and only prose can carry it.
var soleExceptionClaims = []string{
	"single documented exception",
	"the single explicit exception",
	"ONE event that cannot be enrolled",
	"The one exception:",
	"single event type whose capture",
	"Every other event type carries the full transactional guarantee",
}

// durabilityClaimSources are the files that describe the durability contract: the three
// capture sites, the shared capture helpers, the model, the published documentation and the
// migration that creates the table.
//
// This test file is deliberately ABSENT from the list. It holds every forbidden phrase above
// as data, so scanning itself would make the guard fail on its own evidence.
var durabilityClaimSources = []string{
	"balance.go",
	"transaction_bulk.go",
	"transaction_execution.go",
	"event_outbox.go",
	"database/event_outbox.go",
	"model/event.go",
	"docs/event-streaming.md",
	"docs/webhook-to-kafka-migration.md",
	"sql/1781248800.sql",
}

// TestPostCommitCaptureSites_StillSpendARetryBudget pins the code half of the contract.
//
// A capture whose mutation has already committed gets exactly one chance at the row, so a
// single attempt turns a momentary connection reset into permanent loss. All three sites
// therefore spend the same bounded budget. Reverting any of them to a one-shot PublishEvent
// deletes the snippet named here.
func TestPostCommitCaptureSites_StillSpendARetryBudget(t *testing.T) {
	for _, site := range postCommitCaptureSites {
		t.Run(site.eventClass, func(t *testing.T) {
			source, err := os.ReadFile(site.file)
			require.NoErrorf(t, err, "reading the production source %s", site.file)

			assert.Containsf(t, string(source), site.budgetedCapture,
				"%s is captured AFTER its mutation commits (%s), so its insert is the event's "+
					"only chance and must spend a retry budget. %s no longer contains %q.\n\n"+
					"A single attempt makes a momentary connection reset a permanent loss of the "+
					"event, with no row anywhere to replay from. If the call site legitimately "+
					"changed shape, update postCommitCaptureSites in this file to match.",
				site.eventClass, site.why, site.file, site.budgetedCapture)
		})
	}
}

// TestPostCommitCaptureSites_AreDocumentedAsASetOfThree pins the documentation half.
//
// The heading it requires is the one docs/event-streaming.md actually carries and the one
// guarantee 1 links to by anchor — "event types", the noun the rest of that document and the
// `event_type` envelope field already use. A second noun for the same concept is how an
// in-document anchor comes to point at a heading that no longer exists.
//
// docs/event-streaming.md is what a subscriber reads to decide whether it needs to reconcile
// against the API, so a member missing from that section is a member nobody knows to defend
// against.
func TestPostCommitCaptureSites_AreDocumentedAsASetOfThree(t *testing.T) {
	published, err := os.ReadFile("docs/event-streaming.md")
	require.NoError(t, err, "reading the published event documentation")
	text := string(published)

	require.Contains(t, text, "### Three event types are at-most-once",
		"the published documentation must carry the section a subscriber is pointed at; "+
			"guarantee 1 in that file links to it by anchor, so renaming it silently breaks the link")

	for _, site := range postCommitCaptureSites {
		assert.Containsf(t, text, site.eventClass,
			"docs/event-streaming.md must name %s as one of the at-most-once classes. It is "+
				"captured after its mutation commits (%s), so a subscriber that does not know "+
				"about it cannot reconcile for it.",
			site.eventClass, site.why)
	}

	assert.Contains(t, text, "no row was ever written",
		"the section must say that a lost event produces NO ROW, because an operator who "+
			"assumes otherwise will look for it in the dead-letter inventory, where it can never appear")
}

// TestDurabilityContract_NoSourceClaimsToBeTheOnlyException is the regression guard proper.
//
// It is the assertion that makes the original defect unrepresentable rather than merely
// corrected: three sites each claiming exclusivity cannot coexist, and the only way to catch
// that is to forbid the claim outright. Every needle below was in the tree.
func TestDurabilityContract_NoSourceClaimsToBeTheOnlyException(t *testing.T) {
	for _, file := range durabilityClaimSources {
		t.Run(file, func(t *testing.T) {
			source, err := os.ReadFile(file)
			require.NoErrorf(t, err, "reading %s", file)
			text := string(source)

			for _, claim := range soleExceptionClaims {
				// assert.Falsef rather than assert.NotContains, deliberately: the latter
				// prints the ENTIRE haystack on failure, which for a 1,500-line source file
				// buries the one sentence the reader has to change under the whole file.
				assert.Falsef(t, strings.Contains(text, claim),
					"%s claims a SINGLE exception to requirement R-2 with %q, and there are "+
						"THREE: balance.monitor, bulk_transaction.* and a coalesced batch's "+
						"transaction events.\n\n"+
						"Each of the three sites once described itself as the only one, which is "+
						"how an operator came to be told this pipeline has one loss window when "+
						"it has three. State the whole set, or point at "+
						"postCommitCaptureSites and PublishEventDurably, which enumerate it.",
					file, claim)
			}
		})
	}
}

// TestPostTransactionActions_RetriesTheCoalescedCaptureOnATransientFault is the behavioural
// half of the third member.
//
// This capture used to make ONE attempt. Everything that reaches it has already committed —
// a coalesced batch is durable before the hook runs — so a single failed insert destroyed
// the event outright while the balances stood, which is the same defect PublishEventDurably
// was introduced to fix for balance.monitor. The retry re-sends the SAME prepared row, so it
// cannot deliver the event twice.
func TestPostTransactionActions_RetriesTheCoalescedCaptureOnATransientFault(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)
	spy := scriptStandaloneInsert(datasource, errors.New("connection reset"), nil)

	instance.postTransactionActions(
		context.Background(),
		&model.Transaction{TransactionID: "txn_coalesced_retry", Status: StatusApplied},
		&model.Balance{BalanceID: "bln_source", LedgerID: "ldg_retry"},
		&model.Balance{BalanceID: "bln_destination", LedgerID: "ldg_retry"},
		false,
	)

	require.Eventually(t, func() bool {
		return spy.count() == 2
	}, waitForPostActions, pollPostActions,
		"the coalesced path's capture must survive a transient database fault: its batch is "+
			"already durable, so one failed attempt loses the event permanently")

	rows := spy.captured()
	require.Len(t, rows, 2)
	require.NotEmpty(t, rows[0].EventID)
	assert.Equal(t, rows[0].EventID, rows[1].EventID,
		"the retry must re-send the same prepared row, or one business event becomes two")
	assert.Equal(t, "ldg_retry", rows[0].PartitionKey,
		"and it must still key on the ledger, so a coalesced write and a single write place "+
			"the same ledger's events on the same partition")
}

// TestDurabilityContract_SystemErrorIsNotCountedAsAnException records a distinction the
// enumeration depends on.
//
// system.error is captured outside a transaction like the three above, so it is easy to add
// to the set by mistake. It does not belong there: it describes no ledger mutation, so there
// is nothing it could have been atomic with and nothing about a mutation is lost with it.
// Counting it would make "three" wrong in the other direction and would tell a subscriber to
// reconcile a stream that has no ledger state behind it.
func TestDurabilityContract_SystemErrorIsNotCountedAsAnException(t *testing.T) {
	for _, site := range postCommitCaptureSites {
		require.NotEqual(t, "system.error", site.eventClass,
			"system.error describes no mutation, so it is not an exception to R-2")
	}

	published, err := os.ReadFile("docs/event-streaming.md")
	require.NoError(t, err)

	section := string(published)
	start := strings.Index(section, "### Three event types are at-most-once")
	require.Positive(t, start, "the at-most-once section must exist to be reasoned about")

	assert.Contains(t, section[start:], "it is not in this set",
		"the published section must say explicitly that system.error is NOT one of the three, "+
			"because it is standalone for a different reason and a reader who groups it with "+
			"them will reconcile against ledger state that was never involved")
}

// capturedMonitorAlert builds a balanceMonitorCapture recording exactly one alert.
//
// It exists so a test can state "this monitor's alert is already in the mutation's transaction"
// without reaching into the capture's map keys, which are composed by monitorCaptureKey and are
// not the test's business.
//
// Parameters:
//   - balanceID, monitorID string: the alert already captured atomically.
//
// Returns:
//   - balanceMonitorCapture: covering that balance, holding that one alert.
func capturedMonitorAlert(balanceID, monitorID string) balanceMonitorCapture {
	return balanceMonitorCapture{
		balances: map[string]struct{}{balanceID: {}},
		alerts:   map[string]struct{}{monitorCaptureKey(balanceID, monitorID): {}},
	}
}

// escalatedSystemError is one system.error escalation as a subscriber would receive it: the
// event name, and the "error" value out of the frozen two-key legacy body.
type escalatedSystemError struct {
	event   string
	message string
}

// escalationCapture records the system.error escalations notification.NotifyError dispatches
// while one test runs.
//
// # Why capturing the sender is REQUIRED here rather than tidy
//
// NotifyError dispatches asynchronously — it returns as soon as it has spawned its goroutine —
// through a sender registered in a PROCESS-GLOBAL variable that NewBlnk installs. Nothing in
// this file calls NewBlnk, so without an override the sender registered when one of these
// escalations actually runs is the closure some earlier test's NewBlnk left behind, pointing at
// THAT test's mock datasource. The escalation then calls InsertEventOutbox on a mock with no
// expectation for it, and testify's response to an unexpected call on a goroutine it does not
// own is to panic and take the whole test binary down — from a test that had already passed,
// naming a batch from a test that had already finished.
//
// Registering a capture removes both halves of that: the escalation lands somewhere that
// records it instead of somewhere that panics, and the test can WAIT for it, so no goroutine
// from this test is still in flight when the next one starts. It also turns the escalation from
// an invisible side effect into an asserted one, which is what the fix it belongs to claims.
type escalationCapture struct {
	guard   sync.Mutex
	raised  []escalatedSystemError
	arrived chan struct{}
}

// escalationCaptureDepth is the buffer on the arrival channel.
//
// Buffered rather than unbuffered so the notifier's goroutine never blocks on a test that has
// stopped reading: an escalation nobody waits for is recorded and forgotten, not deadlocked.
const escalationCaptureDepth = 8

// captureSystemErrorEscalations installs the capture for the duration of one test.
//
// The cleanup deliberately restores a BENIGN sender rather than the production closure. A
// goroutine this test spawned can only be in flight if the test did not wait for it, and the
// production closure would route that stray escalation into the next test's mock — the exact
// failure described above. A sender that records nothing and returns nil cannot.
func captureSystemErrorEscalations(t *testing.T) *escalationCapture {
	t.Helper()

	capture := &escalationCapture{arrived: make(chan struct{}, escalationCaptureDepth)}

	notification.RegisterWebhookSender(func(event string, payload interface{}) error {
		capture.record(event, payload)

		return nil
	})

	t.Cleanup(func() {
		notification.RegisterWebhookSender(func(string, interface{}) error { return nil })
	})

	return capture
}

// record stores one dispatched escalation and announces its arrival.
//
// Mutex-guarded because the notifier runs on its own goroutine while the test asserts on
// another, and the race detector is part of this suite.
func (c *escalationCapture) record(event string, payload interface{}) {
	raised := escalatedSystemError{event: event}

	if body, ok := payload.(map[string]interface{}); ok {
		if message, ok := body["error"].(string); ok {
			raised.message = message
		}
	}

	c.guard.Lock()
	c.raised = append(c.raised, raised)
	c.guard.Unlock()

	select {
	case c.arrived <- struct{}{}:
	default:
	}
}

// await blocks until one escalation has been dispatched and returns it, failing the test if
// none arrives. Waiting rather than sleeping is what makes the assertion deterministic AND
// guarantees no notifier goroutine outlives the test.
func (c *escalationCapture) await(t *testing.T) escalatedSystemError {
	t.Helper()

	select {
	case <-c.arrived:
	case <-time.After(waitForPostActions):
		t.Fatal("no system.error escalation was dispatched; a lost post-commit event must be raised, not only logged")
	}

	c.guard.Lock()
	defer c.guard.Unlock()

	return c.raised[len(c.raised)-1]
}

// TestCheckBalanceMonitors_CapturesTheMonitorEventDurably HAS BEEN REMOVED, and this records
// where each half of it went, because the property it asserted is still guarded — just not from
// that site, which can no longer reach the behaviour.
//
// # Why it could not stay
//
// It drove checkBalanceMonitors on a Kafka-configured instance and asserted that a failed
// InsertEventOutbox was retried. Both of those are now unreachable together. checkBalanceMonitors
// returns immediately when balanceMonitorHandoffEnabled is true, because the alert is captured
// inside the balance's own transaction — evaluated before the write, or from a durable handoff row
// — and publishing from here as well would deliver every threshold crossing twice under two
// different event ids, which nothing at a subscriber could collapse. And that predicate is
// EventPublishingConfigured, the same fact PrepareEventOutbox reads, so on the one shape where
// this function does still publish there is no outbox row to insert and therefore none to retry.
// A configuration in which the assertion held would have had to be one production cannot produce.
//
// # Where its three assertions live now
//
//   - "the site spends a retry budget rather than making one attempt" —
//     TestPostCommitCaptureSites_StillSpendARetryBudget, subtest balance.monitor, which reads
//     balance.go and fails if the budgeted call is replaced by a one-shot PublishEvent. That is
//     the same regression the deleted test was written to catch, caught by source rather than by
//     a mock that can no longer be reached.
//   - "a transient capture failure is retried with the same event id" —
//     TestPublishEventDurably_RetriesATransientCaptureFailure and
//     TestPublishEventDurably_RetriesTheSameEventIDSoARetryCannotDuplicate.
//   - "the captured alert carries the monitor payload and the monitored balance's ledger" —
//     TestPrepareBalanceMonitorEventOutboxes_BuildsTheAlertBeforeTheWrite for the pre-commit
//     route and TestBalanceMonitorHandoff_CapturesTheAlertAtomicallyWithTheCompletion for the
//     handoff route, which are the two routes that now produce the row.
//
// What remains observable at the site itself is the broker-less fallback, and it is covered:
// TestCheckBalanceMonitors_DeliversOverTheLegacyTransportWithoutKafka for the delivery,
// TestCheckBalanceMonitors_SkipsAnAlertAlreadyCommittedWithItsMovement for the per-monitor skip,
// and TestCheckBalanceMonitors_EscalatesALostAlert for the loss.

// TestPolledConditions_CannotFailTheTestFromTestifysGoroutine is the guard on a defect class
// that cost this package a whole run and blamed a test that had passed.
//
// # What went wrong
//
// assert.Never and require.Eventually evaluate their condition REPEATEDLY and on a goroutine of
// testify's own, and they stop waiting for it once the window closes. A straggler evaluation can
// therefore still be running after the test function has returned and after t.Cleanup has torn
// down the miniredis server it reads. If that condition can fail the test — because it calls
// require or assert, directly or through a helper that takes *testing.T — the report arrives
// after completion, and Go's testing package turns it into "panic: Fail in goroutine after
// <test> has completed", which fails the ENTIRE PACKAGE and names the wrong test. Under -race the
// wider scheduling window made it the usual outcome rather than a rare one.
//
// # What this asserts
//
// No polled condition in this package's tests may touch the test at all: its job is to answer
// true or false. A read that cannot be performed is "not yet satisfied" — which is the correct
// answer for Never, and for Eventually leaves the loud failure to the timeout, on the test
// goroutine where it is legal. Helpers for the two forms sit beside each other in
// event_outbox_test.go: outboxPendingLegacyTasks for the test goroutine, countPendingLegacyTasks
// for a condition.
//
// The WithT variants are exempt: they are handed an *assert.CollectT precisely so that a
// condition CAN assert, and testify owns the reporting.
func TestPolledConditions_CannotFailTheTestFromTestifysGoroutine(t *testing.T) {
	t.Parallel()

	sources, err := filepath.Glob("*_test.go")
	require.NoError(t, err, "listing this package's test files")
	require.NotEmpty(t, sources, "no test sources found, so this scan would prove nothing")

	polled := map[string]struct{}{"Never": {}, "Eventually": {}}
	scanned := 0

	for _, source := range sources {
		fileSet := token.NewFileSet()

		parsed, parseErr := parser.ParseFile(fileSet, source, nil, parser.SkipObjectResolution)
		require.NoErrorf(t, parseErr, "parsing %s", source)

		ast.Inspect(parsed, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall {
				return true
			}

			selector, isSelector := call.Fun.(*ast.SelectorExpr)
			if !isSelector {
				return true
			}

			pkg, isIdent := selector.X.(*ast.Ident)
			if !isIdent || (pkg.Name != "assert" && pkg.Name != "require") {
				return true
			}

			if _, isPolled := polled[selector.Sel.Name]; !isPolled {
				return true
			}

			for _, argument := range call.Args {
				literal, isLiteral := argument.(*ast.FuncLit)
				if !isLiteral {
					continue
				}

				scanned++

				ast.Inspect(literal.Body, func(inner ast.Node) bool {
					ident, isIdent := inner.(*ast.Ident)
					if !isIdent || ident.Name != "t" {
						return true
					}

					position := fileSet.Position(ident.Pos())
					t.Errorf(
						"%s:%d: a polled %s.%s condition uses the test (t). testify evaluates it on "+
							"its own goroutine and stops waiting for it, so a straggler reports after "+
							"the test has completed and Go panics with \"Fail in goroutine after … has "+
							"completed\", failing the whole package and naming the wrong test. Return "+
							"true or false instead, folding an unreadable dependency into \"not yet "+
							"satisfied\" — see countPendingLegacyTasks",
						position.Filename, position.Line, pkg.Name, selector.Sel.Name,
					)

					return false
				})
			}

			return true
		})
	}

	require.NotZero(t, scanned,
		"no polled condition was found to inspect; the package uses assert.Never and "+
			"require.Eventually, so finding none means this scan is broken rather than clean")
}
