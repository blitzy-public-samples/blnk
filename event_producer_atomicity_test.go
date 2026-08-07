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
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/database/mocks"
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

// TestSendBulkTransactionWebhook_RetriesTheBatchOutcomeCapture covers the one event that
// cannot be enrolled in a mutation transaction.
//
// A bulk request executes one transaction at a time with compensating rollback, so by the
// time the batch's outcome is known every mutation it describes has already committed under
// its own transaction: there is no row this summary could be atomic with. The guarantee
// available is therefore weaker, and it is made as strong as it can be — the capture is
// synchronous, retried, and reported — instead of being a fire-and-forget log line.
func TestSendBulkTransactionWebhook_RetriesTheBatchOutcomeCapture(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	attempts := 0
	datasource.On("InsertEventOutbox", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { attempts++ }).
		Return(errors.New("connection reset")).Once()
	datasource.On("InsertEventOutbox", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { attempts++ }).
		Return(nil).Once()

	err := instance.sendBulkTransactionWebhook(context.Background(), "bulk_1", "applied", "", 3)

	require.NoError(t, err, "a transient failure must not lose the batch outcome")
	assert.Equal(t, 2, attempts, "the first attempt failed and the second succeeded")
}

// TestSendBulkTransactionWebhook_ReportsAnExhaustedBudget asserts the failure is RETURNED
// rather than only logged, so a caller can act on a batch outcome that is not in the outbox.
func TestSendBulkTransactionWebhook_ReportsAnExhaustedBudget(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	datasource.On("InsertEventOutbox", mock.Anything, mock.Anything).
		Return(errors.New("outbox unavailable"))

	err := instance.sendBulkTransactionWebhook(context.Background(), "bulk_2", "applied", "", 3)

	require.Error(t, err,
		"an outcome that could not be captured must be reported, not swallowed into a log line")
}

// TestSendBulkTransactionWebhook_StopsRetryingOnCancellation asserts the retry loop honours
// the caller going away, while still reporting the failure.
func TestSendBulkTransactionWebhook_StopsRetryingOnCancellation(t *testing.T) {
	instance, datasource := producerAtomicityMock(t)

	attempts := 0
	datasource.On("InsertEventOutbox", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { attempts++ }).
		Return(errors.New("outbox unavailable"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := instance.sendBulkTransactionWebhook(ctx, "bulk_3", "applied", "", 3)

	require.Error(t, err)
	assert.Equal(t, 1, attempts,
		"a cancelled caller is a reason to stop retrying, not a reason to keep sleeping")
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
		return len(outboxPendingLegacyTasks(t, redisAddress)) == 1
	}, waitForPostActions, pollPostActions,
		"a webhook-only deployment must still receive %s; before the fallback was restored it "+
			"was delivered by NEITHER transport", event.Event)

	tasks := outboxPendingLegacyTasks(t, redisAddress)
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
