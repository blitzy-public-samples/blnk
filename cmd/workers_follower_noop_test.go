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

// AN ALREADY-APPLIED TRANSACTION IS AN EXPECTED OUTCOME, NOT A SYSTEM ERROR.
//
// processTransaction used to answer a duplicate-reference failure by calling
// notification.NotifyError. That call writes one operator-facing ERROR line per occurrence
// and publishes a system.error event — and since events became outbox-backed, publishing
// one means inserting a row into blnk.event_outbox for the relay to carry. So every
// arrival on that branch cost an incident record and a unit of load on the same event
// pipeline the ledger's own events flow through.
//
// The branch is not rare. It is reached whenever work has already been done:
//
//   - Coalescing. A leader gathers its queued siblings and commits all of them together.
//     Each follower still has its own task, and every one of them then arrives here to find
//     its transaction already written. One batch of 2,000 children produces up to 2,000
//     arrivals.
//   - At-least-once delivery. asynq redelivers a task whose handler had already committed,
//     after a worker restart, a lost acknowledgement or an expired lease.
//
// A load run measured the consequence: 1,763 error lines and 1,366 pending system.error
// events, none of which described anything that had gone wrong.
//
// Replaying one task is the faithful, deterministic reproduction of both causes — it is
// literally what redelivery does, and it puts the handler in exactly the position a
// follower is in.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/notification"
)

// captureSystemErrorEvents routes system-error notifications into a channel for the
// duration of a test, and guarantees NotifyError's gate is open so that a missing event
// means the code did not emit one — rather than that nothing could have been emitted.
func captureSystemErrorEvents(t *testing.T) <-chan string {
	t.Helper()

	current, err := config.Fetch()
	require.NoError(t, err, "the suite's configuration must be loaded")

	// NotifyError reads config.Fetch(), not the instance, and emits only when a sender is
	// registered AND a delivery target is configured. A copy is published so the webhook
	// target can be guaranteed without disturbing the DataSource and Redis settings the
	// rest of the harness depends on.
	restore := *current
	withTarget := *current
	withTarget.Notification.Webhook.Url = "http://system-error.invalid/hook"
	config.MockConfig(&withTarget)

	events := make(chan string, 64)
	notification.RegisterWebhookSender(func(event string, _ interface{}) error {
		events <- event

		return nil
	})

	t.Cleanup(func() {
		notification.RegisterWebhookSender(nil)
		config.MockConfig(&restore)
	})

	return events
}

func TestProcessTransaction_AlreadyAppliedIsAcknowledgedWithoutASystemError(t *testing.T) {
	b := newCmdTestInstance(t)
	events := captureSystemErrorEvents(t)

	src, dst := createBalancePair(t, b)

	txn := queuedTransaction(src.BalanceID, dst.BalanceID, 50, true)
	payload, err := json.Marshal(txn)
	require.NoError(t, err)

	task := asynq.NewTask("transaction_queue_cmd_test_1", payload)

	// First delivery: ordinary success.
	require.NoError(t, b.processTransaction(context.Background(), task))

	applied, err := b.blnk.GetDataSource().GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	require.Equal(t, "5000", applied.Balance.String(),
		"the first delivery must actually apply the transaction")

	// Drain anything the successful path legitimately produced, so what follows is
	// attributable to the replay alone.
	drainEvents(events)

	// SECOND DELIVERY OF THE SAME TASK. This is what asynq redelivery does, and the
	// position every coalesced follower is in.
	require.NoError(t, b.processTransaction(context.Background(), task),
		"an already-applied transaction must be ACKNOWLEDGED: returning an error here would "+
			"make asynq retry a task that can never succeed differently")

	// The money must not move twice.
	after, err := b.blnk.GetDataSource().GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "5000", after.Balance.String(),
		"the replay must not credit the destination a second time")

	// And no incident may be raised. NotifyError dispatches on its own goroutine, so the
	// window is generous enough that a real emission would be seen.
	select {
	case event := <-events:
		t.Fatalf("an already-applied transaction raised a %q event. That is what produced "+
			"1,366 pending system.error events and 1,763 error lines under a coalescing load: "+
			"routine deduplication reported as a system fault, and charged to the event "+
			"pipeline as outbox rows.", event)
	case <-time.After(750 * time.Millisecond):
	}

	// POSITIVE CONTROL. The assertion above is an assertion of ABSENCE, which is only worth
	// anything if this capture channel can actually observe an emission. Proving it here, in
	// the same test and the same configuration, is what stops that assertion being vacuous.
	notification.NotifyError(errors.New("positive control: a genuine system error"))

	select {
	case event := <-events:
		assert.Equal(t, "system.error", event,
			"the capture channel must observe a real system error, or the absence asserted above "+
				"proves nothing")
	case <-time.After(3 * time.Second):
		t.Fatal("the positive control did not arrive, so this test cannot distinguish 'no event " +
			"was emitted' from 'no event could have been observed'")
	}
}

// drainEvents empties whatever is currently buffered without blocking.
func drainEvents(events <-chan string) {
	for {
		select {
		case <-events:
		default:
			return
		}
	}
}
