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
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// event_commit_contract_test.go pins WHAT A COMMITTED INFLIGHT TRANSACTION IS ANNOUNCED
// AS, which is a different question from what the status-to-event mapping returns for the
// COMMIT status.
//
// The two were conflated: the catalogue and the documentation both said a committed
// inflight transaction is announced as transaction.unknown, on the strength of COMMIT
// having no case in the mapping. It is not, and never was — over Kafka or over the HTTP
// webhook before it. updateTransactionDetails normalises the status to APPLIED before the
// event name is derived from it, so the name a subscriber receives is
// transaction.applied. These tests hold that fact down so the catalogue cannot drift back
// to the claim, and so a future change that removes the normalisation is a visible one.

// TestCommittedInflightTransaction_IsAnnouncedAsApplied is the real contract.
func TestCommittedInflightTransaction_IsAnnouncedAsApplied(t *testing.T) {
	l := &Blnk{}
	ctx := context.Background()

	source := &model.Balance{BalanceID: "bln_source", LedgerID: "ldg_one"}
	destination := &model.Balance{BalanceID: "bln_destination", LedgerID: "ldg_one"}

	normalised := l.updateTransactionDetails(ctx,
		&model.Transaction{TransactionID: "txn_commit", Status: StatusCommit},
		source, destination)

	require.NotNil(t, normalised)
	assert.Equal(t, StatusApplied, normalised.Status,
		"COMMIT is normalised to APPLIED before anything derives an event name from it. This is "+
			"the pre-Kafka behaviour of the HTTP webhook, preserved so both transports carry the "+
			"same payload during the dual-delivery window")

	assert.Equal(t, model.EventTypeTransactionApplied, getEventFromStatus(normalised.Status),
		"so a committed inflight transaction is announced transaction.applied. The catalogue "+
			"claimed transaction.unknown; that claim was wrong and must not come back")
	assert.NotEqual(t, model.EventTypeTransactionUnknown, getEventFromStatus(normalised.Status),
		"transaction.unknown must not be reachable through the COMMIT path")
}

// TestVoidRemainsVoidThroughNormalisation guards the other arm of the same map, because a
// normalisation that collapsed VOID onto APPLIED would announce a released hold as a
// settlement.
func TestVoidRemainsVoidThroughNormalisation(t *testing.T) {
	l := &Blnk{}

	normalised := l.updateTransactionDetails(context.Background(),
		&model.Transaction{TransactionID: "txn_void", Status: StatusVoid},
		&model.Balance{BalanceID: "bln_source"}, &model.Balance{BalanceID: "bln_destination"})

	require.NotNil(t, normalised)
	assert.Equal(t, StatusVoid, normalised.Status)
	assert.Equal(t, model.EventTypeTransactionVoid, getEventFromStatus(normalised.Status))
}

// TestEventStreamingDoc_DoesNotClaimCommitProducesUnknown keeps the published document
// honest about the same fact.
//
// A subscriber builds its routing from that document, and a consumer written to wait for
// transaction.unknown on a commit would wait for ever.
func TestEventStreamingDoc_DoesNotClaimCommitProducesUnknown(t *testing.T) {
	published, err := os.ReadFile("docs/event-streaming.md")
	require.NoError(t, err, "docs/event-streaming.md is the subscriber-facing contract")

	doc := string(published)

	assert.Contains(t, doc, "a committed inflight transaction is announced today as `transaction.applied`",
		"the document must state the real outcome of a commit, not the mapping's default arm")
	assert.Contains(t, doc, "`transaction.unknown` is a defensive default",
		"and it must say plainly that transaction.unknown is a default nothing produces")
	assert.NotContains(t, doc, "Three names in the vocabulary are not currently reachable",
		"transaction.queued and transaction.scheduled ARE produced now, so the section that "+
			"grouped them with transaction.unknown is false and must be gone")
}

// TestEventStreamingDoc_DocumentsTheQueuedAndScheduledEvents is the other half: the two
// names are produced now, so the document has to tell a subscriber what they mean and how
// they relate to the applied event that follows.
func TestEventStreamingDoc_DocumentsTheQueuedAndScheduledEvents(t *testing.T) {
	published, err := os.ReadFile("docs/event-streaming.md")
	require.NoError(t, err)

	doc := string(published)

	assert.Contains(t, doc, "A queued transaction announces itself twice, under two ids",
		"one submission produces a queued event for the accepted parent and an applied event "+
			"for the executed copy, under two different aggregate ids. A subscriber that does not "+
			"know that reads the second as a duplicate of the first")

	for _, promise := range []string{
		"`transaction.queued` is not a settlement.",
		"payload.data.parent_transaction",
		"produces only `transaction.applied`",
	} {
		assert.Containsf(t, doc, promise,
			"the queued-event section must carry %q; without it a consumer cannot correlate the "+
				"two events or tell which one moved money", promise)
	}

	// And the table rows must no longer say the two names are not produced.
	for _, name := range []string{"transaction.queued", "transaction.scheduled"} {
		for _, line := range strings.Split(doc, "\n") {
			if !strings.HasPrefix(line, "| `"+name+"`") {
				continue
			}

			assert.NotContainsf(t, line, "Not produced today",
				"the catalogue row for %q must not still say it is not produced", name)
		}
	}
}
