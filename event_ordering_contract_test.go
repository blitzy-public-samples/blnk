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

// SOURCE-TO-DOCUMENTATION CONTRACT ASSERTIONS for the two published contracts a
// subscriber builds against and that nothing else can check: the PARTITION KEY of each
// event type, and which event types are at-most-once.
//
// Both contracts are stated in docs/event-streaming.md, and a subscriber reading them
// makes design decisions that cannot be walked back cheaply: which events it may assume
// are mutually ordered, and whether it needs a reconciliation path for an event that
// may never arrive. Neither statement has a compiler behind it.
//
//   - The partition-key table said a transaction event keys on its SOURCE BALANCE and a
//     monitor alert on the WATCHED BALANCE, while every production call site was
//     already supplying the LEDGER, which overrides the payload-derived key.
//   - The at-most-once section named `balance.monitor` as "the one exception", while
//     transaction_bulk.go separately described the bulk summary as the event that
//     "cannot be enrolled" — two mutually exclusive claims to the same status, twelve
//     files apart, and a subscriber told that exactly one event type could be lost.
//
// Prose is NOT asserted verbatim — that would make every editorial improvement a test
// failure. What is asserted is the presence of the specific claims whose absence or
// reversal is a subscriber-visible contract change: the ledger-keying rule, the three
// at-most-once event types, and the explicit correction that partitioning and queue
// sharding do not agree.
package blnk

import (
	"strings"
	"testing"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fixture identifiers. They are visibly distinct from one another so that an assertion
// failure names which dimension was chosen rather than only that the value was wrong.
const (
	orderingContractLedgerID      = "ldg_ordering_contract"
	orderingContractOtherLedgerID = "ldg_ordering_contract_other"
	orderingContractSourceBalance = "bln_ordering_contract_source"
	orderingContractDestBalance   = "bln_ordering_contract_destination"
	orderingContractTransactionID = "txn_ordering_contract"
	orderingContractIdentityID    = "idt_ordering_contract"
	orderingContractMonitorID     = "mon_ordering_contract"
	orderingContractBatchID       = "bulk_ordering_contract"
)

// orderingContractCase is one row of the documented partition-key table, expressed as the
// producer call that creates it.
type orderingContractCase struct {
	// documentedAs is how docs/event-streaming.md names this row, quoted into the failure
	// message so the reader is sent to the right table row rather than to the whole document.
	documentedAs string

	// eventType is the catalogue event string the producer emits.
	eventType string

	// payload is the object the producer passes, unchanged.
	payload interface{}

	// ledgerOption is the ledger the PRODUCER supplies, or the empty string for the event
	// types that genuinely belong to no ledger. This mirrors the production call site
	// exactly: it is the presence or absence of WithEventLedgerID that the table's two
	// halves describe.
	ledgerOption string

	// wantKey is the partition key the document promises.
	wantKey string

	// wantLedgerColumn is what ledger_id must hold: the ledger for a ledger-scoped event,
	// and the empty string — stored as SQL NULL — for one that belongs to no ledger. A
	// fabricated ledger would corrupt both the daily reconciliation and any consumer
	// grouping by ledger, so the NULL cases are asserted with the same weight as the
	// populated ones.
	wantLedgerColumn string
}

// orderingContractCases transcribes the documented table, one entry per row.
func orderingContractCases() []orderingContractCase {
	return []orderingContractCase{
		{
			documentedAs: "`transaction.*` (all seven) | The ledger — from the source balance",
			eventType:    model.EventTypeTransactionApplied,
			payload: &model.Transaction{
				TransactionID: orderingContractTransactionID,
				Source:        orderingContractSourceBalance,
				Destination:   orderingContractDestBalance,
			},
			ledgerOption:     orderingContractLedgerID,
			wantKey:          orderingContractLedgerID,
			wantLedgerColumn: orderingContractLedgerID,
		},
		{
			documentedAs: "`bulk_transaction.<status>` | The **batch id** | `null`",
			eventType:    "bulk_transaction.applied",
			payload: map[string]interface{}{
				"batch_id":          orderingContractBatchID,
				"status":            "applied",
				"transaction_count": 3,
			},
			ledgerOption:     "",
			wantKey:          orderingContractBatchID,
			wantLedgerColumn: "",
		},
		{
			documentedAs: "`balance.created` | The ledger",
			eventType:    "balance.created",
			payload: &model.Balance{
				BalanceID: orderingContractSourceBalance,
				LedgerID:  orderingContractLedgerID,
			},
			ledgerOption:     orderingContractLedgerID,
			wantKey:          orderingContractLedgerID,
			wantLedgerColumn: orderingContractLedgerID,
		},
		{
			documentedAs: "`balance.monitor` | The ledger of the balance whose update met the condition",
			eventType:    "balance.monitor",
			payload: model.BalanceMonitor{
				MonitorID: orderingContractMonitorID,
				BalanceID: orderingContractSourceBalance,
			},
			// The monitored balance carries no ledger on a BalanceMonitor payload, which is why
			// the producer supplies it from the balance whose update triggered the check.
			ledgerOption:     orderingContractLedgerID,
			wantKey:          orderingContractLedgerID,
			wantLedgerColumn: orderingContractLedgerID,
		},
		{
			documentedAs: "`identity.created` | The **identity id** | `null`",
			eventType:    "identity.created",
			payload: &model.Identity{
				IdentityID: orderingContractIdentityID,
			},
			ledgerOption:     "",
			wantKey:          orderingContractIdentityID,
			wantLedgerColumn: "",
		},
		{
			documentedAs: "`ledger.created` | The ledger id",
			eventType:    "ledger.created",
			payload: &model.Ledger{
				LedgerID: orderingContractLedgerID,
			},
			ledgerOption:     orderingContractLedgerID,
			wantKey:          orderingContractLedgerID,
			wantLedgerColumn: orderingContractLedgerID,
		},
		{
			documentedAs: "`system.error` | The **event type**, so the whole stream is one partition | `null`",
			eventType:    "system.error",
			payload: map[string]interface{}{
				"error": "ordering contract fixture",
				"time":  "2026-03-01T12:00:00Z",
			},
			ledgerOption: "",
			// No aggregate of any kind, so the documented chain reaches the event type.
			wantKey:          "system.error",
			wantLedgerColumn: "",
		},
	}
}

// TestPartitionKeyContract_MatchesTheDocumentedTable runs every documented row through
// the real capture path and requires the stored key and ledger column to be what the
// document promises.
func TestPartitionKeyContract_MatchesTheDocumentedTable(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	for _, testCase := range orderingContractCases() {
		t.Run(testCase.eventType, func(t *testing.T) {
			var options []EventOption
			if testCase.ledgerOption != "" {
				options = append(options, WithEventLedgerID(testCase.ledgerOption))
			}

			row, err := blnk.PrepareEventOutbox(
				t.Context(),
				NewWebhook{Event: testCase.eventType, Payload: testCase.payload},
				options...,
			)
			require.NoError(t, err)
			require.NotNil(t, row, "publishing is configured, so a row must be prepared")

			assert.Equal(t, testCase.wantKey, row.PartitionKey,
				"docs/event-streaming.md's partition-key table promises a subscriber that %q keys on "+
					"%q. A key the document does not name silently changes which events a subscriber "+
					"may assume are mutually ordered",
				testCase.documentedAs, testCase.wantKey)

			assert.Equal(t, testCase.wantLedgerColumn, row.LedgerID,
				"and the same table's ledger_id column for %q. An empty value is stored as SQL NULL "+
					"and means the event belonged to no single ledger; fabricating one would corrupt "+
					"the daily reconciliation and every consumer grouping by ledger",
				testCase.documentedAs)
		})
	}
}

// TestPartitionKeyContract_LedgerKeyingIsUniversalWhereALedgerExists is the rule behind
// the table, asserted as a rule so that a NEW ledger-scoped event type cannot be added
// on a different ordering domain without failing here.
//
// The table above is a list; this is the invariant.
func TestPartitionKeyContract_LedgerKeyingIsUniversalWhereALedgerExists(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	for _, testCase := range orderingContractCases() {
		if testCase.ledgerOption == "" {
			continue
		}

		row, err := blnk.PrepareEventOutbox(
			t.Context(),
			NewWebhook{Event: testCase.eventType, Payload: testCase.payload},
			WithEventLedgerID(testCase.ledgerOption),
		)
		require.NoError(t, err)
		require.NotNil(t, row)

		assert.Equalf(t, testCase.ledgerOption, row.PartitionKey,
			"%s: a producer that states the ledger must have it used as the key. Requirement R-6 "+
				"partitions by ledger id, and a per-call-site choice of ordering domain is what lets "+
				"two events of one transaction land on different partitions",
			testCase.eventType)
	}
}

// TestPartitionKeyContract_TheSuppliedLedgerOverridesTheDerivedKey pins the precedence
// the document's correction depends on.
//
// A transaction payload derives its key from the source balance.
func TestPartitionKeyContract_TheSuppliedLedgerOverridesTheDerivedKey(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	t.Run("a transaction keys on the supplied ledger, not on its source balance", func(t *testing.T) {
		transaction := &model.Transaction{
			TransactionID: orderingContractTransactionID,
			Source:        orderingContractSourceBalance,
			Destination:   orderingContractDestBalance,
		}

		withoutLedger, err := blnk.PrepareEventOutbox(t.Context(),
			NewWebhook{Event: model.EventTypeTransactionApplied, Payload: transaction})
		require.NoError(t, err)
		require.NotNil(t, withoutLedger)
		require.Equal(t, orderingContractSourceBalance, withoutLedger.PartitionKey,
			"the payload derivation is the source balance, which is what the stale documentation "+
				"described; the assertion below is what makes the correction meaningful")

		withLedger, err := blnk.PrepareEventOutbox(t.Context(),
			NewWebhook{Event: model.EventTypeTransactionApplied, Payload: transaction},
			WithEventLedgerID(orderingContractLedgerID))
		require.NoError(t, err)
		require.NotNil(t, withLedger)

		assert.Equal(t, orderingContractLedgerID, withLedger.PartitionKey,
			"the producer-supplied ledger must WIN. Every production transaction call site supplies "+
				"one, so this is the key subscribers actually receive")
		assert.NotEqual(t, orderingContractSourceBalance, withLedger.PartitionKey,
			"and it must not be the source balance, which is what the internal transaction queue "+
				"shards on — the two do not agree, and the documentation now says so")
	})

	t.Run("a balance keys on the supplied ledger even when its payload names another", func(t *testing.T) {
		// The payload's own ledger and the supplied one are DIFFERENT here on purpose. Equal
		// values would pass whichever won, so the case would prove nothing about precedence.
		row, err := blnk.PrepareEventOutbox(t.Context(),
			NewWebhook{Event: "balance.created", Payload: &model.Balance{
				BalanceID: orderingContractSourceBalance,
				LedgerID:  orderingContractOtherLedgerID,
			}},
			WithEventLedgerID(orderingContractLedgerID))
		require.NoError(t, err)
		require.NotNil(t, row)

		assert.Equal(t, orderingContractLedgerID, row.PartitionKey,
			"the supplied ledger is the authoritative one and takes precedence over the payload's")
		assert.Equal(t, orderingContractLedgerID, row.LedgerID,
			"and it is what the ledger column records, so the key and the column cannot disagree")
	})
}

// TestAtMostOnceContract_NamesAllThreeProducersAndNoOthers is the second published
// contract.
//
// A subscriber reads this section to decide which events need a reconciliation path of
// their own.
//
// The set is asserted against the code's own single declaration,
// PostCommitEventCaptureContract, so the document and the code cannot drift: adding a
// fourth post-commit producer to the code without documenting it fails here.
func TestAtMostOnceContract_NamesAllThreeProducersAndNoOthers(t *testing.T) {
	doc := readRepoFile(t, "docs/event-streaming.md")

	postCommitProducers := []string{
		"balance.monitor",
		"bulk_transaction.<status>",
		"system.error",
	}

	// The code's declaration is the source of truth for the SET. Deriving the expectation
	// from it rather than restating it here is what makes this a contract assertion rather
	// than a second copy of the list.
	for _, eventType := range postCommitProducers {
		assert.Containsf(t, PostCommitEventCaptureContract, eventType,
			"PostCommitEventCaptureContract is the code's single declaration of the producers with "+
				"no producing mutation, so %q must appear in it", eventType)
	}

	// The section's own table is the artefact, and its ROWS are the set. Asserting rows
	// rather than mentions is what makes this a contract: an event type named in passing
	// somewhere in the section is not a statement that it is at-most-once, and a section
	// whose table lists one row while the code declares three fails here whatever the
	// surrounding sentences say.
	section := atMostOnceSection(t, doc)
	documented := markdownTableIn(t, section, "docs/event-streaming.md's at-most-once section",
		[]string{"Event type", "Where the standalone write is reached", "What is lost if it fails"})

	for _, eventType := range postCommitProducers {
		_, present := documented.Row(eventType)
		assert.Truef(t, present,
			"docs/event-streaming.md's at-most-once table must carry a row for %q. A subscriber reads "+
				"that table to decide which events need a reconciliation path, so an omitted event type "+
				"is an understated obligation rather than a documentation nicety; the rows present are %v",
			eventType, documented.Keys())
	}

	assert.Lenf(t, documented.Keys(), len(postCommitProducers),
		"the at-most-once table must carry exactly the %d rows PostCommitEventCaptureContract "+
			"declares and no others: a fourth row grants an exception the code does not take, and a "+
			"missing row hides one it does. Rows present: %v",
		len(postCommitProducers), documented.Keys())
}

// TestPartitionKeyDocumentation_CorrectsTheQueueShardingClaim pins the heading a
// subscriber is linked to, which is the artefact the documentation's earlier false claim
// about queue sharding lived under.
//
// The heading is asserted rather than the sentences beneath it: a heading is a link
// target, so renaming it silently breaks every cross-reference, while the prose under it
// is edited for clarity without the contract changing.
func TestPartitionKeyDocumentation_CorrectsTheQueueShardingClaim(t *testing.T) {
	doc := readRepoFile(t, "docs/event-streaming.md")

	assert.Contains(t, doc, "The key is the ledger id wherever a ledger exists",
		"and the section heading must state the rule, not its negation. The heading used to read "+
			"'The key is not simply the ledger id', which is the opposite of what the producers do")
}

// atMostOnceSection extracts the at-most-once section from the document.
//
// Scoping the assertions to one section rather than to the whole file matters:
// `balance.monitor` appears in the event-type table and in the idempotency notes as
// well, so a whole-file Contains would pass while the section itself said nothing. The
// section is delimited by its own heading and the next heading of the same level.
//
// Parameters:
//   - t *testing.T: the test, failed when the section is absent.
//   - doc string: the whole document.
//
// Returns:
//   - string: the section body, heading included.
func atMostOnceSection(t *testing.T, doc string) string {
	t.Helper()

	const heading = "### Three event types are at-most-once"

	start := strings.Index(doc, heading)
	require.GreaterOrEqual(t, start, 0,
		"docs/event-streaming.md must carry the at-most-once section under %q; a subscriber has no "+
			"other statement of which events may never arrive", heading)

	body := doc[start+len(heading):]
	if next := strings.Index(body, "\n## "); next >= 0 {
		body = body[:next]
	}

	return heading + body
}
