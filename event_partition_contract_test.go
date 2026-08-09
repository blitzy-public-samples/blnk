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
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The partition-key contract, asserted from the PRODUCTION CALL SITES against the PUBLISHED
// DOCUMENTATION.
//
// # Why this file exists (finding F-04)
//
// Requirement R-6 partitions by ledger id, and production does: every producer that performs a
// ledger-scoped mutation passes WithEventLedgerID, which overrides both the recorded ledger and
// the partition key. The subscriber-facing documentation said something else — that a transaction
// is keyed on its SOURCE BALANCE — and that is not a cosmetic error. The partition key IS the
// ordering and parallelism contract a consumer designs around: a subscriber told it would see
// per-balance ordering, sizing its consumer group against a balance-count fan-out, gets
// per-ledger ordering through however few partitions its ledgers hash to.
//
// The two statements cannot be kept in step by review alone, because nothing links them. This
// file links them: it derives the key from the production producers and asserts the documentation
// says the same thing, so a change to either without the other fails here.
//
// # Why the key is derived rather than restated
//
// A test that hard-coded "the key is the ledger" would pass against a producer that stopped
// supplying the option, because it would only be checking its own literal. Every case below
// therefore runs the real preparation path — l.PrepareEventOutbox through the real
// eventPartitionKey and the real option application — and reads PartitionKey off the row that
// would have been inserted.

// partitionContractBlnk returns an instance wired for event preparation only.
//
// Preparation performs NO I/O: it marshals the payload, derives the key and returns a row. So a
// configuration and nothing else is sufficient, and deliberately so — a datasource here would
// let a test accidentally assert something about persistence instead of about the key.
func partitionContractBlnk(t *testing.T) *Blnk {
	t.Helper()

	return newOutboxBlnk(t, outboxPublishingConfiguration(), nil)
}

// preparedKey runs the production preparation path and returns the row's partition key.
func preparedKey(t *testing.T, instance *Blnk, event NewWebhook, options ...EventOption) string {
	t.Helper()

	row, err := instance.PrepareEventOutbox(context.Background(), event, options...)
	require.NoError(t, err, "preparing the %s event", event.Event)
	require.NotNil(t, row, "event publishing is configured, so a row must be produced")

	return row.PartitionKey
}

// TestPartitionKeyContract_ProductionCallSitesKeyOnTheLedger asserts the key every production
// producer actually produces, one event family at a time.
//
// The three families that CANNOT be keyed on a ledger are asserted too, because "the ledger
// wherever a ledger exists" is only a contract if the exceptions are enumerated. An
// unenumerated exception is how a consumer ends up designing around a rule that silently does
// not hold for one of its topics.
func TestPartitionKeyContract_ProductionCallSitesKeyOnTheLedger(t *testing.T) {
	instance := partitionContractBlnk(t)

	const ledgerID = "ldg_f04c0n7rac7"

	t.Run("a transaction keys on the ledger of the balances it moves value between", func(t *testing.T) {
		// The option and its argument are exactly what transaction_execution.go passes at both
		// its producer sites: prepareTransactionEventOutbox and the post-commit fallback.
		sourceBalance := &model.Balance{BalanceID: "bln_source_f04", LedgerID: ledgerID}
		destinationBalance := &model.Balance{BalanceID: "bln_dest_f04", LedgerID: ledgerID}

		key := preparedKey(t, instance, NewWebhook{
			Event:   model.EventTypeTransactionApplied,
			Payload: &model.Transaction{TransactionID: "txn_f04", Source: sourceBalance.BalanceID, Destination: destinationBalance.BalanceID, PreciseAmount: big.NewInt(1), CreatedAt: time.Now().UTC()},
		}, WithEventLedgerID(transactionLedgerID(sourceBalance, destinationBalance)))

		assert.Equal(t, ledgerID, key,
			"a transaction event must be keyed on its LEDGER. Keyed on the source balance instead, "+
				"one ledger's transactions scatter across partitions and R-6's per-ledger ordering is "+
				"lost — which is what the documentation used to promise")
		assert.NotEqual(t, sourceBalance.BalanceID, key,
			"and specifically not on the source balance")
	})

	t.Run("a transaction with no ledger anywhere falls back to its balances", func(t *testing.T) {
		// The rejection path: RejectTransaction persists with no balances loaded, so
		// transactionLedgerID yields nothing and WithEventLedgerID ignores the blank value. The
		// documented fallback chain is what keeps the event keyed at all.
		key := preparedKey(t, instance, NewWebhook{
			Event:   model.EventTypeTransactionRejected,
			Payload: &model.Transaction{TransactionID: "txn_f04_rejected", Source: "bln_source_reject", Destination: "bln_dest_reject", PreciseAmount: big.NewInt(1), CreatedAt: time.Now().UTC()},
		}, WithEventLedgerID(transactionLedgerID(nil, nil)))

		assert.Equal(t, "bln_source_reject", key,
			"with no ledger available the key must fall back to the source balance rather than to "+
				"nothing: an unkeyed message is scattered round-robin and loses ordering silently")
	})

	t.Run("a balance keys on its ledger", func(t *testing.T) {
		key := preparedKey(t, instance, NewWebhook{
			Event:   "balance.created",
			Payload: &model.Balance{BalanceID: "bln_created_f04", LedgerID: ledgerID},
		}, WithEventLedgerID(ledgerID))

		assert.Equal(t, ledgerID, key)
	})

	t.Run("a balance monitor alert keys on the ledger of the monitored balance", func(t *testing.T) {
		// model.BalanceMonitor carries a balance and a condition and NO ledger, so this key
		// exists only because both producer sites — the atomic
		// prepareBalanceMonitorEventOutboxes and the checkBalanceMonitors fallback — supply it
		// from the balance they hold.
		key := preparedKey(t, instance, NewWebhook{
			Event:   model.EventTypeBalanceMonitor,
			Payload: model.BalanceMonitor{MonitorID: "mon_f04", BalanceID: "bln_monitored_f04"},
		}, WithEventLedgerID(ledgerID))

		assert.Equal(t, ledgerID, key,
			"a monitor alert must be keyed on the ledger, so it is ordered with the movements that "+
				"trigger it rather than on a partition of its own")
		assert.NotEqual(t, "bln_monitored_f04", key)
	})

	t.Run("a ledger keys on its own id", func(t *testing.T) {
		key := preparedKey(t, instance, NewWebhook{
			Event:   "ledger.created",
			Payload: &model.Ledger{LedgerID: ledgerID},
		}, WithEventLedgerID(ledgerID))

		assert.Equal(t, ledgerID, key)
	})

	t.Run("an identity keys on the identity id, because an identity has no ledger", func(t *testing.T) {
		// identity.go supplies NO ledger option, and cannot: an identity is not a ledger-scoped
		// entity. This is an enumerated exception rather than a gap.
		key := preparedKey(t, instance, NewWebhook{
			Event:   "identity.created",
			Payload: &model.Identity{IdentityID: "idt_f04"},
		})

		assert.Equal(t, "idt_f04", key)
	})

	t.Run("a bulk summary keys on the batch id, because a batch spans ledgers", func(t *testing.T) {
		// transaction_bulk.go supplies no ledger option. A batch's transactions may belong to
		// different ledgers, so no single ledger could key the summary.
		key := preparedKey(t, instance, NewWebhook{
			Event:   "bulk_transaction.applied",
			Payload: map[string]interface{}{"batch_id": "bulk_f04", "status": "applied"},
		})

		assert.Equal(t, "bulk_f04", key)
	})

	t.Run("system.error keys on the event type, giving the error stream a total order", func(t *testing.T) {
		// blnk.go's registered sender supplies no ledger option, and system.error has no
		// aggregate of any kind. Falling back to the event type puts the whole stream on one
		// partition, which is what an error stream wants.
		key := preparedKey(t, instance, NewWebhook{
			Event:   model.EventTypeSystemError,
			Payload: map[string]interface{}{"error": "something failed", "time": time.Now().UTC()},
		})

		assert.Equal(t, model.EventTypeSystemError, key)
	})
}

// docMustState asserts a document contains a phrase WITHOUT dumping the document on failure.
//
// testify's Contains prints both operands, and these documents are tens of kilobytes; a single
// failed assertion would bury the CI log and the reason for the failure with it. The phrase and
// the reason are what a reader needs, so those are what is printed.
func docMustState(t *testing.T, document, name, phrase, why string) {
	t.Helper()

	if strings.Contains(document, phrase) {
		return
	}

	t.Errorf("%s must state %q.\n%s", name, phrase, why)
}

// docMustNotState is the absence form, and it likewise prints only the offending phrase.
func docMustNotState(t *testing.T, document, name, phrase, why string) {
	t.Helper()

	if !strings.Contains(document, phrase) {
		return
	}

	t.Errorf("%s must NOT state %q.\n%s", name, phrase, why)
}

// TestPartitionKeyContract_DocumentationMatchesTheProducers is the link that makes the section
// above a contract rather than two independent descriptions.
//
// It asserts on the SUBSCRIBER-FACING documents, because they are what a consumer designs
// against. A source comment that disagreed with the code would be a maintenance problem; a
// PUBLISHED document that disagrees with the code is a broken promise to somebody who cannot
// read the code.
func TestPartitionKeyContract_DocumentationMatchesTheProducers(t *testing.T) {
	streaming := readRepoFile(t, "docs/event-streaming.md")

	t.Run("the ordering contract is stated as the ledger", func(t *testing.T) {
		docMustState(t, streaming, "docs/event-streaming.md", "Blnk partitions by **ledger id**",
			"This is the partitioning dimension requirement R-6 fixes, in the section a subscriber "+
				"reads before sizing its consumer group.")
	})

	t.Run("it does not promise per-balance transaction keying", func(t *testing.T) {
		// The exact claim F-04 found. It is asserted as an ABSENCE because the sentence could
		// come back in any number of rewordings, and the one thing every wording shares is
		// telling a subscriber that a transaction is keyed on a balance.
		forbidden := []string{
			"that is a **balance** id, not a ledger id",
			"A transaction keys on its source balance",
			"The source balance, falling back to the destination balance",
		}
		for _, claim := range forbidden {
			docMustNotState(t, streaming, "docs/event-streaming.md", claim,
				"Production supplies the ledger at every transaction producer site, so telling a "+
					"subscriber a transaction is keyed on a balance promises ordering and partition "+
					"parallelism that do not exist.")
		}
	})

	t.Run("every non-ledger exception is enumerated", func(t *testing.T) {
		// The three event families that genuinely cannot be keyed on a ledger. Each must be
		// named, because an unenumerated exception is worse than none: a subscriber applies the
		// general rule and is silently wrong for one topic.
		for _, exception := range []string{
			model.EventTypeSystemError,
			"bulk_transaction.<status>",
			"identity.created",
		} {
			docMustState(t, streaming, "docs/event-streaming.md", exception,
				"This event family cannot be keyed on a ledger, and an unenumerated exception is worse "+
					"than none: a subscriber applies the general rule and is silently wrong for one topic.")
		}
	})

	t.Run("the operations runbook agrees about the key", func(t *testing.T) {
		operations := readRepoFile(t, "docs/kafka-operations.md")

		docMustNotState(t, operations, "docs/kafka-operations.md", "is a *balance* id rather than a ledger id",
			"The runbook must not contradict the streaming contract about the message key; an operator "+
				"reasoning about a partition-key prefix from it would reason from the wrong dimension.")
		docMustState(t, operations, "docs/kafka-operations.md", "**ledger id**",
			"It must state the same dimension the streaming contract states.")
	})

	t.Run("the anchor the runbook links to still exists", func(t *testing.T) {
		// A cross-document link that 404s is how the two halves of this contract drift apart
		// unnoticed: the reader stops following it and the runbook becomes the only source.
		operations := readRepoFile(t, "docs/kafka-operations.md")
		docMustState(t, operations, "docs/kafka-operations.md",
			"event-streaming.md#the-key-is-the-ledger-id-wherever-a-ledger-exists",
			"The runbook must link to the partitioning section by its current anchor.")
		docMustState(t, streaming, "docs/event-streaming.md",
			"### The key is the ledger id wherever a ledger exists",
			"The heading the runbook links to must exist, or the link resolves to nothing.")
	})

	t.Run("the R-2 guarantee is not documented with an event-type exception", func(t *testing.T) {
		// F-03's other half. balance.monitor is no longer exempt, so the document must not say
		// it is — a subscriber that read the old text would keep polling balances it no longer
		// needs to poll, and an operator would keep treating a missing alert as expected.
		for _, claim := range []string{
			"The one exception: `balance.monitor` is at-most-once",
			"`balance.monitor` is the single event type whose capture is **not** atomic",
			"Treat `balance.monitor` as an at-most-once alerting signal",
		} {
			docMustNotState(t, streaming, "docs/event-streaming.md", claim,
				"balance.monitor alerts are now captured inside the mutation's own transaction, so the "+
					"document must not grant that event type an exception to R-2.")
		}

		docMustState(t, streaming, "docs/event-streaming.md", "no event type that is exempt",
			"It must say so positively, so a reader does not have to infer the change from an absence.")
	})
}

// TestPartitionKeyContract_EveryProducerSiteSuppliesTheLedgerWhereverOneExists is the
// structural half: it reads the producer call sites themselves.
//
// The behavioural test above proves the key that comes out of preparation GIVEN the option.
// This proves the option is actually passed at every site that has a ledger to pass — the
// failure F-04 describes is precisely a site that stops passing it, which the behavioural test
// cannot see because it supplies the option itself.
func TestPartitionKeyContract_EveryProducerSiteSuppliesTheLedgerWhereverOneExists(t *testing.T) {
	// file -> how many producer sites in it must supply the ledger.
	//
	// The counts are exact rather than "at least one", because a site that lost the option
	// would otherwise hide behind its neighbours in the same file.
	ledgerScopedSites := map[string]int{
		// prepareTransactionEventOutbox and the post-commit fallback.
		"transaction_execution.go": 2,
		// CreateLedger and UpdateLedger.
		"ledger.go": 2,
		// The monitor fallback, the atomic monitor preparation, and two balance creations.
		"balance.go": 4,
		// RejectTransaction, which resolves the ledger from the transaction's own source
		// balance because model.Transaction carries none. It is BEST EFFORT and can never fail
		// the rejection — see transactionRejectionLedgerID — and it is required rather than
		// optional: without it this one event in a transaction's lifecycle would be keyed on the
		// source balance while its siblings are keyed on the ledger, so the two would land on
		// different partitions and a subscriber could legitimately observe the rejection before
		// the queueing of the same transaction.
		"transaction_rejection.go": 1,
	}

	for name, expected := range ledgerScopedSites {
		source := readRepoFile(t, name)
		assert.Equalf(t, expected, strings.Count(source, "WithEventLedgerID("), //nolint:testifylint // count, not a length
			"%s must supply the ledger at each of its %d ledger-scoped producer sites; a site that "+
				"drops the option keys its event on the payload's own aggregate instead and breaks "+
				"R-6's per-ledger ordering for that event type alone, which no consumer can detect",
			name, expected)
	}

	// The counterpart: the two producers that must NOT supply one, because their events are
	// genuinely not ledger-scoped. Passing a fabricated ledger here would be worse than passing
	// nothing — it would key the event onto a partition shared with a ledger it does not belong
	// to.
	//
	// transaction_rejection.go was once in this list, on the reading that a rejection is
	// persisted with no balances loaded and therefore has no ledger to give. It has one: the
	// ledger of the balance the transaction names, resolved by a single indexed read on a
	// failure path. "Not loaded" is not the same as "does not exist", and the cost of treating
	// them alike was an ordering defect within one transaction's own event sequence.
	for _, name := range []string{"identity.go", "transaction_bulk.go"} {
		source := readRepoFile(t, name)
		assert.NotContainsf(t, source, "WithEventLedgerID(",
			"%s has no ledger to supply: an identity is not ledger-scoped and a batch spans "+
				"ledgers. Supplying one would be a fabricated routing key", name)
	}
}
