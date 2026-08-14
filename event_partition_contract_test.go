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

// The partition-key contract, asserted from the PRODUCTION CALL SITES against the
// PUBLISHED DOCUMENTATION.

// partitionContractBlnk returns an instance wired for event preparation only.
//
// Preparation performs NO I/O: it marshals the payload, derives the key and returns a
// row.
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

// TestPartitionKeyContract_ProductionCallSitesKeyOnTheLedger asserts the key every
// production producer actually produces, one event family at a time.
//
// The three families that CANNOT be keyed on a ledger are asserted too, because "the
// ledger wherever a ledger exists" is only a contract if the exceptions are enumerated.
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
		// transactionLedgerID yields nothing and WithEventLedgerID ignores the blank value.
		// The documented fallback chain is what keeps the event keyed at all.
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
		// exists only because both producer sites — the atomic prepareBalanceMonitorEvents
		// and the legacy-only checkBalanceMonitors — supply it from the balance they hold.
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

	t.Run("system.error keys on the event itself, so the error stream spreads", func(t *testing.T) {
		// blnk.go's registered sender supplies no ledger option, and system.error has no
		// aggregate of any kind, so the key falls back to the event's own id.
		//
		// IT USED TO FALL BACK TO THE EVENT TYPE, which put the whole stream on one partition
		// and read as a virtue — a total order over the errors. It was a per-category
		// throughput ceiling: one key is claimed by one relay instance and published one
		// message at a time, measured at 1.00 event per second while errors arrived far
		// faster, until the backlog was 59% of the whole outbox. Two unrelated internal
		// errors have no causal order, so nothing was lost that a consumer could have used;
		// occurred_at carries time order.
		//
		// Asserted as two DISTINCT keys rather than against a literal, because the property
		// that matters is the spread: one key per event, so the partition count is the only
		// thing bounding this stream's parallelism.
		first := preparedKey(t, instance, NewWebhook{
			Event:   model.EventTypeSystemError,
			Payload: map[string]interface{}{"error": "something failed", "time": time.Now().UTC()},
		})
		second := preparedKey(t, instance, NewWebhook{
			Event:   model.EventTypeSystemError,
			Payload: map[string]interface{}{"error": "something else failed", "time": time.Now().UTC()},
		})

		assert.NotEqual(t, model.EventTypeSystemError, first,
			"the event TYPE must not be the key: it is the same string for every error, so it "+
				"pins the whole category to one partition and one publisher")
		assert.NotEmpty(t, first, "an event with no aggregate must still be keyed")
		assert.NotEqual(t, first, second,
			"two error events must carry DIFFERENT keys, or they share a partition and the "+
				"category's throughput is bounded by a single ordered stream")
	})
}

// docMustState asserts a document contains a structural token — a heading, a link
// target, an identifier or a table cell — WITHOUT dumping the document on failure.
//
// testify's Contains prints both operands, and these documents are tens of kilobytes; a
// single failed assertion would bury the CI log and the reason for the failure with it.
//
// Only machine-checkable tokens belong here. A sentence does not: the documents are
// edited for clarity far more often than the contract changes, so asserting prose makes
// an editorial rewrite fail the suite while a real divergence still slips past under a
// different wording. Everything a document has to AGREE with the code about is asserted
// through markdownTable instead.
func docMustState(t *testing.T, document, name, token, why string) {
	t.Helper()

	if strings.Contains(document, token) {
		return
	}

	t.Errorf("%s must state %q.\n%s", name, token, why)
}

// markdownTable is one parsed GitHub-flavoured markdown table: its header cells and its
// body rows, each row keyed by the normalised text of its first cell.
type markdownTable struct {
	headers []string
	rows    map[string][]string
	order   []string
}

// Row returns a row by the normalised text of its first cell.
//
// Parameters:
//   - key string: the normalised first-cell text, as normalizeTableCell renders it.
//
// Returns:
//   - []string: the row's cells, header order preserved.
//   - bool: whether the table carries such a row.
func (m markdownTable) Row(key string) ([]string, bool) {
	cells, ok := m.rows[normalizeTableCell(key)]

	return cells, ok
}

// Keys returns the normalised first cell of every body row, in document order.
//
// Returns:
//   - []string: one key per row.
func (m markdownTable) Keys() []string {
	return m.order
}

// Cell returns one cell of one row by column name.
//
// Parameters:
//   - rowKey string: the row's normalised first cell.
//   - header string: the column's header text, normalised the same way.
//
// Returns:
//   - string: the cell text, or the empty string when either lookup misses.
func (m markdownTable) Cell(rowKey, header string) string {
	cells, ok := m.Row(rowKey)
	if !ok {
		return ""
	}

	wanted := normalizeTableCell(header)
	for index, candidate := range m.headers {
		if candidate == wanted && index < len(cells) {
			return cells[index]
		}
	}

	return ""
}

// markdownTableIn parses the first markdown table in document whose header row carries
// every column in headers.
//
// A table is a machine-checkable artefact: its rows are keyed, its columns are named,
// and a divergence between it and the code is a missing or wrong CELL rather than a
// missing sentence. Asserting on one is what lets the surrounding prose be rewritten
// freely.
//
// Parameters:
//   - t *testing.T: the test, failed when no such table exists.
//   - document string: the whole markdown document.
//   - name string: the document's path, quoted into the failure.
//   - headers []string: column names the header row must carry.
//
// Returns:
//   - markdownTable: the parsed table.
func markdownTableIn(t *testing.T, document, name string, headers []string) markdownTable {
	t.Helper()

	lines := strings.Split(document, "\n")
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "|") {
			continue
		}

		cells := splitTableRow(trimmed)
		if !tableHeaderCovers(cells, headers) {
			continue
		}

		return parseMarkdownTable(cells, lines[index+1:])
	}

	require.FailNowf(t, "table not found",
		"%s must carry a table whose header row names %v; the contract is asserted against that "+
			"table's cells rather than against the prose around it", name, headers)

	return markdownTable{}
}

// parseMarkdownTable collects body rows following a header row.
//
// Parameters:
//   - headerCells []string: the already-split header row.
//   - rest []string: the lines after the header row, delimiter included.
//
// Returns:
//   - markdownTable: the header cells and every body row until the table ends.
func parseMarkdownTable(headerCells []string, rest []string) markdownTable {
	table := markdownTable{rows: map[string][]string{}}
	for _, header := range headerCells {
		table.headers = append(table.headers, normalizeTableCell(header))
	}

	for offset, line := range rest {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "|") {
			break
		}

		// The delimiter row immediately under the header carries only dashes and colons.
		if offset == 0 && strings.Trim(strings.ReplaceAll(trimmed, "|", ""), "-: ") == "" {
			continue
		}

		cells := splitTableRow(trimmed)
		if len(cells) == 0 {
			continue
		}

		key := normalizeTableCell(cells[0])
		table.rows[key] = cells
		table.order = append(table.order, key)
	}

	return table
}

// tableHeaderCovers reports whether a candidate header row names every wanted column.
//
// Parameters:
//   - cells []string: the candidate row's cells.
//   - wanted []string: the column names the caller requires.
//
// Returns:
//   - bool: true when every wanted column is present.
func tableHeaderCovers(cells []string, wanted []string) bool {
	present := make(map[string]struct{}, len(cells))
	for _, cell := range cells {
		present[normalizeTableCell(cell)] = struct{}{}
	}

	for _, header := range wanted {
		if _, ok := present[normalizeTableCell(header)]; !ok {
			return false
		}
	}

	return true
}

// splitTableRow splits one pipe-delimited row into trimmed cells.
//
// Parameters:
//   - row string: the row, leading and trailing pipes included.
//
// Returns:
//   - []string: the cells, in column order.
func splitTableRow(row string) []string {
	trimmed := strings.Trim(strings.TrimSpace(row), "|")

	cells := strings.Split(trimmed, "|")
	for index, cell := range cells {
		cells[index] = strings.TrimSpace(cell)
	}

	return cells
}

// normalizeTableCell reduces a cell to the token it names: markdown emphasis and code
// quoting removed, case folded, whitespace collapsed.
//
// Parameters:
//   - cell string: the raw cell text.
//
// Returns:
//   - string: the comparable token.
func normalizeTableCell(cell string) string {
	replaced := strings.NewReplacer("`", "", "*", "", "_", "").Replace(cell)

	return strings.ToLower(strings.Join(strings.Fields(replaced), " "))
}

// TestPartitionKeyContract_DocumentationMatchesTheProducers is the link that makes the
// section above a contract rather than two independent descriptions.
//
// It asserts on the SUBSCRIBER-FACING documents, because they are what a consumer
// designs against.
func TestPartitionKeyContract_DocumentationMatchesTheProducers(t *testing.T) {
	const (
		streamingDoc  = "docs/event-streaming.md"
		operationsDoc = "docs/kafka-operations.md"
		keyHeading    = "### The key is the ledger id wherever a ledger exists"
		keyAnchor     = "event-streaming.md#the-key-is-the-ledger-id-wherever-a-ledger-exists"
	)

	streaming := readRepoFile(t, streamingDoc)

	// The published key table, which is the artefact a subscriber designs against and the
	// only thing in the document this test reads. Every claim below is a CELL of it
	// compared against model's own declaration, so the prose around the table can be
	// rewritten at will and a divergence in the contract still fails here.
	published := markdownTableIn(t, streaming, streamingDoc,
		[]string{"Event type", "Partition key", "ledger_id"})

	t.Run("every declared event type has a row whose dimension matches the code", func(t *testing.T) {
		// The dimension keyword each declaration must be rendered with, and the ledger_id
		// cell that goes with it. A ledger-dimensioned event records the ledger it is keyed
		// on; the other two dimensions have no ledger to record, so the column reads null.
		dimensionKeyword := map[model.EventKeyDimension]string{
			model.EventKeyDimensionLedger:    "ledger",
			model.EventKeyDimensionAggregate: "id",
			// "event id", not "event type": the type-wide key was replaced by a per-event one so
			// that an aggregate-less stream spreads across partitions instead of serialising onto
			// one, and the published table has to say which of the two a subscriber gets — the
			// difference is whether it may size a consumer group above one.
			model.EventKeyDimensionEvent: "event id",
		}

		for eventType, dimension := range model.EventKeyDimensionsByType() {
			row := documentedKeyRow(t, published, eventType)
			if row == "" {
				continue
			}

			keyCell := normalizeTableCell(published.Cell(row, "Partition key"))
			assert.Containsf(t, keyCell, dimensionKeyword[dimension],
				"%s's key table renders %q as %q, and model declares it %s-dimensioned. The table is "+
					"what a subscriber sizes its consumer group from, so a row that names another "+
					"dimension promises ordering the producers do not deliver",
				streamingDoc, eventType, keyCell, dimension)

			ledgerCell := normalizeTableCell(published.Cell(row, "ledger_id"))
			if dimension == model.EventKeyDimensionLedger {
				assert.NotEqualf(t, "null", ledgerCell,
					"%s says %q records no ledger while model declares it ledger-dimensioned",
					streamingDoc, eventType)

				continue
			}

			assert.Equalf(t, "null", ledgerCell,
				"%s records a ledger for %q while model declares it %s-dimensioned; only a "+
					"ledger-dimensioned event has one to record", streamingDoc, eventType, dimension)
		}
	})

	t.Run("every non-ledger event type is enumerated as a row", func(t *testing.T) {
		// An unenumerated exception is worse than none: a subscriber applies the general rule
		// and is silently wrong for one topic. Asserted as ROWS rather than as mentions,
		// because a mention elsewhere in the document is not a statement about the key.
		for _, exception := range []string{
			model.EventTypeSystemError,
			"bulk_transaction.<status>",
			"identity.created",
		} {
			_, documented := published.Row(exception)
			assert.Truef(t, documented,
				"%s's key table must carry a row for %q, which belongs to no ledger and is therefore "+
					"keyed on something else; the rows present are %v",
				streamingDoc, exception, published.Keys())
		}
	})

	t.Run("the anchor the runbook links to still exists", func(t *testing.T) {
		// A cross-document link that 404s is how the two halves of this contract drift apart
		// unnoticed: the reader stops following it and the runbook becomes the only source.
		operations := readRepoFile(t, operationsDoc)
		docMustState(t, operations, operationsDoc, keyAnchor,
			"The runbook must link to the partitioning section by its current anchor.")
		docMustState(t, streaming, streamingDoc, keyHeading,
			"The heading the runbook links to must exist, or the link resolves to nothing.")
	})
}

// documentedKeyRow resolves the key table's row for one event type, tolerating the two
// renderings a composed event family can have.
//
// bulk_transaction.<status> is documented under its published placeholder rather than
// under any single status, because its names are composed at runtime.
//
// Parameters:
//   - t *testing.T: the test, failed when no row matches.
//   - table markdownTable: the parsed key table.
//   - eventType string: the declared event type.
//
// Returns:
//   - string: the row key to read cells with, or "" when the family is documented
//     collectively and this exact name is not a row.
func documentedKeyRow(t *testing.T, table markdownTable, eventType string) string {
	t.Helper()

	if _, ok := table.Row(eventType); ok {
		return eventType
	}

	// A transaction status may be documented collectively as `transaction.*`, which is a
	// statement about all seven and is asserted through that row instead.
	if strings.HasPrefix(eventType, "transaction.") {
		for _, candidate := range []string{"transaction.* (all seven)", "transaction.*"} {
			if _, ok := table.Row(candidate); ok {
				return candidate
			}
		}
	}

	t.Errorf("docs/event-streaming.md's key table must carry a row for %q; the rows present are %v",
		eventType, table.Keys())

	return ""
}

// TestPartitionKeyContract_EveryProducerSiteSuppliesTheLedgerWhereverOneExists is the
// structural half: it reads the producer call sites themselves.
//
// The behavioural test above proves the key that comes out of preparation GIVEN the
// option.
func TestPartitionKeyContract_EveryProducerSiteSuppliesTheLedgerWhereverOneExists(t *testing.T) {
	// file -> how many producer sites in it must supply the ledger.
	ledgerScopedSites := map[string]int{
		// prepareTransactionEventOutbox and the post-commit fallback.
		"transaction_execution.go": 2,
		// CreateLedger and UpdateLedger.
		"ledger.go": 2,
		// The monitor fallback, the atomic monitor preparation, and two balance creations.
		"balance.go": 4,
		// RejectTransaction, which resolves the ledger from the transaction's own source
		// balance because model.Transaction carries none. It is BEST EFFORT and can never
		// fail the rejection — see transactionRejectionLedgerID — and it is required rather
		// than optional: without it this one event in a transaction's lifecycle would be
		// keyed on the source balance while its siblings are keyed on the ledger, so the two
		// would land on different partitions and a subscriber could legitimately observe the
		// rejection before the queueing of the same transaction.
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

	// The counterpart: the two producers that must NOT supply one, because their events
	// are genuinely not ledger-scoped. Passing a fabricated ledger here would be worse
	// than passing nothing — it would key the event onto a partition shared with a ledger
	// it does not belong to.
	for _, name := range []string{"identity.go", "transaction_bulk.go"} {
		source := readRepoFile(t, name)
		assert.NotContainsf(t, source, "WithEventLedgerID(",
			"%s has no ledger to supply: an identity is not ledger-scoped and a batch spans "+
				"ledgers. Supplying one would be a fabricated routing key", name)
	}
}
