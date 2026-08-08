// Copyright 2024 Blnk Finance Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// Broker-record coordinates and the zero-loss audit arithmetic.
//
// Every function covered here was exercised only from the ROOT package, where
// the mutation gate cannot reach it: `make mutate` scores a package by running
// that package's own tests, so a statement whose only coverage lives in another
// package is reported NOT COVERED and is never scored at all. Eleven functions
// in model/event.go were in that position, which meant the arithmetic behind
// the zero-loss verdict (V-2) carried no mutation score despite being money
// -critical.
//
// So these tests are written to KILL MUTANTS rather than merely to execute the
// lines: each one pins a boundary (>= versus >), a connective (&& versus ||),
// an operator (- versus +) or a normalisation (TrimSpace present or absent), so
// that flipping any of them in the source makes a named assertion fail.
// model/mutation_killers_test.go is the in-repository precedent for the style.
// -----------------------------------------------------------------------------

// intPtr and int64Ptr address the nullable broker-coordinate columns. The three
// are written and cleared together, so a test that sets only some of them is
// describing a row the writer cannot produce — which is exactly what the
// accessor's guard exists to reject, and so exactly what is tested below.
func intPtr(v int) *int       { return &v }
func int64Ptr(v int64) *int64 { return &v }

func TestBrokerRecord_ConfirmedRequiresATopicAndANonNegativeOffset(t *testing.T) {
	// Offset ZERO with a topic is CONFIRMED. This is the boundary the coordinate
	// exists to get right: offset 0 is the first record on a fresh partition, an
	// entirely ordinary location, and reading it as "no record" would report a
	// perfectly published event as lost. It also kills the boundary mutant that
	// turns `Offset >= 0` into `Offset > 0`.
	first := BrokerRecord{Topic: "blnk.transactions", Partition: 0, Offset: 0}
	assert.True(t, first.Confirmed(),
		"partition 0 offset 0 is a real record — the first one on a fresh partition — and must "+
			"not be read as an absent coordinate")

	// A NEGATIVE offset is not a location. Kafka never reports one; it is what an
	// unset sentinel looks like, and accepting it would let a fabricated
	// coordinate satisfy the audit.
	assert.False(t, BrokerRecord{Topic: "blnk.transactions", Offset: -1}.Confirmed(),
		"a negative offset names no record")

	// The TOPIC is what distinguishes a zero coordinate from a real one, so an
	// absent topic must fail even with a plausible offset. This kills the mutant
	// that replaces the && with ||, which would confirm any row with an offset.
	assert.False(t, BrokerRecord{Topic: "", Partition: 3, Offset: 148291}.Confirmed(),
		"a coordinate with no topic cannot be read back and is not confirmed")

	// And a topic of only whitespace is an absent topic. Without the TrimSpace it
	// would pass the != "" test and report a coordinate an operator cannot use.
	assert.False(t, BrokerRecord{Topic: "   ", Partition: 3, Offset: 148291}.Confirmed(),
		"a blank topic is an absent topic; the trim is what makes that true")

	// The ordinary confirmed case, so the predicate is not merely refusing
	// everything.
	assert.True(t, BrokerRecord{Topic: "blnk.transactions.dlt", Partition: 3, Offset: 148291}.Confirmed(),
		"a topic with a non-negative offset is a confirmed coordinate")
}

func TestBrokerRecord_StringRendersTheTriageCoordinateOrANamedAbsence(t *testing.T) {
	// The exact rendering matters: it is the string an operator pastes into a
	// console consumer, and the dead-letter API and the runbook both quote this
	// form. Asserting the whole string kills any mutation of the format.
	assert.Equal(t, "blnk.transactions/3@148291",
		BrokerRecord{Topic: "blnk.transactions", Partition: 3, Offset: 148291}.String(),
		"a confirmed coordinate renders as topic/partition@offset")

	// The boundary again, this time through the renderer: offset 0 must render as
	// data rather than as an absence.
	assert.Equal(t, "blnk.balances/0@0",
		BrokerRecord{Topic: "blnk.balances", Partition: 0, Offset: 0}.String(),
		"the first record on a partition renders as the location it is")

	// An unconfirmed coordinate renders as a NAMED ABSENCE. "/0@0" would read as
	// a location and send an operator looking for a record that was never
	// written, which is the failure this branch exists to prevent.
	assert.Equal(t, "unconfirmed", BrokerRecord{}.String(),
		"a zero coordinate must say so rather than rendering as /0@0")
	assert.Equal(t, "unconfirmed", BrokerRecord{Topic: "  ", Partition: 7, Offset: 9}.String(),
		"a blank topic renders as unconfirmed, not as a location with an empty topic")
}

func TestEventOutbox_BrokerRecordRequiresAllThreeColumns(t *testing.T) {
	// All three present is the confirmed case, and the accessor must return the
	// values verbatim — a mutation that swapped partition for offset, or dropped
	// the topic, would be invisible to a test that only checked the boolean.
	row := EventOutbox{
		KafkaTopic:     "blnk.transactions",
		KafkaPartition: intPtr(4),
		KafkaOffset:    int64Ptr(90210),
	}
	record, ok := row.BrokerRecord()
	require.True(t, ok, "a row with all three coordinate columns names a record")
	assert.Equal(t, "blnk.transactions", record.Topic)
	assert.Equal(t, 4, record.Partition)
	assert.Equal(t, int64(90210), record.Offset)

	// Each of the three guards is exercised on its own, because they are joined
	// by ||: a test that omitted all three at once would pass even if two of the
	// three conditions were deleted.
	missing := map[string]EventOutbox{
		"no partition": {KafkaTopic: "blnk.transactions", KafkaOffset: int64Ptr(1)},
		"no offset":    {KafkaTopic: "blnk.transactions", KafkaPartition: intPtr(1)},
		"no topic":     {KafkaPartition: intPtr(1), KafkaOffset: int64Ptr(1)},
		"blank topic":  {KafkaTopic: "\t", KafkaPartition: intPtr(1), KafkaOffset: int64Ptr(1)},
	}
	for name, candidate := range missing {
		t.Run(name, func(t *testing.T) {
			record, ok := candidate.BrokerRecord()
			assert.False(t, ok, "a row with %s does not name a record", name)
			assert.Equal(t, BrokerRecord{}, record,
				"and it must return the ZERO coordinate, so a caller that ignores the boolean "+
					"cannot read a half-populated location as a real one")
		})
	}

	// The three columns present but the offset negative: the accessor defers to
	// Confirmed, so this must be reported as unconfirmed even though nothing is
	// nil. This is what keeps the two functions from drifting apart.
	fabricated := EventOutbox{
		KafkaTopic:     "blnk.transactions",
		KafkaPartition: intPtr(0),
		KafkaOffset:    int64Ptr(-1),
	}
	_, ok = fabricated.BrokerRecord()
	assert.False(t, ok, "a negative offset is not a location even when all three columns are set")

	// Offset zero, all three set: confirmed. The boundary, once more, through the
	// accessor an audit actually calls.
	firstRecord := EventOutbox{
		KafkaTopic:     "blnk.identities",
		KafkaPartition: intPtr(0),
		KafkaOffset:    int64Ptr(0),
	}
	coordinate, ok := firstRecord.BrokerRecord()
	assert.True(t, ok, "the first record on a partition is confirmed")
	assert.Equal(t, "blnk.identities/0@0", coordinate.String())
}

func TestEventOutboxAudit_UnconfirmedRowsIsTheShortfallAndNeverNegative(t *testing.T) {
	// The ordinary shortfall. Also kills the arithmetic mutant that turns the
	// subtraction into an addition: 13 rather than 7 fails here.
	assert.Equal(t, int64(7),
		EventOutboxAudit{PublishedRows: 10, ConfirmedRows: 3}.UnconfirmedRows(),
		"seven rows claim a publication they cannot name a record for")

	// Fully confirmed: nothing outstanding.
	assert.Equal(t, int64(0),
		EventOutboxAudit{PublishedRows: 10, ConfirmedRows: 10}.UnconfirmedRows(),
		"a fully confirmed audit has no unconfirmed rows")

	// MORE confirmed than published cannot happen against an intact schema, and
	// the function clamps rather than returning a negative — a negative would
	// propagate into the reconciliation verdict as a surplus and describe
	// duplication that did not occur.
	assert.Equal(t, int64(0),
		EventOutboxAudit{PublishedRows: 3, ConfirmedRows: 10}.UnconfirmedRows(),
		"the shortfall is clamped at zero rather than going negative")

	// The empty audit, which is what a quiet outbox looks like.
	assert.Equal(t, int64(0), EventOutboxAudit{}.UnconfirmedRows(),
		"an audit of nothing has no shortfall")

	// A single unconfirmed row is the smallest finding the audit can make, and it
	// must be visible: the whole point of the split is that one uncorroborated
	// claim is no longer hidden by a surplus elsewhere.
	assert.Equal(t, int64(1),
		EventOutboxAudit{PublishedRows: 1, ConfirmedRows: 0}.UnconfirmedRows(),
		"one unconfirmed row is reported as one, not rounded away")
}

func TestEventOutboxAudit_FullyConfirmedNeedsBothNoShortfallAndDistinctRecords(t *testing.T) {
	// The conclusive case: every claim named, and every name distinct.
	assert.True(t,
		EventOutboxAudit{PublishedRows: 10, ConfirmedRows: 10, DistinctRecords: 10}.FullyConfirmed(),
		"ten rows naming ten distinct records is a conclusive audit")

	// A shortfall alone must defeat it, even though the distinct count agrees
	// with the confirmed count. This kills the mutant that turns the && into an
	// ||, which would call an audit with three uncorroborated claims conclusive.
	assert.False(t,
		EventOutboxAudit{PublishedRows: 10, ConfirmedRows: 7, DistinctRecords: 7}.FullyConfirmed(),
		"three rows claiming a publication they cannot name defeats the verdict on their own")

	// Two rows sharing a coordinate must defeat it too, for the same reason in
	// the other direction: the partial unique index makes that impossible, so
	// seeing it means the index is gone and the audit must not assume otherwise.
	assert.False(t,
		EventOutboxAudit{PublishedRows: 10, ConfirmedRows: 10, DistinctRecords: 9}.FullyConfirmed(),
		"two rows naming one record defeats the verdict — the unique index that forbids it "+
			"is evidently absent")

	// DistinctRecords must never EXCEED ConfirmedRows, and the audit refuses to
	// call such a reading conclusive.
	//
	// This is not hypothetical. The repository query counted distinct coordinates
	// with COUNT(DISTINCT (topic, partition, offset)), and PostgreSQL counts the
	// all-NULL row constructor as one distinct value — so a single published row
	// with no coordinate contributed a phantom record and pushed this count above
	// the confirmed one. The query now filters those rows out; this assertion is
	// what keeps a healthy outbox from being reported inconclusive if it ever
	// stops doing so.
	assert.False(t,
		EventOutboxAudit{PublishedRows: 10, ConfirmedRows: 10, DistinctRecords: 11}.FullyConfirmed(),
		"more distinct records than confirmed rows is an impossible reading, not a conclusive one")

	// An empty audit is trivially conclusive: nothing claims a publication, so
	// nothing is unaccounted for. Asserting it pins the vacuous case rather than
	// leaving it to be discovered by a caller.
	assert.True(t, EventOutboxAudit{}.FullyConfirmed(),
		"an audit of nothing is conclusive about nothing")
}
