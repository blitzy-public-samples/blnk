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
// -----------------------------------------------------------------------------

// intPtr and int64Ptr address the nullable broker-coordinate columns. The three
// are written and cleared together, so a test that sets only some of them is
// describing a row the writer cannot produce — which is exactly what the
// accessor's guard exists to reject, and so exactly what is tested below.
func intPtr(v int) *int       { return &v }
func int64Ptr(v int64) *int64 { return &v }

func TestBrokerRecord_ConfirmedRequiresATopicAndANonNegativeOffset(t *testing.T) {
	// Offset ZERO with a topic is CONFIRMED. This is the boundary the coordinate exists to
	// get right: offset 0 is the first record on a fresh partition, an entirely ordinary
	// location, and reading it as "no record" would report a perfectly published event as
	// lost.
	first := BrokerRecord{Topic: "blnk.transactions", Partition: 0, Offset: 0}
	assert.True(t, first.Confirmed(),
		"partition 0 offset 0 is a real record — the first one on a fresh partition — and must "+
			"not be read as an absent coordinate")

	// A NEGATIVE offset is not a location. Kafka never reports one; it is what an unset
	// sentinel looks like, and accepting it would let a fabricated coordinate satisfy the
	// audit.
	assert.False(t, BrokerRecord{Topic: "blnk.transactions", Offset: -1}.Confirmed(),
		"a negative offset names no record")

	// The TOPIC is what distinguishes a zero coordinate from a real one, so an absent
	// topic must fail even with a plausible offset. This kills the mutant that replaces
	// the && with ||, which would confirm any row with an offset.
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
	// The exact rendering matters: it is the string an operator pastes into a console
	// consumer, and the dead-letter API and the runbook both quote this form. Asserting
	// the whole string kills any mutation of the format.
	assert.Equal(t, "blnk.transactions/3@148291",
		BrokerRecord{Topic: "blnk.transactions", Partition: 3, Offset: 148291}.String(),
		"a confirmed coordinate renders as topic/partition@offset")

	// The boundary again, this time through the renderer: offset 0 must render as
	// data rather than as an absence.
	assert.Equal(t, "blnk.balances/0@0",
		BrokerRecord{Topic: "blnk.balances", Partition: 0, Offset: 0}.String(),
		"the first record on a partition renders as the location it is")

	// An unconfirmed coordinate renders as a NAMED ABSENCE. "/0@0" would read as a
	// location and send an operator looking for a record that was never written, which is
	// the failure this branch exists to prevent.
	assert.Equal(t, "unconfirmed", BrokerRecord{}.String(),
		"a zero coordinate must say so rather than rendering as /0@0")
	assert.Equal(t, "unconfirmed", BrokerRecord{Topic: "  ", Partition: 7, Offset: 9}.String(),
		"a blank topic renders as unconfirmed, not as a location with an empty topic")
}

func TestEventOutbox_BrokerRecordRequiresAllThreeColumns(t *testing.T) {
	// All three present is the confirmed case, and the accessor must return the values
	// verbatim — a mutation that swapped partition for offset, or dropped the topic, would
	// be invisible to a test that only checked the boolean.
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

	// Each of the three guards is exercised on its own, because they are joined by ||: a
	// test that omitted all three at once would pass even if two of the three conditions
	// were deleted.
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

	// The three columns present but the offset negative: the accessor defers to Confirmed,
	// so this must be reported as unconfirmed even though nothing is nil. This is what
	// keeps the two functions from drifting apart.
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

func TestPartitionOffsetInterval_ContainsIsHalfOpen(t *testing.T) {
	window := PartitionOffsetInterval{
		Topic: "blnk.transactions", Partition: 2, FirstOffset: 100, EndOffset: 200,
	}

	// The two boundaries are the whole contract, and they are asymmetric because Kafka's
	// are: FirstOffset is the earliest RETAINED record, EndOffset is one past the last
	// WRITTEN one.
	assert.True(t, window.Contains(100), "the first retained offset is inside the window")
	assert.True(t, window.Contains(199), "the last written offset is inside the window")
	assert.False(t, window.Contains(200),
		"the end offset is one PAST the last record, so a coordinate there cannot be on the log")
	assert.False(t, window.Contains(99),
		"an offset below the first retained one has been deleted by retention")

	// An offset far beyond the end is the truncation/recreation signal, and it must
	// read as outside rather than being clamped into the window.
	assert.False(t, window.Contains(1_000_000),
		"an offset far beyond the log end must read as outside: it is how a recreated topic is detected")

	// An empty window admits nothing. This kills the mutant that turns the less-than into
	// a less-than-or-equal, which would corroborate a row against a partition holding no
	// records at all.
	empty := PartitionOffsetInterval{Topic: "blnk.balances", FirstOffset: 42, EndOffset: 42}
	assert.False(t, empty.Contains(42), "a zero-width window contains nothing")
	assert.False(t, empty.Contains(41))
}

func TestPartitionOffsetInterval_RecordsIsTheWidthAndNeverNegative(t *testing.T) {
	assert.Equal(t, int64(100),
		PartitionOffsetInterval{FirstOffset: 100, EndOffset: 200}.Records())

	// A partition that has never been written to.
	assert.Equal(t, int64(0), PartitionOffsetInterval{}.Records(),
		"an untouched partition holds no records")

	// A partition every record of which has aged out: first equals end, at a non-zero
	// offset. Zero is the correct reading, and it is a legitimate one rather than a fault.
	assert.Equal(t, int64(0),
		PartitionOffsetInterval{FirstOffset: 9_000, EndOffset: 9_000}.Records(),
		"a fully aged-out partition holds no records, and that is not an error")

	// An inverted reading cannot happen from a coherent broker response, and the width
	// clamps rather than going negative: a negative would propagate into a reported record
	// count and describe a log that holds less than nothing.
	assert.Equal(t, int64(0),
		PartitionOffsetInterval{FirstOffset: 500, EndOffset: 100}.Records(),
		"an inverted window is reported as empty rather than as a negative width")
}

func TestEventRecordIntervalAudit_UncorroboratedRowsSumsEveryReasonAndNothingElse(t *testing.T) {
	// Each bucket contributes, and the four are added rather than any one of them standing
	// in for the rest. This kills the mutants that drop a term: a fix that counted only
	// unconfirmed rows would call a topic recreation conclusive.
	audit := EventRecordIntervalAudit{
		PublishedRows:    10,
		CorroboratedRows: 4,
		UnconfirmedRows:  1,
		UnmeasuredRows:   2,
		AgedOutRows:      2,
		BeyondEndRows:    1,
	}
	assert.Equal(t, int64(6), audit.UncorroboratedRows(),
		"every reason a claim could not be placed counts against the verdict, not just the first")

	// The identity the whole classification rests on: the buckets partition the
	// claims, so corroborated plus uncorroborated is exactly what was published.
	assert.Equal(t, audit.PublishedRows, audit.CorroboratedRows+audit.UncorroboratedRows(),
		"every published row lands in exactly one bucket, or the verdict is describing a different set")

	// Each bucket alone is enough to make a claim uncorroborated.
	for name, single := range map[string]EventRecordIntervalAudit{
		"unconfirmed": {PublishedRows: 1, UnconfirmedRows: 1},
		"unmeasured":  {PublishedRows: 1, UnmeasuredRows: 1},
		"aged out":    {PublishedRows: 1, AgedOutRows: 1},
		"beyond end":  {PublishedRows: 1, BeyondEndRows: 1},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, int64(1), single.UncorroboratedRows(),
				"one unplaceable claim is reported as one, not rounded away")
			assert.False(t, single.FullyCorroborated())
		})
	}

	// A quiet outbox.
	assert.Equal(t, int64(0), EventRecordIntervalAudit{}.UncorroboratedRows(),
		"an audit of nothing has nothing outstanding")
}

func TestEventRecordIntervalAudit_DuplicatedRecordsIsTheShortfallAndNeverNegative(t *testing.T) {
	// Two rows corroborated by one record. Only possible with the partial unique index on
	// the coordinate absent, which is why it is reported rather than absorbed.
	assert.Equal(t, int64(1),
		EventRecordIntervalAudit{CorroboratedRows: 10, DistinctCorroboratedRecords: 9}.DuplicatedRecords(),
		"one record corroborating two rows is double counting and must be visible")

	assert.Equal(t, int64(0),
		EventRecordIntervalAudit{CorroboratedRows: 10, DistinctCorroboratedRecords: 10}.DuplicatedRecords())

	// MORE distinct records than corroborated rows is an impossible reading. It is not
	// hypothetical: the repository counts distinct coordinates with COUNT(DISTINCT (topic,
	// partition, offset)) and PostgreSQL counts the all-NULL row constructor as one
	// distinct value, so an unfiltered query let a coordinate-less row contribute a
	// phantom record.
	assert.Equal(t, int64(0),
		EventRecordIntervalAudit{CorroboratedRows: 9, DistinctCorroboratedRecords: 10}.DuplicatedRecords(),
		"an impossible reading is clamped rather than reported as negative duplication")
}

func TestEventRecordIntervalAudit_FullyCorroboratedNeedsEveryClaimPlacedAndDistinct(t *testing.T) {
	// The conclusive case: every claim placed inside a measured window, every
	// coordinate distinct.
	assert.True(t,
		EventRecordIntervalAudit{
			PublishedRows: 10, CorroboratedRows: 10, DistinctCorroboratedRecords: 10,
		}.FullyCorroborated(),
		"ten rows naming ten distinct records inside the measured windows is conclusive")

	// An unplaced claim defeats it even though the distinct count agrees with the
	// corroborated count. This kills the mutant that turns the && into an ||.
	assert.False(t,
		EventRecordIntervalAudit{
			PublishedRows: 10, CorroboratedRows: 7, DistinctCorroboratedRecords: 7, AgedOutRows: 3,
		}.FullyCorroborated(),
		"three records deleted by retention cannot be corroborated, so the audit is not conclusive")

	// A shared coordinate defeats it in the other direction.
	assert.False(t,
		EventRecordIntervalAudit{
			PublishedRows: 10, CorroboratedRows: 10, DistinctCorroboratedRecords: 9,
		}.FullyCorroborated(),
		"two rows naming one record defeats the verdict — the unique index that forbids it "+
			"is evidently absent")

	// An empty audit is trivially conclusive: nothing claims a publication, so nothing is
	// unaccounted for. Asserting it pins the vacuous case rather than leaving it to be
	// discovered by a caller.
	assert.True(t, EventRecordIntervalAudit{}.FullyCorroborated(),
		"an audit of nothing is conclusive about nothing")
}
