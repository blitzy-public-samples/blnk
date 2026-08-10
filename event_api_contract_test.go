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

package blnk_test

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blnk "github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/middleware"
	apimodel "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/model"
)

// event_api_contract_test.go pins the CLIENT-FACING contract of the Kafka event-streaming
// surface: the request and response shapes in api/model/event.go, and the two authorization
// resource values in api/middleware/scope.go that the new routes resolve to.
//
// # Why this file exists
//
// Every type it covers is, at this point in the work, declared but not yet referenced by a
// handler. That combination — a published contract with no caller — is the one place a
// rename, a dropped omitempty or a pointer quietly demoted to a value passes every check
// the toolchain performs: the package compiles, go vet is silent, and no test notices,
// because nothing yet reads the shape. The first thing to notice would be a subscriber's
// parser failing against a deployed release. So each contract is asserted here directly:
// the exact JSON key of every field, in declaration order (encoding/json emits keys in that
// order, so the order is observable too), which keys disappear at their zero value, which
// fields are pointers because "absent" and "empty" mean different things, and which request
// fields are mandatory.
//
// The same reasoning covers ResourceEvents and ResourceSubscribers. Each value plays two
// roles at once — the first path segment of the new routes, and the resource half of an
// API-key scope string — and a typo in either would compile perfectly and simply reject
// every caller.
//
// # Why it lives in the repository root, in package blnk_test
//
// The two folders that hold the subjects are closed to new test files by the plan: api/model
// is covered from package api by the handler tests, and api/middleware likewise. This file
// respects both boundaries while still pinning the contracts now rather than a checkpoint
// later, and it sits with the rest of the event work under the root's event_*_test.go
// pattern.
//
// It is an EXTERNAL test package, and that is a requirement rather than a preference:
// api/middleware imports the root blnk package, so an in-package blnk test that imported the
// middleware would be an import cycle. The repository already uses external test packages
// where the same problem arises (internal/request, internal/apierror). Being external also
// means every assertion below is made through exactly the surface a real client of these
// packages sees.
//
// # What it deliberately does not assert
//
// Nothing here touches pathToResource in api/middleware/auth.go or the route registration in
// api/api.go. Both are later work, and getResourceFromPath fails closed for an unregistered
// prefix, so those are end-to-end concerns for the handler tests. This file pins the
// vocabulary half only: that the two values exist, that they are spelled exactly as the URL
// segments are, and that the scope machinery treats them like every other resource.

// fieldContract is one struct field's client-facing contract: the Go name, the JSON key it
// serialises under, whether the key vanishes at its zero value, and the binding tag that
// makes it mandatory on a request.
type fieldContract struct {
	name      string
	jsonKey   string
	omitEmpty bool
	binding   string
}

// assertWireContract asserts a struct's full field contract, in declaration order.
//
// The field COUNT is asserted alongside the per-field expectations so that neither a
// dropped field nor an undocumented addition can pass: an added field with a plausible tag
// is exactly the change that reaches a client unannounced.
func assertWireContract(t *testing.T, subject interface{}, contract []fieldContract) {
	t.Helper()

	subjectType := reflect.TypeOf(subject)
	require.Equal(t, reflect.Struct, subjectType.Kind(), "%s must be a struct", subjectType.Name())

	require.Equal(t, len(contract), subjectType.NumField(),
		"%s must declare exactly %d fields: a field added to a published API shape reaches every "+
			"client, and one removed breaks them",
		subjectType.Name(), len(contract))

	for index, expected := range contract {
		field := subjectType.Field(index)

		assert.Equal(t, expected.name, field.Name,
			"%s field %d must be %s — encoding/json emits keys in declaration order, so the order "+
				"is part of the observable shape", subjectType.Name(), index, expected.name)

		tag := field.Tag.Get("json")
		parts := strings.Split(tag, ",")

		assert.Equal(t, expected.jsonKey, parts[0],
			"%s.%s must serialise as %q: the JSON key is the contract clients read, and a rename "+
				"breaks them with no compile error anywhere",
			subjectType.Name(), field.Name, expected.jsonKey)

		hasOmitEmpty := false
		for _, option := range parts[1:] {
			if option == "omitempty" {
				hasOmitEmpty = true
			}
		}

		assert.Equal(t, expected.omitEmpty, hasOmitEmpty,
			"%s.%s omitempty must be %v: it decides whether a client sees an absent key or an "+
				"explicit zero, and those mean different things",
			subjectType.Name(), field.Name, expected.omitEmpty)

		assert.Equal(t, expected.binding, field.Tag.Get("binding"),
			"%s.%s binding tag must be %q: it is what makes a request field mandatory, and adding "+
				"one to a field callers legitimately omit rejects valid requests",
			subjectType.Name(), field.Name, expected.binding)
	}
}

// marshalToKeys marshals a value and returns its top-level keys as raw JSON, which is what
// lets an assertion distinguish an absent key from a null and from an empty value.
func marshalToKeys(t *testing.T, subject interface{}) map[string]json.RawMessage {
	t.Helper()

	encoded, err := json.Marshal(subject)
	require.NoError(t, err, "%T must serialise", subject)

	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &decoded), "%T must serialise as a JSON object", subject)

	return decoded
}

// keysOf returns a value's top-level JSON keys, for a set comparison.
func keysOf(t *testing.T, subject interface{}) []string {
	t.Helper()

	decoded := marshalToKeys(t, subject)
	keys := make([]string, 0, len(decoded))
	for key := range decoded {
		keys = append(keys, key)
	}

	return keys
}

// legacyWebhookPayload is the two-key object the legacy webhook transport POSTed and the
// exact bytes the outbox stores as an event payload. The inner keys are deliberately NOT in
// alphabetical order: that is what makes a re-encode through a Go map detectable.
const legacyWebhookPayload = `{"event":"transaction.applied","data":{"transaction_id":"txn_9f2","status":"APPLIED","amount":100}}`

// ---------------------------------------------------------------------------
// Event-streaming response shapes
// ---------------------------------------------------------------------------

// TestDeadLetterEvent_WireContract pins the item shape of GET /events/dead-letter.
//
// The projection is deliberately MINIMAL. It carries no payload, no raw failure text and no
// nested failure record, because a triage listing is read far more widely than the ledger
// itself and every one of those three carries either customer data or a broker's own error
// string straight out of the service. What replaces them is what triage actually needs: a
// CLASSIFIED failure reason, the two attempt instants, and the payload SIZE — enough to
// decide whether to replay without reproducing the event.
//
// The two attempt instants are pointers so a row that never failed omits them rather than
// emitting a zero time, which a client would otherwise have to recognise as "not applicable".
func TestDeadLetterEvent_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.DeadLetterEvent{}, []fieldContract{
		{name: "EventID", jsonKey: "event_id"},
		{name: "EventType", jsonKey: "event_type"},
		{name: "AggregateID", jsonKey: "aggregate_id"},
		{name: "LedgerID", jsonKey: "ledger_id", omitEmpty: true},
		{name: "PartitionKey", jsonKey: "partition_key", omitEmpty: true},
		{name: "OccurredAt", jsonKey: "occurred_at"},
		{name: "SchemaVersion", jsonKey: "schema_version"},
		{name: "Topic", jsonKey: "topic"},
		{name: "DLTTopic", jsonKey: "dlt_topic", omitEmpty: true},
		{name: "Status", jsonKey: "status"},
		{name: "Attempts", jsonKey: "attempts"},
		{name: "FailureReason", jsonKey: "failure_reason", omitEmpty: true},
		{name: "FirstAttemptedAt", jsonKey: "first_attempted_at", omitEmpty: true},
		{name: "LastAttemptedAt", jsonKey: "last_attempted_at", omitEmpty: true},
		{name: "PayloadBytes", jsonKey: "payload_bytes"},
		// AND NOTHING ELSE. resolved_at and resolution_note were declared here and have been
		// withdrawn with the endpoint that wrote them: an operator's resolution used to exempt
		// an entry from the retention purge and from the dead-letter age gauge, and it left the
		// row in a state from which a broker-acknowledged replay could not be recorded. Every
		// entry in this inventory is now outstanding by construction — a replay is what takes
		// one out of it — so there is nothing for the pair to distinguish.
	})

	t.Run("a bare entry omits every optional key and keeps the mandatory ones", func(t *testing.T) {
		// The state a row is in before it has been dead-lettered: no dead-letter topic, no
		// failure record, and — for a category with no ledger — no ledger id.
		keys := keysOf(t, apimodel.DeadLetterEvent{
			EventID:   "8f14e45f-ea8f-4b3a-9c2d-0a7b6c5d4e3f",
			EventType: "identity.created",
			Status:    model.EventOutboxStatusFailed,
			Topic:     "blnk.identities",
		})

		assert.ElementsMatch(t,
			[]string{
				"event_id", "event_type", "aggregate_id", "occurred_at", "schema_version",
				"topic", "status", "attempts", "payload_bytes",
			},
			keys,
			"the nine non-optional keys must always be present, and the six optional ones absent "+
				"rather than null, so a client never has to distinguish null from missing")
	})

	t.Run("the partition key and the ledger id are separate facts", func(t *testing.T) {
		// The Kafka message key IS the ordering mechanism: every event sharing a key lands
		// in one partition and is therefore consumed in publish order. This projection used
		// to carry only ledger_id and to DOCUMENT it as the key, which is wrong in the case
		// that matters most — a ledger-less event, where the key falls back to the aggregate
		// so the key is present while the ledger is not. An operator answering "why were
		// these two events consumed out of order?" from ledger_id would reach the wrong
		// conclusion for exactly the events whose routing is least obvious.
		decoded := marshalToKeys(t, apimodel.DeadLetterEvent{
			EventID:      "8f14e45f-ea8f-4b3a-9c2d-0a7b6c5d4e3f",
			EventType:    "identity.created",
			AggregateID:  "idt_7c9",
			PartitionKey: "idt_7c9",
			Topic:        "blnk.identities",
			Status:       model.EventOutboxStatusDeadLettered,
		})

		require.Contains(t, decoded, "partition_key",
			"the key the event was published under must be reportable, or no ordering question "+
				"can be answered from this API at all")
		assert.Equal(t, `"idt_7c9"`, string(decoded["partition_key"]))
		assert.NotContains(t, decoded, "ledger_id",
			"a ledger-less event has no ledger to report, and reporting the key in its place is "+
				"the conflation this field exists to end")

		// The other direction: a ledgered event reports both, and they are not required to
		// be equal — the stored key is the key the event was ACTUALLY written with, not one
		// recomputed from the row today.
		both := marshalToKeys(t, apimodel.DeadLetterEvent{
			EventID:      "9a25f56g-fb9g-5c4b-ad3e-1b8c7d6e5f4a",
			EventType:    "transaction.applied",
			AggregateID:  "txn_112",
			LedgerID:     "ldg_003",
			PartitionKey: "ldg_003",
			Topic:        "blnk.transactions",
			Status:       model.EventOutboxStatusDeadLettered,
		})

		assert.Equal(t, `"ldg_003"`, string(both["ledger_id"]))
		assert.Equal(t, `"ldg_003"`, string(both["partition_key"]))
	})

	t.Run("no key can carry a payload or a raw broker error", func(t *testing.T) {
		// The guarantee is STRUCTURAL: there is nowhere on the type to put either, so a
		// handler cannot leak one by mistake. Asserted over the field set rather than over one
		// marshalled instance, because an instance only proves what that instance held.
		shape := reflect.TypeOf(apimodel.DeadLetterEvent{})
		for _, forbidden := range []string{"Payload", "LastError", "FailureMetadata"} {
			_, present := shape.FieldByName(forbidden)
			assert.False(t, present,
				"%s must not exist on the triage projection: a dead-letter listing is read more "+
					"widely than the ledger, and the payload and the broker's own error text are "+
					"exactly what must not travel with it", forbidden)
		}
	})

	t.Run("the attempt instants are omitted for a row that never failed", func(t *testing.T) {
		first := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
		last := time.Date(2026, 3, 1, 10, 0, 31, 0, time.UTC)

		decoded := marshalToKeys(t, apimodel.DeadLetterEvent{
			EventID:          "8f14e45f-ea8f-4b3a-9c2d-0a7b6c5d4e3f",
			FailureReason:    "broker_unavailable",
			Attempts:         5,
			FirstAttemptedAt: &first,
			LastAttemptedAt:  &last,
			PayloadBytes:     len(legacyWebhookPayload),
		})

		require.Contains(t, decoded, "first_attempted_at")
		require.Contains(t, decoded, "last_attempted_at")
		assert.JSONEq(t, `"2026-03-01T10:00:00Z"`, string(decoded["first_attempted_at"]))
		assert.JSONEq(t, `"2026-03-01T10:00:31Z"`, string(decoded["last_attempted_at"]))

		bare := keysOf(t, apimodel.DeadLetterEvent{EventID: "evt"})
		assert.NotContains(t, bare, "first_attempted_at")
		assert.NotContains(t, bare, "last_attempted_at")
	})

	t.Run("the payload is reported by size rather than by value", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.DeadLetterEvent{
			EventID:      "8f14e45f-ea8f-4b3a-9c2d-0a7b6c5d4e3f",
			PayloadBytes: len(legacyWebhookPayload),
		})

		require.Contains(t, decoded, "payload_bytes")
		assert.Equal(t, strconv.Itoa(len(legacyWebhookPayload)), string(decoded["payload_bytes"]),
			"the size is what tells an operator whether a replay will fit inside the message "+
				"limit, and it discloses nothing about the event's contents")
		assert.NotContains(t, decoded, "payload")
	})
}

// TestReplayEventResponse_WireContract pins the body of
// POST /events/dead-letter/:event_id/replay.
func TestReplayEventResponse_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.ReplayEventResponse{}, []fieldContract{
		{name: "EventID", jsonKey: "event_id"},
		{name: "Topic", jsonKey: "topic"},
		{name: "Status", jsonKey: "status", omitEmpty: true},
		{name: "ReplayedAt", jsonKey: "replayed_at"},
	})

	t.Run("it echoes no payload", func(t *testing.T) {
		assert.NotContains(t, keysOf(t, apimodel.ReplayEventResponse{}), "payload",
			"replay fidelity is established by what the consumer receives on the original topic; "+
				"echoing the bytes here would invite callers to diff the wrong pair of strings")
	})

	t.Run("the reported status uses the publish vocabulary", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.ReplayEventResponse{
			EventID:    "evt_1",
			Topic:      "blnk.transactions",
			Status:     string(model.PublishStatusDispatched),
			ReplayedAt: time.Date(2026, 3, 2, 9, 30, 0, 0, time.UTC),
		})

		assert.Equal(t, `"dispatched"`, string(decoded["status"]),
			"the status is the model.PublishStatus vocabulary, so a client sees the same words the "+
				"metrics and the logs use")
		assert.Equal(t, `"2026-03-02T09:30:00Z"`, string(decoded["replayed_at"]),
			"timestamps on this surface are RFC3339 through time.Time's standard encoding")
	})
}

// TestEventOutboxStatsResponse_WireContract pins GET /events/stats, including the validity
// caveats without which its numbers cannot be trusted.
// dispatchedCount is the pointer form the dispatched count now takes on the wire.
//
// It is a pointer because dispatched is the ONE status count that is not always measured
// (PERF-M05): counting the single unbounded population is the deliberate on-demand reading, and
// emitting a zero for "nobody counted" would tell a reconciliation that nothing was published.
// A helper rather than a local variable at each site keeps these contract cases reading as the
// literals they are about.
func dispatchedCount(v int64) *int64 { return &v }

func TestEventOutboxStatsResponse_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.EventOutboxStatsResponse{}, []fieldContract{
		{name: "Pending", jsonKey: "pending"},
		{name: "Processing", jsonKey: "processing"},
		{name: "WebhookPending", jsonKey: "webhook_pending"},
		// PERF-M05: dispatched is the ONE count that is not always taken, so it is the one
		// count that omits. It is a count of the single unbounded population — 43.2 million
		// rows a day at the target rate — and is read only for a request that asked for the
		// broker side, so an emitted zero would say "nothing was dispatched today" when the
		// truth is "nobody counted". DispatchedHistoryCounted beside it is always emitted.
		{name: "Dispatched", jsonKey: "dispatched", omitEmpty: true},
		{name: "DispatchedHistoryCounted", jsonKey: "dispatched_history_counted"},
		{name: "Failed", jsonKey: "failed"},
		{name: "DeadLettered", jsonKey: "dead_lettered"},
		{name: "Replaying", jsonKey: "replaying"},
		{name: "ProducerAtomicity", jsonKey: "producer_atomicity", omitEmpty: true},
		{name: "TopicEndOffsets", jsonKey: "topic_end_offsets", omitEmpty: true},
		{name: "OffsetsComplete", jsonKey: "offsets_complete"},
		{name: "MissingTopics", jsonKey: "missing_topics", omitEmpty: true},
		{name: "PartitionsUnavailable", jsonKey: "partitions_unavailable"},
		// The measured windows are part of the published contract because the verdict is a
		// statement ABOUT them: each row's stored coordinate is checked for membership in the
		// window of its own partition. A verdict reported without its windows cannot be
		// audited by the reader, which is how the unbounded comparison this replaced came to
		// present itself as conclusive.
		{name: "MeasuredWindows", jsonKey: "measured_windows", omitEmpty: true},
		{name: "OffsetsMeasuredAt", jsonKey: "offsets_measured_at", omitEmpty: true},
		// PERF-P04/PERF-P05: the window the two unbounded figures were measured over. It is
		// part of the published contract because the dispatched count and the reconciliation
		// mean different things over different windows, and a caller that could not read the
		// window would be comparing yesterday's figure against today's.
		{name: "WindowStart", jsonKey: "window_start", omitEmpty: true},
		{name: "WindowSeconds", jsonKey: "window_seconds", omitEmpty: true},
		{name: "GeneratedAt", jsonKey: "generated_at"},
		{name: "Reconciliation", jsonKey: "reconciliation", omitEmpty: true},
	})

	// Driven from the model's OWN vocabulary rather than from a list written out here, which
	// is what makes it a completeness check instead of a restatement. A status added to the
	// model and not added to this response would otherwise vanish from the reconciliation
	// silently — and a status that is counted in the table but absent from the response makes
	// the reported counts sum to less than the row count, so a zero-loss check cannot tell a
	// short total from a lost event.
	t.Run("every outbox status in the model is reported as an explicit count", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.EventOutboxStatsResponse{})

		statuses := model.EventOutboxStatuses()
		require.NotEmpty(t, statuses, "the model must publish its status vocabulary for this to check anything")

		for _, status := range statuses {
			// DISPATCHED IS THE ONE EXCEPTION, and it is exercised in its own subtest below
			// (PERF-M05). Counting it is the deliberate on-demand reading rather than part of
			// every response, so an emitted zero would carry the one meaning a zero-loss check
			// cannot survive: "nothing was published" in place of "nobody counted".
			if status == model.EventOutboxStatusDispatched {
				continue
			}

			require.Contains(t, decoded, status,
				"the reconciliation runbook names every relay state, and a count of zero must read "+
					"as 'none in that state' rather than as 'no such state'. %q is in the model's "+
					"vocabulary but missing from EventOutboxStatsResponse", status)
			assert.Equal(t, "0", string(decoded[status]),
				"a zero count must be an explicit 0, never an omitted key")
		}
	})

	// The dispatched count's own contract, which is the inverse of every other count's: it is
	// present exactly when it was measured, and the flag beside it is present always.
	t.Run("the dispatched count is present only when it was taken, and says which", func(t *testing.T) {
		unmeasured := marshalToKeys(t, apimodel.EventOutboxStatsResponse{})

		assert.NotContains(t, unmeasured, "dispatched",
			"an uncounted dispatched population must OMIT the key: emitting 0 would tell a "+
				"reconciliation that nothing was published, which is the opposite of the truth "+
				"and is indistinguishable from a genuinely empty window")
		require.Contains(t, unmeasured, "dispatched_history_counted",
			"and the flag must be present so the omission is readable rather than merely safe")
		assert.Equal(t, "false", string(unmeasured["dispatched_history_counted"]))

		counted := int64(0)
		measuredEmpty := marshalToKeys(t, apimodel.EventOutboxStatsResponse{
			Dispatched:               &counted,
			DispatchedHistoryCounted: true,
		})

		require.Contains(t, measuredEmpty, "dispatched",
			"a MEASURED zero must be emitted: 'we counted, and the window held none' is a real "+
				"finding and is exactly what the omission above must not be confused with")
		assert.Equal(t, "0", string(measuredEmpty["dispatched"]))
		assert.Equal(t, "true", string(measuredEmpty["dispatched_history_counted"]))

		counted = 40
		measured := marshalToKeys(t, apimodel.EventOutboxStatsResponse{
			Dispatched:               &counted,
			DispatchedHistoryCounted: true,
		})

		assert.Equal(t, "40", string(measured["dispatched"]))
		assert.Equal(t, "true", string(measured["dispatched_history_counted"]))
	})

	// The replaying lease deserves its own subtest because it is the status that was
	// missing, and because it is the only NON-TERMINAL state a caller can mistake for a
	// terminal one: a row held by an in-flight replay is neither published nor lost.
	t.Run("the replaying lease is counted and is not terminal", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.EventOutboxStatsResponse{
			Dispatched:               dispatchedCount(40),
			DispatchedHistoryCounted: true,
			DeadLettered:             2,
			Replaying:                3,
			Reconciliation: &apimodel.OutboxReconciliationResult{
				TerminalEvents: 42,
			},
		})

		require.Contains(t, decoded, "replaying")
		assert.Equal(t, "3", string(decoded["replaying"]),
			"a replay in flight must be visible: without it the reported counts sum to less than the "+
				"table's row count and a shortfall cannot be told apart from loss")

		// 40 + 2, not 45. Rows in pending, processing or replaying make no claim to have
		// been published, so they must not enter the terminal total the broker side is
		// compared against.
		var reconciliation map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(decoded["reconciliation"], &reconciliation))
		assert.Equal(t, "42", string(reconciliation["terminal_events"]),
			"terminal events are dispatched plus dead-lettered only; a replay in flight has published "+
				"nothing and must not be counted as though it had")
	})

	t.Run("a reading with no offsets cannot be mistaken for a valid reconciliation", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.EventOutboxStatsResponse{
			Dispatched:               dispatchedCount(12),
			DispatchedHistoryCounted: true,
		})

		assert.NotContains(t, decoded, "topic_end_offsets",
			"a deployment with no brokers is a legitimate steady state: the key is omitted rather "+
				"than emitted as null, so a reconciliation script needs no special case")
		assert.NotContains(t, decoded, "offsets_measured_at",
			"there was no broker reading, so there is no instant to report")
		assert.NotContains(t, decoded, "missing_topics")

		require.Contains(t, decoded, "offsets_complete")
		assert.Equal(t, "false", string(decoded["offsets_complete"]),
			"with nothing read, the comparison is not valid — and the response must say so rather "+
				"than leaving a client to infer it from a missing key")
		require.Contains(t, decoded, "partitions_unavailable")
		assert.Equal(t, "0", string(decoded["partitions_unavailable"]))
	})

	t.Run("a partial reading reports which part is missing", func(t *testing.T) {
		measuredAt := time.Date(2026, 3, 3, 4, 5, 6, 0, time.UTC)
		decoded := marshalToKeys(t, apimodel.EventOutboxStatsResponse{
			Dispatched:               dispatchedCount(41),
			DispatchedHistoryCounted: true,
			DeadLettered:             1,
			TopicEndOffsets:          map[string]int64{"blnk.transactions": 40},
			OffsetsComplete:          false,
			MissingTopics:            []string{"blnk.identities"},
			PartitionsUnavailable:    2,
			OffsetsMeasuredAt:        &measuredAt,
			GeneratedAt:              measuredAt,
		})

		assert.Equal(t, "false", string(decoded["offsets_complete"]))
		assert.Equal(t, `["blnk.identities"]`, string(decoded["missing_topics"]),
			"the topic that lowered the broker side must be named, or its absence reads as loss")
		assert.Equal(t, "2", string(decoded["partitions_unavailable"]))
		assert.Equal(t, `"2026-03-03T04:05:06Z"`, string(decoded["offsets_measured_at"]),
			"the counts and the offsets are read from two different systems, so the instant the "+
				"broker side was read is part of interpreting any difference")
	})

	t.Run("a complete reading omits the caveat list but keeps the verdict", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.EventOutboxStatsResponse{
			TopicEndOffsets: map[string]int64{"blnk.transactions": 40},
			OffsetsComplete: true,
		})

		assert.Equal(t, "true", string(decoded["offsets_complete"]))
		assert.NotContains(t, decoded, "missing_topics",
			"nothing was missing, so an empty list is omitted rather than reported")
		assert.Equal(t, "0", string(decoded["partitions_unavailable"]),
			"an explicit zero is the reassuring answer and must never be omitted")
	})

	// The owed-event side of the reconciliation. The per-status counts above can only see
	// events that were CAPTURED, and two families are captured from an intent recorded
	// earlier, so a reading that omitted them would report a consistent table while alerts
	// and batch summaries were still owed.
	t.Run("outstanding producer intents are reported, and an unread reading is omitted rather than zeroed", func(t *testing.T) {
		unread := marshalToKeys(t, apimodel.EventOutboxStatsResponse{
			Dispatched:               dispatchedCount(7),
			DispatchedHistoryCounted: true,
		})

		assert.NotContains(t, unread, "producer_atomicity",
			"a failed read must omit the object: zero means 'nothing is owed', which is the one "+
				"answer a zero-loss check must not be given when the truth is 'we could not tell'")

		began := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
		read := marshalToKeys(t, apimodel.EventOutboxStatsResponse{
			Dispatched:               dispatchedCount(7),
			DispatchedHistoryCounted: true,
			ProducerAtomicity: &apimodel.ProducerAtomicityStats{
				MonitorHandoffPending:    2,
				MonitorHandoffFailed:     1,
				UnfinalizedBatches:       3,
				OldestUnfinalizedBatchAt: &began,
			},
		})

		require.Contains(t, read, "producer_atomicity",
			"a successful read must be reported even when everything in it is zero")

		var atomicity map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(read["producer_atomicity"], &atomicity))

		assert.Equal(t, "2", string(atomicity["monitor_handoff_pending"]))
		assert.Equal(t, "0", string(atomicity["monitor_handoff_processing"]),
			"a zero count is an explicit 0 here for the same reason it is above: an omitted key "+
				"reads as 'no such state'")
		assert.Equal(t, "1", string(atomicity["monitor_handoff_failed"]),
			"a spent evaluation budget is a monitor alert that will never exist, and it is the "+
				"number an operator alerts on")
		assert.Equal(t, "3", string(atomicity["unfinalized_batches"]))
		assert.Equal(t, `"2026-03-04T05:06:07Z"`, string(atomicity["oldest_unfinalized_batch_at"]),
			"age is what separates a large batch still running from an abandoned one, so the count "+
				"alone is not actionable and this is")
	})
}

// TestProducerAtomicityStats_WireContract pins the owed-event projection reported inside
// GET /events/stats.
//
// It is contracted separately from the response that carries it because it answers a
// different question. The per-status counts are a census of rows that EXIST; this object
// counts intents recorded atomically with a mutation from which an event has not been
// captured yet — events that are owed and are in no status at all. A reconciliation that
// read only the census would find it internally consistent while alerts and batch summaries
// were still pending, which is precisely the blind spot requirement R-2 closes.
func TestProducerAtomicityStats_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.ProducerAtomicityStats{}, []fieldContract{
		{name: "MonitorHandoffPending", jsonKey: "monitor_handoff_pending"},
		{name: "MonitorHandoffProcessing", jsonKey: "monitor_handoff_processing"},
		{name: "MonitorHandoffCompleted", jsonKey: "monitor_handoff_completed"},
		{name: "MonitorHandoffFailed", jsonKey: "monitor_handoff_failed"},
		{name: "UnfinalizedBatches", jsonKey: "unfinalized_batches"},
		{name: "OldestUnfinalizedBatchAt", jsonKey: "oldest_unfinalized_batch_at", omitEmpty: true},
	})

	// Driven from the handoff's OWN status vocabulary rather than from a list restated here,
	// for the same reason the per-status census is: the handoff reuses the lineage outbox
	// statuses, and one of them left uncounted would make the reported handoff totals sum to
	// less than the table's row count — a shortfall a zero-loss check cannot tell apart from
	// an evaluation that was silently dropped.
	t.Run("every handoff status is reported as an explicit count", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.ProducerAtomicityStats{})

		for _, status := range []string{
			model.OutboxStatusPending,
			model.OutboxStatusProcessing,
			model.OutboxStatusCompleted,
			model.OutboxStatusFailed,
		} {
			key := "monitor_handoff_" + status
			require.Contains(t, decoded, key,
				"the handoff moves through the lineage outbox statuses, and %q must be counted or "+
					"the reported totals sum to less than the table's row count", status)
			assert.Equal(t, "0", string(decoded[key]),
				"a zero count must be an explicit 0, never an omitted key")
		}
	})

	// The one window the coordinator cannot close, and the reason the timestamp is not
	// decoration: a batch that began and never reported is indistinguishable from a large
	// batch still running until you know how long ago it started.
	t.Run("no outstanding batch omits the age but still reports the count", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.ProducerAtomicityStats{})

		assert.Equal(t, "0", string(decoded["unfinalized_batches"]),
			"an explicit zero is the reassuring answer: nothing began without reporting")
		assert.NotContains(t, decoded, "oldest_unfinalized_batch_at",
			"there is no oldest outstanding batch, so there is no instant to report — and a null "+
				"would make a client parse a timestamp that does not exist")
	})
}

// TestOutboxReconciliationResult_WireContract pins the verdict acceptance criterion V-2 is
// scored on, and it exists because the response used to carry the two SIDES of the
// comparison and no verdict at all.
//
// Leaving the verdict to the caller meant every caller re-implemented the comparison, and each
// got its own chance to get it wrong in a way that reads as success. The comparison itself was
// then rebuilt, because the only one available from two totals was unsound: whole-topic
// cumulative end offsets and currently-retained outbox rows share no baseline, no readability
// guarantee, no topic incarnation and no producer. So the wire shape carries a BOUNDED MAPPING
// — how many claims were placed inside a measured offset window, and one count per reason the
// rest could not be — plus the window the verdict covers.
func TestOutboxReconciliationResult_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.OutboxReconciliationResult{}, []fieldContract{
		{name: "TerminalEvents", jsonKey: "terminal_events"},
		// The corroboration counts sit here rather than at the end, mirroring the service
		// type's own declaration order: encoding/json emits keys in declaration order, so a
		// reader comparing a response against blnk.OutboxReconciliation reads the two in the
		// same sequence.
		{name: "CorroboratedEvents", jsonKey: "corroborated_events"},
		{name: "UnconfirmedEvents", jsonKey: "unconfirmed_events"},
		{name: "UnmeasuredEvents", jsonKey: "unmeasured_events"},
		{name: "AgedOutEvents", jsonKey: "aged_out_events"},
		{name: "BeyondEndEvents", jsonKey: "beyond_end_events"},
		{name: "DuplicatedRecords", jsonKey: "duplicated_records"},
		{name: "MessagesWritten", jsonKey: "messages_written"},
		// records_retained and blnk_record_share are the SHARED-TOPIC correction, and they are
		// on the wire because without them a shared log cannot be reconciled at all. An end
		// offset counts every record the topic ever accepted from every producer; retention
		// then deletes an arbitrary prefix of them. Measuring Blnk's surplus against the whole
		// retained log therefore compares Blnk's events against somebody else's traffic, and
		// the answer moves whenever that traffic does. blnk_record_share is the part of the
		// retained log this reconciliation actually attributed to Blnk's own published events,
		// which is the only denominator the surplus means anything against. Neither is the
		// verdict — that is what conclusive and loss_detected are for — and both are reported
		// so an operator reading a surplus can see which log it was drawn from.
		{name: "RecordsRetained", jsonKey: "records_retained"},
		{name: "BlnkRecordShare", jsonKey: "blnk_record_share"},
		// FIVE FIELDS WERE RETIRED FROM BETWEEN THESE TWO, and their absence is the point.
		// purged_events, all_time_terminal_events, verified_records, unverifiable_records and
		// missing_records belonged to a whole-history reconciliation that corrected for retention
		// with a purge log and checked each claimed coordinate against the broker in Go. Both
		// mechanisms were replaced by drawing the comparison INSIDE the per-partition windows the
		// offsets were measured in and classifying every claim in SQL over those same windows,
		// which is what corroborated / aged_out / beyond_end / unmeasured above report. Their
		// service-layer counterparts went with the rewrite; leaving the wire half behind meant
		// five documented fields that could only ever be zero, and a reconciliation script
		// reading verified_records: 0 beside a green verdict concludes nothing was verified.
		{name: "Overhead", jsonKey: "overhead"},
		{name: "LossDetected", jsonKey: "loss_detected"},
		{name: "Conclusive", jsonKey: "conclusive"},
		// PERF-P05: the population the verdict compared. Windowed is NOT omitempty — a
		// diagnostic-only verdict must say so explicitly, and an omitted false would read as
		// a field the server forgot rather than as a comparison it declined to conclude from.
		{name: "WindowStart", jsonKey: "window_start", omitEmpty: true},
		{name: "Windowed", jsonKey: "windowed"},
		{name: "Caveats", jsonKey: "caveats", omitEmpty: true},
		{name: "CoveredFrom", jsonKey: "covered_from", omitEmpty: true},
		{name: "CoveredTo", jsonKey: "covered_to", omitEmpty: true},
		{name: "OldestTerminalAt", jsonKey: "oldest_terminal_at", omitEmpty: true},
		{name: "Summary", jsonKey: "summary"},
		{name: "MeasuredAt", jsonKey: "measured_at"},
	})

	t.Run("the field set mirrors the service-layer verdict", func(t *testing.T) {
		// The API shape and the computed verdict must not drift: a field added to the
		// service-layer OutboxReconciliation and not surfaced here is a finding the daily
		// check produces and the API silently withholds. Compared as SETS of names, because
		// the two types legitimately differ in wire tags, in Summary's form — a method on one,
		// a field on the other — and in the time fields' nullability, which the wire needs and
		// the computation does not.
		wire := reflect.TypeOf(apimodel.OutboxReconciliationResult{})
		service := reflect.TypeOf(blnk.OutboxReconciliation{})

		for i := 0; i < service.NumField(); i++ {
			name := service.Field(i).Name
			_, present := wire.FieldByName(name)
			assert.True(t, present,
				"OutboxReconciliation.%s is computed by the daily check but has nowhere to go on the "+
					"API response, so the finding it carries would never reach a caller", name)
		}

		// Summary is a METHOD on the service type and a FIELD here, deliberately: it is the
		// one sentence a runbook or an alert annotation quotes, and recomputing it on the
		// client would mean two descriptions of one verdict.
		_, hasSummary := service.MethodByName("Summary")
		assert.True(t, hasSummary,
			"the wire Summary must be rendered by the service's own method rather than reworded here")
	})

	t.Run("a healthy result states what it corroborated and over which window", func(t *testing.T) {
		measuredAt := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
		from := measuredAt.Add(-24 * time.Hour)
		decoded := marshalToKeys(t, apimodel.OutboxReconciliationResult{
			TerminalEvents:     1000,
			CorroboratedEvents: 1000,
			MessagesWritten:    1003,
			RecordsRetained:    1003,
			BlnkRecordShare:    1000,
			LossDetected:       false,
			Conclusive:         true,
			CoveredFrom:        &from,
			CoveredTo:          &measuredAt,
			OldestTerminalAt:   &from,
			Summary:            "every one of the 1000 outbox rows names a distinct record",
			MeasuredAt:         measuredAt,
		})

		assert.Equal(t, "1000", string(decoded["corroborated_events"]),
			"the corroborated count IS the green verdict; a caller must be able to see it rather "+
				"than infer it from a difference of totals")
		assert.Equal(t, "1000", string(decoded["blnk_record_share"]),
			"how much of a shared topic's traffic the verdict accounts for is not derivable from "+
				"messages_written, so it is reported")
		assert.Equal(t, "false", string(decoded["loss_detected"]))
		assert.Equal(t, "true", string(decoded["conclusive"]))
		assert.Equal(t, `"2026-03-03T05:06:07Z"`, string(decoded["covered_from"]),
			"a green verdict must state the window it covers, or a reconciliation over a pruned "+
				"outbox looks complete")
		assert.NotContains(t, decoded, "caveats",
			"a conclusive result has no caveats, so an empty list is omitted rather than reported")
		assert.Equal(t, `"2026-03-04T05:06:07Z"`, string(decoded["measured_at"]))
	})

	t.Run("a record beyond the log end is reported as loss with its own count", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.OutboxReconciliationResult{
			TerminalEvents:     1000,
			CorroboratedEvents: 960,
			BeyondEndEvents:    40,
			MessagesWritten:    40,
			LossDetected:       true,
			Conclusive:         false,
			Caveats: []string{
				"40 row(s) name an offset at or beyond the end of their partition's log",
			},
		})

		assert.Equal(t, "40", string(decoded["beyond_end_events"]),
			"the count that SETS loss_detected must be on the wire: without it a caller cannot "+
				"tell a recreated topic from any other inconclusive reading")
		assert.Equal(t, "true", string(decoded["loss_detected"]))
	})

	t.Run("each reason a claim could not be placed has its own key", func(t *testing.T) {
		// Collapsing any two of these would destroy the distinction an operator acts on:
		// retention is routine, an unmeasured partition is a broker problem, an unconfirmed
		// row is a relay problem, and a beyond-end offset is a recreated topic.
		decoded := marshalToKeys(t, apimodel.OutboxReconciliationResult{
			TerminalEvents:     100,
			CorroboratedEvents: 60,
			UnconfirmedEvents:  10,
			UnmeasuredEvents:   12,
			AgedOutEvents:      15,
			BeyondEndEvents:    3,
			Conclusive:         false,
		})

		for key, want := range map[string]string{
			"unconfirmed_events": "10",
			"unmeasured_events":  "12",
			"aged_out_events":    "15",
			"beyond_end_events":  "3",
		} {
			assert.Equal(t, want, string(decoded[key]),
				"%s must be reported on its own, and never omitted: a zero is the reassuring "+
					"answer and an absent key reads as one", key)
		}
	})

	t.Run("an inconclusive result is never mistaken for a clean one", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.OutboxReconciliationResult{
			TerminalEvents:     1000,
			CorroboratedEvents: 400,
			AgedOutEvents:      600,
			MessagesWritten:    1000,
			LossDetected:       false,
			Conclusive:         false,
			Caveats: []string{
				"600 row(s) name a record Kafka retention has already deleted",
			},
		})

		require.Contains(t, decoded, "conclusive")
		assert.Equal(t, "false", string(decoded["conclusive"]),
			"the key must be present on the inconclusive path: a vanishing key would leave the most "+
				"dangerous state looking like the healthiest one")
		assert.Equal(t, `["600 row(s) name a record Kafka retention has already deleted"]`,
			string(decoded["caveats"]),
			"an operator must be told WHY the mapping is incomplete, not merely that it is")
	})

	t.Run("a stats response with no offsets carries no verdict and no windows", func(t *testing.T) {
		// With nothing measured on the broker side there is no verdict to report, and
		// emitting an empty one would read as "reconciled, nothing written".
		decoded := marshalToKeys(t, apimodel.EventOutboxStatsResponse{
			Dispatched:               dispatchedCount(12),
			DispatchedHistoryCounted: true,
		})

		assert.NotContains(t, decoded, "reconciliation",
			"a deployment with no brokers is a legitimate steady state; an absent verdict is honest "+
				"where a zeroed one would be a false all-clear")
		assert.NotContains(t, decoded, "measured_windows",
			"no windows were measured, so none are reported rather than an empty array that reads "+
				"as 'the broker has no partitions'")
	})
}

// TestMeasuredOffsetWindow_WireContract pins the windows the verdict is computed against.
//
// They are on the response because the verdict is a statement ABOUT them: each outbox row's
// stored coordinate is checked for membership in the window of its own partition. A verdict
// reported without its windows cannot be audited by the reader — which is exactly how the
// unbounded comparison this replaced came to present itself as conclusive.
func TestMeasuredOffsetWindow_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.MeasuredOffsetWindow{}, []fieldContract{
		{name: "Topic", jsonKey: "topic"},
		{name: "Partition", jsonKey: "partition"},
		{name: "FirstOffset", jsonKey: "first_offset"},
		{name: "EndOffset", jsonKey: "end_offset"},
		{name: "Records", jsonKey: "records"},
	})

	t.Run("the projection carries every measured window, in order", func(t *testing.T) {
		windows := apimodel.NewMeasuredOffsetWindows([]model.PartitionOffsetInterval{
			{Topic: "blnk.transactions", Partition: 0, FirstOffset: 0, EndOffset: 500},
			{Topic: "blnk.transactions", Partition: 1, FirstOffset: 120, EndOffset: 640},
			{Topic: "blnk.balances", Partition: 0, FirstOffset: 9_000, EndOffset: 9_000},
		})

		require.Len(t, windows, 3)
		assert.Equal(t, apimodel.MeasuredOffsetWindow{
			Topic: "blnk.transactions", Partition: 0, FirstOffset: 0, EndOffset: 500, Records: 500,
		}, windows[0])
		assert.Equal(t, int64(520), windows[1].Records,
			"the width is reported so a reader does not have to subtract")
		assert.Zero(t, windows[2].Records,
			"a partition every record of which has aged out is a real, measured window of zero "+
				"records, not a missing one")
	})

	t.Run("partition zero and offset zero survive the projection", func(t *testing.T) {
		// The first record on a fresh partition is at 0/0, so no field here may be omitempty:
		// a response that dropped them would describe a window starting nowhere.
		decoded := marshalToKeys(t, apimodel.MeasuredOffsetWindow{Topic: "blnk.system"})

		assert.Equal(t, "0", string(decoded["partition"]))
		assert.Equal(t, "0", string(decoded["first_offset"]))
		assert.Equal(t, "0", string(decoded["end_offset"]))
		assert.Equal(t, "0", string(decoded["records"]))
	})

	t.Run("nothing measured projects to nothing", func(t *testing.T) {
		assert.Nil(t, apimodel.NewMeasuredOffsetWindows(nil),
			"a nil projection lets the response omit the key rather than report an empty array")
	})
}

// ---------------------------------------------------------------------------
// Subscriber shapes
// ---------------------------------------------------------------------------

// TestCreateSubscriber_RequestContract pins the POST /subscribers body.
func TestCreateSubscriber_RequestContract(t *testing.T) {
	assertWireContract(t, apimodel.CreateSubscriber{}, []fieldContract{
		{name: "SubscriberID", jsonKey: "subscriber_id"},
		// NO binding:"required" on Name. The requirement is real and is enforced by
		// Validate; the tag made a missing name a BINDER failure, answering
		// GEN_MALFORMED_REQUEST where the endpoint's contract and every other rule this
		// DTO applies answer GEN_VALIDATION_ERROR. It also split one rule across two
		// mechanisms, so an omitted name and a whitespace-only one produced different
		// codes for the same broken rule.
		{name: "Name", jsonKey: "name"},
		{name: "AuthorizedTopics", jsonKey: "authorized_topics", binding: "max=16,dive,max=249"},
		{name: "PartitionKeyPrefix", jsonKey: "partition_key_prefix", omitEmpty: true},
		// NO WebhookURL. C-01: this route is not deprecated and is not fronted by the
		// sunset guard, so accepting legacy webhook state here left a write path open
		// after the four guarded routes had begun answering 410 Gone.
	})

	t.Run("the principal and the consumer group are not accepted from a client", func(t *testing.T) {
		// Both are DERIVED from the subscriber identifier, and the derivation is enforced
		// by CHECK constraints on blnk.event_subscribers rather than by this shape alone.
		// Accepting either here would let a caller name a principal that does not belong
		// to its own identifier — the principal is the join key to every ACL binding, so a
		// caller choosing it is a caller choosing which subscriber's grants it inherits —
		// and it would let a row be written that the schema then rejects, which surfaces
		// as a server fault for what is really a rejected request.
		subjectType := reflect.TypeOf(apimodel.CreateSubscriber{})

		for _, derived := range []string{"KafkaPrincipal", "ConsumerGroupID"} {
			_, present := subjectType.FieldByName(derived)
			assert.False(t, present,
				"%s must not be settable on create: it is derived from subscriber_id", derived)
		}
	})

	t.Run("no field is mandatory at the BINDER, name included", func(t *testing.T) {
		// C-24. name IS required, and the requirement now lives in Validate rather than in
		// a binding tag, so a missing one answers GEN_VALIDATION_ERROR like every other
		// broken rule this DTO applies instead of the binder's GEN_MALFORMED_REQUEST. The
		// binder keeps only what it is the right layer for: TYPE errors, and the bounds
		// below.
		subjectType := reflect.TypeOf(apimodel.CreateSubscriber{})

		for index := 0; index < subjectType.NumField(); index++ {
			field := subjectType.Field(index)

			// A field may carry BOUNDS without being mandatory, and the distinction is the
			// point: authorized_topics is bounded so an oversized grant is refused at the
			// binding layer instead of composing a very large ACL request, but omitting it
			// entirely is still legal and yields the fail-closed empty grant.
			binding := field.Tag.Get("binding")
			assert.NotContains(t, strings.Split(binding, ","), "required",
				"%s must not be binder-mandatory: the service derives the id when it is omitted, "+
					"an empty topic grant is the deliberate fail-closed default rather than a "+
					"missing value, and name's requirement belongs to Validate so that an omitted "+
					"and a blank name answer the same code", field.Name)
		}
	})

	t.Run("an unknown meta_data key is tolerated", func(t *testing.T) {
		// The API-key middleware rewrites every POST body from a non-master caller, injecting
		// meta_data.BLNK_GENERATED_BY. No field is declared for it — neither table has a
		// meta_data column — so the shape has to ignore it rather than reject the request.
		body := `{"name":"ledger-ops","meta_data":{"BLNK_GENERATED_BY":"key_9f2"}}`

		var request apimodel.CreateSubscriber
		require.NoError(t, json.Unmarshal([]byte(body), &request),
			"binding must stay non-strict; a DisallowUnknownFields decoder here would reject every "+
				"request made with a scoped API key")
		assert.Equal(t, "ledger-ops", request.Name)
	})
}

// TestUpdateSubscriber_RequestContract pins PUT /subscribers/:subscriber_id, whose whole
// point is telling "omitted" from "set to empty".
func TestUpdateSubscriber_RequestContract(t *testing.T) {
	assertWireContract(t, apimodel.UpdateSubscriber{}, []fieldContract{
		{name: "Name", jsonKey: "name", omitEmpty: true, binding: "omitempty,max=1024"},
		{
			name:      "AuthorizedTopics",
			jsonKey:   "authorized_topics",
			omitEmpty: true,
			binding:   "omitempty,max=16,dive,max=249",
		},
		{name: "PartitionKeyPrefix", jsonKey: "partition_key_prefix", omitEmpty: true},
		// NO WebhookURL, for the reason given on CreateSubscriber. PUT
		// /subscribers/:subscriber_id/webhook-subscription is the guarded write path, and
		// the destination policy it applies is unchanged.
	})

	t.Run("every scalar is a pointer so absent and empty stay distinguishable", func(t *testing.T) {
		subjectType := reflect.TypeOf(apimodel.UpdateSubscriber{})

		for _, name := range []string{"Name", "PartitionKeyPrefix"} {
			field, ok := subjectType.FieldByName(name)
			require.True(t, ok, "UpdateSubscriber must declare %s", name)
			assert.Equal(t, reflect.Ptr, field.Type.Kind(),
				"%s must be a pointer: for partition_key_prefix, absent means 'entitled to whole "+
					"topics' while an empty string means 'restricted to the empty prefix', and a "+
					"plain string would make silently inverting the operator's intent possible", name)
		}

		topics, ok := subjectType.FieldByName("AuthorizedTopics")
		require.True(t, ok)
		assert.Equal(t, reflect.Slice, topics.Type.Kind(),
			"a slice is already nilable, so no pointer is needed: nil is omitted and a present "+
				"empty array revokes every topic grant")
	})

	t.Run("omitted and explicitly empty decode differently", func(t *testing.T) {
		var omitted apimodel.UpdateSubscriber
		require.NoError(t, json.Unmarshal([]byte(`{"name":"ledger-ops"}`), &omitted))
		assert.Nil(t, omitted.PartitionKeyPrefix, "an absent key must leave the field nil")
		assert.Nil(t, omitted.AuthorizedTopics, "an absent array must stay nil, not become empty")

		var cleared apimodel.UpdateSubscriber
		require.NoError(t, json.Unmarshal([]byte(`{"partition_key_prefix":"","authorized_topics":[]}`), &cleared))
		require.NotNil(t, cleared.PartitionKeyPrefix,
			"an explicit empty string must be visible as a set value, because it clears the "+
				"restriction back to whole topics")
		assert.Equal(t, "", *cleared.PartitionKeyPrefix)
		require.NotNil(t, cleared.AuthorizedTopics,
			"a present empty array must be visible as a set value, because it revokes every grant")
		assert.Empty(t, cleared.AuthorizedTopics)
	})

	t.Run("nothing a client must not own is accepted", func(t *testing.T) {
		subjectType := reflect.TypeOf(apimodel.UpdateSubscriber{})

		for _, forbidden := range []string{
			"SubscriberID", "KafkaPrincipal", "ConsumerGroupID",
			"CredentialReference", "CredentialFingerprint", "CredentialIssuedAt",
			"MigratedAt", "Password",
		} {
			_, present := subjectType.FieldByName(forbidden)
			assert.False(t, present,
				"%s must not be settable through an update: the identifier addresses the row, the "+
					"principal and the consumer group are DERIVED from that identifier and are the "+
					"join keys to every ACL already bound, the credential fields are the record of "+
					"an issuance the service alone writes, and no request shape anywhere accepts a "+
					"secret", forbidden)
		}
	})
}

// TestSubscriberResponse_WireContract pins the subscriber read shape.
func TestSubscriberResponse_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.SubscriberResponse{}, []fieldContract{
		{name: "SubscriberID", jsonKey: "subscriber_id"},
		// Not omitempty, and that is the contract rather than an oversight: the pseudonym is
		// the ONLY way to resolve a token read off a lag alert or a log line back to a
		// subscriber, so a response that omitted it for some rows would leave those rows
		// unreachable from the observability surface. It is derived from subscriber_id, which
		// is never blank, so there is no legitimate empty case to omit.
		{name: "SubscriberIDHash", jsonKey: "subscriber_id_hash"},
		{name: "Name", jsonKey: "name"},
		{name: "KafkaPrincipal", jsonKey: "kafka_principal"},
		{name: "ConsumerGroupID", jsonKey: "consumer_group_id"},
		{name: "AuthorizedTopics", jsonKey: "authorized_topics"},
		{name: "PartitionKeyPrefix", jsonKey: "partition_key_prefix", omitEmpty: true},
		{name: "EnforcedAccess", jsonKey: "enforced_access"},
		// Present unconditionally, and both of them deliberately. A client that had to infer
		// "no credential will be issued for this row" from the ABSENCE of a field would infer
		// it wrong, and the state was previously discoverable only by triggering the refusal on
		// a later call.
		{name: "CredentialIssuanceBlocked", jsonKey: "credential_issuance_blocked"},
		{name: "CredentialIssuanceBlockedReason", jsonKey: "credential_issuance_blocked_reason", omitEmpty: true},
		{name: "CredentialFingerprint", jsonKey: "credential_fingerprint", omitEmpty: true},
		{name: "CredentialIssuedAt", jsonKey: "credential_issued_at", omitEmpty: true},
		// NO WebhookURL. C-01: the recorded endpoint is disclosed by GET
		// /subscribers/:subscriber_id/webhook-subscription alone, which the sunset guard
		// fronts, so it stops being readable at the retirement instant. Echoing it here
		// as well kept it readable through an unguarded route afterwards, and the two
		// reads then disagreed about whether the legacy surface still existed.
		//
		// MigratedAt STAYS: it is migration progress about this deployment rather than
		// legacy state — no endpoint and no third-party data — and a progress report
		// needs it on both sides of the sunset.
		{name: "MigratedAt", jsonKey: "migrated_at", omitEmpty: true},
		// ORPHAN-01: the three unsettled-state markers. They are PROJECTED rather than
		// database-only because both alerts that fire on them tell an operator to find the
		// affected rows through this API, and before this the answer was "read the table" — a
		// marker an operator cannot see through the registry is a marker they cannot triage.
		// All three are omitempty: present always means something needs attention.
		{name: "RevocationPendingAt", jsonKey: "revocation_pending_at", omitEmpty: true},
		{name: "RevocationFailedAt", jsonKey: "revocation_failed_at", omitEmpty: true},
		{name: "CredentialOrphanedAt", jsonKey: "credential_orphaned_at", omitEmpty: true},
		{name: "CreatedAt", jsonKey: "created_at"},
		{name: "UpdatedAt", jsonKey: "updated_at"},
		// The revocation pair, at the end because it was added last. The boolean is present
		// unconditionally for the same reason CredentialIssuanceBlocked is — a client inferring
		// it from an absent field would infer it wrong — and the reason accompanies it because
		// docs/metrics.md answers the revocation alert with "find the affected subscribers with
		// GET /subscribers", so the response has to carry the remedy and not only the state.
		{name: "RevocationPending", jsonKey: "revocation_pending"},
		{name: "RevocationPendingReason", jsonKey: "revocation_pending_reason", omitEmpty: true},
	})

	t.Run("a registered subscriber with no credential is representable", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.SubscriberResponse{
			SubscriberID:     "sub_9f2",
			Name:             "ledger-ops",
			KafkaPrincipal:   "blnk-subscriber-ledger-ops",
			ConsumerGroupID:  "blnk-sub-ledger-ops",
			AuthorizedTopics: []string{},
		})

		assert.NotContains(t, decoded, "credential_issued_at",
			"a nil issuance instant is the reliable test for 'registered, but never provisioned', "+
				"which is a real state the registry has to represent")
		assert.NotContains(t, decoded, "credential_reference")
		assert.Equal(t, "[]", string(decoded["authorized_topics"]),
			"an empty grant is reported rather than omitted: it is a meaningful fail-closed state "+
				"an operator needs to see")
	})

	t.Run("the stored credential reference is never published, only a fingerprint", func(t *testing.T) {
		// The read shape carries a SHORT DIGEST FRAGMENT of the reference, not the
		// reference. The reference is not a secret — it is non-reversible and nobody can
		// authenticate with it — but it is internal correlation state, and publishing it
		// invites a client to send it back or compare against it, which makes it part of
		// the API contract by accident. The fingerprint answers the only legitimate
		// question ("is this the same issuance I saw last time?") and nothing else.
		subjectType := reflect.TypeOf(apimodel.SubscriberResponse{})

		_, present := subjectType.FieldByName("CredentialReference")
		assert.False(t, present, "the full reference must not be a field of the read shape")

		issued := time.Date(2026, 3, 3, 4, 5, 6, 0, time.UTC)
		reference, err := model.DeriveCredentialReference("blnk-sub-sub_9f2", "a-generated-secret")
		require.NoError(t, err)
		decoded := marshalToKeys(t, apimodel.SubscriberResponse{
			SubscriberID:          "sub_9f2",
			Name:                  "ledger-ops",
			CredentialFingerprint: model.CredentialFingerprint(reference),
			CredentialIssuedAt:    &issued,
		})

		require.Contains(t, decoded, "credential_fingerprint")
		fingerprint := string(decoded["credential_fingerprint"])
		assert.NotContains(t, decoded, "credential_reference")
		assert.Len(t, fingerprint, model.CredentialFingerprintLen+2,
			"the fingerprint is a fixed-length fragment, quoted")
		assert.NotContains(t, string(decoded["credential_fingerprint"]), reference,
			"the stored reference may not be reproduced verbatim")
		assert.NotEqual(t, `"`+reference+`"`, fingerprint)
	})

	t.Run("the nullable instants are pointers", func(t *testing.T) {
		subjectType := reflect.TypeOf(apimodel.SubscriberResponse{})

		for _, name := range []string{"CredentialIssuedAt", "MigratedAt"} {
			field, ok := subjectType.FieldByName(name)
			require.True(t, ok)
			assert.Equal(t, reflect.TypeOf(&time.Time{}), field.Type,
				"%s is nullable in blnk.event_subscribers and must be a *time.Time, so 'never' is "+
					"an absent key rather than the zero instant", name)
		}

		for _, name := range []string{"CreatedAt", "UpdatedAt"} {
			field, ok := subjectType.FieldByName(name)
			require.True(t, ok)
			assert.Equal(t, reflect.TypeOf(time.Time{}), field.Type,
				"%s is always known, so it is a plain time.Time", name)
		}
	})
}

// TestKafkaCredentialsResponse_WireContract pins the one-time credential issuance body of
// POST /subscribers/:subscriber_id/kafka-credentials.
func TestKafkaCredentialsResponse_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.KafkaCredentialsResponse{}, []fieldContract{
		{name: "Brokers", jsonKey: "brokers"},
		{name: "BrokerEndpoint", jsonKey: "broker_endpoint", omitEmpty: true},
		{name: "AuthorizedTopics", jsonKey: "authorized_topics"},
		{name: "ConsumerGroupID", jsonKey: "consumer_group_id"},
		{name: "EnforcedAccess", jsonKey: "enforced_access"},
		{name: "Username", jsonKey: "username"},
		{name: "Password", jsonKey: "password"},
		{name: "Mechanism", jsonKey: "mechanism"},
		{name: "IssuedAt", jsonKey: "issued_at"},
		// The two the service established and the response used to drop. Neither is omitempty:
		// an empty fingerprint and a false Replaced are both meaningful readings, and omitting
		// them would make "not replaced" indistinguishable from "the field is not returned".
		{name: "CredentialFingerprint", jsonKey: "credential_fingerprint"},
		{name: "Replaced", jsonKey: "replaced"},
	})

	t.Run("the fingerprint and the replacement flag reach the caller", func(t *testing.T) {
		// The password is returned once and nothing persists it, so the FINGERPRINT is the only
		// handle a client has on an issuance afterwards — and it is the same value a subscriber
		// read reports, which is what makes the two comparable. REPLACED is the destructive-action
		// confirmation: Kafka stores one credential per principal, so true means a live consumer's
		// password has just stopped working.
		decoded := marshalToKeys(t, apimodel.KafkaCredentialsResponse{
			CredentialFingerprint: "9f1c8a72",
			Replaced:              true,
		})

		assert.Equal(t, `"9f1c8a72"`, string(decoded["credential_fingerprint"]))
		assert.Equal(t, "true", string(decoded["replaced"]))

		// AND BOTH ARE PRESENT IN THEIR ZERO FORM, which is the property omitempty would break.
		zero := marshalToKeys(t, apimodel.KafkaCredentialsResponse{})
		assert.Contains(t, zero, "replaced",
			"a first issuance must say so rather than omitting the field")
		assert.Equal(t, "false", string(zero["replaced"]))
	})

	t.Run("everything a subscriber needs to start consuming is present at once", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.KafkaCredentialsResponse{
			Brokers:          []string{"kafka-0:9092", "kafka-1:9092"},
			BrokerEndpoint:   "kafka-0:9092,kafka-1:9092",
			AuthorizedTopics: []string{"blnk.transactions"},
			ConsumerGroupID:  "blnk-sub-ledger-ops",
			Username:         "blnk-subscriber-ledger-ops",
			Password:         "issued-once-never-stored",
			Mechanism:        "SCRAM-SHA-512",
			IssuedAt:         time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC),
		})

		for _, key := range []string{
			"brokers", "broker_endpoint", "authorized_topics", "consumer_group_id",
			"username", "password", "mechanism", "issued_at",
		} {
			assert.Contains(t, decoded, key,
				"%q is part of the issuance contract; without it the subscriber cannot connect", key)
		}
		assert.Equal(t, `"SCRAM-SHA-512"`, string(decoded["mechanism"]),
			"the mechanism is reported so a client configures the same one the broker was seeded with")
	})

	t.Run("a response-only shape carries no binding tags", func(t *testing.T) {
		subjectType := reflect.TypeOf(apimodel.KafkaCredentialsResponse{})

		for index := 0; index < subjectType.NumField(); index++ {
			assert.Empty(t, subjectType.Field(index).Tag.Get("binding"),
				"%s is response-only; a binding tag on it is meaningless and misleading",
				subjectType.Field(index).Name)
		}
	})
}

// TestEventAPIShapes_CarryNoSecretOutsideTheIssuanceResponse is the falsifiable form of the
// secret-handling posture.
//
// One type may carry a password, and exactly one does. The guarantee is structurally backed
// — blnk.event_subscribers has no column capable of holding a plaintext or reversibly
// encrypted secret, so a field on any read shape could never be populated from persistence
// and could only ever leak one — but "structurally backed" is not the same as "checked", and
// a field added later would compile silently. This checks it.
//
// The scan covers the deprecated webhook-subscription shapes too, and must: they are live
// until the sunset date, and the legacy transport's signing secret and configured headers are
// exactly the sort of deployment-wide value somebody might be tempted to surface per
// subscriber. Naming them is what lets them be checked, hence the local suppression.
//
//nolint:staticcheck // SA1019: the deprecated shapes ship until sunset and must be scanned too.
func TestEventAPIShapes_CarryNoSecretOutsideTheIssuanceResponse(t *testing.T) {
	forbiddenFragments := []string{"password", "secret", "passphrase", "plaintext", "credentials"}

	shapes := []interface{}{
		apimodel.DeadLetterEvent{},
		apimodel.ReplayEventResponse{},
		apimodel.EventOutboxStatsResponse{},
		apimodel.CreateSubscriber{},
		apimodel.UpdateSubscriber{},
		apimodel.SubscriberResponse{},
		apimodel.CreateWebhookSubscription{},
		apimodel.UpdateWebhookSubscription{},
		apimodel.WebhookSubscriptionResponse{},
	}

	for _, shape := range shapes {
		shapeType := reflect.TypeOf(shape)

		for index := 0; index < shapeType.NumField(); index++ {
			field := shapeType.Field(index)
			name := strings.ToLower(field.Name)
			key := strings.ToLower(field.Tag.Get("json"))

			for _, fragment := range forbiddenFragments {
				assert.NotContains(t, name, fragment,
					"%s.%s could carry a secret; only the credential issuance response may, and it "+
						"returns one exactly once", shapeType.Name(), field.Name)
				assert.NotContains(t, key, fragment,
					"%s.%s serialises under %q, which reads as secret-bearing",
					shapeType.Name(), field.Name, field.Tag.Get("json"))
			}
		}
	}

	credentials := reflect.TypeOf(apimodel.KafkaCredentialsResponse{})
	password, ok := credentials.FieldByName("Password")
	require.True(t, ok,
		"the issuance response is expected to carry the password; if it stopped doing so, this "+
			"test would be guarding nothing")
	assert.Equal(t, `json:"password"`, string(password.Tag),
		"the one password field is reported under the documented key, and never omitempty — an "+
			"issuance that returned no password would be a silent failure")
}

// ---------------------------------------------------------------------------
// Deprecated legacy webhook-subscription shapes
// ---------------------------------------------------------------------------

// TestWebhookSubscriptionShapes_WireContract pins the transitional surface that exists only
// for the dual-delivery window, and on which the 410 Gone sunset becomes observable.
//
// The three types it names are marked Deprecated, which is correct and deliberate: they are
// scheduled for deletion once the sunset date passes. Until then they ship, and a shape that
// ships is a shape a client parses, so it is pinned here like any other. Naming a deprecated
// type is precisely what a test of its contract has to do, which is why the deprecation
// warning is suppressed for this function and nowhere else.
//
//nolint:staticcheck // SA1019: pinning a deprecated-but-shipping contract requires naming it.
func TestWebhookSubscriptionShapes_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.CreateWebhookSubscription{}, []fieldContract{
		{name: "WebhookURL", jsonKey: "webhook_url", binding: "required"},
	})

	assertWireContract(t, apimodel.UpdateWebhookSubscription{}, []fieldContract{
		{name: "WebhookURL", jsonKey: "webhook_url", binding: "required"},
	})

	assertWireContract(t, apimodel.WebhookSubscriptionResponse{}, []fieldContract{
		{name: "SubscriberID", jsonKey: "subscriber_id"},
		{name: "WebhookURL", jsonKey: "webhook_url", omitEmpty: true},
		{name: "MigratedAt", jsonKey: "migrated_at", omitEmpty: true},
	})

	t.Run("a migrated subscriber reports no URL and no migration gap", func(t *testing.T) {
		migratedAt := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
		decoded := marshalToKeys(t, apimodel.WebhookSubscriptionResponse{
			SubscriberID: "sub_9f2",
			MigratedAt:   &migratedAt,
		})

		assert.NotContains(t, decoded, "webhook_url",
			"after migration there is no legacy endpoint, and the key is omitted rather than empty")
		assert.Equal(t, `"2026-04-01T00:00:00Z"`, string(decoded["migrated_at"]),
			"a nil instant is what migration-progress reporting counts as outstanding, so the "+
				"stamped value must be a real RFC3339 instant")
	})

	t.Run("the response carries no transport secret", func(t *testing.T) {
		subjectType := reflect.TypeOf(apimodel.WebhookSubscriptionResponse{})

		for _, forbidden := range []string{"Headers", "Secret", "SigningSecret"} {
			_, present := subjectType.FieldByName(forbidden)
			assert.False(t, present,
				"%s is deployment-wide configuration for the legacy transport, never per-subscriber "+
					"data, and surfacing it would turn a migration-tracking response into a "+
					"secret-bearing one", forbidden)
		}
	})
}

// ---------------------------------------------------------------------------
// Authorization vocabulary for the new route prefixes
// ---------------------------------------------------------------------------

// TestScopeResources_CoverTheEventAndSubscriberSurfaces pins the two resource values the new
// routes authorize against.
//
// Each value does two jobs, and a typo in it would compile cleanly and break both. It is the
// first path segment the auth middleware resolves a request by — /events/dead-letter,
// /events/stats, /subscribers, /subscribers/:id/kafka-credentials,
// /subscribers/:id/webhook-subscription — and it is the resource half of an API-key scope
// string such as "events:read". A hyphenated variant, a singular slip or a stray capital
// would silently deny every caller, with no compile error and no failing test anywhere else.
//
// Registration in api/middleware/auth.go's pathToResource map is the other half of making
// these routes reachable, and it is deliberately NOT asserted here: it is later work, and
// the end-to-end proof belongs to the handler tests. What this test fixes is the vocabulary,
// so that when the map is written it is written against values that cannot have drifted.
func TestScopeResources_CoverTheEventAndSubscriberSurfaces(t *testing.T) {
	assert.Equal(t, middleware.Resource("events"), middleware.ResourceEvents,
		"ResourceEvents must be exactly \"events\": it is the first segment of /events/dead-letter, "+
			"/events/dead-letter/:event_id/replay and /events/stats")
	assert.Equal(t, middleware.Resource("subscribers"), middleware.ResourceSubscribers,
		"ResourceSubscribers must be exactly \"subscribers\": it is the first segment of "+
			"/subscribers, /subscribers/:subscriber_id, .../kafka-credentials and "+
			".../webhook-subscription — the deprecated webhook surface resolves here too, which is "+
			"why no separate webhook resource exists")

	t.Run("the scope strings a key is granted are the documented ones", func(t *testing.T) {
		assert.Equal(t, "events:read", middleware.BuildScope(middleware.ResourceEvents, middleware.ActionRead))
		assert.Equal(t, "events:write", middleware.BuildScope(middleware.ResourceEvents, middleware.ActionWrite))
		assert.Equal(t, "subscribers:read", middleware.BuildScope(middleware.ResourceSubscribers, middleware.ActionRead))
		assert.Equal(t, "subscribers:write", middleware.BuildScope(middleware.ResourceSubscribers, middleware.ActionWrite))
		assert.Equal(t, "subscribers:delete", middleware.BuildScope(middleware.ResourceSubscribers, middleware.ActionDelete))
	})

	t.Run("parsing is the exact inverse of building", func(t *testing.T) {
		resource, action := middleware.ParseScope("events:read")
		assert.Equal(t, middleware.ResourceEvents, resource)
		assert.Equal(t, middleware.ActionRead, action)

		resource, action = middleware.ParseScope("subscribers:write")
		assert.Equal(t, middleware.ResourceSubscribers, resource)
		assert.Equal(t, middleware.ActionWrite, action)

		for _, resourceValue := range []middleware.Resource{middleware.ResourceEvents, middleware.ResourceSubscribers} {
			for _, actionValue := range []middleware.Action{
				middleware.ActionRead, middleware.ActionWrite, middleware.ActionDelete, middleware.ActionAll,
			} {
				parsedResource, parsedAction := middleware.ParseScope(middleware.BuildScope(resourceValue, actionValue))
				assert.Equal(t, resourceValue, parsedResource,
					"round-tripping %s:%s must preserve the resource", resourceValue, actionValue)
				assert.Equal(t, actionValue, parsedAction,
					"round-tripping %s:%s must preserve the action", resourceValue, actionValue)
			}
		}
	})

	t.Run("permission follows the HTTP method the route uses", func(t *testing.T) {
		// GET /events/dead-letter and GET /events/stats are reads; the replay is a POST, so a
		// read-only key must not be able to trigger one.
		assert.True(t, middleware.HasPermission([]string{"events:read"}, middleware.ResourceEvents, http.MethodGet))
		assert.False(t, middleware.HasPermission([]string{"events:read"}, middleware.ResourceEvents, http.MethodPost),
			"replaying a dead-lettered event re-publishes it, so a read scope must not authorize it")
		assert.True(t, middleware.HasPermission([]string{"events:write"}, middleware.ResourceEvents, http.MethodPost))

		// Subscriber management: create and credential issuance are writes, deletion is its own
		// action, and a write scope must not imply it.
		assert.True(t, middleware.HasPermission([]string{"subscribers:write"}, middleware.ResourceSubscribers, http.MethodPost))
		assert.True(t, middleware.HasPermission([]string{"subscribers:write"}, middleware.ResourceSubscribers, http.MethodPut))
		assert.False(t, middleware.HasPermission([]string{"subscribers:write"}, middleware.ResourceSubscribers, http.MethodDelete),
			"deleting a subscriber revokes access for a live consumer and needs its own action")
		assert.True(t, middleware.HasPermission([]string{"subscribers:delete"}, middleware.ResourceSubscribers, http.MethodDelete))

		// A scope for one of the two new resources must never authorize the other, or the
		// dead-letter inventory would be reachable with a subscriber-management key.
		assert.False(t, middleware.HasPermission([]string{"subscribers:read"}, middleware.ResourceEvents, http.MethodGet))
		assert.False(t, middleware.HasPermission([]string{"events:read"}, middleware.ResourceSubscribers, http.MethodGet))

		// The wildcards behave here exactly as they do for every pre-existing resource.
		assert.True(t, middleware.HasPermission([]string{"*:*"}, middleware.ResourceEvents, http.MethodPost))
		assert.True(t, middleware.HasPermission([]string{"events:*"}, middleware.ResourceEvents, http.MethodDelete))
	})

	t.Run("a key cannot mint a scope broader than its own", func(t *testing.T) {
		assert.True(t, middleware.ScopeCovers("events:*", "events:read"))
		assert.False(t, middleware.ScopeCovers("events:read", "events:write"))
		assert.False(t, middleware.ScopeCovers("events:*", "subscribers:read"),
			"breadth over one resource must never leak into the other")

		assert.True(t, middleware.CanGrantScopes(
			[]string{"*:*"}, []string{"events:read", "subscribers:write"}))
		assert.True(t, middleware.CanGrantScopes(
			[]string{"events:*", "subscribers:*"}, []string{"events:read", "subscribers:delete"}))
		assert.False(t, middleware.CanGrantScopes(
			[]string{"events:read"}, []string{"events:read", "subscribers:read"}),
			"a caller holding only an events scope must not be able to issue a subscriber scope, "+
				"which would let it grant itself credential issuance")
	})
}

// TestSubscriberEnforcedAccess_StatesEachDimensionAndTheComponentThatEnforcesIt is the API half
// of the partition-key-prefix hazard.
//
// # The hazard
//
// A subscriber's record carries three access-shaped values — authorized_topics, a consumer
// group, and partition_key_prefix — and only the first two are ACL bindings. Kafka's authorizer
// has no message-key dimension, so no ACL confines a consumer to a slice of a topic by key.
//
// The response used to answer that by declaring the key dimension UNENFORCED and asking the
// subscriber to filter for itself. A security review rejected it: cooperation is not an access
// boundary, and a subscriber that ignored the request — or used any other Kafka client — read
// every record on the shared topic, including records written for other ledgers.
//
// So the dimension is enforced now, by a component of Blnk's rather than by the broker, and this
// body is where the response says which component enforces what. That is the property these
// assertions protect, in both directions: a response must not claim the prefix is a broker
// boundary, and it must not report it as nobody's boundary either.
func TestSubscriberEnforcedAccess_StatesEachDimensionAndTheComponentThatEnforcesIt(t *testing.T) {
	subscriberID := "acme_prod"
	topics := []string{"blnk.transactions", "blnk.balances"}
	const keyScope = "ldg_9f1c8a72"

	enforced := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, keyScope)

	t.Run("partition key filtering is declared ENFORCED, in the body", func(t *testing.T) {
		assert.True(t, enforced.PartitionKeyPrefixEnforced,
			"a recorded prefix is kept by Blnk, so this field states it: false here was the "+
				"declaration that a boundary an operator recorded was nobody's obligation")

		assert.Contains(t, enforced.EnforcedBy, apimodel.EnforcementDimensionPartitionKey,
			"and the machine-readable list carries the dimension, so a client branching on the "+
				"list reaches the same conclusion as one reading the boolean")

		// THE BODY, not the documentation. A client cannot branch on a comment.
		keys := marshalToKeys(t, enforced)
		require.Contains(t, keys, "partition_key_prefix_enforced")
		require.Contains(t, keys, "enforced_by")
	})

	t.Run("the key scope is reported together with where it IS enforced", func(t *testing.T) {
		// Both fields or neither. The prefix alone reads as a limit the CREDENTIAL carries, which
		// is what made the previous contract dangerous — the prefix was echoed with nothing in
		// the body saying that no component applied it.
		assert.Equal(t, keyScope, enforced.PartitionKeyPrefix,
			"the recorded scope must reach the client whose records it selects")
		assert.Equal(t, model.KeyScopeEnforcementGateway, enforced.PartitionKeyPrefixEnforcedBy,
			"and it must say WHERE, or 'enforced' is the unverifiable claim it replaced")

		keys := marshalToKeys(t, enforced)
		require.Contains(t, keys, "partition_key_prefix_enforced_by",
			"the enforcement point must be in the BODY, not only in documentation")
	})

	t.Run("with no key scope the enforcement point is reported as none", func(t *testing.T) {
		// Always present, so a client can branch on it without first testing whether the
		// prefix is empty.
		unscoped := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, "")
		assert.Empty(t, unscoped.PartitionKeyPrefix, "nothing recorded means nothing echoed")
		assert.Equal(t, model.KeyScopeEnforcementNone, unscoped.PartitionKeyPrefixEnforcedBy)
		assert.False(t, unscoped.PartitionKeyPrefixEnforced,
			"and nothing enforced: there is no prefix, which is a third state and not a gap")

		keys := marshalToKeys(t, unscoped)
		require.Contains(t, keys, "partition_key_prefix_enforced_by")
		assert.NotContains(t, keys, "partition_key_prefix",
			"an absent scope is omitted rather than reported as an empty string")
	})

	t.Run("a whitespace-only key scope is reported as absent", func(t *testing.T) {
		// It agrees with model.EventSubscriber.DeclaresKeyScope, which reads whitespace as
		// absent for the same reason: a scope on nothing is not an intent anybody has. It must
		// not produce an ENFORCED declaration either, or a body would claim a filter that
		// matches every key is a boundary.
		blank := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, "   \t ")
		assert.Empty(t, blank.PartitionKeyPrefix)
		assert.Equal(t, model.KeyScopeEnforcementNone, blank.PartitionKeyPrefixEnforcedBy)
		assert.False(t, blank.PartitionKeyPrefixEnforced)
		assert.NotContains(t, blank.EnforcedBy, apimodel.EnforcementDimensionPartitionKey)
	})

	t.Run("the dimensions that ARE enforced are named, and the list follows the row", func(t *testing.T) {
		assert.Equal(t, []string{
			apimodel.EnforcementDimensionTopic,
			apimodel.EnforcementDimensionConsumerGroup,
			apimodel.EnforcementDimensionPartitionKey,
		}, enforced.EnforcedBy,
			"three dimensions for a key-scoped subscriber, and exactly those: a missing one "+
				"understates the isolation that exists, an extra one claims isolation that does not")

		unscoped := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, "")
		assert.Equal(t, []string{
			apimodel.EnforcementDimensionTopic,
			apimodel.EnforcementDimensionConsumerGroup,
		}, unscoped.EnforcedBy,
			"and two for a subscriber that recorded no prefix: the key dimension is claimed only "+
				"where there is a key scope to apply, or the flag teaches a client nothing")

		assert.Equal(t, topics, enforced.Topics, "the topic set must be reported exactly")
	})

	// M-1 kept, inverted. The negative claim used to be the load-bearing statement in this
	// object: partition_key sat in not_enforced_by, and a client had to read it to learn that
	// the prefix was its own problem. The boundary is enforced now, so the list is EMPTY — and
	// the key stays in the body, because an absent key would be indistinguishable from a
	// response that simply forgot to state it.
	t.Run("nothing is reported as unenforced, on any path", func(t *testing.T) {
		assert.Empty(t, enforced.NotEnforcedBy,
			"a key-scoped subscriber has no unenforced dimension: the prefix is applied by the "+
				"gateway, which is what replaced asking the subscriber to apply it")

		unscoped := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, "")
		assert.Empty(t, unscoped.NotEnforcedBy,
			"and neither does a subscriber with no prefix, which has nothing to enforce rather "+
				"than something enforced by nobody")

		// The two lists remain disjoint, which is what makes a dimension MOVING between them a
		// visible contract change — the change this very test records having happened once.
		for _, dimension := range enforced.NotEnforcedBy {
			assert.NotContains(t, enforced.EnforcedBy, dimension,
				"a dimension cannot be both enforced and not enforced")
		}

		// Never omitempty, for the same reason exclusive_grant_verified is not: a missing key
		// would be indistinguishable from "not stated", and [] is a claim.
		for name, declaration := range map[string]apimodel.SubscriberEnforcedAccess{
			"key-scoped": enforced,
			"unscoped":   unscoped,
		} {
			keys := marshalToKeys(t, declaration)
			require.Contains(t, keys, "not_enforced_by", name+" must carry the key")
			assert.NotNil(t, keys["not_enforced_by"],
				name+" must carry [] rather than null, so 'nothing is unenforced' is stated")
		}
	})

	t.Run("the consumer group namespace is derived, not restated", func(t *testing.T) {
		// Derived through the same model helper the ACL binding is built from, so the value
		// reported cannot describe a namespace different from the one actually reserved.
		namespace, err := model.CanonicalConsumerGroupNamespace(subscriberID)
		require.NoError(t, err)
		assert.Equal(t, namespace, enforced.ConsumerGroupNamespace)

		group, err := model.CanonicalConsumerGroupID(subscriberID)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(group, enforced.ConsumerGroupNamespace),
			"the subscriber's default group must lie inside the namespace reported as reserved, or "+
				"the report describes a boundary the subscriber's own group falls outside")
	})

	t.Run("an empty grant reports an empty set rather than null", func(t *testing.T) {
		keys := marshalToKeys(t, apimodel.NewSubscriberEnforcedAccess(subscriberID, nil, ""))
		assert.NotNil(t, keys["topics"],
			"a fail-closed empty grant is a state an operator must be able to read; null would "+
				"force every client to special-case it")
	})

	// AUTH-03: the declaration used to describe what Blnk ASKED FOR and nothing else, so a
	// principal carrying a hand-made ALLOW binding was issued a credential whose response
	// declared this exact boundary while the broker enforced a wider one. Issuance now reads
	// the principal's complete grant and refuses a broader one, and the response says so — but
	// only on the path where the reading actually happened.
	t.Run("the exclusivity claim is only made where it was verified", func(t *testing.T) {
		assert.False(t, enforced.ExclusiveGrantVerified,
			"a projection built from a registry row makes no broker round trip, so it must not "+
				"claim the grant was observed to be exclusive")

		// THE SAME key scope the unverified fixture was built with. The two constructors are
		// compared field by field below, so passing a different scope here would make them
		// differ in three fields and the comparison would pass for the wrong reason — it is
		// asserting that they differ in exactly ONE.
		verified := apimodel.NewVerifiedSubscriberEnforcedAccess(subscriberID, topics, keyScope)
		assert.True(t, verified.ExclusiveGrantVerified,
			"issuance reads the principal's complete ACL grant and refuses a broader one, so the "+
				"credential response may state that it verified exclusivity")

		// Identical in every other respect: the two constructors must not be able to drift
		// into describing different boundaries.
		verified.ExclusiveGrantVerified = false
		assert.Equal(t, enforced, verified,
			"the verified form must differ from the unverified one in exactly one field")
	})

	t.Run("the exclusivity claim is always present in the body", func(t *testing.T) {
		// No omitempty: false must be transmitted rather than dropped. A client that reads a
		// missing key as "verified" would draw exactly the wrong conclusion, and false is the
		// value on the path where nothing was verified.
		keys := marshalToKeys(t, apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, ""))
		assert.Contains(t, keys, "exclusive_grant_verified",
			"an absent key would be indistinguishable from a verified boundary")

		assert.Contains(t,
			marshalToKeys(t, apimodel.NewVerifiedSubscriberEnforcedAccess(subscriberID, topics, "")),
			"exclusive_grant_verified")
	})

	t.Run("every subscriber response carries the declaration", func(t *testing.T) {
		// BUILT FROM THE MODEL DIRECTLY, because the declaration has to be right for a row
		// whatever produced it — one registered with a prefix, one updated into a prefix, and
		// one written before the enforcement point existed all read through this projection.
		prefix := "ledger-42"
		response := apimodel.NewSubscriberResponse(model.EventSubscriber{
			SubscriberID:       subscriberID,
			AuthorizedTopics:   topics,
			PartitionKeyPrefix: &prefix,
		})

		// The prefix and what enforces it travel in ONE body, and that adjacency is the point:
		// the two cannot be read apart, so whoever finds the prefix also reads where it applies.
		assert.Equal(t, prefix, response.PartitionKeyPrefix)
		assert.True(t, response.EnforcedAccess.PartitionKeyPrefixEnforced)
		assert.Equal(t, prefix, response.EnforcedAccess.PartitionKeyPrefix,
			"the declaration echoes the same value the top-level field carries, so a client reading "+
				"either one reads the same contract")
		assert.Equal(t, model.KeyScopeEnforcementGateway,
			response.EnforcedAccess.PartitionKeyPrefixEnforcedBy)
		assert.True(t, response.EnforcedAccess.GatewayDeliveryRequired,
			"and the registry view gives the same transport instruction the credential view does, "+
				"so an integrator reading either one wires the same consumer")
		assert.False(t, response.EnforcedAccess.BrokerRecordAccess)

		keys := marshalToKeys(t, response)
		require.Contains(t, keys, "enforced_access",
			"the declaration must be present on every subscriber body, including one that has "+
				"never been issued a credential — an integrator needs it before wiring a consumer")
	})
}

// TestSubscriberEnforcedAccess_NamesWhoseObligationTheKeyNarrowingIs is C-02's API half.
//
// # Why this field exists rather than a refusal
//
// Credential issuance used to REFUSE any subscriber recording a partition key prefix, with the
// typed code SUBSCRIBER_ISOLATION_UNENFORCEABLE and a 409, permanently. That withdrew a mandatory
// capability for a state the registry is designed to hold: a subscriber registered with a prefix
// could never obtain credentials at all, and a database CHECK constraint made the combination
// unrepresentable as well.
//
// What replaced it was DISCLOSURE — issue whole-topic Read, echo the prefix, and declare that
// applying it was the consumer's own obligation. That was accurate prose about an absent boundary.
// A subscriber that ignored the obligation, or used any other Kafka client, read every record on
// the shared category topic, including records written for other ledgers and other subscribers,
// and nothing in the platform could prevent or detect it. A security review named exactly that:
// disclosure and client cooperation are not an authorization boundary.
//
// So the boundary is ENFORCED now, and this is where the contract says so. A subscriber recording
// a prefix is granted Describe but NOT Read on its topics — the broker refuses every direct fetch
// — and its records are delivered by Blnk's stream gateway, which applies the prefix to each
// record's key. The fields below are the whole of that statement, and they are asserted as a
// group because their VALUE is in their agreement: any one of them alone is either ambiguous or
// ignorable.
func TestSubscriberEnforcedAccess_NamesWhoseObligationTheKeyNarrowingIs(t *testing.T) {
	subscriberID := "acme_prod"
	topics := []string{"blnk.transactions"}

	t.Run("a recorded prefix declares an enforced boundary and where records come from", func(t *testing.T) {
		declared := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, "ldg_9f2c")

		assert.Equal(t, "ldg_9f2c", declared.PartitionKeyPrefix,
			"the prefix is echoed, because it is the boundary the gateway applies on this "+
				"subscriber's behalf")
		assert.True(t, declared.PartitionKeyPrefixEnforced,
			"and it is stated as ENFORCED in the same object: a recorded prefix is kept by Blnk, "+
				"which is what replaced granting whole-topic Read and asking the consumer to filter")
		assert.Equal(t, model.KeyScopeEnforcementGateway, declared.PartitionKeyPrefixEnforcedBy,
			"and the enforcement point is named, because 'enforced' without a component is the "+
				"claim that used to be false")
		assert.True(t, declared.GatewayDeliveryRequired,
			"the branchable field says where the records come from — this is the field whose wrong "+
				"answer is a broken integration, because a client reading false here would fetch "+
				"from a broker that refuses it")
		assert.False(t, declared.BrokerRecordAccess,
			"and its complement says WHY the broker refuses: no topic Read binding exists for a "+
				"key-scoped principal, which is the boundary itself")
		assert.Contains(t, declared.EnforcedBy, apimodel.EnforcementDimensionPartitionKey,
			"partition_key is an ENFORCED dimension now, and listing it is the machine-readable "+
				"form of that change")
		assert.Empty(t, declared.NotEnforcedBy,
			"and nothing is left unenforced: a response can no longer say that a boundary an "+
				"operator recorded is kept by nobody")
	})

	t.Run("no prefix declares no key boundary and direct broker access", func(t *testing.T) {
		// The mutant this kills is the one that hard-codes true. A flag that is always true
		// teaches a client nothing and gets ignored, which returns the contract to the state
		// where the boundary was implied rather than stated.
		declared := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, "")

		assert.Empty(t, declared.PartitionKeyPrefix)
		assert.False(t, declared.GatewayDeliveryRequired,
			"a whole-topic entitlement is delivered by the broker itself")
		assert.True(t, declared.BrokerRecordAccess,
			"and such a subscriber really does hold topic Read")
		assert.False(t, declared.PartitionKeyPrefixEnforced,
			"still false rather than absent: there is no prefix, so there is no enforced prefix")
		assert.NotContains(t, declared.EnforcedBy, apimodel.EnforcementDimensionPartitionKey,
			"and the dimension is not claimed for a subscriber that recorded none")
		assert.Empty(t, declared.NotEnforcedBy,
			"nor is it reported as unenforced: there is nothing to enforce, which is a third state")
	})

	t.Run("every flag is DERIVED from the prefix, so they cannot disagree", func(t *testing.T) {
		// Passing the flags alongside the value would allow a caller to send a prefix with no
		// enforcement point attached, or gateway delivery with no prefix to filter on. Either is
		// worse than neither, so the constructor computes all of them from one fact.
		for name, prefix := range map[string]string{
			"padded":     "  ldg_9f2c  ",
			"whitespace": "   ",
			"empty":      "",
			"plain":      "ldg_9f2c",
		} {
			t.Run(name, func(t *testing.T) {
				declared := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, prefix)
				scoped := declared.PartitionKeyPrefix != ""

				assert.Equal(t, scoped, declared.GatewayDeliveryRequired,
					"gateway delivery is required exactly when a prefix survives normalisation")
				assert.Equal(t, scoped, declared.PartitionKeyPrefixEnforced,
					"and the enforcement flag agrees with it")
				assert.Equal(t, !scoped, declared.BrokerRecordAccess,
					"and broker record access is its exact complement, never both and never neither")
				assert.Equal(t, strings.TrimSpace(prefix), declared.PartitionKeyPrefix,
					"and the echoed prefix is trimmed, because storage padding is not part of the "+
						"key a subscriber compares against")
			})
		}
	})

	t.Run("every declaration is always present in the body", func(t *testing.T) {
		// No omitempty on any of the booleans. An absent gateway_delivery_required reads as "not
		// applicable", and it has to read as a definite yes or no.
		for name, prefix := range map[string]string{"with a prefix": "ldg_9f2c", "without one": ""} {
			t.Run(name, func(t *testing.T) {
				keys := marshalToKeys(t, apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, prefix))
				assert.Contains(t, keys, "gateway_delivery_required",
					"an absent key would be indistinguishable from direct broker consumption")
				assert.Contains(t, keys, "broker_record_access")
				assert.Contains(t, keys, "partition_key_prefix_enforced")
				assert.NotContains(t, keys, "client_side_key_filtering_required",
					"the retired field must be GONE rather than left reporting a false alongside "+
						"its replacement, which would read as two contradictory instructions")
			})
		}
	})

	t.Run("a subscriber read declares the same obligation as its credential", func(t *testing.T) {
		// The credential is delivered ONCE, so every later reader — an operator, a migration
		// report, an onboarding script — learns the obligation from the subscriber projection
		// instead. If the two could differ, whichever was read second would be believed.
		prefix := "ldg_9f2c"
		response := apimodel.NewSubscriberResponse(model.EventSubscriber{
			SubscriberID:       subscriberID,
			AuthorizedTopics:   topics,
			PartitionKeyPrefix: &prefix,
		})

		assert.Equal(t, prefix, response.PartitionKeyPrefix)
		assert.Equal(t, prefix, response.EnforcedAccess.PartitionKeyPrefix,
			"the declaration resolves the prefix from the same row the top-level field does")
		assert.True(t, response.EnforcedAccess.GatewayDeliveryRequired,
			"so a reader of the registry is told what a holder of the credential was told")
		assert.False(t, response.EnforcedAccess.BrokerRecordAccess,
			"including the fact that this subscriber cannot fetch from the broker at all")
		assert.False(t, response.EnforcedAccess.ExclusiveGrantVerified,
			"and still claims no verified exclusivity: reading a row observes no broker grant")
	})
}

// TestSubscriberEnforcedAccess_CarriesTheRemedyBesideTheLimitation is SEC-01's API half.
//
// # The gap this closes
//
// The declaration's negative facts were complete: partition_key_prefix_enforced false,
// not_enforced_by naming the key dimension, client_side_key_filtering_required saying whose job
// the narrowing is. What none of them said is what to do INSTEAD, and the answer existed only
// outside the response — in docs/kafka-operations.md, in a WARNING in Blnk's own log, and in the
// SubscriberKeyScopeGuidance constant, which was declared and referenced by nothing at all.
//
// A runtime security review reproduced the consequence: a subscriber granted blnk.transactions
// read tens of thousands of records whose keys fell outside its recorded prefix, exactly as this
// declaration says it would, while a reader of the requirement that promised key-prefix scoping
// had no way to learn from any response how to obtain a boundary that is actually kept. The two
// readings could not be reconciled from the API alone.
//
// So the remedy now travels in the same object as the limitation. This asserts it is there, that
// it is the shared constant rather than a per-handler paraphrase, and that it is present for
// every subscriber rather than only for the key-scoped ones — a remedy that appeared only beside
// a prefix would let a reader of any other subscriber conclude the key dimension is enforced for
// them.
func TestSubscriberEnforcedAccess_CarriesTheRemedyBesideTheLimitation(t *testing.T) {
	subscriberID := "acme_prod"
	topics := []string{"blnk.transactions"}

	t.Run("the remedy is the shared constant, not a paraphrase", func(t *testing.T) {
		declared := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, "ldg_9f2c")

		assert.Equal(t, apimodel.SubscriberKeyScopeGuidance, declared.Guidance,
			"one sentence, from one constant, so the registry view and the credential view cannot "+
				"give different advice about the same limitation")
		assert.NotEmpty(t, declared.Guidance,
			"an empty remedy is the state this field was added to end: the limitation was stated "+
				"and the answer to it was not")
	})

	t.Run("the remedy names what IS enforced, so it is actionable", func(t *testing.T) {
		// The value of a remedy is that it points somewhere real. authorized_topics is the
		// dimension the broker does evaluate, so guidance that did not name it would restate
		// the problem rather than answer it.
		assert.Contains(t, apimodel.SubscriberKeyScopeGuidance, "authorized_topics",
			"the remedy must name the dimension the broker actually enforces")
		assert.Contains(t, apimodel.SubscriberKeyScopeGuidance, "readable in full",
			"and must state the exposure plainly, because a granted topic is read whole")
	})

	t.Run("it is present whether or not a prefix is recorded", func(t *testing.T) {
		// It describes what Kafka's authorizer can evaluate, which is not a property of this
		// row. A remedy that appeared only for key-scoped subscribers would read, on every
		// other subscriber, as though the key dimension were enforced for them.
		for name, prefix := range map[string]string{
			"with a prefix": "ldg_9f2c",
			"without one":   "",
			"whitespace":    "   ",
		} {
			t.Run(name, func(t *testing.T) {
				declared := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, prefix)
				assert.Equal(t, apimodel.SubscriberKeyScopeGuidance, declared.Guidance)

				keys := marshalToKeys(t, declared)
				assert.Contains(t, keys, "guidance",
					"no omitempty: an absent remedy is what the review found, and it must not be "+
						"reachable again by leaving the prefix out")
			})
		}
	})

	t.Run("every subscriber and credential body carries it", func(t *testing.T) {
		// The two projections an integrator actually reads. The credential is delivered once,
		// so a later reader learns the boundary — and now the remedy — from the subscriber
		// projection instead.
		prefix := "ldg_9f2c"
		subscriber := apimodel.NewSubscriberResponse(model.EventSubscriber{
			SubscriberID:       subscriberID,
			AuthorizedTopics:   topics,
			PartitionKeyPrefix: &prefix,
		})
		assert.Equal(t, apimodel.SubscriberKeyScopeGuidance, subscriber.EnforcedAccess.Guidance)

		credential := apimodel.KafkaCredentialsResponse{
			EnforcedAccess: apimodel.NewVerifiedSubscriberEnforcedAccess(
				subscriberID, topics, prefix,
			),
		}
		assert.Equal(t, apimodel.SubscriberKeyScopeGuidance, credential.EnforcedAccess.Guidance,
			"the issuance response is where a consumer is configured, so it is the one body that "+
				"must not state the limitation without the remedy")

		for name, body := range map[string]interface{}{
			"subscriber": subscriber,
			"credential": credential,
		} {
			t.Run(name, func(t *testing.T) {
				enforced := marshalToKeys(t, body)["enforced_access"]
				require.NotNil(t, enforced, "the declaration must be present to carry a remedy")
				assert.Contains(t, string(enforced), `"guidance"`,
					"the remedy must survive marshalling into the body an integrator reads")
			})
		}
	})

	t.Run("verifying exclusivity does not change the remedy", func(t *testing.T) {
		// The verified constructor differs from the unverified one in exactly one field, and
		// this keeps the remedy out of that difference: advice that changed depending on
		// whether a broker round trip happened would be advice about the wrong thing.
		unverified := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics, "ldg_9f2c")
		verified := apimodel.NewVerifiedSubscriberEnforcedAccess(subscriberID, topics, "ldg_9f2c")

		assert.Equal(t, unverified.Guidance, verified.Guidance)

		verified.ExclusiveGrantVerified = false
		assert.Equal(t, unverified, verified,
			"the two constructors must still differ in exactly one field")
	})
}
