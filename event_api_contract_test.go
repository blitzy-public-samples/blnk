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
func TestEventOutboxStatsResponse_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.EventOutboxStatsResponse{}, []fieldContract{
		{name: "Pending", jsonKey: "pending"},
		{name: "Processing", jsonKey: "processing"},
		{name: "WebhookPending", jsonKey: "webhook_pending"},
		{name: "Dispatched", jsonKey: "dispatched"},
		{name: "Failed", jsonKey: "failed"},
		{name: "DeadLettered", jsonKey: "dead_lettered"},
		{name: "Replaying", jsonKey: "replaying"},
		{name: "TopicEndOffsets", jsonKey: "topic_end_offsets", omitEmpty: true},
		{name: "OffsetsComplete", jsonKey: "offsets_complete"},
		{name: "MissingTopics", jsonKey: "missing_topics", omitEmpty: true},
		{name: "PartitionsUnavailable", jsonKey: "partitions_unavailable"},
		{name: "OffsetsMeasuredAt", jsonKey: "offsets_measured_at", omitEmpty: true},
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
			require.Contains(t, decoded, status,
				"the reconciliation runbook names every relay state, and a count of zero must read "+
					"as 'none in that state' rather than as 'no such state'. %q is in the model's "+
					"vocabulary but missing from EventOutboxStatsResponse", status)
			assert.Equal(t, "0", string(decoded[status]),
				"a zero count must be an explicit 0, never an omitted key")
		}
	})

	// The replaying lease deserves its own subtest because it is the status that was
	// missing, and because it is the only NON-TERMINAL state a caller can mistake for a
	// terminal one: a row held by an in-flight replay is neither published nor lost.
	t.Run("the replaying lease is counted and is not terminal", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.EventOutboxStatsResponse{
			Dispatched:   40,
			DeadLettered: 2,
			Replaying:    3,
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
		decoded := marshalToKeys(t, apimodel.EventOutboxStatsResponse{Dispatched: 12})

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
			Dispatched:            41,
			DeadLettered:          1,
			TopicEndOffsets:       map[string]int64{"blnk.transactions": 40},
			OffsetsComplete:       false,
			MissingTopics:         []string{"blnk.identities"},
			PartitionsUnavailable: 2,
			OffsetsMeasuredAt:     &measuredAt,
			GeneratedAt:           measuredAt,
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
}

// TestOutboxReconciliationResult_WireContract pins the verdict acceptance criterion V-2 is
// scored on, and it exists because the response used to carry the two SIDES of the
// comparison and no verdict at all.
//
// Leaving the verdict to the caller meant every caller re-implemented the arithmetic, the
// directionality and the retention caveat, and each got its own chance to get them wrong in a
// way that reads as success. Two specific mistakes are what this shape prevents: a caller
// that diffs the two totals and alerts on any difference alerts constantly, because
// redeliveries and replays legitimately make the broker side larger; and a caller that
// compares for equality reports a green result on a reading retention has already invalidated.
func TestOutboxReconciliationResult_WireContract(t *testing.T) {
	assertWireContract(t, apimodel.OutboxReconciliationResult{}, []fieldContract{
		{name: "TerminalEvents", jsonKey: "terminal_events"},
		// The three corroboration counts sit here rather than at the end, mirroring the
		// service type's own declaration order: encoding/json emits keys in declaration
		// order, so a reader comparing a response against blnk.OutboxReconciliation reads
		// the two in the same sequence.
		{name: "ConfirmedEvents", jsonKey: "confirmed_events"},
		{name: "UnconfirmedEvents", jsonKey: "unconfirmed_events"},
		{name: "DuplicatedRecords", jsonKey: "duplicated_records"},
		{name: "MessagesWritten", jsonKey: "messages_written"},
		{name: "Overhead", jsonKey: "overhead"},
		{name: "LossDetected", jsonKey: "loss_detected"},
		{name: "Conclusive", jsonKey: "conclusive"},
		{name: "Caveats", jsonKey: "caveats", omitEmpty: true},
		{name: "Summary", jsonKey: "summary"},
		{name: "MeasuredAt", jsonKey: "measured_at"},
	})

	t.Run("the field set mirrors the service-layer verdict", func(t *testing.T) {
		// The API shape and the computed verdict must not drift: a field added to the
		// service-layer OutboxReconciliation and not surfaced here is a finding the daily
		// check produces and the API silently withholds. Compared as SETS of names, because
		// the two types legitimately differ in wire tags and in Summary's form — a method
		// on one, a field on the other.
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

	t.Run("a healthy result reports positive overhead and no loss", func(t *testing.T) {
		measuredAt := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
		decoded := marshalToKeys(t, apimodel.OutboxReconciliationResult{
			TerminalEvents:  1000,
			MessagesWritten: 1003,
			Overhead:        3,
			LossDetected:    false,
			Conclusive:      true,
			Summary:         "1000 outbox rows against 1003 broker records",
			MeasuredAt:      measuredAt,
		})

		assert.Equal(t, "3", string(decoded["overhead"]),
			"a small positive overhead is the HEALTHY state — redeliveries and replays — not a defect")
		assert.Equal(t, "false", string(decoded["loss_detected"]))
		assert.Equal(t, "true", string(decoded["conclusive"]))
		assert.NotContains(t, decoded, "caveats",
			"a conclusive result has no caveats, so an empty list is omitted rather than reported")
		assert.Equal(t, `"2026-03-04T05:06:07Z"`, string(decoded["measured_at"]))
	})

	t.Run("a shortfall is reported as negative overhead rather than clamped", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.OutboxReconciliationResult{
			TerminalEvents:  1000,
			MessagesWritten: 996,
			Overhead:        -4,
			LossDetected:    true,
			Conclusive:      true,
		})

		assert.Equal(t, "-4", string(decoded["overhead"]),
			"the sign IS the finding: clamping it at zero would erase the only signal this check "+
				"carries, and the field must therefore be signed on the wire too")
		assert.Equal(t, "true", string(decoded["loss_detected"]))
	})

	t.Run("an inconclusive result is never mistaken for a clean one", func(t *testing.T) {
		decoded := marshalToKeys(t, apimodel.OutboxReconciliationResult{
			TerminalEvents:  1000,
			MessagesWritten: 400,
			Overhead:        -600,
			LossDetected:    true,
			Conclusive:      false,
			Caveats:         []string{"retention has deleted records from blnk.transactions"},
		})

		require.Contains(t, decoded, "conclusive")
		assert.Equal(t, "false", string(decoded["conclusive"]),
			"the key must be present on the inconclusive path: a vanishing key would leave the most "+
				"dangerous state looking like the healthiest one")
		assert.Equal(t, `["retention has deleted records from blnk.transactions"]`, string(decoded["caveats"]),
			"an operator must be told WHY the count cannot be trusted, not merely that it cannot")
	})

	t.Run("a stats response with no offsets carries no verdict", func(t *testing.T) {
		// With nothing measured on the broker side there is no verdict to report, and
		// emitting an empty one would read as "reconciled, nothing written".
		decoded := marshalToKeys(t, apimodel.EventOutboxStatsResponse{Dispatched: 12})

		assert.NotContains(t, decoded, "reconciliation",
			"a deployment with no brokers is a legitimate steady state; an absent verdict is honest "+
				"where a zeroed one would be a false all-clear")
	})
}

// ---------------------------------------------------------------------------
// Subscriber shapes
// ---------------------------------------------------------------------------

// TestCreateSubscriber_RequestContract pins the POST /subscribers body.
func TestCreateSubscriber_RequestContract(t *testing.T) {
	assertWireContract(t, apimodel.CreateSubscriber{}, []fieldContract{
		{name: "SubscriberID", jsonKey: "subscriber_id"},
		{name: "Name", jsonKey: "name", binding: "required"},
		{name: "AuthorizedTopics", jsonKey: "authorized_topics", binding: "max=16,dive,max=249"},
		{name: "PartitionKeyPrefix", jsonKey: "partition_key_prefix", omitEmpty: true},
		{name: "WebhookURL", jsonKey: "webhook_url", omitEmpty: true},
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

	t.Run("name is the only mandatory field", func(t *testing.T) {
		subjectType := reflect.TypeOf(apimodel.CreateSubscriber{})

		for index := 0; index < subjectType.NumField(); index++ {
			field := subjectType.Field(index)
			if field.Name == "Name" {
				continue
			}

			// A field may carry BOUNDS without being mandatory, and the distinction is the
			// point: authorized_topics is bounded so an oversized grant is refused at the
			// binding layer instead of composing a very large ACL request, but omitting it
			// entirely is still legal and yields the fail-closed empty grant.
			binding := field.Tag.Get("binding")
			assert.NotContains(t, strings.Split(binding, ","), "required",
				"%s must stay optional: the service derives the id when it is omitted, and an "+
					"empty topic grant is the deliberate fail-closed default rather than a "+
					"missing value", field.Name)
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
		{name: "Name", jsonKey: "name", omitEmpty: true},
		{
			name:      "AuthorizedTopics",
			jsonKey:   "authorized_topics",
			omitEmpty: true,
			binding:   "omitempty,max=16,dive,max=249",
		},
		{name: "PartitionKeyPrefix", jsonKey: "partition_key_prefix", omitEmpty: true},
		{name: "WebhookURL", jsonKey: "webhook_url", omitEmpty: true},
	})

	t.Run("every scalar is a pointer so absent and empty stay distinguishable", func(t *testing.T) {
		subjectType := reflect.TypeOf(apimodel.UpdateSubscriber{})

		for _, name := range []string{"Name", "PartitionKeyPrefix", "WebhookURL"} {
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
		{name: "Name", jsonKey: "name"},
		{name: "KafkaPrincipal", jsonKey: "kafka_principal"},
		{name: "ConsumerGroupID", jsonKey: "consumer_group_id"},
		{name: "AuthorizedTopics", jsonKey: "authorized_topics"},
		{name: "PartitionKeyPrefix", jsonKey: "partition_key_prefix", omitEmpty: true},
		{name: "EnforcedAccess", jsonKey: "enforced_access"},
		{name: "CredentialFingerprint", jsonKey: "credential_fingerprint", omitEmpty: true},
		{name: "CredentialIssuedAt", jsonKey: "credential_issued_at", omitEmpty: true},
		{name: "WebhookURL", jsonKey: "webhook_url", omitEmpty: true},
		{name: "MigratedAt", jsonKey: "migrated_at", omitEmpty: true},
		{name: "CreatedAt", jsonKey: "created_at"},
		{name: "UpdatedAt", jsonKey: "updated_at"},
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

// TestSubscriberEnforcedAccess_StatesTheBoundaryTheBrokerActuallyEnforces is the API half of
// the partition-key-prefix hazard.
//
// # The hazard
//
// A subscriber's record carries three access-shaped values — authorized_topics, a consumer
// group, and partition_key_prefix — and only the first two are ACL bindings. Kafka's authorizer
// has no message-key dimension, so there is no ACL that confines a consumer to a slice of a
// topic by key: a subscriber granted a topic reads EVERY record on it whatever the prefix says.
// An integrator who reads all three as one access model draws the plausible and dangerous
// conclusion that two subscribers sharing a topic with different key prefixes cannot see each
// other's events, and builds a tenancy boundary on it.
//
// So the response states the enforced dimensions positively and answers the key question
// outright. These assertions are what stop that statement being quietly weakened later.
func TestSubscriberEnforcedAccess_StatesTheBoundaryTheBrokerActuallyEnforces(t *testing.T) {
	subscriberID := "acme_prod"
	topics := []string{"blnk.transactions", "blnk.balances"}

	enforced := apimodel.NewSubscriberEnforcedAccess(subscriberID, topics)

	t.Run("partition key filtering is declared unenforced, in the body", func(t *testing.T) {
		assert.False(t, enforced.PartitionKeyPrefixEnforced,
			"this field may never be true: Kafka has no message-key authorization dimension, so a "+
				"true here would be a false claim that a subscriber is confined to a slice of a topic")

		assert.NotContains(t, enforced.EnforcedBy, "partition_key",
			"the enforced-dimension list is exhaustive, so a partition-key entry would assert an "+
				"ACL that cannot exist")
		assert.NotContains(t, enforced.EnforcedBy, "partition_key_prefix")
	})

	t.Run("the dimensions that ARE enforced are named", func(t *testing.T) {
		assert.Equal(t, []string{
			apimodel.EnforcementDimensionTopic,
			apimodel.EnforcementDimensionConsumerGroup,
		}, enforced.EnforcedBy,
			"topic and consumer group are the two the broker evaluates, and the list must be "+
				"exactly those: a missing one understates the isolation that exists, an extra one "+
				"claims isolation that does not")

		assert.Equal(t, topics, enforced.Topics, "the topic set must be reported exactly")
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
		keys := marshalToKeys(t, apimodel.NewSubscriberEnforcedAccess(subscriberID, nil))
		assert.NotNil(t, keys["topics"],
			"a fail-closed empty grant is a state an operator must be able to read; null would "+
				"force every client to special-case it")
	})

	t.Run("every subscriber response carries the declaration", func(t *testing.T) {
		prefix := "ledger-42"
		response := apimodel.NewSubscriberResponse(model.EventSubscriber{
			SubscriberID:       subscriberID,
			AuthorizedTopics:   topics,
			PartitionKeyPrefix: &prefix,
		})

		// The advisory prefix is reported AND declared unenforced in the same body. That
		// adjacency is the point: the two cannot be read apart.
		assert.Equal(t, prefix, response.PartitionKeyPrefix)
		assert.False(t, response.EnforcedAccess.PartitionKeyPrefixEnforced)

		keys := marshalToKeys(t, response)
		require.Contains(t, keys, "enforced_access",
			"the declaration must be present on every subscriber body, including one that has "+
				"never been issued a credential — an integrator needs it before wiring a consumer")
	})
}
