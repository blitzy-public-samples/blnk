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
package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/blnkfinance/blnk/model"
)

// This file holds the API-boundary request and response shapes for the Kafka
// event-streaming endpoints (GET /events/dead-letter, POST
// /events/dead-letter/:event_id/replay and GET /events/stats), for the
// subscriber-management endpoints (POST|GET /subscribers, GET|PUT|DELETE
// /subscribers/:subscriber_id and POST /subscribers/:subscriber_id/kafka-credentials),
// and for the legacy webhook-subscription surface that exists only for the 30-day
// dual-delivery window.
//
// Three things are deliberately NOT declared here:
//
//   - No error shape. Error responses are the exclusive business of api/errors.go's
//     respondCode/respondError, which emit {"error", "error_detail"} from a typed code
//     in internal/apierror/codes.go.
//   - No list envelope and no pagination request. api.FilterResponse and
//     api.FilterRequest already exist in api/filter_helper.go, and the handlers wrap
//     the item types below in those. A second envelope here would compete with them.
//   - No consumer-side shapes. Blnk publishes the <topic>.dlt naming convention and
//     builds nothing subscriber-side: no consumer error handling and no
//     subscriber-managed dead-lettering.

// DeadLetterEvent is the item shape returned by GET /events/dead-letter, and the read
// shape for a single dead-lettered event. It is a projection of the persisted
// model.EventOutbox row: the event envelope, the relay's terminal state, and a
// MINIMIZED account of why the retry budget was spent.
type DeadLetterEvent struct {
	// EventID is the UUID that uniquely identifies the event. It is the value
	// callers pass to the replay endpoint, and it doubles as the subscriber
	// idempotency key.
	EventID string `json:"event_id"`

	// EventType is the event name, for example "transaction.applied". It
	// duplicates the event name inside Payload, hoisted to the envelope so an
	// operator can triage without parsing the payload.
	EventType string `json:"event_type"`

	// AggregateID identifies the aggregate the event belongs to: the
	// transaction, balance, identity or ledger the mutation acted on.
	AggregateID string `json:"aggregate_id"`

	// LedgerID is the ledger the event belongs to, when it belongs to one.
	//
	// Optional because not every event category carries a ledger, so it is omitted rather
	// than reported as an empty string.
	LedgerID string `json:"ledger_id,omitempty"`

	// PartitionKey is the Kafka message key this event was published under, and therefore
	// what pinned it to its partition.
	PartitionKey string `json:"partition_key,omitempty"`

	// OccurredAt is the instant the domain action happened, RFC3339 on the
	// wire. It is the ordering column throughout the outbox, never created_at.
	OccurredAt time.Time `json:"occurred_at"`

	// SchemaVersion is the envelope schema version the event was written
	// under, starting at 1.
	SchemaVersion int `json:"schema_version"`

	// Topic is the category topic the event was originally destined for, and
	// the topic a replay re-publishes it to.
	Topic string `json:"topic"`

	// DLTTopic is the dead-letter topic the event was actually written to: the
	// "<topic>.dlt" sibling of Topic. Empty, and so omitted, on a row that has
	// not been dead-lettered.
	DLTTopic string `json:"dlt_topic,omitempty"`

	// Status is the relay state machine's durable state, drawn from the
	// model.EventOutboxStatus* vocabulary. Only a dead_lettered row is
	// eligible for replay.
	Status string `json:"status"`

	// Attempts is the number of publish attempts made before the event was
	// dead-lettered.
	Attempts int `json:"attempts"`

	// FailureReason is a CLASSIFIED reason the event was dead-lettered, drawn from a fixed
	// vocabulary — "broker_unavailable", "message_too_large", "authorization_denied" and
	// so on. It is what an operator triages on, and it deliberately replaces the raw
	// driver text.
	//
	// The raw text is retained in full in the outbox row's last_error column and in
	// failure_metadata.error_reason, where the operations runbook reads it through the
	// database. It is not on the wire because it is verbatim client output: a Kafka write
	// error renders as "write tcp 10.0.0.4:34918->10.0.0.7:9092: broken pipe", naming
	// Blnk's internal addressing and broker topology, and a database error renders with
	// the schema, table, constraint, source file and routine that produced it. A
	// classified reason answers the triage question — is this the broker, the event, or
	// the grant?
	FailureReason string `json:"failure_reason,omitempty"`

	// FirstAttemptedAt is when the relay first tried to publish the event, and
	// LastAttemptedAt is when it last tried. Together with Attempts they are the whole of
	// what the dead-letter age alert and the triage runbook need from the failure record,
	// and neither carries transport detail.
	FirstAttemptedAt *time.Time `json:"first_attempted_at,omitempty"`
	LastAttemptedAt  *time.Time `json:"last_attempted_at,omitempty"`

	// PayloadBytes is the size of the stored event body. It is what an operator
	// needs from the payload for triage — a message_too_large classification is
	// confirmed or refuted by this number alone — without the body itself.
	PayloadBytes int `json:"payload_bytes"`
}

// The failure-reason vocabulary. Every value an operator can see is one of these, and
// each one answers a different triage question with a different next action:
//
//   - FailureReasonBrokerUnavailable: the broker did not answer or dropped the
//     connection. Check the cluster, then replay.
//   - FailureReasonAuthorizationDenied: the producer's own credential was refused or
//     lacks Write on the topic. Replaying will fail identically until the grant is
//     fixed.
//   - FailureReasonMessageTooLarge: the event exceeds the configured maximum. It will
//     never publish as it stands; PayloadBytes confirms it.
//   - FailureReasonTopicMissing: the destination topic does not exist. Provision it,
//     then replay.
//   - FailureReasonTimeout: the attempt exceeded its deadline. Usually load; replay.
//   - FailureReasonPersistence: the failure was Blnk's own database rather than Kafka.
//     The event is intact; the relay's bookkeeping failed.
//   - FailureReasonUnclassified: the stored text matched nothing above. The full text
//     is in the outbox row for an operator with database access, and this value says so
//     rather than guessing.
const (
	FailureReasonBrokerUnavailable   = "broker_unavailable"
	FailureReasonAuthorizationDenied = "authorization_denied"
	FailureReasonMessageTooLarge     = "message_too_large"
	FailureReasonTopicMissing        = "topic_missing"
	FailureReasonTimeout             = "timeout"
	FailureReasonPersistence         = "persistence_failure"
	FailureReasonUnclassified        = "unclassified"
)

// failureReasonSignatures maps a lowercase substring of stored failure text to the
// classification it implies, most specific first.
//
// Order matters. "authorization" is checked before "unavailable" because a broker can
// report both in one message, and the authorization failure is the actionable half: it
// will not clear on its own, whereas an unavailable broker might.
var failureReasonSignatures = []struct {
	signature string
	reason    string
}{
	{"authoriz", FailureReasonAuthorizationDenied},
	{"authentic", FailureReasonAuthorizationDenied},
	{"sasl", FailureReasonAuthorizationDenied},
	{"too large", FailureReasonMessageTooLarge},
	{"message size", FailureReasonMessageTooLarge},
	{"unknown topic", FailureReasonTopicMissing},
	{"topic does not exist", FailureReasonTopicMissing},
	{"leader not available", FailureReasonTopicMissing},
	{"timeout", FailureReasonTimeout},
	{"deadline exceeded", FailureReasonTimeout},
	{"database", FailureReasonPersistence},
	{"sql", FailureReasonPersistence},
	{"connection refused", FailureReasonBrokerUnavailable},
	{"broken pipe", FailureReasonBrokerUnavailable},
	{"no such host", FailureReasonBrokerUnavailable},
	{"unavailable", FailureReasonBrokerUnavailable},
	{"reset by peer", FailureReasonBrokerUnavailable},
	{"eof", FailureReasonBrokerUnavailable},
	{"not acknowledge", FailureReasonBrokerUnavailable},
}

// classifyFailureReason reduces stored failure text to one vocabulary value.
//
// The RETURN IS ALWAYS FROM THE VOCABULARY — never a fragment of the input, never the
// input itself. That is the property that makes this function the sanitizer rather than
// merely a formatter of one: no input, however constructed, can produce output that
// describes the deployment.
//
// Parameters:
//   - raw string: the stored last_error or failure_metadata.error_reason text.
//
// Returns:
//   - string: a vocabulary value, or "" when there was no failure text at all.
func classifyFailureReason(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}

	lowered := strings.ToLower(raw)
	for _, candidate := range failureReasonSignatures {
		if strings.Contains(lowered, candidate.signature) {
			return candidate.reason
		}
	}

	return FailureReasonUnclassified
}

// NewDeadLetterEvent projects a stored outbox row into its minimized response shape.
//
// Every handler that returns a dead-lettered event goes through here, and that is the
// point: the payload, the raw error text and the failure struct are dropped in exactly
// one place. A handler cannot leak them by writing the obvious field assignment,
// because the fields to assign them to do not exist on the result.
//
// Parameters:
//   - row model.DeadLetterInventoryEntry: the NARROW inventory projection. It carries
//     the body's size and not the body, so this function cannot leak a payload even in
//     principle
//
// Returns:
//   - DeadLetterEvent: the response item, carrying no payload and no raw failure text.
func NewDeadLetterEvent(row model.DeadLetterInventoryEntry) DeadLetterEvent {
	item := DeadLetterEvent{
		EventID:     row.EventID,
		EventType:   row.EventType,
		AggregateID: row.AggregateID,
		LedgerID:    row.LedgerID,
		// THE EFFECTIVE KEY — what the publish path actually keyed on, resolved through the
		// model's own rule rather than read straight off the stored column.
		PartitionKey:     row.EffectiveKey(),
		OccurredAt:       row.OccurredAt,
		SchemaVersion:    row.SchemaVersion,
		Topic:            row.Topic,
		DLTTopic:         row.DLTTopic,
		Status:           row.Status,
		Attempts:         row.Attempts,
		FailureReason:    classifyFailureReason(row.LastError),
		FirstAttemptedAt: row.FirstAttemptedAt,
		LastAttemptedAt:  row.LastAttemptedAt,
		// Measured in SQL by the projection, not from bytes held in memory here The inventory
		// query never reads the body, so this is the only place its size can come from — and
		// reading whole rows to compute a length was what moved gibibytes to build a listing
		// of kilobytes.
		PayloadBytes: row.PayloadBytes,
	}

	// The failure metadata is read ONLY to fill gaps the row's own columns leave, and only
	// for values that carry no transport detail. error_reason is deliberately not read
	// here when last_error already classified: both are the same raw text, and the
	// classification of either is the same answer.
	if len(row.FailureMetadata) > 0 {
		var metadata model.FailureMetadata
		if err := json.Unmarshal(row.FailureMetadata, &metadata); err == nil {
			if item.FailureReason == "" {
				item.FailureReason = classifyFailureReason(metadata.ErrorReason)
			}
			if item.FirstAttemptedAt == nil && !metadata.FirstAttemptedAt.IsZero() {
				first := metadata.FirstAttemptedAt
				item.FirstAttemptedAt = &first
			}
			if item.LastAttemptedAt == nil && !metadata.LastAttemptedAt.IsZero() {
				last := metadata.LastAttemptedAt
				item.LastAttemptedAt = &last
			}
			if item.Attempts == 0 {
				item.Attempts = metadata.AttemptCount
			}
		}
		// A metadata blob that does not unmarshal is passed over in silence rather than
		// surfaced: it is Blnk's own bookkeeping, the envelope fields above are already
		// populated from the row's columns, and an error here would report an internal
		// inconsistency to a caller who can do nothing about it.
	}

	return item
}

// ReplayEventResponse is returned by POST /events/dead-letter/:event_id/replay.
//
// Topic is the topic the event was replayed TO, which is its original category topic
// rather than the dead-letter topic it was read from. The response deliberately does
// not echo the payload bytes back: replay fidelity is established by what the consumer
// receives on the original topic, not by the body of this acknowledgement, and echoing
// a payload would invite callers to diff the wrong pair of byte strings.
type ReplayEventResponse struct {
	// EventID is the event that was replayed, unchanged from the original.
	EventID string `json:"event_id"`

	// Topic is the original category topic the event was re-published to.
	Topic string `json:"topic"`

	// Status reports the outcome of the re-publish using the
	// model.PublishStatus vocabulary. Omitted when the handler has nothing
	// more specific to add than the 200 itself.
	Status string `json:"status,omitempty"`

	// ReplayedAt is the instant the re-publish was acknowledged.
	ReplayedAt time.Time `json:"replayed_at"`
}

// ProducerAtomicityStats reports how much of the two pre-recorded intents is
// outstanding.
//
// Almost every event is written inside the database transaction that performs its
// mutation. Where something ELSE is written atomically instead — an intent — the event
// is captured from it later, also atomically, and while the intent is outstanding the
// event is OWED.
//
// There are two intents, and they are not equally common:
//
//   - a bulk batch coordinator row, written before the batch begins, meaning "this
//     batch has not reported an outcome yet". This is the ordinary route for every
//     batch summary, because a summary belongs to no single member transaction.
//   - a balance-monitor handoff, written inside the balance's own transaction, meaning
//     "these monitors have not been judged yet". This is NO LONGER the ordinary route:
//     the atomic writers evaluate a moved balance's monitors inside their own
//     transaction and insert the alert row there, so a movement made by any current
//     release records no handoff.
type ProducerAtomicityStats struct {
	// MonitorHandoffPending counts balance movements whose monitors are waiting to be
	// evaluated. Only pre-capture rows reach this count, so on a deployment that has
	// finished draining them it sits at zero permanently, and a number that starts
	// climbing again is worth investigating rather than ignoring.
	MonitorHandoffPending int64 `json:"monitor_handoff_pending"`

	// MonitorHandoffProcessing counts handoffs a processor currently holds.
	MonitorHandoffProcessing int64 `json:"monitor_handoff_processing"`

	// MonitorHandoffCompleted counts handoffs that have been evaluated. It includes the
	// overwhelmingly common case of "evaluated, nothing fired", which is why it is far
	// larger than the number of alerts ever published.
	MonitorHandoffCompleted int64 `json:"monitor_handoff_completed"`

	// MonitorHandoffFailed counts handoffs whose evaluation budget is spent.
	MonitorHandoffFailed int64 `json:"monitor_handoff_failed"`

	// UnfinalizedBatches counts asynchronous bulk batches that began and never reported an
	// outcome, past a grace period so batches still legitimately running are excluded.
	UnfinalizedBatches int64 `json:"unfinalized_batches"`

	// OldestUnfinalizedBatchAt is when the oldest outstanding batch began, omitted when
	// there are none. Age is what separates a large batch still running from one that was
	// abandoned, so the count alone is not actionable and this is.
	OldestUnfinalizedBatchAt *time.Time `json:"oldest_unfinalized_batch_at,omitempty"`
}

// EventOutboxStatsResponse is returned by GET /events/stats and serves the daily
// zero-loss reconciliation procedure in the operations runbook: the sum of dispatched
// and dead-lettered rows is reconciled against the end offsets of the main and
// dead-letter topics, and the two must agree.
type EventOutboxStatsResponse struct {
	// Pending counts rows written and committed but not yet claimed.
	Pending int64 `json:"pending"`

	// Processing counts rows currently claimed by a relay instance.
	Processing int64 `json:"processing"`

	// WebhookPending counts rows whose KAFKA leg is complete and acknowledged and whose
	// legacy HTTP leg is still owed.
	WebhookPending int64 `json:"webhook_pending"`

	// Dispatched counts rows the broker has acknowledged, inside the reported window.
	// Terminal.
	Dispatched *int64 `json:"dispatched,omitempty"`

	// DispatchedHistoryCounted reports whether the dispatched count above was taken.
	//
	// It is false whenever `include_offsets` was absent or false, which is the cheap
	// reading every routine caller should take, and true whenever it was true.
	DispatchedHistoryCounted bool `json:"dispatched_history_counted"`

	// Failed counts rows whose retry budget is spent but which have not yet
	// been written to a dead-letter topic.
	Failed int64 `json:"failed"`

	// DeadLettered counts rows written to a dead-letter topic. Terminal, and
	// the state from which an event may be replayed.
	DeadLettered int64 `json:"dead_lettered"`

	// Replaying counts dead-lettered rows a replay has CLAIMED and not yet finished with.
	//
	// It is a lease, not a resting place: the row is held so two concurrent replays of the
	// same event cannot both publish it, and it returns to dead_lettered when the replay
	// completes or its lease expires and the relay's recovery sweep reclaims it.
	//
	// It is reported for two reasons. Omitting it made the six statuses sum to less than
	// the table's row count whenever a replay was in flight, so a reconciliation reading
	// the response could not tell a short total from a lost event — which is precisely the
	// distinction it exists to make.
	//
	// It is NOT terminal, so it does not contribute to TerminalEvents in the
	// reconciliation below.
	Replaying int64 `json:"replaying"`

	// ProducerAtomicity reports the two PRE-RECORDED INTENTS an event can still be
	// captured from, and specifically how much of each is outstanding.
	ProducerAtomicity *ProducerAtomicityStats `json:"producer_atomicity,omitempty"`

	// TopicEndOffsets is the per-topic end offset read from the broker, the right-hand
	// side of the reconciliation.
	TopicEndOffsets map[string]int64 `json:"topic_end_offsets,omitempty"`

	// OffsetsComplete reports whether TopicEndOffsets is a COMPLETE reading of the broker
	// side, and therefore whether the reconciliation may be performed at all. It is the
	// single field a script should branch on before comparing anything.
	//
	// It carries no omitempty, deliberately, so it is present on every response and a
	// client never has to infer completeness from a missing key. The three states it
	// distinguishes are:
	//
	//   - true: every requested topic was found and every partition reported. The
	//     comparison the runbook makes is valid.
	//   - false with TopicEndOffsets absent: no offsets could be read at all — a
	//     deployment with no brokers configured, or a broker that could not be reached.
	//     There is nothing to compare against, which is not an error.
	//   - false with TopicEndOffsets present: the reading is PARTIAL. The sums are short
	//     through unreadability rather than through loss, and MissingTopics and
	//     PartitionsUnavailable say which part is missing.
	OffsetsComplete bool `json:"offsets_complete"`

	// MissingTopics lists topics the reading asked for that do not exist on the broker.
	// They contribute nothing to TopicEndOffsets, so their absence lowers the broker side
	// of the comparison without any event having been lost. Omitted when none were
	// missing.
	MissingTopics []string `json:"missing_topics,omitempty"`

	// PartitionsUnavailable counts partitions the broker could not report, summed across
	// every topic measured. Each one is a partition whose records are missing from
	// TopicEndOffsets, so a non-zero value invalidates the comparison in the same way a
	// missing topic does.
	PartitionsUnavailable int `json:"partitions_unavailable"`

	// MeasuredWindows is the per-partition offset window the zero-loss verdict was
	// computed against: [first_offset, end_offset) for every partition the broker
	// reported.
	//
	// It is reported because the verdict is a statement ABOUT these windows — each outbox
	// row's stored coordinate is checked for membership in the window of its own partition
	// — so without them a reader cannot tell what was covered. That is not a hypothetical
	// gap: the check this replaced compared whole-topic cumulative end offsets against
	// all-time retained rows, two populations with no common window, and reported itself
	// as conclusive anyway.
	//
	// A partition the broker could not report is ABSENT here rather than present with
	// zeroed bounds, because a zero-width window would misclassify every row on it as
	// beyond the log end. Its rows appear in the verdict's unmeasured_events instead, and
	// partitions_unavailable says how many.
	//
	// Nil, and so omitted, when no offsets were read.
	MeasuredWindows []MeasuredOffsetWindow `json:"measured_windows,omitempty"`

	// OffsetsMeasuredAt is when the broker-side reading was taken. It is a different
	// instant from GeneratedAt: the counts come from PostgreSQL and the offsets from
	// Kafka, in separate round trips, so under live traffic a small difference between the
	// two sides is expected rather than suspicious, and its size is only interpretable
	// against the gap between these two timestamps. Nil, and so omitted, when no offsets
	// were read.
	OffsetsMeasuredAt *time.Time `json:"offsets_measured_at,omitempty"`

	// WindowStart is the earliest instant the WINDOWED figures cover, and WindowSeconds is
	// its length.
	//
	// The window is chosen with ?window=, defaults to 24h and is capped at 168h.
	WindowStart   *time.Time `json:"window_start,omitempty"`
	WindowSeconds int64      `json:"window_seconds,omitempty"`

	// GeneratedAt is the instant the snapshot was taken. The counts and the offsets are
	// read at slightly different moments under live traffic, so a reconciliation that
	// compares them needs to know when the snapshot was made.
	GeneratedAt time.Time `json:"generated_at"`

	// Reconciliation is the SERVER'S OWN VERDICT on the zero-loss comparison, present
	// whenever the broker side could be measured at all.
	Reconciliation *OutboxReconciliationResult `json:"reconciliation,omitempty"`
}

// MeasuredOffsetWindow is one partition's measured offset window, as reported to a
// caller of the statistics endpoint.
//
// It exists because the zero-loss verdict is a statement ABOUT these windows: each
// outbox row's stored coordinate is checked for membership in the window of its own
// partition. Reporting the verdict without the windows would leave a reader unable to
// tell which offsets were actually covered — which is precisely how an unbounded
// comparison of two whole-topic totals came to present itself as conclusive.
type MeasuredOffsetWindow struct {
	// Topic is the fully-qualified topic name.
	Topic string `json:"topic"`

	// Partition is the partition ID.
	Partition int `json:"partition"`

	// FirstOffset is the earliest offset the broker still retains, INCLUSIVE.
	FirstOffset int64 `json:"first_offset"`

	// EndOffset is one past the last offset written, EXCLUSIVE. A stored coordinate
	// at or above it cannot be on the log, which is why that condition is reported
	// as loss rather than as a caveat.
	EndOffset int64 `json:"end_offset"`

	// Records is how many records the window holds: end minus first, never negative. It is
	// included so a reader does not have to subtract, and because zero is a meaningful
	// reading — an empty partition, or one every record of which has aged out.
	Records int64 `json:"records"`
}

// NewMeasuredOffsetWindows projects the measured intervals onto the wire.
//
// The projection is one-way and total: every interval measured is reported, in the
// order the measurement produced, so the response cannot describe a narrower set of
// windows than the verdict was computed from.
//
// Parameters:
//   - intervals []model.PartitionOffsetInterval: the measured windows.
//
// Returns:
//   - []MeasuredOffsetWindow: one entry per interval, nil when nothing was measured so
//     the key is omitted rather than rendered as an empty array.
func NewMeasuredOffsetWindows(intervals []model.PartitionOffsetInterval) []MeasuredOffsetWindow {
	if len(intervals) == 0 {
		return nil
	}

	windows := make([]MeasuredOffsetWindow, 0, len(intervals))
	for _, interval := range intervals {
		windows = append(windows, MeasuredOffsetWindow{
			Topic:       interval.Topic,
			Partition:   interval.Partition,
			FirstOffset: interval.FirstOffset,
			EndOffset:   interval.EndOffset,
			Records:     interval.Records(),
		})
	}

	return windows
}

// placed inside the measured offset window of the partition its record is on.
//
// So the verdict is now per row. Each row either names a record inside the measured
// window of its own partition — CorroboratedEvents — or it is counted under the
// specific reason it could not be placed: it names no record, it names an unmeasured
// topic or partition, its record has aged out of retention, or its offset is AT OR
// ABOVE the log end.
type OutboxReconciliationResult struct {
	// TerminalEvents is how many outbox rows claim to have been published: dispatched and
	// webhook_pending, whose Kafka leg completed, plus dead-lettered, each counted exactly
	// once. Rows in pending, processing or replaying make no such claim and are excluded.
	TerminalEvents int64 `json:"terminal_events"`

	// CorroboratedEvents is how many of those rows name a record INSIDE the measured
	// window of its partition — a record the broker can serve right now.
	CorroboratedEvents int64 `json:"corroborated_events"`

	// UnconfirmedEvents is how many rows claim a publication they cannot name a
	// record for, and it is the field that stops a surplus reading as health.
	UnconfirmedEvents int64 `json:"unconfirmed_events"`

	// UnmeasuredEvents is how many rows name a topic or partition this measurement did not
	// cover — a missing topic, an unavailable partition, or a partition count that has
	// since shrunk. Their records may well be there; this reading neither confirmed nor
	// ruled them out.
	UnmeasuredEvents int64 `json:"unmeasured_events"`

	// AgedOutEvents is how many rows name a record Kafka retention has already deleted:
	// written, evidenced by the stored offset, and no longer readable.
	AgedOutEvents int64 `json:"aged_out_events"`

	// BeyondEndEvents is how many rows name an offset AT OR ABOVE the end of their
	// partition's log.
	BeyondEndEvents int64 `json:"beyond_end_events"`

	// DuplicatedRecords is how many corroborated rows share a coordinate with another row.
	// It should be zero always.
	DuplicatedRecords int64 `json:"duplicated_records"`

	// MessagesWritten is how many records the broker has accepted across the measured
	// topics, from summed end offsets, and RecordsRetained how many of those it still
	// holds.
	MessagesWritten int64 `json:"messages_written"`
	RecordsRetained int64 `json:"records_retained"`

	// BlnkRecordShare is how many of the retained records this reconciliation attributed to
	// Blnk's own published events, so a topic shared with another producer can still be
	// reconciled: the surplus is measured against Blnk's share rather than the whole log.
	BlnkRecordShare int64 `json:"blnk_record_share"`

	// This DTO reports a WINDOWED comparison: both sides are measured over the same
	// per-partition windows and every claim is classified in SQL over those windows, which
	// is what corroborated_events, aged_out_events, beyond_end_events and unmeasured_events
	// above report. There is deliberately no whole-history field — inside a window there is
	// nothing for a purge log to correct for, because rows retention has deleted are older
	// than the window and so are on neither side of it.

	// Overhead is MessagesWritten minus AllTimeTerminalEvents: the redeliveries, replays
	// and dead-letter copies.
	Overhead int64 `json:"overhead"`

	// LossDetected is true when specific records this outbox recorded are provably not on
	// the log — a row naming an offset at or beyond its partition's end.
	LossDetected bool `json:"loss_detected"`

	// Conclusive reports whether the mapping accounted for EVERY retained claim: false
	// when any row was unconfirmed, unmeasured, aged out or beyond the log end, when two
	// rows shared a coordinate, when a measured topic was missing, or when a partition did
	// not report.
	Conclusive bool `json:"conclusive"`

	// WindowStart is the instant BOTH sides of this comparison were measured from, and
	// Windowed reports whether they were measured over a common window at all.
	WindowStart *time.Time `json:"window_start,omitempty"`
	Windowed    bool       `json:"windowed"`

	// Caveats names, in plain words, every reason the result is inconclusive, so
	// an operator is told what to do rather than merely that something is wrong.
	// Empty and omitted when Conclusive is true.
	Caveats []string `json:"caveats,omitempty"`

	// CoveredFrom and CoveredTo bound the publication instants of the corroborated
	// population: the window a green verdict actually speaks about. Nil, and
	// omitted, when nothing was corroborated.
	CoveredFrom *time.Time `json:"covered_from,omitempty"`
	CoveredTo   *time.Time `json:"covered_to,omitempty"`

	// OldestTerminalAt is the earliest publication instant among all retained terminal
	// rows. Events published before it have been pruned from the outbox and are outside
	// the reach of any verdict, so it is reported rather than left implicit — a
	// reconciliation over an aggressively pruned outbox would otherwise look complete.
	// Nil, and omitted, when no terminal rows exist.
	OldestTerminalAt *time.Time `json:"oldest_terminal_at,omitempty"`

	// Summary is the verdict as one sentence, always naming the numbers it rests
	// on so the sentence is checkable against the fields above. It is what a
	// runbook or an alert annotation quotes.
	Summary string `json:"summary"`

	// MeasuredAt is when the broker side was measured. It is the same instant as
	// the enclosing response's OffsetsMeasuredAt and is repeated here so this
	// object stands alone when it is logged or forwarded on its own.
	MeasuredAt time.Time `json:"measured_at"`
}

// CreateSubscriber is the request body for POST /subscribers.
//
// A subscriber is a Kafka principal: registering one records who may consume, which
// topics they are entitled to and under which consumer group, and it is the row a later
// credential issuance attaches its non-reversible reference to. Registration and
// provisioning are separate steps, so a freshly created subscriber legitimately holds
// no credential at all.
//
// AuthorizedTopics is intentionally not required. The column defaults to the empty
// array, which means a subscriber registered without an explicit grant is authorised
// for nothing rather than for everything: the registry fails closed, and widening a
// grant is a deliberate follow-up call.
type CreateSubscriber struct {
	// SubscriberID is the business key, and the {id} in POST
	// /subscribers/{id}/kafka-credentials. Omit it to have the service generate one in the
	// repository's "<prefix>_<uuid>" form.
	SubscriberID string `json:"subscriber_id"`

	// Name is the human label the subscriber is triaged by. Required: it is the one field
	// the service cannot invent a meaningful value for, it is NOT NULL in
	// blnk.event_subscribers, and it is what an operator recognises a principal by months
	// later.
	Name string `json:"name"`

	// AuthorizedTopics is the set of topics this subscriber may Read and Describe, mapping
	// to the authorized_topics TEXT[] column and to the exact set of ACL bindings
	// provisioned for the principal. Omitted or empty means no grant, which is the safe
	// default rather than a missing value.
	AuthorizedTopics []string `json:"authorized_topics" binding:"max=16,dive,max=249"`

	// PartitionKeyPrefix names the THIRD SCOPE of the access model: the subscriber is
	// entitled only to records whose message key carries this prefix. Because every Blnk
	// event is keyed by ledger id, that is a ledger boundary.
	//
	// KAFKA DOES NOT ENFORCE IT, and the credential response says so in the same object
	// that carries it — see SubscriberEnforcedAccess.PartitionKeyPrefixEnforcedBy, which
	// names the component that does. There is no ACL that narrows a principal to a key
	// range, and per-tenant topics, the only Kafka-native alternative, are deliberately
	// not created.
	//
	// SO REGISTERING ONE IS NOT THE SAME AS BEING ABLE TO USE IT. The row is accepted
	// here, and a row is cheap; what a prefix costs is a credential.
	//
	// BLNK SHIPS NO SUCH COMPONENT AND SERVES NO RECORDS ITSELF. So on the shipped
	// default, POST /subscribers/:subscriber_id/kafka-credentials answers 409
	// SUBSCRIBER_KEY_SCOPE_UNENFORCED for a row carrying this field rather than minting a
	// principal that can fetch nothing.
	//
	// AND A DECLARED COMPONENT IS VERIFIED, NOT TAKEN ON TRUST. Where one is declared,
	// issuance calls its control endpoint over an authenticated channel before a secret
	// exists and requires it to confirm that it enforces key scopes, for this exact
	// principal, with THIS EXACT PREFIX byte-for-byte. An unreachable, refusing or
	// disagreeing component answers 409 SUBSCRIBER_KEY_SCOPE_UNATTESTED.
	//
	// OMITTING IT IS ALSO A DECISION, and in a key-scoped deployment it is refused. A row
	// with no prefix is issued literal topic Read, so it reads every ledger's records on
	// each granted topic; where the deployment declares the key-scoped model that is the
	// one principal the boundary does not cover, and issuance answers 409
	// SUBSCRIBER_KEY_SCOPE_REQUIRED.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// NO webhook_url FIELD, and its absence is the sunset being enforceable.
	//
	// The four deprecated webhook-subscription routes are each fronted by
	// middleware.WebhookSunsetGuard and answer 410 Gone after the retirement instant — but
	// this route is not deprecated and is not guarded, so a caller could keep writing
	// legacy webhook state through it indefinitely after the surface that owns it had been
	// retired. The sunset was bypassable by using a different route, which is the same as
	// not having one.
}

// errSubscriberNameRequired is the single refusal for a missing subscriber name.
//
// One value, so the prefix-aware Validate and the prefix-free
// ValidateCreateSubscriber cannot answer the same broken rule with two different
// messages — and so a caller matching on it matches one string.
var errSubscriberNameRequired = errors.New("name is required")

// Derived returns the Kafka principal and consumer group for this request.
//
// It is the ONLY way a handler obtains either, which is what keeps them derived rather
// than chosen. Calling it on a request whose SubscriberID is empty is a programming
// error the handler must avoid by generating the identifier first — there is nothing to
// derive an identity from until one exists.
//
// Returns:
//   - principal string: "blnk-sub-<subscriber_id>".
//   - consumerGroup string: "blnk-sub-<subscriber_id>.default".
//   - err error: wrapping model.ErrInvalidSubscriberIdentifier when the identifier is
//     not canonical.
func (c CreateSubscriber) Derived() (principal, consumerGroup string, err error) {
	principal, err = model.CanonicalKafkaPrincipal(c.SubscriberID)
	if err != nil {
		return "", "", err
	}

	consumerGroup, err = model.CanonicalConsumerGroupID(c.SubscriberID)
	if err != nil {
		return "", "", err
	}

	return principal, consumerGroup, nil
}

// Validate checks every caller-supplied value on the request.
//
// It exists because binding tags cannot express any of these rules, and because the
// alternative — leaving them to the handler — means the rules hold only for the
// handlers that remember them. A DTO that can validate itself is validated the same way
// by every caller.
//
// Parameters:
//   - topicPrefix string: the configured KAFKA_TOPIC_PREFIX, needed to resolve which
//     topic names this deployment owns. A blank value falls back to the strictest
//     namespace rather than a permissive one.
//   - options ...SubscriberGrantOption: the deployment facts a grant is judged against,
//     currently WithInternalTopicAccess. Passing no option means nothing is
//     acknowledged, so `<prefix>.system` is refused; pass WithInternalTopicAccess(true)
//     only where KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS is declared. Omission is
//     therefore fail-closed, which is why the variadic form is safe for callers that do
//     not know about the privileged category at all.
//
// Returns:
//   - error: describing the first violation, nil when the request is usable.
func (c CreateSubscriber) Validate(topicPrefix string, options ...SubscriberGrantOption) error {
	// The prefix-independent rules run FIRST and are shared with nothing else, which is
	// what makes this the single validation entry point.
	if err := c.validateBodyRules(); err != nil {
		return err
	}

	// The identifier is checked only when supplied: an omitted one is generated
	// by the service, and generation produces a canonical value by construction.
	if c.SubscriberID != "" {
		if _, err := model.CanonicalizeSubscriberIdentifier(c.SubscriberID); err != nil {
			return err
		}
	}

	if err := validateGrantableTopics(c.AuthorizedTopics, topicPrefix, options...); err != nil {
		return err
	}

	return validateSubscriberKeyScope(c.PartitionKeyPrefix)
}

// UpdateSubscriber is the request body for PUT /subscribers/:subscriber_id and carries
// the mutable subset of a subscriber only.
//
// Every field is a pointer so that "omitted" is distinguishable from "explicitly set to
// empty", which this shape genuinely needs rather than merely benefits from.
// partition_key_prefix is the clear case: NULL means the subscriber is entitled to
// whole topics, while the empty string would mean restricted to the empty prefix, and
// those are opposite intents. A plain string cannot express the difference, so it would
// make silently inverting an operator's intent possible.
//
// Four groups of fields are deliberately absent:
//
//   - KafkaPrincipal. It is the join key to every ACL binding already provisioned for
//     this subscriber, so changing it would orphan them all and leave the registry
//     claiming access the broker does not grant.
//   - ConsumerGroupID, absent for the same two reasons. It is derived from the
//     subscriber ID, and it is the resource the group ACL is granted over with a
//     PREFIXED pattern type — so a caller able to edit it could name a prefix spanning
//     other subscribers' group namespaces, join their consumer groups, and take their
//     partition assignments.
//   - CredentialReference and CredentialIssuedAt. Those two together are the record of
//     an issuance, written only by the credential endpoint.
//   - MigratedAt, and any secret of any kind. The service owns stamping the migration
//     instant, and no request shape anywhere accepts a credential.
type UpdateSubscriber struct {
	// Name replaces the human label when present. Bounded in the binder for the
	// same reason the create request's is; see validateSubscriberName.
	Name *string `json:"name,omitempty" binding:"omitempty,max=1024"`

	// AuthorizedTopics replaces the whole authorised set when present. It is already
	// nilable as a slice, so no pointer is needed to tell "omitted" from "set to empty":
	// nil is omitted, and a present empty array revokes every topic grant.
	AuthorizedTopics []string `json:"authorized_topics,omitempty" binding:"omitempty,max=16,dive,max=249"`

	// PartitionKeyPrefix RECORDS the key-scoped authorization when present and non-empty,
	// and CLEARS it when present and empty. See the type comment for why this cannot be a
	// plain string: absent and "clear it" are different requests.
	PartitionKeyPrefix *string `json:"partition_key_prefix,omitempty"`

	// NO webhook_url FIELD. See the note on CreateSubscriber: this route is not
	// deprecated and not fronted by the sunset guard, so accepting legacy webhook state
	// here would let a caller keep writing it after the guarded routes had begun
	// answering 410 Gone. PUT /subscribers/:subscriber_id/webhook-subscription is the
	// one write path for it, and it is guarded.
}

// Validate checks every caller-supplied value that is present on the request.
//
// Absent fields are not checked, because absent means "leave as stored" and the stored
// value was validated when it was written. A field present but empty IS checked,
// because that is an explicit instruction to clear, and clearing is legitimate for the
// key scope, the topic list and the legacy URL alike.
//
// Parameters:
//   - topicPrefix string: the configured KAFKA_TOPIC_PREFIX.
//   - options ...SubscriberGrantOption: the deployment facts a grant is judged against,
//     on the same fail-closed terms as CreateSubscriber.Validate — omitting
//     WithInternalTopicAccess(true) refuses `<prefix>.system`.
//
// Returns:
//   - error: describing the first violation, nil when the request is usable.
func (u UpdateSubscriber) Validate(topicPrefix string, options ...SubscriberGrantOption) error {
	// The prefix-independent rules first, for the reason CreateSubscriber.Validate gives.
	if err := u.validateBodyRules(); err != nil {
		return err
	}

	if u.AuthorizedTopics != nil {
		if err := validateGrantableTopics(u.AuthorizedTopics, topicPrefix, options...); err != nil {
			return err
		}
	}

	// PartitionKeyPrefix is checked at the SAME standard as create. The prefix is issued
	// with the credential as a disclosed, consumer-side filtering contract, so it is
	// persisted, echoed in the credential response and written into log fields, and this
	// validator is the only thing standing between a caller and a control character or a
	// half-kilobyte value in all three. A rule applied only on creation is a rule with an
	// edit-shaped hole, and an edit is how a hostile value arrives: creation is scripted
	// from a template, editing is done by hand.
	if u.PartitionKeyPrefix != nil {
		if err := validateSubscriberKeyScope(*u.PartitionKeyPrefix); err != nil {
			return err
		}
	}

	return nil
}

// MaxSubscriberNameLength bounds the one free-text, caller-supplied field on a
// subscriber.
//
// 256 characters is generous for a human label in any script — it is not a description
// field — while staying far below anything that could matter for storage, response size
// or log volume. The DTO refuses it here so a malformed body is rejected where it is
// cheapest; the service refuses it so a CLI or migration cannot bypass the DTO; and
// event_subscribers_name_length_chk refuses it so a psql session, a data migration or a
// restored backup cannot either. Each layer covers callers the layer above does not
// see.
const MaxSubscriberNameLength = 256

// validateBodyRules applies the rules on a create body that need no topic prefix.
//
// Validate is the single entry point, and it has two kinds of rule inside it. These are
// facts about the BODY — a name is present, it is bounded, the topic list is within its
// resource limits — and they hold identically in every deployment.
//
// Returns:
//   - error: describing the first violation, nil when the body's own rules hold.
func (c CreateSubscriber) validateBodyRules() error {
	if err := validateSubscriberName(c.Name, true); err != nil {
		return err
	}

	// The RESOURCE bounds on the grant — cardinality, blank elements, name length, the
	// Kafka character set and duplicates. They are prefix-independent by nature, and they
	// are applied here rather than left to the binding tags because the tags can express
	// only two of the five.
	return model.ValidateSubscriberTopics(c.AuthorizedTopics)
}

// validateBodyRules applies the rules on an update body that need no topic prefix.
//
// No field is required: an update carries the mutable subset a caller chose to change,
// and every field is a pointer or a nilable slice precisely so that "omitted" stays
// distinguishable from "set to empty".
//
// Returns:
//   - error: describing the first violation, nil when the body's own rules hold.
func (u UpdateSubscriber) validateBodyRules() error {
	if u.Name != nil {
		if err := validateSubscriberName(*u.Name, true); err != nil {
			return err
		}
	}

	if u.AuthorizedTopics == nil {
		return nil
	}

	return model.ValidateSubscriberTopics(u.AuthorizedTopics)
}

// validateSubscriberName applies the one bound and the one requirement on a subscriber
// name.
//
// The name is a LABEL, not an authorization: nothing is granted on the strength of it,
// so the rules are about it being storable, displayable and bounded rather than about
// isolation.
//
//   - PRESENCE, when required. It is the one field no service can invent a meaningful
//     value for, it is NOT NULL in blnk.event_subscribers, and being able to answer
//     "who is this principal?" months later is most of the reason the registry exists.
//   - LENGTH, always. See MaxSubscriberNameLength for why an unbounded value is
//     amplified by every response and every log line.
//   - CONTROL CHARACTERS, always. The value is echoed into API responses, log lines and
//     trace attributes; a newline in it splits a log line in two and forges a second
//     entry, and a carriage return can overwrite one on a terminal.
//
// Parameters:
//   - name string: the value as supplied.
//   - required bool: whether a blank value is a violation. False is unused today and
//     exists so an optional-name shape can reuse the bound rather than reimplementing
//     it.
//
// Returns:
//   - error: describing the violation, nil when the name is usable.
func validateSubscriberName(name string, required bool) error {
	trimmed := strings.TrimSpace(name)

	if trimmed == "" {
		if required {
			// THE SENTINEL, not a fresh fmt.Errorf. One shared validator already guarantees the
			// two Validate paths answer a missing name with the same words; returning the
			// package's own error value additionally makes the refusal MATCHABLE, so a caller
			// that has to distinguish "no name" from every other validation failure uses
			// errors.Is rather than comparing prose.
			return errSubscriberNameRequired
		}

		return nil
	}

	if utf8.RuneCountInString(trimmed) > MaxSubscriberNameLength {
		return fmt.Errorf(
			"name must be at most %d characters, got %d",
			MaxSubscriberNameLength, utf8.RuneCountInString(trimmed),
		)
	}

	for _, r := range trimmed {
		if unicode.IsControl(r) {
			return fmt.Errorf("name must not contain control characters")
		}
	}

	return nil
}

// validateGrantableTopics refuses an authorised-topic list containing anything a
// subscriber may not be granted.
//
// The list becomes the ACL bindings, so whatever is accepted here is what the issued
// credential can read. Membership is EXACT against model.SubscriberGrantableTopics —
// not a prefix test, not a normalising test — and that is what makes three distinct
// attacks plain non-members rather than special cases somebody has to remember to
// write:
//
//   - "*", which Kafka reads as matching every resource, so a single such entry
//     converts a per-topic grant into a cluster-wide one.
//   - A FOREIGN topic such as "attacker.transactions", which has exactly the shape of
//     an owned name and none of the meaning: granting it is a grant into somebody
//     else's data on a broker Blnk may share.
//   - A DEAD-LETTER topic. Every DLT carries other subscribers' failed events together
//     with Blnk's own failure metadata, so it has no subscriber audience.
//   - THE INTERNAL CATEGORY TOPIC, "<prefix>.system", UNLESS THE DEPLOYMENT HAS
//     ACKNOWLEDGED IT. It carries system.error's frozen verbatim-error body and is the
//     catalogue's catch-all, so it is an operator surface: the default grantable set is
//     the three TENANT category topics. It becomes grantable when
//     KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS is declared and the caller passes
//     WithInternalTopicAccess(true), and the refusal names that variable when it is
//     not, because "not grantable" and "not grantable HERE" are different operator
//     actions. model.SubscriberPrivilegedEventCategories owns that decision.
//
// An EMPTY list is accepted, because a subscriber authorised for nothing is the
// fail-closed default of a fresh registration. An empty or whitespace-only ENTRY is
// refused rather than skipped: it is always a bug in whatever assembled the list, and
// silently dropping it would let a caller believe it had requested a grant it did not
// receive.
//
// Parameters:
//   - topics []string: the requested authorised topics.
//   - topicPrefix string: the configured KAFKA_TOPIC_PREFIX.
//   - options ...SubscriberGrantOption: the deployment's acknowledgement state. Omitted
//     means "nothing acknowledged", which is the fail-closed answer and the shipped
//     default.
//
// Returns:
//   - error: naming the first offending topic and listing what is grantable.
func validateGrantableTopics(topics []string, topicPrefix string, options ...SubscriberGrantOption) error {
	if len(topics) == 0 {
		return nil
	}

	policy := resolveSubscriberGrantPolicy(options)
	grantable := model.SubscriberAuthorizableTopics(topicPrefix, policy.internalTopicAccess)

	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" {
			return fmt.Errorf("authorized_topics contains an empty topic name")
		}

		if model.IsSubscriberAuthorizableTopicName(topic, topicPrefix, policy.internalTopicAccess) {
			continue
		}

		// A PRIVILEGED NAME REFUSED FOR WANT OF THE ACKNOWLEDGEMENT GETS ITS OWN MESSAGE. The
		// generic refusal below would send the operator to read the allowlist, which does not
		// contain the answer: the name IS a Blnk category topic and the missing piece is a
		// deployment declaration. Naming the variable is what makes the refusal actionable,
		// and it discloses nothing — the variable is documented in .env.example, both Compose
		// files and the Kubernetes configuration.
		if model.IsSubscriberPrivilegedTopicName(topic, topicPrefix) {
			return fmt.Errorf(
				"authorized_topics entry %q is the internal category topic: it carries Blnk's own "+
					"error text verbatim and every event type the catalogue does not yet recognise, "+
					"so it is grantable only where the deployment has acknowledged that by setting "+
					"KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS. Set it to grant this topic, or grant "+
					"only the tenant category topics (%s)",
				topic, strings.Join(model.SubscriberGrantableTopics(topicPrefix), ", "),
			)
		}

		return fmt.Errorf(
			"authorized_topics entry %q is not grantable; a subscriber may be granted only "+
				"Blnk-owned category topics (%s). A dead-letter topic carries Blnk's own "+
				"failure metadata and every other subscriber's failed events, so it has no "+
				"subscriber audience",
			topic, strings.Join(grantable, ", "),
		)
	}

	return nil
}

// subscriberGrantPolicy carries the DEPLOYMENT facts that decide how wide the grant
// allowlist is for one validation.
//
// It is a struct behind an option function rather than a positional parameter for one
// reason: every existing caller of Validate and validateGrantableTopics — two handlers
// and forty-odd tests — means "nothing acknowledged", and that is also the fail-closed
// answer. A required parameter would have made all of them state it explicitly, and a
// bare boolean at a call site says nothing about which way round it goes.
type subscriberGrantPolicy struct {
	// internalTopicAccess is KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS as resolved by the
	// caller. It widens the allowlist by exactly one name and nothing else.
	internalTopicAccess bool
}

// SubscriberGrantOption declares one deployment fact for a single grant validation.
type SubscriberGrantOption func(*subscriberGrantPolicy)

// WithInternalTopicAccess declares whether this deployment has acknowledged that a
// subscriber may hold the internal category topic.
//
// The handler resolves it from configuration once per request, so the DTO stays free of
// configuration while validating against the deployment it is actually running in.
//
// Parameters:
//   - declared bool: the value of KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS.
//
// Returns:
//   - SubscriberGrantOption: applied by Validate and validateGrantableTopics.
func WithInternalTopicAccess(declared bool) SubscriberGrantOption {
	return func(policy *subscriberGrantPolicy) {
		policy.internalTopicAccess = declared
	}
}

// resolveSubscriberGrantPolicy folds the options into one policy value, so the zero value —
// nothing acknowledged — is what an absent option resolves to.
func resolveSubscriberGrantPolicy(options []SubscriberGrantOption) subscriberGrantPolicy {
	var policy subscriberGrantPolicy
	for _, option := range options {
		if option != nil {
			option(&policy)
		}
	}

	return policy
}

// validateSubscriberKeyScope constrains the recorded key-scoped authorization.
//
// The value grants nothing at the broker — it decides whether a credential can be
// issued at all — so the rules here are about it being STORABLE AND DISPLAYABLE rather
// than about isolation. Two things are refused, and both are about what the string does
// after it is stored:
//
//   - CONTROL CHARACTERS. The value is echoed into API responses, log lines and trace
//     attributes.
//   - Surrounding WHITESPACE, because a scope with a trailing space describes a
//     different set of keys from the one the caller meant, and the difference is
//     invisible in every rendering of the value.
//
// IT VALIDATES SHAPE, NOT MEANING. A well-formed prefix is accepted and recorded; what
// this refuses is a value that cannot be held honestly — surrounding whitespace, which
// would make two prefixes filtering different record sets look alike, an unbounded
// value, which is amplified by every registry read and every log line naming the
// subscriber, and a control character, which corrupts every log line and dashboard
// label it reaches. The service applies the identical rules so a CLI caller, a
// migration and a fixture are held to the same standard.
//
// Parameters:
//   - prefix string: the recorded constraint, empty when none is recorded.
//
// Returns:
//   - error: describing the violation, nil when the value is storable.
func validateSubscriberKeyScope(prefix string) error {
	if prefix == "" {
		return nil
	}

	if prefix != strings.TrimSpace(prefix) {
		return fmt.Errorf("partition_key_prefix must not have surrounding whitespace")
	}

	if len(prefix) > maxSubscriberKeyScopeLen {
		return fmt.Errorf("partition_key_prefix must be at most %d characters, got %d",
			maxSubscriberKeyScopeLen, len(prefix))
	}

	for _, character := range prefix {
		if character < 0x20 || character == 0x7f {
			return fmt.Errorf("partition_key_prefix must not contain control characters")
		}
	}

	return nil
}

// maxSubscriberKeyScopeLen bounds the recorded key-scoped authorization. A key prefix is a fragment
// of a ledger ID — a '<prefix>_<uuid>' string — so 256 characters is far more than any
// legitimate value needs and still refuses an unbounded one.
const maxSubscriberKeyScopeLen = 256

// maxSubscriberNameLen bounds the human label, in runes.
//
// 256 is the same allowance the recorded key scope gets, and it is chosen for the same
// reason: far more than any legitimate value needs, while refusing one that is
// unbounded. A label is what an operator recognises a principal by in a list — if it
// does not fit in a line, it is not doing that job.
const maxSubscriberNameLen = MaxSubscriberNameLength

// validateLegacyWebhookURL applies the destination policy to the dual-run webhook URL.
//
// Nothing in Blnk sends to this URL today. It is recorded so a subscriber already
// receiving HTTP pushes has somewhere to be migrated FROM.
//
// Two rules, each closing a distinct route:
//
//   - HTTPS ONLY. A subscriber's event stream carries ledger data — identity events
//     include names, addresses and dates of birth — and http:// would put it on the
//     wire in clear text. It also refuses the non-HTTP schemes that turn a URL field
//     into a local-resource read: file://, gopher://, ftp:// and friends.
//   - NO INTERNAL DESTINATION. Loopback, link-local (including the 169.254.169.254
//     cloud metadata endpoint), private ranges, the unspecified address, multicast, and
//     the hostnames that resolve to them.
//
// This is a literal-address check, and it is deliberately not sold as more than that: a
// hostname resolving to an internal address at send time is not detectable here, and
// defeating that needs resolution-time validation in the sender. The repository layer
// applies the same policy, so a URL arriving by another path is refused too.
//
// Parameters:
//   - rawURL string: the URL, empty when none is recorded or when clearing.
//
// Returns:
//   - error: describing the violation without echoing more of the URL than the host.
func validateLegacyWebhookURL(rawURL string) error {
	// THE ONE POLICY, in model.ValidateWebhookURL. This function carries no copy of the
	// https, host, whitespace and internal-destination rules and no destination classifier
	// of its own: a second copy beside the repository's would give one column two rules,
	// with the same rejected host producing two different reason phrases depending on which
	// door the request came through. What stays here is the ERROR SHAPE: a DTO
	// validation error rather than a typed apierror, and prefixed with the field name
	// because that is what a caller of this endpoint needs in order to know which body key
	// to correct.
	//
	// An empty value passes, meaning "no endpoint recorded", which is what the nullable
	// column is for.
	message, reason := model.ValidateWebhookURL(rawURL)
	if message == "" {
		return nil
	}

	// The MESSAGE and the REASON together, because the reason names the offending host or scheme —
	// the caller's own value, and the one thing that tells them what to change. Neither echoes the
	// whole URL or the parser's rendering of it.
	return fmt.Errorf("webhook_url: %s (%s)", strings.ToLower(message[:1])+message[1:], reason)
}

// SubscriberResponse is the read shape for GET /subscribers, GET
// /subscribers/:subscriber_id, and the bodies returned by the create and update routes.
//
// The full reference was once returned here. The reference is not a secret — it is a
// non-reversible derivation and nobody can authenticate with it — but it is not
// information a client needs either, and returning it had two costs worth avoiding.
type SubscriberResponse struct {
	// SubscriberID is the business key callers address the subscriber by.
	SubscriberID string `json:"subscriber_id"`

	// SubscriberIDHash is the pseudonym this subscriber appears under in METRICS AND LOGS,
	// published here so that a token read off a dashboard, an alert notification or a log
	// line can be resolved back to the subscriber.
	SubscriberIDHash string `json:"subscriber_id_hash"`

	// Name is the human label the subscriber is triaged by.
	Name string `json:"name"`

	// KafkaPrincipal is the SASL/SCRAM username the subscriber's ACLs are
	// granted to.
	KafkaPrincipal string `json:"kafka_principal"`

	// ConsumerGroupID is the consumer group the subscriber reads under.
	ConsumerGroupID string `json:"consumer_group_id"`

	// AuthorizedTopics is the set of topics the subscriber may Read and
	// Describe. Reported even when empty, because an empty grant is a
	// meaningful, fail-closed state an operator needs to see.
	AuthorizedTopics []string `json:"authorized_topics"`

	// PartitionKeyPrefix is the key-scoped authorization recorded for this subscriber: it
	// is entitled only to records whose message key carries this prefix. Omitted when none
	// is recorded.
	//
	// WHETHER IT CAN BE ISSUED A CREDENTIAL DEPENDS ON THE DEPLOYMENT, not on this value.
	// Kafka's authorizer has no message-key dimension, so the boundary is kept by the
	// key-authorising component a deployment declares in KAFKA_KEY_SCOPE_ENFORCEMENT: with
	// one declared the credential is issued (granted Describe and no topic Read, records
	// delivered through that component), and with none declared issuance refuses rather
	// than hand out access the registry does not describe.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// EnforcedAccess declares which parts of this subscriber's access model are actually
	// enforced, and where. It is always present, including when no credential has been
	// issued, because it describes the boundary issuance WILL establish and an integrator
	// needs it before wiring a consumer.
	EnforcedAccess SubscriberEnforcedAccess `json:"enforced_access"`

	// CredentialIssuanceBlocked reports that POST /subscribers/{id}/kafka-credentials will
	// REFUSE for this row as it currently stands. Always present, including when false,
	// because a client that had to infer it from the absence of a field would infer it
	// wrong.
	CredentialIssuanceBlocked bool `json:"credential_issuance_blocked"`

	// CredentialIssuanceBlockedReason names what to change, in the imperative, and is
	// present only when CredentialIssuanceBlocked is true.
	//
	// It carries the remedy rather than only the diagnosis, because the two remedies are
	// not interchangeable and an operator cannot pick between them from the diagnosis
	// alone: clearing the prefix ACCEPTS whole-topic access, which is a decision somebody
	// has now taken explicitly, while narrowing authorized_topics is the enforceable form
	// of the same intent whenever the intended boundary maps onto topics.
	CredentialIssuanceBlockedReason string `json:"credential_issuance_blocked_reason,omitempty"`

	// CredentialFingerprint is a short, non-sensitive digest fragment identifying which
	// credential issuance this row records. It answers "is this the same credential I saw
	// last time?" and nothing else — it cannot be authenticated with, and it is not the
	// stored reference. Empty, and so omitted, when no credential has been issued.
	CredentialFingerprint string `json:"credential_fingerprint,omitempty"`

	// CredentialIssuedAt is when the current credential was issued. Nil, and so
	// omitted, when none ever has been.
	CredentialIssuedAt *time.Time `json:"credential_issued_at,omitempty"`

	// NO webhook_url FIELD. The recorded URL is read through GET
	// /subscribers/:subscriber_id/webhook-subscription, which is fronted by the sunset
	// guard and stops disclosing it at the retirement instant. Echoing it here as well
	// would keep it readable through an unguarded route after that, so the two reads would
	// disagree about whether the legacy surface still exists.

	// MigratedAt is when the subscriber completed its move to Kafka
	// consumption. Nil, and so omitted, means not yet migrated, which is what
	// migration-progress reporting counts.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`

	// ===================================================================
	// the three unsettled-state markers, projected
	//
	// A marker an operator cannot see through the registry is a marker they cannot triage,
	// whatever a runbook says.
	// ===================================================================

	// RevocationPendingAt is when a deregistration began taking this subscriber's
	// broker-side access away. Present means THE ROW IS NOT AN ACTIVE SUBSCRIBER —
	// credential issuance is refused for it — and a principal that may still authenticate
	// is awaiting revocation.
	//
	// The remedy is to RETRY THE DEREGISTRATION (DELETE this subscriber), which is
	// idempotent at the broker. Re-issuing is refused while this is set, so it is not an
	// alternative here.
	RevocationPendingAt *time.Time `json:"revocation_pending_at,omitempty"`

	// RevocationFailedAt is when the MOST RECENT revocation attempt was refused by the
	// broker, as distinct from a deregistration that merely began. It is cleared at the
	// start of every new attempt, so a present value means no attempt has been made since
	// the refusal.
	RevocationFailedAt *time.Time `json:"revocation_failed_at,omitempty"`

	// CredentialOrphanedAt is when an issuance left a credential at the broker that Blnk
	// could neither record nor revoke. Present means a principal can authenticate while
	// CredentialFingerprint above does not describe the credential that works.
	CredentialOrphanedAt *time.Time `json:"credential_orphaned_at,omitempty"`

	// CreatedAt is when the subscriber was registered.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the registry row was last written.
	UpdatedAt time.Time `json:"updated_at"`

	// RevocationPending reports that broker-side revocation is still owed for this
	// subscriber: the row is tombstoned for deregistration and its principal may still be
	// able to authenticate until the revocation completes. Always present, including when
	// false, because a client that had to infer it from a missing field would infer it
	// wrong.
	RevocationPending bool `json:"revocation_pending"`

	// RevocationPendingReason names what is outstanding and what to do about it. Present
	// only when RevocationPending is true.
	RevocationPendingReason string `json:"revocation_pending_reason,omitempty"`
}

// Enforcement dimensions reported by SubscriberEnforcedAccess.EnforcedBy. They name the
// dimensions enforced on every request a subscriber makes — two of them at the broker's own
// authorizer, and one at the key-authorising component the deployment DECLARED in front of the
// brokers (KAFKA_KEY_SCOPE_ENFORCEMENT). Blnk ships no such component and serves no records
// itself, so the third dimension is listed only on a credential that exists, and such a
// credential is issued only where a component is declared.
const (
	// EnforcementDimensionTopic is Read and Describe bound to an EXACT topic name, so a
	// topic absent from the grant is refused at the broker.
	EnforcementDimensionTopic = "topic"

	// EnforcementDimensionConsumerGroup is Read bound to the subscriber's consumer-group
	// namespace as a PREFIXED pattern, which reserves that namespace to the subscriber and
	// refuses every group outside it.
	EnforcementDimensionConsumerGroup = "consumer_group"
)

// SubscriberEnforcedAccess declares, inside the response body, WHERE each part of a
// subscriber's recorded access model is enforced.
//
// A subscriber's record carries three access-shaped values: authorized_topics, a
// consumer group, and partition_key_prefix. Two of them are ACL bindings the broker
// evaluates.
//
// An integrator reading a response that lists all three together has no way to tell
// which is which, and for a long time the plausible wrong conclusion was also the true
// one in the worst sense: two subscribers sharing a topic with different key prefixes
// really COULD see each other's events, because the platform granted both whole-topic
// Read and asked them to filter. This object exists so nobody has to guess, and — since
// the correction described below — so that the answer it gives is an enforced boundary
// rather than a request for cooperation.
//
// A security review of the running system named the consequence plainly — whole-topic
// credentials expose records belonging to other ledgers and other subscribers, and
// disclosure plus client cooperation is not an authorization boundary. A subscriber
// that ignored the obligation, or simply used a different consumer, read everything on
// the shared category topic and nothing in the platform could tell.
//
// So the boundary moved rather than the wording. A subscriber that records a
// partition-key prefix is now provisioned WITHOUT Read on any topic: the broker refuses
// every record fetch it attempts, with any client, from any host. Its records are
// delivered by the KEY-AUTHORISING COMPONENT THE DEPLOYMENT DECLARED in front of the
// brokers, which authenticates the credential Blnk issued and applies the recorded
// prefix to every record's key before returning it.
//
// BLNK DOES NOT SHIP THAT COMPONENT and exposes no data-plane route of its own — there
// is no GET under /subscribers that returns records. Where no component is declared,
// which is the shipped default, no CREDENTIAL is produced for a key-scoped subscriber:
// issuance refuses with SUBSCRIBER_KEY_SCOPE_UNENFORCED (409) rather than emitting a
// credential body that claims an enforcement point nothing is running.
type SubscriberEnforcedAccess struct {
	// EnforcedBy names every dimension Blnk enforces for this subscriber, and is
	// exhaustive.
	EnforcedBy []string `json:"enforced_by"`

	// NotEnforcedBy names every access-shaped dimension this API accepts and NOTHING
	// enforces.
	NotEnforcedBy []string `json:"not_enforced_by"`

	// Topics is the exact set of topics the principal may Read and Describe. Anything
	// outside it is refused at the broker, not filtered by the client.
	Topics []string `json:"topics"`

	// ConsumerGroupNamespace is the group-id prefix reserved to this subscriber. Any group
	// beginning with it is usable; any group outside it is refused. Empty only when the
	// subscriber's consumer group is not derivable, which registration prevents.
	ConsumerGroupNamespace string `json:"consumer_group_namespace,omitempty"`

	// PartitionKeyScopeState is HOW FAR this subscriber's key scope has actually got, and
	// it is the field to read when the booleans below are not enough.
	PartitionKeyScopeState model.SubscriberKeyScopeState `json:"partition_key_scope_state"`

	// PartitionKeyPrefixEnforced answers the one question whose wrong answer is a
	// data-disclosure bug, outright rather than by inference from EnforcedBy.
	//
	// ON A CREDENTIAL RESPONSE A TRUE HERE IS BACKED BY AN ATTESTATION rather than by
	// configuration, and PartitionKeyScopeState says so by reporting "attested". A mode
	// and a distinct bootstrap list are assertions a deployment makes about itself; the
	// attestation is an authenticated request the declared component answered, naming this
	// principal and this prefix byte-for-byte, before any secret was generated. A
	// component that could not be reached or disagreed produces no credential at all —
	// SUBSCRIBER_KEY_SCOPE_UNATTESTED.
	PartitionKeyPrefixEnforced bool `json:"partition_key_prefix_enforced"`

	// PartitionKeyPrefix echoes the routing hint recorded on the subscriber, or is empty
	// when none is recorded.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// GatewayDeliveryRequired is TRUE exactly when PartitionKeyPrefixEnforced is, and it
	// is the actionable instruction the rest of this object only implies: CONSUME THROUGH
	// THE KEY-AUTHORISING COMPONENT THE DEPLOYMENT DECLARED, whose address BrokerEndpoint
	// carries, not directly from the Kafka brokers.
	GatewayDeliveryRequired bool `json:"gateway_delivery_required"`

	// BrokerRecordAccess reports whether this subscriber's credential may fetch records
	// DIRECTLY from the broker. It is TRUE exactly when no key scope is recorded.
	//
	// It is stated separately because it is the fact that explains the other: a key-scoped
	// subscriber is not being ASKED to use the declared component as a courtesy, it is
	// granted Describe on its topics and Read on its consumer-group namespace and no topic
	// Read at all, so every fetch it attempts at the broker is refused by the authorizer.
	// An integrator debugging a TOPIC_AUTHORIZATION_FAILED on a granted topic reads this
	// field and knows immediately that the refusal is the design.
	BrokerRecordAccess bool `json:"broker_record_access"`

	// PartitionKeyPrefixEnforcedBy names WHICH COMPONENT enforces the key scope, and it is
	// the same fact the booleans above state, said as a place rather than as yes/no
	// answers: "broker_gateway" when a recorded prefix has an enforcement point in this
	// deployment, "none" when no prefix is recorded OR when nothing is declared to keep
	// the one that is. The value is the SAME WORD the deployment declares in
	// KAFKA_KEY_SCOPE_ENFORCEMENT, so a response and the configuration that made it
	// issuable cannot name two different components.
	PartitionKeyPrefixEnforcedBy model.KeyScopeEnforcementStatus `json:"partition_key_prefix_enforced_by"`

	// ExclusiveGrantVerified reports that Blnk READ the principal's complete ACL grant at
	// the broker and found no ALLOW binding outside the set described above.
	//
	// Issuance now REFUSES while any such binding exists, so on a successful response this
	// is necessarily true. It is stated anyway, for the same reason
	// PartitionKeyPrefixEnforced is: a client hard-coding an assumption about the boundary
	// should be reading it out of the response, and a future posture in which a broader
	// grant is tolerated must be visible here rather than silently changing what the other
	// fields mean.
	ExclusiveGrantVerified bool `json:"exclusive_grant_verified"`

	// Guidance is the REMEDY, carried in the same object as the boundary it describes, and
	// it is always SubscriberKeyScopeGuidance.
	Guidance string `json:"guidance"`
}

// EnforcementDimensionPartitionKey names the access-shaped dimension the BROKER does
// not evaluate: a subscriber's partition-key prefix, enforced by the key-authorising
// component the deployment declared in front of the brokers.
//
// It was the sole member of NotEnforcedBy before the isolation correction, which is the
// one change worth noting about it: the name did not move, the list it appears in did.
const EnforcementDimensionPartitionKey = "partition_key"

// SubscriberKeyScopeGuidance is the routing instruction every subscriber and credential
// response carries, in SubscriberEnforcedAccess.Guidance.
//
// Stated once, so the registry view and the credential view cannot give different
// advice about the same boundary.
const SubscriberKeyScopeGuidance = "Kafka authorises whole topics and consumer groups and has " +
	"no message-key dimension, so a subscriber's partition-key prefix is enforced outside the " +
	"broker: a subscriber that records one is granted Describe but NOT Read on its topics, so " +
	"the broker refuses every direct fetch, and its records are delivered by the key-authorising " +
	"component this deployment declared — dial the broker_endpoint in this response, not the " +
	"Kafka brokers. Blnk does not ship that component and serves no records itself, so where " +
	"none is declared a key-scoped subscriber is refused a credential outright rather than " +
	"issued one that can fetch nothing. A subscriber with no prefix consumes directly from the " +
	"broker and is confined by its authorized_topics alone, which means a granted topic is " +
	"readable in full — including records written for other ledgers and other subscribers on " +
	"that topic."

// NewSubscriberEnforcedAccess builds the enforced-access declaration for a subscriber
// whose DEPLOYMENT STATE THE CALLER DOES NOT HOLD.
//
// It is the fail-closed form, and it answers as if no key-scope enforcement point were
// declared: a recorded prefix reports state "requested", partition_key in
// NotEnforcedBy, and "none" as the enforcement point.
//
// Parameters:
//   - subscriberID string: the business key the principal and group are derived from.
//   - topics []string: the subscriber's exact topic grant.
//   - partitionKeyPrefix string: the recorded prefix, trimmed and echoed verbatim.
//
// Returns:
//   - SubscriberEnforcedAccess: the declaration, with no key-scope enforcement claimed.
func NewSubscriberEnforcedAccess(
	subscriberID string,
	topics []string,
	partitionKeyPrefix string,
) SubscriberEnforcedAccess {
	return NewSubscriberEnforcedAccessUnder(
		subscriberID, topics, partitionKeyPrefix, model.KeyScopeEnforcementNone,
	)
}

// NewSubscriberEnforcedAccessUnder builds the declaration for a KNOWN enforcement
// point, and it is the form every truthful projection uses.
//
// The parameter is now the fact the fields derive from, which is the whole of the
// correction.
//
// Parameters:
//   - subscriberID string: used to derive the consumer-group namespace.
//   - topics []string: the exact topics the principal may Read and Describe.
//   - partitionKeyPrefix string: the recorded prefix, trimmed here.
//   - enforcement model.KeyScopeEnforcementStatus: where this DEPLOYMENT can enforce a
//     key scope. Only broker_gateway can make PartitionKeyPrefixEnforced true, and only
//     for a row that records a prefix.
//
// Returns:
//   - SubscriberEnforcedAccess: the declaration, internally consistent by construction.
//     With a prefix recorded AND enforcement declared: partition_key joins EnforcedBy,
//     PartitionKeyPrefixEnforced and GatewayDeliveryRequired are true,
//     BrokerRecordAccess is false, the point is broker_gateway and the state is
//     available.
func NewSubscriberEnforcedAccessUnder(
	subscriberID string,
	topics []string,
	partitionKeyPrefix string,
	enforcement model.KeyScopeEnforcementStatus,
) SubscriberEnforcedAccess {
	partitionKeyPrefix = strings.TrimSpace(partitionKeyPrefix)

	// THE ROW'S HALF of the derivation. Trimmed, because a whitespace-only column is not a scope
	// — and because model.EventSubscriber's own predicates trim, so a row that reached here with
	// one must not be reported as narrowed when nothing narrows it.
	keyScoped := partitionKeyPrefix != ""

	// THE DEPLOYMENT'S HALF, and the conjunction is what every enforcement field below is
	// derived from. Two facts, not one: an intent recorded on the row, and a component
	// declared in the environment able to keep it. Neither alone is an enforced boundary,
	// and treating the first as though it were the pair is exactly what made this
	// projection untruthful.
	keyScopeEnforced := keyScoped && enforcement == model.KeyScopeEnforcementGateway

	// THE STATE, resolved by the one derivation in model so a caller reading the scale and a
	// caller reading the booleans cannot be told different things. Issuance upgrades it to
	// attested afterwards; nothing here can, because nothing here made the round trip.
	state := model.SubscriberAccessDeployment{KeyScopeEnforcement: enforcement}.
		KeyScopeStateFor(partitionKeyPrefix)

	enforced := SubscriberEnforcedAccess{
		EnforcedBy: []string{
			EnforcementDimensionTopic,
			EnforcementDimensionConsumerGroup,
		},
		// An explicitly allocated empty slice rather than nil, so the body carries [] instead
		// of null: "nothing is unenforced" is a claim the response makes, and null would read
		// as "not stated". Populated below for the one state in which a dimension really is
		// kept by nobody.
		NotEnforcedBy:          []string{},
		Topics:                 topics,
		PartitionKeyScopeState: state,
		// TRUE only for a recorded prefix WITH a declared enforcement point. The component named
		// below applies it to every record's key before returning one; where none is declared
		// there is nothing to name and nothing to claim.
		PartitionKeyPrefixEnforced: keyScopeEnforced,
		// THE RECORDED VALUE, trimmed, and empty when there is none. It is reported whatever
		// the deployment state, because an operator inspecting a blocked row needs to see the
		// prefix that is blocking it. A sentinel was tried here and removed: this string is
		// the prefix record keys are matched against, so any stand-in for "no restriction"
		// describes a filter that matches nothing.
		PartitionKeyPrefix: partitionKeyPrefix,
		// The transport instruction, and it points somewhere only when there IS somewhere to
		// point. A key-scoped row with nothing declared has neither this nor broker record
		// access, which is the "no usable path yet" state CredentialIssuanceBlocked explains.
		GatewayDeliveryRequired: keyScopeEnforced,
		BrokerRecordAccess:      !keyScoped,
		// The place-shaped form of the same conjunction, so a caller reading the enforcement
		// point and a caller reading the booleans are told the same thing. Derived rather
		// than echoed from the parameter: the parameter is a deployment-wide fact, and a row
		// with no prefix has no key scope for that component to enforce.
		PartitionKeyPrefixEnforcedBy: model.KeyScopeEnforcementNone,
		// THE ROUTING INSTRUCTION, in the same object as the boundary, and the same sentence
		// for every caller of this constructor. Assigned unconditionally: it describes what
		// Kafka's authorizer can evaluate and where Blnk puts the boundary it cannot, neither
		// of which is a property of this row. Its text already covers the undeclared case,
		// which is why there is one sentence rather than one per state.
		Guidance: SubscriberKeyScopeGuidance,
		// FALSE by default, and the default is the honest answer for every caller of this
		// constructor except issuance. This builds the REQUESTED boundary from a registry
		// row, which involves no broker round trip, so nothing here observed what the broker
		// actually grants. Only NewVerifiedSubscriberEnforcedAccess sets it, and only because
		// issuance really did read the grant and refuse a broader one.
		ExclusiveGrantVerified: false,
	}

	// THE TWO LISTS, and which one the key dimension lands in is the response's answer to "is
	// this boundary kept?". Appended here rather than listed above so both are visibly derived
	// from one condition and the dimension cannot appear in both.
	if keyScopeEnforced {
		enforced.PartitionKeyPrefixEnforcedBy = model.KeyScopeEnforcementGateway
		enforced.EnforcedBy = append(enforced.EnforcedBy, EnforcementDimensionPartitionKey)
	} else if keyScoped {
		enforced.NotEnforcedBy = append(enforced.NotEnforcedBy, EnforcementDimensionPartitionKey)
	}

	if namespace, err := model.CanonicalConsumerGroupNamespace(subscriberID); err == nil {
		enforced.ConsumerGroupNamespace = namespace
	}

	if enforced.Topics == nil {
		enforced.Topics = []string{}
	}

	return enforced
}

// NewVerifiedSubscriberEnforcedAccess builds the enforced-access declaration for a
// boundary Blnk has just READ AT THE BROKER and found exclusive.
//
// It is the credential-issuance form, and it exists as a separate constructor rather
// than as a boolean parameter so that the claim cannot be made by accident.
// Provisioning describes the principal's complete ACL grant and REFUSES to return a
// password while any ALLOW binding sits outside the declared set, so a response
// assembled here is one whose boundary was verified and not merely requested. Every
// other caller — every read of a registry row — must use NewSubscriberEnforcedAccess or
// its Under form, which report the claim as unverified because no broker round trip
// happened.
//
// Parameters:
//   - subscriberID string: the business key the principal and group are derived from.
//   - topics []string: the subscriber's exact topic grant.
//   - partitionKeyPrefix string: the routing hint the credential was issued under.
//     Empty when none is recorded.
//
// Returns:
//   - SubscriberEnforcedAccess: the declaration, with ExclusiveGrantVerified true.
func NewVerifiedSubscriberEnforcedAccess(
	subscriberID string,
	topics []string,
	partitionKeyPrefix string,
) SubscriberEnforcedAccess {
	return NewVerifiedSubscriberEnforcedAccessUnder(
		subscriberID, topics, partitionKeyPrefix, model.KeyScopeEnforcementNone,
	)
}

// NewVerifiedSubscriberEnforcedAccessUnder is the issuance form: a verified grant AND a
// known enforcement point.
//
// Issuance is the only caller. It read the broker's complete ACL grant for this
// principal and refused a broader one, and it read the deployment's key-scope
// enforcement mode to decide whether to mint at all — so it is the one caller able to
// answer both questions truthfully in one object.
//
// Parameters:
//   - subscriberID string: used to derive the consumer-group namespace.
//   - topics []string: the exact topics the principal may Read and Describe.
//   - partitionKeyPrefix string: the recorded prefix.
//   - enforcement model.KeyScopeEnforcementStatus: where the scope is enforced.
//
// Returns:
//   - SubscriberEnforcedAccess: with ExclusiveGrantVerified set, and the key-scope
//     state attested whenever a prefix was issued under a declared enforcement point.
func NewVerifiedSubscriberEnforcedAccessUnder(
	subscriberID string,
	topics []string,
	partitionKeyPrefix string,
	enforcement model.KeyScopeEnforcementStatus,
) SubscriberEnforcedAccess {
	enforced := NewSubscriberEnforcedAccessUnder(subscriberID, topics, partitionKeyPrefix, enforcement)
	enforced.ExclusiveGrantVerified = true

	// THE UPGRADE, and it is conditioned on the state the shared constructor resolved
	// rather than on the parameters again. Available is precisely "a prefix is recorded
	// and this deployment declares a component that can keep it", which is the state
	// issuance had to be in to have asked for an attestation at all — so reading it back
	// is what keeps this from being a second, drifting derivation of the same conjunction.
	if enforced.PartitionKeyScopeState == model.SubscriberKeyScopeStateAvailable {
		enforced.PartitionKeyScopeState = model.SubscriberKeyScopeStateAttested
	}

	return enforced
}

// NewSubscriberResponse projects a stored subscriber into its response shape.
//
// Every handler that returns a subscriber goes through here, and that is the point: the
// credential reference is reduced to a fingerprint in exactly one place, so no handler
// can return the raw reference by writing the obvious assignment. A projection that
// must be constructed is a projection that cannot be assembled wrongly by omission.
//
// The zero deployment value is the fail-closed reading and safe to pass when nothing is
// known; blnk.SubscriberAccessDeployment resolves the real one.
//
// Parameters:
//   - subscriber model.EventSubscriber: the stored registry row.
//   - deployment model.SubscriberAccessDeployment: the resolved configuration this
//     subscriber lives in — where a key scope can be enforced, whether
//     subscriber-facing brokers are advertised, and whether whole-topic access has been
//     declared.
//
// Returns:
//   - SubscriberResponse: the response body, carrying no credential material.
func NewSubscriberResponse(
	subscriber model.EventSubscriber,
	deployment model.SubscriberAccessDeployment,
) SubscriberResponse {
	response := SubscriberResponse{
		SubscriberID: subscriber.SubscriberID,
		// The pivot back from a metric label or a log line. Projected here rather than by the
		// handler for the same reason the credential fingerprint is: a field that must be
		// constructed cannot be omitted by a handler that forgot it, and a subscriber
		// response missing it is a token nothing can resolve.
		SubscriberIDHash:   model.HashIdentifier(subscriber.SubscriberID),
		Name:               subscriber.Name,
		KafkaPrincipal:     subscriber.KafkaPrincipal,
		ConsumerGroupID:    subscriber.ConsumerGroupID,
		AuthorizedTopics:   subscriber.AuthorizedTopics,
		CredentialIssuedAt: subscriber.CredentialIssuedAt,
		MigratedAt:         subscriber.MigratedAt,
		// the unsettled-state markers travel with the row. Copied
		// rather than derived, because each is a durable fact the registry
		// recorded and the response must not soften or summarise it.
		RevocationPendingAt:  subscriber.RevocationPendingAt,
		RevocationFailedAt:   subscriber.RevocationFailedAt,
		CredentialOrphanedAt: subscriber.CredentialOrphanedAt,
		CreatedAt:            subscriber.CreatedAt,
		UpdatedAt:            subscriber.UpdatedAt,
	}

	// AuthorizedTopics is reported even when empty — an empty grant is a meaningful,
	// fail-closed state an operator needs to see — so a nil slice becomes [] rather
	// than null, which a client would otherwise have to special-case.
	if response.AuthorizedTopics == nil {
		response.AuthorizedTopics = []string{}
	}

	// Assembled here rather than by the handler, so every subscriber response states the
	// enforced boundary and none can imply that the advisory key prefix is one.
	if subscriber.PartitionKeyPrefix != nil {
		response.PartitionKeyPrefix = *subscriber.PartitionKeyPrefix
	}

	// AFTER the top-level prefix is resolved, and built FROM it, so the two cannot describe
	// different values. The declaration restates the prefix inside the object that says the
	// broker does not enforce it, which is the only place a reader cannot take it for a boundary.
	response.EnforcedAccess = NewSubscriberEnforcedAccessUnder(
		subscriber.SubscriberID, response.AuthorizedTopics, response.PartitionKeyPrefix,
		deployment.KeyScopeEnforcement,
	)

	// CredentialFingerprint, never the reference. model.CredentialFingerprint yields
	// "" for anything that is not a validly derived reference, so a value mis-stored
	// by some other path is dropped entirely instead of being echoed.
	if subscriber.CredentialReference != nil {
		response.CredentialFingerprint = model.CredentialFingerprint(*subscriber.CredentialReference)
	}

	response.CredentialIssuanceBlocked, response.CredentialIssuanceBlockedReason =
		credentialIssuanceBlock(subscriber, deployment)

	// Assigned as a PAIR, from one reading of the tombstone, so the flag and the reason
	// cannot disagree: a reader that only needs "is this row on its way out" does not have
	// to know that an absent instant means no, and a reader acting on the reason is never
	// handed one for a row that is settled.
	response.RevocationPending, response.RevocationPendingReason =
		outstandingRevocation(subscriber)

	return response
}

// outstandingRevocation reports whether broker-side revocation is still owed for this
// subscriber, and names the remedy when it is.
//
// The tombstone this reads is the SAME column CountSubscriberRevocationsPending counts
// and SubscriberRevocationOutstanding fires on, so the gauge, the alert and this
// response cannot disagree about which rows are outstanding. What the column alone
// cannot say is which of two opposite situations produced it.
//
// Parameters:
//   - subscriber model.EventSubscriber: the row being projected.
//
// Returns:
//   - bool: true when broker-side revocation is still owed.
//   - string: the remedy in the imperative, empty when nothing is owed.
func outstandingRevocation(subscriber model.EventSubscriber) (bool, string) {
	if !subscriber.IsRevocationPending() {
		return false, ""
	}

	return true, "A broker-side credential revocation is outstanding: this subscriber's SASL " +
		"credential may still authenticate, and its registry row is kept because it names the " +
		"principal that has to be revoked. Retry the deregistration (DELETE this subscriber), " +
		"which is idempotent at the broker, or revoke the principal at the broker by hand."
}

// credentialIssuanceBlock predicts whether POST /subscribers/{id}/kafka-credentials
// will refuse for this row in this deployment, and names the remedy when it will.
//
// The six conditions below are the six IssueSubscriberCredential applies before it
// touches the broker, in the same order, so the reason reported here is the reason that
// would come back:
//
//  1. requireActiveSubscriber — a deregistration in flight.
//  2. requireProvisionableKeyScope — a recorded prefix with no enforcement point
//     declared.
//  3. requireKeyScopeWhenEnforced — no prefix in a deployment that declares the
//     key-scoped model.
//  4. requireAcknowledgedSharedTopicAccess — a whole-topic credential in a secure
//     deployment that has declared neither access model.
//  5. requireGrantedTopics — an empty topic grant.
//  6. subscriberFacingBrokers — no externally advertised bootstrap list.
//
// Parameters:
//   - subscriber model.EventSubscriber: the row being projected.
//   - deployment model.SubscriberAccessDeployment: the resolved configuration. Its zero
//     value is the fail-closed reading, under which a blocked row is reported as
//     blocked.
//
// Returns:
//   - bool: true when the next credential call will refuse for a reason knowable now.
//   - string: the remedy in the imperative, empty when nothing is blocked.
func credentialIssuanceBlock(
	subscriber model.EventSubscriber,
	deployment model.SubscriberAccessDeployment,
) (bool, string) {
	if subscriber.IsRevocationPending() {
		return true, "This subscriber is being deregistered and its broker-side revocation is " +
			"still owed, so issuing a credential would re-arm a principal that is on its way out. " +
			"Complete or abandon the deregistration first."
	}

	keyScoped := subscriber.DeclaresKeyScope()
	keyScopeAvailable := deployment.KeyScopeEnforcement == model.KeyScopeEnforcementGateway

	// (2) A RECORDED PREFIX WITH NOTHING TO KEEP IT. Kafka's authorizer has no message-key
	// dimension, so this row's boundary can only be applied by a component in front of the
	// brokers, and this deployment declares none — issuance refuses rather than minting a
	// credential whose declared scope nothing enforces. All three remedies are real, which
	// is why all three are named.
	if keyScoped && !keyScopeAvailable {
		return true, "This subscriber records a partition key prefix and this deployment declares " +
			"no component that can enforce one, so no credential will be issued for it: Kafka " +
			"authorises topics and consumer groups and has no message-key dimension. Declare a " +
			"key-authorising component with KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway together " +
			"with its gateway addresses and attestation endpoint, or clear the partition key " +
			"prefix to accept access to whole topics, or narrow the subscriber's authorized " +
			"topics, which the broker does enforce."
	}

	// (3) THE MIRROR REFUSAL. In a deployment whose subscribers are confined by record key, the
	// one subscriber with no prefix is the one credential that escapes the model — granted literal
	// topic Read while every other principal is confined — so it is refused too.
	if !keyScoped && keyScopeAvailable {
		return true, "This deployment enforces subscriber access by record key, and this " +
			"subscriber records no partition key prefix, so a credential for it would be granted " +
			"whole-topic reads while every other subscriber is confined to its own ledgers. Record " +
			"a partition_key_prefix on this subscriber."
	}

	// (4) THE WHOLE-TOPIC MODEL AS A DECISION RATHER THAN A DEFAULT. Reached only for a
	// prefix-less row outside a key-scoped deployment, which is exactly when the credential would
	// read a granted topic in full.
	if !keyScoped && !deployment.WholeTopicAccessPermitted {
		return true, "This deployment has not declared how subscriber access is scoped, and a " +
			"credential for this subscriber would read every record on each topic it is granted — " +
			"every ledger's, and every other subscriber's. Declare the model once: set " +
			"KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS=true to acknowledge whole-topic subscriber " +
			"reads, or declare the key-scoped model and record a partition key prefix on each " +
			"subscriber."
	}

	if len(subscriber.AuthorizedTopics) == 0 {
		return true, "This subscriber is authorised for no topics, so any credential issued for " +
			"it would be a live Kafka principal that may read nothing. Set authorized_topics to " +
			"the category topics it is entitled to."
	}

	// (6) THE ADDRESS THE SUBSCRIBER WOULD DIAL. There is no fallback to KAFKA_BROKERS —
	// those are the addresses Blnk dials, internal in every real deployment — so issuance
	// answers a typed 503 rather than handing out a one-time secret together with an
	// endpoint nothing outside can reach.
	if !deployment.SubscriberBrokersAdvertised {
		return true, "This deployment advertises no subscriber-facing Kafka brokers, so a " +
			"credential issued now would name no endpoint the subscriber could dial and issuance " +
			"answers 503 instead. Set KAFKA_SUBSCRIBER_BROKERS to the externally advertised " +
			"listener addresses."
	}

	return false, ""
}

// KafkaCredentialsResponse is returned by POST
// /subscribers/:subscriber_id/kafka-credentials. It hands a subscriber everything
// needed to start consuming: where the brokers are, which topics it may read, which
// consumer group to read under, and the SASL/SCRAM credential to authenticate with.
//
// SECURITY: this is the ONLY type in the API surface that carries a password, and it
// must stay that way.
//
// Two limits are stated here rather than left to be inferred, because a caller
// integrating against this response will otherwise assume the narrower boundary:
//
//   - WITHIN an authorised topic there is NO further restriction. The credential reads
//     every record on that topic, including records belonging to other tenants, ledgers
//     or organisations.
//   - The recorded partition-key prefix IS carried, inside
//     EnforcedAccess.PartitionKeyScope, and only there. That placement is the whole
//     design: it sits beside PartitionKeyPrefixEnforced, which is false, so it cannot
//     be read as a limit on the credential's reach.
type KafkaCredentialsResponse struct {
	// Brokers is the SUBSCRIBER-FACING bootstrap broker list: the externally advertised
	// addresses this subscriber connects to, from KAFKA_SUBSCRIBER_BROKERS.
	Brokers []string `json:"brokers"`

	// BrokerEndpoint is a convenience rendering of the same subscriber-facing
	// list as a single connection string, for clients configured with one
	// endpoint string rather than a list.
	BrokerEndpoint string `json:"broker_endpoint,omitempty"`

	// AuthorizedTopics is the exact set of topics the issued credential is
	// granted Read and Describe on. Reading anything outside it fails
	// authorization at the broker.
	AuthorizedTopics []string `json:"authorized_topics"`

	// ConsumerGroupID is the consumer group the credential is granted Read on.
	// Any group inside EnforcedAccess.ConsumerGroupNamespace is equally usable —
	// this is the default leaf, not the only permitted value.
	ConsumerGroupID string `json:"consumer_group_id"`

	// EnforcedAccess declares which parts of the subscriber's access model the broker
	// enforces for this credential. It is the field a consumer is configured from: the
	// topic set is exact and the group namespace is reserved, and no key-based filtering
	// is applied by the broker to either.
	EnforcedAccess SubscriberEnforcedAccess `json:"enforced_access"`

	// Username is the SASL/SCRAM username, i.e. the subscriber's Kafka
	// principal.
	Username string `json:"username"`

	// Password is the generated SASL/SCRAM secret. Returned once, never
	// persisted, never retrievable again, and never to be logged. See the type
	// comment.
	Password string `json:"password"`

	// Mechanism is the SASL mechanism the credential authenticates with,
	// SCRAM-SHA-512.
	Mechanism string `json:"mechanism"`

	// IssuedAt is the instant the credential was minted, matching the
	// credential_issued_at recorded on the subscriber.
	IssuedAt time.Time `json:"issued_at"`

	// CredentialFingerprint is the short, non-sensitive digest fragment of the stored
	// credential reference — the SAME value GET /subscribers reports for this row once the
	// issuance is recorded.
	CredentialFingerprint string `json:"credential_fingerprint"`

	// Replaced reports that this principal ALREADY held a SCRAM credential and this
	// issuance replaced it.
	Replaced bool `json:"replaced"`
}

// CreateWebhookSubscription is the request body for POST
// /subscribers/:subscriber_id/webhook-subscription.
//
// This is not the /hooks surface. Those are the PRE_TRANSACTION and POST_TRANSACTION
// request-time callouts, they carry a response contract that can influence transaction
// processing, they remain fully functional, and they have nothing to do with these
// types.
//
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window during which Kafka publishing and HTTP webhook delivery run side
// by side from the same outbox events. Once WEBHOOK_DEPRECATION_SUNSET_DATE has passed,
// every request to these routes is answered with 410 Gone by the sunset guard. Use the
// Kafka event stream and the subscriber credential endpoint instead; see
// docs/webhook-to-kafka-migration.md.
type CreateWebhookSubscription struct {
	// WebhookURL is the legacy HTTP endpoint to record for this subscriber. Required: a
	// record whose only field is absent records nothing. It is the subscriber's own
	// endpoint, written down for migration tracking — see the type comment for why nothing
	// is delivered to it.
	WebhookURL string `json:"webhook_url" binding:"required"`
}

// Validate applies the destination policy to the recorded URL.
//
// This route is the ONLY one whose entire purpose is to accept a URL, which makes it
// the most likely way an internal address reaches the column. The rules and the
// reasoning are in validateLegacyWebhookURL; the same policy is applied by the
// subscriber DTOs and again at the persistence boundary, so no path stores a URL that
// another would have refused.
//
// Returns:
//   - error: describing the violation, nil when the URL is acceptable.
func (c CreateWebhookSubscription) Validate() error {
	return validateLegacyWebhookURL(c.WebhookURL)
}

// UpdateWebhookSubscription is the request body for PUT
// /subscribers/:subscriber_id/webhook-subscription. It follows the Create/Update split
// this package uses, and carries the same single mutable field, so correcting a
// recorded URL does not have to go through a delete and re-create.
//
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window during which Kafka publishing and HTTP webhook delivery run side
// by side from the same outbox events. Once WEBHOOK_DEPRECATION_SUNSET_DATE has passed,
// every request to these routes is answered with 410 Gone by the sunset guard. Use the
// Kafka event stream and the subscriber credential endpoint instead; see
// docs/webhook-to-kafka-migration.md.
type UpdateWebhookSubscription struct {
	// WebhookURL is the replacement legacy HTTP endpoint. Required for the same reason it
	// is on create, and delivered to for the same reason: none.
	//
	// Must be HTTPS and must not address an internal destination — see Validate.
	WebhookURL string `json:"webhook_url" binding:"required"`
}

// Validate applies the destination policy to the replacement URL.
//
// Update is checked at exactly the same standard as create, because a URL that arrives
// by an edit is stored in the same column and read by the same readers. A policy
// applied only on creation is a policy with an edit-shaped hole.
//
// Returns:
//   - error: describing the violation, nil when the URL is acceptable.
func (u UpdateWebhookSubscription) Validate() error {
	return validateLegacyWebhookURL(u.WebhookURL)
}

// WebhookSubscriptionResponse is the read shape for the legacy webhook-subscription
// routes, returned by the create, read and update calls. The delete route answers 204
// with no body and so needs no shape.
//
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window during which Kafka publishing and HTTP webhook delivery run side
// by side from the same outbox events. Once WEBHOOK_DEPRECATION_SUNSET_DATE has passed,
// every request to these routes is answered with 410 Gone by the sunset guard. Use the
// Kafka event stream and the subscriber credential endpoint instead; see
// docs/webhook-to-kafka-migration.md.
type WebhookSubscriptionResponse struct {
	// SubscriberID is the subscriber the recorded subscription belongs to.
	SubscriberID string `json:"subscriber_id"`

	// WebhookURL is the recorded legacy HTTP endpoint. Omitted when the
	// subscriber has none, which is the normal state after migration.
	WebhookURL string `json:"webhook_url,omitempty"`

	// MigratedAt is when the subscriber completed its move to Kafka
	// consumption. Nil, and so omitted, means the subscriber is still counted
	// as awaiting migration.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`
}
