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
	"strings"
	"time"

	"github.com/blnkfinance/blnk/model"
)

// This file holds the API-boundary request and response shapes for the Kafka
// event-streaming endpoints (GET /events/dead-letter, POST
// /events/dead-letter/:event_id/replay and GET /events/stats), for the
// subscriber-management endpoints (POST|GET /subscribers, GET|PUT|DELETE
// /subscribers/:subscriber_id and POST /subscribers/:subscriber_id/kafka-credentials),
// and for the legacy webhook-subscription surface that exists only for the 30-day
// dual-delivery window.

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
	Dispatched *int64 `json:"dispatched,omitempty"`

	// DispatchedHistoryCounted reports whether the dispatched count above was taken.
	DispatchedHistoryCounted bool `json:"dispatched_history_counted"`

	// Failed counts rows whose retry budget is spent but which have not yet
	// been written to a dead-letter topic.
	Failed int64 `json:"failed"`

	// DeadLettered counts rows written to a dead-letter topic. Terminal, and
	// the state from which an event may be replayed.
	DeadLettered int64 `json:"dead_lettered"`

	// Replaying counts dead-lettered rows a replay has CLAIMED and not yet finished with.
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
	OffsetsComplete bool `json:"offsets_complete"`

	// MissingTopics lists topics the reading asked for that do not exist on the broker.
	MissingTopics []string `json:"missing_topics,omitempty"`

	// PartitionsUnavailable counts partitions the broker could not report, summed across
	// every topic measured. Each one is a partition whose records are missing from
	// TopicEndOffsets, so a non-zero value invalidates the comparison in the same way a
	// missing topic does.
	PartitionsUnavailable int `json:"partitions_unavailable"`

	// MeasuredWindows is the per-partition offset window the zero-loss verdict was
	// computed against: [first_offset, end_offset) for every partition the broker
	// reported.
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
	AgedOutEvents int64 `json:"aged_out_events"`

	// BeyondEndEvents is how many rows name an offset AT OR ABOVE the end of their
	// partition's log.
	BeyondEndEvents int64 `json:"beyond_end_events"`

	// DuplicatedRecords is how many corroborated rows share a coordinate with another row.
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
