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
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/model"
)

// This file holds the API-boundary request and response shapes for the Kafka
// event-streaming endpoints (GET /events/dead-letter,
// POST /events/dead-letter/:event_id/replay and GET /events/stats), for the
// subscriber-management endpoints (POST|GET /subscribers,
// GET|PUT|DELETE /subscribers/:subscriber_id and
// POST /subscribers/:subscriber_id/kafka-credentials), and for the legacy
// webhook-subscription surface that exists only for the 30-day dual-delivery
// window.
//
// Every type below is a passive struct: no constructors, no methods, no
// validation, no normalisation, no conversion. That is deliberate and it
// matches the rest of this package. The behavioural layer lives in model.go;
// any normalisation a handler needs, the handler applies itself or delegates to
// the root package's subscriber service.
//
// Three things are deliberately NOT declared here:
//
//   - No error shape. Error responses are the exclusive business of
//     api/errors.go's respondCode/respondError, which emit
//     {"error", "error_detail"} from a typed code in
//     internal/apierror/codes.go. A DTO carrying an HTTP status or an error
//     code would create a second, competing error contract, and an ad-hoc
//     status would bypass the single source of truth for code-to-status
//     mapping.
//   - No list envelope and no pagination request. api.FilterResponse and
//     api.FilterRequest already exist in api/filter_helper.go, and the
//     handlers wrap the item types below in those. A second envelope here
//     would compete with them.
//   - No consumer-side shapes. Blnk publishes the <topic>.dlt naming
//     convention and builds nothing subscriber-side: no consumer error
//     handling and no subscriber-managed dead-lettering.
//
// The JSON tags are a contract rather than a preference. The handler tests
// assert these shapes over real HTTP, and the tags reuse the column names of
// blnk.event_outbox and blnk.event_subscribers so that an operator reading a
// response sees the same keys as the DDL and the same keys as the root
// model.EventOutbox / model.EventSubscriber entities.
//
// Request types define no custom UnmarshalJSON and rely on Gin's default,
// non-strict JSON binding, which ignores unknown fields. That is required
// rather than incidental: the API-key middleware rewrites every POST body from
// a non-master caller, unmarshalling it to a map and injecting
// meta_data.BLNK_GENERATED_BY before re-marshalling, so every request shape
// here has to tolerate an unexpected meta_data key. No field is declared for
// it, because neither blnk.event_subscribers nor blnk.event_outbox has a
// meta_data column to persist it to, and declaring one would advertise
// persistence that cannot happen.
//
// A note on timestamps, applied consistently below. A nullable instant is
// *time.Time with omitempty, exactly as model.LineageOutbox does for
// processed_at and locked_until; an instant the handler always stamps is a
// plain time.Time with a plain tag. omitempty is deliberately NOT used on any
// non-pointer time.Time, because encoding/json never treats a struct value as
// empty, so the tag would be inert and would advertise an omission that can
// never happen.

// DeadLetterEvent is the item shape returned by GET /events/dead-letter, and
// the read shape for a single dead-lettered event. It is a projection of the
// persisted model.EventOutbox row: the event envelope, the relay's terminal
// state, and a MINIMIZED account of why the retry budget was spent.
//
// # IT IS AN INVENTORY, NOT A DUMP (DATA-01)
//
// This type once carried the full event payload, the raw last_error string and
// the whole model.FailureMetadata struct. That was more than the endpoint needs
// and more than it should say, on three counts:
//
// THE PAYLOAD IS NOT REQUIRED BY ANY OPERATION THIS ENDPOINT SUPPORTS. A
// dead-lettered event is triaged and then replayed, and replay re-publishes the
// STORED BYTES server-side — the operator never supplies them, and could not
// usefully alter them if they did, because replay's guarantee is byte-fidelity
// against the original. Meanwhile the payload is the marshaled ledger event
// itself: an identity event carries a name, email, phone, address and date of
// birth, and a transaction event carries amounts and balance identifiers. Listing
// a page of dead-lettered events would have returned all of it in one response,
// to any master-key holder, for a triage task that needs none of it. PayloadBytes
// is what triage actually uses, and it is a number.
//
// THE RAW ERROR TEXT DESCRIBES THE INSIDE OF THE DEPLOYMENT. A Kafka client error
// renders as "write tcp 10.0.0.4:34918->10.0.0.7:9092: broken pipe", naming
// internal addresses and broker topology; a database error renders with schema,
// table, constraint, source file and routine. FailureReason carries the
// classification instead, which is what distinguishes a broker problem from an
// oversized event from a denied grant — the actual triage question.
//
// THE FULL FAILURE STRUCT ADDED NOTHING THE ENVELOPE DOES NOT ALREADY CARRY. Its
// original_topic duplicates Topic, its attempt_count duplicates Attempts, and its
// error_reason is the raw text above. Only the two attempt instants were unique to
// it, so those are hoisted to fields and the struct is gone.
//
// None of this is lost data. The outbox row keeps last_error and failure_metadata
// in full, and the operations runbook reads them there, through the database,
// where the audience is a database operator rather than an HTTP response.
//
// This type is deliberately not wrapped in a list envelope. The handler
// returns []DeadLetterEvent directly, or nests it in api.FilterResponse when
// the caller asks for a total count.
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
	// It is NOT the Kafka message key, and this field's documentation used to say
	// that it was. The distinction matters to anyone triaging an ordering
	// question: the key is PartitionKey below, which is derived from the ledger
	// when there is one and falls back to the aggregate when there is not, so on
	// a ledger-less event the key is present while this field is empty. Reading
	// this as the key would produce the wrong answer for exactly the events whose
	// routing is least obvious.
	//
	// Optional because not every event category carries a ledger, so it is
	// omitted rather than reported as an empty string.
	LedgerID string `json:"ledger_id,omitempty"`

	// PartitionKey is the Kafka message key this event was published under, and
	// therefore what pinned it to its partition.
	//
	// It is the stored key from the outbox row, not a value recomputed here, so
	// it is the key the event was ACTUALLY written with rather than the key it
	// would be written with today — the two differ for a row committed before a
	// keying change, and only the stored one explains the partition the event is
	// really in.
	//
	// This is the field to reason about ordering with. Per-aggregate ordering is
	// a property of the key: every event sharing a key lands in one partition and
	// is therefore consumed in publish order, and two events an operator expected
	// to be ordered but which carry different keys are not ordered and never
	// were. Without this field on the response, that question could not be
	// answered from the API at all.
	//
	// Optional only for defensiveness: every row the relay writes carries a key,
	// so an empty value here means a row predating that guarantee.
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

	// FailureReason is a CLASSIFIED reason the event was dead-lettered, drawn
	// from a fixed vocabulary — "broker_unavailable", "message_too_large",
	// "authorization_denied" and so on. It is what an operator triages on, and
	// it deliberately replaces the raw driver text.
	//
	// The raw text is retained in full in the outbox row's last_error column and
	// in failure_metadata.error_reason, where the operations runbook reads it
	// through the database. It is not on the wire because it is verbatim client
	// output: a Kafka write error renders as
	// "write tcp 10.0.0.4:34918->10.0.0.7:9092: broken pipe", naming Blnk's
	// internal addressing and broker topology, and a database error renders with
	// the schema, table, constraint, source file and routine that produced it.
	// A classified reason answers the triage question — is this the broker, the
	// event, or the grant? — without describing the inside of the deployment.
	//
	// Omitted when the row has never failed.
	FailureReason string `json:"failure_reason,omitempty"`

	// FirstAttemptedAt is when the relay first tried to publish the event, and
	// LastAttemptedAt is when it last tried. Together with Attempts they are the
	// whole of what the dead-letter age alert and the triage runbook need from
	// the failure record, and neither carries transport detail.
	//
	// Nil, and so omitted, on a row that has not been dead-lettered.
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
//     is in the outbox row for an operator with database access, and this value says
//     so rather than guessing.
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
// Matching on text is not elegant, and the alternative was considered: storing a
// classification column on blnk.event_outbox at the moment of failure, where the typed
// error is still in hand. That is the better long-term shape, and it is a schema change
// plus a write-path change for a value that is only ever read by one endpoint — so this
// derives the classification at the boundary instead, where being wrong costs a label
// and never a decision.
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
// describes the deployment. An unrecognised input yields
// FailureReasonUnclassified, which is honest about the limit and reveals nothing.
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
// The attempt instants are taken from the row's own columns and, when those are unset,
// from the failure metadata — the two are written by different steps of the same
// failure, and a row can legitimately carry one and not the other.
//
// Parameters:
//   - row model.EventOutbox: the stored outbox row.
//
// Returns:
//   - DeadLetterEvent: the response item, carrying no payload and no raw failure text.
func NewDeadLetterEvent(row model.EventOutbox) DeadLetterEvent {
	item := DeadLetterEvent{
		EventID:     row.EventID,
		EventType:   row.EventType,
		AggregateID: row.AggregateID,
		LedgerID:    row.LedgerID,
		// The STORED key, never a recomputed one — see the field's own documentation. It
		// is what pinned the event to its partition, so it is the only field on this
		// response an ordering question can be answered from, and it was declared and
		// documented here while never being assigned: every dead-letter projection
		// omitted it (the tag is omitempty), so the answer read as "this event had no
		// key" rather than as a gap.
		PartitionKey:     row.PartitionKey,
		OccurredAt:       row.OccurredAt,
		SchemaVersion:    row.SchemaVersion,
		Topic:            row.Topic,
		DLTTopic:         row.DLTTopic,
		Status:           row.Status,
		Attempts:         row.Attempts,
		FailureReason:    classifyFailureReason(row.LastError),
		FirstAttemptedAt: row.FirstAttemptedAt,
		LastAttemptedAt:  row.LastAttemptedAt,
		PayloadBytes:     len(row.Payload),
	}

	// The failure metadata is read ONLY to fill gaps the row's own columns leave, and
	// only for values that carry no transport detail. error_reason is deliberately not
	// read here when last_error already classified: both are the same raw text, and the
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
// Topic is the topic the event was replayed TO, which is its original category
// topic rather than the dead-letter topic it was read from. The response
// deliberately does not echo the payload bytes back: replay fidelity is
// established by what the consumer receives on the original topic, not by the
// body of this acknowledgement, and echoing a payload would invite callers to
// diff the wrong pair of byte strings.
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

// EventOutboxStatsResponse is returned by GET /events/stats and serves the
// daily zero-loss reconciliation procedure in the operations runbook: the sum
// of dispatched and dead-lettered rows is reconciled against the end offsets
// of the main and dead-letter topics, and the two must agree.
//
// The per-status counts are explicit fields rather than a map keyed by
// status. That is on purpose: the runbook names each one, and a map can
// silently omit a status whose count happens to be zero, which reads as
// "no such state" rather than "none in that state". EVERY member of the
// model.EventOutboxStatus* vocabulary is present, and the wire-contract test is
// what keeps that true — it drives the check from model.EventOutboxStatuses
// rather than from a list restated here, so a status added to the model and not
// added here fails rather than vanishing from the reconciliation silently, which
// is the one failure mode a zero-loss check cannot tolerate.
type EventOutboxStatsResponse struct {
	// Pending counts rows written and committed but not yet claimed.
	Pending int64 `json:"pending"`

	// Processing counts rows currently claimed by a relay instance.
	Processing int64 `json:"processing"`

	// WebhookPending counts rows whose KAFKA leg is complete and acknowledged and
	// whose legacy HTTP leg is still owed.
	//
	// It exists because the two delivery legs reach their terminal state
	// independently during the dual-delivery window: a row whose publish succeeded
	// and whose webhook enqueue failed rests here, claimable for the webhook alone,
	// and its Kafka message is already on the topic.
	//
	// Reporting it is not optional for the reconciliation. Such a row IS published,
	// so the daily check counts it in TerminalEvents alongside dispatched and
	// dead-lettered — and a response that omitted the count would leave an operator
	// unable to see the component of a total they are asked to reconcile, while the
	// per-status counts summed to less than the table's row count and made a short
	// total indistinguishable from a lost event.
	//
	// It is NOT terminal: a delivery is still owed, so the retention purge must not
	// reach it.
	WebhookPending int64 `json:"webhook_pending"`

	// Dispatched counts rows the broker has acknowledged. Terminal.
	Dispatched int64 `json:"dispatched"`

	// Failed counts rows whose retry budget is spent but which have not yet
	// been written to a dead-letter topic.
	Failed int64 `json:"failed"`

	// DeadLettered counts rows written to a dead-letter topic. Terminal, and
	// the state from which an event may be replayed.
	DeadLettered int64 `json:"dead_lettered"`

	// Replaying counts dead-lettered rows a replay has CLAIMED and not yet
	// finished with.
	//
	// It is a lease, not a resting place: the row is held so two concurrent
	// replays of the same event cannot both publish it, and it returns to
	// dead_lettered when the replay completes or its lease expires and the
	// relay's recovery sweep reclaims it.
	//
	// It is reported for two reasons. Omitting it made the six statuses sum to
	// less than the table's row count whenever a replay was in flight, so a
	// reconciliation reading the response could not tell a short total from a
	// lost event — which is precisely the distinction it exists to make. And a
	// count that stays non-zero across successive snapshots is the visible
	// symptom of replays whose leases are not being released, which is
	// otherwise only observable in the database.
	//
	// It is NOT terminal, so it does not contribute to TerminalEvents in the
	// reconciliation below.
	Replaying int64 `json:"replaying"`

	// TopicEndOffsets is the per-topic end offset read from the broker, the
	// right-hand side of the reconciliation.
	//
	// The omitempty is mandatory rather than cosmetic. A deployment with no
	// brokers configured is a legitimate steady state, not an error: the
	// publisher resolves to its no-op implementation and no offsets can be
	// read. This endpoint must still serialise cleanly there, reporting the
	// outbox counts it does know and omitting the key entirely rather than
	// emitting a null the reconciliation script would have to special-case.
	TopicEndOffsets map[string]int64 `json:"topic_end_offsets,omitempty"`

	// OffsetsComplete reports whether TopicEndOffsets is a COMPLETE reading of
	// the broker side, and therefore whether the reconciliation may be performed
	// at all. It is the single field a script should branch on before comparing
	// anything.
	//
	// It carries no omitempty, deliberately, so it is present on every response
	// and a client never has to infer completeness from a missing key. The three
	// states it distinguishes are:
	//
	//   - true: every requested topic was found and every partition reported.
	//     The comparison the runbook makes is valid.
	//   - false with TopicEndOffsets absent: no offsets could be read at all —
	//     a deployment with no brokers configured, or a broker that could not be
	//     reached. There is nothing to compare against, which is not an error.
	//   - false with TopicEndOffsets present: the reading is PARTIAL. The sums
	//     are short through unreadability rather than through loss, and
	//     MissingTopics and PartitionsUnavailable say which part is missing.
	OffsetsComplete bool `json:"offsets_complete"`

	// MissingTopics lists topics the reading asked for that do not exist on the
	// broker. They contribute nothing to TopicEndOffsets, so their absence
	// lowers the broker side of the comparison without any event having been
	// lost. Omitted when none were missing.
	MissingTopics []string `json:"missing_topics,omitempty"`

	// PartitionsUnavailable counts partitions the broker could not report,
	// summed across every topic measured. Each one is a partition whose records
	// are missing from TopicEndOffsets, so a non-zero value invalidates the
	// comparison in the same way a missing topic does.
	//
	// It carries no omitempty because an explicit zero is the meaningful,
	// reassuring answer — "every partition was readable" — and a key that
	// vanished on the healthy path would leave a client unable to distinguish
	// that from an old server that never reported it.
	PartitionsUnavailable int `json:"partitions_unavailable"`

	// OffsetsMeasuredAt is when the broker-side reading was taken. It is a
	// different instant from GeneratedAt: the counts come from PostgreSQL and
	// the offsets from Kafka, in separate round trips, so under live traffic a
	// small difference between the two sides is expected rather than suspicious,
	// and its size is only interpretable against the gap between these two
	// timestamps. Nil, and so omitted, when no offsets were read.
	OffsetsMeasuredAt *time.Time `json:"offsets_measured_at,omitempty"`

	// GeneratedAt is the instant the snapshot was taken. The counts and the
	// offsets are read at slightly different moments under live traffic, so a
	// reconciliation that compares them needs to know when the snapshot was
	// made.
	GeneratedAt time.Time `json:"generated_at"`

	// Reconciliation is the SERVER'S OWN VERDICT on the zero-loss comparison,
	// present whenever the broker side could be measured at all.
	//
	// Without it this response carried the two sides of the comparison and left
	// the verdict to whoever read it — so every caller re-implemented the
	// arithmetic, the retention caveat and the directionality, and each got its
	// own chance to get them wrong. In particular a caller subtracting the two
	// sums and alerting on any difference alerts constantly, because redeliveries
	// and replays legitimately make the broker side LARGER; and a caller
	// comparing only for equality reports a green result on a reading that
	// retention has already invalidated.
	//
	// Nil, and so omitted, when no offsets could be read: with nothing to compare
	// against there is no verdict to report, which is not an error. Acceptance
	// criterion V-2 is read from this object.
	Reconciliation *OutboxReconciliationResult `json:"reconciliation,omitempty"`
}

// OutboxReconciliationResult is the wire form of the daily zero-loss check that
// acceptance criterion V-2 is scored on: outbox rows claiming publication,
// against records the broker actually holds.
//
// # The comparison is DIRECTIONAL, and that is the whole design
//
// Every dispatched or dead-lettered row must have produced at least one broker
// record, so the broker side may legitimately exceed the outbox side — a
// redelivery after a crash between the publish and the row being marked, a
// replay, a dead-letter copy — and routinely does. Only a SHORTFALL is evidence
// of loss: rows claiming a publication that no record corresponds to. Fields are
// therefore named to make the asymmetry unmissable (Overhead, LossDetected)
// rather than inviting a caller to diff two totals and alert on any difference.
//
// # And it is RETENTION-AWARE, which is why a green result is not the only good one
//
// Records the broker has deleted under its retention policy are gone from the
// end-offset reading while their outbox rows remain, which manufactures a
// shortfall that is not loss. So "no loss detected" and "the count can be
// trusted" are two separate questions with two separate fields, and a caller must
// answer the second before reporting the first. Conclusive false is not a
// failure — it is the honest statement that counting cannot decide the matter
// today, with Caveats naming why.
//
// It mirrors the service-layer verdict field for field so the API reports exactly
// what the runbook computes, rather than a second, quietly different arithmetic.
type OutboxReconciliationResult struct {
	// TerminalEvents is how many outbox rows claim to have been published:
	// dispatched plus dead-lettered, each counted exactly once. Rows in pending,
	// processing or replaying make no such claim and are excluded.
	TerminalEvents int64 `json:"terminal_events"`

	// ConfirmedEvents is how many of those rows NAME the record they produced,
	// by carrying the topic, partition and offset the broker assigned.
	//
	// It is surfaced rather than left to the arithmetic because it is what makes
	// the verdict below auditable: a caller can see how much of the claim is
	// individually corroborated instead of having to trust a single total.
	ConfirmedEvents int64 `json:"confirmed_events"`

	// UnconfirmedEvents is how many rows claim a publication they cannot name a
	// record for, and it is the field that stops a surplus reading as health.
	//
	// The comparison below is directional — records are a lower bound on events,
	// so a surplus is expected — and its weakness is that a surplus is
	// INDISTINGUISHABLE FROM COMPENSATED LOSS: ten redeliveries and ten lost
	// events produce exactly the totals of a healthy pipeline. An unconfirmed row
	// is precisely a claim nothing corroborates, so while any exist the result is
	// inconclusive rather than green. Withholding this count from the response
	// would leave a caller with a verdict it could not interrogate.
	UnconfirmedEvents int64 `json:"unconfirmed_events"`

	// DuplicatedRecords is how many confirmed rows share a coordinate with
	// another row. It should be zero always.
	//
	// One record is produced by one acknowledged write of one row, and a partial
	// unique index on the coordinate forbids two rows naming the same one, so a
	// non-zero value reports a BROKEN SCHEMA rather than tolerable duplication —
	// and it makes the verdict inconclusive for the same reason an unconfirmed row
	// does, because two rows sharing one record's corroboration is double
	// counting.
	DuplicatedRecords int64 `json:"duplicated_records"`

	// MessagesWritten is how many records the broker holds across the measured
	// topics, from summed end offsets. It counts every redelivery, replay and
	// dead-letter copy separately, which is why it is expected to exceed
	// TerminalEvents rather than to equal it.
	MessagesWritten int64 `json:"messages_written"`

	// Overhead is MessagesWritten minus TerminalEvents: the redeliveries,
	// replays and dead-letter copies.
	//
	// It is SIGNED and is never clamped. A negative value is the only signal
	// this reconciliation carries, and clamping it at zero would erase exactly
	// the finding the check exists to produce. A healthy system's overhead is
	// small and positive, not zero.
	Overhead int64 `json:"overhead"`

	// LossDetected is true when the broker holds FEWER records than the outbox
	// has terminal rows — rows claiming a publication no record corresponds to.
	//
	// False does NOT mean "proven no loss". It means no loss is detectable by
	// counting, which is only a meaningful statement when Conclusive is true.
	LossDetected bool `json:"loss_detected"`

	// Conclusive reports whether the count can be trusted at all: false when a
	// measured topic was missing, when any partition's offsets were unavailable,
	// or when retention has deleted records.
	//
	// It carries no omitempty, deliberately. This is the field a caller must
	// branch on BEFORE reporting a green reconciliation, and a key that vanished
	// on the inconclusive path would leave the most dangerous state looking like
	// the healthiest one.
	Conclusive bool `json:"conclusive"`

	// Caveats names, in plain words, every reason the result is inconclusive, so
	// an operator is told what to do rather than merely that something is wrong.
	// Empty and omitted when Conclusive is true.
	Caveats []string `json:"caveats,omitempty"`

	// Summary is the verdict as one sentence, always naming both numbers so the
	// sentence is checkable against the fields above. It is what a runbook or an
	// alert annotation quotes.
	Summary string `json:"summary"`

	// MeasuredAt is when the broker side was measured. It is the same instant as
	// the enclosing response's OffsetsMeasuredAt and is repeated here so this
	// object stands alone when it is logged or forwarded on its own.
	MeasuredAt time.Time `json:"measured_at"`
}

// CreateSubscriber is the request body for POST /subscribers.
//
// A subscriber is a Kafka principal: registering one records who may consume,
// which topics they are entitled to and under which consumer group, and it is
// the row a later credential issuance attaches its non-reversible reference to.
// Registration and provisioning are separate steps, so a freshly created
// subscriber legitimately holds no credential at all.
//
// Only Name is required. Every other field is either generated by the service
// when omitted (the subscriber ID, the Kafka principal and the consumer group
// are all derivable from the subscriber's identity) or genuinely optional. Name
// cannot be derived: it is the human label an operator recognises the principal
// by months later, it is NOT NULL in blnk.event_subscribers, and marking it
// required here matches CreateAPIKeyRequest, the closest analogue in this
// package.
//
// AuthorizedTopics is intentionally not required. The column defaults to the
// empty array, which means a subscriber registered without an explicit grant is
// authorised for nothing rather than for everything: the registry fails closed,
// and widening a grant is a deliberate follow-up call.
//
// # THE PRINCIPAL AND THE CONSUMER GROUP ARE NOT FIELDS HERE (SEC-03)
//
// Both were once accepted from the caller, "omit to have the service derive it".
// That is the wrong shape for a value that IS an authorization boundary, and the
// reason is worth stating plainly, because "optional, derived when absent" reads
// like a convenience:
//
// The Kafka principal is what every ACL binding is granted TO. A request that can
// choose it is a request that can choose which identity receives a grant — so it
// can name another subscriber's principal and have its own authorised topics
// added to that subscriber's grant, or name the administrative principal. The
// consumer group is what the group ACL is granted OVER, with a PREFIXED pattern
// type, so a caller choosing it can name a prefix that spans other subscribers'
// group namespaces and then join their groups and take their partition
// assignments.
//
// Neither is a name the caller wants; each is a boundary the caller would be
// selecting. So they are DERIVED, always, from the subscriber identifier by
// model.CanonicalKafkaPrincipal and model.CanonicalConsumerGroupID, and there is
// no field through which a value can be offered. Removing the fields rather than
// validating them is deliberate: a field that is validated on every path today
// can be read by a path added tomorrow, whereas a field that does not exist
// cannot be read at all.
//
// Validate covers what remains caller-supplied — the identifier, the topic list,
// the recorded key scope and the legacy URL — and must be called before the
// request is trusted.
type CreateSubscriber struct {
	// SubscriberID is the business key, and the {id} in
	// POST /subscribers/{id}/kafka-credentials. Omit it to have the service
	// generate one in the repository's "<prefix>_<uuid>" form.
	//
	// When supplied it must be canonical — see
	// model.CanonicalizeSubscriberIdentifier — because the principal and the
	// consumer group are derived from it, which makes this value the root of
	// the subscriber's whole identity rather than a label.
	SubscriberID string `json:"subscriber_id"`

	// Name is the human label the subscriber is triaged by. Required: it is
	// the one field the service cannot invent a meaningful value for.
	Name string `json:"name" binding:"required"`

	// AuthorizedTopics is the set of topics this subscriber may Read and
	// Describe, mapping to the authorized_topics TEXT[] column and to the exact
	// set of ACL bindings provisioned for the principal. Omitted or empty means
	// no grant, which is the safe default rather than a missing value.
	//
	// Every entry must be a Blnk-owned subscriber-facing category topic;
	// Validate enforces that against model.SubscriberGrantableTopics.
	//
	// The binding tags cap the two dimensions a tag can express — how many
	// topics, and how long each may be — and they do it in the BINDER, before
	// any handler code runs and whether or not Validate is called at all. That
	// is what keeps the dimensions bounding allocation from depending on a
	// handler remembering to validate.
	AuthorizedTopics []string `json:"authorized_topics" binding:"max=16,dive,max=249"`

	// PartitionKeyPrefix records a key-scoped authorization Kafka CANNOT ENFORCE,
	// so supplying one makes the subscriber UNPROVISIONABLE: credential issuance
	// refuses for it, with 409, until the value is cleared.
	//
	// There is no ACL that narrows a principal to a key range, so a credential
	// issued for such a row would read every record on every authorised topic
	// while the registry described something narrower. Supply it only to record
	// an authorization somebody intends to satisfy another way; to restrict what
	// a subscriber can read, narrow AuthorizedTopics, which is enforceable.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// WebhookURL records the legacy HTTP webhook URL a migrating subscriber
	// received pushes on before moving to Kafka. It exists only for the
	// dual-run window and is never required; a subscriber onboarded after the
	// cutover never had one. Must be HTTPS and must not address an internal
	// destination — see Validate.
	WebhookURL string `json:"webhook_url,omitempty"`
}

// Derived returns the Kafka principal and consumer group for this request.
//
// It is the ONLY way a handler obtains either, which is what keeps them derived
// rather than chosen. Calling it on a request whose SubscriberID is empty is a
// programming error the handler must avoid by generating the identifier first —
// there is nothing to derive an identity from until one exists.
//
// Returns:
//   - principal string: "blnk-sub-<subscriber_id>".
//   - consumerGroup string: "blnk-sub-<subscriber_id>.default".
//   - err error: wrapping model.ErrInvalidSubscriberIdentifier when the
//     identifier is not canonical.
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
// It exists because binding tags cannot express any of these rules, and because
// the alternative — leaving them to the handler — means the rules hold only for
// the handlers that remember them. A DTO that can validate itself is validated
// the same way by every caller.
//
// It does NOT check the principal or the consumer group, because neither is a
// field: see the type comment.
//
// Parameters:
//   - topicPrefix string: the configured KAFKA_TOPIC_PREFIX, needed to resolve
//     which topic names this deployment owns. A blank value falls back to the
//     strictest namespace rather than a permissive one.
//
// Returns:
//   - error: describing the first violation, nil when the request is usable.
func (c CreateSubscriber) Validate(topicPrefix string) error {
	// The identifier is checked only when supplied: an omitted one is generated
	// by the service, and generation produces a canonical value by construction.
	if c.SubscriberID != "" {
		if _, err := model.CanonicalizeSubscriberIdentifier(c.SubscriberID); err != nil {
			return err
		}
	}

	if err := validateGrantableTopics(c.AuthorizedTopics, topicPrefix); err != nil {
		return err
	}

	if err := validateSubscriberKeyScope(c.PartitionKeyPrefix); err != nil {
		return err
	}

	return validateLegacyWebhookURL(c.WebhookURL)
}

// UpdateSubscriber is the request body for PUT /subscribers/:subscriber_id and
// carries the mutable subset of a subscriber only.
//
// Every field is a pointer so that "omitted" is distinguishable from
// "explicitly set to empty", which this shape genuinely needs rather than
// merely benefits from. partition_key_prefix is the clear case: NULL means the
// subscriber is entitled to whole topics, while the empty string would mean
// restricted to the empty prefix, and those are opposite intents. A plain
// string cannot express the difference, so it would make silently inverting an
// operator's intent possible. The nullable columns are represented the same way
// on the root model.EventSubscriber entity, so pointers here are the
// established representation rather than a new convention.
//
// Four groups of fields are deliberately absent:
//
//   - KafkaPrincipal. It is the join key to every ACL binding already
//     provisioned for this subscriber, so changing it would orphan them all
//     and leave the registry claiming access the broker does not grant.
//     Re-pointing a subscriber at a new principal is a re-provisioning
//     operation, not an attribute edit. It is also derived from the immutable
//     subscriber ID, so there is no value a caller could legitimately supply.
//   - ConsumerGroupID, absent for the same two reasons (SEC-03). It is derived
//     from the subscriber ID, and it is the resource the group ACL is granted
//     over with a PREFIXED pattern type — so a caller able to edit it could
//     name a prefix spanning other subscribers' group namespaces, join their
//     consumer groups, and take their partition assignments. It was previously
//     editable here; that was the defect.
//   - CredentialReference and CredentialIssuedAt. Those two together are the
//     record of an issuance, written only by the credential endpoint. A client
//     that could set them could claim an issuance that never happened.
//   - MigratedAt, and any secret of any kind. The service owns stamping the
//     migration instant, and no request shape anywhere accepts a credential.
type UpdateSubscriber struct {
	// Name replaces the human label when present.
	Name *string `json:"name,omitempty"`

	// AuthorizedTopics replaces the whole authorised set when present. It is
	// already nilable as a slice, so no pointer is needed to tell "omitted"
	// from "set to empty": nil is omitted, and a present empty array revokes
	// every topic grant.
	//
	// Every entry must be a Blnk-owned subscriber-facing category topic;
	// Validate enforces that. Widening a grant here does not by itself widen
	// what the subscriber can read — the ACL bindings must be re-provisioned —
	// but the row is what the next provisioning reads, so it is checked at the
	// same standard as a fresh registration.
	AuthorizedTopics []string `json:"authorized_topics,omitempty" binding:"omitempty,max=16,dive,max=249"`

	// PartitionKeyPrefix replaces the recorded key-scoped authorization when
	// present, and a present empty string CLEARS it — see the type comment for
	// why this cannot be a plain string.
	//
	// It grants and revokes nothing at the broker. What it decides is whether a
	// credential can be issued at all: Kafka cannot enforce a key scope, so a row
	// carrying one is refused a credential, and clearing it here is what makes the
	// subscriber provisionable again.
	PartitionKeyPrefix *string `json:"partition_key_prefix,omitempty"`

	// WebhookURL replaces the recorded legacy webhook URL when present, so an
	// operator can correct a subscriber's dual-run record. A present empty
	// string clears it. Any non-empty value must be HTTPS and must not address
	// an internal destination — see Validate.
	WebhookURL *string `json:"webhook_url,omitempty"`
}

// Validate checks every caller-supplied value that is present on the request.
//
// Absent fields are not checked, because absent means "leave as stored" and the
// stored value was validated when it was written. A field present but empty IS
// checked, because that is an explicit instruction to clear, and clearing is
// legitimate for the key scope, the topic list and the legacy URL alike.
//
// Parameters:
//   - topicPrefix string: the configured KAFKA_TOPIC_PREFIX.
//
// Returns:
//   - error: describing the first violation, nil when the request is usable.
func (u UpdateSubscriber) Validate(topicPrefix string) error {
	if u.AuthorizedTopics != nil {
		if err := validateGrantableTopics(u.AuthorizedTopics, topicPrefix); err != nil {
			return err
		}
	}

	if u.PartitionKeyPrefix != nil {
		if err := validateSubscriberKeyScope(*u.PartitionKeyPrefix); err != nil {
			return err
		}
	}

	if u.WebhookURL != nil {
		if err := validateLegacyWebhookURL(*u.WebhookURL); err != nil {
			return err
		}
	}

	return nil
}

// ValidateCreateSubscriber validates a POST /subscribers body without a configured topic
// prefix in hand.
//
// It is the prefix-independent half of Validate, and it exists so that the request body can
// be rejected at the DTO layer — where a malformed body is cheapest to refuse — by a caller
// that has no configuration snapshot: a binder hook, a table-driven contract test, a CLI.
// Validate remains the authoritative check because only it can compare a grant against the
// deployment's own topic namespace.
//
// Name is required for the reason the registry exists: it is the label an operator recognises
// a principal by months later, it is NOT NULL in blnk.event_subscribers, and it is the one
// field no service can invent a meaningful value for.
//
// The grant is checked in two ways. model.ValidateSubscriberTopics applies the resource
// bounds — cardinality, blank elements, length, the Kafka character set and duplicates — and
// the grantable allowlist is applied under model.DefaultEventTopicPrefix, which is the
// STRICTEST available answer when the configured prefix is unknown: a deployment that renamed
// its prefix is validated again, against its real prefix, by Validate.
//
// Returns:
//   - error: nil when the body is acceptable, otherwise an error naming the broken rule.
func (c CreateSubscriber) ValidateCreateSubscriber() error {
	if strings.TrimSpace(c.Name) == "" {
		return fmt.Errorf("name is required")
	}

	if err := model.ValidateSubscriberTopics(c.AuthorizedTopics); err != nil {
		return err
	}

	return validateGrantableTopics(c.AuthorizedTopics, model.DefaultEventTopicPrefix)
}

// ValidateUpdateSubscriber validates a PUT /subscribers/:subscriber_id body without a
// configured topic prefix in hand.
//
// No field is required: an update carries the mutable subset a caller chose to change, and
// every field is a pointer or a nilable slice precisely so that "omitted" stays
// distinguishable from "set to empty".
//
// The grant is validated only when PRESENT. nil means the caller is not touching the
// authorised set, so validating it would reject an update to an unrelated field on a
// subscriber whose stored grant predates a rule — while a present empty array is a
// deliberate revocation of every topic and must continue to be accepted. When a grant IS
// present it replaces the whole set, so it is validated exactly as strictly as on create; an
// update path that checked less would be the way around the create path's bounds.
//
// Returns:
//   - error: nil when the body is acceptable, otherwise an error naming the broken rule.
func (u UpdateSubscriber) ValidateUpdateSubscriber() error {
	if u.AuthorizedTopics == nil {
		return nil
	}

	if err := model.ValidateSubscriberTopics(u.AuthorizedTopics); err != nil {
		return err
	}

	return validateGrantableTopics(u.AuthorizedTopics, model.DefaultEventTopicPrefix)
}

// validateGrantableTopics refuses an authorised-topic list containing anything a
// subscriber may not be granted (SEC-03).
//
// The list becomes the ACL bindings, so whatever is accepted here is what the
// issued credential can read. Membership is EXACT against
// model.SubscriberGrantableTopics — not a prefix test, not a normalising test —
// and that is what makes three distinct attacks plain non-members rather than
// special cases somebody has to remember to write:
//
//   - "*", which Kafka reads as matching every resource, so a single such entry
//     converts a per-topic grant into a cluster-wide one.
//   - A FOREIGN topic such as "attacker.transactions", which has exactly the
//     shape of an owned name and none of the meaning: granting it is a grant into
//     somebody else's data on a broker Blnk may share.
//   - A DEAD-LETTER or INTERNAL topic. The DLTs carry other subscribers' failed
//     events together with Blnk's failure metadata; the system category carries
//     Blnk's own diagnostics, and as the catch-all it carries payloads nothing has
//     yet classified. None has a subscriber audience.
//
// An EMPTY list is accepted, because a subscriber authorised for nothing is the
// fail-closed default of a fresh registration. An empty or whitespace-only ENTRY
// is refused rather than skipped: it is always a bug in whatever assembled the
// list, and silently dropping it would let a caller believe it had requested a
// grant it did not receive.
//
// Parameters:
//   - topics []string: the requested authorised topics.
//   - topicPrefix string: the configured KAFKA_TOPIC_PREFIX.
//
// Returns:
//   - error: naming the first offending topic and listing what is grantable.
func validateGrantableTopics(topics []string, topicPrefix string) error {
	if len(topics) == 0 {
		return nil
	}

	grantable := model.SubscriberGrantableTopics(topicPrefix)

	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" {
			return fmt.Errorf("authorized_topics contains an empty topic name")
		}

		if !model.IsSubscriberGrantableTopicName(topic, topicPrefix) {
			return fmt.Errorf(
				"authorized_topics entry %q is not grantable; a subscriber may be granted only "+
					"Blnk-owned subscriber-facing category topics (%s). Dead-letter and internal "+
					"topics carry Blnk's own failure and diagnostic data and have no subscriber "+
					"audience",
				topic, strings.Join(grantable, ", "),
			)
		}
	}

	return nil
}

// validateSubscriberKeyScope constrains the recorded key-scoped authorization.
//
// The value grants nothing at the broker — it decides whether a credential can be
// issued at all — so the rules here are about it being STORABLE AND DISPLAYABLE
// rather than about isolation. Two things are refused, and both are about what the
// string does after it is stored:
//
//   - CONTROL CHARACTERS. The value is echoed into API responses, log lines and
//     trace attributes. A newline in it splits a log line in two and forges a
//     second entry; a carriage return can overwrite one on a terminal.
//   - Surrounding WHITESPACE, because a scope with a trailing space describes a
//     different set of keys from the one the caller meant, and the difference is
//     invisible in every rendering of the value.
//
// An empty value is legitimate and means no key-scoped authorization is recorded,
// which is the only state a credential can be issued in. The length bound is
// generous — a key prefix is a fragment of a ledger ID, not a document — and exists
// so that an unbounded string cannot be parked in the registry.
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

// validateLegacyWebhookURL applies the destination policy to the dual-run webhook URL
// (SSRF-01).
//
// # Why a column with no sender is validated at all
//
// Nothing in Blnk sends to this URL today. It is recorded so a subscriber already
// receiving HTTP pushes has somewhere to be migrated FROM. But a stored URL is a
// future sink: the moment any code sends to it, whatever is in this field becomes a
// request Blnk makes from inside its own network, with its own network position.
// Constraining it now costs one function; constraining it after a sender exists means
// auditing every row already written and hoping none was used first.
//
// Two rules, each closing a distinct route:
//
//   - HTTPS ONLY. A subscriber's event stream carries ledger data — identity events
//     include names, addresses and dates of birth — and http:// would put it on the
//     wire in clear text. It also refuses the non-HTTP schemes that turn a URL field
//     into a local-resource read: file://, gopher://, ftp:// and friends.
//   - NO INTERNAL DESTINATION. Loopback, link-local (including the 169.254.169.254
//     cloud metadata endpoint), private ranges, the unspecified address, multicast,
//     and the hostnames that resolve to them. Blnk runs alongside its own database,
//     Redis, TypeSense, brokers and — in a cloud deployment — an instance metadata
//     service that hands out credentials to anything that asks from the right place.
//
// This is a literal-address check, and it is deliberately not sold as more than that:
// a hostname resolving to an internal address at send time is not detectable here, and
// defeating that needs resolution-time validation in the sender. The repository layer
// applies the same policy, so a URL arriving by another path is refused too.
//
// Parameters:
//   - rawURL string: the URL, empty when none is recorded or when clearing.
//
// Returns:
//   - error: describing the violation without echoing more of the URL than the host.
func validateLegacyWebhookURL(rawURL string) error {
	if rawURL == "" {
		return nil
	}

	if rawURL != strings.TrimSpace(rawURL) {
		return fmt.Errorf("webhook_url must not have surrounding whitespace")
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		// The parse error is not echoed: it quotes the input, and the input is a
		// third party's endpoint that has no business in Blnk's error responses.
		return fmt.Errorf("webhook_url is not a valid URL")
	}

	if parsed.Scheme != "https" {
		return fmt.Errorf("webhook_url must use https, got scheme %q", parsed.Scheme)
	}

	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("webhook_url must include a host")
	}

	if reason := internalWebhookDestinationReason(host); reason != "" {
		return fmt.Errorf("webhook_url host %q is not an allowed destination: %s", host, reason)
	}

	return nil
}

// internalWebhookDestinationReason reports why a host is an internal destination, or ""
// when it is not.
//
// It returns a REASON rather than a boolean so the caller can say which rule was hit.
// "not allowed" sends an operator looking for a policy document; "the cloud instance
// metadata endpoint" tells them what they just tried to point Blnk at.
//
// Parameters:
//   - host string: the hostname or IP literal from the URL, without a port.
//
// Returns:
//   - string: the reason, or "" when the host is acceptable.
func internalWebhookDestinationReason(host string) string {
	lowered := strings.ToLower(host)

	// IP literals are decided on the parsed address, never on the text. net.ParseIP
	// resolves the IPv4-mapped IPv6 forms too, so "::ffff:127.0.0.1" is recognised as
	// loopback rather than passing as an unfamiliar-looking string.
	if address := net.ParseIP(host); address != nil {
		switch {
		case address.IsLoopback():
			return "it is a loopback address"
		case address.IsLinkLocalUnicast(), address.IsLinkLocalMulticast():
			return "it is a link-local address, which reaches the cloud instance metadata endpoint"
		case address.IsPrivate():
			return "it is a private address inside Blnk's own network"
		case address.IsUnspecified():
			return "it is the unspecified address"
		case address.IsMulticast():
			return "it is a multicast address"
		}

		return ""
	}

	if lowered == "localhost" || strings.HasSuffix(lowered, ".localhost") {
		return "it resolves to loopback"
	}

	// .local is mDNS and .internal is the conventional private zone — and the name
	// metadata.google.internal is one of the two best-known metadata endpoints.
	if strings.HasSuffix(lowered, ".local") || strings.HasSuffix(lowered, ".internal") {
		return "it is an internal-only hostname"
	}

	// An unqualified single-label name can only resolve through a local search domain,
	// which is by definition inside the network Blnk runs in.
	if !strings.Contains(lowered, ".") {
		return "it is an unqualified hostname that can only resolve inside Blnk's own network"
	}

	return ""
}

// SubscriberResponse is the read shape for GET /subscribers,
// GET /subscribers/:subscriber_id, and the bodies returned by the create and
// update routes.
//
// This type deliberately exposes NO credential material of any kind, never the
// credential itself. That is not merely a convention here, it is structurally
// guaranteed: blnk.event_subscribers has no column capable of holding a plaintext
// or reversibly-encrypted secret, so a password field on this type could never be
// populated from persistence. It could only ever leak one. The posture mirrors
// blnk.api_keys, where the stored value is a bcrypt hash and the raw key is never
// kept.
//
// # It reports a FINGERPRINT, not the credential reference (SECRET-01)
//
// The full reference was once returned here. The reference is not a secret — it is
// a non-reversible derivation and nobody can authenticate with it — but it is not
// information a client needs either, and returning it had two costs worth
// avoiding. It is internal correlation state, so exposing it invites a caller to
// treat it as an identifier to send back or to compare against something, which
// makes it an accidental part of the API contract. And "the reference is
// non-reversible" is a property of the code that DERIVES it, not of the column
// that stores it: a value mislabeled as a reference by some future path would be
// published verbatim by this response.
//
// model.CredentialFingerprint closes both. It answers the only question a client
// legitimately has — "is this the same issuance I saw last time?" — and it yields
// the empty string for anything that fails reference validation, so a mis-stored
// value produces no output at all rather than a partial rendering of itself.
//
// A nil CredentialIssuedAt is the reliable test for "registered, but no
// credential has ever been issued", which is a real state the registry has to
// represent.
type SubscriberResponse struct {
	// SubscriberID is the business key callers address the subscriber by.
	SubscriberID string `json:"subscriber_id"`

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

	// PartitionKeyPrefix is the key-scoped authorization recorded for this
	// subscriber, and a non-empty value means NO CREDENTIAL CAN BE ISSUED for it.
	// Kafka authorises at topic and group granularity only, so a subscriber
	// authorised for a topic reads every record in it whatever this says, and
	// issuance refuses rather than hand out access the registry does not
	// describe. Omitted when no such authorization is recorded, which is the
	// provisionable state.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// EnforcedAccess declares which parts of this subscriber's access model the
	// broker actually enforces. It is always present, including when no
	// credential has been issued, because it describes the boundary issuance
	// WILL establish and an integrator needs it before wiring a consumer.
	EnforcedAccess SubscriberEnforcedAccess `json:"enforced_access"`

	// CredentialFingerprint is a short, non-sensitive digest fragment
	// identifying which credential issuance this row records. It answers "is
	// this the same credential I saw last time?" and nothing else — it cannot
	// be authenticated with, and it is not the stored reference. Empty, and so
	// omitted, when no credential has been issued.
	CredentialFingerprint string `json:"credential_fingerprint,omitempty"`

	// CredentialIssuedAt is when the current credential was issued. Nil, and so
	// omitted, when none ever has been.
	CredentialIssuedAt *time.Time `json:"credential_issued_at,omitempty"`

	// WebhookURL is the legacy HTTP webhook URL recorded for the dual-run
	// window. Omitted for a subscriber onboarded after the cutover.
	WebhookURL string `json:"webhook_url,omitempty"`

	// MigratedAt is when the subscriber completed its move to Kafka
	// consumption. Nil, and so omitted, means not yet migrated, which is what
	// migration-progress reporting counts.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`

	// CreatedAt is when the subscriber was registered.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the registry row was last written.
	UpdatedAt time.Time `json:"updated_at"`
}

// Enforcement dimensions reported by SubscriberEnforcedAccess.EnforcedBy. They name what
// Kafka's authorizer actually evaluates on every request a subscriber makes.
const (
	// EnforcementDimensionTopic is Read and Describe bound to an EXACT topic name, so a
	// topic absent from the grant is refused at the broker.
	EnforcementDimensionTopic = "topic"

	// EnforcementDimensionConsumerGroup is Read bound to the subscriber's consumer-group
	// namespace as a PREFIXED pattern, which reserves that namespace to the subscriber and
	// refuses every group outside it.
	EnforcementDimensionConsumerGroup = "consumer_group"
)

// SubscriberEnforcedAccess declares, inside the response body, which parts of a subscriber's
// recorded access model the broker actually enforces.
//
// # Why the API states this rather than leaving it to documentation
//
// A subscriber's record carries three access-shaped values: authorized_topics, a consumer
// group, and partition_key_prefix. Two of them are ACL bindings. The third is not, and cannot
// be: Kafka's authorizer has no message-key dimension, so there is no ACL that confines a
// consumer to a slice of a topic by key. A subscriber granted a topic reads EVERY record on
// that topic whatever its key prefix says.
//
// An integrator reading a response that lists all three together has no way to tell which is
// which, and the plausible wrong conclusion is the dangerous one: that two subscribers sharing
// a topic with different key prefixes cannot see each other's events. They can. Building a
// tenancy boundary on that belief would expose every tenant's ledger events to every other
// tenant on the same topic.
//
// So the enforced dimensions are enumerated positively, in the body, and the one question whose
// wrong answer is a disclosure bug is answered outright by PartitionKeyPrefixEnforced. A client
// deciding whether key filtering is a security control reads a false, or scans EnforcedBy and
// finds no key dimension among the values declared above. Either way the answer is in the
// response rather than in prose nobody reads, and it cannot drift from the implementation.
//
// This is not per-tenant topics. Blnk deliberately does not create a topic per subscriber;
// isolation is delivered by the exact topic grant plus the reserved consumer-group namespace,
// which is what these two dimensions describe.
type SubscriberEnforcedAccess struct {
	// EnforcedBy names every dimension the broker evaluates, and is exhaustive.
	// partition_key_prefix is absent by construction, not by omission.
	EnforcedBy []string `json:"enforced_by"`

	// Topics is the exact set of topics the principal may Read and Describe. Anything
	// outside it is refused at the broker, not filtered by the client.
	Topics []string `json:"topics"`

	// ConsumerGroupNamespace is the group-id prefix reserved to this subscriber. Any group
	// beginning with it is usable; any group outside it is refused. Empty only when the
	// subscriber's consumer group is not derivable, which registration prevents.
	ConsumerGroupNamespace string `json:"consumer_group_namespace,omitempty"`

	// PartitionKeyPrefixEnforced is always false and is stated rather than implied. It is
	// the one question whose wrong answer is a data-disclosure bug, so the response answers
	// it outright instead of leaving it to be inferred from EnforcedBy.
	PartitionKeyPrefixEnforced bool `json:"partition_key_prefix_enforced"`
}

// NewSubscriberEnforcedAccess builds the enforced-access declaration for a subscriber.
//
// It is the single place the declaration is assembled, so no handler can publish a response
// that claims key filtering is enforced, and the enforced dimension list cannot diverge between
// the subscriber view and the credential view. The consumer-group namespace is DERIVED here
// through the same model helper the ACL binding itself is built from, rather than accepted as a
// parameter, so the value reported can never describe a namespace different from the one the
// broker actually reserved.
//
// A subscriber id that does not canonicalize yields an empty namespace rather than an error:
// this is a response projection, and registration already refuses such an id, so the only way
// to reach that branch is a row that predates the constraint. Reporting no namespace is the
// honest answer, and it is preferable to failing a read of a row an operator is probably trying
// to inspect precisely because it is malformed.
//
// Parameters:
//   - subscriberID string: the business key the principal and group are derived from.
//   - topics []string: the subscriber's exact topic grant. A nil slice becomes [] so the body
//     never carries null for a set an operator has to read.
//
// Returns:
//   - SubscriberEnforcedAccess: the declaration, with PartitionKeyPrefixEnforced false.
func NewSubscriberEnforcedAccess(subscriberID string, topics []string) SubscriberEnforcedAccess {
	enforced := SubscriberEnforcedAccess{
		EnforcedBy: []string{
			EnforcementDimensionTopic,
			EnforcementDimensionConsumerGroup,
		},
		Topics: topics,
		// Never set true anywhere. Kafka has no message-key authorization dimension, so a
		// true here could only ever be a false claim.
		PartitionKeyPrefixEnforced: false,
	}

	if namespace, err := model.CanonicalConsumerGroupNamespace(subscriberID); err == nil {
		enforced.ConsumerGroupNamespace = namespace
	}

	if enforced.Topics == nil {
		enforced.Topics = []string{}
	}

	return enforced
}

// NewSubscriberResponse projects a stored subscriber into its response shape.
//
// Every handler that returns a subscriber goes through here, and that is the point:
// the credential reference is reduced to a fingerprint in exactly one place, so no
// handler can return the raw reference by writing the obvious assignment. A
// projection that must be constructed is a projection that cannot be assembled
// wrongly by omission.
//
// The nullable columns are flattened to their zero values, which the omitempty tags
// then drop from the body. That is correct for every one of them: an absent key
// scope, an absent legacy URL and an absent credential are all "not recorded",
// which is precisely what an omitted key means.
//
// Parameters:
//   - subscriber model.EventSubscriber: the stored registry row.
//
// Returns:
//   - SubscriberResponse: the response body, carrying no credential material.
func NewSubscriberResponse(subscriber model.EventSubscriber) SubscriberResponse {
	response := SubscriberResponse{
		SubscriberID:       subscriber.SubscriberID,
		Name:               subscriber.Name,
		KafkaPrincipal:     subscriber.KafkaPrincipal,
		ConsumerGroupID:    subscriber.ConsumerGroupID,
		AuthorizedTopics:   subscriber.AuthorizedTopics,
		CredentialIssuedAt: subscriber.CredentialIssuedAt,
		MigratedAt:         subscriber.MigratedAt,
		CreatedAt:          subscriber.CreatedAt,
		UpdatedAt:          subscriber.UpdatedAt,
	}

	// AuthorizedTopics is reported even when empty — an empty grant is a meaningful,
	// fail-closed state an operator needs to see — so a nil slice becomes [] rather
	// than null, which a client would otherwise have to special-case.
	if response.AuthorizedTopics == nil {
		response.AuthorizedTopics = []string{}
	}

	// Assembled here rather than by the handler, so every subscriber response states the
	// enforced boundary and none can imply that the advisory key prefix is one.
	response.EnforcedAccess = NewSubscriberEnforcedAccess(
		subscriber.SubscriberID, response.AuthorizedTopics,
	)

	if subscriber.PartitionKeyPrefix != nil {
		response.PartitionKeyPrefix = *subscriber.PartitionKeyPrefix
	}

	if subscriber.WebhookURL != nil {
		response.WebhookURL = *subscriber.WebhookURL
	}

	// CredentialFingerprint, never the reference. model.CredentialFingerprint yields
	// "" for anything that is not a validly derived reference, so a value mis-stored
	// by some other path is dropped entirely instead of being echoed.
	if subscriber.CredentialReference != nil {
		response.CredentialFingerprint = model.CredentialFingerprint(*subscriber.CredentialReference)
	}

	return response
}

// KafkaCredentialsResponse is returned by
// POST /subscribers/:subscriber_id/kafka-credentials. It hands a subscriber
// everything needed to start consuming: where the brokers are, which topics it
// may read, which consumer group to read under, and the SASL/SCRAM credential
// to authenticate with.
//
// SECURITY: this is the ONLY type in the API surface that carries a password,
// and it must stay that way.
//
// Password is populated exactly once, in the response to the issuing call. It
// is never persisted: blnk.event_subscribers stores only a non-reversible
// reference and the issuance instant, and has no column capable of holding the
// secret. It is therefore not retrievable afterwards by any route, including
// this one re-called, which mints a NEW credential rather than returning the
// old one. A lost password can only be replaced, never recovered. It must never
// be logged, echoed into an error message, or written to a trace attribute.
// This mirrors Blnk's API-key posture, where the stored value is a bcrypt hash
// and the raw key is never kept.
//
// # THE ACCESS BOUNDARY THIS RESPONSE DESCRIBES
//
// The credential grants TOPIC-LEVEL Read and Describe on AuthorizedTopics, plus
// Read on the subscriber's prefixed consumer-group namespace. Nothing further.
//
// Two limits are stated here rather than left to be inferred, because a caller
// integrating against this response will otherwise assume the narrower boundary:
//
//   - WITHIN an authorised topic there is NO further restriction. The credential
//     reads every record on that topic, including records belonging to other
//     tenants, ledgers or organisations. Kafka authorises at topic and group
//     granularity and has no message-key dimension, so per-key filtering cannot
//     be enforced by the broker and is not claimed anywhere.
//   - This response carries NO partition-key prefix, deliberately. The prefix
//     recorded on a subscriber is an advisory client-side filter hint, and
//     returning it beside a credential would present it as a limit on the
//     credential's reach. It is reported on SubscriberResponse, where it is
//     described as what it is.
//
// Dead-letter topics are never granted to a subscriber, so no '<topic>.dlt' can
// appear in AuthorizedTopics.
//
// Being response-only, no field carries a binding tag.
type KafkaCredentialsResponse struct {
	// Brokers is the SUBSCRIBER-FACING bootstrap broker list: the externally
	// advertised addresses this subscriber connects to, from
	// KAFKA_SUBSCRIBER_BROKERS.
	//
	// It is NOT the address list Blnk itself dials. Those are internal —
	// "kafka:9092" on a compose network, a ClusterIP Service name in Kubernetes
	// — and they do not resolve outside the deployment; a broker also answers
	// each client with the advertised address of the listener it arrived on, so
	// an external subscriber must be given an externally advertised address.
	// Issuance is refused with 503 when none is configured, so a successful
	// response never carries an endpoint the subscriber cannot dial.
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

	// EnforcedAccess declares which parts of the subscriber's access model the
	// broker enforces for this credential. It is the field a consumer is
	// configured from: the topic set is exact and the group namespace is
	// reserved, and no key-based filtering is applied by the broker to either.
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
}

// CreateWebhookSubscription is the request body for
// POST /subscribers/:subscriber_id/webhook-subscription.
//
// The route exists because the requirement to migrate existing subscribers off
// the webhook subscription REST API meets a repository in which no such API
// exists: the entire subscription surface today is one global webhook URL in the
// configuration, with no per-subscriber storage and no registration endpoint.
// Recording a legacy URL per subscriber gives an existing subscriber somewhere
// to be recorded and migrated FROM, and it is what makes the sunset behaviour
// observable at all, since without a subscription route there is no request on
// which a 410 could ever be seen.
//
// This is not the /hooks surface. Those are the PRE_TRANSACTION and
// POST_TRANSACTION request-time callouts, they carry a response contract that
// can influence transaction processing, they remain fully functional, and they
// have nothing to do with these types.
//
// Deprecated: the legacy webhook-subscription surface exists only for the
// 30-day dual-delivery window during which Kafka publishing and HTTP webhook
// delivery run side by side from the same outbox events. Once
// WEBHOOK_DEPRECATION_SUNSET_DATE has passed, every request to these routes is
// answered with 410 Gone by the sunset guard. Use the Kafka event stream and the
// subscriber credential endpoint instead; see docs/webhook-to-kafka-migration.md.
//
// WHAT THIS SURFACE DOES NOT DO, stated plainly because the name invites the
// opposite reading: recording a URL here does NOT cause anything to be delivered
// to it. Blnk has one webhook destination and it is deployment-wide —
// Notification.Webhook.Url — and the relay's legacy leg posts every event of the
// dual-delivery window to that one endpoint. The column is a MIGRATION RECORD:
// it is where a subscriber's existing endpoint is written down so the move to
// Kafka can be tracked and so the sunset has a route on which a 410 is
// observable. Per-subscriber HTTP fan-out is deliberately not built, because
// building new delivery into the transport being retired is the opposite of
// retiring it.
type CreateWebhookSubscription struct {
	// WebhookURL is the legacy HTTP endpoint to record for this subscriber.
	// Required: a record whose only field is absent records nothing. It is the
	// subscriber's own endpoint, written down for migration tracking — see the
	// type comment for why nothing is delivered to it.
	//
	// Must be HTTPS and must not address an internal destination — see Validate.
	WebhookURL string `json:"webhook_url" binding:"required"`
}

// Validate applies the destination policy to the recorded URL (SSRF-01).
//
// This route is the ONLY one whose entire purpose is to accept a URL, which makes
// it the most likely way an internal address reaches the column. The rules and the
// reasoning are in validateLegacyWebhookURL; the same policy is applied by the
// subscriber DTOs and again at the persistence boundary, so no path stores a URL
// that another would have refused.
//
// Returns:
//   - error: describing the violation, nil when the URL is acceptable.
func (c CreateWebhookSubscription) Validate() error {
	return validateLegacyWebhookURL(c.WebhookURL)
}

// UpdateWebhookSubscription is the request body for
// PUT /subscribers/:subscriber_id/webhook-subscription. It follows the
// Create/Update split this package uses, and carries the same single mutable
// field, so correcting a recorded URL does not have to go through a delete and
// re-create.
//
// Deprecated: the legacy webhook-subscription surface exists only for the
// 30-day dual-delivery window during which Kafka publishing and HTTP webhook
// delivery run side by side from the same outbox events. Once
// WEBHOOK_DEPRECATION_SUNSET_DATE has passed, every request to these routes is
// answered with 410 Gone by the sunset guard. Use the Kafka event stream and the
// subscriber credential endpoint instead; see docs/webhook-to-kafka-migration.md.
//
// WHAT THIS SURFACE DOES NOT DO, stated plainly because the name invites the
// opposite reading: recording a URL here does NOT cause anything to be delivered
// to it. Blnk has one webhook destination and it is deployment-wide —
// Notification.Webhook.Url — and the relay's legacy leg posts every event of the
// dual-delivery window to that one endpoint. The column is a MIGRATION RECORD:
// it is where a subscriber's existing endpoint is written down so the move to
// Kafka can be tracked and so the sunset has a route on which a 410 is
// observable. Per-subscriber HTTP fan-out is deliberately not built, because
// building new delivery into the transport being retired is the opposite of
// retiring it.
type UpdateWebhookSubscription struct {
	// WebhookURL is the replacement legacy HTTP endpoint. Required for the same
	// reason it is on create, and delivered to for the same reason: none.
	//
	// Must be HTTPS and must not address an internal destination — see Validate.
	WebhookURL string `json:"webhook_url" binding:"required"`
}

// Validate applies the destination policy to the replacement URL (SSRF-01).
//
// Update is checked at exactly the same standard as create, because a URL that
// arrives by an edit is stored in the same column and read by the same readers.
// A policy applied only on creation is a policy with an edit-shaped hole.
//
// Returns:
//   - error: describing the violation, nil when the URL is acceptable.
func (u UpdateWebhookSubscription) Validate() error {
	return validateLegacyWebhookURL(u.WebhookURL)
}

// WebhookSubscriptionResponse is the read shape for the legacy
// webhook-subscription routes, returned by the create, read and update calls.
// The delete route answers 204 with no body and so needs no shape.
//
// It carries no credential and no headers: the legacy transport's signing secret
// and configured headers are deployment-wide configuration, never per-subscriber
// data, and surfacing them here would turn a migration-tracking response into a
// secret-bearing one.
//
// Deprecated: the legacy webhook-subscription surface exists only for the
// 30-day dual-delivery window during which Kafka publishing and HTTP webhook
// delivery run side by side from the same outbox events. Once
// WEBHOOK_DEPRECATION_SUNSET_DATE has passed, every request to these routes is
// answered with 410 Gone by the sunset guard. Use the Kafka event stream and the
// subscriber credential endpoint instead; see docs/webhook-to-kafka-migration.md.
//
// WHAT THIS SURFACE DOES NOT DO, stated plainly because the name invites the
// opposite reading: recording a URL here does NOT cause anything to be delivered
// to it. Blnk has one webhook destination and it is deployment-wide —
// Notification.Webhook.Url — and the relay's legacy leg posts every event of the
// dual-delivery window to that one endpoint. The column is a MIGRATION RECORD:
// it is where a subscriber's existing endpoint is written down so the move to
// Kafka can be tracked and so the sunset has a route on which a 410 is
// observable. Per-subscriber HTTP fan-out is deliberately not built, because
// building new delivery into the transport being retired is the opposite of
// retiring it.
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
