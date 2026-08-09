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
// EVERY listing response wraps these in api.DeadLetterPageResponse — `{data,
// next_cursor, has_more, total_count?}` — and a client reads `.data[]`, never the
// body as an array. That is not optional or count-dependent: cursor paging replaced
// offset paging (PERF-P08), and the cursor has to be returned somewhere, so there is
// no shape in which the body is a bare array. This comment used to say the handler
// returned the slice directly and nested it only when a total was asked for, which
// described a response the code has not produced since.
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
	// PartitionKey below — not this field — is the message key, and this field's
	// documentation used to claim otherwise. The two coincide when a ledger is
	// present, because requirement R-6 partitions by ledger ID and the publish
	// path prefers it; they do not coincide on a LEDGER-LESS event, where the key
	// is present and this field is empty. An identity, a bulk batch and a system
	// error are all in that case, so reading this field as the key gives the wrong
	// answer for exactly the events whose routing is least obvious.
	//
	// It is retained beside PartitionKey rather than folded into it because the two
	// together are what make the routing decision legible: a non-empty ledger_id
	// means the key IS the ledger, and an empty one means the key came from the
	// row's stored partition key.
	//
	// Optional because not every event category carries a ledger, so it is
	// omitted rather than reported as an empty string.
	LedgerID string `json:"ledger_id,omitempty"`

	// PartitionKey is the Kafka message key this event was published under, and
	// therefore what pinned it to its partition.
	//
	// It is the EFFECTIVE key, resolved from the row exactly as the publish path resolves
	// it — the ledger id when the event has one, and the stored partition_key column
	// otherwise — because requirement R-6 partitions by ledger ID. It is not a key
	// recomputed from the payload or from today's rules: both inputs come from the stored
	// row, so this is the key the event was ACTUALLY written with rather than the key it
	// would be written with today.
	//
	// Reporting the stored column alone was wrong for the rows where it matters. The column
	// is the publisher's SECOND choice whenever a ledger is present, so on a row whose
	// partition key was derived before the ledger was known the two differ — and that row
	// is precisely the one an operator is investigating. ledger_id is on this response too,
	// so the stored column remains derivable: an event with a ledger_id was keyed on it.
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
//   - row model.DeadLetterInventoryEntry: the NARROW inventory projection. It carries the
//     body's size and not the body, so this function cannot leak a payload even in principle
//     (PERF-P06).
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
		//
		// The column alone is the SECOND choice for any row carrying a ledger, because
		// requirement R-6 partitions by ledger ID and the publisher prefers it. The two agree on
		// almost every row and diverge on exactly the ones an operator is investigating: a row
		// written before the ledger was threaded through, or one whose partition key was derived
		// from the payload first. Reporting the column there would name the key the event was NOT
		// routed by, and both fields are on this response so the stored value stays derivable
		// from ledger_id.
		//
		// It was also, before that, declared and documented while never being assigned: every
		// dead-letter projection omitted it (the tag is omitempty), so the answer read as "this
		// event had no key" rather than as a gap.
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
		// Measured in SQL by the projection, not from bytes held in memory here
		// (PERF-P06). The inventory query never reads the body, so this is the only
		// place its size can come from — and reading whole rows to compute a length
		// was what moved gibibytes to build a listing of kilobytes.
		PayloadBytes: row.PayloadBytes,
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

// ProducerAtomicityStats reports how much of the two pre-recorded intents is outstanding.
//
// # What an intent is, and why it is counted separately from an event
//
// Every event but two is written inside the database transaction that performs its mutation.
// The two exceptions could not be, for structural reasons: a monitor alert does not exist until
// its balance is committed, and a bulk batch's summary belongs to no single transaction. Each
// therefore has something ELSE written atomically — an intent — from which the event is later
// captured, also atomically:
//
//   - a balance-monitor handoff, written inside the balance's own transaction, meaning "these
//     monitors have not been judged yet";
//   - a bulk batch coordinator row, written before the batch begins, meaning "this batch has
//     not reported an outcome yet".
//
// An outstanding intent is therefore an event that is OWED. It is invisible to the per-status
// counts, because nothing has been captured for it, which is exactly why these are reported
// alongside them.
type ProducerAtomicityStats struct {
	// MonitorHandoffPending counts balance movements whose monitors are waiting to be
	// evaluated. A small non-zero number is normal — it is one poll interval of work.
	MonitorHandoffPending int64 `json:"monitor_handoff_pending"`

	// MonitorHandoffProcessing counts handoffs a processor currently holds.
	MonitorHandoffProcessing int64 `json:"monitor_handoff_processing"`

	// MonitorHandoffCompleted counts handoffs that have been evaluated. It includes the
	// overwhelmingly common case of "evaluated, nothing fired", which is why it is far
	// larger than the number of alerts ever published.
	MonitorHandoffCompleted int64 `json:"monitor_handoff_completed"`

	// MonitorHandoffFailed counts handoffs whose evaluation budget is spent.
	//
	// This is the number to alert on. Each one is a balance movement whose monitor
	// conditions were never judged, so any alert it should have produced does not exist
	// and never will without intervention — a materially different fact from an alert
	// that was judged and did not fire, and one nothing else in this response can
	// distinguish.
	MonitorHandoffFailed int64 `json:"monitor_handoff_failed"`

	// UnfinalizedBatches counts asynchronous bulk batches that began and never reported an
	// outcome, past a grace period so batches still legitimately running are excluded.
	//
	// This is the one window the coordinator cannot close: a process that dies before the
	// finalising transaction leaves its batch here. It is not silent loss — the member
	// transactions are durable and carry the batch id — but the batch-level summary was
	// never computed, and this count is what makes that state queryable instead of
	// requiring log archaeology.
	UnfinalizedBatches int64 `json:"unfinalized_batches"`

	// OldestUnfinalizedBatchAt is when the oldest outstanding batch began, omitted when
	// there are none. Age is what separates a large batch still running from one that was
	// abandoned, so the count alone is not actionable and this is.
	OldestUnfinalizedBatchAt *time.Time `json:"oldest_unfinalized_batch_at,omitempty"`
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

	// ProducerAtomicity reports the two PRE-RECORDED INTENTS that bring the last two
	// event families under requirement R-2, and specifically how much of each is
	// outstanding.
	//
	// It belongs in this response rather than in a separate endpoint because it answers
	// the same question: whether every event that should exist does. The per-status
	// counts above can only see events that were CAPTURED. Two families are captured
	// from an intent recorded earlier — a balance-monitor handoff and a bulk batch
	// coordinator — and an outstanding intent is an event that is owed and not yet in
	// the table at all. A reconciliation that read only the counts above would find
	// them consistent while alerts and batch summaries were still pending.
	//
	// It is a pointer with omitempty so a read failure omits the object rather than
	// reporting zeros. Zero is a meaningful and reassuring value here — nothing is
	// outstanding — and emitting it for "we could not tell" would be the one
	// misreading a zero-loss check cannot afford.
	ProducerAtomicity *ProducerAtomicityStats `json:"producer_atomicity,omitempty"`

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

	// MeasuredWindows is the per-partition offset window the zero-loss verdict was
	// computed against: [first_offset, end_offset) for every partition the broker
	// reported.
	//
	// It is reported because the verdict is a statement ABOUT these windows — each
	// outbox row's stored coordinate is checked for membership in the window of its
	// own partition — so without them a reader cannot tell what was covered. That
	// is not a hypothetical gap: the check this replaced compared whole-topic
	// cumulative end offsets against all-time retained rows, two populations with
	// no common window, and reported itself as conclusive anyway.
	//
	// A partition the broker could not report is ABSENT here rather than present
	// with zeroed bounds, because a zero-width window would misclassify every row
	// on it as beyond the log end. Its rows appear in the verdict's
	// unmeasured_events instead, and partitions_unavailable says how many.
	//
	// Nil, and so omitted, when no offsets were read.
	MeasuredWindows []MeasuredOffsetWindow `json:"measured_windows,omitempty"`

	// OffsetsMeasuredAt is when the broker-side reading was taken. It is a
	// different instant from GeneratedAt: the counts come from PostgreSQL and
	// the offsets from Kafka, in separate round trips, so under live traffic a
	// small difference between the two sides is expected rather than suspicious,
	// and its size is only interpretable against the gap between these two
	// timestamps. Nil, and so omitted, when no offsets were read.
	OffsetsMeasuredAt *time.Time `json:"offsets_measured_at,omitempty"`

	// WindowStart is the earliest instant the WINDOWED figures cover, and
	// WindowSeconds is its length.
	//
	// # Which figures are windowed, and which are not (PERF-P04, PERF-P05)
	//
	// Dispatched, the reconciliation and the topic offsets are measured from
	// WindowStart. Every other per-status count is exact and COMPLETE regardless of
	// the window — pending, processing, webhook_pending, failed and dead_lettered
	// are the populations an operator acts on, and any of them can legitimately be
	// older than any window, so bounding them would hide a row stuck for a week from
	// a one-day reading.
	//
	// The window exists because the two unbounded figures could not stay honest
	// without one. Over the whole history the dispatched count is a scan of a table
	// gaining 43.2 million rows a day, and the reconciliation compares an outbox that
	// forgets — the retention sweep deletes rows — against broker end offsets that
	// never do, so its tolerated surplus grows without bound until it can conceal any
	// amount of loss. Both sides are now measured from this instant.
	//
	// The window is chosen with ?window=, defaults to 24h and is capped at 168h.
	WindowStart   *time.Time `json:"window_start,omitempty"`
	WindowSeconds int64      `json:"window_seconds,omitempty"`

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

// MeasuredOffsetWindow is one partition's measured offset window, as reported to a
// caller of the statistics endpoint.
//
// It exists because the zero-loss verdict is a statement ABOUT these windows: each
// outbox row's stored coordinate is checked for membership in the window of its own
// partition. Reporting the verdict without the windows would leave a reader unable
// to tell which offsets were actually covered — which is precisely how an unbounded
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

	// Records is how many records the window holds: end minus first, never
	// negative. It is included so a reader does not have to subtract, and because
	// zero is a meaningful reading — an empty partition, or one every record of
	// which has aged out.
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
//   - []MeasuredOffsetWindow: one entry per interval, nil when nothing was measured
//     so the key is omitted rather than rendered as an empty array.
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

// OutboxReconciliationResult is the wire form of the daily zero-loss check that
// acceptance criterion V-2 is scored on: every outbox row claiming publication,
// placed inside the measured offset window of the partition its record is on.
//
// # It is a BOUNDED MAPPING, not a comparison of totals
//
// The check used to subtract two numbers: outbox rows claiming publication, against
// the broker's cumulative end offsets. Those describe different populations, and no
// arithmetic reconciles them — outbox pruning shrinks one side while the other only
// climbs, Kafka retention deletes records the end offset still counts, recreating a
// topic resets it to zero, and on a shared topic any other producer's traffic
// inflates it by an unknown amount. Worse, the surplus such a comparison tolerates
// is INDISTINGUISHABLE FROM COMPENSATED LOSS: ten redeliveries and ten lost events
// produce exactly the totals of a healthy pipeline.
//
// So the verdict is now per row. Each row either names a record inside the measured
// window of its own partition — CorroboratedEvents — or it is counted under the
// specific reason it could not be placed: it names no record, it names an unmeasured
// topic or partition, its record has aged out of retention, or its offset is AT OR
// ABOVE the log end. That last one is the only unambiguous signal in the whole
// check, and it means the partition was truncated or the topic recreated.
//
// # Which is why "conclusive" is a separate question from "no loss"
//
// A green result requires BOTH that no row names a vanished record AND that every
// row was placed. Conclusive false is not a failure — it is the honest statement
// that today's measurement cannot decide the matter, with Caveats naming why and
// with the per-reason counts saying how much is unaccounted for.
//
// It mirrors the service-layer verdict field for field so the API reports exactly
// what the runbook computes, rather than a second, quietly different arithmetic.
type OutboxReconciliationResult struct {
	// TerminalEvents is how many outbox rows claim to have been published:
	// dispatched and webhook_pending, whose Kafka leg completed, plus
	// dead-lettered, each counted exactly once. Rows in pending, processing or
	// replaying make no such claim and are excluded.
	//
	// It counts rows THE OUTBOX STILL RETAINS; see OldestTerminalAt.
	TerminalEvents int64 `json:"terminal_events"`

	// CorroboratedEvents is how many of those rows name a record INSIDE the
	// measured window of its partition — a record the broker can serve right now.
	//
	// It is the only population a green verdict consists of, and it is strictly
	// stronger than the "names a coordinate" count it replaced: a coordinate whose
	// record has aged out, or whose partition was not measured, is no longer
	// counted as corroboration, because nothing available today confirms it.
	CorroboratedEvents int64 `json:"corroborated_events"`

	// UnconfirmedEvents is how many rows claim a publication they cannot name a
	// record for, and it is the field that stops a surplus reading as health.
	UnconfirmedEvents int64 `json:"unconfirmed_events"`

	// UnmeasuredEvents is how many rows name a topic or partition this measurement
	// did not cover — a missing topic, an unavailable partition, or a partition
	// count that has since shrunk. Their records may well be there; this reading
	// neither confirmed nor ruled them out.
	UnmeasuredEvents int64 `json:"unmeasured_events"`

	// AgedOutEvents is how many rows name a record Kafka retention has already
	// deleted: written, evidenced by the stored offset, and no longer readable.
	//
	// It is reported as a count of SPECIFIC EVENTS rather than as a topic-wide
	// caveat, because the old topic-wide form fired whenever anything at all had
	// aged out of a shared topic — including records Blnk never wrote — and so was
	// permanently on in any long-lived deployment.
	AgedOutEvents int64 `json:"aged_out_events"`

	// BeyondEndEvents is how many rows name an offset AT OR ABOVE the end of their
	// partition's log.
	//
	// On an intact log this is impossible, because the broker assigned that offset
	// when it accepted the write. It means the partition was truncated or the topic
	// was deleted and recreated, so those records are gone — which is why this, and
	// not a shortfall in totals, is what sets LossDetected.
	BeyondEndEvents int64 `json:"beyond_end_events"`

	// DuplicatedRecords is how many corroborated rows share a coordinate with
	// another row. It should be zero always.
	//
	// One record is produced by one acknowledged write of one row, and a partial
	// unique index on the coordinate forbids two rows naming the same one, so a
	// non-zero value reports a BROKEN SCHEMA rather than tolerable duplication —
	// and it makes the verdict inconclusive, because two rows sharing one record's
	// corroboration is double counting.
	DuplicatedRecords int64 `json:"duplicated_records"`

	// MessagesWritten is how many records the broker has accepted across the
	// measured topics, from summed end offsets, and RecordsRetained how many of
	// those it still holds.
	//
	// NEITHER IS THE PROOF, and they are reported for exactly that reason: on a
	// shared topic they count every redelivery, every replay, every dead-letter
	// copy and every record any other producer ever wrote, so they can exceed the
	// number of Blnk events arbitrarily without meaning anything. Read them as
	// context beside the mapping, never as the verdict.
	MessagesWritten int64 `json:"messages_written"`
	RecordsRetained int64 `json:"records_retained"`

	// BlnkRecordShare is how many of the retained records this reconciliation attributed to
	// Blnk's own published events, so a topic shared with another producer can still be
	// reconciled: the surplus is measured against Blnk's share rather than the whole log.
	BlnkRecordShare int64 `json:"blnk_record_share"`

	// FIVE FIELDS WERE RETIRED FROM HERE, and this note is where each one's question is now
	// answered. They were purged_events, all_time_terminal_events, verified_records,
	// unverifiable_records and missing_records, and they belonged to a WHOLE-HISTORY
	// reconciliation that corrected for retention with a purge log and then checked each
	// claimed coordinate against the broker in Go.
	//
	// The comparison is now drawn INSIDE the per-partition windows the offsets were measured
	// in, and the audit classifies every claim in SQL over those same windows — which answers
	// both questions more directly than the two mechanisms it replaced:
	//
	//   * purged_events and all_time_terminal_events existed so that an outbox which forgets
	//     could be compared with offsets that never do. Inside a window there is nothing to
	//     correct for: rows retention has deleted are older than the window, so they are on
	//     neither side of it. `windowed` states whether that scoping was achieved, and it is
	//     the flag to read before believing `overhead`.
	//   * verified_records is corroborated_events, missing_records is beyond_end_events — the
	//     one signal that is unambiguous loss — and unverifiable_records split into the two
	//     causes it used to fuse: aged_out_events, where retention removed a record the stored
	//     offset still evidences, and unmeasured_events, where the measurement did not cover
	//     that topic or partition. Fusing them made a healthy cluster with any retention policy
	//     permanently inconclusive.
	//
	// They are removed rather than left at zero because a documented field that can only ever
	// be zero is worse than an absent one: a reconciliation script reading verified_records: 0
	// beside a green verdict concludes that nothing was verified.

	// Overhead is MessagesWritten minus AllTimeTerminalEvents: the redeliveries,
	// replays and dead-letter copies.
	//
	// It is computed from the ALL-TIME count rather than from the surviving rows,
	// which is what stops retention's deletions being silently absorbed into it.
	//
	// It is SIGNED and is never clamped. A negative value is one of the two loss
	// signals this reconciliation carries — MissingRecords is the other, and the
	// stronger — and clamping it at zero would erase exactly the finding the check
	// exists to produce. A healthy system's overhead is small and positive, not
	// zero.
	Overhead int64 `json:"overhead"`

	// LossDetected is true when the broker holds FEWER records than the outbox
	// has terminal rows — rows claiming a publication no record corresponds to.
	//
	// False does NOT mean "proven no loss". It means no loss is provable from the
	// coordinates that were checkable, which is only a meaningful statement when
	// Conclusive is true.
	LossDetected bool `json:"loss_detected"`

	// Conclusive reports whether the mapping accounted for EVERY retained claim:
	// false when any row was unconfirmed, unmeasured, aged out or beyond the log
	// end, when two rows shared a coordinate, when a measured topic was missing, or
	// when a partition did not report.
	//
	// It carries no omitempty, deliberately. This is the field a caller must branch
	// on BEFORE reporting a green reconciliation, and a key that vanished on the
	// inconclusive path would leave the most dangerous state looking like the
	// healthiest one.
	Conclusive bool `json:"conclusive"`

	// WindowStart is the instant BOTH sides of this comparison were measured from, and
	// Windowed reports whether they were measured over a common window at all.
	//
	// # Why a verdict has to say which population it compared (PERF-P05)
	//
	// Broker end offsets are cumulative and count records retention has already deleted;
	// the outbox forgets, because its retention sweep deletes terminal rows. So a
	// comparison drawn over "everything" compares two different populations, and its
	// surplus grows by however much the outbox has forgotten — until it can conceal any
	// amount of loss while still reading as a healthy overhead.
	//
	// Windowed false therefore means the verdict is DIAGNOSTIC ONLY, and Caveats says so
	// in words; it is never Conclusive. WindowStart is omitted when there was no window,
	// so its absence and Windowed false always agree.
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

	// OldestTerminalAt is the earliest publication instant among all retained
	// terminal rows. Events published before it have been pruned from the outbox
	// and are outside the reach of any verdict, so it is reported rather than left
	// implicit — a reconciliation over an aggressively pruned outbox would
	// otherwise look complete. Nil, and omitted, when no terminal rows exist.
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

	// Name is the human label the subscriber is triaged by. Required: it is the
	// one field the service cannot invent a meaningful value for, it is NOT NULL
	// in blnk.event_subscribers, and it is what an operator recognises a principal
	// by months later.
	//
	// The requirement is enforced by Validate and ValidateCreateSubscriber, NOT by
	// a binding:"required" tag. The tag refused a missing name in the BINDER, which
	// made the response GEN_MALFORMED_REQUEST — "this body could not be read" —
	// while the endpoint's contract, and every other rule this DTO applies,
	// answers GEN_VALIDATION_ERROR: "this body was read and one field is wrong".
	// A caller distinguishing a transport problem from a field problem was told the
	// wrong one, and a blank-but-present name reached validation by a different
	// path from an omitted one. Both now take the same path and answer the same
	// code.
	Name string `json:"name"`

	// AuthorizedTopics is the set of topics this subscriber may Read and
	// Describe, mapping to the authorized_topics TEXT[] column and to the exact
	// set of ACL bindings provisioned for the principal. Omitted or empty means
	// no grant, which is the safe default rather than a missing value.
	//
	// Every entry must be a Blnk-owned category topic; Validate enforces that
	// against model.SubscriberGrantableTopics.
	//
	// The binding tags cap the two dimensions a tag can express — how many
	// topics, and how long each may be — and they do it in the BINDER, before
	// any handler code runs and whether or not Validate is called at all. That
	// is what keeps the dimensions bounding allocation from depending on a
	// handler remembering to validate.
	AuthorizedTopics []string `json:"authorized_topics" binding:"max=16,dive,max=249"`

	// PartitionKeyPrefix names the THIRD SCOPE of the access model: the subscriber is
	// entitled only to records whose message key carries this prefix. Because every
	// Blnk event is keyed by ledger id, that is a ledger boundary.
	//
	// KAFKA DOES NOT ENFORCE IT, and the credential response says so in the same
	// object that carries it — see SubscriberEnforcedAccess.PartitionKeyPrefixEnforced,
	// which is always false. The subscriber's own consumer keeps this boundary; nothing
	// upstream of the consumer discards a record whose key falls outside it.
	//
	// It is ACCEPTED rather than refused, and that is a deliberate reversal. Issuance
	// used to answer 409 for any row carrying a prefix, which implemented no part of
	// the scope — it withheld the CREDENTIAL instead, so a key-scoped subscriber could
	// not consume at all. Recording the boundary and stating who keeps it delivers as
	// much of the requirement as exists without per-tenant topics, which the access
	// model forbids.
	//
	// What is still refused is a value that cannot be held honestly: surrounding
	// whitespace, an over-long value, or a control character — with
	// GEN_VALIDATION_ERROR, because those are malformed rather than unenforceable.
	//
	// There is no ACL that narrows a principal to a key range — Kafka's authorizer
	// has no message-key dimension — and per-tenant topics, the only Kafka-native
	// alternative, are deliberately not created. To restrict what a subscriber can
	// read, narrow AuthorizedTopics, which is enforced at the broker. A subscriber
	// that must not see another's records must not share a topic with it.
	//
	// Omit it, or send an empty string, to register normally.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// NO webhook_url FIELD, and its absence is the sunset being enforceable.
	//
	// This DTO used to accept one. The four deprecated webhook-subscription routes are
	// each fronted by middleware.WebhookSunsetGuard and answer 410 Gone after the
	// retirement instant — but this route is not deprecated and is not guarded, so a
	// caller could keep writing legacy webhook state through it indefinitely after the
	// surface that owns it had been retired. The sunset was bypassable by using a
	// different route, which is the same as not having one.
	//
	// Legacy webhook state is therefore written ONLY through the guarded routes: POST
	// and PUT /subscribers/:subscriber_id/webhook-subscription. A subscriber that needs
	// one recorded is registered here first and has its URL recorded there, which is one
	// extra call on a path that exists only for a 30-day migration and is being removed.
}

// errSubscriberNameRequired is the single refusal for a missing subscriber name.
//
// One value, so the prefix-aware Validate and the prefix-free
// ValidateCreateSubscriber cannot answer the same broken rule with two different
// messages — and so a caller matching on it matches one string.
var errSubscriberNameRequired = errors.New("name is required")

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
// ValidateCreateSubscriber and ValidateUpdateSubscriber WERE RETIRED from this file.
//
// They were the "prefix-independent half" of the two Validate methods below, and their stated
// reason for existing was a caller that has no configuration snapshot — a binder hook, a CLI, a
// table-driven test. No such caller was ever written: every handler calls Validate(topicPrefix),
// and no test called them either.
//
// They are removed rather than left for a future caller because they were STRICTLY WEAKER in a
// way that would not announce itself. Both applied the grantable allowlist under
// DefaultEventTopicPrefix instead of the deployment's own prefix, so on any deployment with a
// custom KAFKA_TOPIC_PREFIX they would have accepted a grant naming topics that do not exist
// here and refused the ones that do. A future caller reaching for the shorter name would have
// got that silently. Validate is the only correct entry point, so it is the only one offered.

func (c CreateSubscriber) Validate(topicPrefix string) error {
	// The prefix-independent rules run FIRST and are shared with nothing else, which is
	// what makes this the single validation entry point. They were once a second exported
	// method that no handler called — two functions checking overlapping rules, one of them
	// dead, which is how the two drift apart until the authoritative one is the weaker.
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

	if err := validateGrantableTopics(c.AuthorizedTopics, topicPrefix); err != nil {
		return err
	}

	return validateSubscriberKeyScope(c.PartitionKeyPrefix)
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
	// Name replaces the human label when present. Bounded in the binder for the
	// same reason the create request's is; see validateSubscriberName.
	Name *string `json:"name,omitempty" binding:"omitempty,max=1024"`

	// AuthorizedTopics replaces the whole authorised set when present. It is
	// already nilable as a slice, so no pointer is needed to tell "omitted"
	// from "set to empty": nil is omitted, and a present empty array revokes
	// every topic grant.
	//
	// Every entry must be a Blnk-owned category topic; Validate enforces
	// that. Widening a grant here does not by itself widen
	// what the subscriber can read — the ACL bindings must be re-provisioned —
	// but the row is what the next provisioning reads, so it is checked at the
	// same standard as a fresh registration.
	AuthorizedTopics []string `json:"authorized_topics,omitempty" binding:"omitempty,max=16,dive,max=249"`

	// PartitionKeyPrefix RECORDS the key-scoped authorization when present and
	// non-empty, and CLEARS it when present and empty. See the type comment for why
	// this cannot be a plain string: absent and "clear it" are different requests.
	//
	// Both directions are supported. A prefix recorded on an already-provisioned
	// subscriber takes effect on the NEXT issuance — re-issue to hand the consumer
	// its new boundary, because the credential already in the field carries the old
	// one. Nothing is revoked by the edit itself.
	//
	// Refusal reads the value in THIS REQUEST, not the resulting row, so an edit
	// that never mentions the field is unaffected — including on a legacy row that
	// still carries a prefix, which an operator must be able to rename while
	// deciding what to do about it.
	PartitionKeyPrefix *string `json:"partition_key_prefix,omitempty"`

	// NO webhook_url FIELD. See the note on CreateSubscriber: this route is not
	// deprecated and not fronted by the sunset guard, so accepting legacy webhook state
	// here would let a caller keep writing it after the guarded routes had begun
	// answering 410 Gone. PUT /subscribers/:subscriber_id/webhook-subscription is the
	// one write path for it, and it is guarded.
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
	// The prefix-independent rules first, for the reason CreateSubscriber.Validate gives.
	if err := u.validateBodyRules(); err != nil {
		return err
	}

	if u.AuthorizedTopics != nil {
		if err := validateGrantableTopics(u.AuthorizedTopics, topicPrefix); err != nil {
			return err
		}
	}

	// PartitionKeyPrefix is checked at the SAME standard as create, and it used to be skipped
	// here with a note saying the service refused every non-blank value anyway. It no longer
	// does: the prefix is issued with the credential as a disclosed, consumer-side filtering
	// contract, so it is persisted, echoed in the credential response and written into log
	// fields — and this validator is the only thing standing between a caller and a control
	// character or a half-kilobyte value in all three. A rule applied only on creation is a rule
	// with an edit-shaped hole, and an edit is exactly how a hostile value arrives: creation is
	// scripted from a template, editing is done by hand.
	//
	// A present EMPTY value is not a violation. It is the documented remedy for a legacy row
	// that carries a prefix, and validateSubscriberKeyScope admits it for that reason.
	if u.PartitionKeyPrefix != nil {
		if err := validateSubscriberKeyScope(*u.PartitionKeyPrefix); err != nil {
			return err
		}
	}

	return nil
}

// MaxSubscriberNameLength bounds the one free-text, caller-supplied field on a subscriber.
//
// # Why a bound is needed at all
//
// Nothing bounded name, so the effective limit was the global 5 MiB request-body cap — and
// the value is STORED, returned in every registry response, and written into log fields on
// every issuance, revocation and provisioning failure. A multi-megabyte name is therefore not
// merely untidy: it is amplified by every read of the registry and by every log line that
// names the subscriber, so a single registration could inflate an unrelated response and a
// day of logs.
//
// # Why 256, and why it is enforced in three places
//
// 256 characters is generous for a human label in any script — it is not a description field
// — while staying far below anything that could matter for storage, response size or log
// volume. The DTO refuses it here so a malformed body is rejected where it is cheapest; the
// service refuses it so a CLI or migration cannot bypass the DTO; and
// event_subscribers_name_length_chk refuses it so a psql session, a data migration or a
// restored backup cannot either. Each layer covers callers the layer above does not see.
const MaxSubscriberNameLength = 256

// validateBodyRules applies the rules on a create body that need no topic prefix.
//
// # Why these are separated from the prefix-dependent ones rather than merged
//
// Validate is the single entry point, and it has two kinds of rule inside it. These are
// facts about the BODY — a name is present, it is bounded, the topic list is within its
// resource limits — and they hold identically in every deployment. The rest compare the
// grant against THIS deployment's topic namespace, which requires the configured prefix.
//
// Keeping the prefix-independent half here is what removes a real duplication. There used to
// be a second EXPORTED validator that no handler called, and it applied the grantable
// allowlist under model.DefaultEventTopicPrefix — the strictest namespace — as a stand-in
// for the configured one. Folding that call into Validate would have REFUSED a deployment
// that legitimately renamed its prefix, so the allowlist deliberately stays in Validate,
// where it is checked exactly once against the prefix that actually applies.
//
// Returns:
//   - error: describing the first violation, nil when the body's own rules hold.
func (c CreateSubscriber) validateBodyRules() error {
	if err := validateSubscriberName(c.Name, true); err != nil {
		return err
	}

	// The RESOURCE bounds on the grant — cardinality, blank elements, name length, the Kafka
	// character set and duplicates. They are prefix-independent by nature, and they are
	// applied here rather than left to the binding tags because the tags can express only
	// two of the five.
	return model.ValidateSubscriberTopics(c.AuthorizedTopics)
}

// validateBodyRules applies the rules on an update body that need no topic prefix.
//
// No field is required: an update carries the mutable subset a caller chose to change, and
// every field is a pointer or a nilable slice precisely so that "omitted" stays
// distinguishable from "set to empty".
//
// The name is checked only when PRESENT, and a present name may not be blank — an update is
// an instruction, and "set the name to nothing" is not one the registry can honour, because
// the column is NOT NULL and an unnamed principal cannot be triaged.
//
// The grant's resource bounds are likewise checked only when present: nil means the caller is
// not touching the authorised set, while a present empty array is a deliberate revocation of
// every topic and must continue to be accepted. When a grant IS present it replaces the whole
// set, so it is held to exactly the standard a fresh registration is — an update path that
// checked less would be the way around the create path's bounds.
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

// validateSubscriberName applies the one bound and the one requirement on a subscriber name.
//
// # What is checked, and what deliberately is not
//
// The name is a LABEL, not an authorization: nothing is granted on the strength of it, so the
// rules are about it being storable, displayable and bounded rather than about isolation.
//
//   - PRESENCE, when required. It is the one field no service can invent a meaningful value
//     for, it is NOT NULL in blnk.event_subscribers, and being able to answer "who is this
//     principal?" months later is most of the reason the registry exists.
//   - LENGTH, always. See MaxSubscriberNameLength for why an unbounded value is amplified
//     by every response and every log line.
//   - CONTROL CHARACTERS, always. The value is echoed into API responses, log lines and trace
//     attributes; a newline in it splits a log line in two and forges a second entry, and a
//     carriage return can overwrite one on a terminal. The service's log fields are
//     sanitized, but a bound at the boundary means the value never has to be trusted by a
//     path that forgets.
//
// The length is measured on the TRIMMED value, matching what the service stores and what
// event_subscribers_name_length_chk asserts, so trailing whitespace cannot be used to
// approach the bound in one layer and exceed it in another.
//
// Parameters:
//   - name string: the value as supplied.
//   - required bool: whether a blank value is a violation. False is unused today and exists
//     so an optional-name shape can reuse the bound rather than reimplementing it.
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
//   - A DEAD-LETTER topic. Every DLT carries other subscribers' failed events
//     together with Blnk's own failure metadata, so it has no subscriber
//     audience. This is the ONLY owned-name class that is refused: all four
//     category topics — including blnk.system — are grantable, because
//     model.SubscriberGrantableTopics composes only "<prefix>.<category>" names
//     and a ".dlt" name can therefore never be a member.
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
					"Blnk-owned category topics (%s). A dead-letter topic carries Blnk's own "+
					"failure metadata and every other subscriber's failed events, so it has no "+
					"subscriber audience",
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
// which is the only state a subscriber can be registered or provisioned in. The
// length bound is generous — a key prefix is a fragment of a ledger ID, not a
// document — and exists so that an unbounded string cannot be parked in a log line
// or an error message.
//
// IT VALIDATES SHAPE, NOT MEANING. A well-formed prefix is accepted and recorded;
// what this refuses is a value that cannot be held honestly — surrounding
// whitespace, which would make two prefixes filtering different record sets look
// alike, an unbounded value, which is amplified by every registry read and every
// log line naming the subscriber, and a control character, which corrupts every
// log line and dashboard label it reaches. The service applies the identical rules
// so a CLI caller, a migration and a fixture are held to the same standard.
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
	// THE ONE POLICY, in model.ValidateWebhookURL. This function used to carry its own copy of the
	// https, host, whitespace and internal-destination rules, including its own destination
	// classifier, beside a second copy in the repository — one column, two rules, with the same
	// rejected host producing two different reason phrases depending on which door the request came
	// through. What stays here is the ERROR SHAPE: a DTO validation error rather than a typed
	// apierror, and prefixed with the field name because that is what a caller of this endpoint
	// needs in order to know which body key to correct.
	//
	// An empty value passes, meaning "no endpoint recorded", which is what the nullable column is
	// for.
	message, reason := model.ValidateWebhookURL(rawURL)
	if message == "" {
		return nil
	}

	// The MESSAGE and the REASON together, because the reason names the offending host or scheme —
	// the caller's own value, and the one thing that tells them what to change. Neither echoes the
	// whole URL or the parser's rendering of it.
	return fmt.Errorf("webhook_url: %s (%s)", strings.ToLower(message[:1])+message[1:], reason)
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

	// SubscriberIDHash is the pseudonym this subscriber appears under in METRICS AND
	// LOGS, published here so that a token read off a dashboard, an alert notification
	// or a log line can be resolved back to the subscriber.
	//
	// # It is the pivot, and without it the pseudonyms are unusable
	//
	// blnk.kafka.consumer_lag carries a hashed 'subscriber' attribute, and every log
	// line about a subscriber carries subscriber_id_hash, both for the reason
	// subscriberLagLabel documents: a subscriber identifier is a tenant name, and a
	// metric label is the most widely readable thing the service emits. That protection
	// costs nothing only if the operator holding a token can still find the subscriber.
	// This field is how — GET /subscribers returns it on every row, and the list
	// endpoint accepts it as a filter so one request resolves a token instead of a walk
	// through every page.
	//
	// Computed rather than stored, by model.HashIdentifier, which is the single rule all
	// three layers use. docs/kafka-operations.md publishes the shell equivalent for an
	// operator who wants to hash a candidate identifier without calling the API.
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

	// PartitionKeyPrefix is the key-scoped authorization recorded for this
	// subscriber, and a non-empty value means NO CREDENTIAL CAN BE ISSUED for it.
	// Kafka authorises at topic and group granularity only, so a subscriber
	// authorised for a topic reads every record in it whatever this says, and
	// issuance refuses rather than hand out access the registry does not
	// describe. Omitted when no such authorization is recorded, which is the
	// provisionable state.
	//
	// Nothing this API accepts can put a value here: registration and update both
	// refuse a non-blank prefix. A value therefore identifies a LEGACY row — one
	// predating that refusal, or written outside this service — and it is reported
	// rather than hidden precisely so it can be found and cleared.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// EnforcedAccess declares which parts of this subscriber's access model the
	// broker actually enforces. It is always present, including when no
	// credential has been issued, because it describes the boundary issuance
	// WILL establish and an integrator needs it before wiring a consumer.
	EnforcedAccess SubscriberEnforcedAccess `json:"enforced_access"`

	// CredentialIssuanceBlocked reports that POST
	// /subscribers/{id}/kafka-credentials will REFUSE for this row as it
	// currently stands. Always present, including when false, because a client
	// that had to infer it from the absence of a field would infer it wrong.
	//
	// # Why this is a field rather than a documented consequence
	//
	// The refusal it predicts was previously discoverable only by triggering
	// it. A caller could register a subscriber carrying a partition key prefix,
	// receive 201 Created, wire a consumer around the returned principal and
	// consumer group, and learn at the credential call — a separate request,
	// possibly a separate day, possibly a separate person — that no credential
	// would ever be issued for it. Nothing in the created resource said so. The
	// state was reported accurately field by field and the CONSEQUENCE, which is
	// the only part anybody acts on, was in a Go doc comment.
	//
	// So the resource now states it at the moment it is created or changed. This
	// is deliberately a prediction about the NEXT call rather than a description
	// of this one: it is what turns a late 409 into a fact visible on the row.
	CredentialIssuanceBlocked bool `json:"credential_issuance_blocked"`

	// CredentialIssuanceBlockedReason names what to change, in the imperative,
	// and is present only when CredentialIssuanceBlocked is true.
	//
	// It carries the remedy rather than only the diagnosis, because the two
	// remedies are not interchangeable and an operator cannot pick between them
	// from the diagnosis alone: clearing the prefix ACCEPTS whole-topic access,
	// which is a decision somebody has now taken explicitly, while narrowing
	// authorized_topics is the enforceable form of the same intent whenever the
	// intended boundary maps onto topics.
	CredentialIssuanceBlockedReason string `json:"credential_issuance_blocked_reason,omitempty"`

	// CredentialFingerprint is a short, non-sensitive digest fragment
	// identifying which credential issuance this row records. It answers "is
	// this the same credential I saw last time?" and nothing else — it cannot
	// be authenticated with, and it is not the stored reference. Empty, and so
	// omitted, when no credential has been issued.
	CredentialFingerprint string `json:"credential_fingerprint,omitempty"`

	// CredentialIssuedAt is when the current credential was issued. Nil, and so
	// omitted, when none ever has been.
	CredentialIssuedAt *time.Time `json:"credential_issued_at,omitempty"`

	// NO webhook_url FIELD. The recorded URL is read through GET
	// /subscribers/:subscriber_id/webhook-subscription, which is fronted by the sunset
	// guard and stops disclosing it at the retirement instant. Echoing it here as well
	// would keep it readable through an unguarded route after that, so the two reads
	// would disagree about whether the legacy surface still exists.
	//
	// MigratedAt STAYS. It is migration PROGRESS rather than legacy webhook state — a
	// timestamp saying this subscriber finished moving to Kafka — and it is what a
	// migration-progress report counts, before and after the sunset alike. It discloses
	// no endpoint.

	// MigratedAt is when the subscriber completed its move to Kafka
	// consumption. Nil, and so omitted, means not yet migrated, which is what
	// migration-progress reporting counts.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`

	// ===================================================================
	// ORPHAN-01: the three unsettled-state markers, projected
	//
	// These were previously readable only from the database, which made every
	// documented remedy start with a psql session — and made the alerts that
	// fire on them unactionable through the API they tell an operator to use.
	// A marker an operator cannot see through the registry is a marker they
	// cannot triage, whatever a runbook says.
	//
	// All three are omitted when unset, which is the healthy state, so a
	// present field always means something needs attention. They are markers,
	// never a substitute for the broker: the broker is the authority on what a
	// principal can actually do.
	// ===================================================================

	// RevocationPendingAt is when a deregistration began taking this
	// subscriber's broker-side access away. Present means THE ROW IS NOT AN
	// ACTIVE SUBSCRIBER — credential issuance is refused for it — and a
	// principal that may still authenticate is awaiting revocation.
	//
	// The remedy is to RETRY THE DEREGISTRATION (DELETE this subscriber), which
	// is idempotent at the broker. Re-issuing is refused while this is set, so
	// it is not an alternative here.
	RevocationPendingAt *time.Time `json:"revocation_pending_at,omitempty"`

	// RevocationFailedAt is when the MOST RECENT revocation attempt was refused
	// by the broker, as distinct from a deregistration that merely began. It is
	// cleared at the start of every new attempt, so a present value means no
	// attempt has been made since the refusal.
	//
	// It always accompanies RevocationPendingAt, and it carries the fact that
	// marker cannot: RETRYING ALONE WILL NOT HELP. A cluster-authorization
	// failure means the administrative principal has lost its grants; a
	// transport error means the broker is unreachable. Fix that first.
	RevocationFailedAt *time.Time `json:"revocation_failed_at,omitempty"`

	// CredentialOrphanedAt is when an issuance left a credential at the broker
	// that Blnk could neither record nor revoke. Present means a principal can
	// authenticate while CredentialFingerprint above does not describe the
	// credential that works.
	//
	// THIS IS NOT RevocationPendingAt AND THE REMEDY IS THE OPPOSITE. The
	// subscriber is still ACTIVE, so RE-ISSUING settles it by construction —
	// Kafka stores one credential per principal, so a new issuance replaces the
	// orphan — and deprovisioning settles it by revoking. Choose by whether the
	// subscriber should still have access.
	CredentialOrphanedAt *time.Time `json:"credential_orphaned_at,omitempty"`

	// CreatedAt is when the subscriber was registered.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the registry row was last written.
	UpdatedAt time.Time `json:"updated_at"`

	// RevocationPending reports that broker-side revocation is still owed for this
	// subscriber: the row is tombstoned for deregistration and its principal may still be
	// able to authenticate until the revocation completes. Always present, including when
	// false, because a client that had to infer it from a missing field would infer it wrong.
	RevocationPending bool `json:"revocation_pending"`

	// RevocationPendingReason names what is outstanding and what to do about it.
	// Present only when RevocationPending is true.
	//
	// The state it describes is BROKER-SIDE access that has not been confirmed
	// gone, and only that: it is the state blnk_subscribers_revocation_pending
	// counts and SubscriberRevocationOutstanding fires on, so the action is to
	// retry the deregistration or revoke the principal at the broker by hand.
	//
	// The tombstone used to mean this OR "the broker is clean and only the row
	// deletion failed", which are opposite situations sharing one column — so an
	// operator answering the alert went after principals that no longer existed.
	// A confirmed revocation now clears the tombstone with the credential record,
	// which is why this reason can state one thing rather than branch. Where the
	// retry itself is what failed, RevocationFailedAt above carries the part this
	// reason cannot: that retrying alone will not help.
	//
	// It exists for the same reason CredentialIssuanceBlockedReason does:
	// docs/metrics.md answers the revocation alert with "find the affected
	// subscribers with GET /subscribers", and a response that reported the state
	// without naming the remedy sent every operator back to a psql session.
	RevocationPendingReason string `json:"revocation_pending_reason,omitempty"`
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
// # This field IS where the truth is stated, and that is the design
//
// A prefix is accepted, recorded and returned, so something has to say who keeps the boundary —
// and it has to be machine-readable, in the same object, because prose nobody reads is not a
// disclosure control. This field is that statement. It is never true: Kafka's authorizer has no
// message-key resource type, so a true here could only ever be a false claim, and the
// constructors below hard-code the false rather than accepting it as a parameter precisely so no
// caller can forge one.
//
// The alternative was refusing the prefix outright, which earlier builds did. That implemented no
// part of the scope — it withheld the credential instead — and it left this field as a disclaimer
// about data the API would no longer accept. Delivering the boundary beside the statement of who
// enforces it is strictly more useful and no less honest.
//
// This is not per-tenant topics. Blnk deliberately does not create a topic per subscriber;
// isolation is delivered by the exact topic grant plus the reserved consumer-group namespace,
// which is what these two dimensions describe. The consequence is worth stating plainly to
// whoever is designing on top of it: A SUBSCRIBER THAT MUST NOT SEE ANOTHER'S RECORDS MUST NOT
// SHARE A TOPIC WITH IT.
type SubscriberEnforcedAccess struct {
	// EnforcedBy names every dimension the broker evaluates, and is exhaustive.
	// partition_key_prefix is absent by construction, not by omission.
	EnforcedBy []string `json:"enforced_by"`

	// NotEnforcedBy names every access-shaped dimension the API accepts and the broker does NOT
	// evaluate, and it is exhaustive in the same way EnforcedBy is.
	//
	// It exists because the negative claim was previously only INFERABLE. A client had to notice
	// that "partition_key" was absent from EnforcedBy, and an absence proves nothing about a
	// contract — a dimension missing from a list reads identically to a dimension the server
	// forgot, or to one a newer version added. Stating it makes the two readings distinguishable
	// and gives a client something to assert on: EnforcedBy and NotEnforcedBy together enumerate
	// every dimension this API names, so a dimension moving from one list to the other is a
	// visible change rather than a silent one.
	//
	// It is populated unconditionally — with or without a prefix recorded — because it describes
	// the BROKER's capabilities, not this subscriber's configuration. Kafka's authorizer has no
	// message-key resource type whether or not anybody asked it to use one, and a field that
	// appeared only for subscribers carrying a prefix would let a reader conclude the dimension
	// is enforced for everyone else.
	NotEnforcedBy []string `json:"not_enforced_by"`

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
	//
	// It is false whenever a prefix is recorded, and — read together with
	// ClientSideKeyFilteringRequired below — it tells the consumer that IT is the component
	// applying the narrowing. With no prefix recorded there is nothing to narrow and the topic
	// grant is the whole boundary, which the broker does enforce.
	PartitionKeyPrefixEnforced bool `json:"partition_key_prefix_enforced"`

	// PartitionKeyPrefix echoes the routing hint recorded on the subscriber, or is empty when
	// none is recorded.
	//
	// It is stated HERE, beside PartitionKeyPrefixEnforced and inside the same object, and that
	// adjacency is the whole point: the value and the fact that the broker ignores it cannot be
	// read apart. The subscriber body reports the prefix at the top level too, where a reader
	// could take it for an access boundary; in here it is unmistakable.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// ClientSideKeyFilteringRequired is TRUE exactly when a prefix is recorded, and it is the
	// actionable instruction the rest of this object only implies.
	//
	// A consumer granted a topic reads EVERY record on that topic whatever its key. So a
	// subscriber that recorded a prefix and expects to see only its own records must discard the
	// rest itself, in its own consumer, and nothing at the broker will do it for them. Stating
	// that as a boolean rather than leaving it to be deduced from "prefix present AND not
	// enforced" is what makes it impossible to miss — and it is the field a client should branch
	// on when deciding whether to install a key filter.
	//
	// Issuance used to REFUSE for such a subscriber, permanently, rather than say this. That
	// withdrew a required capability for a state the registry is designed to hold; declaring the
	// obligation is the honest form of the same concern.
	ClientSideKeyFilteringRequired bool `json:"client_side_key_filtering_required"`

	// PartitionKeyPrefixEnforcedBy names WHERE the key scope is enforced, and it is the same
	// fact the two booleans above state, said as a place rather than as a pair of yes/no
	// answers: "consumer_side" when a prefix is recorded, "none" when none is.
	//
	// It is carried IN ADDITION to the booleans rather than instead of them because the two
	// readings serve different callers. A client deciding whether to install a key filter wants
	// the boolean it can branch on; a client or operator asking what the boundary IS wants the
	// place, and "enforced: false" answers that question with a denial rather than an answer —
	// which is how a field that could only ever be false came to be read as "unenforceable,
	// therefore refuse to issue". All three are derived from ONE value here, so they cannot
	// disagree.
	PartitionKeyPrefixEnforcedBy model.KeyScopeEnforcementStatus `json:"partition_key_prefix_enforced_by"`

	// ExclusiveGrantVerified reports that Blnk READ the principal's complete ACL grant at the
	// broker and found no ALLOW binding outside the set described above.
	//
	// # Why this field exists rather than being assumed
	//
	// Everything else in this struct describes what Blnk ASKED FOR. This describes what Blnk
	// OBSERVED, and the two used to be able to disagree without saying so: a principal
	// carrying a hand-made ALLOW binding — granting a topic outside Topics, or a wildcard, or
	// a cluster resource — was detected, logged at warning level, and then issued a credential
	// anyway. The response declared this exact boundary while the broker enforced a wider one,
	// and nothing an integrator could read said which they had.
	//
	// Issuance now REFUSES while any such binding exists, so on a successful response this is
	// necessarily true. It is stated anyway, for the same reason
	// PartitionKeyPrefixEnforced is: a client hard-coding an assumption about the boundary
	// should be reading it out of the response, and a future posture in which a broader grant
	// is tolerated must be visible here rather than silently changing what the other fields
	// mean.
	//
	// A DENY binding outside the set does NOT clear it. A DENY only subtracts from what the
	// ALLOW bindings grant, so it makes the effective access narrower than declared — which
	// cannot be an isolation failure.
	//
	// It is FALSE on a subscriber read. Reading a registry row makes no broker round trip, so
	// nothing about the broker's actual grant was observed, and reporting true there would be
	// the same unverified claim in a different response. Only credential issuance verifies.
	ExclusiveGrantVerified bool `json:"exclusive_grant_verified"`
}

// EnforcementDimensionPartitionKey names the access-shaped dimension Kafka does NOT evaluate.
//
// It is a constant rather than a literal for the same reason the enforced dimensions are: the
// value appears in a response body — it is the sole member of SubscriberEnforcedAccess.NotEnforcedBy
// — so it is part of the API contract, and a reworded literal would silently change what a client
// is matching on.
const EnforcementDimensionPartitionKey = "partition_key"

// SubscriberKeyScopeGuidance is the remedy every subscriber and credential response carries.
//
// One sentence, stated once, so the registry view and the credential view cannot give different
// advice about the same limitation.
const SubscriberKeyScopeGuidance = "Kafka authorises whole topics and consumer groups and has " +
	"no message-key dimension, so a partition-key prefix is never enforced: a granted topic is " +
	"readable in full. To confine a subscriber, narrow its authorized_topics, which the broker " +
	"does enforce, or isolate the data at the deployment boundary."

// keyScopeEnforcementFor renders a recorded prefix as the enforcement point it implies.
//
// One derivation, read by every constructor in this file, so the boolean pair and the place
// name can never describe different states of the same subscriber.
//
// Parameters:
//   - partitionKeyPrefix string: the recorded prefix, already trimmed.
//
// Returns:
//   - model.KeyScopeEnforcementStatus: ConsumerSide when a prefix is recorded, otherwise None.
func keyScopeEnforcementFor(partitionKeyPrefix string) model.KeyScopeEnforcementStatus {
	if strings.TrimSpace(partitionKeyPrefix) == "" {
		return model.KeyScopeEnforcementNone
	}

	return model.KeyScopeEnforcementConsumerSide
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
// THE KEY SCOPE IS DECLARED HERE TOO, and pairing it with its enforcement point is the whole
// reason this constructor takes it. A prefix reported on its own reads as a limit on the
// credential; reported beside "enforced_by: consumer_side" it reads as the filtering contract
// it is. Neither field is settable without the other, because this is the only place either is
// assigned.
//
// Parameters:
//   - subscriberID string: the business key the principal and group are derived from.
//   - topics []string: the subscriber's exact topic grant. A nil slice becomes [] so the body
//     never carries null for a set an operator has to read.
//   - partitionKeyPrefix string: the key boundary the consumer must apply, trimmed and echoed
//     VERBATIM. Empty stays empty and is the "no restriction" case; it is deliberately NOT
//     replaced by model.SubscriberKeyScopeAllKeys, because this field is what a consumer
//     filters on and a sentinel here would have it discard every record whose key does not
//     begin with the sentinel. The English form of "no restriction" belongs to
//     model.SubscriberCredential.KeyScope, which returns the scope and who enforces it as a
//     pair and cannot be read as a filter.
//
// Returns:
//   - SubscriberEnforcedAccess: the declaration, with PartitionKeyPrefixEnforced false and the
//     enforcement point reported as consumer_side or none.
func NewSubscriberEnforcedAccess(
	subscriberID string,
	topics []string,
	partitionKeyPrefix string,
) SubscriberEnforcedAccess {
	partitionKeyPrefix = strings.TrimSpace(partitionKeyPrefix)

	enforced := SubscriberEnforcedAccess{
		EnforcedBy: []string{
			EnforcementDimensionTopic,
			EnforcementDimensionConsumerGroup,
		},
		// The counterpart list, stated for every subscriber rather than only for one carrying a
		// prefix: it describes what the BROKER cannot evaluate, which does not vary by row.
		NotEnforcedBy: []string{
			EnforcementDimensionPartitionKey,
		},
		Topics: topics,
		// Never set true anywhere. Kafka has no message-key authorization dimension, so a
		// true here could only ever be a false claim.
		PartitionKeyPrefixEnforced: false,
		// THE RECORDED VALUE, trimmed, and empty when there is none. A sentinel was tried here
		// and removed: this string is what a consumer compares record keys against, so any
		// stand-in for "no restriction" is a filter that matches nothing, and a client applying
		// it would silently discard its entire stream. The flag below is what states whether
		// there is anything to apply at all.
		PartitionKeyPrefix: partitionKeyPrefix,
		// Derived from the prefix rather than passed in, so the flag and the value it describes
		// cannot disagree — a true beside an empty prefix, or a recorded prefix with no
		// instruction attached, would each be worse than either alone.
		ClientSideKeyFilteringRequired: partitionKeyPrefix != "",
		// The place-shaped form of the same derivation, so a caller reading the enforcement
		// point and a caller reading the flag are told the same thing.
		PartitionKeyPrefixEnforcedBy: keyScopeEnforcementFor(partitionKeyPrefix),
		// FALSE by default, and the default is the honest answer for every caller of this
		// constructor except issuance. This builds the REQUESTED boundary from a registry row,
		// which involves no broker round trip, so nothing here observed what the broker
		// actually grants. Only NewVerifiedSubscriberEnforcedAccess sets it, and only because
		// issuance really did read the grant and refuse a broader one.
		ExclusiveGrantVerified: false,
	}

	if namespace, err := model.CanonicalConsumerGroupNamespace(subscriberID); err == nil {
		enforced.ConsumerGroupNamespace = namespace
	}

	if enforced.Topics == nil {
		enforced.Topics = []string{}
	}

	return enforced
}

// NewVerifiedSubscriberEnforcedAccess builds the enforced-access declaration for a boundary
// Blnk has just READ AT THE BROKER and found exclusive.
//
// It is the credential-issuance form, and it exists as a separate constructor rather than as a
// boolean parameter so that the claim cannot be made by accident. Provisioning describes the
// principal's complete ACL grant and REFUSES to return a password while any ALLOW binding sits
// outside the declared set, so a response assembled here is one whose boundary was verified and
// not merely requested. Every other caller — every read of a registry row — must use
// NewSubscriberEnforcedAccess, which reports the claim as unverified because no broker round
// trip happened.
//
// Parameters:
//   - subscriberID string: the business key the principal and group are derived from.
//   - topics []string: the subscriber's exact topic grant.
//   - partitionKeyPrefix string: the routing hint the credential was issued under. Empty when
//     none is recorded.
//
// Returns:
//   - SubscriberEnforcedAccess: the declaration, with ExclusiveGrantVerified true.
func NewVerifiedSubscriberEnforcedAccess(
	subscriberID string,
	topics []string,
	partitionKeyPrefix string,
) SubscriberEnforcedAccess {
	enforced := NewSubscriberEnforcedAccess(subscriberID, topics, partitionKeyPrefix)
	enforced.ExclusiveGrantVerified = true

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
		SubscriberID: subscriber.SubscriberID,
		// The pivot back from a metric label or a log line. Projected here rather than by
		// the handler for the same reason the credential fingerprint is: a field that must
		// be constructed cannot be omitted by a handler that forgot it, and a subscriber
		// response missing it is a token nothing can resolve.
		SubscriberIDHash:   model.HashIdentifier(subscriber.SubscriberID),
		Name:               subscriber.Name,
		KafkaPrincipal:     subscriber.KafkaPrincipal,
		ConsumerGroupID:    subscriber.ConsumerGroupID,
		AuthorizedTopics:   subscriber.AuthorizedTopics,
		CredentialIssuedAt: subscriber.CredentialIssuedAt,
		MigratedAt:         subscriber.MigratedAt,
		// ORPHAN-01: the unsettled-state markers travel with the row. Copied
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
	response.EnforcedAccess = NewSubscriberEnforcedAccess(
		subscriber.SubscriberID, response.AuthorizedTopics, response.PartitionKeyPrefix,
	)

	// CredentialFingerprint, never the reference. model.CredentialFingerprint yields
	// "" for anything that is not a validly derived reference, so a value mis-stored
	// by some other path is dropped entirely instead of being echoed.
	if subscriber.CredentialReference != nil {
		response.CredentialFingerprint = model.CredentialFingerprint(*subscriber.CredentialReference)
	}

	response.CredentialIssuanceBlocked, response.CredentialIssuanceBlockedReason =
		credentialIssuanceBlock(subscriber)

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
// # Why the reason is separate from the marker
//
// The tombstone this reads is the SAME column CountSubscriberRevocationsPending counts and
// SubscriberRevocationOutstanding fires on, so the gauge, the alert and this response cannot
// disagree about which rows are outstanding. What the column alone cannot say is which of two
// opposite situations produced it. It once meant "the broker still honours this principal" OR
// "the broker is clean and only the registry deletion failed", and an operator answering the
// alert had no way to tell, so they went hunting for principals that no longer existed. A
// confirmed revocation now clears the tombstone along with the credential record, which is
// what lets this reason state ONE thing instead of branching — the debt it names is
// broker-side access that has not been confirmed gone.
//
// The instant is reported separately, in RevocationPendingAt, because the age is what the
// alert is stated over; this pair carries the fact and the action.
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

// credentialIssuanceBlock predicts whether POST /subscribers/{id}/kafka-credentials will refuse
// for this row as it stands, and names the remedy when it will.
//
// # Why the projection predicts a later refusal
//
// Both blocking states are properties of the ROW, not of the request, so they are knowable the
// moment the row is read — and an operator who learns them from a 409 on the credential call
// learns them at the least convenient moment, often in a script that has already reported
// success for the registration. Stating the prediction on the resource turns a late refusal into
// a fact visible on every read of it.
//
// # What blocks issuance, and what deliberately does not
//
// A DEREGISTRATION IN FLIGHT blocks it: issuing would re-arm a principal whose revocation is
// already owed, so the refusal is a safety property rather than a convenience.
//
// AN EMPTY TOPIC GRANT blocks it: a credential authorised for nothing is a live SCRAM principal
// with no purpose, and the remedy is to say what the subscriber may read.
//
// A RECORDED partition_key_prefix DOES NOT block it, and once did. The refusal implemented no
// part of the key scope — it withheld the credential instead — so the scope is now issued and the
// response states that the broker does not enforce it. See SubscriberEnforcedAccess, where
// partition_key_prefix_enforced and client_side_key_filtering_required carry that fact in
// machine-readable form. A prediction of a refusal that no longer happens would be worse than no
// prediction at all.
//
// Parameters:
//   - subscriber model.EventSubscriber: the row being projected.
//
// Returns:
//   - bool: true when the next credential call will refuse.
//   - string: the remedy in the imperative, empty when nothing is blocked.
func credentialIssuanceBlock(subscriber model.EventSubscriber) (bool, string) {
	if subscriber.IsRevocationPending() {
		return true, "This subscriber is being deregistered and its broker-side revocation is " +
			"still owed, so issuing a credential would re-arm a principal that is on its way out. " +
			"Complete or abandon the deregistration first."
	}

	if len(subscriber.AuthorizedTopics) == 0 {
		return true, "This subscriber is authorised for no topics, so any credential issued for " +
			"it would be a live Kafka principal that may read nothing. Set authorized_topics to " +
			"the category topics it is entitled to."
	}

	return false, ""
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
//   - The recorded partition-key prefix IS carried, inside
//     EnforcedAccess.PartitionKeyScope, and only there. That placement is the
//     whole design: it sits beside PartitionKeyPrefixEnforced, which is false, so
//     it cannot be read as a limit on the credential's reach. This response
//     previously withheld it altogether — which did not make the boundary
//     enforceable, it made it undeliverable, leaving a key-scoped subscriber with
//     no way to learn the scope it is expected to apply.
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

	// CredentialFingerprint is the short, non-sensitive digest fragment of the stored
	// credential reference — the SAME value GET /subscribers reports for this row once the
	// issuance is recorded.
	//
	// It is what makes an issuance identifiable after the fact. The password is returned
	// exactly once and nothing persists it, so without this field a client holding a
	// credential had no way to ask "is the credential I am using the one the registry
	// records?": it could read credential_fingerprint from a subscriber read and have
	// nothing to compare it against. The service computed it on every issuance and the
	// response dropped it.
	//
	// It is not a secret and cannot be authenticated with, and the reference cannot be
	// recovered from it — which is why it is safe to return beside the password and safe to
	// log, unlike either of them.
	CredentialFingerprint string `json:"credential_fingerprint"`

	// Replaced reports that this principal ALREADY held a SCRAM credential and this issuance
	// replaced it.
	//
	// Kafka stores one credential per principal, so a re-issue is destructive by
	// construction: the moment this returns true, any consumer still authenticating with the
	// previous password has stopped working. That is a fact the caller needs at the moment of
	// issuance — it is the difference between "provision a new subscriber" and "you have just
	// cut off a live consumer" — and the service established it from the broker's own
	// describe while the response said nothing.
	//
	// It is also the honest form of the orphan remedy: re-issuing is what settles a
	// credential Blnk could not account for, precisely because it replaces it, and this field
	// is the confirmation that the replacement happened.
	Replaced bool `json:"replaced"`
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
