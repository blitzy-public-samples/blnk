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
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// event_dlt.go is the dead-letter half of the Kafka event pipeline. Exactly three
// operations live here, and nothing else:
//
//  1. DEAD-LETTER PUBLICATION. When the relay has spent an event's retry budget, the
//     event is written to its `<topic>.dlt` sibling with failure metadata attached and
//     the outbox row is moved to its terminal dead-lettered state.
//  2. LISTING. The dead-letter inventory an operator triages from, read from the
//     OUTBOX TABLE rather than by consuming a topic.
//  3. REPLAY. Re-publishing a dead-lettered event to the topic it was originally
//     destined for, byte-for-byte identical to the message that was first produced.
//
// SCOPE BOUNDARY: subscriber-side dead-lettering is NOT Blnk's, and must not be built
// here. Read "dead letter" in this file as "Blnk's own dead-letter topics" and nothing
// wider. Blnk publishes the `<topic>.dlt` naming convention — documented in
// docs/event-streaming.md so a subscriber building its own consumer-side dead-lettering
// cannot collide with a Blnk-owned name — and stops there. So this file must never grow a
// consumer or consumer-group library, management of a dead-letter topic a subscriber owns,
// or a poison-message framework. Nothing here reads from Kafka at all: the listing and the
// replay both work from the outbox row, which carries dlt_topic and failure_metadata
// precisely so that no consumer is needed. Reaching for a kafka.Reader in this file is a
// sign the boundary has been misread.

// failureMetadataMember is the top-level JSON member the failure metadata is attached
// under, separator and colon included, ready to splice.
const failureMetadataMember = `,"failure_metadata":`

// failureMetadataKey is the bare member name, used when a stored dead-letter message has
// to be taken apart again by StripFailureMetadata.
const failureMetadataKey = `"failure_metadata"`

// unrecordedDeadLetterReason is the error reason stored when an event is dead-lettered
// with no failure cause available from either the caller or the row.
//
// An EMPTY reason is the failure mode this constant exists to prevent. FailureMetadata
// exists to answer "why did this event not get there", and an empty string answers
// nothing while looking like a successful read of a missing value.
const unrecordedDeadLetterReason = "the retry budget was exhausted; no failure reason was recorded"

// Dead-letter listing and gauge-scan bounds.
const (
	// defaultDeadLetterListLimit is the page size for a request that names none.
	defaultDeadLetterListLimit = 50

	// maxDeadLetterListLimit is the ceiling on a single page, so a triage endpoint
	// cannot be turned into a full-table scan.
	maxDeadLetterListLimit = 500

	// deadLetterScanMaxRows bounds how many rows the age scan will examine.
	deadLetterScanMaxRows = 5000
)

// replayAttemptOffset is added to an exhausted row's attempt count to label the
// publish-duration histogram for a REPLAY.
const replayAttemptOffset = 1

// DeadLetterRequest is the explicit form of a dead-letter publication: the row that has
// exhausted its retry budget, why it failed, and the attempt window it failed over.
//
// Only Row is required. Every other field has a documented fallback drawn from the row
// itself, so `DeadLetterRequest{Row: row, Cause: err}` is a complete request — which is
// exactly what the DeadLetter convenience method submits.
type DeadLetterRequest struct {
	// Row is the outbox row being dead-lettered. It must carry a database id, because a
	// dead-letter that cannot be recorded on its row is invisible to the listing and
	// unreachable by replay.
	Row model.EventOutbox

	// Cause is the failure from the final publish attempt. When nil, the row's
	// last_error is used, and failing that unrecordedDeadLetterReason.
	Cause error

	// Attempts is the number of publish attempts made. When it and the row's counter
	// disagree the LARGER is reported, because both are lower bounds on the truth: the
	// caller's count can be stale after a restart, and the row's count can be stale
	// relative to an in-progress claim.
	Attempts int

	// FirstAttemptedAt overrides the start of the failure window. Zero means "use the
	// row's first_attempted_at".
	FirstAttemptedAt time.Time

	// LastAttemptedAt overrides the end of the failure window. Zero means "use the row's
	// last_attempted_at".
	LastAttemptedAt time.Time
}

// DeadLetterOutcome is the record of one completed dead-letter publication. It exists
// because the caller — the relay — has to log what happened, and because the properties
// acceptance testing checks are properties of this value rather than of a returned
// error: which `.dlt` topic the event landed on, what metadata was attached, and what
// bytes were actually written.
type DeadLetterOutcome struct {
	// EventID is the event's UUID, unchanged. It is the subscriber idempotency key, so
	// dead-lettering and replaying an event must never alter it.
	EventID string

	// EventType is the event name, carried so a log line names the event without
	// re-reading the row.
	EventType string

	// OriginalTopic is the topic the event failed to reach, and the topic a replay sends
	// it back to. It is the attribution of the dead-letter counter, NOT the `.dlt` name,
	// so the counter is directly comparable with the published-events counter.
	OriginalTopic string

	// DeadLetterTopic is the `<topic>.dlt` sibling the event was written to, exactly as
	// resolved by DLTFor.
	DeadLetterTopic string

	// PartitionKey is the message key the dead-letter message was written with — the same
	// key the original publish used, so the dead-letter topic preserves the same
	// per-aggregate ordering the category topic has.
	PartitionKey string

	// Metadata is the failure metadata attached to the message and stored on the row.
	Metadata model.FailureMetadata

	// MetadataJSON is that metadata as it was serialised, which is the exact byte string
	// stored in the row's failure_metadata column and spliced into the message.
	MetadataJSON json.RawMessage

	// Message is the complete dead-letter message value that was composed: the original
	// event envelope bytes, unaltered, followed by the failure_metadata member. The
	// original envelope is a byte-exact PREFIX of this value — that is the invariant
	// byte-faithful replay rests on, and StripFailureMetadata recovers it.
	Message []byte

	// Published reports whether a broker ACKNOWLEDGED the dead-letter message.
	//
	// See PublishToDeadLetter for why that combination was unsafe.
	Published bool

	// Status is the pipeline-level outcome, always model.PublishStatusDeadLettered on a
	// successful return. It is reported through the same vocabulary the metrics layer is
	// attributed by, so a caller never has to translate.
	Status model.PublishStatus
}

// LogFields renders the outcome as logrus fields.
//
// THE PARTITION KEY IS HASHED. It is derived from a balance, transaction or identity
// identifier, so emitting it raw copies a financial identifier into the log stream —
// which is shipped off the host, retained on its own schedule and readable by a wider
// set of people than may query the ledger.
//
// Returns:
//   - logrus.Fields: a fresh map the caller may extend.
func (o DeadLetterOutcome) LogFields() logrus.Fields {
	return logrus.Fields{
		"event_id":           o.EventID,
		"event_type":         o.EventType,
		"topic":              o.OriginalTopic,
		"dlt_topic":          o.DeadLetterTopic,
		"partition_key_hash": hashLogIdentifier(o.PartitionKey),
		"attempt_count":      o.Metadata.AttemptCount,
		"failure_class":      classifyDeadLetterFailure(o.Metadata.ErrorReason),
		"error_reason":       redactLogValue(o.Metadata.ErrorReason, maxLoggedErrorLength),
		"published":          o.Published,
		"status":             string(o.Status),
		"message_bytes":      len(o.Message),
		"first_attempted":    o.Metadata.FirstAttemptedAt.Format(time.RFC3339Nano),
		"last_attempted_at":  o.Metadata.LastAttemptedAt.Format(time.RFC3339Nano),
	}
}

// The dead-letter failure vocabulary. It is FIXED and small, which is what makes it
// loggable: every value is bounded in length, contains no data from the failure itself,
// and can be grouped on in a log query or turned into a metric label without unbounded
// cardinality.
const (
	// deadLetterFailureClassBroker is the common case: the broker was unreachable,
	// leaderless, under-replicated, or timed out. Look at the cluster.
	deadLetterFailureClassBroker = "broker_unavailable"

	// deadLetterFailureClassAuth is a credential or ACL rejection. Look at the
	// principal's grants, not at the cluster's health.
	deadLetterFailureClassAuth = "authorization_denied"

	// deadLetterFailureClassTooLarge is a message the broker or Blnk refused on size.
	// Look at the producer's payload; retrying will not help.
	deadLetterFailureClassTooLarge = "message_too_large"

	// deadLetterFailureClassSerialization is a payload that would not serialise, or a
	// topic that could not be resolved. A defect, not a transient condition.
	deadLetterFailureClassSerialization = "serialization"

	// deadLetterFailureClassClosed is a publish attempted through a closed transport,
	// which means the process was shutting down.
	deadLetterFailureClassClosed = "transport_closed"

	// deadLetterFailureClassNone is "no reason was recorded". Distinct from unclassified:
	// it means the row carried nothing, which is itself a defect worth seeing rather than
	// a reason this function failed to place.
	deadLetterFailureClassNone = "unrecorded"

	// deadLetterFailureClassOther is everything else. It exists so the vocabulary stays
	// closed; a rising count here is the signal that a class is missing.
	deadLetterFailureClassOther = "other"
)

// deadLetterFailureSignatures maps a lower-cased substring of a recorded reason onto
// its class, in priority order.
var deadLetterFailureSignatures = []struct {
	signature string
	class     string
}{
	{"sasl", deadLetterFailureClassAuth},
	{"authentication", deadLetterFailureClassAuth},
	{"authorization", deadLetterFailureClassAuth},
	{"unauthorized", deadLetterFailureClassAuth},
	{"not authorized", deadLetterFailureClassAuth},
	{"topic authorization failed", deadLetterFailureClassAuth},
	{"group authorization failed", deadLetterFailureClassAuth},
	{"cluster authorization failed", deadLetterFailureClassAuth},

	{"too large", deadLetterFailureClassTooLarge},
	{"message size", deadLetterFailureClassTooLarge},
	{"record too large", deadLetterFailureClassTooLarge},

	{"marshal", deadLetterFailureClassSerialization},
	{"unmarshal", deadLetterFailureClassSerialization},
	{"serial", deadLetterFailureClassSerialization},
	// encoding/json prefixes every one of its errors with "json: ", and a JSON error is by
	// definition a serialisation failure however it is worded. The prefix is a far more
	// reliable signal than any of its individual messages, none of which contains the word
	// "marshal" — "json: unsupported type: chan int" being the one that made this obvious.
	{"json:", deadLetterFailureClassSerialization},
	{"unsupported type", deadLetterFailureClassSerialization},
	{"invalid topic", deadLetterFailureClassSerialization},
	{"unknown topic", deadLetterFailureClassSerialization},

	{"publisher is closed", deadLetterFailureClassClosed},
	{"closed", deadLetterFailureClassClosed},

	{"connection refused", deadLetterFailureClassBroker},
	{"no such host", deadLetterFailureClassBroker},
	{"timeout", deadLetterFailureClassBroker},
	{"timed out", deadLetterFailureClassBroker},
	{"deadline exceeded", deadLetterFailureClassBroker},
	{"broken pipe", deadLetterFailureClassBroker},
	{"reset by peer", deadLetterFailureClassBroker},
	{"leader", deadLetterFailureClassBroker},
	{"not available", deadLetterFailureClassBroker},
	{"unavailable", deadLetterFailureClassBroker},
	{"broker", deadLetterFailureClassBroker},
	{"i/o", deadLetterFailureClassBroker},
	{"eof", deadLetterFailureClassBroker},
	{"network", deadLetterFailureClassBroker},
	{"dial", deadLetterFailureClassBroker},
	{"refused", deadLetterFailureClassBroker},
}

// errorText renders an error for classification, and NOTHING ELSE reads its result.
//
// Parameters:
//   - err error: the error to render. May be nil.
//
// Returns:
//   - string: the error's message, or "" when there is no error.
func errorText(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

// classifyDeadLetterFailure reduces a recorded failure reason to one value from the
// fixed vocabulary above.
//
// Parameters:
//   - reason string: the recorded failure reason. May be empty.
//
// Returns:
//   - string: one of the deadLetterFailureClass* constants.
func classifyDeadLetterFailure(reason string) string {
	if strings.TrimSpace(reason) == "" {
		return deadLetterFailureClassNone
	}

	lowered := strings.ToLower(reason)
	for _, candidate := range deadLetterFailureSignatures {
		if strings.Contains(lowered, candidate.signature) {
			return candidate.class
		}
	}

	return deadLetterFailureClassOther
}

// ReplayOutcome is the record of one replay: which event went back to which topic,
// under which key, and when.
//
// It carries the publisher's own PublishResult rather than flattening it, so the caller
// keeps the partition key, the duration and the attempt label without this file having
// to re-describe them. The API projection reads EventID, Topic, Status and ReplayedAt.
type ReplayOutcome struct {
	// EventID is the replayed event's UUID, UNCHANGED from the original. A replay that
	// minted a new id would be indistinguishable from a new event and would defeat the
	// subscriber-side duplicate suppression that id exists for.
	EventID string

	// EventType is the event name, unchanged.
	EventType string

	// Topic is the topic the event was replayed TO: its original category topic, never
	// the dead-letter topic it was listed from.
	Topic string

	// PartitionKey is the key the replay was published with — the same key as the original
	// publish, so the replay lands on the same partition and cannot itself violate
	// per-aggregate ordering.
	PartitionKey string

	// Status is the outcome, model.PublishStatusDispatched on a successful return.
	Status model.PublishStatus

	// ReplayedAt is the instant the broker acknowledgement was observed.
	ReplayedAt time.Time

	// Recorded reports whether the outbox row was successfully moved out of its
	// dead-lettered state. It is false only when the re-publish succeeded and the
	// bookkeeping update did not, which is the one case where a successful replay still
	// returns an error — see ReplayDeadLetteredEvent.
	Recorded bool

	// Result is the publisher's record of the re-publish attempt.
	Result PublishResult
}

// LogFields renders the replay outcome as logrus fields, reusing the publisher's field
// names for the publish itself so that a replay is searchable alongside ordinary
// publishes.
//
// Returns:
//   - logrus.Fields: a fresh map the caller may extend.
func (o ReplayOutcome) LogFields() logrus.Fields {
	fields := o.Result.LogFields()
	fields["replayed"] = true
	fields["replayed_at"] = o.ReplayedAt.Format(time.RFC3339Nano)
	fields["recorded"] = o.Recorded

	return fields
}

// DeadLetterListOptions is the query behind the dead-letter inventory: one page, with
// optional narrowing.
//
// The zero value is a valid request for the first default-sized page of everything. The
// filters are exact-match and optional, and every one of them is applied IN SQL by the
// repository.
type DeadLetterListOptions struct {
	// Limit is the maximum number of entries to return. Zero or negative selects
	// defaultDeadLetterListLimit; anything above maxDeadLetterListLimit is clamped to it.
	Limit int

	// Offset is how many matching entries to skip. Negative is clamped to zero.
	Offset int

	// Cursor resumes a keyset page: it names the (occurred_at, id) coordinate of the last entry
	// the previous page returned. Nil starts at the newest entry.
	Cursor *model.DeadLetterCursor

	// EventType narrows to one event name, for example "transaction.applied". Empty means
	// no narrowing. Surrounding whitespace is ignored.
	EventType string

	// Topic narrows to one ORIGINAL category topic, for example "blnk.transactions".
	// Filtering on the original topic rather than on the `.dlt` sibling is what makes
	// "show me the transaction events that are stuck" expressible without the caller
	// having to know the suffix convention.
	Topic string

	// Status narrows to one failure state. Only the two states the inventory contains are
	// accepted — model.EventOutboxStatusFailed and model.EventOutboxStatusDeadLettered —
	// and anything else is rejected as a validation error rather than silently matching
	// nothing, because a filter that quietly returns an empty page reads as "nothing is
	// stuck" and is exactly the wrong answer to give an operator.
	//
	// failed is the more urgent of the two and is filterable for that reason: the retry
	// budget is spent and, while dlt_topic is still NULL, the dead-letter copy has not
	// landed, so the event exists nowhere but the outbox row.
	Status string

	// OccurredFrom and OccurredTo bound the event's OCCURRENCE instant inclusively, and
	// either may be zero to leave that end unbounded.
	OccurredFrom time.Time
	OccurredTo   time.Time
}

// filtered reports whether any narrowing was requested.
//
// It is observability only. There is one query path now, filtered or not, because the
// narrowing is applied in SQL; this exists so a span can say whether a page was
// narrowed without restating the field list.
//
// Returns:
//   - bool: true when at least one filter is set.
func (o DeadLetterListOptions) filtered() bool {
	return strings.TrimSpace(o.EventType) != "" ||
		strings.TrimSpace(o.Topic) != "" ||
		strings.TrimSpace(o.Status) != "" ||
		!o.OccurredFrom.IsZero() ||
		!o.OccurredTo.IsZero()
}

// deadLetterQuery translates the service's options into the repository's narrowing
// contract.
//
// It must be called on ALREADY NORMALISED options.
//
// Returns:
// inventoryQuery is the same narrowing expressed as the KEYSET query the inventory listing is
// drawn with.
//
// Returns:
//   - model.DeadLetterInventoryQuery: the repository-facing keyset query.
func (o DeadLetterListOptions) inventoryQuery() model.DeadLetterInventoryQuery {
	return model.DeadLetterInventoryQuery{
		Limit:        o.Limit,
		EventType:    o.EventType,
		Topic:        o.Topic,
		Status:       o.Status,
		Cursor:       o.Cursor,
		OccurredFrom: o.OccurredFrom,
		OccurredTo:   o.OccurredTo,
	}
}

// - model.DeadLetterQuery: the same narrowing and page, in repository terms.
func (o DeadLetterListOptions) deadLetterQuery() model.DeadLetterQuery {
	return model.DeadLetterQuery{
		EventType:    o.EventType,
		Topic:        o.Topic,
		Status:       o.Status,
		OccurredFrom: o.OccurredFrom,
		OccurredTo:   o.OccurredTo,
		Limit:        o.Limit,
		Offset:       o.Offset,
	}
}

// DeadLetterAgeReport is the result of refreshing the dead-letter age gauge: how many
// entries are outstanding, and how old the oldest one on each dead-letter topic is.
//
// It is returned rather than merely recorded so that the value the gauge was set to is
// assertable in a test and readable by an operator through the same call, instead of
// only observable by scraping the metrics endpoint.
type DeadLetterAgeReport struct {
	// GeneratedAt is the instant the ages were computed against.
	GeneratedAt time.Time

	// Outstanding is the number of unresolved entries: rows in the dead-lettered state
	// plus rows whose retry budget is spent but which have not reached a dead-letter topic
	// yet. Both are counted because both are events an operator has to act on.
	Outstanding int64

	// OldestByTopic maps a dead-letter topic to the age of the OLDEST unresolved entry
	// destined for it. Every dead-letter topic Blnk owns is present, and a topic with
	// nothing outstanding maps to zero — the gauge must fall back to zero rather than hold
	// a stale age after the last entry is cleared.
	OldestByTopic map[string]time.Duration

	// FailedAwaitingDeadLetter is the subset of Outstanding whose retry budget is spent
	// but which has NOT reached a dead-letter topic yet.
	//
	// It is broken out because the two populations need different responses. A
	// dead-lettered event is on a topic an operator can list and replay; an event in this
	// state is on no topic at all, because the dead-letter WRITE itself failed.
	//
	// It counts the one pre-dead-letter literal: failed, which the exhaustion arm sets and
	// in which the hand-off stays re-claimable for as long as dlt_topic is NULL. A
	// steadily non-zero value means dead-letter writes are failing, not that events are
	// momentarily in flight.
	FailedAwaitingDeadLetter int64

	// Scanned is how many rows were examined.
	Scanned int

	// Truncated reports that Outstanding exceeded the scan bound, so the ages are drawn
	// from the OLDEST deadLetterScanMaxRows entries rather than from all of them. The walk
	// starts at the oldest end precisely so that a truncated scan still reports the oldest
	// entry it can see, making the reported age a lower bound that only ever understates
	// by rows even older than the ones examined.
	Truncated bool
}

// OldestAge returns the greatest age across every topic, which is the single number the
// 15-minute dead-letter alert is expressed against.
//
// Returns:
//   - time.Duration: the oldest outstanding entry's age, or zero when nothing is
//     outstanding.
func (r DeadLetterAgeReport) OldestAge() time.Duration {
	var oldest time.Duration
	for _, age := range r.OldestByTopic {
		if age > oldest {
			oldest = age
		}
	}

	return oldest
}

// eventDeadLetterStore is the repository surface the dead-letter operations need, and
// deliberately no more of it.
type eventDeadLetterStore interface {
	// GetEventByID fetches one row by its business event_id, returning a typed
	// not-found error when nothing matches.
	GetEventByID(ctx context.Context, eventID string) (*model.EventOutbox, error)

	// ListDeadLetteredEvents pages the inventory, newest occurrence first, over both
	// terminal failure states, applying every narrowing the query expresses IN SQL. The
	// zero-valued query is the whole inventory at the repository's default page size,
	// which is what lets the age scan and the operator listing share one method.
	ListDeadLetteredEvents(ctx context.Context, query model.DeadLetterQuery) ([]model.EventOutbox, error)

	// ListDeadLetterInventory pages the inventory as the narrow triage projection,
	// resuming from a keyset cursor. It is what the HTTP listing reads; the full-row
	// listing above is for the replay path, which needs the stored bytes.
	ListDeadLetterInventory(
		ctx context.Context,
		query model.DeadLetterInventoryQuery,
	) (model.DeadLetterInventoryPage, error)

	// CountDeadLetteredEvents counts what the SAME narrowing matches, ignoring the page.
	// It is the exact total behind include_count, and it must be driven from the same
	// predicate as the listing or the total describes a different set than the page.
	CountDeadLetteredEvents(ctx context.Context, query model.DeadLetterQuery) (int64, error)

	// CountDeadLetterInventory counts what a listing query matches, sharing the INVENTORY
	// listing's predicate so a paging caller can be told the true size of its backlog.
	CountDeadLetterInventory(ctx context.Context, query model.DeadLetterQuery) (int64, error)

	// ListAndCountDeadLetterInventory answers the page and its total from ONE SNAPSHOT,
	// and is what a listing that asked for a total reads.
	//
	// Sharing a predicate was never sufficient on its own. Two statements on two
	// connections observe two populations, so an entry dead-lettered between them is
	// counted by one and absent from the other and the total then describes a set the page
	// is not a slice of — which on a triage endpoint reads as a different amount of stuck
	// work than there is.
	ListAndCountDeadLetterInventory(
		ctx context.Context,
		query model.DeadLetterInventoryQuery,
	) (model.DeadLetterInventoryPage, int64, error)

	// CountUnresolvedEventOutbox returns a status-keyed count of every NON-DISPATCHED row,
	// which is how the age report counts the rows awaiting a dead-letter write without a
	// bespoke query.
	CountUnresolvedEventOutbox(ctx context.Context) (map[string]int64, error)

	// MarkEventDeadLettered records the dead-letter topic and metadata and moves the row
	// to its dead-lettered terminal state, CONDITIONAL on the caller still holding the
	// row's claim token. It returns a conflict when the claim has been lost, which is what
	// stops two workers each writing the event to the dead-letter topic.
	MarkEventDeadLettered(ctx context.Context, id int64, claimToken, dltTopic string, failureMetadata json.RawMessage, record model.BrokerRecord) error

	// MarkEventDispatched is the post-replay transition: a successfully replayed row
	// becomes dispatched, which removes it from the dead-letter inventory and makes a
	// second replay attempt fail closed. It too is conditional on the claim token — here,
	// the one ClaimEventForReplay issued.
	MarkEventDispatched(ctx context.Context, id int64, claimToken string, record model.BrokerRecord) error

	// ClaimEventForReplay atomically moves a dead-lettered row to replaying and returns it
	// with a fresh claim token, so a replay is a CLAIM rather than a read followed by a
	// check.
	//
	// This is what makes concurrent replay safe. The read-then-check form let two requests
	// for one event both see a dead_lettered row, both pass the precondition, and both
	// publish — an operator clicking twice, or two operators working the same backlog,
	// putting two copies on the topic.
	ClaimEventForReplay(ctx context.Context, eventID string, lockDuration time.Duration) (*model.EventOutbox, error)

	// ReleaseEventReplay returns a replaying row to dead_lettered, recording the reason
	// when one is given. It is the rollback that keeps a FAILED replay replayable: without
	// it the row would be stranded in replaying, outside both the relay's claimable set
	// and the dead-letter inventory, with nothing left to pick it up.
	ReleaseEventReplay(ctx context.Context, id int64, claimToken, replayErr string) error
	// OldestDeadLetterAgeByTopic reports the oldest outstanding entry per dead-letter
	// topic as one grouped aggregate, which is what the age gauge is computed from.
	OldestDeadLetterAgeByTopic(ctx context.Context, deadLetterSuffix string) ([]model.DeadLetterTopicAge, error)
}

// deadLetterMessageWriter is the minimum Kafka surface a dead-letter write needs.
type deadLetterMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// deadLetterWriterResolver resolves the writer for a dead-letter topic.
type deadLetterWriterResolver func(topic string) (deadLetterMessageWriter, error)

// Compile-time proofs that the two seams are faithful subsets of the real types. Both
// fail the build here, on the lines that state the contract, rather than at a call site.
var (
	_ eventDeadLetterStore    = (database.IDataSource)(nil)
	_ deadLetterMessageWriter = (*kafka.Writer)(nil)
)

// EventDeadLetterService owns dead-letter publication, listing and replay.
//
// One instance is enough for a process and it is safe for concurrent use: the store and
// the resolver are read-only after construction, and the lazily-resolved publisher is
// guarded by a mutex. The relay constructs it ONCE with its shared publisher, which is
// what keeps the hot path free of per-call writer and connection setup.
type EventDeadLetterService struct {
	// store is the repository. It may be nil, which every operation reports as a clear
	// error rather than a nil dereference, because NewBlnk(nil) is a supported
	// construction in this codebase and a service built from such an instance must fail
	// legibly.
	store eventDeadLetterStore

	// mu guards publisher, resolveWriter and ownsPublisher, which are assigned together
	// on first use when the publisher was not injected.
	mu sync.Mutex

	// publisher is the transport for replays and the source of dead-letter writers. Nil
	// until resolved.
	publisher TopicEventPublisher

	// resolveWriter resolves a dead-letter topic to a writer. Assigned alongside
	// publisher, and overridable in tests to exercise the composition and routing rules
	// without a broker.
	resolveWriter deadLetterWriterResolver

	// ownsPublisher records that this service built the publisher itself, and is therefore
	// the only thing allowed to close it. A publisher passed in by the relay outlives this
	// service and must not be closed by it.
	ownsPublisher bool

	// now is the clock. It is always set by the constructor, and an in-package test may
	// replace it directly so that reported ages and failure windows are exact rather than
	// approximately now.
	now func() time.Time

	// scanMaxRows bounds the age scan, and ONLY the age scan. The listing does not walk
	// the inventory: it asks the repository for the page the caller requested with the
	// filters applied in SQL, so no bound of this kind can hide a match from it.
	scanMaxRows int
}

// NewEventDeadLetterService builds the dead-letter service.
//
// Pass the publisher the process already has — the relay's — so that dead-letter writes
// and replays reuse its writers and connections. Pass nil to have one resolved from
// live configuration on first use and released by Close, which is the right choice for
// a short-lived, operator-triggered operation and the wrong one inside a loop.
//
// Parameters:
//   - store eventDeadLetterStore: the repository. database.IDataSource satisfies it.
//   - publisher EventPublisher: the process publisher, or nil to resolve one lazily.
//
// Returns:
//   - *EventDeadLetterService: a ready service.
func NewEventDeadLetterService(store eventDeadLetterStore, publisher EventPublisher) *EventDeadLetterService {
	service := &EventDeadLetterService{
		store:       store,
		now:         time.Now,
		scanMaxRows: deadLetterScanMaxRows,
	}

	// A publisher is adopted only if it can be given a destination topic. Both
	// implementations in event_publisher.go satisfy TopicEventPublisher, so this narrowing
	// only rejects something that could not have served a dead-letter write anyway — and
	// rejecting it here means the lazy path builds a usable one instead. A nil publisher
	// fails the assertion, which is what routes it to the lazy path.
	if topicPublisher, ok := publisher.(TopicEventPublisher); ok {
		service.withTransport(topicPublisher, publisherWriterResolver(topicPublisher))
	}

	return service
}

// WithScanLimit sets how many rows the AGE SCAN may examine.
//
// It follows the fluent configurator convention the outbox processors already use. A
// non-positive value restores the default rather than disabling the bound, because an
// unbounded scan of an arbitrarily large inventory is never the intent — an operator
// raising this is asking for a bigger window, not for no window.
//
// Parameters:
//   - rows int: the maximum number of rows to examine.
//
// Returns:
//   - *EventDeadLetterService: the service, for chaining.
func (s *EventDeadLetterService) WithScanLimit(rows int) *EventDeadLetterService {
	if rows <= 0 {
		rows = deadLetterScanMaxRows
	}
	s.scanMaxRows = rows

	return s
}

// withTransport installs the publisher replays go through and the resolver dead-letter
// writes are taken from, as ONE assignment.
//
// ownsPublisher is cleared because an installed publisher belongs to whoever supplied
// it: the relay's publisher outlives this service and must not be closed by it.
//
// Parameters:
//   - publisher TopicEventPublisher: the publisher replays go through.
//   - resolver deadLetterWriterResolver: the writer resolution.
//
// Returns:
//   - *EventDeadLetterService: the service, for chaining.
func (s *EventDeadLetterService) withTransport(
	publisher TopicEventPublisher,
	resolver deadLetterWriterResolver,
) *EventDeadLetterService {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.publisher = publisher
	s.resolveWriter = resolver
	s.ownsPublisher = false

	return s
}

// publisherWriterResolver derives a writer resolver from a publisher.
//
// The three cases are exhaustive and each is deliberate:
//
//   - The KAFKA publisher hands back the writer it already holds for that topic. Its
//     pool is pre-populated with every topic Blnk owns, dead-letter siblings included,
//     and it grows lazily for a topic recorded before a prefix change — which is exactly
//     the case a stored row can present.
//   - The NO-OP publisher, and a nil one, resolve to no writer and no error. A
//     deployment with no brokers is a legitimate steady state, and the dead-letter path
//     degrades to recording the row.
//   - ANYTHING ELSE is an error, loudly. A third implementation cannot be written to
//     with a composed message, and silently skipping the write would lose the
//     dead-letter message while reporting success — the one outcome worth failing over.
//
// Parameters:
//   - publisher TopicEventPublisher: the publisher to derive from.
//
// Returns:
//   - deadLetterWriterResolver: the resolver.
func publisherWriterResolver(publisher TopicEventPublisher) deadLetterWriterResolver {
	return func(topic string) (deadLetterMessageWriter, error) {
		if IsNoopEventPublisher(publisher) {
			return nil, nil
		}

		kafkaBacked, ok := publisher.(*kafkaPublisher)
		if !ok {
			return nil, apierror.NewAPIError(
				apierror.ErrKafkaUnavailable,
				"Dead-letter publishing requires the Kafka event publisher",
				fmt.Errorf("blnk: cannot write to dead-letter topic %q through a %T publisher", topic, publisher),
			)
		}

		writer, err := kafkaBacked.writerFor(topic)
		if err != nil {
			return nil, apierror.NewAPIError(
				apierror.ErrKafkaUnavailable,
				"Failed to obtain a Kafka writer for the dead-letter topic",
				fmt.Errorf("blnk: resolving a writer for dead-letter topic %q: %w", topic, err),
			)
		}

		return writer, nil
	}
}

// transport returns the publisher and the dead-letter writer resolver, building them
// from live configuration on first use if none was injected.
//
// Resolution is deferred to first use rather than done in the constructor so that
// constructing the service performs no work at all, and so that a process which only
// ever LISTS dead letters — the common case for the inventory endpoint — never builds a
// publisher it does not need.
//
// Conflating them would let a transient configuration failure in a deployment that runs
// Kafka every day produce a row marked dead_lettered, a dead-letter counter increment,
// and NO MESSAGE ANYWHERE — the event gone, and every signal saying it was safe. So the
// two states are kept apart.
//
// This function is reached only from write paths — the dead-letter write and the replay
// publish. Listing the inventory reads the outbox and never calls it, so an operator
// can still triage with the broker down and with configuration unavailable.
//
// Returns:
//   - TopicEventPublisher: the publisher, never nil when the error is nil.
//   - deadLetterWriterResolver: the resolver, never nil when the error is nil.
//   - error: a typed ErrKafkaUnavailable when configuration cannot be read or the
//     publisher cannot be built.
func (s *EventDeadLetterService) transport() (TopicEventPublisher, deadLetterWriterResolver, error) {
	if publisher, resolver, ready := s.installedTransport(); ready {
		return publisher, resolver, nil
	}

	// fetchConfiguration is the package's configuration seam (declared in
	// event_sunset.go), used here rather than config.Fetch so that a test swapping it sees
	// consistent behaviour across every event file.
	cnf, err := fetchConfiguration()
	if err != nil {
		withLoggableCause(nil, err).Error(
			"configuration is unavailable, so whether this deployment publishes to Kafka " +
				"cannot be determined; refusing to write or replay a dead-letter message rather " +
				"than reporting one that was never sent",
		)

		return nil, nil, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Configuration is unavailable, so the dead-letter transport cannot be resolved",
			fmt.Errorf("blnk: loading configuration for the dead-letter writer: %w", err),
		)
	}

	// Built with NO LOCK HELD. This is the call that can block.
	publisher, err := NewEventPublisher(cnf)
	if err != nil {
		return nil, nil, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Failed to build the Kafka event publisher for the dead-letter writer",
			err,
		)
	}

	topicPublisher, ok := publisher.(TopicEventPublisher)
	if !ok {
		// Unreachable with the implementations in event_publisher.go, both of which satisfy
		// the fuller contract, and asserted rather than assumed so that a future
		// implementation which does not cannot fail obscurely at the write. Closed here
		// because this function built it and is not going to install it.
		closeEventPublisher(publisher)

		return nil, nil, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"The configured event publisher cannot target a dead-letter topic",
			fmt.Errorf("blnk: %T does not implement TopicEventPublisher", publisher),
		)
	}

	return s.installTransport(topicPublisher)
}

// closeEventPublisher releases a publisher that was built and then not installed.
//
// It exists because construction now happens OUTSIDE the service mutex, which means two
// callers can legitimately build one at the same time and exactly one of them must
// throw its away. A publisher that is merely dropped keeps its connection pool and its
// SASL session for the lifetime of the process, so the discard has to be explicit.
//
// Parameters:
//   - publisher EventPublisher: the publisher to release. May be nil.
func closeEventPublisher(publisher EventPublisher) {
	closer, ok := publisher.(interface{ Close() error })
	if !ok || publisher == nil {
		return
	}

	if err := closer.Close(); err != nil {
		withLoggableCause(nil, err).Warn(
			"closing a redundantly built dead-letter publisher failed; another caller's publisher " +
				"is in use and this one is discarded",
		)
	}
}

// installedTransport reports the transport if one is already installed.
//
// It is a separate method purely so that the fast path holds the lock for a field read
// and nothing else, which is what makes "the mutex is never held across construction" a
// property of the code rather than a comment about it.
//
// Returns:
//   - TopicEventPublisher, deadLetterWriterResolver: the installed transport, or nil.
//   - bool: true when both are installed and the caller may use them.
func (s *EventDeadLetterService) installedTransport() (TopicEventPublisher, deadLetterWriterResolver, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.publisher != nil && s.resolveWriter != nil {
		return s.publisher, s.resolveWriter, true
	}

	return nil, nil, false
}

// installTransport publishes a freshly built publisher as the service's, or discards it
// in favour of one another caller installed first.
//
// Parameters:
//   - candidate TopicEventPublisher: the publisher this caller built.
//
// Returns:
//   - TopicEventPublisher, deadLetterWriterResolver: the installed transport, which may
//     be another caller's.
//   - error: always nil; the signature matches transport's so the caller is one line.
func (s *EventDeadLetterService) installTransport(
	candidate TopicEventPublisher,
) (TopicEventPublisher, deadLetterWriterResolver, error) {
	s.mu.Lock()

	if s.publisher != nil && s.resolveWriter != nil {
		installed, resolver := s.publisher, s.resolveWriter
		s.mu.Unlock()

		// Another caller won. Close ours outside the lock.
		closeEventPublisher(candidate)

		return installed, resolver, nil
	}

	s.publisher = candidate
	s.resolveWriter = publisherWriterResolver(candidate)
	s.ownsPublisher = true

	installed, resolver := s.publisher, s.resolveWriter
	s.mu.Unlock()

	return installed, resolver, nil
}

// Close releases a publisher this service built for itself.
//
// It is idempotent and nil-safe, and it NEVER closes a publisher that was passed in:
// the relay's publisher outlives the service and is closed with the process. That
// ownership rule is the reason Close is safe to call from a handler's defer without
// having to know where the publisher came from.
//
// Returns:
//   - error: the publisher's close error, or nil when there was nothing to close.
func (s *EventDeadLetterService) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	publisher := s.publisher
	owned := s.ownsPublisher
	if owned {
		s.publisher = nil
		s.resolveWriter = nil
		s.ownsPublisher = false
	}
	s.mu.Unlock()

	if !owned || publisher == nil {
		return nil
	}

	return publisher.Close()
}

// PublishToDeadLetter writes an event whose retry budget is exhausted to its
// `<topic>.dlt` sibling with failure metadata attached, then records the dead-letter on
// its outbox row.
//
// Parameters:
//   - ctx context.Context: cancels the write, the recording and the metric recording.
//   - req DeadLetterRequest: the row, the failure cause and the optional attempt-window
//     overrides.
//
// Returns:
//   - DeadLetterOutcome: the record of what was written and stored.
//   - error: a validation error for an unusable row, ErrKafkaUnavailable when there is
//     no transport or the write fails, or the repository's own typed error when the row
//     cannot be recorded.
func (s *EventDeadLetterService) PublishToDeadLetter(
	ctx context.Context,
	req DeadLetterRequest,
) (DeadLetterOutcome, error) {
	ctx, span := tracer.Start(ctx, "PublishToDeadLetter")
	defer span.End()

	if s == nil || s.store == nil {
		return DeadLetterOutcome{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Dead-letter publishing requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	row := req.Row
	if row.ID <= 0 {
		// A row with no database identity cannot be recorded, and an unrecorded dead-letter
		// is invisible to the inventory and unreachable by replay. Failing before the write
		// is what keeps "it is on the dead-letter topic" and "an operator can find it" from
		// diverging.
		return DeadLetterOutcome{}, apierror.NewAPIError(
			apierror.ErrInvalidInput,
			"Cannot dead-letter an event outbox entry without a database id",
			fmt.Errorf("blnk: event %q has no outbox id", row.EventID),
		)
	}

	// The request's window overrides are applied to a copy of the row rather than being
	// threaded through the metadata builder, so that the builder stays a pure function of
	// a row and is usable on its own. Only the two attempt timestamps are touched, none of
	// which takes part in the envelope, so the composed message is unaffected.
	metadata := BuildFailureMetadata(
		applyAttemptWindowOverrides(row, req), req.Cause, req.Attempts, s.now().UTC(),
	)

	metadataJSON, err := marshalFailureMetadata(metadata)
	if err != nil {
		span.RecordError(err)

		return DeadLetterOutcome{}, err
	}

	dltTopic := DLTFor(metadata.OriginalTopic)
	if dltTopic == "" {
		// Unreachable in practice: the original topic falls back through TopicForEvent, which
		// never returns an empty name. Guarded anyway, because a blank topic would be
		// published to a topic named ".dlt" or rejected by the broker with a message that
		// names neither the event nor the cause.
		err = apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Could not resolve a dead-letter topic for the event",
			fmt.Errorf("blnk: event %q (%s) has no resolvable topic", row.EventID, row.EventType),
		)
		span.RecordError(err)

		return DeadLetterOutcome{}, err
	}

	message, err := ComposeDeadLetterMessage(row, metadataJSON)
	if err != nil {
		span.RecordError(err)

		return DeadLetterOutcome{}, err
	}

	// The SAME key the original publish used, resolved through the publisher's own rule
	// (the row's stored partition key, falling back to its ledger id and then its
	// aggregate id) so the two cannot diverge. Keeping them identical is what makes the
	// dead-letter topic preserve the same per-aggregate ordering as the topic the event
	// failed to reach, and it is why this is resolved through PublishRequestFromOutbox
	// rather than by reading a column here. The attempt argument plays no part in key
	// resolution.
	partitionKey := resolvePartitionKey(PublishRequestFromOutbox(row, 1))

	outcome := DeadLetterOutcome{
		EventID:         row.EventID,
		EventType:       row.EventType,
		OriginalTopic:   metadata.OriginalTopic,
		DeadLetterTopic: dltTopic,
		PartitionKey:    partitionKey,
		Metadata:        metadata,
		MetadataJSON:    metadataJSON,
		Message:         message,
		Status:          model.PublishStatusDeadLettered,
	}

	span.SetAttributes(
		attribute.String("event.id", row.EventID),
		attribute.String("event.type", row.EventType),
		attribute.String("event.topic", metadata.OriginalTopic),
		attribute.String("event.dlt_topic", dltTopic),
		attribute.Int("event.attempts", metadata.AttemptCount),
	)

	record, err := s.writeDeadLetterMessage(ctx, outcome)
	if err != nil {
		span.RecordError(err)
		logrus.WithFields(outcome.LogFields()).WithField("failure_detail_class", classifyDeadLetterFailure(errorText(err))).
			Error("writing a ledger event to its dead-letter topic failed")

		// The row is deliberately left alone: not marked, not counted. Its status is whatever
		// the relay set before calling — failed on exhaustion — which keeps the event in the
		// dead-letter inventory and out of the terminal set.
		return outcome, err
	}

	// Set only after acknowledgement, so Published and "no error" say the same thing.
	outcome.Published = true

	// The claim token travels on the row: the relay put it there when it claimed the row,
	// and MarkEventFailed retained it on its exhaustion arm precisely so that this step
	// remains the exclusive property of the worker that spent the last attempt.
	if err = s.store.MarkEventDeadLettered(
		ctx, row.ID, row.ClaimToken, dltTopic, metadataJSON, record,
	); err != nil {
		span.RecordError(err)
		logrus.WithFields(outcome.LogFields()).WithField("failure_detail_class", classifyDeadLetterFailure(errorText(err))).
			Error("recording a dead-lettered ledger event on its outbox row failed")

		return outcome, err
	}

	// Counted here, and only here: once the row is recorded the dead-lettering is
	// complete, so the counter answers "how many events ended up dead-lettered" without
	// double-counting a write whose bookkeeping had to be retried. Both labels are bounded
	// — see boundedTopicLabel and boundedEventTypeLabel — because both values come from a
	// stored row and this counter is compared against EventsPublishedTotal, which bounds
	// them the same way. Bounding one side and not the other would make the
	// dead-letter-rate query divide series that do not correspond.
	metrics.EventsDeadLetteredTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(outcome.OriginalTopic)),
		attribute.String(publishAttrEventType, boundedEventTypeLabel(outcome.EventType)),
	))

	// The message says WHAT happened and not WHY, because the why is not this file's to
	// know. The relay states the reason in its own line, where the decision was taken, and
	// the attempt count in these fields is the honest number either way.
	logrus.WithFields(outcome.LogFields()).Warn("ledger event dead-lettered and preserved on its dead-letter topic")

	return outcome, nil
}

// DeadLetter dead-letters a row with the failure that ended its retry budget.
//
// It is PublishToDeadLetter with the request built for you, which is the call the
// relay's exhaustion branch makes. Everything the request could override is derived
// from the row.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - row model.EventOutbox: the exhausted row.
//   - cause error: the failure from the final attempt. May be nil, in which case the
//     row's last_error is reported.
//
// Returns:
//   - DeadLetterOutcome: the record of what was written and stored.
//   - error: as PublishToDeadLetter.
func (s *EventDeadLetterService) DeadLetter(
	ctx context.Context,
	row model.EventOutbox,
	cause error,
) (DeadLetterOutcome, error) {
	return s.PublishToDeadLetter(ctx, DeadLetterRequest{Row: row, Cause: cause})
}

// writeDeadLetterMessage performs the one raw Kafka write in this file and records the
// attempt metrics for it.
//
// It is separated from PublishToDeadLetter so that the composition and recording steps
// read as one sequence rather than being interrupted by transport handling, and so the
// "no transport" branch has exactly one place to live.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - outcome DeadLetterOutcome: the composed message and its routing.
//
// Returns:
//   - error: nil ONLY when a broker acknowledged the write.
func (s *EventDeadLetterService) writeDeadLetterMessage(
	ctx context.Context,
	outcome DeadLetterOutcome,
) (model.BrokerRecord, error) {
	// STARTED BEFORE ANY RESOLUTION, so every exit below can be measured. The three
	// failure exits are the ones an operator most needs on the instruments: a broker
	// refusing every dead-letter message, or a deployment with no transport at all, must
	// not look like an empty dead-letter path.
	started := s.now()

	// EVERY EXIT RECORDS ONE ATTEMPT, and it is attributed to this write rather than to
	// the retry sequence that led here.
	record := func(status model.PublishStatus) {
		recordPublishAttempt(ctx, PublishResult{
			Status:       status,
			Purpose:      PublishPurposeDeadLetter,
			EventID:      outcome.EventID,
			EventType:    outcome.EventType,
			Topic:        outcome.DeadLetterTopic,
			PartitionKey: outcome.PartitionKey,
			Attempt:      outcome.Metadata.AttemptCount,
			Duration:     s.now().Sub(started),
		})
	}

	_, resolveWriter, err := s.transport()
	if err != nil {
		record(model.PublishStatusRetrying)

		return model.BrokerRecord{}, err
	}

	writer, err := resolveWriter(outcome.DeadLetterTopic)
	if err != nil {
		record(model.PublishStatusRetrying)

		return model.BrokerRecord{}, err
	}

	if writer == nil {
		// no transport means NO DEAD-LETTER MESSAGE, and that is a failure.
		logrus.WithFields(outcome.LogFields()).Error(
			"no Kafka transport is configured, so the dead-letter message cannot be written; " +
				"leaving the event outbox row in its non-terminal state rather than recording a " +
				"dead-lettering that did not happen",
		)

		record(model.PublishStatusRetrying)

		return model.BrokerRecord{}, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"No Kafka transport is configured, so the event cannot be dead-lettered",
			fmt.Errorf(
				"blnk: dead-lettering event %q (%s) to %q requires a Kafka transport; KAFKA_BROKERS is not configured",
				outcome.EventID, outcome.EventType, outcome.DeadLetterTopic,
			),
		)
	}

	// The carrier the broker's coordinate comes back on. The writers this resolver hands
	// back are the publisher's own, so completeWrite is already installed on them and
	// fills this in before WriteMessages returns; a test double that does not call
	// Completion simply leaves it unconfirmed, which the row then records honestly as
	// unconfirmed.
	acknowledgement := &publishAcknowledgement{}

	writeErr := writer.WriteMessages(ctx, kafka.Message{
		// Topic is left empty on purpose: kafka-go rejects a message that names a topic
		// when the writer already has one, and every writer here is per-topic.
		Key:   partitionKeyBytes(outcome.PartitionKey),
		Value: outcome.Message,
		// Correlates the completion back to THIS write. It never reaches the wire, so byte
		// fidelity is untouched.
		WriterData: acknowledgement,
		// The instant the event was GIVEN UP ON, not the instant it occurred. A dead-letter
		// message is a new message on a different topic, and its broker timestamp should say
		// when it arrived there: stamping it with an original occurrence that may be hours
		// old would expose a message whose whole purpose is preservation to time-based
		// retention as though it were that old. The timestamp is transport metadata and not
		// part of the message value, so byte fidelity is untouched either way — and the
		// outbox row, not the topic, is the authoritative record the inventory and replay
		// read from.
		Time: outcome.Metadata.LastAttemptedAt,
	})
	if writeErr != nil {
		// quite possibly a *net.OpError naming the broker's address — so it is logged here
		// and a bounded detail is attached to the error instead of the cause itself. See
		// EventTransportErrorDetail.
		logrus.WithFields(outcome.LogFields()).WithField("failure_detail_class", classifyDeadLetterFailure(errorText(writeErr))).Error(
			"the Kafka broker did not acknowledge a dead-letter message",
		)

		record(model.PublishStatusRetrying)

		return model.BrokerRecord{}, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Failed to publish the event to its dead-letter topic",
			NewEventTransportErrorDetail(
				"the Kafka broker did not acknowledge the dead-letter message",
				outcome.EventID, outcome.EventType, outcome.DeadLetterTopic, writeErr,
			),
		)
	}

	// Stamped with the DEAD-LETTERED outcome rather than dispatched: the broker did accept
	// this message, but the pipeline-level statement about the event is that it ended its
	// life on a dead-letter topic.
	record(model.PublishStatusDeadLettered)

	// AND COUNTED AS A REAL BROKER ACKNOWLEDGEMENT, under purpose="dead_letter".
	metrics.EventBrokerAcknowledgementsTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(outcome.DeadLetterTopic)),
		attribute.String(publishAttrEventType, boundedEventTypeLabel(outcome.EventType)),
		attribute.String(publishAttrPurpose, string(PublishPurposeDeadLetter)),
	))

	// The coordinate names the record on the DEAD-LETTER topic, which is where this
	// event's only surviving copy now lives. Persisting it is what lets an operator
	// triaging the inventory read the exact record back, and what lets the zero-loss audit
	// account for a dead-lettered event on the broker side rather than treating its record
	// as surplus.
	brokerRecord, _ := acknowledgement.coordinate()

	return brokerRecord, nil
}

// ListDeadLetterEvents pages the dead-letter inventory an operator triages from.
//
// It reads the OUTBOX TABLE and never a Kafka topic. That is a design commitment, not
// an implementation detail: Blnk implements no consumer, and it does not need one,
// because the row already carries the dead-letter topic and the failure metadata.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - opts DeadLetterListOptions: the page and its optional narrowing.
//
// Returns:
//   - []model.EventOutbox: the matching entries, oldest last. Never nil on success.
//   - error: a validation error for an unusable status filter or a reversed occurrence
//     window, or the repository's own typed error.
func (s *EventDeadLetterService) ListDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (model.DeadLetterInventoryPage, error) {
	ctx, span := tracer.Start(ctx, "ListDeadLetterEvents")
	defer span.End()

	if s == nil || s.store == nil {
		return model.DeadLetterInventoryPage{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Listing dead-lettered events requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	normalized, err := normalizeDeadLetterListOptions(opts)
	if err != nil {
		span.RecordError(err)

		return model.DeadLetterInventoryPage{}, err
	}

	span.SetAttributes(deadLetterListSpanAttributes(normalized)...)

	// THE NARROW PROJECTION, PAGED BY CURSOR. The listing reads the triage coordinate of
	// each entry and the SIZE of its payload rather than the payload itself, so an
	// inventory of large events costs a page of metadata instead of a page of event bodies
	// — and the cursor keeps that cost the same at any depth. The full stored bytes are
	// read only by the replay path, which is the one caller that needs them.
	page, listErr := s.store.ListDeadLetterInventory(ctx, normalized.inventoryQuery())
	if listErr != nil {
		span.RecordError(listErr)

		return model.DeadLetterInventoryPage{}, listErr
	}

	// The repository allocates the slice, but a defensive normalisation keeps the contract
	// true for any future store implementation: a handler marshals this directly and [] is
	// the right empty JSON, not null.
	if page.Entries == nil {
		page.Entries = []model.DeadLetterInventoryEntry{}
	}
	span.SetAttributes(
		attribute.Int("dead_letter.returned", len(page.Entries)),
		attribute.Bool("dead_letter.has_more", page.HasMore),
	)

	return page, nil
}

// ListAndCountDeadLetterEvents returns one page of the inventory together with how many
// entries the same narrowing matches, both drawn from ONE DATABASE SNAPSHOT.
//
// A total that describes a set the page is not a slice of is not a rounding error on
// this endpoint — an operator triaging a backlog reads it as how much work is stuck,
// and a paging client comparing the page against the total does not terminate.
//
// Parameters:
//   - ctx context.Context: cancels the read.
//   - opts DeadLetterListOptions: the page and its narrowing. The count applies the
//     same narrowing and ignores the page.
//
// Returns:
//   - model.DeadLetterInventoryPage: the page, as ListDeadLetterEvents.
//   - int64: how many entries the narrowing matches in the same snapshot.
//   - error: a validation error for an unusable status filter or a reversed occurrence
//     window, or the repository's own typed error.
func (s *EventDeadLetterService) ListAndCountDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (model.DeadLetterInventoryPage, int64, error) {
	ctx, span := tracer.Start(ctx, "ListAndCountDeadLetterEvents")
	defer span.End()

	if s == nil || s.store == nil {
		return model.DeadLetterInventoryPage{}, 0, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Listing dead-lettered events requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	normalized, err := normalizeDeadLetterListOptions(opts)
	if err != nil {
		span.RecordError(err)

		return model.DeadLetterInventoryPage{}, 0, err
	}

	span.SetAttributes(deadLetterListSpanAttributes(normalized)...)

	page, total, err := s.store.ListAndCountDeadLetterInventory(ctx, normalized.inventoryQuery())
	if err != nil {
		span.RecordError(err)

		return model.DeadLetterInventoryPage{}, 0, err
	}

	// Defensive, exactly as in ListDeadLetterEvents: a handler marshals this directly and [] is
	// the right empty JSON, not null.
	if page.Entries == nil {
		page.Entries = []model.DeadLetterInventoryEntry{}
	}

	span.SetAttributes(
		attribute.Int("dead_letter.returned", len(page.Entries)),
		attribute.Bool("dead_letter.has_more", page.HasMore),
		attribute.Int64("dead_letter.total", total),
	)

	return page, total, nil
}

// CountDeadLetterEvents reports how many inventory entries the SAME narrowing matches,
// ignoring the page.
//
// It is the exact total behind `include_count` on the dead-letter listing, and it
// exists because a page cannot say how much is behind it.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - opts DeadLetterListOptions: the narrowing. Limit and Offset are ignored.
//
// Returns:
//   - int64: the number of matching entries; zero when none match.
//   - error: a validation error for an unusable status filter or a reversed occurrence
//     window, or the repository's own typed error.
func (s *EventDeadLetterService) CountDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (int64, error) {
	ctx, span := tracer.Start(ctx, "CountDeadLetterEvents")
	defer span.End()

	if s == nil || s.store == nil {
		return 0, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Counting dead-lettered events requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	normalized, err := normalizeDeadLetterListOptions(opts)
	if err != nil {
		span.RecordError(err)

		return 0, err
	}

	span.SetAttributes(deadLetterListSpanAttributes(normalized)...)

	total, err := s.store.CountDeadLetteredEvents(ctx, normalized.deadLetterQuery())
	if err != nil {
		span.RecordError(err)

		return 0, err
	}

	span.SetAttributes(attribute.Int64("dead_letter.total", total))

	return total, nil
}

// deadLetterListSpanAttributes describes a normalised listing request on a span.
//
// Parameters:
//   - opts DeadLetterListOptions: already normalised.
//
// Returns:
//   - []attribute.KeyValue: the page bounds, whether the request was narrowed, and each
//     set filter.
func deadLetterListSpanAttributes(opts DeadLetterListOptions) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		attribute.Int("dead_letter.limit", opts.Limit),
		attribute.Int("dead_letter.offset", opts.Offset),
		attribute.Bool("dead_letter.cursor_present", opts.Cursor != nil),
		attribute.Bool("dead_letter.filtered", opts.filtered()),
	}
	if opts.EventType != "" {
		attributes = append(attributes, attribute.String("dead_letter.event_type", opts.EventType))
	}
	if opts.Topic != "" {
		attributes = append(attributes, attribute.String("dead_letter.topic", opts.Topic))
	}
	if opts.Status != "" {
		attributes = append(attributes, attribute.String("dead_letter.status", opts.Status))
	}
	if !opts.OccurredFrom.IsZero() {
		attributes = append(attributes, attribute.String("dead_letter.occurred_from", opts.OccurredFrom.UTC().Format(time.RFC3339Nano)))
	}
	if !opts.OccurredTo.IsZero() {
		attributes = append(attributes, attribute.String("dead_letter.occurred_to", opts.OccurredTo.UTC().Format(time.RFC3339Nano)))
	}

	return attributes
}

// ReplayDeadLetteredEvent re-publishes a dead-lettered event to the topic it was
// originally destined for.
//
// Only the failure metadata is absent, which is what "aside from the failure metadata"
// means: the metadata was attached as an additive sibling member on the dead-letter
// message and is simply not part of the envelope being republished.
//
// Parameters:
//   - ctx context.Context: cancels the lookup, the publish and the recording.
//   - eventID string: the event's UUID, as listed by the inventory.
//
// Returns:
//   - ReplayOutcome: the record of the replay.
//   - error: ErrGenValidation for a blank id, ErrEventNotFound when no such event
//     exists, ErrEventNotDeadLettered when the event is not in the dead-lettered state,
//     ErrKafkaUnavailable when there is no transport or the broker is unavailable, or
//     ErrEventReplayFailed when the re-publish fails for any other reason and when the
//     event was republished but its row could not be marked dispatched.
func (s *EventDeadLetterService) ReplayDeadLetteredEvent(
	ctx context.Context,
	eventID string,
) (ReplayOutcome, error) {
	ctx, span := tracer.Start(ctx, "ReplayDeadLetteredEvent")
	defer span.End()

	if s == nil || s.store == nil {
		return ReplayOutcome{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Replaying a dead-lettered event requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return ReplayOutcome{}, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"An event id is required to replay a dead-lettered event",
			errors.New("blnk: replay called with a blank event id"),
		)
	}
	span.SetAttributes(attribute.String("event.id", eventID))

	// CLAIMED, not merely read. The claim is what makes concurrent replay safe; see
	// claimReplayableEvent and the eventDeadLetterStore contract.
	row, err := s.claimReplayableEvent(ctx, eventID)
	if err != nil {
		span.RecordError(err)

		return ReplayOutcome{}, err
	}

	// From here on the row is held in the replaying state, so EVERY exit path must either
	// mark it dispatched or release the claim. Releasing is idempotent from the caller's
	// point of view — releaseReplayClaim reports its own failures and never masks the
	// error being returned — so the deferred-style guard below is safe to pair with the
	// explicit success transition further down.
	released := false
	releaseOnFailure := func(reason error) {
		if released {
			return
		}
		released = true
		s.releaseReplayClaim(ctx, row, reason)
	}

	topic := ReplayTopicFor(*row)
	attempt := replayAttemptNumber(*row)

	span.SetAttributes(
		attribute.String("event.type", row.EventType),
		attribute.String("event.topic", topic),
		attribute.String("event.dlt_topic", row.DLTTopic),
		attribute.Int("event.replay_attempt", attempt),
	)

	publisher, _, err := s.transport()
	if err != nil {
		span.RecordError(err)
		releaseOnFailure(err)

		return ReplayOutcome{}, err
	}

	if IsNoopEventPublisher(publisher) {
		err = apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"No Kafka broker is configured, so the event cannot be replayed",
			fmt.Errorf("blnk: replay of event %q requires KAFKA_BROKERS to be configured", eventID),
		)
		span.RecordError(err)
		releaseOnFailure(err)

		return ReplayOutcome{}, err
	}

	// The request is built from the stored row, so the bytes on the wire are the bytes the
	// first publish produced. Only the destination and the attempt label are stated here,
	// and neither takes part in the message value.
	request := PublishRequestFromOutbox(*row, attempt)
	request.Topic = topic

	// A replay is NOT attempt N+1 of a live retry sequence — that sequence ended when the
	// event was dead-lettered. Stating the purpose is what keeps it out of the attempt="1"
	// latency population the sub-2-second target is read from, and out of the published-
	// events counter that is the denominator of the dead-letter rate.
	request.Purpose = PublishPurposeReplay

	result, publishErr := publisher.PublishToTopic(ctx, request)

	outcome := ReplayOutcome{
		EventID:      row.EventID,
		EventType:    row.EventType,
		Topic:        result.Topic,
		PartitionKey: result.PartitionKey,
		Status:       result.Status,
		ReplayedAt:   s.now().UTC(),
		Result:       result,
	}

	if publishErr != nil {
		// Same boundary as the dead-letter write: the publisher's error carries the broker's
		// own words — and, through *net.OpError, its address — so the log line below keeps
		// them while the caller receives the bounded diagnosis.
		code, message := replayFailureOutcome(publishErr)
		detail := NewEventTransportErrorDetail(
			"the Kafka broker did not acknowledge the replayed message",
			eventID, row.EventType, topic, publishErr,
		)
		// The detail's retryability is taken from the SAME verdict as the code, and not from
		// the detail constructor's own narrower classifier. Those two answer very nearly the
		// same question but not identically — the constructor does not know about a closed
		// transport, about BrokerNotAvailable, or about a PublishError's explicit verdict —
		// so leaving them independent would let one response say 503 in its status and
		// "transient": false in its body. A caller deciding whether to retry reads whichever
		// it happens to trust, and half of them would be wrong.
		detail.Transient = code == apierror.ErrKafkaUnavailable

		err = apierror.NewAPIError(code, message, detail)
		span.RecordError(err)
		logrus.WithFields(outcome.LogFields()).WithField("failure_detail_class", classifyDeadLetterFailure(errorText(publishErr))).
			Error("replaying a dead-lettered ledger event failed")
		// The publish failed, so the row is owed nothing further and must go back to
		// dead_lettered — otherwise a failed replay would cost the event its replayability by
		// stranding it in replaying.
		releaseOnFailure(publishErr)

		return outcome, err
	}

	if markErr := s.store.MarkEventDispatched(
		ctx, row.ID, row.ClaimToken, result.Record,
	); markErr != nil {
		// The event HAS been republished. The bookkeeping has not, so the row is still listed
		// as dead-lettered and can be replayed again — a duplicate that the subscriber's
		// idempotency on the unchanged event id absorbs. An error is returned rather than
		// swallowed precisely because the operator must know the entry has not cleared;
		// reporting success here would leave a phantom in the inventory with nobody looking
		// for it.
		err = apierror.NewAPIError(
			apierror.ErrEventReplayFailed,
			"The event was republished but its outbox entry is still marked dead-lettered",
			fmt.Errorf("blnk: marking replayed event %q dispatched: %w", eventID, markErr),
		)
		span.RecordError(err)
		logrus.WithFields(outcome.LogFields()).WithField("failure_detail_class", classifyDeadLetterFailure(errorText(markErr))).
			Error("a replayed ledger event could not be marked dispatched")
		// The event IS on the topic but the success transition did not land, so the row is
		// returned to dead_lettered rather than left in replaying. That keeps the entry
		// visible in the inventory — which is what the operator needs, since the error above
		// tells them it has not cleared — instead of hiding it in a state no view reports on.
		releaseOnFailure(markErr)

		return outcome, err
	}
	released = true

	outcome.Recorded = true
	logrus.WithFields(outcome.LogFields()).Info("dead-lettered ledger event replayed to its original topic")

	return outcome, nil
}

// replayClaimLease is how long a replay holds its claim on a row.
const replayClaimLease = 2 * time.Minute

// replayFailureOutcome maps a failed re-publish onto the typed code and the
// operator-facing message the replay endpoint must answer with.
//
// It is the one place the distinction is drawn, so the code and the message can never
// disagree about what went wrong. The three arms:
//
//   - The replay was ABANDONED — the caller went away, or the request's deadline expired,
//     with no network or protocol failure anywhere in the error's chain. ErrEventReplayFailed,
//     which statusByCode maps to 500, carrying a message that says the event is still
//     dead-lettered so repeating the request is obviously safe. The arm exists to keep this
//     case OUT of the availability arm below, which would answer a 503 whose message tells an
//     operator to wait for a broker that was never unhealthy; what it must not do is answer
//     with a code outside the approved contract, so the distinction lives in the message and
//     the log line rather than in a fourth public code and a fourth status.
//   - The broker is unavailable — unreachable, leaderless, under-replicated, or a
//     transport that has been closed. ErrKafkaUnavailable, which statusByCode maps to 503.
//     This is the same code the no-transport branch above returns, and deliberately so:
//     both mean the event is intact and the request should be repeated once the broker is
//     back. The message says the broker, not the event, is the problem, because an
//     operator reading it needs to know where to look.
//   - Anything else. ErrEventReplayFailed too, with the message that names a failure this
//     service owns — bytes that cannot be published, a destination that cannot be resolved —
//     where a retry changes nothing. The first and third arms share a code and are told
//     apart by their message, which is exactly the split a two-code contract can express.
//
// Parameters:
//   - cause error: the non-nil error PublishToTopic returned.
//
// Returns:
//   - apierror.ErrorCode: the typed code, which has an explicit statusByCode entry.
//   - string: the message that accompanies it.
func replayFailureOutcome(cause error) (apierror.ErrorCode, string) {
	if localContextTermination(cause) {
		return apierror.ErrEventReplayFailed,
			"The replay was abandoned before the broker acknowledged it; the event is still " +
				"dead-lettered, so the request can be repeated"
	}

	if IsBrokerUnavailableError(cause) {
		return apierror.ErrKafkaUnavailable,
			"The Kafka broker is unavailable, so the event could not be replayed; retry once it recovers"
	}

	return apierror.ErrEventReplayFailed, "Failed to replay the dead-lettered event to its original topic"
}

// claimReplayableEvent CLAIMS a dead-lettered row for replay and translates the
// repository's failures into the typed errors this API answers with.
//
// An operator double-clicking, or two operators working the same backlog, therefore put
// two copies of the event on the topic. Because a replay re-publishes the stored bytes
// those copies are byte-identical, so a subscriber deduplicating on event_id discards
// one — but the duplicate is real, it occupies a partition slot, and leaning on
// consumer behaviour to paper over a defect on the publishing side is not a guarantee.
//
// Moving the precondition INTO the transition closes it. dead_lettered → replaying is a
// conditional update, so exactly one concurrent request changes a row and receives the
// claim token; every other request is refused before it can publish anything.
//
// The two rejections stay distinct, because they are different operator situations: a
// missing event is a wrong id, whereas a present but not-dead-lettered event is a state
// error — most often a second replay of something already replayed, which is exactly
// the accidental duplication the precondition exists to prevent. A row found in the
// replaying state is now also reachable, and it means a concurrent replay holds it;
// that is reported as a state error too, with the status named, so the message says
// what is actually happening.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - eventID string: the trimmed, non-blank event id.
//
// Returns:
//   - *model.EventOutbox: the claimed row, carrying the claim token in ClaimToken.
//   - error: ErrEventNotFound or ErrEventNotDeadLettered, or the repository's own
//     error.
func (s *EventDeadLetterService) claimReplayableEvent(
	ctx context.Context,
	eventID string,
) (*model.EventOutbox, error) {
	row, err := s.store.ClaimEventForReplay(ctx, eventID, replayClaimLease)
	if err != nil {
		if isNotFoundError(err) {
			return nil, apierror.NewAPIError(
				apierror.ErrEventNotFound,
				"No event with that id exists",
				fmt.Errorf("blnk: event %q not found: %w", eventID, err),
			)
		}
		if isConflictError(err) {
			// The row exists but is not dead-lettered. WHICH state it is in decides what the
			// operator is told, because "already replayed", "still being delivered" and "another
			// replay is in flight" are three different situations and only one of them is a
			// mistake.
			return nil, s.describeUnreplayableEvent(ctx, eventID, err)
		}

		return nil, err
	}

	if row == nil {
		// Defensive: the repository returns a typed error rather than a nil row, and
		// this keeps a future change to that contract from becoming a nil dereference.
		return nil, apierror.NewAPIError(
			apierror.ErrEventNotFound,
			"No event with that id exists",
			fmt.Errorf("blnk: event %q not found", eventID),
		)
	}

	return row, nil
}

// describeUnreplayableEvent turns a refused replay claim into the message that names
// the operator's actual situation.
//
// The claim itself can only report that the precondition failed; it cannot say why in a
// way an operator can act on. Reading the row afterwards is what supplies that, and it
// costs one query on the failure path only.
//
// Parameters:
//   - ctx context.Context: cancels the explanatory read.
//   - eventID string: the event that could not be claimed.
//   - cause error: the repository's own refusal, preserved as the error detail so the
//     status it reported survives even if the read below fails.
//
// Returns:
//   - error: always ErrEventNotDeadLettered, with a message naming the situation.
func (s *EventDeadLetterService) describeUnreplayableEvent(ctx context.Context, eventID string, cause error) error {
	message := "Only a dead-lettered event can be replayed"

	if row, err := s.store.GetEventByID(ctx, eventID); err == nil && row != nil {
		switch {
		case row.Status == model.EventOutboxStatusDispatched && row.DLTTopic != "":
			message = "This event has already been replayed and cannot be replayed again"
		case row.Status == model.EventOutboxStatusReplaying:
			message = "This event is already being replayed; wait for that replay to finish"
		default:
			message = fmt.Sprintf("Only a dead-lettered event can be replayed; this one is %s", row.Status)
		}
	}

	return apierror.NewAPIError(
		apierror.ErrEventNotDeadLettered,
		message,
		fmt.Errorf("blnk: event %q could not be claimed for replay: %w", eventID, cause),
	)
}

// releaseReplayClaim returns a claimed row to dead_lettered after a replay that did not
// complete.
//
// The release is bookkeeping this service already owes, so it is completed on a context
// detached from the caller's cancellation and bounded by its own timeout — the same
// treatment the relay gives its own transitions, for the same reason.
//
// Parameters:
//   - ctx context.Context: used only for its values; cancellation is deliberately not
//     inherited.
//   - row *model.EventOutbox: the claimed row, carrying its claim token.
//   - reason error: why the replay did not complete; recorded in last_error when it has
//     a message, so the next operator sees the most recent cause rather than the
//     original publish failure.
func (s *EventDeadLetterService) releaseReplayClaim(ctx context.Context, row *model.EventOutbox, reason error) {
	if row == nil {
		return
	}

	var replayErr string
	if reason != nil {
		replayErr = sanitizeLogValue(reason.Error(), maxLoggedErrorLength)
	}

	release, cancel := context.WithTimeout(context.WithoutCancel(ctx), replayReleaseTimeout)
	defer cancel()

	if err := s.store.ReleaseEventReplay(release, row.ID, row.ClaimToken, replayErr); err != nil {
		withLoggableCause(logrus.WithFields(logrus.Fields{
			"event_id":   row.EventID,
			"event_type": row.EventType,
			"dlt_topic":  row.DLTTopic,
		}), err).Error(
			"a claimed replay could not be returned to the dead-lettered state; it clears when its " +
				"claim lease expires, because the replay claim reclaims an expired claim atomically",
		)
	}
}

// replayReleaseTimeout bounds the detached rollback of a replay claim. One UPDATE, so it
// matches the relay's bookkeeping budget: long enough for a healthy database, short enough
// that a database that has gone away cannot hold a request open on work whose failure the
// claim's own lease recovery already covers.
const replayReleaseTimeout = 5 * time.Second

// RefreshDeadLetterAgeGauge recomputes and publishes the dead-letter age gauge.
//
// The age of an entry is measured from its LAST ATTEMPT, which is the instant it was
// given up on and therefore the instant it started sitting in the dead-letter topic.
// Where that is unknown the occurrence instant is used instead, which is older and so
// errs toward reporting a problem rather than hiding one.
//
// Parameters:
//   - ctx context.Context: cancels the counts, the walk and the gauge recording.
//
// Returns:
//   - DeadLetterAgeReport: the ages the gauge was set from. Its map is never nil on
//     success.
//   - error: the repository's own typed error.
func (s *EventDeadLetterService) RefreshDeadLetterAgeGauge(ctx context.Context) (DeadLetterAgeReport, error) {
	ctx, span := tracer.Start(ctx, "RefreshDeadLetterAgeGauge")
	defer span.End()

	if s == nil || s.store == nil {
		return DeadLetterAgeReport{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Refreshing the dead-letter age gauge requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	now := s.now().UTC()
	report := DeadLetterAgeReport{
		GeneratedAt:   now,
		OldestByTopic: make(map[string]time.Duration),
	}

	// Every topic Blnk owns starts at zero, so a topic that has nothing outstanding is
	// actively reported as clear instead of keeping a stale age.
	for _, topic := range AllDeadLetterTopics() {
		report.OldestByTopic[topic] = 0
	}

	ages, err := s.store.OldestDeadLetterAgeByTopic(ctx, DeadLetterTopicSuffix)
	if err != nil {
		span.RecordError(err)

		return DeadLetterAgeReport{}, err
	}

	for _, age := range ages {
		// The topic label must be bounded before it reaches a metric, and it is bounded here
		// rather than at the recording call. Bounding it in one place
		// keeps the returned report and the published series identical, so an operator
		// reading the report and an alert reading the gauge cannot disagree about which topic
		// an age belongs to.
		topic := boundedTopicLabel(age.Topic)

		report.Outstanding += age.Outstanding

		elapsed := now.Sub(age.Oldest)
		if elapsed < 0 {
			// Clock skew, or an occurrence dated in the future. A negative age would read
			// as "newer than now" and would silently lower the maximum.
			elapsed = 0
		}

		if elapsed > report.OldestByTopic[topic] {
			report.OldestByTopic[topic] = elapsed
		}
	}

	// The pre-dead-letter population, counted from the same aggregate rather than from a
	// second query: a row whose retry budget is spent but whose `<topic>.dlt` write has
	// not landed is grouped under the sibling topic it is BOUND FOR, and its dlt_topic is
	// still NULL. Reporting it separately is what keeps a broker refusing dead-letter
	// writes distinguishable from a busy triage queue.
	failedAwaiting, err := s.countFailedAwaitingDeadLetter(ctx)
	if err != nil {
		span.RecordError(err)

		return DeadLetterAgeReport{}, err
	}
	report.FailedAwaitingDeadLetter = failedAwaiting

	for topic, age := range report.OldestByTopic {
		metrics.DLTOldestMessageAgeSeconds.Record(ctx, age.Seconds(), otelmetric.WithAttributes(
			attribute.String(publishAttrTopic, topic),
		))
	}

	span.SetAttributes(
		attribute.Int64("dead_letter.outstanding", report.Outstanding),
		attribute.Int64("dead_letter.failed_awaiting_dlt", report.FailedAwaitingDeadLetter),
		attribute.Int("dead_letter.topics", len(ages)),
		attribute.Float64("dead_letter.oldest_age_seconds", report.OldestAge().Seconds()),
	)

	return report, nil
}

// BuildFailureMetadata assembles the failure record appended to a dead-lettered event.
//
// Parameters:
//   - row model.EventOutbox: the exhausted row.
//   - cause error: the failure from the final attempt. May be nil.
//   - attempts int: the caller's attempt count. Zero or negative means "use the row".
//   - at time.Time: the dead-letter instant, used as the terminal fallback for the
//     window.
//
// Returns:
//   - model.FailureMetadata: the fully-populated record.
func BuildFailureMetadata(row model.EventOutbox, cause error, attempts int, at time.Time) model.FailureMetadata {
	if at.IsZero() {
		at = time.Now()
	}
	at = at.UTC()

	metadata := model.FailureMetadata{
		OriginalTopic:    originalTopicOf(row),
		ErrorReason:      deadLetterReason(row, cause),
		AttemptCount:     resolveDeadLetterAttempts(row, attempts),
		FirstAttemptedAt: at,
		LastAttemptedAt:  at,
	}

	if row.FirstAttemptedAt != nil && !row.FirstAttemptedAt.IsZero() {
		metadata.FirstAttemptedAt = row.FirstAttemptedAt.UTC()
	}
	if row.LastAttemptedAt != nil && !row.LastAttemptedAt.IsZero() {
		metadata.LastAttemptedAt = row.LastAttemptedAt.UTC()
	}

	// A window that runs backwards can only come from a clock adjustment or a stale
	// override. Collapsing it to a zero-length window keeps the reported duration
	// meaningful; publishing it would put a negative number in an operator's report.
	if metadata.LastAttemptedAt.Before(metadata.FirstAttemptedAt) {
		metadata.LastAttemptedAt = metadata.FirstAttemptedAt
	}

	return metadata
}

// applyAttemptWindowOverrides returns a copy of the row carrying the request's non-zero
// attempt-window overrides.
//
// Parameters:
//   - row model.EventOutbox: the row to copy.
//   - req DeadLetterRequest: the request whose overrides are applied.
//
// Returns:
//   - model.EventOutbox: the copy. The original row is not modified.
func applyAttemptWindowOverrides(row model.EventOutbox, req DeadLetterRequest) model.EventOutbox {
	if !req.FirstAttemptedAt.IsZero() {
		first := req.FirstAttemptedAt
		row.FirstAttemptedAt = &first
	}
	if !req.LastAttemptedAt.IsZero() {
		last := req.LastAttemptedAt
		row.LastAttemptedAt = &last
	}

	return row
}

// originalTopicOf returns the topic an event was destined for.
//
// The row's recorded topic wins over the event type's current mapping, because the row
// recorded its destination at insert time precisely so it stays replayable to the topic
// it was always meant for even if KAFKA_TOPIC_PREFIX changed since. TopicForEvent never
// returns an empty name, so the result is always usable.
//
// Parameters:
//   - row model.EventOutbox: the row to read.
//
// Returns:
//   - string: a non-empty topic name.
func originalTopicOf(row model.EventOutbox) string {
	if topic := strings.TrimSpace(row.Topic); topic != "" {
		return topic
	}

	return TopicForEvent(row.EventType)
}

// ReplayTopicFor returns the topic a dead-lettered event must be replayed to.
//
// It is exported because the replay response reports the destination, and a caller
// composing that response must arrive at the same answer the publish did rather than
// guessing.
//
// Parameters:
//   - row model.EventOutbox: the dead-lettered row.
//
// Returns:
//   - string: a non-empty topic name, never a `.dlt` name.
func ReplayTopicFor(row model.EventOutbox) string {
	if metadata, err := DecodeFailureMetadata(row.FailureMetadata); err == nil && metadata != nil {
		if topic := strings.TrimSpace(metadata.OriginalTopic); topic != "" && !IsDeadLetterTopic(topic) {
			return topic
		}
	}

	return originalTopicOf(row)
}

// deadLetterReason returns the error reason recorded on a dead-lettered event,
// SANITIZED AND BOUNDED.
//
// This value does not merely reach a log line. It goes into the failure metadata that
// is marshaled into the dead-letter MESSAGE on the `<topic>.dlt` topic, and into the
// row's persisted failure_metadata column that the dead-letter API reads back.
//
// Parameters:
//   - row model.EventOutbox: the row, whose last_error is the fallback.
//   - cause error: the caller's failure. May be nil.
//
// Returns:
//   - string: a non-empty, control-character-free reason no longer than
//     maxLoggedErrorLength.
func deadLetterReason(row model.EventOutbox, cause error) string {
	if cause != nil {
		if reason := sanitizeLogValue(cause.Error(), maxLoggedErrorLength); reason != "" {
			return reason
		}
	}

	if reason := sanitizeLogValue(row.LastError, maxLoggedErrorLength); reason != "" {
		return reason
	}

	return unrecordedDeadLetterReason
}

// resolveDeadLetterAttempts returns the attempt count reported in the failure metadata.
//
// Parameters:
//   - row model.EventOutbox: the row, whose counter and budget are the fallbacks.
//   - attempts int: the caller's count. Zero or negative means "not stated".
//
// Returns:
//   - int: a strictly positive attempt count.
func resolveDeadLetterAttempts(row model.EventOutbox, attempts int) int {
	resolved := attempts
	if row.Attempts > resolved {
		resolved = row.Attempts
	}

	if resolved <= 0 {
		// An exhausted row has spent its whole budget, so the budget is the count it must
		// report. This is the path a caller that states nothing and a row whose counter was
		// never read back both take.
		resolved = row.MaxAttempts
	}

	if resolved <= 0 {
		return 1
	}

	return resolved
}

// marshalFailureMetadata serialises the failure metadata for storage and for the
// message.
//
// Parameters:
//   - metadata model.FailureMetadata: the record to serialise.
//
// Returns:
//   - json.RawMessage: the serialised metadata.
//   - error: a typed internal error when the record cannot be marshaled, which can only
//     happen if the struct gains an unmarshalable field.
func marshalFailureMetadata(metadata model.FailureMetadata) (json.RawMessage, error) {
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to serialise the dead-letter failure metadata",
			fmt.Errorf("blnk: marshalling failure metadata for topic %q: %w", metadata.OriginalTopic, err),
		)
	}

	return encoded, nil
}

// DecodeFailureMetadata decodes the failure metadata stored on an outbox row.
//
// The JSON null literal is treated as absent for the same reason: it is what a nullable
// column can produce, and decoding it would otherwise yield a zero-valued record that
// looks like a real one.
//
// Parameters:
//   - raw json.RawMessage: the stored bytes. May be nil, empty or JSON null.
//
// Returns:
//   - *model.FailureMetadata: the decoded record, or nil when there is none.
//   - error: a typed internal error when the stored bytes are not valid metadata.
func DecodeFailureMetadata(raw json.RawMessage) (*model.FailureMetadata, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, jsonNull) {
		return nil, nil
	}

	var metadata model.FailureMetadata
	if err := json.Unmarshal(trimmed, &metadata); err != nil {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to decode the stored dead-letter failure metadata",
			fmt.Errorf("blnk: decoding failure metadata: %w", err),
		)
	}

	return &metadata, nil
}

// ComposeDeadLetterMessage builds the dead-letter message for a row: the event
// envelope, unaltered, with the failure metadata attached as an additive sibling
// member.
//
// Parameters:
//   - row model.EventOutbox: the row whose envelope is composed.
//   - metadata json.RawMessage: the serialised failure metadata. Must be non-empty,
//     valid JSON.
//
// Returns:
//   - []byte: the dead-letter message value.
//   - error: a typed internal error when the envelope cannot be built or the metadata
//     is not valid JSON.
func ComposeDeadLetterMessage(row model.EventOutbox, metadata json.RawMessage) ([]byte, error) {
	trimmedMetadata := bytes.TrimSpace(metadata)
	if len(trimmedMetadata) == 0 {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Cannot compose a dead-letter message without failure metadata",
			fmt.Errorf("blnk: no failure metadata supplied for event %q", row.EventID),
		)
	}
	if !json.Valid(trimmedMetadata) {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The dead-letter failure metadata is not valid JSON",
			fmt.Errorf("blnk: invalid failure metadata for event %q", row.EventID),
		)
	}

	// THE STORED CANONICAL ENVELOPE, spliced onto rather than rebuilt. Re-serialising here
	// would make that equality hold only within one build of Blnk.
	envelope, _, err := row.CanonicalEventBytes()
	if err != nil {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to serialise the event for its dead-letter topic",
			fmt.Errorf("blnk: composing the dead-letter envelope for event %q: %w", row.EventID, err),
		)
	}

	envelope = bytes.TrimRight(envelope, " \t\r\n")
	if len(envelope) < 2 || envelope[len(envelope)-1] != '}' || envelope[len(envelope)-2] == '{' {
		// Unreachable while marshalLedgerEvent emits all six members, and checked because
		// the splice below is only valid for an object that already has one.
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The serialised event is not a JSON object that failure metadata can be attached to",
			fmt.Errorf("blnk: unexpected envelope shape for event %q", row.EventID),
		)
	}

	message := make([]byte, 0, len(envelope)+len(failureMetadataMember)+len(trimmedMetadata)+1)
	message = append(message, envelope[:len(envelope)-1]...)
	message = append(message, failureMetadataMember...)
	message = append(message, trimmedMetadata...)
	message = append(message, '}')

	return message, nil
}

// StripFailureMetadata recovers the original event envelope from a dead-letter message.
//
// It is IDEMPOTENT: a message that carries no failure metadata is returned unchanged. A
// caller need not know whether it is holding an original or a dead-lettered message,
// which is what makes this safe to apply on the way into a comparison.
//
// Parameters:
//   - message []byte: a dead-letter message value, or an ordinary event envelope.
//
// Returns:
//   - []byte: the original envelope bytes. A fresh slice when metadata was removed, and
//     the input itself when there was none.
//   - error: a typed internal error when the input is not a JSON object, or when the
//     metadata member is present but the object does not end as it must.
func StripFailureMetadata(message []byte) ([]byte, error) {
	trimmed := bytes.TrimSpace(message)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"A dead-letter message must be a JSON object",
			errors.New("blnk: the dead-letter message is not a JSON object"),
		)
	}

	index := bytes.LastIndex(trimmed, []byte(failureMetadataMember))
	if index < 0 {
		// No attachment. Either an ordinary envelope or a message whose metadata has
		// already been stripped; both are returned as they arrived.
		if bytes.Contains(trimmed, []byte(failureMetadataKey)) {
			return nil, apierror.NewAPIError(
				apierror.ErrInternalServer,
				"The dead-letter message carries failure metadata in an unexpected position",
				errors.New("blnk: failure metadata is present but is not the final member"),
			)
		}

		return message, nil
	}

	envelope := make([]byte, 0, index+1)
	envelope = append(envelope, trimmed[:index]...)
	envelope = append(envelope, '}')

	return envelope, nil
}

// normalizeDeadLetterListOptions validates and normalises a listing request.
//
// Page bounds are clamped rather than rejected, because a caller asking for a page that
// is too large or an offset below zero has made a recoverable mistake and degrading to
// a sane page is more useful than an error. The two FILTERS are the exceptions, and
// both are rejected rather than degraded for the same reason: a filter that cannot
// match anything returns an empty page, and an empty page reads to an operator as
// "nothing is stuck", which is the wrong answer to a question they did not ask.
//
//   - An unrecognised status is rejected. The inventory contains exactly two states and a
//     third would match no row.
//   - A REVERSED occurrence window — From strictly after To — is rejected. It is
//     unsatisfiable by construction, so no row can ever be inside it.
//
// A window whose ends are equal is NOT reversed and is accepted: both bounds are
// inclusive, so it selects the events at exactly that instant, which is a legitimate
// thing to ask for when correlating against a precise timestamp.
//
// Parameters:
//   - opts DeadLetterListOptions: the caller's request.
//
// Returns:
//   - DeadLetterListOptions: the normalised request, with filters trimmed.
//   - error: ErrGenValidation when the status filter is not a terminal failure state,
//     or when the occurrence window is reversed.
func normalizeDeadLetterListOptions(opts DeadLetterListOptions) (DeadLetterListOptions, error) {
	if opts.Limit <= 0 {
		opts.Limit = defaultDeadLetterListLimit
	}
	if opts.Limit > maxDeadLetterListLimit {
		opts.Limit = maxDeadLetterListLimit
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}

	opts.EventType = strings.TrimSpace(opts.EventType)
	opts.Topic = strings.TrimSpace(opts.Topic)
	opts.Status = strings.TrimSpace(opts.Status)

	switch opts.Status {
	case "",
		model.EventOutboxStatusDeadLettered,
		model.EventOutboxStatusFailed:
	default:
		return opts, apierror.NewAPIError(
			apierror.ErrGenValidation,
			fmt.Sprintf(
				"A dead-letter status filter must be %q or %q",
				model.EventOutboxStatusFailed,
				model.EventOutboxStatusDeadLettered,
			),
			fmt.Errorf("blnk: unsupported dead-letter status filter %q", opts.Status),
		)
	}

	if !opts.OccurredFrom.IsZero() && !opts.OccurredTo.IsZero() && opts.OccurredFrom.After(opts.OccurredTo) {
		return opts, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"The start of a dead-letter occurrence window must not be after its end: "+
				"occurred_from must be earlier than or equal to occurred_to",
			fmt.Errorf(
				"blnk: reversed dead-letter occurrence window: from %s is after to %s",
				opts.OccurredFrom.UTC().Format(time.RFC3339Nano),
				opts.OccurredTo.UTC().Format(time.RFC3339Nano),
			),
		)
	}

	return opts, nil
}

// replayAttemptNumber returns the attempt label a replay is recorded under.
//
// It is one past the exhausted budget, so a replay is visible in the attempt metrics
// without contaminating the attempt="1" series that the publish-latency target is read
// from. The larger of the row's counter and its budget is used, so the label is past
// both.
//
// Parameters:
//   - row model.EventOutbox: the dead-lettered row.
//
// Returns:
//   - int: an attempt number strictly greater than one.
func replayAttemptNumber(row model.EventOutbox) int {
	attempts := row.Attempts
	if row.MaxAttempts > attempts {
		attempts = row.MaxAttempts
	}
	if attempts < 1 {
		attempts = 1
	}

	return attempts + replayAttemptOffset
}

// isNotFoundError reports whether an error means "no such row".
//
// Parameters:
//   - err error: the error to classify. May be nil.
//
// Returns:
//   - bool: true when the error means the row does not exist.
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, sql.ErrNoRows) {
		return true
	}

	isNotFoundCode := func(code apierror.ErrorCode) bool {
		switch apierror.Normalize(code) {
		case apierror.ErrGenNotFound, apierror.ErrEventNotFound:
			return true
		default:
			return false
		}
	}

	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		return isNotFoundCode(apiErr.Code)
	}

	var apiErrPtr *apierror.APIError
	if errors.As(err, &apiErrPtr) && apiErrPtr != nil {
		return isNotFoundCode(apiErrPtr.Code)
	}

	return false
}

// isConflictError reports whether an error means "the row exists but is not in the
// state this operation requires".
//
// It is the companion to isNotFoundError, and it exists because the replay claim can
// fail for two reasons that call for different answers: no such event, or an event
// whose status is not dead_lettered — including one a concurrent replay already holds.
// Collapsing them would tell an operator who replayed twice that their id was wrong.
//
// Both the legacy and the canonical conflict codes are accepted because the repository
// layer still constructs the legacy one; the classification is by CODE because APIError
// does not unwrap to the error it wrapped.
//
// Parameters:
//   - err error: the error to classify. May be nil.
//
// Returns:
//   - bool: true when the error means the row is in the wrong state.
func isConflictError(err error) bool {
	if err == nil {
		return false
	}

	isConflictCode := func(code apierror.ErrorCode) bool {
		return apierror.Normalize(code) == apierror.ErrGenConflict
	}

	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		return isConflictCode(apiErr.Code)
	}

	var apiErrPtr *apierror.APIError
	if errors.As(err, &apiErrPtr) && apiErrPtr != nil {
		return isConflictCode(apiErrPtr.Code)
	}

	return false
}

// ---------------------------------------------------------------------------
// Blnk-instance entry points

// EventDeadLetters returns a dead-letter service bound to this instance's datasource.
//
// The returned service resolves a publisher from live configuration on first use, so
// the CALLER MUST CLOSE IT — `defer service.Close()` — or the connections that
// publisher opens are held until the process ends. Prefer NewEventDeadLetterService
// with an already-built publisher wherever one is available.
//
// Returns:
//   - *EventDeadLetterService: a ready service the caller owns.
func (b *Blnk) EventDeadLetters() *EventDeadLetterService {
	if b == nil {
		return NewEventDeadLetterService(nil, nil)
	}

	return NewEventDeadLetterService(b.datasource, nil)
}

// ListDeadLetterEvents pages the dead-letter inventory. It is the read behind GET
// /events/dead-letter.
//
// No publisher is resolved: the inventory is read from the outbox table, so this works
// with the broker down and costs one query.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - opts DeadLetterListOptions: the page and its optional narrowing.
//
// Returns:
//   - []model.EventOutbox: the matching entries.
//   - error: as EventDeadLetterService.ListDeadLetterEvents.
func (b *Blnk) ListDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (model.DeadLetterInventoryPage, error) {
	return b.EventDeadLetters().ListDeadLetterEvents(ctx, opts)
}

// ListAndCountDeadLetterEvents pages the inventory and counts it from one snapshot. It
// is the read behind GET /events/dead-letter?include_count=true.
//
// Both are read inside one read-only REPEATABLE READ transaction here, so they describe
// one population.
//
// Parameters:
//   - ctx context.Context: cancels the read.
//   - opts DeadLetterListOptions: the page and its narrowing.
//
// Returns:
//   - model.DeadLetterInventoryPage: the page.
//   - int64: the total the same narrowing matches, as at that page.
//   - error: as EventDeadLetterService.ListAndCountDeadLetterEvents.
func (b *Blnk) ListAndCountDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (model.DeadLetterInventoryPage, int64, error) {
	return b.EventDeadLetters().ListAndCountDeadLetterEvents(ctx, opts)
}

// CountDeadLetterEvents counts the inventory a listing with the same options pages
// through. It is the total behind a standalone count.
//
// It narrows through the SAME options type the listing takes, so a total is always of
// the set the page came from.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - opts DeadLetterListOptions: the narrowing. Limit and Offset are ignored.
//
// Returns:
//   - int64: the number of matching entries.
//   - error: as EventDeadLetterService.CountDeadLetterEvents.
func (b *Blnk) CountDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (int64, error) {
	return b.EventDeadLetters().CountDeadLetterEvents(ctx, opts)
}

// ReplayDeadLetteredEvent replays one dead-lettered event to its original topic. It is
// the write behind POST /events/dead-letter/:event_id/replay.
//
// Parameters:
//   - ctx context.Context: cancels the lookup, the publish and the recording.
//   - eventID string: the event's UUID.
//
// Returns:
//   - ReplayOutcome: the record of the replay.
//   - error: as EventDeadLetterService.ReplayDeadLetteredEvent.
func (b *Blnk) ReplayDeadLetteredEvent(ctx context.Context, eventID string) (ReplayOutcome, error) {
	service := b.EventDeadLetters()
	defer closeDeadLetterService(service)

	return service.ReplayDeadLetteredEvent(ctx, eventID)
}

// RefreshDeadLetterAgeGauge recomputes and publishes the dead-letter age gauge for this
// instance. It is what the statistics endpoint and any periodic caller invoke.
//
// No publisher is resolved: the ages come from the outbox table.
//
// Parameters:
//   - ctx context.Context: cancels the counts and the walk.
//
// Returns:
//   - DeadLetterAgeReport: the ages the gauge was set from.
//   - error: as EventDeadLetterService.RefreshDeadLetterAgeGauge.
func (b *Blnk) RefreshDeadLetterAgeGauge(ctx context.Context) (DeadLetterAgeReport, error) {
	return b.EventDeadLetters().RefreshDeadLetterAgeGauge(ctx)
}

// closeDeadLetterService closes a service built for one operation, logging rather than
// propagating a close failure.
//
// It exists so the deferred close reads as one call and so the error is handled exactly
// once, in one place: errcheck is satisfied, and a teardown problem cannot be mistaken
// for an operation failure.
//
// Parameters:
//   - service *EventDeadLetterService: the service to close. May be nil.
func closeDeadLetterService(service *EventDeadLetterService) {
	if err := service.Close(); err != nil {
		withLoggableCause(nil, err).Warn("closing the short-lived dead-letter event publisher failed")
	}
}

// countFailedAwaitingDeadLetter reads how many rows have spent their retry budget
// without reaching a dead-letter topic.
//
// The window argument is gone with the reason for it.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//
// Returns:
//   - int64: the number of rows awaiting a dead-letter write.
//   - error: the repository's own typed error.
func (s *EventDeadLetterService) countFailedAwaitingDeadLetter(ctx context.Context) (int64, error) {
	counts, err := s.store.CountUnresolvedEventOutbox(ctx)
	if err != nil {
		return 0, err
	}

	// A status with no rows is absent from the map rather than present with a zero, so the
	// two-value read is not optional — but the zero value is the correct reading of an
	// absent key here, which is what makes the single-value form safe.
	return counts[model.EventOutboxStatusFailed], nil
}
