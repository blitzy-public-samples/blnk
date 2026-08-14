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

// failureMetadataMember is the top-level JSON member the failure metadata is attached
// under, separator and colon included, ready to splice.
const failureMetadataMember = `,"failure_metadata":`

// failureMetadataKey is the bare member name, used when a stored dead-letter message has
// to be taken apart again by StripFailureMetadata.
const failureMetadataKey = `"failure_metadata"`

// unrecordedDeadLetterReason is the error reason stored when an event is dead-lettered
// with no failure cause available from either the caller or the row.
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
	Published bool

	// Status is the pipeline-level outcome, always model.PublishStatusDeadLettered on a
	// successful return. It is reported through the same vocabulary the metrics layer is
	// attributed by, so a caller never has to translate.
	Status model.PublishStatus
}

// LogFields renders the outcome as logrus fields.
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
	deadLetterFailureClassTooLarge = "message_too_large"

	// deadLetterFailureClassTopicUnavailable is "the topic Blnk asked for was not there
	// as far as this principal is concerned". The name states BOTH possibilities on
	// purpose, because the broker does not distinguish them for us: a principal that
	// holds no Describe on a topic is answered UNKNOWN_TOPIC_OR_PARTITION rather than a
	// denial, so an ACL gap and a genuinely missing topic arrive as the same sentence.
	// Classifying that sentence as a defect would send an operator to read the publisher
	// code when the remedy is `make kafka_provision` or an ACL grant; classifying it as
	// broker_unavailable would send them to a cluster that is perfectly healthy. This
	// class exists so the log names the two things worth checking and nothing else.
	// An explicit TOPIC_AUTHORIZATION_FAILED is NOT this class — it is a denial the
	// broker was willing to state, and the auth signatures above claim it first.
	deadLetterFailureClassTopicUnavailable = "topic_missing_or_unauthorized"

	// deadLetterFailureClassSerialization is something BLNK produced that could not be
	// encoded or that Kafka rejected as malformed: a payload that would not serialise, or
	// a topic name that violates Kafka's naming rules. A defect, not a transient
	// condition — retrying it unchanged cannot help. A topic that is merely absent or
	// invisible to the principal is deadLetterFailureClassTopicUnavailable instead.
	deadLetterFailureClassSerialization = "serialization"

	// deadLetterFailureClassClosed is a publish attempted through a closed transport,
	// which means the process was shutting down.
	deadLetterFailureClassClosed = "transport_closed"

	// deadLetterFailureClassNone is "no reason was recorded". Distinct from unclassified:
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

	// The topic signatures are claimed BEFORE the serialisation and broker blocks below,
	// and that ordering is the whole point of them. kafka-go renders error 3 as
	// "[3] Unknown Topic Or Partition: the request is for a topic or partition that does
	// not exist on this broker" — a sentence that contains the word "broker", so the
	// broker block would otherwise swallow it and blame a healthy cluster.
	{"unknown topic", deadLetterFailureClassTopicUnavailable},
	{"partition that does not exist", deadLetterFailureClassTopicUnavailable},
	{"topic does not exist", deadLetterFailureClassTopicUnavailable},

	{"marshal", deadLetterFailureClassSerialization},
	{"unmarshal", deadLetterFailureClassSerialization},
	{"serial", deadLetterFailureClassSerialization},
	// encoding/json prefixes every one of its errors with "json: ", and a JSON error is by
	// definition a serialisation failure however it is worded. The prefix is a far more
	// reliable signal than any of its individual messages, none of which contains the word
	// "marshal" — "json: unsupported type: chan int" being the one that made this obvious.
	{"json:", deadLetterFailureClassSerialization},
	{"unsupported type", deadLetterFailureClassSerialization},
	// An INVALID topic name stays here: error 17 is Kafka refusing a name as illegal, or
	// refusing a write to an internal topic. Both are things Blnk composed wrongly, which
	// is a defect in the same sense a payload that will not encode is.
	{"invalid topic", deadLetterFailureClassSerialization},

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
func errorText(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

// classifyDeadLetterFailure reduces a recorded failure reason to one value from the
// fixed vocabulary above.
//
// It feeds TWO log fields that describe TWO DIFFERENT ERRORS, and reading them as one is
// the mistake this comment exists to prevent:
//
//   - `failure_class` classifies Metadata.ErrorReason — why the ORIGINAL publish to the
//     category topic failed, i.e. the reason the event is being dead-lettered at all.
//   - `failure_detail_class` classifies the error of the step being logged — why the
//     dead-letter WRITE failed, why the row could not be marked, or on a replay why the
//     re-publish or its bookkeeping failed.
//
// So one record can legitimately read failure_class=authorization_denied with
// failure_detail_class=broker_unavailable: the event was refused a topic it may not
// write, and the attempt to preserve it then hit a cluster that had gone away. The two
// are classified through the same vocabulary precisely so they are comparable.
//
// The API's failure_reason (api/model.classifyFailureReason) is a THIRD, independent
// classification of the same stored text, deliberately narrower: it splits `timeout` and
// `persistence_failure` out and has no serialisation value. docs/kafka-operations.md
// carries the full mapping between the two so a script can move between them.
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
	Topic string

	// Status narrows to one failure state. Only the two states the inventory contains are
	// accepted — model.EventOutboxStatusFailed and model.EventOutboxStatusDeadLettered —
	// and anything else is rejected as a validation error rather than silently matching
	// nothing, because a filter that quietly returns an empty page reads as "nothing is
	// stuck" and is exactly the wrong answer to give an operator.
	Status string

	// OccurredFrom and OccurredTo bound the event's OCCURRENCE instant inclusively, and
	// either may be zero to leave that end unbounded.
	OccurredFrom time.Time
	OccurredTo   time.Time
}

// filtered reports whether any narrowing was requested.
func (o DeadLetterListOptions) filtered() bool {
	return strings.TrimSpace(o.EventType) != "" ||
		strings.TrimSpace(o.Topic) != "" ||
		strings.TrimSpace(o.Status) != "" ||
		!o.OccurredFrom.IsZero() ||
		!o.OccurredTo.IsZero()
}

// deadLetterQuery translates the service's options into the repository's narrowing
// contract.
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
	CountDeadLetteredEvents(ctx context.Context, query model.DeadLetterQuery) (int64, error)

	// CountDeadLetterInventory counts what a listing query matches, sharing the INVENTORY
	// listing's predicate so a paging caller can be told the true size of its backlog.
	CountDeadLetterInventory(ctx context.Context, query model.DeadLetterQuery) (int64, error)

	// ListAndCountDeadLetterInventory answers the page and its total from ONE SNAPSHOT,
	// and is what a listing that asked for a total reads.
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
	// settleLegacyLeg is always FALSE here, and that is the point: a replay says nothing
	// about the legacy webhook leg, which may still be owed on a row that was dead-lettered
	// before its enqueue succeeded. Folding a marker in on a replay would strand that leg.
	MarkEventDispatched(ctx context.Context, id int64, claimToken string, record model.BrokerRecord, settleLegacyLeg bool) error

	// ClaimEventForReplay atomically moves a dead-lettered row to replaying and returns it
	// with a fresh claim token, so a replay is a CLAIM rather than a read followed by a
	// check.
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
