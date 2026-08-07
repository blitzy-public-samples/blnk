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
// Five responsibilities that live NEXT DOOR are deliberately absent, because a second
// implementation of any of them would be a second source of truth for one decision:
//
//   - The PUBLISHER and its writers belong to event_publisher.go. Replay goes through
//     that publisher's PublishToTopic, and the one raw write this file performs borrows
//     a writer from the publisher's own per-topic pool rather than building one.
//   - TOPIC AND DEAD-LETTER NAMING belongs to event_topics.go. Every destination here
//     is resolved with DLTFor; the `.dlt` suffix is never spelled out in this file.
//   - The RETRY SCHEDULE and the relay loop belong to event_relay.go. Nothing here
//     sleeps, counts attempts against a budget, or decides that an event has failed for
//     the last time — this file is told that it has.
//   - Every SQL STATEMENT belongs to database/event_outbox.go. This file calls
//     repository methods and writes no SQL.
//   - The HTTP ENDPOINTS and their master-key gate belong to api/events.go. This file
//     returns typed apierror codes and never an HTTP status.
//
// # SCOPE BOUNDARY: subscriber-side dead-lettering is NOT Blnk's, and must not be built here
//
// Read "dead letter" in this file as "Blnk's own dead-letter topics" and nothing wider.
// Blnk PUBLISHES THE `<topic>.dlt` NAMING CONVENTION AND STOPS THERE. The convention is
// documented externally (see docs/event-streaming.md) for one reason: so that a
// subscriber building its own consumer-side dead-lettering does not collide with a
// Blnk-owned topic name.
//
// Blnk therefore does NOT, and this file must never grow, any of the following:
//
//   - a consumer or consumer-group library of any kind,
//   - subscriber-side dead-letter management: creating, reading, draining or replaying a
//     dead-letter topic that a subscriber owns,
//   - a consumer error-handling or poison-message framework.
//
// A subscriber's consumption failures are the subscriber's to handle. Nothing here reads
// from Kafka at all: the listing and the replay both work from the outbox row, which
// already carries dlt_topic and failure_metadata precisely so that no consumer is
// needed. A future contributor reaching for a kafka.Reader in this file is a sign the
// boundary has been misread.

// failureMetadataMember is the top-level JSON member the failure metadata is attached
// under, separator and colon included, ready to splice.
//
// It is spelled out once, here, because the member name is a published contract: the
// dead-letter triage runbook reads it, and the API projection decodes it. The leading
// comma makes it an ADDITIVE splice onto an envelope that already has members, which is
// the whole mechanism behind byte-faithful replay — see ComposeDeadLetterMessage.
const failureMetadataMember = `,"failure_metadata":`

// failureMetadataKey is the bare member name, used when a stored dead-letter message has
// to be taken apart again by StripFailureMetadata.
const failureMetadataKey = `"failure_metadata"`

// unrecordedDeadLetterReason is the error reason stored when an event is dead-lettered
// with no failure cause available from either the caller or the row.
//
// An EMPTY reason is the failure mode this constant exists to prevent. FailureMetadata
// exists to answer "why did this event not get there", and an empty string answers
// nothing while looking like a successful read of a missing value. This is a real,
// reachable state: a row can be dead-lettered after a relay restart lost the in-memory
// error, and last_error can be NULL if the row reached its budget through claims that
// never recorded a reason.
const unrecordedDeadLetterReason = "the retry budget was exhausted; no failure reason was recorded"

// Dead-letter listing and gauge-scan bounds.
//
// The page-size values match database/event_outbox.go's own defaults exactly, and that
// alignment is deliberate: the unfiltered listing path passes its normalised page
// straight through to the repository, so a caller sees identical bounds whether or not a
// filter was supplied. Duplicating the numbers is not duplication of policy — the
// repository still enforces its own bounds, and these make the service's behaviour
// explicit at the call site instead of implicit in a layer below.
const (
	// defaultDeadLetterListLimit is the page size for a request that names none.
	defaultDeadLetterListLimit = 50

	// maxDeadLetterListLimit is the ceiling on a single page, so a triage endpoint
	// cannot be turned into a full-table scan.
	maxDeadLetterListLimit = 500

	// deadLetterScanPageSize is the page size used when walking the inventory: for the
	// filtered listing, and for the oldest-entry scan that maintains the age gauge. It
	// is the repository's own maximum, so each page is one query doing as much useful
	// work as the repository will allow.
	deadLetterScanPageSize = maxDeadLetterListLimit

	// deadLetterScanMaxRows bounds how many rows either walk will examine.
	//
	// The bound exists because both walks are unbounded in principle — the inventory
	// could be arbitrarily large — and neither is worth an unbounded scan. It is set
	// where it is because the dead-letter rate is required to stay under 0.1% of
	// events, so an inventory beyond this size is itself the incident, and a gauge
	// computed from the oldest 5,000 of those rows is not going to be the reason the
	// alert does or does not fire. A truncated walk is logged, never silently
	// swallowed.
	deadLetterScanMaxRows = 5000
)

// replayAttemptOffset is added to an exhausted row's attempt count to label the
// publish-duration histogram for a REPLAY.
//
// Replays must not be recorded as attempt 1. The publish-latency target is read as the
// p99 of the histogram filtered to attempt="1" — first-attempt, non-retried publishes —
// and an operator-triggered replay of an event that has been sitting in a dead-letter
// topic for hours is not that. Labelling it one past the exhausted budget keeps it
// visible, keeps the label set small and bounded, and keeps it out of the reading the
// target is measured against.
const replayAttemptOffset = 1

// DeadLetterRequest is the explicit form of a dead-letter publication: the row that has
// exhausted its retry budget, why it failed, and the attempt window it failed over.
//
// Only Row is required. Every other field has a documented fallback drawn from the row
// itself, so `DeadLetterRequest{Row: row, Cause: err}` is a complete request — which is
// exactly what the DeadLetter convenience method submits. The overrides exist because
// the relay knows two things the row cannot tell you: the attempt number it actually
// reached inside a single claim (the row's own counter is only as fresh as its last
// database write), and the error from the final attempt, which is in hand before it is
// ever persisted.
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

	// PartitionKey is the message key the dead-letter message was written with — the
	// same key the original publish used, so the dead-letter topic preserves the same
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
	//
	// It is populated even when nothing was published, so that a deployment with no
	// broker can still be shown what would have been written.
	Message []byte

	// Published reports whether a broker ACKNOWLEDGED the dead-letter message.
	//
	// On a nil-error return it is always true, and that is the contract: an event is
	// reported as dead-lettered only when its dead-letter message exists on the broker.
	// It is false only on the partially-populated outcome returned alongside an error,
	// where it says "the message was composed but never landed" — which is exactly what
	// the row's non-terminal status then also says.
	//
	// It was previously false-with-no-error whenever the deployment had no Kafka
	// transport, and the row was recorded as dead-lettered anyway. See PublishToDeadLetter
	// for why that combination was unsafe.
	Published bool

	// Status is the pipeline-level outcome, always model.PublishStatusDeadLettered on a
	// successful return. It is reported through the same vocabulary the metrics layer is
	// attributed by, so a caller never has to translate.
	Status model.PublishStatus
}

// LogFields renders the outcome as logrus fields.
//
// It mirrors PublishResult.LogFields so that a dead-letter log line and a publish log
// line share field names and one query can follow an event across both. The message
// bytes are reported as a LENGTH rather than a value: the payload can be arbitrarily
// large and is already durable in the outbox row, so logging it would bloat the log
// without adding anything an operator cannot fetch.
//
// # Two fields are deliberately not what the outcome holds
//
// THE PARTITION KEY IS HASHED. It is derived from a balance, transaction or identity
// identifier, so emitting it raw copies a financial identifier into the log stream —
// which is shipped off the host, retained on its own schedule and readable by a wider
// set of people than may query the ledger. What a log needs from the key is that the
// same key always renders the same token, so two lines can be recognised as belonging to
// one aggregate; hashLogIdentifier keeps exactly that. The same treatment the publisher
// already applies to its own log line, and for the same reason.
//
// THE FAILURE REASON IS CLASSIFIED **AND** BOUNDED, and both fields are present because
// they answer different questions.
//
// failure_class is one value from a fixed vocabulary. It is what a log query groups on and
// what turns "dead letters are rising" into "look at the cluster" or "look at the
// principal's grants", and being closed it can do that without unbounded cardinality.
//
// error_reason is the broker's or the driver's own words, and removing it entirely was the
// wrong trade: "broker_unavailable" does not distinguish a broken pipe from a leaderless
// partition, so an operator reading the class alone still has to go and fetch the text
// before they can act — at exactly the moment the broker they would fetch it through is the
// thing that is broken. What made the raw value unsuitable is real, though: a kafka-go error
// surfaces broker hostnames and ports through *net.OpError, its length is bounded by
// nothing, and a newline in it forges a second entry in a line-oriented aggregator.
//
// So it is SANITIZED rather than dropped — control characters removed, newlines folded to
// spaces, and the result capped with a visible truncation marker — which is the same
// treatment internal/notification applies to a system error's text for the same reasons.
// The verbatim, unbounded value stays where it is already protected and where triage can
// still reach it: the dead-lettered message's failure_metadata, which the dead-letter API
// returns, and the row's last_error.
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
		"error_reason":       sanitizeLogValue(o.Metadata.ErrorReason, maxLoggedErrorLength),
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
//
// The classes are chosen to answer the only question a log line has to answer, which is
// where to look next. Broker, authorisation, size and serialisation failures each send
// an operator somewhere different, and that is the whole value of the distinction — the
// exact text belongs in the dead-letter record, not here.
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

	// deadLetterFailureClassNone is "no reason was recorded". Distinct from
	// unclassified: it means the row carried nothing, which is itself a defect worth
	// seeing rather than a reason this function failed to place.
	deadLetterFailureClassNone = "unrecorded"

	// deadLetterFailureClassOther is everything else. It exists so the vocabulary stays
	// closed; a rising count here is the signal that a class is missing.
	deadLetterFailureClassOther = "other"
)

// deadLetterFailureSignatures maps a lower-cased substring of a recorded reason onto its
// class, in priority order.
//
// Substring matching, because the reason is a STRING by the time it reaches here — the
// error value itself was consumed when the failure was recorded, possibly in another
// process on an earlier claim, so errors.As is not available. The order matters: the
// authorisation signatures are tested before the generic connection ones, because a
// SASL failure reported as a connection error would otherwise be filed as a broker
// outage and send an operator to the wrong place entirely.
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
// It exists so the classifier can be given a live error value on the paths that hold one,
// without any caller being tempted to log the string it produces. The distinction matters:
// classifyDeadLetterFailure returns no part of its input, so passing raw error text
// through it is safe, while passing that same text to a log field would be exactly the
// exposure this section removes.
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

// classifyDeadLetterFailure reduces a recorded failure reason to one value from the fixed
// vocabulary above.
//
// It returns a CLASS and never any part of its input, which is the property that makes it
// safe to log: whatever the broker or the driver wrote — an address, a schema name, a
// constraint, an arbitrarily long chain of wrapped errors — cannot reach the log stream
// through this function's return value.
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

// ReplayOutcome is the record of one replay: which event went back to which topic, under
// which key, and when.
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

	// PartitionKey is the key the replay was published with — the same key as the
	// original publish, so the replay lands on the same partition and cannot itself
	// violate per-aggregate ordering.
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
// filters are exact-match and optional, and they are applied BY THIS SERVICE rather than
// in SQL, because the repository's listing is deliberately a simple index-backed page
// and pushing predicates into it would mean a second, filter-aware query to maintain.
// The cost is bounded and documented on ListDeadLetterEvents.
type DeadLetterListOptions struct {
	// Limit is the maximum number of entries to return. Zero or negative selects
	// defaultDeadLetterListLimit; anything above maxDeadLetterListLimit is clamped to it.
	Limit int

	// Offset is how many matching entries to skip. Negative is clamped to zero.
	Offset int

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
}

// filtered reports whether any narrowing was requested, which is what decides between
// the single pass-through query and the bounded walk.
//
// Returns:
//   - bool: true when at least one filter is set.
func (o DeadLetterListOptions) filtered() bool {
	return strings.TrimSpace(o.EventType) != "" ||
		strings.TrimSpace(o.Topic) != "" ||
		strings.TrimSpace(o.Status) != ""
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
	// plus rows whose retry budget is spent but which have not reached a dead-letter
	// topic yet. Both are counted because both are events an operator has to act on.
	Outstanding int64

	// OldestByTopic maps a dead-letter topic to the age of the OLDEST unresolved entry
	// destined for it. Every dead-letter topic Blnk owns is present, and a topic with
	// nothing outstanding maps to zero — the gauge must fall back to zero rather than
	// hold a stale age after the last entry is cleared.
	OldestByTopic map[string]time.Duration

	// FailedAwaitingDeadLetter is the subset of Outstanding whose retry budget is spent but
	// which has NOT reached a dead-letter topic yet.
	//
	// It is broken out because the two populations need different responses. A
	// dead-lettered event is on a topic an operator can list and replay; an event in this
	// state is on no topic at all, because the dead-letter WRITE itself failed. Collapsing
	// them into one number makes a broker that is refusing dead-letter writes look exactly
	// like a busy triage queue.
	//
	// It counts the one pre-dead-letter literal: failed, which the exhaustion arm sets and in
	// which the hand-off stays re-claimable for as long as dlt_topic is NULL. A steadily
	// non-zero value means dead-letter writes are failing, not that events are momentarily
	// in flight.
	FailedAwaitingDeadLetter int64

	// Scanned is how many rows were examined.
	Scanned int

	// Truncated reports that Outstanding exceeded the scan bound, so the ages are drawn
	// from the OLDEST deadLetterScanMaxRows entries rather than from all of them. The
	// walk starts at the oldest end precisely so that a truncated scan still reports the
	// oldest entry it can see, making the reported age a lower bound that only ever
	// understates by rows even older than the ones examined.
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
//
// Depending on a five-method interface rather than on the whole ten-sub-interface
// IDataSource is what makes every operation in this file testable with a small fake, and
// it documents the blast radius precisely: dead-lettering reads one row, pages the
// inventory, counts by status, and drives exactly two state transitions. It can neither
// insert an event nor claim one, so it cannot accidentally take part in the relay's job.
//
// MarkEventFailed is POINTEDLY ABSENT. It increments the attempts counter, which is the
// relay's per-attempt bookkeeping; calling it from the dead-letter path would spend a
// sixth attempt against a five-attempt budget and make the attempt count reported in the
// failure metadata disagree with the configured maximum. The exhaustion arm of
// MarkEventFailed has already moved the row to failed by the time this file is called;
// MarkEventDeadLettered completes the transition.
type eventDeadLetterStore interface {
	// GetEventByID fetches one row by its business event_id, returning a typed
	// not-found error when nothing matches.
	GetEventByID(ctx context.Context, eventID string) (*model.EventOutbox, error)

	// ListDeadLetteredEvents pages the inventory, newest occurrence first, over both
	// terminal failure states.
	ListDeadLetteredEvents(ctx context.Context, limit, offset int) ([]model.EventOutbox, error)

	// CountEventOutboxByStatus returns a status-keyed count of every row, which is how
	// the age scan sizes its window without a bespoke query.
	CountEventOutboxByStatus(ctx context.Context) (map[string]int64, error)

	// MarkEventDeadLettered records the dead-letter topic and metadata and moves the row
	// to its dead-lettered terminal state, CONDITIONAL on the caller still holding the
	// row's claim token. It returns a conflict when the claim has been lost, which is
	// what stops two workers each writing the event to the dead-letter topic.
	MarkEventDeadLettered(ctx context.Context, id int64, claimToken, dltTopic string, failureMetadata json.RawMessage, record model.BrokerRecord) error

	// MarkEventDispatched is the post-replay transition: a successfully replayed row
	// becomes dispatched, which removes it from the dead-letter inventory and makes a
	// second replay attempt fail closed. It too is conditional on the claim token —
	// here, the one ClaimEventForReplay issued.
	MarkEventDispatched(ctx context.Context, id int64, claimToken string, record model.BrokerRecord) error

	// ClaimEventForReplay atomically moves a dead-lettered row to replaying and returns
	// it with a fresh claim token, so a replay is a CLAIM rather than a read followed by
	// a check.
	//
	// This is what makes concurrent replay safe. The read-then-check form let two
	// requests for one event both see a dead_lettered row, both pass the precondition,
	// and both publish — an operator clicking twice, or two operators working the same
	// backlog, putting two copies on the topic. Only the caller whose update actually
	// changed a row gets the token, and only it publishes.
	ClaimEventForReplay(ctx context.Context, eventID string, lockDuration time.Duration) (*model.EventOutbox, error)

	// ReleaseEventReplay returns a replaying row to dead_lettered, recording the reason
	// when one is given. It is the rollback that keeps a FAILED replay replayable: without
	// it the row would be stranded in replaying, outside both the relay's claimable set
	// and the dead-letter inventory, with nothing left to pick it up.
	ReleaseEventReplay(ctx context.Context, id int64, claimToken, replayErr string) error
}

// deadLetterMessageWriter is the minimum Kafka surface a dead-letter write needs.
//
// *kafka.Writer satisfies it as declared, with no adapter, which is the point: the
// production path borrows a writer from the publisher's own per-topic pool instead of
// building one, so dead-letter writes share the same connections, the same SASL session
// and the same acknowledgement settings as every other publish. A test substitutes its
// own implementation and needs no broker.
type deadLetterMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// deadLetterWriterResolver resolves the writer for a dead-letter topic.
//
// A (nil, nil) return REPORTS THAT THERE IS NO KAFKA TRANSPORT IN THIS DEPLOYMENT. It is
// not itself an error — the resolver's job is to report what exists, not to decide what
// that means — but writeDeadLetterMessage converts it into one, because a dead-letter
// write that cannot happen must not be reported as a dead-lettering that did. Keeping the
// report and the policy in different places is deliberate: the policy then lives in exactly
// one function and a test can still drive the no-transport condition through this seam.
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
//
// The publisher may be omitted, in which case one is built from live configuration on
// first use and closed by Close. That exists for the operator-facing API path, where a
// replay is a rare, human-triggered action and a short-lived publisher costs one
// connection and one SASL handshake — a price worth paying to keep the handler from
// having to own publisher lifecycle. It is the wrong choice for the relay, which
// dead-letters as part of a loop and should inject its own.
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

	// ownsPublisher records that this service built the publisher itself, and is
	// therefore the only thing allowed to close it. A publisher passed in by the relay
	// outlives this service and must not be closed by it.
	ownsPublisher bool

	// now is the clock. It is always set by the constructor, and an in-package test may
	// replace it directly so that reported ages and failure windows are exact rather than
	// approximately now.
	now func() time.Time

	// scanMaxRows bounds the filtered listing walk and the age scan.
	scanMaxRows int
}

// NewEventDeadLetterService builds the dead-letter service.
//
// Pass the publisher the process already has — the relay's — so that dead-letter writes
// and replays reuse its writers and connections. Pass nil to have one resolved from live
// configuration on first use and released by Close, which is the right choice for a
// short-lived, operator-triggered operation and the wrong one inside a loop.
//
// A nil store is accepted rather than rejected: construction is not where that becomes a
// problem, and every operation reports it with a clear error. That keeps this
// constructor free of an error return it would otherwise need for a case no production
// caller hits.
//
// Parameters:
//   - store eventDeadLetterStore: the repository. database.IDataSource satisfies it. May
//     be nil.
//   - publisher EventPublisher: the process publisher, or nil to resolve one lazily. A
//     publisher that does not implement TopicEventPublisher is treated as absent, since
//     the minimal contract cannot express a destination topic.
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
	// implementations in event_publisher.go satisfy TopicEventPublisher, so this
	// narrowing only rejects something that could not have served a dead-letter write
	// anyway — and rejecting it here means the lazy path builds a usable one instead. A
	// nil publisher fails the assertion, which is what routes it to the lazy path.
	if topicPublisher, ok := publisher.(TopicEventPublisher); ok {
		service.withTransport(topicPublisher, publisherWriterResolver(topicPublisher))
	}

	return service
}

// WithScanLimit sets how many rows the filtered listing and the age scan may examine.
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
// The two are set together, always, because a service holding one without the other is a
// state no production path can reach and one that transport() would silently repair by
// building a publisher — overwriting whatever was installed. The constructor uses it for
// an injected publisher, and an in-package test uses it to substitute a writer that
// captures what would have been written, so the composition, routing and metric behaviour
// of a dead-letter write are assertable byte for byte with no broker anywhere.
//
// ownsPublisher is cleared because an installed publisher belongs to whoever supplied it:
// the relay's publisher outlives this service and must not be closed by it.
//
// Parameters:
//   - publisher TopicEventPublisher: the publisher replays go through.
//   - resolver deadLetterWriterResolver: the writer resolution. A resolver returning no
//     writer models a deployment with no Kafka transport, which is a legitimate state.
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
//   - publisher TopicEventPublisher: the publisher to derive from. May be nil.
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

// transport returns the publisher and the dead-letter writer resolver, building them from
// live configuration on first use if none was injected.
//
// Resolution is deferred to first use rather than done in the constructor so that
// constructing the service performs no work at all, and so that a process which only ever
// LISTS dead letters — the common case for the inventory endpoint — never builds a
// publisher it does not need.
//
// # DLT-01: a configuration that cannot be READ is not a configuration that says "no Kafka"
//
// A failed configuration fetch used to be downgraded to "not configured", which resolved to
// the no-op publisher — and the no-op publisher's nil writer used to be reported as a
// successful dead-lettering. So a transient configuration failure in a deployment that runs
// Kafka every day silently produced a row marked dead_lettered, a dead-letter counter
// increment, and NO MESSAGE ANYWHERE. The event was gone, and every signal said it was safe.
//
// The two states are not the same and are no longer conflated. "Brokers are empty" is an
// OBSERVED configuration and remains a legitimate steady state that resolves to the no-op
// publisher, because that is the graceful-degradation contract the whole pipeline keeps.
// "The configuration could not be read" is an UNKNOWN state, and the honest answer to an
// unknown state on a write path is to fail: the caller retries, or an operator sees it, and
// either way the event is still in the table.
//
// This function is reached only from write paths — the dead-letter write and the replay
// publish. Listing the inventory reads the outbox and never calls it, so an operator can
// still triage with the broker down and with configuration unavailable.
//
// The other genuine error is a publisher that refuses to be built because the configured
// SASL credentials or TLS material cannot be prepared. That is a fatal misconfiguration and
// must not be silently downgraded to publishing nothing either.
//
// # THE MUTEX IS NOT HELD ACROSS CONSTRUCTION
//
// NewEventPublisher reads configuration, resolves the SASL mechanism and prepares TLS
// material — it opens files and can block. Holding the service mutex across that serialised
// every concurrent caller behind one construction, so a slow or hanging build stalled the
// relay's dead-letter hand-off, the inventory endpoint's replay and the age-gauge refresh
// together, on a lock none of them needed.
//
// The lock is therefore taken twice and released in between, with a DOUBLE CHECK on the
// second acquisition. Two callers arriving at once may both build a publisher; the first to
// install wins and the loser CLOSES the one it built, so nothing is leaked and no caller
// receives a publisher that is not the service's. That trade — a rare redundant build for a
// lock that is never held across I/O — is the standard one, and it is the only arrangement
// in which a hanging build cannot take the rest of the service with it.
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
	// event_sunset.go), used here rather than config.Fetch so that a test swapping it
	// sees consistent behaviour across every event file.
	cnf, err := fetchConfiguration()
	if err != nil {
		logrus.WithError(err).Error(
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
		// Unreachable with the implementations in event_publisher.go, both of which
		// satisfy the fuller contract, and asserted rather than assumed so that a future
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
// callers can legitimately build one at the same time and exactly one of them must throw its
// away. A publisher that is merely dropped keeps its connection pool and its SASL session for
// the lifetime of the process, so the discard has to be explicit.
//
// Close is only reachable through TopicEventPublisher, so the type assertion is the honest
// way to attempt it: a publisher that does not expose Close holds nothing to release. The
// error is logged rather than returned, because the caller is on its way to reporting a
// different, more informative outcome and a failed close on a publisher nobody will use again
// must not displace it.
//
// Parameters:
//   - publisher EventPublisher: the publisher to release. May be nil.
func closeEventPublisher(publisher EventPublisher) {
	closer, ok := publisher.(interface{ Close() error })
	if !ok || publisher == nil {
		return
	}

	if err := closer.Close(); err != nil {
		logrus.WithError(err).Warn(
			"closing a redundantly built dead-letter publisher failed; another caller's publisher " +
				"is in use and this one is discarded",
		)
	}
}

// installedTransport reports the transport if one is already installed.
//
// It is a separate method purely so that the fast path holds the lock for a field read and
// nothing else, which is what makes "the mutex is never held across construction" a property
// of the code rather than a comment about it.
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

// installTransport publishes a freshly built publisher as the service's, or discards it in
// favour of one another caller installed first.
//
// The double check is the whole point. Between the fast-path read and this call another
// goroutine may have installed its own, and returning the caller's instead would leave two
// live publishers with only one of them tracked by Close — a connection-pool and SASL-session
// leak for the lifetime of the process. The loser is closed here, immediately, while the lock
// is NOT held for the close: releasing before closing keeps the promise that this mutex never
// covers I/O.
//
// Parameters:
//   - candidate TopicEventPublisher: the publisher this caller built. Never nil.
//
// Returns:
//   - TopicEventPublisher, deadLetterWriterResolver: the installed transport, which may be
//     another caller's.
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
// It is idempotent and nil-safe, and it NEVER closes a publisher that was passed in: the
// relay's publisher outlives the service and is closed with the process. That ownership
// rule is the reason Close is safe to call from a handler's defer without having to know
// where the publisher came from.
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
// It is the terminal step of the failure path and is called by the relay on retry
// exhaustion — after RELAY_MAX_RETRY_ATTEMPTS attempts — never speculatively. Nothing
// here decides that the budget is spent; that decision, and the backoff schedule that
// leads to it, belong to the relay.
//
// # What is written, and why the shape matters
//
// The message is the ORIGINAL EVENT ENVELOPE, byte for byte, followed by one additional
// top-level member:
//
//	{ "event_id": …, "event_type": …, "aggregate_id": …, "occurred_at": …,
//	  "payload": {…}, "schema_version": 1,
//	  "failure_metadata": { "original_topic": …, "error_reason": …, "attempt_count": …,
//	                        "first_attempted_at": …, "last_attempted_at": … } }
//
// The attachment is strictly ADDITIVE: not one of the six envelope keys is rewritten,
// reordered or removed, and the metadata is never nested inside payload. That is what
// leaves the original bytes recoverable unchanged — see StripFailureMetadata — and it is
// the precondition for replaying an event byte-for-byte. Folding the metadata into the
// envelope and subtracting it later would not survive the round trip through a Go struct.
//
// The destination is resolved with DLTFor and never composed here, so the five published
// names — blnk.transactions.dlt, blnk.balances.dlt, blnk.identities.dlt,
// blnk.ledgers.dlt and blnk.system.dlt under the default prefix — have exactly one
// source of truth. The
// message keeps the ORIGINAL PARTITION KEY, so the dead-letter topic preserves the same
// per-aggregate ordering as the topic the event failed to reach.
//
// # Recording, and the order of operations
//
// The write happens first and the row is recorded second, deliberately, and THE ROW IS
// RECORDED ONLY WHEN A BROKER HAS ACKNOWLEDGED THE MESSAGE. If the write fails for any
// reason — no transport, an unresolvable writer, an unavailable broker — the row is left in
// the failed state the relay already put it in, nothing is counted, and an error is
// returned. The dead-letter listing covers the failed state precisely so that such an event
// stays visible to an operator instead of being reported as safely dead-lettered when its
// message never left the process.
//
// That ordering is what makes the terminal state mean something. dead_lettered asserts
// "there is a message on `<topic>.dlt` that this row can be replayed from", and a row is
// simultaneously removed from the relay's claimable set when it reaches that state, so
// recording it without the message would strand an event with nothing behind it and no
// process left to notice.
//
// If the RECORDING fails after a successful write, an error is returned and the caller will
// try again; the repeat produces a duplicate dead-letter message, suppressed at the
// subscriber's idempotency boundary on event_id exactly as any other redelivery is. That is
// the deliberate asymmetry: a duplicate message is recoverable at the subscriber, a missing
// one is not recoverable anywhere.
//
// The row is recorded with MarkEventDeadLettered and NOT with MarkEventFailed. The latter
// increments the attempts counter, and spending a sixth attempt against a five-attempt
// budget would make the attempt count reported in the metadata disagree with the
// configured maximum.
//
// NO PRECONDITION IS ENFORCED ON THE ROW'S CURRENT STATUS, deliberately, and there are two
// reasons rather than one. The relay calls this immediately after the exhaustion arm of
// MarkEventFailed, when the row it holds in memory still reads as processing — a status
// check against that stale value would reject every real call. And an operator has a
// legitimate need to dead-letter a row that is stuck in the failed state because its
// original dead-letter write failed, which is the one route by which such a row becomes
// replayable again. Deciding that an event is finished is the relay's job, and this
// function is told, not asked.
//
// # No broker configured, and why that is a failure here
//
// When the deployment has no Kafka transport at all — or when configuration cannot be read,
// so whether it has one is unknown — this returns ErrKafkaUnavailable and changes nothing.
// The partially-populated outcome still carries the composed message, so a caller can log or
// show what would have been written.
//
// This is the ONE place the pipeline's no-op-when-unconfigured contract does not extend to,
// and the reason is that the contract exists to let events be skipped harmlessly, whereas
// here it would let an event be DECLARED FINISHED without existing anywhere but a row that
// says it is finished. Publishing nothing and raising nothing is graceful when the event is
// still pending and still claimable; it is data loss when the row is about to leave the
// claimable set. A deployment with no brokers does not start the relay in the first place,
// so this branch is not a steady state — it is a misconfiguration, and it now reads as one.
//
// # Metrics
//
// EventsDeadLetteredTotal is incremented EXACTLY ONCE per completed dead-lettering — after
// the broker has acknowledged the message AND the row has been recorded, so the counter
// cannot overstate what is on the topic — attributed by the ORIGINAL category topic and
// event type so it is directly comparable with EventsPublishedTotal — their ratio is the dead-letter rate the
// 0.1% target is stated against. A successful dead-letter write additionally records one
// publish attempt with the dead-lettered outcome, which is the third value that
// counter's documented label set names.
//
// The oldest-message age gauge is deliberately NOT touched here. The event being
// dead-lettered right now is the NEWEST entry, and setting a gauge that must report the
// OLDEST from it would understate the age and silently defeat the 15-minute alert. That
// gauge has one maintainer, RefreshDeadLetterAgeGauge.
//
// Parameters:
//   - ctx context.Context: cancels the write, the recording and the metric recording.
//   - req DeadLetterRequest: the row, the failure cause and the optional attempt-window
//     overrides.
//
// Returns:
//   - DeadLetterOutcome: the record of what was written and stored. Populated on success;
//     partially populated alongside an error so a caller can log what it got to.
//   - error: a validation error for an unusable row, ErrKafkaUnavailable when there is no
//     transport or the write fails, or the repository's own typed error when the row cannot
//     be recorded. On any error the row is left exactly as the caller had it.
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
		// A row with no database identity cannot be recorded, and an unrecorded
		// dead-letter is invisible to the inventory and unreachable by replay. Failing
		// before the write is what keeps "it is on the dead-letter topic" and "an
		// operator can find it" from diverging.
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
		// Unreachable in practice: the original topic falls back through
		// TopicForEvent, which never returns an empty name. Guarded anyway, because a
		// blank topic would be published to a topic named ".dlt" or rejected by the
		// broker with a message that names neither the event nor the cause.
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

		// The row is deliberately left alone: not marked, not counted. Its status is
		// whatever the relay set before calling — failed on exhaustion — which keeps the
		// event in the dead-letter inventory and out of the terminal set.
		return outcome, err
	}

	// Set only after acknowledgement, so Published and "no error" say the same thing.
	outcome.Published = true

	// The claim token travels on the row: the relay put it there when it claimed the
	// row, and MarkEventFailed retained it on its exhaustion arm precisely so that this
	// step remains the exclusive property of the worker that spent the last attempt.
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
	// double-counting a write whose bookkeeping had to be retried.
	// Both labels are bounded — see boundedTopicLabel and boundedEventTypeLabel — because
	// both values come from a stored row and this counter is compared against
	// EventsPublishedTotal, which bounds them the same way. Bounding one side and not the
	// other would make the dead-letter-rate query divide series that do not correspond.
	metrics.EventsDeadLetteredTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(outcome.OriginalTopic)),
		attribute.String(publishAttrEventType, boundedEventTypeLabel(outcome.EventType)),
	))

	logrus.WithFields(outcome.LogFields()).Warn("ledger event dead-lettered after exhausting its retry budget")

	return outcome, nil
}

// DeadLetter dead-letters a row with the failure that ended its retry budget.
//
// It is PublishToDeadLetter with the request built for you, which is the call the relay's
// exhaustion branch makes. Everything the request could override is derived from the row.
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
//   - error: nil ONLY when a broker acknowledged the write. A typed ErrKafkaUnavailable
//     when there is no transport, when the writer cannot be resolved, or when the write
//     fails.
func (s *EventDeadLetterService) writeDeadLetterMessage(
	ctx context.Context,
	outcome DeadLetterOutcome,
) (model.BrokerRecord, error) {
	// STARTED BEFORE ANY RESOLUTION, so every exit below can be measured. The three failure
	// exits are the ones an operator most needs on the instruments: a broker refusing every
	// dead-letter message, or a deployment with no transport at all, must not look like an
	// empty dead-letter path.
	started := s.now()

	// EVERY EXIT RECORDS ONE ATTEMPT, and it is attributed to this write rather than to the
	// retry sequence that led here.
	//
	// PublishPurposeDeadLetter is what fixes the attempt label at the `dead_letter` token.
	// Without the purpose the label is the ORIGINAL attempt count — "5" — which both
	// misdescribes the observation and drops dead-letter latency into the numeric population
	// the first-attempt latency target is read against.
	//
	// There is no double counting to worry about: the relay's own attempts carry a NUMBER in
	// that attribute, so a dead-letter write is a distinct label tuple however it turns out.
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
		record(model.PublishStatusFailed)

		return model.BrokerRecord{}, err
	}

	writer, err := resolveWriter(outcome.DeadLetterTopic)
	if err != nil {
		record(model.PublishStatusFailed)

		return model.BrokerRecord{}, err
	}

	if writer == nil {
		// DLT-01: no transport means NO DEAD-LETTER MESSAGE, and that is a failure.
		//
		// This used to return success, and the caller then marked the row dead_lettered and
		// incremented the dead-letter counter. The event's only copy was the outbox row, the
		// row said it had been dead-lettered, the counter said so too, and there was nothing
		// on any topic to replay from — the row was out of the relay's claimable set with no
		// message behind it. An operator reading either signal would have concluded the event
		// was preserved.
		//
		// Failing instead leaves the row in the non-terminal state the relay put it in, where
		// the dead-letter inventory still lists it — the listing covers both failed and
		// dead_lettered precisely so an event whose dead-letter write failed stays visible —
		// and where a later call, with a transport, can still complete it.
		logrus.WithFields(outcome.LogFields()).Error(
			"no Kafka transport is configured, so the dead-letter message cannot be written; " +
				"leaving the event outbox row in its non-terminal state rather than recording a " +
				"dead-lettering that did not happen",
		)

		record(model.PublishStatusFailed)

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
	// back are the publisher's own, so completeWrite is already installed on them and fills
	// this in before WriteMessages returns; a test double that does not call Completion
	// simply leaves it unconfirmed, which the row then records honestly as unconfirmed.
	acknowledgement := &publishAcknowledgement{}

	writeErr := writer.WriteMessages(ctx, kafka.Message{
		// Topic is left empty on purpose: kafka-go rejects a message that names a topic
		// when the writer already has one, and every writer here is per-topic.
		Key:   partitionKeyBytes(outcome.PartitionKey),
		Value: outcome.Message,
		// Correlates the completion back to THIS write. It never reaches the wire, so byte
		// fidelity is untouched.
		WriterData: acknowledgement,
		// The instant the event was GIVEN UP ON, not the instant it occurred. A
		// dead-letter message is a new message on a different topic, and its broker
		// timestamp should say when it arrived there: stamping it with an original
		// occurrence that may be hours old would expose a message whose whole purpose is
		// preservation to time-based retention as though it were that old. The timestamp
		// is transport metadata and not part of the message value, so byte fidelity is
		// untouched either way — and the outbox row, not the topic, is the authoritative
		// record the inventory and replay read from.
		Time: outcome.Metadata.LastAttemptedAt,
	})
	if writeErr != nil {
		// DATA-01: the cause is a KAFKA CLIENT error — quite possibly a *net.OpError naming
		// the broker's address — so it is logged here and a bounded detail is attached to
		// the error instead of the cause itself. See EventTransportErrorDetail.
		logrus.WithFields(outcome.LogFields()).WithField("failure_detail_class", classifyDeadLetterFailure(errorText(writeErr))).Error(
			"the Kafka broker did not acknowledge a dead-letter message",
		)

		record(model.PublishStatusFailed)

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

	// The coordinate names the record on the DEAD-LETTER topic, which is where this event's
	// only surviving copy now lives. Persisting it is what lets an operator triaging the
	// inventory read the exact record back, and what lets the zero-loss audit account for a
	// dead-lettered event on the broker side rather than treating its record as surplus.
	brokerRecord, _ := acknowledgement.coordinate()

	return brokerRecord, nil
}

// ListDeadLetterEvents pages the dead-letter inventory an operator triages from.
//
// It reads the OUTBOX TABLE and never a Kafka topic. That is a design commitment, not an
// implementation detail: Blnk implements no consumer, and it does not need one, because
// the row already carries the dead-letter topic and the failure metadata. The listing is
// consequently available with the broker down, which is precisely when it is wanted.
//
// Both terminal failure states are returned. A row becomes failed the moment its retry
// budget is spent and dead_lettered only once the event has additionally reached its
// `<topic>.dlt` sibling, so listing only the latter would hide the events whose
// dead-letter write itself failed — the ones most in need of attention. Only a
// dead_lettered row can be replayed; a failed one has no dead-letter message to replay
// from, and PublishToDeadLetter is what moves it on.
//
// # Filtering, and what it costs
//
// With no filter set this is one repository query: the page is passed straight through.
// With a filter set the inventory is WALKED in repository-sized pages and the predicates
// are applied here, because the repository's listing is deliberately a single
// index-backed page and a second, filter-aware query would be a second thing to keep
// correct. The walk is bounded by the service scan limit and reports a truncated result
// through a warning rather than by silently returning a short page.
//
// Ordering is the repository's — newest occurrence first, ties broken by descending id —
// on both paths, so paging is stable and a row can neither be shown twice nor skipped.
//
// Parameters:
//   - ctx context.Context: cancels the query or the walk.
//   - opts DeadLetterListOptions: the page and its optional narrowing. The zero value is
//     valid and returns the first default-sized page of everything.
//
// Returns:
//   - []model.EventOutbox: the matching entries, oldest last. Never nil on success.
//   - error: a validation error for an unusable status filter, or the repository's own
//     typed error.
func (s *EventDeadLetterService) ListDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) ([]model.EventOutbox, error) {
	ctx, span := tracer.Start(ctx, "ListDeadLetterEvents")
	defer span.End()

	if s == nil || s.store == nil {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Listing dead-lettered events requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	normalized, err := normalizeDeadLetterListOptions(opts)
	if err != nil {
		span.RecordError(err)

		return nil, err
	}

	span.SetAttributes(
		attribute.Int("dead_letter.limit", normalized.Limit),
		attribute.Int("dead_letter.offset", normalized.Offset),
		attribute.String("dead_letter.event_type", normalized.EventType),
		attribute.String("dead_letter.topic", normalized.Topic),
		attribute.String("dead_letter.status", normalized.Status),
	)

	if !normalized.filtered() {
		entries, listErr := s.store.ListDeadLetteredEvents(ctx, normalized.Limit, normalized.Offset)
		if listErr != nil {
			span.RecordError(listErr)

			return nil, listErr
		}

		// The repository returns a nil slice for an empty page. Normalising it to an
		// empty slice means a caller can range over — and a handler can marshal — the
		// result without a nil check, and the JSON is [] rather than null.
		if entries == nil {
			entries = []model.EventOutbox{}
		}
		span.SetAttributes(attribute.Int("dead_letter.returned", len(entries)))

		return entries, nil
	}

	return s.walkDeadLetterInventory(ctx, normalized)
}

// walkDeadLetterInventory serves a filtered page by paging the inventory and applying the
// predicates here.
//
// It skips Offset MATCHES rather than Offset rows, which is what makes the filtered page
// behave like a page of the filtered set instead of a filtered page of the unfiltered
// set — the latter would return a short and unstable page as the filter's hit rate
// varied.
//
// Parameters:
//   - ctx context.Context: cancels the walk.
//   - opts DeadLetterListOptions: already normalised.
//
// Returns:
//   - []model.EventOutbox: up to Limit matching entries. Never nil on success.
//   - error: the repository's own typed error.
func (s *EventDeadLetterService) walkDeadLetterInventory(
	ctx context.Context,
	opts DeadLetterListOptions,
) ([]model.EventOutbox, error) {
	matches := make([]model.EventOutbox, 0, opts.Limit)
	skipped := 0
	scanned := 0

	for scanned < s.scanMaxRows {
		page, err := s.store.ListDeadLetteredEvents(ctx, deadLetterScanPageSize, scanned)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return matches, nil
		}
		scanned += len(page)

		for i := range page {
			if !deadLetterEntryMatches(page[i], opts) {
				continue
			}
			if skipped < opts.Offset {
				skipped++

				continue
			}

			matches = append(matches, page[i])
			if len(matches) == opts.Limit {
				return matches, nil
			}
		}

		// A short page is the end of the inventory: the repository fills a page whenever
		// it can, so fewer rows than requested means there are no more.
		if len(page) < deadLetterScanPageSize {
			return matches, nil
		}
	}

	logrus.WithFields(logrus.Fields{
		"scanned":    scanned,
		"scan_limit": s.scanMaxRows,
		"matches":    len(matches),
		"event_type": opts.EventType,
		"topic":      opts.Topic,
		"status":     opts.Status,
	}).Warn(
		"the dead-letter scan limit was reached while filtering the inventory; " +
			"the page may be incomplete. Narrow the filter or raise the scan limit",
	)

	return matches, nil
}

// ReplayDeadLetteredEvent re-publishes a dead-lettered event to the topic it was
// originally destined for.
//
// # Byte fidelity is the whole point
//
// The replayed message is IDENTICAL to the message originally published, byte for byte.
// That holds because the event is rebuilt from the STORED ROW and its payload bytes are
// spliced into the envelope untransformed, exactly as the first publish did — the replay
// does not decode and re-encode anything. A round trip through a Go map or a typed struct
// would reorder keys, drop members the struct does not declare and renormalise number and
// timestamp literals, any one of which would break the guarantee. Nothing here parses the
// payload, and nothing here reads the dead-letter topic.
//
// Only the failure metadata is absent, which is what "aside from the failure metadata"
// means: the metadata was attached as an additive sibling member on the dead-letter
// message and is simply not part of the envelope being republished.
//
// Two further properties are preserved deliberately:
//
//   - The MESSAGE KEY is the row's ledger id, the same key the original publish used, so
//     the replay lands on the same partition and the replay itself cannot violate the
//     per-aggregate ordering guarantee.
//   - The EVENT ID is unchanged. It is the subscriber's idempotency key: a replay that
//     minted a new one would be indistinguishable from a new event and would defeat the
//     duplicate suppression that makes at-least-once delivery safe.
//
// # Where the destination comes from
//
// The topic is taken from the STORED FAILURE METADATA's original_topic, which is the
// authoritative record of where the event was headed, falling back to the row's topic
// column and finally to the event type's current mapping. Re-deriving it from the event
// type first would be wrong whenever KAFKA_TOPIC_PREFIX changed after the event was
// stored: the two should agree, and when they do not, what was recorded wins.
//
// # State transition, and why a second replay is refused
//
// A successful replay moves the row to DISPATCHED, the same terminal success state an
// ordinary publish reaches. Three things follow, all of them intended: the row leaves the
// dead-letter inventory, so it is no longer presented as needing attention; its
// dlt_topic and failure_metadata are retained, so the history of what went wrong is not
// erased; and a second replay of the same event is REFUSED with ErrEventNotDeadLettered,
// because the status precondition no longer holds. Repeat replay is therefore explicitly
// rejected rather than silently duplicating the event.
//
// # No broker configured
//
// A deployment with no Kafka transport cannot replay, and says so with
// ErrKafkaUnavailable. Reporting success would be a lie — nothing would have been
// published — and this is an explicit, operator-triggered request rather than a
// background write, so failing closed is the honest answer.
//
// # A failed re-publish is classified, not lumped together
//
// The two ways a re-publish can fail are different situations for whoever called this
// endpoint, so they resolve to different codes and therefore different HTTP statuses.
// A broker that is unreachable, leaderless or under-replicated is a retryable UPSTREAM
// condition: it answers ErrKafkaUnavailable (503), the same code the no-transport case
// uses, because in both the event is intact and the correct response is to try again once
// the broker recovers. Anything else — bytes that cannot be published, a destination that
// cannot be resolved, a failure this service cannot attribute to the broker — answers
// ErrEventReplayFailed (500), because it is Blnk's problem and retrying will not fix it.
// The classification is IsBrokerUnavailableError's, which reads the publisher's own
// per-attempt verdict rather than re-deriving one here. Answering 500 for an outage would
// send an operator looking for a defect that is not there and would tell a client that
// retrying is pointless at the one moment it is the only thing that helps.
//
// Parameters:
//   - ctx context.Context: cancels the lookup, the publish and the recording.
//   - eventID string: the event's UUID, as listed by the inventory.
//
// Returns:
//   - ReplayOutcome: the record of the replay. Populated on success, and populated
//     alongside the error in the one case where the publish succeeded but the row could
//     not be updated.
//   - error: ErrGenValidation for a blank id, ErrEventNotFound when no such event exists,
//     ErrEventNotDeadLettered when the event is not in the dead-lettered state,
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

	// From here on the row is held in the replaying state, so EVERY exit path must
	// either mark it dispatched or release the claim. Releasing is idempotent from the
	// caller's point of view — releaseReplayClaim reports its own failures and never
	// masks the error being returned — so the deferred-style guard below is safe to pair
	// with the explicit success transition further down.
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
		// DATA-01: same boundary as the dead-letter write. The publisher's error carries the
		// broker's own words — and, through *net.OpError, its address — so the log line below
		// keeps them and the caller receives the bounded diagnosis.
		//
		// The CODE is classified rather than fixed, so a broker outage answers 503 and only
		// a failure this service owns answers 500. The sanitised detail is unchanged either
		// way: what the caller is told about the cause does not depend on whose fault it is.
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
		// dead_lettered — otherwise a failed replay would cost the event its
		// replayability by stranding it in replaying.
		releaseOnFailure(publishErr)

		return outcome, err
	}

	if markErr := s.store.MarkEventDispatched(
		ctx, row.ID, row.ClaimToken, result.Record,
	); markErr != nil {
		// The event HAS been republished. The bookkeeping has not, so the row is still
		// listed as dead-lettered and can be replayed again — a duplicate that the
		// subscriber's idempotency on the unchanged event id absorbs. An error is returned
		// rather than swallowed precisely because the operator must know the entry has not
		// cleared; reporting success here would leave a phantom in the inventory with
		// nobody looking for it.
		err = apierror.NewAPIError(
			apierror.ErrEventReplayFailed,
			"The event was republished but its outbox entry is still marked dead-lettered",
			fmt.Errorf("blnk: marking replayed event %q dispatched: %w", eventID, markErr),
		)
		span.RecordError(err)
		logrus.WithFields(outcome.LogFields()).WithField("failure_detail_class", classifyDeadLetterFailure(errorText(markErr))).
			Error("a replayed ledger event could not be marked dispatched")
		// The event IS on the topic but the success transition did not land, so the row
		// is returned to dead_lettered rather than left in replaying. That keeps the
		// entry visible in the inventory — which is what the operator needs, since the
		// error above tells them it has not cleared — instead of hiding it in a state
		// no view reports on.
		releaseOnFailure(markErr)

		return outcome, err
	}
	released = true

	outcome.Recorded = true
	logrus.WithFields(outcome.LogFields()).Info("dead-lettered ledger event replayed to its original topic")

	return outcome, nil
}

// replayClaimLease is how long a replay holds its claim on a row.
//
// It exists so a process that dies mid-replay cannot strand the row in replaying
// forever: once the lease has expired, locked_until shows an operator that the claim
// is stale. It is generous relative to a single publish because a replay is a rare,
// human-triggered action and the cost of a lease that is slightly too long is only a
// delayed second attempt, whereas one that is too short would let a second request
// publish while the first is still in flight — the exact duplication the claim exists
// to prevent.
const replayClaimLease = 2 * time.Minute

// replayFailureOutcome maps a failed re-publish onto the typed code and the operator-facing
// message the replay endpoint must answer with.
//
// It is the one place the distinction is drawn, so the code and the message can never
// disagree about what went wrong. The two arms:
//
//   - The broker is unavailable — unreachable, leaderless, under-replicated, or a
//     transport that has been closed. ErrKafkaUnavailable, which statusByCode maps to 503.
//     This is the same code the no-transport branch above returns, and deliberately so:
//     both mean the event is intact and the request should be repeated once the broker is
//     back. The message says the broker, not the event, is the problem, because an
//     operator reading it needs to know where to look.
//   - Anything else. ErrEventReplayFailed, which maps to 500. Reserved for failures this
//     service owns — bytes that cannot be published, a destination that cannot be
//     resolved — where a retry changes nothing.
//
// The verdict comes from IsBrokerUnavailableError, which reads the publisher's own
// per-attempt classification rather than re-deriving one from the error text. Matching on
// a message here would be the fragile version of this function: broker error strings are
// not a contract, and a library upgrade that reworded one would silently move every
// outage back to a 500.
//
// Parameters:
//   - cause error: the non-nil error PublishToTopic returned.
//
// Returns:
//   - apierror.ErrorCode: the typed code, which has an explicit statusByCode entry.
//   - string: the message that accompanies it.
func replayFailureOutcome(cause error) (apierror.ErrorCode, string) {
	if IsBrokerUnavailableError(cause) {
		return apierror.ErrKafkaUnavailable,
			"The Kafka broker is unavailable, so the event could not be replayed; retry once it recovers"
	}

	return apierror.ErrEventReplayFailed, "Failed to replay the dead-lettered event to its original topic"
}

// claimReplayableEvent CLAIMS a dead-lettered row for replay and translates the
// repository's failures into the typed errors this API answers with.
//
// # Why this is a claim and not a lookup
//
// It used to be a lookup followed by a status check, and that shape had a race in it
// that no amount of care at the call site could remove: two replay requests for one
// event both read a dead_lettered row, both saw the precondition satisfied, and both
// published. An operator double-clicking, or two operators working the same backlog,
// therefore put two copies of the event on the topic. Because a replay re-publishes
// the stored bytes those copies are byte-identical, so a subscriber deduplicating on
// event_id discards one — but the duplicate is real, it occupies a partition slot,
// and leaning on consumer behaviour to paper over a defect on the publishing side is
// not a guarantee.
//
// Moving the precondition INTO the transition closes it. dead_lettered → replaying is
// a conditional update, so exactly one concurrent request changes a row and receives
// the claim token; every other request is refused before it can publish anything.
//
// The two rejections stay distinct, because they are different operator situations: a
// missing event is a wrong id, whereas a present but not-dead-lettered event is a
// state error — most often a second replay of something already replayed, which is
// exactly the accidental duplication the precondition exists to prevent. A row found
// in the replaying state is now also reachable, and it means a concurrent replay holds
// it; that is reported as a state error too, with the status named, so the message says
// what is actually happening.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - eventID string: the trimmed, non-blank event id.
//
// Returns:
//   - *model.EventOutbox: the claimed row, carrying the claim token in ClaimToken.
//   - error: ErrEventNotFound or ErrEventNotDeadLettered, or the repository's own error.
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
			// The row exists but is not dead-lettered. WHICH state it is in decides what
			// the operator is told, because "already replayed", "still being delivered"
			// and "another replay is in flight" are three different situations and only
			// one of them is a mistake.
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

// describeUnreplayableEvent turns a refused replay claim into the message that names the
// operator's actual situation.
//
// The claim itself can only report that the precondition failed; it cannot say why in a
// way an operator can act on. Reading the row afterwards is what supplies that, and it
// costs one query on the failure path only.
//
// The three cases are genuinely different problems. A row that is DISPATCHED with a
// dead-letter history has already been replayed — the operator clicked twice, and telling
// them merely "not dead-lettered" would send them looking for a state error that does not
// exist. A row that is REPLAYING is held by a concurrent replay, so the right answer is to
// wait rather than to retry. Anything else is an event still working its way through
// ordinary delivery, which was never replayable in the first place.
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

// releaseReplayClaim returns a claimed row to dead_lettered after a replay that did
// not complete.
//
// It NEVER returns an error, and that is deliberate. It is called on paths that are
// already returning a failure to the caller, and replacing that failure with this
// one — "the replay failed, and also the rollback failed" collapsed into a single
// error value — would hide the reason the replay failed in the first place. The
// rollback failure is logged at error level with the event identity instead.
//
// # It runs on a DETACHED, BOUNDED context
//
// The rollback is work the service already owes: the row is held in replaying and the
// replay is over. Running it on the caller's context makes the most common failure the
// least recoverable — a request cancelled or timed out mid-replay cancels the rollback
// too, so the very path most likely to need it is the one where it cannot run.
//
// The row is not lost when the rollback fails: the replay claim reclaims a row whose
// lease has expired, atomically and with a fresh fencing token, so an abandoned claim
// clears itself. That recovery is the reason this can afford to swallow the error —
// before the claim enforced the lease it could not, because a failed rollback made the
// event permanently unreplayable.
//
// # IT RUNS ON A DETACHED CONTEXT, and that is what makes it work at all
//
// The commonest way a replay fails is the caller's context being cancelled — an operator
// closing the browser tab, a proxy timing the request out, a rolling deploy taking the
// process down mid-replay. Running the release on that same context meant the release
// failed for exactly the reason the replay did, every time, so the row was left in
// replaying with a token nobody held. The claim's lease was written down and, until
// ClaimEventForReplay and ReclaimStaleEventReplays began enforcing it, nothing read it.
//
// The release is bookkeeping this service already owes, so it is completed on a context
// detached from the caller's cancellation and bounded by its own timeout — the same
// treatment the relay gives its own transitions, for the same reason.
//
// Parameters:
//   - ctx context.Context: used only for its values; cancellation is deliberately not
//     inherited.
//   - row *model.EventOutbox: the claimed row, carrying its claim token.
//   - reason error: why the replay did not complete; recorded in last_error when it
//     has a message, so the next operator sees the most recent cause rather than the
//     original publish failure. It is sanitized and bounded before it is persisted,
//     because a driver error can carry a broker address, a payload fragment or many
//     kilobytes of nested detail, and last_error is read back into an API response.
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
		logrus.WithError(err).WithFields(logrus.Fields{
			"event_id":   row.EventID,
			"event_type": row.EventType,
			"dlt_topic":  row.DLTTopic,
		}).Error(
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
// It is the ONLY maintainer of DLTOldestMessageAgeSeconds, and it exists as a separate
// operation for one reason: the gauge must report the OLDEST unresolved entry, and the
// dead-letter path only ever sees the newest. Setting the gauge where an event is
// dead-lettered would make it report the age of something that just happened — always
// near zero, never firing the alert that a message has been stuck for 15 minutes, and
// looking healthy while doing it. Call this from the relay's poll, and from the statistics
// endpoint.
//
// # What "age" means here
//
// The age of an entry is measured from its LAST ATTEMPT, which is the instant it was given
// up on and therefore the instant it started sitting in the dead-letter topic. Where that
// is unknown the occurrence instant is used instead, which is older and so errs toward
// reporting a problem rather than hiding one.
//
// Rows in the failed state — budget spent, dead-letter write not yet done — are included
// and attributed to the dead-letter topic they are BOUND FOR. They are the most urgent
// entries in the inventory, and excluding them would let an event whose dead-letter write
// keeps failing age indefinitely without the alert noticing.
//
// # Every topic is published, including the empty ones
//
// A dead-letter topic with nothing outstanding is set to ZERO rather than left alone. An
// unset gauge keeps its last value in the exporter, so an inventory that was just cleared
// would keep alerting on an age that no longer exists.
//
// # Bounding
//
// The scan walks the OLDEST entries first, sizing its window from the status counts, so a
// truncated scan still sees the oldest rows it can and the reported age is a lower bound
// that only understates by entries even older than those examined. Truncation is reported
// on the returned report and logged.
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

	counts, err := s.store.CountEventOutboxByStatus(ctx)
	if err != nil {
		span.RecordError(err)

		return DeadLetterAgeReport{}, err
	}

	// A status with no rows is absent from the map rather than present with a zero, so
	// the two-value read is not optional here.
	//
	// failed WITH NO dlt_topic is where a row waits for its `<topic>.dlt` write, and it is the
	// only such state: that pair is what the exhaustion arm writes and what
	// ClaimFailedEventOutboxForDeadLetter selects. This figure exists to surface exactly that
	// population — events whose only copy is the outbox row — so under-counting it would also
	// skip the age scan below whenever every stuck row was uncounted.
	report.FailedAwaitingDeadLetter = counts[model.EventOutboxStatusFailed]
	report.Outstanding = counts[model.EventOutboxStatusDeadLettered] + report.FailedAwaitingDeadLetter

	if report.Outstanding > 0 {
		if err = s.scanOldestDeadLetters(ctx, &report, now); err != nil {
			span.RecordError(err)

			return DeadLetterAgeReport{}, err
		}
	}

	for topic, age := range report.OldestByTopic {
		metrics.DLTOldestMessageAgeSeconds.Record(ctx, age.Seconds(), otelmetric.WithAttributes(
			attribute.String(publishAttrTopic, topic),
		))
	}

	span.SetAttributes(
		attribute.Int64("dead_letter.outstanding", report.Outstanding),
		attribute.Int64("dead_letter.failed_awaiting_dlt", report.FailedAwaitingDeadLetter),
		attribute.Int("dead_letter.scanned", report.Scanned),
		attribute.Bool("dead_letter.truncated", report.Truncated),
		attribute.Float64("dead_letter.oldest_age_seconds", report.OldestAge().Seconds()),
	)

	return report, nil
}

// scanOldestDeadLetters fills the report's per-topic ages from the oldest entries in the
// inventory.
//
// The inventory is ordered newest first, so the oldest entries are at its END: the walk
// starts from the last page and moves backwards. That is what makes a bounded scan
// correct for a gauge that must report the oldest entry — a forward walk would examine the
// newest rows and, on truncation, report an age drawn from exactly the wrong end.
//
// Parameters:
//   - ctx context.Context: cancels the walk.
//   - report *DeadLetterAgeReport: the report to fill. Its Outstanding count sizes the
//     window and its OldestByTopic map is updated in place.
//   - now time.Time: the instant ages are measured against.
//
// Returns:
//   - error: the repository's own typed error.
func (s *EventDeadLetterService) scanOldestDeadLetters(
	ctx context.Context,
	report *DeadLetterAgeReport,
	now time.Time,
) error {
	// The window is the oldest min(outstanding, scanLimit) entries. Offsets are computed
	// from the tail of the inventory, and a negative offset is impossible because the
	// window can never exceed the count it was derived from.
	window := report.Outstanding
	if limit := int64(s.scanMaxRows); window > limit {
		window = limit
		report.Truncated = true
	}

	offset := report.Outstanding - window
	for remaining := window; remaining > 0; {
		size := int64(deadLetterScanPageSize)
		if remaining < size {
			size = remaining
		}

		page, err := s.store.ListDeadLetteredEvents(ctx, int(size), int(offset))
		if err != nil {
			return err
		}
		if len(page) == 0 {
			// The inventory shrank between the count and the walk — entries were
			// replayed, or the count raced an insert. There is nothing older left to
			// examine, and the ages gathered so far are still correct for what remains.
			return nil
		}

		for i := range page {
			// DATA-01: the row's recorded dead-letter topic is a STORED STRING, and this
			// map's keys become gauge labels. Bounding it here rather than at the
			// recording call keeps the returned report and the published series identical,
			// so an operator reading the report and an alert reading the gauge cannot
			// disagree about which topic an age belongs to.
			topic := boundedTopicLabel(deadLetterTopicOf(page[i]))
			age := now.Sub(deadLetterAgedFrom(page[i]))
			if age < 0 {
				// A clock skew or a future-dated occurrence must not report a negative
				// age, which would read as "newer than now" and silently lower the
				// maximum.
				age = 0
			}
			if age > report.OldestByTopic[topic] {
				report.OldestByTopic[topic] = age
			}
		}

		report.Scanned += len(page)
		offset += int64(len(page))
		remaining -= int64(len(page))
	}

	if report.Truncated {
		logrus.WithFields(logrus.Fields{
			"outstanding": report.Outstanding,
			"scanned":     report.Scanned,
			"scan_limit":  s.scanMaxRows,
			"oldest_age":  report.OldestAge().String(),
		}).Warn(
			"the dead-letter inventory is larger than the scan limit; " +
				"the reported oldest age is a lower bound drawn from the oldest entries examined",
		)
	}

	return nil
}

// BuildFailureMetadata composes the failure record attached to a dead-lettered event.
//
// It produces EXACTLY THE FIVE FIELDS the dead-letter contract names — the original topic,
// the error reason, the attempt count, and the first- and last-attempted instants — and no
// others. A sixth field would not be a harmless addition: the metadata is a published
// shape that the triage runbook and the API projection both read.
//
// Every field has a fallback chain, because each one is separately capable of being absent
// on a real row and NONE of them may come out empty or zero-valued:
//
//   - ORIGINAL TOPIC: the row's recorded topic, else the topic its event type maps to
//     today. The recorded value wins because it is where the event was actually headed,
//     which is what a replay has to honour.
//   - ERROR REASON: the caller's cause, else the row's last_error, else an explicit "no
//     reason was recorded" sentence. Never empty — an empty reason answers nothing while
//     looking like a value.
//   - ATTEMPT COUNT: the larger of the caller's count and the row's counter, since both
//     are lower bounds on the truth; else the row's budget, which is the configured
//     maximum and the count an exhausted row must report; else one, because reaching this
//     function at all means at least one attempt was made.
//   - THE WINDOW: the row's attempt timestamps, else the dead-letter instant. A zero
//     time.Time would serialise as year one and make the window nonsense.
//
// Both instants are normalised to UTC so their RFC3339 rendering is unambiguous wherever
// the process runs, and an inverted window is squared up rather than published — a last
// attempt earlier than the first would report a negative duration to whoever subtracts
// them.
//
// Parameters:
//   - row model.EventOutbox: the exhausted row.
//   - cause error: the failure from the final attempt. May be nil.
//   - attempts int: the caller's attempt count. Zero or negative means "use the row".
//   - at time.Time: the dead-letter instant, used as the terminal fallback for the window.
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
// The overrides exist because the relay knows the window it actually retried over, which
// the row only knows as of its last database write. Only the two timestamps are replaced,
// and neither takes part in the event envelope, so the message composed from the returned
// row is byte-identical to one composed from the original.
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
// recorded its destination at insert time precisely so it stays replayable to the topic it
// was always meant for even if KAFKA_TOPIC_PREFIX changed since. TopicForEvent never
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
// The STORED FAILURE METADATA is authoritative: original_topic is the record of where the
// event was headed when it failed, so a replay honours it in preference to anything that
// can be re-derived now. The row's own topic column is the next best record, and the event
// type's current mapping is the last resort for a row whose metadata never got written.
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

// deadLetterTopicOf returns the dead-letter topic an inventory entry belongs to.
//
// A dead-lettered row records the topic it was written to, and that recorded value is used
// as-is. A row in the failed state has not reached a dead-letter topic yet, so it is
// attributed to the sibling of the topic it was BOUND FOR — which is what lets the age
// gauge cover events whose dead-letter write itself keeps failing.
//
// Parameters:
//   - row model.EventOutbox: the inventory entry.
//
// Returns:
//   - string: a non-empty dead-letter topic name.
func deadLetterTopicOf(row model.EventOutbox) string {
	if topic := strings.TrimSpace(row.DLTTopic); topic != "" {
		return DLTFor(topic)
	}

	return DLTFor(originalTopicOf(row))
}

// deadLetterAgedFrom returns the instant an entry's age is measured from.
//
// The LAST ATTEMPT is the moment the event was given up on, and therefore the moment it
// started waiting in a dead-letter topic. Where that is unknown the occurrence instant is
// used instead: it is older, so the age it produces errs toward reporting a problem rather
// than hiding one.
//
// Parameters:
//   - row model.EventOutbox: the inventory entry.
//
// Returns:
//   - time.Time: the reference instant. Zero only for a row with neither timestamp, which
//     the caller clamps to a zero age.
func deadLetterAgedFrom(row model.EventOutbox) time.Time {
	if row.LastAttemptedAt != nil && !row.LastAttemptedAt.IsZero() {
		return row.LastAttemptedAt.UTC()
	}

	return row.OccurredAt.UTC()
}

// deadLetterReason returns the error reason recorded on a dead-lettered event, SANITIZED AND
// BOUNDED.
//
// # Why this one is bounded at the point of construction rather than at the point of logging
//
// This value does not merely reach a log line. It goes into the failure metadata that is
// marshaled into the dead-letter MESSAGE on the `<topic>.dlt` topic, and into the row's
// persisted failure_metadata column that the dead-letter API reads back. So an unbounded value
// is written three times and kept indefinitely, and a driver error is exactly the shape that
// abuses that: kafka-go's WriteErrors aggregates one error per message in a batch, so a failed
// batch of a hundred produces a hundred concatenated errors, and a TLS or DNS failure can carry
// a broker address and a certificate chain.
//
// Three consequences, all of which the bound removes: a dead-letter message that a topic's
// max.message.bytes could reject — losing the event at precisely the moment it most needed
// keeping — an API response carrying kilobytes of driver text per entry, and control characters
// reaching a log aggregator inside a field that came from a remote system.
//
// Truncation is safe here because the reason is DIAGNOSTIC. The event's own bytes are preserved
// verbatim and separately; nothing about replay fidelity depends on this string, and the first
// few hundred characters of a driver error are where its meaning is.
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
		// report. This is the path a caller that states nothing and a row whose counter
		// was never read back both take.
		resolved = row.MaxAttempts
	}

	if resolved <= 0 {
		return 1
	}

	return resolved
}

// marshalFailureMetadata serialises the failure metadata for storage and for the message.
//
// The SAME bytes are used in both places, which is what makes the row's failure_metadata
// column and the dead-letter message's failure_metadata member provably identical rather
// than merely similar. Being a struct, it marshals in declaration order, so the member
// order is deterministic.
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
// It exists so that the API projection and any operator tooling read the stored bytes
// through ONE decoder rather than each declaring its own view of the shape. Absent
// metadata — a row that was never dead-lettered, or a SQL NULL — is (nil, nil) and not an
// error, because "this row has no failure record" is an ordinary state of the inventory
// and forcing every caller to distinguish it from a decode failure would invite exactly
// the wrong branch.
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

// ComposeDeadLetterMessage builds the dead-letter message for a row: the event envelope,
// unaltered, with the failure metadata attached as an additive sibling member.
//
// # The invariant this function exists to guarantee
//
// The envelope bytes are a byte-exact PREFIX of the result. The envelope is produced by
// exactly the same serialiser the ordinary publish uses — payload bytes spliced through
// untransformed, scalars encoded identically — and the metadata is appended by replacing
// the envelope's closing brace with `,"failure_metadata":<metadata>}`. Nothing inside the
// envelope is decoded, re-encoded, reordered or escaped again.
//
// That is what makes byte-faithful replay possible. A replay re-publishes an envelope
// built from the same stored row, so it reproduces those same bytes exactly, and
// StripFailureMetadata recovers them from a stored dead-letter message. The alternative —
// decoding the envelope into a map, adding a key and re-encoding — would sort the keys,
// renormalise numbers and re-escape strings, and the byte-for-byte guarantee would be
// unachievable rather than merely harder.
//
// # What is rejected
//
// Invalid metadata bytes are refused rather than spliced, exactly as an invalid payload is
// refused by the publisher: splicing them would emit a message that breaks every
// subscriber's parser, and no retry turns malformed bytes into valid ones. An envelope that
// does not end in a member and a closing brace is likewise refused, because appending to
// it would produce `{,"failure_metadata":…}`.
//
// Parameters:
//   - row model.EventOutbox: the row whose envelope is composed.
//   - metadata json.RawMessage: the serialised failure metadata. Must be non-empty, valid
//     JSON.
//
// Returns:
//   - []byte: the dead-letter message value.
//   - error: a typed internal error when the envelope cannot be built or the metadata is
//     not valid JSON.
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

	// THE STORED CANONICAL ENVELOPE, spliced onto rather than rebuilt. These are the bytes
	// the broker was given — or would have been given — so the dead-letter copy differs from
	// the message that failed by exactly one member, which is what requirement R-5 asks for
	// and what a byte comparison in event_replay_fidelity_test.go asserts. Re-serialising
	// here would make that equality hold only within one build of Blnk.
	//
	// The fallback inside CanonicalEventBytes covers a row written before the column
	// existed; it composes the same bytes this version stores.
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
// It is the exact inverse of the splice ComposeDeadLetterMessage performs, and it works on
// BYTES rather than on a decoded document: the metadata member is the last member of the
// object, so removing it and restoring the closing brace returns the original envelope
// byte for byte. Decoding and re-encoding would defeat the purpose — the point of this
// function is to demonstrate, and to let a caller verify, that the original bytes survived
// unaltered.
//
// It is IDEMPOTENT: a message that carries no failure metadata is returned unchanged. A
// caller need not know whether it is holding an original or a dead-lettered message, which
// is what makes this safe to apply on the way into a comparison.
//
// The search is for the LAST occurrence of the member, which is correct precisely because
// the splice always appends: a payload that happens to contain the same member name deeper
// inside cannot be mistaken for the attachment.
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
// Page bounds are clamped rather than rejected, because a caller asking for a page that is
// too large or an offset below zero has made a recoverable mistake and degrading to a sane
// page is more useful than an error. The STATUS FILTER is the one exception: an
// unrecognised status is rejected, because silently matching nothing would answer "nothing
// is stuck" to an operator who asked a different question.
//
// Parameters:
//   - opts DeadLetterListOptions: the caller's request.
//
// Returns:
//   - DeadLetterListOptions: the normalised request, with filters trimmed.
//   - error: ErrGenValidation when the status filter is not a terminal failure state.
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
		return opts, nil
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
}

// deadLetterEntryMatches reports whether an inventory entry satisfies the requested
// filters.
//
// Comparison is exact and case-sensitive throughout, matching how event types and topics
// are compared everywhere else in this pipeline: producers emit fixed literals, and
// case-folding here would be a difference in behaviour between the filtered and unfiltered
// paths for no benefit. The topic filter is applied to the entry's ORIGINAL topic, resolved
// through the same rule the rest of the file uses, so filtering by "blnk.transactions"
// matches an entry whose row recorded that topic without the caller having to know the
// `.dlt` convention.
//
// Parameters:
//   - row model.EventOutbox: the entry to test.
//   - opts DeadLetterListOptions: the normalised filters.
//
// Returns:
//   - bool: true when every set filter matches.
func deadLetterEntryMatches(row model.EventOutbox, opts DeadLetterListOptions) bool {
	if opts.EventType != "" && row.EventType != opts.EventType {
		return false
	}
	if opts.Status != "" && row.Status != opts.Status {
		return false
	}
	if opts.Topic != "" && originalTopicOf(row) != opts.Topic {
		return false
	}

	return true
}

// replayAttemptNumber returns the attempt label a replay is recorded under.
//
// It is one past the exhausted budget, so a replay is visible in the attempt metrics
// without contaminating the attempt="1" series that the publish-latency target is read
// from. The larger of the row's counter and its budget is used, so the label is past both.
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
// The repository reports a missing event as a typed APIError rather than as a bare
// sql.ErrNoRows, and APIError does not unwrap to the error it wrapped, so the CODE is what
// has to be inspected. Both the legacy and the canonical not-found codes are accepted
// because the repository layer still constructs the legacy one, and the event-specific code
// is accepted so that re-wrapping an already-mapped error stays idempotent. A bare
// sql.ErrNoRows is recognised as well, which keeps a direct store implementation from
// having to know the convention.
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
//
// The three methods below are how the rest of the codebase reaches the dead-letter
// surface: the API handlers hold a *Blnk and call through it, exactly as they do for
// every other domain operation.
//
// Each builds a service bound to this instance's datasource, uses it, and closes it. The
// publisher such a service resolves for itself is short-lived, which is the right trade for
// a rare, operator-triggered request and the WRONG one inside a loop: the relay must build
// ONE service with NewEventDeadLetterService, passing the publisher it already holds, and
// keep it for its lifetime. Nothing here caches a service on the instance, because that
// would put publisher lifecycle inside a struct whose Close does not own it.
// ---------------------------------------------------------------------------

// EventDeadLetters returns a dead-letter service bound to this instance's datasource.
//
// The returned service resolves a publisher from live configuration on first use, so the
// CALLER MUST CLOSE IT — `defer service.Close()` — or the connections that publisher opens
// are held until the process ends. Prefer NewEventDeadLetterService with an already-built
// publisher wherever one is available.
//
// It is nil-safe: a nil instance yields a service whose operations report a missing
// datasource rather than panicking.
//
// Returns:
//   - *EventDeadLetterService: a ready service the caller owns.
func (b *Blnk) EventDeadLetters() *EventDeadLetterService {
	if b == nil {
		return NewEventDeadLetterService(nil, nil)
	}

	return NewEventDeadLetterService(b.datasource, nil)
}

// ListDeadLetterEvents pages the dead-letter inventory. It is the read behind
// GET /events/dead-letter.
//
// No publisher is resolved: the inventory is read from the outbox table, so this works with
// the broker down and costs one query.
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
) ([]model.EventOutbox, error) {
	return b.EventDeadLetters().ListDeadLetterEvents(ctx, opts)
}

// ReplayDeadLetteredEvent replays one dead-lettered event to its original topic. It is the
// write behind POST /events/dead-letter/:event_id/replay.
//
// The short-lived publisher this resolves is closed before returning, so the handler needs
// no lifecycle handling of its own. A close failure is logged rather than returned: the
// replay's own outcome is what the caller asked about, and reporting a connection-teardown
// problem as a failed replay would send an operator to retry something that already
// succeeded.
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
// once, in one place: errcheck is satisfied, and a teardown problem cannot be mistaken for
// an operation failure.
//
// Parameters:
//   - service *EventDeadLetterService: the service to close. May be nil.
func closeDeadLetterService(service *EventDeadLetterService) {
	if err := service.Close(); err != nil {
		logrus.WithError(err).Warn("closing the short-lived dead-letter event publisher failed")
	}
}
