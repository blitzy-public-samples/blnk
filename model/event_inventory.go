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
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

// DeadLetterFilter narrows the dead-letter inventory at the REPOSITORY, which is the
// only layer that can narrow it correctly.
type DeadLetterFilter = DeadLetterQuery

// Narrows reports whether the filter constrains anything at all.
//
// Returns:
//   - bool: true when at least one field is set.
func (q DeadLetterQuery) Narrows() bool {
	return q.Filtered()
}

// PartitionOffsetInterval is one partition's MEASURED, currently-readable offset
// window: the half-open range [FirstOffset, EndOffset) the broker reported for it.
type PartitionOffsetInterval struct {
	// Topic is the fully-qualified topic name, exactly as a row's kafka_topic records it.
	Topic string

	// Partition is the partition ID.
	Partition int

	// FirstOffset is the earliest offset still retained. INCLUSIVE.
	FirstOffset int64

	// EndOffset is the log end offset: one past the last record written. EXCLUSIVE, which
	// is why a coordinate at or above it cannot be on the log at all.
	EndOffset int64
}

// Contains reports whether an offset lies inside the measured window.
//
// Parameters:
//   - offset int64: the coordinate's offset.
//
// Returns:
//   - bool: true when the offset is readable within this window.
func (i PartitionOffsetInterval) Contains(offset int64) bool {
	return offset >= i.FirstOffset && offset < i.EndOffset
}

// Records is how many records the window holds, never negative.
//
// Returns:
//   - int64: the number of readable records.
func (i PartitionOffsetInterval) Records() int64 {
	if i.EndOffset <= i.FirstOffset {
		return 0
	}

	return i.EndOffset - i.FirstOffset
}

// EventRecordIntervalAudit is the outbox side of the zero-loss reconciliation,
// classified AGAINST THE MEASURED BROKER WINDOWS rather than counted in aggregate.
type EventRecordIntervalAudit struct {
	// PublishedRows is how many rows claim a record on the broker: every row whose Kafka
	// leg completed, plus every dead-lettered row, each counted exactly once by virtue of
	// the unique index on event_id.
	PublishedRows int64

	// CorroboratedRows is how many of those name a record inside a measured window. It is
	// the only bucket a green verdict may contain.
	CorroboratedRows int64

	// DistinctCorroboratedRecords is how many DISTINCT coordinates the corroborated rows
	// name. It equals CorroboratedRows unless two rows claim the same record, which the
	// partial unique index on the coordinate makes impossible — so a discrepancy means
	// that index is missing or has been dropped, and the audit reports it rather than
	// assuming the schema is intact.
	DistinctCorroboratedRecords int64

	// UnconfirmedRows claim a publication without naming any record.
	UnconfirmedRows int64

	// UnmeasuredRows name a topic or partition the measurement did not cover.
	UnmeasuredRows int64

	// AgedOutRows name an offset below the retained window: written, then deleted by
	// retention.
	AgedOutRows int64

	// BeyondEndRows name an offset at or above the log end: evidence of truncation or topic
	// recreation.
	BeyondEndRows int64

	// OldestTerminalAt is the earliest publication instant among ALL terminal rows still
	// retained in the outbox, which is the floor of what any verdict can speak about.
	OldestTerminalAt time.Time

	// CorroboratedFrom and CorroboratedTo bound the publication instants of the
	// CORROBORATED population: the window the green verdict actually covers.
	CorroboratedFrom time.Time
	CorroboratedTo   time.Time

	// WindowStart is the earliest publication instant the three counts above include.
	WindowStart time.Time

	// MeasuredAt is when the outbox side was read.
	MeasuredAt time.Time
}

// EventTopicBacklog is how much work an outbox topic still owes, for ONE topic name as
// it is stored on the rows.
type EventTopicBacklog struct {
	// Topic is the destination as recorded on the rows, verbatim.
	Topic string

	// UndeliveredRows is how many rows still owe a first successful publish to Topic —
	// pending, processing, failed and replaying.
	UndeliveredRows int64

	// ReplayableRows is how many dead-lettered rows could be replayed to Topic.
	ReplayableRows int64

	// OldestOccurredAt is the occurrence instant of the oldest row counted here, which is
	// what turns "some rows are stranded" into "events from three days ago are stranded".
	OldestOccurredAt time.Time
}

// TotalRows is how many rows this topic still owes something for.
//
// Returns:
//   - int64: the sum of the two counts.
func (b EventTopicBacklog) TotalRows() int64 {
	return b.UndeliveredRows + b.ReplayableRows
}

// UnconfirmedRows is how many rows claim a publication they cannot name a record for.
//
// Returns:
//   - int64: never negative.
func (a EventRecordIntervalAudit) DuplicatedRecords() int64 {
	duplicated := a.CorroboratedRows - a.DistinctCorroboratedRecords
	if duplicated < 0 {
		return 0
	}

	return duplicated
}

// FullyCorroborated reports whether every row claiming a publication names a distinct
// record inside a measured window.
//
// Returns:
//   - bool: true when nothing is uncorroborated and no two rows share a coordinate.
func (a EventRecordIntervalAudit) FullyCorroborated() bool {
	return a.UncorroboratedRows() == 0 && a.DuplicatedRecords() == 0
}

// DeadLetterInventoryEntry is one row of the dead-letter inventory, projected to
// exactly what a triage listing shows.
type DeadLetterInventoryEntry struct {
	// ID is the surrogate key, carried so a keyset cursor can break ties on it.
	ID int64

	// EventID is the event's UUID and the subscriber's idempotency key. It is what a
	// replay is addressed by.
	EventID string

	// EventType is the event's type string, as published.
	EventType string

	// AggregateID is the entity the event is about.
	AggregateID string

	// PartitionKey is the STORED key the original publish used, so an ordering question
	// can be answered from the listing without re-deriving it.
	PartitionKey string

	// LedgerID is the ledger the event belongs to, when it has one.
	LedgerID string

	// Topic is the original category topic the event was destined for.
	Topic string

	// SchemaVersion is the envelope version the event was published under.
	SchemaVersion int

	// OccurredAt is when the event happened. It is the first component of the keyset
	// cursor and the ordering key of the listing.
	OccurredAt time.Time

	// Status is the row's terminal failure state: failed, or dead_lettered.
	Status string

	// Attempts is how many publish attempts were made.
	Attempts int

	// LastError is the raw failure text the last attempt recorded. It is CLASSIFIED
	// before it reaches a response body; nothing publishes it verbatim.
	LastError string

	// FirstAttemptedAt and LastAttemptedAt bound the retry window.
	FirstAttemptedAt *time.Time
	LastAttemptedAt  *time.Time

	// DLTTopic is the `<topic>.dlt` sibling the event was preserved on, empty while the
	// dead-letter write is still owed.
	DLTTopic string

	// FailureMetadata is the stored failure record, or nil when none was written.
	FailureMetadata json.RawMessage

	// PayloadBytes is the size of the stored body in bytes, measured in SQL so the bytes
	// themselves never leave the database.
	PayloadBytes int
}

// EffectiveKey is the Kafka message key this entry was published under, resolved
// through the same rule the publish path uses. See EffectivePartitionKey.
//
// Returns:
//   - string: the ledger id when the entry has one, otherwise the stored partition key.
func (e DeadLetterInventoryEntry) EffectiveKey() string {
	return EffectivePartitionKey(e.LedgerID, e.PartitionKey)
}

// DeadLetterCursor is a position in the dead-letter inventory, expressed as the
// ordering key of the last row a page returned rather than as a row count.
type DeadLetterCursor struct {
	// OccurredAt is the occurrence instant of the last row returned.
	OccurredAt time.Time

	// ID is that row's surrogate key, which breaks ties between rows sharing an instant.
	ID int64
}

// eventCursorSeparator joins a cursor's two components. A colon cannot appear in either —
// one is a decimal nanosecond count, the other a decimal integer — so the split is
// unambiguous.
const eventCursorSeparator = ":"

// Encode renders the cursor as the opaque token an API hands back to a caller.
//
// Returns:
//   - string: the token, or "" for a zero cursor, which means "start at the beginning".
func (c DeadLetterCursor) Encode() string {
	if c.OccurredAt.IsZero() && c.ID == 0 {
		return ""
	}

	return base64.RawURLEncoding.EncodeToString([]byte(
		strconv.FormatInt(c.OccurredAt.UTC().UnixNano(), 10) + eventCursorSeparator +
			strconv.FormatInt(c.ID, 10),
	))
}

// ParseDeadLetterCursor decodes a token produced by Encode.
//
// Parameters:
//   - token string: the opaque cursor. Empty yields a nil cursor and no error, which
//     means "start at the beginning".
//
// Returns:
//   - *DeadLetterCursor: the decoded position, or nil for an empty token.
//   - error: ErrInvalidDeadLetterCursor for anything that does not decode.
func ParseDeadLetterCursor(token string) (*DeadLetterCursor, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrInvalidDeadLetterCursor
	}

	instant, identifier, found := strings.Cut(string(raw), eventCursorSeparator)
	if !found {
		return nil, ErrInvalidDeadLetterCursor
	}

	nanos, err := strconv.ParseInt(instant, 10, 64)
	if err != nil {
		return nil, ErrInvalidDeadLetterCursor
	}

	id, err := strconv.ParseInt(identifier, 10, 64)
	if err != nil {
		return nil, ErrInvalidDeadLetterCursor
	}

	return &DeadLetterCursor{OccurredAt: time.Unix(0, nanos).UTC(), ID: id}, nil
}

// ErrInvalidDeadLetterCursor reports a cursor token that cannot be decoded. It is a
// sentinel so the API layer can answer it as a validation error naming the parameter
// rather than as an internal fault.
var ErrInvalidDeadLetterCursor = errors.New("model: the dead-letter cursor is not a token this inventory issued")

// DeadLetterInventoryQuery narrows and pages the dead-letter inventory.
type DeadLetterInventoryQuery struct {
	// Limit is the maximum number of entries to return. The repository defaults and caps
	// it, so a malformed request degrades to a cheap page.
	Limit int

	// EventType narrows to one event type exactly. Empty means every type.
	EventType string

	// Topic narrows to one ORIGINAL category topic. The `.dlt` spelling is resolved to
	// the original by the API before it reaches here, so this compares one value.
	Topic string

	// Status narrows to one terminal failure state. Empty means both — which is the
	// default, because a row that exhausted its retries but whose dead-letter write also
	// failed is the one an operator most needs to see.
	Status string

	// Cursor resumes a previous page. Nil starts at the newest entry.
	Cursor *DeadLetterCursor
	// OccurredFrom and OccurredTo bound the event's OCCURRENCE instant inclusively, and
	// either may be zero to leave that end unbounded.
	OccurredFrom time.Time
	OccurredTo   time.Time
}

// FilterQuery is this page's narrowing with the PAGE dropped: the same event type,
// topic, status and occurrence window, without the cursor or the limit.
//
// Returns:
//   - DeadLetterQuery: the same narrowing, page-unbounded.
func (q DeadLetterInventoryQuery) FilterQuery() DeadLetterQuery {
	return DeadLetterQuery{
		EventType:    q.EventType,
		Topic:        q.Topic,
		Status:       q.Status,
		OccurredFrom: q.OccurredFrom,
		OccurredTo:   q.OccurredTo,
	}
}

// DeadLetterInventoryPage is one page of the inventory, plus what a caller needs to ask for
// the next one.
type DeadLetterInventoryPage struct {
	// Entries are the matching rows, newest occurrence first. Never nil on success.
	Entries []DeadLetterInventoryEntry

	// NextCursor is the position to resume from, nil when this page is the last one.
	NextCursor *DeadLetterCursor

	// HasMore mirrors NextCursor != nil, so a response can report the fact without
	// exposing the token to a reader that does not need it.
	HasMore bool
}

// SubscriberCursor is a position in the subscriber registry, expressed as the ordering
// key of the last row a page returned.
type SubscriberCursor struct {
	// CreatedAt is the registration instant of the last row returned.
	CreatedAt time.Time

	// ID is that row's surrogate key.
	ID int64
}

// Encode renders the cursor as the opaque token an API hands back. See
// DeadLetterCursor.Encode for the format and for why it is opaque.
//
// Returns:
//   - string: the token, or "" for a zero cursor.
func (c SubscriberCursor) Encode() string {
	if c.CreatedAt.IsZero() && c.ID == 0 {
		return ""
	}

	return base64.RawURLEncoding.EncodeToString([]byte(
		strconv.FormatInt(c.CreatedAt.UTC().UnixNano(), 10) + eventCursorSeparator +
			strconv.FormatInt(c.ID, 10),
	))
}

// ParseSubscriberCursor decodes a token produced by SubscriberCursor.Encode.
//
// Parameters:
//   - token string: the opaque cursor. Empty yields a nil cursor and no error.
//
// Returns:
//   - *SubscriberCursor: the decoded position, or nil for an empty token.
//   - error: ErrInvalidSubscriberCursor for anything that does not decode.
func ParseSubscriberCursor(token string) (*SubscriberCursor, error) {
	position, err := ParseDeadLetterCursor(token)
	if err != nil {
		return nil, ErrInvalidSubscriberCursor
	}
	if position == nil {
		return nil, nil
	}

	return &SubscriberCursor{CreatedAt: position.OccurredAt, ID: position.ID}, nil
}

// ErrInvalidSubscriberCursor reports a subscriber cursor token that cannot be decoded.
var ErrInvalidSubscriberCursor = errors.New("model: the subscriber cursor is not a token this registry issued")

// SubscriberPageQuery pages the subscriber registry by key rather than by depth.
type SubscriberPageQuery struct {
	// Limit is the maximum number of subscribers to return. The repository defaults and
	// caps it.
	Limit int

	// Cursor resumes a previous page. Nil starts at the most recently registered
	// subscriber.
	Cursor *SubscriberCursor
}

// SubscriberPage is one page of the registry plus the position to resume from.
type SubscriberPage struct {
	// Subscribers are the rows, newest registration first. Never nil on success.
	Subscribers []EventSubscriber

	// NextCursor is the position to resume from, nil when this page is the last one.
	NextCursor *SubscriberCursor

	// HasMore mirrors NextCursor != nil.
	HasMore bool
}

// DeadLetterTopicAge is the oldest outstanding dead-letter entry on one topic, and how
// many are outstanding there.
type DeadLetterTopicAge struct {
	// Topic is the dead-letter topic the entries belong to, or the original topic's
	// `.dlt` sibling for a row whose dead-letter write has not happened yet.
	Topic string

	// Oldest is the age instant of the oldest outstanding entry on that topic.
	Oldest time.Time

	// Outstanding is how many entries the topic holds.
	Outstanding int64
}

// SubscriberRevocationBacklog is how much broker-side credential revocation is still
// OWED, and for how long the oldest debt has been outstanding.
type SubscriberRevocationBacklog struct {
	// Pending is how many subscribers carry an unsettled revocation marker. Zero is the
	// healthy steady state and is a meaningful reading rather than an absent one.
	Pending int64

	// OldestPendingAt is when the OLDEST outstanding marker was recorded. It is the zero
	// value when nothing is outstanding, which is why callers must test it rather than
	// subtracting blindly — an epoch-zero instant would otherwise render as an age of
	// fifty-odd years and pin every alert on it.
	OldestPendingAt time.Time
}

// OldestAge is how long the oldest outstanding revocation has been owed, measured
// against the supplied instant.
//
// Parameters:
//   - now time.Time: the instant to measure against, supplied so a caller can use its
//     own clock and a test can be exact.
//
// Returns:
//   - time.Duration: never negative.
func (b SubscriberRevocationBacklog) OldestAge(now time.Time) time.Duration {
	if b.OldestPendingAt.IsZero() {
		return 0
	}

	age := now.Sub(b.OldestPendingAt)
	if age < 0 {
		return 0
	}

	return age
}

// SubscriberAccessResidue is how much broker-side access is UNACCOUNTED FOR:
type SubscriberAccessResidue struct {
	// OrphanedCredentials is how many rows carry an unsettled CredentialOrphanedAt. Zero
	// is the healthy steady state and is a meaningful reading rather than an absent one.
	OrphanedCredentials int64

	// OldestOrphanedAt is when the OLDEST unsettled orphan was recorded, or the zero
	// instant when there is none. Callers must test it rather than subtracting blindly —
	// an epoch-zero instant renders as an age of fifty-odd years and would pin every alert
	// built on it.
	OldestOrphanedAt time.Time

	// FailedRevocations is how many rows carry an unsettled RevocationFailedAt.
	FailedRevocations int64

	// OldestFailedRevocationAt is when the OLDEST of those attempts failed, or the zero
	// instant when none has.
	OldestFailedRevocationAt time.Time
}

// OldestOrphanAge is how long the oldest orphaned credential has been outstanding.
//
// Parameters:
//   - now time.Time: the instant to measure against, supplied so a caller can use its
//     own clock and a test can be exact.
//
// Returns:
//   - time.Duration: never negative.
func (r SubscriberAccessResidue) OldestOrphanAge(now time.Time) time.Duration {
	return nonNegativeAgeSince(r.OldestOrphanedAt, now)
}

// OldestFailedRevocationAge is how long the oldest refused revocation has stood.
//
// Parameters:
//   - now time.Time: the instant to measure against.
//
// Returns:
//   - time.Duration: never negative, and zero when no attempt has failed.
func (r SubscriberAccessResidue) OldestFailedRevocationAge(now time.Time) time.Duration {
	return nonNegativeAgeSince(r.OldestFailedRevocationAt, now)
}

// Settled reports whether there is no unaccounted broker-side access at all.
//
// Returns:
//   - bool: true when both counts are zero.
func (r SubscriberAccessResidue) Settled() bool {
	return r.OrphanedCredentials == 0 && r.FailedRevocations == 0
}

// nonNegativeAgeSince measures an age from a possibly-zero instant.
func nonNegativeAgeSince(at, now time.Time) time.Duration {
	if at.IsZero() {
		return 0
	}

	age := now.Sub(at)
	if age < 0 {
		return 0
	}

	return age
}

// FullyConfirmed reports whether every row claiming a publication names a distinct
// record.
type SubscriberSettlementBacklog struct {
	// Outstanding is how many subscribers owe EITHER obligation. It is not the sum of the two
	// counts below, because one subscriber can owe both.
	Outstanding int64

	// GrantReconcilePending is how many owe a broker-side grant reconciliation.
	GrantReconcilePending int64

	// CredentialCleanupPending is how many owe a credential cleanup.
	CredentialCleanupPending int64

	// OldestPendingAt is when the oldest outstanding obligation of EITHER kind was
	// recorded. It is the zero value when nothing is outstanding, which is why callers
	// must test it rather than subtracting blindly — an epoch-zero instant would render as
	// an age of fifty-odd years and pin every alert on it.
	OldestPendingAt time.Time
}

// OldestAge is how long the oldest outstanding obligation has been owed, measured
// against the supplied instant.
//
// Parameters:
//   - now time.Time: the instant to measure against, supplied so a caller can use its
//     own clock and a test can be exact.
//
// Returns:
//   - time.Duration: never negative.
func (b SubscriberSettlementBacklog) OldestAge(now time.Time) time.Duration {
	if b.OldestPendingAt.IsZero() {
		return 0
	}

	age := now.Sub(b.OldestPendingAt)
	if age < 0 {
		return 0
	}

	return age
}

// EventOutboxPurgeTotals is what retention has removed from the event outbox over the
// table's whole life, and it is what lets the zero-loss reconciliation compare two
// quantities that describe the same interval.
type EventOutboxPurgeTotals struct {
	// Recorded reports whether the totals could be read at all.
	Recorded bool

	// RowsRemoved is how many terminal rows retention has deleted in total. It is added
	// to the surviving terminal rows to restore the all-time count.
	RowsRemoved int64

	// ConfirmedRemoved is how many of those rows carried a broker coordinate, and
	// therefore how many records the broker still counts have lost the row that named
	// them.
	ConfirmedRemoved int64

	// Batches is how many purge sweeps have recorded a deletion. Zero with Recorded true
	// means retention has never removed a terminal row, which is the one state in which
	// the arithmetic needs no correction at all.
	Batches int64

	// LastPurgedAt is when the most recent recorded batch committed, or nil if none has.
	LastPurgedAt *time.Time

	// NewestPurgedOccurrence is the latest occurrence instant among all purged rows, or
	// nil when unknown. It bounds the purged window from above, which is what an operator
	// needs in order to tell whether a measurement window overlaps a purge.
	NewestPurgedOccurrence *time.Time
}

// PurgeHasOccurred reports whether retention is known to have removed terminal rows.
//
// Returns:
//   - bool: true only when the log was read AND it records at least one deletion.
func (t EventOutboxPurgeTotals) PurgeHasOccurred() bool {
	return t.Recorded && t.RowsRemoved > 0
}

// EventRecordCoordinate is what one outbox row claims about the broker: the exact
// record its publish produced.
type EventRecordCoordinate struct {
	// Topic and Partition identify the log the claims belong to.
	Topic     string
	Partition int

	// Rows is how many terminal rows claim a record in this partition.
	Rows int64

	// MinOffset and MaxOffset bound the claimed offsets. They are compared against the
	// partition's live bounds: MaxOffset at or beyond the end offset means rows claim
	// records the log does not contain, and MinOffset below the first retained offset
	// means the oldest claims can no longer be verified because those records have aged
	// out.
	MinOffset int64
	MaxOffset int64
}

// EventRecordCoordinateAudit is every partition the outbox claims a record in.
type EventRecordCoordinateAudit struct {
	// Coordinates is one entry per (topic, partition) the outbox names, in topic then
	// partition order so a report reads deterministically.
	Coordinates []EventRecordCoordinate

	// MeasuredAt is when the outbox was read.
	MeasuredAt time.Time
}

// TotalRows sums the claims across every partition.
//
// Returns:
//   - int64: how many terminal rows name a broker record.
func (a EventRecordCoordinateAudit) TotalRows() int64 {
	var total int64
	for _, coordinate := range a.Coordinates {
		total += coordinate.Rows
	}

	return total
}

// HasFilters reports whether any narrowing was requested.
//
// Returns:
//   - bool: true when at least one of the three filters is set.
func (q DeadLetterQuery) HasFilters() bool {
	return q.EventType != "" || q.Topic != "" || q.Status != ""
}

// UncorroboratedRows is how many claims of publication could not be placed inside a
// measured window, for any reason.
//
// Returns:
//   - int64: never negative.
func (a EventRecordIntervalAudit) UncorroboratedRows() int64 {
	uncorroborated := a.UnconfirmedRows + a.UnmeasuredRows + a.AgedOutRows + a.BeyondEndRows
	if uncorroborated < 0 {
		return 0
	}

	return uncorroborated
}

// EventOutboxAudit is the outbox side of the zero-loss reconciliation: how many rows
// claim a Kafka record, and how many of those can actually name the record they
// produced.
type EventOutboxAudit struct {
	// PublishedRows is how many rows claim a record on the broker: every row whose Kafka
	// leg completed, plus every dead-lettered row, each counted exactly once by virtue of
	// the unique index on event_id.
	PublishedRows int64

	// ConfirmedRows is how many of those name the record they produced.
	ConfirmedRows int64

	// DistinctRecords is how many DISTINCT coordinates those rows name. It equals
	// ConfirmedRows unless two rows claim the same record, which the partial unique index
	// on the coordinate makes impossible — so a discrepancy here means the index is
	// missing or has been dropped, and the audit says so rather than assuming the schema
	// is intact.
	DistinctRecords int64

	// MeasuredAt is when the outbox side was read.
	MeasuredAt time.Time

	// WindowStart is the earliest publication instant the three counts above include.
	WindowStart time.Time
}

// UnconfirmedRows is how many rows claim a publication they cannot name a record for.
//
// Returns:
//   - int64: never negative.
func (a EventOutboxAudit) UnconfirmedRows() int64 {
	if a.ConfirmedRows >= a.PublishedRows {
		return 0
	}

	return a.PublishedRows - a.ConfirmedRows
}

// FullyConfirmed reports whether every row claiming a publication names a distinct
// record.
//
// Returns:
//   - bool: true when nothing is unconfirmed and no two rows share a coordinate.
func (a EventOutboxAudit) FullyConfirmed() bool {
	return a.UnconfirmedRows() == 0 && a.DistinctRecords == a.ConfirmedRows
}
