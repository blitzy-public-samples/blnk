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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

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
func originalTopicOf(row model.EventOutbox) string {
	if topic := strings.TrimSpace(row.Topic); topic != "" {
		return topic
	}

	return TopicForEvent(row.EventType)
}

// ReplayTopicFor returns the topic a dead-lettered event must be replayed to.
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
