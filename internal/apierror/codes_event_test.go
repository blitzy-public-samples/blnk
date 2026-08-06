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

package apierror

import (
	"fmt"
	"net/http"
	"testing"
)

// This file covers the error codes of the Kafka event-streaming pipeline: EVENT_*
// for dead-letter triage and replay, SUBSCRIBER_* for the registry and credential
// provisioning, and GEN_GONE for a deprecated webhook route past its sunset.
//
// A missing statusByCode entry is invisible — no compile error, no panic, no log
// line — because StatusForCode falls through to its documented 500 default for
// unknown codes. Telling "mapped" apart from "silently defaulted to 500" therefore
// takes two assertions: the resolved status per code, and, where the intended status
// is itself 500, a direct lookup of the key in the map. The second is why this file
// is in-package rather than in package apierror_test.

// eventStreamingCodeCases is the single inventory of the new codes — identifier,
// the status statusByCode must resolve it to, and the exact string on the wire.
// Every test here iterates it, so the codes are enumerated once.
var eventStreamingCodeCases = []struct {
	code   ErrorCode // the exported identifier under test
	status int       // the status statusByCode must map it to
	value  string    // the exact client-facing string value
}{
	{ErrGenGone, http.StatusGone, "GEN_GONE"},
	{ErrEventNotFound, http.StatusNotFound, "EVENT_NOT_FOUND"},
	{ErrEventNotDeadLettered, http.StatusConflict, "EVENT_NOT_DEAD_LETTERED"},
	{ErrEventReplayFailed, http.StatusInternalServerError, "EVENT_REPLAY_FAILED"},
	// The identifier says Kafka but the string carries the EVENT_ family prefix, and
	// that asymmetry is deliberate. So is the 503: an unreachable broker is a
	// retryable upstream condition, not a defect here, so it must not resolve to 500.
	{ErrKafkaUnavailable, http.StatusServiceUnavailable, "EVENT_KAFKA_UNAVAILABLE"},
	{ErrSubscriberNotFound, http.StatusNotFound, "SUBSCRIBER_NOT_FOUND"},
	{ErrSubscriberProvisioningFailed, http.StatusServiceUnavailable, "SUBSCRIBER_PROVISIONING_FAILED"},
}

// TestStatusForCode_EventStreamingCodes states the mapping positively;
// TestStatusForCode_EventCodesAreMappedNotDefaulted below is what proves it is real
// rather than the 500 default.
func TestStatusForCode_EventStreamingCodes(t *testing.T) {
	// Guard the inventory itself: a table that no longer holds one row per code in
	// codes.go means a code was added or removed without its assertions.
	if len(eventStreamingCodeCases) != 7 {
		t.Fatalf("eventStreamingCodeCases has %d rows, want 7 (one per new code in codes.go)", len(eventStreamingCodeCases))
	}
	for _, tt := range eventStreamingCodeCases {
		t.Run(string(tt.code), func(t *testing.T) {
			if got := StatusForCode(tt.code); got != tt.status {
				t.Errorf("StatusForCode(%s) = %d, want %d", tt.code, got, tt.status)
			}
		})
	}
}

// TestStatusForCode_GenGoneIsGone is called out separately because it is the one
// mapping the webhook sunset depends on: a deprecated route can answer 410 only
// because this code carries an explicit statusByCode entry, and without one it
// would report a server error instead.
func TestStatusForCode_GenGoneIsGone(t *testing.T) {
	if got := StatusForCode(ErrGenGone); got != http.StatusGone {
		t.Errorf("StatusForCode(%s) = %d, want %d (410 Gone)", ErrGenGone, got, http.StatusGone)
	}
	if got := StatusForCode(ErrGenGone); got == http.StatusInternalServerError {
		t.Errorf("StatusForCode(%s) = %d: the code is not mapped and fell through to the unknown-code default", ErrGenGone, got)
	}
}

// TestStatusForCode_EventCodesAreMappedNotDefaulted proves each new code has a
// real statusByCode entry rather than benefitting from the 500 default.
func TestStatusForCode_EventCodesAreMappedNotDefaulted(t *testing.T) {
	for _, tt := range eventStreamingCodeCases {
		t.Run(string(tt.code), func(t *testing.T) {
			// A two-value lookup rather than a StatusForCode comparison, because
			// StatusForCode returns 500 both for a code deliberately mapped to 500
			// and for one not mapped at all. For ErrEventReplayFailed, whose
			// intended status is 500, only this lookup tells the two apart.
			status, ok := statusByCode[tt.code]
			if !ok {
				t.Fatalf("statusByCode has no entry for %s: StatusForCode would silently default it to %d", tt.code, http.StatusInternalServerError)
			}
			if status != tt.status {
				t.Errorf("statusByCode[%s] = %d, want %d", tt.code, status, tt.status)
			}
			// Where the intended status is not 500, a 500 can only mean the entry
			// went missing.
			if tt.status != http.StatusInternalServerError {
				if got := StatusForCode(tt.code); got == http.StatusInternalServerError {
					t.Errorf("StatusForCode(%s) = %d, want the mapped %d rather than the unknown-code default", tt.code, got, tt.status)
				}
			}
		})
	}
}

// TestNormalize_EventCodesPassThrough matters because every response path calls
// Normalize immediately before StatusForCode. A stray legacyToCanonical entry would
// rewrite one of these codes to a GEN_* code in every response, changing both the
// client contract and the resolved status with nothing at the call site to show it.
func TestNormalize_EventCodesPassThrough(t *testing.T) {
	for _, tt := range eventStreamingCodeCases {
		t.Run(string(tt.code), func(t *testing.T) {
			if got := Normalize(tt.code); got != tt.code {
				t.Errorf("Normalize(%s) = %s, want unchanged", tt.code, got)
			}
			if canonical, ok := legacyToCanonical[tt.code]; ok {
				t.Errorf("legacyToCanonical rewrites %s to %s: the new codes are canonical and must never be aliased", tt.code, canonical)
			}
			if got := StatusForCode(Normalize(tt.code)); got != tt.status {
				t.Errorf("StatusForCode(Normalize(%s)) = %d, want %d", tt.code, got, tt.status)
			}
		})
	}
}

// TestEventCodeStringValues pins each string value because these strings are the
// client-facing contract — they appear verbatim in error_detail.code — so changing
// one is a breaking change rather than a rename.
func TestEventCodeStringValues(t *testing.T) {
	seen := make(map[string]ErrorCode, len(eventStreamingCodeCases))
	for _, tt := range eventStreamingCodeCases {
		t.Run(string(tt.code), func(t *testing.T) {
			if string(tt.code) != tt.value {
				t.Errorf("code value = %q, want %q", string(tt.code), tt.value)
			}
		})
		// Two consts sharing a string would collapse to one statusByCode key, and
		// one code would silently inherit the other's status.
		if previous, duplicate := seen[string(tt.code)]; duplicate {
			t.Errorf("codes %s and %s share the string value %q", previous, tt.code, tt.code)
			continue
		}
		seen[string(tt.code)] = tt.code
	}
}

// TestMapErrorToHTTPStatus_GenGone covers three shapes because the codebase
// produces all three: a bare APIError, that value wrapped with %w by an
// intermediate layer, and the ErrorResponse envelope api/errors.go resolves through
// Normalize and StatusForCode.
func TestMapErrorToHTTPStatus_GenGone(t *testing.T) {
	base := NewAPIError(ErrGenGone, "webhook delivery was removed on the sunset date", nil)
	if got := MapErrorToHTTPStatus(base); got != http.StatusGone {
		t.Errorf("MapErrorToHTTPStatus(%s) = %d, want %d", ErrGenGone, got, http.StatusGone)
	}
	wrapped := fmt.Errorf("webhook management is gone: %w", base)
	if got := MapErrorToHTTPStatus(wrapped); got != http.StatusGone {
		t.Errorf("MapErrorToHTTPStatus(wrapped) = %d, want %d", got, http.StatusGone)
	}
	// The response writers never call MapErrorToHTTPStatus: they build the envelope
	// and resolve Normalize -> StatusForCode, so assert that composition too.
	resp := NewErrorResponse(ErrGenGone, "webhook management is gone", nil)
	if resp.Error.Code != ErrGenGone {
		t.Errorf("NewErrorResponse code = %s, want %s", resp.Error.Code, ErrGenGone)
	}
	if got := StatusForCode(Normalize(resp.Error.Code)); got != http.StatusGone {
		t.Errorf("StatusForCode(Normalize(%s)) = %d, want %d", resp.Error.Code, got, http.StatusGone)
	}
}

// TestMapErrorToHTTPStatus_EventStreamingCodes resolves every code as an error
// value, bare and wrapped, because a handler may return either.
func TestMapErrorToHTTPStatus_EventStreamingCodes(t *testing.T) {
	for _, tt := range eventStreamingCodeCases {
		t.Run(string(tt.code), func(t *testing.T) {
			base := NewAPIError(tt.code, "event streaming failure", nil)
			if got := MapErrorToHTTPStatus(base); got != tt.status {
				t.Errorf("MapErrorToHTTPStatus(%s) = %d, want %d", tt.code, got, tt.status)
			}
			wrapped := fmt.Errorf("handling failed: %w", base)
			if got := MapErrorToHTTPStatus(wrapped); got != tt.status {
				t.Errorf("MapErrorToHTTPStatus(wrapped %s) = %d, want %d", tt.code, got, tt.status)
			}
		})
	}
}
