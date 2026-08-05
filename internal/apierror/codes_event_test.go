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

// This file covers the seven error codes added for the Kafka event-streaming
// pipeline: dead-letter triage and replay (EVENT_*), Kafka subscriber registry
// and credential provisioning (SUBSCRIBER_*), and the sunset of the legacy
// webhook management surface (GEN_GONE).
//
// Why these assertions are not redundant with codes_test.go's TestStatusForCode
// table: a missing statusByCode entry is invisible. It produces no compile
// error, no panic and no log line — StatusForCode simply falls through to its
// documented 500 default for unknown codes. The only way to make the difference
// between "mapped" and "silently defaulted to 500" observable is to assert the
// resolved status per code and, where the intended status is itself 500, to look
// the key up in the map directly. Both are done below.
//
// The codes reach clients through one resolution chain, which every test here
// exercises rather than approximating: api/errors.go calls Normalize and then
// StatusForCode in writeError, respondBareAPIError and respondNestedAPIError,
// and api/middleware/auth.go passes a code straight to StatusForCode when it
// aborts. The webhook sunset guard reuses that same chain, so GEN_GONE resolving
// to 410 here is what makes the sunset behaviour possible at the HTTP boundary.

// eventStreamingCodeCases is the single inventory of the seven new codes: the
// exported identifier, the HTTP status statusByCode must resolve it to, and the
// exact string that goes on the wire.
//
// Every test in this file iterates this one table, so the seven codes are
// enumerated exactly once and the coverage areas cannot drift apart as the
// catalog evolves. The anonymous-struct shape matches the tables in
// codes_test.go; it is hoisted to file scope only so it can be shared.
var eventStreamingCodeCases = []struct {
	code   ErrorCode // the exported identifier under test
	status int       // the status statusByCode must map it to
	value  string    // the exact client-facing string value
}{
	{ErrGenGone, http.StatusGone, "GEN_GONE"},
	{ErrEventNotFound, http.StatusNotFound, "EVENT_NOT_FOUND"},
	{ErrEventNotDeadLettered, http.StatusConflict, "EVENT_NOT_DEAD_LETTERED"},
	{ErrEventReplayFailed, http.StatusInternalServerError, "EVENT_REPLAY_FAILED"},
	// The identifier says Kafka but the string carries the EVENT_ family prefix.
	// That asymmetry is deliberate: assert it exactly, do not "correct" it. Its
	// 503 is equally deliberate — an unreachable broker is a retryable upstream
	// condition, not a defect in this service, so it must not resolve to 500.
	{ErrKafkaUnavailable, http.StatusServiceUnavailable, "EVENT_KAFKA_UNAVAILABLE"},
	{ErrSubscriberNotFound, http.StatusNotFound, "SUBSCRIBER_NOT_FOUND"},
	{ErrSubscriberProvisioningFailed, http.StatusServiceUnavailable, "SUBSCRIBER_PROVISIONING_FAILED"},
}

// TestStatusForCode_EventStreamingCodes asserts that each of the seven new codes
// resolves through StatusForCode to its intended HTTP status. This is the
// positive statement of the mapping; TestStatusForCode_EventCodesAreMappedNotDefaulted
// below is the one that proves the mapping is real rather than the 500 default.
func TestStatusForCode_EventStreamingCodes(t *testing.T) {
	// Guard the inventory itself. codes.go declares seven new consts and seven
	// matching statusByCode entries, so a table that no longer holds seven rows
	// means a code was added or removed without its assertions coming along.
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

// TestStatusForCode_GenGoneIsGone asserts GEN_GONE resolves to 410 Gone on its
// own, separately from the table above.
//
// It is called out because it is the single mapping the webhook sunset depends
// on: the deprecated webhook management routes answer 410 only because this code
// carries an explicit statusByCode entry. Without one, StatusForCode would
// return 500 for it and the sunset would report a server error instead of a
// permanently removed surface.
func TestStatusForCode_GenGoneIsGone(t *testing.T) {
	if got := StatusForCode(ErrGenGone); got != http.StatusGone {
		t.Errorf("StatusForCode(%s) = %d, want %d (410 Gone)", ErrGenGone, got, http.StatusGone)
	}
	// State the failure mode explicitly: 500 is what an unmapped code produces,
	// and it is the exact symptom a missing GEN_GONE entry would present as.
	if got := StatusForCode(ErrGenGone); got == http.StatusInternalServerError {
		t.Errorf("StatusForCode(%s) = %d: the code is not mapped and fell through to the unknown-code default", ErrGenGone, got)
	}
}

// TestStatusForCode_EventCodesAreMappedNotDefaulted proves each new code has a
// real statusByCode entry rather than benefitting from the 500 default.
func TestStatusForCode_EventCodesAreMappedNotDefaulted(t *testing.T) {
	for _, tt := range eventStreamingCodeCases {
		t.Run(string(tt.code), func(t *testing.T) {
			// Direct, two-value membership lookup in the unexported statusByCode
			// map. This is the assertion that cannot be fooled, and the reason
			// this file is in-package rather than in package apierror_test.
			//
			// Do NOT simplify it into a StatusForCode comparison. StatusForCode
			// returns 500 both for a code deliberately mapped to 500 and for a
			// code that is not mapped at all, so for ErrEventReplayFailed —
			// whose intended status IS 500 — a status comparison proves nothing:
			// deleting its map entry would leave that comparison passing by
			// accident. Only this lookup distinguishes the two cases.
			status, ok := statusByCode[tt.code]
			if !ok {
				t.Fatalf("statusByCode has no entry for %s: StatusForCode would silently default it to %d", tt.code, http.StatusInternalServerError)
			}
			if status != tt.status {
				t.Errorf("statusByCode[%s] = %d, want %d", tt.code, status, tt.status)
			}
			// For the six codes whose intended status is not 500, the negative
			// assertion is available too and is the clearest statement of
			// intent: a 500 here can only mean the entry went missing.
			if tt.status != http.StatusInternalServerError {
				if got := StatusForCode(tt.code); got == http.StatusInternalServerError {
					t.Errorf("StatusForCode(%s) = %d, want the mapped %d rather than the unknown-code default", tt.code, got, tt.status)
				}
			}
		})
	}
}

// TestNormalize_EventCodesPassThrough asserts the new codes are canonical: they
// pass through Normalize unchanged and none of them was added to the legacy
// alias map.
//
// This matters because api/errors.go calls Normalize immediately before
// StatusForCode on every response path. A stray legacyToCanonical entry would
// silently rewrite one of these codes to a GEN_* code in every response — the
// client contract and the resolved status would both change, with nothing at the
// call site to show it.
func TestNormalize_EventCodesPassThrough(t *testing.T) {
	for _, tt := range eventStreamingCodeCases {
		t.Run(string(tt.code), func(t *testing.T) {
			if got := Normalize(tt.code); got != tt.code {
				t.Errorf("Normalize(%s) = %s, want unchanged", tt.code, got)
			}
			// Direct absence from the unexported alias map, so the intent is
			// asserted at the source rather than inferred from the result.
			if canonical, ok := legacyToCanonical[tt.code]; ok {
				t.Errorf("legacyToCanonical rewrites %s to %s: the new codes are canonical and must never be aliased", tt.code, canonical)
			}
			// The composition the response writers actually evaluate.
			if got := StatusForCode(Normalize(tt.code)); got != tt.status {
				t.Errorf("StatusForCode(Normalize(%s)) = %d, want %d", tt.code, got, tt.status)
			}
		})
	}
}

// TestEventCodeStringValues pins the exact string value of each new code.
//
// These strings are the client-facing contract — they appear verbatim in the
// error_detail.code field of every error response — so changing one is a
// breaking change for consumers. Asserting them here means such a change cannot
// slip in as a rename.
func TestEventCodeStringValues(t *testing.T) {
	seen := make(map[string]ErrorCode, len(eventStreamingCodeCases))
	for _, tt := range eventStreamingCodeCases {
		t.Run(string(tt.code), func(t *testing.T) {
			if string(tt.code) != tt.value {
				t.Errorf("code value = %q, want %q", string(tt.code), tt.value)
			}
		})
		// Distinct values matter as much as correct ones: two consts sharing a
		// string would collapse to a single statusByCode key, and one code would
		// silently inherit the other's status.
		if previous, duplicate := seen[string(tt.code)]; duplicate {
			t.Errorf("codes %s and %s share the string value %q", previous, tt.code, tt.code)
			continue
		}
		seen[string(tt.code)] = tt.code
	}
}

// TestMapErrorToHTTPStatus_GenGone exercises the error-value resolution path for
// GEN_GONE, which is the path the webhook sunset guard depends on.
//
// Three shapes are covered because the codebase produces all three: a bare
// APIError value, that value wrapped with %w by an intermediate layer, and the
// ErrorResponse envelope whose status api/errors.go resolves through Normalize
// and StatusForCode.
func TestMapErrorToHTTPStatus_GenGone(t *testing.T) {
	base := NewAPIError(ErrGenGone, "webhook delivery was removed on the sunset date", nil)
	if got := MapErrorToHTTPStatus(base); got != http.StatusGone {
		t.Errorf("MapErrorToHTTPStatus(%s) = %d, want %d", ErrGenGone, got, http.StatusGone)
	}
	// Wrapped, mirroring how a handler surfaces an error raised further down.
	wrapped := fmt.Errorf("webhook management is gone: %w", base)
	if got := MapErrorToHTTPStatus(wrapped); got != http.StatusGone {
		t.Errorf("MapErrorToHTTPStatus(wrapped) = %d, want %d", got, http.StatusGone)
	}
	// The response writers never call MapErrorToHTTPStatus; they build the
	// envelope and then resolve Normalize -> StatusForCode. Assert that exact
	// composition, and that the envelope preserves the code rather than
	// normalizing it into a different one.
	resp := NewErrorResponse(ErrGenGone, "webhook management is gone", nil)
	if resp.Error.Code != ErrGenGone {
		t.Errorf("NewErrorResponse code = %s, want %s", resp.Error.Code, ErrGenGone)
	}
	if got := StatusForCode(Normalize(resp.Error.Code)); got != http.StatusGone {
		t.Errorf("StatusForCode(Normalize(%s)) = %d, want %d", resp.Error.Code, got, http.StatusGone)
	}
}

// TestMapErrorToHTTPStatus_EventStreamingCodes resolves all seven codes as error
// values, covering the handlers that return them: the dead-letter list and
// replay endpoints, the outbox statistics endpoint, and subscriber credential
// provisioning. Each is asserted both bare and wrapped, because a handler may
// return either.
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
