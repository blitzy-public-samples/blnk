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
	// 409 and not 404: the row exists and the caller's request is well formed — the resolution
	// it asks for has already been recorded, so repeating it would overwrite one operator's note
	// with another's. The remedy is to read the existing resolution, not to retry.
	{ErrEventAlreadyResolved, http.StatusConflict, "EVENT_ALREADY_RESOLVED"},
	// The identifier says Kafka but the string carries the EVENT_ family prefix, and
	// that asymmetry is deliberate. So is the 503: an unreachable broker is a
	// retryable upstream condition, not a defect here, so it must not resolve to 500.
	{ErrKafkaUnavailable, http.StatusServiceUnavailable, "EVENT_KAFKA_UNAVAILABLE"},
	{ErrSubscriberNotFound, http.StatusNotFound, "SUBSCRIBER_NOT_FOUND"},
	{ErrSubscriberProvisioningFailed, http.StatusServiceUnavailable, "SUBSCRIBER_PROVISIONING_FAILED"},
	// A dependency of issuance being unconfigured is not a malformed request, so 503
	// and never 400: only an operator can supply the externally advertised list.
	{ErrSubscriberBrokersNotConfigured, http.StatusServiceUnavailable, "SUBSCRIBER_BROKERS_NOT_CONFIGURED"},
	// The two STATE refusals. Both 409, because the request is well formed and it is
	// the registry row that has to change before the identical request can succeed.
	//
	// SUBSCRIBER_ISOLATION_UNENFORCEABLE was a third, and it is deliberately GONE rather
	// than retained unused: it named the refusal to issue a credential to a subscriber
	// recording a partition-key prefix, which is no longer refused — the prefix is a
	// consumer-side filtering contract, disclosed with the credential instead of standing in
	// the way of it. A code nothing can return documents a refusal that does not happen.
	{ErrSubscriberDeprovisioning, http.StatusConflict, "SUBSCRIBER_DEPROVISIONING"},
	{ErrSubscriberGrantEmpty, http.StatusConflict, "SUBSCRIBER_GRANT_EMPTY"},
	// A fourth state refusal, and the one whose state lives at the BROKER rather than in the
	// registry row. 409 for the same reason and never the 503 of the *_FAILED codes: the
	// broker answered, so there is no upstream condition for a retry to outlast.
	{ErrSubscriberAccessExceedsAuthorization, http.StatusConflict, "SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION"},
	// 403, and the only one here that is about the CHANNEL rather than about state: the request
	// is well formed and the caller is authorised, and the server is refusing to put a one-time
	// secret on a transport it cannot establish as confidential. A retry over the same transport
	// cannot succeed, which is why it is not a 503.
	{ErrSubscriberInsecureTransport, http.StatusForbidden, "SUBSCRIBER_INSECURE_TRANSPORT"},
	// 504, not 503 and emphatically not the 500 a missing entry would produce: the
	// dependency answered too slowly, or the caller went away.
	{ErrSubscriberProvisioningTimeout, http.StatusGatewayTimeout, "SUBSCRIBER_PROVISIONING_TIMEOUT"},
}

// TestStatusForCode_EventStreamingCodes states the mapping positively;
// TestStatusForCode_EventCodesAreMappedNotDefaulted below is what proves it is real
// rather than the 500 default.
func TestStatusForCode_EventStreamingCodes(t *testing.T) {
	// Guard the inventory itself: a table that no longer holds one row per code in
	// codes.go means a code was added or removed without its assertions.
	//
	// The count is stated literally rather than derived, because deriving it from the
	// catalog is what the guard exists to prevent: a code that arrives with neither a
	// status entry nor a row here would then satisfy a self-referential comparison and
	// resolve to the unknown-code 500 in production. Adding a code is a deliberate edit
	// of this number — as is REMOVING one, and the arithmetic that produced 14 is worth
	// recording because every step of it was a separate edit:
	//
	//   12 — the original event-streaming family.
	//   −11 — SUBSCRIBER_ISOLATION_UNENFORCEABLE retired, because the refusal it named was
	//        replaced by issuing the credential and delivering the key scope to the consumer.
	//   +14 — EVENT_ALREADY_RESOLVED, SUBSCRIBER_INSECURE_TRANSPORT and
	//        SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION added.
	//
	// The number was left at 11 while the rows were added, which is how the guard came to fail
	// for the right reason — the inventory no longer matched codes.go — and it caught a genuine
	// omission underneath: SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION had no statusByCode entry at
	// all, so a deliberate 409 was resolving to 500.
	if len(eventStreamingCodeCases) != 14 {
		t.Fatalf("eventStreamingCodeCases has %d rows, want 14 (one per event-streaming code in codes.go)", len(eventStreamingCodeCases))
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

// TestMapErrorToHTTPStatus_PointerShapedAPIError covers the *APIError branch of
// MapErrorToHTTPStatus, which nothing else in this package reached.
//
// # Why the branch exists at all
//
// APIError declares Error() on its VALUE receiver, so both APIError and *APIError
// satisfy the error interface, and errors.As only matches a target whose element
// type the concrete type is assignable to. A handler returning &APIError{...} —
// or any layer that took an address along the way — therefore misses the value
// target entirely and is resolved by the second lookup. Without it such an error
// would fall through to 500, and a deprecated surface past its sunset would answer
// 500 instead of the 410 acceptance criterion V-10 requires.
//
// # Why this test was written
//
// The mutation gate found it. `internal/apierror` scores one viable mutant, the
// `apiErrPtr != nil` guard on this branch, and it LIVED: the branch was covered but
// no assertion depended on its answer, so negating the guard — which sends every
// pointer-shaped API error to 500 — changed nothing any test could see. Both halves
// of the guard are now asserted, so a mutation of either is caught.
func TestMapErrorToHTTPStatus_PointerShapedAPIError(t *testing.T) {
	// A pointer-shaped error must resolve to its mapped status, not to 500. This is
	// the half that fails if the nil guard is negated.
	pointer := &APIError{Code: ErrGenGone, Message: "webhook management is gone"}
	if got := MapErrorToHTTPStatus(pointer); got != http.StatusGone {
		t.Errorf("MapErrorToHTTPStatus(*APIError %s) = %d, want %d", ErrGenGone, got, http.StatusGone)
	}

	// And through a wrapping layer, because that is how it reaches the middleware.
	if got := MapErrorToHTTPStatus(fmt.Errorf("sunset guard: %w", pointer)); got != http.StatusGone {
		t.Errorf("MapErrorToHTTPStatus(wrapped *APIError) = %d, want %d", got, http.StatusGone)
	}

	// Every event-streaming code resolves the same way when carried by a pointer, so
	// the branch is not correct for one code by accident.
	for _, tt := range eventStreamingCodeCases {
		t.Run(string(tt.code), func(t *testing.T) {
			carried := &APIError{Code: tt.code, Message: "event streaming failure"}
			if got := MapErrorToHTTPStatus(carried); got != tt.status {
				t.Errorf("MapErrorToHTTPStatus(*APIError %s) = %d, want %d", tt.code, got, tt.status)
			}
		})
	}

	// A TYPED NIL is the half that fails if the guard is removed rather than
	// negated. errors.As succeeds against a nil *APIError — the type matches — so
	// without the guard the next line would dereference nil and take down the
	// process that was merely trying to choose a status code. 500 is the right
	// answer: an error carrying no code is exactly the unclassified case.
	var absent *APIError
	var carried error = absent
	if got := MapErrorToHTTPStatus(carried); got != http.StatusInternalServerError {
		t.Errorf("MapErrorToHTTPStatus(typed-nil *APIError) = %d, want %d",
			got, http.StatusInternalServerError)
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
