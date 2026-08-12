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
	"strings"
	"testing"
)

// This file covers the error codes of the Kafka event-streaming pipeline: EVENT_* for
// dead-letter triage and replay, SUBSCRIBER_* for the registry and credential
// provisioning, and GEN_GONE for a deprecated webhook route past its sunset.

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
	// The second code in this family deliberately mapped to 500, and the ONE reason it is
	// inventoried here despite sharing the unknown-code default: an unkeyable event is a
	// producer defect inside this service rather than anything a caller did, so 500 is the
	// judgement and not the absence of one.
	// TestStatusForCode_EventCodesAreMappedNotDefaulted is what tells the two apart,
	// because it does a two-value lookup instead of comparing StatusForCode against 500.
	{ErrEventKeyUnresolvable, http.StatusInternalServerError, "EVENT_KEY_UNRESOLVABLE"},
	// The identifier says Kafka but the string carries the EVENT_ family prefix, and that
	// asymmetry is deliberate. So is the 503: an unreachable broker is a retryable
	// upstream condition, not a defect here, so it must not resolve to 500.
	{ErrKafkaUnavailable, http.StatusServiceUnavailable, "EVENT_KAFKA_UNAVAILABLE"},
	// NO TIMEOUT CODE FOLLOWS IT, and that is the contract rather than a gap in this
	// table. A replay abandoned by a cancelled caller or a spent deadline answers
	// EVENT_REPLAY_FAILED and says so in its message; an EVENT_REPLAY_TIMEOUT mapped to
	// 504 was added here once and withdrawn, because widening this family's public surface
	// is a contract change and not an implementation detail.
	{ErrSubscriberNotFound, http.StatusNotFound, "SUBSCRIBER_NOT_FOUND"},
	{ErrSubscriberProvisioningFailed, http.StatusServiceUnavailable, "SUBSCRIBER_PROVISIONING_FAILED"},
	// A dependency of issuance being unconfigured is not a malformed request, so 503
	// and never 400: only an operator can supply the externally advertised list.
	{ErrSubscriberBrokersNotConfigured, http.StatusServiceUnavailable, "SUBSCRIBER_BROKERS_NOT_CONFIGURED"},
	// The THREE STATE refusals. All 409, because the request is well formed and it is the
	// registry row — or the deployment's configuration — that has to change before the
	// identical request can succeed.
	//
	// SUBSCRIBER_KEY_SCOPE_UNENFORCED is the fail-closed one, and it is the ONLY code for
	// its judgement in either order: issuing for a row that already records a
	// partition-key prefix, and recording a prefix on a row that already holds a
	// credential. A second code for one judgement (SUBSCRIBER_ISOLATION_UNENFORCEABLE)
	// existed and is gone, because two codes for one refusal is how a client comes to
	// handle one and not the other.
	{ErrSubscriberKeyScopeUnenforced, http.StatusConflict, "SUBSCRIBER_KEY_SCOPE_UNENFORCED"},
	// Its MIRROR, and a separate code because the remedy is the opposite edit. UNENFORCED
	// means "this row records a key scope and nothing keeps it"; REQUIRED means "this
	// deployment declared a key-scoped model and this row records no scope", which is the
	// one credential that would escape the model with whole-topic Read. One code for both
	// would tell an operator to change a prefix without saying in which direction.
	{ErrSubscriberKeyScopeRequired, http.StatusConflict, "SUBSCRIBER_KEY_SCOPE_REQUIRED"},
	// The declaration refusal: a secure-mode deployment that has said nothing about
	// whether its subscribers read whole topics. 409 because the CONFIGURATION is the
	// state that changes, and not 403, which would blame the caller holding the master
	// key.
	{ErrSubscriberSharedTopicAccessUnacknowledged, http.StatusConflict, "SUBSCRIBER_SHARED_TOPIC_ACCESS_UNACKNOWLEDGED"},
	// The verification refusal. 409 even when the attestation call timed out, because the
	// declared enforcement point is the deployment's state rather than a Blnk dependency:
	// a 503 would send an operator to a Kafka that never stopped answering. The detail's
	// retryable flag is what separates "unreachable, try again" from "it attested a
	// different prefix".
	{ErrSubscriberKeyScopeUnattested, http.StatusConflict, "SUBSCRIBER_KEY_SCOPE_UNATTESTED"},
	{ErrSubscriberDeprovisioning, http.StatusConflict, "SUBSCRIBER_DEPROVISIONING"},
	{ErrSubscriberGrantEmpty, http.StatusConflict, "SUBSCRIBER_GRANT_EMPTY"},
	// A fourth state refusal, and the one whose state lives at the BROKER rather than in
	// the registry row. 409 for the same reason and never the 503 of the *_FAILED codes:
	// the broker answered, so there is no upstream condition for a retry to outlast.
	{ErrSubscriberAccessExceedsAuthorization, http.StatusConflict, "SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION"},
	// 403, and the only one here that is about the CHANNEL rather than about state: the
	// request is well formed and the caller is authorised, and the server is refusing to
	// put a one-time secret on a transport it cannot establish as confidential. A retry
	// over the same transport cannot succeed, which is why it is not a 503.
	{ErrSubscriberInsecureTransport, http.StatusForbidden, "SUBSCRIBER_INSECURE_TRANSPORT"},
	// AND NO TIMEOUT CODE HERE EITHER, for the same reason as in the EVENT_ family above:
	// a spent issuance budget or a caller that went away answers the retryable
	// SUBSCRIBER_PROVISIONING_FAILED (503) inventoried above, at every layer of the
	// issuance path, with the spent budget named in the message and the broker residue in
	// the detail. THERE ARE NO DATA-PLANE CODES IN THIS FAMILY, and their absence is the
	// access model rather than an omission.
}

// TestStatusForCode_EventStreamingCodes states the mapping positively;
// TestStatusForCode_EventCodesAreMappedNotDefaulted below is what proves it is real
// rather than the 500 default.
func TestStatusForCode_EventStreamingCodes(t *testing.T) {
	// Guard the inventory itself: a table that no longer holds one row per code in
	// codes.go means a code was added or removed without its assertions.
	//
	//   +1 — EVENT_KEY_UNRESOLVABLE added with the producer-side capture guard: an event that
	//        can be assigned no Kafka message key cannot preserve per-aggregate ordering, so it
	//        is refused at capture rather than published unordered.
	//
	//   −1 — SUBSCRIBER_ISOLATION_UNENFORCEABLE retired. It and SUBSCRIBER_KEY_SCOPE_UNENFORCED
	//        were two codes for ONE judgement — a declared key boundary nothing enforces — and
	//        two codes for one judgement is how a client comes to handle one and not the other.
	//        The surviving code answers both orders it can arise in.
	//
	//   +1 — SUBSCRIBER_KEY_SCOPE_UNENFORCED itself, which had NO ROW HERE while it was
	//        declared and mapped in codes.go. That is the omission this guard exists to catch,
	//        and it was caught by the same recount that removed the three above.
	//
	//   +3 — SUBSCRIBER_KEY_SCOPE_REQUIRED, SUBSCRIBER_SHARED_TOPIC_ACCESS_UNACKNOWLEDGED and
	//        SUBSCRIBER_KEY_SCOPE_UNATTESTED added with the enforceable key-scope model. The
	//        first two make the deployment state its subscriber access model — key-scoped, or
	//        whole-topic and acknowledged — instead of defaulting silently to the widest one;
	//        the third refuses a key-scoped credential whose declared enforcement point did not
	//        attest the exact recorded prefix over an authenticated channel. Three codes and not
	//        one, because the three remedies are three different edits: the ROW, the
	//        DEPLOYMENT'S DECLARATION, and the COMPONENT.
	//
	//   +1 — SUBSCRIBER_PROVISIONING_TIMEOUT added alongside the replay timeout, then
	//   −1 — WITHDRAWN with it and for the identical reason. Every deadline expiry in the
	//        issuance path — the broker half and the registry half alike — answers the
	//        retryable SUBSCRIBER_PROVISIONING_FAILED (503) it already had, so one code still
	//        covers the condition at every layer and no client has to know which dependency
	//        consumed the budget. Recorded rather than netted out, because a reader finding
	//        either name in git history should find the reason here.
	//
	//   = 17.
	if len(eventStreamingCodeCases) != 17 {
		t.Fatalf("eventStreamingCodeCases has %d rows, want 17 (one per event-streaming code in codes.go)", len(eventStreamingCodeCases))
	}
	for _, tt := range eventStreamingCodeCases {
		t.Run(string(tt.code), func(t *testing.T) {
			if got := StatusForCode(tt.code); got != tt.status {
				t.Errorf("StatusForCode(%s) = %d, want %d", tt.code, got, tt.status)
			}
		})
	}
}

// TestStatusForCode_EventStreamingInventoryIsComplete makes the literal count above
// FALSIFIABLE rather than a number a reader has to trust.
//
// The count guard one function up catches a row being deleted.
//
// GEN_GONE is deliberately outside the derived set.
func TestStatusForCode_EventStreamingInventoryIsComplete(t *testing.T) {
	inventoried := make(map[ErrorCode]bool, len(eventStreamingCodeCases))
	for _, tt := range eventStreamingCodeCases {
		if inventoried[tt.code] {
			t.Errorf("%s appears twice in eventStreamingCodeCases: a duplicate row satisfies the count guard while masking a missing code", tt.code)
		}

		inventoried[tt.code] = true
	}

	for code, status := range statusByCode {
		name := string(code)
		if !strings.HasPrefix(name, "EVENT_") && !strings.HasPrefix(name, "SUBSCRIBER_") {
			continue
		}

		if !inventoried[code] {
			t.Errorf(
				"%s is mapped to %d in statusByCode but has no row in eventStreamingCodeCases: "+
					"add one, and correct the literal count in TestStatusForCode_EventStreamingCodes",
				code, status,
			)
		}
	}

	// AND THE REVERSE DIRECTION, which is what keeps a retired code from lingering as an
	// assertion about a symbol nothing produces. A row for an unmapped code would resolve
	// to the 500 default and the status assertion would fail — but only if the row's
	// expected status happened to differ from 500, and EVENT_REPLAY_FAILED legitimately
	// expects 500.
	for _, tt := range eventStreamingCodeCases {
		if _, mapped := statusByCode[tt.code]; !mapped {
			t.Errorf(
				"%s has a row in eventStreamingCodeCases but no statusByCode entry: it resolves to the "+
					"unknown-code 500 default in production",
				tt.code,
			)
		}
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
			// A two-value lookup rather than a StatusForCode comparison, because StatusForCode
			// returns 500 both for a code deliberately mapped to 500 and for one not mapped at
			// all. For ErrEventReplayFailed, whose intended status is 500, only this lookup
			// tells the two apart.
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
// The mutation gate found it.
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

	// A TYPED NIL is the half that fails if the guard is removed rather than negated.
	// errors.As succeeds against a nil *APIError — the type matches — so without the guard
	// the next line would dereference nil and take down the process that was merely trying
	// to choose a status code. 500 is the right answer: an error carrying no code is
	// exactly the unclassified case.
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
