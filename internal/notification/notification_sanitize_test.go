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
package notification

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"testing"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file guards the system.error sanitizer.
//
// It is a new file rather than an addition to the two existing test files here,
// because both of those are upstream tests of the notification transport and this
// covers a different subject: what a system.error payload is permitted to contain.
// Keeping it separate leaves the upstream files untouched apart from the single
// assertion that pinned the raw-error payload the DATA-01 finding identified.

// hostileErrors are the error shapes Blnk actually produces, each carrying something
// that must never reach a published payload. They are used by several tests below, so
// they are declared once with the reason each one is dangerous.
func hostileErrors() map[string]struct {
	err     error
	secrets []string
} {
	return map[string]struct {
		err     error
		secrets []string
	}{
		// A PostgreSQL error renders with the schema, table, column, constraint, source
		// file and routine that produced it — a description of the database's internal
		// structure.
		"a postgres constraint violation": {
			err: fmt.Errorf(`pq: duplicate key value violates unique constraint ` +
				`"event_subscribers_kafka_principal_uidx" (schema blnk, table event_subscribers, ` +
				`file nbtinsert.c, routine _bt_check_unique)`),
			secrets: []string{"event_subscribers_kafka_principal_uidx", "nbtinsert.c", "_bt_check_unique"},
		},
		// A network error names internal addresses and ports: broker topology.
		"a kafka write failure": {
			err: &net.OpError{
				Op:   "write",
				Net:  "tcp",
				Addr: &net.TCPAddr{IP: net.ParseIP("10.0.0.7"), Port: 9092},
				Err:  syscall.EPIPE,
			},
			secrets: []string{"10.0.0.7", "9092"},
		},
		// A validation error can quote the offending input, which in a ledger is
		// somebody's account reference or amount.
		"a validation error quoting input": {
			err:     fmt.Errorf(`invalid reference: %q is already used`, "acct-4451-jane-doe"),
			secrets: []string{"acct-4451-jane-doe"},
		},
		// A configuration error can quote a DSN, which carries a password.
		"a configuration error quoting a DSN": {
			err: errors.New(`config error: cannot dial ` +
				`postgres://blnk:sup3r-s3cret@db.internal:5432/blnk`),
			secrets: []string{"sup3r-s3cret", "db.internal"},
		},
	}
}

// TestSanitizedSystemErrorPayload_CarriesNoErrorText is the DATA-01 guard, and it is
// the assertion that matters most in this file.
//
// system.error is published to a Kafka topic, which is replicated and retained, and it
// is also stored in blnk.event_outbox and copied to a dead-letter topic if it fails. So
// anything in the payload is durable in several places at once. The payload used to be
// {"error": systemError.Error(), "time": now}.
func TestSanitizedSystemErrorPayload_CarriesNoErrorText(t *testing.T) {
	for name, hostile := range hostileErrors() {
		t.Run(name, func(t *testing.T) {
			payload := sanitizedSystemErrorPayload(hostile.err, "a-correlation-id")

			// Checked through the JSON a subscriber would actually receive, not only
			// field by field: a value nested inside a map or a struct is invisible to a
			// field-by-field check and fully visible once marshalled.
			marshalled, err := json.Marshal(payload)
			require.NoError(t, err)
			rendered := string(marshalled)

			for _, secret := range hostile.secrets {
				assert.NotContains(t, rendered, secret,
					"the payload must not carry %q; the full error belongs in the log", secret)
			}

			assert.NotContains(t, payload, "error",
				"the payload must not carry an error key at all")
			assert.NotContains(t, rendered, hostile.err.Error(),
				"the payload must not carry the rendered error")
		})
	}
}

// TestSanitizedSystemErrorPayload_CarriesADiagnosisAndAHandle is the other half:
// sanitizing must not leave the notification useless.
func TestSanitizedSystemErrorPayload_CarriesADiagnosisAndAHandle(t *testing.T) {
	payload := sanitizedSystemErrorPayload(
		errors.New("dial tcp: connection refused"), "0f6e2c8a-1b4d-4e9f-8a7c-2d5b6e3f1a9c")

	assert.Equal(t, SystemErrorReasonTransport, payload["reason"],
		"the recipient must learn WHERE the fault is, which is the only thing it can act on")
	assert.Equal(t, "0f6e2c8a-1b4d-4e9f-8a7c-2d5b6e3f1a9c", payload["correlation_id"],
		"the correlation ID is the operator's route to the full error")
	assert.Contains(t, payload, "time")
}

// TestSanitizedSystemErrorPayload_PublishesATypedCodeWhenThereIsOne checks that a
// typed apierror contributes its code.
//
// The code is safe and worth publishing: it is a fixed vocabulary from
// internal/apierror and it is the same value the HTTP API already returns for the same
// condition, so it tells a recipient nothing it could not learn by making a request.
func TestSanitizedSystemErrorPayload_PublishesATypedCodeWhenThereIsOne(t *testing.T) {
	typed := apierror.NewAPIError(apierror.ErrKafkaUnavailable, "Kafka is unavailable", nil)

	payload := sanitizedSystemErrorPayload(typed, "a-correlation-id")

	assert.Equal(t, string(apierror.ErrKafkaUnavailable), payload["error_code"])
	assert.Equal(t, SystemErrorReasonTransport, payload["reason"],
		"a 503 code classifies as a transport failure")
}

// TestSanitizedSystemErrorPayload_OmitsTheCodeWhenThereIsNone — omitted rather than
// blank, so a recipient can tell "no code" from "the code is empty".
func TestSanitizedSystemErrorPayload_OmitsTheCodeWhenThereIsNone(t *testing.T) {
	payload := sanitizedSystemErrorPayload(errors.New("something went wrong"), "a-correlation-id")

	assert.NotContains(t, payload, "error_code")
}

// TestClassifySystemError_AlwaysReturnsVocabulary is the property that makes the
// classifier a sanitizer rather than a formatter: no error, however constructed, can
// produce output that describes the deployment.
func TestClassifySystemError_AlwaysReturnsVocabulary(t *testing.T) {
	vocabulary := map[string]struct{}{
		SystemErrorReasonPersistence:   {},
		SystemErrorReasonTransport:     {},
		SystemErrorReasonAuthorization: {},
		SystemErrorReasonTimeout:       {},
		SystemErrorReasonValidation:    {},
		SystemErrorReasonConfiguration: {},
		SystemErrorReasonUnclassified:  {},
	}

	candidates := []error{
		nil,
		errors.New(""),
		errors.New("\x00\x1b[2J"),
		errors.New(strings.Repeat("A", 8192)),
		errors.New("an entirely novel failure from /usr/src/blnk/secret.go:42"),
		fmt.Errorf("wrapped: %w", errors.New("pq: deadlock detected")),
		apierror.NewAPIError(apierror.ErrInternalServer, "boom", nil),
	}
	for _, hostile := range hostileErrors() {
		candidates = append(candidates, hostile.err)
	}

	for _, candidate := range candidates {
		reason := classifySystemError(candidate)
		_, known := vocabulary[reason]
		require.True(t, known, "classifying %v produced %q, outside the vocabulary", candidate, reason)

		if candidate != nil && candidate.Error() != "" {
			// The decisive property: the output is never a piece of the input.
			assert.NotContains(t, candidate.Error(), reason,
				"the reason %q must not be a substring of the error it came from, or it "+
					"would be echoing rather than classifying", reason)
		}
	}
}

// TestClassifySystemError_DistinguishesTheActionableCases checks the classification is
// USEFUL, not merely safe — each value implies a different first question.
func TestClassifySystemError_DistinguishesTheActionableCases(t *testing.T) {
	cases := map[string]string{
		"pq: deadlock detected":                               SystemErrorReasonPersistence,
		`duplicate key value violates unique constraint`:      SystemErrorReasonPersistence,
		"dial tcp 10.1.2.3:9092: connect: connection refused": SystemErrorReasonTransport,
		"write tcp: broken pipe":                              SystemErrorReasonTransport,
		"kafka: TOPIC_AUTHORIZATION_FAILED":                   SystemErrorReasonAuthorization,
		"SASL authentication failed":                          SystemErrorReasonAuthorization,
		"context deadline exceeded":                           SystemErrorReasonTimeout,
		"i/o timeout":                                         SystemErrorReasonTimeout,
		"invalid currency code":                               SystemErrorReasonValidation,
		"config error: brokers not configured":                SystemErrorReasonConfiguration,
		"something nobody has seen before":                    SystemErrorReasonUnclassified,
	}

	for text, want := range cases {
		assert.Equal(t, want, classifySystemError(errors.New(text)), "misclassified %q", text)
	}

	t.Run("authorization outranks transport", func(t *testing.T) {
		// A broker can report both in one message, and the authorization failure is the
		// actionable half: it will not clear on its own.
		assert.Equal(t, SystemErrorReasonAuthorization,
			classifySystemError(errors.New("connection refused after authorization failed")))
	})

	t.Run("timeout outranks transport", func(t *testing.T) {
		// "i/o timeout" is a timeout that happens to be on a connection; calling it a
		// transport failure would send an operator to look at the wrong thing.
		assert.Equal(t, SystemErrorReasonTimeout,
			classifySystemError(errors.New("read tcp: i/o timeout")))
	})

	t.Run("a typed code outranks the text", func(t *testing.T) {
		// The code is an explicit classification the raising site already made, and is
		// strictly better than inferring one from prose. Here the text alone would say
		// "validation" (it contains "invalid"), while the 401 code says authorization.
		typed := apierror.NewAPIError(apierror.ErrAuthInvalidBearerToken, "Invalid bearer token", nil)
		assert.Equal(t, SystemErrorReasonAuthorization, classifySystemError(typed))
	})
}

// TestReasonForErrorCode_ReadsTheStatusRatherThanEnumerating documents why the code
// classifier switches on the HTTP status: a code added to internal/apierror later is
// classified without an edit here. Enumerating codes would mean this function silently
// returning "" for every new one — a vocabulary quietly degrading into unclassified.
func TestReasonForErrorCode_ReadsTheStatusRatherThanEnumerating(t *testing.T) {
	assert.Equal(t, SystemErrorReasonAuthorization, reasonForErrorCode(apierror.ErrAuthMissingAPIKey))
	assert.Equal(t, SystemErrorReasonAuthorization, reasonForErrorCode(apierror.ErrAuthInsufficientPermissions))
	assert.Equal(t, SystemErrorReasonValidation, reasonForErrorCode(apierror.ErrInvalidInput))
	assert.Equal(t, SystemErrorReasonValidation, reasonForErrorCode(apierror.ErrConflict))
	assert.Equal(t, SystemErrorReasonTransport, reasonForErrorCode(apierror.ErrKafkaUnavailable))

	// A 500 implies nothing more specific than the text matching would find, so it
	// defers rather than claiming a category.
	assert.Empty(t, reasonForErrorCode(apierror.ErrInternalServer))

	// An unknown code defaults to 500 in StatusForCode, so it defers too rather than
	// being miscategorised.
	assert.Empty(t, reasonForErrorCode(apierror.ErrorCode("A_CODE_THAT_DOES_NOT_EXIST")))
}

// TestNewCorrelationID_IsUniquePerCall — a repeated ID would make two unrelated events
// point at each other's log lines, which is worse than having none.
func TestNewCorrelationID_IsUniquePerCall(t *testing.T) {
	seen := make(map[string]struct{}, 64)
	for i := 0; i < 64; i++ {
		id := newCorrelationID()
		require.NotEmpty(t, id)
		_, repeated := seen[id]
		require.False(t, repeated, "correlation ID %q was issued twice", id)
		seen[id] = struct{}{}
	}
}

// TestSystemErrorEventType_MatchesTheCatalogue pins the event name.
//
// It is one of the thirteen event strings the catalogue routes on, and a typo would
// route the event to the quarantine category instead of the system category with
// nothing failing anywhere.
func TestSystemErrorEventType_MatchesTheCatalogue(t *testing.T) {
	assert.Equal(t, "system.error", systemErrorEventType)
}
