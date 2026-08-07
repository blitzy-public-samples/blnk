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
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file guards the system.error PAYLOAD CONTRACT and the bounded classification that is
// logged beside it.
//
// It is a separate file from the two upstream notification tests because those cover the
// transport — Slack delivery, sender registration, the dispatch gate — and this covers a
// different subject: what a system.error payload must contain, and what it must not.
//
// The subject changed once already, which is why the reasoning is written down here. An
// earlier revision reshaped the payload into a classified reason, a correlation id and an
// optional error code, to keep Blnk's internal error text off a durable, retained topic. The
// concern is legitimate; the remedy was not, because requirement R-8 freezes this payload to
// the webhook body it has always been and substituting one shape for another under the same
// event name breaks every subscriber parser with nothing announcing it. The payload is back
// to {"error", "time"}; the classification survives as the LOG line's diagnosis, where it
// costs no contract anything.

// hostileErrors are the error shapes Blnk actually produces, each carrying deployment
// detail. They are used by several tests below, so they are declared once with a note
// on what each one reveals.
//
// They are NOT a list of values the payload must suppress. The payload carries the
// error text verbatim because that is the contract subscribers already parse, and
// because the legacy webhook delivered exactly this text to exactly this audience;
// narrowing it is a versioned schema change, not a transport detail. What these shapes
// prove here is that the CLASSIFIER never echoes them — the reason it returns is drawn
// from a fixed vocabulary whatever it is handed, so the log line — and any future
// versioned schema that chooses to carry a summary instead of the error — has a safe
// value to use.
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

// TestSystemErrorPayload_IsTheFrozenLegacyContract is the R-8 guard on the one payload in
// the catalogue that a well-intentioned change had already altered.
//
// Requirement R-8 requires a LedgerEvent's payload to match today's webhook body
// FIELD-FOR-FIELD, and the webhook body for system.error has always been {"error", "time"}.
// Every subscriber's parser reads those two keys. A revision of this function replaced them
// with a classified reason, a correlation id and an optional code — a strictly better payload
// to design from scratch, and a BREAKING CHANGE to a published contract when substituted
// under the same event name as a side effect of a transport migration.
//
// So this test asserts the legacy shape exactly, including the key COUNT: a third key is a
// contract change and must fail here rather than reach a subscriber.
func TestSystemErrorPayload_IsTheFrozenLegacyContract(t *testing.T) {
	systemError := errors.New("queue worker crashed")

	payload := systemErrorPayload(systemError)

	require.Len(t, payload, 2,
		"the system.error payload is a published contract: exactly error and time")
	assert.Equal(t, "queue worker crashed", payload["error"],
		"the error text is the value subscribers parse; it must be carried verbatim")

	ts, ok := payload["time"].(time.Time)
	require.True(t, ok, "time must remain a time.Time, as the legacy payload carried it")
	assert.WithinDuration(t, time.Now(), ts, 10*time.Second)

	assert.NotContains(t, payload, "reason",
		"a diagnosis belongs in the log, not in a frozen payload")
	assert.NotContains(t, payload, "correlation_id")
	assert.NotContains(t, payload, "error_code")
}

// TestSystemErrorPayload_CarriesEveryErrorTextVerbatim covers the payload against the same
// hostile errors the sanitizing revision was written for.
//
// It asserts the OPPOSITE of what those tests asserted, and deliberately: the error text IS
// the contract, so it reaches the payload intact however awkward its content. The concern the
// hostile fixtures encode is real and is addressed by the ACL model — system.error routes to
// the internal blnk.system category, which model.SubscriberGrantableTopics excludes, so no
// subscriber can be granted it at all — and by keeping the raw text off the structured log
// lines. It is NOT addressed by silently reshaping a published payload.
func TestSystemErrorPayload_CarriesEveryErrorTextVerbatim(t *testing.T) {
	for name, hostile := range hostileErrors() {
		t.Run(name, func(t *testing.T) {
			payload := systemErrorPayload(hostile.err)

			assert.Equal(t, hostile.err.Error(), payload["error"],
				"the payload must carry the rendered error exactly, as the webhook body did")

			// Round-tripped through the JSON a subscriber would actually receive, so the
			// assertion is about what arrives on the wire rather than about a Go map.
			// DECODED rather than string-matched, because the encoder escapes quotes and
			// backslashes — several of these fixtures quote their input — and a raw
			// substring check would fail on the escaping rather than on the content.
			marshalled, err := json.Marshal(payload)
			require.NoError(t, err)

			var decoded map[string]interface{}
			require.NoError(t, json.Unmarshal(marshalled, &decoded))
			assert.Equal(t, hostile.err.Error(), decoded["error"],
				"the error text must survive the round trip a subscriber performs")
		})
	}
}

// TestClassifySystemError_RemainsTheLoggedDiagnosis pins what the classification is FOR now
// that it is not in the payload.
//
// It is the bounded value logged at the dispatch site beside the correlation id, which is how
// an operator gets a diagnosis without the raw error text being duplicated into a second
// structured record. Both vocabularies stay fixed, and the tests below still hold them to
// that, because a classifier that could echo its input would put deployment detail into the
// log fields it was introduced to keep clean.
func TestClassifySystemError_RemainsTheLoggedDiagnosis(t *testing.T) {
	assert.Equal(t, SystemErrorReasonTransport,
		classifySystemError(errors.New("dial tcp: connection refused")),
		"the logged diagnosis must still answer WHERE the fault is")

	typed := apierror.NewAPIError(apierror.ErrKafkaUnavailable, "Kafka is unavailable", nil)
	assert.Equal(t, string(apierror.ErrKafkaUnavailable), systemErrorCode(typed),
		"a typed error still contributes its code to the log line")
	assert.Empty(t, systemErrorCode(errors.New("untyped")),
		"an untyped error contributes no code, so the field reads as absent rather than blank")
}

// TestClassifySystemError_AlwaysReturnsVocabulary is the property that makes the
// classifier a bounded projection rather than a formatter: no error, however constructed, can
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
// route the event to the catch-all instead of the system category deliberately, with
// nothing failing anywhere.
func TestSystemErrorEventType_MatchesTheCatalogue(t *testing.T) {
	assert.Equal(t, "system.error", systemErrorEventType)
}
