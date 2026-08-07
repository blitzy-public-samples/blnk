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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file guards what NotifyError WRITES TO THE LOG, which is a different subject from
// the two adjacent files: notification_sanitize_test.go covers what a published payload may
// contain, and notification_dispatch_test.go covers whether a notification is attempted at
// all. What the log holds sits between them, and it had its own defect.
//
// The log is where the error's full text legitimately lives — a system error nobody can read
// is an undiagnosable outage, not a security improvement — so these tests are not about
// removing it. They are about there being ONE record of it, correlated to the event
// subscribers receive, with its rendering bounded and stripped of control characters.

// notifyErrorLogCapture is what one NotifyError call wrote to the log.
type notifyErrorLogCapture struct {
	entries []map[string]interface{}
	raw     string
}

// syncLogBuffer is the buffer captureNotifyErrorLog redirects the standard logger into.
//
// IT IS MUTEX-GUARDED BECAUSE NOTIFYERROR LOGS FROM A GOROUTINE IT SPAWNS. logrus serialises
// its own writes behind the logger's mutex, so a plain bytes.Buffer is safe against two
// concurrent log calls — but nothing in logrus guards a READER, and captureNotifyErrorLog has
// two: the arrival poll, which reads the buffer every 5ms while that goroutine is still
// writing, and the final rendering, which reads it once the settling window closes.
//
// An unguarded read alongside a logrus write is a data race on the buffer's length and on its
// backing array, reported by `go test -race` — which .github/workflows/go.yml runs across the
// whole module on every push, so the package fails to pass CI rather than merely logging a
// warning. It is not reporting-only either: Buffer.grow reallocates mid-Write, so a String
// taken at the wrong moment can observe a half-copied record and fail the JSON decode, which
// would read as a defect in NotifyError's rendering rather than in this helper.
//
// Every method takes the same mutex, which is what orders the reads against the writes
// instead of merely making a collision rare.
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p under the lock. This is the io.Writer logrus is handed.
//
// Parameters:
//   - p []byte: the rendered record.
//
// Returns:
//   - int: the number of bytes written.
//   - error: always nil — bytes.Buffer writes do not fail.
func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

// String renders everything written so far.
//
// Returns:
//   - string: the accumulated rendering.
func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// Bytes returns a COPY of everything written so far.
//
// The copy is the point. bytes.Buffer.Bytes aliases the live backing array, so decoding
// straight out of it would read that array after the lock is dropped, while the goroutine may
// still be appending — the same race, moved one call away from where it is visible.
//
// Returns:
//   - []byte: an independent snapshot, safe to decode without holding the lock.
func (b *syncLogBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]byte(nil), b.buf.Bytes()...)
}

// count reports how many captured entries contain needle in their message.
func (c notifyErrorLogCapture) count(needle string) int {
	matches := 0
	for _, entry := range c.entries {
		message, _ := entry["msg"].(string)
		if strings.Contains(message, needle) {
			matches++
		}
	}

	return matches
}

// errorEntries returns every entry logged at the error level.
func (c notifyErrorLogCapture) errorEntries() []map[string]interface{} {
	matched := []map[string]interface{}{}
	for _, entry := range c.entries {
		if entry["level"] == "error" {
			matched = append(matched, entry)
		}
	}

	return matched
}

// captureNotifyErrorLog runs fn with the standard logger redirected, waits for the
// asynchronous notification goroutine to finish writing, and returns what it wrote.
//
// NotifyError does its work in a goroutine, so there is nothing to synchronise on from
// outside. The wait is on the OUTPUT rather than on a fixed sleep: it polls until at least
// one entry has been decoded and then allows a short settling window for any second entry,
// so the "exactly one record" assertions cannot pass merely because the test looked too
// early. A test asserting an absence therefore still waits for the presence of the record
// that is expected.
//
// Parameters:
//   - t *testing.T: for cleanup registration and decode failures.
//   - expected int: how many entries to wait for before settling.
//   - fn func(): the call under test.
//
// Returns:
//   - notifyErrorLogCapture: the decoded entries and the raw rendering.
func captureNotifyErrorLog(t *testing.T, expected int, fn func()) notifyErrorLogCapture {
	t.Helper()

	logger := logrus.StandardLogger()
	previousOut := logger.Out
	previousFormatter := logger.Formatter
	previousLevel := logger.GetLevel()
	t.Cleanup(func() {
		logger.SetOutput(previousOut)
		logger.SetFormatter(previousFormatter)
		logger.SetLevel(previousLevel)
	})

	// logrus serialises the WRITES behind the logger's mutex, but the arrival poll below reads
	// the buffer while NotifyError's goroutine is still writing into it, so the buffer has to
	// guard the read side itself. See syncLogBuffer for why a bytes.Buffer here is a data race
	// rather than a settled-by-then read.
	buf := &syncLogBuffer{}
	logger.SetOutput(buf)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.DebugLevel)

	fn()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Count(buf.String(), "\n") >= expected {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The settling window is what makes a "no second record" assertion meaningful: a
	// duplicate would be written immediately after the first, so allowing time for it and
	// then finding none is evidence rather than a race.
	time.Sleep(150 * time.Millisecond)

	captured := notifyErrorLogCapture{raw: buf.String()}
	decoder := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	for decoder.More() {
		entry := map[string]interface{}{}
		require.NoError(t, decoder.Decode(&entry), "the captured log must be decodable JSON")
		captured.entries = append(captured.entries, entry)
	}

	return captured
}

// TestNotifyError_EmitsExactlyOneCorrelatedRecordForOneError is the duplication fix.
//
// # What was wrong
//
// Two records carried the same raw error. A bare logrus.Error(systemError) ran first, and then
// the transport branch emitted a second record whose message asserted "the full error is on
// this line only" — which the first record had already made untrue.
//
// The cost was threefold: the error's text, which describes the inside of the deployment, was
// written into log retention twice for no diagnostic gain; the first record carried no
// correlation id, because the id was minted inside the transport branch, so it could not be
// tied to the system.error event a subscriber received; and it was rendered by
// logrus.Error(err) with no length bound and no control-character handling at all.
//
// # What is asserted
//
// One record, at error level, carrying the correlation id, the classified reason and the
// bounded full text — and a PAYLOAD THAT IS STILL THE FROZEN LEGACY BODY. Both halves matter
// together: the narrowing this fix performs is worth nothing if it changes what a subscriber
// receives, because the payload's byte-for-byte equivalence is what makes the dual-delivery
// window verifiable, and it would change silently since both transports read the same bytes.
func TestNotifyError_EmitsExactlyOneCorrelatedRecordForOneError(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeNotificationConfig(t, "", "http://example.invalid/webhook-target")

	const errorText = "queue worker crashed while sealing txn_0f6e2c8a"

	payloads := make(chan map[string]interface{}, 4)
	RegisterWebhookSender(func(_ string, payload interface{}) error {
		if typed, ok := payload.(map[string]interface{}); ok {
			payloads <- typed
		}

		return nil
	})

	captured := captureNotifyErrorLog(t, 1, func() {
		NotifyError(errors.New(errorText))
	})

	// EXACTLY ONE record mentions the error, and exactly one error-level record exists at
	// all. Both are asserted: the first catches a duplicate of this message, the second
	// catches a duplicate emitted under any other wording.
	errorRecords := captured.errorEntries()
	require.Len(t, errorRecords, 1,
		"one system error must produce ONE error-level record; the raw error used to be logged twice")
	assert.Equal(t, 1, strings.Count(captured.raw, errorText),
		"the error's text must appear once in the log, not once per record that carries it")

	record := errorRecords[0]
	assert.Contains(t, record["msg"], "this is the only record carrying its text",
		"the surviving record must be the correlated one, not the bare logrus.Error")
	assert.Equal(t, errorText, record["error"],
		"the full text must still be present: an unreadable system error is an undiagnosable outage")
	assert.Equal(t, "system.error", record["event"])
	assert.Equal(t, SystemErrorReasonUnclassified, record["reason"],
		"an untyped error with no recognised signature is unclassified, and the field must say so")

	logCorrelation, ok := record["correlation_id"].(string)
	require.True(t, ok, "the record must carry a correlation id")
	require.NotEmpty(t, logCorrelation)

	// THE PAYLOAD IS UNCHANGED BY THIS FIX, and that is the point of asserting it here.
	//
	// The correlation id and the classified, bounded diagnosis live on the OPERATOR RECORD.
	// They are deliberately NOT added to the payload, and the error text is deliberately not
	// removed from it: the payload is the frozen legacy body {"error", "time"}, so a
	// subscriber's existing parser keeps working when only the transport changes (AAP R-8 and
	// AMBIGUITY-3). Adding a key or substituting the error value would break the very
	// equivalence the dual-delivery window exists to guarantee — and it would break it
	// invisibly, since both transports read the same bytes.
	//
	// So the correlation is one-directional by design: an operator goes from a subscriber's
	// event to the log by time and error text, and the log line is where the full story is.
	select {
	case payload := <-payloads:
		assert.Equal(t, errorText, payload["error"],
			"the payload keeps the legacy error key verbatim: it IS the pre-migration body")
		assert.Contains(t, payload, "time",
			"and its second key, so the object has exactly the two the legacy transport sent")
		assert.Len(t, payload, 2,
			"exactly two keys. A correlation id belongs on the operator record, not in a body a "+
				"subscriber's parser has already been written against")
		assert.NotContains(t, payload, "correlation_id",
			"specifically not this one, however useful it would be: the payload is frozen")
	case <-time.After(2 * time.Second):
		t.Fatal("the system.error event was never published")
	}
}

// TestNotifyError_LogsTheErrorEvenWithNoTransportConfigured pins the property the
// reorganisation could most easily have broken.
//
// The single record is emitted BEFORE the configuration is read and before either transport is
// consulted, precisely so that a deployment with no Slack webhook and no event transport still
// records its system errors. Folding the record into the transport branch — which is where the
// correlation id used to be minted — would have made an unconfigured deployment silently
// discard every system error, which is a far worse outcome than the duplication being fixed.
func TestNotifyError_LogsTheErrorEvenWithNoTransportConfigured(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	// Neither Slack nor a webhook URL, and no sender registered.
	storeNotificationConfig(t, "", "")
	RegisterWebhookSender(nil)

	const errorText = "no transport is configured but this still happened"

	captured := captureNotifyErrorLog(t, 1, func() {
		NotifyError(errors.New(errorText))
	})

	errorRecords := captured.errorEntries()
	require.Len(t, errorRecords, 1,
		"a system error must be recorded whether or not anything can be notified about it")
	assert.Equal(t, errorText, errorRecords[0]["error"])
	assert.NotEmpty(t, errorRecords[0]["correlation_id"],
		"the correlation id is minted unconditionally, so it is present even when nothing is published")
}

// TestNotifyError_BoundsTheSenderFailureDetail covers the second half of the finding.
//
// A failed publish is a DISTINCT event from the system error itself — the error happened, and
// separately the attempt to report it did not — so it gets its own record. What it must not do
// is interpolate the sender's error into the message text with %v, which is what it used to do:
// that error comes from the event pipeline or an HTTP client, so it can carry broker addresses,
// a topic name or a response body, at any length and with any control characters in it, and a
// newline in a line-oriented aggregator forges a log entry.
func TestNotifyError_BoundsTheSenderFailureDetail(t *testing.T) {
	originalSender := webhookSender
	defer RegisterWebhookSender(originalSender)

	storeNotificationConfig(t, "", "http://example.invalid/webhook-target")

	// A hostile sender failure: multi-line, control characters, and far longer than the bound.
	hostile := fmt.Errorf(
		"write tcp 10.0.0.4:34918->10.0.0.7:9092: broken pipe\nFORGED level=info msg=\"all clear\"\t%s",
		strings.Repeat("q", maxLoggedErrorLength*2),
	)

	RegisterWebhookSender(func(_ string, _ interface{}) error {
		return hostile
	})

	captured := captureNotifyErrorLog(t, 2, func() {
		NotifyError(errors.New("the underlying system error"))
	})

	require.Len(t, captured.errorEntries(), 2,
		"the system error and the failure to report it are two distinct facts and get two records")
	require.Equal(t, 1, captured.count("could not be published"))

	var failure map[string]interface{}
	for _, entry := range captured.errorEntries() {
		if message, _ := entry["msg"].(string); strings.Contains(message, "could not be published") {
			failure = entry
		}
	}
	require.NotNil(t, failure)

	detail, ok := failure["sender_error"].(string)
	require.True(t, ok, "the sender's error must be carried as a bounded FIELD, not spliced into the message")

	assert.NotContains(t, detail, "\n", "a newline would forge a log entry in a line-oriented aggregator")
	assert.NotContains(t, detail, "\t")
	assert.Contains(t, detail, "broken pipe", "the diagnosis itself must survive the bounding")
	assert.Contains(t, detail, logTruncationSuffix, "an unbounded error must be truncated, and say so")
	assert.LessOrEqual(t, len([]rune(detail)), maxLoggedErrorLength+len([]rune(logTruncationSuffix)))

	// Correlated to the system error it failed to report, so the two records read as one story.
	assert.Equal(t, failure["correlation_id"], captured.errorEntries()[0]["correlation_id"],
		"both records must carry the same correlation id")

	// And the forged entry must not have become a real one: it is inside a field value, so the
	// JSON formatter escapes it and the decoded entry count stays at two.
	assert.Len(t, captured.entries, 2,
		"a control character inside a field cannot manufacture a log entry")
}

// TestBoundedErrorText_NeutralisesAndBounds covers the renderer directly, including the cases
// the NotifyError tests above cannot reach conveniently.
func TestBoundedErrorText_NeutralisesAndBounds(t *testing.T) {
	assert.Empty(t, boundedErrorText(nil),
		"a nil error reads as an absent field rather than as the string \"<nil>\"")

	assert.Equal(t, "one two three",
		boundedErrorText(errors.New("one\ntwo\tthree")),
		"line and tab breaks become spaces, so words do not run together")

	assert.Equal(t, "clean", boundedErrorText(errors.New("cl\x00e\x07an")),
		"other control characters are dropped; an escape sequence can rewrite what an operator sees")

	assert.Equal(t, "keep", boundedErrorText(errors.New("keep\x7f")),
		"delete is a control character too")

	t.Run("truncation happens on a rune boundary", func(t *testing.T) {
		// Multi-byte runes, so a byte-wise cut would produce invalid UTF-8 and corrupt the
		// log line rather than shorten it.
		multiByte := strings.Repeat("é", maxLoggedErrorLength+50)
		rendered := boundedErrorText(errors.New(multiByte))

		assert.True(t, strings.HasSuffix(rendered, logTruncationSuffix))
		assert.True(t, utf8ValidString(rendered), "the truncated rendering must still be valid UTF-8")
		assert.Equal(t, maxLoggedErrorLength+len([]rune(logTruncationSuffix)), len([]rune(rendered)),
			"the bound is counted in runes, not bytes")
	})

	t.Run("a short error is returned unchanged and unmarked", func(t *testing.T) {
		rendered := boundedErrorText(errors.New("short and clean"))
		assert.Equal(t, "short and clean", rendered)
		assert.NotContains(t, rendered, logTruncationSuffix,
			"marking an untruncated error would make a reader distrust a complete message")
	})
}

// utf8ValidString reports whether every rune in s decoded successfully.
//
// It is spelled out rather than imported so the assertion above states exactly what it means:
// no RuneError produced from a one-byte sequence, which is what a byte-wise truncation of a
// multi-byte character produces.
func utf8ValidString(s string) bool {
	for _, character := range s {
		if character == '\uFFFD' {
			return false
		}
	}

	return true
}
