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

// This file guards what NotifyError WRITES TO THE LOG, which is a different subject
// from the two adjacent files: notification_sanitize_test.go covers what a published
// payload may contain, and notification_dispatch_test.go covers whether a notification
// is attempted at all. What the log holds sits between them, and it had its own defect.

// notifyErrorLogCapture is what one NotifyError call wrote to the log.
type notifyErrorLogCapture struct {
	entries []map[string]interface{}
	raw     string
}

// syncLogBuffer is the buffer captureNotifyErrorLog redirects the standard logger into.
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
// outside.
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

	// logrus serialises the WRITES behind the logger's mutex, but the arrival poll below
	// reads the buffer while NotifyError's goroutine is still writing into it, so the
	// buffer has to guard the read side itself. See syncLogBuffer for why a bytes.Buffer
	// here is a data race rather than a settled-by-then read.
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

	// ONE snapshot, decoded from the same bytes the failure message quotes. Reading the
	// sink twice could return two different renderings if a straggler arrived between
	// them, and the assertions and the diagnostic would then be describing different logs.
	written := buf.Bytes()

	captured := notifyErrorLogCapture{raw: string(written)}
	decoder := json.NewDecoder(bytes.NewReader(written))
	for decoder.More() {
		entry := map[string]interface{}{}
		require.NoError(t, decoder.Decode(&entry), "the captured log must be decodable JSON")
		captured.entries = append(captured.entries, entry)
	}

	return captured
}

// TestNotifyError_EmitsExactlyOneCorrelatedRecordForOneError is the duplication fix.
//
// Two records carried the same raw error.
//
// One record, at error level, carrying the correlation id, the classified reason and
// the bounded full text — and a PAYLOAD THAT IS STILL THE FROZEN LEGACY BODY.
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

// TestNotifyError_BoundsTheSenderFailureDetail covers the second half of the bounding rule.
//
// A failed publish is a DISTINCT event from the system error itself — the error
// happened, and separately the attempt to report it did not — so it gets its own
// record.
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

	assert.Equal(t, "csi31m", boundedErrorText(errors.New("csi\u009b31m")),
		"a C1 control is dropped as well: U+009B is a CSI introducer on its own, so a test "+
			"that stopped at DEL left an escape introducer in the field")

	// THE SECOND CLASS. A direction override does not write log structure, it reorders the
	// rendering of everything after it — so a dependency's error text carrying one rewrites
	// how the rest of the entry reads — and the zero-width characters hide differences
	// between two entries entirely.
	assert.Equal(t, "brokerdesufer", boundedErrorText(errors.New("broker\u202edesufer")),
		"a right-to-left override is dropped so it cannot reorder the entry it appears in")
	assert.Equal(t, "broker down", boundedErrorText(errors.New("bro\u200bker\u00ad down\ufeff")),
		"invisible format characters are dropped so two different messages cannot look alike")

	// KEPT: both carry orthography rather than deception, so dropping them would corrupt a
	// legitimate message instead of sanitising a hostile one.
	assert.Equal(t, "کتاب\u200cها", boundedErrorText(errors.New("کتاب\u200cها")),
		"a zero-width non-joiner spells the word and must survive")
	assert.Equal(t, "\U0001F468\u200D\U0001F4BB", boundedErrorText(errors.New("\U0001F468\u200D\U0001F4BB")),
		"a zero-width joiner binds an emoji sequence and must survive")

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
// It is spelled out rather than imported so the assertion above states exactly what it
// means: no RuneError produced from a one-byte sequence, which is what a byte-wise
// truncation of a multi-byte character produces.
func utf8ValidString(s string) bool {
	for _, character := range s {
		if character == '\uFFFD' {
			return false
		}
	}

	return true
}
