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

package logsafe

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValue_StripsForgeryCharactersAndBounds pins the two properties every log field
// depends on: a value cannot forge a second log line, and a value cannot dominate the
// log.
func TestValue_StripsForgeryCharactersAndBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		max   int
		want  string
	}{
		{name: "empty stays empty", value: "", max: 32, want: ""},
		{name: "zero budget yields nothing", value: "anything", max: 0, want: ""},
		{name: "negative budget yields nothing", value: "anything", max: -1, want: ""},
		{
			name:  "newline becomes a space so a line cannot be split",
			value: "first\nlevel=error msg=\"forged\"",
			max:   128,
			want:  "first level=error msg=\"forged\"",
		},
		{
			name:  "carriage return and tab become spaces",
			value: "a\rb\tc",
			max:   128,
			want:  "a b c",
		},
		{
			name:  "other control characters are removed outright",
			value: "esc\x1b[31mred\x00nul",
			max:   128,
			want:  "esc[31mrednul",
		},
		{name: "surrounding whitespace is trimmed", value: "  padded  ", max: 128, want: "padded"},
		{name: "value at the cap is untouched", value: "abcde", max: 5, want: "abcde"},
		{
			name:  "value over the cap is marked",
			value: "abcdef",
			max:   5,
			want:  "abcde" + TruncationSuffix,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, Value(tt.value, tt.max))
		})
	}
}

// TestValue_TruncatesOnRuneBoundary proves the cap cannot cut a multi-byte character in
// half, which would put invalid UTF-8 into a structured log.
func TestValue_TruncatesOnRuneBoundary(t *testing.T) {
	t.Parallel()

	got := Value("日本語テキスト", 3)

	require.True(t, strings.HasSuffix(got, TruncationSuffix))
	assert.Equal(t, "日本語"+TruncationSuffix, got)
	assert.True(t, isValidUTF8(got), "truncated value must remain valid UTF-8")
}

// TestCause_RedactsNetworkTopologyAndKeepsDiagnosis is the core of the finding: the
// words that tell an operator what happened survive, and the addresses that tell a
// reader where it happened do not.
func TestCause_RedactsNetworkTopologyAndKeepsDiagnosis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		err         error
		mustContain []string
		mustNotHave []string
	}{
		{
			name:        "dial failure keeps the reason and loses the broker address",
			err:         errors.New("dial tcp 10.0.3.14:9092: connect: connection refused"),
			mustContain: []string{"connection refused", Placeholder},
			mustNotHave: []string{"10.0.3.14", "9092"},
		},
		{
			name: "resolver failure loses both the name and the resolver",
			err: errors.New(
				"dial tcp: lookup kafka-0.kafka-headless.blnk.svc.cluster.local " +
					"on 10.96.0.10:53: no such host",
			),
			mustContain: []string{"no such host", Placeholder},
			mustNotHave: []string{"kafka-headless", "cluster.local", "10.96.0.10"},
		},
		{
			name:        "connection tuple loses both ends",
			err:         errors.New("read tcp 10.244.1.5:41232->10.0.3.14:9092: i/o timeout"),
			mustContain: []string{"i/o timeout"},
			mustNotHave: []string{"10.244.1.5", "41232", "10.0.3.14", "9092"},
		},
		{
			name:        "bracketed IPv6 endpoint is redacted",
			err:         errors.New("dial tcp [2001:db8::1]:9092: connect: network is unreachable"),
			mustContain: []string{"network is unreachable", Placeholder},
			mustNotHave: []string{"2001:db8", "9092"},
		},
		{
			name:        "a DSN in an error loses its host and its password",
			err:         errors.New("failed to connect to postgres://blnk:hunter2@db.internal:5432/blnk"),
			mustContain: []string{"postgres://" + Placeholder},
			mustNotHave: []string{"hunter2", "db.internal", "5432"},
		},
		{
			name:        "a quoted key-value secret loses only its value",
			err:         errors.New("bad connection string host=db.internal password=hunter2 sslmode=disable"),
			mustContain: []string{"sslmode=disable", "password=" + Placeholder, "host=" + Placeholder},
			mustNotHave: []string{"hunter2", "db.internal"},
		},
		{
			name:        "a word for a server followed by prose keeps the prose",
			err:         errors.New("relay test: broker unavailable, host unreachable, server busy"),
			mustContain: []string{"broker unavailable", "host unreachable", "server busy"},
			mustNotHave: []string{Placeholder},
		},
		{
			name:        "a word for a server followed by an address loses the address",
			err:         errors.New("broker kafka-0.blnk.svc:9092 rejected the write"),
			mustContain: []string{"rejected the write", Placeholder},
			mustNotHave: []string{"kafka-0.blnk.svc", "9092"},
		},
		{
			name:        "a broker authorization error is left entirely intact",
			err:         errors.New("[29] Topic Authorization Failed: the client is not authorized"),
			mustContain: []string{"Topic Authorization Failed", "not authorized"},
			mustNotHave: []string{Placeholder},
		},
		{
			name:        "ordinary dotted prose is not mistaken for a host",
			err:         errors.New("Field 'meta_data.test' must be a string"),
			mustContain: []string{"meta_data.test", "must be a string"},
			mustNotHave: []string{Placeholder},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := Cause(tt.err)

			for _, want := range tt.mustContain {
				assert.Contains(t, got, want, "diagnosis must survive redaction: %s", got)
			}

			for _, unwanted := range tt.mustNotHave {
				assert.NotContains(t, got, unwanted, "topology must not survive redaction: %s", got)
			}
		})
	}
}

// TestCause_SanitizesAndBounds proves redaction did not displace the other two
// protections: a transport error is still incapable of forging a line or of running
// past the cap.
func TestCause_SanitizesAndBounds(t *testing.T) {
	t.Parallel()

	t.Run("nil error yields an empty string", func(t *testing.T) {
		t.Parallel()

		assert.Empty(t, Cause(nil))
	})

	t.Run("newlines cannot split the line", func(t *testing.T) {
		t.Parallel()

		got := Cause(errors.New("broken\nlevel=fatal msg=\"forged\""))

		assert.NotContains(t, got, "\n")
		assert.Contains(t, got, "forged")
	})

	t.Run("an oversized error is bounded and marked", func(t *testing.T) {
		t.Parallel()

		got := Cause(errors.New(strings.Repeat("x", MaxErrorLength*3)))

		assert.True(t, strings.HasSuffix(got, TruncationSuffix))
		assert.Len(t, []rune(got), MaxErrorLength+len([]rune(TruncationSuffix)))
	})

	t.Run("a wrapped cause is redacted through the wrapping", func(t *testing.T) {
		t.Parallel()

		inner := errors.New("dial tcp 10.0.3.14:9092: connect: connection refused")
		got := Cause(fmt.Errorf("ensuring topic blnk.transactions: %w", inner))

		assert.Contains(t, got, "blnk.transactions")
		assert.Contains(t, got, "connection refused")
		assert.NotContains(t, got, "10.0.3.14")
	})
}

// TestCauseVerbatim_KeepsTopologyButStillSanitizes proves the debug escape hatch is a
// real escape hatch — the address is there — while remaining incapable of forging a
// line or overrunning the cap.
func TestCauseVerbatim_KeepsTopologyButStillSanitizes(t *testing.T) {
	t.Parallel()

	t.Run("nil error yields an empty string", func(t *testing.T) {
		t.Parallel()

		assert.Empty(t, CauseVerbatim(nil))
	})

	t.Run("the address survives", func(t *testing.T) {
		t.Parallel()

		got := CauseVerbatim(errors.New("dial tcp 10.0.3.14:9092: connect: connection refused"))

		assert.Contains(t, got, "10.0.3.14:9092")
		assert.NotContains(t, got, Placeholder)
	})

	t.Run("forgery characters are still removed", func(t *testing.T) {
		t.Parallel()

		got := CauseVerbatim(errors.New("host 10.0.0.1\nlevel=fatal"))

		assert.NotContains(t, got, "\n")
	})

	t.Run("the cap still applies", func(t *testing.T) {
		t.Parallel()

		got := CauseVerbatim(errors.New(strings.Repeat("y", MaxErrorLength*2)))

		assert.True(t, strings.HasSuffix(got, TruncationSuffix))
	})
}

// TestIdentifier_IsStableCorrelatableAndNotReversible pins the three properties a
// hashed identifier is used for.
func TestIdentifier_IsStableCorrelatableAndNotReversible(t *testing.T) {
	t.Parallel()

	const subject = "ldg_9f2b1c4e"

	first := Identifier(subject)
	second := Identifier(subject)

	assert.Equal(t, first, second, "the same subject must correlate across lines")
	assert.Len(t, first, IdentifierHashLength)
	assert.NotContains(t, first, subject)
	assert.NotEqual(t, first, Identifier("ldg_other"))

	assert.Empty(t, Identifier(""),
		"an absent value must stay distinguishable from a hidden one")
}

// TestRedactEndpoints_PreservesSurroundingPunctuation proves a redacted line still
// reads as a sentence, so nobody mistakes redaction for a truncated or corrupt error.
func TestRedactEndpoints_PreservesSurroundingPunctuation(t *testing.T) {
	t.Parallel()

	got := RedactEndpoints("failed at 10.0.0.1:9092, retrying")

	assert.Equal(t, "failed at "+Placeholder+", retrying", got)
}

// TestRedactEndpoints_EmptyInput guards the trivial path the callers rely on so that an
// error with no text does not become the placeholder.
func TestRedactEndpoints_EmptyInput(t *testing.T) {
	t.Parallel()

	assert.Empty(t, RedactEndpoints(""))
}

// isValidUTF8 reports whether every rune in the string decoded cleanly, which is the
// property a rune-boundary truncation exists to protect.
//
// Parameters:
//   - value string: the string to check.
//
// Returns:
//   - bool: false if any byte sequence decoded to the replacement rune.
func isValidUTF8(value string) bool {
	for _, r := range value {
		if r == '\uFFFD' {
			return false
		}
	}

	return true
}
