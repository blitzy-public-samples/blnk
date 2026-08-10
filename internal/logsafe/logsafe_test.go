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

// TestRedactedValue_RemovesTopologyFromTextTheSameWayCauseDoesFromAnError is the guard on the
// rendering a failure gets when it arrives as a STRING rather than as an error.
//
// The two have to agree. A dead-lettered event's reason is recorded on its outbox row before it
// is ever logged, so the log site holds text — and it used to pass that text through Value,
// which makes a value's FORM safe and leaves the broker's address, its port and the internal
// resolver's address exactly where they were. The consequence was a log line at the default
// level naming the deployment's internal topology, from the one path whose input was not an
// error value.
//
// The ORDER inside this helper is the reason it exists rather than being composed at the call
// site: redaction splits on whitespace, so it must run on cleaned text, and it must run BEFORE
// bounding, or a cap that truncated an address mid-token would leave the fragment unmatched by
// every endpoint rule and therefore unredacted.
func TestRedactedValue_RemovesTopologyFromTextTheSameWayCauseDoesFromAnError(t *testing.T) {
	for name, testCase := range map[string]struct {
		text      string
		diagnosis string
		leaks     []string
	}{
		"a refused dial": {
			text:      "kafka.(*Client).Produce: dial tcp 172.21.0.2:9092: connect: connection refused",
			diagnosis: "connection refused",
			leaks:     []string{"172.21.0.2", "9092"},
		},
		"a resolution failure, which also names the resolver": {
			text:      "dial tcp: lookup kafka on 127.0.0.11:53: no such host",
			diagnosis: "no such host",
			leaks:     []string{"127.0.0.11", ":53", "lookup kafka on"},
		},
		"a connection tuple with both ends": {
			text:      "write tcp 10.0.0.4:34918->10.0.0.7:9092: write: broken pipe",
			diagnosis: "broken pipe",
			leaks:     []string{"10.0.0.4", "10.0.0.7", "34918"},
		},
		"a quoted connection string": {
			text:      `pq: connection failed password=hunter2 host=db.internal:5432`,
			diagnosis: "connection failed",
			leaks:     []string{"hunter2", "db.internal", "5432"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			rendered := RedactedValue(testCase.text, MaxErrorLength)

			for _, leak := range testCase.leaks {
				assert.NotContains(t, rendered, leak,
					"%q is reconnaissance once it is sitting in a log aggregator", leak)
			}

			assert.Contains(t, rendered, testCase.diagnosis,
				"the diagnosis must survive: redaction removes addresses and secrets, not prose")
			assert.Contains(t, rendered, Placeholder,
				"and the removal must be visible, so the line is not read as corrupted")

			// The identical text handed to Cause as an error must render identically, or the two
			// sinks disagree about the same failure.
			assert.Equal(t, Cause(errors.New(testCase.text)), rendered,
				"a failure must render the same whether it reached the log site as text or as an error")
		})
	}
}

// TestRedactedValue_SanitizesAndBoundsLikeValue pins the properties it inherits, because
// redaction is an ADDITION to sanitization rather than a replacement for it: a forged newline
// still splits a line and an unbounded broker error still dominates a log, redacted or not.
func TestRedactedValue_SanitizesAndBoundsLikeValue(t *testing.T) {
	forged := "dial tcp 10.0.0.4:9092: connect: connection refused\nERROR everything is fine\x07" +
		strings.Repeat("y", MaxErrorLength*2)

	rendered := RedactedValue(forged, MaxErrorLength)

	assert.NotContains(t, rendered, "\n", "a newline would forge a second log entry")
	assert.NotContains(t, rendered, "\x07", "control characters corrupt terminals and parsers")
	assert.NotContains(t, rendered, "10.0.0.4", "and the address is still removed")
	assert.LessOrEqual(t, len([]rune(rendered)), MaxErrorLength+len([]rune(TruncationSuffix)),
		"the cap still applies")
	assert.True(t, strings.HasSuffix(rendered, TruncationSuffix), "truncation stays marked")

	assert.Empty(t, RedactedValue("", MaxErrorLength),
		"an empty input yields an empty string rather than a placeholder for nothing")
	assert.Empty(t, RedactedValue("dial tcp 10.0.0.4:9092", 0),
		"and a non-positive cap yields nothing at all, exactly as Value does")
}

// TestCause_RedactsASecretInEverySpellingOfItsKey is the guard on the defect that whole-key
// matching cannot avoid: the same secret is spelled "password" in a Postgres DSN,
// "sasl.password" in a Kafka property, "ssl.keystore.password" in a client config and
// "KAFKA_SASL_ADMIN_SECRET" in this service's own environment, and every one of those
// reached a log line intact while only the bare words were enumerated.
//
// Each case here is a spelling this deployment can actually produce, and each asserts BOTH
// halves of the contract: the value is gone, and the key that names which setting failed is
// still there, because "the DSN was rejected" without the field name is not a diagnosis.
func TestCause_RedactsASecretInEverySpellingOfItsKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		err    error
		secret string
		keptAs string
	}{
		{
			name:   "a Kafka dotted property",
			err:    errors.New("sasl.password=hunter2 was rejected by the broker"),
			secret: "hunter2",
			keptAs: "sasl.password=",
		},
		{
			name:   "a Kafka dotted principal",
			err:    errors.New("sasl.username=blnk-producer is unknown"),
			secret: "blnk-producer",
			keptAs: "sasl.username=",
		},
		{
			name:   "a nested keystore property",
			err:    errors.New("ssl.keystore.password=changeit did not open the keystore"),
			secret: "changeit",
			keptAs: "ssl.keystore.password=",
		},
		{
			name:   "this service's own admin credential variable",
			err:    errors.New("KAFKA_SASL_ADMIN_SECRET=hunter2 is malformed"),
			secret: "hunter2",
			keptAs: "KAFKA_SASL_ADMIN_SECRET=",
		},
		{
			name:   "an underscored spelling no enumeration held",
			err:    errors.New("sasl_secret=hunter2 was rejected"),
			secret: "hunter2",
			keptAs: "sasl_secret=",
		},
		{
			name:   "a hyphenated header spelling",
			err:    errors.New("proxy-authorization=Bearer-abc123 was refused"),
			secret: "abc123",
			keptAs: "proxy-authorization=",
		},
		{
			name:   "a bearer token variable",
			err:    errors.New("BLNK_METRICS_BEARER_TOKEN=abc123 did not match"),
			secret: "abc123",
			keptAs: "BLNK_METRICS_BEARER_TOKEN=",
		},
		{
			name:   "a credential word that was never enumerated at all",
			err:    errors.New("credentials=abc123 were refused"),
			secret: "abc123",
			keptAs: "credentials=",
		},
		{
			name:   "a key qualified by a service rather than by a routing word",
			err:    errors.New("BLNK_TYPESENSE_KEY=abc123 was rejected"),
			secret: "abc123",
			keptAs: "BLNK_TYPESENSE_KEY=",
		},
		{
			name:   "a JAAS fragment that arrives as one token",
			err:    errors.New(`sasl.jaas.config=PlainLoginModule/required/password="s3cr3t" is invalid`),
			secret: "s3cr3t",
			keptAs: "sasl.jaas.config=",
		},
		{
			name:   "an address under a prefixed key, which carries no port for shape to catch",
			err:    errors.New("KAFKA_BROKERS=broker.internal is unreachable"),
			secret: "broker.internal",
			keptAs: "KAFKA_BROKERS=",
		},
		{
			name:   "an address under a nested key",
			err:    errors.New("kafka.bootstrap.servers=broker.internal is down"),
			secret: "broker.internal",
			keptAs: "kafka.bootstrap.servers=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := Cause(tt.err)

			assert.NotContains(t, got, tt.secret,
				"the value must not survive in any spelling of its key: %s", got)
			assert.Contains(t, got, tt.keptAs+Placeholder,
				"the key must survive so the line still names which setting failed: %s", got)
		})
	}
}

// TestCause_KeepsTheAssignmentsThePipelineLogsOnPurpose is the other side of the rule above,
// and the reason the segment sets are not simply widened until nothing gets through.
//
// Every case here is a value this codebase logs deliberately. "partition_key" in particular
// is what the relay's ordering diagnosis is written in: redacting it would leave an operator
// investigating an out-of-order delivery unable to see which key was affected. A test that
// only proved secrets vanish would be satisfied by a rule that redacts everything.
func TestCause_KeepsTheAssignmentsThePipelineLogsOnPurpose(t *testing.T) {
	t.Parallel()

	kept := []string{
		"partition_key=ldg_0192",
		"partition_keys=2",
		"event_key=evt_a1b2",
		"idempotency-key=evt_a1b2",
		"sort_key=occurred_at",
		"aggregate_key=ldg_0192",
		"user_agent=blnk/1.0",
		"pass_rate=0.99",
		"sasl.mechanism=SCRAM-SHA-512",
		"KAFKA_TOPIC_PREFIX=blnk",
		"RELAY_MAX_RETRY_ATTEMPTS=5",
		"attempts=5",
		"sslmode=disable",
	}

	for _, assignment := range kept {
		t.Run(assignment, func(t *testing.T) {
			t.Parallel()

			got := Cause(errors.New(assignment + " was not accepted"))

			assert.Contains(t, got, assignment,
				"a diagnostic assignment must survive: %s", got)
			assert.NotContains(t, got, Placeholder,
				"and nothing in it may be redacted: %s", got)
		})
	}
}

// TestKeyNamesRedactableValue_HoldsForEverySpellingOfEveryWordItKnows derives its inputs from
// the word sets themselves rather than restating them, so a word added later is covered in all
// four spellings the moment it is added, and the three matching layers each stay load-bearing.
//
// This is what makes the rule a rule instead of a list. The defect it replaces was not a
// missing entry; it was that every entry had to be written in advance in the exact spelling it
// would arrive in.
func TestKeyNamesRedactableValue_HoldsForEverySpellingOfEveryWordItKnows(t *testing.T) {
	t.Parallel()

	segmentWords := make([]string, 0, len(secretKeySegments)+len(endpointKeySegments))
	for word := range secretKeySegments {
		segmentWords = append(segmentWords, word)
	}

	for word := range endpointKeySegments {
		segmentWords = append(segmentWords, word)
	}

	require.NotEmpty(t, segmentWords, "the sets must not be empty, or this test proves nothing")

	t.Run("every segment word is caught wherever it sits and however it is separated", func(t *testing.T) {
		t.Parallel()

		for _, word := range segmentWords {
			for _, spelling := range []string{
				word,
				"kafka." + word,
				"KAFKA_" + strings.ToUpper(word),
				"kafka-" + word,
				"ssl." + word + ".override",
			} {
				assert.True(t, keyNamesRedactableValue(strings.ToLower(spelling)),
					"%q must be recognised through the word %q", spelling, word)
			}
		}
	})

	t.Run("every enumerated whole key is caught in each separator spelling", func(t *testing.T) {
		t.Parallel()

		for _, set := range []map[string]struct{}{sensitiveKeys, endpointKeys} {
			for key := range set {
				for _, separator := range []string{"_", ".", "-"} {
					spelling := strings.ReplaceAll(key, "_", separator)
					spelling = strings.ReplaceAll(spelling, ".", separator)

					assert.True(t, keyNamesRedactableValue(spelling),
						"%q must reach the entry %q whichever separator it is written with",
						spelling, key)
				}
			}
		}
	})

	t.Run("a routing key survives and an unqualified one does not", func(t *testing.T) {
		t.Parallel()

		for qualifier := range routingKeyQualifiers {
			assert.False(t, keyNamesRedactableValue(qualifier+"_key"),
				"%s_key names a routing value and must survive", qualifier)
			assert.False(t, keyNamesRedactableValue(qualifier+"_keys"),
				"%s_keys names routing values and must survive", qualifier)
		}

		for _, credential := range []string{"key", "keys", "api_key", "signing.key", "typesense_key"} {
			assert.True(t, keyNamesRedactableValue(credential),
				"%q is not qualified by a routing word, so it takes the safe reading", credential)
		}
	})

	t.Run("a key naming neither a secret nor an address is left alone", func(t *testing.T) {
		t.Parallel()

		for _, ordinary := range []string{
			"sslmode", "attempts", "topic", "event_type", "schema_version",
			"sasl.mechanism", "log.retention.hours", "user_agent", "pass_rate",
		} {
			assert.False(t, keyNamesRedactableValue(ordinary),
				"%q names a diagnostic value and must not be redacted", ordinary)
		}
	})
}
