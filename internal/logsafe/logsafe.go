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

// Package logsafe holds the primitives that make an untrusted or internally
// revealing value safe to write to a log line.
package logsafe

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"strings"
	"unicode"
)

const (
	// MaxErrorLength caps an error string from a dependency. Generous enough to keep
	// the diagnostic part of a real broker error — which leads with the useful text —
	// and short enough that no single line can dominate a log.
	MaxErrorLength = 512

	// MaxValueLength caps a caller-supplied value echoed back into a log line.
	MaxValueLength = 128

	// TruncationSuffix marks a value the cap shortened, so a truncated line is never
	// mistaken for a complete one.
	TruncationSuffix = "…[truncated]"

	// IdentifierHashLength is how many hex characters of the SHA-256 digest a hashed
	// identifier keeps. Sixteen hex characters is 64 bits: ample to distinguish the
	// subjects a single operator is looking at without being reversible, and the full
	// digest would only make the line longer.
	IdentifierHashLength = 16

	// Placeholder is what a redacted endpoint becomes. A fixed marker rather than
	// removal, so a reader can see that something was withheld and go to the
	// debug-level verbatim rendering for it instead of concluding the error text was
	// malformed.
	Placeholder = "[redacted]"
)

// strictEndpointIntroducers are the words after which the next token IS an address, so
// the token following one is replaced whatever its shape.
var strictEndpointIntroducers = map[string]struct{}{
	"lookup": {},
}

// looseEndpointIntroducers are the words after which the next token MAY be an address,
// so the token following one is replaced only when it is also shaped like a host.
var looseEndpointIntroducers = map[string]struct{}{
	"dial": {}, "read": {}, "write": {}, "listen": {}, "accept": {},
	"tcp": {}, "tcp4": {}, "tcp6": {}, "udp": {}, "udp4": {}, "udp6": {},
	"unix": {}, "unixgram": {}, "unixpacket": {}, "ip": {}, "ip4": {}, "ip6": {},
	"on": {}, "from": {}, "to": {}, "at": {}, "via": {}, "reaching": {},
	"host": {}, "hostname": {}, "broker": {}, "brokers": {}, "bootstrap": {},
	"server": {}, "servers": {}, "endpoint": {}, "address": {}, "addr": {},
	"peer": {}, "upstream": {},
}

// endpointKeys are the connection-string keys whose VALUE is an address.
var endpointKeys = map[string]struct{}{
	"host": {}, "hostname": {}, "server": {}, "servers": {},
	"addr": {}, "address": {}, "broker": {}, "brokers": {},
	"bootstrap": {}, "bootstrap_servers": {}, "bootstrap.servers": {},
	"endpoint": {}, "peer": {}, "port": {},
}

// sensitiveKeys are the connection-string and query-parameter keys whose VALUE is a
// secret. Postgres and Redis errors quote the DSN back, so "password=hunter2" reaches
// a log line intact unless the value is removed.
var sensitiveKeys = map[string]struct{}{
	"password": {}, "passwd": {}, "pwd": {}, "pass": {},
	"secret": {}, "token": {}, "apikey": {}, "api_key": {}, "auth": {},
	"sasl_password": {}, "sasl_username": {}, "user": {}, "username": {},
}

// secretKeySegments are the words that make a key's value a secret WHEREVER they appear
// as a whole dot-, underscore- or hyphen-separated segment of it.
var secretKeySegments = map[string]struct{}{
	"password": {}, "passwd": {}, "pwd": {},
	"secret": {}, "secrets": {}, "token": {}, "apikey": {},
	"credential": {}, "credentials": {},
	"auth": {}, "authorization": {}, "jaas": {},
	"username": {}, "userinfo": {},
}

// routingKeyQualifiers name the "…_key" keys whose value is a ROUTING value and must
// survive, which is how "key" can be treated as a credential word without silencing the
// diagnostics this pipeline logs on purpose.
var routingKeyQualifiers = map[string]struct{}{
	"partition": {}, "idempotency": {}, "aggregate": {}, "event": {}, "message": {},
	"sort": {}, "primary": {}, "foreign": {}, "natural": {}, "business": {},
	"dedup": {}, "cache": {}, "row": {}, "record": {}, "shard": {}, "routing": {},
}

// endpointKeySegments are the words that make a key's value an address wherever they
// appear as a whole segment of it, for the same reason secretKeySegments exists: shape
// alone misses "KAFKA_BROKERS=broker.internal", which carries no port, and whole-key
// matching misses it too because of the prefix.
var endpointKeySegments = map[string]struct{}{
	"host": {}, "hostname": {}, "hosts": {},
	"server": {}, "servers": {},
	"broker": {}, "brokers": {}, "bootstrap": {},
	"endpoint": {}, "endpoints": {},
	"addr": {}, "address": {}, "addresses": {},
	"peer": {}, "peers": {}, "port": {},
	"dsn": {}, "url": {}, "uri": {},
}

// keySeparatorReplacer folds the separators a configuration key is spelled with onto
// one, so that "sasl.username", "sasl-username" and "sasl_username" are one key rather
// than three entries.
var keySeparatorReplacer = strings.NewReplacer(".", "_", "-", "_")

// Value makes an untrusted string safe to log: it strips the characters that let a
// value forge log structure, and it caps the length.
//
// Parameters:
//   - value string: the untrusted text.
//   - max int: the maximum number of runes to keep. Values below 1 yield an empty
//     string.
//
// Returns:
//   - string: the sanitized, bounded text.
func Value(value string, max int) string {
	if value == "" || max < 1 {
		return ""
	}

	return bound(clean(value), max)
}

// RedactedValue is Value with network topology removed as well: the rendering for a STRING
// that carries a dependency's own words to a log line at a normal level.
//
// Parameters:
//   - value string: the untrusted text.
//   - max int: the maximum number of runes to keep. Values below 1 yield an empty string.
//
// Returns:
//   - string: the sanitized, redacted, bounded text.
func RedactedValue(value string, max int) string {
	if value == "" || max < 1 {
		return ""
	}

	return bound(RedactEndpoints(clean(value)), max)
}

// Cause renders an error for an operational log line: control characters stripped,
// network topology redacted, length bounded.
//
// Parameters:
//   - err error: the error to render. A nil error yields an empty string, so a caller
//     can attach the field unconditionally without inventing a "no error" word.
//
// Returns:
//   - string: the sanitized, redacted, bounded rendering.
func Cause(err error) string {
	if err == nil {
		return ""
	}

	return bound(RedactEndpoints(clean(err.Error())), MaxErrorLength)
}

// CauseVerbatim renders an error for a DEBUG-level field: sanitized and bounded, but
// with nothing redacted.
//
// Parameters:
//   - err error: the error to render. A nil error yields an empty string.
//
// Returns:
//   - string: the sanitized, bounded, unredacted rendering.
func CauseVerbatim(err error) string {
	if err == nil {
		return ""
	}

	return bound(clean(err.Error()), MaxErrorLength)
}

// Identifier replaces a value with a short, stable digest of it.
//
// Parameters:
//   - value string: the value to hash.
//
// Returns:
//   - string: IdentifierHashLength hex characters, or "" for an empty input.
func Identifier(value string) string {
	if value == "" {
		return ""
	}

	sum := sha256.Sum256([]byte(value))

	return hex.EncodeToString(sum[:])[:IdentifierHashLength]
}

// RedactEndpoints removes network addresses and secret values from text, leaving the
// surrounding words in place.
//
// Parameters:
//   - text string: the text to redact. Expected to be already free of control
//     characters, so that whitespace splitting is well defined.
//
// Returns:
//   - string: the text with endpoints and secret values replaced by Placeholder.
func RedactEndpoints(text string) string {
	if text == "" {
		return ""
	}

	tokens := strings.Split(text, " ")
	previous := ""

	for i, token := range tokens {
		if token == "" {
			continue
		}

		tokens[i] = redactToken(token, previous)
		previous = normalizeIntroducer(token)
	}

	return strings.Join(tokens, " ")
}

// redactToken applies the per-token rules to one token, preserving the punctuation
// that surrounds it.
func redactToken(token, previous string) string {
	if strings.Contains(token, "->") {
		sides := strings.Split(token, "->")
		for i, side := range sides {
			sides[i] = redactToken(side, previous)
		}

		return strings.Join(sides, "->")
	}

	prefix, core, suffix := splitPunctuation(token)
	if core == "" {
		return token
	}

	if redacted, ok := redactURL(core); ok {
		return prefix + redacted + suffix
	}

	if redacted, ok := redactSensitiveAssignment(core); ok {
		return prefix + redacted + suffix
	}

	if isEndpoint(core) {
		return prefix + Placeholder + suffix
	}

	if _, strict := strictEndpointIntroducers[previous]; strict {
		return prefix + Placeholder + suffix
	}

	if _, loose := looseEndpointIntroducers[previous]; loose && looksAddressable(core) {
		return prefix + Placeholder + suffix
	}

	return token
}

// redactURL replaces everything after a scheme, keeping the scheme itself.
func redactURL(core string) (string, bool) {
	index := strings.Index(core, "://")
	if index <= 0 {
		return "", false
	}

	return core[:index+len("://")] + Placeholder, true
}

// redactSensitiveAssignment replaces the value of a "key=value" token when the key names
// a secret or an endpoint.
func redactSensitiveAssignment(core string) (string, bool) {
	index := strings.Index(core, "=")
	if index <= 0 || index == len(core)-1 {
		return "", false
	}

	if !keyNamesRedactableValue(strings.ToLower(strings.Trim(core[:index], "\"'"))) {
		return "", false
	}

	return core[:index+1] + Placeholder, true
}

// keyNamesRedactableValue reports whether a configuration key's value must be replaced,
// in three layers.
func keyNamesRedactableValue(key string) bool {
	if key == "" {
		return false
	}

	if _, ok := sensitiveKeys[key]; ok {
		return true
	}

	if _, ok := endpointKeys[key]; ok {
		return true
	}

	normalized := key
	if strings.ContainsAny(key, ".-") {
		normalized = keySeparatorReplacer.Replace(key)

		if _, ok := sensitiveKeys[normalized]; ok {
			return true
		}

		if _, ok := endpointKeys[normalized]; ok {
			return true
		}
	}

	segments := strings.Split(normalized, "_")
	for position, segment := range segments {
		if segment == "" {
			continue
		}

		if _, ok := secretKeySegments[segment]; ok {
			return true
		}

		if _, ok := endpointKeySegments[segment]; ok {
			return true
		}

		if segment != "key" && segment != "keys" {
			continue
		}

		// A bare "key=" has nothing qualifying it, so it takes the safe reading.
		if position == 0 {
			return true
		}

		if _, routing := routingKeyQualifiers[segments[position-1]]; !routing {
			return true
		}
	}

	return false
}

// isEndpoint reports whether a token is an address by SHAPE.
func isEndpoint(core string) bool {
	if net.ParseIP(core) != nil {
		return true
	}

	if host, port, err := net.SplitHostPort(core); err == nil && host != "" && isPort(port) {
		return true
	}

	// A bracketed IPv6 address without a port, which SplitHostPort rejects.
	if strings.HasPrefix(core, "[") && strings.HasSuffix(core, "]") {
		return net.ParseIP(strings.Trim(core, "[]")) != nil
	}

	return false
}

// looksAddressable reports whether a token could be a host name, for use only after a
// loose introducer.
func looksAddressable(core string) bool {
	if core == "" {
		return false
	}

	for _, r := range core {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.' || r == '-' || r == '_' || r == ':' || r == '[' || r == ']' || r == '/':
		default:
			return false
		}
	}

	// A single bare word with no dot, colon or slash is far more likely to be prose
	// than a host, and the introducer list contains words ("read", "to", "at") that
	// routinely precede ordinary text.
	return strings.ContainsAny(core, ".:[")
}

// isPort reports whether a string is a decimal port number.
func isPort(port string) bool {
	if port == "" || len(port) > 5 {
		return false
	}

	for _, r := range port {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

// normalizeIntroducer reduces a token to the bare word the positional rule matches on.
func normalizeIntroducer(token string) string {
	_, core, _ := splitPunctuation(token)

	return strings.ToLower(core)
}

// splitPunctuation separates a token into leading punctuation, core, and trailing
// punctuation.
func splitPunctuation(token string) (string, string, string) {
	const cutset = `,;:.!?"'()<>`

	core := strings.Trim(token, cutset)
	if core == "" {
		return "", "", ""
	}

	start := strings.Index(token, core)

	return token[:start], core, token[start+len(core):]
}

// zeroWidthNonJoiner and zeroWidthJoiner are the two Unicode format characters this
// package keeps: they carry orthographic meaning in Persian, Arabic and Indic scripts
// and bind emoji sequences, so dropping them would corrupt a legitimate value rather
// than sanitise a hostile one. Every other format character is removed below.
const (
	zeroWidthNonJoiner = '\u200C'
	zeroWidthJoiner    = '\u200D'
)

// clean removes the characters that let a value forge log structure.
//
// Two classes forge that structure, not one. Control characters do it by writing it — a
// newline starts a second entry, a carriage return overwrites the entry already there,
// an escape sequence rewrites the terminal rendering it. Unicode FORMAT characters do it
// by reordering or hiding it: U+202E RIGHT-TO-LEFT OVERRIDE reverses everything after it
// on the rendered line, so a value can make the fields that follow read as something
// else entirely, and U+200B, U+00AD and U+FEFF are invisible, so two different values
// can present one appearance to whoever reads the log. Both classes are dropped. This
// mirrors model.InspectSubscriberText, which REFUSES the same two classes at the
// subscriber write boundary; this package cannot import that rule because it is a
// stdlib-only leaf every layer logs through, so the two exceptions are restated above.
func clean(value string) string {
	cleaned := strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case unicode.IsControl(r):
			// C0, DEL and the C1 range: U+009B is a CSI introducer on its own, so a test that
			// stopped at 0x7f left an escape introducer in the line.
			return -1
		case r == zeroWidthNonJoiner || r == zeroWidthJoiner:
			return r
		case unicode.Is(unicode.Cf, r):
			return -1
		default:
			return r
		}
	}, value)

	return strings.TrimSpace(cleaned)
}

// bound caps a string at a rune count, marking the cut.
func bound(value string, max int) string {
	if max < 1 {
		return ""
	}

	runes := []rune(value)
	if len(runes) <= max {
		return value
	}

	return string(runes[:max]) + TruncationSuffix
}
