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
//
// # Why this is a package rather than a helper beside its caller
//
// Two packages need the same answers. The event pipeline in package blnk logs
// broker errors, topic names and subscriber principals; the HTTP layer in package
// api logs request paths, client addresses and recovered panics. Both are the same
// problem — a value that may carry forged log structure, unbounded length, or
// internal topology — and both were originally solved only in package blnk, where
// the helpers are unexported and therefore unreachable from package api.
//
// The alternative was to copy them. A redaction routine that exists twice is a
// redaction routine that will eventually disagree with itself, and the half that
// falls behind is a silent information-disclosure defect rather than a visible bug.
// So the implementation lives here once, package blnk's existing unexported helpers
// delegate to it (keeping every one of their call sites untouched), and package api
// calls it directly.
//
// # The three concerns, kept separate on purpose
//
//   - Value sanitizes and bounds. It is for a value whose CONTENT is legitimate to
//     log but whose FORM cannot be trusted: a topic name, a query parameter, an
//     identifier.
//   - Cause sanitizes, bounds AND redacts network topology. It is for an error
//     raised by a dependency, whose text routinely carries broker addresses,
//     resolver addresses and connection tuples that no normal log line needs.
//   - Identifier replaces a value entirely with a short digest. It is for something
//     that must be correlatable across lines without being readable.
//
// A caller that reaches for Value where Cause is required gets a bounded line that
// still names an internal address, which is why the two are named for their intent
// rather than for their mechanics.
package logsafe

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"strings"
)

const (
	// MaxErrorLength caps an error string from a dependency. Generous enough to keep
	// the diagnostic part of a real broker error — which leads with the useful text —
	// and short enough that no single line can dominate a log.
	MaxErrorLength = 512

	// MaxValueLength caps a caller-supplied value echoed back into a log line.
	// Shorter than an error cap because these are identifiers and topic names, where
	// anything long is malformed input rather than detail.
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
//
// This is the rule that catches a bare host name: Go renders a DNS failure as
// "lookup <name> on <resolver>: <reason>", and <name> carries no port, so shape alone
// cannot recognise it — only the word in front of it can.
//
// THE LIST HOLDS EXACTLY ONE WORD, and adding to it is almost always a mistake. Words
// like "broker", "host" and "server" look like they belong here and do not: this
// codebase's own messages say "broker unavailable" and "server could not be reached",
// and an unconditional rule behind those words redacts the diagnosis instead of the
// address. "lookup" earns its place because it is a fixed prefix of a Go error format
// and is never followed by prose. Everything else belongs in looseEndpointIntroducers,
// where the shape requirement protects the words.
var strictEndpointIntroducers = map[string]struct{}{
	"lookup": {},
}

// looseEndpointIntroducers are the words after which the next token MAY be an address,
// so the token following one is replaced only when it is also shaped like a host.
//
// Every one of these words routinely precedes ordinary text as well: "dial tcp:
// connect: connection refused" puts the word "connect" straight after a network type,
// and "broker unavailable" puts a plain adjective after a word for a server. Requiring
// the shape too is what keeps the rule from redacting the reason.
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
//
// Separate from the introducer rules because the "=" makes the intent unambiguous:
// "host=db.internal" is an endpoint whatever its shape, and there is no prose risk,
// whereas the bare word "host" followed by something is frequently a sentence.
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

// Value makes an untrusted string safe to log: it strips the characters that let a
// value forge log structure, and it caps the length.
//
// Both halves matter. Newlines and carriage returns become spaces because a value
// carrying them SPLITS a line, and in a line-oriented log a forged newline followed
// by a plausible prefix is a fabricated entry. Other control characters are removed
// because they corrupt terminals and confuse structured-log parsers. The length cap
// then bounds what remains.
//
// Truncation is marked rather than silent, so nobody reads a shortened broker error
// as the whole of it, and it happens on a RUNE boundary so a multi-byte character is
// never cut in half into invalid UTF-8.
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

// Cause renders an error for an operational log line: control characters stripped,
// network topology redacted, length bounded.
//
// It is the rendering for a NORMAL log level. The verbatim text is not discarded —
// CauseVerbatim returns it for a debug-level companion field — but it is not what a
// running deployment writes at info, warn or error, because a dependency's error text
// is written for a developer at a terminal rather than for a log aggregator that many
// people can read. A kafka-go dial failure names the broker's address and port, a
// resolver failure names the internal DNS server, and a Postgres failure can quote the
// DSN; none of that is needed to know that the broker is unreachable, and all of it is
// reconnaissance for anyone who should not have it.
//
// What survives is the diagnosis. Redaction removes address-shaped tokens and secret
// values only, so "connection refused", "i/o timeout" and "Cluster Authorization
// Failed" — the words that actually tell an operator what to do — are still there.
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
// This is the escape hatch that makes redaction acceptable. An operator diagnosing a
// broker problem genuinely needs the address that failed, and withholding it
// everywhere would trade a disclosure risk for an outage-lengthening one. Turning the
// standard logger to debug (BLNK_LOG_LEVEL=debug) is an explicit, auditable act that
// says "I accept this detail in my log for now" — so raw diagnostics are reachable
// without being the default.
//
// Sanitization still applies: even a trusted reader must not be shown a line an error
// string could have forged, and an unbounded error must not be able to dominate the
// log.
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
// It is for a value that has to be CORRELATABLE without being READABLE: two lines
// about the same subject carry the same digest, so an operator can follow one subject
// through a log, while the log itself never names it.
//
// An empty input yields an empty string rather than the digest of "". A hash of
// nothing looks exactly like a hash of something, and "no value was present" is a
// materially different fact from "a value was present and is hidden".
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
// It works token by token rather than by matching the whole string against a pattern.
// That is deliberate: an error string is prose with addresses embedded in it, and a
// pattern broad enough to catch every address shape in one pass is also broad enough
// to eat the prose. Deciding per token keeps each rule small enough to state, and each
// rule's false positives bounded to a single word.
//
// The rules, in order of application per token:
//
//  1. A token containing "://" is a URL: the scheme is kept and everything after it is
//     replaced, since userinfo, host, port and path are all disclosure.
//  2. A "key=value" token whose key is a secret name loses its value only, so the
//     shape of a quoted DSN is still legible.
//  3. A token that is an IP literal, a bracketed IPv6 address, or anything ending in
//     ":<port>" is an endpoint and is replaced whole.
//  4. A token following a strict introducer is an endpoint by POSITION even when its
//     shape is innocent, which is how a bare host name is caught; after a loose
//     introducer it must look addressable as well.
//
// A token of the form "local->remote", which is how Go renders a connection tuple, is
// split and each side judged on its own.
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
//
// Punctuation is separated and re-attached rather than redacted along with the token,
// because the colon that ends "…:9092:" belongs to the error's own grammar and a line
// that loses it reads as though text were missing.
//
// Parameters:
//   - token string: the token, punctuation included.
//   - previous string: the previous token, normalized, used for the positional rule.
//
// Returns:
//   - string: the token, redacted if any rule matched.
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
//
// The scheme is worth keeping: "postgres://[redacted]" and "https://[redacted]" tell a
// reader which dependency failed, which is the part of a URL that is diagnosis rather
// than topology.
//
// Parameters:
//   - core string: the token with surrounding punctuation removed.
//
// Returns:
//   - string: the redacted URL, when the token was one.
//   - bool: whether the token was a URL.
func redactURL(core string) (string, bool) {
	index := strings.Index(core, "://")
	if index <= 0 {
		return "", false
	}

	return core[:index+len("://")] + Placeholder, true
}

// redactSensitiveAssignment replaces the value of a "key=value" token when the key names
// a secret or an endpoint.
//
// The key is kept and only the value replaced, so the shape of a quoted connection string
// stays legible — "host=[redacted] password=[redacted] sslmode=disable" still tells a
// reader which settings were present, which is most of what makes a DSN error useful.
//
// Parameters:
//   - core string: the token with surrounding punctuation removed.
//
// Returns:
//   - string: "key=[redacted]", when the key was one of the two kinds.
//   - bool: whether the token was such an assignment.
func redactSensitiveAssignment(core string) (string, bool) {
	index := strings.Index(core, "=")
	if index <= 0 || index == len(core)-1 {
		return "", false
	}

	key := strings.ToLower(strings.Trim(core[:index], "\"'"))

	_, sensitive := sensitiveKeys[key]
	if !sensitive {
		if _, endpoint := endpointKeys[key]; !endpoint {
			return "", false
		}
	}

	return core[:index+1] + Placeholder, true
}

// isEndpoint reports whether a token is an address by SHAPE.
//
// Parameters:
//   - core string: the token with surrounding punctuation removed.
//
// Returns:
//   - bool: true for an IP literal, a bracketed IPv6 address, or a "host:port" pair.
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
//
// It excludes tokens that are plainly prose — anything containing a character no host
// or endpoint may hold, and anything that is a single bare word — so that "dial tcp:
// connect: connection refused", where a network type is followed by the reason rather
// than an address, is left intact.
//
// Parameters:
//   - core string: the token with surrounding punctuation removed.
//
// Returns:
//   - bool: whether the token is shaped like something addressable.
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
//
// Parameters:
//   - port string: the candidate.
//
// Returns:
//   - bool: whether it is one to five digits.
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
//
// Parameters:
//   - token string: the raw previous token.
//
// Returns:
//   - string: the lowercase word, punctuation removed.
func normalizeIntroducer(token string) string {
	_, core, _ := splitPunctuation(token)

	return strings.ToLower(core)
}

// splitPunctuation separates a token into leading punctuation, core, and trailing
// punctuation.
//
// Square brackets are NOT treated as punctuation, because they are part of a bracketed
// IPv6 address and stripping them would turn an endpoint into something unrecognisable.
//
// Parameters:
//   - token string: the raw token.
//
// Returns:
//   - string: leading punctuation.
//   - string: the core.
//   - string: trailing punctuation.
func splitPunctuation(token string) (string, string, string) {
	const cutset = `,;:.!?"'()<>`

	core := strings.Trim(token, cutset)
	if core == "" {
		return "", "", ""
	}

	start := strings.Index(token, core)

	return token[:start], core, token[start+len(core):]
}

// clean removes the characters that let a value forge log structure.
//
// Parameters:
//   - value string: the raw text.
//
// Returns:
//   - string: the text with line breaks and tabs turned into spaces, other control
//     characters removed, and the result trimmed.
func clean(value string) string {
	cleaned := strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case r < 0x20 || r == 0x7f:
			return -1
		default:
			return r
		}
	}, value)

	return strings.TrimSpace(cleaned)
}

// bound caps a string at a rune count, marking the cut.
//
// Parameters:
//   - value string: the cleaned text.
//   - max int: the maximum number of runes to keep.
//
// Returns:
//   - string: the text, with TruncationSuffix appended if it was shortened.
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
