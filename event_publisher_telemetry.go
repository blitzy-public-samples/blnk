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

package blnk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/logsafe"
)

// This file holds the publisher's TELEMETRY VOCABULARY and its PROCESS-WIDE LIFECYCLE — the
// two concerns that are about a publish rather than part of performing one, which is why
// they sit beside event_publisher.go instead of inside it.
//
// The vocabulary half exists because two of the pipeline's instruments carry attributes
// whose domain has to stay CLOSED. blnk.events.publish.duration is a histogram, so its
// cardinality is multiplied by the bucket count and an unbounded attribute value there is
// the difference between a handful of series and an unbounded number of them. attemptLabel
// is the only producer of the "attempt" value and PublishPurpose is the only vocabulary the
// three publish paths are distinguished by; nothing else may mint either.
//
// The lifecycle half exists because ONE publisher per process is the goal. Two would double
// the broker connections and the SASL sessions while each looked correct on its own, and the
// dead-letter service, a replay handler and the relay all need one without any of them
// owning the others.

const (
	// publishAttemptLabelOverflow is the single bucket every attempt beyond the supported
	// budget collapses into. Reachable only for a row whose max_attempts was raised
	// directly in the database, since configuration is clamped; it makes such a row
	// visible without minting one label value per attempt number.
	publishAttemptLabelOverflow = "over"

	// publishAttemptLabelReplay marks an operator-triggered replay. Fixed rather than
	// numeric because a replay is not attempt N+1 of a live sequence — the sequence
	// already ended — and because a number would extend the domain past the budget.
	publishAttemptLabelReplay = "replay"

	// publishAttemptLabelDeadLetter marks a write to a `<topic>.dlt` sibling, which is not
	// part of any retry sequence.
	publishAttemptLabelDeadLetter = "dead_letter"
)

// logIdentifierHashLength is how many hex characters of the SHA-256 digest a hashed
// identifier keeps. Sixteen hex characters is 64 bits: ample to distinguish the ledgers a
// single operator is looking at without being reversible, and the full digest would only
// make the line longer.
//
// An alias of the canonical value rather than a second declaration: package api hashes
// identifiers for its request log too, and two lengths would make the same subject
// correlate under two different digests.
const logIdentifierHashLength = logsafe.IdentifierHashLength

// maxLazyTopicWriters bounds how many writers may be cached for topics that were not part
// of the publisher's constructed inventory.
//
// Blnk owns one topic and one dead-letter sibling per category, so ONE prefix generation is
// 2 * len(model.AllEventCategories()) writers. The bound is two generations' worth, which
// covers the case lazy growth exists for — a topic-prefix change, with stored rows still
// arriving for the previous name — and one further change on top of it, without a restart.
//
// The number is a literal because a const cannot call a function, which makes it exactly the
// kind of value that goes stale whenever a category is added or removed. It has gone stale in
// both directions already — too low while the catalogue briefly carried a fifth category, then
// too high once that category was withdrawn — and neither build nor run time noticed either
// time. TestMaxLazyTopicWriters_IsTwoPrefixGenerations ties it back to the inventory so the
// arithmetic is checked rather than trusted, which is what turns that silent staleness into a
// failing test: change the catalogue without changing this line and that test fails.
//
// Four categories → 8 writers per generation → 16 for two.
//
// Past the bound the oldest lazily-created writer is retired. Retirement is an eviction and
// never a refusal: a retired topic published to again simply gets a new writer, so bounding
// this cache costs at most one reconnection for a topic that has not been used recently, and
// prevents a pool of connections that only ever grows.
const maxLazyTopicWriters = 16

// PublishPurpose distinguishes the three reasons a message is written, so that one event's
// telemetry cannot be confused with another's.
//
// It exists because all three go through the same writer and would otherwise be
// indistinguishable in the metrics, with two concrete consequences:
//
//   - The dead-letter RATE divides dead-lettered events by the TOTAL TERMINAL OUTCOMES —
//     dead-lettered plus DISPATCHED — so both counters have to count the same population,
//     each event once. An operator replaying fifty dead-lettered events would otherwise add
//     fifty to the delivered side of that sum and make the rate depend on how much triage
//     happened that day. (The denominator is the sum rather than the dispatched count alone
//     because the two counters are DISJOINT: dividing by successes would report
//     dead-letters as a fraction of successes, which overstates the rate and diverges
//     without bound as failures rise. The delivered side is metrics.EventsDispatchedTotal
//     and NOT metrics.EventsPublishedTotal, which counts acknowledged writes rather than
//     events and therefore includes redeliveries — see PERF-P21. Purpose still governs the
//     write counter for the same reason it governs everything else here: a replay is not
//     original throughput.)
//   - The latency TARGET is stated for first-attempt original publishes. A replay is attempt
//     N+1 of an event that already failed five times, and a dead-letter write is not part of
//     any retry sequence at all; giving either of them a numeric attempt label would extend
//     the attempt domain past the configured budget, which is both a cardinality problem and
//     a lie about what the number means.
type PublishPurpose string

const (
	// PublishPurposeOriginal is a first delivery of an event to its category topic. An
	// UNSTATED purpose — the empty string a zero-valued PublishRequest carries — resolves
	// to it, which is the only thing the mandated envelope-only Publish method can mean.
	// It is the only purpose the relay publishes under, and therefore the only one that
	// can reach the published-events counter — which the RELAY increments, after the
	// transition that makes the delivery durable, rather than the publisher on the
	// broker's acknowledgement.
	//
	// It is spelled out rather than being the empty string because it is a LABEL VALUE and
	// a log field: "original" is greppable and self-describing on a dashboard, whereas an
	// empty attribute value is indistinguishable from a missing attribute.
	PublishPurposeOriginal PublishPurpose = "original"

	// PublishPurposeReplay is an operator-triggered re-publication of a dead-lettered event
	// to its original topic. It is recorded under a fixed attempt label and never counted as
	// a newly published event: the event is being delivered again, not for the first time.
	// It IS counted on the broker-acknowledgement counter, under this purpose — that counter
	// measures traffic the broker accepted, and a replay is real traffic.
	PublishPurposeReplay PublishPurpose = "replay"

	// PublishPurposeDeadLetter is a write to a `<topic>.dlt` sibling. It is recorded under
	// its own fixed attempt label, and its terminal accounting is the dead-lettered counter
	// rather than the published counter. Like a replay, it is counted on the
	// broker-acknowledgement counter under this purpose.
	PublishPurposeDeadLetter PublishPurpose = "dead_letter"
)

// resolvePurpose normalises the publish purpose, defaulting an unstated one to original.
//
// An UNSTATED purpose — the empty string — is the documented default and resolves SILENTLY.
// It is what the mandated envelope-only Publish method submits on every call, so warning
// about it would put a warning on the most ordinary path there is, and a warning that fires
// constantly on correct behaviour is worse than no warning: it teaches an operator to filter
// the level out, taking the real warnings with it.
//
// A NON-EMPTY unrecognised value is different. It is a caller that meant something specific
// and got the vocabulary wrong, which is worth exactly one line. It is still collapsed to
// original rather than passed through, because the purpose selects a metric attribute value
// and accepting an arbitrary string would reopen the very label domain PublishPurpose exists
// to close.
//
// Parameters:
//   - req PublishRequest: the request to resolve.
//
// Returns:
//   - PublishPurpose: one of the three declared values, never the empty string.
func resolvePurpose(req PublishRequest) PublishPurpose {
	switch req.Purpose {
	case PublishPurposeReplay:
		return PublishPurposeReplay
	case PublishPurposeDeadLetter:
		return PublishPurposeDeadLetter
	case PublishPurposeOriginal, "":
		// "" is the unstated default and is not a mistake, so it passes without comment.
		return PublishPurposeOriginal
	default:
		logrus.WithField("purpose", sanitizeLogValue(string(req.Purpose), maxLoggedFilterLength)).Warn(
			"an unrecognised publish purpose was submitted and is being recorded as an original " +
				"publish; the purpose selects a bounded metric attribute and cannot be extended ad hoc",
		)

		return PublishPurposeOriginal
	}
}

// resolveMaxAttempts normalises the stated retry budget.
//
// A value below 1 is not a budget of zero attempts, it is an unstated budget: the relay path
// always states one (from the row's max_attempts), and the envelope-only path has none to
// state. Both collapse to 0, which fail and attemptBudgetSpent read as "unknown".
//
// Parameters:
//   - req PublishRequest: the request to resolve.
//
// Returns:
//   - int: the budget, or 0 when none was stated.
func resolveMaxAttempts(req PublishRequest) int {
	if req.MaxAttempts < 1 {
		return 0
	}

	return req.MaxAttempts
}

// attemptBudgetSpent reports whether an attempt was the last one a budget allowed.
//
// A budget of zero or less means none was stated, which is NOT the same as a budget of zero
// attempts: the caller simply did not say, so nothing about the attempt count can rule out a
// further attempt and this returns false.
//
// Parameters:
//   - attempt int: the 1-based attempt number, already normalised.
//   - maxAttempts int: the stated budget, or zero when unstated.
//
// Returns:
//   - bool: true when a budget was stated and this attempt reached or passed it.
func attemptBudgetSpent(attempt, maxAttempts int) bool {
	return maxAttempts > 0 && attempt >= maxAttempts
}

// attemptLabel renders the attempt attribute, and it is the ONLY place that attribute's
// value is produced.
//
// The domain it returns is closed and exactly eight values: "1" through "5", "over",
// "replay" and "dead_letter". That is a hard requirement rather than a preference, because
// this attribute sits on a HISTOGRAM: its cardinality is multiplied by the bucket count, so
// an unbounded value here is the difference between a handful of series and an unbounded
// number of them.
//
// Three inputs could otherwise widen it, and each is closed off here:
//
//   - A configured budget above the ceiling. config.setRelayDefaults already clamps
//     RELAY_MAX_RETRY_ATTEMPTS, so this is the second line of defence.
//   - A row whose max_attempts was raised directly in the database, bypassing configuration
//     entirely. Such an attempt collapses into "over" — visible as abnormal, but a single
//     label rather than a new one per value.
//   - A replay or a dead-letter write. Neither is part of a retry sequence, so neither gets a
//     number: labelling a replay "attempt 6" would extend the numeric domain past the budget
//     and would also misdescribe it, since the sequence it belongs to ended. Their fixed
//     tokens are what keep them out of the attempt="1" population the latency target is read
//     from.
//
// Parameters:
//   - purpose PublishPurpose: the resolved purpose.
//   - attempt int: the resolved, 1-based attempt number.
//
// Returns:
//   - string: one of the eight declared label values.
func attemptLabel(purpose PublishPurpose, attempt int) string {
	switch purpose {
	case PublishPurposeReplay:
		return publishAttemptLabelReplay
	case PublishPurposeDeadLetter:
		return publishAttemptLabelDeadLetter
	}

	if attempt < 1 {
		attempt = 1
	}

	if attempt > config.MaxRelayRetryAttempts {
		return publishAttemptLabelOverflow
	}

	return strconv.Itoa(attempt)
}

// hashLogIdentifier turns a financial identifier into a stable, non-reversible token
// suitable for a log field.
//
// The point is to keep the one property a log needs — that the same identifier always
// produces the same token, so two lines can be recognised as belonging to the same ledger or
// partition — while giving up the one it does not need, the identifier itself. SHA-256
// truncated to logIdentifierHashLength hex characters does that.
//
// An empty input returns an empty string rather than the digest of the empty string, because
// "no key" and "some key" must stay distinguishable: a keyless message was balanced across
// partitions rather than pinned to one, which is exactly what an operator investigating an
// ordering complaint needs to see.
//
// Parameters:
//   - value string: the identifier. May be empty.
//
// The implementation lives in internal/logsafe so that package api hashes identifiers by
// exactly the same rule; this remains the name package blnk calls.
//
// Returns:
//   - string: a short hex token, or "" for an empty input.
func hashLogIdentifier(value string) string {
	return model.HashIdentifier(value)
}

// HashLogIdentifier is the exported form of hashLogIdentifier, so that every layer produces
// the SAME token for the same identifier.
//
// It exists because the pseudonym has to be a PIVOT rather than a per-package convention. An
// operator reads subscriber_id_hash in a log line or on a metric series and needs to resolve
// it to a subscriber; the API layer therefore has to publish the same token on the subscriber
// resource, and the API layer is a different package. A second implementation there — even a
// correct one — would be a second thing to keep in step, and the failure mode of drift is
// silent: two tokens for one subscriber, and a pivot that returns nothing.
//
// The rule is deliberately simple enough to reproduce outside Go, which is the other half of
// making the pseudonym usable: SHA-256 of the exact identifier bytes, hex-encoded, truncated
// to the first 16 characters. docs/kafka-operations.md publishes the shell equivalent so an
// operator can hash a candidate identifier without this binary.
//
// Parameters:
//   - value string: the identifier. May be empty.
//
// Returns:
//   - string: a short hex token, or "" for an empty input.
func HashLogIdentifier(value string) string {
	return hashLogIdentifier(value)
}

// The bounded classes a Kafka failure is reported as in a LOG LINE.
//
// Separate from the adminSpanError* vocabulary in event_tracing.go, and deliberately so: that
// set describes an administrative operation, while a log line is also emitted for a produce, a
// metadata read and an offset read, and it has to distinguish the two conditions that are
// nobody's fault — no broker configured, and a caller that gave up — from the ones that are.
//
// Every member is a fixed literal and the set is closed, which is what makes the value safe as
// a log field. A kafka-go error is not: it renders with broker hostnames, listener addresses,
// the topic and partition it was acting on and, for a credential operation, the principal.
const (
	kafkaErrorClassNone          = "none"
	kafkaErrorClassNotConfigured = "not_configured"
	kafkaErrorClassCancelled     = "context_cancelled"
	kafkaErrorClassDeadline      = "context_deadline_exceeded"
	kafkaErrorClassPublisher     = "publisher_state"
	kafkaErrorClassTopicRefused  = "topic_not_owned"
	kafkaErrorClassMessageTooBig = "message_too_large"
	kafkaErrorClassAuthorizer    = "authorizer_not_enforcing"
	kafkaErrorClassGeometry      = "topic_geometry_refused"
	kafkaErrorClassBroker        = "broker_error"
)

// KafkaErrorClass maps a Kafka failure to one of the bounded classes above.
//
// The two context conditions are tested FIRST because they are reachable through every other
// condition: a produce that was cancelled mid-flight surfaces as a wrapped context error on
// one path and as a broker write error on another, and reporting one operator-visible event
// under two classes depending on where it landed makes the series useless for alerting.
//
// not_configured is separated from every failure class because IT IS NOT A FAILURE. A
// deployment with no brokers is a supported steady state — the publisher degrades to the no-op
// and the ledger serves traffic exactly as before — so a caller that receives it must be able
// to decide not to warn. See GetEventOutboxStats, which is the endpoint that was warning on
// every request in precisely that configuration.
//
// Parameters:
//   - cause error: the Kafka failure. May be nil.
//
// Returns:
//   - string: one of the kafkaErrorClass* constants. Never empty.
func KafkaErrorClass(cause error) string {
	switch {
	case cause == nil:
		return kafkaErrorClassNone
	case errors.Is(cause, context.Canceled):
		return kafkaErrorClassCancelled
	case errors.Is(cause, context.DeadlineExceeded):
		return kafkaErrorClassDeadline
	case errors.Is(cause, ErrKafkaAdminNotConfigured):
		return kafkaErrorClassNotConfigured
	case errors.Is(cause, ErrEventPublisherClosed), errors.Is(cause, ErrKafkaProducerCredentialsRequired):
		return kafkaErrorClassPublisher
	case errors.Is(cause, ErrTopicNotOwned):
		return kafkaErrorClassTopicRefused
	case errors.Is(cause, ErrEventMessageTooLarge):
		return kafkaErrorClassMessageTooBig
	case errors.Is(cause, ErrAuthorizerNotEnforcing):
		return kafkaErrorClassAuthorizer
	case errors.Is(cause, ErrPartitionGrowthRefused), errors.Is(cause, ErrReplicationFactorInadequate):
		return kafkaErrorClassGeometry
	default:
		return kafkaErrorClassBroker
	}
}

// LogKafkaDiagnostic sends a Kafka client's own error text to the trace-level diagnostic sink.
//
// It is the counterpart of the database package's logDatabaseDiagnostic and exists for the same
// reason: the raw text is genuinely useful when a broker is misbehaving, and it names brokers,
// listeners, topics and principals, so it must not be in the log by default. TRACE is the sink
// because DEBUG is already this pipeline's routine per-event volume — see the LogLevel field in
// config — so an operator who raises the level to debug to follow a delivery must not thereby
// start shipping cluster topology.
//
// The level is checked before the error is formatted rather than left to logrus, because
// WithError formats eagerly and this is on failure paths that a broker outage makes hot.
//
// Parameters:
//   - operation string: a FIXED literal naming what was attempted, for correlation with the
//     bounded line that precedes it.
//   - cause error: the raw failure. A nil cause is a no-op.
func LogKafkaDiagnostic(operation string, cause error) {
	if cause == nil || !logrus.IsLevelEnabled(logrus.TraceLevel) {
		return
	}

	// THE RAW CAUSE IS ATTACHED VERBATIM HERE, and only here. This sink exists precisely so
	// the driver's or client's own text is reachable when an operator asks for it explicitly,
	// which is why it is gated on the trace level and why it does NOT go through the
	// redacting helper every other site uses — redacting the one place the full text is
	// supposed to be available would leave it available nowhere.
	logrus.WithField("operation", operation).WithField(logrus.ErrorKey, cause).Trace(
		"kafka diagnostic: the client's own error text, which may name brokers, listeners, " +
			"topics and principals and is therefore emitted at trace only",
	)
}

// withKafkaError attaches the BOUNDED CLASS of a Kafka failure to a log entry and routes the
// raw cause to the trace-level diagnostic sink.
//
// It exists so a call site can be converted by wrapping one expression rather than by
// restructuring the statement, which is what keeps twenty conversions reviewable. The
// diagnostic is emitted here rather than left to each caller for the same reason: a caller that
// forgets it loses the raw text entirely, and the failure is silent.
//
// # THE ONE EXCEPTION, AND WHY IT IS NOT THIS
//
// PublishResult.LogFields keeps a rendered "error" field, and deliberately: AAP requirement R-4
// mandates that "the attempt count and error reason must be logged on every attempt, not only
// on final failure", and §0.1.3 names the field list — attempt, max attempts, error, event ID,
// topic. That is an explicit AAP requirement, so it outranks the general rule this helper
// enforces; the value is length-bounded by sanitizeLogValue instead. The same applies to the
// error reason stored in failure_metadata and last_error, which R-5 requires as DATA rather
// than as a log field. Every OTHER Kafka failure in this package goes through here.
//
// Parameters:
//   - entry *logrus.Entry: the entry to extend. Must not be nil.
//   - operation string: a FIXED literal naming what was attempted.
//   - cause error: the failure. A nil cause still yields the "none" class, so a shared line
//     keeps a stable field set.
//
// Returns:
//   - *logrus.Entry: the entry carrying error_class, ready for a level call.
func withKafkaError(entry *logrus.Entry, operation string, cause error) *logrus.Entry {
	LogKafkaDiagnostic(operation, cause)

	return entry.WithField("error_class", KafkaErrorClass(cause))
}

// kafkaErrorClassField is withKafkaError for a site that builds a logrus.Fields map rather than
// chaining onto an entry.
//
// It routes the raw cause to the trace-level sink and returns the class, so converting a field
// is a value substitution inside the map literal and the statement around it is untouched.
//
// Parameters:
//   - operation string: a FIXED literal naming what was attempted.
//   - cause error: the failure. May be nil.
//
// Returns:
//   - string: a bounded class, suitable as an "error_class" field value.
func kafkaErrorClassField(operation string, cause error) string {
	LogKafkaDiagnostic(operation, cause)

	return KafkaErrorClass(cause)
}

// kafkaErrorEntry starts a log entry carrying only the bounded class of a Kafka failure.
//
// The convenience form of withKafkaError, for a site that previously read
// logrus.WithField("error", ...) and has no other fields to carry.
//
// Parameters:
//   - operation string: a FIXED literal naming what was attempted.
//   - cause error: the failure. May be nil.
//
// Returns:
//   - *logrus.Entry: ready for a level call.
func kafkaErrorEntry(operation string, cause error) *logrus.Entry {
	return withKafkaError(logrus.NewEntry(logrus.StandardLogger()), operation, cause)
}

// publisherAuthMode names the authentication the publisher will use, without disclosing
// anything an attacker could use.
//
// It returns the MECHANISM only. The administrative username is not included even though it
// is not a secret: paired with the SCRAM mechanism it is half of a credential and names a
// valid principal on the cluster, which is a starting point for a brute-force attempt that a
// mode name is not.
//
// Parameters:
//   - cfg config.KafkaConfig: the Kafka configuration block.
//
// Returns:
//   - string: "scram-sha-512" or "none".
func publisherAuthMode(cfg config.KafkaConfig) string {
	// The PRODUCER pair first, because that is the one the publisher prefers, and reading
	// only the administrative pair reported "none" for the recommended configuration — a
	// deployment with a dedicated producer principal and no admin credentials in the
	// publishing process, which is exactly the least-privilege arrangement the split
	// exists to encourage.
	//
	// Both members are required, matching KafkaConfig.SASLAdminCredentials' reading and
	// the transport's own: a username with no secret cannot complete a SCRAM exchange, so
	// reporting "scram-sha-512" for it would state an authentication that will not
	// happen. Trimming matters for the same reason it does there — a value that is only
	// whitespace is absence.
	if strings.TrimSpace(cfg.SASLUser) != "" && strings.TrimSpace(cfg.SASLSecret) != "" {
		return "scram-sha-512"
	}

	if _, _, enabled := cfg.SASLAdminCredentials(); enabled {
		return "scram-sha-512"
	}

	return "none"
}

// sharedEventPublisher is the PROCESS-OWNED publisher: one transport, one SASL session and
// one writer set, resolved lazily so a process that never publishes never opens anything and
// a process whose configuration has not been loaded yet is not forced to resolve one early.
var sharedEventPublisher struct {
	// mu guards all three fields below. It is a plain Mutex rather than an RWMutex because
	// the read path may have to replace the publisher, so it is never purely a read.
	mu sync.Mutex

	// publisher is the shared instance, nil until first use.
	//
	// It is held as TopicEventPublisher rather than EventPublisher because a shared instance
	// must be closeable at shutdown, and Close is part of the fuller contract.
	publisher TopicEventPublisher

	// fingerprint identifies the configuration publisher was built from, so a configuration
	// change — most importantly a rotated SASL credential — is detected and the stale
	// publisher replaced rather than used indefinitely.
	fingerprint string

	// selfBuilt distinguishes a publisher this package built from one an owner injected, and
	// it is what makes closing safe: an injected publisher belongs to its injector.
	selfBuilt bool
}

// eventPublisherFingerprint digests the configuration a publisher would be built from.
//
// # Every field that changes the TRANSPORT, and why omitting one is a security defect
//
// The fingerprint is the cache key: an unchanged digest means the cached publisher — with its
// existing connections, its existing SASL session and its existing TLS configuration — keeps
// being handed out. So a transport-affecting field left out of the digest is not merely an
// optimisation gone wrong; it is a configuration change that NEVER TAKES EFFECT, silently, for
// the lifetime of the process.
//
// Three of the groups below were missing, and each has a concrete cost:
//
//   - THE PRODUCER PAIR (SASLUser / SASLSecret) is the credential the publisher PREFERS — see
//     kafkaTransportCredentials, which reads it before the administrative pair. Only the
//     administrative pair was digested, so rotating the producer secret left every publish
//     authenticating with the old one until a restart. The recommended least-privilege
//     deployment — a producer principal and no administrative credentials in the publishing
//     process — was the one where rotation was completely inert.
//   - THE TLS BLOCK. Enabling TLS, changing the CA bundle, adding a client certificate for
//     mutual TLS, or correcting a server name all rebuild the transport. Without them in the
//     digest, a deployment that switched from SASL_PLAINTEXT to SASL_SSL kept publishing IN
//     THE CLEAR, which is the failure mode a reader would least expect to be silent.
//   - InsecureSkipVerify and InsecureLocalDev, both of which decide whether the transport
//     will dial at all and under what verification. Turning verification back ON must take
//     effect; leaving it out means it does not.
//
// What is deliberately NOT here: MinPartitions, ReplicationFactor and AllowPartitionGrowth.
// Those are TOPIC GEOMETRY, consumed by the admin client's topic assurance rather than by the
// writer, so a change to them must not throw away a working transport and its connections.
//
// A separator byte between fields prevents two different configurations from colliding by
// shifting characters across a boundary, and booleans are written as distinct bytes so that
// false is a value rather than an absence.
//
// Parameters:
//   - cnf *config.Configuration: the configuration snapshot. May be nil.
//
// Returns:
//   - string: a hex digest, or "nil" when there is no configuration.
func eventPublisherFingerprint(cnf *config.Configuration) string {
	if cnf == nil {
		return "nil"
	}

	digest := sha256.New()
	for _, broker := range normalizeBrokers(cnf.Kafka.Brokers) {
		digest.Write([]byte(broker))
		digest.Write([]byte{0})
	}

	// Written through one helper so that no field can be added without its separator.
	writeField := func(value string) {
		digest.Write([]byte{1})
		digest.Write([]byte(value))
	}
	writeBool := func(value bool) {
		digest.Write([]byte{1})
		if value {
			digest.Write([]byte{'t'})

			return
		}
		digest.Write([]byte{'f'})
	}

	writeField(cnf.Kafka.TopicPrefix)
	writeField(cnf.Kafka.SASLAdminUser)
	writeField(cnf.Kafka.SASLAdminSecret)
	writeField(cnf.Kafka.SASLUser)
	writeField(cnf.Kafka.SASLSecret)
	writeBool(cnf.Kafka.TLS.Enabled)
	writeField(cnf.Kafka.TLS.CAFile)
	writeField(cnf.Kafka.TLS.CertFile)
	writeField(cnf.Kafka.TLS.KeyFile)
	writeField(cnf.Kafka.TLS.ServerName)
	writeBool(cnf.Kafka.TLS.InsecureSkipVerify)
	writeBool(cnf.Kafka.InsecureLocalDev)

	return hex.EncodeToString(digest.Sum(nil))
}

// SharedEventPublisher returns the process-owned publisher, building it at most once per
// configuration.
//
// Use it wherever a publisher is needed but none is held: a request handler, a dead-letter
// replay, a maintenance task. What it prevents is a second transport — every extra publisher
// is another connection pool and another SASL session against the same brokers.
//
// Returns:
//   - TopicEventPublisher: the shared publisher. Never nil when the error is nil, and the
//     no-op implementation when no brokers are configured.
//   - error: what NewEventPublisher can return, or ErrKafkaUnavailable if a publisher
//     implementation cannot target a topic.
func SharedEventPublisher() (TopicEventPublisher, error) {
	// fetchConfiguration is the package's configuration seam (declared in event_sunset.go)
	// rather than config.Fetch, so a test swapping it sees consistent behaviour across every
	// event file.
	cnf, err := fetchConfiguration()
	if err != nil {
		cnf = nil
	}

	fingerprint := eventPublisherFingerprint(cnf)

	sharedEventPublisher.mu.Lock()
	defer sharedEventPublisher.mu.Unlock()

	if sharedEventPublisher.publisher != nil {
		if !sharedEventPublisher.selfBuilt || sharedEventPublisher.fingerprint == fingerprint {
			return sharedEventPublisher.publisher, nil
		}

		// The configuration changed under a publisher this package built. Close it before
		// dropping the reference; a superseded publisher still holds broker connections, and
		// losing the handle would leak them for the life of the process.
		superseded := sharedEventPublisher.publisher
		sharedEventPublisher.publisher = nil
		if closeErr := superseded.Close(); closeErr != nil {
			withLoggableCause(nil, closeErr).Warn(
				"failed to close the superseded shared event publisher after a configuration change",
			)
		}
	}

	built, err := NewEventPublisher(cnf)
	if err != nil {
		return nil, err
	}

	publisher, ok := built.(TopicEventPublisher)
	if !ok {
		// Unreachable with the implementations in this package, both of which satisfy the
		// fuller contract. Asserted rather than assumed so that a future implementation
		// which does not cannot fail obscurely at a write or a shutdown.
		return nil, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"The configured event publisher cannot target a topic",
			fmt.Errorf("blnk: %T does not implement TopicEventPublisher", built),
		)
	}

	sharedEventPublisher.publisher = publisher
	sharedEventPublisher.fingerprint = fingerprint
	sharedEventPublisher.selfBuilt = true

	return publisher, nil
}

// SetSharedEventPublisher installs a publisher the caller already owns as the shared one.
//
// It exists so that a process which builds a publisher for its own use — the relay in the
// server role — can donate that instance instead of leaving this package to build a second
// one. One publisher per process is the goal; two would double the connections and the SASL
// sessions while each looked correct on its own.
//
// The installed publisher is NOT owned by this package: CloseSharedEventPublisher will not
// close it, and a later configuration change will not replace it. A previously self-built
// publisher is closed as it is displaced, because nothing else holds a reference to it.
//
// Parameters:
//   - publisher TopicEventPublisher: the publisher to share. Nil clears the slot.
func SetSharedEventPublisher(publisher TopicEventPublisher) {
	sharedEventPublisher.mu.Lock()
	defer sharedEventPublisher.mu.Unlock()

	if sharedEventPublisher.selfBuilt && sharedEventPublisher.publisher != nil {
		displaced := sharedEventPublisher.publisher
		if closeErr := displaced.Close(); closeErr != nil {
			withLoggableCause(nil, closeErr).Warn(
				"failed to close the self-built shared event publisher as it was displaced",
			)
		}
	}

	sharedEventPublisher.publisher = publisher
	sharedEventPublisher.selfBuilt = false
	sharedEventPublisher.fingerprint = ""
}

// CloseSharedEventPublisher releases the shared publisher at process shutdown.
//
// It closes ONLY a publisher this package built, following the same ownership rule as
// everywhere else in this feature: an injected publisher belongs to its injector, and closing
// it here would tear down a transport its owner is still using. The slot is cleared either
// way, so a subsequent SharedEventPublisher call builds afresh.
//
// It is idempotent and safe to call when nothing was ever built.
//
// Returns:
//   - error: the publisher's close error, or nil when there was nothing this package owned
//     to close.
func CloseSharedEventPublisher() error {
	sharedEventPublisher.mu.Lock()
	publisher := sharedEventPublisher.publisher
	selfBuilt := sharedEventPublisher.selfBuilt
	sharedEventPublisher.publisher = nil
	sharedEventPublisher.selfBuilt = false
	sharedEventPublisher.fingerprint = ""
	sharedEventPublisher.mu.Unlock()

	if publisher == nil || !selfBuilt {
		return nil
	}

	return publisher.Close()
}
