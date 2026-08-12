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

// This file holds the publisher's TELEMETRY VOCABULARY and its PROCESS-WIDE LIFECYCLE —
// the two concerns that are about a publish rather than part of performing one, which
// is why they sit beside event_publisher.go instead of inside it.

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
// identifier keeps. Sixteen hex characters is 64 bits: ample to distinguish the ledgers
// a single operator is looking at without being reversible, and the full digest would
// only make the line longer.
const logIdentifierHashLength = logsafe.IdentifierHashLength

// maxLazyTopicWriters bounds how many writers may be cached for topics that were not
// part of the publisher's constructed inventory.
const maxLazyTopicWriters = 16

// PublishPurpose distinguishes the three reasons a message is written, so that one
// event's telemetry cannot be confused with another's.
type PublishPurpose string

const (
	// PublishPurposeOriginal is a first delivery of an event to its category topic. An
	// UNSTATED purpose — the empty string a zero-valued PublishRequest carries — resolves
	// to it, which is the only thing the mandated envelope-only Publish method can mean.
	PublishPurposeOriginal PublishPurpose = "original"

	// PublishPurposeReplay is an operator-triggered re-publication of a dead-lettered
	// event to its original topic. It is recorded under a fixed attempt label and never
	// counted as a newly published event: the event is being delivered again, not for the
	// first time. It IS counted on the broker-acknowledgement counter, under this purpose
	// — that counter measures traffic the broker accepted, and a replay is real traffic.
	PublishPurposeReplay PublishPurpose = "replay"

	// PublishPurposeDeadLetter is a write to a `<topic>.dlt` sibling. It is recorded under
	// its own fixed attempt label, and its terminal accounting is the dead-lettered
	// counter rather than the published counter. Like a replay, it is counted on the
	// broker-acknowledgement counter under this purpose.
	PublishPurposeDeadLetter PublishPurpose = "dead_letter"
)

// resolvePurpose normalises the publish purpose, defaulting an unstated one to
// original.
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
func resolveMaxAttempts(req PublishRequest) int {
	if req.MaxAttempts < 1 {
		return 0
	}

	return req.MaxAttempts
}

// attemptBudgetSpent reports whether an attempt was the last one a budget allowed.
func attemptBudgetSpent(attempt, maxAttempts int) bool {
	return maxAttempts > 0 && attempt >= maxAttempts
}

// attemptLabel renders the attempt attribute, and it is the ONLY place that attribute's
// value is produced.
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
func hashLogIdentifier(value string) string {
	return model.HashIdentifier(value)
}

// HashLogIdentifier is the exported form of hashLogIdentifier, so that every layer
// produces the SAME token for the same identifier.
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

// LogKafkaDiagnostic sends a Kafka client's own error text to the trace-level
// diagnostic sink.
//
// Parameters:
//   - operation string: a FIXED literal naming what was attempted, for correlation with
//     the bounded line that precedes it.
//   - cause error: the raw failure. A nil cause is a no-op.
func LogKafkaDiagnostic(operation string, cause error) {
	if cause == nil || !logrus.IsLevelEnabled(logrus.TraceLevel) {
		return
	}

	// THE RAW CAUSE IS ATTACHED VERBATIM HERE, and only here. This sink exists precisely
	// so the driver's or client's own text is reachable when an operator asks for it
	// explicitly, which is why it is gated on the trace level and why it does NOT go
	// through the redacting helper every other site uses — redacting the one place the
	// full text is supposed to be available would leave it available nowhere.
	logrus.WithField("operation", operation).WithField(logrus.ErrorKey, cause).Trace(
		"kafka diagnostic: the client's own error text, which may name brokers, listeners, " +
			"topics and principals and is therefore emitted at trace only",
	)
}

// withKafkaError attaches the BOUNDED CLASS of a Kafka failure to a log entry and
// routes the raw cause to the trace-level diagnostic sink.
func withKafkaError(entry *logrus.Entry, operation string, cause error) *logrus.Entry {
	LogKafkaDiagnostic(operation, cause)

	return entry.WithField("error_class", KafkaErrorClass(cause))
}

// kafkaErrorClassField is withKafkaError for a site that builds a logrus.Fields map
// rather than chaining onto an entry.
func kafkaErrorClassField(operation string, cause error) string {
	LogKafkaDiagnostic(operation, cause)

	return KafkaErrorClass(cause)
}

// kafkaErrorEntry starts a log entry carrying only the bounded class of a Kafka
// failure.
func kafkaErrorEntry(operation string, cause error) *logrus.Entry {
	return withKafkaError(logrus.NewEntry(logrus.StandardLogger()), operation, cause)
}

// publisherAuthMode names the authentication the publisher will use, without disclosing
// anything an attacker could use.
func publisherAuthMode(cfg config.KafkaConfig) string {
	// The PRODUCER pair first, because that is the one the publisher prefers, and reading
	// only the administrative pair reported "none" for the recommended configuration — a
	// deployment with a dedicated producer principal and no admin credentials in the
	// publishing process, which is exactly the least-privilege arrangement the split
	// exists to encourage.
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

// SharedEventPublisher returns the process-owned publisher, building it at most once
// per configuration.
//
// Returns:
//   - TopicEventPublisher: the shared publisher. Never nil when the error is nil, and
//     the no-op implementation when no brokers are configured.
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

// SetSharedEventPublisher installs a publisher the caller already owns as the shared
// one.
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
// Returns:
//   - error: the publisher's close error, or nil when there was nothing this package
//     owned to close.
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
