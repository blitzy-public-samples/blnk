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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
)

// kafkaPublisher is the Kafka-backed publisher. It holds ONE writer per topic and one
// shared transport, all constructed once and reused for the process lifetime —
// mirroring how a single HTTP client is constructed once and shared for the legacy
// webhook transport rather than rebuilt per request.
type kafkaPublisher struct {
	// brokers is the normalised bootstrap list, retained for logging and for building
	// writers lazily.
	brokers []string

	// addr is the pre-built broker address shared by every writer. kafka.TCP performs
	// no name resolution, so building it is pure string work.
	addr net.Addr

	// transport is shared by every writer so that all of them draw on ONE connection pool
	// and ONE authentication and TLS configuration; kafka.Transport establishes
	// connections lazily per broker and reuses them, authenticating each as it is opened.
	transport *kafka.Transport

	// mu guards writers and closed. *kafka.Writer is itself safe for concurrent use;
	// what needs guarding is the map that hands them out and the shutdown flag.
	mu sync.RWMutex

	// writers is keyed by topic. It is pre-populated with every topic Blnk owns and
	// grows lazily — see writerFor for the two real cases that require growth.
	writers map[string]*kafka.Writer

	// lazyTopics is the topics added AFTER construction, in creation order.
	lazyTopics []string

	// closed makes Close idempotent and turns a publish after shutdown into a clear
	// error rather than a panic on a closed writer.
	closed bool
}

// Compile-time proof that both implementations satisfy both contracts. These
// assertions are the cheapest possible guard against the interface and its
// implementations drifting apart, and they fail the build rather than a test.
var (
	_ EventPublisher      = (*kafkaPublisher)(nil)
	_ EventPublisher      = (*NoopEventPublisher)(nil)
	_ TopicEventPublisher = (*kafkaPublisher)(nil)
	_ TopicEventPublisher = (*NoopEventPublisher)(nil)

	// The MANDATED SIGNATURE itself, pinned at compile time. "We documented it" is not a
	// guarantee: assigning each implementation's method to a variable of the exact
	// function type means any drift — an added parameter, a tuple return, a renamed method
	// — stops the build here, on the lines that state the contract, rather than surfacing
	// as a puzzling failure somewhere downstream. The assignment is the whole point, so
	// the values are discarded.
	_ func(context.Context, model.LedgerEvent) error = (&NoopEventPublisher{}).Publish
	_ func(context.Context, model.LedgerEvent) error = (&kafkaPublisher{}).Publish
)

// NewEventPublisher builds the event publisher from configuration.
//
// Parameters:
//   - cnf *config.Configuration: the loaded configuration. May be nil.
//
// Returns:
//   - EventPublisher: the Kafka publisher when brokers are configured, otherwise the
//     no-op.
//   - error: non-nil when the transport cannot be assembled from the configuration —
//     administrative or producer SASL credentials, TLS material, or the plaintext
//     policy.
func NewEventPublisher(cnf *config.Configuration) (EventPublisher, error) {
	if cnf == nil {
		logrus.Debug(
			"no configuration available for the event publisher; " +
				"selecting the no-op publisher so no ledger event is published",
		)

		return NewNoopEventPublisher(), nil
	}

	brokers := normalizeBrokers(cnf.Kafka.Brokers)
	if len(brokers) == 0 {
		logrus.Debug(
			"KAFKA_BROKERS is not configured; selecting the no-op event publisher. " +
				"Ledger events are not published to Kafka and no error is raised",
		)

		return NewNoopEventPublisher(), nil
	}

	publisher, err := newKafkaPublisher(brokers, cnf.Kafka)
	if err != nil {
		return nil, err
	}

	// The principal and the encryption state are the two facts an operator needs to
	// confirm the producer connected as intended, so both are named explicitly rather than
	// inferred from which variables happen to be set.
	producerUser, _, credErr := kafkaTransportCredentials(cnf.Kafka, KafkaTransportRoleProducer)
	if credErr != nil {
		// Unreachable: newKafkaPublisher resolved the same pair a moment ago and would have
		// returned the error. Logged rather than ignored so a future divergence between the
		// two calls is visible instead of silent.
		withLoggableCause(nil, credErr).Warn(
			"could not re-resolve the producer SASL principal for the initialisation log",
		)
	}

	// broker_count and auth_mode rather than the endpoint list and the principal. The
	// address list is topology: it tells a reader of the log where to aim, and it tells an
	// operator nothing they cannot get from their own configuration. The principal is the
	// other half of a SCRAM credential whose mechanism this same line publishes, so naming
	// it at info narrows a guess to one unknown.
	logrus.WithFields(logrus.Fields{
		"broker_count":    len(brokers),
		"topics":          len(publisher.writers),
		"sasl":            producerUser != "",
		"auth_mode":       publisherAuthMode(cnf.Kafka),
		"dedicated_sasl":  cnf.Kafka.SASLUser != "",
		"tls":             cnf.Kafka.TLS.Enabled,
		"required_acks":   "all",
		"balancer":        "murmur2",
		"topic_prefix":    TopicPrefix(),
		"internal_retry":  false,
		"max_event_bytes": model.MaxEventMessageBytes,
	}).Info("kafka event publisher initialised")

	// The identity-confirmation detail, at the level an operator turns on when they are
	// confirming exactly this. sanitizeLogValue because both values come from
	// configuration and neither their length nor their content is this codebase's to
	// assume.
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logrus.WithFields(logrus.Fields{
			"brokers":        sanitizeLogValue(strings.Join(brokers, ","), maxLoggedErrorLength),
			"sasl_principal": sanitizeLogValue(producerUser, maxLoggedFilterLength),
		}).Debug("kafka event publisher endpoints and principal")
	}

	return publisher, nil
}

// newKafkaPublisher assembles the transport and the per-topic writers.
func newKafkaPublisher(brokers []string, cfg config.KafkaConfig) (*kafkaPublisher, error) {
	if err := cfg.ValidateSASLAdminCredentials(); err != nil {
		return nil, fmt.Errorf("blnk: cannot build the Kafka event publisher: %w", err)
	}

	addr := kafka.TCP(brokers...)

	transport, err := NewKafkaTransport(cfg, KafkaTransportRoleProducer)
	if err != nil {
		return nil, err
	}

	publisher := &kafkaPublisher{
		brokers:   brokers,
		addr:      addr,
		transport: transport,
		writers:   make(map[string]*kafka.Writer),
	}

	// Pre-create a writer for every topic Blnk owns, taken from the one inventory the
	// admin path and the provisioning script also work from. Pre-creating costs nothing —
	// a writer performs no I/O until its first write — and it means the steady-state
	// publish path never takes the write lock.
	for _, topic := range AllOwnedTopicsAcrossPrefixes() {
		publisher.writers[topic] = publisher.newWriter(topic)
	}

	return publisher, nil
}

// KafkaTransportRole names which principal a transport authenticates as.
type KafkaTransportRole string

const (
	// KafkaTransportRoleProducer is the steady-state event-publishing principal. It needs
	// Write and Describe on the Blnk-owned topics and NOTHING ELSE — no topic creation, no
	// credential alteration, no ACL management.
	KafkaTransportRoleProducer KafkaTransportRole = "producer"

	// KafkaTransportRoleAdmin is the provisioning principal.
	KafkaTransportRoleAdmin KafkaTransportRole = "admin"
)

// NewKafkaTransport builds the shared kafka.Transport for one role, applying the TLS
// and credential policy.
//
// Parameters:
//   - cfg config.KafkaConfig: the Kafka block, read for TLS material and both
//     credential pairs.
//   - role KafkaTransportRole: which principal to authenticate as.
//
// Returns:
//   - *kafka.Transport: the assembled transport.
//   - error: a malformed or half-configured credential pair, unreadable or invalid TLS
//     material, or plaintext without the explicit local-dev acknowledgement.
func NewKafkaTransport(cfg config.KafkaConfig, role KafkaTransportRole) (*kafka.Transport, error) {
	transport := &kafka.Transport{
		DialTimeout: eventTransportDialTimeout,
		IdleTimeout: eventTransportIdleTimeout,
		ClientID:    eventTransportClientID,
	}

	// Both halves are resolved before either is judged, and their errors are JOINED.
	tlsConfig, tlsErr := kafkaTLSConfig(cfg)
	user, secret, credErr := kafkaTransportCredentials(cfg, role)
	if err := errors.Join(tlsErr, credErr); err != nil {
		return nil, err
	}

	transport.TLS = tlsConfig

	if user != "" {
		mechanism, mechErr := scram.Mechanism(scram.SHA512, user, secret)
		if mechErr != nil {
			return nil, saslCredentialError(role, user)
		}

		transport.SASL = mechanism
	} else if tlsConfig == nil {
		// Neither authenticated nor encrypted. That is only ever acceptable on a local
		// broker, and only when the operator has said so.
		logrus.WithField("role", string(role)).Warn(
			"the Kafka transport is neither authenticated nor encrypted; " +
				"this is only supported under KAFKA_INSECURE_LOCAL_DEV",
		)
	}

	return transport, nil
}

// requireLocalBrokersForPlaintext refuses plaintext to any broker that is not local.
func requireLocalBrokersForPlaintext(brokers []string) error {
	var remote []string

	for _, broker := range brokers {
		candidate := strings.TrimSpace(broker)
		if candidate == "" {
			continue
		}

		// SplitHostPort fails on a bare host, which is a legitimate way to write a
		// broker, so fall back to the whole value rather than rejecting it.
		host, _, err := net.SplitHostPort(candidate)
		if err != nil {
			host = candidate
		}

		// A non-empty reason means the classifier recognised the host as internal, which is
		// what this branch requires. An empty reason means it did not, and that is the
		// refusal.
		if model.InternalDestinationReason(host) == "" {
			remote = append(remote, candidate)
		}
	}

	if len(remote) == 0 {
		return nil
	}

	return fmt.Errorf(
		"blnk: KAFKA_INSECURE_LOCAL_DEV asserts that Kafka is a local development broker, but "+
			"%s not local: %s. Plaintext is refused. Ledger amounts, identity records and the "+
			"SASL/SCRAM handshake would all travel unencrypted to a remote host, and no later "+
			"rotation undoes a credential that has already crossed the network in the clear. "+
			"Either set KAFKA_TLS_ENABLED=true and configure KAFKA_TLS_CA_FILE for this broker, "+
			"or point KAFKA_BROKERS back at the local stack. KAFKA_INSECURE_LOCAL_DEV is not a "+
			"way to disable TLS for a remote cluster",
		map[bool]string{true: "this broker is", false: "these brokers are"}[len(remote) == 1],
		strings.Join(remote, ", "),
	)
}

// kafkaTLSConfig builds the verified TLS configuration, or returns nil when plaintext
// has been explicitly permitted.
func kafkaTLSConfig(cfg config.KafkaConfig) (*tls.Config, error) {
	if !cfg.TLS.Enabled {
		if !cfg.InsecureLocalDev {
			return nil, errors.New(
				"blnk: KAFKA_TLS_ENABLED is false, so ledger events and the SASL/SCRAM handshake " +
					"would travel in the clear. Enable TLS, or set KAFKA_INSECURE_LOCAL_DEV=true to " +
					"acknowledge that this is a local development broker",
			)
		}

		// THE ACKNOWLEDGEMENT IS SCOPED TO WHAT IT CLAIMS TO BE.
		if err := requireLocalBrokersForPlaintext(cfg.Brokers); err != nil {
			return nil, err
		}

		logrus.Warn(
			"KAFKA_TLS_ENABLED is false and KAFKA_INSECURE_LOCAL_DEV is set: " +
				"connecting to Kafka WITHOUT TLS. Ledger event payloads and SASL credentials are " +
				"not encrypted in transit. This configuration must never be used outside local development",
		)

		return nil, nil
	}

	if cfg.TLS.InsecureSkipVerify && !cfg.InsecureLocalDev {
		return nil, errors.New(
			"blnk: KAFKA_TLS_INSECURE_SKIP_VERIFY disables broker certificate verification, which " +
				"leaves the connection open to an active attacker while appearing encrypted. It is " +
				"permitted only alongside KAFKA_INSECURE_LOCAL_DEV=true",
		)
	}

	tlsConfig := &tls.Config{
		// TLS 1.2 is the floor. Anything earlier has known weaknesses and no reason to be
		// offered to a broker that this deployment controls.
		MinVersion:         tls.VersionTLS12,
		ServerName:         strings.TrimSpace(cfg.TLS.ServerName),
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify, //nolint:gosec // refused above unless KAFKA_INSECURE_LOCAL_DEV is set
	}

	if caFile := strings.TrimSpace(cfg.TLS.CAFile); caFile != "" {
		pem, readErr := os.ReadFile(caFile)
		if readErr != nil {
			return nil, fmt.Errorf("blnk: reading KAFKA_TLS_CA_FILE %q: %w", caFile, readErr)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf(
				"blnk: KAFKA_TLS_CA_FILE %q contains no usable PEM certificate; "+
					"an empty trust pool would fall back to the system roots and silently trust "+
					"a broker this deployment never intended to", caFile)
		}

		tlsConfig.RootCAs = pool
	}

	certFile := strings.TrimSpace(cfg.TLS.CertFile)
	keyFile := strings.TrimSpace(cfg.TLS.KeyFile)

	switch {
	case certFile != "" && keyFile != "":
		certificate, certErr := tls.LoadX509KeyPair(certFile, keyFile)
		if certErr != nil {
			// The paths are named; the key material is not rendered.
			return nil, fmt.Errorf(
				"blnk: loading the Kafka client certificate from KAFKA_TLS_CERT_FILE %q and "+
					"KAFKA_TLS_KEY_FILE %q: %w", certFile, keyFile, certErr)
		}

		tlsConfig.Certificates = []tls.Certificate{certificate}

	case certFile != "" || keyFile != "":
		// Half a client certificate is not a weaker mutual-TLS setup; it is no mutual TLS
		// at all, silently, while the operator believes otherwise.
		return nil, errors.New(
			"blnk: KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE must be set together; " +
				"one without the other yields no client certificate and mutual TLS would not be in effect",
		)
	}

	return tlsConfig, nil
}

// kafkaTransportCredentials selects and validates the credential pair for a role.
func kafkaTransportCredentials(cfg config.KafkaConfig, role KafkaTransportRole) (string, string, error) {
	if role == KafkaTransportRoleAdmin {
		if err := cfg.ValidateSASLAdminCredentials(); err != nil {
			return "", "", err
		}

		// Resolved through the ONE named reading rather than by trimming the fields here.
		user, secret, _ := cfg.SASLAdminCredentials()

		return user, secret, nil
	}

	if err := config.ValidateSASLPair("producer", cfg.SASLUser, cfg.SASLSecret); err != nil {
		return "", "", err
	}

	if producer := strings.TrimSpace(cfg.SASLUser); producer != "" {
		return producer, cfg.SASLSecret, nil
	}

	// No dedicated producer principal. The administrative pair is validated first so that
	// a half-configured one is reported as the configuration defect it is, rather than
	// being silently read as "no admin credentials" and turned into the different
	// diagnosis below.
	if err := cfg.ValidateSASLAdminCredentials(); err != nil {
		return "", "", err
	}

	admin, adminSecret, enabled := cfg.SASLAdminCredentials()
	if !enabled {
		// Neither role is configured. An unauthenticated broker, which the local stack
		// is entitled to be.
		return "", "", nil
	}

	if !cfg.AllowAdminProducer {
		// THE PRINCIPAL IS NAMED AND THE SECRET IS NOT, and the asymmetry is deliberate. A
		// caller logs this error, so the secret must never be in it — but the principal is an
		// identifier, and naming it turns the message from "configure a producer" into
		// "configure a producer INSTEAD OF blnk-kafka-admin", which is what tells an operator
		// with several credentials in play which one the refusal is about.
		return "", "", fmt.Errorf(
			"%w. Kafka brokers and administrative credentials are configured but no producer "+
				"principal is, and publishing as the administrator %q is refused: that principal "+
				"can create topics, alter SCRAM credentials and grant or revoke ACLs, so a leaked "+
				"producer credential would compromise the cluster's authorization state rather "+
				"than merely permit publishing, and the broker's audit trail could no longer tell "+
				"routine publishing from administration. Provision a principal with Write and "+
				"Describe on the Blnk-owned topics — scripts/kafka-provision.sh does this for the "+
				"local stack — and set KAFKA_SASL_USER and KAFKA_SASL_SECRET. To keep publishing "+
				"as the administrator while that is arranged, set KAFKA_ALLOW_ADMIN_PRODUCER=true "+
				"deliberately",
			ErrKafkaProducerCredentialsRequired, admin,
		)
	}

	logrus.WithFields(logrus.Fields{
		"principal": admin,
		"setting":   "KAFKA_ALLOW_ADMIN_PRODUCER",
	}).Warn(
		"SECURITY: the event publisher is authenticating with the KAFKA ADMIN credentials " +
			"because KAFKA_ALLOW_ADMIN_PRODUCER is set and no dedicated producer principal is " +
			"configured. The admin principal can create topics, alter SCRAM credentials and " +
			"manage ACLs, so every ledger event is being published at the privilege level that " +
			"controls the cluster's authorization state. This is an upgrade-window compatibility " +
			"setting, not a configuration to run on: provision a producer principal with Write " +
			"and Describe on the Blnk-owned topics, set KAFKA_SASL_USER and KAFKA_SASL_SECRET, " +
			"and clear KAFKA_ALLOW_ADMIN_PRODUCER",
	)

	return admin, adminSecret, nil
}

// saslProbeValue is a fixed, non-secret placeholder used ONLY to establish which of the two
// configured SASL values is malformed. It never leaves the process: it is not sent to a
// broker, not stored, and not logged. It is deliberately a plain ASCII literal, so
// preparing it can only ever succeed and the probe's verdict is unambiguous.
const saslProbeValue = "sasl-probe"

// saslCredentialError reports that the configured SASL credentials cannot be prepared,
// and does so WITHOUT wrapping the underlying library error.
func saslCredentialError(role KafkaTransportRole, username string) error {
	userVar, secretVar := "KAFKA_SASL_USER", "KAFKA_SASL_SECRET"
	if role == KafkaTransportRoleAdmin {
		userVar, secretVar = "KAFKA_SASL_ADMIN_USER", "KAFKA_SASL_ADMIN_SECRET"
	}

	if _, err := scram.Mechanism(scram.SHA512, username, saslProbeValue); err != nil {
		return fmt.Errorf(
			"blnk: %s %q cannot be prepared as a SASL/SCRAM-SHA-512 credential; "+
				"SASLprep rejects it, typically because of a prohibited or unassigned Unicode code point",
			userVar, username,
		)
	}

	return fmt.Errorf(
		"blnk: %s cannot be prepared as a SASL/SCRAM-SHA-512 credential for user %q; "+
			"SASLprep rejects it, typically because of a prohibited or unassigned Unicode code point. "+
			"The secret is deliberately omitted from this message",
		secretVar, username,
	)
}
