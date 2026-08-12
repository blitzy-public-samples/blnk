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

package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/internal/logsafe"
)

// MaxMetricsSubscriberBudget is the supported ceiling on how many subscribers one
// metrics collection tick may measure.
const MaxMetricsSubscriberBudget = 5000

// setKafkaDefaults fills only the unset Kafka topic-geometry values. Brokers,
// SASLAdminUser and SASLAdminSecret are never defaulted: an empty broker list selects
// the no-op event publisher, and the two SASL fields are credentials.
func (cnf *Configuration) setKafkaDefaults() {
	if cnf.Kafka.TopicPrefix == "" {
		cnf.Kafka.TopicPrefix = defaultKafka.TopicPrefix
	}
	if cnf.Kafka.MinPartitions == 0 {
		cnf.Kafka.MinPartitions = defaultKafka.MinPartitions
	}
	if cnf.Kafka.ReplicationFactor == 0 {
		cnf.Kafka.ReplicationFactor = defaultKafka.ReplicationFactor
	}
	// The lag-sweep budget: defaulted when unset or nonsensical, and CLAMPED above.
	if cnf.Kafka.MetricsSubscriberBudget <= 0 {
		cnf.Kafka.MetricsSubscriberBudget = defaultKafka.MetricsSubscriberBudget
	}
	if cnf.Kafka.MetricsSubscriberBudget > MaxMetricsSubscriberBudget {
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Kafka.MetricsSubscriberBudget,
			"applied":    MaxMetricsSubscriberBudget,
		}).Warn(
			"EVENT_METRICS_SUBSCRIBER_BUDGET is above the supported ceiling and has been clamped; " +
				"each subscriber measured costs two broker round trips per authorised topic and one " +
				"exported gauge series per topic, so an unbounded budget makes a single collection " +
				"tick unbounded in both duration and cardinality",
		)
		cnf.Kafka.MetricsSubscriberBudget = MaxMetricsSubscriberBudget
	}
	cnf.Kafka.Brokers = normalizeBrokers(cnf.Kafka.Brokers)
	// NOT DEFAULTED TO Brokers, deliberately. Falling back would hand every subscriber
	// Blnk's internal broker addresses in a 200 response, which is the failure
	// SubscriberBrokers exists to prevent; issuance refuses instead. Normalised the same
	// way so that KAFKA_SUBSCRIBER_BROKERS="" and "," — both of which envconfig parses
	// into a non-empty slice carrying nothing usable — read as "not configured" rather
	// than as a list of blank endpoints.
	cnf.Kafka.SubscriberBrokers = normalizeBrokers(cnf.Kafka.SubscriberBrokers)

	// THE ENFORCEMENT MODE FAILS CLOSED, and normalising it here rather than at each
	// reader is what makes that single-valued. An unrecognised value — a typo, a mode
	// borrowed from another product, a value a future release adds and this build does not
	// understand — is forced to "none", because the only safe reading of "I do not know
	// what enforcement this names" is "nothing is enforcing". Silence would let a
	// misspelled mode read as enforcement by whichever reader happened to compare
	// case-sensitively.
	cnf.Kafka.KeyScopeEnforcement = strings.ToLower(strings.TrimSpace(cnf.Kafka.KeyScopeEnforcement))
	switch cnf.Kafka.KeyScopeEnforcement {
	case "":
		cnf.Kafka.KeyScopeEnforcement = KeyScopeEnforcementNone
	case KeyScopeEnforcementNone, KeyScopeEnforcementBrokerGateway:
	default:
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Kafka.KeyScopeEnforcement,
			"applied":    KeyScopeEnforcementNone,
			"variable":   "KAFKA_KEY_SCOPE_ENFORCEMENT",
			"supported":  []string{KeyScopeEnforcementNone, KeyScopeEnforcementBrokerGateway},
		}).Warn(
			"KAFKA_KEY_SCOPE_ENFORCEMENT names no supported enforcement mode and has been reset " +
				"to none, so credential issuance will refuse every subscriber that records a " +
				"partition_key_prefix; an unrecognised mode must not read as enforcement",
		)

		cnf.Kafka.KeyScopeEnforcement = KeyScopeEnforcementNone
	}
	cnf.Kafka.KeyScopeGatewayBrokers = normalizeBrokers(cnf.Kafka.KeyScopeGatewayBrokers)

	// DECLARED-BUT-UNUSABLE IS ANNOUNCED, because it is the one combination that looks
	// configured and enforces nothing. KeyScopeGateway already refuses it, so issuance
	// behaves correctly either way; without this line the operator's only clue would be a
	// 409 on a request they believe they configured for.
	if cnf.Kafka.KeyScopeEnforcement == KeyScopeEnforcementBrokerGateway {
		if _, active := cnf.Kafka.KeyScopeGateway(); !active {
			// WHICH HALF IS MISSING IS NAMED, because the two remedies are different variables
			// and a single "not active" line sends an operator to check the one that is already
			// correct. The bootstrap list is reported first: it is the older requirement, and a
			// deployment upgrading into the attestation requirement will usually have it.
			if _, _, attestable := cnf.Kafka.KeyScopeAttestation(); !attestable {
				logrus.WithFields(logrus.Fields{
					"attestation_url_set":   strings.TrimSpace(cnf.Kafka.KeyScopeGatewayAttestationURL) != "",
					"attestation_token_set": strings.TrimSpace(cnf.Kafka.KeyScopeGatewayAttestationToken) != "",
					"variables": []string{
						"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL",
						"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TOKEN",
					},
				}).Warn(
					"KAFKA_KEY_SCOPE_ENFORCEMENT is broker_gateway but no usable attestation " +
						"endpoint is configured, so key-scope enforcement is NOT active and issuance " +
						"will refuse every subscriber that records a partition_key_prefix. Blnk will " +
						"not mint a credential declaring a key boundary it could not ask the " +
						"component to confirm: set KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL to the " +
						"component's control endpoint — https, or http only for a loopback host — " +
						"and KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TOKEN to the bearer credential it " +
						"authenticates Blnk with. The wire contract is in docs/kafka-operations.md",
				)
			}

			if len(cnf.Kafka.KeyScopeGatewayBrokers) == 0 ||
				brokerListsEqual(cnf.Kafka.KeyScopeGatewayBrokers, cnf.Kafka.Brokers) {
				logrus.WithFields(logrus.Fields{
					"gateway_broker_count": len(cnf.Kafka.KeyScopeGatewayBrokers),
					"variable":             "KAFKA_KEY_SCOPE_GATEWAY_BROKERS",
				}).Warn(
					"KAFKA_KEY_SCOPE_ENFORCEMENT is broker_gateway but no distinct gateway bootstrap " +
						"list is configured, so key-scope enforcement is NOT active and issuance will " +
						"refuse every subscriber that records a partition_key_prefix; set " +
						"KAFKA_KEY_SCOPE_GATEWAY_BROKERS to the enforcing endpoint, which must differ " +
						"from KAFKA_BROKERS",
				)
			}
		}
	}

	// Credentials are trimmed here rather than in trimWhitespace because a stray newline
	// from an environment file turns a correct SASL username into one the broker has never
	// heard of, and the resulting authentication failure reads exactly like a wrong
	// password. trimWhitespace is a fixed list of long-standing fields; extending it would
	// change behaviour for those, so the Kafka fields are normalised in their own domain
	// setter instead.
	cnf.Kafka.SASLUser = strings.TrimSpace(cnf.Kafka.SASLUser)
	cnf.Kafka.SASLSecret = strings.TrimSpace(cnf.Kafka.SASLSecret)
	cnf.Kafka.SASLAdminUser = strings.TrimSpace(cnf.Kafka.SASLAdminUser)
	cnf.Kafka.SASLAdminSecret = strings.TrimSpace(cnf.Kafka.SASLAdminSecret)
	cnf.Kafka.TLS.CAFile = strings.TrimSpace(cnf.Kafka.TLS.CAFile)
	cnf.Kafka.TLS.CertFile = strings.TrimSpace(cnf.Kafka.TLS.CertFile)
	cnf.Kafka.TLS.KeyFile = strings.TrimSpace(cnf.Kafka.TLS.KeyFile)
	cnf.Kafka.TLS.ServerName = strings.TrimSpace(cnf.Kafka.TLS.ServerName)

	// A HALF-CONFIGURED pair is reported HERE, at load, and not only when a transport is
	// eventually built.
	if err := cnf.Kafka.ValidateSASLAdminCredentials(); err != nil {
		logrus.WithField("cause", logsafe.Cause(err)).Warn(
			"the Kafka administrative SASL credential is half-configured; topic assurance, " +
				"subscriber provisioning and the offset reads behind reconciliation will all be " +
				"refused rather than run unauthenticated",
		)
	}
	if err := ValidateSASLPair("producer", cnf.Kafka.SASLUser, cnf.Kafka.SASLSecret); err != nil {
		logrus.WithField("cause", logsafe.Cause(err)).Warn(
			"the Kafka producer SASL credential is half-configured; event publishing will be " +
				"refused rather than run unauthenticated",
		)
	}

	cnf.warnOnUnusableKafkaTopicGeometry()
}

// warnOnUnusableKafkaTopicGeometry reports a topic geometry that will be silently
// corrected at the point of use.
func (cnf *Configuration) warnOnUnusableKafkaTopicGeometry() {
	if cnf.Kafka.MinPartitions < 0 {
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Kafka.MinPartitions,
			"will_use":   defaultKafka.MinPartitions,
		}).Warn(
			"KAFKA_MIN_PARTITIONS is negative, which cannot be provisioned; topic assurance will " +
				"raise it to the required minimum, so the configured value will not be the value " +
				"in effect",
		)
	}

	if cnf.Kafka.ReplicationFactor < 0 {
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Kafka.ReplicationFactor,
			"will_use":   defaultKafka.ReplicationFactor,
		}).Warn(
			"KAFKA_REPLICATION_FACTOR is negative; a negative replica count is read by Kafka as " +
				"'use the broker default', so topics would be created with a durability that was " +
				"never chosen",
		)
	}
}

// kafkaTopicNameCutset is the set of characters Kafka permits in a topic name:
const kafkaTopicNameCutset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"0123456789._-"

// MaxKafkaTopicNameLength is the longest topic name a Kafka broker will accept.
const MaxKafkaTopicNameLength = 249

// maxComposedTopicSuffixLength is the length of the LONGEST suffix event_topics.go
// appends to the prefix — ".transactions" plus the ".dlt" dead-letter sibling, 17
// characters. Reserving it here is what makes the prefix budget below correct for every
// name in the catalogue rather than only for the shortest one.
const maxComposedTopicSuffixLength = len(".transactions") + len(DeadLetterTopicSuffixForValidation)

// DeadLetterTopicSuffixForValidation duplicates event_topics.go's DeadLetterTopicSuffix
// so that this package can size the prefix budget without importing the root package,
// which imports this one. It is validated against the real constant by
// TestKafkaTopicPrefixBudgetMatchesTopicSuffix in the root package's tests.
const DeadLetterTopicSuffixForValidation = ".dlt"

// MaxKafkaTopicPrefixLength is the longest KAFKA_TOPIC_PREFIX that can compose a legal
// topic name for every category in the catalogue.
const MaxKafkaTopicPrefixLength = MaxKafkaTopicNameLength - maxComposedTopicSuffixLength

// MaxHistoricalTopicPrefixes bounds KAFKA_HISTORICAL_TOPIC_PREFIXES.
const MaxHistoricalTopicPrefixes = 4

// validateKafkaTopicPrefix REFUSES a topic prefix that cannot compose a legal Kafka
// topic name.
func (cnf *Configuration) validateKafkaTopicPrefix() error {
	// The composer's own normalisation, applied to the STORED value so that the prefix
	// this validates is the prefix that will be used. topicPrefixFrom in event_topics.go
	// trims the same cutset; doing it here as well means the two cannot disagree.
	prefix := strings.Trim(cnf.Kafka.TopicPrefix, " \t\n\v\f\r.")
	if prefix == "" {
		// Blank resolves to the default downstream. Leaving the field as configured would
		// make the effective prefix depend on which code path read it, so it is set here.
		cnf.Kafka.TopicPrefix = defaultKafka.TopicPrefix
	} else {
		cnf.Kafka.TopicPrefix = prefix

		if err := validateKafkaTopicPrefixValue("KAFKA_TOPIC_PREFIX", prefix); err != nil {
			return err
		}
	}

	return cnf.validateKafkaHistoricalTopicPrefixes()
}

// validateKafkaTopicPrefixValue applies the length and character rules a topic prefix
// must satisfy, whatever variable it arrived in.
func validateKafkaTopicPrefixValue(variable, prefix string) error {
	if len(prefix) > MaxKafkaTopicPrefixLength {
		return fmt.Errorf(
			"%s is %d characters, which is longer than the %d a topic prefix may "+
				"be: Kafka refuses any topic name over %d characters and the longest name composed "+
				"from the prefix is \"<prefix>.transactions%s\"",
			variable, len(prefix), MaxKafkaTopicPrefixLength, MaxKafkaTopicNameLength,
			DeadLetterTopicSuffixForValidation,
		)
	}

	seen := map[rune]struct{}{}
	var offenders []string
	for _, char := range prefix {
		if strings.ContainsRune(kafkaTopicNameCutset, char) {
			continue
		}
		if _, already := seen[char]; already {
			continue
		}
		seen[char] = struct{}{}
		// QuoteRune, never the raw rune: this string reaches an error message and from
		// there a log record, and the whole point of refusing the value is that it may
		// carry newlines or terminal control sequences.
		offenders = append(offenders, strconv.QuoteRune(char))
	}

	if len(offenders) == 0 {
		return nil
	}

	return fmt.Errorf(
		"%s contains %d character(s) Kafka does not permit in a topic name "+
			"(%s); only letters, digits, '.', '_' and '-' are legal, and every topic composed "+
			"from this prefix would be refused by the broker, so events would be captured and "+
			"never delivered",
		variable, len(offenders), strings.Join(offenders, ", "),
	)
}

// validateKafkaHistoricalTopicPrefixes normalises and checks the historical-prefix
// allowlist, and rewrites the field so every reader sees the normalised list.
func (cnf *Configuration) validateKafkaHistoricalTopicPrefixes() error {
	if len(cnf.Kafka.HistoricalTopicPrefixes) == 0 {
		cnf.Kafka.HistoricalTopicPrefixes = nil

		return nil
	}

	current := strings.Trim(cnf.Kafka.TopicPrefix, " \t\n\v\f\r.")
	if current == "" {
		current = defaultKafka.TopicPrefix
	}

	seen := map[string]struct{}{current: {}}
	normalised := make([]string, 0, len(cnf.Kafka.HistoricalTopicPrefixes))

	for _, raw := range cnf.Kafka.HistoricalTopicPrefixes {
		prefix := strings.Trim(raw, " \t\n\v\f\r.")
		if prefix == "" {
			continue
		}
		if _, already := seen[prefix]; already {
			continue
		}

		if err := validateKafkaTopicPrefixValue("KAFKA_HISTORICAL_TOPIC_PREFIXES", prefix); err != nil {
			return err
		}

		seen[prefix] = struct{}{}
		normalised = append(normalised, prefix)
	}

	if len(normalised) > MaxHistoricalTopicPrefixes {
		return fmt.Errorf(
			"KAFKA_HISTORICAL_TOPIC_PREFIXES declares %d distinct prefixes, which is more than "+
				"the %d permitted: every prefix in the list has a writer pre-created for each of "+
				"its topics on every process start, so an unbounded list is memory spent on "+
				"topics nothing writes to. This list is meant to be DRAINED — remove a prefix "+
				"once no non-terminal and no replayable outbox row still names it",
			len(normalised), MaxHistoricalTopicPrefixes,
		)
	}

	if len(normalised) == 0 {
		cnf.Kafka.HistoricalTopicPrefixes = nil

		return nil
	}

	cnf.Kafka.HistoricalTopicPrefixes = normalised

	return nil
}

// ErrProducerPrincipalRequired reports that a deployment configured an ADMINISTRATIVE
// Kafka principal but no dedicated producer principal.
var ErrProducerPrincipalRequired = errors.New(
	"a dedicated Kafka producer principal is required: set KAFKA_SASL_USER and " +
		"KAFKA_SASL_SECRET. The event publisher must not authenticate with " +
		"KAFKA_SASL_ADMIN_USER, which can create topics, mint SCRAM credentials and " +
		"rewrite ACLs, because publishing every ledger event as that principal turns a " +
		"leaked producer credential into full control of the cluster's authorization " +
		"state. Provision a producer principal with Write and Describe on the Blnk-owned " +
		"topics only",
)

// ProducerSASL returns the SASL identity the steady-state event publisher must
// authenticate as, and reports the one misconfiguration that has no safe answer.
//
// Returns:
//   - user, secret string: the dedicated producer credentials. Both empty means no SASL
//     at all, which is legitimate on a broker that requires none.
//   - adminOnly bool: true when an administrative principal is configured and no
//     producer principal is. The caller must treat this as fatal, not as a credential
//     to use.
func (cnf *Configuration) ProducerSASL() (user, secret string, adminOnly bool) {
	if cnf.Kafka.SASLUser != "" || cnf.Kafka.SASLSecret != "" {
		return cnf.Kafka.SASLUser, cnf.Kafka.SASLSecret, false
	}

	if cnf.Kafka.SASLAdminUser == "" && cnf.Kafka.SASLAdminSecret == "" {
		return "", "", false
	}

	return "", "", true
}

// SASLAdminCredentials is THE one reading of the ADMINISTRATIVE SASL/SCRAM credential,
// and every component that authenticates to a broker as the administrator must resolve
// it through this method rather than inspecting the two fields itself.
//
// Returns:
//   - user string: the trimmed principal, empty when SASL is not configured.
//   - secret string: the trimmed secret, empty when SASL is not configured. Never log
//     this value.
//   - enabled bool: true only when BOTH values are present.
func (k KafkaConfig) SASLAdminCredentials() (user, secret string, enabled bool) {
	user = strings.TrimSpace(k.SASLAdminUser)
	secret = strings.TrimSpace(k.SASLAdminSecret)

	if user == "" || secret == "" {
		return "", "", false
	}

	return user, secret, true
}

// OwnedTopicPrefixes returns every topic namespace this deployment owns: the configured
// prefix first, then each historical prefix in declared order.
//
// Returns:
//   - []string: one or more distinct, non-blank prefixes, configured prefix first.
func (k KafkaConfig) OwnedTopicPrefixes() []string {
	current := strings.Trim(k.TopicPrefix, " \t\n\v\f\r.")
	if current == "" {
		current = defaultKafka.TopicPrefix
	}

	prefixes := make([]string, 0, 1+len(k.HistoricalTopicPrefixes))
	prefixes = append(prefixes, current)

	seen := map[string]struct{}{current: {}}
	for _, raw := range k.HistoricalTopicPrefixes {
		prefix := strings.Trim(raw, " \t\n\v\f\r.")
		if prefix == "" {
			continue
		}
		if _, already := seen[prefix]; already {
			continue
		}
		seen[prefix] = struct{}{}
		prefixes = append(prefixes, prefix)
	}

	return prefixes
}

// SubscriberFacingBrokers returns the bootstrap list to HAND TO A SUBSCRIBER, and
// reports whether an operator has advertised one at all.
//
// Returns:
//   - brokers []string: a copy of KAFKA_SUBSCRIBER_BROKERS, nil when it is unset.
//   - advertised bool: true exactly when a non-empty list was configured. It is
//     retained as a second return value rather than folded into the nil check so that
//     callers written against the old fallback contract cannot silently reinterpret an
//     empty list as a usable one.
func (k KafkaConfig) SubscriberFacingBrokers() (brokers []string, advertised bool) {
	normalized := normalizeBrokers(k.SubscriberBrokers)
	if len(normalized) == 0 {
		return nil, false
	}

	brokers = make([]string, len(normalized))
	copy(brokers, normalized)

	return brokers, true
}

// KeyScopeEnforcementNone is the shipped default: NOTHING evaluates record keys.
const KeyScopeEnforcementNone = "none"

// KeyScopeEnforcementBrokerGateway declares that subscriber connections are terminated
// by a gateway that authorises record keys against each principal's granted prefix.
const KeyScopeEnforcementBrokerGateway = "broker_gateway"

// KeyScopeGateway resolves the key-scope enforcement point for subscriber credentials.
//
// Returns:
//   - brokers []string: a copy of the gateway bootstrap list, nil unless enforcement is
//     active. Never the broker list.
//   - active bool: true only when the mode is broker_gateway AND the gateway list is
//     non-empty AND it differs from the internal broker list.
func (k KafkaConfig) KeyScopeGateway() (brokers []string, active bool) {
	if !strings.EqualFold(strings.TrimSpace(k.KeyScopeEnforcement), KeyScopeEnforcementBrokerGateway) {
		return nil, false
	}

	gateway := normalizeBrokers(k.KeyScopeGatewayBrokers)
	if len(gateway) == 0 {
		return nil, false
	}

	if brokerListsEqual(gateway, k.Brokers) {
		return nil, false
	}

	// THE CONTROL ENDPOINT IS PART OF THE DECLARATION, not an optional extra. Enforcement
	// is "active" here only when Blnk can ASK the component to confirm the boundary,
	// because everything active-ness unlocks is downstream of that confirmation: issuance
	// stops refusing key-scoped rows, and the credential response declares the scope
	// enforced. A mode and a bootstrap address are assertions a deployment makes about
	// itself; an authenticated attestation is evidence.
	if _, _, attestable := k.KeyScopeAttestation(); !attestable {
		return nil, false
	}

	brokers = make([]string, len(gateway))
	copy(brokers, gateway)

	return brokers, true
}

// KeyScopeAttestation resolves the key-authorising component's CONTROL endpoint and the
// credential Blnk presents to it.
//
// Returns:
//   - endpoint string: the trimmed URL, empty unless attestable.
//   - token string: the bearer credential, empty unless attestable. Never logged.
//   - attestable bool: true only when all four conditions above hold.
func (k KafkaConfig) KeyScopeAttestation() (endpoint string, token string, attestable bool) {
	if !strings.EqualFold(strings.TrimSpace(k.KeyScopeEnforcement), KeyScopeEnforcementBrokerGateway) {
		return "", "", false
	}

	endpoint = strings.TrimSpace(k.KeyScopeGatewayAttestationURL)
	token = strings.TrimSpace(k.KeyScopeGatewayAttestationToken)

	if endpoint == "" || token == "" {
		return "", "", false
	}

	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return "", "", false
	}

	switch strings.ToLower(parsed.Scheme) {
	case "https":
	case "http":
		if !isLoopbackHostname(parsed.Hostname()) {
			return "", "", false
		}
	default:
		return "", "", false
	}

	return endpoint, token, true
}

// KeyScopeAttestationTimeout is how long a single attestation or revocation call may
// take.
//
// Returns:
//   - time.Duration: always positive, never above MaxKeyScopeAttestationTimeout.
func (k KafkaConfig) KeyScopeAttestationTimeout() time.Duration {
	if k.KeyScopeGatewayAttestationTimeoutMS <= 0 {
		return DEFAULT_KEY_SCOPE_ATTESTATION_TIMEOUT_MS * time.Millisecond
	}

	timeout := time.Duration(k.KeyScopeGatewayAttestationTimeoutMS) * time.Millisecond
	if timeout > MaxKeyScopeAttestationTimeout {
		return MaxKeyScopeAttestationTimeout
	}

	return timeout
}

// brokerListsEqual reports whether two bootstrap lists name the same endpoints in the
// same order.
func brokerListsEqual(left, right []string) bool {
	first := normalizeBrokers(left)
	second := normalizeBrokers(right)

	if len(first) != len(second) {
		return false
	}

	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}

	return true
}

// isLoopbackHostname reports whether a URL host is a loopback address or the loopback
// name.
func isLoopbackHostname(hostname string) bool {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return false
	}

	if strings.EqualFold(hostname, "localhost") {
		return true
	}

	if ip := net.ParseIP(hostname); ip != nil {
		return ip.IsLoopback()
	}

	return false
}

// ValidateSASLAdminCredentials reports a half-configured administrative credential.
//
// Returns:
//   - error: non-nil only for a half-configured pair. Both-empty and both-set are valid
//     and return nil.
func (k KafkaConfig) ValidateSASLAdminCredentials() error {
	return ValidateSASLPair("admin", k.SASLAdminUser, k.SASLAdminSecret)
}

// ValidateSASLPair rejects a half-configured SASL credential.
//
// Parameters:
//   - role string: "producer" or "admin", used only to name the offending pair in the
//     error.
//   - user, secret string: the configured pair. The secret is never echoed.
//
// Returns:
//   - error: non-nil when exactly one of the two is set.
func ValidateSASLPair(role, user, secret string) error {
	user = strings.TrimSpace(user)
	secret = strings.TrimSpace(secret)

	userVar, secretVar := saslEnvNames(role)

	switch {
	case user == "" && secret == "":
		return nil
	case user == "":
		// The DANGEROUS half. A component that ignored a secret with no username would connect
		// anonymously while looking configured, so this arm is the reason the function exists
		// rather than an afterthought. The secret is never echoed.
		return fmt.Errorf(
			"kafka %s SASL: %s is set but %s is empty; SASL/SCRAM needs both. Without a principal "+
				"the secret cannot be used and the connection would be anonymous. Set the user, or "+
				"clear both to reach a broker that has no SASL listener", role, secretVar, userVar,
		)
	case secret == "":
		return fmt.Errorf(
			"kafka %s SASL: %s is set to %q but %s is empty; SASL/SCRAM needs both. Set the "+
				"secret, or clear both to reach a broker that has no SASL listener",
			role, userVar, user, secretVar,
		)
	default:
		return nil
	}
}

// saslEnvNames maps a role onto the two environment variables that configure it.
func saslEnvNames(role string) (userVar, secretVar string) {
	if role == "admin" {
		return "KAFKA_SASL_ADMIN_USER", "KAFKA_SASL_ADMIN_SECRET"
	}

	return "KAFKA_SASL_USER", "KAFKA_SASL_SECRET"
}

// normalizeBrokers trims surrounding whitespace from each broker address and drops
// empty entries, preserving the configured order.
func normalizeBrokers(brokers []string) []string {
	if len(brokers) == 0 {
		return brokers
	}

	normalized := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			continue
		}
		normalized = append(normalized, broker)
	}
	return normalized
}

// setRelayDefaults fills the unset relay retry values and BOUNDS the attempt count.
func (cnf *Configuration) setRelayDefaults() {
	// THE THREE RETRY VALUES ARE NORMALISED HERE, AND ONLY HERE.
	switch {
	case cnf.Relay.MaxRetryAttempts < 0:
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Relay.MaxRetryAttempts,
			"applied":    defaultRelay.MaxRetryAttempts,
			"variable":   "RELAY_MAX_RETRY_ATTEMPTS",
		}).Warn(
			"relay max_retry_attempts is negative, which cannot be a retry budget; the default " +
				"is being applied instead, so events are retried the documented number of times " +
				"before being dead-lettered",
		)
		cnf.Relay.MaxRetryAttempts = defaultRelay.MaxRetryAttempts
	case cnf.Relay.MaxRetryAttempts == 0:
		cnf.Relay.MaxRetryAttempts = defaultRelay.MaxRetryAttempts
	case cnf.Relay.MaxRetryAttempts > MaxRelayRetryAttempts:
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Relay.MaxRetryAttempts,
			"applied":    MaxRelayRetryAttempts,
			"maximum":    MaxRelayRetryAttempts,
			"variable":   "RELAY_MAX_RETRY_ATTEMPTS",
		}).Warn(
			"relay max_retry_attempts exceeds the supported maximum and is being clamped; " +
				"the attempt number is an exported metric label and its domain is fixed",
		)
		cnf.Relay.MaxRetryAttempts = MaxRelayRetryAttempts
	}

	switch {
	case cnf.Relay.RetryBaseBackoffMS < 0:
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Relay.RetryBaseBackoffMS,
			"applied":    defaultRelay.RetryBaseBackoffMS,
			"variable":   "RELAY_RETRY_BASE_BACKOFF_MS",
		}).Warn(
			"relay retry_base_backoff_ms is negative, which cannot be a delay; the default is " +
				"being applied instead, so a retry storm cannot spend the whole retry budget " +
				"inside one poll interval",
		)
		cnf.Relay.RetryBaseBackoffMS = defaultRelay.RetryBaseBackoffMS
	case cnf.Relay.RetryBaseBackoffMS == 0:
		cnf.Relay.RetryBaseBackoffMS = defaultRelay.RetryBaseBackoffMS
	}

	switch {
	case cnf.Relay.RetryMaxBackoffMS < 0:
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Relay.RetryMaxBackoffMS,
			"applied":    defaultRelay.RetryMaxBackoffMS,
			"variable":   "RELAY_RETRY_MAX_BACKOFF_MS",
		}).Warn(
			"relay retry_max_backoff_ms is negative, which cannot be a delay cap; the default is " +
				"being applied instead",
		)
		cnf.Relay.RetryMaxBackoffMS = defaultRelay.RetryMaxBackoffMS
	case cnf.Relay.RetryMaxBackoffMS == 0:
		cnf.Relay.RetryMaxBackoffMS = defaultRelay.RetryMaxBackoffMS
	}

	// RETENTION IS NOT DEFAULTED, and the asymmetry with the three values above is the
	// point. Those three have a correct answer that the requirement fixes, so an unset
	// value is filled in. A retention period has no correct answer this code can know — it
	// depends on jurisdiction, audit programme and any legal hold in force — and getting
	// it wrong deletes ledger-adjacent evidence.
	if cnf.Relay.EventRetentionDays < 0 {
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Relay.EventRetentionDays,
			"variable":   "RELAY_EVENT_RETENTION_DAYS",
		}).Warn(
			"relay event_retention_days is negative, which would place the retention cutoff in the " +
				"future and delete every delivered event; retention is being disabled instead",
		)
		cnf.Relay.EventRetentionDays = 0
	}

	// REPAIR CAPACITY. All three are filled in when unset and normalised when nonsensical,
	// because every one of them has a correct answer this code can supply and none of them
	// means "off" at zero:
	if cnf.Relay.RepairBatchSize <= 0 {
		cnf.Relay.RepairBatchSize = defaultRelay.RepairBatchSize
	}
	if cnf.Relay.RepairMaxBatchesPerTick <= 0 {
		cnf.Relay.RepairMaxBatchesPerTick = defaultRelay.RepairMaxBatchesPerTick
	}
	if cnf.Relay.RepairConcurrency <= 0 {
		cnf.Relay.RepairConcurrency = defaultRelay.RepairConcurrency
	}

	// PURGE CAPACITY. Unlike the retention PERIOD above, these two DO have a correct
	// answer this code can supply, so an unset value is filled in rather than left to mean
	// "off": a batch size of zero would delete nothing at all, which is a silently broken
	// sweeper rather than a disabled one. Retention is switched off by the period, in one
	// place, and these only decide how fast it is enforced.
	if cnf.Relay.EventRetentionBatchSize <= 0 {
		cnf.Relay.EventRetentionBatchSize = defaultRelay.EventRetentionBatchSize
	}

	// The batch CEILING distinguishes unset from unbounded, because an unset int is zero
	// and both readings of zero cannot be honoured. Zero DEFAULTS — the per-sweep bound is
	// what protects a live relay from a single sweep attempting an entire historical
	// backlog, and a deployment that never mentioned this setting must keep that
	// protection. Asking for no ceiling is spelled EventRetentionUnboundedSweep, and any
	// negative is normalised onto it so a reader downstream has one value to recognise
	// rather than a sign to test.
	switch {
	case cnf.Relay.EventRetentionMaxBatchesPerSweep == 0:
		cnf.Relay.EventRetentionMaxBatchesPerSweep = defaultRelay.EventRetentionMaxBatchesPerSweep
	case cnf.Relay.EventRetentionMaxBatchesPerSweep < 0:
		cnf.Relay.EventRetentionMaxBatchesPerSweep = EventRetentionUnboundedSweep
	}

	// Reported when retention is ON, because capacity only matters if something is
	// deleting, and reported as ROWS PER HOUR rather than as its two factors: that is the
	// number an operator compares against their arrival rate, and a capacity left as two
	// factors to multiply is one nobody checks until the backlog grows.
	if cnf.Relay.EventRetentionDays > 0 {
		fields := logrus.Fields{
			"batch_size":  cnf.Relay.EventRetentionBatchSize,
			"max_batches": cnf.Relay.EventRetentionMaxBatchesPerSweep,
		}

		if cnf.Relay.EventRetentionMaxBatchesPerSweep > 0 {
			fields["purge_capacity_rows_per_hour"] =
				cnf.Relay.EventRetentionMaxBatchesPerSweep * cnf.Relay.EventRetentionBatchSize
		} else {
			fields["purge_capacity_rows_per_hour"] = "unbounded"
		}

		logrus.WithFields(fields).Info(
			"event outbox retention is enabled; compare the purge capacity against your event " +
				"arrival rate, because capacity below arrivals means the table grows without bound " +
				"however short the retention period is",
		)
	}
	// TWO PUBLISHED SPELLINGS OF ONE CEILING, AND ONE OF THEM IS CANONICAL.
	if cnf.Relay.SubscriberMetricsBudget < 0 {
		logrus.WithFields(logrus.Fields{
			"configured": cnf.Relay.SubscriberMetricsBudget,
			"variable":   "RELAY_SUBSCRIBER_METRICS_BUDGET",
			"using":      defaultRelay.SubscriberMetricsBudget,
		}).Warn(
			"relay subscriber_metrics_budget is negative, which would leave every subscriber's " +
				"consumer lag unmeasured; the default is being used instead",
		)
		cnf.Relay.SubscriberMetricsBudget = 0
	}

	switch {
	case cnf.Relay.SubscriberMetricsBudget == 0:
		// Unset under this spelling: adopt whatever setKafkaDefaults settled on, which is
		// either the operator's EVENT_METRICS_SUBSCRIBER_BUDGET or the shipped default.
		cnf.Relay.SubscriberMetricsBudget = cnf.Kafka.MetricsSubscriberBudget
	case cnf.Kafka.MetricsSubscriberBudget != defaultKafka.MetricsSubscriberBudget &&
		cnf.Kafka.MetricsSubscriberBudget != cnf.Relay.SubscriberMetricsBudget:
		// Both spellings were set, to different values. Neither can be silently discarded, so
		// the disagreement is reported naming both variables and both values, and the
		// CANONICAL spelling is applied. It then falls through to the clamp below, so the
		// value that wins is subject to the same ceiling as one set on its own.
		logrus.WithFields(logrus.Fields{
			"relay_subscriber_metrics_budget": cnf.Relay.SubscriberMetricsBudget,
			"event_metrics_subscriber_budget": cnf.Kafka.MetricsSubscriberBudget,
			"applied":                         cnf.Relay.SubscriberMetricsBudget,
		}).Warn(
			"RELAY_SUBSCRIBER_METRICS_BUDGET and EVENT_METRICS_SUBSCRIBER_BUDGET name the same " +
				"consumer-lag sweep ceiling and were set to different values; " +
				"RELAY_SUBSCRIBER_METRICS_BUDGET has been applied to both, because it is the " +
				"canonical spelling — EVENT_METRICS_SUBSCRIBER_BUDGET is an accepted alias of it",
		)

		fallthrough
	default:
		// Set here and either agreeing with the other spelling or the only one set: this is
		// the value that must reach the collector, clamped by the same ceiling.
		if cnf.Relay.SubscriberMetricsBudget > MaxMetricsSubscriberBudget {
			logrus.WithFields(logrus.Fields{
				"configured": cnf.Relay.SubscriberMetricsBudget,
				"applied":    MaxMetricsSubscriberBudget,
				"variable":   "RELAY_SUBSCRIBER_METRICS_BUDGET",
			}).Warn(
				"relay subscriber_metrics_budget is above the supported ceiling and has been " +
					"clamped; each subscriber measured costs two broker round trips per authorised " +
					"topic and one exported gauge series per topic",
			)
			cnf.Relay.SubscriberMetricsBudget = MaxMetricsSubscriberBudget
		}
		cnf.Kafka.MetricsSubscriberBudget = cnf.Relay.SubscriberMetricsBudget
	}

}

// EventRetentionPeriod returns the configured retention period as a duration, and zero
// when retention is disabled.
//
// Returns:
//   - time.Duration: the retention period, or 0 when retention is disabled.
func (cnf *Configuration) EventRetentionPeriod() time.Duration {
	if cnf == nil || cnf.Relay.EventRetentionDays <= 0 {
		return 0
	}

	return time.Duration(cnf.Relay.EventRetentionDays) * 24 * time.Hour
}
