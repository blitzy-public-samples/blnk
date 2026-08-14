// Copyright 2024 Blnk Finance Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// event_keyscope_gateway.go is the CONTROL-PLANE integration with the key-authorising
// component a deployment declares in front of its Kafka brokers.
package blnk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// keyScopeAttestationMediaType is the content type of both request and response bodies.
const keyScopeAttestationMediaType = "application/json"

// keyScopeAttestationMaxResponseBytes caps how much of a gateway's response is read.
const keyScopeAttestationMaxResponseBytes = 4096

// keyScopeAttestationPrincipalParam is the query parameter a revocation names the principal in.
const keyScopeAttestationPrincipalParam = "principal"

// ErrKeyScopeGatewayNotConfigured is returned by the constructor when the deployment has not
// declared a usable attestation endpoint.
var ErrKeyScopeGatewayNotConfigured = errors.New(
	"event subscriber: no key-scope enforcement gateway is configured",
)

// KeyScopeBinding is what Blnk asks the gateway to hold and confirm for one principal.
type KeyScopeBinding struct {
	// Principal is the SASL/SCRAM identity the gateway will see authenticate. It is the
	// subscriber's derived Kafka principal, which is what ties the gateway's binding to the
	// broker's own credential.
	Principal string `json:"principal"`

	// SubscriberID is carried for the gateway's own logs and dashboards. It is NOT the
	// identity: the principal above is, and a component keying its bindings off this field
	// would key them off a value that is not what authenticates.
	SubscriberID string `json:"subscriber_id"`

	// PartitionKeyPrefix is the boundary itself: the gateway must return only records whose
	// message key carries it. This is the field the attestation compares byte-for-byte.
	PartitionKeyPrefix string `json:"partition_key_prefix"`

	// AuthorizedTopics is the topic set the row authorises, sent so the component can refuse
	// a fetch on a topic outside it rather than relying solely on the broker — defence in
	// depth, since the principal holds Describe and no Read on those topics.
	AuthorizedTopics []string `json:"authorized_topics"`

	// ConsumerGroupPrefix is the group namespace reserved for this subscriber, sent for the
	// same reason.
	ConsumerGroupPrefix string `json:"consumer_group_prefix"`
}

// keyScopeAttestation is the gateway's answer.
type keyScopeAttestation struct {
	// KeyScopeEnforced is the component asserting that it applies key scopes. Anything other
	// than true is a refusal, including its absence, which decodes to false.
	KeyScopeEnforced bool `json:"key_scope_enforced"`

	// Principal is echoed back so a component cannot confirm a binding for somebody else —
	// a proxy that ignored the request body and always answered "yes" would fail here.
	Principal string `json:"principal"`

	// PartitionKeyPrefix is echoed back and compared byte-for-byte with what was sent. This
	// is the field that makes the attestation about THIS boundary rather than about the
	// component's general willingness.
	PartitionKeyPrefix string `json:"partition_key_prefix"`

	// Detail is optional, operator-facing, and used only in Blnk's own log line. It is
	// bounded and sanitised before it is logged, and it never reaches an API response: it
	// comes from a component Blnk does not control, so it is treated exactly as a broker
	// error string is.
	Detail string `json:"detail,omitempty"`
}

// KeyScopeGatewayClient verifies and maintains key-scope bindings at the declared component.
type KeyScopeGatewayClient interface {
	// AttestBinding registers a binding and requires the component to confirm it. It returns
	// nil ONLY when the component answered 2xx, asserted enforcement, echoed the principal,
	// and echoed the prefix byte-for-byte.
	AttestBinding(ctx context.Context, binding KeyScopeBinding) error

	// RevokeBinding withdraws a binding. It is called when a subscriber is deregistered, so
	// the component stops holding a scope for a principal that no longer exists.
	RevokeBinding(ctx context.Context, principal string) error

	// Health reports whether the component answers and asserts key-scope enforcement, without
	// naming any principal. It is the start-up and runbook probe.
	Health(ctx context.Context) error

	// Endpoint returns the configured control endpoint, for logging. It never includes the
	// bearer token, which is carried in a header and is not part of the URL.
	Endpoint() string
}

// keyScopeGateway is the HTTP implementation of KeyScopeGatewayClient.
type keyScopeGateway struct {
	endpoint string
	token    string
	timeout  time.Duration
	client   *http.Client
}

// NewKeyScopeGatewayClient builds the client from live configuration, or reports that
// none is declared.
//
// Parameters:
//   - cnf *config.Configuration: the live configuration. A nil configuration declares
//     nothing.
//
// Returns:
//   - KeyScopeGatewayClient: the client, nil when no gateway is declared.
//   - error: ErrKeyScopeGatewayNotConfigured when nothing usable is declared.
func NewKeyScopeGatewayClient(cnf *config.Configuration) (KeyScopeGatewayClient, error) {
	if cnf == nil {
		return nil, ErrKeyScopeGatewayNotConfigured
	}

	endpoint, token, attestable := cnf.Kafka.KeyScopeAttestation()
	if !attestable {
		return nil, ErrKeyScopeGatewayNotConfigured
	}

	timeout := cnf.Kafka.KeyScopeAttestationTimeout()

	return &keyScopeGateway{
		endpoint: endpoint,
		token:    token,
		timeout:  timeout,
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(req *http.Request, _ []*http.Request) error {
				return fmt.Errorf(
					"key-scope gateway: refusing to follow a redirect to %q; the control request "+
						"carries a bearer credential and must reach only the configured endpoint",
					redactedRedirectTarget(req),
				)
			},
			Transport: &http.Transport{
				MaxIdleConns:        4,
				MaxIdleConnsPerHost: 2,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}, nil
}

// Endpoint returns the configured control endpoint.
func (g *keyScopeGateway) Endpoint() string {
	if g == nil {
		return ""
	}

	return g.endpoint
}

// AttestBinding registers a binding and requires the component to confirm it.
//
// Parameters:
//   - ctx context.Context: the caller's context. Bounded further by the attestation
//     timeout.
//   - binding KeyScopeBinding: the binding, derived entirely from the registry row.
//
// Returns:
//   - error: nil when the component attested the exact binding; otherwise a typed
//     ErrSubscriberKeyScopeUnattested carrying a bounded, non-disclosing detail, with
//     the retryable flag set only for a transport-level failure.
func (g *keyScopeGateway) AttestBinding(ctx context.Context, binding KeyScopeBinding) error {
	if g == nil {
		return ErrKeyScopeGatewayNotConfigured
	}

	if err := binding.validate(); err != nil {
		return err
	}

	body, err := json.Marshal(binding)
	if err != nil {
		// Marshalling a struct of strings and a string slice cannot fail in practice; the
		// branch exists so a future field cannot make it fail silently.
		return g.refusal(binding, "the key-scope binding could not be encoded", false, err)
	}

	response, err := g.do(ctx, http.MethodPost, g.endpoint, body)
	if err != nil {
		// TRANSPORT, so retryable: nothing has been minted, and a component that was briefly
		// unreachable may answer on the next attempt.
		return g.refusal(binding, "the key-scope enforcement gateway could not be reached", true, err)
	}

	payload, readErr := readBoundedBody(response)
	if readErr != nil {
		return g.refusal(binding, "the key-scope enforcement gateway's response could not be read", true, readErr)
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return g.refusal(
			binding,
			fmt.Sprintf(
				"the key-scope enforcement gateway refused the binding with HTTP %d",
				response.StatusCode,
			),
			// A 5xx is worth repeating; a 4xx is a decision, and repeating it changes nothing.
			response.StatusCode >= http.StatusInternalServerError,
			fmt.Errorf("key-scope gateway: attestation returned HTTP %d", response.StatusCode),
		)
	}

	var attested keyScopeAttestation
	if err := json.Unmarshal(payload, &attested); err != nil {
		return g.refusal(
			binding, "the key-scope enforcement gateway's attestation could not be decoded", false, err,
		)
	}

	if err := attested.matches(binding); err != nil {
		return g.refusal(binding, attestationMismatchMessage, false, err)
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(binding.SubscriberID),
		"principal_hash":     subscriberLogLabel(binding.Principal),
		"gateway_endpoint":   g.endpoint,
		"gateway_detail":     sanitizeLogValue(attested.Detail, maxLoggedFilterLength),
	}).Info(
		"event subscriber: the declared key-scope enforcement gateway attested this principal's " +
			"recorded partition key prefix, so a key-scoped credential may be minted",
	)

	return nil
}

// RevokeBinding withdraws a binding at the gateway.
//
// Parameters:
//   - ctx context.Context: the caller's context, typically a bounded cleanup context.
//   - principal string: the subscriber's Kafka principal.
//
// Returns:
//   - error: non-nil when the component answered neither 2xx nor 404, or could not be
//     reached.
func (g *keyScopeGateway) RevokeBinding(ctx context.Context, principal string) error {
	if g == nil {
		return ErrKeyScopeGatewayNotConfigured
	}

	principal = strings.TrimSpace(principal)
	if principal == "" {
		return errors.New("key-scope gateway: revoking a binding requires a principal")
	}

	target, err := appendPrincipalQuery(g.endpoint, principal)
	if err != nil {
		return err
	}

	response, err := g.do(ctx, http.MethodDelete, target, nil)
	if err != nil {
		return fmt.Errorf(
			"key-scope gateway: revoking the binding at %s failed: %w", g.endpoint, err,
		)
	}

	if _, readErr := readBoundedBody(response); readErr != nil {
		// The body is not needed; draining it is what lets the connection be reused. A read
		// failure after a status has been received is not itself a revocation failure.
		logrus.WithFields(logrus.Fields{
			"gateway_endpoint": g.endpoint,
			"error_class":      kafkaErrorClassField("key_scope_gateway_revoke", readErr),
		}).Debug("event subscriber: draining the key-scope gateway's revocation response failed")
	}

	if response.StatusCode == http.StatusNotFound {
		return nil
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf(
			"key-scope gateway: revoking the binding at %s returned HTTP %d",
			g.endpoint, response.StatusCode,
		)
	}

	return nil
}

// Health reports whether the declared component answers and asserts key-scope
// enforcement.
//
// Parameters:
//   - ctx context.Context: the caller's context.
//
// Returns:
//   - error: nil when the component answered 2xx with key_scope_enforced true.
func (g *keyScopeGateway) Health(ctx context.Context) error {
	if g == nil {
		return ErrKeyScopeGatewayNotConfigured
	}

	response, err := g.do(ctx, http.MethodGet, g.endpoint, nil)
	if err != nil {
		return fmt.Errorf("key-scope gateway: %s did not answer a health probe: %w", g.endpoint, err)
	}

	payload, readErr := readBoundedBody(response)
	if readErr != nil {
		return fmt.Errorf(
			"key-scope gateway: %s answered a health probe with an unreadable body: %w",
			g.endpoint, readErr,
		)
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf(
			"key-scope gateway: %s answered a health probe with HTTP %d",
			g.endpoint, response.StatusCode,
		)
	}

	var reported keyScopeAttestation
	if err := json.Unmarshal(payload, &reported); err != nil {
		return fmt.Errorf(
			"key-scope gateway: %s answered a health probe with a body that is not the "+
				"documented attestation shape: %w", g.endpoint, err,
		)
	}

	if !reported.KeyScopeEnforced {
		return fmt.Errorf(
			"key-scope gateway: %s answered a health probe reporting key_scope_enforced=false, so "+
				"it is not currently an enforcement point", g.endpoint,
		)
	}

	return nil
}

// do issues one authenticated request against the control endpoint.
func (g *keyScopeGateway) do(
	ctx context.Context, method, target string, body []byte,
) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	callCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}

	request, err := http.NewRequestWithContext(callCtx, method, target, reader)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Authorization", "Bearer "+g.token)
	request.Header.Set("Accept", keyScopeAttestationMediaType)
	if len(body) > 0 {
		request.Header.Set("Content-Type", keyScopeAttestationMediaType)
	}

	return g.client.Do(request)
}

// refusal builds the typed error every attestation failure answers with.
func (g *keyScopeGateway) refusal(
	binding KeyScopeBinding, message string, retryable bool, cause error,
) error {
	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(binding.SubscriberID),
		"principal_hash":     subscriberLogLabel(binding.Principal),
		"gateway_endpoint":   g.endpoint,
		"retryable":          retryable,
		"error_class":        kafkaErrorClassField("key_scope_gateway_attestation", cause),
	}).Error(
		"event subscriber: the declared key-scope enforcement gateway did not attest this " +
			"principal's recorded partition key prefix, so no credential was minted: " + message,
	)

	return apierror.NewAPIError(
		apierror.ErrSubscriberKeyScopeUnattested,
		message+". No credential was issued. Blnk will not mint a credential declaring a key "+
			"boundary the declared enforcement point did not confirm: check "+
			"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL and its bearer token, confirm the component "+
			"holds the same partition key prefix as the registry row, or clear the prefix and "+
			"narrow authorized_topics, which the broker enforces in full",
		NewSubscriberErrorDetail(message, binding.SubscriberID, retryable),
	)
}

// attestationMismatchMessage is the operator-facing summary of the check that matters most.
const attestationMismatchMessage = "the key-scope enforcement gateway attested a different " +
	"principal or a different partition key prefix from the one recorded on this subscriber"

// validate refuses a binding that could not honestly be attested.
func (b KeyScopeBinding) validate() error {
	if strings.TrimSpace(b.Principal) == "" {
		return errors.New("key-scope gateway: attesting a binding requires a principal")
	}

	if b.PartitionKeyPrefix == "" {
		return errors.New(
			"key-scope gateway: attesting a binding requires the recorded partition key prefix; " +
				"a subscriber with no key scope has no binding to attest",
		)
	}

	return nil
}

// matches reports whether the component attested THIS binding.
func (a keyScopeAttestation) matches(binding KeyScopeBinding) error {
	if !a.KeyScopeEnforced {
		return errors.New(
			"key-scope gateway: the component answered key_scope_enforced=false for this principal",
		)
	}

	if a.Principal != binding.Principal {
		return errors.New(
			"key-scope gateway: the component attested a different principal from the one sent, so " +
				"its answer does not describe this subscriber",
		)
	}

	if a.PartitionKeyPrefix != binding.PartitionKeyPrefix {
		return fmt.Errorf(
			"key-scope gateway: the component holds a partition key prefix of %d bytes for this "+
				"principal and the registry records one of %d; a boundary that differs by any byte "+
				"is a different boundary",
			len(a.PartitionKeyPrefix), len(binding.PartitionKeyPrefix),
		)
	}

	return nil
}

// readBoundedBody reads and closes a response body, up to
// keyScopeAttestationMaxResponseBytes.
func readBoundedBody(response *http.Response) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, nil
	}

	defer func() {
		// The close error is deliberately discarded: the bytes are already read, and a close
		// failure changes neither the status nor the payload this call is judged on.
		_ = response.Body.Close()
	}()

	return io.ReadAll(io.LimitReader(response.Body, keyScopeAttestationMaxResponseBytes))
}

// appendPrincipalQuery adds the principal to the control endpoint's query string.
func appendPrincipalQuery(endpoint, principal string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("key-scope gateway: the configured control endpoint does not parse: %w", err)
	}

	query := parsed.Query()
	query.Set(keyScopeAttestationPrincipalParam, principal)
	parsed.RawQuery = query.Encode()

	return parsed.String(), nil
}

// redactedRedirectTarget renders a refused redirect's destination for an error message.
func redactedRedirectTarget(request *http.Request) string {
	if request == nil || request.URL == nil {
		return "unknown"
	}

	host := request.URL.Host
	if host == "" {
		return "unknown"
	}

	if hostname, _, err := net.SplitHostPort(host); err == nil && hostname != "" {
		host = hostname
	}

	return request.URL.Scheme + "://" + host
}

// keyScopeBindingFor composes the binding for a subscriber row.
func keyScopeBindingFor(subscriber *model.EventSubscriber) KeyScopeBinding {
	if subscriber == nil {
		return KeyScopeBinding{}
	}

	groupPrefix, err := SubscriberConsumerGroupNamespace(subscriber.SubscriberID)
	if err != nil {
		// An identifier no namespace can be derived from has already been refused upstream by
		// requireSubscriberIdentifier. Sending an empty group prefix rather than failing here
		// keeps this a pure composer: the attestation's own comparisons do not involve it.
		groupPrefix = ""
	}

	return KeyScopeBinding{
		Principal:           subscriber.KafkaPrincipal,
		SubscriberID:        subscriber.SubscriberID,
		PartitionKeyPrefix:  SubscriberPartitionKeyPrefix(subscriber),
		AuthorizedTopics:    append([]string(nil), subscriber.AuthorizedTopics...),
		ConsumerGroupPrefix: groupPrefix,
	}
}
